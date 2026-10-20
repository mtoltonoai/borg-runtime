#!/usr/bin/env python3
"""Record already-bound bootstrap sessions on Board before enabling native Fleet.

Read-only by default. --apply writes only Board host/adoption records. This helper
NEVER provisions a workspace, starts/stops/resumes a session, resolves a tool,
changes a native database, or activates a service. A lost adoption reply is read
back by canonical request key before any idempotent Board retry.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TERMINAL_TASKS = {"done", "cancelled", "icebox"}
TERMINAL_SESSIONS = {"completed", "cancelled"}
# stopped is the legacy safe-drain posture; failed also retains a durable repair
# association. Record both exactly, never resume them or pretend they are running.
ADOPTABLE = {"ready", "running", "retrying", "waiting", "stopping", "stopped", "failed", "needs_resolution"}
BINDING_FIELDS = ("request_id", "session_id", "agent_id", "task_id", "host_id", "runtime_url", "workspace_cwd", "generation")


class ContractError(RuntimeError):
    pass


class HttpError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


def api_url(value):
    if not isinstance(value, str):
        raise ContractError("runtime/Board URL must be text")
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ContractError("URL must be http(s), without credentials/query/fragment")
    base = value.rstrip("/")
    return base if base.endswith("/api") else base + "/api"


def absolute(value, label):
    if not isinstance(value, str) or not Path(value).is_absolute() or ".." in Path(value).parts or "\0" in value:
        raise ContractError(f"{label} must be an absolute path without traversal")
    return value


def metadata(record):
    value = record.get("metadata", {})
    value = json.loads(value) if isinstance(value, str) else value
    if not isinstance(value, dict):
        raise ContractError("metadata must be an object")
    return value


def task_number(value):
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if isinstance(value, str):
        match = re.fullmatch(r"(?:task_|#)?([1-9][0-9]*)", value.strip())
        if match:
            return int(match.group(1))
    return None


def attempt(task):
    value = metadata(task).get("native_attempt", 1)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 1


class Client:
    def __init__(self, *, timeout=10, token_env=None):
        self.timeout = timeout
        self.token = None
        if token_env:
            self.token = os.environ.get(token_env)
            if not self.token:
                raise ContractError(f"required credential environment variable {token_env} is unavailable")

    def request(self, method, url, body=None):
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        raw = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            raw = json.dumps(body).encode()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=raw, headers=headers, method=method), timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise HttpError(error.code, f"{method} returned HTTP {error.code}") from error
        except (OSError, ValueError, urllib.error.URLError) as error:
            raise HttpError(None, f"{method} response unavailable ({type(error).__name__})") from error


def host_specs(manifest):
    hosts = manifest.get("hosts") if isinstance(manifest, dict) else manifest
    if not isinstance(hosts, list) or not hosts:
        raise ContractError("manifest requires a nonempty hosts array")
    seen = set()
    normalized = []
    for item in hosts:
        if not isinstance(item, dict):
            raise ContractError("each manifest host must be an object")
        host = dict(item)
        host_id = host.get("host_id")
        if not isinstance(host_id, str) or not host_id.strip() or host_id in seen:
            raise ContractError("host_id must be nonempty and unique")
        seen.add(host_id)
        host["runtime_url"] = api_url(host["runtime_url"])
        host["probe_url"] = api_url(host.get("probe_url", host["runtime_url"]))
        absolute(host.get("workspace_root"), "workspace_root")
        capacity = host.get("capacity", 4)
        if not isinstance(capacity, int) or isinstance(capacity, bool) or not 0 <= capacity <= 10000:
            raise ContractError("capacity must be an integer 0..10000")
        host["capacity"] = capacity
        host.setdefault("enabled", capacity > 0)
        if not isinstance(host["enabled"], bool):
            raise ContractError("enabled must be boolean")
        ids = host.get("session_ids")
        if ids is not None and (not isinstance(ids, list) or any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids)):
            raise ContractError("session_ids must be distinct nonempty strings")
        normalized.append(host)
    return normalized


def binding(host, session, task):
    if session.get("agent_id") != task.get("assignee"):
        raise ContractError(f"task {task['id']} owner differs from bound session identity")
    if session.get("host_id") != host["host_id"]:
        raise ContractError("bootstrap session is not bound to the original host_id")
    cwd = absolute(session.get("workspace_cwd"), "bound workspace_cwd")
    generation = session.get("generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
        raise ContractError("bootstrap session lacks a nonnegative bound generation")
    runtime = api_url(session.get("runtime_url"))
    if runtime != host["runtime_url"]:
        raise ContractError("bound session runtime_url differs from the advertised host URL; fix profile binding, not the receipt")
    if not isinstance(session.get("id"), str) or not session["id"]:
        raise ContractError("bound session lacks id")
    return {"request_id": f"task:{task['id']}:attempt:{attempt(task)}", "session_id": session["id"],
            "agent_id": session["agent_id"], "task_id": task["id"], "host_id": session["host_id"],
            "runtime_url": runtime, "workspace_cwd": cwd, "generation": generation, "status": session["status"]}


def existing(board, base, key):
    query = urllib.parse.urlencode({"request_id": key, "limit": 2, "offset": 0})
    rows = board.request("GET", base + "/fleet/dispatches?" + query)
    if not isinstance(rows, list) or len(rows) > 1:
        raise ContractError("canonical dispatch lookup is not a unique array")
    return rows[0] if rows else None


def verify(receipt, body):
    mismatches = [key for key in BINDING_FIELDS if receipt.get(key) != body[key]]
    if mismatches:
        raise ContractError("existing adoption has a different binding: " + ", ".join(mismatches))
    if not isinstance(receipt.get("id"), int):
        raise ContractError("adoption response has no durable dispatch id")
    return receipt


def receipt_summary(receipt):
    # Evidence needs bindings/status, not arbitrary task input or free-form
    # metadata copied from someone else's record.
    fields = (*BINDING_FIELDS, "id", "version", "status", "created_at", "updated_at")
    return {field: receipt.get(field) for field in fields}


def adopt_one(board, base, body, retries=3):
    # Reading first makes re-runs safe even if the original process died after
    # commit. Never use absence of an HTTP reply as evidence that nothing wrote.
    receipt = existing(board, base, body["request_id"])
    if receipt is not None:
        return {"receipt": receipt_summary(verify(receipt, body)), "reconciled": True, "posted": False}
    for round_number in range(retries):
        try:
            receipt = board.request("POST", base + "/fleet/dispatches/adopt", body)
            verify(receipt, body)
            durable = existing(board, base, body["request_id"])
            if durable is None:
                raise ContractError("adoption reply was not present in the Board read-back")
            return {"receipt": receipt_summary(verify(durable, body)), "reconciled": False, "posted": True}
        except HttpError as error:
            receipt = existing(board, base, body["request_id"])
            if receipt is not None:
                return {"receipt": receipt_summary(verify(receipt, body)), "reconciled": True, "posted": True}
            if error.status is not None and error.status not in (408, 429) and error.status < 500:
                raise
            if round_number + 1 == retries:
                raise
            time.sleep(.2 * (round_number + 1))
    raise AssertionError("unreachable")


def run(manifest, board_url, *, apply=False, board=None, runtime_factory=None):
    base = api_url(board_url)
    board = board or Client()
    runtime_factory = runtime_factory or (lambda host: Client(token_env=host.get("bearer_token_env")))
    hosts = host_specs(manifest)
    result = {"observed_at": datetime.now(timezone.utc).isoformat(), "applied": apply,
              "hosts": [], "adoptions": [], "skipped": [], "errors": [], "uncovered_terminal_tasks": [],
              "runtime_mutations": 0}
    candidates = []
    seen_sessions, seen_tasks = set(), set()
    # Preflight the complete cohort before any Board writes. A half-bound or
    # duplicate-active-task cohort must not silently enable a second execution.
    for host in hosts:
        runtime = runtime_factory(host)
        health = runtime.request("GET", host["probe_url"].removesuffix("/api") + "/health")
        sessions = runtime.request("GET", host["probe_url"] + "/sessions")
        if not isinstance(sessions, list):
            raise ContractError("native session inventory must be an array")
        selected = host.get("session_ids")
        if selected is not None:
            missing = set(selected) - {s.get("id") for s in sessions}
            if missing:
                raise ContractError("selected bootstrap session absent: " + ", ".join(sorted(missing)))
            sessions = [s for s in sessions if s.get("id") in selected]
        ready = (health.get("status") == "ok" and health.get("service") == "borg-native"
                 and health.get("ready") is True and health.get("host_id") == host["host_id"]
                 and not health.get("draining", False))
        result["hosts"].append({"host_id": host["host_id"], "runtime_url": host["runtime_url"],
                                "workspace_root": host["workspace_root"], "capacity": host["capacity"],
                                "enabled": host["enabled"], "runtime_ready": ready})
        for session in sessions:
            sid = session.get("id")
            if sid in seen_sessions:
                raise ContractError("a bootstrap session appears under multiple hosts")
            seen_sessions.add(sid)
            task_id = task_number(session.get("task"))
            context = {"session_id": sid, "host_id": host["host_id"], "task_id": task_id}
            if task_id is None:
                result["skipped"].append({**context, "reason": "no canonical Board task"})
                continue
            task = board.request("GET", base + f"/tasks/{task_id}")
            if task.get("status") in TERMINAL_TASKS or task.get("archived_at"):
                result["skipped"].append({**context, "reason": "terminal or parked task"})
                continue
            if session.get("status") in TERMINAL_SESSIONS:
                result["skipped"].append({**context, "reason": "terminal session", "status": session["status"]})
                key = f"task:{task_id}:attempt:{attempt(task)}"
                receipt = existing(board, base, key)
                if receipt is None and task.get("status") in ("todo", "in_progress") and metadata(task).get("native_dispatch") is not False:
                    # Do not pretend it was adopted or silently let Fleet replay
                    # completed effects. Report concrete uncovered work for the
                    # rollout/recovery owner to disposition before enablement.
                    result["uncovered_terminal_tasks"].append(context)
                continue
            if session.get("status") not in ADOPTABLE:
                raise ContractError("unknown native session status")
            if task_id in seen_tasks:
                raise ContractError(f"multiple live bootstrap sessions claim task {task_id}")
            seen_tasks.add(task_id)
            body = binding(host, session, task)
            receipt = existing(board, base, body["request_id"])
            if receipt is not None:
                verify(receipt, body)
            candidates.append(body)
    if apply:
        current = board.request("GET", base + "/fleet/hosts")
        current = {h["host_id"]: h for h in current}
        for host in result["hosts"]:
            prior = current.get(host["host_id"])
            heartbeat = {key: host[key] for key in ("capacity", "runtime_url", "workspace_root", "runtime_ready")}
            heartbeat.update(revision=(prior or {}).get("revision", ""),
                             metadata={"source": "bootstrap-adoption", "bootstrap_only": not host["enabled"]})
            url = base + "/fleet/hosts/" + urllib.parse.quote(host["host_id"], safe="")
            try:
                board.request("POST", url + "/heartbeat", heartbeat)
            except HttpError:
                observed = {h["host_id"]: h for h in board.request("GET", base + "/fleet/hosts")}.get(host["host_id"])
                if not observed or any(observed.get(k) != heartbeat[k] for k in ("runtime_url", "workspace_root", "runtime_ready")):
                    raise
            # Preserve operator policy for existing pool hosts. Explicitly keep
            # bootstrap-only builder outside placement; never enable it by drift.
            if not host["enabled"]:
                board.request("PATCH", url, {"enabled": False})
        for body in candidates:
            result["adoptions"].append(adopt_one(board, base, body))
    else:
        result["adoptions"] = [{"planned": body} for body in candidates]
    covered = {body["task_id"] for body in candidates}
    result["uncovered_terminal_tasks"] = [t for t in result["uncovered_terminal_tasks"] if t["task_id"] not in covered]
    result["plan_valid"] = not result["errors"] and not result["uncovered_terminal_tasks"]
    result["safe_to_enable_fleet"] = apply and result["plan_valid"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--board-api", default="http://127.0.0.1:8079/api")
    parser.add_argument("--board-token-env")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = run(json.loads(args.manifest.read_text()), args.board_api, apply=args.apply,
                     board=Client(timeout=args.timeout, token_env=args.board_token_env),
                     runtime_factory=lambda host: Client(timeout=args.timeout, token_env=host.get("bearer_token_env")))
    except (OSError, ValueError, KeyError, ContractError, HttpError) as error:
        result = {"safe_to_enable_fleet": False, "applied": args.apply,
                  "runtime_mutations": 0, "error": str(error), "error_type": type(error).__name__}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result.get("plan_valid", False) else 1


if __name__ == "__main__":
    sys.exit(main())
