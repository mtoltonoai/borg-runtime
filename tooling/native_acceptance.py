#!/usr/bin/env python3
"""Read-only, fail-closed native deployment audit and restart receipt comparison.

This is an operational check, not a substitute for the live acceptance demos.
Only GETs are issued. Transcripts, instructions, credentials, arguments and tool
result bodies are never written to evidence; completed results are SHA256'd.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SCHEMA = 1
HOSTS = ("agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4")
LIVE = {"ready", "running", "retrying", "stopping"}
RESERVING = {"reserved", "provisioning", "launching", "ready", "running", "retrying", "waiting", "stopping", "needs_resolution"}
SUCCESS = {"succeeded", "completed"}
PROFILE_FIELDS = ("id", "model", "reasoning_effort", "max_output_tokens", "context_window_tokens",
                  "max_turns", "max_retries", "retry_initial_ms", "retry_max_ms", "retry_jitter_percent",
                  "config_source", "profile_source")
BINDING_FIELDS = ("host_id", "workspace_cwd", "workspace_mcp_url", "lifecycle_mcp_url", "runtime_url", "generation")
SESSION_FIELDS = ("id", "agent_id", "task", "status", "phase", "step", "driver_running", "generation",
                  "created_at", "updated_at", "last_progress_at") + BINDING_FIELDS
HOST_FIELDS = ("host_id", "capacity", "runtime_url", "workspace_root", "revision", "last_seen", "enabled", "status", "runtime_ready", "ttl_seconds", "fresh")
DISPATCH_FIELDS = ("request_id", "task_id", "agent_id", "host_id", "runtime_url", "session_id",
                   "workspace_cwd", "workspace_mcp_url", "generation", "status", "updated_at", "created_at")
TASK_FIELDS = ("id", "assignee", "status", "parent_id", "blocked_on_kind", "blocked_on_ref", "monitor_exempt", "updated_at")
METADATA_FIELDS = ("harness", "role", "host", "branch", "workspace_cwd", "native_attempt", "recovery_task_id",
                   "parent_session_id", "request_id", "source_task_id", "incident_kind")
DEMONSTRATIONS = [
    "Create a new Board identity and change its effective role/tool profile without host JSON edits.",
    "A native parent's successful lifecycle tool receipt leads to an independent same-identity child.",
    "A controlled capacity-aware dispatch batch executes useful work on all four hosts.",
    "Distinct workspaces pass real sentinel cross-read checks (paths alone are not isolation proof).",
    "Services are enabled; real process restarts preserve DB/session IDs and completed tool receipts.",
    "Inject a failure; an independent monitor creates owned triage and a cause repair/regression is deployed.",
    "A tested native-authored commit is integrated, deployed at the reported revision, and published to GitHub.",
    "Inspect Board/UI against live session/task/host/failure state.",
]


def pick(value, fields):
    result = {key: value[key] for key in fields if key in value}
    for key in result:
        if key.endswith("_url") and isinstance(result[key], str):
            try:
                url = urllib.parse.urlsplit(result[key])
                host = url.hostname or ""
                if ":" in host:
                    host = "[" + host + "]"
                port = ":" + str(url.port) if url.port else ""
                result[key] = urllib.parse.urlunsplit((url.scheme, host + port, url.path, "", ""))
            except ValueError:
                result[key] = "<invalid URL>"
    return result


def obj(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def epoch(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        if result > 100_000_000_000:  # Native epoch milliseconds; Board uses RFC3339.
            result /= 1000
        return result if math.isfinite(result) else None
    try:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.replace(tzinfo=stamp.tzinfo or dt.timezone.utc).timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def fresh(value, now, age):
    stamp = epoch(value)
    return stamp is not None and -30 <= now - stamp <= age


def task_id(value):
    if isinstance(value, bool):
        return None
    match = re.fullmatch(r"(?:task[_:]|#)?([1-9][0-9]*)", str(value))
    return int(match[1]) if match else None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def binding(session, field):
    return session.get(field, obj(session.get("placement")).get(field))


def sanitize_profile(value):
    out = pick(value, PROFILE_FIELDS)
    if isinstance(value.get("retry_policy"), dict):
        out["retry_policy"] = pick(value["retry_policy"], ("mode", "max_backoff_ms", "jitter", "respect_retry_after"))
    return out


def sanitize_task(value):
    out = pick(value, TASK_FIELDS)
    out["metadata"] = pick(obj(value.get("metadata")), METADATA_FIELDS)
    block = obj(value.get("blocked_on"))
    out["blocked_on"] = pick(block, ("kind", "target", "ref"))
    # Evidence records that an explanatory note exists, never arbitrary note text.
    out["blocked_note_present"] = bool(block.get("note") or value.get("blocked_on_note"))
    return out


def sanitize_session(value):
    out = pick(value, SESSION_FIELDS)
    out["placement"] = pick(obj(value.get("placement")), BINDING_FIELDS)
    out["metadata"] = pick(obj(value.get("metadata")), METADATA_FIELDS)
    return out


def tool_receipt(value):
    out = pick(value, ("call_id", "operation_id", "name", "status"))
    if value.get("status") in SUCCESS and "result" in value:
        out["result_sha256"] = digest(value["result"])
    return out


class ReadError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Do not leak bearer headers to another origin.


class Reader:
    def __init__(self, timeout=10, max_bytes=64 * 1024 * 1024):
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.opener = urllib.request.build_opener(NoRedirect)

    @staticmethod
    def base(value):
        parsed = urllib.parse.urlsplit(value)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment):
            raise ReadError("base URL must be HTTP(S) without credentials, query or fragment")
        return value.rstrip("/")

    def get(self, base, path, token=None):
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(self.base(base) + path, headers=headers, method="GET")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(self.max_bytes + 1)
                if len(raw) > self.max_bytes:
                    raise ReadError("JSON response exceeds evidence size bound")
                if response.headers.get_content_type() != "application/json":
                    raise ReadError("expected application/json, not HTML/readiness fallback")
                return json.loads(raw)
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            raise ReadError(f"HTTP {code}") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise ReadError("transport unavailable or timed out") from None
        except (ValueError, UnicodeError):
            raise ReadError("invalid JSON response") from None


def rows(value, key):
    if isinstance(value, dict):
        value = value.get(key)
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ReadError(f"expected {key} array of objects")
    return value


def collect(board_url, runtimes, project=1, reader=None, board_token=None, native_token=None, tools=True):
    reader = reader or Reader()
    snapshot = {"schema_version": SCHEMA, "collected_at": time.time(), "project_id": project,
                "board": {}, "runtimes": {}, "errors": []}

    def read(label, base, path, token, transform=lambda value: value):
        try:
            return transform(reader.get(base, path, token))
        except ReadError as error:
            snapshot["errors"].append({"source": label, "error": str(error)})
            return None

    board = snapshot["board"]
    board["health"] = read("board.health", board_url, "/api/health", board_token,
                           lambda v: pick(obj(v), ("ok", "db", "commit")))
    board["tasks"] = read("board.tasks", board_url, f"/api/tasks?project_id={project}", board_token,
                          lambda v: [sanitize_task(r) for r in rows(v, "tasks")])
    # REST task listing is unpaginated; the MCP compact task feed is paginated.
    # Agent REST is paginated. Do not silently accept a full first page.
    agents = []
    offset = 0
    seen = set()
    while True:
        page = read("board.agents", board_url, f"/api/agents?limit=1000&offset={offset}", board_token,
                    lambda v: rows(v, "agents"))
        if page is None:
            agents = None
            break
        ids = [v.get("id") for v in page]
        if any(i is None or i in seen for i in ids) or len(set(ids)) != len(ids):
            snapshot["errors"].append({"source": "board.agents", "error": "duplicate IDs or ignored pagination"})
            agents = None
            break
        seen.update(ids)
        for value in page:
            agent = pick(value, ("id", "lifecycle_intent", "retired"))
            agent["metadata"] = pick(obj(value.get("metadata")), METADATA_FIELDS)
            agents.append(agent)
        if len(page) < 1000:
            break
        offset += len(page)
    board["agents"] = agents
    board["hosts"] = read("board.hosts", board_url, "/api/fleet/hosts", board_token,
                          lambda v: [pick(r, HOST_FIELDS) for r in rows(v, "hosts")])
    dispatches = []
    offset = 0
    seen = set()
    while True:
        page = read("board.dispatches", board_url, f"/api/fleet/dispatches?limit=1000&offset={offset}", board_token,
                    lambda v: rows(v, "dispatches"))
        if page is None:
            dispatches = None
            break
        ids = [v.get("request_id") for v in page]
        if any(i is None or i in seen for i in ids) or len(set(ids)) != len(ids):
            snapshot["errors"].append({"source": "board.dispatches", "error": "duplicate request IDs or ignored pagination"})
            dispatches = None
            break
        seen.update(ids)
        dispatches.extend(pick(value, DISPATCH_FIELDS) for value in page)
        if len(page) < 1000:
            break
        offset += len(page)
    board["dispatches"] = dispatches
    for host, url in runtimes.items():
        runtime = snapshot["runtimes"][host] = {}
        runtime["health"] = read(f"{host}.health", url, "/health", native_token,
                                  lambda v: pick(obj(v), ("status", "service", "host_id", "revision", "instance_id", "config_source", "ready", "draining")))
        runtime["agents"] = read(f"{host}.agents", url, "/api/agents", native_token,
                                  lambda v: [sanitize_profile(r) for r in rows(v, "agents")])
        runtime["sessions"] = read(f"{host}.sessions", url, "/api/sessions", native_token,
                                    lambda v: [sanitize_session(r) for r in rows(v, "sessions")])
        runtime["tools"] = {}
        if tools:
            for session in runtime["sessions"] or []:
                sid = session.get("id")
                if not isinstance(sid, str):
                    snapshot["errors"].append({"source": f"{host}.sessions", "error": "missing session id"})
                    continue
                runtime["tools"][sid] = read(f"{host}.tools.{sid}", url,
                    f"/api/sessions/{urllib.parse.quote(sid, safe='')}/tools", native_token,
                    lambda v: [tool_receipt(r) for r in rows(v, "tools")])
    snapshot["finished_at"] = time.time()
    return snapshot


def audit(snapshot, expected_hosts=HOSTS, revision=None, now=None, max_host_age=90):
    now = time.time() if now is None else now
    checks = []

    def check(name, ok, details):
        checks.append({"check": name, "status": "pass" if ok else "fail", "details": details})

    check("collection", not snapshot.get("errors"), snapshot.get("errors", []))
    check("expected_revision", bool(revision and re.fullmatch(r"[0-9a-f]{40}", revision)),
          "Supply the full integrated commit; readiness alone does not identify deployed code.")
    check("snapshot_fresh", fresh(snapshot.get("finished_at", snapshot.get("collected_at")), now, max_host_age),
          "Evidence must be current; a saved old healthy snapshot is not current readiness.")
    board = obj(snapshot.get("board"))
    health = obj(board.get("health"))
    check("board_ready", health.get("ok") is True and health.get("db") is True,
          "Board /api/health must verify its database, not return the SPA.")
    if revision:
        check("board_revision", health.get("commit") == revision, {"expected": revision, "actual": health.get("commit")})
    host_rows = board.get("hosts") or []
    hosts = {h.get("host_id"): h for h in host_rows}
    check("unique_hosts", len(host_rows) == len(hosts) and None not in hosts,
          "The host registry cannot contain duplicate/missing host IDs.")
    runtimes = obj(snapshot.get("runtimes"))
    agents = {a.get("id"): a for a in board.get("agents") or []}
    required_agents = {a for a, v in agents.items() if obj(v.get("metadata")).get("harness") == "native" and not v.get("retired") and v.get("lifecycle_intent") not in {"paused", "retired"}}
    check("native_identities", bool(required_agents), sorted(required_agents))
    all_sessions = []
    for host, runtime in runtimes.items():
        all_sessions.extend((host, s) for s in runtime.get("sessions") or [])
    sessions = {(host, s.get("id")): s for host, s in all_sessions}
    dispatches = board.get("dispatches") or []
    live_host = {}
    for host in expected_hosts:
        row = hosts.get(host, {})
        runtime = runtimes.get(host, {})
        h = obj(runtime.get("health"))
        capacity = row.get("capacity")
        valid_capacity = isinstance(capacity, int) and not isinstance(capacity, bool) and capacity > 0
        ready = (h.get("status") == "ok" and h.get("host_id") == host
                 and h.get("ready", True) is True and h.get("draining", False) is False)
        host_age = row.get("ttl_seconds", max_host_age)
        if not isinstance(host_age, (int, float)) or isinstance(host_age, bool) or not math.isfinite(host_age) or host_age <= 0:
            host_age = 0
        live_host[host] = (ready and fresh(row.get("last_seen"), now, min(max_host_age, host_age))
                           and row.get("runtime_ready") is True and row.get("fresh", True) is True)
        check(f"host:{host}", live_host[host] and valid_capacity and row.get("enabled", True) is not False,
              {"native_ready": ready, "last_seen": row.get("last_seen"), "capacity": capacity})
        if revision:
            check(f"revision:{host}", h.get("revision") == revision and row.get("revision") == revision,
                  {"expected": revision, "native": h.get("revision"), "fleet": row.get("revision")})
        profiles = {p.get("id"): p for p in runtime.get("agents") or []}
        invalid = []
        finite = []
        for agent in required_agents:
            profile = profiles.get(agent, {})
            expected = {"model": "gpt-6-astra", "reasoning_effort": "max", "max_output_tokens": 128000,
                        "context_window_tokens": 1050000, "max_turns": None}
            if any(key not in profile or profile[key] != value for key, value in expected.items()):
                invalid.append(agent)
            unlimited = (("max_retries" in profile and profile["max_retries"] is None)
                         or obj(profile.get("retry_policy")).get("mode") == "unlimited")
            if not unlimited:
                finite.append(agent)
        check(f"profiles:{host}", bool(required_agents) and not invalid, {"missing_or_changed_limits": sorted(invalid)})
        check(f"indefinite_retry:{host}", bool(required_agents) and not finite,
              {"not_explicitly_unlimited": sorted(finite), "note": "u32::MAX is bootstrap mitigation, not unlimited semantics"})
        occupancy = sum(d.get("host_id") == host and d.get("status") in RESERVING for d in dispatches)
        check(f"capacity:{host}", valid_capacity and occupancy <= capacity,
              {"reserved_or_active": occupancy, "configured_capacity": capacity})
    ids = [d.get("request_id") for d in dispatches]
    check("dispatch_idempotency", all(isinstance(i, str) and i for i in ids) and len(ids) == len(set(ids)),
          "Each persisted request_id has exactly one dispatch reservation.")
    dispatch_session_ids = [d.get("session_id") for d in dispatches]
    check("unique_dispatch_sessions", all(isinstance(i, str) and i for i in dispatch_session_ids)
          and len(dispatch_session_ids) == len(set(dispatch_session_ids)),
          "One preallocated session ID belongs to one durable dispatch.")
    live_ids = [s.get("id") for _, s in all_sessions if s.get("status") in LIVE and s.get("driver_running") is True]
    check("single_live_session_owner", len(live_ids) == len(set(live_ids)),
          "A session cannot have live drivers on two hosts/generations.")
    placements = collections.Counter()
    workspace_owners = {}
    bad_bindings = []
    for dispatch in dispatches:
        if dispatch.get("status") not in {"running", "completed", "retrying"}:
            continue
        host, sid = dispatch.get("host_id"), dispatch.get("session_id")
        session = sessions.get((host, sid), {})
        path = binding(session, "workspace_cwd")
        generation = binding(session, "generation")
        valid = (bool(session) and session.get("agent_id") == dispatch.get("agent_id")
                 and task_id(session.get("task")) == task_id(dispatch.get("task_id"))
                 and binding(session, "host_id") == host and isinstance(generation, int) and not isinstance(generation, bool)
                 and generation >= 0 and generation == dispatch.get("generation")
                 and isinstance(path, str) and path.startswith("/") and ".." not in PurePosixPath(path).parts
                 and path != "/" and path == dispatch.get("workspace_cwd"))
        if not valid:
            bad_bindings.append(dispatch.get("request_id"))
            continue
        key = (host, str(PurePosixPath(path)))
        for (other_host, other_path), other_sid in workspace_owners.items():
            if other_host == host and other_sid != sid and (PurePosixPath(path).is_relative_to(other_path)
                    or PurePosixPath(other_path).is_relative_to(path)):
                bad_bindings.append(dispatch.get("request_id"))
        workspace_owners[key] = sid
        placements[host] += 1
    check("session_placement_binding", not bad_bindings, {"invalid_or_reused": bad_bindings})
    check("four_host_dispatch", all(placements[h] > 0 for h in expected_hosts), dict(placements))
    tasks = {t.get("id"): t for t in board.get("tasks") or []}
    coverage = {}

    def covered(tid, seen):
        if tid in seen or tid not in tasks:
            return None
        seen = seen | {tid}
        task = tasks[tid]
        owner = task.get("assignee")
        if not owner or owner not in agents or agents[owner].get("retired"):
            return None
        status = task.get("status")
        if status == "blocked":
            block = obj(task.get("blocked_on"))
            kind = block.get("kind", task.get("blocked_on_kind"))
            target = block.get("target", block.get("ref", task.get("blocked_on_ref")))
            if kind == "operator" or (kind == "external" and task.get("blocked_note_present")) or (kind in {"task", "agent", "team"} and target):
                return "visible_owned_wait"
            return None
        if status not in {"todo", "in_progress"}:
            return None
        related = {tid}
        parent = task.get("parent_id")
        while parent in tasks and parent not in related:
            related.add(parent)
            parent = tasks[parent].get("parent_id")
        for host, session in all_sessions:
            if (session.get("agent_id") == owner and task_id(session.get("task")) in related
                    and session.get("status") in LIVE and session.get("driver_running") is True
                    and obj(runtimes[host].get("health")).get("status") == "ok"):
                return "live_session:" + str(session.get("id"))
        for dispatch in dispatches:
            if (dispatch.get("agent_id") == owner and task_id(dispatch.get("task_id")) in related
                    and dispatch.get("status") in {"reserved", "provisioning", "launching"}
                    and live_host.get(dispatch.get("host_id"))
                    and fresh(dispatch.get("updated_at"), now, max_host_age)):
                return "fresh_dispatch:" + str(dispatch.get("request_id"))
        repair = task_id(obj(task.get("metadata")).get("recovery_task_id"))
        if repair and covered(repair, seen):
            return "owned_recovery:" + str(repair)
        if agents[owner].get("lifecycle_intent") == "paused":
            return "explicitly_paused_owner"
        return None

    for tid, task in tasks.items():
        if task.get("status") in {"todo", "in_progress", "blocked"}:
            coverage[str(tid)] = covered(tid, set())
    check("actionable_task_coverage", bool(tasks) and all(coverage.values()), coverage)
    return {"schema_version": SCHEMA, "scope": "read-only operational audit; live demonstrations still required",
            "ok": all(c["status"] == "pass" for c in checks), "checked_at": now,
            "checks": checks, "demonstrations_required": DEMONSTRATIONS}


def compare_restart(before, after, hosts=HOSTS):
    """Require a changed process ID and identical prior successful tool receipts."""
    failures = []
    retained = 0
    for host in hosts:
        old = obj(before.get("runtimes")).get(host, {})
        new = obj(after.get("runtimes")).get(host, {})
        old_id = obj(old.get("health")).get("instance_id")
        new_id = obj(new.get("health")).get("instance_id")
        if not old_id or not new_id or old_id == new_id:
            failures.append({"host": host, "reason": "no observed process restart"})
        current = {s.get("id"): s for s in new.get("sessions") or []}
        for session in old.get("sessions") or []:
            sid = session.get("id")
            present = current.get(sid)
            if not present or any(present.get(k) != session.get(k) for k in ("agent_id", "task")):
                failures.append({"host": host, "session": sid, "reason": "session identity/task lost"})
                continue
            previous_calls = obj(old.get("tools")).get(sid)
            current_calls = obj(new.get("tools")).get(sid)
            if previous_calls is None or current_calls is None:
                failures.append({"host": host, "session": sid, "reason": "tool receipts unavailable"})
                continue
            by_operation = collections.defaultdict(list)
            for call in current_calls:
                by_operation[call.get("operation_id")].append(call)
            for call in previous_calls:
                if call.get("status") not in SUCCESS:
                    continue
                matches = by_operation.get(call.get("operation_id"), [])
                if (not call.get("operation_id") or not call.get("result_sha256") or len(matches) != 1
                        or matches[0].get("result_sha256") != call["result_sha256"]
                        or matches[0].get("call_id") != call.get("call_id")
                        or matches[0].get("status") not in SUCCESS):
                    failures.append({"host": host, "session": sid, "operation": call.get("operation_id"),
                                     "reason": "completed tool receipt lost, changed or duplicated"})
                else:
                    retained += 1
    if not retained:
        failures.append({"reason": "no completed tool receipts witnessed before and after"})
    if before.get("errors") or after.get("errors"):
        failures.append({"reason": "incomplete snapshot cannot prove restart preservation"})
    return {"ok": not failures, "preserved_completed_tools": retained, "failures": failures,
            "scope": "receipt preservation, not proof that external side effects occurred only once"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--board", default="http://127.0.0.1:8079")
    parser.add_argument("--runtime", action="append", default=[], metavar="HOST=URL")
    parser.add_argument("--expected-host", action="append", dest="hosts")
    parser.add_argument("--project", type=int, default=1)
    parser.add_argument("--expected-revision")
    parser.add_argument("--board-token-env")
    parser.add_argument("--native-token-env")
    parser.add_argument("--snapshot-in", type=Path)
    parser.add_argument("--snapshot-out", type=Path)
    parser.add_argument("--report-out", type=Path)
    parser.add_argument("--before", type=Path, help="Require real restart and completed-result preservation against this snapshot")
    parser.add_argument("--max-host-age", type=float, default=90)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args(argv)
    try:
        runtimes = {}
        for item in args.runtime:
            host, sep, url = item.partition("=")
            if not sep or not host or host in runtimes:
                raise ReadError("--runtime requires unique HOST=URL entries")
            runtimes[host] = Reader.base(url)
        for env in (args.board_token_env, args.native_token_env):
            if env and not os.environ.get(env):
                raise ReadError("requested credential environment variable is unset")
        if args.timeout <= 0 or args.max_host_age <= 0 or not math.isfinite(args.timeout + args.max_host_age):
            raise ReadError("timeouts/ages must be positive finite numbers")
        if args.snapshot_in:
            snapshot = json.loads(args.snapshot_in.read_text())
        else:
            if not runtimes:
                raise ReadError("supply --runtime HOST=URL for each target host")
            snapshot = collect(args.board, runtimes, args.project, Reader(args.timeout),
                               os.environ.get(args.board_token_env) if args.board_token_env else None,
                               os.environ.get(args.native_token_env) if args.native_token_env else None)
        if not isinstance(snapshot, dict) or snapshot.get("schema_version") != SCHEMA:
            raise ReadError("unsupported snapshot schema")
        report = audit(snapshot, args.hosts or HOSTS, args.expected_revision, max_host_age=args.max_host_age)
        if args.before:
            report["restart"] = compare_restart(json.loads(args.before.read_text()), snapshot, args.hosts or HOSTS)
            report["ok"] = report["ok"] and report["restart"]["ok"]
        for path, value in ((args.snapshot_out, snapshot), (args.report_out, report)):
            if path:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 1
    except (ReadError, OSError, ValueError, TypeError) as error:
        # Do not print URL tokens, response bodies, env values or arbitrary exception messages.
        print(json.dumps({"ok": False, "error": str(error) if isinstance(error, ReadError) else "invalid input or evidence file"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
