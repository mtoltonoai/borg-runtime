#!/usr/bin/env python3
"""Independent, durable native-session recovery control plane (stdlib only).

The Board owns tasks; this SQLite journal only owns observations, idempotent
outbox receipts and retry scheduling. Never stop/resume/replay an original
session here. Fleet schedules a separate diagnostic session for an owned repair
task. A slow live model call, an intentional stop, and an unknown write are
three different observations, not interchangeable restart instructions.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import email.utils
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

LOG = logging.getLogger("borg-recovery")
ACTIVE = {"ready", "running", "retrying", "stopping", "inferring", "executing_tools"}
QUEUED = {"reserved", "provisioning", "launching"}
TERMINAL = {"completed", "failed", "stopped", "cancelled"}
ACTIONABLE = {"todo", "in_progress"}
EXTERNAL_SOURCE = "borg-native-recovery-v1"


def epoch(value: Any) -> float | None:
    """Accept Board RFC3339, Fleet seconds, and Native epoch milliseconds."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            return None
        return value / 1000 if value > 10_000_000_000 else float(value)
    if isinstance(value, str):
        try:
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except (ValueError, OverflowError):
            return None
    return None


def iso(now: float) -> str:
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def object_value(value: Any) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def task_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        for prefix in ("task_", "task:", "#"):
            value = value.removeprefix(prefix)
        if value.isdigit() and int(value) > 0:
            return int(value)
    return None


def api_url(value: str) -> str:
    parts = urllib.parse.urlsplit(value)
    if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username
            or parts.password or parts.query or parts.fragment):
        raise ValueError("service URL must be HTTP(S) without credentials, query or fragment")
    url = value.rstrip("/")
    return url if url.endswith("/api") else url + "/api"


def retry_after(value: str | None, now: float) -> float:
    if value:
        try:
            seconds = float(value)
            if math.isfinite(seconds):
                return max(0, seconds)
        except ValueError:
            try:
                stamp = email.utils.parsedate_to_datetime(value).timestamp()
                return max(0, stamp - now)
            except (ValueError, TypeError, OverflowError):
                pass
    return 0


def backoff(attempt: int, initial: float, cap: float, floor: float = 0,
            random_value: float | None = None) -> float:
    """Unlimited retries; capped exponential with jitter, Retry-After is a floor."""
    value = min(cap, initial * 2 ** min(max(attempt - 1, 0), 30))
    value *= 0.8 + 0.2 * (random.random() if random_value is None else random_value)
    return max(floor, value)


class HttpFailure(Exception):
    def __init__(self, method: str, status: int | None = None, delay: float = 0):
        # Do not log URLs, response bodies, credentials or tool arguments.
        self.status, self.delay = status, delay
        super().__init__(f"{method} HTTP {status}" if status else f"{method} transport/read unavailable")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class JsonHttp:
    def __init__(self, timeout: float = 10, max_bytes: int = 16 * 1024 * 1024,
                 headers_env: dict[str, str] | None = None):
        self.timeout, self.max_bytes = timeout, max_bytes
        self.headers = {"Accept": "application/json"}
        for name, variable in (headers_env or {}).items():
            if name.lower() in {"host", "content-length", "transfer-encoding", "connection"}:
                raise ValueError("unsupported configured HTTP header")
            self.headers[name] = os.environ[variable]
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method: str, url: str, data: Any = None) -> Any:
        body = None if data is None else json.dumps(data, ensure_ascii=True).encode()
        headers = dict(self.headers)
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(self.max_bytes + 1)
                if len(raw) > self.max_bytes:
                    raise HttpFailure(method)
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            delay = retry_after(exc.headers.get("Retry-After"), time.time())
            status = exc.code
            exc.close()
            raise HttpFailure(method, status, delay) from None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            raise HttpFailure(method) from None


class Board:
    def __init__(self, base: str, http: JsonHttp):
        self.base, self.http = api_url(base), http

    def call(self, method: str, path: str, data: Any = None) -> Any:
        return self.http.request(method, self.base + path, data)

    def pages(self, path: str, size: int = 500) -> list[dict]:
        rows, offset = [], 0
        while True:
            separator = "&" if "?" in path else "?"
            page = self.call("GET", f"{path}{separator}limit={size}&offset={offset}")
            if not isinstance(page, list):
                raise HttpFailure("GET")
            rows.extend(page)
            if len(page) < size:
                return rows
            offset += len(page)


@dataclass
class Policy:
    enabled: bool = True
    triage_agent: str = "recovery-engineer"
    fallback_agent: str = "pm"
    missing_session_grace_seconds: float = 120
    runtime_failure_grace_seconds: float = 60
    runtime_failure_observations: int = 3
    dispatch_grace_seconds: float = 180
    progress_stall_seconds: float = 900
    live_wait_notice_seconds: float = 3600
    triage_grace_seconds: float = 300
    retry_initial_seconds: float = 2
    retry_cap_seconds: float = 60
    maintenance: dict = field(default_factory=dict)

    @classmethod
    def from_metadata(cls, *layers: dict) -> "Policy":
        values = {}
        for layer in layers:
            values.update({key: value for key, value in object_value(layer).items()
                           if key in cls.__dataclass_fields__})
        policy = cls(**values)
        if type(policy.enabled) is not bool:
            raise ValueError("recovery enabled must be a boolean")
        for key in cls.__dataclass_fields__:
            if key.endswith("_seconds") or key.endswith("_observations"):
                value = getattr(policy, key)
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    raise ValueError(f"invalid recovery policy {key}")
        if not policy.triage_agent or not policy.fallback_agent or policy.retry_cap_seconds <= 0 or policy.retry_initial_seconds <= 0:
            raise ValueError("recovery owners and positive retry delays are required")
        return policy


def hold(metadata: dict, now: float) -> str | None:
    """Only an explicit reason and finite unexpired deadline can suppress recovery."""
    for name in ("maintenance", "wait"):
        value = object_value(metadata.get(name))
        until = epoch(value.get("until"))
        if value.get("reason") and until is not None and until > now:
            if name == "maintenance" or value.get("verified") is True:
                return name + ": " + str(value["reason"])[:200]
    return None


@dataclass
class Fault:
    kind: str
    subject: str
    project_id: int
    agent_id: str | None = None
    source_task_id: int | None = None
    session_id: str | None = None
    evidence: dict = field(default_factory=dict)
    depth: int = 0
    delay: float = 0
    observations: int = 1

    @property
    def key(self) -> str:
        raw = f"{self.project_id}:{self.kind}:{self.subject}"
        return "recovery-v1:" + hashlib.sha256(raw.encode()).hexdigest()[:32]

    def dump(self) -> dict:
        return dict(self.__dict__)


@dataclass
class Snapshot:
    project: dict
    agents: dict[str, dict]
    tasks: list[dict]
    hosts: list[dict]
    dispatches: list[dict]
    sessions: list[dict]
    endpoints: dict[str, bool]
    policy: Policy
    now: float
    endpoint_scopes: dict[str, dict] = field(default_factory=dict)


class Collector:
    def __init__(self, board: Board, native: JsonHttp, extra_runtimes: list[dict] | None = None,
                 defaults: dict | None = None):
        self.board, self.native = board, native
        self.extra_runtimes, self.defaults = extra_runtimes or [], defaults or {}

    def collect(self, project_id: int, now: float) -> Snapshot:
        project = self.board.call("GET", f"/projects/{project_id}")
        policy = Policy.from_metadata(self.defaults, object_value(project.get("metadata")).get("recovery", {}))
        # The compact projection omits stand_down_requested_at. An explicit stop
        # must not be mistaken for a missing worker; request the full identity.
        agents = {a["id"]: a for a in self.board.pages("/agents?verbose=true")}
        # The Board REST tasks list is unpaged; only the MCP task list is paged.
        tasks = self.board.call("GET", f"/tasks?project_id={project_id}")
        hosts = self.board.call("GET", "/fleet/hosts")
        dispatches = self.board.pages("/fleet/dispatches")
        if not all(isinstance(value, list) for value in (tasks, hosts, dispatches)):
            raise HttpFailure("GET")
        catalog, scopes = {}, {}
        project_tasks = {int(t["id"]) for t in tasks}
        # Historical terminal dispatches are receipts, not current endpoints.
        # A retired bootstrap URL cannot poison unrelated task completeness.
        active_dispatches = [d for d in dispatches if d.get("status") not in TERMINAL
                             and (not d.get("task_id") or int(d["task_id"]) in project_tasks)]
        active_hosts = {d.get("host_id") for d in active_dispatches}
        current_hosts = [h for h in hosts if h.get("enabled") is not False or h.get("host_id") in active_hosts]
        for row in [*current_hosts, *active_dispatches, *self.extra_runtimes]:
            if not row.get("runtime_url"):
                continue
            url = api_url(row["runtime_url"])
            catalog.setdefault(url, row.get("host_id"))
            scope = scopes.setdefault(url, {"task_ids": [], "agent_ids": [], "unscoped": False, "held": False})
            if row.get("task_id") and int(row["task_id"]) not in scope["task_ids"]:
                scope["task_ids"].append(int(row["task_id"]))
            if row in self.extra_runtimes:
                scope["task_ids"].extend(row.get("task_ids", []))
                scope["agent_ids"].extend(row.get("agent_ids", []))
                scope["unscoped"] = not row.get("task_ids") and not row.get("agent_ids")
            if row in current_hosts:
                scope["held"] = row.get("enabled") is False or bool(hold(object_value(row.get("metadata")), now))
        sessions, endpoints = [], {}
        seen = set()
        for url, host in sorted(catalog.items()):
            try:
                rows = self.native.request("GET", url + "/sessions")
                if not isinstance(rows, list):
                    raise HttpFailure("GET")
                endpoints[url] = True
                for row in rows:
                    if not isinstance(row, dict) or not row.get("id") or not row.get("agent_id"):
                        raise HttpFailure("GET")
                    # Endpoint identity remains part of the key: duplicated UUIDs on two
                    # live hosts are visible evidence, not silently overwritten.
                    key = (url, row["id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    item = dict(row, runtime_url=url, observed_host_id=host)
                    if row.get("status") in {"failed", "needs_resolution"}:
                        try:
                            tools = self.native.request("GET", url + "/sessions/" + urllib.parse.quote(row["id"], safe="") + "/tools")
                            if not isinstance(tools, list):
                                raise HttpFailure("GET")
                            item["tool_receipts"] = [{k: tool.get(k) for k in
                                ("call_id", "operation_id", "name", "status")} for tool in tools]
                            item["tool_receipts_verified"] = True
                        except HttpFailure:
                            item["tool_receipts_verified"] = False
                    sessions.append(item)
            except HttpFailure:
                endpoints[url] = False
        return Snapshot(project, agents, tasks, hosts, dispatches, sessions, endpoints, policy, now, scopes)


def effective_policy(snapshot: Snapshot, agent: dict | None = None, task: dict | None = None) -> Policy:
    return Policy.from_metadata(snapshot.policy.__dict__,
        object_value((agent or {}).get("metadata")).get("recovery", {}),
        object_value((task or {}).get("metadata")).get("recovery", {}))


def agent_runs(agent: dict) -> bool:
    metadata = object_value(agent.get("metadata"))
    intent = agent.get("lifecycle_intent") or metadata.get("lifecycle_intent", "run")
    return not agent.get("retired") and intent == "run" and not agent.get("stand_down_requested_at")


def task_snapshot_complete(snapshot: Snapshot, tid: int, owner: str | None) -> bool:
    """Absence proof is scoped to current associations, never historical URLs.

    Fleet is the placement authority. An unavailable current dispatch endpoint
    (or an explicitly unscoped bootstrap endpoint) can hide this task's session;
    another task's retired/drained runtime cannot. At least one current endpoint
    must be observable so a totally blind monitor reports infrastructure instead.
    """
    if not any(snapshot.endpoints.values()):
        return False
    for url, reachable in snapshot.endpoints.items():
        if reachable:
            continue
        scope = snapshot.endpoint_scopes.get(url)
        if scope is None or scope.get("unscoped") or tid in scope.get("task_ids", []) or owner in scope.get("agent_ids", []):
            return False
    return True


def generation(row: dict) -> int:
    value = row.get("generation", 0)
    return value if type(value) is int and value >= 0 else 0


class SessionLineage:
    """Resolve retained session identity, never agent identity or request-id text.

    All generations contribute ownership evidence. Conflicting anchors are not
    resolved by list order or by borrowing another project's nearest live task.
    Completed dispatch/session records remain valid lineage anchors.
    """
    def __init__(self, snapshot: Snapshot):
        self.nodes: dict[str, list[tuple[int | None, str | None]]] = {}
        for rows, sid_key, task_key in ((snapshot.dispatches, "session_id", "task_id"),
                                        (snapshot.sessions, "id", "task")):
            for row in rows:
                sid = row.get(sid_key)
                if not isinstance(sid, str) or not sid:
                    continue
                parent = object_value(row.get("metadata")).get("parent_session_id")
                parent = parent if isinstance(parent, str) and parent else None
                self.nodes.setdefault(sid, []).append((task_id(row.get(task_key)), parent))

    def resolve(self, sid: str) -> tuple[int | None, list[str], str | None]:
        chain, seen = [], set()
        while sid:
            if sid in seen:
                return None, chain, "cyclic child session lineage"
            seen.add(sid)
            chain.append(sid)
            rows = self.nodes.get(sid, [])
            anchors = {tid for tid, _ in rows if tid}
            parents = {parent for _, parent in rows if parent}
            if len(anchors) > 1 or (not anchors and len(parents) > 1):
                return None, chain, "conflicting child session lineage across retained generations"
            if anchors:
                return next(iter(anchors)), chain, None
            if not parents:
                return None, chain, "child session lineage has no retained task anchor"
            sid = next(iter(parents))
        return None, chain, "child session lineage unavailable"


def child_resolution(dispatch: dict) -> bool:
    """An owner can explicitly retire a historical failed receipt, not replay it.

    A parent completing, a generic note, or an old generation's resolution is
    not a child's disposition. The monitor never writes this acknowledgement.
    """
    value = object_value(object_value(dispatch.get("metadata")).get("recovery_resolution"))
    return (dispatch.get("status") == "failed"
            and type(value.get("generation")) is int
            and value["generation"] == generation(dispatch)
            and value.get("disposition") in {"resolved", "cancelled"}
            and all(isinstance(value.get(k), str) and value[k].strip()
                    for k in ("cause", "fix", "regression")))


def dispatch_evidence(dispatch: dict) -> dict:
    # Do not carry arbitrary metadata, error text, inputs or tool bodies forward.
    evidence = {k: dispatch.get(k) for k in ("id", "session_id", "status", "host_id",
        "runtime_url", "generation", "version", "created_at", "updated_at")}
    stage = object_value(dispatch.get("metadata")).get("failure_stage")
    evidence["failure_stage"] = stage if stage in {"provisioning", "launching", "runtime"} else None
    evidence["error_present"] = bool(dispatch.get("error"))
    return evidence


def child_session(snapshot: Snapshot, dispatch: dict) -> tuple[dict | None, bool]:
    """Ignore stale generations; refuse ambiguous same-generation observations."""
    candidates = [s for s in snapshot.sessions if s["id"] == dispatch.get("session_id")
                  and generation(s) >= generation(dispatch)]
    if not candidates:
        return None, False
    newest = max(generation(s) for s in candidates)
    candidates = [s for s in candidates if generation(s) == newest]
    if len(candidates) != 1:
        return None, True
    return candidates[0], False


def inspect_session(snapshot: Snapshot, session: dict, local: Policy,
                    owner: str, tid: int) -> tuple[bool, list[Fault], list[dict]]:
    """Shared observation policy for directly assigned work and independent children."""
    now = snapshot.now
    status, sid = session.get("status"), session["id"]
    metadata = object_value(session.get("metadata"))
    reason = hold(object_value(metadata.get("recovery")), now)
    if status == "stopped" or reason:
        return True, [], [{"scope": f"session:{sid}", "reason": reason or "explicitly stopped session"}]
    last = epoch(session.get("last_progress_at")) or epoch(session.get("updated_at")) or now
    age = max(0, now - last)
    evidence = {key: session.get(key) for key in ("status", "phase", "driver_running",
        "step", "turns", "generation", "last_progress_at", "updated_at", "host_id", "runtime_url", "failure_kind")}
    evidence["error_present"] = bool(session.get("error"))
    evidence["safe_action"] = "diagnose_without_replay"
    if session.get("tool_receipts") is not None:
        evidence["tool_receipts"] = session["tool_receipts"]
        evidence["tool_receipts_verified"] = session.get("tool_receipts_verified", False)
    kind, covered, waits = None, False, []
    if status == "needs_resolution" or any(t.get("status") == "unknown" for t in session.get("tool_receipts", [])):
        kind, covered = "unknown_tool_effect", True
    elif status == "failed":
        kind, covered = "session_failed", True
    elif status == "waiting":
        covered = True
        if age >= local.progress_stall_seconds:
            kind = "unexplained_session_wait"
    elif status in ACTIVE:
        covered = True
        inference = session.get("phase") in {"awaiting-model", "streaming"} or "model response" in str(session.get("progress", "")).lower()
        if session.get("driver_running") is False and age >= local.progress_stall_seconds:
            kind = "driver_missing"
        elif (inference or status == "retrying") and session.get("driver_running") is not False:
            waits.append({"scope": f"session:{sid}", "reason": "live inference/retry; elapsed time is not process death"})
            if local.live_wait_notice_seconds and age >= local.live_wait_notice_seconds:
                kind = "long_live_wait"
        elif age >= local.progress_stall_seconds:
            kind = "progress_stalled"
    faults = [Fault(kind, f"session:{sid}", int(snapshot.project["id"]), owner, tid, sid, evidence)] if kind else []
    return covered, faults, waits


def classify_children(snapshot: Snapshot) -> tuple[list[Fault], list[dict]]:
    """Independent child execution is not covered or closed by its parent's state."""
    lineage = SessionLineage(snapshot)
    tasks = {int(t["id"]): t for t in snapshot.tasks}
    current: dict[str, list[dict]] = {}
    for dispatch in snapshot.dispatches:
        sid = dispatch.get("session_id")
        if not isinstance(sid, str) or not sid:
            continue
        previous = current.get(sid, [])
        if not previous or generation(dispatch) > generation(previous[0]):
            current[sid] = [dispatch]
        elif generation(dispatch) == generation(previous[0]):
            current[sid].append(dispatch)
    faults, waits = [], []
    for candidates in current.values():
        for dispatch in candidates:
            if task_id(dispatch.get("task_id")):
                continue
            scope = f"dispatch:{dispatch['id']}"
            tid, chain, reason = lineage.resolve(dispatch["session_id"])
            if reason:
                waits.append({"scope": scope, "reason": reason})
                continue
            if tid not in tasks:
                continue  # Ownership belongs to another project, never this one.
            if len(candidates) != 1:
                waits.append({"scope": scope, "reason": "conflicting current child dispatch receipts"})
                continue
            task = tasks[tid]
            tm = object_value(task.get("metadata"))
            owner = dispatch.get("agent_id")
            agent = snapshot.agents.get(owner, {})
            am = object_value(agent.get("metadata"))
            dm = object_value(dispatch.get("metadata"))
            local = Policy.from_metadata(effective_policy(snapshot, agent, task).__dict__, dm.get("recovery", {}))
            reason = (hold(object_value(tm.get("recovery")), snapshot.now)
                      or hold(object_value(am.get("recovery")), snapshot.now)
                      or hold(object_value(dm.get("recovery")), snapshot.now))
            if (not local.enabled or reason or task.get("monitor_exempt") or tm.get("monitor_exempt")
                    or (agent and not agent_runs(agent))):
                waits.append({"scope": scope, "reason": reason or "child recovery explicitly disabled or identity paused/retired"})
                continue
            if object_value(tm.get("recovery")).get("incident_key"):
                waits.append({"scope": scope, "reason": "repair child belongs to bounded triage control-path supervision"})
                continue
            session, ambiguous = child_session(snapshot, dispatch)
            if ambiguous:
                waits.append({"scope": scope, "reason": "ambiguous child runtime observations at the same generation"})
                continue
            unknown = session and (session.get("status") == "needs_resolution" or any(
                t.get("status") == "unknown" for t in session.get("tool_receipts", [])))
            historical = not session or (generation(session) == generation(dispatch)
                and session.get("status") in TERMINAL)
            if child_resolution(dispatch) and historical and not unknown:
                waits.append({"scope": scope, "reason": "failed child explicitly reconciled for this generation"})
                continue
            evidence = {"dispatch": dispatch_evidence(dispatch), "safe_action": "diagnose_without_replay",
                "lineage": {"parent_session_ids": chain[1:], "source_task_id": tid,
                            "source_project_id": snapshot.project["id"], "source_task_status": task.get("status")}}
            kind = None
            if session:
                _, observations, session_waits = inspect_session(snapshot, session, local, owner, tid)
                waits.extend(session_waits)
                if observations:
                    kind = observations[0].kind
                    evidence.update(observations[0].evidence)
            elif dispatch.get("status") == "failed":
                # This is a definite Fleet failure, not proof of no native effects.
                # Even if the endpoint is down, triage can inspect the retained receipt.
                kind = "dispatch_failed"
            elif dispatch.get("status") in {"stopped", "cancelled"}:
                waits.append({"scope": scope, "reason": "child explicitly stopped/cancelled"})
            elif dispatch.get("status") != "completed":
                url = api_url(dispatch["runtime_url"])
                age = snapshot.now - (epoch(dispatch.get("updated_at")) or epoch(dispatch.get("created_at")) or snapshot.now)
                if not snapshot.endpoints.get(url, False):
                    waits.append({"scope": scope, "reason": "incomplete child runtime observation; absence not proven"})
                elif age >= local.dispatch_grace_seconds:
                    kind = "dispatch_without_live_session"
            if kind:
                evidence["observation"] = kind
                evidence["runtime_session_observed"] = session is not None
                # One incident survives failed provisioning -> unknown tool -> live
                # recovery transitions. Generation and observations stay evidence,
                # not a new incident/create/dispatch intent on each poll.
                faults.append(Fault("child_execution_fault", scope, int(snapshot.project["id"]),
                    owner, tid, dispatch.get("session_id"), evidence))
    return faults, waits


def fault_observable(snapshot: Snapshot, fault: Fault) -> bool:
    if fault.depth:
        # A fallback control-path repair is observable again once every open
        # primary repair for that identity has a verified live driver. Merely
        # setting the fallback task done cannot erase a still-broken path.
        primary = [t for t in snapshot.tasks
                   if t.get("assignee") == fault.subject
                   and object_value(object_value(t.get("metadata")).get("recovery")).get("incident_key")
                   and not object_value(object_value(t.get("metadata")).get("recovery")).get("depth")
                   and t.get("status") in ACTIONABLE]
        return all(any(task_id(v.get("task")) == t["id"] and v.get("status") in ACTIVE
                       and v.get("driver_running") is not False for v in snapshot.sessions) for t in primary)
    if fault.kind == "runtime_unreachable":
        return snapshot.endpoints.get(fault.subject, True)
    if fault.kind == "child_execution_fault":
        # Parent completion never proves child recovery. Preserve an unresolved
        # observation when its own current runtime cannot be read.
        did = object_value(fault.evidence.get("dispatch")).get("id")
        dispatch = next((d for d in snapshot.dispatches if d.get("id") == did), None)
        if not dispatch:
            return False
        tid, _, reason = SessionLineage(snapshot).resolve(dispatch.get("session_id"))
        if reason or tid != fault.source_task_id:
            return False  # Changed/ambiguous ownership is not proof of recovery.
        if dispatch.get("status") in TERMINAL or child_resolution(dispatch):
            return True
        session, ambiguous = child_session(snapshot, dispatch)
        if ambiguous:
            return False
        return session is not None or snapshot.endpoints.get(api_url(dispatch["runtime_url"]), False)
    source = next((t for t in snapshot.tasks if t["id"] == fault.source_task_id), None)
    if source and source.get("status") not in ACTIONABLE:
        return True
    url = fault.evidence.get("runtime_url")
    if url:
        return snapshot.endpoints.get(api_url(url), False)
    return task_snapshot_complete(snapshot, fault.source_task_id, fault.agent_id)


def classify(snapshot: Snapshot) -> tuple[list[Fault], list[dict]]:
    """Pure safety policy: every action is separate triage, never original replay."""
    now, project, policy = snapshot.now, snapshot.project, snapshot.policy
    project_id = int(project["id"])
    faults, waits = [], []
    if not policy.enabled or project.get("status") == "archived":
        return [], [{"scope": "project", "reason": "recovery disabled or project archived"}]
    reason = hold({"maintenance": policy.maintenance}, now)
    if reason:
        return [], [{"scope": "project", "reason": reason}]
    tasks = {int(t["id"]): t for t in snapshot.tasks}
    sessions_by_task: dict[int, list[dict]] = {}
    for s in snapshot.sessions:
        tid = task_id(s.get("task"))
        if tid:
            sessions_by_task.setdefault(tid, []).append(s)
    dispatch_by_task: dict[int, list[dict]] = {}
    for d in snapshot.dispatches:
        if d.get("task_id"):
            dispatch_by_task.setdefault(int(d["task_id"]), []).append(d)
    # An incomplete runtime read is not proof that work has no session. Still
    # create an owned observability incident so the blind spot is never silent.
    for url, reachable in snapshot.endpoints.items():
        if reachable:
            continue
        host = next((h for h in snapshot.hosts if api_url(h["runtime_url"]) == url), {})
        reason = hold(object_value(host.get("metadata")), now)
        if host.get("enabled") is False or reason:
            waits.append({"scope": url, "reason": reason or "host explicitly drained"})
            continue
        faults.append(Fault("runtime_unreachable", url, project_id,
            evidence={"runtime_url": url, "host_id": host.get("host_id"),
                "runtime_ready": host.get("runtime_ready"), "fresh": host.get("fresh"),
                "process_death_verified": False, "safe_action": "diagnose_without_replay"},
            delay=policy.runtime_failure_grace_seconds, observations=max(1, policy.runtime_failure_observations)))
    if not snapshot.endpoints and any(t.get("status") in ACTIONABLE for t in snapshot.tasks):
        if snapshot.hosts and all(h.get("enabled") is False for h in snapshot.hosts):
            waits.append({"scope": "fleet", "reason": "all hosts explicitly drained"})
        else:
            faults.append(Fault("no_runtime_capacity", "fleet", project_id,
                evidence={"configured_hosts": len(snapshot.hosts), "safe_action": "restore_execution_capacity"},
                delay=policy.runtime_failure_grace_seconds))
    for tid, task in tasks.items():
        tm = object_value(task.get("metadata"))
        if task.get("status") not in ACTIONABLE or task.get("effectively_blocked") or task.get("question_blocked") or task.get("monitor_exempt") or tm.get("monitor_exempt"):
            if task.get("status") == "blocked" or task.get("question_blocked"):
                waits.append({"scope": f"task:{tid}", "reason": "Board blocked dependency/question"})
            continue
        # Repair tasks have their own bounded control-path supervision below;
        # they must not recursively create a new incident on every poll.
        if object_value(tm.get("recovery")).get("incident_key"):
            continue
        owner = task.get("assignee")
        agent = snapshot.agents.get(owner, {})
        local = effective_policy(snapshot, agent, task)
        am = object_value(agent.get("metadata"))
        reason = hold(object_value(tm.get("recovery")), now) or hold(object_value(am.get("recovery")), now)
        if not local.enabled or reason or (agent and not agent_runs(agent)):
            waits.append({"scope": f"task:{tid}", "reason": reason or "identity paused/retired or monitoring explicitly disabled"})
            continue
        if not owner or not agent:
            faults.append(Fault("unowned_actionable_task", f"task:{tid}", project_id,
                source_task_id=tid, evidence={"assignee": owner}, delay=local.missing_session_grace_seconds))
            continue
        if am.get("harness") != "native":
            waits.append({"scope": f"task:{tid}", "reason": "owner uses a non-native execution path"})
            continue
        related = [s for s in sessions_by_task.get(tid, []) if s["agent_id"] == owner]
        # A live same-owner parent session may intentionally perform child work;
        # any unrelated session under that identity is not task coverage.
        ancestor = task.get("parent_id")
        visited = {tid}
        while ancestor and int(ancestor) not in visited:
            ancestor = int(ancestor)
            visited.add(ancestor)
            related.extend(s for s in sessions_by_task.get(ancestor, [])
                           if s["agent_id"] == owner and s.get("status") in ACTIVE
                           and s.get("driver_running") is not False)
            ancestor = tasks.get(ancestor, {}).get("parent_id")
        has_path = False
        for session in related:
            covered, observations, session_waits = inspect_session(snapshot, session, local, owner, tid)
            has_path = has_path or covered
            faults.extend(observations)
            waits.extend(session_waits)
        if has_path:
            continue
        queued = [d for d in dispatch_by_task.get(tid, []) if d.get("agent_id") == owner and d.get("status") not in TERMINAL]
        if queued:
            for d in queued:
                age = now - (epoch(d.get("updated_at")) or epoch(d.get("created_at")) or now)
                if age >= local.dispatch_grace_seconds:
                    faults.append(Fault("dispatch_without_live_session", f"dispatch:{d['id']}", project_id,
                        owner, tid, d.get("session_id"), {key: d.get(key) for key in
                            ("id", "status", "host_id", "runtime_url", "generation", "version", "updated_at")}))
            continue
        if not task_snapshot_complete(snapshot, tid, owner):
            waits.append({"scope": f"task:{tid}", "reason": "incomplete runtime snapshot; absence not proven"})
            continue
        faults.append(Fault("missing_actionable_session", f"task:{tid}", project_id, owner, tid,
            evidence={"verified_endpoints": len(snapshot.endpoints), "task_status": task.get("status")},
            delay=local.missing_session_grace_seconds, observations=2))
    child_faults, child_waits = classify_children(snapshot)
    faults.extend(child_faults)
    waits.extend(child_waits)
    return faults, waits


class Journal:
    """Single-controller journal with durable outbox checkpoints; Board is authoritative."""
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=15)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript("""
            PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS observations (
                key TEXT PRIMARY KEY, project_id INTEGER NOT NULL, first_seen REAL NOT NULL,
                last_seen REAL NOT NULL, samples INTEGER NOT NULL, present INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS incidents (
                key TEXT PRIMARY KEY, project_id INTEGER NOT NULL, payload TEXT NOT NULL,
                task_id INTEGER, dispatch_id TEXT, state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
                last_error TEXT, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                key TEXT PRIMARY KEY, method TEXT NOT NULL, path TEXT NOT NULL,
                body TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receipts (
                key TEXT PRIMARY KEY, result TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS health (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)

    @contextlib.contextmanager
    def exclusive(self):
        # OS releases flock on death; no time lease can let two live controllers
        # dispatch concurrently. Run one service with this durable path.
        with self.path.with_suffix(self.path.suffix + ".lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("another recovery controller owns this journal") from None
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def observe(self, project_id: int, faults: list[Fault], now: float, complete: bool,
                preserve: set[str] | None = None) -> None:
        present = {f.key for f in faults}
        with self.connection:
            if complete:
                for row in self.connection.execute("SELECT key FROM observations WHERE project_id=? AND present=1", (project_id,)).fetchall():
                    if row["key"] not in present and row["key"] not in (preserve or set()):
                        self.connection.execute("UPDATE observations SET present=0 WHERE key=?", (row["key"],))
            for fault in faults:
                old = self.connection.execute("SELECT * FROM observations WHERE key=?", (fault.key,)).fetchone()
                first = old["first_seen"] if old and old["present"] else now
                samples = old["samples"] + 1 if old and old["present"] else 1
                payload = json.dumps(fault.dump(), sort_keys=True)
                self.connection.execute("INSERT OR REPLACE INTO observations VALUES(?,?,?,?,?,?,?)",
                    (fault.key, project_id, first, now, samples, 1, payload))
                if now - first >= fault.delay and samples >= fault.observations:
                    # Freeze the creation intent. Retrying an ambiguous HTTP create
                    # must use identical external id and task/dispatch parameters.
                    self.connection.execute("INSERT OR IGNORE INTO incidents(key,project_id,payload,updated_at) VALUES(?,?,?,?)",
                        (fault.key, project_id, payload, now))

    def rows(self, project_id: int | None = None) -> list[dict]:
        sql, args = "SELECT * FROM incidents", ()
        if project_id is not None:
            sql, args = sql + " WHERE project_id=?", (project_id,)
        return [dict(r) for r in self.connection.execute(sql + " ORDER BY updated_at,key", args)]

    def update(self, key: str, **fields) -> None:
        allowed = {"task_id", "dispatch_id", "state", "attempts", "retry_at", "last_error", "updated_at"}
        if set(fields) - allowed:
            raise ValueError("invalid incident update")
        with self.connection:
            self.connection.execute("UPDATE incidents SET " + ",".join(k + "=?" for k in fields) + " WHERE key=?",
                                    (*fields.values(), key))

    def intent(self, key: str, method: str, path: str, data: dict, now: float) -> tuple[str, str, dict]:
        # Commit the exact request BEFORE any network side effect. A lost reply
        # plus owner/policy changes must never reuse a key with different intent.
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO outbox VALUES(?,?,?,?,?)",
                                    (key, method, path, json.dumps(data, sort_keys=True), now))
        row = self.connection.execute("SELECT method,path,body FROM outbox WHERE key=?", (key,)).fetchone()
        return row["method"], row["path"], json.loads(row["body"])

    def receipt(self, key: str) -> Any:
        row = self.connection.execute("SELECT result FROM receipts WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def save_receipt(self, key: str, result: Any, now: float) -> None:
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO receipts VALUES(?,?,?)", (key, json.dumps(result), now))

    def present(self, key: str) -> bool:
        row = self.connection.execute("SELECT * FROM observations WHERE key=?", (key,)).fetchone()
        if not row or not row["present"]:
            return False
        fault = Fault(**json.loads(row["payload"]))
        return row["samples"] >= fault.observations and row["last_seen"] - row["first_seen"] >= fault.delay

    def save_health(self, value: dict) -> None:
        with self.connection:
            self.connection.execute("INSERT OR REPLACE INTO health VALUES('latest',?)", (json.dumps(value),))

    def close(self):
        self.connection.close()


def triage_description(fault: Fault) -> str:
    return (
        f"Independent recovery observed {fault.kind} for {fault.subject}.\n"
        f"Source task: {fault.source_task_id}; session: {fault.session_id}; owner: {fault.agent_id}.\n\n"
        "Diagnose the source-level cause and repair it durably. Inspect current Board/Fleet/runtime "
        "state before acting; this is a diagnostic session, not permission to replay the original work. "
        "A live model/retry wait is not a dead worker. Respect explicit stops, pauses and verified waits. "
        "Preserve completed tool results. Reconcile exact receipts for unknown writes before any resume "
        "or repeat; never fabricate success or blindly replay. Coordinate service deployment with integrator.\n\n"
        "Deliver a tested source/configuration fix and a regression demonstrating this failure and recovery. "
        "Record cause, fix (commit/deployment), and regression evidence in task metadata.recovery_resolution "
        "and a comment before closing. Merely waking a session is not repair. Transient inference and "
        "read-only discovery retry indefinitely with capped jitter/Retry-After; do not restore finite budgets.\n\n"
        "Safe observation (no tool arguments/results or credentials):\n" + json.dumps(fault.evidence, indent=2, sort_keys=True)
    )


class Controller:
    def __init__(self, board: Board, journal: Journal, monitor_id: str = "native-recovery-monitor", revision: str | None = None):
        self.board, self.journal, self.monitor_id, self.revision = board, journal, monitor_id, revision

    def once(self, key: str, method: str, path: str, data: dict, now: float) -> Any:
        receipt = self.journal.receipt(key)
        if receipt is not None:
            return receipt
        method, path, data = self.journal.intent(key, method, path, data, now)
        result = self.board.call(method, path, data)
        self.journal.save_receipt(key, result, now)
        return result

    def comment(self, task: int, key: str, body: str, now: float):
        return self.once(key, "POST", f"/tasks/{task}/comments", {
            "body": body, "principal": self.monitor_id, "acknowledge_banned": True,
            "external_link": {"source": EXTERNAL_SOURCE, "external_id": key},
        }, now)

    def _create(self, row: dict, snapshot: Snapshot) -> dict:
        fault = Fault(**json.loads(row["payload"]))
        policy = snapshot.policy
        # Avoid routing a failed triage identity back into itself.
        owner = policy.fallback_agent if fault.depth or fault.agent_id == policy.triage_agent else policy.triage_agent
        payload = {"project_id": fault.project_id, "assignee": owner, "priority": "high",
            "principal": self.monitor_id, "title": f"Repair native {fault.kind}: {fault.subject}"[:240],
            "description": triage_description(fault), "external_link": {"source": EXTERNAL_SOURCE, "external_id": fault.key},
            "metadata": {"recovery": {"incident_key": fault.key, "kind": fault.kind, "subject": fault.subject,
                "source_task_id": fault.source_task_id, "session_id": fault.session_id, "depth": fault.depth},
                "recovery_monitor_state": "triage_pending", "recovery_evidence": fault.evidence}}
        task = self.once(fault.key + ":create", "POST", "/tasks", payload, snapshot.now)
        if not isinstance(task, dict) or not task.get("id"):
            raise HttpFailure("POST")
        self.journal.update(row["key"], task_id=int(task["id"]), updated_at=snapshot.now)
        return task

    def _fallback(self, original: dict, task: dict, fault: Fault, snapshot: Snapshot, reason: str):
        if fault.depth >= 1:
            # A broken recovery control plane is one visible owned dependency,
            # not an exponentially branching tree of failed triage sessions.
            key = fault.key + ":control-plane-blocked"
            self.once(key, "PATCH", f"/tasks/{task['id']}", {
                "status": "blocked", "principal": self.monitor_id,
                "blocked_on": {"kind": "external", "note": "Recovery control path unavailable; preserve original receipts. " + reason},
                "metadata": {"recovery_monitor_state": "control_path_requires_repair"}}, snapshot.now)
            self.comment(task["id"], key + ":note",
                "Recovery control path is unavailable. This owned task remains visible; controller continues "
                "observing and retrying idempotent reads without a retry budget. No original session was replayed. " + reason, snapshot.now)
            return
        fallback = Fault("triage_path_unavailable", str(task.get("assignee") or snapshot.policy.triage_agent),
            fault.project_id, task.get("assignee"), task["id"],
            evidence={"repair_task_id": task["id"], "incident_key": fault.key, "reason": reason}, depth=1)
        self.journal.observe(fault.project_id, [fallback], snapshot.now, complete=False)
        self.comment(task["id"], fault.key + ":triage-escalated",
            "Triage did not reach a live repair path. A single deduplicated fallback-owner control-path repair "
            f"is queued ({fallback.key}); no recursive retry storm and no original write replay. {reason}", snapshot.now)
        self.journal.update(original["key"], state="triage_escalated", updated_at=snapshot.now)

    def advance(self, row: dict, snapshot: Snapshot):
        now, policy = snapshot.now, snapshot.policy
        if row["retry_at"] > now:
            return
        fault = Fault(**json.loads(row["payload"]))
        try:
            task = self.board.call("GET", f"/tasks/{row['task_id']}?comments_limit=0") if row["task_id"] else self._create(row, snapshot)
            tid = int(task["id"])
            if fault.source_task_id:
                self.once(fault.key + ":source-link", "PATCH", f"/tasks/{fault.source_task_id}/props",
                    {"recovery_task_id": tid}, now)
            metadata = object_value(task.get("metadata"))
            resolution = object_value(metadata.get("recovery_resolution"))
            present = self.journal.present(fault.key)
            if task.get("status") in {"done", "cancelled"}:
                if task.get("status") == "cancelled" or (all(resolution.get(k) for k in ("cause", "fix", "regression")) and not present):
                    self.journal.update(fault.key, state="closed", attempts=0, last_error=None, updated_at=now)
                    return
                # The Board updated_at is the observed closure generation. A
                # lifetime ':reopen' receipt would hide every later premature
                # closure/recurrence after the first successful reopen.
                version = task.get("updated_at") or task.get("version")
                if version is None:
                    raise ValueError("task closure lacks a version")
                token = hashlib.sha256(str(version).encode()).hexdigest()[:20]
                closure_key = fault.key + ":closure:" + token
                self.comment(tid, closure_key + ":incomplete",
                    "The repair was marked done but the symptom remains or cause/fix/regression evidence is missing. "
                    "Record metadata.recovery_resolution and verify current runtime/Board receipts; a one-time wake is insufficient.", now)
                self.once(closure_key + ":reopen", "PATCH", f"/tasks/{tid}",
                    {"status": "in_progress", "principal": self.monitor_id}, now)
            if task.get("status") in {"blocked", "icebox"}:
                self.journal.update(fault.key, state="owned_wait", attempts=0, last_error=None, updated_at=now)
                return
            owner = task.get("assignee")
            owner_agent = snapshot.agents.get(owner, {})
            if owner_agent and not agent_runs(owner_agent):
                self.journal.update(fault.key, state="owned_wait", attempts=0, last_error=None, updated_at=now)
                return
            if not owner or not owner_agent:
                self._fallback(row, task, fault, snapshot, "triage owner missing")
                return
            # Same key as automatic Fleet task dispatch. No input/metadata override:
            # both creators must resolve to the same durable intent, including an
            # already terminal dispatch. Never bump native_attempt automatically.
            dispatch = self.once(fault.key + ":dispatch", "POST", "/fleet/dispatches", {
                "request_id": f"task:{tid}:attempt:1", "task_id": tid, "agent_id": owner}, now)
            did = str(dispatch["id"])
            self.journal.update(fault.key, dispatch_id=did, attempts=0, last_error=None, retry_at=0, updated_at=now)
            current = next((d for d in snapshot.dispatches if str(d["id"]) == did), dispatch)
            session = next((s for s in snapshot.sessions if s["id"] == current.get("session_id")
                            and s["runtime_url"] == api_url(current["runtime_url"])), None)
            status = (session or current).get("status")
            age = now - (epoch(current.get("created_at")) or now)
            live = session and status in ACTIVE and session.get("driver_running") is not False
            state = "triage_live" if live else "triage_scheduled"
            if status == "stopped":
                # Explicit operator stop is not a failed recovery attempt.
                state = "owned_wait"
            elif status == "needs_resolution":
                self._fallback(row, task, fault, snapshot, "triage has unknown tool effects; receipt reconciliation required")
                state = "triage_needs_resolution"
            elif status in TERMINAL or (not live and age >= policy.triage_grace_seconds):
                self._fallback(row, task, fault, snapshot, f"triage status {status}; no verified live driver")
                state = "triage_escalated"
            elif not present:
                state = "symptom_cleared_awaiting_evidence"
                self.comment(tid, fault.key + ":symptom-cleared",
                    "The latest complete observation no longer sees this symptom. The incident stays owned/open "
                    "until cause, durable fix and regression evidence are recorded; recovery alone is not repair.", now)
            if metadata.get("recovery_monitor_state") != state:
                # Re-entering a former state must update Board visibility too;
                # this idempotent value write is not a once-per-lifetime action.
                self.board.call("PATCH", f"/tasks/{tid}/props", {"recovery_monitor_state": state})
            self.journal.update(fault.key, state=state, attempts=0, last_error=None, updated_at=now)
        except (HttpFailure, KeyError, TypeError, ValueError) as exc:
            attempt = row["attempts"] + 1
            delay = backoff(attempt, policy.retry_initial_seconds, policy.retry_cap_seconds,
                            exc.delay if isinstance(exc, HttpFailure) else 0)
            safe_error = str(exc) if isinstance(exc, HttpFailure) else "invalid control-plane response"
            self.journal.update(fault.key, attempts=attempt, retry_at=now + delay,
                                last_error=safe_error, state="retrying_control_plane", updated_at=now)
            LOG.warning("incident %s control-plane retry %s in %.1fs: %s", fault.key, attempt, delay, safe_error)
            if isinstance(exc, HttpFailure) and exc.status == 503 and "tid" in locals():
                available = any(h.get("fresh") and h.get("enabled") is not False
                                and h.get("active", 0) < h.get("capacity", 0) for h in snapshot.hosts)
                if not available:
                    try:
                        self.comment(tid, fault.key + ":capacity-wait",
                            "Recovery dispatch is waiting for fresh Fleet placement capacity. The repair remains owned "
                            "and actionable; the controller retries the same frozen request indefinitely. Unknown tool "
                            "sessions are not stopped or discarded to manufacture a free slot. Restore a healthy "
                            "worker or increase configured recovery capacity.", now)
                        self.board.call("PATCH", f"/tasks/{tid}/props",
                            {"recovery_blocked_on": {"kind": "capacity", "observed_at": iso(now)}})
                    except HttpFailure:
                        pass  # The durable incident and retry intent are already committed.

    def tick(self, snapshot: Snapshot) -> dict:
        faults, waits = classify(snapshot)
        policy, now = snapshot.policy, snapshot.now
        held = not policy.enabled or hold({"maintenance": policy.maintenance}, now) or snapshot.project.get("status") == "archived"
        if not held:
            preserve = {row["key"] for row in self.journal.rows(int(snapshot.project["id"]))
                        if not fault_observable(snapshot, Fault(**json.loads(row["payload"])))}
            self.journal.observe(int(snapshot.project["id"]), faults, now, complete=True, preserve=preserve)
            for row in self.journal.rows(int(snapshot.project["id"])):
                # Even closed incidents are re-read: a recurrent symptom must not
                # disappear just because an old repair ticket was once closed.
                self.advance(row, snapshot)
        rows = self.journal.rows(int(snapshot.project["id"]))
        health = {"observed_at": iso(now), "project_id": snapshot.project["id"], "revision": self.revision,
            "state": "maintenance" if held else "observing", "candidate_faults": len(faults),
            "open_incidents": sum(r["state"] != "closed" for r in rows),
            "retrying_incidents": sum(r["state"] == "retrying_control_plane" for r in rows),
            "runtime_endpoints": len(snapshot.endpoints), "unreachable_endpoints": sum(not v for v in snapshot.endpoints.values()),
            "visible_waits": waits[:100], "visible_wait_count": len(waits), "unlimited_control_plane_retry": True}
        self.journal.save_health(health)
        return health

    def heartbeat(self, health: dict):
        self.board.call("POST", "/agents", {"agent_id": self.monitor_id,
            "display_name": "Independent native recovery monitor", "kind": "service",
            "metadata": {"role": "recovery-monitor", "harness": "supervisor", "recovery_monitor": health}})


def selected_projects(board: Board, configured: list[int] | None, defaults: dict | None = None) -> list[int]:
    """Omission discovers live Board scope on EVERY pass; a list is an opt-in pin."""
    if configured is not None:
        if not isinstance(configured, list) or not all(type(p) is int and p > 0 for p in configured):
            raise ValueError("project_ids must be a list of positive integers, null, or omitted")
        return sorted(set(configured))
    projects = board.call("GET", "/projects?status=active")
    if not isinstance(projects, list):
        raise HttpFailure("GET")
    selected = []
    for project in projects:
        if not isinstance(project, dict) or type(project.get("id")) is not int:
            raise HttpFailure("GET")
        policy = object_value(object_value(project.get("metadata")).get("recovery"))
        enabled = policy.get("enabled", (defaults or {}).get("enabled", True))
        if project.get("status", "active") != "archived" and enabled is not False:
            selected.append(project["id"])
    return sorted(set(selected))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true", help="one observation/outbox pass, for deployment verification")
    parser.add_argument("--status", action="store_true", help="read local durable incident/health summary without acting")
    args = parser.parse_args(argv)
    config = json.loads(Path(args.config).read_text())
    configured_projects = config.get("project_ids")
    if configured_projects is not None and (not isinstance(configured_projects, list)
            or not all(type(p) is int and p > 0 for p in configured_projects)):
        raise ValueError("project_ids must be a list of positive integers, null, or omitted")
    journal = Journal(config["state_path"])
    if args.status:
        row = journal.connection.execute("SELECT value FROM health WHERE key='latest'").fetchone()
        print(json.dumps({"health": json.loads(row[0]) if row else None,
                          "incidents": journal.rows()}, indent=2))
        journal.close()
        return 0
    timeout = float(config.get("http_timeout_seconds", 10))
    if timeout <= 0:
        raise ValueError("http_timeout_seconds must be positive")
    poll = float(config.get("poll_seconds", 15))
    if poll <= 0:
        raise ValueError("poll_seconds must be positive")
    board = Board(config["board_url"], JsonHttp(timeout, headers_env=config.get("board_headers_env")))
    collector = Collector(board, JsonHttp(timeout, headers_env=config.get("runtime_headers_env")),
                          config.get("extra_runtimes"), config.get("defaults"))
    controller = Controller(board, journal, config.get("monitor_id", "native-recovery-monitor"), config.get("revision"))
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    failures = 0
    with journal.exclusive():
        while not stop.is_set():
            delay, failed = poll, False
            try:
                projects = selected_projects(board, configured_projects, config.get("defaults"))
                if not projects:
                    health = {"state": "idle", "observed_at": iso(time.time()), "project_count": 0,
                        "scope_mode": "discovered" if configured_projects is None else "explicit",
                        "unlimited_control_plane_retry": True}
                    journal.save_health(health)
                    controller.heartbeat(health)
            except (HttpFailure, ValueError, KeyError, TypeError) as exc:
                failures += 1
                delay = max(delay, backoff(failures, 2, 60, exc.delay if isinstance(exc, HttpFailure) else 0))
                diagnostic = str(exc) if isinstance(exc, HttpFailure) else "invalid project discovery response"
                journal.save_health({"state": "project_discovery_retry", "observed_at": iso(time.time()),
                    "attempt": failures, "retry_after_seconds": delay, "error": diagnostic,
                    "unlimited_control_plane_retry": True})
                LOG.warning("project discovery retry %s in %.1fs: %s", failures, delay, diagnostic)
                if args.once:
                    journal.close()
                    return 1
                stop.wait(delay)
                continue
            for project in projects:
                try:
                    health = controller.tick(collector.collect(project, time.time()))
                    controller.heartbeat(health)
                    LOG.info("project %s state=%s candidates=%s incidents=%s", project,
                             health["state"], health["candidate_faults"], health["open_incidents"])
                except (HttpFailure, ValueError, KeyError, TypeError) as exc:
                    failures += 1
                    failed = True
                    delay = max(delay, backoff(failures, 2, 60, exc.delay if isinstance(exc, HttpFailure) else 0))
                    diagnostic = str(exc) if isinstance(exc, HttpFailure) else "invalid observation/configuration"
                    health = {"state": "observation_retry", "project_id": project,
                        "observed_at": iso(time.time()), "attempt": failures,
                        "retry_after_seconds": delay, "error": diagnostic, "unlimited_control_plane_retry": True}
                    journal.save_health(health)
                    LOG.warning("project %s observation retry %s in %.1fs: %s", project, failures, delay, diagnostic)
            if not failed:
                failures = 0
            if args.once:
                journal.close()
                return 1 if failed else 0
            stop.wait(delay)
    journal.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
