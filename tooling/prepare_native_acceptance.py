#!/usr/bin/env python3
"""Start a fresh metadata-only identity/project acceptance cohort on a live fleet.

No host edits or restarts. Refuses old/unready/mixed revisions. All creates have
stable names/IDs and read-back receipts. The parent MODEL delegates via native
MCP; this script does not manufacture child sessions or fake their tool results.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import sys
import uuid
from configure_native_board import Board, ConfigError, LIMITS

HOSTS = ("agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4")
ENDPOINTS = {host: f"http://127.0.0.1:{17871 + index}" for index, host in enumerate(HOSTS)}


def metadata(row):
    value = row.get("metadata", {})
    return json.loads(value) if isinstance(value, str) else value


def parent_instructions(run_id):
    marker = f".runtime/native-acceptance/{run_id}/marker.txt"
    return f'''Execute this bounded LIVE acceptance task, not a plan. Run ID: {run_id}.
Your profile, tools, and identity were created only in Task Board. Keep the selected
Astra/max, 128000 output, 1050000 shared context, null turn cap and unlimited retries.

1. Read this full task and your own charter. Set this task in_progress. Do not
register/rename an identity, change runtime/configuration, or edit tracked source.
Use workspace_run for hostname, pwd, and git rev-parse HEAD. Write {marker} with
JSON containing run_id={run_id}, slot=parent, and your actual session ID and cwd.
Read it back through workspace_read. Save the absolute parent marker path.

2. Use YOUR native__session_create tool to create exactly THREE concurrent children
under YOUR SAME agent identity, with no task_id. Stable request_ids are
{run_id}:child-1, {run_id}:child-2, {run_id}:child-3. Include the same run ID, this
parent task ID, the marker relative path, actual absolute parent marker path, and
unique slot child-1/child-2/child-3 in each child's input. Tell each child:
- Do not delegate or edit source/config. Get own hostname/cwd/git revision via
workspace_run, write the SAME relative marker path with own slot/session/cwd,
then read it back. Never use shell commands to inspect another workspace.
- Attempt workspace_read of the given absolute parent marker path. It must be a
normal denied tool result, not an escape, and must not strand the native session.
- Post one Board comment on the parent task with run ID, slot, session, host, cwd,
read-back result and denied cross-read. Omit principal/author and do not call
register_agent: this proves runtime-bound Board attribution without host env edits.
Include ordinary technical text 'GET PM - native MCP' without a lint bypass.
- Finish after reporting. Do not mark the parent's task done.
Do not bypass uncertain create outcomes with another key. Reconcile original
receipts if needed; a known capacity rejection may be retried with the SAME intent.

3. After all three create receipts, keep this parent session concurrently alive
with ONE workspace_run: python3 -c "import time; time.sleep(90); print('acceptance overlap barrier complete')",
timeout_seconds=120. This is a declared bounded tool wait, not a dead worker.
Then read your own marker again and confirm it still says parent. Report the
three original child request/session IDs, actual hostname/cwd/revision, marker
readback and same-identity delegation on this task; set this task done and finish.
Children may report later; pm independently verifies all four outcomes, host spread,
actual overlap, filesystem isolation and deployment. Do not claim project completion.
'''


def specs(run_id):
    identity = "native-acceptance-" + run_id
    return {
        "agent": {"agent_id": identity, "display_name": "Native acceptance engineer",
                  "charter": "Perform only the assigned live acceptance task. Use metadata-selected tools and full model limits. Preserve original tool receipts, reconcile uncertainty, and report concrete evidence. Do not alter host configuration or credentials.",
                  "metadata": {"role": "native-engineer", "harness": "native", "acceptance_run_id": run_id,
                               "placement": {"hosts": list(HOSTS)}}},
        "project": {"name": "Native live acceptance " + run_id, "principal": "pm",
                    "description": "Live metadata identity, model self-delegation, four-host placement and workspace receipt demonstration. Coordinated from native delivery task 1.",
                    "metadata": {"acceptance_run_id": run_id, "delivery_project_id": 1, "owner": "pm"}},
        "task": {"title": "Run live same-identity delegation and workspace probe " + run_id,
                 "assignee": identity, "principal": "pm", "description": parent_instructions(run_id),
                 "metadata": {"acceptance_run_id": run_id},
                 "external_link": {"source": "native-live-acceptance", "external_id": run_id + ":parent"}},
        "marker": f".runtime/native-acceptance/{run_id}/marker.txt",
    }


def check_health(health, host, revision):
    if not (health.get("host_id") == host and health.get("revision") == revision
            and health.get("ready") is True and health.get("draining") is False and health.get("instance_id")):
        raise ConfigError(f"{host} is not verified ready at the selected integrated revision")


def check_profile(profile):
    if not profile.get("profile_source") or any(key not in profile or profile[key] != value for key, value in LIMITS.items()):
        raise ConfigError("fresh identity profile did not inherit full limits/unlimited policy from Board")


def ensure_record(find, create, matches):
    current = find()
    if current is not None:
        if not matches(current):
            raise ConfigError("existing acceptance record has different intent; refusing to overwrite")
        return current
    try:
        create()
    except ConfigError:
        pass  # May have committed; use a separate receipt, never retry the POST here.
    current = find()
    if current is None or not matches(current):
        raise ConfigError("acceptance create not confirmed; inspect original stable ID before retrying")
    return current


def start(board, runtimes, run_id, revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision or ""):
        raise ConfigError("a full integrated revision is required for a live acceptance run")
    if set(runtimes) != set(HOSTS):
        raise ConfigError("all four lab endpoints are required")
    if board.request("GET", "/api/health").get("commit") != revision:
        raise ConfigError("shared Board has not activated the selected integrated revision")
    before = {}
    for host, client in runtimes.items():
        before[host] = client.request("GET", "/health")
        check_health(before[host], host, revision)
    hosts = {h["host_id"]: h for h in board.request("GET", "/api/fleet/hosts")}
    if any(h not in hosts or not hosts[h].get("fresh") or not hosts[h].get("enabled") for h in HOSTS):
        raise ConfigError("not all four Fleet workers have fresh enabled heartbeats")
    if sum(max(0, hosts[h]["capacity"] - hosts[h]["active"]) for h in HOSTS) < 4:
        raise ConfigError("fewer than four free session slots for the controlled cohort")
    desired = specs(run_id)
    identity = desired["agent"]["agent_id"]
    def find_agent():
        try:
            return board.request("GET", "/api/agents/" + identity)
        except ConfigError as error:
            if str(error) == "Board HTTP 404":
                return None
            raise
    agent = ensure_record(find_agent, lambda: board.request("POST", "/api/agents", desired["agent"]),
        lambda row: row.get("id") == identity and metadata(row).get("acceptance_run_id") == run_id
            and metadata(row).get("role") == "native-engineer" and metadata(row).get("harness") == "native")
    profiles = {}
    for host, client in runtimes.items():
        candidates = [p for p in client.request("GET", "/api/agents") if p.get("id") == agent["id"]]
        if len(candidates) != 1:
            raise ConfigError("new Board identity is not visible in every running native service")
        check_profile(candidates[0])
        after = client.request("GET", "/health")
        check_health(after, host, revision)
        if after["instance_id"] != before[host]["instance_id"]:
            raise ConfigError("runtime restarted during metadata-only identity demonstration")
        profiles[host] = {key: candidates[0][key] for key in (*LIMITS, "profile_source")}
    project = ensure_record(
        lambda: next((p for p in board.request("GET", "/api/projects") if p["name"] == desired["project"]["name"]), None),
        lambda: board.request("POST", "/api/projects", desired["project"]),
        lambda row: metadata(row).get("acceptance_run_id") == run_id)
    task_body = {**desired["task"], "project_id": project["id"]}
    task = ensure_record(
        lambda: next((t for t in board.request("GET", f'/api/tasks?project_id={project["id"]}')
                      if metadata(t).get("acceptance_run_id") == run_id), None),
        lambda: board.request("POST", "/api/tasks", task_body),
        lambda row: row.get("assignee") == identity and row.get("description") == task_body["description"])
    return {"run_id": run_id, "revision": revision, "agent_id": identity, "project_id": project["id"], "task_id": task["id"],
            "marker": desired["marker"], "profiles": profiles,
            "instances": {h: v["instance_id"] for h, v in before.items()},
            "scope": "fresh identity loaded on four unchanged processes; task admitted for MODEL self-delegation, not yet claimed complete"}


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--board", default="http://127.0.0.1:8079")
    parser.add_argument("--revision")
    parser.add_argument("--state", type=Path, default=Path(".runtime/acceptance/live-run.json"))
    parser.add_argument("--start", action="store_true", help="Create the live identity/project/task after readiness checks")
    args = parser.parse_args(argv)
    try:
        if args.state.exists():
            state = json.loads(args.state.read_text())
        else:
            run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
            state = {"run_id": run_id, "prepared": specs(run_id)}
            atomic_json(args.state, state)  # Stable IDs precede any remote mutation.
        if args.start:
            state["started"] = start(Board(args.board), {h: Board(u) for h, u in ENDPOINTS.items()}, state["run_id"], args.revision)
            atomic_json(args.state, state)
            print(json.dumps(state["started"], indent=2, sort_keys=True))
        else:
            print(json.dumps({"prepared": True, "run_id": state["run_id"], "identity": state["prepared"]["agent"]["agent_id"],
                              "state": str(args.state), "remote_writes": 0}))
        return 0
    except (ConfigError, OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"ok": False, "error": str(error) if isinstance(error, ConfigError) else "invalid acceptance state/input"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
