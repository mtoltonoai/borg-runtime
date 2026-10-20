#!/usr/bin/env python3
"""Read-only, repeatable evidence collector for a live native parent/child proof.

Never enqueues, resumes or retries a session. GETs durable dispatch/session/tool
records and calls only workspace_read on the two allocated MCP endpoints.
Use a stable parent Fleet request_id and a durable trace to retain overlap.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.error
import urllib.request

MARKER = "native-lifecycle-proof.json"


def api(base, path):
    return base.rstrip("/").removesuffix("/api") + "/api/" + path.lstrip("/")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


class Client:
    def __init__(self, token=None, workspace_origins=None):
        self.token = token
        self.workspace_origins = workspace_origins or {}
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url, body=None, agent=None):
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if agent:
            headers["X-Fleet-Agent"] = agent
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, headers=headers,
                                         data=None if body is None else json.dumps(body).encode())
        with self.opener.open(request, timeout=15) as response:
            return json.load(response)

    def marker(self, session):
        url = session["workspace_mcp_url"]
        url = url.replace("{session_id}", urllib.parse.quote(session["id"], safe=""))
        url = url.replace("{agent_id}", urllib.parse.quote(session["agent_id"], safe=""))
        url = url.replace("{runtime_url}", session.get("runtime_url", "").rstrip("/").removesuffix("/api"))
        # Workspace URLs are intentionally host-local for the executing native
        # runtime. A cross-host observer must use an explicit owner-host route,
        # never silently read a different host's identically numbered helper.
        origin = self.workspace_origins.get(session.get("host_id"))
        if origin is not None:
            parsed = urllib.parse.urlsplit(url)
            url = origin.rstrip("/") + parsed.path
        response = self.request(url, {"jsonrpc": "2.0", "id": "read-proof", "method": "tools/call",
                                     "params": {"name": "workspace_read", "arguments": {"path": MARKER}}},
                                agent=session["agent_id"])
        return json.loads(content(response["result"])["content"])


def content(result):
    if not isinstance(result, dict) or result.get("isError"):
        raise ValueError("original tool result is not a verified success")
    if "structuredContent" in result:
        return result["structuredContent"]
    text = next(item["text"] for item in result["content"] if item.get("type") == "text")
    return json.loads(text)


def summary(session):
    return {key: session.get(key) for key in ("id", "agent_id", "host_id", "workspace_cwd", "status",
                                            "driver_running", "created_at", "updated_at")}


def query(client, board_url, **filters):
    rows = client.request(api(board_url, "fleet/dispatches?") + urllib.parse.urlencode(filters))
    if not isinstance(rows, list):
        raise ValueError("Fleet did not return a dispatch list")
    return rows


def inspect(client, board_url, request_id, token, previous_overlap=False):
    rows = query(client, board_url, request_id=request_id, limit=2)
    if len(rows) != 1:
        raise ValueError("expected exactly one existing parent dispatch; no new work was created")
    dispatch = rows[0]
    parent_id = dispatch["session_id"]
    try:
        parent = client.request(api(dispatch["runtime_url"], f"sessions/{parent_id}"))
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return {"observed_at": datetime.now(timezone.utc).isoformat(), "request_id": request_id,
                "parent": {"id": parent_id}, "state": "waiting", "overlap_seen": previous_overlap,
                "reason": "Fleet is provisioning the parent session"}
    parent_tools = client.request(api(dispatch["runtime_url"], f"sessions/{parent_id}/tools"))
    sample = {"observed_at": datetime.now(timezone.utc).isoformat(), "request_id": request_id,
              "parent": summary(parent), "state": "waiting", "overlap_seen": previous_overlap}
    creates = [call for call in parent_tools if call["name"] == "native__session_create"]
    if any(call["status"] == "unknown" for call in creates):
        return dict(sample, state="blocked", reason="Original lifecycle outcome needs resolution; no replay attempted")
    successful = [call for call in creates if call["status"] == "succeeded" and not call["result"].get("isError")]
    if not successful:
        return dict(sample, reason="Parent has not committed a successful child creation receipt")
    if len(successful) != 1:
        return dict(sample, state="failed", reason="Expected one successful child creation receipt")
    call = successful[0]
    receipt = content(call["result"])
    child_id = receipt["session_id"]
    children = query(client, board_url, session_id=child_id, limit=2)
    if len(children) != 1:
        return dict(sample, reason="Child dispatch is not yet observable")
    child_dispatch = children[0]
    try:
        child = client.request(api(child_dispatch["runtime_url"], f"sessions/{child_id}"))
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return dict(sample, reason="Fleet is provisioning the child session", child_id=child_id)
    sample.update(child=summary(child), operation_id=call["operation_id"],
                  child_request_id=receipt["request_id"], child_dispatch_id=child_dispatch["id"])
    sample["overlap_seen"] |= bool(parent.get("driver_running") and child.get("driver_running"))
    if any(session["status"] in ("failed", "needs_resolution") for session in (parent, child)):
        return dict(sample, state="blocked", reason="A native session needs owned repair; no control operation attempted")
    if parent["status"] != "completed" or child["status"] != "completed":
        return sample
    child_tools = client.request(api(child_dispatch["runtime_url"], f"sessions/{child_id}/tools"))
    parent_marker, child_marker = client.marker(parent), client.marker(child)
    sample.update(parent_marker=parent_marker, child_marker=child_marker)
    def wrote_marker(calls):
        return any(call["name"] == "workspace__workspace_write" and call["status"] == "succeeded"
                   and not call["result"].get("isError") and call["arguments"].get("path") == MARKER for call in calls)
    expected_request = f"mcp:{parent_id}:{call['arguments'].get('request_id', call['operation_id'])}"
    checks = {
        "independent_sessions": parent_id != child_id,
        "same_identity": parent["agent_id"] == child["agent_id"] == receipt["agent_id"],
        "parent_receipt": receipt["parent_session_id"] == parent_id,
        "operation_request_dispatch_join": receipt["request_id"] == child_dispatch["request_id"] == expected_request,
        "distinct_workspaces": bool(parent.get("workspace_cwd")) and parent["workspace_cwd"] != child.get("workspace_cwd"),
        "both_used_workspace_tools": wrote_marker(parent_tools) and wrote_marker(child_tools),
        "parent_marker_unchanged": parent_marker == {"role": "parent", "session_id": parent_id, "token": token},
        "child_marker_independent": child_marker == {"role": "child", "session_id": child_id, "token": token},
        "concurrent_drivers_observed": sample["overlap_seen"],
    }
    return dict(sample, checks=checks, state="passed" if all(checks.values()) else "failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--board-url", default="http://127.0.0.1:8079")
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--token", required=True, help="Public marker proof token, not an authentication credential")
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--bearer-token-env")
    parser.add_argument("--workspace-origin", action="append", default=[], metavar="HOST=ORIGIN",
                        help="Explicit owner-host helper route, e.g. agent-lab=http://127.0.0.1:18791 via an SSH forward")
    parser.add_argument("--watch-seconds", type=float, default=0)
    parser.add_argument("--interval", type=float, default=2)
    args = parser.parse_args()
    if args.watch_seconds < 0 or args.interval <= 0:
        parser.error("watch duration must be nonnegative; interval must be positive")
    bearer = os.environ[args.bearer_token_env] if args.bearer_token_env else None
    origins = {}
    for binding in args.workspace_origin:
        host, sep, origin = binding.partition("=")
        parsed = urllib.parse.urlsplit(origin)
        if not host or not sep or parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            parser.error("workspace routes must be HOST=http(s)://host:port without credentials/path/query/fragment")
        origins[host] = origin
    client = Client(bearer, origins)
    overlap = False
    if args.trace.exists():
        for line in args.trace.read_text().splitlines():
            previous = json.loads(line)
            if previous["request_id"] != args.request_id:
                raise ValueError("trace belongs to another proof request")
            overlap |= previous.get("overlap_seen", False)
    args.trace.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.watch_seconds
    while True:
        sample = inspect(client, args.board_url, args.request_id, args.token, overlap)
        overlap |= sample["overlap_seen"]
        with args.trace.open("a") as log:
            log.write(json.dumps(sample, sort_keys=True) + "\n")
            log.flush()
            os.fsync(log.fileno())
        if sample["state"] != "waiting" or time.monotonic() >= deadline:
            print(json.dumps(sample, indent=2, sort_keys=True))
            return 0 if sample["state"] == "passed" else 2
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
