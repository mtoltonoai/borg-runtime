#!/usr/bin/env python3
"""Bounded read-only dependency probes for native systemd startup.

Unlike an HTTP 200 check this rejects the Board's SPA fallback. A failed probe
exits nonzero; systemd Restart=always retries startup, not any model/tool action.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def request_json(url, body=None, headers=None, timeout=5):
    headers = dict(headers or {})
    headers.setdefault("Accept", "application/json, text/event-stream")
    data = None if body is None else json.dumps(body).encode()
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise ValueError("oversized discovery response")
        content_type = response.headers.get("Content-Type", "")
        if "text/event-stream" in content_type:
            messages = [line[5:].strip() for line in raw.decode().splitlines()
                        if line.startswith("data:")]
            values = [json.loads(line) for line in messages if line]
            if not values:
                raise ValueError("no MCP response")
            value = next((v for v in values if v.get("id") == (body or {}).get("id")), values[-1])
        else:
            value = json.loads(raw)
        return value, response.headers.get("Mcp-Session-Id")


def probe_board(url):
    value, _ = request_json(url.rstrip("/") + "/projects")
    if not isinstance(value, list):
        raise ValueError("Board projects is not a JSON array")


def probe_native(url):
    value, _ = request_json(url.rstrip("/") + "/health")
    if not isinstance(value, dict) or value.get("service") != "borg-native" or value.get("status") != "ok":
        raise ValueError("native health did not identify a ready runtime")


def probe_workspace(url):
    value, session = request_json(url, {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                   "clientInfo": {"name": "borg-deployment-readiness", "version": "1"}},
    })
    if "result" not in value or "error" in value:
        raise ValueError("workspace MCP initialize failed")
    headers = {"MCP-Protocol-Version": value["result"].get("protocolVersion", "2025-03-26")}
    if session:
        headers["Mcp-Session-Id"] = session
    # Workspace's stateless MCP supports list after initialization; no tool is executed.
    value, _ = request_json(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, headers)
    tools = value.get("result", {}).get("tools")
    if not isinstance(tools, list) or not any(t.get("name") == "workspace_run" for t in tools):
        raise ValueError("workspace MCP tool discovery failed")


def wait_ready(probes, wait_seconds=60, interval=2):
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            for name, probe in probes:
                probe()
            return
        except (OSError, ValueError, KeyError, TypeError) as error:
            if time.monotonic() >= deadline:
                # No response bodies, credentials or configured headers in logs.
                raise RuntimeError(f"dependency {name} not ready ({type(error).__name__})") from None
            time.sleep(min(interval, max(0, deadline - time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--board-api")
    parser.add_argument("--native-url")
    parser.add_argument("--workspace-url")
    parser.add_argument("--wait-seconds", type=float, default=60)
    args = parser.parse_args()
    probes = []
    if args.board_api:
        probes.append(("board", lambda: probe_board(args.board_api)))
    if args.native_url:
        probes.append(("native", lambda: probe_native(args.native_url)))
    if args.workspace_url:
        probes.append(("workspace", lambda: probe_workspace(args.workspace_url)))
    if not probes:
        parser.error("at least one dependency is required")
    try:
        wait_ready(probes, args.wait_seconds)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps({"ready": [name for name, _ in probes]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
