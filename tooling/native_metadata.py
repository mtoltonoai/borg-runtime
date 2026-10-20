#!/usr/bin/env python3
"""Seed Board-owned native role profiles/tools; render infrastructure-only JSON.

No new identity requires a host JSON edit. Role configuration is idempotently
updated only when its desired entry changes. Credentials are environment names,
never values. Rendering a host config does not activate it on a running actor.
"""
import argparse
import json
from pathlib import Path
import urllib.parse
import urllib.request

ROLES = ("pm", "native-core", "fleet-engineer", "recovery-engineer", "tools-engineer", "integrator", "native-worker")
PROFILE = {
    "model": "gpt-6-astra", "reasoning_effort": "max", "max_output_tokens": 128000,
    "context_window_tokens": 1050000, "max_turns": None, "max_retries": None,
    "retry_initial_ms": 5000, "retry_max_ms": 60000, "retry_jitter_percent": 20,
    "provider_url": "https://api.openai.com/v1/responses", "api_key_env": "OPENAI_API_KEY",
}


def role_entries(board_tools=None):
    board = {"kind": "mcp", "name": "board", "url": "http://127.0.0.1:8079/mcp"}
    if board_tools is not None:
        board["tools_allowlist"] = board_tools
    return {
        "runtime": [{"entry_id": "profile", "payload": dict(PROFILE)}],
        "tool": [
            {"entry_id": "board", "payload": board},
            {"entry_id": "workspace", "payload": {"kind": "mcp", "name": "workspace", "url": "http://127.0.0.1:8791/mcp/{session_id}"}},
            {"entry_id": "native", "payload": {"kind": "mcp", "name": "native", "url": "{runtime_url}/mcp/sessions/{session_id}"}},
            {"entry_id": "web", "payload": {"kind": "provider", "tool": {"type": "web_search"}}},
        ],
    }


def request(base, path, body=None):
    req = urllib.request.Request(base.rstrip("/") + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Fleet-Agent": "integrator"})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def seed(board_api, roles, entries, call=request, overwrite_existing=False):
    evidence = []
    for role in roles:
        for kind, desired in entries.items():
            path = "/roles/" + urllib.parse.quote(role, safe="") + "/config/" + kind
            current = call(board_api, path)
            old = {entry["id"]: entry for entry in current.get("entries", [])}
            for position, entry in enumerate(desired):
                want = {"enabled": True, "scope": None, "payload": entry["payload"], "position": position}
                prior = old.get(entry["entry_id"], {})
                unchanged = all(prior.get(key) == value for key, value in want.items())
                changed = not unchanged and (overwrite_existing or not prior)
                if changed:
                    result = call(board_api, path, {"entry_id": entry["entry_id"], **want, "actor": "integrator"})
                else:
                    result = current
                evidence.append({"role": role, "config_kind": kind, "entry_id": entry["entry_id"],
                                 "changed": changed, "preserved_existing": bool(prior) and not unchanged and not overwrite_existing,
                                 "version": result.get("version")})
    return evidence


def runtime_config(base, host_id, runtime_origin):
    parsed = urllib.parse.urlparse(runtime_origin)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path not in ("", "/"):
        raise ValueError("runtime_url is an origin, NOT the Fleet /api URL")
    if parsed.username or parsed.password:
        raise ValueError("credentials must not be embedded in runtime URLs")
    return {
        "database_path": str(Path(base) / "state" / "native.sqlite"),
        "listen": "127.0.0.1:8787", "agents": [],
        "board": {"url": "http://127.0.0.1:8079", "timeout_ms": 10000, "report_interval_ms": 5000},
        "host_id": host_id, "runtime_url": runtime_origin.rstrip("/"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("seed")
    p.add_argument("--board-api", default="http://127.0.0.1:8079/api")
    p.add_argument("--roles", nargs="+", default=list(ROLES))
    p.add_argument("--overwrite-existing", action="store_true", help="explicitly replace existing role policy; default preserves it")
    p.add_argument("--example", type=Path, default=Path(__file__).resolve().parents[1] / "config/runtime.example.json")
    p = sub.add_parser("render-runtime")
    p.add_argument("--base", default="/home/dev-user/.local/share/borg")
    p.add_argument("--host-id", required=True)
    p.add_argument("--runtime-origin", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "seed":
        example = json.loads(args.example.read_text())
        tools = next(s for s in example["agents"][0]["mcp_servers"] if s["name"] == "board").get("tools_allowlist")
        print(json.dumps(seed(args.board_api, args.roles, role_entries(tools), overwrite_existing=args.overwrite_existing)))
    else:
        from native_release import atomic_json
        config = runtime_config(args.base, args.host_id, args.runtime_origin)
        atomic_json(args.output, config)
        print(json.dumps({"candidate": str(args.output), "host": args.host_id, "identity_list": [], "activated": False}))


if __name__ == "__main__":
    main()
