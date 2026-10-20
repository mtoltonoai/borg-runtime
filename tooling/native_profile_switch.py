#!/usr/bin/env python3
"""Migrate already-drained bootstrap sessions to Board profiles and explicit bindings.

Infrastructure config changes only after a passive receipt-preserving drain.
Existing worktrees and original committed intents are never copied or moved.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.request

sys.dont_write_bytecode = True
from native_metadata import runtime_config
from native_release import BASE, atomic_json, get_json, require_quiet, session_snapshot, unit, sha256


def request(path, body=None):
    request = urllib.request.Request("http://127.0.0.1:8787/api" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def builder_route_units(proxy):
    return {
        "borg-native-builder-route.socket": "[Unit]\nDescription=Borg builder native self-route\n[Socket]\nListenStream=127.0.0.1:17870\nNoDelay=true\n[Install]\nWantedBy=sockets.target\n",
        "borg-native-builder-route.service": f"[Unit]\nDescription=Borg builder native loopback proxy\n[Service]\nUser=mtolton\nExecStart={proxy} 127.0.0.1:8787\nRestart=on-failure\n",
    }


def ensure_origin(args, sessions):
    if args.host_id == "borg-builder":
        proxy = next((p for p in ("/lib/systemd/systemd-socket-proxyd", "/usr/lib/systemd/systemd-socket-proxyd") if Path(p).is_file()), None)
        if proxy is None:
            raise RuntimeError("systemd socket proxy missing for builder self-route")
        for name, text in builder_route_units(proxy).items():
            path = args.base / "config" / name
            path.write_text(text)
            subprocess.run(["sudo", "-n", "install", "-m", "644", str(path), "/etc/systemd/system/" + name], check=True)
        subprocess.run(["sudo", "-n", "systemctl", "daemon-reload"], check=True)
        subprocess.run(["sudo", "-n", "systemctl", "enable", "--now", "borg-native-builder-route.socket"], check=True)
    from native_ready import probe_native, wait_ready
    wait_ready([("advertised-native-origin", lambda: probe_native(args.runtime_origin))])
    advertised = get_json(args.runtime_origin.rstrip("/") + "/api/sessions")
    if not {s["id"] for s in sessions}.issubset({s["id"] for s in advertised}):
        raise RuntimeError("advertised runtime origin reaches another session database")


def prepare(args):
    require_quiet(session_snapshot("http://127.0.0.1:8787"))
    journal = args.base / "config" / ("profile-switch-" + args.transaction + ".json")
    if journal.exists():
        raise RuntimeError("profile switch already has a journal; reconcile before reapplying")
    path = args.base / "config/runtime.json"
    previous = json.loads(path.read_text())
    sessions = request("/sessions")
    ensure_origin(args, sessions)
    bindings = []
    for session in sessions:
        # Preserve existing explicit placement; adopt only missing bootstrap roots.
        root = session.get("workspace_cwd") or (
            "/home/dev-user/borg-pm-workspace" if session["agent_id"] == "pm" else
            "/home/dev-user/borg-native-integration" if args.host_id == "borg-builder" else
            "/home/dev-user/borg-native-workspace")
        endpoint = session.get("workspace_mcp_url") or (
            "http://127.0.0.1:8792/mcp" if session["agent_id"] == "pm" else "http://127.0.0.1:8766/mcp")
        if not Path(root).is_dir():
            raise RuntimeError("bootstrap workspace missing; no silent replacement")
        bindings.append({"id": session["id"], "agent_id": session["agent_id"], "options": {
            "session_id": session["id"], "host_id": args.host_id, "workspace_cwd": root,
            "workspace_mcp_url": endpoint,
            "lifecycle_mcp_url": args.runtime_origin.rstrip("/") + "/mcp/sessions/" + session["id"],
            "generation": session.get("generation", 0),
            "metadata": {**(session.get("metadata") or {}), "bootstrap_adopted": True},
        }})
    candidate = runtime_config(args.base, args.host_id, args.runtime_origin)
    if previous.get("bearer_token_env"):
        candidate["bearer_token_env"] = previous["bearer_token_env"]
    previous_unit = args.base / "config" / ("native-before-" + args.transaction + ".service")
    shutil.copyfile("/etc/systemd/system/borg-native.service", previous_unit)
    previous_unit.chmod(0o600)
    pid = subprocess.check_output(["systemctl", "show", "borg-native", "--property=MainPID", "--value"], text=True).strip()
    previous_image = sha256(Path("/proc") / pid / "exe")
    atomic_json(journal, {"state": "prepared", "previous_unit": str(previous_unit), "previous_image": previous_image, "previous_config": previous, "candidate": candidate,
                          "bindings": bindings, "bound": [], "owner": "integrator", "task_id": 6})
    atomic_json(path, candidate)
    return {"journal": str(journal), "state": "prepared", "sessions": len(bindings)}


def bind(args):
    journal = args.base / "config" / ("profile-switch-" + args.transaction + ".json")
    value = json.loads(journal.read_text())
    require_quiet(session_snapshot("http://127.0.0.1:8787"))
    pm = next((b for b in value["bindings"] if b["agent_id"] == "pm"), None)
    if pm:
        # The PM's actual source checkout was already separate during bootstrap;
        # this separate endpoint makes the binding true without moving its WIP.
        text = unit("workspace", args.release, args.base, args.host_id,
                    workspace=pm["options"]["workspace_cwd"]).replace("--port 8766", "--port 8792")
        path = args.base / "config/borg-pm-workspace.service"
        path.write_text(text)
        subprocess.run(["sudo", "-n", "install", "-m", "644", str(path), "/etc/systemd/system/borg-pm-workspace.service"], check=True)
        subprocess.run(["sudo", "-n", "systemctl", "daemon-reload"], check=True)
        subprocess.run(["sudo", "-n", "systemctl", "enable", "--now", "borg-pm-workspace.service"], check=True)
        from native_ready import probe_workspace, wait_ready
        wait_ready([("pm-workspace", lambda: probe_workspace("http://127.0.0.1:8792/mcp"))])
    for binding in value["bindings"]:
        if binding["id"] in value["bound"]:
            continue
        try:
            result = request("/sessions/" + binding["id"] + "/bind", binding["options"])
        except OSError:
            # Reconcile the single binding write before any possible retry.
            result = request("/sessions/" + binding["id"])
        for key in ("host_id", "workspace_cwd", "workspace_mcp_url", "lifecycle_mcp_url", "generation"):
            if result.get(key) != binding["options"][key]:
                raise RuntimeError("binding not verified; no resume: " + binding["id"])
        value["bound"].append(binding["id"])
        atomic_json(journal, value)
    value["state"] = "bound"
    atomic_json(journal, value)
    return {"journal": str(journal), "state": "bound", "sessions": len(value["bound"])}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("prepare", "bind", "restore-config", "restore-runtime"))
    p.add_argument("--base", type=Path, default=BASE)
    p.add_argument("--release", type=Path, required=True)
    p.add_argument("--transaction", required=True)
    p.add_argument("--host-id", required=True)
    p.add_argument("--runtime-origin", required=True)
    args = p.parse_args()
    if args.action == "prepare":
        result = prepare(args)
    elif args.action == "bind":
        result = bind(args)
    else:
        journal = args.base / "config" / ("profile-switch-" + args.transaction + ".json")
        value = json.loads(journal.read_text())
        atomic_json(args.base / "config/runtime.json", value["previous_config"])
        value["state"] = "config_restored"
        if args.action == "restore-runtime":
            require_quiet(session_snapshot("http://127.0.0.1:8787"))
            subprocess.run(["sudo", "-n", "install", "-m", "644", value["previous_unit"], "/etc/systemd/system/borg-native.service"], check=True)
            subprocess.run(["sudo", "-n", "systemctl", "daemon-reload"], check=True)
            subprocess.run(["sudo", "-n", "systemctl", "restart", "borg-native.service"], check=True)
            from native_ready import probe_native, wait_ready
            wait_ready([("native", lambda: probe_native("http://127.0.0.1:8787"))])
            pid = subprocess.check_output(["systemctl", "show", "borg-native", "--property=MainPID", "--value"], text=True).strip()
            if sha256(Path("/proc") / pid / "exe") != value["previous_image"]:
                raise RuntimeError("previous runtime image did not restore; keep work paused")
            value["state"] = "runtime_restored"
        atomic_json(journal, value)
        result = {"state": value["state"]}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
