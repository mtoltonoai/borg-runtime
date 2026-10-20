#!/usr/bin/env python3
"""Immutable release verification, guarded systemd activation and live evidence.

Run on the destination host. Never copies/replaces a live database, cancels a
session, resolves an unknown tool, or reads credential files. Active bootstrap
sessions are automatically drained at a receipt-preserving execution boundary.
"""
import argparse
import datetime as dt
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.request

sys.dont_write_bytecode = True

BASE = Path("/home/dev-user/.local/share/borg")
COMPONENTS = ("board", "workspace", "session-workspace", "native", "fleet", "recovery")
TERMINAL = {"completed", "stopped", "waiting", "failed", "needs_resolution", "quiesced"}
LEGACY = ("borg-controller.service", "borg-worker.service", "borg-deployer.service")


def run(args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def release_files(path):
    result = {}
    for item in sorted(path.rglob("*")):
        if item.is_symlink():
            raise ValueError("release symlinks are not allowed")
        if item.is_file() and item.relative_to(path).as_posix() != "revision.json":
            result[item.relative_to(path).as_posix()] = sha256(item)
    return result


def manifest(path, revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("revision must be a full Git commit")
    value = {"schema": 1, "revision": revision, "built_at": stamp(),
             "files": release_files(path)}
    atomic_json(path / "revision.json", value)
    return value


def verify(path):
    if path.is_symlink() or not path.is_dir():
        raise ValueError("release must be a real directory")
    value = json.loads((path / "revision.json").read_text())
    if value.get("schema") != 1 or not re.fullmatch(r"[0-9a-f]{40}", value.get("revision", "")):
        raise ValueError("invalid manifest")
    if release_files(path) != value.get("files"):
        raise ValueError("release contents do not match checksums")
    for required in ("borg-native", "fleet", "task-board", "workspace_mcp.py",
                     "native_ready.py", "native_release.py", "web/index.html"):
        if required not in value["files"]:
            raise ValueError(f"release missing {required}")
    return value


def get_json(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def session_snapshot(url):
    url = url.rstrip("/")
    sessions = get_json(url + "/api/sessions")
    if not isinstance(sessions, list):
        raise ValueError("native sessions response is not an array")
    result = []
    for session in sessions:
        sid = session["id"]
        tools = get_json(url + "/api/sessions/" + sid + "/tools")
        transcript = get_json(url + "/api/sessions/" + sid + "/transcript")
        receipts = {}
        for entry in transcript:
            if entry.get("kind") == "tool_result":
                data = json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()
                receipts[str(entry["sequence"])] = hashlib.sha256(data).hexdigest()
        result.append({"id": sid, "agent_id": session["agent_id"],
                       "status": session["status"], "progress": session.get("progress"),
                       "tools": {t["call_id"]: t["status"] for t in tools},
                       "receipt_hashes": receipts})
    return result


def require_quiet(sessions):
    unsafe = []
    for session in sessions:
        if session["status"] not in TERMINAL:
            unsafe.append(session["id"] + ":" + session["status"])
        if any(state in ("running", "unknown") for state in session["tools"].values()):
            unsafe.append(session["id"] + ":unresolved-tool")
    if unsafe:
        raise RuntimeError("refusing activation; preserve active/unknown tool results: " + ", ".join(unsafe))


def verify_preserved(before, after):
    by_id = {s["id"]: s for s in after}
    for old in before:
        if old["id"] not in by_id:
            raise RuntimeError("session disappeared across deployment")
        new = by_id[old["id"]]
        for sequence, digest in old["receipt_hashes"].items():
            if new["receipt_hashes"].get(sequence) != digest:
                raise RuntimeError("committed tool result changed across deployment")
        for call_id, state in old["tools"].items():
            if state == "succeeded" and new["tools"].get(call_id) != "succeeded":
                raise RuntimeError("succeeded tool receipt changed across deployment")


def database_backup(base, destination):
    destination.mkdir(parents=True, exist_ok=False)
    result = []
    for source in sorted((base / "state").glob("*.sqlite")):
        target = destination / source.name
        # SQLite online backup includes committed WAL frames; cp would not.
        with sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=30) as src:
            with sqlite3.connect(target) as dst:
                src.backup(dst)
                integrity = dst.execute("PRAGMA integrity_check").fetchone()[0]
                if integrity != "ok":
                    raise RuntimeError("backup integrity check failed")
        os.chmod(target, 0o600)
        result.append({"name": source.name, "bytes": target.stat().st_size,
                       "sha256": sha256(target), "integrity": "ok"})
    return result


def unit(component, release, base, host_id, user="mtolton", workspace=None):
    if component not in COMPONENTS:
        raise ValueError("unknown component")
    # Systemd values cannot contain line/control/specifier injection.
    for value in (release, base, host_id, user, workspace or base):
        if not re.fullmatch(r"[A-Za-z0-9_./:@+-]+", str(value)):
            raise ValueError("unsupported character in unit path/identity")
    release, base = Path(release), Path(base)
    dependency = "borg-board.service" if host_id == "agent-lab" else "borg-board-tunnel.service"
    dependencies = {
        "board": [], "workspace": [], "session-workspace": [], "native": [dependency, "borg-workspace.service", "borg-session-workspace.service"],
        "fleet": [dependency, "borg-native.service", "borg-session-workspace.service"],
        "recovery": [dependency, "borg-native.service"],
    }[component]
    commands = {
        "board": f"{release}/task-board --config {base}/config/board.toml --web-dir {release}/web",
        "workspace": f"/usr/bin/python3 {release}/workspace_mcp.py --root {workspace or '/home/dev-user/borg-native-workspace'} --port 8766",
        "session-workspace": f"/usr/bin/python3 -B {release}/workspace_mcp.py --session-root {base}/workspaces --port 8791",
        "native": f"{release}/borg-native --config {base}/config/runtime.json",
        "fleet": f"{release}/fleet --config {base}/config/native-fleet.toml native-worker",
        "recovery": f"/usr/bin/python3 {release}/native_recovery.py --config {base}/config/native-recovery.json",
    }
    pre = ""
    if component in ("native", "fleet", "recovery"):
        pre = f"ExecStartPre=/usr/bin/python3 {release}/native_ready.py --board-api http://127.0.0.1:8079/api"
        if component == "native":
            pre += " --workspace-url http://127.0.0.1:8766/mcp"
        else:
            pre += " --native-url http://127.0.0.1:8787"
        pre += " --wait-seconds 60\n"
    credential = f"EnvironmentFile={base}/config/native.env\n" if component == "native" else ""
    return f"""[Unit]
Description=Borg persistent native {component} ({host_id})
After=network-online.target {' '.join(dependencies)}
Wants=network-online.target {' '.join(dependencies)}
StartLimitIntervalSec=0

[Service]
Type=simple
User={user}
Group={user}
WorkingDirectory={base}
Environment=PATH=/home/dev-user/.cargo/bin:/home/dev-user/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=BORG_BUILD_REV={release.name}
Environment=BORG_HOST_ID={host_id}
Environment=PYTHONDONTWRITEBYTECODE=1
{credential}{pre}ExecStart={commands[component]}
Restart=always
RestartSec=3
KillMode=control-group
TimeoutStartSec=90
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
"""


def service_state(name):
    output = run(["systemctl", "show", name, "--property=Id,ActiveState,SubState,UnitFileState,MainPID,ExecMainStartTimestamp"])
    state = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    pid = int(state.get("MainPID") or 0)
    if pid:
        executable = Path(f"/proc/{pid}/exe")
        try:
            state["executable"] = str(executable.readlink())
            state["executable_sha256"] = sha256(executable)
        except (OSError, PermissionError):
            state["executable_unreadable"] = True
    return state


def inventory(base):
    return {"observed_at": stamp(), "host": os.uname().nodename,
            "services": {name: service_state(name) for name in
                         tuple("borg-" + c + ".service" for c in COMPONENTS) +
                         ("borg-board-tunnel.service",) + LEGACY},
            "databases": [p.name for p in sorted((base / "state").glob("*.sqlite"))]}


def verify_running_images(snapshot, metadata, components):
    for component in components:
        state = snapshot["services"]["borg-" + component + ".service"]
        if state.get("ActiveState") != "active" or state.get("UnitFileState") != "enabled":
            raise RuntimeError(component + " is not enabled/active")
    for component, executable in (("native", "borg-native"), ("fleet", "fleet"), ("board", "task-board")):
        if component in components:
            observed = snapshot["services"]["borg-" + component + ".service"]
            if observed.get("executable_sha256") != metadata["files"][executable]:
                raise RuntimeError(component + " running executable differs from release manifest")


def install(args):
    args.base.mkdir(parents=True, exist_ok=True)
    lock_path = args.base / "state" / "deployment.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return install_locked(args)


def install_locked(args):
    release, base = args.release.resolve(), args.base
    metadata = verify(release)
    if release.name != metadata["revision"]:
        raise ValueError("activation release directory must equal its revision")
    components = args.components.split(",")
    if any(c not in COMPONENTS for c in components):
        raise ValueError("invalid component selection")
    if any(c in components for c in ("board", "recovery")) and args.host_id != "agent-lab":
        raise ValueError("shared Board and recovery ledger belong only on agent-lab")
    for name in LEGACY:
        state = service_state(name)
        if state.get("ActiveState") == "active" or state.get("UnitFileState") in ("enabled", "enabled-runtime"):
            raise RuntimeError("legacy service must stay disabled: " + name)
    for component, filename in (("board", "board.toml"), ("native", "runtime.json"),
                                ("fleet", "native-fleet.toml"), ("recovery", "native-recovery.json")):
        if component in components and not (base / "config" / filename).is_file():
            raise RuntimeError("missing infrastructure config: " + filename)
    if "recovery" in components and not (release / "native_recovery.py").is_file():
        raise RuntimeError("recovery implementation missing from release")
    record = {"started_at": stamp(), "revision": metadata["revision"],
              "host": args.host_id, "components": components, "status": "preparing",
              "owner": "integrator", "task_id": 6, "before_inventory": inventory(base)}
    record_path = base / "state" / "deployments" / (record["started_at"] + ".json")
    drain_journal = record_path.with_suffix(".drain.json")
    backup = base / "backups" / record["started_at"]
    atomic_json(record_path, record)
    before = []
    fleet_was_active = service_state("borg-fleet.service").get("ActiveState") == "active"
    from native_drain import drain, resume
    try:
        # Fence this host's scheduler before the native execution boundary. An
        # uncertain Fleet create remains reserved on Board and is reconciled.
        if fleet_was_active:
            run(["sudo", "-n", "systemctl", "stop", "borg-fleet.service"])
        if any(c in components for c in ("native", "workspace", "session-workspace", "board")):
            record["drain_journal"] = str(drain_journal)
            atomic_json(record_path, record)
            drain(base / "state" / "native.sqlite", args.runtime_url, drain_journal,
                  wait_seconds=args.wait_seconds)
            before = session_snapshot(args.runtime_url)
            require_quiet(before)
            record["before_sessions"] = before
        record["backups"] = database_backup(base, backup)
        atomic_json(record_path, record)
        for component in components:
            target = base / "config" / ("borg-" + component + ".service")
            target.parent.mkdir(parents=True, exist_ok=True)
            text = unit(component, release, base, args.host_id, args.user, args.workspace)
            target.write_text(text)
            old = Path("/etc/systemd/system") / target.name
            if old.exists():
                shutil.copy2(old, backup / old.name)
            run(["sudo", "-n", "install", "-m", "644", target, old])
        (base / "workspaces").mkdir(parents=True, exist_ok=True)
        run(["sudo", "-n", "systemctl", "daemon-reload"])
        # Reject retained ExecStart overrides before launching any work.
        for component, executable in (("native", "borg-native"), ("fleet", "fleet"), ("board", "task-board")):
            if component in components:
                effective = run(["systemctl", "show", "borg-" + component + ".service", "--property=ExecStart", "--value"])
                if "path=" + str(release / executable) + " ;" not in effective:
                    raise RuntimeError(component + " effective ExecStart overrides selected release")
        for component in COMPONENTS:
            if component not in components or component in ("fleet", "recovery"):
                continue
            name = "borg-" + component + ".service"
            run(["sudo", "-n", "systemctl", "enable", name])
            run(["sudo", "-n", "systemctl", "restart", name])
            state = service_state(name)
            if state.get("ActiveState") != "active" or state.get("UnitFileState") != "enabled":
                raise RuntimeError(name + " did not become enabled/active")
        sys.path.insert(0, str(release))
        from native_ready import probe_board, probe_native, probe_workspace, wait_ready
        probes = []
        if "board" in components:
            probes.append(("board", lambda: probe_board("http://127.0.0.1:8079/api")))
        if "native" in components:
            probes.append(("native", lambda: probe_native(args.runtime_url)))
        if "workspace" in components:
            probes.append(("workspace", lambda: probe_workspace("http://127.0.0.1:8766/mcp")))
        if probes:
            wait_ready(probes, 60)
        if before:
            after = session_snapshot(args.runtime_url)
            verify_preserved(before, after)
            record["after_sessions"] = after
        # Prove actual images and preserved results BEFORE reopening sessions.
        record["verified_before_resume"] = inventory(base)
        verify_running_images(record["verified_before_resume"], metadata,
                              [c for c in components if c not in ("fleet", "recovery")])
        atomic_json(record_path, record)
        if drain_journal.exists():
            record["resume"] = resume(args.runtime_url, drain_journal)
        # Do not expose temporarily stopped sessions to Fleet terminal reporting.
        for component in ("fleet", "recovery"):
            if component in components:
                name = "borg-" + component + ".service"
                run(["sudo", "-n", "systemctl", "enable", name])
                run(["sudo", "-n", "systemctl", "restart", name])
        if fleet_was_active and "fleet" not in components:
            run(["sudo", "-n", "systemctl", "start", "borg-fleet.service"])
        record["inventory"] = inventory(base)
        verify_running_images(record["inventory"], metadata, components)
        record.update(status="verified", finished_at=stamp(), committed_results_preserved=True)
    except Exception as error:
        record.update(status="recovery_pending", error_type=type(error).__name__, finished_at=stamp(),
                      recovery_owner="integrator", recovery_task=6)
        atomic_json(record_path, record)
        # Resume on failure ONLY if the original known-running image is still
        # running unchanged. A new/unverified image must never receive work.
        current = inventory(base)
        old = record["before_inventory"]["services"]["borg-native.service"]
        now = current["services"]["borg-native.service"]
        unchanged_native = (old.get("executable_sha256") is not None and
                            now.get("executable_sha256") == old.get("executable_sha256") and
                            now.get("ActiveState") == "active")
        unchanged_board = True
        if "board" in components:
            old_board = record["before_inventory"]["services"]["borg-board.service"]
            now_board = current["services"]["borg-board.service"]
            unchanged_board = (old_board.get("executable_sha256") is not None and
                               now_board.get("executable_sha256") == old_board.get("executable_sha256") and
                               now_board.get("ActiveState") == "active")
        if drain_journal.exists() and unchanged_native and unchanged_board:
            try:
                record["resume_after_failure"] = resume(args.runtime_url, drain_journal)
            except Exception as resume_error:
                record["resume_error_type"] = type(resume_error).__name__
        else:
            record["resume_pending_verification"] = True
        # Keep admission closed after failed activation. The task-6 owner and
        # durable record identify the repair path; no silent unverified rollback.
        record["failure_inventory"] = current
        raise
    finally:
        atomic_json(record_path, record)
    return {"evidence_path": str(record_path), **record}


def launch(args):
    """A rollout must outlive the calling workspace tool (including self-upgrade)."""
    metadata = verify(args.release)
    job = "borg-native-rollout-" + metadata["revision"][:12] + "-" + uuid.uuid4().hex[:8]
    command = ["sudo", "-n", "systemd-run", "--unit=" + job, "--uid=" + args.user,
               "--working-directory=" + str(args.base), "--no-block", "--property=Type=oneshot",
               "--property=RemainAfterExit=yes", "--property=TimeoutStartSec=infinity",
               "/usr/bin/python3", "-B", str(args.release / "native_release.py"), "install",
               "--base", str(args.base), "--release", str(args.release),
               "--host-id", args.host_id, "--user", args.user, "--workspace", args.workspace,
               "--runtime-url", args.runtime_url, "--components", args.components,
               "--wait-seconds", str(args.wait_seconds)]
    run(command)
    return {"job": job + ".service", "revision": metadata["revision"],
            "host": args.host_id, "launched": True,
            "evidence_directory": str(args.base / "state" / "deployments")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("manifest", "verify", "inventory", "install", "launch"))
    parser.add_argument("--base", type=Path, default=BASE)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--revision")
    parser.add_argument("--wait-seconds", type=float, default=600)
    parser.add_argument("--host-id", default=os.uname().nodename)
    parser.add_argument("--user", default="mtolton")
    parser.add_argument("--workspace", default="/home/dev-user/borg-native-workspace")
    parser.add_argument("--runtime-url", default="http://127.0.0.1:8787")
    parser.add_argument("--components", default="workspace,session-workspace,native,fleet")
    args = parser.parse_args()
    if args.action != "inventory" and not args.release:
        parser.error("--release required")
    try:
        if args.action == "manifest":
            value = manifest(args.release, args.revision or "")
        elif args.action == "verify":
            value = verify(args.release)
        elif args.action == "inventory":
            value = inventory(args.base)
        elif args.action == "launch":
            value = launch(args)
        else:
            value = install(args)
        print(json.dumps(value, sort_keys=True))
    except Exception as error:
        # Logs never include arbitrary HTTP responses or environment contents.
        print(f"native release {args.action} failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
