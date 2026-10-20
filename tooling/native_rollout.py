#!/usr/bin/env python3
"""Detached whole-cluster rollout: drain every Board caller before shared updates.

The coordinator, not only each installer, survives the calling workspace tool.
The builder is a native-only target; its workspace/publication environment is
preserved. No caller polls its own drain. Persistent receipts own partial work.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
import uuid

from deploy_native import ssh, stage_one
from native_release import BASE, atomic_json, verify

LABS = ("borg-agent-lab", "borg-agent-lab-2", "borg-agent-lab-3", "borg-agent-lab-4")


def command(host, args, ssh_config, timeout=1800):
    if host == "local":
        result = subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout)
        return result.stdout
    return ssh(host, shlex.join([str(a) for a in args]), ssh_config, timeout=timeout).decode()


def journal_path(base, transaction):
    return base / "state" / "deployments" / ("cluster-" + transaction + ".drain.json")


def target_components(host, components):
    if host == "local":
        return ["native"] if "native" in components else []
    return [c for c in components if c not in ("board", "fleet", "recovery")]


def board_snapshot(ssh_config, base):
    code = r"""
import hashlib,json,pathlib,subprocess,urllib.request
b=pathlib.Path(__import__('sys').argv[1]); out=subprocess.check_output(['systemctl','show','borg-board','--property=MainPID,ActiveState,ExecStart'],text=True)
s=dict(x.split('=',1) for x in out.splitlines() if '=' in x); pid=int(s.get('MainPID') or 0)
s['image']=hashlib.sha256(pathlib.Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest() if pid else None
p=b/'state/board.sqlite'; st=p.stat(); s['database']=[st.st_dev,st.st_ino]
s['expected_config']=('--config '+str(b/'config/board.toml')) in s.get('ExecStart','')
try:
 with urllib.request.urlopen('http://127.0.0.1:8079/api/projects',timeout=5) as r:s['ready']=isinstance(json.load(r),list)
except Exception:s['ready']=False
print(json.dumps(s))
"""
    return json.loads(command("borg-agent-lab", ["python3", "-c", code, str(base)], ssh_config, timeout=20))


def trusted_board(observed, original, selected_image):
    return (observed.get("ready") is True and observed.get("ActiveState") == "active"
            and observed.get("expected_config") is True
            and observed.get("image") in {original.get("image"), selected_image}
            and observed.get("image") is not None
            and observed.get("database") == original.get("database"))


def save_board_unit(ssh_config, base, transaction):
    backup = base / "config" / ("board-before-" + transaction + ".service")
    command("borg-agent-lab", ["cp", "/etc/systemd/system/borg-board.service", str(backup)], ssh_config)
    return backup


def recover_board(args, record, record_path, manifest, backup):
    """Do not exit after parking our recovery owner: reconcile or actively repair."""
    attempt = 0
    while True:
        try:
            observed = board_snapshot(args.ssh_config, args.base)
        except Exception:
            observed = {"ready": False}
        if trusted_board(observed, record["board_before"], manifest["files"]["task-board"]):
            record["board_failure_reconciled"] = observed
            atomic_json(record_path, record)
            return True
        attempt += 1
        record.update(status="repairing_shared_board", recovery_owner="integrator", recovery_task=6,
                      active_recovery_continuation=True, recovery_attempt=attempt)
        atomic_json(record_path, record)
        # Restore only the saved known service command; NEVER replace the DB.
        # A real detached continuation remains alive until identity+readiness
        # proves callers can resume. Retries back off instead of restart storms.
        try:
            command("borg-agent-lab", ["sudo", "-n", "install", "-m", "644", str(backup),
                "/etc/systemd/system/borg-board.service"], args.ssh_config)
            command("borg-agent-lab", ["sudo", "-n", "systemctl", "daemon-reload"], args.ssh_config)
            command("borg-agent-lab", ["sudo", "-n", "systemctl", "restart", "borg-board.service"], args.ssh_config)
        except Exception as error:
            record["recovery_error"] = type(error).__name__
            atomic_json(record_path, record)
        time.sleep(min(60, 2 ** min(attempt, 6)))


def origin_for(host):
    if host == "local":
        port = 17870
    else:
        port = 17871 + list(LABS).index(host)
    return "http://127.0.0.1:" + str(port)


def profile_command(args, host, target, action, host_id):
    return ["python3", "-B", str(target / "native_profile_switch.py"), action,
            "--base", str(args.base), "--release", str(target), "--transaction", args.transaction,
            "--host-id", host_id, "--runtime-origin", origin_for(host)]


def execute(args):
    manifest = verify(args.release)
    root = args.base / "state" / "cluster-rollouts"
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / "coordinator.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    record_path = root / (args.transaction + ".json")
    if record_path.exists():
        raise RuntimeError("transaction already has a receipt; reconcile it instead of replaying")
    record = {"transaction": args.transaction, "owner": "integrator", "task_id": 6,
              "revision": manifest["revision"], "status": "staging", "hosts": {}}
    atomic_json(record_path, record)
    hosts = list(LABS) + ["local"]
    target = args.base / "releases" / manifest["revision"]
    attempted, verified_hosts, drained, prepared = set(), set(), set(), set()
    board_verified = "board" not in args.components
    board_attempted = False
    board_backup = None
    paused_supervisors = []
    try:
        if "board" in args.components:
            record["board_before"] = board_snapshot(args.ssh_config, args.base)
            if not record["board_before"].get("ready") or not record["board_before"].get("image"):
                raise RuntimeError("shared Board not ready before drain; callers remain live")
            board_backup = save_board_unit(args.ssh_config, args.base, args.transaction)
            record["board_unit_backup"] = str(board_backup)
            atomic_json(record_path, record)
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            futures = {pool.submit(stage_one, h, args.release, args.ssh_config, args.base): h for h in LABS}
            for future in concurrent.futures.as_completed(futures):
                h = futures[future]
                record["hosts"][h] = future.result()
                atomic_json(record_path, record)
        if not target.exists():
            shutil.copytree(args.release, target)
        if verify(target)["files"] != manifest["files"]:
            raise RuntimeError("builder staging differs from release")
        record["hosts"]["local"] = {"host_id": os.uname().nodename, "release": str(target)}
        # Stop placement/reporting loops before their sessions are temporarily
        # parked; otherwise a maintenance stop could be misreported as terminal.
        for host in LABS:
            services = ["borg-fleet.service"] + (["borg-recovery.service"] if host == "borg-agent-lab" else [])
            for service in services:
                state = command(host, ["systemctl", "show", service, "--property=ActiveState", "--value"], args.ssh_config).strip()
                if state == "active":
                    command(host, ["sudo", "-n", "systemctl", "stop", service], args.ssh_config)
                    paused_supervisors.append([host, service])
        record["paused_supervisors"] = paused_supervisors
        record["status"] = "draining_all_board_callers"
        atomic_json(record_path, record)
        journal = journal_path(args.base, args.transaction)
        def drain_host(host):
            result = json.loads(command(host, ["python3", "-B", str(target / "native_drain.py"), "drain",
                "--database", str(args.base / "state/native.sqlite"), "--journal", str(journal),
                "--wait-seconds", "900"], args.ssh_config))
            if result.get("state") != "drained":
                raise RuntimeError(host + " did not drain")
            return result
        failures = []
        with concurrent.futures.ThreadPoolExecutor(5) as pool:
            futures = {pool.submit(drain_host, h): h for h in hosts}
            for future in concurrent.futures.as_completed(futures):
                host = futures[future]
                try:
                    result = future.result()
                    drained.add(host)
                    record["hosts"][host]["drain"] = {"journal": str(journal), "sessions": result["sessions"]}
                except Exception as error:
                    failures.append(host)
                    record["hosts"][host]["drain_error"] = type(error).__name__
                atomic_json(record_path, record)
        if failures:
            raise RuntimeError("drain requires owned reconciliation: " + ",".join(failures))
        def install(host, components):
            host_id = record["hosts"][host]["host_id"]
            return json.loads(command(host, ["python3", "-B", str(target / "native_release.py"), "install",
                "--release", str(target), "--base", str(args.base), "--host-id", host_id,
                "--components", ",".join(components), "--workspace",
                "/home/dev-user/borg-native-integration" if host == "local" else "/home/dev-user/borg-native-workspace"], args.ssh_config))
        if "board" in args.components:
            record["status"] = "upgrading_shared_board"
            atomic_json(record_path, record)
            board_attempted = True
            result = install("borg-agent-lab", ["board"])
            board_verified = result.get("status") == "verified"
            if not board_verified:
                raise RuntimeError("shared Board not verified; keep all callers paused")
            record["board_evidence"] = result["evidence_path"]
        record["status"] = "upgrading_runtimes"
        atomic_json(record_path, record)
        for host in hosts:
            components = target_components(host, args.components)
            if not components:
                continue
            if args.board_profiles and "native" in components:
                host_id = record["hosts"][host]["host_id"]
                result = json.loads(command(host, profile_command(args, host, target, "prepare", host_id), args.ssh_config))
                prepared.add(host)
                record["hosts"][host]["profile_prepare"] = result
                atomic_json(record_path, record)
            attempted.add(host)
            result = install(host, components)
            if result.get("status") != "verified":
                raise RuntimeError(host + " activation is not verified")
            if host in prepared:
                binding = json.loads(command(host, profile_command(args, host, target, "bind", record["hosts"][host]["host_id"]), args.ssh_config))
                if binding.get("state") != "bound":
                    raise RuntimeError(host + " bootstrap binding not verified")
                record["hosts"][host]["bindings"] = binding
            verified_hosts.add(host)
            record["hosts"][host]["activation"] = result["evidence_path"]
            atomic_json(record_path, record)
        # Fleet/recovery are deliberately not admitted here: adopt existing
        # sessions and verify metadata/bindings first, then enable supervisors.
        record["status"] = "resuming_verified_work"
        atomic_json(record_path, record)
        for host in hosts:
            result = json.loads(command(host, ["python3", "-B", str(target / "native_drain.py"), "resume",
                                               "--journal", str(journal)], args.ssh_config))
            if result.get("state") != "resumed":
                raise RuntimeError(host + " resume not verified")
            record["hosts"][host]["resumed"] = result["resumed"]
            atomic_json(record_path, record)
        for host, service in paused_supervisors:
            command(host, ["sudo", "-n", "systemctl", "start", service], args.ssh_config)
        record["status"] = "verified"
        atomic_json(record_path, record)
    except Exception as error:
        record.update(status="recovery_pending", error_type=type(error).__name__,
                      recovery_owner="integrator", recovery_task=6)
        atomic_json(record_path, record)
        # Only unchanged or verified runtimes may resume, and never against an
        # unverified new shared Board. Preserve journal ownership on any refusal.
        if board_attempted and not board_verified:
            board_verified = recover_board(args, record, record_path, manifest, board_backup)
        if board_verified or not board_attempted:
            # A failed builder migration must not strand the very agent assigned
            # to repair it. Restore its saved, hashed runtime/config using this
            # independent continuation before releasing its original session.
            if "local" in prepared and "local" not in verified_hosts:
                attempt = 0
                while True:
                    try:
                        result = json.loads(command("local", profile_command(args, "local", target,
                            "restore-runtime", record["hosts"]["local"]["host_id"]), args.ssh_config))
                        record["hosts"]["local"]["recovery_restore"] = result
                        verified_hosts.add("local")
                        break
                    except Exception as restore_error:
                        attempt += 1
                        record.update(status="repairing_recovery_owner", active_recovery_continuation=True,
                                      recovery_attempt=attempt, recovery_error=type(restore_error).__name__)
                        atomic_json(record_path, record)
                        time.sleep(min(60, 2 ** min(attempt, 6)))
            for host in hosts:
                if host in attempted and host not in verified_hosts:
                    continue
                try:
                    result = json.loads(command(host, ["python3", "-B", str(target / "native_drain.py"), "resume",
                        "--journal", str(journal_path(args.base, args.transaction))], args.ssh_config))
                    record["hosts"][host]["failure_resume"] = result.get("state")
                except Exception as resume_error:
                    record["hosts"].setdefault(host, {})["resume_error"] = type(resume_error).__name__
                atomic_json(record_path, record)
        raise
    finally:
        lock.close()
    return record_path


def launch(args):
    manifest = verify(args.release)
    transaction = uuid.uuid4().hex
    job = "borg-cluster-rollout-" + transaction[:12]
    command_line = ["sudo", "-n", "systemd-run", "--no-block", "--unit=" + job,
        "--uid=mtolton", "--property=Type=oneshot", "--property=RemainAfterExit=yes",
        "--property=TimeoutStartSec=infinity", "--working-directory=" + str(Path(__file__).resolve().parents[1]),
        "/usr/bin/python3", "-B", str(Path(__file__).resolve()), "run", "--release", str(args.release.resolve()),
        "--base", str(args.base), "--ssh-config", str(args.ssh_config), "--transaction", transaction,
        "--components", ",".join(args.components)]
    if args.board_profiles:
        command_line.append("--board-profiles")
    subprocess.run(command_line, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return {"job": job + ".service", "transaction": transaction, "revision": manifest["revision"],
            "receipt": str(args.base / "state/cluster-rollouts" / (transaction + ".json")),
            "detached": True, "poll_after_initiating_tool_settles": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("launch", "run"))
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--base", type=Path, default=BASE)
    parser.add_argument("--ssh-config", type=Path, default=Path("/home/dev-user/.ssh/borg_config"))
    parser.add_argument("--components", default="board,native")
    parser.add_argument("--transaction")
    parser.add_argument("--board-profiles", action="store_true", help="switch drained bootstrap sessions to Board profiles and bind existing workspaces")
    args = parser.parse_args()
    args.components = args.components.split(",")
    if any(c not in ("board", "native", "workspace", "session-workspace") for c in args.components):
        parser.error("supervisor admission is separate; allowed components board,native,workspace,session-workspace")
    if args.action == "launch":
        print(json.dumps(launch(args)))
    else:
        if not args.transaction:
            parser.error("run requires --transaction")
        print(json.dumps({"receipt": str(execute(args)), "status": "verified"}))


if __name__ == "__main__":
    main()
