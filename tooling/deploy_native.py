#!/usr/bin/env python3
"""Stage immutable native releases over existing SSH; activation is explicit.

Examples:
  python3 tooling/deploy_native.py .runtime/releases/COMMIT --hosts borg-agent-lab-2
  python3 tooling/deploy_native.py .runtime/releases/COMMIT --hosts borg-agent-lab-2 --activate

Stage does not alter services/config/database. Activate automatically drains at
a safe execution boundary; it never cancels or replays an unknown tool write.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid

from native_release import BASE, verify


def ssh(host, command, ssh_config, data=None, timeout=300):
    result = subprocess.run(["ssh", "-F", str(ssh_config), "-o", "BatchMode=yes",
                             "-o", "ConnectTimeout=15", host, command],
                            input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{host}: remote command failed ({result.returncode}): " +
                           result.stderr.decode(errors="replace")[-4000:])
    return result.stdout


def stage_one(host, release, config, base):
    metadata = verify(release)
    destination = base / "releases" / metadata["revision"]
    incoming = base / "releases" / (".incoming-" + uuid.uuid4().hex)
    q = shlex.quote
    ssh(host, "umask 077; mkdir -p " + q(str(incoming)), config)
    try:
        tar = subprocess.Popen(["tar", "-czf", "-", "-C", str(release), "."], stdout=subprocess.PIPE)
        try:
            upload = subprocess.run(["ssh", "-F", str(config), "-o", "BatchMode=yes", host,
                                     "tar -xzf - -C " + q(str(incoming))],
                                    stdin=tar.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=300)
            tar.stdout.close()
            code = tar.wait(timeout=30)
            if upload.returncode or code:
                raise RuntimeError("release transfer failed")
        finally:
            if tar.poll() is None:
                tar.kill()
                tar.wait()
        ssh(host, f"python3 -B {q(str(incoming / 'native_release.py'))} verify --release {q(str(incoming))} >/dev/null", config)
        # The same revision can be staged repeatedly, but never replaced in place.
        code = """import json,os,pathlib,shutil,sys
incoming,destination=map(pathlib.Path,sys.argv[1:])
if destination.exists():
 a=json.loads((incoming/'revision.json').read_text());b=json.loads((destination/'revision.json').read_text())
 if a['revision'] != b['revision'] or a['files'] != b['files']:raise RuntimeError('same revision has different artifacts')
 shutil.rmtree(incoming)
else:os.rename(incoming,destination)
"""
        ssh(host, "python3 -B -c " + q(code) + " " + q(str(incoming)) + " " + q(str(destination)), config)
    except Exception:
        # Only this randomly named staging directory is ours to remove.
        ssh(host, "rm -rf -- " + q(str(incoming)), config)
        raise
    return {"host": host, "host_id": ssh(host, "hostname", config).decode().strip(),
            "release": str(destination), "revision": metadata["revision"], "staged": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", type=Path)
    parser.add_argument("--hosts", nargs="+", required=True)
    parser.add_argument("--ssh-config", type=Path, default=Path("/home/dev-user/.ssh/borg_config"))
    parser.add_argument("--base", type=Path, default=BASE)
    parser.add_argument("--activate", action="store_true")
    parser.add_argument("--components", default="workspace,session-workspace,native,fleet")
    args = parser.parse_args()
    release = args.release.resolve()
    verify(release)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(stage_one, host, release, args.ssh_config, args.base) for host in args.hosts]
        staged = [future.result() for future in futures]
    for result in staged:
        print(json.dumps(result), flush=True)
    # Rolling, not simultaneous. Stop on first failed verification.
    if args.activate:
        for result in staged:
            path = Path(result["release"])
            host_id = result["host_id"]
            command = shlex.join(["python3", "-B", str(path / "native_release.py"), "launch",
                                  "--release", str(path), "--base", str(args.base),
                                  "--host-id", host_id, "--components", args.components])
            output = ssh(result["host"], command, args.ssh_config)
            handle = json.loads(output)
            print(json.dumps(handle), flush=True)
            if host_id == os.uname().nodename:
                # The initiating workspace_run is itself an in-flight tool.
                # Return its receipt before a detached self-upgrade drains it.
                print(json.dumps({"host": result["host"], "job": handle["job"],
                                  "poll_deferred_for_self_upgrade": True}), flush=True)
                continue
            deadline = time.monotonic() + 900
            while True:
                state = ssh(result["host"], "systemctl show " + shlex.quote(handle["job"]) +
                            " --property=ActiveState,SubState,Result,ExecMainStatus", args.ssh_config).decode()
                status = dict(line.split("=", 1) for line in state.splitlines() if "=" in line)
                if status.get("ActiveState") == "active" and status.get("SubState") == "exited":
                    print(json.dumps({"host": result["host"], "job": handle["job"], "verified": True}), flush=True)
                    break
                if status.get("ActiveState") == "failed" or time.monotonic() > deadline:
                    raise RuntimeError("inspect durable rollout evidence for " + handle["job"])
                time.sleep(1)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"deployment failed: {error}", file=sys.stderr)
        sys.exit(1)
