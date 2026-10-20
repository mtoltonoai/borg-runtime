#!/usr/bin/env python3
"""Render native Fleet's four-host route/config join and audit it from each lab.

This tool NEVER installs, enables, stops, or restarts a service. The integrator
owns activation. SSH uses the already-provisioned Borg key/config; no credential
contents are read, copied, printed, or written. Existing Board tunnels are not
changed (they may carry a pending tool result).
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys

HOSTS = ("agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4")
PORTS = dict(zip(HOSTS, range(17871, 17875)))
DEFAULT_BASE = "/home/dev-user/.local/share/borg"
DEFAULT_SSH = "/home/dev-user/.ssh/borg_config"


def origin(host):
    return f"http://127.0.0.1:{PORTS[host]}"


def fleet_config(host, base=DEFAULT_BASE, capacity=4, project=None, ssh_config=DEFAULT_SSH):
    # JSON strings are also valid TOML basic strings for these absolute paths.
    project_line = f"project_id = {project}\n" if project is not None else ""
    return f'''# Generated connectivity only; identity/model/tools remain Board metadata.
board_api = "http://127.0.0.1:8079/api"
host = {json.dumps(host)}

[native]
runtime_url = {json.dumps(origin(host) + "/api")}
workspace_root = {json.dumps(str(Path(base) / "workspaces"))}
workspace_mcp_url = "http://127.0.0.1:8791/mcp/{{session_id}}"
capacity = {capacity}
poll_seconds = 5
ttl_seconds = 60
{project_line}repo_url = "ssh://borg-agent-lab/home/dev-user/.local/share/borg/git/borg-runtime.git"
repo_ref = "main"
git_ssh_config = {json.dumps(ssh_config)}
http_timeout_seconds = 15
provision_timeout_seconds = 300
'''


def runtime_patch(host):
    # MERGE these infrastructure fields. Never replace the runtime file: it also
    # owns database_path, credential variable names and bootstrap compatibility.
    return {"host_id": host, "runtime_url": origin(host), "board": {"url": "http://127.0.0.1:8079"}}


def worker_unit(user="mtolton", ssh_config=DEFAULT_SSH):
    # Port 17871 on the worker goes directly to the controller's native :8787;
    # the others go to existing reverse-forward listeners on the controller.
    # No 8079 forward or reverse forward here: leave the live Board unit alone.
    argv = ["/usr/bin/ssh", "-F", ssh_config, "-N", "-T", "-o", "BatchMode=yes",
            "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=3", "-o", "ConnectTimeout=10"]
    argv.extend(["-L", "127.0.0.1:17870:127.0.0.1:17870"]) # existing builder/bootstrap reverse route
    for host, port in PORTS.items():
        remote_port = 8787 if host == "agent-lab" else port
        argv.extend(["-L", f"127.0.0.1:{port}:127.0.0.1:{remote_port}"])
    argv.append("borg-agent-lab")
    # These values become systemd argv, not a shell command. Paths/users are
    # validated by render(), avoiding shell/systemd expansion surprises.
    return f'''[Unit]
Description=Borg globally consistent native runtime routes (existing SSH identity)
Wants=network-online.target borg-board-tunnel.service
After=network-online.target borg-board-tunnel.service
StartLimitIntervalSec=0

[Service]
Type=simple
User={user}
Environment=HOME=/home/dev-user
ExecStart={" ".join(argv)}
Restart=always
RestartSec=3
KillMode=control-group
NoNewPrivileges=yes
UMask=0077

[Install]
WantedBy=multi-user.target
'''


def controller_units(user="mtolton", proxy="/lib/systemd/systemd-socket-proxyd"):
    return {
        "borg-native-controller-route.socket": '''[Unit]
Description=Borg controller native runtime global loopback address

[Socket]
ListenStream=127.0.0.1:17871
NoDelay=true

[Install]
WantedBy=sockets.target
''',
        "borg-native-controller-route.service": f'''[Unit]
Description=Borg controller native runtime loopback proxy
After=network.target borg-native.service

[Service]
User={user}
ExecStart={proxy} 127.0.0.1:8787
Restart=on-failure
RestartSec=3
NoNewPrivileges=yes
PrivateTmp=yes
''',
    }


def safe_unit_value(value, name):
    if not value or any(c.isspace() or c in "\n\r\0%$\"'\\;" for c in value):
        raise ValueError(f"{name} must be a nonempty literal systemd argument without whitespace/expansion")
    return value


def render(out, *, base=DEFAULT_BASE, user="mtolton", ssh_config=DEFAULT_SSH,
           proxy="/lib/systemd/systemd-socket-proxyd", capacity=4, project=None):
    for key, value in (("base", base), ("user", user), ("ssh_config", ssh_config), ("proxy", proxy)):
        safe_unit_value(value, key)
    if not Path(base).is_absolute() or not Path(ssh_config).is_absolute() or not Path(proxy).is_absolute():
        raise ValueError("base, ssh_config and proxy must be absolute")
    if not 0 <= capacity <= 10000 or (project is not None and project <= 0):
        raise ValueError("invalid capacity/project")
    out = Path(out)
    for host in HOSTS:
        folder = out / host
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "native-fleet.toml").write_text(fleet_config(host, base, capacity, project, ssh_config))
        (folder / "runtime-patch.json").write_text(json.dumps(runtime_patch(host), indent=2) + "\n")
        units = controller_units(user, proxy) if host == "agent-lab" else {"borg-native-routes.service": worker_unit(user, ssh_config)}
        for name, content in units.items():
            (folder / name).write_text(content)
    (out / "README.txt").write_text(f'''Native routes/config join: rendered only, no service activation performed.

Controller agent-lab already receives reverse native listeners 17872/3/4 from
worker borg-board-tunnel services. Workers currently have only Board local8079.
Keep those live tunnels unchanged. These additional route units add no 8079 or
reverse forwards and use existing {ssh_config} and its existing key.

1. Integrator checks each target listener with ss -ltnp; do not activate a second
   owner on 17871..17874 if a route service was already installed concurrently.
2. Controller: install the two borg-native-controller-route.* units under
   /etc/systemd/system; daemon-reload; enable --now the .socket (not the service).
   This exposes 127.0.0.1:17871 -> local8787, with no native restart.
3. Each worker: install borg-native-routes.service under /etc/systemd/system;
   daemon-reload; enable --now borg-native-routes.service. It adds four persistent
   -L forwards to the controller (four lab runtimes plus existing builder17870),
   without restarting borg-board-tunnel.
4. Audit from every lab with this tool. Require all16 health routes and, after
   integrated runtime deployment, host_id identity matching. A builder-only
   probe is insufficient. Existing builder forwards need no replacement.
5. Stage each host's native-fleet.toml at {base}/config/native-fleet.toml.
   DEEP-MERGE runtime-patch.json fields into the existing runtime config during the
   integrator's safe drain/deploy; do not replace database/credential settings
   or discard existing board.bearer_token_env when merging board.url.
   Native runtime_url is the origin, Fleet runtime_url ends /api.
   The shared fleet omits project_id; Board metadata admits new identities and
   projects without editing host configs. --project is only for isolated tests.
6. Preserve/adopt bootstrap dispatches before enabling Fleet, then run the live
   placement/restart acceptance. New route activation alone never stops them.

Every listener is loopback-only. SSH is authenticated by already installed
keys; no key or bearer value is included in rendered output.
''')
    return {"output": str(out.resolve()), "hosts": list(HOSTS), "activation_performed": False,
            "runtime_origins": {host: origin(host) for host in HOSTS}}


# Executed through SSH on the caller host, so localhost is THAT caller. The
# payload reports only health fields and sanitized errors, never auth material.
PROBE = r'''
import json, socket, urllib.request
host = socket.gethostname()
rows = []
for target, port in [("agent-lab",17871),("agent-lab-2",17872),("agent-lab-3",17873),("agent-lab-4",17874),("borg-builder",17870)]:
    row = {"target":target,"origin":f"http://127.0.0.1:{port}"}
    try:
        with urllib.request.urlopen(row["origin"]+"/health",timeout=3) as response:
            value = json.load(response)
        row.update(reachable=value.get("status")=="ok",service=value.get("service"),host_id=value.get("host_id"),
                   revision=value.get("revision"),ready=value.get("ready"),draining=value.get("draining"),
                   identity_verified=value.get("host_id")==target)
    except Exception as error:
        row.update(reachable=False,identity_verified=False,error=type(error).__name__)
    rows.append(row)
print(json.dumps({"reported_caller":host,"routes":rows[:4],"builder_route":rows[4]}))
'''


def audit_host(host, ssh_config=DEFAULT_SSH):
    try:
        if socket.gethostname().split(".")[0] == host:
            # Self-SSH may not be authorized even when the existing outbound key
            # reaches every peer; this is still a probe from the actual caller.
            argv = [sys.executable, "-c", PROBE]
        else:
            argv = ["/usr/bin/ssh", "-F", ssh_config, "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=8", "borg-" + host,
                    "python3 -c " + shlex.quote(PROBE)]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=35, check=True)
        value = json.loads(result.stdout)
        value["caller"] = host
        value["caller_verified"] = value.get("reported_caller", "").split(".")[0] == host
        return value
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        return {"caller": host, "routes": [], "error": type(error).__name__}


def summarize(callers, require_identity=False):
    count = sum(len(c.get("routes", [])) for c in callers)
    reachable = sum(bool(r.get("reachable")) for c in callers for r in c.get("routes", []))
    identified = sum(bool(r.get("identity_verified")) for c in callers for r in c.get("routes", []))
    builder_reachable = sum(bool(c.get("builder_route", {}).get("reachable")) for c in callers)
    return {"observed_at": datetime.now(timezone.utc).isoformat(), "callers": callers,
            "expected_routes": 16, "observed_routes": count, "reachable_routes": reachable,
            "identity_verified_routes": identified, "builder_reachable_routes": builder_reachable,
            "ok": len(callers) == 4 and all(c.get("caller_verified") for c in callers) and count == reachable == 16 and builder_reachable == 4 and (not require_identity or identified == 16)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("render")
    plan.add_argument("--out", type=Path, required=True)
    plan.add_argument("--base", default=DEFAULT_BASE)
    plan.add_argument("--user", default="mtolton")
    plan.add_argument("--ssh-config", default=DEFAULT_SSH)
    plan.add_argument("--proxy", default="/lib/systemd/systemd-socket-proxyd")
    plan.add_argument("--capacity", type=int, default=4)
    plan.add_argument("--project", type=int, help="optional isolated fleet scope; omit for the shared fleet")
    probe = sub.add_parser("audit")
    probe.add_argument("--ssh-config", default=DEFAULT_SSH)
    probe.add_argument("--require-identity", action="store_true")
    probe.add_argument("--output", type=Path)
    local = sub.add_parser("audit-local", help="probe the four advertised origins and builder from this process host")
    local.add_argument("--require-identity", action="store_true")
    args = parser.parse_args()
    if args.command == "render":
        result = render(args.out, base=args.base, user=args.user, ssh_config=args.ssh_config,
                        proxy=args.proxy, capacity=args.capacity, project=args.project)
    elif args.command == "audit-local":
        value = json.loads(subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                                         timeout=25, check=True).stdout)
        value["ok"] = all(row["reachable"] and (not args.require_identity or row["identity_verified"])
                          for row in value["routes"])
        result = value
    else:
        with ThreadPoolExecutor(max_workers=4) as pool:
            callers = list(pool.map(lambda host: audit_host(host, args.ssh_config), HOSTS))
        result = summarize(callers, args.require_identity)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    sys.exit(main())
