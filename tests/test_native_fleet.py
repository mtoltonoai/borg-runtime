#!/usr/bin/env python3
"""Real Board + Fleet processes, Git clones, and session workspace MCP boundary.

The native engine HTTP fixture injects lost responses; it is not a model/API test.
Native-core's tests and the live deployment cover the actual engine. Nothing in
this suite uses production ports, credentials, databases, or session services.

Build both binaries first; then run from the repository root:
  python3 tests/test_native_fleet.py
Use --workspace to test a separately published session-workspace helper before
integration. All subprocesses and temp directories are owned by the test.
"""
from __future__ import annotations

import argparse
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
FLEET = ROOT / "components/fleet/target/debug/fleet"
BOARD = ROOT / "components/task-board/target/debug/task-board"
WORKSPACE = ROOT / "tooling/workspace_mcp.py"
SSH_REPO = None
SSH_CONFIG = None


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(method, url, body=None, timeout=20):
    payload = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=payload, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise AssertionError(f"{method} {url}: {error.code} {error.read().decode()}") from error


def wait_for(check, seconds=15):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception as error:
            last = error
        time.sleep(.05)
    raise AssertionError(f"timed out waiting: {last}")


def stop(proc):
    if proc.poll() is None:
        # Each fixture service has its own group. Never signal a production service.
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


class Engine:
    def __init__(self, host):
        self.host = host
        self.ready = True
        self.lock = threading.RLock()
        self.sessions = {}
        self.intents = {}
        self.calls = Counter()
        self.effects = Counter()
        self.lose_before_once = False
        self.lose_after_once = False
        self.slow_tool = False
        self.tool_started = threading.Event()
        self.tool_finished = threading.Event()
        engine = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, body):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                try:
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def lose_response(self):
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            def do_GET(self):
                if self.path == "/health":
                    return self.reply(200, {"status": "ok", "service": "borg-native", "host_id": engine.host, "ready": engine.ready, "draining": not engine.ready})
                if self.path.startswith("/api/sessions/"):
                    key = self.path.rsplit("/", 1)[1]
                    with engine.lock:
                        value = engine.sessions.get(key)
                    return self.reply(200 if value else 404, value or {"error": "not found"})
                self.reply(404, {"error": "not found"})

            def do_POST(self):
                if self.path != "/api/sessions":
                    return self.reply(404, {"error": "not found"})
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                key = body["session_id"]
                with engine.lock:
                    engine.calls[key] += 1
                    if engine.lose_before_once:
                        engine.lose_before_once = False
                        return self.lose_response()
                    if key in engine.sessions:
                        if engine.intents[key] != body:
                            return self.reply(409, {"error": "changed intent"})
                        return self.reply(200, engine.sessions[key])
                    session = dict(id=key, agent_id=body["agent_id"], task=body.get("task"),
                                   status="running", progress="fixture tool", error=None,
                                   workspace_cwd=body["workspace_cwd"], host_id=body["host_id"],
                                   generation=body["generation"], updated_at=int(time.time()),
                                   phase="awaiting-tool-result", step=1, driver_active=True)
                    engine.intents[key] = body
                    engine.sessions[key] = session
                # This is an actual MCP call through the separately running helper,
                # not a direct write by Fleet or by this fixture.
                engine.tool_started.set()
                command = "sleep 2; printf preserved > result.txt" if engine.slow_tool else "pwd > cwd.txt; printf preserved > result.txt"
                result = http("POST", body["workspace_mcp_url"], {
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
                        "name": "workspace_run", "arguments": {"command": command, "timeout_seconds": 10}}})
                if result.get("error") or result.get("result", {}).get("isError"):
                    with engine.lock:
                        session.update(status="failed", error=json.dumps(result))
                else:
                    with engine.lock:
                        engine.effects[key] += 1
                        session.update(progress="fixture tool durably completed", phase="idle", step=2,
                                       last_progress_at=int(time.time()))
                engine.tool_finished.set()
                with engine.lock:
                    if engine.lose_after_once:
                        engine.lose_after_once = False
                        return self.lose_response()
                self.reply(201, session)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/api"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class FleetIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="native-fleet-integration-")
        self.root = Path(self.temp.name)
        self.processes = []
        self.logs = []
        self.engines = []
        self.addCleanup(self.cleanup_cluster)
        self.hosts = ["agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4"]
        self.board_port = free_port()
        self.board_url = f"http://127.0.0.1:{self.board_port}/api"
        self.board_config = self.root / "board.toml"
        self.board_config.write_text(f'db_path={json.dumps(str(self.root / "board.db"))}\nhost="127.0.0.1"\nport={self.board_port}\n')
        self.board_process = self.start_board()
        self.source = self.root / "source"
        subprocess.run(["git", "init", "-b", "main", str(self.source)], check=True, capture_output=True)
        (self.source / "source.txt").write_text("base")
        subprocess.run(["git", "-C", str(self.source), "add", "source.txt"], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(self.source), "-c", "user.name=fleet-test", "-c", "user.email=fleet@example.invalid",
                        "commit", "-m", "base"], check=True, capture_output=True)
        self.project = http("POST", self.board_url + "/projects", {"name": "fleet fixture"})["id"]
        http("POST", self.board_url + "/agents", {"agent_id": "engineer", "charter": "test isolated native work",
             "metadata": {"harness": "native", "host": "ignored-observation", "model": "gpt-6-astra",
                          "reasoning_effort": "max", "max_output_tokens": 128000, "context_window_tokens": 1050000}})
        self.configs = []
        self.roots = []
        for i, host in enumerate(self.hosts):
            root = self.root / host / "workspaces"
            root.mkdir(parents=True)
            self.roots.append(root)
            workspace_port = free_port()
            self.spawn([sys.executable, str(WORKSPACE), "--session-root", str(root), "--port", str(workspace_port)], f"workspace-{i}")
            wait_for(lambda: socket.create_connection(("127.0.0.1", workspace_port), timeout=.1).close() is None)
            engine = Engine(host)
            self.engines.append(engine)
            config = self.root / f"fleet-{i}.toml"
            config.write_text(f'''board_api={json.dumps(self.board_url)}
host={json.dumps(host)}
[native]
runtime_url={json.dumps(engine.url)}
workspace_root={json.dumps(str(root))}
workspace_mcp_url="http://127.0.0.1:{workspace_port}/mcp/{{session_id}}"
capacity=4
poll_seconds=1
ttl_seconds=60
project_id={self.project}
repo_url={json.dumps(str(self.source))}
repo_ref="main"
http_timeout_seconds=5
provision_timeout_seconds=30
''')
            self.configs.append(config)
            # Make all four fresh before creating work, so the controlled batch
            # proves equal-capacity fairness rather than service startup ordering.
            http("POST", self.board_url + f"/fleet/hosts/{host}/heartbeat", {
                "capacity": 4, "runtime_url": engine.url, "workspace_root": str(root), "runtime_ready": True,
                "revision": "fixture-registration"})

    def spawn(self, argv, label):
        log = open(self.root / f"{label}.log", "ab")
        self.logs.append(log)
        proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.processes.append(proc)
        return proc

    def start_board(self):
        proc = self.spawn([str(BOARD), "--config", str(self.board_config)], "board")
        wait_for(lambda: http("GET", self.board_url + "/health"))
        return proc

    def cleanup_cluster(self):
        for proc in reversed(self.processes):
            stop(proc)
        for engine in self.engines:
            engine.close()
        for log in self.logs:
            log.close()
        self.temp.cleanup()

    def task(self, title="fixture task", metadata=None):
        return http("POST", self.board_url + "/tasks", {"project_id": self.project, "title": title,
                    "assignee": "engineer", "metadata": metadata or {}})

    def workers(self):
        procs = [self.spawn([str(FLEET), "--config", str(config), "native-worker", "--once"], f"worker-{i}")
                 for i, config in enumerate(self.configs)]
        for i, proc in enumerate(procs):
            self.assertEqual(proc.wait(timeout=45), 0, (self.root / f"worker-{i}.log").read_text())

    def dispatches(self):
        return http("GET", self.board_url + "/fleet/dispatches?limit=1000")

    def test_four_workers_balance_dedup_lost_responses_isolation_and_restart(self):
        self.engines[1].lose_after_once = True
        self.engines[2].lose_before_once = True
        for i in range(8):
            self.task(f"fixture task {i}")
        self.workers()
        time.sleep(2.2)  # first uncertain-create retry deadline; not an inference timeout
        self.workers()
        rows = self.dispatches()
        self.assertEqual(len(rows), 8)
        self.assertEqual(Counter(row["host_id"] for row in rows), Counter(dict.fromkeys(self.hosts, 2)))
        self.assertEqual(len({row["session_id"] for row in rows}), 8)
        self.assertEqual(len({row["workspace_cwd"] for row in rows}), 8)
        self.assertTrue(all(row["status"] == "running" for row in rows), rows)
        for row in rows:
            cwd = Path(row["workspace_cwd"])
            self.assertEqual((cwd / "result.txt").read_text(), "preserved")
            self.assertEqual((cwd / "cwd.txt").read_text().strip(), str(cwd))
            self.assertEqual((cwd / "source.txt").read_text(), "base")
            self.assertTrue((cwd / ".git").is_dir())
        a, b = map(lambda r: Path(r["workspace_cwd"]), rows[:2])
        (a / "only-a").write_text("isolated")
        self.assertFalse((b / "only-a").exists())
        self.assertEqual(sum(sum(e.effects.values()) for e in self.engines), 8)
        self.assertEqual(sum(sum(e.calls.values()) for e in self.engines), 9)  # only pre-commit loss retries create
        for engine in self.engines:
            with engine.lock:
                for session in engine.sessions.values():
                    session["status"] = "completed"
                    session["driver_active"] = False
        self.workers()
        self.assertTrue(all(row["status"] == "completed" for row in self.dispatches()))
        saved = [(r["id"], r["session_id"], r["workspace_cwd"]) for r in rows]
        stop(self.board_process)
        self.board_process = self.start_board()
        self.workers()
        self.assertEqual([(r["id"], r["session_id"], r["workspace_cwd"]) for r in self.dispatches()], saved)
        self.assertEqual(sum(sum(e.calls.values()) for e in self.engines), 9)
        self.assertEqual(sum(sum(e.effects.values()) for e in self.engines), 8)
        self.assertEqual((a / "only-a").read_text(), "isolated")

    def test_worker_restart_does_not_kill_an_active_workspace_tool(self):
        self.engines[0].slow_tool = True
        self.task(metadata={"placement": {"host": self.hosts[0]}})
        proc = self.spawn([str(FLEET), "--config", str(self.configs[0]), "native-worker"], "worker-live")
        self.assertTrue(self.engines[0].tool_started.wait(timeout=15))
        stop(proc)  # native HTTP engine + workspace tool are NOT Fleet children
        self.assertTrue(self.engines[0].tool_finished.wait(timeout=15))
        row = self.dispatches()[0]
        self.assertEqual((Path(row["workspace_cwd"]) / "result.txt").read_text(), "preserved")
        self.workers()
        self.assertEqual(sum(self.engines[0].calls.values()), 1)
        self.assertEqual(sum(self.engines[0].effects.values()), 1)
        self.assertEqual(self.dispatches()[0]["status"], "running")

    def test_opt_in_real_ssh_clone_and_persisted_session_git_route(self):
        if not SSH_REPO:
            self.skipTest("pass --ssh-repo and --ssh-config for the real deployment Git boundary")
        for config in self.configs:
            text = config.read_text().replace("repo_url=" + json.dumps(str(self.source)),
                                               "repo_url=" + json.dumps(SSH_REPO))
            config.write_text(text + "git_ssh_config=" + json.dumps(str(SSH_CONFIG)) + "\n")
        self.task("real ssh clone route proof", {"placement": {"host": self.hosts[0]}})
        self.workers()
        row = self.dispatches()[0]
        if row["status"] != "running":
            for log in self.roots[0].glob(".*.provision.log"):
                print(log.read_text()[-3000:])
        self.assertEqual(row["status"], "running", row)
        cwd = Path(row["workspace_cwd"])
        self.assertTrue((cwd / ".git").is_dir())
        result = subprocess.run(["git", "-C", str(cwd), "ls-remote", "origin", "refs/heads/main"],
                                capture_output=True, text=True, check=True, timeout=20)
        self.assertTrue(result.stdout.strip())
        route = subprocess.run(["git", "-C", str(cwd), "config", "--local", "core.sshCommand"],
                               capture_output=True, text=True, check=True).stdout
        self.assertIn(str(SSH_CONFIG), route)
        self.assertEqual((cwd / "result.txt").read_text(), "preserved")
        self.assertEqual(sum(sum(e.effects.values()) for e in self.engines), 1)

    def test_runtime_drain_blocks_new_reservations_until_ready(self):
        for engine in self.engines:
            engine.ready = False
        self.workers()  # publish runtime_ready=false before introducing work
        self.task("waiting for runtime readiness")
        self.workers()
        self.assertFalse(self.dispatches())
        self.assertTrue(all(not h["runtime_ready"] for h in http("GET", self.board_url + "/fleet/hosts")))
        for engine in self.engines:
            engine.ready = True
        self.workers()
        # A worker may reserve on a host that already completed its once pass;
        # another bounded reconciliation observes/launches the durable binding.
        self.workers()
        self.assertEqual(len(self.dispatches()), 1)
        self.assertEqual(self.dispatches()[0]["status"], "running")

    def test_shared_fleet_admits_a_new_project_and_identity_without_host_edits(self):
        # A shared daemon omits the optional project filter from its initial
        # deployment. All later project/identity changes are Board writes only.
        for cfg in self.configs:
            cfg.write_text("\n".join(line for line in cfg.read_text().splitlines()
                                     if not line.startswith("project_id=")) + "\n")
        configs_before = [cfg.read_bytes() for cfg in self.configs]
        first = self.task("first project task")
        self.workers()
        project = http("POST", self.board_url + "/projects", {"name": "new native project"})["id"]
        http("POST", self.board_url + "/agents", {"agent_id": "new-engineer", "charter": "new metadata identity",
             "metadata": {"harness": "native"}})
        second = http("POST", self.board_url + "/tasks", {"project_id": project, "title": "new project task",
                      "assignee": "new-engineer"})
        self.workers()
        rows = self.dispatches()
        self.assertEqual({r["task_id"] for r in rows}, {first["id"], second["id"]})
        self.assertEqual({r["agent_id"] for r in rows}, {"engineer", "new-engineer"})
        self.assertTrue(all(r["status"] == "running" for r in rows), rows)
        self.assertEqual([cfg.read_bytes() for cfg in self.configs], configs_before)

    def test_more_than_100_actionable_tasks_do_not_loop_the_unpaginated_tasks_api(self):
        # Regression: REST /tasks ignores limit/offset and returns the whole
        # status group. A generic pagination loop would never return here.
        for i in range(105):
            self.task(f"large backlog task {i}")
        started = time.monotonic()
        self.workers()
        self.assertLess(time.monotonic() - started, 40)
        rows = self.dispatches()
        self.assertEqual(len(rows), 16)  # capacity, not missing tasks or repeated launches
        self.assertEqual(Counter(r["host_id"] for r in rows), Counter(dict.fromkeys(self.hosts, 4)))
        first = {h["host_id"]: h["last_seen"] for h in http("GET", self.board_url + "/fleet/hosts")}
        time.sleep(1.1)
        self.workers()
        second = http("GET", self.board_url + "/fleet/hosts")
        self.assertTrue(all(h["last_seen"] > first[h["host_id"]] and h["fresh"] for h in second))
        self.assertEqual(len(self.dispatches()), 16)
        self.assertEqual(sum(sum(e.calls.values()) for e in self.engines), 16)

    def test_adopt_bootstrap_without_provisioning_and_keep_host_drained(self):
        task = self.task("bootstrap task")
        session_id = str(uuid.uuid4())
        bootstrap = self.root / "original-bootstrap"
        bootstrap.mkdir()
        (bootstrap / "uncommitted").write_text("do not touch")
        self.engines[3].sessions[session_id] = {"id": session_id, "agent_id": "engineer", "task": f'task_{task["id"]}',
            "status": "running", "progress": "bootstrap inference", "error": None}
        http("POST", self.board_url + "/fleet/dispatches/adopt", {"request_id": f'task:{task["id"]}:attempt:1',
            "agent_id": "engineer", "task_id": task["id"], "session_id": session_id, "host_id": self.hosts[3],
            "runtime_url": self.engines[3].url, "workspace_cwd": str(bootstrap), "generation": 0})
        http("PATCH", self.board_url + f"/fleet/hosts/{self.hosts[3]}", {"enabled": False, "capacity": 1})
        self.task("waiting on drained capacity", {"placement": {"host": self.hosts[3]}})
        self.workers()
        self.assertEqual(len(self.dispatches()), 1)
        self.assertEqual(self.dispatches()[0]["session_id"], session_id)
        self.assertEqual((bootstrap / "uncommitted").read_text(), "do not touch")
        self.assertFalse((bootstrap / ".fleet-workspace.json").exists())
        self.assertFalse(self.engines[3].calls)
        host = next(h for h in http("GET", self.board_url + "/fleet/hosts") if h["host_id"] == self.hosts[3])
        self.assertFalse(host["enabled"])
        self.assertEqual(host["capacity"], 1)

    def test_explicit_attempt_two_uses_authoritative_task_metadata(self):
        task = self.task(metadata={"workspace": {"repo": str(self.root / "missing-repo")}})
        self.workers()
        failed = self.dispatches()[0]
        self.assertEqual(failed["status"], "failed")
        # The deployed older REST /tasks list omits metadata. Only GET /tasks/id
        # contains the explicit repair attempt and the corrected workspace spec.
        http("PATCH", self.board_url + f'/tasks/{task["id"]}', {"metadata": {
            "native_attempt": 2, "workspace": {"repo": str(self.source)}}})
        self.workers()
        self.workers()
        rows = self.dispatches()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], failed, "original failed attempt must remain unchanged")
        self.assertEqual(rows[1]["request_id"], f'task:{task["id"]}:attempt:2')
        self.assertEqual(rows[1]["metadata"]["native_attempt"], 2)
        self.assertEqual(rows[1]["status"], "running")
        self.assertNotEqual(rows[0]["session_id"], rows[1]["session_id"])
        self.assertEqual(sum(sum(e.calls.values()) for e in self.engines), 1)

    def test_provision_failure_remains_owned_visible_and_not_retried_as_new_session(self):
        task = self.task(metadata={"workspace": {"repo": str(self.root / "missing-repo")}})
        self.workers()
        self.workers()
        row = self.dispatches()[0]
        self.assertEqual(row["status"], "failed")
        self.assertIn("workspace git exited", row["error"])
        self.assertEqual(row["metadata"]["failure_stage"], "provisioning")
        current = http("GET", self.board_url + f'/tasks/{task["id"]}')
        self.assertEqual(current["assignee"], "engineer")
        self.assertEqual(current["metadata"]["native_dispatch"]["status"], "failed")
        self.assertEqual(len(self.dispatches()), 1)
        self.assertEqual(sum(sum(e.calls.values()) for e in self.engines), 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fleet", type=Path, default=FLEET)
    parser.add_argument("--board", type=Path, default=BOARD)
    parser.add_argument("--workspace", type=Path, default=WORKSPACE)
    parser.add_argument("--ssh-repo", help="optional read-only clone proof against a real deployment repository")
    parser.add_argument("--ssh-config", type=Path, help="existing SSH config; no keys are copied")
    args, remaining = parser.parse_known_args()
    SSH_REPO, SSH_CONFIG = args.ssh_repo, args.ssh_config
    if SSH_REPO and not SSH_CONFIG:
        parser.error("--ssh-repo requires explicit --ssh-config")
    FLEET, BOARD, WORKSPACE = map(lambda p: p.resolve(), (args.fleet, args.board, args.workspace))
    for path in (FLEET, BOARD, WORKSPACE):
        if not path.is_file():
            parser.error(f"required test artifact missing: {path}")
    if "--session-root" not in WORKSPACE.read_text():
        parser.error("workspace helper must support --session-root (tools-engineer integration dependency)")
    unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)
