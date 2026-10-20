"""Real Board/Fleet REST contract tests for the independent monitor.

BORG_TEST_BOARD_BINARY=/path/to/integrated/task-board python3 -m unittest \
    discover -s tests -p test_native_recovery_board.py -v
Each test starts a fresh private Board/SQLite database; no production writes.
"""
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import test_native_recovery as fixture
r = fixture.r


@unittest.skipUnless(os.environ.get("BORG_TEST_BOARD_BINARY"), "requires integrated BORG_TEST_BOARD_BINARY")
class RealBoardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        directory = Path(self.tmp.name)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = directory / "board.toml"
        config.write_text(f'db_path = "{directory / "board.sqlite"}"\nhost = "127.0.0.1"\nport = {port}\n')
        env = {k:v for k,v in os.environ.items() if k not in {"LISTEN_FDS", "LISTEN_PID", "LISTEN_FDNAMES"}}
        self.log = (directory / "board.log").open("w+")
        self.addCleanup(self.log.close)
        self.process = subprocess.Popen([os.environ["BORG_TEST_BOARD_BINARY"], "--config", str(config)],
            stdout=self.log, stderr=self.log, env=env)
        self.addCleanup(self.stop_board)
        self.board = r.Board(f"http://127.0.0.1:{port}", r.JsonHttp(timeout=2))
        deadline = time.monotonic()+20
        while True:
            self.assertIsNone(self.process.poll(), "isolated Board exited during startup")
            try:
                self.board.call("GET", "/health")
                break
            except r.HttpFailure:
                if time.monotonic() > deadline:
                    self.fail("isolated Board readiness timeout")
                time.sleep(.05)
        for name in ["engineer", "recovery-engineer", "pm"]:
            self.board.call("POST", "/agents", {"agent_id":name, "metadata":{"harness":"native"}})
        self.project = self.board.call("POST", "/projects", {"name":"Isolated recovery contract", "metadata":{"recovery":{
            "missing_session_grace_seconds":0,"runtime_failure_grace_seconds":0,"runtime_failure_observations":1}}})
        self.task = self.board.call("POST", "/tasks", {"project_id":self.project["id"], "assignee":"engineer",
            "title":"Simulated failed inference needs source repair"})
        now = time.time()
        self.sessions = [dict(fixture.session(status="failed", driver_running=False), task=f"task_{self.task['id']}",
            updated_at=int(now*1000), last_progress_at=int(now*1000))]
        self.native_requests = []
        owner = self
        class NativeHandler(BaseHTTPRequestHandler):
            def log_message(self, *_args): pass
            def do_GET(self):
                owner.native_requests.append(("GET",self.path))
                result = [] if self.path.endswith("/tools") else owner.sessions
                payload = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            def do_POST(self):
                owner.native_requests.append(("POST",self.path))
                self.send_response(500)
                self.end_headers()
        self.native = ThreadingHTTPServer(("127.0.0.1",0), NativeHandler)
        self.native_thread = threading.Thread(target=self.native.serve_forever, daemon=True)
        self.native_thread.start()
        self.addCleanup(self.stop_native)
        self.runtime_url = f"http://127.0.0.1:{self.native.server_port}/api"
        self.board.call("POST", "/fleet/hosts/test-host/heartbeat", {
            "runtime_url":self.runtime_url, "capacity":8,"workspace_root":str(directory / "sessions"),
            "runtime_ready":True,"ttl_seconds":120,"revision":"test"})
        self.journal = r.Journal(directory / "recovery.sqlite")
        self.addCleanup(self.journal.close)
        self.controller = r.Controller(self.board, self.journal, revision="test-monitor")
        self.collector = r.Collector(self.board, r.JsonHttp(timeout=2))

    def stop_board(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(5)

    def stop_native(self):
        self.native.shutdown()
        self.native.server_close()
        self.native_thread.join(5)

    def tick(self, now=None):
        snap = self.collector.collect(self.project["id"], now or time.time())
        return self.controller.tick(snap)

    def test_real_registry_receipts_and_monitor_identity_match_documented_contract(self):
        health = self.tick()
        self.controller.heartbeat(health)
        rows = self.journal.rows()
        self.assertEqual(len(rows),1)
        self.assertIsNotNone(rows[0]["dispatch_id"], rows[0])
        self.assertIsNone(rows[0]["last_error"])
        repaired = self.board.call("GET",f"/tasks/{rows[0]['task_id']}")
        original = self.board.call("GET",f"/tasks/{self.task['id']}")
        self.assertEqual(repaired["assignee"],"recovery-engineer")
        self.assertEqual(original["metadata"]["recovery_task_id"],repaired["id"])
        self.assertEqual(repaired["metadata"]["recovery"]["kind"],"session_failed")
        dispatches = self.board.pages("/fleet/dispatches")
        self.assertEqual(len(dispatches),1)
        self.assertEqual(dispatches[0]["request_id"],f"task:{repaired['id']}:attempt:1")
        self.assertEqual(dispatches[0]["agent_id"],"recovery-engineer")
        self.assertEqual(dispatches[0]["host_id"],"test-host")
        monitor = self.board.call("GET","/agents/native-recovery-monitor")
        self.assertEqual(monitor["metadata"]["recovery_monitor"]["revision"],"test-monitor")
        for _ in range(5): self.tick()
        self.assertEqual(len(self.board.pages("/fleet/dispatches")),1)
        self.assertEqual(len(self.board.call("GET",f"/tasks?project_id={self.project['id']}")),2)
        self.assertTrue(all(method == "GET" for method,_ in self.native_requests), "monitor never sends native mutation")

    def test_running_monitor_discovers_new_project_without_host_config_change(self):
        directory = Path(self.tmp.name)
        config = directory / "monitor.json"
        config.write_text(json.dumps({"board_url":self.board.base,
            "state_path":str(directory / "monitor.sqlite"),"poll_seconds":.05,"http_timeout_seconds":2,
            "defaults":{"missing_session_grace_seconds":0}}))
        self.assertNotIn("project_ids",json.loads(config.read_text()))
        monitor = subprocess.Popen([sys.executable,str(fixture.MODULE),"--config",str(config)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def cleanup():
            if monitor.poll() is None:
                monitor.terminate()
                try: monitor.wait(5)
                except subprocess.TimeoutExpired:
                    monitor.kill(); monitor.wait(3)
        self.addCleanup(cleanup)
        def wait_for_link(tid):
            deadline = time.monotonic()+10
            while time.monotonic()<deadline:
                self.assertIsNone(monitor.poll(),"monitor exited")
                observed = self.board.call("GET",f"/tasks/{tid}")
                if observed["metadata"].get("recovery_task_id"):
                    return observed["metadata"]["recovery_task_id"]
                time.sleep(.05)
            self.fail("new project's actionable task never got an owned recovery path")
        # First pass is known complete before the second project even exists.
        wait_for_link(self.task["id"])
        added = self.board.call("POST","/projects",{"name":"Created after monitor startup"})
        work = self.board.call("POST","/tasks",{"project_id":added["id"],"assignee":"engineer",
            "title":"New-project actionable orphan"})
        repair_id = wait_for_link(work["id"])
        repair = self.board.call("GET",f"/tasks/{repair_id}")
        self.assertEqual(repair["project_id"],added["id"])
        self.assertEqual(repair["assignee"],"recovery-engineer")
        self.assertEqual(repair["metadata"]["recovery"]["kind"],"missing_actionable_session")
        excluded = []
        for name, archived in [("Archived project",True),("Recovery disabled project",False)]:
            project = self.board.call("POST","/projects",{"name":name,
                "metadata":{"recovery":{"enabled":False}}})
            if archived:
                self.board.call("PATCH",f"/projects/{project['id']}",{"status":"archived"})
            excluded.append(self.board.call("POST","/tasks",{"project_id":project["id"],
                "assignee":"engineer","title":"Intentionally excluded work"}))
        time.sleep(.4)
        for work in excluded:
            observed = self.board.call("GET",f"/tasks/{work['id']}")
            self.assertNotIn("recovery_task_id",observed["metadata"])
        self.assertNotIn("project_ids",json.loads(config.read_text()),"no config edit or restart was used")

    def test_frozen_dispatch_receipt_survives_response_loss_and_real_board_reassignment(self):
        original = self.board.call
        sent = []
        lost = False
        def wrapped(method,path,data=None):
            nonlocal lost
            value = original(method,path,data)
            if method == "POST" and path == "/fleet/dispatches":
                sent.append(copy.deepcopy(data))
                if not lost:
                    lost = True
                    raise r.HttpFailure(method)
            return value
        self.board.call = wrapped
        self.tick()
        row = self.journal.rows()[0]
        self.assertIsNone(row["dispatch_id"])
        original("PATCH",f"/tasks/{row['task_id']}",{"assignee":"pm"})
        self.tick(time.time()+100)
        row = self.journal.rows()[0]
        self.assertEqual(len(sent),2, row)
        self.assertEqual(sent[0],sent[1])
        self.assertIsNotNone(row["dispatch_id"],row)
        self.assertEqual(len(self.board.pages("/fleet/dispatches")),1)


if __name__ == "__main__":
    unittest.main()
