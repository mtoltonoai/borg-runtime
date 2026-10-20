"""Automatic drain regression, including actual native HTTP/MCP execution."""
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tooling"))
import native_drain as drain
import native_release as release


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DrainUnitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "native.sqlite"
        self.journal = self.root / "drain.json"
        with sqlite3.connect(self.database) as db:
            db.executescript("CREATE TABLE sessions(id TEXT PRIMARY KEY,body TEXT,stop_requested INTEGER DEFAULT 0); CREATE TABLE tool_calls(session_id TEXT,body TEXT);")

    def session(self, sid, status="running", stop=0, progress="Calling workspace__workspace_run", tool_status="running"):
        body = {"id": sid, "status": status, "progress": progress}
        tool = {"call": {"call_id": "call-1", "status": tool_status}}
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO sessions VALUES(?,?,?)", (sid, json.dumps(body), stop))
            db.execute("INSERT INTO tool_calls VALUES(?,?)", (sid, json.dumps(tool)))
        return body

    def test_intent_is_durable_and_body_tool_receipt_not_rewritten(self):
        body = self.session("active")
        value = drain.request_drain(self.database, self.journal)
        self.assertEqual(value["sessions"], ["active"])
        with sqlite3.connect(self.database) as db:
            observed = drain.rows(db)[0]
            self.assertTrue(observed["stop_requested"])
            self.assertEqual(observed["tools"], {"call-1": "running"})
            self.assertEqual(json.loads(db.execute("SELECT body FROM sessions").fetchone()[0]), body)

    def test_operator_stopped_session_not_owned_or_resumed(self):
        self.session("operator", status="stopped", stop=1, tool_status="succeeded")
        value = drain.request_drain(self.database, self.journal)
        self.assertEqual(value["sessions"], [])
        with mock.patch.object(drain, "api") as api:
            drain.resume("http://unused", self.journal)
            api.assert_not_called()

    def test_running_tool_never_receives_cancellation(self):
        self.session("active")
        with mock.patch.object(drain, "api") as api:
            with self.assertRaises(TimeoutError):
                drain.drain(self.database, "http://unused", self.journal, wait_seconds=0)
            api.assert_not_called()
        self.assertEqual(json.loads(self.journal.read_text())["state"], "waiting_for_boundary")

    def test_unknown_write_stays_visible_no_replay(self):
        self.session("unknown", status="needs_resolution", tool_status="unknown")
        with mock.patch.object(drain, "api") as api:
            with self.assertRaisesRegex(RuntimeError, "reconciliation"):
                drain.drain(self.database, "http://unused", self.journal, wait_seconds=0)
            api.assert_not_called()
        self.assertEqual(json.loads(self.journal.read_text())["state"], "needs_reconciliation")

    def test_polling_never_rewrites_stop_flag_or_holds_writer_transaction(self):
        body = self.session("active")
        statements = []
        original_connect = sqlite3.connect
        def traced(*args, **kwargs):
            db = original_connect(*args, **kwargs)
            db.set_trace_callback(statements.append)
            return db
        def settle():
            while not self.journal.exists():
                time.sleep(0.001)
            time.sleep(0.06)
            with original_connect(self.database) as db:
                db.execute("UPDATE sessions SET body=?", (json.dumps(dict(body, status="stopped")),))
                db.execute("UPDATE tool_calls SET body=?", (json.dumps({"call": {"call_id": "call-1", "status": "succeeded"}}),))
        with mock.patch.object(drain.sqlite3, "connect", side_effect=traced), concurrent.futures.ThreadPoolExecutor(1) as pool:
            future = pool.submit(settle)
            drain.drain(self.database, "http://unused", self.journal, wait_seconds=2, interval=0.005)
            future.result(timeout=2)
        writes = [s for s in statements if s.startswith("UPDATE sessions SET stop_requested=1")]
        self.assertEqual(len(writes), 1, statements)
        self.assertEqual(statements.count("BEGIN IMMEDIATE"), 1, statements)
        self.assertGreater(statements.count("BEGIN"), 2)

    def test_stale_inference_snapshot_and_raced_tool_never_cancelled(self):
        self.session("active", progress="Waiting for model response", tool_status="succeeded")
        def actor_after_old_stop_check():
            while not self.journal.exists():
                time.sleep(0.001)
            # Actor had already passed stopped=false before drain's commit.
            # It dispatches AFTER a stale inference/no-running-tool snapshot.
            with sqlite3.connect(self.database) as db:
                body = json.loads(db.execute("SELECT body FROM sessions").fetchone()[0])
                body.update(progress="Calling original raced tool")
                db.execute("UPDATE sessions SET body=?", (json.dumps(body),))
                db.execute("UPDATE tool_calls SET body=?", (json.dumps({"call": {"call_id": "call-1", "status": "running"}}),))
            time.sleep(0.04)
            with sqlite3.connect(self.database) as db:
                body["status"] = "stopped"
                db.execute("UPDATE sessions SET body=?", (json.dumps(body),))
                db.execute("UPDATE tool_calls SET body=?", (json.dumps({"call": {"call_id": "call-1", "status": "succeeded"}}),))
        with mock.patch.object(drain, "api") as api, concurrent.futures.ThreadPoolExecutor(1) as pool:
            actor = pool.submit(actor_after_old_stop_check)
            value = drain.drain(self.database, "http://unused", self.journal, wait_seconds=2, interval=0.005)
            actor.result(timeout=2)
            api.assert_not_called()
        self.assertEqual(value["state"], "drained")
        self.assertEqual(value["after"][0]["tools"], {"call-1": "succeeded"})

    def test_timeout_cleanup_does_not_claim_running_tool_resumed(self):
        self.session("active")
        with self.assertRaises(TimeoutError):
            drain.drain(self.database, "http://unused", self.journal, wait_seconds=0)
        with mock.patch.object(drain, "api") as api:
            with self.assertRaises(TimeoutError):
                drain.resume("http://unused", self.journal, wait_seconds=0)
            api.assert_not_called()
        value = json.loads(self.journal.read_text())
        self.assertEqual(value["state"], "resume_pending")
        self.assertEqual(value["resumed"], [])
        with sqlite3.connect(self.database) as db:
            self.assertEqual(db.execute("SELECT stop_requested FROM sessions").fetchone()[0], 1)

    def test_timeout_then_late_tool_boundary_gets_verified_real_resume(self):
        body = self.session("active")
        with self.assertRaises(TimeoutError):
            drain.drain(self.database, "http://unused", self.journal, wait_seconds=0)
        def runtime_resume(url, sid, action):
            self.assertEqual(action, "resume")
            with sqlite3.connect(self.database) as db:
                old, flag = db.execute("SELECT body,stop_requested FROM sessions").fetchone()
                self.assertEqual(json.loads(old)["status"], "stopped")
                self.assertEqual(flag, 1)
                value = dict(body, status="ready")
                db.execute("UPDATE sessions SET body=?,stop_requested=0", (json.dumps(value),))
            return value
        with mock.patch.object(drain, "api", side_effect=runtime_resume) as api, concurrent.futures.ThreadPoolExecutor(1) as pool:
            future = pool.submit(drain.resume, "http://unused", self.journal, 2, 0.005)
            time.sleep(0.03)
            self.assertFalse(future.done())
            self.assertEqual(json.loads(self.journal.read_text())["resumed"], [])
            api.assert_not_called()
            # This late stopped write is the race which used to orphan work.
            with sqlite3.connect(self.database) as db:
                db.execute("UPDATE sessions SET body=?", (json.dumps(dict(body, status="stopped")),))
                db.execute("UPDATE tool_calls SET body=?", (json.dumps({"call": {"call_id": "call-1", "status": "succeeded"}}),))
            value = future.result(timeout=2)
        self.assertEqual(value["state"], "resumed")
        self.assertEqual(value["resumed"], ["active"])
        self.assertEqual(api.call_count, 1)
        with sqlite3.connect(self.database) as db:
            self.assertEqual(db.execute("SELECT stop_requested FROM sessions").fetchone()[0], 0)

    def test_lost_resume_reply_is_reconciled_without_repeating_write(self):
        body = self.session("active")
        drain.request_drain(self.database, self.journal)
        with sqlite3.connect(self.database) as db:
            db.execute("UPDATE sessions SET body=?", (json.dumps(dict(body, status="stopped")),))
        def committed_but_lost(url, sid, action):
            with sqlite3.connect(self.database) as db:
                db.execute("UPDATE sessions SET body=?,stop_requested=0", (json.dumps(dict(body, status="running")),))
            raise OSError("lost HTTP reply after commit")
        with mock.patch.object(drain, "api", side_effect=committed_but_lost) as api:
            value = drain.resume("http://unused", self.journal, wait_seconds=1)
        self.assertEqual(value["state"], "resumed")
        self.assertEqual(api.call_count, 1)


@unittest.skipUnless(os.environ.get("BORG_NATIVE_TEST_BIN"), "set BORG_NATIVE_TEST_BIN for real native execution")
class LiveNativeDrainTests(unittest.TestCase):
    def test_inflight_tool_commits_once_across_drain_restart_resume(self):
        binary = Path(os.environ["BORG_NATIVE_TEST_BIN"]).resolve()
        entered = threading.Event()
        finish = threading.Event()
        counter = {"tool": 0, "provider": 0}

        class Backend(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send_json(self, value):
                raw = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/responses":
                    counter["provider"] += 1
                    if any(v.get("type") == "function_call_output" for v in body.get("input", [])):
                        output = [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]}]
                    else:
                        output = [{"type": "function_call", "call_id": "call-preserve", "name": "test__record", "arguments": "{}"}]
                    self.send_json({"id": "response-" + str(counter["provider"]), "status": "completed", "output": output})
                    return
                method = body.get("method")
                if method == "notifications/initialized":
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if method == "initialize":
                    result = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "test", "version": "1"}}
                elif method == "tools/list":
                    result = {"tools": [{"name": "record", "description": "Record one durable side effect", "inputSchema": {"type": "object", "properties": {}}}]}
                elif method == "tools/call":
                    counter["tool"] += 1
                    entered.set()
                    finish.wait(10)
                    result = {"content": [{"type": "text", "text": "original receipt"}], "isError": False}
                else:
                    raise AssertionError(method)
                self.send_json({"jsonrpc": "2.0", "id": body.get("id"), "result": result})

        backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(backend.server_close)
        self.addCleanup(backend.shutdown)
        self.addCleanup(finish.set)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, journal = root / "native.sqlite", root / "drain.json"
            port = free_port()
            base = f"http://127.0.0.1:{port}"
            backend_url = f"http://127.0.0.1:{backend.server_port}"
            config = {"database_path": str(database), "listen": f"127.0.0.1:{port}", "agents": [{
                "id": "verification", "model": "gpt-6-astra", "instructions": "test", "api_key_env": "DRAIN_TEST_API_KEY",
                "provider_url": backend_url + "/responses", "mcp_servers": [{"name": "test", "url": backend_url + "/mcp"}],
            }]}
            config_path = root / "runtime.json"
            config_path.write_text(json.dumps(config))
            log = (root / "runtime.log").open("w+")
            env = dict(os.environ, DRAIN_TEST_API_KEY="test-only-not-a-real-credential")
            def start():
                process = subprocess.Popen([str(binary), "--config", str(config_path)], stdout=log, stderr=log, env=env)
                for _ in range(200):
                    if process.poll() is not None:
                        log.seek(0)
                        self.fail("runtime failed: " + log.read())
                    try:
                        release.get_json(base + "/health")
                        return process
                    except OSError:
                        time.sleep(0.02)
                self.fail("runtime did not become ready")
            process = start()
            try:
                import urllib.request
                request = urllib.request.Request(base + "/api/sessions", data=json.dumps({"agent_id": "verification", "input": "record"}).encode(), headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=5) as response:
                    session = json.load(response)
                self.assertTrue(entered.wait(5), "native did not reach MCP tool")
                with concurrent.futures.ThreadPoolExecutor(1) as pool:
                    future = pool.submit(drain.drain, database, base, journal, 10, 0.02)
                    for _ in range(100):
                        if journal.exists():
                            break
                        time.sleep(0.01)
                    time.sleep(0.05)
                    self.assertFalse(future.done(), "drain must wait for original running tool")
                    finish.set()
                    self.assertEqual(future.result(timeout=10)["state"], "drained")
                before = release.session_snapshot(base)
                self.assertEqual(before[0]["tools"], {"call-preserve": "succeeded"})
                self.assertEqual(len(before[0]["receipt_hashes"]), 1)
                process.terminate()
                process.wait(timeout=5)
                process = start()
                release.verify_preserved(before, release.session_snapshot(base))
                drain.resume(base, journal)
                for _ in range(200):
                    observed = drain.api(base, session["id"])
                    if observed["status"] == "completed":
                        break
                    time.sleep(0.02)
                self.assertEqual(observed["status"], "completed")
                self.assertEqual(counter["tool"], 1, "mutating tool was replayed")
                release.verify_preserved(before, release.session_snapshot(base))
            finally:
                finish.set()
                process.terminate()
                process.wait(timeout=5)
                log.close()


if __name__ == "__main__":
    unittest.main()
