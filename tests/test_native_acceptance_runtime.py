"""Custody audit against a disposable actual native process and durable SQLite.

Set BORG_NATIVE_TEST_BIN to the built borg-native. Only the model/MCP peer is a
loopback fixture; the native engine, public tool API, storage and process restart
are real. All mutations/corruption injection target this test's temporary DB.
"""
import copy
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from test_native_acceptance import audit


@unittest.skipUnless(os.environ.get("BORG_NATIVE_TEST_BIN"), "set BORG_NATIVE_TEST_BIN for actual native receipt audit")
class NativeReceiptCustodyTests(unittest.TestCase):
    def test_actual_receipt_restart_and_same_result_changed_arguments(self):
        counter = {"tool": 0}
        private = "PRIVATE-fixture-argument-and-result"

        class Backend(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self, value):
                payload = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/responses":
                    if any(i.get("type") == "function_call_output" for i in body.get("input", [])):
                        output = [{"type": "message", "role": "assistant",
                                   "content": [{"type": "output_text", "text": "done"}]}]
                    else:
                        output = [{"type": "function_call", "call_id": "call-custody", "name": "fixture__record",
                                   "arguments": json.dumps({"value": private})}]
                    return self.reply({"id": "fixture-response", "status": "completed", "output": output})
                method = body.get("method")
                if method == "notifications/initialized":
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if method == "initialize":
                    result = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}},
                              "serverInfo": {"name": "fixture", "version": "1"}}
                elif method == "tools/list":
                    result = {"tools": [{"name": "record", "description": "Return one fixture receipt",
                                         "inputSchema": {"type": "object", "properties": {"value": {"type": "string"}}}}]}
                elif method == "tools/call":
                    counter["tool"] += 1
                    # A normal completed MCP application-error receipt must not
                    # be mistaken for an unknown side effect or get replayed.
                    result = {"content": [{"type": "text", "text": private}], "isError": True}
                else:
                    raise AssertionError("unexpected fixture method")
                self.reply({"jsonrpc": "2.0", "id": body.get("id"), "result": result})

        backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(backend.server_close)
        self.addCleanup(thread.join, 5)
        self.addCleanup(backend.shutdown)
        with tempfile.TemporaryDirectory(prefix="native-custody-") as directory:
            root = Path(directory)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            origin = f"http://127.0.0.1:{port}"
            peer = f"http://127.0.0.1:{backend.server_port}"
            database = root / "native.sqlite"
            config = root / "native.json"
            config.write_text(json.dumps({"listen": f"127.0.0.1:{port}", "database_path": str(database),
                "agents": [{"id": "custody-fixture", "model": "gpt-6-astra", "reasoning_effort": "max",
                    "max_output_tokens": 128000, "context_window_tokens": 1050000, "max_turns": None,
                    "max_retries": None, "instructions": "test", "api_key_env": "CUSTODY_TEST_API_KEY",
                    "provider_url": peer + "/responses", "mcp_servers": [{"name": "fixture", "url": peer + "/mcp"}]}]}))
            env = {k: v for k, v in os.environ.items() if k not in {"LISTEN_FDS", "LISTEN_PID", "LISTEN_FDNAMES"}}
            env["CUSTODY_TEST_API_KEY"] = "test-only-not-a-real-credential"
            reader = audit.Reader(timeout=2)
            process = None

            def stop():
                nonlocal process
                if process is not None:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(5)
                    process = None

            def start(log):
                nonlocal process
                process = subprocess.Popen([str(Path(os.environ["BORG_NATIVE_TEST_BIN"]).resolve()),
                    "--config", str(config)], stdout=log, stderr=log, env=env)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    self.assertIsNone(process.poll(), "test native exited before readiness")
                    try:
                        reader.get(origin, "/health")
                        return
                    except audit.ReadError:
                        time.sleep(.02)
                self.fail("test native readiness deadline exceeded")

            def snapshot():
                # The evidence path is GET-only, using the same sanitizer as
                # production collection, not fake saved ToolCall fixtures.
                sessions = reader.get(origin, "/api/sessions")
                return {"errors": [], "runtimes": {"fixture": {
                    "health": reader.get(origin, "/health"),
                    "sessions": [audit.sanitize_session(s) for s in sessions],
                    "tools": {s["id"]: [audit.tool_receipt(c) for c in reader.get(
                        origin, f"/api/sessions/{s['id']}/tools")] for s in sessions}}}}

            with (root / "native.log").open("w+") as log:
                try:
                    start(log)
                    request = urllib.request.Request(origin + "/api/sessions", method="POST",
                        headers={"Content-Type": "application/json"},
                        data=json.dumps({"agent_id": "custody-fixture", "input": "record"}).encode())
                    with urllib.request.urlopen(request, timeout=5) as response:
                        sid = json.load(response)["id"]
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        session = reader.get(origin, f"/api/sessions/{sid}")
                        if session["status"] == "completed":
                            break
                        time.sleep(.02)
                    self.assertEqual(session["status"], "completed")
                    before = snapshot()
                    original = before["runtimes"]["fixture"]["tools"][sid][0]
                    self.assertEqual(original["status"], "succeeded")
                    self.assertIn("receipt_sha256", original)
                    self.assertNotIn(private, json.dumps(before))
                    stop()
                    start(log)
                    after = snapshot()
                    result = audit.compare_restart(before, after, ["fixture"])
                    self.assertTrue(result["ok"], result)
                    self.assertEqual(result["preserved_completed_tools"], 1)
                    self.assertEqual(counter["tool"], 1, "completed application-error receipt was replayed")
                    stop()
                    # Simulate a corrupt recovery rewriting only arguments in
                    # this isolated DB while retaining the same result/call IDs.
                    with sqlite3.connect(database) as db:
                        row = json.loads(db.execute("SELECT body FROM tool_calls WHERE session_id=?", (sid,)).fetchone()[0])
                        frozen_servers = copy.deepcopy(row["servers"])
                        row["call"]["arguments"] = {"value": "REPLACEMENT-private"}
                        db.execute("UPDATE tool_calls SET body=? WHERE session_id=?", (json.dumps(row), sid))
                    start(log)
                    corrupt = snapshot()
                    changed = corrupt["runtimes"]["fixture"]["tools"][sid][0]
                    self.assertEqual(changed["result_sha256"], original["result_sha256"])
                    self.assertNotEqual(changed["receipt_sha256"], original["receipt_sha256"])
                    result = audit.compare_restart(before, corrupt, ["fixture"])
                    self.assertFalse(result["ok"], result)
                    self.assertNotIn(private, json.dumps(corrupt))
                    self.assertNotIn("REPLACEMENT", json.dumps(corrupt))
                    self.assertEqual(counter["tool"], 1, "the evidence reader must never cause replay")
                    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
                        current = json.loads(db.execute("SELECT body FROM tool_calls WHERE session_id=?", (sid,)).fetchone()[0])
                    self.assertEqual(current["servers"], frozen_servers)
                finally:
                    stop()


if __name__ == "__main__":
    unittest.main()
