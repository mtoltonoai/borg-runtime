"""Shared persistent workspace service: real HTTP, files and process boundaries."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
import uuid

from test_workspace_mcp import mcp


class SessionWorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "worktrees"
        self.root.mkdir()
        self.ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        for session in self.ids:
            (self.root / session).mkdir()
        self.server = mcp.WorkspaceServer(("127.0.0.1", 0), session_root=self.root)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.thread.join(2)
        self.server.server_close()

    def request(self, path, message=None, method="POST", headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        try:
            request_headers = {"Content-Type": "application/json"}
            request_headers.update(headers or {})
            connection.request(method, path, json.dumps(message) if method == "POST" else None,
                               headers=request_headers)
            response = connection.getresponse()
            body = response.read()
            return response.status, json.loads(body) if body else None
        finally:
            connection.close()

    def call(self, session, name, arguments):
        status, response = self.request(f"/mcp/{session}", {
            "jsonrpc": "2.0", "id": "effect", "method": "tools/call", "params": {
                "name": name, "arguments": arguments,
                "_meta": {"borg/operation_id": f"{session}:test-effect"}}})
        self.assertEqual(status, 200)
        result = response["result"]
        return result["isError"], json.loads(result["content"][0]["text"])

    def test_two_sessions_concurrently_write_read_and_run_in_separate_worktrees(self):
        def work(session):
            failed, result = self.call(session, "workspace_write", {"path": "same.txt", "content": session})
            self.assertFalse(failed)
            failed, result = self.call(session, "workspace_run", {
                "command": "printf '%s\\n' \"$PWD\"; cat same.txt; printf shell > from-shell"})
            self.assertFalse(failed)
            self.assertIn(str(self.root / session), result["output"])
            self.assertIn(session, result["output"])
            failed, read = self.call(session, "workspace_read", {"path": "same.txt"})
            self.assertFalse(failed)
            self.assertEqual(read["content"], session)
            return result["output"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            outputs = list(pool.map(work, self.ids))
        self.assertNotIn(self.ids[1], outputs[0])
        self.assertNotIn(self.ids[0], outputs[1])
        for session in self.ids:
            self.assertEqual((self.root / session / "same.txt").read_text(), session)
            self.assertEqual((self.root / session / "from-shell").read_text(), "shell")
        self.assertFalse((self.root / "same.txt").exists())

    def test_routing_rejects_missing_alias_traversal_and_symlink_without_side_effects(self):
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "workspace_write", "arguments": {"path": "escaped", "content": "bad"}}}
        absent = str(uuid.uuid4())
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        linked = str(uuid.uuid4())
        (self.root / linked).symlink_to(outside, target_is_directory=True)
        for path in ("/mcp", "/mcp/", f"/mcp/{absent}", "/mcp/../outside", "/mcp/%2e%2e",
                     f"/mcp/{self.ids[0]}/..", f"/mcp/{self.ids[0]}?workspace=other",
                     f"/mcp/{self.ids[0].upper()}", f"/mcp/{self.ids[0].replace('-', '')}",
                     f"/mcp/{linked}"):
            with self.subTest(path=path):
                self.assertEqual(self.request(path, payload)[0], 404)
        self.assertFalse((outside / "escaped").exists())
        self.assertFalse((self.root / absent).exists())
        self.assertFalse((self.root / self.ids[0] / "escaped").exists())

    def test_opened_binding_pins_inode_for_files_and_shell_even_after_path_replacement(self):
        workspace = self.server.sessions.open(f"/mcp/{self.ids[0]}")
        self.addCleanup(os.close, workspace._root_fd)
        original = self.root / self.ids[0]
        pinned = self.root / "renamed-original"
        original.rename(pinned)
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        original.symlink_to(outside, target_is_directory=True)
        workspace.write("file.txt", "pinned")
        result = workspace.run("printf pinned-shell > shell.txt")
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual((pinned / "file.txt").read_text(), "pinned")
        self.assertEqual((pinned / "shell.txt").read_text(), "pinned-shell")
        self.assertEqual(list(outside.iterdir()), [])

    def test_health_and_protocol_do_not_allocate_workspaces_and_auth_is_enforced(self):
        before = set(self.root.iterdir())
        status, health = self.request("/health", method="GET")
        self.assertEqual(status, 200)
        self.assertEqual(health["mode"], "sessions")
        self.assertEqual(health["status"], "ok")
        self.assertEqual(self.request("/health", method="GET")[1]["instance_id"], health["instance_id"])
        self.assertEqual(self.request("/health", {})[0], 405)
        path = f"/mcp/{self.ids[0]}"
        self.assertEqual(self.request(path, {"jsonrpc": "2.0", "method": "notifications/initialized"})[0], 202)
        status, response = self.request(path, {"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
        self.assertEqual(status, 200)
        self.assertEqual(len(response["result"]["tools"]), 4)
        self.assertEqual(self.request(path, method="GET")[0], 405)
        self.assertEqual(before, set(self.root.iterdir()))
        self.server.bearer_token = "fixture-secret"
        self.assertEqual(self.request(path, {})[0], 401)
        self.assertEqual(self.request(path, {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                                      headers={"Authorization": "Bearer fixture-secret"})[0], 200)
        self.assertEqual(self.request(path, {}, headers={"Authorization": "Bearer fixture-secret",
                                                        "Origin": "https://untrusted.example"})[0], 403)


if __name__ == "__main__":
    unittest.main()
