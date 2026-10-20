"""Real workspace, process-group and HTTP tests; no external services needed."""
import http.client
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
import threading
import time
import unittest

MODULE = Path(__file__).resolve().parents[1] / "tooling/workspace_mcp.py"
spec = importlib.util.spec_from_file_location("workspace_mcp", MODULE)
mcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp)


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "workspace"
        self.root.mkdir()
        self.workspace = mcp.Workspace(self.root)

    def call(self, name, **arguments):
        response = mcp.dispatch(self.workspace, {
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": name, "arguments": arguments,
                       "_meta": {"borg/operation_id": "caller-owned-operation"}}})
        result = response["result"]
        return result["isError"], json.loads(result["content"][0]["text"])

    def test_write_read_append_and_list_real_files(self):
        failed, result = self.call("workspace_write", path="sub/hello.txt", content="héllo")
        self.assertFalse(failed)
        self.assertEqual(result["bytes_written"], 6)
        self.assertFalse(self.call("workspace_write", path="sub/hello.txt", content="!", append=True)[0])
        self.assertEqual(self.call("workspace_read", path="sub/hello.txt")[1]["content"], "héllo!")
        result = self.call("workspace_list", path="sub")[1]
        self.assertEqual(result["entries"], [{"name": "hello.txt", "kind": "file", "bytes": 7}])
        self.assertFalse(result["truncated"])
        self.call("workspace_write", path="sub/hello.txt", content="x")
        self.assertEqual((self.root / "sub/hello.txt").read_text(), "x")

    def test_read_bounds_utf8_and_list_bound(self):
        (self.root / "a").write_text("ééé")
        (self.root / "b").touch()
        result = self.call("workspace_read", path="a", max_bytes=3)[1]
        self.assertEqual(result["content"], "é")
        self.assertTrue(result["truncated"])
        self.assertEqual(result["bytes_returned"], 2)
        self.assertTrue(self.call("workspace_list", limit=1)[1]["truncated"])

    def test_escape_and_symlink_paths_do_not_read_or_write_outside(self):
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        secret = outside / "secret"
        secret.write_text("retained")
        (self.root / "link").symlink_to(outside, target_is_directory=True)
        (self.root / "leaf").symlink_to(secret)
        for path in ("../outside/secret", str(secret), "link/secret", "leaf"):
            with self.subTest(path=path):
                self.assertTrue(self.call("workspace_read", path=path)[0])
                self.assertTrue(self.call("workspace_write", path=path, content="changed")[0])
        self.assertTrue(self.call("workspace_write", path="link/new/sub/file", content="x")[0])
        self.assertTrue(self.call("workspace_list", path="link")[0])
        self.assertEqual(secret.read_text(), "retained")
        self.assertFalse((outside / "new").exists())

    def test_fifo_is_refused_without_blocking_or_truncating(self):
        os.mkfifo(self.root / "fifo")
        started = time.monotonic()
        self.assertTrue(self.call("workspace_read", path="fifo")[0])
        self.assertTrue(self.call("workspace_write", path="fifo", content="x")[0])
        self.assertLess(time.monotonic() - started, 1)

    def test_bad_arguments_are_tool_errors_without_writes(self):
        for name, arguments in (
            ("workspace_write", {"path": "x", "content": "x", "append": 1}),
            ("workspace_read", {"path": "x", "max_bytes": True}),
            ("workspace_write", {"path": "x", "content": "x" * (mcp.MAX_BYTES + 1)}),
            ("workspace_run", {"command": "touch x", "timeout_seconds": float("nan")}),
            ("workspace_run", {"command": "touch x", "timeout_seconds": 10 ** 400}),
            ("workspace_write", {"path": "x", "content": "x", "unexpected": True}),
        ):
            with self.subTest(name=name, arguments=list(arguments)):
                self.assertTrue(self.call(name, **arguments)[0])
        self.assertFalse((self.root / "x").exists())

    def test_shell_cwd_no_stdin_exit_and_bounded_output(self):
        command = ("printf '%s\\n' \"$PWD\"; "
                   "if read value; then printf unexpected; else printf eof; fi; "
                   "printf error >&2; exit 7")
        failed, result = self.call("workspace_run", command=command)
        self.assertTrue(failed)
        self.assertEqual(result["exit_code"], 7)
        self.assertIn(str(self.root), result["output"])
        self.assertIn("eoferror", result["output"])
        self.assertFalse(result["timed_out"])
        command = shlex.join([sys.executable, "-c", "print('x' * 200000)"])
        failed, result = self.call("workspace_run", command=command, max_output_bytes=100)
        self.assertFalse(failed)
        self.assertEqual(len(result["output"]), 100)
        self.assertEqual(result["output_bytes_seen"], 200001)
        self.assertTrue(result["truncated"])

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux /proc child-state observation")
    def test_timeout_kills_shell_and_child_process_group(self):
        program = ("import os,time,pathlib\n"
                   "child=os.fork()\n"
                   "if child == 0:\n"
                   " pathlib.Path('child.pid').write_text(str(os.getpid()))\n"
                   " time.sleep(30)\n"
                   " pathlib.Path('escaped').write_text('bad')\n"
                   "else:\n"
                   " pathlib.Path('parent.pid').write_text(str(os.getpid()))\n"
                   " time.sleep(30)\n")
        failed, result = self.call("workspace_run",
                                  command="exec " + shlex.join([sys.executable, "-c", program]),
                                  timeout_seconds=0.5)
        self.assertTrue(failed)
        self.assertTrue(result["timed_out"])
        self.assertLess(result["seconds"], 2)
        self.assertFalse((self.root / "escaped").exists())
        for name in ("parent.pid", "child.pid"):
            pid = int((self.root / name).read_text())
            path = Path("/proc") / str(pid) / "stat"
            deadline = time.monotonic() + 1
            while path.exists() and time.monotonic() < deadline:
                if path.read_text().split(") ", 1)[1].split()[0] == "Z":
                    break
                time.sleep(0.01)
            if path.exists():
                self.assertEqual(path.read_text().split(") ", 1)[1].split()[0], "Z")

    def test_child_holding_pipe_cannot_outlive_deadline_after_shell_exits(self):
        command = shlex.join([sys.executable, "-c", "import time; time.sleep(30)"]) + " & exit 0"
        failed, result = self.call("workspace_run", command=command, timeout_seconds=0.2)
        self.assertTrue(failed)
        self.assertTrue(result["timed_out"])
        self.assertLess(result["seconds"], 2)

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux /proc child-state observation")
    def test_redirected_background_child_is_cleaned_up_after_normal_exit(self):
        failed, result = self.call(
            "workspace_run", command="sleep 30 >/dev/null 2>&1 & echo $! > background.pid; exit 0")
        self.assertFalse(failed)
        self.assertEqual(result["exit_code"], 0)
        self.assertFalse(result["timed_out"])
        pid = int((self.root / "background.pid").read_text())
        path = Path("/proc") / str(pid) / "stat"
        deadline = time.monotonic() + 1
        while path.exists() and time.monotonic() < deadline:
            if path.read_text().split(") ", 1)[1].split()[0] == "Z":
                break
            time.sleep(0.01)
        if path.exists():
            self.assertEqual(path.read_text().split(") ", 1)[1].split()[0], "Z")

    def test_notification_cannot_execute_a_tool(self):
        response = mcp.dispatch(self.workspace, {"jsonrpc": "2.0", "method": "tools/call",
            "params": {"name": "workspace_write", "arguments": {"path": "x", "content": "x"}}})
        self.assertIn("error", response)
        self.assertFalse((self.root / "x").exists())


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.server = mcp.WorkspaceServer(("127.0.0.1", 0), mcp.Workspace(self.tmp.name))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, value=None, method="POST", headers=None, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        selected = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        selected.update(headers or {})
        try:
            connection.request(method, "/mcp", body=body if body is not None else json.dumps(value),
                               headers=selected)
            response = connection.getresponse()
            data = response.read()
            return response.status, response.headers, json.loads(data) if data else None
        finally:
            connection.close()

    def test_initialize_notification_tools_and_real_call_over_http(self):
        for version in mcp.PROTOCOLS:
            status, headers, response = self.request({"jsonrpc": "2.0", "id": "init",
                "method": "initialize", "params": {"protocolVersion": version,
                "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}})
            self.assertEqual(status, 200)
            self.assertEqual(response["id"], "init")
            self.assertEqual(response["result"]["protocolVersion"], version)
            self.assertNotIn("Mcp-Session-Id", headers)
        self.assertEqual(self.request({"jsonrpc": "2.0", "method": "notifications/initialized"})[::2], (202, None))
        tools = self.request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})[2]["result"]["tools"]
        self.assertEqual({t["name"] for t in tools}, {"workspace_list", "workspace_read", "workspace_write", "workspace_run"})
        result = self.request({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "workspace_write", "arguments": {"path": "http.txt", "content": "written"}}})[2]
        self.assertFalse(result["result"]["isError"])
        self.assertEqual(Path(self.tmp.name, "http.txt").read_text(), "written")

    def test_parse_rpc_origin_auth_and_no_sse(self):
        self.assertEqual(self.request(body="{")[0], 400)
        self.assertEqual(self.request({"jsonrpc": "2.0", "id": 1, "method": "unknown"})[2]["error"]["code"], -32601)
        self.assertEqual(self.request(method="GET")[0], 405)
        self.assertEqual(self.request({}, headers={"Origin": "https://untrusted.example"})[0], 403)
        self.assertEqual(self.request({}, headers={"Host": "untrusted.example"})[0], 403)
        self.assertEqual(self.request({}, headers={"Host": "["})[0], 400)
        self.server.bearer_token = "test-only-token"
        self.assertEqual(self.request({})[0], 401)
        self.assertEqual(self.request({}, headers={"Authorization": "Bearer é"})[0], 401)
        status, _, result = self.request({"jsonrpc": "2.0", "id": 1, "method": "ping"},
                                        headers={"Authorization": "Bearer test-only-token"})
        self.assertEqual(status, 200)
        self.assertEqual(result["result"], {})


if __name__ == "__main__":
    unittest.main()
