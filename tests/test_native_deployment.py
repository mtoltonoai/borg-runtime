"""Deployment safety and real read-only HTTP boundary tests."""
import importlib.util
import argparse
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

TOOLING = Path(__file__).resolve().parents[1] / "tooling"
sys.path.insert(0, str(TOOLING))
import native_ready as ready
import native_release as release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def artifact(self):
        path = self.root / ("a" * 40)
        path.mkdir()
        for name in ("borg-native", "fleet", "task-board", "workspace_mcp.py",
                     "native_ready.py", "native_release.py", "web/index.html"):
            target = path / name
            target.parent.mkdir(exist_ok=True)
            target.write_text("content of " + name)
        release.manifest(path, "a" * 40)
        return path

    def test_manifest_verifies_exact_files_and_revision(self):
        path = self.artifact()
        self.assertEqual(release.verify(path)["revision"], "a" * 40)
        (path / "fleet").write_text("different executable")
        with self.assertRaisesRegex(ValueError, "checksums"):
            release.verify(path)

    def test_manifest_rejects_extra_secret_or_stale_file(self):
        path = self.artifact()
        (path / "native.env").write_text("do not package credential files")
        with self.assertRaisesRegex(ValueError, "checksums"):
            release.verify(path)

    def test_manifest_rejects_symlink(self):
        path = self.artifact()
        (path / "outside").symlink_to(self.root)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            release.verify(path)

    def test_full_git_revision_required(self):
        with self.assertRaisesRegex(ValueError, "full Git commit"):
            release.manifest(self.root, "main")

    def test_native_only_manifest_requires_no_unrelated_build_outputs(self):
        path = self.root / ("b" * 40)
        path.mkdir()
        for name in ("borg-native", "native_ready.py", "native_release.py", "native_drain.py"):
            (path / name).write_text(name)
        release.manifest(path, "b" * 40, ["native"])
        value = release.verify(path)
        self.assertEqual(value["components"], ["native"])
        self.assertNotIn("fleet", value["files"])
        self.assertNotIn("task-board", value["files"])

    def test_actual_image_mismatch_is_rejected_before_admission(self):
        metadata = {"files": {"borg-native": "expected"}}
        snapshot = {"services": {"borg-native.service": {
            "ActiveState": "active", "UnitFileState": "enabled", "executable_sha256": "wrong"}}}
        with self.assertRaisesRegex(RuntimeError, "differs from release"):
            release.verify_running_images(snapshot, metadata, ["native"])
        snapshot["services"]["borg-native.service"]["executable_sha256"] = "expected"
        release.verify_running_images(snapshot, metadata, ["native"])

    def test_missing_component_not_a_deliverable(self):
        path = self.artifact()
        (path / "fleet").unlink()
        release.manifest(path, "a" * 40)
        with self.assertRaisesRegex(ValueError, "missing fleet"):
            release.verify(path)

    def test_atomic_json_round_trip_no_temporary_left(self):
        path = self.root / "nested" / "receipt.json"
        release.atomic_json(path, {"first": 1})
        release.atomic_json(path, {"second": 2})
        self.assertEqual(json.loads(path.read_text()), {"second": 2})
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_online_backup_includes_wal_and_never_replaces_source(self):
        state = self.root / "state"
        state.mkdir()
        source = state / "native.sqlite"
        with sqlite3.connect(source) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE results (id INTEGER, content TEXT)")
            db.execute("INSERT INTO results VALUES (1,'committed tool receipt')")
            db.commit()
            inode = source.stat().st_ino
            backup = self.root / "backup"
            evidence = release.database_backup(self.root, backup)
            with sqlite3.connect(backup / "native.sqlite") as copy:
                self.assertEqual(copy.execute("SELECT content FROM results").fetchone()[0],
                                 "committed tool receipt")
            self.assertEqual(source.stat().st_ino, inode)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM results").fetchone()[0], 1)
        self.assertEqual(evidence[0]["integrity"], "ok")
        self.assertEqual((backup / "native.sqlite").stat().st_mode & 0o777, 0o600)

    def test_native_unit_orders_real_dependency_probes(self):
        unit = release.unit("native", Path("/release/" + "a" * 40), Path("/state"), "agent-lab-2")
        self.assertIn("borg-board-tunnel.service", unit)
        self.assertIn("borg-session-workspace.service", unit)
        self.assertIn("--board-api http://127.0.0.1:8079/api", unit)
        self.assertIn("--workspace-url http://127.0.0.1:8766/mcp", unit)
        self.assertIn("Restart=always", unit)
        self.assertIn("StartLimitIntervalSec=0", unit)
        self.assertIn("EnvironmentFile=/state/config/native.env", unit)
        self.assertNotIn("OPENAI_API_KEY=", unit)

    def test_board_host_dependency_does_not_start_second_board(self):
        worker = release.unit("fleet", Path("/release/abc"), Path("/state"), "agent-lab-4")
        controller = release.unit("fleet", Path("/release/abc"), Path("/state"), "agent-lab")
        self.assertNotIn("borg-board.service", worker)
        self.assertIn("borg-board.service", controller)

    def test_monitor_only_update_does_not_stop_unselected_fleet(self):
        self.assertFalse(release.pauses_fleet(["recovery"]))
        self.assertTrue(release.pauses_fleet(["native"]))
        self.assertTrue(release.pauses_fleet(["fleet", "recovery"]))

    def test_recovery_starts_even_when_watched_services_are_down(self):
        text = release.unit("recovery", Path("/release/abc"), Path("/state"), "agent-lab")
        self.assertNotIn("ExecStartPre", text)
        self.assertNotIn("borg-native.service", text)
        self.assertNotIn("borg-board.service", text)
        self.assertIn("After=network-online.target", text)
        self.assertIn("Restart=always", text)

    def test_session_workspace_is_separate_from_bootstrap(self):
        unit = release.unit("session-workspace", Path("/release/abc"), Path("/state"), "agent-lab")
        self.assertIn("--session-root /state/workspaces --port 8791", unit)
        self.assertNotIn("--root ", unit)

    def test_detached_launch_is_nonblocking_and_survives_calling_tool(self):
        path = self.artifact()
        args = argparse.Namespace(release=path, base=self.root, user="mtolton", host_id="agent-lab-2",
                                  workspace="/workspace", runtime_url="http://127.0.0.1:8787",
                                  components="native", wait_seconds=600)
        with mock.patch.object(release, "run", return_value="") as run:
            result = release.launch(args)
        command = run.call_args.args[0]
        self.assertIn("--no-block", command)
        self.assertIn("--property=TimeoutStartSec=infinity", command)
        self.assertEqual(command[:3], ["sudo", "-n", "systemd-run"])
        self.assertTrue(result["launched"])

    def test_unit_rejects_systemd_injection(self):
        with self.assertRaises(ValueError):
            release.unit("native", Path("/release/abc"), Path("/state"), "host\nExecStart=/bad")
        with self.assertRaises(ValueError):
            release.unit("native", Path("/release/%n"), Path("/state"), "host")


class DrainSafetyTests(unittest.TestCase):
    def session(self, state="completed", tools=None, hashes=None):
        return {"id": "session-1", "status": state, "tools": tools or {},
                "receipt_hashes": hashes or {}}

    def test_all_active_states_refuse_without_stop_or_cancel(self):
        for state in ("running", "ready", "retrying", "new-state-not-recognized"):
            with self.subTest(state=state), self.assertRaisesRegex(RuntimeError, "refusing activation"):
                release.require_quiet([self.session(state)])

    def test_unknown_tool_refuses_even_when_session_terminal(self):
        for state in ("unknown", "running"):
            with self.subTest(state=state), self.assertRaisesRegex(RuntimeError, "unresolved-tool"):
                release.require_quiet([self.session("failed", {"call": state})])

    def test_completed_receipts_allow_quiet_restart(self):
        release.require_quiet([self.session(tools={"call": "succeeded"})])

    def test_receipt_preservation_accepts_appends_but_not_mutation(self):
        before = [self.session(tools={"a": "succeeded"}, hashes={"4": "hash"})]
        after = [self.session(tools={"a": "succeeded", "b": "succeeded"}, hashes={"4": "hash", "7": "new"})]
        release.verify_preserved(before, after)
        after[0]["receipt_hashes"]["4"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "committed tool result changed"):
            release.verify_preserved(before, after)

    def test_receipt_preservation_detects_missing_session(self):
        with self.assertRaisesRegex(RuntimeError, "disappeared"):
            release.verify_preserved([self.session()], [])


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.failures = 0
        self.spa = False
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, value, code=200):
                raw = json.dumps(value).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                outer.calls.append(self.path)
                if outer.failures:
                    outer.failures -= 1
                    self.reply({"error": "temporarily unavailable"}, 503)
                elif self.path == "/api/projects":
                    self.reply("<html>SPA is not readiness</html>" if outer.spa else [{"id": 1}])
                elif self.path == "/health":
                    self.reply({"service": "borg-native", "status": "ok"})
                else:
                    self.reply({}, 404)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.calls.append(body["method"])
                if body["method"] == "initialize":
                    self.reply({"jsonrpc": "2.0", "id": body["id"], "result": {
                        "protocolVersion": "2025-03-26", "capabilities": {}}})
                elif body["method"] == "tools/list":
                    self.reply({"jsonrpc": "2.0", "id": body["id"], "result": {
                        "tools": [{"name": "workspace_run"}]}})
                else:
                    self.reply({"error": "mutating method must never be called"}, 400)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:" + str(self.server.server_port)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_real_mcp_discovery_never_executes_a_tool(self):
        ready.probe_workspace(self.base + "/mcp")
        self.assertEqual(self.calls, ["initialize", "tools/list"])

    def test_html_success_cannot_satisfy_board_readiness(self):
        self.spa = True
        with self.assertRaises(ValueError):
            ready.probe_board(self.base + "/api")

    def test_temporarily_unready_read_recovers_without_a_write(self):
        self.failures = 2
        ready.wait_ready([("board", lambda: ready.probe_board(self.base + "/api"))],
                         wait_seconds=2, interval=0.01)
        self.assertEqual(self.calls, ["/api/projects"] * 3)

    def test_bounded_startup_probe_is_nonzero_not_false_health(self):
        self.failures = 5
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            ready.wait_ready([("board", lambda: ready.probe_board(self.base + "/api"))],
                             wait_seconds=0)

    def test_native_health_checks_service_identity(self):
        ready.probe_native(self.base)
        with mock.patch.object(ready, "request_json", return_value=({"status": "ok", "service": "other"}, None)):
            with self.assertRaises(ValueError):
                ready.probe_native(self.base)


if __name__ == "__main__":
    unittest.main()
