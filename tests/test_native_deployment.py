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
import native_drain as drain
import native_ready as ready
import native_release as release


# The current native deployment has no dependency on retired migration units.
CURRENT_SERVICES = (
    "borg-board.service", "borg-workspace.service", "borg-session-workspace.service",
    "borg-native.service", "borg-fleet.service", "borg-recovery.service",
    "borg-board-tunnel.service", "borg-native-routes.service",
    "borg-native-controller-route.socket", "borg-native-controller-route.service",
    "borg-native-builder-route.socket", "borg-native-builder-route.service",
    "borg-pm-workspace.service",
)


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

    def test_inventory_only_observes_current_native_services(self):
        state = self.root / "state"
        state.mkdir()
        (state / "native.sqlite").touch()
        (state / "board.sqlite").touch()
        with mock.patch.object(release, "service_state", return_value={"ActiveState": "inactive"}) as service:
            observed = release.inventory(self.root)
        self.assertEqual([call.args[0] for call in service.call_args_list], list(CURRENT_SERVICES))
        self.assertEqual(list(observed["services"]), list(CURRENT_SERVICES))
        self.assertEqual(observed["databases"], ["board.sqlite", "native.sqlite"])

    def test_native_install_uses_current_checks_and_preserves_receipts(self):
        path = self.artifact()
        base = self.root / "persistent"
        (base / "config").mkdir(parents=True)
        (base / "config/runtime.json").write_text("{}")
        (base / "state").mkdir()
        database = base / "state/native.sqlite"
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE results (content TEXT)")
            db.execute("INSERT INTO results VALUES ('original tool receipt')")
        inode = database.stat().st_ino
        before = [{"id": "session-1", "agent_id": "engineer", "status": "stopped",
                   "tools": {"call-1": "succeeded"}, "receipt_hashes": {"4": "original"}}]
        args = argparse.Namespace(release=path, base=base, user="mtolton", host_id="agent-lab-2",
                                  workspace="/workspace", runtime_url="http://fixture-runtime",
                                  components="native", wait_seconds=1)
        events = []
        verify_receipts = release.verify_preserved
        verify_images = release.verify_running_images

        def current_service(name):
            self.assertIn(name, CURRENT_SERVICES, "activation must not query retired migration units")
            return {"ActiveState": "active", "UnitFileState": "enabled",
                    "executable_sha256": release.sha256(path / "borg-native")}

        def command(argv):
            # Systemd/install is the only host-mutation boundary; never execute it.
            if argv[-2:] == ["stop", "borg-fleet.service"]:
                events.append("stop-fleet")
            elif argv[-2:] == ["restart", "borg-native.service"]:
                events.append("restart-native")
            elif argv[-2:] == ["start", "borg-fleet.service"]:
                events.append("start-fleet")
            if "--property=ExecStart" in argv:
                return "path=" + str(path / "borg-native") + " ;"
            return ""

        def drain_session(db, url, journal, wait_seconds):
            self.assertEqual(db, database)
            self.assertEqual(url, args.runtime_url)
            events.append("drain")
            release.atomic_json(journal, {"state": "drained"})

        def probe_native(url):
            self.assertEqual(url, args.runtime_url)
            events.append("readiness")

        def preserved(old, new):
            verify_receipts(old, new)
            events.append("receipts")

        def images(snapshot, metadata, components):
            verify_images(snapshot, metadata, components)
            events.append("images")

        def resume_session(url, journal):
            self.assertEqual(url, args.runtime_url)
            self.assertEqual(json.loads(journal.read_text())["state"], "drained")
            events.append("resume")
            return {"state": "resumed"}

        with (
            mock.patch.object(release, "service_state", side_effect=current_service),
            mock.patch.object(release, "run", side_effect=command),
            mock.patch.object(release.shutil, "copy2"),
            mock.patch.object(release, "session_snapshot", return_value=before),
            mock.patch.object(release, "require_quiet", wraps=release.require_quiet) as quiet,
            mock.patch.object(release, "verify_preserved", side_effect=preserved),
            mock.patch.object(release, "verify_running_images", side_effect=images),
            mock.patch.object(drain, "drain", side_effect=drain_session),
            mock.patch.object(drain, "resume", side_effect=resume_session),
            mock.patch.object(ready, "probe_native", side_effect=probe_native),
            mock.patch.object(sys, "path", sys.path[:]),
        ):
            result = release.install(args)
        quiet.assert_called_once_with(before)
        self.assertEqual(events, ["stop-fleet", "drain", "restart-native", "readiness",
                                  "receipts", "images", "resume", "start-fleet", "images"])
        self.assertEqual(result["status"], "verified")
        self.assertTrue(result["committed_results_preserved"])
        self.assertEqual(result["before_sessions"], result["after_sessions"])
        self.assertEqual(json.loads(Path(result["evidence_path"]).read_text())["status"], "verified")
        self.assertEqual(database.stat().st_ino, inode)
        backup = base / "backups" / result["started_at"] / "native.sqlite"
        for db_path in (database, backup):
            with sqlite3.connect(db_path) as db:
                self.assertEqual(db.execute("SELECT content FROM results").fetchall(),
                                 [("original tool receipt",)])

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
