"""Operational acceptance must fail closed; test HTTP and saved receipt boundaries."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SPEC = importlib.util.spec_from_file_location("native_acceptance", Path(__file__).resolve().parents[1] / "tooling/native_acceptance.py")
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)
NOW = 1792464000
REV = "a" * 40


def fixture():
    snapshot = {"schema_version": 1, "collected_at": NOW, "finished_at": NOW, "project_id": 1, "errors": [],
                "board": {"health": {"ok": True, "db": True, "commit": REV}, "hosts": [], "dispatches": [],
                          "agents": [{"id": "engineer", "lifecycle_intent": "run", "metadata": {"harness": "native"}}], "tasks": []},
                "runtimes": {}}
    for number, host in enumerate(audit.HOSTS, 1):
        sid = f"session-{number}"
        path = f"/sessions/{sid}"
        profile = {"id": "engineer", "model": "gpt-6-astra", "reasoning_effort": "max", "max_output_tokens": 128000,
                   "context_window_tokens": 1050000, "max_turns": None, "max_retries": None}
        session = {"id": sid, "agent_id": "engineer", "task": f"task_{number}", "status": "running", "driver_running": True,
                   "phase": "awaiting-model", "last_progress_at": NOW * 1000, "step": 10,
                   "placement": {"host_id": host, "workspace_cwd": path, "generation": 1}}
        snapshot["board"]["hosts"].append({"host_id": host, "capacity": 3, "revision": REV, "last_seen": NOW, "runtime_ready": True, "enabled": True, "fresh": True, "ttl_seconds": 90})
        snapshot["board"]["dispatches"].append({"request_id": f"task:{number}:attempt:1", "task_id": number,
            "agent_id": "engineer", "host_id": host, "session_id": sid, "workspace_cwd": path,
            "generation": 1, "status": "running", "updated_at": NOW})
        snapshot["board"]["tasks"].append({"id": number, "assignee": "engineer", "status": "in_progress"})
        snapshot["runtimes"][host] = {"health": {"status": "ok", "host_id": host, "revision": REV, "instance_id": f"old-{host}"},
            "agents": [profile], "sessions": [session], "tools": {sid: [{"call_id": "call-1", "operation_id": f"{sid}:call-1",
                "name": "workspace__workspace_run", "status": "succeeded", "result_sha256": audit.digest({"exit_code": 0})}]}}
    return snapshot


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = fixture()

    def checks(self, snapshot=None):
        return {v["check"]: v for v in audit.audit(snapshot or self.snapshot, revision=REV, now=NOW)["checks"]}

    def fail(self, name):
        self.assertEqual(self.checks()[name]["status"], "fail")

    def test_complete_operational_snapshot_passes_but_does_not_claim_live_demos(self):
        result = audit.audit(self.snapshot, revision=REV, now=NOW)
        self.assertTrue(result["ok"], result)
        self.assertGreaterEqual(len(result["demonstrations_required"]), 8)
        self.assertIn("live demonstrations still required", result["scope"])

    def test_absent_fleet_cannot_pass_as_bootstrap_health(self):
        self.snapshot["board"]["hosts"] = []
        self.snapshot["board"]["dispatches"] = []
        self.fail("host:agent-lab")
        self.fail("four_host_dispatch")

    def test_health_without_database_or_host_identity_fails(self):
        self.snapshot["board"]["health"] = {"ok": True}
        self.fail("board_ready")
        self.snapshot["runtimes"]["agent-lab"]["health"].pop("host_id")
        self.fail("host:agent-lab")

    def test_stale_and_future_heartbeats_fail(self):
        for value in (NOW - 91, NOW + 31, None, True, float("nan")):
            with self.subTest(value=value):
                self.snapshot["board"]["hosts"][0]["last_seen"] = value
                self.fail("host:agent-lab")

    def test_saved_stale_snapshot_is_not_current_readiness(self):
        self.snapshot["finished_at"] = NOW - 100
        self.fail("snapshot_fresh")

    def test_missing_lowered_limits_and_finite_retry_fail(self):
        profile = self.snapshot["runtimes"]["agent-lab"]["agents"][0]
        profile["context_window_tokens"] = 128000
        profile["max_retries"] = 4294967295
        self.fail("profiles:agent-lab")
        self.fail("indefinite_retry:agent-lab")
        profile["context_window_tokens"] = 1050000
        profile.pop("max_turns")
        self.fail("profiles:agent-lab")

    def test_missing_all_profiles_cannot_vacuously_pass(self):
        self.snapshot["board"]["agents"] = []
        self.fail("native_identities")
        self.fail("profiles:agent-lab")

    def test_live_slow_inference_is_not_failed_by_progress_age(self):
        session = self.snapshot["runtimes"]["agent-lab"]["sessions"][0]
        session["last_progress_at"] = 1
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")

    def test_retrying_live_driver_remains_owned(self):
        self.snapshot["runtimes"]["agent-lab"]["sessions"][0]["status"] = "retrying"
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")

    def test_failed_unknown_and_absent_driver_do_not_cover_work(self):
        session = self.snapshot["runtimes"]["agent-lab"]["sessions"][0]
        for status in ("failed", "needs_resolution", "completed", "stopped"):
            session["status"] = status
            self.fail("actionable_task_coverage")
        session["status"] = "running"
        session.pop("driver_running")
        self.fail("actionable_task_coverage")

    def test_parent_session_covers_assigned_child_not_other_owner(self):
        self.snapshot["board"]["tasks"].append({"id": 9, "parent_id": 1, "assignee": "engineer", "status": "in_progress"})
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")
        self.snapshot["board"]["tasks"][-1]["assignee"] = "not-registered"
        self.fail("actionable_task_coverage")

    def test_assignee_online_or_monitor_exempt_does_not_hide_orphan(self):
        self.snapshot["board"]["tasks"].append({"id": 9, "assignee": "engineer", "status": "todo", "monitor_exempt": True})
        self.fail("actionable_task_coverage")

    def test_owned_repair_must_have_live_path_and_no_cycles(self):
        self.snapshot["runtimes"]["agent-lab"]["sessions"][0]["status"] = "failed"
        task = self.snapshot["board"]["tasks"][0]
        task["metadata"] = {"recovery_task_id": 2}
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")
        self.snapshot["runtimes"]["agent-lab-2"]["sessions"][0]["status"] = "failed"
        self.snapshot["board"]["tasks"][1]["metadata"] = {"recovery_task_id": 1}
        self.fail("actionable_task_coverage")

    def test_blocked_needs_visible_reason_and_owner(self):
        task = self.snapshot["board"]["tasks"][0]
        task.update(status="blocked", blocked_on={"kind": "external"})
        self.fail("actionable_task_coverage")
        task["blocked_note_present"] = True
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")
        task["assignee"] = None
        self.fail("actionable_task_coverage")

    def test_explicit_pause_is_visible_not_a_fake_live_driver(self):
        self.snapshot["board"]["agents"][0]["lifecycle_intent"] = "paused"
        self.snapshot["runtimes"]["agent-lab"]["sessions"][0]["driver_running"] = False
        coverage = self.checks()["actionable_task_coverage"]
        self.assertEqual(coverage["details"]["1"], "explicitly_paused_owner")

    def test_fresh_dispatch_covers_pending_only_until_stale(self):
        self.snapshot["runtimes"]["agent-lab"]["sessions"][0]["driver_running"] = False
        dispatch = self.snapshot["board"]["dispatches"][0]
        dispatch["status"] = "reserved"
        self.assertEqual(self.checks()["actionable_task_coverage"]["status"], "pass")
        dispatch["updated_at"] = NOW - 91
        self.fail("actionable_task_coverage")

    def test_duplicate_request_and_live_host_owners_fail(self):
        self.snapshot["board"]["dispatches"].append(copy.deepcopy(self.snapshot["board"]["dispatches"][0]))
        self.fail("dispatch_idempotency")
        self.snapshot["runtimes"]["agent-lab-2"]["sessions"].append(copy.deepcopy(self.snapshot["runtimes"]["agent-lab"]["sessions"][0]))
        self.fail("single_live_session_owner")

    def test_capacity_zero_bool_or_overbooked_fails(self):
        for capacity in (0, True, -1, "3"):
            self.snapshot["board"]["hosts"][0]["capacity"] = capacity
            self.fail("capacity:agent-lab")
        self.snapshot["board"]["hosts"][0]["capacity"] = 1
        extra = copy.deepcopy(self.snapshot["board"]["dispatches"][0])
        extra["request_id"] = "extra"
        self.snapshot["board"]["dispatches"].append(extra)
        self.fail("capacity:agent-lab")

    def test_wrong_agent_task_generation_and_workspace_binding_fail(self):
        session = self.snapshot["runtimes"]["agent-lab"]["sessions"][0]
        for field, bad in (("agent_id", "other"), ("task", "task_90")):
            old = session[field]
            session[field] = bad
            self.fail("session_placement_binding")
            session[field] = old
        for field, bad in (("generation", 2), ("host_id", "agent-lab-2"), ("workspace_cwd", "/sessions/../shared")):
            old = session["placement"][field]
            session["placement"][field] = bad
            self.fail("session_placement_binding")
            session["placement"][field] = old

    def test_same_host_nested_workspace_roots_are_not_isolation(self):
        parent = self.snapshot["runtimes"]["agent-lab"]["sessions"][0]
        child = copy.deepcopy(parent)
        child["id"] = "nested"
        child["placement"]["workspace_cwd"] += "/subdir"
        self.snapshot["runtimes"]["agent-lab"]["sessions"].append(child)
        dispatch = copy.deepcopy(self.snapshot["board"]["dispatches"][0])
        dispatch.update(request_id="nested", session_id="nested", workspace_cwd=child["placement"]["workspace_cwd"])
        self.snapshot["board"]["dispatches"].append(dispatch)
        self.fail("session_placement_binding")

    def test_false_runtime_readiness_and_duplicate_host_registry_fail(self):
        self.snapshot["board"]["hosts"][0]["runtime_ready"] = False
        self.fail("host:agent-lab")
        self.snapshot["board"]["hosts"].append(copy.deepcopy(self.snapshot["board"]["hosts"][1]))
        self.fail("unique_hosts")

    def test_full_expected_revision_required(self):
        result = audit.audit(self.snapshot, now=NOW)
        self.assertFalse(result["ok"])
        self.assertEqual(next(c for c in result["checks"] if c["check"] == "expected_revision")["status"], "fail")

    def test_epoch_seconds_milliseconds_iso_and_task_refs(self):
        self.assertEqual(audit.epoch(NOW), NOW)
        self.assertEqual(audit.epoch(NOW * 1000), NOW)
        self.assertEqual(audit.epoch("2026-10-20T02:40:00Z"), NOW)
        for value in (1, "task_1", "task:1", "#1"):
            self.assertEqual(audit.task_id(value), 1)
        for value in (True, "task_0", "arbitrary", None):
            self.assertIsNone(audit.task_id(value))

    def test_restart_requires_real_process_change_and_nonempty_custody(self):
        result = audit.compare_restart(self.snapshot, self.snapshot)
        self.assertFalse(result["ok"])
        after = copy.deepcopy(self.snapshot)
        for runtime in after["runtimes"].values():
            runtime["health"]["instance_id"] += "-new"
        result = audit.compare_restart(self.snapshot, after)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["preserved_completed_tools"], 4)

    def test_restart_lost_changed_or_duplicate_result_fails(self):
        for change in ("lost", "changed", "duplicate", "missing-session", "missing-catalog"):
            with self.subTest(change=change):
                after = copy.deepcopy(self.snapshot)
                for runtime in after["runtimes"].values():
                    runtime["health"]["instance_id"] += "new"
                runtime = after["runtimes"]["agent-lab"]
                calls = runtime["tools"]["session-1"]
                if change == "lost":
                    calls.clear()
                elif change == "changed":
                    calls[0]["result_sha256"] = "different"
                elif change == "duplicate":
                    calls.append(copy.deepcopy(calls[0]))
                elif change == "missing-session":
                    runtime["sessions"] = []
                else:
                    runtime["tools"].clear()
                self.assertFalse(audit.compare_restart(self.snapshot, after)["ok"])


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.routes = {}
        self.requests = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                outer.requests.append((self.command, self.path))
                content_type, payload, status = outer.routes.get(self.path, ("application/json", {"error": "not found"}, 404))
                raw = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)
        self.addCleanup(self.server.shutdown)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def route(self, path, value):
        self.routes[path] = ("application/json", value, 200)

    def test_html_200_is_not_json_health_and_no_body_secrets_leak(self):
        self.routes["/health"] = ("text/html", "secret-body", 200)
        with self.assertRaisesRegex(audit.ReadError, "expected application/json"):
            audit.Reader().get(self.base, "/health")
        self.routes["/health"] = ("application/json", {"error": "secret-body"}, 401)
        with self.assertRaisesRegex(audit.ReadError, "HTTP 401") as got:
            audit.Reader().get(self.base, "/health")
        self.assertNotIn("secret-body", str(got.exception))

    def test_invalid_json_and_oversized_response_fail_closed(self):
        self.routes["/bad"] = ("application/json", "not-json", 200)
        with self.assertRaisesRegex(audit.ReadError, "invalid JSON"):
            audit.Reader().get(self.base, "/bad")
        self.route("/big", {"x": "x" * 100})
        with self.assertRaisesRegex(audit.ReadError, "size bound"):
            audit.Reader(max_bytes=50).get(self.base, "/big")

    def test_collector_is_get_only_and_redacts_private_payloads(self):
        self.route("/api/health", {"ok": True, "db": True, "commit": REV})
        self.route("/api/tasks?project_id=1", [{"id": 1, "description": "PRIVATE", "metadata": {"password": "PRIVATE"}}])
        self.route("/api/agents?limit=1000&offset=0", [{"id": "engineer", "charter": "PRIVATE", "metadata": {"api_key": "PRIVATE", "harness": "native"}}])
        self.route("/api/fleet/hosts", {"hosts": []})
        self.route("/api/fleet/dispatches?limit=1000&offset=0", [])
        self.route("/health", {"status": "ok", "authorization": "PRIVATE"})
        self.route("/api/agents", [{"id": "engineer", "instructions": "PRIVATE", "api_key_env": "PRIVATE"}])
        self.route("/api/sessions", [{"id": "sid", "agent_id": "engineer", "error": "PRIVATE"}])
        self.route("/api/sessions/sid/tools", [{"call_id": "c", "operation_id": "o", "name": "workspace__workspace_run", "status": "succeeded", "arguments": {"secret": "PRIVATE"}, "result": {"output": "PRIVATE"}}])
        result = audit.collect(self.base, {"lab": self.base})
        self.assertEqual(result["errors"], [])
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertEqual(result["runtimes"]["lab"]["tools"]["sid"][0]["result_sha256"], audit.digest({"output": "PRIVATE"}))
        self.assertTrue(all(method == "GET" for method, path in self.requests))

    def test_url_fields_cannot_retain_query_or_userinfo_credentials(self):
        result = audit.sanitize_session({"id": "sid", "placement": {
            "workspace_mcp_url": "http://user:PRIVATE@[::1]:8791/mcp/sid?token=PRIVATE#PRIVATE"}})
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertEqual(result["placement"]["workspace_mcp_url"], "http://[::1]:8791/mcp/sid")

    def test_dispatches_page_past_default_limit_and_detect_ignored_offset(self):
        first = [{"request_id": f"request-{i}"} for i in range(1000)]
        self.route("/api/fleet/dispatches?limit=1000&offset=0", first)
        self.route("/api/fleet/dispatches?limit=1000&offset=1000", [{"request_id": "last"}])
        result = audit.collect(self.base, {})
        self.assertEqual(len(result["board"]["dispatches"]), 1001)
        self.assertEqual(result["board"]["dispatches"][-1]["request_id"], "last")
        self.route("/api/fleet/dispatches?limit=1000&offset=1000", first)
        result = audit.collect(self.base, {})
        self.assertIsNone(result["board"]["dispatches"])
        self.assertIn("ignored pagination", next(e["error"] for e in result["errors"] if e["source"] == "board.dispatches"))

    def test_auth_bearing_base_urls_rejected(self):
        for url in ("http://user:password@localhost", "http://localhost?token=secret", "file:///tmp/foo", "http://localhost#secret"):
            with self.assertRaises(audit.ReadError):
                audit.Reader.base(url)

    def test_missing_endpoints_are_recorded_not_silently_ignored(self):
        result = audit.collect(self.base, {"lab": self.base})
        self.assertGreater(len(result["errors"]), 5)
        self.assertFalse(audit.audit(result, now=result["finished_at"])["ok"])


if __name__ == "__main__":
    unittest.main()
