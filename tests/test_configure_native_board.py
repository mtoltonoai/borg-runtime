"""Metadata deployment is idempotent and reconciles missing write receipts."""
import copy
import importlib.util
import json
from pathlib import Path
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("configure_native_board", ROOT / "tooling/configure_native_board.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


def plan():
    return json.loads((ROOT / "config/native-board-profiles.json").read_text())


class FakeBoard:
    def __init__(self):
        self.roles = {}
        self.agents = {agent: {"id": agent, "metadata": {"keep": "original"}} for agent in plan()["agents"]}
        self.overrides = {}
        self.requests = []
        self.lose_next_write = False
        self.reject_next_write = False

    def request(self, method, url, body=None):
        self.requests.append((method, url, copy.deepcopy(body)))
        path = urllib.parse.urlsplit(url).path
        parts = [urllib.parse.unquote(p) for p in path.split('/') if p]
        if parts[1] == "roles":
            key = (parts[2], parts[4])
            entries = self.roles.setdefault(key, {})
            if method == "POST":
                if self.reject_next_write:
                    self.reject_next_write = False
                    raise config.ConfigError("rejected before mutation")
                value = {k: copy.deepcopy(v) for k, v in body.items() if k in ("enabled", "scope", "payload", "position")}
                value["id"] = body["entry_id"]
                entries[value["id"]] = {**entries.get(value["id"], {}), **value}
                if self.lose_next_write:
                    self.lose_next_write = False
                    raise config.ConfigError("response lost after commit")
            return {"entries": list(copy.deepcopy(entries).values()), "version": len(entries)}
        agent = parts[2]
        if len(parts) == 3:
            if method == "PATCH":
                self.agents[agent]["metadata"].update(copy.deepcopy(body["metadata"]))
            return copy.deepcopy(self.agents[agent])
        role = self.agents[agent]["metadata"].get("role")
        kind = parts[4]
        if method == "DELETE":
            self.overrides.get((agent, kind), {}).pop(parts[5], None)
            return {"removed": True}
        effective = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).get("effective") == ["true"]
        merged = copy.deepcopy(self.roles.get((role, kind), {})) if effective else {}
        merged.update(copy.deepcopy(self.overrides.get((agent, kind), {})))
        return {"entries": list(merged.values()), "role_version": len(merged), "agent_version": 0}

    def writes(self):
        return [r for r in self.requests if r[0] != "GET"]


class ConfigurationTests(unittest.TestCase):
    def test_plan_is_read_only_and_apply_then_verify_is_idempotent(self):
        board = FakeBoard()
        initial = config.configure(board, plan(), "plan")
        self.assertTrue(initial["ok"])
        self.assertEqual(board.writes(), [])
        self.assertEqual(len(initial["outcomes"]), 41)
        self.assertTrue(all(v["action"] == "would_update" for v in initial["outcomes"]))
        report = config.configure(board, plan(), "apply")
        self.assertTrue(report["ok"], report)
        self.assertEqual(len(board.writes()), 41)
        self.assertEqual(len(board.roles[("native-engineer", "tool")]), 4)
        self.assertEqual(board.agents["pm"]["metadata"]["keep"], "original")
        board.requests.clear()
        self.assertTrue(config.configure(board, plan(), "apply")["ok"])
        self.assertEqual(board.writes(), [])
        self.assertTrue(config.configure(board, plan(), "verify")["ok"])

    def test_verify_reports_missing_config_without_writes(self):
        board = FakeBoard()
        result = config.configure(board, plan(), "verify")
        self.assertFalse(result["ok"])
        self.assertEqual(board.writes(), [])

    def test_lost_write_ack_is_reconciled_not_replayed(self):
        board = FakeBoard()
        board.lose_next_write = True
        result = config.configure(board, plan(), "apply")
        self.assertTrue(result["ok"])
        self.assertEqual(result["outcomes"][0]["action"], "reconciled")
        first = [r for r in board.writes() if r[1] == '/api/roles/pm/config/runtime']
        self.assertEqual(len(first), 1)

    def test_rejected_mutation_is_not_claimed_success_or_repeated(self):
        board = FakeBoard()
        board.reject_next_write = True
        with self.assertRaisesRegex(config.ConfigError, "not confirmed"):
            config.configure(board, plan(), "apply")
        self.assertEqual(len(board.writes()), 1)

    def test_agent_override_and_unexpected_enabled_grant_are_not_silently_overridden(self):
        board = FakeBoard()
        config.configure(board, plan(), "apply")
        board.overrides[("pm", "tool")] = {"native": {"id": "native", "enabled": False}, "extra": {"id": "extra", "enabled": True}}
        board.requests.clear()
        report = config.configure(board, plan(), "apply")
        self.assertFalse(report["ok"])
        self.assertEqual(board.writes(), [])
        entry = next(v for v in report["outcomes"] if v["target"].endswith('/pm/config/tool?effective=true'))
        self.assertEqual(entry["missing_or_overridden"], ["native"])
        self.assertEqual(entry["unexpected_enabled"], ["extra"])

    def test_full_limits_and_unlimited_policy_cannot_be_lowered_even_in_overrides(self):
        for key, value in (("max_retries", 4294967295), ("max_output_tokens", 8192), ("context_window_tokens", 128000), ("max_turns", 5), ("model", "smaller")):
            candidate = plan()
            candidate["runtime_profile"][key] = value
            with self.subTest(key=key), self.assertRaises(config.ConfigError):
                config.validate(candidate)
            candidate = plan()
            candidate["agents"]["pm"]["metadata"] = {"native": {key: value}}
            with self.subTest(override=key), self.assertRaises(config.ConfigError):
                config.validate(candidate)

    def test_board_attribution_is_runtime_owned_not_a_per_identity_host_secret(self):
        for payload in ({"bound_agent_id": "pm"}, {"headers_env": {"X-Fleet-Agent": "BORG_AGENT_PM"}}, {"headers": {"Authorization": "not-a-real-secret"}}):
            candidate = plan()
            candidate["tools"][0]["payload"].update(payload)
            with self.subTest(payload=payload), self.assertRaises(config.ConfigError):
                config.validate(candidate)

    def test_credential_urls_and_missing_required_grants_fail_closed(self):
        candidate = plan()
        candidate["runtime_profile"]["provider_url"] = "https://user:password@example.test/responses"
        with self.assertRaises(config.ConfigError):
            config.validate(candidate)
        candidate = plan()
        candidate["tools"] = candidate["tools"][1:]
        with self.assertRaises(config.ConfigError):
            config.validate(candidate)

    def test_explicit_alias_cleanup_and_profile_inheritance_preserve_canonical_grants(self):
        board = FakeBoard()
        board.roles[("pm", "tool")] = {"web_search": {"id": "web_search", "enabled": True,
            "payload": {"kind": "provider", "tool": {"type": "web_search"}}, "position": 3, "scope": []}}
        board.overrides[("native-core", "runtime")] = {"profile": {"id": "profile", "enabled": True,
            "payload": plan()["agents"]["native-core"]["remove_runtime_profile_if"], "position": 1, "scope": None}}
        result = config.configure(board, plan(), "apply")
        self.assertTrue(result["ok"], result)
        self.assertFalse(board.roles[("pm", "tool")]["web_search"]["enabled"])
        self.assertTrue(board.roles[("pm", "tool")]["web"]["enabled"])
        self.assertNotIn("profile", board.overrides[("native-core", "runtime")])
        self.assertEqual(board.roles[("pm", "tool")]["native"]["payload"]["allowed_agent_ids"], ["*"])
        self.assertNotIn("allowed_agent_ids", board.roles[("native-engineer", "tool")]["native"]["payload"])

    def test_cleanup_does_not_clobber_concurrently_changed_metadata(self):
        board = FakeBoard()
        board.roles[("pm", "tool")] = {"web_search": {"id": "web_search", "enabled": True, "payload": {"different": True}}}
        with self.assertRaisesRegex(config.ConfigError, "unrelated grant"):
            config.configure(board, plan(), "apply")
        self.assertEqual(board.writes(), [])

    def test_invalid_scope_not_treated_as_unscoped(self):
        wanted = {"enabled": True, "payload": {}, "position": 1}
        for bad in (False, 0, {}, ["write"]):
            self.assertFalse(config.equal_entry({**wanted, "scope": bad}, wanted))
        self.assertTrue(config.equal_entry({**wanted, "scope": []}, wanted))
        self.assertTrue(config.equal_entry({**wanted, "scope": None}, wanted))


class RealHTTPTests(unittest.TestCase):
    def test_actual_rest_body_and_lost_reply_reconciliation(self):
        backend = FakeBoard()
        lost = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def perform(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length']))) if self.headers.get('Content-Length') else None
                value = backend.request(self.command, self.path, body)
                if self.command == 'POST' and not lost:
                    lost.append(self.path)
                    self.close_connection = True  # Commit happened; no response arrives.
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(value).encode())

            do_GET = do_POST = do_PATCH = do_DELETE = perform

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.shutdown)
        client = config.Board(f'http://127.0.0.1:{server.server_port}')
        report = config.configure(client, plan(), 'apply')
        self.assertTrue(report['ok'])
        self.assertEqual(report['outcomes'][0]['action'], 'reconciled')
        self.assertEqual(len([r for r in backend.writes() if r[1] == lost[0]]), 1)
        self.assertTrue(config.configure(client, plan(), 'verify')['ok'])


if __name__ == '__main__':
    unittest.main()
