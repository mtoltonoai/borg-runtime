import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tooling"))
import native_metadata as metadata


class MetadataTests(unittest.TestCase):
    def test_profile_preserves_full_limits_and_explicit_unlimited(self):
        p = metadata.PROFILE
        self.assertEqual((p["model"], p["reasoning_effort"]), ("gpt-6-astra", "max"))
        self.assertEqual((p["max_output_tokens"], p["context_window_tokens"]), (128000, 1050000))
        self.assertIsNone(p["max_turns"])
        self.assertIsNone(p["max_retries"])
        self.assertEqual(p["retry_max_ms"], 60000)

    def test_new_identity_role_grants_all_required_bound_tools_without_env_hacks(self):
        entries = metadata.role_entries(["get_task", "comment_task"])
        self.assertEqual({e["entry_id"] for e in entries["tool"]}, {"board", "workspace", "native", "web"})
        self.assertNotIn("headers_env", entries["tool"][0]["payload"])
        self.assertEqual(entries["tool"][1]["payload"]["url"], "http://127.0.0.1:8791/mcp/{session_id}")
        self.assertEqual(entries["tool"][2]["payload"]["url"], "{runtime_url}/mcp/sessions/{session_id}")

    def test_render_contains_infrastructure_not_an_identity_catalog(self):
        config = metadata.runtime_config("/state", "agent-lab-4", "http://127.0.0.1:17874")
        self.assertEqual(config["agents"], [])
        self.assertEqual(config["database_path"], "/state/state/native.sqlite")
        self.assertEqual(config["board"]["url"], "http://127.0.0.1:8079")
        self.assertNotIn("api_key", json.dumps(config))

    def test_runtime_origin_is_not_fleet_api_url_or_credential_carrier(self):
        for url in ("http://127.0.0.1:17874/api", "http://user:secret@host", "file:///tmp/runtime"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                metadata.runtime_config("/state", "host", url)

    def test_seed_is_idempotent_including_order_and_enabled_state(self):
        store = {}
        posts = []
        def call(base, path, body=None):
            current = store.setdefault(path, {"version": 0, "entries": []})
            if body is not None:
                posts.append(body)
                current["entries"] = [e for e in current["entries"] if e["id"] != body["entry_id"]]
                current["entries"].append({"id": body["entry_id"], **{k: body[k] for k in ("enabled", "scope", "payload", "position")}})
                current["version"] += 1
            return current
        entries = metadata.role_entries()
        first = metadata.seed("http://board/api", ["test-role"], entries, call)
        self.assertEqual(len(posts), 5)
        self.assertTrue(all(e["changed"] for e in first))
        second = metadata.seed("http://board/api", ["test-role"], entries, call)
        self.assertEqual(len(posts), 5)
        self.assertTrue(all(not e["changed"] for e in second))

    def test_seeding_does_not_reenable_operator_withheld_grant(self):
        calls = []
        def call(base, path, body=None):
            if body is not None:
                calls.append(body)
            return {"version": 9, "entries": [{"id": "board", "enabled": False, "payload": {}, "scope": None, "position": 0}]}
        evidence = metadata.seed("http://board/api", ["role"], {"tool": metadata.role_entries()["tool"][:1]}, call)
        self.assertEqual(calls, [])
        self.assertTrue(evidence[0]["preserved_existing"])


if __name__ == "__main__":
    unittest.main()
