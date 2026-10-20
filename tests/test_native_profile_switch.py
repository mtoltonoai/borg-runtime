import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tooling"))
import native_profile_switch as switch


class ProfileSwitchTests(unittest.TestCase):
    def test_builder_self_route_is_separate_socket_not_a_tunnel_restart(self):
        units = switch.builder_route_units("/lib/systemd/systemd-socket-proxyd")
        self.assertIn("ListenStream=127.0.0.1:17870", units["borg-native-builder-route.socket"])
        self.assertIn("127.0.0.1:8787", units["borg-native-builder-route.service"])
        self.assertNotIn("borg-board-tunnel", json.dumps(units))

    def test_advertised_origin_must_reach_same_session_database(self):
        args = argparse.Namespace(host_id="agent-lab-2", runtime_origin="http://127.0.0.1:17872")
        import native_ready
        with mock.patch.object(native_ready, "wait_ready"), mock.patch.object(switch, "get_json", return_value=[{"id": "another-host-session"}]):
            with self.assertRaisesRegex(RuntimeError, "another session database"):
                switch.ensure_origin(args, [{"id": "local-session"}])

    def test_prepare_refuses_active_actor_before_any_configuration_write(self):
        args = argparse.Namespace()
        with mock.patch.object(switch, "session_snapshot", return_value=[{"id": "s", "status": "running", "tools": {}}]):
            with self.assertRaisesRegex(RuntimeError, "refusing activation"):
                switch.prepare(args)

    def test_bind_reconciles_lost_reply_and_preserves_previous_config(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            (base / "config").mkdir()
            journal = base / "config/profile-switch-t.json"
            options = {"session_id": "s", "host_id": "agent-lab-2", "workspace_cwd": temp,
                       "workspace_mcp_url": "http://127.0.0.1:8766/mcp", "lifecycle_mcp_url": "http://127.0.0.1:17872/mcp/sessions/s", "generation": 0}
            previous = {"agents": [{"id": "engineer"}]}
            journal.write_text(json.dumps({"previous_config": previous, "bindings": [{"id": "s", "agent_id": "engineer", "options": options}], "bound": []}))
            args = argparse.Namespace(base=base, release=base, host_id="agent-lab-2", transaction="t")
            with mock.patch.object(switch, "session_snapshot", return_value=[]), mock.patch.object(switch, "request", side_effect=[OSError("reply lost"), options]) as request:
                result = switch.bind(args)
            self.assertEqual(result["state"], "bound")
            self.assertEqual(request.call_count, 2)
            self.assertEqual(request.call_args_list[0].args[0], "/sessions/s/bind")
            self.assertEqual(request.call_args_list[1].args[0], "/sessions/s")
            self.assertEqual(json.loads(journal.read_text())["previous_config"], previous)


if __name__ == "__main__":
    unittest.main()
