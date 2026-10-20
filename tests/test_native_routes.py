#!/usr/bin/env python3
"""Deployment route generation tests; no service activation or credentials."""
import importlib.util
import json
from pathlib import Path
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("native_routes", ROOT / "tooling/native_routes.py")
routes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(routes)


class NativeRouteTests(unittest.TestCase):
    def test_all_host_configs_join_origins_api_and_session_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = routes.render(tmp)
            self.assertFalse(result["activation_performed"])
            for host, port in routes.PORTS.items():
                folder = Path(tmp) / host
                cfg = tomllib.loads((folder / "native-fleet.toml").read_text())
                patch = json.loads((folder / "runtime-patch.json").read_text())
                self.assertEqual(cfg["host"], host)
                self.assertEqual(patch["host_id"], host)
                self.assertEqual(patch["runtime_url"], f"http://127.0.0.1:{port}")
                self.assertEqual(cfg["native"]["runtime_url"], patch["runtime_url"] + "/api")
                self.assertEqual(cfg["board_api"], patch["board"]["url"] + "/api")
                self.assertIn("{session_id}", cfg["native"]["workspace_mcp_url"])
                self.assertNotIn("project_id", cfg["native"], "shared fleet must admit metadata-defined new projects")
                for key in ("agents", "model", "api_key", "max_turns", "max_output_tokens"):
                    self.assertNotIn(key, cfg["native"])
                    self.assertNotIn(key, patch)

    def test_optional_project_filter_is_only_an_explicit_scoped_choice(self):
        cfg = tomllib.loads(routes.fleet_config("agent-lab-2", project=17))
        self.assertEqual(cfg["native"]["project_id"], 17)
        for host in routes.HOSTS:
            self.assertNotIn("project_id", tomllib.loads(routes.fleet_config(host))["native"])

    def test_worker_route_unit_does_not_replace_board_or_reverse_tunnels(self):
        unit = routes.worker_unit()
        exec_line = next(line for line in unit.splitlines() if line.startswith("ExecStart="))
        self.assertIn("-F /home/dev-user/.ssh/borg_config", exec_line)
        self.assertEqual(exec_line.count(" -L "), 5)
        self.assertIn("-L 127.0.0.1:17870:127.0.0.1:17870", exec_line)
        self.assertNotIn("8079", exec_line)
        self.assertNotIn(" -R ", exec_line)
        self.assertIn("-L 127.0.0.1:17871:127.0.0.1:8787", exec_line)
        for port in (17872, 17873, 17874):
            self.assertIn(f"-L 127.0.0.1:{port}:127.0.0.1:{port}", exec_line)
        self.assertNotIn("0.0.0.0", exec_line)
        for value in ("BatchMode=yes", "ExitOnForwardFailure=yes", "ServerAliveInterval=15",
                      "ServerAliveCountMax=3", "Restart=always"):
            self.assertIn(value, unit)

    def test_controller_socket_proxy_is_separate_from_native_engine(self):
        units = routes.controller_units()
        self.assertEqual(set(units), {"borg-native-controller-route.socket", "borg-native-controller-route.service"})
        self.assertIn("ListenStream=127.0.0.1:17871", units["borg-native-controller-route.socket"])
        self.assertIn("ExecStart=/lib/systemd/systemd-socket-proxyd 127.0.0.1:8787", units["borg-native-controller-route.service"])
        self.assertNotIn("ExecStart=/usr/bin/ssh", units["borg-native-controller-route.service"])
        self.assertNotIn("borg-native --config", units["borg-native-controller-route.service"])

    def test_reachability_summary_requires_every_caller_not_only_controller(self):
        callers = [{"caller": host, "caller_verified": True, "builder_route": {"reachable": True}, "routes": [{"reachable": True, "identity_verified": True} for _ in range(4)]}
                   for host in routes.HOSTS]
        self.assertTrue(routes.summarize(callers, True)["ok"])
        self.assertFalse(routes.summarize(callers[:1], True)["ok"])
        callers[2]["routes"][3]["reachable"] = False
        self.assertFalse(routes.summarize(callers)["ok"])
        callers[2]["routes"][3]["reachable"] = True
        callers[2]["routes"][3]["identity_verified"] = False
        self.assertTrue(routes.summarize(callers)["ok"])
        self.assertFalse(routes.summarize(callers, True)["ok"])

    def test_systemd_interpolation_or_unsafe_arguments_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            for value in ("bad name", "bad\nExecStart=", "%h/ssh", "$HOME/key", ""):
                with self.assertRaises(ValueError):
                    routes.render(tmp, ssh_config=value)
            with self.assertRaises(ValueError):
                routes.render(tmp, base="relative")


if __name__ == "__main__":
    unittest.main(verbosity=2)
