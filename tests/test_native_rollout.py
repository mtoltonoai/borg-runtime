import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tooling"))
import native_rollout as rollout


class RolloutRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.args = argparse.Namespace(base=self.base, ssh_config=self.base / "ssh", transaction="test")
        self.original = {"image": "old", "ready": True, "ActiveState": "active",
                         "expected_config": True, "database": [7, 11]}
        self.record = {"board_before": self.original}
        self.path = self.base / "receipt.json"
        self.manifest = {"files": {"task-board": "selected"}}

    def test_pre_mutation_failure_releases_owner_against_unchanged_ready_board(self):
        with mock.patch.object(rollout, "board_snapshot", return_value=self.original), mock.patch.object(rollout, "command") as command:
            self.assertTrue(rollout.recover_board(self.args, self.record, self.path, self.manifest, self.base / "old.service"))
            command.assert_not_called()
        self.assertEqual(json.loads(self.path.read_text())["board_failure_reconciled"]["image"], "old")

    def test_interrupted_install_accepts_independently_verified_selected_board(self):
        selected = dict(self.original, image="selected")
        with mock.patch.object(rollout, "board_snapshot", return_value=selected), mock.patch.object(rollout, "command") as command:
            self.assertTrue(rollout.recover_board(self.args, self.record, self.path, self.manifest, self.base / "old.service"))
            command.assert_not_called()

    def test_unavailable_board_has_live_restore_continuation_not_parked_owner_exit(self):
        with mock.patch.object(rollout, "board_snapshot", side_effect=[{"ready": False}, self.original]), mock.patch.object(rollout, "command", return_value="") as command, mock.patch.object(rollout.time, "sleep"):
            self.assertTrue(rollout.recover_board(self.args, self.record, self.path, self.manifest, self.base / "old.service"))
        actions = [call.args[1] for call in command.call_args_list]
        self.assertEqual(len(actions), 3)
        self.assertIn("install", actions[0])
        self.assertIn("daemon-reload", actions[1])
        self.assertIn("restart", actions[2])
        self.assertTrue(self.record["active_recovery_continuation"])
        self.assertEqual(self.record["recovery_owner"], "integrator")
        self.assertFalse(any("sqlite" in str(action) for action in actions), "DB must never be replaced")

    def test_healthy_unknown_image_or_different_database_cannot_release_callers(self):
        for changed in ({"image": "unexpected"}, {"database": [7, 12]}, {"expected_config": False}, {"ready": False}):
            with self.subTest(changed=changed):
                self.assertFalse(rollout.trusted_board(dict(self.original, **changed), self.original, "selected"))

    def test_builder_workspace_is_never_restarted_by_global_coordinator(self):
        self.assertEqual(rollout.target_components("local", ["board", "native", "workspace", "session-workspace"]), ["native"])
        self.assertEqual(rollout.origin_for("local"), "http://127.0.0.1:17870")
        self.assertEqual(rollout.origin_for("borg-agent-lab-4"), "http://127.0.0.1:17874")

    def test_global_launch_returns_before_its_own_drain(self):
        args = argparse.Namespace(release=self.base, base=self.base, ssh_config=self.base / "ssh",
                                  components=["board", "native"], board_profiles=True)
        with mock.patch.object(rollout, "verify", return_value={"revision": "a" * 40}), mock.patch.object(rollout.subprocess, "run") as run:
            handle = rollout.launch(args)
        invocation = run.call_args.args[0]
        self.assertIn("--no-block", invocation)
        self.assertIn("--property=TimeoutStartSec=infinity", invocation)
        self.assertIn("--board-profiles", invocation)
        self.assertTrue(handle["poll_after_initiating_tool_settles"])


if __name__ == "__main__":
    unittest.main()
