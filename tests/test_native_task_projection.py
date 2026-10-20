"""Real Board/Fleet task-list contract regressions (no production writes).

BORG_TEST_BOARD_BINARY=/path/to/task-board BORG_TEST_FLEET_BINARY=/path/to/fleet \
  python3 -m unittest discover -s tests -p test_native_task_projection.py -v

Use the actual Board REST/SQLite list/detail boundary and four Fleet processes.
Only native inference is a fixture; its workspace tool is a real MCP process.
"""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
import unittest
import urllib.error
import urllib.request

import test_native_fleet as fleet

BOARD = os.environ.get("BORG_TEST_BOARD_BINARY")
FLEET = os.environ.get("BORG_TEST_FLEET_BINARY")


class RecordingBoard:
    """Forward to the real private Board, recording attempted reservations too.

    Merely counting successful dispatches would miss a broken Fleet filter that
    sends disabled work to Board and depends on its conflict response every tick.
    """
    def __init__(self, upstream):
        self.reservations = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def forward(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.command == "POST" and self.path == "/api/fleet/dispatches":
                    owner.reservations.append(json.loads(raw))
                request = urllib.request.Request(
                    upstream.removesuffix("/api") + self.path,
                    data=raw or None, method=self.command,
                    headers={"Content-Type": "application/json"})
                try:
                    response = urllib.request.urlopen(request, timeout=20)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    self.send_response(response.code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

            do_GET = do_POST = do_PATCH = forward

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/api"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@unittest.skipUnless(BOARD and FLEET, "requires BORG_TEST_BOARD_BINARY and BORG_TEST_FLEET_BINARY")
class NativeTaskProjection(unittest.TestCase):
    def setUp(self):
        # Reuse the established real-process fixture without inheriting/rerunning
        # its unrelated tests or touching any service/global configuration.
        fleet.BOARD, fleet.FLEET = Path(BOARD), Path(FLEET)
        self.cluster = fleet.FleetIntegration(methodName="runTest")
        self.addCleanup(self.cluster.doCleanups)
        self.cluster.setUp()
        self.proxy = RecordingBoard(self.cluster.board_url)
        self.addCleanup(self.proxy.close)
        for path in self.cluster.configs:
            path.write_text(path.read_text().replace(
                json.dumps(self.cluster.board_url), json.dumps(self.proxy.url)))

    def detail(self, tid):
        return fleet.http("GET", self.cluster.board_url + f"/tasks/{tid}?comments_limit=0")

    def listing(self, tid):
        rows = fleet.http("GET", self.cluster.board_url + f"/tasks?project_id={self.cluster.project}")
        return next(row for row in rows if row["id"] == tid)

    def test_failed_attempt_one_then_explicitly_reconciled_attempt_two(self):
        c = self.cluster
        task = c.task(metadata={"workspace": {"repo": str(c.root / "absent-repository")}})
        c.workers()
        c.workers()
        failed, = c.dispatches()
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["metadata"]["failure_stage"], "provisioning")
        self.assertEqual(sum(sum(e.calls.values()) for e in c.engines), 0)
        original_receipt = copy.deepcopy(failed)

        # This is explicit known-pre-native reconciliation, not an inference or
        # unknown-tool retry. A fixed canonical next attempt is retained on Board.
        fleet.http("PATCH", c.board_url + f'/tasks/{task["id"]}', {"metadata": {
            "native_attempt": 2,
            "workspace": {"repo": str(c.source)},
            "provisioning_reconciliation": {
                "failed_dispatch_id": failed["id"],
                "failed_session_id": failed["session_id"],
                "known_pre_native": True,
                "runtime_session_absent": True,
            },
        }})
        self.assertEqual(self.detail(task["id"])["metadata"]["native_attempt"], 2)
        c.workers()
        c.workers()
        dispatches = c.dispatches()
        self.assertEqual(len(dispatches), 2,
                         "Fleet must see reconciled attempt 2, not silently reuse failed key 1")
        first, second = dispatches
        self.assertEqual(first, original_receipt, "the failed original receipt must remain unchanged")
        self.assertEqual(second["request_id"], f'task:{task["id"]}:attempt:2')
        self.assertEqual(second["metadata"]["native_attempt"], 2)
        self.assertNotEqual(second["session_id"], failed["session_id"])
        self.assertEqual(sum(sum(e.effects.values()) for e in c.engines), 1)
        self.assertEqual((Path(second["workspace_cwd"]) / "result.txt").read_text(), "preserved")
        listed, detailed = self.listing(task["id"]), self.detail(task["id"])
        self.assertEqual(listed["metadata"], detailed["metadata"])
        self.assertNotIn("description", listed)
        self.assertNotIn("comments", listed)
        c.workers()
        self.assertEqual(len(c.dispatches()), 2)
        self.assertEqual(sum(sum(e.effects.values()) for e in c.engines), 1)
        self.assertEqual({r["request_id"] for r in self.proxy.reservations}, {
            f'task:{task["id"]}:attempt:1', f'task:{task["id"]}:attempt:2'})

    def test_native_dispatch_false_is_filtered_before_reservation(self):
        c = self.cluster
        disabled = c.task("disabled task", {"native_dispatch": False, "native_attempt": 2})
        enabled = c.task("enabled task")
        c.workers()
        c.workers()
        posted = {r["task_id"] for r in self.proxy.reservations}
        self.assertNotIn(disabled["id"], posted,
                         "do not rely on Board conflict rejection of disabled tasks every poll")
        self.assertIn(enabled["id"], posted)
        self.assertEqual({d["task_id"] for d in c.dispatches()}, {enabled["id"]})
        self.assertIs(self.listing(disabled["id"])["metadata"]["native_dispatch"], False)
        self.assertEqual(self.listing(disabled["id"])["metadata"],
                         self.detail(disabled["id"])["metadata"])

    def test_blocking_questions_and_recovery_policy_survive_list_projection(self):
        c = self.cluster
        metadata = {"recovery": {"incident_key": "test-owned-repair", "enabled": False},
                    "recovery_task_id": 123, "native_attempt": 2}
        task = c.task("question-held repair", metadata)
        question = fleet.http("POST", c.board_url + f'/tasks/{task["id"]}/questions', {
            "principal": "engineer", "routed_to": "engineer",
            "kind": "yes_no", "prompt": "Keep this work held?", "blocking": True})
        listed, detailed = self.listing(task["id"]), self.detail(task["id"])
        self.assertEqual(listed["metadata"], detailed["metadata"])
        self.assertEqual(listed["metadata"], metadata)
        self.assertIs(listed["question_blocked"], True)
        self.assertIs(listed["effectively_blocked"], True)
        c.workers()
        self.assertEqual(c.dispatches(), [])
        self.assertEqual(self.proxy.reservations, [])
        # Clearing a question must restore eligibility, rather than leave a
        # cached blocker in the compact projection.
        fleet.http("POST", c.board_url + f'/comments/{question["id"]}/answer', {
            "principal": "engineer", "shape": "bool", "value": False})
        self.assertIs(self.listing(task["id"])["question_blocked"], False)
        self.assertIs(self.listing(task["id"])["effectively_blocked"], False)
        c.workers()
        c.workers()
        self.assertEqual(len(c.dispatches()), 1)
        self.assertEqual(c.dispatches()[0]["request_id"], f'task:{task["id"]}:attempt:2')


if __name__ == "__main__":
    unittest.main(verbosity=2)
