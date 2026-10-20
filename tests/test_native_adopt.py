#!/usr/bin/env python3
"""Bootstrap adoption boundary: actual Board SQLite/REST, read-only native fixtures."""
import copy
import importlib.util
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("native_adopt", ROOT / "tooling/native_adopt.py")
adopt = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adopt)
BOARD = ROOT / "components/task-board/target/debug/task-board"


class RuntimeFixture:
    def __init__(self, host, sessions):
        self.host, self.sessions, self.calls = host, sessions, []

    def request(self, method, url, body=None):
        self.calls.append((method, url))
        if method != "GET":
            raise AssertionError("adoption must never mutate a runtime")
        if url.endswith("/health"):
            return {"status": "ok", "service": "borg-native", "host_id": self.host,
                    "ready": False, "draining": True}
        if url.endswith("/sessions"):
            return copy.deepcopy(self.sessions)
        raise AssertionError(url)


class LosingReply:
    def __init__(self, client, when="after"):
        self.client, self.when, self.posts, self.lost = client, when, 0, False

    def request(self, method, url, body=None):
        target = method == "POST" and url.endswith("/fleet/dispatches/adopt")
        if target:
            self.posts += 1
            if not self.lost and self.when == "before":
                self.lost = True
                raise adopt.HttpError(None, "injected before commit")
        value = self.client.request(method, url, body)
        if target and not self.lost:
            self.lost = True
            raise adopt.HttpError(None, "injected after real Board committed")
        return value


class AdoptionContractTests(unittest.TestCase):
    def test_url_normalization_and_canonical_task_keys(self):
        self.assertEqual(adopt.api_url("http://127.0.0.1:17872/"), "http://127.0.0.1:17872/api")
        self.assertEqual(adopt.api_url("http://127.0.0.1:17872/api/"), "http://127.0.0.1:17872/api")
        for value in ("task_3", "#3", "3", 3):
            self.assertEqual(adopt.task_number(value), 3)
        self.assertIsNone(adopt.task_number("unrelated text 3"))
        self.assertIsNone(adopt.task_number(True))
        for url in ("file:///etc/passwd", "http://user:password@localhost", "http://localhost?token=secret"):
            with self.assertRaises(adopt.ContractError):
                adopt.api_url(url)

    def test_host_manifest_is_explicit_and_builder_can_be_outside_placement(self):
        values = adopt.host_specs({"hosts": [{"host_id": "borg-builder", "runtime_url": "http://127.0.0.1:17870",
                                             "probe_url": "http://127.0.0.1:8787", "workspace_root": "/builder/workspaces", "capacity": 0}]})
        self.assertFalse(values[0]["enabled"])
        self.assertEqual(values[0]["probe_url"], "http://127.0.0.1:8787/api")
        self.assertEqual(values[0]["runtime_url"], "http://127.0.0.1:17870/api")
        with self.assertRaises(adopt.ContractError):
            adopt.host_specs({"hosts": values * 2})

    def test_binding_preserves_session_data_and_rejects_invented_placement(self):
        host = {"host_id": "lab", "runtime_url": "http://localhost:8787/api"}
        task = {"id": 3, "assignee": "engineer", "metadata": {"native_attempt": 2}}
        session = {"id": "existing", "agent_id": "engineer", "host_id": "lab", "runtime_url": "http://localhost:8787",
                   "workspace_cwd": "/original/bootstrap", "generation": 7, "status": "stopped"}
        body = adopt.binding(host, session, task)
        self.assertEqual(body["request_id"], "task:3:attempt:2")
        self.assertEqual(body["generation"], 7)
        self.assertEqual(body["status"], "stopped")
        self.assertNotIn("input", body)
        for field, wrong in (("host_id", "other"), ("runtime_url", "http://localhost:9999"),
                             ("workspace_cwd", "../wrong"), ("generation", -1), ("agent_id", "other")):
            altered = dict(session, **{field: wrong})
            with self.assertRaises(adopt.ContractError):
                adopt.binding(host, altered, task)


@unittest.skipUnless(BOARD.is_file(), "build the actual task-board binary for REST/SQLite adoption tests")
class RealBoardAdoption(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="native-adopt-test-")
        self.root = Path(self.temp.name)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = self.root / "board.toml"
        config.write_text(f'db_path={json.dumps(str(self.root / "board.db"))}\nhost="127.0.0.1"\nport={port}\n')
        self.log = open(self.root / "board.log", "wb")
        self.process = subprocess.Popen([str(BOARD), "--config", str(config)], stdout=self.log, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        self.addCleanup(self.cleanup)
        self.base = f"http://127.0.0.1:{port}/api"
        self.client = adopt.Client(timeout=3)
        deadline = time.monotonic() + 10
        while True:
            try:
                self.client.request("GET", self.base + "/health")
                break
            except adopt.HttpError:
                if time.monotonic() > deadline:
                    self.fail("Board failed to start")
                time.sleep(.05)
        self.client.request("POST", self.base + "/agents", {"agent_id": "engineer", "metadata": {"harness": "native"}})
        self.project = self.client.request("POST", self.base + "/projects", {"name": "adoption fixture"})["id"]
        self.runtimes = {}

    def cleanup(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()
        self.temp.cleanup()

    def task(self):
        return self.client.request("POST", self.base + "/tasks", {
            "project_id": self.project, "title": "adoption task", "assignee": "engineer"})

    def cohort(self, status="stopped", builder=True):
        hosts = []
        names = ["agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4"] + (["borg-builder"] if builder else [])
        for i, name in enumerate(names):
            task = self.task()
            port = 17870 if name == "borg-builder" else 17871 + i
            url = f"http://127.0.0.1:{port}"
            session = {"id": str(uuid.uuid4()), "agent_id": "engineer", "host_id": name, "task": f'task_{task["id"]}',
                       "runtime_url": url, "workspace_cwd": str(self.root / name / "original-bootstrap"),
                       "generation": i, "status": status}
            self.runtimes[name] = RuntimeFixture(name, [session])
            hosts.append({"host_id": name, "runtime_url": url, "workspace_root": str(self.root / name / "workspaces"),
                          "capacity": 0 if name == "borg-builder" else 4, "session_ids": [session["id"]],
                          **({"probe_url": "http://127.0.0.1:8787"} if name == "borg-builder" else {})})
        return {"hosts": hosts}

    def apply(self, manifest, client=None, apply=True):
        return adopt.run(manifest, self.base, apply=apply, board=client or self.client,
                         runtime_factory=lambda host: self.runtimes[host["host_id"]])

    def test_real_five_host_adoption_repeated_and_builder_not_a_fifth_worker(self):
        manifest = self.cohort()
        original = {name: copy.deepcopy(runtime.sessions) for name, runtime in self.runtimes.items()}
        first = self.apply(manifest)
        self.assertTrue(first["safe_to_enable_fleet"])
        self.assertEqual(len(first["adoptions"]), 5)
        second = self.apply(manifest)
        self.assertTrue(all(not row["posted"] for row in second["adoptions"]))
        self.assertEqual([a["receipt"]["id"] for a in first["adoptions"]], [a["receipt"]["id"] for a in second["adoptions"]])
        rows = self.client.request("GET", self.base + "/fleet/dispatches")
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(row["status"] == "stopped" for row in rows))
        for row in rows:
            self.assertFalse(Path(row["workspace_cwd"]).exists(), "helper must not provision a local directory")
            self.assertEqual(row["workspace_cwd"], original[row["host_id"]][0]["workspace_cwd"])
            self.assertEqual(row["session_id"], original[row["host_id"]][0]["id"])
            self.assertEqual(row["generation"], original[row["host_id"]][0]["generation"])
        builder = next(h for h in self.client.request("GET", self.base + "/fleet/hosts") if h["host_id"] == "borg-builder")
        self.assertEqual(builder["capacity"], 0)
        self.assertFalse(builder["enabled"])
        self.assertEqual(original, {name: runtime.sessions for name, runtime in self.runtimes.items()})
        self.assertTrue(all(method == "GET" for runtime in self.runtimes.values() for method, _ in runtime.calls))

    def test_lost_after_commit_reply_is_verified_without_duplicate_post(self):
        manifest = self.cohort(builder=False)
        manifest["hosts"] = manifest["hosts"][:1]
        losing = LosingReply(self.client, "after")
        result = self.apply(manifest, losing)
        self.assertTrue(result["safe_to_enable_fleet"])
        self.assertTrue(result["adoptions"][0]["reconciled"])
        self.assertEqual(losing.posts, 1)
        self.assertEqual(len(self.client.request("GET", self.base + "/fleet/dispatches")), 1)
        self.apply(manifest, losing)
        self.assertEqual(losing.posts, 1)

    def test_lost_before_commit_retries_same_canonical_key(self):
        manifest = self.cohort(builder=False)
        manifest["hosts"] = manifest["hosts"][:1]
        losing = LosingReply(self.client, "before")
        result = self.apply(manifest, losing)
        self.assertTrue(result["safe_to_enable_fleet"])
        self.assertEqual(losing.posts, 2)
        self.assertEqual(len(self.client.request("GET", self.base + "/fleet/dispatches")), 1)

    def test_dry_run_never_claims_enabled_safety_or_writes_board(self):
        result = self.apply(self.cohort(), apply=False)
        self.assertTrue(result["plan_valid"])
        self.assertFalse(result["safe_to_enable_fleet"])
        self.assertFalse(self.client.request("GET", self.base + "/fleet/hosts"))
        self.assertFalse(self.client.request("GET", self.base + "/fleet/dispatches"))

    def test_skip_completed_tasks_and_sessions_without_silently_replaying_active_work(self):
        manifest = self.cohort(status="completed", builder=False)
        self.runtimes["agent-lab"].sessions[0]["status"] = "running"
        done_id = adopt.task_number(self.runtimes["agent-lab"].sessions[0]["task"])
        self.client.request("PATCH", self.base + f"/tasks/{done_id}", {"status": "done"})
        result = self.apply(manifest)
        self.assertFalse(result["safe_to_enable_fleet"])
        self.assertEqual(len(result["skipped"]), 4)
        self.assertEqual(len(result["uncovered_terminal_tasks"]), 3)
        self.assertFalse(result["adoptions"])
        self.assertFalse(self.client.request("GET", self.base + "/fleet/dispatches"))

    def test_changed_binding_fails_preflight_without_overwrite(self):
        manifest = self.cohort(builder=False)
        self.apply(manifest)
        original = self.client.request("GET", self.base + "/fleet/dispatches")[0]
        self.runtimes["agent-lab"].sessions[0]["workspace_cwd"] = "/different/owned/workspace"
        with self.assertRaises(adopt.ContractError):
            self.apply(manifest)
        self.assertEqual(self.client.request("GET", self.base + f'/fleet/dispatches/{original["id"]}')["workspace_cwd"], original["workspace_cwd"])

    def test_duplicate_live_task_is_rejected_before_any_board_write(self):
        manifest = self.cohort(builder=False)
        self.runtimes["agent-lab-2"].sessions[0]["task"] = self.runtimes["agent-lab"].sessions[0]["task"]
        with self.assertRaises(adopt.ContractError):
            self.apply(manifest)
        self.assertFalse(self.client.request("GET", self.base + "/fleet/hosts"))
        self.assertFalse(self.client.request("GET", self.base + "/fleet/dispatches"))

    def test_host_capacity_and_drain_policy_survive_repeated_adoption(self):
        manifest = self.cohort(builder=False)
        self.apply(manifest)
        self.client.request("PATCH", self.base + "/fleet/hosts/agent-lab", {"capacity": 9, "enabled": False})
        self.apply(manifest)
        host = next(h for h in self.client.request("GET", self.base + "/fleet/hosts") if h["host_id"] == "agent-lab")
        self.assertEqual(host["capacity"], 9)
        self.assertFalse(host["enabled"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
