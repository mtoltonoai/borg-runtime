"""Independent-controller regressions, including crash-window and real HTTP boundaries."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

MODULE = Path(__file__).resolve().parents[1] / "tooling/native_recovery.py"
spec = importlib.util.spec_from_file_location("native_recovery", MODULE)
r = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = r
spec.loader.exec_module(r)

NOW = 1792464000.0


def agent(name="engineer", **kwargs):
    result = {"id": name, "lifecycle_intent": "run", "metadata": {"harness": "native"}}
    result.update(kwargs)
    return result


def task(identifier=1, **kwargs):
    result = {"id": identifier, "project_id": 1, "assignee": "engineer", "status": "in_progress",
              "created_at": r.iso(NOW - 5000), "updated_at": r.iso(NOW-5000), "metadata": {}}
    result.update(kwargs)
    return result


def session(**kwargs):
    result = {"id": "s1", "agent_id": "engineer", "task": "task_1", "status": "running",
              "driver_running": True, "phase": "awaiting-model", "turns": 2, "step": 10,
              "updated_at": int((NOW - 30) * 1000), "last_progress_at": int((NOW - 30) * 1000),
              "runtime_url": "http://worker/api"}
    result.update(kwargs)
    return result


def snapshot(**kwargs):
    result = dict(project={"id": 1, "status": "active", "metadata": {}},
                  agents={a["id"]: a for a in [agent(), agent("recovery-engineer"), agent("pm")]},
                  tasks=[task()], hosts=[], dispatches=[], sessions=[],
                  endpoints={"http://worker/api": True}, policy=r.Policy(), now=NOW)
    result.update(kwargs)
    return r.Snapshot(**result)


class PolicyTests(unittest.TestCase):
    def kinds(self, snap):
        return [f.kind for f in r.classify(snap)[0]]

    def test_missing_task_is_not_covered_by_unrelated_same_identity_session(self):
        self.assertEqual(self.kinds(snapshot(sessions=[session(task="task_99")])), ["missing_actionable_session"])
        self.assertEqual(self.kinds(snapshot(sessions=[session()])), [])

    def test_same_owner_live_ancestor_covers_child_but_other_owner_does_not(self):
        snap = snapshot(tasks=[task(1), task(2, parent_id=1)], sessions=[session()])
        self.assertEqual(self.kinds(snap), [])
        snap.tasks[1]["assignee"] = "pm"
        self.assertEqual(self.kinds(snap), ["missing_actionable_session"])

    def test_failed_session_and_unknown_effect_are_not_silently_missing(self):
        snap = snapshot(sessions=[session(status="failed", driver_running=False)])
        self.assertEqual(self.kinds(snap), ["session_failed"])
        snap.sessions[0].update(status="needs_resolution", tool_receipts=[{"status":"unknown","call_id":"c"}])
        self.assertEqual(self.kinds(snap), ["unknown_tool_effect"])
        fault = r.classify(snap)[0][0]
        self.assertEqual(fault.evidence["safe_action"], "diagnose_without_replay")

    def test_long_live_inference_and_unlimited_retries_never_become_dead(self):
        for status, phase in [("running", "awaiting-model"), ("retrying", "idle"), ("running", "streaming")]:
            with self.subTest(status=status, phase=phase):
                s = session(status=status, phase=phase, last_progress_at=int((NOW-100000)*1000))
                faults, waits = r.classify(snapshot(sessions=[s]))
                self.assertEqual([f.kind for f in faults], ["long_live_wait"])
                self.assertTrue(waits)
                self.assertEqual(faults[0].evidence["driver_running"], True)
                self.assertNotIn("restart", faults[0].evidence["safe_action"])

    def test_driver_absence_is_not_inferred_from_stale_progress(self):
        s = session(driver_running=False, last_progress_at=int((NOW-2000)*1000))
        self.assertEqual(self.kinds(snapshot(sessions=[s])), ["driver_missing"])
        s.update(driver_running=True, phase="awaiting-tool-result")
        self.assertEqual(self.kinds(snapshot(sessions=[s])), ["progress_stalled"])

    def test_heartbeat_does_not_hide_stopped_progress(self):
        s = session(phase="idle", last_progress_at=int((NOW-2000)*1000), updated_at=int(NOW*1000))
        self.assertEqual(self.kinds(snapshot(sessions=[s])), ["progress_stalled"])

    def test_pause_stop_verified_wait_and_blocked_tasks_are_visible(self):
        cases = [snapshot(tasks=[task(status="blocked")]), snapshot(sessions=[session(status="stopped")]),
                 snapshot(agents={"engineer":agent(lifecycle_intent="paused")}),
                 snapshot(tasks=[task(metadata={"recovery":{"wait":{"verified":True,
                    "reason":"external dependency","until":r.iso(NOW+60)}}})])]
        for snap in cases:
            with self.subTest(snapshot=snap):
                faults, waits = r.classify(snap)
                self.assertEqual(faults, [])
                self.assertTrue(waits)

    def test_standdown_is_an_explicit_stop_even_when_presence_and_intent_say_run(self):
        snap = snapshot(agents={"engineer":agent(stand_down_requested_at=r.iso(NOW-10))})
        faults, waits = r.classify(snap)
        self.assertEqual(faults, [])
        self.assertTrue(waits)

    def test_unreachable_other_task_host_does_not_hide_unrelated_orphan(self):
        snap = snapshot(endpoints={"http://worker/api":True,"http://drained/api":False},
            endpoint_scopes={"http://worker/api":{"task_ids":[]},
                             "http://drained/api":{"task_ids":[99],"held":True}},
            hosts=[{"host_id":"old","runtime_url":"http://drained/api","enabled":False}])
        self.assertEqual(self.kinds(snap), ["missing_actionable_session"])
        snap.endpoint_scopes["http://drained/api"]["task_ids"] = [1]
        self.assertEqual(self.kinds(snap), [], "this task's own uncertain endpoint still prevents an absence claim")

    def test_expired_unverified_or_reasonless_wait_does_not_hide_work(self):
        for wait in [{"verified":False,"reason":"waiting","until":r.iso(NOW+60)},
                     {"verified":True,"until":r.iso(NOW+60)},
                     {"verified":True,"reason":"waiting","until":r.iso(NOW-1)}]:
            self.assertEqual(self.kinds(snapshot(tasks=[task(metadata={"recovery":{"wait":wait}})])),
                             ["missing_actionable_session"])

    def test_project_maintenance_has_reason_deadline_and_expiry(self):
        policy = r.Policy(maintenance={"reason":"draining tools", "until":r.iso(NOW+60)})
        self.assertEqual(self.kinds(snapshot(policy=policy)), [])
        self.assertEqual(self.kinds(snapshot(policy=policy, now=NOW+61)), ["missing_actionable_session"])

    def test_incomplete_observation_never_proves_missing_session(self):
        snap = snapshot(endpoints={"http://worker/api":False})
        self.assertEqual(self.kinds(snap), ["runtime_unreachable"])
        self.assertFalse(r.classify(snap)[0][0].evidence["process_death_verified"])
        self.assertEqual(self.kinds(snapshot(endpoints={})), ["no_runtime_capacity"])

    def test_queued_dispatch_has_grace_and_stale_dispatch_has_owner(self):
        d = {"id":"d1", "agent_id":"engineer", "task_id":1, "status":"launching", "created_at":NOW-20}
        self.assertEqual(self.kinds(snapshot(dispatches=[d])), [])
        d["created_at"] = NOW - 2000
        self.assertEqual(self.kinds(snapshot(dispatches=[d])), ["dispatch_without_live_session"])

    def test_unowned_task_and_unknown_agent_are_actionable(self):
        for assignee in (None, "vanished"):
            self.assertEqual(self.kinds(snapshot(tasks=[task(assignee=assignee)])), ["unowned_actionable_task"])

    def test_repair_tasks_do_not_recursively_generate_missing_task_storms(self):
        repair = task(metadata={"recovery":{"incident_key":"incident"}})
        self.assertEqual(self.kinds(snapshot(tasks=[repair])), [])

    def test_metadata_override_and_invalid_values(self):
        policy = r.Policy.from_metadata({"progress_stall_seconds":999}, {"progress_stall_seconds":10})
        self.assertEqual(policy.progress_stall_seconds, 10)
        for value in [-1, "forever", False, float("inf")]:
            with self.assertRaises(ValueError):
                r.Policy.from_metadata({"progress_stall_seconds":value})

    def test_bounded_jitter_never_has_retry_count_exhaustion(self):
        for attempt in [1, 21, 1000, 2**64]:
            delay = r.backoff(attempt, 2, 60, random_value=0.5)
            self.assertGreater(delay, 0)
            self.assertLessEqual(delay, 60)
        self.assertEqual(r.backoff(1000, 2, 60, floor=180), 180)
        self.assertEqual(r.retry_after("120", NOW), 120)
        self.assertEqual(r.retry_after("nonsense", NOW), 0)


class FakeBoard:
    def __init__(self):
        self.tasks = {1: task()}
        self.task_keys = {}
        self.dispatch_keys = {}
        self.dispatches = {}
        self.comments = {}
        self.calls = []
        self.create_response_lost = False
        self.dispatch_response_lost = False
        self.unavailable = False
        self.next_task = 20
        self.runtime_url = "http://worker/api"
        self.write_version = 0
        self.dispatch_intents = {}

    def call(self, method, path, data=None):
        self.calls.append((method, path, copy.deepcopy(data)))
        if self.unavailable:
            raise r.HttpFailure(method, 503)
        clean = path.split("?")[0]
        if method == "POST" and clean == "/tasks":
            key = data["external_link"]["external_id"]
            if key not in self.task_keys:
                self.next_task += 1
                self.task_keys[key] = self.next_task
                self.tasks[self.next_task] = dict(copy.deepcopy(data), id=self.next_task, status="todo", updated_at=r.iso(NOW))
            result = self.tasks[self.task_keys[key]]
            if self.create_response_lost:
                self.create_response_lost = False
                raise r.HttpFailure(method)  # committed on the server; reply was lost
            return copy.deepcopy(result)
        if clean.startswith("/tasks/"):
            tid = int(clean.split("/")[2])
            if method == "GET":
                return copy.deepcopy(self.tasks[tid])
            if clean.endswith("/props"):
                self.tasks[tid].setdefault("metadata", {}).update(copy.deepcopy(data))
                return copy.deepcopy(self.tasks[tid]["metadata"])
            if clean.endswith("/comments"):
                key = data["external_link"]["external_id"]
                self.comments.setdefault(key, copy.deepcopy(data))
                return {"comment_id":list(self.comments).index(key)+1, "created":True}
            if method == "PATCH":
                self.write_version += 1
                self.tasks[tid].update(copy.deepcopy(data), updated_at=r.iso(NOW + self.write_version))
                return copy.deepcopy(self.tasks[tid])
        if method == "POST" and clean == "/fleet/dispatches":
            key = data["request_id"]
            if key in self.dispatch_intents and self.dispatch_intents[key] != data:
                raise r.HttpFailure(method, 409)
            if key not in self.dispatch_keys:
                self.dispatch_intents[key] = copy.deepcopy(data)
                did = str(len(self.dispatches)+1)
                self.dispatch_keys[key] = did
                self.dispatches[did] = dict(copy.deepcopy(data), id=did, status="reserved",
                    session_id="triage-"+did, runtime_url=self.runtime_url, created_at=NOW, updated_at=NOW)
            result = self.dispatches[self.dispatch_keys[key]]
            if self.dispatch_response_lost:
                self.dispatch_response_lost = False
                raise r.HttpFailure(method)
            return copy.deepcopy(result)
        raise AssertionError((method, path, data))

    def snapshot(self, **kwargs):
        defaults = {"tasks":copy.deepcopy(list(self.tasks.values())),
                    "dispatches":copy.deepcopy(list(self.dispatches.values())),
                    "policy":r.Policy(missing_session_grace_seconds=0, triage_grace_seconds=10_000)}
        defaults.update(kwargs)
        return snapshot(**defaults)


class CollectorContractTests(unittest.TestCase):
    def test_historical_terminal_and_drained_host_urls_are_excluded_and_full_identity_loaded(self):
        calls = []
        class Catalog:
            def pages(self, path):
                calls.append(path)
                if path.startswith("/agents"):
                    self_verbose = "verbose=true" in path
                    return [agent(stand_down_requested_at=r.iso(NOW)) if self_verbose else agent()]
                return [{"id":"old", "task_id":1,"host_id":"retired", "agent_id":"engineer",
                         "runtime_url":"http://retired/api", "status":"completed"}]
            def call(self, method, path, data=None):
                if path == "/projects/1": return {"id":1,"metadata":{}}
                if path.startswith("/tasks"): return [task()]
                if path == "/fleet/hosts": return [
                    {"host_id":"current","runtime_url":"http://worker/api","enabled":True},
                    {"host_id":"retired","runtime_url":"http://retired/api","enabled":False}]
                raise AssertionError(path)
        class Native:
            def request(self, method, url):
                calls.append(url)
                if "retired" in url: raise AssertionError("terminal historical runtime was probed")
                return []
        snap = r.Collector(Catalog(), Native()).collect(1,NOW)
        self.assertEqual(snap.endpoints, {"http://worker/api":True})
        self.assertIn("/agents?verbose=true", calls)
        self.assertFalse(r.agent_runs(snap.agents["engineer"]))
        snap.agents["engineer"].pop("stand_down_requested_at")
        self.assertEqual([f.kind for f in r.classify(snap)[0]], ["missing_actionable_session"])


class ProjectDiscoveryTests(unittest.TestCase):
    def test_omitted_scope_rediscovers_new_projects_and_filters_archived_disabled(self):
        class Catalog:
            projects = [{"id":1,"status":"active","metadata":{}}]
            calls = 0
            def call(self, method, path, data=None):
                self.calls += 1
                self.last = (method,path)
                return copy.deepcopy(self.projects)
        board = Catalog()
        self.assertEqual(r.selected_projects(board,None),[1])
        board.projects.extend([
            {"id":2,"status":"active","metadata":{}},
            {"id":3,"status":"archived","metadata":{}},
            {"id":4,"status":"active","metadata":{"recovery":{"enabled":False}}}])
        self.assertEqual(r.selected_projects(board,None),[1,2])
        self.assertEqual(board.calls,2)
        self.assertEqual(board.last,("GET","/projects?status=active"))
        board.projects[0]["metadata"] = {"recovery":{"enabled":False}}
        self.assertEqual(r.selected_projects(board,None),[2])

    def test_explicit_scope_and_empty_scope_are_respected_without_discovery(self):
        class NoDiscovery:
            def call(self,*args): raise AssertionError("explicit scope performed discovery")
        board = NoDiscovery()
        self.assertEqual(r.selected_projects(board,[9,3,9]),[3,9])
        self.assertEqual(r.selected_projects(board,[]),[])
        for invalid in ("all", [True], [0], [-1]):
            with self.assertRaises(ValueError): r.selected_projects(board,invalid)

    def test_discovery_failure_does_not_fall_back_to_project_one(self):
        class Down:
            def call(self,*args): raise r.HttpFailure("GET",503,180)
        with self.assertRaises(r.HttpFailure) as caught:
            r.selected_projects(Down(),None)
        self.assertEqual(caught.exception.delay,180)


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "recovery.sqlite"
        self.journal = r.Journal(self.path)
        self.addCleanup(lambda: self.journal.close())
        self.board = FakeBoard()
        self.controller = r.Controller(self.board, self.journal)

    def detect(self, **kwargs):
        self.controller.tick(self.board.snapshot(**kwargs))
        self.controller.tick(self.board.snapshot(**dict(kwargs, now=kwargs.get("now", NOW)+1)))

    def test_missing_work_creates_owned_repair_and_one_idempotent_fleet_dispatch(self):
        self.detect()
        row = self.journal.rows()[0]
        repair = self.board.tasks[row["task_id"]]
        self.assertEqual(repair["assignee"], "recovery-engineer")
        self.assertIn("regression", repair["description"])
        self.assertEqual(self.board.tasks[1]["metadata"]["recovery_task_id"], row["task_id"])
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertEqual(next(iter(self.board.dispatches.values()))["request_id"], f"task:{row['task_id']}:attempt:1")
        for offset in range(30):
            self.controller.tick(self.board.snapshot(now=NOW+2+offset))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertFalse(any("/resume" in path or "/stop" in path or "/resolve-tool" in path for _, path, _ in self.board.calls))

    def test_crash_after_board_commit_before_receipt_does_not_duplicate_task(self):
        self.board.create_response_lost = True
        self.detect()
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertIsNone(self.journal.rows()[0]["task_id"])
        self.journal.close()
        self.journal = r.Journal(self.path)
        self.controller = r.Controller(self.board, self.journal)
        self.controller.tick(self.board.snapshot(now=NOW+10))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertIsNotNone(self.journal.rows()[0]["task_id"])

    def test_lost_fleet_create_response_is_reconciled_with_same_key_after_restart(self):
        self.board.dispatch_response_lost = True
        self.detect()
        self.assertEqual(len(self.board.dispatches), 1)
        self.journal.close()
        self.journal = r.Journal(self.path)
        self.controller = r.Controller(self.board, self.journal)
        self.controller.tick(self.board.snapshot(now=NOW+10))
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertIsNotNone(self.journal.rows()[0]["dispatch_id"])

    def test_board_outage_retries_beyond_finite_budgets_then_repairs(self):
        self.board.unavailable = True
        self.detect()
        for count in range(40):
            self.controller.tick(self.board.snapshot(now=NOW+100+count*100))
        self.assertGreaterEqual(self.journal.rows()[0]["attempts"], 40)
        self.board.unavailable = False
        self.controller.tick(self.board.snapshot(now=NOW+10000))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertEqual(self.journal.rows()[0]["attempts"], 0)

    def test_unknown_tool_effect_creates_diagnosis_never_replays(self):
        self.detect(sessions=[session(status="needs_resolution", driver_running=False,
            tool_receipts=[{"call_id":"write", "operation_id":"s1:write", "status":"unknown"}])])
        repair = self.board.tasks[self.journal.rows()[0]["task_id"]]
        self.assertEqual(repair["metadata"]["recovery"]["kind"], "unknown_tool_effect")
        self.assertIn("Reconcile exact receipts", repair["description"])
        self.assertFalse(any("/sessions" in path for _, path, _ in self.board.calls))

    def test_suppression_and_detection_grace_survive_restart(self):
        p = r.Policy(runtime_failure_grace_seconds=60, runtime_failure_observations=3)
        self.controller.tick(self.board.snapshot(policy=p, endpoints={"http://worker/api":False}))
        self.controller.tick(self.board.snapshot(policy=p, endpoints={"http://worker/api":False}, now=NOW+30))
        self.assertEqual(self.journal.rows(), [])
        self.journal.close()
        self.journal = r.Journal(self.path)
        self.controller = r.Controller(self.board, self.journal)
        self.controller.tick(self.board.snapshot(policy=p, endpoints={"http://worker/api":False}, now=NOW+61))
        self.assertEqual(len(self.journal.rows()), 1)
        self.assertEqual(len(self.board.task_keys), 1)

    def test_controller_process_lock_excludes_duplicate_writers(self):
        other = r.Journal(self.path)
        self.addCleanup(other.close)
        with self.journal.exclusive():
            with self.assertRaises(RuntimeError):
                with other.exclusive():
                    pass
        with other.exclusive():
            pass

    def test_symptom_clear_is_not_false_claim_of_durable_repair(self):
        self.detect()
        row = self.journal.rows()[0]
        self.controller.tick(self.board.snapshot(sessions=[session()], now=NOW+3))
        self.assertEqual(self.journal.rows()[0]["state"], "symptom_cleared_awaiting_evidence")
        self.assertNotEqual(self.board.tasks[row["task_id"]]["status"], "done")
        repair = self.board.tasks[row["task_id"]]
        repair.update(status="done")
        repair["metadata"]["recovery_resolution"] = {"cause":"lost driver", "fix":"commit abc", "regression":"driver restart test"}
        self.controller.tick(self.board.snapshot(sessions=[session()], now=NOW+4))
        self.assertEqual(self.journal.rows()[0]["state"], "closed")

    def test_done_without_cause_and_regression_reopens_owned_repair(self):
        self.detect()
        row = self.journal.rows()[0]
        self.board.tasks[row["task_id"]]["status"] = "done"
        self.controller.tick(self.board.snapshot(now=NOW+3))
        self.assertEqual(self.board.tasks[row["task_id"]]["status"], "in_progress")
        self.assertTrue(any("incomplete" in key for key in self.board.comments))

    def test_repeated_premature_closures_and_post_repair_recurrence_reopen_each_generation(self):
        self.detect()
        row = self.journal.rows()[0]
        repair = self.board.tasks[row["task_id"]]
        for offset in (10, 20):
            repair.update(status="done", updated_at=r.iso(NOW+offset))
            self.controller.tick(self.board.snapshot(now=NOW+offset+1))
            self.assertEqual(repair["status"], "in_progress")
        repair.update(status="done", updated_at=r.iso(NOW+30))
        repair["metadata"]["recovery_resolution"] = {"cause":"lost driver", "fix":"commit abc", "regression":"restart"}
        self.controller.tick(self.board.snapshot(now=NOW+31, sessions=[session()]))
        self.assertEqual(self.journal.rows()[0]["state"], "closed")
        self.detect(now=NOW+40)
        self.assertEqual(repair["status"], "in_progress", "a recurring fault does not stay invisibly Board-done")
        reopen_intents = [x for x in self.journal.connection.execute("SELECT key FROM outbox") if x[0].endswith(":reopen")]
        self.assertEqual(len(reopen_intents), 3)

    def test_frozen_dispatch_survives_lost_response_and_assignee_change(self):
        self.board.dispatch_response_lost = True
        self.detect()
        row = self.journal.rows()[0]
        self.board.tasks[row["task_id"]]["assignee"] = "pm"
        self.journal.close()
        self.journal = r.Journal(self.path)
        self.controller = r.Controller(self.board, self.journal)
        self.controller.tick(self.board.snapshot(now=NOW+10))
        posts = [data for method,path,data in self.board.calls if method == "POST" and path == "/fleet/dispatches"]
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0], posts[1], "same request key has byte-equivalent canonical intent")
        self.assertEqual(posts[0]["agent_id"], "recovery-engineer")
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertEqual(self.journal.rows()[0]["last_error"], None)

    def test_outbox_intent_is_durable_before_first_network_effect(self):
        original = self.board.call
        def check(method, path, data=None):
            if method == "POST" and path == "/tasks":
                frozen = self.journal.connection.execute("SELECT body FROM outbox WHERE path='/tasks'").fetchone()
                self.assertIsNotNone(frozen)
                self.assertEqual(json.loads(frozen[0]), data)
            return original(method, path, data)
        self.board.call = check
        self.detect()

    def test_task_scoped_incomplete_snapshot_does_not_hide_cleared_incident(self):
        self.detect()
        self.controller.tick(self.board.snapshot(now=NOW+5,sessions=[session()],
            endpoints={"http://worker/api":True,"http://other/api":False},
            endpoint_scopes={"http://worker/api":{"task_ids":[1]},"http://other/api":{"task_ids":[99]}}))
        self.assertEqual(self.journal.rows()[0]["state"], "symptom_cleared_awaiting_evidence")

    def test_explicit_triage_stop_stays_visible_without_fallback_spawn(self):
        self.detect()
        next(iter(self.board.dispatches.values()))["status"] = "stopped"
        self.controller.tick(self.board.snapshot(now=NOW+10))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(self.journal.rows()[0]["state"], "owned_wait")

    def test_failed_triage_escalates_once_not_recursive_storm(self):
        self.detect()
        next(iter(self.board.dispatches.values()))["status"] = "failed"
        for count in range(10):
            self.controller.tick(self.board.snapshot(now=NOW+2+count))
        self.assertEqual(len(self.board.task_keys), 2)
        self.assertEqual(len(self.board.dispatches), 2)
        fallback = [t for t in self.board.tasks.values() if t.get("metadata", {}).get("recovery", {}).get("depth") == 1][0]
        self.assertEqual(fallback["assignee"], "pm")
        for dispatch in self.board.dispatches.values():
            dispatch["status"] = "needs_resolution"
        for count in range(10):
            self.controller.tick(self.board.snapshot(now=NOW+20+count))
        self.assertEqual(len(self.board.task_keys), 2)
        self.assertEqual(self.board.tasks[fallback["id"]]["status"], "blocked")
        self.assertIn("receipts", self.board.tasks[fallback["id"]]["blocked_on"]["note"])

    def test_maintenance_stops_new_effects_without_forgetting_incidents(self):
        self.detect()
        before = len(self.board.calls)
        p = r.Policy(maintenance={"reason":"rollout", "until":r.iso(NOW+600)})
        health = self.controller.tick(self.board.snapshot(policy=p, now=NOW+3))
        self.assertEqual(len(self.board.calls), before)
        self.assertEqual(health["state"], "maintenance")
        self.assertEqual(health["open_incidents"], 1)


class HttpBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.board = FakeBoard()
        self.http_requests = []
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass
            def do_GET(self):
                self.serve("GET")
            def do_POST(self):
                self.serve("POST")
            def do_PATCH(self):
                self.serve("PATCH")
            def serve(self, method):
                url = urlsplit(self.path)
                path = url.path.removeprefix("/api")
                data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)))) if method != "GET" else None
                owner.http_requests.append((method, path))
                try:
                    if method == "GET" and path == "/projects/1":
                        result = {"id":1,"status":"active","metadata":{"recovery":{
                            "runtime_failure_grace_seconds":0,"runtime_failure_observations":1,
                            "missing_session_grace_seconds":0}}}
                    elif method == "GET" and path == "/agents":
                        result = [agent(), agent("recovery-engineer"), agent("pm")]
                    elif method == "GET" and path == "/tasks":
                        result = list(owner.board.tasks.values())
                    elif method == "GET" and path == "/fleet/hosts":
                        result = [{"host_id":"worker","runtime_url":owner.native_url,"enabled":True,"runtime_ready":True}]
                    elif method == "GET" and path == "/fleet/dispatches":
                        result = list(owner.board.dispatches.values())
                    else:
                        result = owner.board.call(method, path, data)
                    payload = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except r.HttpFailure:
                    self.send_response(503)
                    self.send_header("Retry-After", "0")
                    self.end_headers()
        self.server = ThreadingHTTPServer(("127.0.0.1",0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.client = r.Board(self.url, r.JsonHttp(timeout=1))
        self.journal = r.Journal(Path(self.tmp.name)/"journal.sqlite")
        self.addCleanup(self.journal.close)
        self.controller = r.Controller(self.client, self.journal)
        self.start_native()
        self.addCleanup(self.kill_native)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def start_native(self):
        # A separate OS process supplies the native observation endpoint. SIGKILL
        # tests real process loss/connection refusal, not a mocked classifier flag.
        script = """
from http.server import HTTPServer,BaseHTTPRequestHandler
import json
class H(BaseHTTPRequestHandler):
 def log_message(self,*a): pass
 def do_GET(self):
  b=json.dumps([{\"id\":\"original\",\"agent_id\":\"engineer\",\"task\":\"task_1\",\"status\":\"running\",\"phase\":\"awaiting-model\",\"driver_running\":True}]).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
s=HTTPServer(('127.0.0.1',0),H)
print(s.server_port,flush=True)
s.serve_forever()
"""
        self.native = subprocess.Popen([sys.executable,"-u","-c",script], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        port = int(self.native.stdout.readline())
        self.native_url = f"http://127.0.0.1:{port}/api"
        self.board.runtime_url = self.native_url

    def kill_native(self):
        if self.native.poll() is None:
            self.native.kill()
        self.native.wait(3)
        self.native.stdout.close()

    def test_real_process_death_creates_owned_incident_and_durable_triage_dispatch(self):
        collector = r.Collector(self.client, r.JsonHttp(timeout=1))
        healthy = collector.collect(1, NOW)
        self.assertEqual(self.controller.tick(healthy)["open_incidents"], 0)
        self.kill_native()
        unhealthy = collector.collect(1, NOW+1)
        self.controller.tick(unhealthy)
        rows = self.journal.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0]["payload"])["kind"], "runtime_unreachable")
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(len(self.board.dispatches), 1)
        self.assertTrue(self.board.tasks[rows[0]["task_id"]]["assignee"])
        self.controller.tick(collector.collect(1, NOW+2))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertFalse(any("resume" in path or "stop" in path for _, path in self.http_requests))

    def test_http_create_ack_loss_preserves_exactly_one_repair(self):
        collector = r.Collector(self.client, r.JsonHttp(timeout=1))
        self.kill_native()
        self.board.create_response_lost = True
        self.controller.tick(collector.collect(1, NOW))
        self.controller.tick(collector.collect(1, NOW+100))
        self.assertEqual(len(self.board.task_keys), 1)
        self.assertEqual(len(self.board.dispatches), 1)


if __name__ == "__main__":
    unittest.main()
