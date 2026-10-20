"""Independent children remain owned after their parent completes (tasks 23/24)."""
import copy
import json
import tempfile
from pathlib import Path
import unittest

import test_native_recovery as fixture
r = fixture.r
NOW = fixture.NOW


def dispatch(identifier=7, **changes):
    value = {"id": identifier, "session_id": "child", "agent_id": "engineer", "task_id": None,
             "generation": 1, "version": 3, "status": "failed", "host_id": "worker",
             "runtime_url": "http://worker/api", "created_at": NOW-500, "updated_at": NOW-400,
             "metadata": {"parent_session_id": "parent", "failure_stage": "provisioning"}}
    value.update(changes)
    return value


def snap(**changes):
    parent = dispatch(6, session_id="parent", task_id=1, status="completed", metadata={})
    values = {"tasks": [fixture.task(status="done")], "dispatches": [parent, dispatch()],
              "sessions": [fixture.session(id="parent", status="completed", driver_running=False)]}
    values.update(changes)
    return fixture.snapshot(**values)


class ChildLineageTests(unittest.TestCase):
    def test_failed_child_survives_completed_parent_and_preserves_safe_receipts(self):
        state = snap()
        state.dispatches[1].update(input="secret input", error="sensitive error body")
        faults, _ = r.classify(state)
        self.assertEqual(len(faults), 1)
        fault = faults[0]
        self.assertEqual((fault.kind, fault.subject, fault.source_task_id, fault.session_id),
                         ("child_execution_fault", "dispatch:7", 1, "child"))
        self.assertEqual(fault.evidence["observation"], "dispatch_failed")
        self.assertEqual(fault.evidence["dispatch"]["failure_stage"], "provisioning")
        self.assertEqual(fault.evidence["lineage"]["parent_session_ids"], ["parent"])
        self.assertEqual(fault.evidence["lineage"]["source_task_status"], "done")
        self.assertEqual(fault.evidence["safe_action"], "diagnose_without_replay")
        self.assertNotIn("secret input", r.triage_description(fault))
        self.assertNotIn("sensitive error", r.triage_description(fault))

    def test_healthy_parent_and_sibling_do_not_cover_failed_child(self):
        state = snap(tasks=[fixture.task()], sessions=[fixture.session(id="parent")])
        state.dispatches.append(dispatch(8, session_id="healthy-child", status="running"))
        state.sessions.append(fixture.session(id="healthy-child", task=None, generation=1))
        faults, _ = r.classify(state)
        self.assertEqual([f.subject for f in faults], ["dispatch:7"])

    def test_nested_retained_lineage_works_without_live_parent_on_drained_host(self):
        state = snap(sessions=[])
        state.dispatches[0].update(runtime_url="http://retired/api", generation=4)
        state.dispatches[1]["metadata"]["parent_session_id"] = "middle"
        state.dispatches.append(dispatch(9, session_id="middle", status="completed"))
        faults, _ = r.classify(state)
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0].evidence["lineage"]["parent_session_ids"], ["middle", "parent"])

    def test_runtime_parent_anchor_without_fleet_parent_receipt(self):
        state = snap(dispatches=[dispatch()])
        self.assertEqual(r.classify(state)[0][0].source_task_id, 1)

    def test_generation_conflicting_parent_ownership_is_not_guessed(self):
        state = snap(sessions=[])
        state.dispatches.append(dispatch(10, session_id="parent", task_id=99, generation=2, metadata={}))
        faults, waits = r.classify(state)
        self.assertEqual(faults, [])
        self.assertTrue(any("conflicting" in w["reason"] for w in waits))

    def test_missing_and_cyclic_lineage_remain_visible_without_false_attribution(self):
        for parent in ("missing", "cycle"):
            with self.subTest(parent=parent):
                state = snap(sessions=[], dispatches=[dispatch(metadata={"parent_session_id": parent})])
                if parent == "cycle":
                    state.dispatches.append(dispatch(10, session_id="cycle", status="completed",
                        metadata={"parent_session_id": "child"}))
                faults, waits = r.classify(state)
                self.assertEqual(faults, [])
                self.assertTrue(any("lineage" in w["reason"] for w in waits))

    def test_other_project_never_inherits_this_child(self):
        state = snap(project={"id": 2}, tasks=[fixture.task(2, project_id=2, status="done")])
        self.assertEqual(r.classify(state)[0], [])

    def test_distinct_failed_children_have_distinct_stable_incidents(self):
        state = snap()
        state.dispatches.append(dispatch(8, session_id="child-2"))
        faults, _ = r.classify(state)
        self.assertEqual(len({f.key for f in faults}), 2)
        self.assertEqual({f.subject for f in faults}, {"dispatch:7", "dispatch:8"})

    def test_newest_generation_supersedes_failed_receipt_without_mutating_it(self):
        state = snap()
        original = copy.deepcopy(state.dispatches[1])
        state.dispatches.append(dispatch(8, generation=2, status="running"))
        state.sessions.append(fixture.session(id="child", task=None, generation=2))
        self.assertEqual(r.classify(state)[0], [])
        self.assertEqual(state.dispatches[1], original)
        state.sessions[-1].update(generation=1)
        state.dispatches[-1]["updated_at"] = NOW-1000
        faults, _ = r.classify(state)
        self.assertEqual([f.subject for f in faults], ["dispatch:8"])
        self.assertEqual(faults[0].evidence["observation"], "dispatch_without_live_session")

    def test_queued_child_grace_and_unreachable_endpoint_do_not_prove_missing(self):
        state = snap()
        child = state.dispatches[1]
        child.update(status="launching", updated_at=NOW-1)
        self.assertEqual(r.classify(state)[0], [])
        child["updated_at"] = NOW-1000
        self.assertEqual(r.classify(state)[0][0].evidence["observation"], "dispatch_without_live_session")
        state.endpoints["http://worker/api"] = False
        self.assertEqual([f.kind for f in r.classify(state)[0]], ["runtime_unreachable"])
        child["status"] = "failed"
        self.assertIn("child_execution_fault", [f.kind for f in r.classify(state)[0]],
                      "a durable failed dispatch is known even when its runtime cannot be read")

    def test_unknown_child_effect_keeps_same_incident_key_and_completed_results(self):
        state = snap()
        first = r.classify(state)[0][0]
        child = fixture.session(id="child", task=None, generation=1, status="needs_resolution",
            driver_running=False, tool_receipts=[{"call_id": "done", "status": "succeeded"},
                                                {"call_id": "write", "status": "unknown"}])
        state.sessions.append(child)
        later = r.classify(state)[0][0]
        self.assertEqual(first.key, later.key)
        self.assertEqual(later.evidence["observation"], "unknown_tool_effect")
        self.assertEqual(later.evidence["tool_receipts"], child["tool_receipts"])

    def test_child_stop_completed_and_pause_are_not_restarted(self):
        for status in ("stopped", "cancelled", "completed"):
            with self.subTest(status=status):
                state = snap()
                state.dispatches[1]["status"] = status
                self.assertEqual(r.classify(state)[0], [])
        state = snap()
        state.sessions.append(fixture.session(id="child", task=None, generation=1,
                                              status="stopped", driver_running=False))
        self.assertEqual(r.classify(state)[0], [])
        state = snap()
        state.agents["engineer"]["lifecycle_intent"] = "paused"
        self.assertEqual(r.classify(state)[0], [])

    def test_explicit_resolution_is_bound_to_generation_and_requires_evidence(self):
        state = snap()
        resolution = {"generation": 1, "disposition": "resolved", "cause": "Git route refused",
                      "fix": "tested source revision", "regression": "real clone and receipt proof"}
        state.dispatches[1]["metadata"]["recovery_resolution"] = resolution
        faults, waits = r.classify(state)
        self.assertEqual(faults, [])
        self.assertTrue(any("reconciled" in w["reason"] for w in waits))
        for bad in ({**resolution, "generation": 0}, {**resolution, "regression": ""},
                    {**resolution, "disposition": "pending"}):
            state.dispatches[1]["metadata"]["recovery_resolution"] = bad
            self.assertEqual(len(r.classify(state)[0]), 1)

    def test_completed_parent_does_not_clear_unobservable_child_fault(self):
        state = snap()
        state.dispatches[1]["status"] = "running"
        state.sessions.append(fixture.session(id="child", task=None, generation=1,
            status="needs_resolution", driver_running=False))
        fault = r.classify(state)[0][0]
        state.sessions = state.sessions[:1]
        state.endpoints["http://worker/api"] = False
        self.assertFalse(r.fault_observable(state, fault))

    def test_resolution_never_hides_observed_unknown_effect_or_newer_live_generation(self):
        state = snap()
        state.dispatches[1]["metadata"]["recovery_resolution"] = {
            "generation": 1, "disposition": "resolved", "cause": "old cause",
            "fix": "old fix", "regression": "old regression"}
        for gen, status in ((1, "needs_resolution"), (2, "failed")):
            with self.subTest(generation=gen, status=status):
                state.sessions = [fixture.session(id="child", task=None, generation=gen,
                    status=status, driver_running=False, tool_receipts=[{"status": "unknown"}])]
                faults, _ = r.classify(state)
                self.assertEqual(len(faults), 1)
                self.assertEqual(faults[0].evidence["observation"], "unknown_tool_effect")

    def test_conflicting_lineage_does_not_clear_a_previously_owned_incident(self):
        state = snap()
        fault = r.classify(state)[0][0]
        state.dispatches.append(dispatch(10, session_id="parent", task_id=99, generation=2, metadata={}))
        self.assertEqual(r.classify(state)[0], [])
        self.assertFalse(r.fault_observable(state, fault))

    def test_parent_policy_hold_is_explicit_not_just_parent_completion(self):
        state = snap()
        state.tasks[0]["metadata"] = {"recovery": {"wait": {
            "verified": True, "reason": "operator investigating", "until": r.iso(NOW+60)}}}
        self.assertEqual(r.classify(state)[0], [])
        state.now += 61
        self.assertEqual(len(r.classify(state)[0]), 1)


class ChildJournalTests(unittest.TestCase):
    def test_existing_child_incident_survives_ambiguous_receipts_restart_and_attempted_closure(self):
        for ambiguity in ("runtime", "dispatch", "newer-dispatch"):
            with self.subTest(ambiguity=ambiguity), tempfile.TemporaryDirectory() as directory:
                board = fixture.FakeBoard()
                board.tasks[1]["status"] = "done"
                state = snap()
                original = copy.deepcopy(state.dispatches[1])
                path = Path(directory) / "recovery.sqlite"
                journal = r.Journal(path)
                try:
                    r.Controller(board, journal).tick(state)
                    row = journal.rows()[0]
                    fault = r.Fault(**json.loads(row["payload"]))
                    repair = board.tasks[row["task_id"]]
                    repair["metadata"]["recovery_resolution"] = {
                        "cause": "provisioning refusal", "fix": "source revision",
                        "regression": "clone proof; child reconciliation still pending"}
                    if ambiguity == "runtime":
                        state.sessions.extend([
                            fixture.session(id="child", task=None, generation=1,
                                status="failed", driver_running=False, runtime_url=url)
                            for url in ("http://worker/api", "http://other/api")])
                    elif ambiguity == "dispatch":
                        state.dispatches.append(dispatch(8))
                    else:
                        state.dispatches.extend([dispatch(8, generation=2, status="running"),
                                                  dispatch(9, generation=2, status="running")])
                    self.assertEqual(r.classify(state)[0], [])
                    # Reopen from the durable observation twice. Neither restart
                    # nor a fresh closure timestamp may turn uncertainty absent.
                    for offset in (10, 20):
                        repair.update(status="done", updated_at=r.iso(NOW+offset))
                        state.now = NOW+offset+1
                        journal.close()
                        journal = r.Journal(path)
                        r.Controller(board, journal).tick(state)
                        self.assertTrue(journal.present(fault.key))
                        self.assertEqual(repair["status"], "in_progress")
                        self.assertNotEqual(journal.rows()[0]["state"], "closed")
                    self.assertFalse(r.fault_observable(state, fault),
                                     "ambiguous current state cannot clear a retained terminal failure")
                    self.assertEqual(len(board.task_keys), 1)
                    self.assertEqual(len(board.dispatches), 1)
                    self.assertEqual(state.dispatches[1], original)
                    self.assertFalse(any("symptom-cleared" in key for key in board.comments))
                    self.assertFalse(any("/sessions" in p for _, p, _ in board.calls))
                finally:
                    journal.close()

    def test_child_failure_restart_and_resolution_preserve_one_incident_and_original_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            board = fixture.FakeBoard()
            board.tasks[1]["status"] = "done"
            state = snap()
            originals = copy.deepcopy(state.dispatches)
            path = Path(directory) / "recovery.sqlite"
            journal = r.Journal(path)
            try:
                controller = r.Controller(board, journal)
                controller.tick(state)
                self.assertEqual(len(journal.rows()), 1)
                row = journal.rows()[0]
                repair_id = row["task_id"]
                frozen = list(journal.connection.execute("SELECT key,body FROM outbox ORDER BY key"))
                journal.close()
                journal = r.Journal(path)
                controller = r.Controller(board, journal)
                for offset in range(5):
                    state.now = NOW+offset+1
                    controller.tick(state)
                self.assertEqual(len(board.task_keys), 1)
                self.assertEqual(len(board.dispatches), 1)
                self.assertEqual(state.dispatches, originals)
                self.assertEqual([tuple(x) for x in journal.connection.execute(
                    "SELECT key,body FROM outbox ORDER BY key")], [tuple(x) for x in frozen])
                repair = board.tasks[repair_id]
                resolution = {"generation": 1, "disposition": "resolved", "cause": "bad Git route",
                              "fix": "source and deployment", "regression": "clone proof"}
                repair.update(status="done", updated_at=r.iso(NOW+10))
                repair["metadata"]["recovery_resolution"] = resolution
                controller.tick(state)
                self.assertEqual(repair["status"], "in_progress", "parent done is not child reconciliation")
                state.dispatches[1]["metadata"]["recovery_resolution"] = resolution
                repair.update(status="done", updated_at=r.iso(NOW+11))
                controller.tick(state)
                self.assertEqual(journal.rows()[0]["state"], "closed")
                self.assertFalse(any("/sessions" in p for _, p, _ in board.calls))
                self.assertFalse(any(p.startswith("/fleet/dispatches/") for _, p, _ in board.calls))
            finally:
                journal.close()


if __name__ == "__main__":
    unittest.main()
