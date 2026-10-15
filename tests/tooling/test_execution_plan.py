"""Failure-oriented tests for declared milestone readiness, not runtime behavior."""
import copy
from contextlib import redirect_stderr
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import execution_plan as plan
import spec_set


def stage(identity="S1", order=1):
    return {
        "id": identity,
        "order": order,
        "heading": "Stage " + str(order) + ": Reviewed Contract",
        "approval": "approved",
        "approval_ref": "review:stage-" + identity,
    }


def capability(identity="A", prerequisites=()):
    return {
        "id": identity,
        "prerequisites": list(prerequisites),
        "approval": "approved",
        "approval_ref": "review:capability-" + identity,
    }


def milestone(identity="M1", capability_id="A", stage_id="S1", **overrides):
    value = {
        "id": identity,
        "capability": capability_id,
        "stage": stage_id,
        "title": "Observable milestone " + identity,
        "acceptance": "The recorded recovery check preserves the committed value.",
        "provides": ["READY"],
        "requires": [],
        "owns": ["tools/" + identity + ".py"],
        "gates": [{"id": "RECOVERY", "expectation": "Recovery check passes."}],
        "approval": "approved",
        "approval_ref": "review:milestone-" + identity,
    }
    value.update(overrides)
    return value


def toml_value(value):
    if isinstance(value, dict):
        return "{ " + ", ".join(key + " = " + toml_value(item)
                                  for key, item in value.items()) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    return json.dumps(value)


def declarations(kind, entries):
    blocks = []
    for entry in entries:
        blocks.append("[[" + kind + "]]\n" + "\n".join(
            key + " = " + toml_value(value) for key, value in entry.items()))
    body = "\n\n".join(blocks) if blocks else kind + " = []"
    headings = ""
    if kind == "stages":
        headings = "\n\n".join("### " + entry["heading"] for entry in entries
                                 if "heading" in entry)
    return headings + "\n\n```duvet-cap-" + kind + "\n" + body + "\n```\n"


class ExecutionPlanSchemaTests(unittest.TestCase):
    def test_stage_heading_must_reference_an_actual_heading(self):
        text = declarations("stages", [stage()])
        self.assertEqual(plan.parse_stages(text)[0]["id"], "S1")
        for broken in [text.replace("### Stage 1:", "## Stage 1:", 1),
                       text.replace("### Stage 1: Reviewed Contract", "A prose mention only", 1)]:
            with self.subTest(text=broken), self.assertRaises(ValueError):
                plan.parse_stages(broken)

    def test_stages_require_one_well_formed_declaration_block(self):
        valid = declarations("stages", [stage()])
        for broken in ["# No stages yet\n", valid + valid,
                       "```duvet-cap-stages\n[[stages]]\nid = [\n```\n",
                       valid.rsplit("```", 1)[0]]:
            with self.subTest(text=broken), self.assertRaises(ValueError):
                plan.parse_stages(broken)

    def test_hidden_declaration_blocks_do_not_create_or_duplicate_authored_records(self):
        for kind, parser, item in [("stages", plan.parse_stages, stage()),
                                   ("milestones", plan.parse_milestones, milestone())]:
            valid = declarations(kind, [item])
            for hidden in ["<!--\n" + valid + "-->\n",
                           "````text\n" + valid + "````\n",
                           "~~~text\n" + valid + "~~~\n"]:
                with self.subTest(kind=kind, hidden=hidden):
                    self.assertEqual(parser(hidden + valid), parser(valid))
                    if kind == "milestones":
                        self.assertEqual(parser(hidden), [])
                    else:
                        with self.assertRaises(ValueError):
                            parser(hidden)

    def test_malformed_or_unterminated_declaration_fences_fail(self):
        valid = declarations("milestones", [milestone()])
        malformed = [valid.rsplit("```", 1)[0],
                     valid.rsplit("```", 1)[0] + "``\n",
                     valid.replace("```duvet-cap-milestones", "```duvet-cap-milestones unexpected"),
                     "<!--\n" + valid,
                     "````text\n" + valid]
        for text in malformed:
            with self.subTest(text=text), self.assertRaises(ValueError):
                plan.parse_milestones(text)

    def test_stage_ids_orders_and_schema_cannot_be_ambiguous(self):
        entries = [stage(), stage("S2", 2)]
        malformed = []
        for key in ["id", "order", "heading"]:
            item = stage()
            del item[key]
            malformed.append([item])
        for identity in ["", "s1", "S-1", "1S"]:
            malformed.append([stage(identity)])
        malformed += [[stage(), stage()], [stage(), stage("S2", 1)],
                      [dict(stage(), order=0)], [dict(stage(), order=True)],
                      [dict(stage(), unknown="not an extension point")]]
        for items in malformed:
            with self.subTest(items=items), self.assertRaises(ValueError):
                plan.parse_stages(declarations("stages", items))
        self.assertEqual(len(plan.parse_stages(declarations("stages", entries))), 2)

    def test_missing_milestone_block_and_explicit_empty_are_empty(self):
        self.assertEqual(plan.parse_milestones("# No approved milestones yet\n"), [])
        self.assertEqual(plan.parse_milestones(declarations("milestones", [])), [])

    def test_milestone_ids_fields_and_block_are_unambiguous(self):
        valid = declarations("milestones", [milestone()])
        self.assertEqual(plan.parse_milestones(valid)[0]["id"], "M1")
        malformed = []
        for key in ["id", "capability", "stage", "title", "acceptance",
                    "provides", "requires", "gates"]:
            item = milestone()
            del item[key]
            malformed.append([item])
        for field in ["id", "capability", "stage"]:
            malformed.append([milestone(**{field: "bad-id"})])
        malformed += [[milestone(), milestone()],
                      [milestone(title="")], [milestone(acceptance="")],
                      [milestone(unknown="silently ignored behavior")]]
        for items in malformed:
            with self.subTest(items=items), self.assertRaises(ValueError):
                plan.parse_milestones(declarations("milestones", items))
        with self.assertRaises(ValueError):
            plan.parse_milestones(valid + valid)

    def test_interface_and_gate_declarations_are_explicit_and_unique(self):
        malformed = [milestone(provides=["READY", "READY"]),
                     milestone(provides=["bad-id"]),
                     milestone(requires=["M2:READY", "M2:READY"]),
                     milestone(requires=["READY"]),
                     milestone(requires=["M2:READY:EXTRA"]),
                     milestone(gates=[]),
                     milestone(gates=[{"id": "G", "expectation": "A"},
                                      {"id": "G", "expectation": "B"}]),
                     milestone(gates=[{"id": "G"}]),
                     milestone(gates=[{"id": "G", "expectation": ""}])]
        for item in malformed:
            with self.subTest(item=item), self.assertRaises(ValueError):
                plan.parse_milestones(declarations("milestones", [item]))

    def test_invalid_approval_label_is_rejected_not_upgraded(self):
        for parser, kind, item in [(plan.parse_stages, "stages", stage()),
                                   (plan.parse_milestones, "milestones", milestone())]:
            for label in ["complete", "APPROVED", True, []]:
                with self.subTest(kind=kind, label=label), self.assertRaises(ValueError):
                    parser(declarations(kind, [dict(item, approval=label)]))

    def test_scope_is_conservative_and_explicit(self):
        for scope in ["interface", "integration"]:
            with self.subTest(scope=scope):
                self.assertEqual(len(plan.parse_milestones(declarations(
                    "milestones", [milestone(scope=scope)]))), 1)
        for scope in ["any", "", True, {}]:
            with self.subTest(scope=scope), self.assertRaises(ValueError):
                plan.parse_milestones(declarations("milestones", [milestone(scope=scope)]))

    def test_ownership_requires_canonical_concrete_relative_paths(self):
        for path in [".", "tools/*.py", "tools/?", "tools/[ab].py", "/tools/a.py",
                     "../tools/a.py", "tools/../a.py", "tools//a.py", "./tools/a.py"]:
            with self.subTest(path=path), self.assertRaises(ValueError):
                plan.parse_milestones(declarations("milestones", [milestone(owns=[path])]))

    def test_missing_approval_fields_remain_parseable_for_blocked_views(self):
        for parser, kind, original in [(plan.parse_stages, "stages", stage()),
                                       (plan.parse_milestones, "milestones", milestone())]:
            for missing in ["approval", "approval_ref"]:
                item = copy.deepcopy(original)
                del item[missing]
                with self.subTest(kind=kind, missing=missing):
                    self.assertEqual(len(parser(declarations(kind, [item]))), 1)


class ExecutionPlanReadinessTests(unittest.TestCase):
    def build(self, milestones=None, stages=None, capabilities=None, progress=None,
              draft=False):
        return plan.build_plan(
            capabilities if capabilities is not None else [capability()],
            stages if stages is not None else [stage()],
            milestones if milestones is not None else [milestone()],
            progress=progress, draft=draft)

    @staticmethod
    def view(result, identity):
        return next(item for item in result["milestones"] if item["id"] == identity)

    @staticmethod
    def receipt(status, evidence="artifacts/review/receipt.json"):
        return {"status": status, "evidence": evidence}

    def test_empty_approved_capability_set_never_invents_executable_work(self):
        result = self.build(capabilities=[], milestones=[])
        self.assertEqual(result["ready"], [])
        self.assertEqual(result["dependency_order"], [])
        self.assertEqual(result["parallel_ready_sets"], [])
        self.assertTrue(result["blockers"])
        self.assertFalse(result["execution_authorized"])

    def test_semantic_result_is_deterministic_and_source_order_invariant(self):
        stages = [stage(), stage("S2", 2)]
        milestones = [milestone("M3", stage_id="S2", requires=["M1:READY", "M2:READY"]),
                      milestone("M2"), milestone("M1")]
        original = copy.deepcopy((stages, milestones))
        first = self.build(milestones, stages)
        reordered = copy.deepcopy(list(reversed(milestones)))
        reordered[2]["requires"].reverse()
        second = self.build(reordered, list(reversed(stages)))
        self.assertEqual(first, second)
        self.assertEqual(first, self.build(milestones, stages))
        self.assertEqual(first["dependency_order"], ["M1", "M2", "M3"])
        self.assertEqual((stages, milestones), original)

    def test_unknown_capability_stage_and_interface_references_fail(self):
        malformed = [[milestone(capability_id="UNKNOWN")],
                     [milestone(stage_id="UNKNOWN")],
                     [milestone(requires=["UNKNOWN:READY"])],
                     [milestone("M1"), milestone("M2", requires=["M1:UNKNOWN"])]]
        for items in malformed:
            with self.subTest(items=items), self.assertRaises(ValueError):
                self.build(items)

    def test_unknown_capability_prerequisites_are_not_ignored(self):
        with self.assertRaises(ValueError):
            self.build(capabilities=[capability(prerequisites=["UNKNOWN"])])

    def test_cycles_fail_even_when_the_cycle_crosses_stages(self):
        stages = [stage(), stage("S2", 2)]
        for items in [[milestone(requires=["M1:READY"])],
                      [milestone("M1", requires=["M2:READY"]),
                       milestone("M2", stage_id="S2", requires=["M1:READY"])]]:
            with self.subTest(items=items), self.assertRaises(ValueError):
                self.build(items, stages)

    def test_explicit_backward_stage_dependency_is_allowed_when_acyclic(self):
        items = [milestone("M1", stage_id="S2"),
                 milestone("M2", stage_id="S1", requires=["M1:READY"])]
        progress = {"M1": {"interfaces": {"READY": self.receipt("ready")}}}
        result = self.build(items, [stage(), stage("S2", 2)], progress=progress)
        self.assertEqual(result["dependency_order"], ["M1", "M2"])
        self.assertTrue(self.view(result, "M2")["ready"])

    def test_unbound_capability_edge_is_visible_without_stopping_independent_work(self):
        result = self.build([milestone("M1"), milestone("M2", capability_id="B")],
                            capabilities=[capability(), capability("B", ["A"])])
        self.assertTrue(result["blockers"])
        self.assertTrue(self.view(result, "M1")["ready"])
        self.assertFalse(self.view(result, "M2")["ready"])
        self.assertTrue(self.view(result, "M2")["readiness_blockers"])

    def test_explicit_interface_scope_can_define_contract_before_consumer_integration(self):
        result = self.build([milestone("M1"), milestone("M2", capability_id="B", scope="interface")],
                            capabilities=[capability(), capability("B", ["A"])])
        self.assertTrue(self.view(result, "M1")["ready"])
        self.assertTrue(self.view(result, "M2")["ready"])
        self.assertFalse(self.view(result, "M2")["complete"])

    def test_capability_prerequisite_can_bind_through_explicit_interface_ancestry(self):
        items = [milestone("M1", capability_id="A"),
                 milestone("M2", capability_id="B", requires=["M1:READY"]),
                 milestone("M3", capability_id="C", requires=["M2:READY"])]
        progress = {identity: {"interfaces": {"READY": self.receipt("ready")}}
                    for identity in ["M1", "M2"]}
        result = self.build(items,
                            capabilities=[capability(), capability("B", ["A"]),
                                          capability("C", ["A"])], progress=progress)
        self.assertTrue(self.view(result, "M3")["ready"])

    def test_later_stage_independent_work_has_no_global_stage_barrier(self):
        items = [milestone("M1"), milestone("M2", stage_id="S2"),
                 milestone("M3", stage_id="S2", requires=["M1:READY"])]
        result = self.build(items, [stage(), stage("S2", 2)])
        self.assertEqual(result["ready"], ["M1", "M2"])
        self.assertEqual(result["parallel_ready_sets"], [["M1", "M2"]])
        self.assertTrue(self.view(result, "M3")["readiness_blockers"])
        self.assertFalse(result["execution_authorized"])

    def test_verified_interface_receipt_unblocks_without_full_producer_completion(self):
        items = [milestone("M1"), milestone("M2", stage_id="S2", requires=["M1:READY"])]
        progress = {"M1": {"interfaces": {"READY": self.receipt("ready")}}}
        result = self.build(items, [stage(), stage("S2", 2)], progress=progress)
        self.assertTrue(self.view(result, "M2")["ready"])
        self.assertFalse(self.view(result, "M1")["complete"])
        self.assertTrue(self.view(result, "M1")["completion_blockers"])

    def test_missing_or_not_ready_interface_never_unblocks_a_consumer(self):
        items = [milestone("M1"), milestone("M2", requires=["M1:READY"])]
        for progress in [None,
                         {"M1": {"interfaces": {"READY": self.receipt("unknown")}}},
                         {"M1": {"interfaces": {"READY": self.receipt("not_ready")}}}]:
            with self.subTest(progress=progress):
                result = self.build(items, progress=progress)
                self.assertFalse(self.view(result, "M2")["ready"])
                self.assertTrue(self.view(result, "M2")["readiness_blockers"])

    def test_missing_and_failed_gate_evidence_are_distinct_not_complete(self):
        missing = self.view(self.build(), "M1")
        self.assertFalse(missing["complete"])
        self.assertEqual(missing["gates"][0]["status"], "missing")
        self.assertTrue(missing["completion_blockers"])
        failed = self.view(self.build(progress={"M1": {"gates": {
            "RECOVERY": self.receipt("fail")}}}), "M1")
        self.assertFalse(failed["complete"])
        self.assertEqual(failed["gates"][0]["status"], "fail")
        self.assertEqual(failed["gates"][0]["evidence"], "artifacts/review/receipt.json")
        self.assertTrue(failed["completion_blockers"])
        self.assertNotEqual(missing["completion_blockers"], failed["completion_blockers"])

    def test_all_declared_gates_are_required_for_completion(self):
        items = [milestone(gates=[{"id": "RECOVERY", "expectation": "Recovery passes."},
                                  {"id": "PARITY", "expectation": "Probe agrees."}])]
        progress = {"M1": {"interfaces": {"READY": self.receipt("ready")},
                            "gates": {"RECOVERY": self.receipt("pass")}}}
        self.assertFalse(self.view(self.build(items, progress=progress), "M1")["complete"])
        progress["M1"]["gates"]["PARITY"] = self.receipt("pass")
        result = self.build(items, progress=progress)
        self.assertTrue(self.view(result, "M1")["complete"])
        self.assertFalse(result["execution_authorized"])

    def test_progress_cannot_create_unknown_milestones_interfaces_or_gates(self):
        malformed = [{"UNKNOWN": {}},
                     {"M1": {"interfaces": {"UNKNOWN": self.receipt("ready")}}},
                     {"M1": {"gates": {"UNKNOWN": self.receipt("pass")}}},
                     {"M1": {"gates": {"RECOVERY": self.receipt("ready")}}},
                     {"M1": {"interfaces": {"READY": self.receipt("pass")}}}]
        for progress in malformed:
            with self.subTest(progress=progress), self.assertRaises(ValueError):
                self.build(progress=progress)

    def test_ready_or_pass_progress_needs_a_nonempty_evidence_reference(self):
        for field, identity, status in [("interfaces", "READY", "ready"),
                                        ("gates", "RECOVERY", "pass")]:
            for receipt in [{"status": status}, self.receipt(status, ""),
                            self.receipt(status, "   ")]:
                with self.subTest(field=field, receipt=receipt):
                    items = [milestone("M1"), milestone("M2", requires=["M1:READY"])]
                    result = self.build(items, progress={"M1": {field: {identity: receipt}}})
                    if field == "interfaces":
                        self.assertFalse(self.view(result, "M2")["ready"])
                        self.assertTrue(self.view(result, "M2")["readiness_blockers"])
                    else:
                        self.assertFalse(self.view(result, "M1")["complete"])
                        self.assertTrue(self.view(result, "M1")["completion_blockers"])

    def test_absent_or_proposed_approval_blocks_each_declaration_level(self):
        for kind in ["capability", "stage", "milestone"]:
            for change in ["missing_approval", "missing_ref", "proposed"]:
                cap, st, item = capability(), stage(), milestone()
                selected = {"capability": cap, "stage": st, "milestone": item}[kind]
                if change == "missing_approval":
                    del selected["approval"]
                elif change == "missing_ref":
                    del selected["approval_ref"]
                else:
                    selected["approval"] = "proposed"
                with self.subTest(kind=kind, change=change):
                    result = self.build([item], [st], [cap])
                    self.assertEqual(result["ready"], [])
                    self.assertTrue(self.view(result, "M1")["readiness_blockers"])
                    self.assertFalse(self.view(result, "M1")["complete"])

    def test_receipt_cannot_turn_an_unapproved_provider_into_ready_dependency(self):
        items = [milestone("M1", approval="proposed"),
                 milestone("M2", requires=["M1:READY"])]
        progress = {"M1": {"interfaces": {"READY": self.receipt("ready")},
                            "gates": {"RECOVERY": self.receipt("pass")}}}
        result = self.build(items, progress=progress)
        self.assertFalse(self.view(result, "M2")["ready"])
        self.assertFalse(self.view(result, "M1")["complete"])

    def test_draft_view_cannot_grant_execution_readiness(self):
        result = self.build(draft=True, progress={"M1": {"gates": {
            "RECOVERY": self.receipt("pass")}}})
        self.assertEqual(result["ready"], [])
        self.assertFalse(self.view(result, "M1")["ready"])
        self.assertFalse(self.view(result, "M1")["complete"])
        self.assertTrue(result["blockers"])
        self.assertIn("unapproved-draft", json.dumps(result))
        self.assertFalse(result["execution_authorized"])

    def test_missing_ownership_blocks_instead_of_guessing_file_boundaries(self):
        for missing in [True, False]:
            item = milestone(owns=[])
            if missing:
                del item["owns"]
            with self.subTest(missing=missing):
                result = self.build([item])
                self.assertFalse(self.view(result, "M1")["ready"])
                self.assertTrue(self.view(result, "M1")["readiness_blockers"])

    def test_overlapping_ready_ownership_never_shares_a_parallel_set(self):
        items = [milestone("M1", owns=["tools"]),
                 milestone("M2", owns=["tools/adapter.py"]),
                 milestone("M3", owns=["docs/review.md"])]
        result = self.build(items)
        self.assertEqual(result["ready"], ["M1", "M2", "M3"])
        flattened = [identity for group in result["parallel_ready_sets"] for identity in group]
        self.assertEqual(sorted(flattened), ["M1", "M2", "M3"])
        self.assertTrue(all(not {"M1", "M2"}.issubset(group)
                            for group in result["parallel_ready_sets"]))
        self.assertEqual(result, self.build(list(reversed(items))))


class SpecificationSetRoutingTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory) / "repo"
        documents = root / "spec"
        documents.mkdir(parents=True)
        main = documents / "implementation.md"
        capabilities = documents / "implementation-capabilities.md"
        phases = documents / "implementation-phases.md"
        capabilities.write_text("# Approved capabilities\nNo entries.\n")
        phases.write_text("# Approved phase structure\n")
        text = ("# Implementation Specification\n\n"
                "[Implementation Capability Specification](implementation-capabilities.md)\n\n"
                "[Phased Implementation Specification](implementation-phases.md)\n")
        main.write_text(text)
        return main, capabilities, phases, text

    def test_routing_pins_the_three_distinct_authoritative_documents(self):
        with tempfile.TemporaryDirectory() as directory:
            main, capabilities, phases, _ = self.fixture(directory)
            bundle = spec_set.load(main)
            self.assertEqual(bundle["paths"], {"main": str(main),
                                               "capabilities": str(capabilities),
                                               "phases": str(phases)})
            self.assertEqual(set(bundle["hashes"]), {"main", "capabilities", "phases"})
            self.assertEqual(bundle, spec_set.load(main))
            spec_set.verify(bundle)

    def test_links_hidden_in_comments_and_examples_are_not_authoritative(self):
        with tempfile.TemporaryDirectory() as directory:
            main, _, _, text = self.fixture(directory)
            for hidden in ["<!--\n" + text + "-->\n",
                           "````text\n" + text + "````\n",
                           "```text\n" + text + "```\n",
                           "\n".join("    " + line for line in text.splitlines()) + "\n"]:
                with self.subTest(hidden=hidden):
                    main.write_text(hidden + text)
                    self.assertEqual(set(spec_set.load(main)["paths"]),
                                     {"main", "capabilities", "phases"})
                    main.write_text(hidden)
                    with self.assertRaises(ValueError):
                        spec_set.load(main)

    def test_duplicate_missing_and_aliased_roles_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            main, _, _, text = self.fixture(directory)
            cap_link = "[Implementation Capability Specification](implementation-capabilities.md)"
            for altered in [text + cap_link + "\n", text.replace(cap_link, ""),
                            text.replace("(implementation-phases.md)", "(implementation-capabilities.md)"),
                            text.replace("(implementation-capabilities.md)", "(implementation.md)")]:
                with self.subTest(text=altered):
                    main.write_text(altered)
                    with self.assertRaises(ValueError):
                        spec_set.load(main)

    def test_link_cannot_escape_the_specification_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            main, _, _, text = self.fixture(directory)
            outside = Path(directory) / "outside.md"
            outside.write_text("Not part of the authoritative repository.\n")
            symlink = main.parent / "outside-link.md"
            symlink.symlink_to(outside)
            for target in ["../../outside.md", "outside-link.md", str(outside),
                           "https://example.com/capabilities.md",
                           "implementation-capabilities.md#other", "missing.md"]:
                with self.subTest(target=target):
                    main.write_text(text.replace("(implementation-capabilities.md)",
                                                 "(" + target + ")"))
                    with self.assertRaises(ValueError):
                        spec_set.load(main)

    def test_verify_catches_linked_content_and_role_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            main, capabilities, phases, text = self.fixture(directory)
            bundle = spec_set.load(main)
            previous = capabilities.read_bytes()
            capabilities.write_bytes(previous + b"A new declaration.\n")
            with self.assertRaises(ValueError):
                spec_set.verify(bundle)
            capabilities.write_bytes(previous)
            spec_set.verify(bundle)
            main.write_text(text.replace("(implementation-capabilities.md)", "(swapped.md)")
                            .replace("(implementation-phases.md)", "(implementation-capabilities.md)")
                            .replace("(swapped.md)", "(implementation-phases.md)"))
            with self.assertRaises(ValueError):
                spec_set.verify(bundle)
            main.write_text(text)
            phases.unlink()
            with self.assertRaises(ValueError):
                spec_set.verify(bundle)

    def test_external_declarations_require_explicit_proposal_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            main, capabilities, phases, _ = self.fixture(directory)
            bundle = spec_set.load(main)
            self.assertEqual(spec_set.declaration_source(main, bundle), capabilities)
            self.assertEqual(spec_set.declaration_source(capabilities, bundle), capabilities)
            with self.assertRaises(ValueError):
                spec_set.declaration_source(phases, bundle)
            self.assertEqual(spec_set.declaration_source(phases, bundle, proposal=True), phases)

    def test_draft_cli_rejects_progress_before_native_report_or_output_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            main, _, _, _ = self.fixture(directory)
            output = Path(directory) / "generated"
            for flag in ["--proposal-capabilities", "--proposal-milestones"]:
                arguments = ["execution_plan.py", "--main-spec", str(main),
                             "--duvet-metrics", str(Path(directory) / "metrics.json"),
                             "--output-dir", str(output),
                             flag, str(Path(directory) / "proposal.md"),
                             "--progress", str(Path(directory) / "progress.json")]
                errors = StringIO()
                with self.subTest(flag=flag), patch.object(plan.sys, "argv", arguments), \
                        patch.object(plan.dag, "load_duvet") as native_report, \
                        redirect_stderr(errors):
                    self.assertEqual(plan.main(), 2)
                    native_report.assert_not_called()
                self.assertIn("draft views do not accept progress overlays", errors.getvalue())
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
