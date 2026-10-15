import copy
import tomllib
import unittest

import capability_dag as dag
import planning_check as planning


class PlanningCheckTests(unittest.TestCase):
    def test_generated_citations_preserve_capability_associations_deterministically(self):
        ref = {"file": "spec/a.md", "section": "rule", "sentence": "A MUST work."}
        nodes = [{"id": "B", "requirements": [ref]}, {"id": "A", "requirements": [ref]}]
        rendered = planning.citations(nodes)
        self.assertEqual(rendered, planning.citations(list(reversed(nodes))))
        self.assertEqual(rendered.count("//# A MUST work."), 2)
        self.assertIn("reason=Capability A;", rendered)
        self.assertIn("reason=Capability B;", rendered)
        self.assertNotIn("type=test", rendered)

    def test_config_retains_selection_and_excludes_runtime_and_old_reports(self):
        original = {"$schema": "https://awslabs.github.io/duvet/config/v0.4.0.json",
                    "specification": [{"source": "constitution.md", "format": "markdown"}],
                    "source": [{"pattern": "components/**/*.rs"}],
                    "requirement": [{"pattern": "unsafe-old-sidecar.toml"}],
                    "report": {"snapshot": {"enabled": True}}}
        result = tomllib.loads(planning.planning_config(original))
        self.assertEqual(result["$schema"], original["$schema"])
        self.assertEqual(result["specification"], original["specification"])
        self.assertEqual(result["source"], [{"pattern": "generated/planning-citations.txt", "type": "implementation"}])
        self.assertNotIn("requirement", result)
        self.assertFalse(result["report"]["snapshot"]["enabled"])
        self.assertEqual(result["report"]["json"]["path"], "reports/planning.json")

    def fixture(self):
        return {"annotations": [{"type": "SPEC"} for _ in range(3)] +
                [{"source": "generated/planning-citations.txt"} for _ in range(3)],
                "statuses": {"0": {"spec": 10, "citation": 10, "related": [3, 4]},
                             "1": {"spec": 10, "citation": 5},
                             "2": {"spec": 10, "test": 10}}}

    def test_native_byte_coverage_not_test_presence_or_duplicate_count(self):
        result = planning.native_metrics(self.fixture())
        self.assertEqual((result["covered"], result["total"], result["partial"], result["unmapped"]), (1, 3, 1, 1))
        self.assertEqual(result["unmatched_citation_indices"], [5])
        self.assertEqual(result["missing_full_association"], 2)

    def test_runtime_and_test_annotations_are_rejected(self):
        for annotation in [{"type": "TEST"}, {"source": "components/logic.rs"}]:
            data = copy.deepcopy(self.fixture())
            data["annotations"].append(annotation)
            with self.assertRaisesRegex(ValueError, "non-planning"):
                planning.native_metrics(data)

    def test_duplicate_within_capability_reuses_existing_parser_rejection(self):
        text = "\n".join(["ID: A", "Title: A", "Prerequisites: none", "Deliverable: A",
                          "Acceptance: A", "Source: spec/a.md#rule | A MUST work.",
                          "Source: spec/a.md#rule | A MUST work."])
        with self.assertRaisesRegex(ValueError, "duplicate source"):
            dag.parse(text)

    def test_invalid_native_denominator_is_not_zero_percent_success(self):
        data = self.fixture()
        data["statuses"]["0"] = {}
        with self.assertRaisesRegex(ValueError, "coverage size"):
            planning.native_metrics(data)


if __name__ == "__main__":
    unittest.main()
