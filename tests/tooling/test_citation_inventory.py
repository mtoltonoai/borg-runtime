"""Behavior tests for the local inventory; these are not Borg runtime evidence."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import citation_inventory as inventory


SPEC = """# Example

> The capitalized MUST keyword in a header is descriptive here.

## Stable Heading

A writer MUST commit its state and
cursor atomically.

A writer SHOULD report its latency.

## Stable Heading

A reader MUST reject an invalid value.
"""
ATOMIC = "A writer MUST commit its state and cursor atomically."
LATENCY = "A writer SHOULD report its latency."
INVALID = "A reader MUST reject an invalid value."


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.snapshot = self.root / "spec-snapshot"
        self.snapshot.mkdir()
        self.write_snapshot("spec/core/example.md", SPEC)
        self.write_snapshot("spec/decisions/example/disabled.md", "# Disabled\n\n## Clause\n\nA server MUST refuse a call.\n")
        self.write_snapshot(".duvet/config.toml", """[[source]]
pattern = "components/**/*.rs"

[[specification]]
source = "spec/core/example.md"
format = "markdown"

# [[specification]]
# source = "spec/decisions/example/disabled.md"
# format = "markdown"
""")
        self.lock_snapshot()

    def write_snapshot(self, name, contents):
        path = self.snapshot / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def lock_snapshot(self):
        files = {
            path.relative_to(self.snapshot).as_posix(): inventory.digest(path.read_bytes())
            for path in self.snapshot.rglob("*") if path.is_file()
        }
        self.lock = {
            "commit": "example-commit",
            "files": files,
            "snapshot_sha256": inventory.manifest_digest(files),
        }
        (self.root / "spec-lock.json").write_text(json.dumps(self.lock))

    def source(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    @staticmethod
    def cite(sentence=ATOMIC, section="stable-heading", test=False, source="spec/core/example.md"):
        marker = "//= type=test\n" if test else ""
        return f"//= {source}#{section}\n{marker}//# {sentence}\nfn witness() {{}}\n"

    def test_paired_presence_and_multiline_quote(self):
        self.source("components/kernel/src/lib.rs", self.cite())
        self.source("components/kernel/tests/state.rs", self.cite("A writer MUST commit its state and\n//# cursor atomically.", test=True))
        result = inventory.inventory(self.root)
        self.assertEqual(result["totals"], {
            "total": 3, "absolute": 2, "paired_presence": 1, "absolute_presence_gaps": 1,
        })
        self.assertEqual(result["citation_errors"], [])
        requirement = result["requirements"][0]
        self.assertEqual(requirement["test_citations"], ["components/kernel/tests/state.rs:1"])
        self.assertFalse(requirement["execution_witnessed"])
        self.assertFalse(result["promotion_allowed"])

    def test_test_filename_is_not_execution_or_test_kind_evidence(self):
        self.source("tests/unwitnessed.rs", self.cite())
        result = inventory.inventory(self.root)
        requirement = result["requirements"][0]
        self.assertEqual(requirement["test_citations"], [])
        self.assertEqual(requirement["implementation_citations"], ["tests/unwitnessed.rs:1"])
        self.assertEqual(result["totals"]["paired_presence"], 0)

    def test_stale_sentence_reports_source_location(self):
        self.source("src/lib.rs", "// ordinary comment\n" + self.cite(ATOMIC.replace("atomically", "separately")))
        result = inventory.inventory(self.root)
        self.assertEqual(result["totals"]["paired_presence"], 0)
        self.assertEqual(result["citation_errors"][0]["location"], "src/lib.rs:2")
        self.assertIn("stale", result["citation_errors"][0]["reason"])

    def test_disabled_outcome_citation_fails(self):
        self.source("src/lib.rs", self.cite("A server MUST refuse a call.", "clause", source="spec/decisions/example/disabled.md"))
        result = inventory.inventory(self.root)
        self.assertEqual(len(result["selected_specifications"]), 1)
        self.assertIn("unselected specification", result["citation_errors"][0]["reason"])

    def test_duplicate_headings_have_distinct_slugs(self):
        self.source("src/lib.rs", self.cite(INVALID, "stable-heading-1"))
        self.source("tests/check.rs", self.cite(INVALID, "stable-heading-1", test=True))
        result = inventory.inventory(self.root)
        requirement = next(row for row in result["requirements"] if row["sentence"] == INVALID)
        self.assertEqual(requirement["section"], "stable-heading-1")
        self.assertTrue(requirement["paired_presence"])
        self.assertEqual(result["citation_errors"], [])

    def test_punctuation_slugs_match_duvet_instead_of_joining_words(self):
        self.assertEqual(inventory.heading_slug("A Tenant's Keys"), "a-tenant-s-keys")
        self.assertEqual(inventory.heading_slug("I. Minimal And Concrete"), "i-minimal-and-concrete")
        self.assertEqual(inventory.heading_slug(" The Checker's -- Own Record "), "the-checker-s-own-record")

    def test_missing_quote_and_orphan_type_are_errors(self):
        self.source("src/lib.rs", "//= spec/core/example.md#stable-heading\nfn f() {}\n//= type=test\n")
        errors = inventory.inventory(self.root)["citation_errors"]
        self.assertEqual(len(errors), 2)
        self.assertEqual(errors[0]["location"], "src/lib.rs:1")
        self.assertEqual(errors[1]["location"], "src/lib.rs:3")

    def test_specification_byte_change_is_rejected(self):
        self.write_snapshot("spec/core/example.md", SPEC + "\nchanged\n")
        with self.assertRaisesRegex(inventory.InventoryError, "snapshot bytes changed"):
            inventory.inventory(self.root)

    def test_gate_configuration_change_is_rejected(self):
        self.write_snapshot(".duvet/config.toml", "")
        with self.assertRaisesRegex(inventory.InventoryError, "snapshot bytes changed"):
            inventory.inventory(self.root)

    def test_missing_snapshot_file_is_rejected(self):
        (self.snapshot / "spec/core/example.md").unlink()
        with self.assertRaisesRegex(inventory.InventoryError, "missing=.*example.md"):
            inventory.inventory(self.root)

    def test_unpinned_snapshot_file_is_rejected(self):
        self.write_snapshot("extra.md", "not in manifest")
        with self.assertRaisesRegex(inventory.InventoryError, "extra=.*extra.md"):
            inventory.inventory(self.root)

    def test_manifest_aggregate_mismatch_is_rejected(self):
        self.lock["snapshot_sha256"] = "0" * 64
        (self.root / "spec-lock.json").write_text(json.dumps(self.lock))
        with self.assertRaisesRegex(inventory.InventoryError, "does not match its manifest"):
            inventory.inventory(self.root)

    def test_snapshot_symlink_is_rejected(self):
        path = self.snapshot / "spec/core/example.md"
        path.unlink()
        target = self.root / "outside.md"
        target.write_text(SPEC)
        path.symlink_to(target)
        with self.assertRaisesRegex(inventory.InventoryError, "symlink"):
            inventory.inventory(self.root)

    def test_noncanonical_manifest_path_is_rejected(self):
        self.lock["files"]["../outside.md"] = "0" * 64
        self.lock["snapshot_sha256"] = inventory.manifest_digest(self.lock["files"])
        (self.root / "spec-lock.json").write_text(json.dumps(self.lock))
        with self.assertRaisesRegex(inventory.InventoryError, "noncanonical relative path"):
            inventory.inventory(self.root)

    def test_selection_must_be_pinned_even_if_file_missing(self):
        config = self.snapshot / ".duvet/config.toml"
        config.write_text(config.read_text().replace("spec/core/example.md", "spec/core/unpinned.md"))
        self.lock_snapshot()
        with self.assertRaisesRegex(inventory.InventoryError, "selected specification not pinned"):
            inventory.inventory(self.root)

    def test_keyword_prose_and_code_fences_do_not_create_requirements(self):
        self.write_snapshot("spec/core/example.md", SPEC + "\n```\nA fake MUST not count.\n```\n")
        self.lock_snapshot()
        self.assertEqual(inventory.inventory(self.root)["totals"]["total"], 3)

    def test_bundled_normative_paragraph_is_rejected(self):
        self.write_snapshot("spec/core/example.md", "# Example\n\n## Clause\n\nA writer MUST persist and MUST report.\n")
        self.lock_snapshot()
        with self.assertRaisesRegex(inventory.InventoryError, "one key word"):
            inventory.inventory(self.root)

    def test_strict_mode_refuses_even_complete_citation_presence(self):
        implementation = ""
        tests = ""
        for sentence, section in [(ATOMIC, "stable-heading"), (LATENCY, "stable-heading"), (INVALID, "stable-heading-1")]:
            implementation += self.cite(sentence, section)
            tests += self.cite(sentence, section, test=True)
        self.source("src/lib.rs", implementation)
        self.source("tests/check.rs", tests)
        result = inventory.inventory(self.root)
        self.assertEqual(result["totals"]["absolute_presence_gaps"], 0)
        self.assertEqual(result["totals"]["paired_presence"], 3)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(inventory.main(["--root", str(self.root)]), 0)
            self.assertEqual(inventory.main(["--root", str(self.root), "--strict"]), 1)
        self.assertIn("Promotion: BLOCKED", output.getvalue())

    def test_cli_missing_snapshot_and_stale_citation_fail(self):
        self.source("src/lib.rs", self.cite("A writer MUST change the text."))
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(inventory.main(["--root", str(self.root)]), 1)
            (self.root / "spec-lock.json").unlink()
            self.assertEqual(inventory.main(["--root", str(self.root)]), 2)

    def test_json_report_includes_exact_requirement_and_configuration(self):
        output = self.root / "report.json"
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(inventory.main(["--root", str(self.root), "--json", str(output)]), 0)
        result = json.loads(output.read_text())
        self.assertEqual(result["requirements"][0]["sentence"], ATOMIC)
        self.assertEqual(result["gate_configuration_sha256"], self.lock["files"][".duvet/config.toml"])
        self.assertEqual(result["requirements"][0]["governing_classification"], "unreviewed")


if __name__ == "__main__":
    unittest.main()
