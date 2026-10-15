"""Review-ready wording is not an authorization input; external state is bound."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import capability_dag as dag
import execution_plan as ep
import spec_set


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReviewMetadataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'v1'
        self.root.mkdir()
        self.main = self.root / 'implementation.md'
        self.cap = self.root / 'cap.md'
        self.phase = self.root / 'phase.md'
        self.record = self.root / 'decisions.md'
        self.meta = self.root / spec_set.REVIEW_FILE
        self.main.write_text('[Implementation Capability Specification](cap.md)\n'
                             '[Phased Implementation Specification](phase.md)\n')
        self.cap.write_text('ID: A\nTitle: A\nPrerequisites: none\n'
                            'Deliverable: The intended implementation MUST work.\n'
                            'Acceptance: Defined behavior.\n'
                            'Source: source.md#rule | A MUST work.\n')
        self.phase.write_text('### Stage One\n\n```duvet-cap-stages\n'
                              '[[stages]]\nid="S1"\norder=1\nheading="Stage One"\n```\n'
                              '```duvet-cap-milestones\n[[milestones]]\nid="A1"\n'
                              'capability="A"\nstage="S1"\ntitle="Deliver A"\n'
                              'acceptance="The implementation MUST work."\n'
                              'scope="interface"\nprovides=["DATA"]\nrequires=[]\n'
                              'owns=[]\ngates=[{id="WORKS",expectation="Check behavior."}]\n```\n')
        self.record.write_text('# Decisions\n\n## Scoped Decision\nHistorical evidence.\n')
        self.data = {'schema_version': 1, 'source_hashes': {}, 'provenance': {},
                     'capabilities': {'A': {'approval': 'proposed', 'approval_ref': ''}},
                     'stages': {'S1': {'approval': 'approved', 'approval_ref': 'decisions.md#scoped-decision'}},
                     'milestones': {'A1': {'approval': 'proposed', 'approval_ref': ''}}}
        self.rebind()

    def write(self):
        self.meta.write_text(json.dumps(self.data, sort_keys=True))

    def rebind(self):
        self.data['source_hashes'] = spec_set.load(self.main)['hashes']
        self.data['provenance'] = {'decisions.md': sha(self.record)}
        self.write()

    def load(self):
        return spec_set.load_reviewed(self.main)

    def test_normative_wording_never_supplies_approval_or_execution(self):
        bundle = self.load()
        caps, stages, milestones = spec_set.reviewed_declarations(bundle)
        self.assertEqual(set(bundle['paths']), {'main', 'capabilities', 'phases'})
        self.assertEqual(len(bundle['review_inputs']), 2)
        self.assertFalse(ep.approved(caps[0]))
        plan = ep.build_plan(caps, stages, milestones)
        self.assertEqual(plan['ready'], [])
        self.assertFalse(plan['execution_authorized'])
        self.assertFalse(plan['promotion_allowed'])
        self.assertIn('capability_approval_missing', plan['milestones'][0]['readiness_blockers'])

    def test_missing_metadata_fails_closed(self):
        self.meta.unlink()
        with self.assertRaisesRegex(ValueError, 'missing.*review metadata'):
            self.load()

    def test_missing_and_unknown_ids_are_rejected_without_defaults(self):
        for group, identity in [('capabilities', 'A'), ('stages', 'S1'), ('milestones', 'A1')]:
            original = copy.deepcopy(self.data)
            del self.data[group][identity]
            self.write()
            with self.assertRaisesRegex(ValueError, 'IDs differ'):
                spec_set.reviewed_declarations(self.load())
            self.data = copy.deepcopy(original)
            self.data[group]['UNKNOWN'] = {'approval': 'proposed', 'approval_ref': ''}
            self.write()
            with self.assertRaisesRegex(ValueError, 'IDs differ'):
                spec_set.reviewed_declarations(self.load())
            self.data = original

    def test_duplicate_json_keys_are_rejected(self):
        text = self.meta.read_text()
        self.meta.write_text(text.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'))
        with self.assertRaisesRegex(ValueError, 'duplicate review metadata key'):
            self.load()

    def test_unknown_fields_and_invalid_status_types_are_rejected(self):
        original = copy.deepcopy(self.data)
        for value in ['unknown', ['approved'], None]:
            self.data['capabilities']['A']['approval'] = value
            self.write()
            with self.assertRaisesRegex(ValueError, 'invalid review status'):
                self.load()
        self.data = original
        self.data['extra'] = True
        self.write()
        with self.assertRaisesRegex(ValueError, 'schema'):
            self.load()

    def test_all_normative_role_hashes_are_bound(self):
        for p in [self.main, self.cap, self.phase]:
            before = p.read_bytes()
            p.write_bytes(before + b'\nChanged words.\n')
            with self.assertRaisesRegex(ValueError, 'stale normative source'):
                self.load()
            p.write_bytes(before)

    def test_provenance_bytes_and_anchor_are_bound(self):
        self.record.write_text('# Decisions\n\n## Other Decision\n')
        with self.assertRaisesRegex(ValueError, 'provenance hash changed'):
            self.load()
        self.rebind()
        with self.assertRaisesRegex(ValueError, 'anchor'):
            self.load()

    def test_ambiguous_provenance_anchor_is_rejected(self):
        self.record.write_text('# Decisions\n\n## Scoped Decision\nFirst.\n\n## Scoped Decision\nSecond.\n')
        self.rebind()
        with self.assertRaisesRegex(ValueError, 'uniquely'):
            self.load()

    def test_source_order_invariance_after_explicit_metadata_rebinding(self):
        a = self.cap.read_text().rstrip()
        b = a.replace('ID: A', 'ID: B').replace('Title: A', 'Title: B')
        self.cap.write_text(a + '\n---\n' + b + '\n')
        self.data['capabilities']['B'] = {'approval': 'proposed', 'approval_ref': ''}
        self.rebind()
        old = self.load()
        first = spec_set.reviewed_declarations(old)
        self.cap.write_text(b + '\n---\n' + a + '\n')
        with self.assertRaisesRegex(ValueError, 'stale normative source'):
            self.load()
        self.rebind()
        new = self.load()
        second = spec_set.reviewed_declarations(new)
        self.assertEqual(first, second)
        self.assertEqual(ep.build_plan(*first), ep.build_plan(*second))
        self.assertNotEqual(old['digest'], new['digest'])

    def test_proposed_status_cannot_carry_conflicting_approval_reference(self):
        self.data['capabilities']['A']['approval_ref'] = 'decisions.md#scoped-decision'
        self.write()
        with self.assertRaisesRegex(ValueError, 'conflicting proposed'):
            self.load()

    def test_approved_status_needs_bound_provenance(self):
        self.data['capabilities']['A'] = {'approval': 'approved', 'approval_ref': 'elsewhere.md#decision'}
        self.write()
        with self.assertRaisesRegex(ValueError, 'bound provenance'):
            self.load()

    def test_provenance_cannot_escape_implementation(self):
        other = self.root.parent / 'v2.md'
        other.write_text('# External\n')
        self.data['provenance'] = {'../v2.md': sha(other)}
        self.write()
        with self.assertRaisesRegex(ValueError, 'escaping'):
            self.load()
        self.rebind()
        self.meta.unlink()
        self.meta.symlink_to(other)
        with self.assertRaisesRegex(ValueError, 'nonlocal'):
            self.load()

    def test_tamper_after_capture_invalidates_bundle_and_report_context(self):
        bundle = self.load()
        source = self.cap
        metrics = {'spec_set': bundle, 'source': str(source), 'planning_tools': {'tool': 'hash'}}
        self.data['capabilities']['A'] = {'approval': 'approved', 'approval_ref': 'decisions.md#scoped-decision'}
        self.write()
        with self.assertRaisesRegex(ValueError, 'review inputs'):
            spec_set.verify(bundle)
        current = self.load()
        with self.assertRaisesRegex(ValueError, 'review metadata inputs'):
            spec_set.verify_report_context(metrics, current, source, {'tool': 'hash'})

    def test_old_report_without_review_binding_is_rejected(self):
        bundle = self.load()
        old = spec_set.load(self.main)
        metrics = {'spec_set': old, 'source': str(self.cap), 'planning_tools': {}}
        with self.assertRaisesRegex(ValueError, 'review metadata inputs'):
            spec_set.verify_report_context(metrics, bundle, self.cap, {})

    def test_inline_status_conflicts_with_separate_source(self):
        for path, before, after in [
            (self.cap, 'Acceptance:', 'Approval: proposed\nAcceptance:'),
            (self.phase, '[[stages]]', '[[stages]]\napproval="proposed"'),
            (self.phase, '[[milestones]]', '[[milestones]]\napproval_ref=""'),
        ]:
            original = path.read_text()
            path.write_text(original.replace(before, after))
            self.rebind()
            with self.assertRaisesRegex(ValueError, 'inline.*conflicts'):
                spec_set.reviewed_declarations(self.load())
            path.write_text(original)
            self.rebind()

    def test_metadata_order_changes_hash_not_semantic_status_or_plan(self):
        old = self.load()
        first = spec_set.reviewed_declarations(old)
        self.meta.write_text(json.dumps(dict(reversed(list(self.data.items()))), indent=4))
        new = self.load()
        second = spec_set.reviewed_declarations(new)
        self.assertNotEqual(old['digest'], new['digest'])
        self.assertEqual(first, second)
        self.assertEqual(ep.build_plan(*first), ep.build_plan(*second))

    def test_external_draft_never_inherits_approval_by_id(self):
        self.data['capabilities']['A'] = {'approval': 'approved', 'approval_ref': 'decisions.md#scoped-decision'}
        self.write()
        caps, stages, milestones = spec_set.reviewed_declarations(self.load())
        self.assertTrue(ep.approved(caps[0]))
        external = spec_set.without_review(caps)
        self.assertFalse(ep.approved(external[0]))
        plan = ep.build_plan(external, stages, milestones, draft=True)
        self.assertIn('unapproved_draft_view', plan['blockers'])
        self.assertFalse(plan['execution_authorized'])


if __name__ == '__main__':
    unittest.main()
