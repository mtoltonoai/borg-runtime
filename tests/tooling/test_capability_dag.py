"""Failure-oriented checks for the small explicit-declaration extractor."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import capability_dag as dag
import duvet_native as native

PINS = {'snapshot_sha256': 'a' * 64, 'selection_sha256': 'b' * 64}
HEADER = 'Source snapshot: ' + PINS['snapshot_sha256'] + '\nSource selection digest: ' + PINS['selection_sha256'] + '\n\n'
REPORT = {'annotations': [
    {'type': 'SPEC', 'target_path': 'spec/a.md', 'target_section': 'rule', 'comment': 'A MUST work.'},
    {'type': 'SPEC', 'target_path': 'spec/b.md', 'target_section': 'other', 'comment': 'B MUST persist.'},
    {'source': 'generated/planning-citations.txt', 'comment': 'Capability A; unapproved planning association only.'},
    {'source': 'generated/planning-citations.txt', 'comment': 'Capability B; unapproved planning association only.'},
], 'statuses': {'0': {'spec': 12, 'citation': 12, 'related': [2, 3]}, '1': {'spec': 15}}}


def render(text, source, kind, report, pins):
    return dag.render(text, source, kind, report,
                      dict(pins, planning_source_sha256=dag.digest(text.encode()), duvet_json_sha256='c'*64))


def entry(identity='A', parents=()):
    values = [f'ID: {identity}', f'Title: Capability {identity}',
              'Prerequisites: ' + (', '.join(parents) or 'none'),
              'Deliverable: A concrete result.', 'Acceptance: The behavior survives its fault test.']
    values += [f'Dependency: {parent} | inferred | Consumes the explicit interface.' for parent in parents]
    return '\n'.join(values + ['Source: spec/a.md#rule | A MUST work.'])


class CapabilityDagTests(unittest.TestCase):
    def test_ids_are_required_unique_and_delimited(self):
        for text in ['Title: Missing ID', entry().replace('ID: A\n', ''),
                     entry()+'\n---\n'+entry(), entry()+'\n'+entry('B')]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                dag.parse(text)

    def test_unknown_prerequisite_and_cycles_are_rejected(self):
        for text in [entry('B', ('UNKNOWN',)), entry('A', ('A',)),
                     entry('A', ('B',))+'\n---\n'+entry('B', ('A',))]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                dag.dependency_order(dag.parse(text))

    def test_tie_breaking_is_stable_id_order_not_declaration_order(self):
        nodes = dag.parse(entry('C', ('B', 'A'))+'\n---\n'+entry('B')+'\n---\n'+entry('A'))
        self.assertEqual(dag.dependency_order(nodes), ['A', 'B', 'C'])
        self.assertEqual(dag.dependency_order(list(reversed(nodes))), ['A', 'B', 'C'])

    def test_every_edge_has_explicit_review_provenance(self):
        for text in [entry('B', ('A',)).replace('Dependency: A | inferred | Consumes the explicit interface.\n', ''),
                     entry('B', ('A',)).replace('| inferred |', '| automatic |'),
                     entry('B', ('A',)).replace('Prerequisites: A', 'Prerequisites: none')]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                dag.parse(text)
        node = dag.parse(entry('B', ('A',)).replace('| inferred |', '| source-constraint |'))[0]
        self.assertEqual(node['dependencies'][0]['basis'], 'source-constraint')

    def test_file_section_and_exact_sentence_all_bind_the_reference(self):
        for old, new in [('spec/a.md', 'spec/other.md'), ('#rule', '#missing'), ('A MUST work.', 'A MUST Work.')]:
            nodes = dag.parse(entry().replace(old, new))
            with self.subTest(change=new), self.assertRaises(ValueError):
                native.validate_references(nodes, REPORT)

    def test_coverage_and_links_come_from_native_report_not_source_occurrence_counts(self):
        nodes = dag.parse(entry()+'\n---\n'+entry('B'))
        view = native.planning_traceability(nodes, REPORT, 'c'*64)
        self.assertEqual((view['metrics']['covered'], view['metrics']['total'], view['metrics']['unmapped']), (1, 2, 1))
        self.assertEqual(view['metrics']['unmapped_spec_indices'], [1])
        self.assertEqual(len(view['links']), 2)
        self.assertTrue(all(link['native_report_sha256']=='c'*64 for link in view['links']))
        self.assertFalse(view['runtime_evidence'])

    def test_status_assignments_and_nested_structures_are_not_structural_fields(self):
        for line in ['Status: completed', 'Worker: root', 'Subgraphs: child']:
            with self.subTest(line=line), self.assertRaises(ValueError):
                dag.parse(entry()+'\n'+line)

    def test_native_indices_are_resolved_again_against_each_pinned_report(self):
        order = [1, 3, 0, 2]
        moved = {old: new for new, old in enumerate(order)}
        report = {'annotations': [copy.deepcopy(REPORT['annotations'][old]) for old in order],
                  'statuses': {str(moved[int(old)]): dict(status, related=[moved[i] for i in status.get('related', [])])
                               for old, status in REPORT['statuses'].items()}}
        before = native.planning_traceability(dag.parse(entry()), REPORT, 'c'*64)
        after = native.planning_traceability(dag.parse(entry()), report, 'e'*64)
        self.assertEqual(before['links'][0]['requirement'], after['links'][0]['requirement'])
        self.assertEqual(before['links'][0]['report_local_spec_index'], 0)
        self.assertEqual(after['links'][0]['report_local_spec_index'], 2)
        self.assertEqual(after['links'][0]['native_report_sha256'], 'e'*64)
        self.assertEqual(after['metrics']['covered'], before['metrics']['covered'])

    def test_authored_reference_is_not_a_substitute_for_a_native_citation_link(self):
        report = copy.deepcopy(REPORT)
        report['statuses']['0'] = {'spec': 12}
        native.validate_references(dag.parse(entry()), report)
        with self.assertRaisesRegex(ValueError, 'no native planning citation'):
            native.planning_traceability(dag.parse(entry()), report, 'c'*64)

    def test_graph_source_must_match_the_planning_report_input(self):
        with self.assertRaisesRegex(ValueError, 'declaration source differs'):
            dag.render(HEADER+entry(), 'proposal.md', 'proposal', REPORT,
                       dict(PINS, planning_source_sha256='d'*64, duvet_json_sha256='c'*64))

    def test_empty_approved_spec_is_not_populated_from_a_proposal(self):
        empty_report = copy.deepcopy(REPORT)
        empty_report['annotations'] = empty_report['annotations'][:2]
        empty_report['statuses']['0'] = {'spec': 12}
        result = render('# Implementation Specification\nProcess only.\n', '/spec/implementation.md', 'implementation-spec', empty_report, PINS)
        graph = json.loads(result['implementation-dependency-dag.json'])
        self.assertEqual(graph['nodes'], [])
        self.assertEqual(graph['native_traceability']['metrics']['unmapped'], 2)
        self.assertIn('No capability declarations', result['implementation-dependency-dag.md'])
        self.assertEqual(result['dependency-order.txt'], '')

    def test_examples_in_code_fences_are_not_authored_capabilities(self):
        self.assertEqual(dag.parse('Example:\n```text\n'+entry()+'\n```\n'), [])

    def test_indented_commented_tilde_and_nested_fence_examples_are_not_nodes(self):
        examples = ['\n'.join('    '+line for line in entry().splitlines()),
                    '<!--\n'+entry()+'\n---\n-->',
                    '~~~text\n'+entry()+'\n~~~',
                    '````text\n```text\n'+entry()+'\n```\n````']
        for text in examples:
            with self.subTest(text=text):
                self.assertEqual(dag.parse(text), [])
        for text in ['```\n'+entry(), '<!--\n'+entry()]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                dag.parse(text)

    def test_pins_hidden_in_examples_or_comments_do_not_bind_declarations(self):
        for hidden in ['```\n'+HEADER+'```\n', '<!--\n'+HEADER+'-->\n',
                       '\n'.join('    '+line for line in HEADER.splitlines())+'\n']:
            with self.subTest(hidden=hidden), self.assertRaises(ValueError):
                render(hidden+entry(), 'proposal.md', 'proposal', REPORT, PINS)

    def test_indented_comment_example_does_not_hide_the_following_declaration(self):
        self.assertEqual(len(dag.parse('    <!-- code example\n'+entry())), 1)

    def test_source_hash_and_stale_check_bind_exact_bytes_including_newlines(self):
        text = (HEADER+entry()).replace('\n', '\r\n')
        outputs = render(text, 'source.md', 'proposal', REPORT, PINS)
        self.assertEqual(json.loads(outputs['implementation-dependency-dag.json'])['source']['sha256'],
                         dag.digest(text.encode('utf-8')))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dag.write_outputs(root, outputs, False)
            path = root/'implementation-dependency-dag.md'
            path.write_bytes(path.read_bytes().replace(b'\n', b'\r\n'))
            self.assertEqual(dag.write_outputs(root, outputs, True), [path.name])

    def test_snapshot_and_selection_pins_cannot_silently_drift(self):
        for text in [entry(), HEADER.replace('a'*64, 'c'*64)+entry(), HEADER.replace('b'*64, 'c'*64)+entry()]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                render(text, 'proposal.md', 'proposal', REPORT, PINS)

    def test_outputs_reproduce_and_check_detects_stale_or_missing_files(self):
        text = HEADER+entry()+'\n---\n'+entry('B', ('A',))
        first = render(text, 'proposal.md', 'proposal', REPORT, PINS)
        self.assertEqual(first, render(text, 'proposal.md', 'proposal', copy.deepcopy(REPORT), dict(PINS)))
        self.assertIn('  A --> B', first['implementation-dependency-dag.md'])
        self.assertNotIn('subgraph', first['implementation-dependency-dag.md'])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dag.write_outputs(root, first, False)
            self.assertEqual(dag.write_outputs(root, first, True), [])
            (root/'dependency-order.txt').write_text('B\nA\n')
            self.assertEqual(dag.write_outputs(root, first, True), ['dependency-order.txt'])
            (root/'dependency-order.txt').unlink()
            self.assertEqual(dag.write_outputs(root, first, True), ['dependency-order.txt'])

    def test_duplicate_reference_or_ambiguous_extraction_fails(self):
        with self.assertRaises(ValueError):
            dag.parse(entry()+'\nSource: spec/a.md#rule | A MUST work.')
        report = copy.deepcopy(REPORT)
        report['annotations'].append(report['annotations'][0])
        with self.assertRaises(ValueError):
            native.validate_references(dag.parse(entry()), report)

    def test_tool_input_binding_checks_actual_staged_source_and_selection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            stage = root/'duvet-root'; path = stage/'.duvet/reports/report.json'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(REPORT))
            (stage/'source.md').write_text('original')
            lock = {'commit': 'commit', 'snapshot_sha256': 'a'*64,
                    'files': {'source.md': dag.digest(b'original'), '.duvet/config.toml': dag.digest(b'config')}}
            (stage/'.duvet/config.toml').write_text('config')
            metadata = {'report_json': str(path), 'original_config_sha256': dag.digest(b'config'),
                        'report_json_sha256': dag.digest(path.read_bytes()),
                        'duvet_version': '0.4.3', 'commands': [{'exit_code': 0}],
                        'runtime_evidence': False, 'staging_root': str(stage), 'source_sha256': 'd'*64}
            metrics = root/'metrics.json'; metrics.write_text(json.dumps(metadata))
            with patch.object(dag.inventory, 'verify_snapshot', return_value=(lock, None)):
                report, pins = dag.load_duvet(root, metrics)
                self.assertEqual(report, REPORT)
                self.assertEqual(pins['duvet_json_sha256'], dag.digest(path.read_bytes()))
                original = path.read_bytes()
                path.write_bytes(original+b' ')
                with self.assertRaises(ValueError):
                    dag.load_duvet(root, metrics)
                path.write_bytes(original)
                (stage/'source.md').write_text('changed')
                with self.assertRaises(ValueError):
                    dag.load_duvet(root, metrics)
                (stage/'source.md').write_text('original')
                metadata['original_config_sha256'] = 'wrong'; metrics.write_text(json.dumps(metadata))
                with self.assertRaises(ValueError):
                    dag.load_duvet(root, metrics)


if __name__ == '__main__':
    unittest.main()
