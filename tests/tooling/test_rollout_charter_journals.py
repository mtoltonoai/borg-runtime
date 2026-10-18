"""Same-byte consumed charter journals; reuses the real-file manifest fixture.

Record envelopes follow mutations.rs/charter.rs. Accepted limit values are a
local reader projection, not a claim that a Rust runtime admitted these fixtures.
"""
import copy
from pathlib import Path
import unittest
from unittest import mock

from test_rollout_charter_manifests import C, fixture, hostmod


def original_journal(f, kind):
    accepted = f.command['task']['resolved_limits']['global_metadata']
    accepted.update(revision='local-original-policy', worker={'mutation_record_bytes': 65536},
                    transport={'body_bytes': 32768})
    f.retained['worker']['mutation_record_bytes'] = 65536
    f.host._verified_manifest['source']['files']['src/worker/mutations.rs'] = {
        'sha256': hostmod._CHARTER_JOURNAL_SOURCES['src/worker/mutations.rs']}
    observation = copy.deepcopy(f.manifest['observation'])
    operation = f.operation_id
    source = {'definition_id': 'original-definition', 'definition_revision': 'one'}
    reply = {'reply_id': operation, 'turn_id': f.occurrence, 'effective': source,
             'observation': copy.deepcopy(observation['reply']), 'audit': {'findings': []}}
    result = {'kind': 'reply', 'value': reply}
    if kind == 'output_chunk':
        reference = copy.deepcopy(observation['reply']['output_reference'])
        raw = (f.root / f.chunk_names[0]).read_bytes()
        observation = {'kind': 'output_chunk', 'chunk': {'reference': reference, 'index': 0, 'data': raw.hex()}}
        operation = 'charter-output-' + C.digest(f.occurrence.encode()) + '-0'
        result = {'kind': 'output_chunk', 'value': {'reference': copy.deepcopy(reference), 'index': 0,
                                                   'sha256': C.digest(raw), 'bytes': len(raw)}}
    elif kind == 'compaction_reply':
        result = {'kind': 'compaction', 'value': {'run_id': f.occurrence, 'session_id': 'original-session',
            'caller': copy.deepcopy(f.caller), 'request_index': 1,
            'effective': {'reference': source, 'source': {'files': [], 'clauses': []}},
            'reply': reply, 'summary': observation['summary'], 'disposition': 'current_source_summary'}}
    value = {'limits': copy.deepcopy(accepted), 'revision': 3,
        'request': {'operation': 'observe_charter', 'input': {'operation_id': operation,
                    'caller': copy.deepcopy(f.caller), 'observation': observation}},
        'outcome': {'status': 'ok', 'data': {'kind': 'charter', 'value': result}},
        'outcome_context': {'commit': {'operation_id': operation, 'controller_revision': 7},
                            'observed_controller_revision': 9},
        'consumed': True, 'recovery_epoch': 0}
    return 'charter/' + operation.encode().hex() + '.json', value


def baseline(f, name, value):
    f.write(name, value)
    f.baseline()
    return copy.deepcopy(f.stop)


def replace_journal(f, name, value=None):
    raw = (f.root / name).read_bytes() if value is None else C.canonical(value)
    f.write(name, raw)
    f.current = f.host.state_files()
    assert f.before[name]['identity'][1] != f.current[name]['identity'][1]


class CharterJournalTests(unittest.TestCase):
    def setUp(self):
        for name in ('subprocess.Popen', 'socket.socket', 'os.system', 'os.posix_spawn', 'os.posix_spawnp'):
            guard = mock.patch(name, side_effect=AssertionError('external effects forbidden'))
            guard.start(); self.addCleanup(guard.stop)

    def test_consumed_reply_supplied_reply_compaction_and_chunk_preserve_exact_original_joins(self):
        for kind in ('reply', 'supplied_reply', 'compaction_reply', 'output_chunk'):
            with self.subTest(kind=kind), fixture('supplied_reply' if kind == 'output_chunk' else kind) as f:
                name, value = original_journal(f, kind)
                before = baseline(f, name, value); replace_journal(f, name)
                current = copy.deepcopy(f.current)
                preserved, observations = f.history()
                self.assertEqual(set(observations), {name})
                self.assertEqual(preserved, {n: row for n, row in f.before.items() if n != name})
                proof = f.host._charter_manifests.proof()
                row = proof['journal_observations'][C.digest(name.encode())]
                self.assertEqual(row['original'], f.before[name]); self.assertEqual(row['sampled'], f.current[name])
                self.assertEqual(row['manifest_sha256'], f.before[f.name]['sha256'])
                self.assertEqual(row['original_command_row'], f.before[f.command_name])
                self.assertEqual(row['observation_kind'], kind)
                self.assertEqual(row['original_request_sha256'], C.digest(C.canonical(value['request'])))
                self.assertEqual(row['original_context_sha256'], C.digest(C.canonical(value['outcome_context'])))
                self.assertEqual(row['original_outcome_sha256'], C.digest(C.canonical(value['outcome'])))
                self.assertTrue(row['original_content_exact'] and row['inode_only'] and row['inode_changed'])
                self.assertFalse(row['actual_writer_attributed'] or row['new_operation_authority'] or proof['host_effects'])
                self.assertFalse(proof['observations'][C.digest(f.name.encode())]['inode_changed'])
                self.assertEqual(f.stop, before); self.assertEqual(f.current, current)
                f.host.api.assert_not_called()

    def test_request_result_context_consumption_and_epoch_bytes_cannot_change(self):
        changes = [lambda j: j['request']['input']['caller'].update(task_id='new-task'),
                   lambda j: j['outcome']['data']['value']['value'].update(reply_id='new-reply'),
                   lambda j: j['outcome_context']['commit'].update(controller_revision=8),
                   lambda j: j.update(consumed=False), lambda j: j.update(recovery_epoch=1),
                   lambda j: j['limits'].update(revision='new-policy')]
        for index, change in enumerate(changes):
            with self.subTest(index=index), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'supplied_reply'); baseline(f, name, value)
                change(value); replace_journal(f, name, value)
                with self.assertRaisesRegex(ValueError, 'charter_journal_content_or_presence_changed'): f.history()

    def test_pending_refused_other_operations_and_nonzero_epoch_are_outside_scope(self):
        changes = [lambda j: j.update(consumed=False, outcome=None, outcome_context=None),
                   lambda j: j.update(recovery_epoch=1), lambda j: j.update(revision=2),
                   lambda j: j['request'].update(operation='register_worker'),
                   lambda j: j['request']['input']['observation'].update(kind='begin_turn'),
                   lambda j: j['outcome'].update(status='refused'),
                   lambda j: j['outcome_context']['commit'].update(operation_id='other-operation'),
                   lambda j: j['outcome_context'].update(observed_controller_revision=6),
                   lambda j: j['outcome_context']['commit'].update(controller_revision=True)]
        for index, change in enumerate(changes):
            with self.subTest(index=index), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'supplied_reply'); change(value)
                baseline(f, name, value); replace_journal(f, name)
                with self.assertRaises(ValueError): f.history()
        with fixture('supplied_reply') as f:
            name, value = original_journal(f, 'supplied_reply'); name = 'mutations/other-original.json'
            baseline(f, name, value); replace_journal(f, name)
            with self.assertRaisesRegex(ValueError, 'original_worker_history_changed'): f.history()

    def test_paired_manifest_caller_operation_outcome_and_accepted_policy_are_required(self):
        for change in ('missing-manifest', 'caller', 'observation', 'result-id', 'policy', 'path'):
            with self.subTest(change=change), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'supplied_reply')
                if change == 'caller': value['request']['input']['caller']['task_id'] = 'unrelated-task'
                elif change == 'observation': value['request']['input']['observation']['terminal']['thread_id'] = 'unrelated-thread'
                elif change == 'result-id': value['outcome']['data']['value']['value']['reply_id'] = 'unrelated-reply'
                elif change == 'policy': value['limits']['revision'] = 'foreign-policy'
                elif change == 'path': name = 'charter/' + b'wrong-operation'.hex() + '.json'
                baseline(f, name, value)
                if change == 'missing-manifest':
                    (f.root / f.name).unlink(); f.stop['state'].pop(f.name)
                replace_journal(f, name)
                with self.assertRaises(ValueError): f.history()

    def test_output_chunk_requires_original_bytes_reference_index_and_exact_receipt(self):
        for change in ('data', 'reference', 'index', 'receipt-hash', 'receipt-bytes', 'receipt-reference'):
            with self.subTest(change=change), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'output_chunk')
                chunk = value['request']['input']['observation']['chunk']; result = value['outcome']['data']['value']['value']
                if change == 'data': chunk['data'] = ('x' * len(bytes.fromhex(chunk['data']))).encode().hex()
                elif change == 'reference': chunk['reference']['sha256'] = '0' * 64
                elif change == 'index': chunk['index'] = 1
                elif change == 'receipt-hash': result['sha256'] = '0' * 64
                elif change == 'receipt-bytes': result['bytes'] += 1
                else: result['reference']['request_id'] = 'other-turn'
                baseline(f, name, value); replace_journal(f, name)
                with self.assertRaisesRegex(ValueError, 'charter_journal_chunk_(identity|receipt)'): f.history()

    def test_original_command_and_mutation_writer_pin_cannot_be_substituted(self):
        for change in ('missing-pin', 'drifted-pin', 'original-command'):
            with self.subTest(change=change), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'supplied_reply'); baseline(f, name, value)
                replace_journal(f, name)
                if change == 'missing-pin': del f.host._verified_manifest['source']['files']['src/worker/mutations.rs']
                elif change == 'drifted-pin': f.host._verified_manifest['source']['files']['src/worker/mutations.rs']['sha256'] = '0' * 64
                else:
                    f.command['task']['resolved_limits']['global_metadata']['revision'] = 'changed-policy'
                    f.write(f.command_name, f.record); f.current = f.host.state_files()
                with self.assertRaises(ValueError): f.history()

    def test_journal_sample_non_inode_identity_deadline_and_byte_bounds_stay_strict(self):
        for change in ('sample-replaced', 'mode', 'device', 'deadline', 'mutation-bound'):
            with self.subTest(change=change), fixture('supplied_reply') as f:
                name, value = original_journal(f, 'supplied_reply'); baseline(f, name, value)
                replace_journal(f, name)
                if change == 'sample-replaced': f.write(name, (f.root / name).read_bytes())
                elif change == 'mode':
                    (f.root / name).chmod(0o640); f.current = f.host.state_files()
                elif change == 'device': f.current[name]['identity'][0] += 1
                elif change == 'deadline': f.host.r['deadline_ms'] = 0
                else: f.retained['worker']['mutation_record_bytes'] = 1
                with self.assertRaises(ValueError): f.history()


if __name__ == '__main__':
    unittest.main()
