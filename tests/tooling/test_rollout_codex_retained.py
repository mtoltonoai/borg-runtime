"""Retained Codex-home readback uses original inventory, not a lost sidecar body.

These are local file/descriptor fixtures. They neither run cleanup nor establish
canonical settlement, a prior progress transition, or permission to reclaim.
"""
import contextlib
import copy
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import test_rollout_reconciliation as original


hostmod = original.hostmod
C = original.C


class CodexRetainedTests(original.NoExternalEffects):
    FAMILY=hostmod._COMPOSED_CODEX_HOME_SOURCES
    FORMATS=hostmod._COMPOSED_CODEX_HOME_FORMATS
    @contextlib.contextmanager
    def fixture(self, reason='uncertain_effect', *, status=None, proof=False):
        with tempfile.TemporaryDirectory(prefix='codex-retained-readback-') as directory:
            f = original.CodexHomeFixture(Path(directory))
            f.host._verified_manifest = {'source': {'files': {
                name: {'sha256': digest}
                for name, digest in self.FAMILY.items()}}}
            f.host.p['formats']['before'] = dict(self.FORMATS)
            f.host.p['formats']['after'] = dict(self.FORMATS)
            (f.root / 'cleanup-codex').mkdir(mode=0o700)
            saved = f.stop['codex_homes'][f.key]
            f.home_proof = {'path': str(f.root / f.base),
                            'parents': copy.deepcopy(saved['parents']),
                            'directory': copy.deepcopy(saved['home'])}
            f.record = {
                'format_revision': 1, 'command_id': f.command['command_id'],
                'attempt_id': f.attempt['id'],
                'policy': copy.deepcopy(f.f.record['policy']),
                'policy_revision': f.f.record['policy_revision'],
                'status': None, 'checkpoint': None, 'first_safe_ms': None,
                'last_observed_ms': 1000,
                'next_due_ms': 1000 + f.f.record['policy']['retry_delay_ms'],
                'proof': None, 'archive': None, 'exports': None,
                'transcript_expected': None, 'transcript_sources': None,
                'removing': None, 'started': False, 'done': False,
                'reason': 'uncertain_effect'}
            f.write_progress()
            f.f.refresh()
            f.stop = copy.deepcopy(f.f.stop)
            f.stop['codex_homes'] = hostmod.codex_home_snapshot(f.host, f.stop['state'])
            # The stopped evidence contains only this sidecar's hash/identity.
            # It deliberately has no cleanup-codex before-body dictionary.
            self.assertEqual(set(f.stop['state'][f.name]), {'sha256', 'identity'})
            self.assertNotIn(f.name, f.stop['cleanup_records'])
            f.record.update(reason=reason, status=status, last_observed_ms=1100,
                            next_due_ms=1100 + f.record['policy']['retry_delay_ms'],
                            proof=copy.deepcopy(f.home_proof) if proof else None)
            if reason == 'grace':
                grace = {'succeeded': 'success_grace_ms', 'failed': 'failed_grace_ms',
                         'cancelled': 'cancelled_grace_ms'}[status]
                f.record.update(first_safe_ms=1000,
                                next_due_ms=1000 + f.record['policy'][grace],
                                proof=copy.deepcopy(f.home_proof))
                self.assertLess(f.record['last_observed_ms'], f.record['next_due_ms'])
            f.write_progress()
            f.host.r.pop('cleanup_authority')
            f.host.api = mock.Mock(side_effect=AssertionError('retained readback must not call API'))
            yield f
            f.host.api.assert_not_called()

    def observe(self, f):
        f.current = f.host.state_files()
        hook = hostmod.CodexHomePreservation(f.host, f.stop, f.current)
        accepted = hook.observe_retained(f.name, f.stop['state'][f.name], f.current[f.name])
        return hook, accepted

    def refused(self, f, message):
        f.write_progress()
        with self.assertRaisesRegex(ValueError, message):
            self.observe(f)

    def test_original_home_intact_hold_needs_no_task_authority_or_api(self):
        for reason in ('uncertain_effect', 'canonical_binding'):
            for status, proof in ((None, False), ('failed', True)):
                with self.subTest(reason=reason, status=status, proof=proof), self.fixture(
                        reason, status=status, proof=proof) as f:
                    before = copy.deepcopy(f.stop)
                    with mock.patch.object(hostmod, 'verify_task', side_effect=AssertionError(
                            'retained readback must not claim canonical settlement')):
                        preserved, changes = f.check()
                    self.assertEqual(f.stop, before)
                    self.assertEqual(set(changes), {f.name})
                    home = {name: row for name, row in f.stop['state'].items()
                            if name.startswith(f.base + '/')}
                    self.assertTrue(home)
                    self.assertEqual({name: f.current[name] for name in home}, home)
                    self.assertTrue(set(home).issubset(preserved))
                    evidence = f.host._codex_home_proof['retained_attempts'][f.key]
                    self.assertEqual(evidence['original_progress_row'], before['state'][f.name])
                    self.assertEqual(evidence['original_home_rows_sha256'], C.digest(C.canonical(home)))
                    self.assertEqual(evidence['current_reason'], reason)
                    self.assertEqual(evidence['original_stopped_home'], before['codex_homes'][f.key])
                    self.assertEqual(f.host._codex_home_proof['attempts'], {})
                    for field in ('old_progress_body_available', 'unchanged_progress_fields_claimed',
                                  'canonical_settlement_claimed', 'reclamation_claimed',
                                  'simultaneous_snapshot'):
                        self.assertIs(evidence[field], False)

    def test_terminal_grace_uses_selected_fixed_deadline_and_can_be_overdue(self):
        for status in ('failed', 'cancelled'):
            with self.subTest(status=status), self.fixture('grace', status=status) as f:
                # A later reader does not turn an overdue saved sample into an
                # invalid record or restart its original grace deadline.
                with mock.patch.object(C, 'now_ms', return_value=f.record['next_due_ms'] + 1):
                    _, changes = f.check()
                self.assertEqual(set(changes), {f.name})
                self.assertEqual(f.host._codex_home_proof['retained_attempts'][f.key]['next_due_ms'],
                                 f.record['next_due_ms'])
        with self.fixture(proof=True) as f:
            # This selected policy has zero success grace, so its producer
            # cannot emit an in-grace successful record before copyout.
            self.assertEqual(f.record['policy']['success_grace_ms'], 0)
            f.record.update(reason='grace', status='succeeded', first_safe_ms=1000,
                            next_due_ms=1000)
            self.refused(f, 'codex_retained_grace_binding')

    def test_each_selected_writer_pin_is_required(self):
        for name in hostmod._CODEX_RETAINED_SOURCES:
            for change in ('missing', 'changed'):
                with self.subTest(name=name, change=change), self.fixture() as f:
                    files = f.host._verified_manifest['source']['files']
                    if change == 'missing':
                        del files[name]
                    else:
                        files[name]['sha256'] = '0' * 64
                    _, accepted = self.observe(f)
                    self.assertIs(accepted, False)
                    with self.assertRaises(ValueError):
                        f.check()

    def test_original_home_and_original_sidecar_row_are_required(self):
        with self.fixture() as f:
            del f.stop['codex_homes'][f.key]
            self.refused(f, 'codex_retained_unbound_original_home')
        with self.fixture() as f:
            f.current = f.host.state_files()
            hook = hostmod.CodexHomePreservation(f.host, f.stop, f.current)
            with self.assertRaisesRegex(ValueError, 'codex_retained_original_row_missing'):
                hook.observe_retained(f.name, None, f.current[f.name])
            self.assertFalse(hook.observe_retained('unrelated/' + f.key + '.json', None, None))

    def test_added_missing_changed_or_replaced_home_file_refuses(self):
        for change in ('added', 'missing', 'content', 'same_bytes_new_inode'):
            with self.subTest(change=change), self.fixture() as f:
                path = f.root / f.base / 'plugins/cache/vendor.db'
                if change == 'added':
                    f.f.write(f.base + '/plugins/cache/new.json', {'unexpected': True})
                elif change == 'missing':
                    path.unlink()
                elif change == 'content':
                    path.write_bytes(b'changed database')
                else:
                    replacement = path.with_name('replacement')
                    replacement.write_bytes(path.read_bytes())
                    replacement.chmod(0o600)
                    replacement.replace(path)
                with self.assertRaisesRegex(ValueError, 'codex_retained_original_home_rows_changed'):
                    self.observe(f)
                with self.assertRaises(ValueError):
                    f.check()

    def test_original_command_bytes_and_inode_are_exact(self):
        for change in ('caller', 'same_bytes_new_inode'):
            with self.subTest(change=change), self.fixture() as f:
                receipt = C.strict((f.root / f.f.command).read_bytes())
                if change == 'caller':
                    receipt['command']['attempt']['session_id'] = 'other-session'
                f.f.write(f.f.command, receipt)
                with self.assertRaisesRegex(ValueError, 'codex_retained_original_command_changed'):
                    self.observe(f)

    def test_original_command_kind_executor_and_worker_binding_are_required(self):
        for path, value in ((('command', 'kind'), 'stop_attempt'),
                            (('command', 'task', 'definition', 'executor'), 'native'),
                            (('command', 'attempt', 'worker_id'), 'other-worker'),
                            (('command', 'attempt', 'id'), 'other-attempt'),
                            (('completed',), False)):
            with self.subTest(path=path), self.fixture() as f:
                receipt = C.strict((f.root / f.f.command).read_bytes())
                target = receipt
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
                f.f.write(f.f.command, receipt)
                # A matching row alone is insufficient; validate its binding.
                current = f.host.state_files()
                f.stop['state'][f.f.command] = copy.deepcopy(current[f.f.command])
                with self.assertRaisesRegex(ValueError, 'codex_retained_command_policy_binding'):
                    self.observe(f)

    def test_policy_and_progress_identity_must_match_selection(self):
        for field, value in (('format_revision', 2), ('format_revision', True),
                             ('command_id', 'other-command'), ('attempt_id', 'other-attempt'),
                             ('policy_revision', 'other-revision')):
            with self.subTest(field=field, value=value), self.fixture() as f:
                f.record[field] = value
                self.refused(f, 'codex_retained_command_policy_binding')
        with self.fixture() as f:
            f.record['policy']['retry_delay_ms'] += 1
            self.refused(f, 'codex_retained_command_policy_binding')
        with self.fixture() as f:
            f.host.cleanup_policy = lambda: None
            self.refused(f, 'codex_retained_policy_unselected')

    def test_home_proof_cannot_substitute_path_parent_or_inode(self):
        for change in ('path', 'parent', 'directory'):
            with self.subTest(change=change), self.fixture(proof=True) as f:
                if change == 'path':
                    f.record['proof']['path'] += '-other'
                elif change == 'parent':
                    f.record['proof']['parents'][0]['inode'] += 1
                else:
                    f.record['proof']['directory']['inode'] += 1
                self.refused(f, 'codex_retained_home_proof')

    def test_checkpoint_requires_exact_typed_hash_bound_original_workspace_record(self):
        checkpoint = {'registration': {'repository_id': 'repo', 'registration_revision': 'one'},
                      'repository': 'file:///checkpoint', 'commit': 'a' * 40,
                      'remote_ref': 'refs/heads/checkpoint', 'operation_id': 'checkpoint'}
        for change in ('valid', 'foreign', 'malformed', 'current_new_only',
                       'original_body_changed', 'original_body_missing', 'original_row_missing',
                       'original_typed_checkpoint_malformed'):
            with self.subTest(change=change), self.fixture(status='failed', proof=True) as f:
                workspace = f.f.name
                f.f.record['checkpoint'] = copy.deepcopy(checkpoint)
                f.f.write(workspace, f.f.record)
                if change != 'current_new_only':
                    # Capture the real original workspace bytes and their exact
                    # row together; never invent an old cleanup-codex body.
                    rows = f.host.state_files()
                    f.stop['state'][workspace] = copy.deepcopy(rows[workspace])
                    f.stop['cleanup_records'][workspace] = (f.root / workspace).read_text()
                f.record['checkpoint'] = copy.deepcopy(checkpoint)
                error = 'codex_retained_checkpoint_binding'
                if change == 'foreign':
                    f.record['checkpoint']['commit'] = 'b' * 40
                elif change == 'malformed':
                    f.record['checkpoint'] = {'commit': 'not-a-checkpoint'}
                elif change == 'original_body_changed':
                    f.stop['cleanup_records'][workspace] += ' '
                    error = 'codex_retained_checkpoint_source_changed'
                elif change == 'original_body_missing':
                    del f.stop['cleanup_records'][workspace]
                    error = 'codex_retained_checkpoint_source_absent'
                elif change == 'original_row_missing':
                    del f.stop['state'][workspace]
                    error = 'codex_retained_checkpoint_source_changed'
                elif change == 'original_typed_checkpoint_malformed':
                    f.f.record['checkpoint'] = {'commit': 'not-a-checkpoint'}
                    f.f.write(workspace, f.f.record)
                    rows = f.host.state_files()
                    f.stop['state'][workspace] = copy.deepcopy(rows[workspace])
                    f.stop['cleanup_records'][workspace] = (f.root / workspace).read_text()
                    f.record['checkpoint'] = copy.deepcopy(f.f.record['checkpoint'])
                    # Equal malformed checkpoint objects still must pass the
                    # existing typed workspace parser before they can bind.
                    error = 'checkpoint'
                if change == 'valid':
                    f.write_progress()
                    _, changes = f.check()
                    self.assertEqual(set(changes), {f.name})
                    self.assertFalse(f.host._codex_home_proof['retained_attempts'][f.key]
                                     ['canonical_settlement_claimed'])
                else:
                    self.refused(f, error)

    def test_known_home_parent_and_journal_directory_metadata_are_preserved(self):
        # Changes to these directories must not become a generic directory
        # timestamp/inode allowance just because sidecar progress changed.
        for relative, change in (('codex', 'mode'), ('home', 'mode'),
                                 ('journal', 'empty_child'), ('journal', 'clock')):
            with self.subTest(relative=relative, change=change), self.fixture() as f:
                path = f.root / ('codex' if relative == 'codex' else f.base)
                if relative == 'journal':
                    path /= 'completion'
                if change == 'mode':
                    path.chmod(0o750)
                    error = 'codex_retained_home_replaced'
                elif change == 'empty_child':
                    (path / 'new-empty-directory').mkdir(mode=0o700)
                    error = 'codex_retained_journal_directories_changed'
                else:
                    original.advance_directory_clock(path)
                    error = 'codex_retained_journal_directories_changed'
                with self.assertRaisesRegex(ValueError, error):
                    self.observe(f)

    def test_transcript_directory_membership_cannot_change_without_file_changes(self):
        with self.fixture() as f:
            (f.root / f.base / 'sessions' / 'added-empty-directory').mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, 'codex_retained_original_transcripts_changed'):
                self.observe(f)

    def test_sidecar_owner_mode_device_and_missing_current_row_refuse(self):
        with self.fixture() as f:
            (f.root / f.name).chmod(0o640)
            with self.assertRaisesRegex(ValueError, 'codex_retained_owner_mode_device'):
                self.observe(f)
        for index in (0, 2, 3, 4):
            with self.subTest(identity_index=index), self.fixture() as f:
                f.current = f.host.state_files()
                old = copy.deepcopy(f.stop['state'][f.name])
                old['identity'][index] += 1
                hook = hostmod.CodexHomePreservation(f.host, f.stop, f.current)
                with self.assertRaisesRegex(ValueError, 'codex_retained_owner_mode_device'):
                    hook.observe_retained(f.name, old, f.current[f.name])
        with self.fixture() as f:
            (f.root / f.name).unlink()
            with self.assertRaisesRegex(ValueError, 'codex_copyout_progress_removed'):
                f.check()

    def test_no_copyout_material_is_accepted_as_retained(self):
        for field, value in (('archive', {}), ('exports', []),
                             ('transcript_expected', False), ('transcript_sources', []),
                             ('removing', 'plugins')):
            with self.subTest(field=field), self.fixture() as f:
                f.record[field] = value
                self.refused(f, 'codex_retained_copyout_material_present')

    def test_started_done_or_nonboolean_flags_do_not_enter_retained_branch(self):
        for field in ('started', 'done'):
            for value in (True, 0, None):
                with self.subTest(field=field, value=value), self.fixture() as f:
                    f.record[field] = value
                    f.write_progress()
                    _, accepted = self.observe(f)
                    self.assertIs(accepted, False)
                    with self.assertRaisesRegex(ValueError, 'codex_copyout_authority_missing'):
                        f.check()

    def test_only_selected_actual_hold_reasons_are_eligible(self):
        for reason in ('source_or_path', 'clock', 'filesystem', 'startup', 'complete',
                       'local_recovery', 'canonical_unavailable', 'not_terminal', 'not_stopped',
                       'checkpoint', 'active', 'cache_dependency', 'unknown'):
            with self.subTest(reason=reason), self.fixture() as f:
                f.record['reason'] = reason
                self.refused(f, 'codex_retained_reason_outside_scope')

    def test_hold_requires_cleared_first_safe_and_exact_retry_due(self):
        for field, value in (('first_safe_ms', 1000), ('next_due_ms', None),
                             ('next_due_ms', True), ('last_observed_ms', True),
                             ('last_observed_ms', -1), ('next_due_ms', 2**64)):
            with self.subTest(field=field, value=value), self.fixture() as f:
                f.record[field] = value
                self.refused(f, 'codex_retained_(hold_binding|clock_shape)')
        for offset in (-1, 1):
            with self.subTest(offset=offset), self.fixture() as f:
                f.record['next_due_ms'] += offset
                self.refused(f, 'codex_retained_hold_binding')
        with self.fixture() as f:
            f.record['last_observed_ms'] = C.now_ms() + 60000
            f.record['next_due_ms'] = f.record['last_observed_ms'] + f.record['policy']['retry_delay_ms']
            self.refused(f, 'codex_retained_clock_shape')

    def test_grace_requires_terminal_status_proof_and_original_fixed_deadline(self):
        for change in ('status', 'proof', 'first_safe', 'last_at_due', 'deadline'):
            with self.subTest(change=change), self.fixture('grace', status='failed') as f:
                if change == 'status':
                    f.record['status'] = 'active'
                elif change == 'proof':
                    f.record['proof'] = None
                elif change == 'first_safe':
                    f.record['first_safe_ms'] = None
                elif change == 'last_at_due':
                    f.record['last_observed_ms'] = f.record['next_due_ms']
                else:
                    f.record['next_due_ms'] += 1
                self.refused(f, 'codex_retained_status_shape' if change == 'status'
                             else 'codex_retained_grace_binding')

    def test_progress_is_rechecked_after_observation_and_proof_is_bounded(self):
        with self.fixture() as f:
            hook, accepted = self.observe(f)
            self.assertTrue(accepted)
            f.record['last_observed_ms'] += 1
            f.record['next_due_ms'] += 1
            f.write_progress()
            with self.assertRaises(ValueError):
                hook.finish()
        with self.fixture() as f:
            f.current = f.host.state_files()
            hook = hostmod.CodexHomePreservation(f.host, f.stop, f.current)
            f.host.l['receipt_bytes'] = max((f.root / f.name).stat().st_size,
                                            (f.root / f.f.command).stat().st_size)
            with self.assertRaisesRegex(ValueError, 'codex_retained_proof_bound'):
                hook.observe_retained(f.name, f.stop['state'][f.name], f.current[f.name])

    def test_completed_copyout_still_requires_its_canonical_authority(self):
        with self.fixture() as f:
            # compact() creates the actual archived fixture and removes vendor
            # files. Its completed claim must not use the no-authority branch.
            (f.root / f.name).unlink()
            (f.root / 'cleanup-codex').rmdir()
            f.compact()
            _, accepted = self.observe(f)
            self.assertIs(accepted, False)
            with self.assertRaisesRegex(ValueError, 'codex_copyout_authority_missing'):
                f.check()




class RecoveredCodexRetainedTests(CodexRetainedTests):
    FAMILY=hostmod._RECOVERED_CODEX_HOME_SOURCES
    FORMATS=hostmod._RECOVERED_CODEX_HOME_FORMATS

    def test_retained_proof_uses_selected20_writers_and_refuses_mixed19_cleanup(self):
        with self.fixture() as f:
            f.check()
            proof=f.host._codex_home_proof
            self.assertEqual(proof['retained_source_files'],hostmod._RECOVERED_CODEX_RETAINED_SOURCES)
            self.assertFalse(any(row['canonical_settlement_claimed'] or row['reclamation_claimed']
                                 for row in proof['retained_attempts'].values()))
        for name,digest in hostmod._CODEX_RETAINED_SOURCES.items():
            with self.subTest(source=name),self.fixture() as f:
                f.host._verified_manifest['source']['files'][name]['sha256']=digest
                self.assertFalse(f.host.codex_home_scope())
                _,accepted=self.observe(f);self.assertFalse(accepted)
                with self.assertRaises(ValueError):f.check()

if __name__ == '__main__':
    unittest.main()
