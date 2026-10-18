"""Local descriptors for the exact later Codex outer-boundary lock.

Uses the existing retained/copyout fixtures, with the selected publication
writer family. No runtime, API, process, socket, cleanup or provider is run.
The saved diagnostic d90f06f7a562afd25bb752a81e5fda1672959f3e08d9a9b98229eee269206d7e
observed three new empty0600 single-link locks (Task3/4/9). Actual task/path,
device and UID/GID are explicitly replaced by each local fixture's bindings.
Original calls.json below is opaque retained fixture data, not a claim of
canonical settlement or a successfully decoded production MCP journal.
"""
import contextlib
import copy
import os
import tempfile
from pathlib import Path
from unittest import mock

import test_rollout_reconciliation as original
import test_rollout_codex_retained as retained


hostmod = original.hostmod
C = original.C


class OuterBoundaryLockTests(original.NoExternalEffects):
    FAMILY = hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES
    FORMATS = hostmod._PUBLICATION_UNION_CODEX_HOME_FORMATS
    WRITERS = ('src/worker/codex_mcp.rs', 'src/worker/cleanup_reads.rs',
               'src/worker/process.rs', 'src/worker/cleanup.rs')
    observe = retained.CodexRetainedTests.observe

    def lock_path(self, f):
        return f.root / f.base / 'collaboration-codex/outer-boundary.lock'

    def add_lock(self, f, body=b''):
        path = self.lock_path(f)
        with path.open('xb') as stream:
            stream.write(body)
        path.chmod(0o600)
        return path

    def snapshot(self, f):
        current = f.host.state_files()
        f.stop['state'] = copy.deepcopy(current)
        f.stop['codex_homes'] = hostmod.codex_home_snapshot(f.host, current)

    @contextlib.contextmanager
    def fixture(self, *, old_lock=False, calls=True):
        with retained.CodexRetainedTests.fixture(self, 'local_recovery') as f:
            if calls:
                f.f.write(f.base + '/collaboration-codex/calls.json',
                          {'retained_fixture_journal': True})
            if old_lock:
                self.add_lock(f)
            self.snapshot(f)
            f.record['last_observed_ms'] += 1
            f.record['next_due_ms'] += 1
            f.write_progress()
            yield f

    def assert_original_home_exact(self, f):
        original_rows = {n: row for n, row in f.stop['state'].items()
                         if n.startswith(f.base + '/')}
        self.assertTrue(original_rows)
        self.assertEqual({n: f.current[n] for n in original_rows}, original_rows)

    def assert_retained_scope(self, f):
        proof = f.host._codex_home_proof
        evidence = proof['retained_attempts'][f.key]
        self.assertIn('outer_boundary_observation', evidence)
        for field in ('canonical_settlement_claimed', 'reclamation_claimed',
                      'unchanged_progress_fields_claimed', 'simultaneous_snapshot'):
            self.assertIs(evidence[field], False)
        self.assertIs(evidence['current_not_started'], True)
        self.assertIs(evidence['current_not_done'], True)
        self.assertEqual(proof['attempts'], {})
        self.assert_original_home_exact(f)

    def test_new_empty_lock_preserves_exact_original_home_and_observation_scope(self):
        with self.fixture() as f:
            before = copy.deepcopy(f.stop)
            path = self.add_lock(f)
            with mock.patch.object(hostmod, 'verify_task', side_effect=AssertionError(
                    'no canonical settlement inference')):
                preserved, changes = f.check()
            self.assertEqual(f.stop, before)
            self.assertIn(f.name, changes)
            self.assertEqual(path.read_bytes(), b'')
            self.assertEqual(set(f.current) - set(f.stop['state']),
                             {str(path.relative_to(f.root))})
            self.assertTrue(all(n in preserved for n in f.stop['state']
                                if n.startswith(f.base + '/')))
            self.assert_retained_scope(f)
            self.assertIsInstance(f.host._codex_home_proof['retained_attempts'][f.key]
                                  ['outer_boundary_observation'], dict)

    def test_existing_empty_lock_exact_and_parent_ctime_only_are_accepted(self):
        for chmod_parent in (False, True):
            with self.subTest(chmod_parent=chmod_parent), self.fixture(old_lock=True) as f:
                path = self.lock_path(f)
                old = path.parent.stat()
                if chmod_parent:
                    path.parent.chmod(0o700)
                    self.assertEqual(path.parent.stat().st_mtime_ns, old.st_mtime_ns)
                    self.assertGreaterEqual(path.parent.stat().st_ctime_ns, old.st_ctime_ns)
                f.check()
                self.assert_retained_scope(f)

    def test_absent_lock_does_not_allow_parent_clock_change(self):
        with self.fixture() as f:
            self.lock_path(f).parent.chmod(0o700)
            with self.assertRaises(ValueError):
                f.check()

    def test_existing_lock_does_not_allow_parent_mtime_change(self):
        with self.fixture(old_lock=True) as f:
            original.advance_directory_clock(self.lock_path(f).parent)
            with self.assertRaises(ValueError):
                f.check()

    def test_each_exact_writer_pin_is_required_for_the_new_lock(self):
        for name in self.WRITERS:
            for change in ('missing', 'changed'):
                with self.subTest(name=name, change=change), self.fixture() as f:
                    self.add_lock(f)
                    files = f.host._verified_manifest['source']['files']
                    self.assertIn(name, files)
                    if change == 'missing':
                        del files[name]
                    else:
                        files[name]['sha256'] = '0' * 64
                    with self.assertRaises(ValueError):
                        f.check()

    def test_historical_families_do_not_gain_the_new_lock_allowance(self):
        for family, formats in ((hostmod._COMPOSED_CODEX_HOME_SOURCES,
                                 hostmod._COMPOSED_CODEX_HOME_FORMATS),
                                (hostmod._RECOVERED_CODEX_HOME_SOURCES,
                                 hostmod._RECOVERED_CODEX_HOME_FORMATS)):
            with self.subTest(formats=formats), self.fixture() as f:
                f.host._verified_manifest['source']['files'] = {
                    name: {'sha256': digest} for name, digest in family.items()}
                f.host.p['formats']['before'] = dict(formats)
                f.host.p['formats']['after'] = dict(formats)
                self.add_lock(f)
                with self.assertRaises(ValueError):
                    f.check()

    def test_new_lock_requires_original_calls_and_original_parent(self):
        with self.fixture(calls=False) as f:
            self.add_lock(f)
            with self.assertRaises(ValueError):
                f.check()
        with self.fixture() as f:
            self.add_lock(f)
            f.stop['codex_homes'][f.key]['journal_directories'].pop(
                f.base + '/collaboration-codex')
            with self.assertRaises(ValueError):
                f.check()

    def test_nonempty_wrong_mode_symlink_and_hardlink_refuse(self):
        for change in ('nonempty', 'mode', 'symlink', 'hardlink'):
            with self.subTest(change=change), self.fixture() as f:
                path = self.lock_path(f)
                if change == 'symlink':
                    path.symlink_to(path.parent / 'calls.json')
                else:
                    self.add_lock(f, b'not-empty' if change == 'nonempty' else b'')
                    if change == 'mode':
                        path.chmod(0o640)
                    elif change == 'hardlink':
                        os.link(path, f.root / 'same-lock-another-name')
                with self.assertRaises(ValueError):
                    f.check()

    def test_foreign_lock_device_uid_gid_metadata_refuse(self):
        for index in (0, 2, 3):
            with self.subTest(identity_index=index), self.fixture() as f:
                path = self.add_lock(f)
                f.current = f.host.state_files()
                f.current[str(path.relative_to(f.root))]['identity'][index] += 1
                hook = hostmod.CodexHomePreservation(f.host, f.stop, f.current)
                with self.assertRaises(ValueError):
                    hook.observe_retained(f.name, f.stop['state'][f.name], f.current[f.name])

    def test_original_lock_replacement_removal_or_content_change_refuses(self):
        for change in ('replacement', 'missing', 'content'):
            with self.subTest(change=change), self.fixture(old_lock=True) as f:
                path = self.lock_path(f)
                if change == 'replacement':
                    temporary = f.root / 'replacement-lock'
                    temporary.write_bytes(b'')
                    temporary.chmod(0o600)
                    temporary.replace(path)
                elif change == 'missing':
                    path.unlink()
                else:
                    path.write_bytes(b'changed')
                with self.assertRaises(ValueError):
                    f.check()

    def test_new_lock_cannot_hide_unrelated_file_or_directory_changes(self):
        for change in ('sibling', 'vendor_content', 'calls_content', 'nested_directory',
                       'parent_mode', 'transcript_directory'):
            with self.subTest(change=change), self.fixture() as f:
                path = self.add_lock(f)
                if change == 'sibling':
                    f.f.write(f.base + '/collaboration-codex/unexpected.json', {'new': True})
                elif change == 'vendor_content':
                    (f.root / f.base / 'plugins/cache/vendor.db').write_bytes(b'changed')
                elif change == 'calls_content':
                    f.f.write(f.base + '/collaboration-codex/calls.json', {'changed': True})
                elif change == 'nested_directory':
                    (path.parent / 'new-empty-directory').mkdir(mode=0o700)
                elif change == 'parent_mode':
                    path.parent.chmod(0o750)
                else:
                    (f.root / f.base / 'sessions/new-empty-directory').mkdir(mode=0o700)
                with self.assertRaises(ValueError):
                    f.check()

    def test_new_lock_does_not_relax_started_done_or_copyout_material(self):
        for field, value in (('started', True), ('done', True), ('archive', {}),
                             ('exports', []), ('transcript_sources', []), ('removing', 'plugins')):
            with self.subTest(field=field), self.fixture() as f:
                self.add_lock(f)
                f.record[field] = value
                f.write_progress()
                with self.assertRaises(ValueError):
                    f.check()

    def test_finish_rechecks_the_observed_lock_and_parent(self):
        for change in ('lock_bytes', 'lock_replacement', 'parent_clock'):
            with self.subTest(change=change), self.fixture() as f:
                path = self.add_lock(f)
                hook, accepted = self.observe(f)
                self.assertTrue(accepted)
                if change == 'lock_bytes':
                    path.write_bytes(b'x')
                elif change == 'lock_replacement':
                    temporary = f.root / 'replacement-after-observation'
                    temporary.write_bytes(b'')
                    temporary.chmod(0o600)
                    temporary.replace(path)
                else:
                    original.advance_directory_clock(path.parent)
                with self.assertRaises(ValueError):
                    hook.finish()

    def test_new_lock_evidence_remains_within_existing_receipt_bound(self):
        with self.fixture() as f:
            self.add_lock(f)
            f.host.l['receipt_bytes'] = max((f.root / f.name).stat().st_size,
                                           (f.root / f.f.command).stat().st_size)
            with self.assertRaises(ValueError):
                self.observe(f)

    def test_completed_copyout_accepts_exact_lock_addition_or_parent_ctime(self):
        for old_lock in (False, True):
            with self.subTest(old_lock=old_lock), tempfile.TemporaryDirectory(
                    prefix='codex-outer-completed-') as directory:
                f = original.CodexHomeFixture(Path(directory))
                f.host._verified_manifest = {'source': {'files': {
                    name: {'sha256': digest} for name, digest in self.FAMILY.items()}}}
                f.host.p['formats']['before'] = dict(self.FORMATS)
                f.host.p['formats']['after'] = dict(self.FORMATS)
                f.f.write(f.base + '/collaboration-codex/calls.json',
                          {'retained_fixture_journal': True})
                if old_lock:
                    self.add_lock(f)
                self.snapshot(f)
                if not old_lock:
                    self.add_lock(f)
                f.compact()
                self.lock_path(f).parent.chmod(0o700)
                f.host.api = mock.Mock(side_effect=AssertionError('no fixture API call'))
                preserved, changes = f.check()
                f.host.api.assert_not_called()
                self.assertTrue(changes)
                completed = f.host._codex_home_proof['attempts'][f.key]
                self.assertEqual(completed['progress_sha256'], f.current[f.name]['sha256'])
                self.assertEqual(completed['authority_binding'],
                                 f.host.r['cleanup_authority']['binding'])
                self.assertEqual(completed['archive_exports'], f.record['exports'])
                self.assertIsInstance(completed['outer_boundary_observation'], dict)
                for name, row in f.stop['state'].items():
                    if name.startswith(f.base + '/collaboration-codex/'):
                        self.assertEqual(f.current[name], row)
                        self.assertIn(name, preserved)

