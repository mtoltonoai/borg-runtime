"""Scoped original charter-manifest preservation on real private temporary files.

The authenticated-manifest slot is populated with the complete selected producer
map, as after package verification; these tests do not authenticate a package.
No worker, provider, socket, subprocess, API call, or installed-host action runs.
"""
import contextlib
import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import rollout_host_v2 as hostmod

C = hostmod.c
KINDS = ('reply', 'supplied_reply', 'compaction_reply')


class ManifestFixture:
    def __init__(self, root, kind='reply', chunks=True):
        self.root = Path(root)
        self.root.chmod(0o700)
        self.host = hostmod.Host.__new__(hostmod.Host)
        self.host.h = {'role': 'worker', 'service_uid': os.geteuid(),
                       'state': {'root': str(self.root)}, 'api': {'worker_id': 'local-worker'}}
        self.host.p = {'formats': {'before': copy.deepcopy(hostmod._COMPOSED_CODEX_HOME_FORMATS),
                                  'after': copy.deepcopy(hostmod._COMPOSED_CODEX_HOME_FORMATS),
                                  'source_files': copy.deepcopy(hostmod._COMPOSED_CODEX_HOME_SOURCES)}}
        self.host._verified_manifest = {'source': {'files': {
            name: {'sha256': sha} for name, sha in hostmod._COMPOSED_CODEX_HOME_SOURCES.items()}}}
        self.host.l = {'state_bytes': 1048576, 'receipt_bytes': 1048576,
                       'state_entries': 128, 'io_chunk_bytes': 4096}
        self.host.r = {'deadline_ms': C.now_ms() + 30000}
        self.retained = {'worker': {'command_record_bytes': 65536}}
        self.host.retained_policy = lambda: self.retained
        self.host.api = mock.Mock(side_effect=AssertionError('local manifest fixture cannot call API'))
        self.caller = {'task_id': 'local-task', 'attempt_id': 'local-attempt', 'generation': 1,
                       'worker_id': 'local-worker', 'incarnation': 'local-incarnation'}
        self.occurrence = 'original-compaction' if kind == 'compaction_reply' else 'original-turn'
        self.operation_id = self.occurrence + '-reply'
        self.directory = 'charter-output/' + C.digest(self.operation_id.encode())
        self.name = self.directory + '/manifest.json'
        self.payload = b'{"result":"original exact output"}'
        self.chunk_size = 20
        reference = {'request_id': self.occurrence, 'bytes': len(self.payload),
                     'sha256': C.digest(self.payload), 'chunk_bytes': self.chunk_size,
                     'chunks': (len(self.payload) + self.chunk_size - 1) // self.chunk_size}
        reply = {'response_id': 'original-provider-reply', 'output_item_ids': ['message-1'],
                 'tool_call_ids': [], 'declaration': {'fixture': 'original declaration'}}
        if chunks: reply['output_reference'] = reference
        observation = {'kind': kind, 'reply': reply}
        if kind == 'compaction_reply':
            observation.update(run_id=self.occurrence, summary='original retained summary')
        else:
            observation['turn_id'] = self.occurrence
            if kind == 'supplied_reply':
                observation['terminal'] = {'thread_id': 'original-thread', 'turn_id': 'public-turn',
                                           'status': 'completed'}
        self.manifest = {'revision': 1, 'caller': copy.deepcopy(self.caller),
                         'operation_id': self.operation_id, 'observation': observation}
        self.command = {'kind': 'run_attempt', 'command_id': 'original-run',
            'attempt': {'id': self.caller['attempt_id'], 'task_id': self.caller['task_id'],
                        'session_id': 'original-session', 'generation': self.caller['generation'],
                        'worker_id': self.caller['worker_id'], 'worker_incarnation': self.caller['incarnation']},
            'task': {'definition': {'id': 'original-definition', 'executor': 'coding'},
                     'resolved_limits': {'global_metadata': {'charter': {'record_bytes': 65536, 'records': 8}}}}}
        self.record = {'command': self.command, 'completed': True, 'effects_started': True}
        self.command_name = 'commands/' + self.command['command_id'].encode().hex() + '.json'
        self.chunk_names = []
        if chunks:
            for index in range(reference['chunks']):
                name = self.directory + '/' + format(index, '010d') + '.chunk'
                self.chunk_names.append(name)
                self.write(name, self.payload[index * self.chunk_size:(index + 1) * self.chunk_size])
        self.write('format.json', {'format_revision': hostmod._COMPOSED_CODEX_HOME_FORMATS['worker_format']})
        self.write('opaque-original.json', {'must': 'remain exact'})

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        for parent in path.parents:
            if parent == self.root: break
            parent.chmod(0o700)
        raw = value if isinstance(value, bytes) else C.canonical(value)
        temporary = path.with_name(path.name + '.fixture-new')
        temporary.write_bytes(raw); temporary.chmod(0o600)
        os.replace(temporary, path)
        return raw

    def baseline(self, command=True):
        self.write(self.name, self.manifest)
        if command: self.write(self.command_name, self.record)
        self.before = self.host.state_files()
        self.stop = {'state': copy.deepcopy(self.before), 'cleanup_records': {}}
        return self

    def replace_manifest(self):
        self.write(self.name, (self.root / self.name).read_bytes())
        self.current = self.host.state_files()
        assert self.before[self.name]['identity'][1] != self.current[self.name]['identity'][1]
        return self.current

    def history(self):
        return self.host.worker_history(self.stop, self.current)


@contextlib.contextmanager
def fixture(kind='reply', chunks=True):
    with tempfile.TemporaryDirectory(prefix='charter-manifest-local-') as root:
        yield ManifestFixture(root, kind, chunks)


class CharterManifestTests(unittest.TestCase):
    def setUp(self):
        for name in ('subprocess.Popen', 'socket.socket', 'os.system', 'os.posix_spawn', 'os.posix_spawnp'):
            guard = mock.patch(name, side_effect=AssertionError('external effects forbidden'))
            guard.start(); self.addCleanup(guard.stop)

    def test_all_three_original_kinds_accept_only_same_byte_manifest_replacement(self):
        for kind in KINDS:
            with self.subTest(kind=kind), fixture(kind) as f:
                f.baseline(); f.replace_manifest()
                before, current = copy.deepcopy(f.stop), copy.deepcopy(f.current)
                preserved, observations = f.history()
                self.assertTrue(f.host.codex_home_scope())
                self.assertEqual(preserved, {n: row for n, row in f.before.items() if n != f.name})
                self.assertEqual(set(observations), {f.name})
                proof = f.host._charter_manifests.proof()
                self.assertEqual(proof['records'], 1)
                row = proof['observations'][C.digest(f.name.encode())]
                self.assertEqual(row['observation_kind'], kind)
                self.assertEqual(row['original'], f.before[f.name])
                self.assertEqual(row['sampled'], f.current[f.name])
                self.assertEqual(row['original_command_row'], f.before[f.command_name])
                self.assertEqual(row['original_chunk_rows'], len(f.chunk_names))
                self.assertTrue(row['original_content_exact'] and row['inode_only'] and row['original_command_caller_join'])
                self.assertFalse(row['actual_writer_attributed'] or row['new_output_authority'] or proof['host_effects'])
                self.assertFalse(proof['directory_allowances_added'] or proof['simultaneous_snapshot'])
                self.assertEqual(f.stop, before); self.assertEqual(f.current, current)
                f.host.api.assert_not_called()

    def test_absent_output_retains_no_chunks_and_unchanged_rows_need_no_exception(self):
        with fixture(chunks=False) as f:
            f.baseline(); f.current = f.host.state_files()
            preserved, observations = f.history()
            self.assertEqual(preserved, f.before); self.assertEqual(observations, {})
            self.assertFalse(hasattr(f.host, '_charter_manifests'))
            f.replace_manifest(); preserved, observations = f.history()
            self.assertEqual(observations[f.name]['original_chunk_rows'], 0)
            self.assertEqual((f.root / f.name).read_bytes(), C.canonical(f.manifest))

    def test_original_run_attempt_caller_must_exist_match_and_be_unique(self):
        for kind in ('missing', 'new-only', 'wrong-caller', 'wrong-worker', 'incomplete', 'wrong-kind', 'duplicate'):
            with self.subTest(kind=kind), fixture() as f:
                if kind == 'wrong-caller': f.command['attempt']['task_id'] = 'other-task'
                elif kind == 'wrong-worker': f.command['attempt']['worker_id'] = 'other-worker'
                elif kind == 'incomplete': f.record['completed'] = False
                elif kind == 'wrong-kind': f.command['kind'] = 'stop_attempt'
                elif kind == 'duplicate':
                    duplicate = copy.deepcopy(f.record); duplicate['command']['command_id'] = 'other-run'
                    f.write('commands/' + b'other-run'.hex() + '.json', duplicate)
                f.baseline(command=kind not in ('missing', 'new-only'))
                if kind == 'new-only': f.write(f.command_name, f.record)
                f.replace_manifest()
                with self.assertRaisesRegex(ValueError, 'charter_manifest_(unique_original_command|command_worker|command_identity)'):
                    f.history()

    def test_changed_or_missing_original_command_refuses(self):
        for kind in ('body', 'same-bytes-new-inode', 'missing'):
            with self.subTest(kind=kind), fixture() as f:
                f.baseline(); f.replace_manifest()
                if kind == 'missing': (f.root / f.command_name).unlink()
                else:
                    record = copy.deepcopy(f.record)
                    if kind == 'body': record['command']['attempt']['generation'] += 1
                    f.write(f.command_name, record)
                f.current = f.host.state_files()
                with self.assertRaises(ValueError): f.history()

    def test_missing_drifted_or_profile_only_source_authority_refuses(self):
        cases = [('absent', None)] + [('drift', name) for name in hostmod._CHARTER_MANIFEST_SOURCES]
        for kind, name in cases:
            with self.subTest(kind=kind, source=name), fixture() as f:
                f.baseline(); f.replace_manifest()
                if kind == 'absent': del f.host._verified_manifest
                else: f.host._verified_manifest['source']['files'][name]['sha256'] = '0' * 64
                # The profile's selected source map remains intact in every case.
                self.assertEqual(f.host.p['formats']['source_files'], hostmod._COMPOSED_CODEX_HOME_SOURCES)
                with self.assertRaisesRegex(ValueError, 'original_worker_history_changed'): f.history()
                self.assertFalse(hasattr(f.host, '_charter_manifests'))

    def test_manifest_content_and_every_non_inode_identity_field_refuse(self):
        with fixture() as f:
            f.baseline(); f.manifest['observation']['reply']['response_id'] = 'changed-provider-reply'
            f.write(f.name, f.manifest); f.current = f.host.state_files()
            with self.assertRaisesRegex(ValueError, 'charter_manifest_content_or_presence_changed'): f.history()
        for field in (0, 2, 3, 4):
            with self.subTest(identity_index=field), fixture() as f:
                f.baseline(); f.replace_manifest()
                if field == 4:
                    (f.root / f.name).chmod(0o640); f.current = f.host.state_files()
                else:
                    # Synthetic wrong observed device/UID/GID, starting from an
                    # actual file sample; no privileged chown/mount is attempted.
                    f.current[f.name]['identity'][field] += 1
                with self.assertRaisesRegex(ValueError, 'charter_manifest_identity_changed'): f.history()

    def test_original_chunks_cannot_be_added_removed_replaced_or_changed(self):
        for change in ('added', 'missing', 'inode', 'content'):
            with self.subTest(change=change), fixture() as f:
                f.baseline(); f.replace_manifest(); name = f.chunk_names[0]
                if change == 'added': f.write(f.directory + '/0000000099.chunk', b'extra')
                elif change == 'missing': (f.root / name).unlink()
                else: f.write(name, (f.root / name).read_bytes() if change == 'inode' else b'changed-original-output')
                f.current = f.host.state_files()
                with self.assertRaises(ValueError): f.history()

    def test_operation_directory_and_original_command_paths_are_exact(self):
        for change in ('operation', 'directory', 'family', 'command-path'):
            with self.subTest(change=change), fixture() as f:
                if change == 'operation': f.manifest['operation_id'] = 'unrelated-reply'
                elif change == 'directory':
                    newdir = 'charter-output/' + '0' * 64
                    (f.root / f.directory).rename(f.root / newdir)
                    f.name = newdir + '/manifest.json'
                elif change == 'family': f.name = f.directory + '/nested/manifest.json'
                else: f.command_name = 'commands/not-hex.json'
                f.baseline(); f.replace_manifest()
                with self.assertRaisesRegex(ValueError, 'charter_manifest_(operation_identity|directory_identity|command_path)|original_worker_history_changed'):
                    f.history()

    def test_manifest_shapes_and_reference_extent_refuse_without_content_reinterpretation(self):
        changes = [lambda m: m.update(revision=True),
                   lambda m: m['caller'].update(generation=True),
                   lambda m: m['observation'].update(kind='begin_turn'),
                   lambda m: m['observation']['reply'].update(output=None),
                   lambda m: m['observation']['reply']['output_reference'].update(request_id='other-turn'),
                   lambda m: m['observation']['reply']['output_reference'].update(chunks=100),
                   lambda m: m['observation']['reply']['output_reference'].update(chunk_bytes=True),
                   lambda m: m['observation']['reply']['output_reference'].update(sha256='not-a-sha')]
        for index, change in enumerate(changes):
            with self.subTest(index=index), fixture() as f:
                change(f.manifest); f.baseline(); f.replace_manifest()
                with self.assertRaises(ValueError): f.history()

    def test_real_scanner_rejects_manifest_or_chunk_symlink_and_hardlink(self):
        for leaf in ('manifest', 'chunk'):
            for link in ('symlink', 'hardlink'):
                with self.subTest(leaf=leaf, link=link), fixture() as f:
                    f.baseline()
                    name = f.name if leaf == 'manifest' else f.chunk_names[0]
                    path = f.root / name
                    with tempfile.TemporaryDirectory(prefix='charter-outside-sentinel-') as other:
                        outside = Path(other) / 'sentinel'; outside.write_bytes(path.read_bytes())
                        before = outside.read_bytes(); path.unlink()
                        if link == 'symlink': path.symlink_to(outside)
                        else: os.link(outside, path)
                        with self.assertRaisesRegex(ValueError, 'state nonregular/aliased file'):
                            f.host.state_files()
                        self.assertEqual(outside.read_bytes(), before)

    def test_deadline_and_accepted_and_local_byte_bounds_refuse(self):
        for kind in ('deadline', 'accepted-record', 'local-file', 'inventory-bytes'):
            with self.subTest(kind=kind), fixture() as f:
                if kind == 'accepted-record': f.command['task']['resolved_limits']['global_metadata']['charter']['record_bytes'] = 1
                f.baseline(); f.replace_manifest()
                if kind == 'deadline': f.host.r['deadline_ms'] = 0
                elif kind == 'local-file': f.retained['worker']['command_record_bytes'] = 1
                elif kind == 'inventory-bytes':
                    f.host.l['state_bytes'] = 1
                    with self.assertRaisesRegex(ValueError, 'state inventory byte bound'): f.host.state_files()
                    continue
                with self.assertRaisesRegex(ValueError, 'charter_manifest_(deadline|accepted_record_bound)|cleanup_transition_file_metadata'):
                    f.history()

    def test_sample_revalidation_and_unrelated_row_inode_remain_strict(self):
        with fixture() as f:
            f.baseline(); f.replace_manifest()
            f.write(f.name, (f.root / f.name).read_bytes())
            with self.assertRaisesRegex(ValueError, 'charter_manifest_sample_changed'): f.history()
        with fixture() as f:
            f.baseline(); f.replace_manifest()
            f.write('opaque-original.json', (f.root / 'opaque-original.json').read_bytes())
            f.current = f.host.state_files()
            with self.assertRaisesRegex(ValueError, 'original_worker_history_changed'): f.history()


if __name__ == '__main__':
    unittest.main()
