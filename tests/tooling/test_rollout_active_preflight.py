"""Actual local atomic writers against the complete ACTIVE preflight branch.

Service/package endpoints are controlled; filesystem traversal and all inventory,
cleanup, command, ownership, alias and bound checks use maintained code.
"""
import contextlib
import copy
import os
import stat
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import rollout_host_v2 as hostmod
from rollout_fixture import cleanup_fixture, advance_directory_clock
from test_rollout_reconciliation import NoExternalEffects, clock_successor

C = hostmod.c


@contextlib.contextmanager
def active_fixture():
    with cleanup_fixture() as f:
        f.host.h['helpers'] = []
        (f.root / 'outbox').mkdir(mode=0o700)
        (f.root / 'outbox/acceptance').mkdir(mode=0o700)
        row = C.strict((f.root / f.command).read_bytes())
        row['completed'] = True
        f.write(f.command, row)
        f.host.p['formats']['before'] = {'wire': 23, 'controller_schema': 24, 'worker_format': 18}
        f.host.s = {}
        f.host._verified_before_manifest = {'source': {'files': {
            name: {'sha256': sha} for name, sha in hostmod._ACTIVE_PREFLIGHT_WRITERS.items()}}}
        f.host.compatibility = lambda: None
        f.host.metadata = lambda: {'controlled': True}
        service = {'MainPID': '42', 'InvocationID': 'same', 'ActiveState': 'active', 'SubState': 'running'}
        f.host.check_running = mock.Mock(return_value=service)
        f.host.state_size = mock.Mock(side_effect=AssertionError('active preflight must use one content traversal'))
        f.refresh()
        with mock.patch.object(C, 'verify_package', return_value=(None, f.host._verified_manifest)):
            yield f


def atomic_write(path, raw):
    temporary = path.with_suffix('.pending')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, raw)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def writer(action):
    """A real bounded other thread, synchronized at a scanner cut."""
    go = threading.Event(); done = threading.Event(); errors = []
    def run():
        if not go.wait(5):
            errors.append(AssertionError('writer was not dispatched'))
            return
        try:
            action()
        except BaseException as error:
            errors.append(error)
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    def dispatch():
        go.set()
        if not done.wait(5):
            raise AssertionError('writer did not close')
        if errors:
            raise errors[0]
    try:
        yield dispatch
    finally:
        go.set(); thread.join(5)
        if thread.is_alive():
            raise AssertionError('writer still alive')
        if errors:
            raise errors[0]


@contextlib.contextmanager
def after_directory_scan(path, action):
    # clean() also uses pathname-based scandir; only the descriptor traversal
    # reaches this controlled cut. Both use the real underlying iterator.
    target = path.stat(); original = os.scandir; fstat = os.fstat; fired = []
    @contextlib.contextmanager
    def scan(value):
        selected = False
        if isinstance(value, int):
            actual = fstat(value)
            selected = (actual.st_dev, actual.st_ino) == (target.st_dev, target.st_ino)
        with original(value) as entries:
            if selected and not fired:
                def iterator():
                    yield from entries
                    action(); fired.append(True)
                yield iterator()
            else:
                yield entries
    with mock.patch.object(os, 'scandir', side_effect=scan):
        yield fired


class ActivePreflightTests(NoExternalEffects):
    def test_complete_preflight_allows_actual_cleanup_rename_after_capture(self):
        with active_fixture() as f:
            old = (f.root / f.name).read_bytes()
            successor = C.canonical(clock_successor(f.record))
            with writer(lambda: atomic_write(f.root / f.name, successor)) as update:
                with after_directory_scan(f.root / 'cleanup', update) as fired:
                    result = f.host.execute('preflight')
            self.assertEqual(len(fired), 1)
            self.assertEqual((f.root / f.name).read_bytes(), successor)
            self.assertNotEqual(old, successor)
            observed = result['worker_inventory']
            self.assertFalse(observed['owned_snapshot'])
            self.assertFalse(observed['simultaneous_snapshot'])
            self.assertFalse(observed['complete_historical_membership'])
            self.assertEqual(observed['cleanup_records'], 1)
            self.assertGreater(observed['directory_time_changes'], 0)
            self.assertEqual(f.host.check_running.call_count, 2)
            self.assertNotIn('_worker_capture', f.host.__dict__)
            # Stop remains exact: the same directory race must still refuse.
            with after_directory_scan(f.root / 'cleanup', lambda: advance_directory_clock(f.root / 'cleanup')):
                with self.assertRaisesRegex(ValueError, 'state_directory_changed'):
                    f.host.state_files()

    def test_atomic_mutation_replacement_before_open_and_after_descriptor_read(self):
        for cut in ('before_open', 'after_read'):
            with self.subTest(cut=cut), active_fixture() as f:
                target = f.root / 'mutations/await.json'
                successor = C.canonical({'fixture_slot': 'mutations/await.json', 'observation': 2})
                real_open, real_stat = os.open, os.stat
                fired = []
                with writer(lambda: atomic_write(target, successor)) as update:
                    def opened(name, *args, **kwargs):
                        if cut == 'before_open' and name == 'await.json' and 'dir_fd' in kwargs and not fired:
                            fired.append(True); update()
                        return real_open(name, *args, **kwargs)
                    counts = []
                    def observed(name, *args, **kwargs):
                        if cut == 'after_read' and name == 'await.json' and 'dir_fd' in kwargs:
                            counts.append(True)
                            if len(counts) == 2:
                                fired.append(True); update()
                        return real_stat(name, *args, **kwargs)
                    with mock.patch.object(os, 'open', side_effect=opened), mock.patch.object(os, 'stat', side_effect=observed):
                        result = f.host.execute('preflight')
                self.assertEqual(fired, [True])
                self.assertEqual(result['worker_inventory']['atomic_path_replacements'], 1)
                self.assertEqual(target.read_bytes(), successor)

    def test_exact_incomplete_pending_is_metadata_only_and_requires_target(self):
        with active_fixture() as f:
            pending = f.root / 'mutations/await.pending'
            pending.write_bytes(b'{incomplete'); pending.chmod(0o600)
            result = f.host.execute('preflight')
            inventory = result['worker_inventory']
            self.assertEqual(inventory['temporary_targets_observed'], 1)
            self.assertFalse(inventory['temporary_bodies_validated'])
            self.assertFalse(result['state_size']['content_hashed'])
            self.assertGreater(inventory['stats']['bytes'], sum(p.stat().st_size for p in [pending]))
            # Stopped inventory still includes exact temporary bytes for preservation.
            stopped = f.host.state_files()
            self.assertEqual(stopped['mutations/await.pending']['sha256'], C.digest(b'{incomplete'))
            (f.root / 'mutations/await.json').unlink()
            with self.assertRaisesRegex(ValueError, 'temporary without sampled target'):
                f.host.execute('preflight')

    def test_unknown_pending_and_disappeared_entry_do_not_get_exemption(self):
        with active_fixture() as f:
            unknown = f.root / 'cleanup/unknown.pending'
            unknown.write_bytes(b'{incomplete'); unknown.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'exact sample|required|relative path'):
                f.host.execute('preflight')
        for name in ('mutations/await.json', 'mutations/unknown.pending'):
            with self.subTest(name=name), active_fixture() as f:
                path = f.root / name; path.write_bytes(b'x'); path.chmod(0o600)
                real_stat = os.stat
                def observed(leaf, *args, **kwargs):
                    if leaf == path.name and 'dir_fd' in kwargs and path.exists():
                        path.unlink()
                    return real_stat(leaf, *args, **kwargs)
                with mock.patch.object(os, 'stat', side_effect=observed), self.assertRaises(FileNotFoundError):
                    f.host.execute('preflight')

    def test_exact_pending_rename_between_enumeration_and_stat_is_unobserved(self):
        for name in ('mutations/await.json', 'cleanup'):
            with self.subTest(name=name), active_fixture() as f:
                f.host.l['state_bytes'] = 8 * 1048576
                target = f.root / (f.name if name == 'cleanup' else name)
                pending = target.with_suffix('.pending')
                successor = target.read_bytes()
                pending.write_bytes(b'{partial'); pending.chmod(0o600)
                original_stat = os.stat; fired = []
                with writer(lambda: atomic_write(target, successor)) as update:
                    def observed(leaf, *args, **kwargs):
                        if leaf == pending.name and 'dir_fd' in kwargs and not fired:
                            fired.append(True); update()
                        return original_stat(leaf, *args, **kwargs)
                    with mock.patch.object(os, 'stat', side_effect=observed):
                        result = f.host.execute('preflight')
                inventory = result['worker_inventory']
                self.assertEqual(fired, [True])
                self.assertEqual(inventory['vanished_temporary_names'], 1)
                self.assertFalse(inventory['vanished_temporary_metadata_validated'])
                self.assertFalse(inventory['temporary_bodies_validated'])
                self.assertFalse(result['state_size']['content_hashed'])
                self.assertGreater(inventory['vanished_temporary_reserved_bytes'], 0)
                self.assertFalse(pending.exists())
                self.assertEqual(target.read_bytes(), successor)

    def test_directory_identity_owner_mode_size_and_link_count_remain_exact(self):
        for field in ('st_dev', 'st_ino', 'st_uid', 'st_gid', 'st_mode', 'st_nlink', 'st_size'):
            with self.subTest(field=field), active_fixture() as f:
                target = (f.root / 'cleanup').stat(); changed = []
                original = os.fstat
                def observed(fd):
                    value = original(fd)
                    if changed and (value.st_dev, value.st_ino) == (target.st_dev, target.st_ino):
                        attrs = {name: getattr(value, name) for name in dir(value) if name.startswith('st_')}
                        attrs[field] += 1
                        return SimpleNamespace(**attrs)
                    return value
                with after_directory_scan(f.root / 'cleanup', lambda: changed.append(True)), mock.patch.object(os, 'fstat', side_effect=observed):
                    with self.assertRaises(ValueError):
                        f.host.execute('preflight')

    def test_real_directory_inode_replacement_and_unrelated_clock_refuse(self):
        for change in ('replace', 'unrelated_clock'):
            with self.subTest(change=change), active_fixture() as f:
                path = f.root / ('cleanup' if change == 'replace' else 'mutations/nested')
                def mutate():
                    if change == 'replace':
                        path.rename(path.with_name('cleanup-old')); path.mkdir(mode=0o700)
                    else:
                        advance_directory_clock(path)
                with after_directory_scan(path, mutate), self.assertRaises(ValueError):
                    f.host.execute('preflight')

    def test_selected_mutable_wrong_mode_hardlink_symlink_and_special_refuse(self):
        for change in ('mode', 'hardlink', 'symlink', 'fifo'):
            with self.subTest(change=change), active_fixture() as f:
                target = f.root / 'mutations/await.json'
                if change == 'mode': target.chmod(0o644)
                elif change == 'hardlink': os.link(target, f.root / 'alias')
                else:
                    target.unlink()
                    if change == 'symlink': target.symlink_to(f.root / 'format.json')
                    else: os.mkfifo(target, 0o600)
                with self.assertRaises(ValueError): f.host.execute('preflight')

    def test_unknown_aliases_refuse_before_any_service_effect(self):
        for change in ('hardlink', 'symlink', 'fifo'):
            with self.subTest(change=change), active_fixture() as f:
                path = f.root / 'alien'
                if change == 'hardlink': os.link(f.root / 'format.json', path)
                elif change == 'symlink': path.symlink_to(f.root / 'format.json')
                else: os.mkfifo(path, 0o600)
                with self.assertRaises(ValueError): f.host.execute('preflight')

    def test_changed_in_place_or_immutable_command_refuses(self):
        for name in ('mutations/await.json', 'command'):
            with self.subTest(name=name), active_fixture() as f:
                path = f.root / (f.command if name == 'command' else name)
                original_open = os.open; fired = []
                def opened(leaf, *args, **kwargs):
                    if leaf == path.name and 'dir_fd' in kwargs and not fired:
                        fired.append(True)
                        path.write_bytes(path.read_bytes() + b' ')
                    return original_open(leaf, *args, **kwargs)
                with mock.patch.object(os, 'open', side_effect=opened), self.assertRaisesRegex(ValueError, 'changed before read'):
                    f.host.execute('preflight')

    def test_bounds_deadline_and_service_replacement_refuse(self):
        for guard in ('state_entries', 'state_bytes', 'deadline', 'service'):
            with self.subTest(guard=guard), active_fixture() as f:
                if guard in f.host.l: f.host.l[guard] = 1
                elif guard == 'deadline': f.host.r['deadline_ms'] = C.now_ms() - 1
                else:
                    original = f.host.check_running.return_value
                    f.host.check_running.side_effect = [original, {**original, 'InvocationID': 'replaced'}]
                with self.assertRaises(ValueError): f.host.execute('preflight')

    def test_clean_and_cleanup_command_binding_still_refuse(self):
        for guard in ('unfinished', 'outbox', 'helper', 'cleanup_binding'):
            with self.subTest(guard=guard), active_fixture() as f:
                if guard in ('unfinished', 'cleanup_binding'):
                    command = C.strict((f.root / f.command).read_bytes())
                    if guard == 'unfinished': command['completed'] = False
                    else: command['command']['attempt']['worker_id'] = 'another-worker'
                    f.write(f.command, command)
                elif guard == 'outbox': f.write('outbox/pending.json', {})
                else:
                    f.host.h['helpers'] = [str(f.root / 'processes')]
                    f.write('processes/1.json', {})
                with self.assertRaises(ValueError): f.host.execute('preflight')

    def test_in_place_change_during_open_descriptor_read_refuses(self):
        with active_fixture() as f:
            target = f.root / 'mutations/await.json'
            identity = target.stat(); original = os.fdopen; fired = []
            @contextlib.contextmanager
            def stream(fd, *args, **kwargs):
                with original(fd, *args, **kwargs) as inner:
                    actual = os.fstat(fd)
                    if (actual.st_dev, actual.st_ino) == (identity.st_dev, identity.st_ino):
                        class Reader:
                            def read(self, bound):
                                raw = inner.read(bound)
                                target.write_bytes(raw + b' '); fired.append(True)
                                return raw
                        yield Reader()
                    else:
                        yield inner
            with mock.patch.object(os, 'fdopen', side_effect=stream), self.assertRaisesRegex(ValueError, 'changed during read'):
                f.host.execute('preflight')
            self.assertEqual(fired, [True])

    def test_completed_coding_arg0_link_is_validated_without_following_target(self):
        with active_fixture() as f:
            command = C.strict((f.root / f.command).read_bytes())
            command['command']['task'] = {'definition': {'executor': 'coding'}}
            f.write(f.command, command)
            base = 'codex/' + f.record['attempt_id'].encode().hex()
            directory = f.root / base / 'tmp/arg0-linux/codex-arg0Abc123'
            directory.mkdir(parents=True, mode=0o700)
            parent = directory
            while parent != f.root:
                parent.chmod(0o700); parent = parent.parent
            (directory / 'apply_patch').symlink_to('/absent-and-never-opened')
            self.assertEqual(f.host.execute('preflight')['worker_inventory']['cleanup_records'], 1)
            command['command']['task']['definition']['executor'] = 'native'
            f.write(f.command, command)
            with self.assertRaisesRegex(ValueError, 'no completed original Coding command'):
                f.host.execute('preflight')

    def test_candidate_writer_cannot_authorize_old_installed_sample(self):
        with active_fixture() as f:
            f.host._verified_manifest = copy.deepcopy(f.host._verified_before_manifest)
            f.host._verified_before_manifest['source']['files']['src/worker/process.rs']['sha256'] = '0' * 64
            # It falls back to the original strict preflight, not the candidate hook.
            with self.assertRaisesRegex(AssertionError, 'one content traversal'):
                f.host.execute('preflight')

    def test_stopped_predecessor_inventory_stays_inside_owner_lifetime(self):
        with active_fixture() as f:
            f.host.h['stopped_predecessor'] = {}; held = []
            @contextlib.contextmanager
            def owner():
                held.append(True)
                try: yield
                finally: held.pop()
            f.host.owner = owner
            row = {'MainPID': '0', 'ActiveState': 'inactive'}
            def checked():
                self.assertEqual(held, [True]); return row
            f.host.check_stopped_predecessor = checked
            f.host.stopped = checked
            strict = f.host.state_files
            def state():
                self.assertEqual(held, [True]); return strict()
            f.host.state_files = state
            f.host.state_size = lambda: {'controlled': True}
            result = f.host.execute('preflight')
            self.assertNotIn('atomic_path_replacements', result['worker_inventory'])
            f.host.check_running.assert_not_called()
            self.assertEqual(held, [])


    def test_current_installed_family_samples_actual_mutations_writer_and_keeps_stopped_strict(self):
        with active_fixture() as f:
            f.host.p['formats']['before'] = dict(hostmod._ACTIVE_PREFLIGHT_CURRENT_FORMATS)
            f.host._verified_before_manifest = {'source': {'files': {
                name: {'sha256': sha} for name, sha in hostmod._ACTIVE_PREFLIGHT_CURRENT_WRITERS.items()}}}
            directory = f.root / 'mutations'; target = directory / 'await.json'
            old = directory.stat()
            def update():
                atomic_write(target, target.read_bytes())
                advance_directory_clock(directory)
            with writer(update) as dispatch, after_directory_scan(directory, dispatch) as fired:
                result = f.host.execute('preflight')
            self.assertEqual(fired, [True])
            current = directory.stat()
            self.assertEqual(tuple(getattr(old, k) for k in ('st_dev','st_ino','st_uid','st_gid','st_mode','st_nlink','st_size')),
                             tuple(getattr(current, k) for k in ('st_dev','st_ino','st_uid','st_gid','st_mode','st_nlink','st_size')))
            self.assertNotEqual((old.st_mtime_ns, old.st_ctime_ns), (current.st_mtime_ns, current.st_ctime_ns))
            sample = result['worker_inventory']
            self.assertEqual(sample['installed_writer_sources'], hostmod._ACTIVE_PREFLIGHT_CURRENT_WRITERS)
            self.assertGreater(sample['directory_time_changes'], 0)
            self.assertFalse(sample['owned_snapshot']); self.assertFalse(sample['simultaneous_snapshot'])
            self.assertFalse(sample['complete_historical_membership'])
            f.host.state_size.assert_not_called()
            with after_directory_scan(directory, update), self.assertRaisesRegex(ValueError, 'state_directory_changed'):
                f.host.state_files()

    def test_current_writer_family_requires_each_installed_pin_and_exact_before_formats(self):
        for guard in (*hostmod._ACTIVE_PREFLIGHT_CURRENT_WRITERS, 'missing', 'formats'):
            with self.subTest(guard=guard), active_fixture() as f:
                f.host.p['formats']['before'] = dict(hostmod._ACTIVE_PREFLIGHT_CURRENT_FORMATS)
                files = {name: {'sha256': sha} for name, sha in hostmod._ACTIVE_PREFLIGHT_CURRENT_WRITERS.items()}
                f.host._verified_before_manifest = {'source': {'files': files}}
                f.host._verified_manifest = copy.deepcopy(f.host._verified_before_manifest)
                if guard == 'missing': del files['src/worker/mod.rs']
                elif guard == 'formats': f.host.p['formats']['before']['worker_format'] = 20
                else: files[guard]['sha256'] = '0' * 64
                self.assertIsNone(hostmod.active_preflight_writers(f.host))
                with self.assertRaisesRegex(ValueError, 'active sample installed writer differs'):
                    hostmod.ActiveWorkerSample(f.host)
                with self.assertRaisesRegex(AssertionError, 'one content traversal'):
                    f.host.execute('preflight')

    def test_current_family_cannot_authorize_candidate_only_or_mixed_historical_writer(self):
        for guard in ('candidate_only', 'mixed'):
            with self.subTest(guard=guard), active_fixture() as f:
                f.host.p['formats']['before'] = dict(hostmod._ACTIVE_PREFLIGHT_CURRENT_FORMATS)
                current = {'source': {'files': {name: {'sha256': sha}
                    for name, sha in hostmod._ACTIVE_PREFLIGHT_CURRENT_WRITERS.items()}}}
                f.host._verified_manifest = copy.deepcopy(current)
                if guard == 'candidate_only':
                    f.host._verified_before_manifest = {'source': {'files': {}}}
                else:
                    f.host._verified_before_manifest = current
                    current['source']['files']['src/worker/cleanup.rs']['sha256'] = hostmod._ACTIVE_PREFLIGHT_WRITERS['src/worker/cleanup.rs']
                self.assertIsNone(hostmod.active_preflight_writers(f.host))
                with self.assertRaisesRegex(AssertionError, 'one content traversal'):
                    f.host.execute('preflight')
