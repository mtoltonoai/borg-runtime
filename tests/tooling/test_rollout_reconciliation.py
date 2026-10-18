"""Maintained reconciliation predicates on local files and controlled service/API seams.

No installed-host access, network, subprocess, dynamic source rewriting or retries.
The scratch inventory is sampled evidence, never a simultaneous owned snapshot.
"""
import contextlib, copy, os, signal, sqlite3, stat, tempfile, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import rollout_common_v2 as common
import protected_rollout_v2 as adapter
import rollout_host_v2 as hostmod
from rollout_fixture import (Fixture, FakeProducer, save, CleanupFixture, cleanup_fixture,
                             after_directory_scan, advance_directory_clock, error_cause)
C=hostmod.c
SLOTS=('mutations/registration.json','mutations/await.json')
SIBLINGS=('mutations/settled-original.json','mutations/nested/retained-original.json')
CLEANUP_WRITERS=('a4c913604c5e95cdd39d0be3fd76f6b2101f6427a9c7aff44ceaada409577ab1',
                 'bef4283ed36a64f71f9d2e0b0efaf1e738cdd5e4814389de4c7e85bd8dd92e73')

def clock_successor(value):
    later=copy.deepcopy(value);later['last_observed_ms']+=1;return later

class NoExternalEffects(unittest.TestCase):
    def setUp(self):
        for name in ('subprocess.Popen','socket.socket'):
            guard=mock.patch(name,side_effect=AssertionError('local fixture cannot start process or connect'))
            guard.start();self.addCleanup(guard.stop)

class CaptureTests(NoExternalEffects):
    def test_original_refuses_cleanup_clock_race_but_capture_preserves_exact_sample(self):
        with cleanup_fixture() as f:
            directory = f.root / 'cleanup'
            with after_directory_scan(directory, lambda: advance_directory_clock(directory)) as fired:
                with self.assertRaisesRegex(ValueError, 'state_directory_changed'):
                    f.host.state_files()
            self.assertEqual(len(fired), 1)
            hook = f.capture()
            with after_directory_scan(directory, lambda: advance_directory_clock(directory)) as fired:
                current = hook.inventory()
            self.assertEqual(len(fired), 1)
            self.assertEqual(current, f.before)
            proof = hook.proof()
            self.assertEqual(proof['implementation'], 'direct_scanner_hooks')
            self.assertEqual(proof['records'], 1)
            self.assertEqual(proof['directory_before'][:7], proof['directory_after'][:7])
            self.assertNotEqual(proof['directory_before'][7:], proof['directory_after'][7:])
            self.assertGreater(proof['directory_time_comparisons_changed'], 0)
            self.assertTrue(proof['complete_original_membership'])
            for key in ('later_record_reread', 'simultaneous_snapshot', 'host_mutations'):
                self.assertIs(proof[key], False)


    def test_valid_clock_successor_uses_captured_bytes_identity_and_hash_without_reread(self):
        with cleanup_fixture() as f:
            successor = clock_successor(f.record)
            f.write(f.name, successor)
            captured = (f.root / f.name).read_bytes()
            hook = f.capture()
            current = hook.inventory()
            self.assertEqual(current[f.name]['sha256'], C.digest(captured))
            self.assertNotEqual(current[f.name]['identity'][1], f.before[f.name]['identity'][1])
            # A later replacement cannot become the earlier inventory's input.
            f.write(f.name, clock_successor(successor))
            with mock.patch.object(os, 'open', side_effect=AssertionError('later file open')), \
                 mock.patch.object(C, 'read', side_effect=AssertionError('later file read')):
                self.assertEqual(f.host.cleanup_bytes(f.name, current[f.name], len(captured)), captured)
                observed = f.host.cleanup_observation(f.name, f.before[f.name], current[f.name],
                                                      f.stop['cleanup_records'])
                self.assertEqual(observed['last_observed_ms_after'], successor['last_observed_ms'])
                for field in ('inode', 'hash'):
                    entry = copy.deepcopy(current[f.name])
                    if field == 'inode':
                        entry['identity'][1] += 1
                    else:
                        entry['sha256'] = '0' * 64
                    with self.subTest(field=field), self.assertRaises(ValueError):
                        f.host.cleanup_bytes(f.name, entry, len(captured))
                with self.assertRaises(ValueError):
                    f.host.cleanup_bytes('cleanup/00.json', current[f.name], len(captured))
                with self.assertRaises(ValueError):
                    f.host.cleanup_bytes(f.name, current[f.name], len(captured) - 1)
            self.assertIs(hook.proof()['later_record_reread'], False)
            with self.assertRaisesRegex(ValueError, 'cleanup_capture_no_repeat'):
                hook.inventory()


    def test_added_or_removed_child_after_enumeration_refuses(self):
        for kind in ('added', 'removed'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                def mutate():
                    if kind == 'added':
                        f.write('cleanup/00.json', f.record)
                    else:
                        (f.root / f.name).unlink()
                with after_directory_scan(f.root / 'cleanup', mutate) as fired:
                    with self.assertRaises(ValueError):
                        hook.inventory()
                self.assertEqual(len(fired), 1)
                with self.assertRaises(ValueError):
                    hook.proof()
                with mock.patch.object(os, 'scandir', side_effect=AssertionError('silent retry')):
                    with self.assertRaisesRegex(ValueError, 'cleanup_capture_no_repeat'):
                        hook.inventory()


    def test_cleanup_directory_nonclock_metadata_changes_refuse(self):
        fields = ('st_dev', 'st_ino', 'st_uid', 'st_gid', 'st_mode', 'st_nlink', 'st_size')
        full_fields = fields + ('st_mtime_ns', 'st_ctime_ns')
        for field in fields:
            with self.subTest(field=field), cleanup_fixture() as f:
                hook = f.capture()
                directory = (f.root / 'cleanup').stat()
                fstat = os.fstat
                changed = []
                def metadata(fd):
                    value = fstat(fd)
                    if changed and (value.st_dev, value.st_ino) == (directory.st_dev, directory.st_ino):
                        result = {name: getattr(value, name) for name in full_fields}
                        result[field] += 1
                        return SimpleNamespace(**result)
                    return value
                with after_directory_scan(f.root / 'cleanup', lambda: changed.append(True)), \
                     mock.patch.object(os, 'fstat', side_effect=metadata):
                    with self.assertRaisesRegex(ValueError, 'state_directory_changed'):
                        hook.inventory()
                self.assertTrue(changed)
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_other_directory_clock_change_remains_refused(self):
        with cleanup_fixture() as f:
            hook = f.capture()
            directory = f.root / 'commands'
            with after_directory_scan(directory, lambda: advance_directory_clock(directory)) as fired:
                with self.assertRaisesRegex(ValueError, 'state_directory_changed'):
                    hook.inventory()
            self.assertEqual(len(fired), 1)
            with self.assertRaises(ValueError):
                hook.proof()


    def test_nonclock_record_change_and_missing_original_command_refuse(self):
        for kind in ('record', 'command'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                if kind == 'record':
                    changed = clock_successor(f.record)
                    changed['status'] = 'failed'
                    f.write(f.name, changed)
                else:
                    (f.root / f.command).unlink()
                hook = f.capture()
                with self.assertRaises(ValueError):
                    hook.inventory()
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_cleanup_symlink_and_original_file_read_race_remain_refused(self):
        for kind in ('symlink', 'read_race'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                path = f.root / f.name
                if kind == 'symlink':
                    path.unlink()
                    path.symlink_to(f.root / f.command)
                    with self.assertRaises(ValueError):
                        hook.inventory()
                else:
                    raw = path.read_bytes()
                    digest = C.digest
                    writes = []
                    def racing_digest(value):
                        result = digest(value)
                        if value == raw and not writes:
                            path.write_bytes(C.canonical(clock_successor(f.record)))
                            writes.append(True)
                        return result
                    with mock.patch.object(C, 'digest', side_effect=racing_digest):
                        with self.assertRaisesRegex(ValueError, 'state file changed during read'):
                            hook.inventory()
                    self.assertEqual(len(writes), 1)
                with self.assertRaises(ValueError):
                    hook.proof()


class LiveJournalTests(NoExternalEffects):
    def test_two_opaque_slots_and_mutations_timestamps_with_cleanup_clock_preservation(self):
        with cleanup_fixture() as f:
            for name in SLOTS:
                f.write(name, {'fixture_slot': name, 'observation': 2})
            f.write(f.name, clock_successor(f.record))
            hook = f.capture()
            directory = f.root / 'mutations'
            with after_directory_scan(directory, lambda: advance_directory_clock(directory)) as fired:
                current = hook.inventory()
            self.assertEqual(fired, [True])
            for name in SLOTS:
                self.assertNotEqual(current[name]['sha256'], f.before[name]['sha256'])
            for name in SIBLINGS:
                self.assertEqual(current[name], f.before[name])
            preserved, observed = f.host.worker_history(f.stop, current)
            self.assertEqual(observed[f.name]['last_observed_ms_after'], f.record['last_observed_ms'] + 1)
            self.assertTrue(all(name in preserved for name in SIBLINGS))
            proof = hook.proof()
            self.assertIs(proof['simultaneous_snapshot'], False)
            self.assertIs(proof['mutable_bytes_semantically_decoded'], False)
            self.assertTrue(proof['complete_mutation_membership'])
            self.assertEqual(set(proof['mutation_slots']), set(SLOTS))
            self.assertEqual(proof['mutation_records'], 4)
            self.assertEqual(proof['mutation_directories'], 2)
            self.assertEqual(proof['mutation_immutable_records'], 2)
            self.assertGreater(proof['mutation_directory_time_comparisons_changed'], 0)
            for name, sample in proof['mutation_slots'].items():
                self.assertEqual(sample['sha256'], current[name]['sha256'])
                self.assertEqual(sample['identity'], current[name]['identity'])
                self.assertLessEqual(sample['bytes'], proof['mutation_record_bound'])
            with self.assertRaisesRegex(ValueError, 'no_repeat'):
                hook.inventory()


    def test_added_removed_pending_and_empty_directory_membership_refuse(self):
        for kind in ('added', 'removed_slot', 'removed_sibling', 'pending', 'empty_directory'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                def mutate():
                    if kind == 'added':
                        f.write('mutations/unrecognized.json', {'new': True})
                    elif kind == 'removed_slot':
                        (f.root / SLOTS[0]).unlink()
                    elif kind == 'removed_sibling':
                        (f.root / SIBLINGS[0]).unlink()
                    elif kind == 'pending':
                        f.write('mutations/await.pending', {'pending': True})
                    else:
                        (f.root / 'mutations/unrecognized-empty').mkdir(mode=0o700)
                with after_directory_scan(f.root / 'mutations', mutate) as fired:
                    with self.assertRaises(ValueError):
                        hook.inventory()
                self.assertEqual(fired, [True])
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_nonmutable_direct_and_nested_record_changes_refuse(self):
        for name in SIBLINGS:
            with self.subTest(name=name), cleanup_fixture() as f:
                f.write(name, {'changed': 'nonmutable original history'})
                hook = f.capture()
                with self.assertRaises(ValueError):
                    hook.inventory()
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_mutations_directory_owner_mode_inode_changes_refuse(self):
        names = ('st_dev', 'st_ino', 'st_uid', 'st_gid', 'st_mode',
                 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        for field in ('st_uid', 'st_mode', 'st_ino'):
            with self.subTest(field=field), cleanup_fixture() as f:
                hook = f.capture()
                directory = (f.root / 'mutations').stat()
                fstat = os.fstat
                changed = []
                def metadata(fd):
                    value = fstat(fd)
                    if changed and (value.st_dev, value.st_ino) == (directory.st_dev, directory.st_ino):
                        fields = {name: getattr(value, name) for name in names}
                        fields[field] += 1
                        return SimpleNamespace(**fields)
                    return value
                with after_directory_scan(f.root / 'mutations', lambda: changed.append(True)), \
                     mock.patch.object(os, 'fstat', side_effect=metadata):
                    with self.assertRaises(ValueError):
                        hook.inventory()
                self.assertEqual(changed, [True])
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_post_read_slot_replacement_is_sampled_but_descriptor_or_sibling_race_refuses(self):
        cases = [(name, 'replacement', True) for name in SLOTS]
        cases += [(SLOTS[0], 'in_place', False), (SIBLINGS[0], 'replacement', False)]
        for name, kind, allowed in cases:
            with self.subTest(name=name, kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                path = f.root / name
                raw = path.read_bytes()
                before = copy.deepcopy(f.before[name])
                digest = C.digest
                stat = os.stat
                writes = []
                stats = []
                def replace_after_descriptor_read(target, *args, **kwargs):
                    if (kind == 'replacement' and target == Path(name).name
                            and kwargs.get('dir_fd') is not None):
                        stats.append(True)
                        if len(stats) == 2:
                            f.write(name, {'fixture_slot': name, 'observation': 2})
                            writes.append(True)
                    return stat(target, *args, **kwargs)
                def racing_digest(value):
                    result = digest(value)
                    if kind == 'in_place' and value == raw and not writes:
                        later = {'fixture_slot': name, 'observation': 2}
                        path.write_bytes(C.canonical(later))
                        writes.append(True)
                    return result
                with mock.patch.object(C, 'digest', side_effect=racing_digest), \
                     mock.patch.object(os, 'stat', side_effect=replace_after_descriptor_read):
                    if allowed:
                        current = hook.inventory()
                    else:
                        with self.assertRaises(ValueError):
                            hook.inventory()
                self.assertEqual(writes, [True])
                if allowed:
                    # The pathname now points at another observation. Inventory
                    # must retain the exact bytes/hash/inode of its stable fd.
                    self.assertEqual(current[name], before)
                    self.assertNotEqual(C.digest(path.read_bytes()), current[name]['sha256'])
                    with mock.patch.object(os, 'open', side_effect=AssertionError('later file read')), \
                         mock.patch.object(C, 'read', side_effect=AssertionError('later file read')):
                        proof = hook.proof()
                    self.assertIs(proof['simultaneous_snapshot'], False)
                    self.assertGreater(proof['slot_post_read_path_replacements'], 0)
                    self.assertEqual(proof['mutation_slots'][name]['sha256'], C.digest(raw))
                else:
                    with self.assertRaises(ValueError):
                        hook.proof()


    def test_slot_bound_mode_link_and_no_follow_refusals(self):
        for kind in ('oversize', 'mode', 'hardlink', 'symlink'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                path = f.root / SLOTS[0]
                if kind == 'oversize':
                    path.write_bytes(b'x' * (f.host.l['state_bytes'] + 1))
                elif kind == 'mode':
                    path.chmod(0o640)
                elif kind == 'hardlink':
                    os.link(path, f.root / 'alias-outside-mutations')
                else:
                    path.unlink()
                    path.symlink_to(f.root / SIBLINGS[0])
                hook = f.capture()
                with self.assertRaises(ValueError):
                    hook.inventory()
                with self.assertRaises(ValueError):
                    hook.proof()


def capture_and_history(f):
    current=f.hook.inventory();f.host.worker_history(f.stop,current);return current

class RegistryTests(NoExternalEffects):
    def test_derived_registry_ctime_only_preserves_all_original_members_and_bytes(self):
        for suffix in ('', '/home', '/home/code'):
            with self.subTest(suffix=suffix), cleanup_fixture() as f:
                hook = f.capture()
                directory = f.root / (f.registry + suffix)
                before = directory.stat()
                with after_directory_scan(directory, lambda: directory.chmod(0o700)) as fired:
                    current = capture_and_history(f)
                after = directory.stat()
                self.assertEqual(fired, [True])
                self.assertEqual(before.st_mtime_ns, after.st_mtime_ns)
                self.assertNotEqual(before.st_ctime_ns, after.st_ctime_ns)
                self.assertEqual(current, f.before)
                self.assertIs(hook.proof()['simultaneous_snapshot'], False)


    def test_mtime_owner_mode_inode_or_unknown_directory_ctime_refuse(self):
        fields = ('st_dev', 'st_ino', 'st_uid', 'st_gid', 'st_mode',
                  'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        for kind in ('mtime', 'st_uid', 'st_mode', 'st_ino', 'st_ctime_ns',
                     'unknown', 'unknown_descendant'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                relative = (f.unknown if kind == 'unknown' else
                            f.registry + '/unrecognized-descendant' if kind == 'unknown_descendant'
                            else f.registry)
                directory = f.root / relative
                expected = directory.stat()
                fstat = os.fstat
                changed = []
                def metadata(fd):
                    observed = fstat(fd)
                    if (kind.startswith('st_') and changed
                            and (observed.st_dev, observed.st_ino) == (expected.st_dev, expected.st_ino)):
                        values = {name: getattr(observed, name) for name in fields}
                        values[kind] += -1 if kind == 'st_ctime_ns' else 1
                        return SimpleNamespace(**values)
                    return observed
                def mutate():
                    if kind == 'mtime':
                        advance_directory_clock(directory)
                    elif kind in ('unknown', 'unknown_descendant'):
                        directory.chmod(0o700)
                    changed.append(True)
                with after_directory_scan(directory, mutate), \
                     mock.patch.object(os, 'fstat', side_effect=metadata):
                    with self.assertRaises(ValueError):
                        capture_and_history(f)
                self.assertEqual(changed, [True])
                with self.assertRaises(ValueError):
                    hook.proof()


    def test_added_or_removed_registry_child_refuses_even_with_original_mtime(self):
        for kind in ('added', 'removed'):
            with self.subTest(kind=kind), cleanup_fixture() as f:
                hook = f.capture()
                directory = f.root / f.registry
                before = directory.stat()
                def mutate():
                    if kind == 'added':
                        f.write(f.registry + '/unrecognized.json', {'new': True})
                    else:
                        (directory / 'original-one.json').unlink()
                    os.utime(directory, ns=(before.st_atime_ns, before.st_mtime_ns))
                with after_directory_scan(directory, mutate) as fired:
                    with self.assertRaises(ValueError):
                        capture_and_history(f)
                self.assertEqual(fired, [True])
                self.assertEqual(directory.stat().st_mtime_ns, before.st_mtime_ns)


    def test_retained_registry_content_change_is_not_a_clock_observation(self):
        with cleanup_fixture() as f:
            path = f.root / f.registry / 'original-one.json'
            before = path.stat()
            path.write_bytes(C.canonical({'fixture': 'original-one.json', 'retained': False}))
            self.assertEqual(path.stat().st_ino, before.st_ino)
            f.capture()
            with self.assertRaises(ValueError):
                capture_and_history(f)



    def test_unselected_process_writer_keeps_exact_directory_clock_comparison(self):
        for change in ('missing','wrong'):
            with self.subTest(change=change),cleanup_fixture() as f:
                if change=='missing':del f.host._verified_manifest
                else:f.host._verified_manifest['source']['files']['src/worker/process.rs']['sha256']='0'*64
                hook=f.capture();directory=f.root/f.registry
                with after_directory_scan(directory,lambda:directory.chmod(0o700)):
                    with self.assertRaisesRegex(ValueError,'state_directory_changed'):hook.inventory()
                with self.assertRaises(ValueError):hook.proof()

class TransitionFixture:
    def __init__(self,f,kind):
        self.f=f;self.host=f.host;self.kind=kind
        self.workspace=f.root/'workspace-fixture'
        attempt={'id':f.record['attempt_id'],'task_id':'local-task','session_id':'local-session',
                 'generation':1,'worker_id':'agent-lab-2','worker_incarnation':'local-incarnation',
                 'deadline_unix_ms':2000000000000}
        self.command={'kind':'run_attempt','command_id':f.record['command_id'],'attempt':attempt,
            'task':{'definition':{'id':'local-agent','executor':'coding'},
                    'repository':{'registration':{'repository_id':'local-repository','registration_revision':'local-revision'},
                                  'url':'file:///local-source','checkpoint_remote':'file:///local-checkpoint'}}}
        self.path=self.workspace/'workspaces'/attempt['id'].encode().hex()
        self.cache=self.workspace/'repositories/repository-0123456789abcdef'
        self.gitdir=self.cache/'repository.git/worktrees/local-slot'
        for p in [self.path,self.gitdir]:p.mkdir(mode=0o700,parents=True)
        for p in [self.workspace,*self.workspace.rglob('*')]:
            if p.is_dir():p.chmod(0o700)
        self.raw(self.path/'.git',('gitdir: '+str(self.gitdir)+'\n').encode())
        self.raw(self.gitdir/'commondir',b'../..\n')
        self.raw(self.gitdir/'gitdir',(str(self.path/'.git')+'\n').encode())
        self.raw(self.gitdir/'HEAD',b'a'*40+b'\n')
        self.config=b'[core]\nrepositoryformatversion = 0\nfilemode = true\nbare = true\n[remote \"origin\"]\nurl = file:///local-source\n'
        self.raw(self.cache/'repository.git/config',self.config)
        repo=self.command['task']['repository']
        self.marker={'version':2,'registration':repo['registration'],
                     'source_endpoint':repo['url'],'checkpoint_endpoint':repo['checkpoint_remote']}
        self.raw(self.cache/'identity.json',C.canonical(self.marker))
        def identity(p):
            m=p.stat();return {'device':m.st_dev,'inode':m.st_ino}
        self.newproof={'path':str(self.path),'root':identity(self.path.parent),
                      'directory':identity(self.path),'cache':identity(self.cache),'head':'a'*40,'discard_source':True}
        self.original=copy.deepcopy(f.record)
        self.original['worktrees']=[{'proof':copy.deepcopy(self.newproof),'started':False,'done':False},
                                    {'proof':None,'started':False,'done':False}]
        if kind=='cancelled':
            self.original.update(status=None,reason='local_recovery',checkpoint=None,first_safe_ms=None,
                                 next_due_ms=self.original['last_observed_ms']+self.original['policy']['retry_delay_ms'])
            self.original['worktrees'][0]['proof']=None
        self.observed=copy.deepcopy(self.original)
        if kind=='grace':
            end=self.original['next_due_ms']+100
            self.observed.update(reason='cache_dependency',last_observed_ms=end,
                next_due_ms=end+self.original['policy']['retry_delay_ms'])
            for slot in self.observed['worktrees']:
                slot['started']=slot['proof'] is not None;slot['done']=True
        elif kind=='cancelled':
            first=self.original['last_observed_ms']+100
            self.observed.update(status='cancelled',reason='grace',first_safe_ms=first,
                last_observed_ms=first+100,next_due_ms=first+self.original['policy']['cancelled_grace_ms'])
            self.observed['worktrees'][0]['proof']=copy.deepcopy(self.newproof)
        else:self.observed['last_observed_ms']+=1
        task={'id':attempt['task_id'],'session_id':attempt['session_id'],'assigned_agent_id':'local-agent',
              'status':'cancelled','input_sha256':C.digest(C.canonical(self.command['task'])),
              'attempt':dict(attempt,stopped=True,phase='stopped',uncertain=False),
              'selected_checkpoint':copy.deepcopy(self.original['checkpoint']),'result_binding':None,'task_sha256':'a'*64}
        self.authority={'worker_id':'agent-lab-2','workspace_root':str(self.workspace),'tasks':{'local-task':task},
                        'binding':{'original_preflight_sha256':'b'*64,'current_controller_proof_sha256':'c'*64}}
        self.unit=b'[Service]\nExecStart=/local/runtime worker --workspace-root '+str(self.workspace).encode()+b'\n'
        self.unit_pin={'path':str(f.root/'unit-fixture.service'),'sha256':C.digest(self.unit)}
        self.host.h['new_unit']=self.unit_pin
        self.host.l['module_bytes']=262144
        self.task=task

    def raw(self,path,body):
        path.write_bytes(body);path.chmod(0o600)

    def baseline(self):
        self.f.write(self.f.name,self.original)
        self.f.write(self.f.command,{'completed':True,'command':self.command})
        self.before=self.host.state_files()
        self.stop={'state':self.before,'cleanup_records':self.host.cleanup_snapshot(self.before)}

    def install(self):
        self.capture=hostmod.WorkerStateCapture(self.host,C,self.stop)
        self.host._worker_capture=self.capture
        self.hook=hostmod.CleanupTransitions(self.host,C,self.stop,self.authority)
        self.host._cleanup_transitions=self.hook

    def observe(self):
        self.f.write(self.f.name,self.observed)
        current=self.capture.inventory()
        preserved,observations=self.host.worker_history(self.stop,current)
        return current,preserved,observations


@contextlib.contextmanager
def transition_fixture(kind):
    with cleanup_fixture() as f:
        x=TransitionFixture(f,kind)
        original_pin=C.pin
        def pin(item,bound):
            if item==x.unit_pin:
                if len(x.unit)>bound:raise ValueError('local unit bound')
                return x.unit
            return original_pin(item,bound)
        with mock.patch.object(C,'pin',side_effect=pin):yield x


class CleanupTransitionTests(NoExternalEffects):
    def test_both_selected_cleanup_writers_preserve_material_transition_and_authority_gates(self):
        for writer in CLEANUP_WRITERS:
            with self.subTest(writer=writer),transition_fixture('grace') as x:
                del x.host.cleanup_policy
                policy={'path':'/fixture/selected-policy','sha256':hostmod._CLEANUP_POLICY_SHA}
                x.host.h['state']['policy']=policy
                x.host.p['formats']['source_files']['src/worker/cleanup.rs']=writer
                original_pin=C.pin
                def pin(item,bound):
                    if item==policy:
                        raw=C.canonical(x.f.selected)
                        if len(raw)>bound:raise ValueError('local policy bound')
                        return raw
                    return original_pin(item,bound)
                with mock.patch.object(C,'pin',side_effect=pin):
                    x.baseline();x.install()
                    current,preserved,observations=x.observe()
                    self.assertEqual(observations[x.f.name]['transition']['kind'],'grace_to_cache_dependency')
                    self.assertEqual(x.hook.proof()['records'],1)
                    self.assertEqual(preserved[x.f.command],x.before[x.f.command])
                    for change in ('source','missing_source','policy','kind','role'):
                        oldp,oldh=copy.deepcopy(x.host.p),copy.deepcopy(x.host.h)
                        if change=='source':x.host.p['formats']['source_files']['src/worker/cleanup.rs']='0'*64
                        elif change=='missing_source':x.host.p['formats']['source_files'].pop('src/worker/cleanup.rs')
                        elif change=='policy':x.host.h['state']['policy']['sha256']='0'*64
                        elif change=='kind':x.host.p['formats']['forward']['kind']='unselected'
                        else:x.host.h['role']='controller'
                        with self.subTest(change=change):
                            with self.assertRaises(ValueError):
                                hostmod.CleanupTransitions(x.host,C,x.stop,x.authority)
                        x.host.p,x.host.h=oldp,oldh

    def test_original_clock_path_unchanged_through_capture_and_history(self):
        with transition_fixture('clock') as x:
            x.baseline();x.install()
            with mock.patch.object(x.hook,'new_proof',side_effect=AssertionError('clock path must stay original')):
                current,preserved,observations=x.observe()
            actual=observations[x.f.name]
            expected=hostmod.cleanup_clocks(x.original,x.observed)
            self.assertEqual(actual,{'original':x.before[x.f.name],'observed':current[x.f.name],**expected})
            self.assertEqual(x.hook.proof()['records'],0)
            self.assertIn(x.f.command,preserved)

    def test_grace_to_dependency_completes_only_exact_worktrees(self):
        with transition_fixture('grace') as x:
            x.baseline();x.install()
            with mock.patch.object(x.hook,'new_proof',side_effect=AssertionError('existing proof must remain exact')):
                current,preserved,observations=x.observe()
            proof=observations[x.f.name]['transition']
            self.assertEqual(proof['kind'],'grace_to_cache_dependency')
            self.assertEqual(proof['completed_worktree_slots'],2)
            self.assertTrue(proof['existing_worktree_proofs_exact'])
            self.assertFalse(proof['cache_disposal'])
            self.assertEqual(x.hook.proof()['records'],1)
            self.assertEqual(preserved[x.f.command],x.before[x.f.command])
            self.assertEqual(x.capture.proof()['records'],1)

    def test_cancelled_grace_requires_real_independent_worktree_proof(self):
        with transition_fixture('cancelled') as x:
            x.baseline();x.install()
            current,preserved,observations=x.observe()
            proof=observations[x.f.name]['transition']
            self.assertEqual(proof['kind'],'local_recovery_to_cancelled_grace')
            self.assertEqual(proof['new_worktree_proof']['proof_sha256'],C.digest(C.canonical(x.newproof)))
            for key in ['owned_root_directory_cache_verified','accepted_repository_marker_exact',
                        'git_backreference_common_and_head_verified','bare_and_origin_config_exact']:
                self.assertIs(proof['new_worktree_proof'][key],True)
            self.assertIs(proof['new_worktree_proof']['git_process_executed'],False)
            self.assertIs(proof['worktree_removal_started'],False)
            self.assertEqual(x.hook.proof()['records'],1)
            self.assertEqual(preserved[x.f.command],x.before[x.f.command])

    def test_changed_cache_status_checkpoint_proof_authority_clock_policy_or_command_refuse(self):
        cases=['cache_started','cache','status','checkpoint','existing_proof','attempt','command_id',
               'clock','policy','policy_revision','authority_attempt','authority_status','authority_input',
               'worktree_flags','retry_due','command_file']
        for case in cases:
            with self.subTest(case=case),transition_fixture('grace') as x:
                y=x.observed
                if case=='cache_started':y['cache_started']=True
                elif case=='cache':
                    y['cache']={'identity':copy.deepcopy(x.marker),'root':copy.deepcopy(x.newproof['root']),
                                'directory':copy.deepcopy(x.newproof['cache']),'last_use_ms':1,'last_use_revision':1}
                elif case=='status':y['status']='failed'
                elif case=='checkpoint':y['checkpoint']['commit']='b'*40
                elif case=='existing_proof':y['worktrees'][0]['proof']['head']='b'*40
                elif case=='attempt':y['attempt_id']='different-attempt'
                elif case=='command_id':y['command_id']='different-command'
                elif case=='clock':y['last_observed_ms']=x.original['last_observed_ms']-1
                elif case=='policy':y['policy']['retry_delay_ms']+=1
                elif case=='policy_revision':y['policy_revision']='different-policy'
                elif case=='authority_attempt':x.task['attempt']['worker_incarnation']='different-incarnation'
                elif case=='authority_status':x.task['status']='failed'
                elif case=='authority_input':x.task['input_sha256']='d'*64
                elif case=='worktree_flags':y['worktrees'][0]['started']=False
                elif case=='retry_due':y['next_due_ms']+=1
                x.baseline();x.install()
                if case=='command_file':
                    changed=copy.deepcopy(x.command);changed['attempt']['generation']+=1
                    x.f.write(x.f.command,{'completed':True,'command':changed})
                with self.assertRaises(ValueError):x.observe()
                self.assertEqual(x.hook.proof()['records'],0)

    def test_new_proof_rejects_wrong_worktree_repository_attempt_head_link_or_grace(self):
        for case in ['path','root_inode','cache_inode','head','repository','symlink','new_first_safe','wrong_attempt','config_bare','config_origin','config_include','config_unicode','config_cr']:
            with self.subTest(case=case),transition_fixture('cancelled') as x:
                proof=x.observed['worktrees'][0]['proof']
                if case=='path':proof['path']=str(x.path.parent/'different-worktree')
                elif case=='root_inode':proof['root']['inode']+=1
                elif case=='cache_inode':proof['cache']['inode']+=1
                elif case=='head':proof['head']='b'*40
                elif case=='repository':
                    marker=copy.deepcopy(x.marker);marker['registration']['repository_id']='different-repository'
                    x.raw(x.cache/'identity.json',C.canonical(marker))
                elif case=='symlink':
                    target=x.gitdir/'commondir';target.unlink();target.symlink_to('HEAD')
                elif case=='config_bare':x.raw(x.cache/'repository.git/config',x.config.replace(b'bare = true',b'bare = false'))
                elif case=='config_origin':x.raw(x.cache/'repository.git/config',x.config.replace(b'file:///local-source',b'file:///different-source'))
                elif case=='config_include':x.raw(x.cache/'repository.git/config',x.config+b'[include]\npath = /unselected/config\n')
                elif case=='config_unicode':x.raw(x.cache/'repository.git/config',x.config.replace(b'bare = true\n',b'bare = true'+chr(0x2028).encode()))
                elif case=='config_cr':x.raw(x.cache/'repository.git/config',x.config.replace(b'bare = true',b'bare = true\r'))
                elif case=='new_first_safe':x.observed['first_safe_ms']=x.original['last_observed_ms']-1
                elif case=='wrong_attempt':x.task['attempt']['id']='different-attempt'
                if case=='symlink':
                    # Keep the original inventory safe; substitute the link only
                    # for the independent proof read after capture has completed.
                    target.unlink();x.raw(target,b'../..\n')
                    x.baseline();x.install()
                    original_new_proof=x.hook.new_proof
                    def verify_with_link(p,command):
                        target.unlink();target.symlink_to('HEAD')
                        return original_new_proof(p,command)
                    with mock.patch.object(x.hook,'new_proof',side_effect=verify_with_link):
                        with self.assertRaises((ValueError,OSError)):x.observe()
                else:
                    x.baseline();x.install()
                    with self.assertRaises(ValueError):x.observe()
                self.assertEqual(x.hook.proof()['records'],0)


class StopFixture:
    def __init__(self,root,name=None):
        self.root=root
        self.command={'kind':'stop_attempt','command_id':'local-stop-command','task_id':'local-task',
                      'attempt_id':'local-attempt','generation':1,'incarnation':'local-incarnation'}
        self.name=name or 'stops/'+self.command['attempt_id'].encode().hex()
        self.command_name='commands/'+self.command['command_id'].encode().hex()+'.json'
        self.receipt={'command':copy.deepcopy(self.command),'completed':True}
        self.host=hostmod.Host.__new__(hostmod.Host)
        self.host.h={'role':'worker','service_uid':os.geteuid(),'state':{'root':str(root)},'api':{'worker_id':'local-worker'}}
        self.host.p={'formats':{'forward':{'kind':C.INSTRUCTION_SOURCE_KIND}}}
        self.host._verified_manifest={'source':{'files':{'src/worker/process.rs':{'sha256':hostmod._PROCESS_SOURCE_SHA},'src/worker/mod.rs':{'sha256':hostmod._STOP_SOURCE_SHA}}}}
        self.host.l={'state_bytes':1048576,'receipt_bytes':1048576,'module_bytes':262144,
                     'state_entries':128,'io_chunk_bytes':65536}
        self.host.r={'deadline_ms':C.now_ms()+30000}
        self.host.retained_policy=lambda:common.strict((Path(adapter.__file__).parents[1]/'metadata/limits.json').read_bytes())
    def write(self,name,value):
        p=self.root/name;p.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        for parent in p.parents:
            if parent==self.root:break
            parent.chmod(0o700)
        tmp=p.with_name(p.name+'.new');tmp.write_bytes(C.canonical(value));tmp.chmod(0o600);tmp.replace(p)
    def baseline(self,with_command=True):
        self.write('format.json',{'format_revision':9})
        self.write(self.name,self.command)
        if with_command:self.write(self.command_name,self.receipt)
        self.before=self.host.state_files();self.stop={'state':self.before,'cleanup_records':{}}
    def replace(self):
        self.write(self.name,self.command);self.current=self.host.state_files()
        assert self.current[self.name]['identity'][1]!=self.before[self.name]['identity'][1]
    def install(self):
        self.hook=hostmod.StopMarkers(self.host,C,self.stop)
        self.host._stop_markers=self.hook

@contextlib.contextmanager
def stop_fixture(name=None):
    with tempfile.TemporaryDirectory(prefix='stop-marker-local-') as root:
        yield StopFixture(Path(root),name)

class StopMarkerTests(NoExternalEffects):
    def test_original_replacement_preserves_both_rows_and_typed_command_without_normalizing_maps(self):
        with stop_fixture() as x:
            x.baseline();x.replace()
            current=copy.deepcopy(x.current);stop=copy.deepcopy(x.stop)
            x.install();preserved,cleanup=x.host.worker_history(x.stop,x.current)
            self.assertEqual(preserved,{k:v for k,v in x.before.items() if k!='format.json'});self.assertEqual(cleanup,{})
            self.assertEqual(x.current,current);self.assertEqual(x.stop,stop)
            proof=x.hook.proof();self.assertEqual(proof['records'],1)
            self.assertEqual(proof['implementation'],'direct_history_branch')
            row=proof['observations'][C.digest(x.name.encode())]
            self.assertEqual(row['original'],x.before[x.name]);self.assertEqual(row['sampled'],x.current[x.name])
            self.assertEqual(row['original_command_row'],x.before[x.command_name])
            self.assertEqual(row['command_sha256'],C.digest(C.canonical(x.command)))
            self.assertTrue(row['typed_stop_attempt_join']);self.assertTrue(row['current_read_matched_captured_row'])
            self.assertFalse(row['actual_writer_attributed']);self.assertFalse(proof['host_effects'])

    def test_closed_family_and_missing_original_membership_never_get_an_exemption(self):
        for path in ['stops/abc','stops/aa.json','stops/aa/bb','outbox/opaque-original']:
            with self.subTest(path=path),stop_fixture(path) as x:
                x.baseline();x.replace();x.install()
                with self.assertRaisesRegex(ValueError,'original_worker_history_changed'):
                    x.host.worker_history(x.stop,x.current)
                self.assertEqual(x.hook.proof()['records'],0)
        with stop_fixture() as x:
            x.baseline(with_command=False);x.replace();x.install()
            with self.assertRaisesRegex(ValueError,'original_command_changed'):x.host.worker_history(x.stop,x.current)
            self.assertEqual(x.hook.proof()['records'],0)
        with stop_fixture() as x:
            x.baseline();x.stop=copy.deepcopy(x.stop);x.stop['state'].pop(x.name);x.replace();x.install()
            preserved,_=x.host.worker_history(x.stop,x.current)
            self.assertNotIn(x.name,preserved);self.assertEqual(x.hook.proof()['records'],0)

    def test_content_type_command_join_and_noninode_identity_changes_refuse(self):
        for case in ['bytes','kind','generation_bool','wrong_attempt_path','extra_field','wrong_receipt_join',
                     'command_changed','device','uid','gid','mode']:
            with self.subTest(case=case),stop_fixture() as x:
                if case=='kind':x.command['kind']='run_attempt'
                elif case=='generation_bool':x.command['generation']=True
                elif case=='wrong_attempt_path':x.command['attempt_id']='different-attempt'
                elif case=='extra_field':x.command['unexpected']='not allowed'
                x.receipt['command']=copy.deepcopy(x.command)
                if case=='wrong_receipt_join':x.receipt['command']['task_id']='different-task'
                x.baseline();x.replace();x.install()
                if case=='bytes':
                    changed=copy.deepcopy(x.command);changed['generation']=2;x.write(x.name,changed)
                    x.current=x.host.state_files()
                elif case=='command_changed':
                    x.receipt['completed']=False;x.write(x.command_name,x.receipt)
                    x.current=x.host.state_files()
                elif case in ('device','uid','gid','mode'):
                    index={'device':0,'uid':2,'gid':3,'mode':4}[case]
                    x.current[x.name]['identity'][index]+=1
                with self.assertRaises(ValueError):x.host.worker_history(x.stop,x.current)
                self.assertEqual(x.hook.proof()['records'],0)

    def test_current_read_must_match_captured_row_and_refuses_aliases_without_retry(self):
        for case in ['later_inode','later_body','symlink','hardlink']:
            with self.subTest(case=case),stop_fixture() as x:
                x.baseline();x.replace();x.install()
                path=x.root/x.name
                if case=='later_inode':x.write(x.name,x.command)
                elif case=='later_body':
                    changed=copy.deepcopy(x.command);changed['generation']=2
                    before=path.stat();path.write_bytes(C.canonical(changed));self.assertEqual(path.stat().st_ino,before.st_ino)
                elif case=='symlink':path.unlink();path.symlink_to(x.root/x.command_name)
                else:os.link(path,path.with_name(path.name+'.alias'))
                with mock.patch.object(hostmod,'_file',wraps=hostmod._file) as reads:
                    with self.assertRaises((ValueError,OSError)):x.host.worker_history(x.stop,x.current)
                self.assertEqual(reads.call_count,1)
                self.assertEqual(x.hook.proof()['records'],0)


    def test_stop_writer_scope_is_exact_kind_and_verified_manifest_sources(self):
        for change in ('no_kind','other_kind','no_manifest','wrong_process','wrong_writer'):
            with self.subTest(change=change),stop_fixture() as x:
                x.baseline();x.replace()
                if change=='no_kind':x.host.p['formats'].pop('forward')
                elif change=='other_kind':x.host.p['formats']['forward']['kind']='other'
                elif change=='no_manifest':del x.host._verified_manifest
                else:x.host._verified_manifest['source']['files']['src/worker/process.rs' if change=='wrong_process' else 'src/worker/mod.rs']['sha256']='0'*64
                with self.assertRaisesRegex(ValueError,'original_worker_history_changed'):
                    x.host.worker_history(x.stop,x.current)
                self.assertFalse(hasattr(x.host,'_stop_markers'))

class ReconciliationJourneyTests(NoExternalEffects):
    def setUp(self):
        super().setUp()
        self.temp=tempfile.TemporaryDirectory(prefix='rollout-reconciliation-');self.addCleanup(self.temp.cleanup)
        self.f=Fixture(Path(self.temp.name));self.peer=FakeProducer(self.f.subject)
        context=self.f.seams();context.__enter__();self.addCleanup(context.__exit__,None,None,None)
    def runner(self):
        value=self.f.rollout();value.producer.exchange=self.peer.exchange;return value
    def full(self,result,host):
        reference=result['host_results'][host]['full_proof'];path=Path(reference['path']);raw=path.read_bytes()
        self.assertTrue(path.is_relative_to(Path(self.f.profile['state_root'])))
        self.assertEqual(len(raw),reference['bytes']);self.assertEqual(common.digest(raw),reference['sha256'])
        return common.strict(raw)
    def collide(self):
        self.f.forward_transition(instruction_source=True)
        for identity in ('original','external-only'):
            record={'id':identity,'task_id':'task-original','attempt_id':'attempt-original','generation':1,
                'kind':'checkpoint','intended':{'remote_ref':'refs/heads/original'},'state':'uncertain','outcome':None}
            self.f.external_records[identity]=record
        with sqlite3.connect(self.f.profile['hosts'][0]['state']['database']) as db:
            for identity,record in self.f.external_records.items():
                db.execute('INSERT INTO external_operations VALUES(?,?,?,NULL,0)',
                           (identity,common.canonical(record).decode(),'{}'))
    def snapshots(self):
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        return work,common.strict((work/'stopped-copy/snapshot.json').read_bytes()),common.strict((work/'restart-copy/snapshot.json').read_bytes())
    def test_external_collision_preserves_raw_receipt_and_reads_exact_live_namespace(self):
        self.collide();result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        full=self.full(result,'controller');proof=full['controller_receipt_proof']
        self.assertEqual(full['preserved_controller_receipts'],['original'])
        self.assertEqual(proof['original_receipts'],1);self.assertEqual(proof['external_operation_lookups'],2)
        self.assertEqual(proof['application_mutation_lookups'],0)
        self.assertFalse(proof['current_raw_receipts_read']);self.assertFalse(proof['fabricated_commit_stamp'])
        self.assertFalse(proof['live_database_opened'])
        work,before,after=self.snapshots()
        old=before['tables']['receipts'];new=after['tables']['receipts']
        self.assertEqual(new['columns'],old['columns']+['governance'])
        self.assertEqual([row[:-1] for row in new['rows']],old['rows'])
        self.assertTrue(all(row[-1] is None for row in new['rows']))
        with sqlite3.connect(work/'stopped-copy/bytes/tasks.sqlite') as db:
            self.assertEqual(db.execute('SELECT outcome FROM receipts WHERE id=?',('original',)).fetchone()[0],
                             common.canonical(self.f.original_receipts['original']['outcome']).decode())
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(effects,self.f.effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
    def test_live_namespace_body_identity_kind_and_commit_are_never_fabricated(self):
        self.collide();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        _,before,after=self.snapshots();plan=common.controller_receipt_plan(self.f.profile,before,after)
        expected=next(row for row in plan if row['id']=='original')
        reply={'status':'ok','data':{'kind':'operation','value':copy.deepcopy(expected['record'])},
               'commit':None,'observed_controller_revision':9}
        self.assertEqual(common.verify_controller_lookup(expected,reply),'external')
        for case in ('body','id','kind','commit','revision_bool'):
            with self.subTest(case=case):
                bad=copy.deepcopy(reply)
                if case=='body':bad['data']['value']['outcome']={'invented':'settled'}
                elif case=='id':bad['data']['value']['id']='other'
                elif case=='kind':bad['data']['value']['kind']='application_mutation'
                elif case=='commit':bad['commit']={'operation_id':'original','controller_revision':7}
                else:bad['observed_controller_revision']=True
                with self.assertRaises(ValueError):common.verify_controller_lookup(expected,bad)
        # Exercise the same guard through the actual adapter reconciliation.
        self.f.lookup_transform=lambda value:dict(value,commit={'operation_id':'original','controller_revision':7})
        effects=copy.deepcopy(self.f.effects);result=self.runner().execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'guard_refused');self.assertEqual(error_cause(result)['message_sha256'],common.digest(b'live_receipt_commit_or_status'))
        self.assertEqual(self.f.effects,effects)
    def test_normal_original_receipt_keeps_stamp_and_snapshot_corruption_refuses(self):
        self.f.forward_transition(instruction_source=True)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        work,before,after=self.snapshots();plan=common.controller_receipt_plan(self.f.profile,before,after)
        expected=plan[0];self.assertEqual(expected['namespace'],'application_mutation')
        self.assertEqual(expected['commit'],{'operation_id':'original','controller_revision':7})
        for commit in (None,{'operation_id':'other','controller_revision':7},{'operation_id':'original','controller_revision':8}):
            with self.subTest(commit=commit),self.assertRaises(ValueError):
                common.verify_controller_lookup(expected,{'status':'ok','data':{'kind':'operation','value':expected['record']},
                    'commit':commit,'observed_controller_revision':9})
        changed=copy.deepcopy(after);changed['tables']['receipts']['rows'][0][2]='{}'
        with self.assertRaises(ValueError):common.controller_receipt_plan(self.f.profile,before,changed)
        effects=copy.deepcopy(self.f.effects)
        path=work/'stopped-copy/snapshot.json';save(path,dict(before,unrecognized='not the original snapshot'))
        failed=self.runner().execute('reconcile');self.assertEqual(failed['status'],'UNKNOWN')
        self.assertEqual(error_cause(failed)['code'],'guard_refused');self.assertEqual(error_cause(failed)['message_sha256'],common.digest(b'original_stopped_snapshot_changed'));self.assertEqual(self.f.effects,effects)
    def test_full_proofs_survive_compaction_before_observe_without_private_body_export(self):
        self.f.forward_transition(instruction_source=True)
        self.f.profile['limits']['receipt_bytes']=600000
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        original=hostmod.Host.execute;private='private-proof-marker-'+('x'*330000)
        def substantial(host,phase):
            value=original(host,phase)
            if phase=='inspect':value['private_evidence']=private
            return value
        runner=self.runner();exchange=self.peer.exchange;observations=[]
        def ordered(config,request,**kwargs):
            if request['command']['operation']=='observe_rollout' and request['command']['input']['observed']:
                refs=list(runner.work.glob('verified-host-proof-references-*.json'));self.assertEqual(len(refs),1)
                values=common.strict(refs[0].read_bytes());self.assertEqual(set(values),{'controller','worker'})
                for value in values.values():
                    proof=value['full_proof'];raw=Path(proof['path']).read_bytes()
                    self.assertEqual(common.digest(raw),proof['sha256']);self.assertEqual(len(raw),proof['bytes'])
                    self.assertEqual(common.strict(raw)['private_evidence'],private)
                observations.append(True)
            return exchange(config,request,**kwargs)
        runner.producer.exchange=ordered
        with mock.patch.object(hostmod.Host,'execute',substantial):result=runner.execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(observations,[True])
        full={host:self.full(result,host) for host in ('controller','worker')}
        self.assertGreater(len(common.canonical(full)),self.f.profile['limits']['receipt_bytes'])
        self.assertLess(len(common.canonical(result)),self.f.profile['limits']['receipt_bytes'])
        self.assertNotIn(b'private-proof-marker',common.canonical(result))
        effects=copy.deepcopy(self.f.effects)
        with mock.patch.object(hostmod.Host,'execute',substantial):again=self.runner().execute('reconcile')
        self.assertEqual(again['status'],'APPLIED',again);self.assertEqual(self.f.effects,effects)
    def test_current_kind_preflight_catches_unselected_file_before_start_or_service_effect(self):
        self.f.forward_transition(instruction_source=True)
        root=Path(self.f.profile['hosts'][1]['state']['root'])
        (root/'private-alias').symlink_to(root/'limits.json')
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
        self.assertEqual(error_cause(result)['message_sha256'],common.digest(b'state nonregular/aliased file'))
    def test_sanitized_failure_keeps_exact_cause_and_unknown_without_effect_retry(self):
        private='opaque /private/task/body secret-text'
        runner=self.runner()
        with mock.patch.object(hostmod.Host,'state_files',side_effect=ValueError(private)):
            result=runner.execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'guard_refused')
        self.assertEqual(error_cause(result)['message_sha256'],common.digest(private.encode()))
        self.assertNotIn(private,common.canonical(result).decode());self.assertEqual(self.f.effects,[])
        cause=common.GuardFailure('state_directory_changed',{'kind':'directory','site':'state_files.final_directory',
            'category':'mutations','relative_sha256':'a'*64,'depth':1,'changed_fields':['ctime_ns'],
            'expected':[1]*9,'observed':[1]*8+[2]})
        envelope=common.canonical({'host_error':common.error_record(cause,'inspect')})
        status=SimpleNamespace(si_code=1,si_status=7)
        failure=common.CommandFailure(status,envelope,private.encode())
        detail=common.error_record(failure,'inspect')
        self.assertEqual(detail['command']['host_error']['details']['changed_fields'],['ctime_ns'])
        self.assertEqual(detail['command']['stderr_sha256'],common.digest(private.encode()))
        self.assertNotIn(private,common.canonical(detail).decode())
        with mock.patch.object(adapter.c,'run',side_effect=failure):
            rejected=self.runner().execute('reconcile')
        self.assertEqual(rejected['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])

    def test_cleanup_authority_requires_complete_original_tasks_before_projection(self):
        task={'id':'task','session_id':'session','assigned_agent_id':'agent','status':'cancelled',
              'input':{'definition':{'executor':'coding'}},'attempt':{'id':'attempt','worker_id':'worker'},
              'selected_checkpoint':None,'result':None,'original_history':{'retained':True}}
        before=common.canonical({'tasks':{'task':task}})
        current={'tasks':{'task':copy.deepcopy(task)},'artifact':self.f.artifact}
        context=adapter.verified_task_context(before,current)
        worker=self.f.profile['hosts'][1];projected=adapter.project_cleanup_authority(worker,context)
        self.assertEqual(set(projected['tasks']),{'task'})
        self.assertEqual(projected['tasks']['task']['task_sha256'],common.digest(common.canonical(task)))
        self.assertEqual(projected['tasks']['task']['input_sha256'],common.digest(common.canonical(task['input'])))
        self.assertEqual(projected['binding']['original_preflight_sha256'],common.digest(before))
        self.assertEqual(projected['binding']['current_controller_proof_sha256'],common.digest(common.canonical(current)))
        for change in ('history','added','removed'):
            value=copy.deepcopy(current)
            if change=='history':value['tasks']['task']['original_history']['retained']=False
            elif change=='added':value['tasks']['other']=task
            else:value['tasks'].pop('task')
            with self.subTest(change=change),self.assertRaises(ValueError):adapter.verified_task_context(before,value)
        with self.assertRaises(ValueError):adapter.project_cleanup_authority(worker,None)

    def test_phase_receipt_or_service_change_during_inspect_stays_unknown(self):
        for changed in ('phase','service'):
            with self.subTest(changed=changed):
                # Separate original action and fixtures per cut, same real handler.
                with tempfile.TemporaryDirectory(prefix='inspect-race-') as root:
                    f=Fixture(Path(root));peer=FakeProducer(f.subject)
                    with f.seams():
                        f.forward_transition(instruction_source=True)
                        run=f.rollout();run.producer.exchange=peer.exchange
                        self.assertEqual(run.execute('apply')['status'],'APPLIED')
                        original=hostmod.Host.api;effects=copy.deepcopy(f.effects);fired=[]
                        def race(host,operation,inp):
                            result=original(host,operation,inp)
                            if host.h['role']=='controller' and not fired:
                                if changed=='phase':
                                    path=host.work/'install.result.json';value=common.strict(path.read_bytes())
                                    save(path,dict(value,unrecognized='phase mutation'))
                                else:f.invocations['controller']='different-incarnation'
                                fired.append(True)
                            return result
                        again=f.rollout();again.producer.exchange=peer.exchange
                        with mock.patch.object(hostmod.Host,'api',race):result=again.execute('reconcile')
                        self.assertEqual(fired,[True]);self.assertEqual(result['status'],'UNKNOWN',result)
                        self.assertEqual(f.effects,effects)
                        detail=error_cause(result)
                        self.assertEqual(detail['code'],'guard_refused')
                        self.assertEqual(detail['message_sha256'],common.digest(b'receipt_proof_sample_changed' if changed=='phase' else b'reconciliation_original_evidence_changed'))
                        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in peer.calls),1)

class SelectedCleanupPolicyTests(NoExternalEffects):
    def test_only_selected_current_forward_source_and_policy_enable_capture(self):
        for writer in CLEANUP_WRITERS:
            for kind in (C.PR_PUBLICATION_KIND,C.INSTRUCTION_SOURCE_KIND):
                with self.subTest(writer=writer,kind=kind),cleanup_fixture() as f:
                    host=f.host;del host.cleanup_policy
                    host.h['state']['policy']={'path':'/fixture/selected-policy','sha256':hostmod._CLEANUP_POLICY_SHA}
                    host.p['formats']['forward']={'kind':kind}
                    host.p['formats']['source_files']['src/worker/cleanup.rs']=writer
                    with mock.patch.object(C,'pin',return_value=common.canonical(f.selected)) as pin:
                        self.assertEqual(host.cleanup_policy(),f.selected);pin.assert_called_once()
                        for change in ('source','missing_source','policy','kind','role'):
                            oldp,oldh=copy.deepcopy(host.p),copy.deepcopy(host.h)
                            if change=='source':host.p['formats']['source_files']['src/worker/cleanup.rs']='0'*64
                            elif change=='missing_source':host.p['formats']['source_files'].pop('src/worker/cleanup.rs')
                            elif change=='policy':host.h['state']['policy']['sha256']='0'*64
                            elif change=='kind':host.p['formats']['forward']['kind']='unselected'
                            else:host.h['role']='controller'
                            pin.reset_mock()
                            with self.subTest(change=change):self.assertIsNone(host.cleanup_policy());pin.assert_not_called()
                            host.p,host.h=oldp,oldh

class TransportInputTests(NoExternalEffects):
    def test_early_stdin_close_drains_original_status_and_cleans_even_zero_exit(self):
        for code in (0,7):
            with self.subTest(exit_status=code):
                process=mock.MagicMock();process.pid=43210
                for fd,stream in enumerate((process.stdin,process.stdout,process.stderr),10):
                    stream.fileno.return_value=fd;stream.closed=False
                    stream.close.side_effect=lambda stream=stream:setattr(stream,'closed',True)
                selector=mock.MagicMock();registered={}
                selector.register.side_effect=lambda stream,mask:registered.__setitem__(stream,mask)
                selector.unregister.side_effect=lambda stream:registered.pop(stream)
                selector.get_map.side_effect=lambda:dict(registered)
                def ready(_timeout):
                    order=(process.stdin,process.stdout,process.stderr)
                    return [(SimpleNamespace(fileobj=stream),registered[stream]) for stream in order if stream in registered]
                selector.select.side_effect=ready
                outputs={11:[b'original-output-',b'after-close',b''],12:[b'private stderr ',b'/secret/body',b'']}
                calls=[];reaped=[]
                def kill(pid,sig):
                    calls.append((pid,sig))
                    if sig==0:raise ProcessLookupError()
                    self.assertFalse(reaped,'signal must precede reap')
                process.wait.side_effect=lambda **kwargs:reaped.append(True)
                limits={'command_timeout_ms':1000,'cleanup_timeout_ms':1000,'cleanup_poll_ms':1,
                        'io_chunk_bytes':64,'command_output_bytes':1000}
                with mock.patch.object(common.subprocess,'Popen',return_value=process) as spawn, \
                     mock.patch.object(common.selectors,'DefaultSelector',return_value=selector), \
                     mock.patch.object(common.os,'set_blocking'), \
                     mock.patch.object(common.os,'write',side_effect=[3,BrokenPipeError('private stdin path')]) as writes, \
                     mock.patch.object(common.os,'read',side_effect=lambda fd,bound:outputs[fd].pop(0)), \
                     mock.patch.object(common.os,'waitid',return_value=SimpleNamespace(si_code=os.CLD_EXITED,si_status=code)), \
                     mock.patch.object(common.os,'killpg',side_effect=kill):
                    with self.assertRaises(common.CommandFailure) as caught:
                        common.run(['controlled-no-process'],limits,common.now_ms()+1000,b'original request')
                spawn.assert_called_once();self.assertEqual(writes.call_count,2)
                self.assertEqual(outputs,{11:[],12:[]});self.assertEqual(reaped,[True])
                self.assertEqual(calls,[(43210,signal.SIGKILL),(43210,0)])
                for stream in (process.stdin,process.stdout,process.stderr):self.assertTrue(stream.closed)
                record=common.error_record(caught.exception,'transport');command=record['command']
                self.assertEqual(command['wait_status'],code)
                self.assertEqual(command['stdout_sha256'],common.digest(b'original-output-after-close'))
                self.assertEqual(command['stderr_sha256'],common.digest(b'private stderr /secret/body'))
                self.assertEqual(command['process'],{'leader_pid':43210,'leader_reaped':True,'group_empty':True,
                    'input_bytes':len(b'original request'),'input_sent_bytes':3,'input_closed_early':True})
                self.assertNotIn(b'private',common.canonical(record));self.assertNotIn(b'/secret',common.canonical(record))
                # The exact closed nested error survives the transport envelope.
                wrapped=common.CommandFailure(SimpleNamespace(si_code=os.CLD_EXITED,si_status=1),
                    common.canonical({'host_error':record}),b'')
                self.assertEqual(wrapped.command['host_error']['command']['process'],command['process'])


class CodexHomeFixture:
    """Real private files, controlled canonical projection, no worker/service run."""
    def __init__(self,root):
        import shutil
        self.f=CleanupFixture(root);self.host=self.f.host;self.root=root
        self.host._verified_manifest={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._CODEX_HOME_SOURCES.items()}}}
        self.host.p={'formats':{'before':{'wire':20,'controller_schema':21,'worker_format':16},
                               'after':{'wire':20,'controller_schema':21,'worker_format':16},'source_files':{}}}
        self.attempt={'id':'cleanup-attempt','task_id':'task','session_id':'session','generation':1,
                      'worker_id':'agent-lab-2','worker_incarnation':'old-worker','deadline_unix_ms':999999}
        self.input={'definition':{'executor':'coding','id':'agent'}}
        self.command={'kind':'run_attempt','command_id':'cleanup-run','attempt':self.attempt,'task':self.input}
        self.f.write(self.f.command,{'command':self.command,'completed':True,'effects_started':True})
        self.f.record.update(checkpoint=None,reason='cache_dependency',last_observed_ms=1000+self.f.record['policy']['cancelled_grace_ms'],
                             next_due_ms=1000+self.f.record['policy']['cancelled_grace_ms']+self.f.record['policy']['retry_delay_ms'])
        for slot in self.f.record['worktrees']:slot['done']=True
        self.f.write(self.f.name,self.f.record)
        self.key=self.attempt['id'].encode().hex();self.base='codex/'+self.key;home=root/self.base
        home.mkdir(parents=True,mode=0o700);home.parent.chmod(0o700)
        (root/'outbox').mkdir(mode=0o700)
        for namespace in hostmod._CODEX_JOURNALS:
            if namespace.endswith('.json'):
                self.f.write(self.base+'/'+namespace,{'thread_id':'thread'} if namespace=='borg-thread.json' else {'retained':namespace})
            else:
                directory=home/namespace;directory.mkdir(mode=0o700);(directory/'empty').mkdir(mode=0o700)
                self.f.write(self.base+'/'+namespace+'/original.json',{'retained':namespace})
        for directory in ('sessions/day','archived_sessions','plugins/cache'):
            (home/directory).mkdir(parents=True,mode=0o700)
        for relative,body in [('sessions/day/trace.jsonl',b'{"turn":1}\n'),('archived_sessions/old.jsonl',b'old\n'),
                              ('history.jsonl',b'history\n'),('plugins/cache/vendor.db',b'disposable database')]:
            path=home/relative;path.write_bytes(body);path.chmod(0o600)
        for directory in home.rglob('*'):
            if directory.is_dir():directory.chmod(0o700)
        self.f.refresh();self.stop=copy.deepcopy(self.f.stop)
        self.stop['codex_homes']=hostmod.codex_home_snapshot(self.host,self.stop['state'])
        self.task={'id':'task','session_id':'session','assigned_agent_id':'agent','status':'cancelled',
                   'input_sha256':C.digest(C.canonical(self.input)),
                   'attempt':{**self.attempt,'stopped':True,'phase':'stopped','uncertain':False},
                   'selected_checkpoint':None,'result_binding':None,'task_sha256':'c'*64}
        self.host.r['cleanup_authority']={'worker_id':'agent-lab-2','workspace_root':str(root.parent),
            'tasks':{'task':self.task},'binding':{'original_preflight_sha256':'a'*64,'current_controller_proof_sha256':'b'*64}}
        self.name='cleanup-codex/'+self.key+'.json'
    def compact(self):
        import shutil
        saved=self.stop['codex_homes'][self.key];home=self.root/self.base;archive=self.root/'codex-transcripts'/self.key
        archive.mkdir(parents=True,mode=0o700);archive.parent.chmod(0o700)
        for name in hostmod._CODEX_TRANSCRIPTS:
            source=home/name
            if source.is_dir():shutil.copytree(source,archive/name)
            elif source.exists():shutil.copyfile(source,archive/name);(archive/name).chmod(0o600)
        for source in list(home.iterdir()):
            if source.name in hostmod._CODEX_JOURNALS:continue
            if source.is_dir() and not source.is_symlink():shutil.rmtree(source)
            else:source.unlink()
        (self.root/'cleanup-codex').mkdir(mode=0o700)
        identity=lambda path:{'device':path.stat().st_dev,'inode':path.stat().st_ino}
        self.record={'format_revision':1,'command_id':'cleanup-run','attempt_id':self.attempt['id'],
            'policy':self.f.record['policy'],'policy_revision':self.f.record['policy_revision'],
            'status':'cancelled','checkpoint':None,'first_safe_ms':1000,'last_observed_ms':self.f.record['last_observed_ms'],
            'next_due_ms':None,'proof':{'path':str(home),'parents':saved['parents'],'directory':saved['home']},
            'archive':{'parent':identity(archive.parent),'directory':identity(archive)},
            'exports':copy.deepcopy([v['signature'] for v in saved['transcript_sources']]),
            'transcript_expected':True,'transcript_sources':copy.deepcopy(saved['transcript_sources']),
            'removing':None,'started':True,'done':True,'reason':'complete'}
        self.write_progress()
    def write_progress(self):self.f.write(self.name,self.record)
    def check(self):
        self.current=self.host.state_files();return self.host.worker_history(self.stop,self.current)


class CodexHomePreservationTests(NoExternalEffects):
    @contextlib.contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory(prefix='codex-copyout-readback-') as root:
            yield CodexHomeFixture(Path(root))

    def test_supplied_turn_cleanup_family_requires_every_exact_producer_and_reader(self):
        with self.fixture() as f:
            self.assertTrue(f.host.codex_home_scope())
            old=copy.deepcopy(f.host._verified_manifest)
            fresh={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._SUPPLIED_CODEX_HOME_SOURCES.items()}}}
            f.host._verified_manifest=copy.deepcopy(fresh)
            self.assertTrue(f.host.codex_home_scope())
            for name in hostmod._SUPPLIED_CODEX_HOME_SOURCES:
                f.host._verified_manifest=copy.deepcopy(fresh)
                f.host._verified_manifest['source']['files'][name]['sha256']='0'*64
                with self.subTest(changed=name):self.assertFalse(f.host.codex_home_scope())
                f.host._verified_manifest=copy.deepcopy(fresh)
                del f.host._verified_manifest['source']['files'][name]
                with self.subTest(missing=name):self.assertFalse(f.host.codex_home_scope())
            f.host._verified_manifest=old
            self.assertTrue(f.host.codex_home_scope())

    def test_supplied_turn_cleanup_keeps_outer_intent_dispatch_ack_and_main_output_bytes(self):
        with self.fixture() as f:
            f.host._verified_manifest={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._SUPPLIED_CODEX_HOME_SOURCES.items()}}}
            # The fixture exercises the selected cleanup reader, not a runtime
            # format upgrade or supplied model execution.
            f.host.p['formats']['before']=dict(common.SUPPLIED_TURN_AFTER)
            f.host.p['formats']['after']=dict(common.SUPPLIED_TURN_AFTER)
            paths=[]
            for name in ('intent.json','dispatch.json','ack.json'):
                path=f.root/f.base/'supplied-charter/outer-0'/name
                path.parent.mkdir(mode=0o700,exist_ok=True)
                path.write_bytes(common.canonical({'original_outer_record':name}));path.chmod(0o600);paths.append(path)
            output=f.root/'charter-output';output.mkdir(mode=0o700)
            chunk=output/'original-chunk.json';chunk.write_bytes(b'{"original_output":"exact"}');chunk.chmod(0o600);paths.append(chunk)
            expected={p:p.read_bytes() for p in paths}
            f.f.refresh();f.stop=copy.deepcopy(f.f.stop)
            f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
            f.compact();preserved,changes=f.check()
            self.assertTrue(changes)
            self.assertEqual({p:p.read_bytes() for p in paths},expected)
            for p in paths:self.assertIn(str(p.relative_to(f.root)),preserved)
            self.assertTrue(f.host._codex_home_proof['other_original_rows_exact'])

    def test_agent_directory_cleanup_family_requires_every_exact_producer_and_reader(self):
        with self.fixture() as f:
            self.assertTrue(f.host.codex_home_scope())
            old=copy.deepcopy(f.host._verified_manifest)
            fresh={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._DIRECTORY_CODEX_HOME_SOURCES.items()}}}
            f.host._verified_manifest=copy.deepcopy(fresh)
            self.assertTrue(f.host.codex_home_scope())
            for name in hostmod._DIRECTORY_CODEX_HOME_SOURCES:
                f.host._verified_manifest=copy.deepcopy(fresh)
                f.host._verified_manifest['source']['files'][name]['sha256']='0'*64
                with self.subTest(changed=name):self.assertFalse(f.host.codex_home_scope())
                f.host._verified_manifest=copy.deepcopy(fresh)
                del f.host._verified_manifest['source']['files'][name]
                with self.subTest(missing=name):self.assertFalse(f.host.codex_home_scope())
            f.host._verified_manifest=old
            self.assertTrue(f.host.codex_home_scope())

    def test_agent_directory_cleanup_keeps_outer_intent_dispatch_ack_and_main_output_bytes(self):
        with self.fixture() as f:
            f.host._verified_manifest={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._DIRECTORY_CODEX_HOME_SOURCES.items()}}}
            # The fixture exercises the selected cleanup reader, not a runtime
            # format upgrade or supplied model execution.
            f.host.p['formats']['before']=dict(common.AGENT_DIRECTORY_AFTER)
            f.host.p['formats']['after']=dict(common.AGENT_DIRECTORY_AFTER)
            paths=[]
            for name in ('intent.json','dispatch.json','ack.json'):
                path=f.root/f.base/'supplied-charter/outer-0'/name
                path.parent.mkdir(mode=0o700,exist_ok=True)
                path.write_bytes(common.canonical({'original_outer_record':name}));path.chmod(0o600);paths.append(path)
            output=f.root/'charter-output';output.mkdir(mode=0o700)
            chunk=output/'original-chunk.json';chunk.write_bytes(b'{"original_output":"exact"}');chunk.chmod(0o600);paths.append(chunk)
            expected={p:p.read_bytes() for p in paths}
            f.f.refresh();f.stop=copy.deepcopy(f.f.stop)
            f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
            f.compact();preserved,changes=f.check()
            self.assertTrue(changes)
            self.assertEqual({p:p.read_bytes() for p in paths},expected)
            for p in paths:self.assertIn(str(p.relative_to(f.root)),preserved)
            self.assertTrue(f.host._codex_home_proof['other_original_rows_exact'])

    def test_default_assignment_cleanup_family_retains_historical_pins_and_requires_every_member(self):
        # This family identifies the historical wire23/schema24/worker18 source,
        # not the current checkout. Updating source must not repin old receipts.
        self.assertEqual(common.digest(common.canonical(hostmod._DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES)),
                         '22d0a871b7f93a4d431ae48e71376b53f16d55f65cef8c679579c755bd6779ae')
        with self.fixture() as f:
            self.assertTrue(f.host.codex_home_scope())
            old=copy.deepcopy(f.host._verified_manifest)
            fresh={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES.items()}}}
            f.host._verified_manifest=copy.deepcopy(fresh)
            self.assertTrue(f.host.codex_home_scope())
            for name in hostmod._DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES:
                f.host._verified_manifest=copy.deepcopy(fresh)
                f.host._verified_manifest['source']['files'][name]['sha256']='0'*64
                with self.subTest(changed=name):self.assertFalse(f.host.codex_home_scope())
                f.host._verified_manifest=copy.deepcopy(fresh)
                del f.host._verified_manifest['source']['files'][name]
                with self.subTest(missing=name):self.assertFalse(f.host.codex_home_scope())
            f.host._verified_manifest=old
            self.assertTrue(f.host.codex_home_scope())

    def select_composed(self,f):
        f.host._verified_manifest={'source':{'files':{
            n:{'sha256':sha} for n,sha in hostmod._COMPOSED_CODEX_HOME_SOURCES.items()}}}
        f.host.p['formats']['before']=dict(hostmod._COMPOSED_CODEX_HOME_FORMATS)
        f.host.p['formats']['after']=dict(hostmod._COMPOSED_CODEX_HOME_FORMATS)

    def test_composed_cleanup_family_retains_historical_pins_and_formats(self):
        # This immutable family identifies the qualified wire24/schema25/worker19
        # producer. The selected worker20 source has its own current-source test.
        self.assertEqual(len(hostmod._COMPOSED_CODEX_HOME_SOURCES),53)
        self.assertEqual(common.digest(common.canonical(hostmod._COMPOSED_CODEX_HOME_SOURCES)),
                         '42360fdc812de5653fc8965afea7eb2f2732a8208631b8b0a1436cc80e20a459')
        self.assertEqual(hostmod._COMPOSED_CODEX_HOME_FORMATS,
                         {'wire':24,'controller_schema':25,'worker_format':19})
        with self.fixture() as f:
            self.select_composed(f);common.format_contract(f.host.p)
            self.assertTrue(f.host.codex_home_scope())
            f.host.h['role']='controller'
            self.assertFalse(f.host.codex_home_scope())

    def test_composed_cleanup_family_requires_every_member_and_exact_format(self):
        with self.fixture() as f:
            self.select_composed(f);fresh=copy.deepcopy(f.host._verified_manifest)
            for name in hostmod._COMPOSED_CODEX_HOME_SOURCES:
                for change in ('missing','changed'):
                    f.host._verified_manifest=copy.deepcopy(fresh)
                    if change=='missing':del f.host._verified_manifest['source']['files'][name]
                    else:f.host._verified_manifest['source']['files'][name]['sha256']='0'*64
                    with self.subTest(source=name,change=change):self.assertFalse(f.host.codex_home_scope())
            f.host._verified_manifest=copy.deepcopy(fresh)
            for name,value in hostmod._COMPOSED_CODEX_HOME_FORMATS.items():
                for wrong in (value-1,value+1,float(value),str(value),None):
                    f.host.p['formats']['after']=dict(hostmod._COMPOSED_CODEX_HOME_FORMATS,**{name:wrong})
                    with self.subTest(format=name,value=wrong):self.assertFalse(f.host.codex_home_scope())
            del f.host.p['formats']['after']
            self.assertFalse(f.host.codex_home_scope())
            self.select_composed(f)
            f.host.p['formats']['before']={'wire':23,'controller_schema':24,'worker_format':18}
            # Selecting current cleanup readers never authorizes a new upgrade.
            with self.assertRaisesRegex(ValueError,'qualified forward transition required'):
                common.format_contract(f.host.p)

    def test_composed_cleanup_rejects_mixed_historical_producer_families(self):
        with self.fixture() as f:
            for historical in (hostmod._CODEX_HOME_SOURCES,hostmod._SUPPLIED_CODEX_HOME_SOURCES,
                               hostmod._DIRECTORY_CODEX_HOME_SOURCES,hostmod._DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES):
                differing=[n for n in historical if historical[n]!=hostmod._COMPOSED_CODEX_HOME_SOURCES[n]]
                self.assertGreater(len(differing),1)
                for name in differing:
                    self.select_composed(f)
                    f.host._verified_manifest['source']['files'][name]['sha256']=historical[name]
                    with self.subTest(historical=common.digest(common.canonical(historical)),source=name):
                        self.assertFalse(f.host.codex_home_scope())

    def test_composed_cleanup_preserves_outer_retry_journal_and_all_original_records(self):
        with self.fixture() as f:
            self.select_composed(f)
            journal=f.base+'/borg-supplied-outer.json'
            f.f.write(journal,{'binding':{'session_id':f.attempt['session_id']},
                              'index':2,'thread_id':'thread','committed':True,'dispatched':True})
            f.f.refresh();f.stop=copy.deepcopy(f.f.stop)
            f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
            original=copy.deepcopy(f.stop);raw=(f.root/journal).read_bytes()
            f.compact();preserved,changes=f.check()
            self.assertTrue(changes)
            self.assertEqual((f.root/journal).read_bytes(),raw)
            self.assertEqual(preserved[journal],original['state'][journal])
            self.assertEqual(f.stop,original)
            self.assertTrue(f.host._codex_home_proof['other_original_rows_exact'])

    def test_composed_cleanup_refuses_retry_journal_removal_content_or_session_changes(self):
        for case in ('removed','content','journal_session','current_session'):
            with self.subTest(case=case),self.fixture() as f:
                self.select_composed(f);journal=f.base+'/borg-supplied-outer.json'
                body={'binding':{'session_id':f.attempt['session_id']},'thread_id':'thread','index':2}
                f.f.write(journal,body);f.f.refresh();f.stop=copy.deepcopy(f.f.stop)
                f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
                f.compact()
                if case=='removed':(f.root/journal).unlink()
                elif case=='content':f.f.write(journal,{**body,'index':3})
                elif case=='journal_session':f.f.write(journal,{**body,'binding':{'session_id':'replacement-session'}})
                else:f.task['session_id']='replacement-session'
                with self.assertRaises(ValueError):f.check()

    def test_default_assignment_cleanup_preserves_journals_and_refuses_changed_session(self):
        with self.fixture() as f:
            f.host._verified_manifest={'source':{'files':{n:{'sha256':sha} for n,sha in hostmod._DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES.items()}}}
            # The fixture exercises the selected cleanup reader, not a runtime
            # format upgrade, task reassignment or model execution.
            f.host.p['formats']['before']={'wire':23,'controller_schema':24,'worker_format':18}
            f.host.p['formats']['after']={'wire':23,'controller_schema':24,'worker_format':18}
            paths=[]
            for name in ('intent.json','dispatch.json','ack.json'):
                path=f.root/f.base/'supplied-charter/outer-0'/name
                path.parent.mkdir(mode=0o700,exist_ok=True)
                path.write_bytes(common.canonical({'original_outer_record':name}));path.chmod(0o600);paths.append(path)
            output=f.root/'charter-output';output.mkdir(mode=0o700)
            chunk=output/'original-chunk.json';chunk.write_bytes(b'{"original_output":"exact"}');chunk.chmod(0o600);paths.append(chunk)
            expected={p:p.read_bytes() for p in paths}
            f.f.refresh();f.stop=copy.deepcopy(f.f.stop)
            f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
            f.compact();preserved,changes=f.check()
            self.assertTrue(changes)
            self.assertEqual({p:p.read_bytes() for p in paths},expected)
            for p in paths:self.assertIn(str(p.relative_to(f.root)),preserved)
            self.assertTrue(f.host._codex_home_proof['other_original_rows_exact'])
            # A current task on a later session cannot qualify the old command.
            # This is a refusal boundary, not historical-session cleanup support.
            f.task['session_id']='replacement-session'
            with self.assertRaisesRegex(ValueError,'cleanup_transition_canonical_binding'):
                f.check()

    def test_same_format_completed_copyout_preserves_journals_and_proves_only_vendor_disappearance(self):
        with self.fixture() as f:
            common.format_contract(f.host.p)
            self.assertIsNone(common.forward(f.host.p));self.assertTrue(f.host.codex_home_scope())
            original=copy.deepcopy(f.stop);f.compact();preserved,changes=f.check()
            self.assertEqual(f.stop,original)
            expected={n:v for n,v in original['state'].items() if not n.startswith(f.base+'/') or n[len(f.base)+1:].split('/')[0] in hostmod._CODEX_JOURNALS}
            for n in ('mutations/registration.json','mutations/await.json'):expected.pop(n)
            self.assertEqual(preserved,expected)
            self.assertEqual(len(changes),4)
            self.assertTrue(all(v['observed'] is None for v in changes.values()))
            self.assertTrue(f.host._codex_home_proof['other_original_rows_exact'])
            self.assertFalse(f.host._codex_home_proof['same_format_contract_changed'])
            self.assertFalse(f.host._codex_home_proof['host_effects'])
            self.assertEqual(f.host._codex_home_proof['attempts'][f.key]['archive_exports'],f.record['exports'])

    def test_missing_corrupt_extra_archive_or_forged_original_signature_refuses(self):
        for case in ('missing','corrupt','extra','empty_extra','count','bytes','source_signature','archive_identity'):
            with self.subTest(case=case),self.fixture() as f:
                f.compact();archive=f.root/'codex-transcripts'/f.key
                if case=='missing':(archive/'sessions/day/trace.jsonl').unlink()
                elif case=='corrupt':(archive/'sessions/day/trace.jsonl').write_bytes(b'changed')
                elif case=='extra':(archive/'sessions/injected').write_bytes(b'new')
                elif case=='empty_extra':(archive/'unbound').mkdir()
                elif case=='count':f.record['exports'][0]['files']+=1
                elif case=='bytes':f.record['exports'][0]['bytes']+=1
                elif case=='source_signature':f.record['transcript_sources'][0]['signature']['sha256']='0'*64
                elif case=='archive_identity':f.record['archive']['directory']['inode']+=1
                f.write_progress()
                with self.assertRaises(ValueError):f.check()

    def test_incomplete_wrong_policy_live_uncertain_foreign_and_unacknowledged_refuse(self):
        for case in ('incomplete','wrong_policy','foreign_attempt','foreign_worker','live','uncertain','unacknowledged','missing_authority','missing_original','pending_removal','future_clock','no_grace'):
            with self.subTest(case=case),self.fixture() as f:
                f.compact()
                if case=='incomplete':f.record['done']=False
                elif case=='wrong_policy':f.record['policy_revision']='foreign'
                elif case=='foreign_attempt':f.record['attempt_id']='foreign'
                elif case=='foreign_worker':f.task['attempt']['worker_id']='foreign'
                elif case=='live':f.task['attempt']['stopped']=False
                elif case=='uncertain':f.task['attempt']['uncertain']=True
                elif case=='unacknowledged':f.f.write('outbox/pending.json',{'attempt_id':f.attempt['id'],'task_id':'task'})
                elif case=='missing_authority':f.host.r.pop('cleanup_authority')
                elif case=='missing_original':f.stop.pop('codex_homes')
                elif case=='pending_removal':f.record['removing']={'name':'plugins'}
                elif case=='future_clock':f.record['last_observed_ms']=C.now_ms()+60000
                elif case=='no_grace':f.record['first_safe_ms']=f.record['last_observed_ms']
                f.write_progress()
                with self.assertRaises(ValueError):f.check()

    def test_changed_borg_journal_directory_command_or_unknown_row_never_gets_exception(self):
        for case in ('journal','empty_journal','command','unknown','vendor_left','home_owner_mode'):
            with self.subTest(case=case),self.fixture() as f:
                if case=='unknown':(f.root/'reports').mkdir(mode=0o700)
                if case=='unknown':
                    f.f.write('reports/original.json',{'original':True});f.f.refresh();f.stop['state']=copy.deepcopy(f.f.before)
                f.compact()
                if case=='journal':f.f.write(f.base+'/borg-usage.json',{'changed':True})
                elif case=='empty_journal':(f.root/f.base/'completion/empty').rmdir()
                elif case=='command':f.f.write(f.f.command,{'command':f.command,'completed':False,'effects_started':True})
                elif case=='unknown':(f.root/'reports/original.json').unlink()
                elif case=='vendor_left':(f.root/f.base/'vendor-new').write_bytes(b'new')
                elif case=='home_owner_mode':(f.root/f.base).chmod(0o750)
                with self.assertRaises(ValueError):f.check()

    def test_source_map_is_independent_of_forward_contract_and_refuses_one_changed_producer(self):
        with self.fixture() as f:
            self.assertTrue(f.host.codex_home_scope())
            for name in hostmod._CODEX_HOME_SOURCES:
                original=f.host._verified_manifest['source']['files'][name]['sha256']
                f.host._verified_manifest['source']['files'][name]['sha256']='0'*64
                self.assertFalse(f.host.codex_home_scope())
                f.host._verified_manifest['source']['files'][name]['sha256']=original
            f.compact();f.host._verified_manifest={}
            with self.assertRaisesRegex(ValueError,'original_worker_history_changed'):f.check()

    def test_stopped_signature_matches_literal_encoding_and_detects_source_alias(self):
        import hashlib
        with self.fixture() as f:
            # Assert the length from the bytes independently, not the producer's record.
            raw=b'{"turn":1}\n';expected=hashlib.sha256(b'sessions\0directory\0sessions/day\0directory\0sessions/day/trace.jsonl\0file\0'+len(raw).to_bytes(8,'big')+raw).hexdigest()
            self.assertEqual(f.stop['codex_homes'][f.key]['transcript_sources'][0]['signature']['sha256'],expected)
            source=f.root/f.base/'sessions/day/trace.jsonl';source.unlink();source.symlink_to('/unused-external-transcript')
            with self.assertRaises(ValueError):f.host.state_files()

    def test_exact_arg0_links_are_inert_and_only_completed_copyout_can_remove_them(self):
        with self.fixture() as f:
            directory=f.root/f.base/'tmp/arg0-linux/codex-arg0Abc123';directory.mkdir(parents=True,mode=0o700)
            outside=f.root.parent/(f.root.name+'-outside');outside.write_bytes(b'never followed')
            self.addCleanup(lambda:outside.unlink(missing_ok=True))
            (directory/'apply_patch').symlink_to(outside)
            for parent in (directory,directory.parent,directory.parent.parent):parent.chmod(0o700)
            f.f.refresh();f.stop['state']=copy.deepcopy(f.f.before)
            f.stop['codex_homes']=hostmod.codex_home_snapshot(f.host,f.stop['state'])
            self.assertEqual(f.host.state_size()['files'],len(f.stop['state']))
            f.compact();f.check();self.assertEqual(outside.read_bytes(),b'never followed')
            self.assertIn(f.base+'/tmp/arg0-linux/codex-arg0Abc123/apply_patch',f.stop['state'])

    def test_proof_rechecks_archive_and_sidecar_after_observation(self):
        with self.fixture() as f:
            f.compact();current=f.host.state_files();hook=hostmod.CodexHomePreservation(f.host,f.stop,current)
            hook.validate(f.key)
            (f.root/'codex-transcripts'/f.key/'history.jsonl').write_bytes(b'late corruption')
            with self.assertRaises(ValueError):hook.finish()

    def test_checkpoint_requires_current_exact_acknowledged_original_operation(self):
        checkpoint={'registration':{'repository_id':'repo','registration_revision':'one'},'repository':'file:///checkpoint',
                    'commit':'a'*40,'remote_ref':'refs/heads/checkpoint','operation_id':'checkpoint-op'}
        for case in ('acknowledged','unacknowledged','null_outcome','wrong_attempt','wrong_material'):
            with self.subTest(case=case),self.fixture() as f:
                f.f.record['checkpoint']=checkpoint;f.f.write(f.f.name,f.f.record);f.f.refresh()
                f.stop['state']=copy.deepcopy(f.f.before);f.stop['cleanup_records']=copy.deepcopy(f.f.stop['cleanup_records'])
                f.task['selected_checkpoint']=checkpoint;f.compact();f.record['checkpoint']=checkpoint;f.write_progress()
                operation={'id':'checkpoint-op','task_id':'task','attempt_id':f.attempt['id'],'generation':1,'kind':'checkpoint',
                    'state':'settled','outcome':{'acknowledged':True},'intended':{k:checkpoint[k] for k in ('registration','repository','commit','remote_ref')}}
                if case=='unacknowledged':operation['outcome']['acknowledged']=False
                elif case=='null_outcome':operation['outcome']=None
                elif case=='wrong_attempt':operation['attempt_id']='foreign'
                elif case=='wrong_material':operation['intended']['commit']='b'*40
                f.host.api=mock.Mock(return_value={'data':{'kind':'operation','value':operation}})
                if case=='acknowledged':f.check()
                else:
                    with self.assertRaisesRegex(ValueError,'checkpoint_unacknowledged'):f.check()
                f.host.api.assert_called_once_with('get_operation',{'operation_id':'checkpoint-op'})

    def test_selected_metadata_pin_and_original_stop_owner_lock_are_actual_entrypoints(self):
        with self.fixture() as f:
            policy=save(f.root/'selected-policy.json',f.f.selected);f.host.h['state']['policy']=policy
            # Existing controlled account seam: sandbox ancestors may have mapped
            # owners. Keep real pin/read/digest checks; this is not host ownership proof.
            with mock.patch.object(C,'protected',side_effect=lambda path,directory=False:Fixture.leaf_trust(f,path,directory)):
                self.assertEqual(hostmod.Host.cleanup_policy(f.host),f.f.selected)
                bad=copy.deepcopy(policy);bad['sha256']='0'*64;f.host.h['state']['policy']=bad
                with self.assertRaisesRegex(ValueError,'protected pinned input changed'):
                    hostmod.Host.cleanup_policy(f.host)
            f.host.h['state']['policy']=policy;f.host.h['service']='controlled-worker.service'
            f.host.compatibility=mock.Mock();f.host.check_running=mock.Mock(return_value={'state':'running'})
            f.host.bound_now=mock.Mock();f.host.command=mock.Mock();f.host.stopped=mock.Mock(return_value={'state':'stopped'})
            f.host.metadata=lambda:{};f.host.s={};owned=[]
            @contextlib.contextmanager
            def owner():
                owned.append(True)
                try:yield
                finally:owned.pop()
            f.host.owner=owner;original=hostmod.codex_home_snapshot
            def snapshot(host,state):
                self.assertEqual(owned,[True]);return original(host,state)
            with mock.patch.object(C,'verify_package',return_value=(None,f.host._verified_manifest)), \
                 mock.patch.object(hostmod,'codex_home_snapshot',side_effect=snapshot) as captured:
                result=f.host.execute('stop')
            self.assertEqual(result['codex_homes'][f.key]['command'],f.f.command)
            captured.assert_called_once();f.host.command.assert_called_once_with(['stop','controlled-worker.service'])
            self.assertEqual(owned,[])
