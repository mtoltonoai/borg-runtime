"""Closed current19 marker observation on local files and controlled inputs.

Port of the qualified metadata readback regression. No saved host paths, package
claims, subprocesses or network. Real local descriptors exercise no-follow,
owner/mode/link, exact-byte and post-capture drift refusal.
"""
import copy
import os
from pathlib import Path
import stat
import tempfile
import unittest
from contextlib import ExitStack
from unittest import mock
import rollout_host_v2 as H


class CurrentFormatObservationTests(unittest.TestCase):
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        for name in ('subprocess.Popen','socket.socket','socket.create_connection','os.system','os.posix_spawn','os.posix_spawnp'):
            self.stack.enter_context(mock.patch(name,side_effect=AssertionError('external effects forbidden')))
        for name in ('run','http_read'):
            self.stack.enter_context(mock.patch.object(H.c,name,side_effect=AssertionError('host effects forbidden')))
        self.profile={'formats':{'before':dict(H._COMPOSED_CODEX_HOME_FORMATS),
                                 'after':dict(H._COMPOSED_CODEX_HOME_FORMATS),'source_files':{}}}
        self.manifest={'source':{'files':{n:{'sha256':v} for n,v in H._COMPOSED_CODEX_HOME_SOURCES.items()}}}
        self.host=H.Host.__new__(H.Host)
        self.host.p=copy.deepcopy(self.profile)
        self.host.h={'role':'worker','service_uid':os.geteuid(),'state':{'root':'/fixture/current-format'}}
        self.host.l={'state_bytes':4096,'receipt_bytes':65536,'state_entries':128}
        self.host.r={'deadline_ms':2000}
        self.host._verified_manifest=copy.deepcopy(self.manifest)
        self.host.retained_policy=mock.Mock(return_value={'worker':{'state_format_bytes':64}})
        self.now=1000
        self.stack.enter_context(mock.patch.object(H.c,'now_ms',side_effect=lambda:self.now))
        self.raw=b'{"format_revision":19}'
        self.old={'sha256':H.c.digest(self.raw),'identity':[1,2,os.geteuid(),os.getegid(),0o600]}
        self.new=copy.deepcopy(self.old);self.new['identity'][1]=3
        self.stop={'state':{'format.json':copy.deepcopy(self.old)}}
        self.capture=mock.Mock(side_effect=self.remote_capture)
        self.capture_patch=mock.patch.object(H,'_file',self.capture)
        self.capture_patch.start();self.addCleanup(self.capture_patch.stop)

    def remote_capture(self,path,bound,uid):
        self.assertEqual(path,Path(self.host.h['state']['root'])/'format.json')
        self.assertEqual(uid,self.host.h['service_uid'])
        self.assertLessEqual(len(self.raw),bound)
        return self.raw,copy.deepcopy(self.new['identity'])

    def history(self,name='format.json',full=False):
        stop=self.stop if full else {'state':{name:self.old}}
        current=copy.deepcopy(stop['state']);current[name]=copy.deepcopy(self.new)
        return self.host.worker_history(stop,current)

    def local_file(self,root):
        self.capture_patch.stop()
        root.mkdir(mode=0o700)
        self.host.h['state']['root']=str(root);self.host.h['service_uid']=os.geteuid()
        path=root/'format.json';path.write_bytes(self.raw);path.chmod(0o600)
        self.old=self.local_row(path)
        # Keep the original inode allocated through every negative case. A
        # later replacement may otherwise reuse it and become an unchanged row.
        original_fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        self.addCleanup(os.close,original_fd)
        replacement=root/'replacement';replacement.write_bytes(self.raw);replacement.chmod(0o600)
        os.replace(replacement,path);self.new=self.local_row(path)
        return path

    def local_row(self,path):
        m=path.stat()
        return {'sha256':H.c.digest(path.read_bytes()),'identity':[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]}

    def test_current_marker_atomic_replacement_preserves_both_rows_and_proof_scope(self):
        before=copy.deepcopy(self.stop);current={'format.json':copy.deepcopy(self.new)}
        preserved,observed=self.host.worker_history(self.stop,current)
        self.assertEqual(preserved,{})
        self.assertEqual(set(observed),{'format.json'})
        proof=observed['format.json']
        self.assertEqual((proof['original'],proof['observed']),(self.old,self.new))
        self.assertEqual(proof['format_revision'],19)
        self.assertTrue(proof['original_bytes_preserved'] and proof['noninode_identity_preserved'])
        self.assertFalse(proof['actual_writer_attributed'] or proof['simultaneous_snapshot'] or proof['service_effects'])
        self.assertEqual(set(proof['writer_sources']),{'src/worker/handoff.rs','src/worker/process.rs','src/worker/mod.rs'})
        self.assertEqual(self.stop,before);self.assertEqual(current,{'format.json':self.new})
        self.assertEqual(self.host.p,self.profile)
        self.capture.assert_called_once()

    def test_complete_authenticated_family_and_exact_current_formats_are_required(self):
        cases=('absent','empty','family_missing','handoff_wrong','process_wrong','mod_wrong','before','after','controller')
        for case in cases:
            self.host.p=copy.deepcopy(self.profile);self.host._verified_manifest=copy.deepcopy(self.manifest)
            self.host.h['role']='worker'
            if case=='absent':del self.host._verified_manifest
            elif case=='empty':self.host._verified_manifest={}
            elif case=='family_missing':del self.host._verified_manifest['source']['files']['src/worker/codex.rs']
            elif case.endswith('_wrong'):
                name='src/worker/'+case.split('_')[0]+'.rs'
                self.host._verified_manifest['source']['files'][name]['sha256']='0'*64
            elif case in ('before','after'):self.host.p['formats'][case]['worker_format']=18
            else:self.host.h['role']='controller'
            with self.subTest(case=case),self.assertRaises(ValueError):self.history()
        self.capture.assert_not_called()

    def test_changed_hash_marker_value_and_closed_shape_refuse(self):
        self.new['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'original_bytes_changed'):self.history()
        for raw in (b'{"format_revision":18}',b'{"format_revision":true}',b'{"format_revision":19,"extra":0}',b'{"format_revision":19,"format_revision":19}',b'[]'):
            self.raw=raw;self.old['sha256']=self.new['sha256']=H.c.digest(raw)
            with self.subTest(raw=raw),self.assertRaises(ValueError):self.history()

    def test_noninode_metadata_and_identity_shape_refuse(self):
        old,new=copy.deepcopy(self.old),copy.deepcopy(self.new)
        for field in (0,2,3,4):
            self.old=copy.deepcopy(old);self.new=copy.deepcopy(new);self.new['identity'][field]+=1
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'noninode_identity_changed'):self.history()
        for identity in ([1],[-1,2,999,987,384],[True,2,999,987,384],[64769,2,999,987,420]):
            self.old=copy.deepcopy(old);self.new=copy.deepcopy(new)
            self.new['identity']=copy.deepcopy(identity)
            self.assertNotEqual(self.old,self.new)
            with self.subTest(identity=identity),self.assertRaisesRegex(ValueError,'noninode_identity_changed'):self.history()
        self.capture.assert_not_called()

    def test_local_descriptor_same_byte_replace_and_alias_owner_mode_drift_refuse(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.local_file(Path(temp)/'state')
            self.assertNotEqual(self.old['identity'][1],self.new['identity'][1]);self.history()
            saved=copy.deepcopy(self.new)
            link=path.with_name('hardlink');os.link(path,link)
            with self.assertRaisesRegex(ValueError,'file_metadata'):self.history()
            link.unlink()
            path.chmod(0o666)
            with self.assertRaisesRegex(ValueError,'file_metadata'):self.history()
            path.chmod(0o600)
            target=path.with_name('target');path.rename(target);path.symlink_to(target)
            with self.assertRaisesRegex(ValueError,'file_metadata'):self.history()
            path.unlink();target.rename(path)
            path.write_bytes(b'{"format_revision":18}')
            with self.assertRaisesRegex(ValueError,'sample_changed'):self.history()
            path.write_bytes(self.raw)
            replacement=path.with_name('new');replacement.write_bytes(self.raw);replacement.chmod(0o600)
            os.replace(replacement,path)
            with self.assertRaisesRegex(ValueError,'sample_changed'):self.history()
            self.new=self.local_row(path)
            self.host.h['service_uid']+=1;self.old['identity'][2]=self.new['identity'][2]=self.host.h['service_uid']
            self.assertNotEqual(self.old,self.new)
            with self.assertRaisesRegex(ValueError,'directory_owner'):self.history()
            self.assertEqual(saved['sha256'],H.c.digest(path.read_bytes()))

    def test_deadline_capture_and_receipt_bounds_remain_enforced(self):
        with tempfile.TemporaryDirectory() as temp:
            self.local_file(Path(temp)/'state')
            self.now=2000
            with self.assertRaisesRegex(ValueError,'observation_deadline'):self.history()
            self.now=1000
            with mock.patch.object(H.c,'now_ms',side_effect=[1000,2000]):
                with self.assertRaisesRegex(ValueError,'observation_deadline'):self.history()
            policy=self.host.retained_policy.return_value
            original_bound=policy['worker']['state_format_bytes']
            policy['worker']['state_format_bytes']=len(self.raw)-1
            with self.assertRaisesRegex(ValueError,'file_metadata'):self.history()
            policy['worker']['state_format_bytes']=original_bound
            self.host.l['receipt_bytes']=128
            with self.assertRaisesRegex(ValueError,'proof_bound'):self.history()

    def test_missing_row_capture_mismatch_nonmarker_and_unchanged_row_stay_strict(self):
        expected=copy.deepcopy(self.new)
        self.new=None
        with self.assertRaisesRegex(ValueError,'original_bytes_changed'):self.history()
        self.new=expected
        self.capture.side_effect=lambda *a:(self.raw,[1,2,999,987,384])
        with self.assertRaisesRegex(ValueError,'sample_changed'):self.history()
        self.capture.side_effect=self.remote_capture
        for name in ('other.json','nested/format.json','format.json/child','format.json.pending'):
            with self.subTest(name=name),self.assertRaises(H.c.GuardFailure):self.history(name)
        self.new=copy.deepcopy(self.old);self.capture.reset_mock()
        preserved,observed=self.history()
        self.assertEqual(preserved,{'format.json':self.old});self.assertEqual(observed,{})
        self.capture.assert_not_called()


if __name__ == "__main__":unittest.main()
