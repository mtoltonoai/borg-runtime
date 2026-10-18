"""Observation-tool identity over an immutable effect profile; controlled host seam."""
import argparse
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import protected_rollout_v2 as adapter
import rollout_common_v2 as common
import rollout_host_v2 as hostmod
from rollout_fixture import Fixture, FakeProducer, pin, save, error_cause


class ObservationReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='borg-observation-reader-')
        self.addCleanup(self.temp.cleanup)
        self.f=Fixture(Path(self.temp.name));self.peer=FakeProducer(self.f.subject)
        reader=self.f.root/'reader';reader.mkdir(mode=0o700)
        self.reader_host=reader/'rollout_host_v2.py'
        for source,name in [(adapter.__file__,'protected_rollout_v2.py'),(hostmod.__file__,'rollout_host_v2.py')]:
            dest=reader/name;dest.write_bytes(Path(source).read_bytes());dest.chmod(0o500)
        location=patch.object(adapter,'__file__',str(reader/'protected_rollout_v2.py'))
        location.start();self.addCleanup(location.stop)
        self.stack=self.f.seams();self.stack.__enter__()
        self.addCleanup(self.stack.__exit__,None,None,None)
        for target in ('subprocess.Popen','socket.socket'):
            guard=patch(target,side_effect=AssertionError('controlled test cannot start process or socket'))
            guard.start();self.addCleanup(guard.stop)
        self.original_transport=adapter.c.run.side_effect
        self.dispatched=[]

    def runner(self,observation=None):
        r=adapter.Rollout(self.f.profile,self.f.profile_path,self.f.profile_sha,16777216,
                          self.f.subject,'rollout',observation)
        r.producer.exchange=self.peer.exchange
        return r

    def original(self):
        self.f.lost_phase=['worker','inspect']
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN',result)
        self.assertTrue(all(self.f.active.values()))
        work=self.runner().work
        before={p:p.read_bytes() for p in work.iterdir() if p.is_file()}
        effects=copy.deepcopy(self.f.effects)
        return work,before,effects

    def descriptor(self,work):
        return {'revision':1,'kind':'borg-v2-rollout-observation/1','action_id':'rollout',
            'subject_sha256':common.digest(common.canonical(self.f.subject)),
            'original_intent_sha256':common.digest((work/'intent.json').read_bytes()),
            'effect_profile':{'path':str(self.f.profile_path),'sha256':self.f.profile_sha,'bytes':16777216},
            'original_modules':{k:self.f.profile['modules'][k] for k in ('adapter','host','common')},
            'observation_modules':{'adapter':pin(adapter.__file__),'host':pin(self.reader_host)},
            'host_ids':sorted(h['id'] for h in self.f.profile['hosts'])}

    def binding(self,value,name='observation.json'):
        path=self.f.root/name;row=save(path,value)
        return {**row,'bytes':path.stat().st_size}

    def transport(self,argv,limits,deadline,data):
        """Host CLI is M's separate test scope; exercise its shared validator here."""
        request=common.strict(data);self.dispatched.append(copy.deepcopy(request))
        self.assertEqual(request['phase'],'inspect')
        binding={'path':argv[argv.index('--observation-binding')+1],
            'sha256':argv[argv.index('--observation-binding-sha256')+1],
            'bytes':int(argv[argv.index('--observation-binding-bytes')+1])}
        observation=adapter.validate_observation_binding(self.f.profile,self.f.profile_path,
            self.f.profile_sha,16777216,self.f.subject,'rollout',binding,'host',self.reader_host)
        self.assertEqual(argv[4],observation['descriptor']['observation_modules']['host']['path'])
        result=common.strict(self.original_transport(argv,limits,deadline,data))
        result['observation_reader']=observation['provenance']
        return common.canonical(result)

    def assert_old(self,before,effects):
        self.assertEqual(self.f.effects,effects)
        for path,raw in before.items():self.assertEqual(path.read_bytes(),raw,str(path))
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_completed_effects_reconcile_with_separate_reader_and_preserve_original_bytes(self):
        work,before,effects=self.original();binding=self.binding(self.descriptor(work))
        profile=self.f.profile_path.read_bytes()
        with patch.object(adapter.c,'run',side_effect=self.transport):result=self.runner(binding).execute('reconcile')
        self.assertEqual(result['status'],'APPLIED',result);self.assert_old(before,effects)
        self.assertEqual(self.f.profile_path.read_bytes(),profile)
        self.assertEqual([r['phase'] for r in self.dispatched],['inspect','inspect'])
        self.assertEqual(result['observation_reader']['binding'],binding)
        for host,row in result['host_results'].items():
            proof=Path(row['full_proof']['path'])
            self.assertIn('.readback-',proof.name)
            self.assertEqual(row['observation_reader'],result['observation_reader'])
            full=common.strict(proof.read_bytes())
            self.assertEqual(full['observation_reader'],result['observation_reader'])
            self.assertEqual(set(full['phase_proofs']),{'stop','install','start'})
            key='inspect-'+common.digest(host.encode())
            fresh=common.strict(next(work.glob(key+'.readback-*.request.json')).read_bytes())
            self.assertEqual(fresh['profile_sha256'],self.f.profile_sha)
            self.assertEqual(fresh['dispatch_until_ms'],self.peer.receipts['rollout:start']['reply']['data']['dispatch_until_ms'])

    def test_binding_cannot_enter_apply_or_effect_dispatch(self):
        work,before,effects=self.original();binding=self.binding(self.descriptor(work));r=self.runner(binding)
        names=set(work.iterdir())
        with patch.object(adapter.c,'run') as run:
            with self.assertRaisesRegex(ValueError,'requires reconcile'):r.execute('apply')
            for phase in ('preflight','stop','install','start'):
                with self.subTest(phase=phase),self.assertRaisesRegex(ValueError,'cannot dispatch an effect'):
                    r.host(self.f.profile['hosts'][0],phase,recovery=True)
            run.assert_not_called()
        self.assertEqual(set(work.iterdir()),names);self.assert_old(before,effects)
        for scope in ('apply','preflight','stop','install','start'):
            args=argparse.Namespace(observation_binding='x',observation_binding_sha256='0'*64,observation_binding_bytes=1)
            with self.assertRaisesRegex(ValueError,'readback-only'):adapter.observation_arguments(args,scope)
        args.observation_binding_bytes=None
        with self.assertRaisesRegex(ValueError,'readback-only'):adapter.observation_arguments(args,'reconcile')

    def test_unbound_new_executable_cannot_borrow_old_profile_module_authority(self):
        with self.assertRaisesRegex(ValueError,'explicit binding'):
            adapter.require_selected_reader(self.f.profile,'adapter',adapter.__file__)
        adapter.require_selected_reader(self.f.profile,'adapter',self.f.profile['modules']['adapter']['path'])

    def test_wrong_or_stale_binding_refuses_before_journal_append_or_transport(self):
        work,before,effects=self.original();base=self.descriptor(work);names=set(work.iterdir())
        variants=[]
        for key,value in [('action_id','other'),('subject_sha256','0'*64),('original_intent_sha256','0'*64),('host_ids',['worker']),('kind','other')]:
            x=copy.deepcopy(base);x[key]=value;variants.append(x)
        for key in ('path','sha256','bytes'):
            x=copy.deepcopy(base);x['effect_profile'][key]=0 if key=='bytes' else 'other';variants.append(x)
        for group,key in [('original_modules','host'),('observation_modules','host'),('observation_modules','adapter')]:
            x=copy.deepcopy(base);x[group][key]['sha256']='0'*64;variants.append(x)
        x=copy.deepcopy(base);x['observation_modules']['producer']=base['original_modules']['adapter'];variants.append(x)
        x=copy.deepcopy(base);x['observation_modules']['host']=base['original_modules']['host'];variants.append(x)
        for index,value in enumerate(variants):
            binding=self.binding(value,'invalid-'+str(index)+'.json')
            with self.subTest(index=index),patch.object(adapter.c,'run') as run:
                with self.assertRaises(ValueError):self.runner(binding).execute('reconcile')
                run.assert_not_called()
            self.assertEqual(set(work.iterdir()),names)
        binding=self.binding(base,'tampered.json');Path(binding['path']).write_bytes(b'{}')
        with self.assertRaises(ValueError):self.runner(binding).execute('reconcile')
        binding['path']=str(self.f.root/'absent.json')
        with self.assertRaises(OSError):self.runner(binding).execute('reconcile')
        self.assert_old(before,effects)

    def test_changed_original_phase_still_refuses_with_new_reader(self):
        work,before,effects=self.original();binding=self.binding(self.descriptor(work))
        h=self.f.profile['hosts'][0]
        remote=Path(h['state_root'])/('action-'+common.digest(common.canonical({'action':'rollout','subject':self.f.subject})))
        path=remote/'stop.intent.json';old=common.strict(path.read_bytes());old['profile_sha256']='0'*64;save(path,old)
        with patch.object(adapter.c,'run',side_effect=self.transport):result=self.runner(binding).execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN')
        self.assertEqual(error_cause(result)['message_sha256'],common.digest(b'reconciliation_original_phase_identity'))
        self.assert_old(before,effects)

    def test_missing_reader_provenance_cannot_publish_applied(self):
        work,before,effects=self.original();binding=self.binding(self.descriptor(work))
        with patch.object(adapter.c,'run',side_effect=self.original_transport):result=self.runner(binding).execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN')
        self.assertEqual(error_cause(result)['message_sha256'],common.digest(b'host observation reader proof differs'))
        self.assertNotIn('rollout:observe-applied',self.peer.receipts);self.assert_old(before,effects)

    def test_lost_applied_observation_ack_uses_original_receipt_without_resend(self):
        work,before,effects=self.original();binding=self.binding(self.descriptor(work));r=self.runner(binding)
        def exchange(config,q,**kwargs):
            if q['command']['operation']=='observe_rollout' and q['command']['input']['observed']:
                self.peer.fail='after'
            return self.peer.exchange(config,q,**kwargs)
        r.producer.exchange=exchange
        with patch.object(adapter.c,'run',side_effect=self.transport):
            first=r.execute('reconcile');self.assertEqual(first['status'],'UNKNOWN',first)
            second=self.runner(binding).execute('reconcile')
        self.assertEqual(second['status'],'APPLIED',second);self.assert_old(before,effects)
        applied=[q for q in self.peer.calls if q['command']['operation']=='observe_rollout' and q['command']['input']['observed']]
        self.assertEqual(len(applied),1)
        self.assertTrue(any(q['command']=={'operation':'get_receipt','input':{'operation_id':'rollout:observe-applied'}} for q in self.peer.calls))
