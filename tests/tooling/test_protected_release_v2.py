"""Fixed-producer input/replay mechanics, not protected production compilation."""
from pathlib import Path
import hashlib
import copy
import json
import os
import types
import unittest
from unittest.mock import patch
import package_v2
import protected_release_v2 as producer
import test_package_v2

class ProtectedProducerTests(unittest.TestCase):
    def setUp(self):
        self.case=test_package_v2.PackageTests(methodName='runTest')
        self.case.setUp();self.addCleanup(self.case.doCleanups)
        result=self.case.build(self.case.root/'built')
        self.package=self.case.root/'built/borg-runtime.tar'
        m=package_v2.verify(self.package,result['package_sha256'],self.case.commit)
        a={'format':m['format'],'package_sha256':result['package_sha256'],'package_bytes':self.package.stat().st_size,
           'manifest_sha256':hashlib.sha256(package_v2.canonical(m)).hexdigest(),'source_commit':m['source']['commit'],
           'source_tree':m['source']['tree'],'object_format':m['source']['object_format'],
           'binary_sha256':m['files']['bin/borg-runtime']['sha256'],'binary_bytes':m['files']['bin/borg-runtime']['size'],
           'target':m['files']['bin/borg-runtime']['target']}
        self.subject={'artifact':a,'policy':'test-policy'}
        self.config={'policy':'test-policy','recipe_sha256':'e'*64,'valid_for_ms':30000}
        self.work=self.case.root/'checker';self.work.mkdir(mode=0o700)
    def test_worker_path_swap_cannot_change_verified_snapshot(self):
        def verify(snapshot,*args):
            self.assertNotEqual(snapshot,self.package)
            self.assertEqual(snapshot.stat().st_mode&0o777,0o400)
            self.package.write_bytes(b'changed worker input after copy')
            return package_v2.verify(snapshot,*args)
        proxy=types.SimpleNamespace(MAX_BINARY=package_v2.MAX_BINARY,MAX_MANIFEST=package_v2.MAX_MANIFEST,canonical=package_v2.canonical,verify=verify)
        with patch.object(producer,'load_verifier',return_value=proxy):
            result=producer.package_check(self.config,self.subject,self.package,self.work)
        self.assertEqual(result['results'],{'package_integrity':True})
        self.assertTrue(result['cleanup_verified'])
        self.assertEqual(list(self.work.iterdir()),[])
    def test_bad_copy_digest_never_reaches_verifier_or_emits_check(self):
        self.package.write_bytes(b'wrong input')
        with patch.object(producer,'load_verifier',return_value=package_v2), patch.object(package_v2,'verify') as verify:
            with self.assertRaisesRegex(ValueError,'copied package differs'):
                producer.package_check(self.config,self.subject,self.package,self.work)
            verify.assert_not_called()
        self.assertEqual(list(self.work.iterdir()),[])
    def test_nonregular_input_is_refused_without_waiting_for_writer(self):
        pipe=self.case.root/'worker-pipe';os.mkfifo(pipe)
        with patch.object(producer,'load_verifier',return_value=package_v2):
            with self.assertRaisesRegex(ValueError,'type/bound'):
                producer.package_check(self.config,self.subject,pipe,self.work)
        self.assertEqual(list(self.work.iterdir()),[])
    def test_failed_snapshot_cleanup_cannot_emit_positive_check(self):
        original=Path.unlink
        def fail(path,*a,**kw):
            if path==self.work/'package.snapshot':raise OSError('cleanup failed')
            return original(path,*a,**kw)
        with patch.object(producer,'load_verifier',return_value=package_v2),patch.object(Path,'unlink',fail):
            with self.assertRaisesRegex(OSError,'cleanup failed'):
                producer.package_check(self.config,self.subject,self.package,self.work)
        self.assertTrue((self.work/'package.snapshot').exists())
    def test_retained_files_are_private_independent_of_umask(self):
        old=os.umask(0o002)
        try:
            p=self.work/'retained.json'
            with producer.private_create(p) as out:out.write(b'{}')
            self.assertEqual(p.stat().st_mode&0o777,0o600)
            self.assertEqual(producer.protected_file(p),p)
        finally:os.umask(old)
    def test_evidence_reuse_lost_reply_reconciles_original_request(self):
        import protected_behavior_v2 as behavior
        import test_protected_behavior_v2 as behavior_tests
        lab=behavior_tests.BehaviorProducerTests(methodName='runTest');lab.setUp();self.addCleanup(lab.doCleanups)
        lab.setup_reuse();peer=lab.protected_peer
        synced=set();fsync=os.fsync
        def observed_sync(fd):
            m=os.fstat(fd);fsync(fd);synced.add((m.st_dev,m.st_ino))
        def durable_before_transport(config,request):
            if request['command']['operation']=='check':
                root=lab.last_work
                self.assertEqual(json.loads((root/'request.json').read_bytes()),request)
                bound=json.loads((root/'evidence-binding.json').read_bytes())
                self.assertEqual(bound['request_sha256'],behavior.digest(root/'request.json'))
                self.assertEqual(bound['operation_id'],request['operation_id'])
                for name in ('request.json','evidence-binding.json'):
                    self.assertEqual((root/name).stat().st_mode&0o777,0o600)
                    self.assertEqual((root/name).stat().st_nlink,1)
                for name in ('request.json','evidence-binding.json','applicability.json','execution-evidence.json'):
                    m=(root/name).stat();self.assertIn((m.st_dev,m.st_ino),synced)
            return peer(config,request)
        lab.protected_peer=durable_before_transport;lab.drop_reply=True
        with patch.object(producer.os,'fsync',side_effect=observed_sync):
            with self.assertRaisesRegex(OSError,'lost response'):lab.reusable_run()
        original=lab.last_work;raw=(original/'request.json').read_bytes();request=json.loads(raw)
        kept={p.name:p.read_bytes() for p in original.iterdir() if p.is_file()}
        self.assertEqual(json.loads(kept['receipt.json'])['status'],'uncertain')
        count=sum(len(e.processes) for e in lab.adapter.instances)
        lab.clock_ms=request['command']['input']['valid_until_ms']+1
        retry=types.SimpleNamespace(mode='retry',config=str(lab.producer_config),original_receipt=str(original),
                                   operation_id=request['operation_id'],receipt=str(lab.root/'exact-retry'))
        # Use the normal peer on reconciliation: only the original binding is
        # relevant, and no new applicability or execution is constructed.
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(producer,'exchange',side_effect=peer),\
             patch.object(behavior,'behavior_check',side_effect=AssertionError('retry executed')) as execute,\
             patch.object(behavior,'attest_evidence',side_effect=AssertionError('retry renewed')) as attest:
            self.assertEqual(producer.run(retry),0)
            lookup=types.SimpleNamespace(mode='lookup',config=str(lab.producer_config),original_receipt=str(original),
                                         operation_id='lookup-original',receipt=str(lab.root/'exact-lookup'))
            self.assertEqual(producer.run(lookup),0)
            execute.assert_not_called();attest.assert_not_called()
        self.assertEqual((lab.root/'exact-retry/request.json').read_bytes(),raw)
        self.assertEqual(lab.peer_calls[-1]['command']['input']['operation_id'],request['operation_id'])
        self.assertEqual(sum(len(e.processes) for e in lab.adapter.instances),count)
        for name,value in kept.items():self.assertEqual((original/name).read_bytes(),value)
        wrong=copy.copy(retry);wrong.operation_id='substituted';wrong.receipt=str(lab.root/'wrong-retry')
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(producer,'exchange') as transport:
            with self.assertRaisesRegex(ValueError,'original operation'):producer.run(wrong)
            transport.assert_not_called()
        mismatched=copy.deepcopy(lab.peer_records[request['operation_id']]);mismatched['producer']['id']='foreign-checker'
        lookup.receipt=str(lab.root/'foreign-lookup')
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(producer,'exchange',return_value={'status':'ok','data':mismatched,'replayed':False}):
            with self.assertRaisesRegex(ValueError,'binding differs'):producer.run(lookup)
        mismatched=copy.deepcopy(lab.peer_records[request['operation_id']])
        mismatched['reply']['data']['checked_at_ms']=float(mismatched['reply']['data']['checked_at_ms'])
        lookup.receipt=str(lab.root/'changed-lookup-reply')
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(producer,'exchange',return_value={'status':'ok','data':mismatched,'replayed':False}):
            with self.assertRaisesRegex(ValueError,'Check reply differs'):producer.run(lookup)
        changed=copy.deepcopy(request['command']['input']);changed['checked_at_ms']+=1
        retry.receipt=str(lab.root/'changed-retry-reply')
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(producer,'exchange',return_value={'status':'ok','data':changed,'replayed':True}):
            with self.assertRaisesRegex(ValueError,'retry reply differs'):producer.run(retry)
        # A genuinely new use reconciles that original accepted request first.
        lab.protected_peer=peer
        with patch.object(behavior,'live_step',side_effect=AssertionError('reuse executed')):
            fresh,new=lab.reusable_run(original)
        self.assertGreater(new['checked_at_ms'],request['command']['input']['valid_until_ms'])
        self.assertEqual(sum(len(e.processes) for e in lab.adapter.instances),count)
        # If a later dispatch is unknown and lookup finds no original settlement,
        # a different operation ID is not a way to bypass reconciliation.
        lab.clock_ms+=600001;lab.drop_before_commit=True
        with self.assertRaisesRegex(OSError,'unknown response'):lab.reusable_run(original)
        uncertain=lab.last_work;unknown=json.loads((uncertain/'request.json').read_bytes())
        with self.assertRaisesRegex(ValueError,'original receipt unavailable'):lab.reusable_run(original)
        self.assertFalse((lab.last_work/'request.json').exists())
        self.assertEqual(lab.peer_calls[-1]['command']['input']['operation_id'],unknown['operation_id'])
        self.assertEqual(sum(len(e.processes) for e in lab.adapter.instances),count)


class ProducerStdioTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp=tempfile.TemporaryDirectory(prefix='borg-producer-stdio-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.config=self.root/'producer.json'
        self.value={'socket':'/run/existing-private.sock','server_uid':os.geteuid()+1,'producer_uid':os.geteuid(),'policy':'exact-policy','recipe_sha256':'a'*64,'valid_for_ms':30000}
        self.config.write_bytes(producer.canonical(self.value));self.config.chmod(0o600)
        self.args=types.SimpleNamespace(config=str(self.config),config_sha256=hashlib.sha256(self.config.read_bytes()).hexdigest(),module_sha256=hashlib.sha256(Path(producer.__file__).read_bytes()).hexdigest(),python_sha256=hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest(),max_bytes=4096,timeout_ms=1000)
        # Only the fixture ancestry is outside the installed protected profile.
        self.trust=patch.object(producer,'protected_file',side_effect=lambda p:Path(p));self.trust.start();self.addCleanup(self.trust.stop)
        self.request={'revision':1,'operation_id':'original-op','command':{'operation':'get_receipt','input':{'operation_id':'original-mutating-op'}}}
    def test_fixed_stdio_preserves_exact_request_and_reply(self):
        reply={'status':'ok','data':{'retained':'original'},'replayed':False}
        with patch.object(producer,'stream_bytes',side_effect=[producer.canonical(self.request),None]) as stream,patch.object(producer,'exchange',return_value=reply) as exchange:
            self.assertEqual(producer.exchange_stdio(self.args),0)
        self.assertEqual(exchange.call_args.args,(self.value,self.request))
        self.assertLessEqual(exchange.call_args.kwargs['timeout_ms'],1000)
        self.assertEqual(stream.call_args_list[1].args[3],producer.canonical(reply))
        self.assertEqual(stream.call_args_list[0].args[2],stream.call_args_list[1].args[2])
    def test_stdio_preserves_positive_bounds_above_local_defaults(self):
        self.args.max_bytes=producer.MAX*2;self.args.timeout_ms=120000
        reply={'status':'ok','data':{'retained':'x'*(producer.MAX+1)},'replayed':False}
        self.assertGreater(len(producer.canonical(reply)),producer.MAX)
        with patch.object(producer.time,'monotonic',return_value=10.0),patch.object(producer,'stream_bytes',side_effect=[producer.canonical(self.request),None]) as stream,patch.object(producer,'exchange',return_value=reply) as exchange:
            self.assertEqual(producer.exchange_stdio(self.args),0)
        self.assertEqual(exchange.call_args.args,(self.value,self.request))
        self.assertEqual(exchange.call_args.kwargs,{'max_bytes':producer.MAX*2,'timeout_ms':120000})
        self.assertEqual(stream.call_args_list[0].args,(0,producer.MAX*2,130.0))
        self.assertEqual(stream.call_args_list[1].args,(1,producer.MAX*2,130.0,producer.canonical(reply)))
    def test_stdio_requires_explicit_positive_integer_bounds(self):
        for field in ('max_bytes','timeout_ms'):
            original=getattr(self.args,field)
            for value in (None,False,0,-1,1.5):
                setattr(self.args,field,value)
                with patch.object(producer,'exchange') as exchange:
                    with self.assertRaisesRegex(ValueError,'stdio bounds'):producer.exchange_stdio(self.args)
                    exchange.assert_not_called()
            setattr(self.args,field,original)
    def test_remote_config_module_python_and_uid_are_pinned_before_connect(self):
        for field in ('config_sha256','module_sha256','python_sha256'):
            old=getattr(self.args,field);setattr(self.args,field,'0'*64)
            with patch.object(producer,'exchange') as exchange:
                with self.assertRaisesRegex(ValueError,'differs'):producer.exchange_stdio(self.args)
                exchange.assert_not_called()
            setattr(self.args,field,old)
        self.value['producer_uid']+=10;self.config.write_bytes(producer.canonical(self.value));self.args.config_sha256=hashlib.sha256(self.config.read_bytes()).hexdigest()
        with patch.object(producer,'exchange') as exchange:
            with self.assertRaisesRegex(ValueError,'identity/interval'):producer.exchange_stdio(self.args)
            exchange.assert_not_called()
    def test_stdio_cannot_submit_raw_check_or_internal_observation(self):
        for operation in ('check','start','finish','reconcile_observation','arbitrary'):
            request={**self.request,'command':{'operation':operation,'input':{}}}
            with patch.object(producer,'stream_bytes',return_value=producer.canonical(request)),patch.object(producer,'exchange') as exchange:
                with self.assertRaisesRegex(ValueError,'unsupported producer command'):producer.exchange_stdio(self.args)
                exchange.assert_not_called()
    def test_lost_private_reply_never_retries_or_claims_success(self):
        with patch.object(producer,'stream_bytes',return_value=producer.canonical(self.request)) as stream,patch.object(producer,'exchange',side_effect=TimeoutError('reply lost')) as exchange:
            with self.assertRaisesRegex(TimeoutError,'reply lost'):producer.exchange_stdio(self.args)
        exchange.assert_called_once();self.assertEqual(stream.call_count,1)
    def test_original_stdio_deadline_is_not_reset(self):
        with patch.object(producer.os,'get_blocking',return_value=True),patch.object(producer.os,'set_blocking') as blocking,patch.object(producer.time,'monotonic',side_effect=[0.0,0.8,1.1]),patch.object(producer.select,'select',return_value=([7],[],[])),patch.object(producer.os,'read',return_value=b'x') as read:
            with self.assertRaisesRegex(TimeoutError,'deadline'):producer.stream_bytes(7,100,1.0)
        self.assertEqual(read.call_count,2);self.assertEqual(blocking.call_args.args,(7,True))
    def test_stdio_bound_checks_before_json_and_restores_descriptor(self):
        with patch.object(producer.os,'get_blocking',return_value=True),patch.object(producer.os,'set_blocking') as blocking,patch.object(producer.time,'monotonic',return_value=0),patch.object(producer.select,'select',return_value=([7],[],[])),patch.object(producer.os,'read',return_value=b'12345'):
            with self.assertRaisesRegex(ValueError,'request bound'):producer.stream_bytes(7,4,1.0)
        self.assertEqual(blocking.call_args.args,(7,True))

if __name__=='__main__':unittest.main()
