"""Fixed-producer input/replay mechanics, not protected production compilation."""
from pathlib import Path
import hashlib
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
