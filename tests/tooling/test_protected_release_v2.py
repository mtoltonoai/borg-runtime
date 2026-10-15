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

if __name__=='__main__':unittest.main()
