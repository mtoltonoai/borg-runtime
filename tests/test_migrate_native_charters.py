import importlib.util
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tooling'))
spec = importlib.util.spec_from_file_location('migrate_native_charters', ROOT / 'tooling/migrate_native_charters.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
OLD = '''Deliver the native harness. Keep gpt-6-astra/max, 128000 output and full 1050000 context.

Your /home/dev-user/borg-native-workspace is your isolated source checkout.
Work on your existing native/* branch and push origin.

Treat every observed stall as an owned defect.

ROLE:
Your workspace is /home/dev-user/borg-native-integration on borg-builder; all sessions are here.'''
class Tests(unittest.TestCase):
    def test_all_roles_use_actual_context_and_migration_is_idempotent(self):
        for agent in mod.ROLES:
            value = mod.migrated(agent, OLD)
            self.assertNotIn('/home/dev-user/', value)
            self.assertIn('CURRENT SESSION', value)
            self.assertIn('Retry transient inference', value)
            self.assertEqual(mod.migrated(agent, value), value)
            self.assertEqual(value.count('ROLE:'), 1)
    def test_deployment_ownership_is_task_specific_not_identity_global(self):
        value = mod.migrated('integrator', OLD)
        self.assertIn('task 6 deployment session', value)
        self.assertIn('OTHER integrator sessions are not pinned', value)
    def test_unknown_shape_fails_without_inventing_a_charter(self):
        with self.assertRaises(mod.ConfigError): mod.migrated('pm', 'missing role')
        with self.assertRaises(mod.ConfigError): mod.migrated('unknown', OLD)
    def test_apply_changes_only_charter_and_preserves_metadata(self):
        class Board:
            def __init__(self): self.data={a:{'charter':OLD,'metadata':{'host':'observational','max_retries':None,'max_output_tokens':128000}} for a in mod.ROLES}; self.bodies=[]
            def request(self, method, path, body=None):
                value=self.data[path.rsplit('/',1)[1]]
                if method=='PATCH': self.bodies.append(body); value['charter']=body['charter']
                return {'charter':value['charter'],'metadata':dict(value['metadata'])}
        board=Board(); self.assertTrue(mod.apply(board,True)['ok'])
        self.assertTrue(all(set(b)=={'charter','principal'} for b in board.bodies))
        self.assertTrue(all(r['action']=='unchanged' for r in mod.apply(board,True)['agents']))
if __name__=='__main__': unittest.main()
