import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tooling'))
spec = importlib.util.spec_from_file_location('prepare_native_acceptance', ROOT / 'tooling/prepare_native_acceptance.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ProbeTests(unittest.TestCase):
    def test_spec_uses_only_board_metadata_and_actual_model_delegation(self):
        spec = probe.specs('run-1')
        self.assertEqual(spec['agent']['metadata']['role'], 'native-engineer')
        self.assertNotIn('model', spec['agent']['metadata'])  # Inherits the tested role.
        self.assertEqual(spec['agent']['metadata']['placement']['hosts'], list(probe.HOSTS))
        self.assertIn('native__session_create', spec['task']['description'])
        self.assertIn('exactly THREE', spec['task']['description'])
        self.assertIn('time.sleep(90)', spec['task']['description'])
        self.assertIn('without a lint bypass', spec['task']['description'])
        self.assertEqual(spec['task']['external_link']['external_id'], 'run-1:parent')
        self.assertEqual(probe.specs('run-1'), spec)

    def test_bootstrap_mixed_revision_or_draining_does_not_start_demo(self):
        revision = 'a' * 40
        health = {'host_id': 'agent-lab', 'revision': revision, 'ready': True, 'draining': False, 'instance_id': 'boot'}
        probe.check_health(health, 'agent-lab', revision)
        for changed in ({'ready': False}, {'revision': 'old'}, {'host_id': 'other'}, {'instance_id': None}):
            with self.subTest(changed=changed), self.assertRaises(probe.ConfigError):
                probe.check_health({**health, **changed}, 'agent-lab', revision)

    def test_profile_must_be_complete_unlimited_and_board_sourced(self):
        profile = {**probe.LIMITS, 'profile_source': {'authority': 'board'}}
        probe.check_profile(profile)
        for key in (*probe.LIMITS, 'profile_source'):
            incomplete = dict(profile)
            incomplete.pop(key)
            with self.subTest(key=key), self.assertRaises(probe.ConfigError):
                probe.check_profile(incomplete)

    def test_lost_create_reply_reads_back_without_replaying(self):
        store = []
        calls = []
        def create():
            calls.append(1)
            store.append({'id': 1})
            raise probe.ConfigError('reply lost')
        self.assertEqual(probe.ensure_record(lambda: store[0] if store else None, create, lambda row: row['id'] == 1), {'id': 1})
        probe.ensure_record(lambda: store[0], create, lambda row: row['id'] == 1)
        self.assertEqual(len(calls), 1)

    def test_full_start_reuses_stable_records_and_observes_unchanged_processes(self):
        revision = 'a' * 40
        class FakeBoard:
            def __init__(self):
                self.agents, self.projects, self.tasks, self.writes = {}, [], [], []
            def request(self, method, path, body=None):
                if method != 'GET':
                    self.writes.append(path)
                    if path == '/api/agents': self.agents[body['agent_id']] = {**body, 'id': body['agent_id']}
                    elif path == '/api/projects': self.projects.append({**body, 'id': 42})
                    elif path == '/api/tasks': self.tasks.append({**body, 'id': 73})
                    else: raise AssertionError(path)
                    return {}
                if path == '/api/health': return {'commit': revision, 'ok': True, 'db': True}
                if path == '/api/fleet/hosts': return [{'host_id': h, 'fresh': True, 'enabled': True, 'capacity': 4, 'active': 1} for h in probe.HOSTS]
                if path == '/api/projects': return self.projects
                if path == '/api/tasks?project_id=42': return self.tasks
                if path.startswith('/api/agents/'):
                    name = path.rsplit('/', 1)[1]
                    if name not in self.agents: raise probe.ConfigError('Board HTTP 404')
                    return self.agents[name]
                raise AssertionError(path)
        board = FakeBoard()
        class Native:
            def __init__(self, host): self.host = host
            def request(self, method, path):
                self_host = self.host
                if path == '/health': return {'host_id': self_host, 'revision': revision, 'ready': True, 'draining': False, 'instance_id': 'process-' + self_host}
                if path == '/api/agents': return [{'id': a, **probe.LIMITS, 'profile_source': {'authority': 'board'}} for a in board.agents]
                raise AssertionError(path)
        runtimes = {host: Native(host) for host in probe.HOSTS}
        result = probe.start(board, runtimes, 'run-1', revision)
        self.assertEqual((result['project_id'], result['task_id']), (42, 73))
        self.assertEqual(len(result['profiles']), 4)
        self.assertEqual(board.writes, ['/api/agents', '/api/projects', '/api/tasks'])
        self.assertTrue(result['fresh_registration'])
        repeated = probe.start(board, runtimes, 'run-1', revision)
        self.assertFalse(repeated['fresh_registration'])
        self.assertEqual(repeated['task_id'], result['task_id'])
        self.assertEqual(len(board.writes), 3)

    def test_conflicting_record_or_unconfirmed_write_is_not_success(self):
        with self.assertRaises(probe.ConfigError):
            probe.ensure_record(lambda: {'id': 2}, lambda: self.fail('must not overwrite'), lambda row: row['id'] == 1)
        with self.assertRaises(probe.ConfigError):
            probe.ensure_record(lambda: None, lambda: None, lambda row: True)


if __name__ == '__main__':
    unittest.main()
