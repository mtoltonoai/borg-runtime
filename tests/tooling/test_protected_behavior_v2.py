"""Producer mechanics with real bounded fixture processes, not account isolation.

Positive tests replace only the privileged cgroup/UID adapter. They never publish
production checks or claim that same-UID fixtures establish protected accounts.
"""
from pathlib import Path
import copy, hashlib, json, os, signal, subprocess, sys, types, unittest
from unittest.mock import patch
import package_v2
import protected_behavior_v2 as behavior
import protected_release_v2 as producer
import test_package_v2

class FixtureExecution:
    instances=[]
    fail_cleanup=False
    def __init__(self,recipe,config):
        self.recipe=recipe;self.config=config;self.lock=None;self.owns=False;self.processes=[];self.ocg='oracle';self.cg='candidate';self.released=None
        self.instances.append(self)
    def admit(self,work,operation_id,subject_hash):
        self.job=Path(self.recipe['execution_root'])/work.name;self.job.mkdir();self.workspace=self.job/'workspace';self.workspace.mkdir();self.lock=True;self.owns=True
        behavior.saved(work/'execution-binding.json',{'fixture_only':True,'operation_id':operation_id,'subject_sha256':subject_hash})
    def spawn(self,argv,env):
        p=subprocess.Popen(argv,cwd=self.workspace,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,close_fds=True,start_new_session=True);self.processes.append(p);return p
    def spawn_oracle(self,argv,work):
        p=subprocess.Popen(argv,cwd=work,env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'},stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,close_fds=True,start_new_session=True);self.processes.append(p);return p
    def clean_group(self,group):
        for p in self.processes:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            p.wait(timeout=5)
        if self.fail_cleanup:raise ValueError('fixture cleanup unknown')
    def finish_candidate(self,process):
        try:self.clean_group('all')
        finally:process.wait(timeout=5)
    def clean(self):self.clean_group('all')
    def release(self,clean):self.released=clean;self.lock=None

class BehaviorProducerTests(unittest.TestCase):
    def setUp(self):
        self.case=test_package_v2.PackageTests(methodName='runTest');self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.root=self.case.root
        # Model installed protected tools with private copies. The checkout may
        # be group-writable; tests never chmod it or relax production guards.
        installed=self.root/'installed';installed.mkdir(mode=0o700)
        module_paths={behavior:Path(behavior.__file__),producer:Path(producer.__file__)}
        for name,source in [('protected_behavior_v2.py',module_paths[behavior]),('protected_release_v2.py',module_paths[producer]),('package_v2.py',Path(package_v2.__file__))]:
            target=installed/name;target.write_bytes(source.read_bytes());target.chmod(0o600)
        for module,source in module_paths.items():
            binding=patch.object(module,'__file__',str(installed/source.name));binding.start();self.addCleanup(binding.stop)
        # Only private fixture ancestry under /tmp is mocked; dedicated guard
        # cases below exercise the unchanged production path validator.
        self.real_protected=behavior.protected
        self.path_fixture=patch.object(behavior,'protected',side_effect=self.fixture_protected);self.path_fixture.start();self.addCleanup(self.path_fixture.stop)
        result=self.case.build(self.root/'built');self.package=self.root/'built/borg-runtime.tar'
        m=package_v2.verify(self.package,result['package_sha256'],self.case.commit)
        self.subject={'release_id':'release-1','repository':'fixture-repository','target_ref':'refs/heads/main','expected_base':'1'*40,'policy':'fixture-policy','deployment_target':'fixture-target','target_revision':'revision-1','installed_package_sha256':'a'*64,
          'artifact':{'format':m['format'],'package_sha256':result['package_sha256'],'package_bytes':self.package.stat().st_size,'manifest_sha256':hashlib.sha256(package_v2.canonical(m)).hexdigest(),'source_commit':m['source']['commit'],'source_tree':m['source']['tree'],'object_format':m['source']['object_format'],'binary_sha256':m['files']['bin/borg-runtime']['sha256'],'binary_bytes':m['files']['bin/borg-runtime']['size'],'target':m['files']['bin/borg-runtime']['target']}}
        self.candidate=self.root/'candidate.py';self.candidate.write_text('print("observed-controller-result")\n')
        self.oracle=self.root/'oracle.py';self.oracle.write_text('''import hashlib,json,pathlib,sys
c=json.loads(pathlib.Path(sys.argv[1]).read_text())
raw=pathlib.Path(c['output_path']).read_bytes()
passed=hashlib.sha256(raw).hexdigest()==c['output_sha256'] and raw==b'observed-controller-result\\n'
print(json.dumps({'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':passed,'observations':['Exact bounded fixture output matched the fixed oracle; not production behavioral qualification']}))
''')
        self.candidate.chmod(0o600);self.oracle.chmod(0o600)
        self.execution=self.root/'execution';self.execution.mkdir(mode=0o700)
        self.recipe={'revision':1,'supervisor_sha256':behavior.digest(Path(behavior.__file__).absolute()),'controller_config':str(self.root/'controller.json'),'execution_uid':os.geteuid()+1,'execution_gid':os.getegid()+1,'execution_root':str(self.execution),'execution_cgroup':'/sys/fs/cgroup/operator-fixture-candidate','oracle_cgroup':'/sys/fs/cgroup/operator-fixture-oracle',**{k:self.subject[k] for k in ('policy','deployment_target','target_revision','installed_package_sha256')},'required_checks':['fixture_observation'],'steps':[{'check':'fixture_observation','candidate':{'executable':str(Path(sys.executable).resolve()),'sha256':behavior.digest(Path(sys.executable).resolve()),'arguments':['-I','-S','-B',str(self.candidate)]},'oracle':{'path':str(self.oracle),'sha256':behavior.digest(self.oracle)},'timeout_ms':2000,'output_bytes':4096}]}
        self.config={'socket':str(self.root/'not-contacted.sock'),'server_uid':os.geteuid()+2,'producer_uid':os.geteuid(),'policy':self.subject['policy'],'recipe_sha256':'','valid_for_ms':30000}
        self.recipe_path=self.root/'recipe.json';self.write_recipe();FixtureExecution.instances=[];FixtureExecution.fail_cleanup=False
        self.n=0
    def fixture_protected(self,path,directory=False):
        p=Path(path)
        if p==self.root or self.root in p.parents or p==Path(behavior.__file__).absolute() or p==Path('/usr/bin/python3').resolve():
            m=p.lstat()
            if p.is_symlink() or m.st_mode&0o022:raise ValueError('fixture protected file mode/alias')
            return p
        return self.real_protected(p,directory)
    def write_recipe(self):
        self.recipe_path.write_bytes(behavior.canonical(self.recipe));self.recipe_path.chmod(0o600);self.config['recipe_sha256']=behavior.digest(self.recipe_path)
        self.controller_config={k:self.subject[k] for k in ('policy','repository','target_ref','deployment_target','target_revision','installed_package_sha256')}
        self.controller_config.update(socket=self.config['socket'],recipe_sha256=self.config['recipe_sha256'],worker_uids=[self.recipe['execution_uid']],producers=[{'id':'checker','uid':self.config['producer_uid'],'role':'checker'}],required_checks=['package_integrity',*self.recipe['required_checks']],max_check_age_ms=self.config['valid_for_ms'])
        self.write_controller()
    def write_controller(self):
        p=Path(self.recipe['controller_config']);p.write_bytes(behavior.canonical(self.controller_config));p.chmod(0o600)
    def work(self):
        self.n+=1;p=self.root/('receipt-'+str(self.n));p.mkdir(mode=0o700);return p
    def run_check(self,work=None):
        work=work or self.work()
        with patch.object(behavior,'Execution',FixtureExecution):
            return behavior.behavior_check(self.config,self.subject,self.package,self.recipe_path,work,'check-original')
    def test_trusted_operator_can_have_distinct_roles_without_worker_authority(self):
        for role in ('human','repository','rollout'):
            self.controller_config['producers'].append({'id':role,'uid':self.config['producer_uid'],'role':role})
        self.write_controller()
        result=self.run_check()
        self.assertTrue(result['cleanup_verified']);self.assertTrue(all(result['results'].values()))
        # The candidate remains a separate configured worker, never a producer.
        self.assertNotEqual(self.recipe['execution_uid'],self.config['producer_uid'])
        self.controller_config['worker_uids'].append(self.config['producer_uid']);self.write_controller()
        with self.assertRaisesRegex(ValueError,'identities overlap'):self.run_check()
    def test_duplicate_operator_role_or_producer_id_refuses_before_execution(self):
        for item in ({'id':'second-checker','uid':self.config['producer_uid'],'role':'checker'},
                     {'id':'checker','uid':self.config['producer_uid'],'role':'human'}):
            self.write_recipe();self.controller_config['producers'].append(item);self.write_controller()
            with patch.object(behavior,'Execution') as execution:
                with self.assertRaisesRegex(ValueError,'identities overlap'):self.run_check()
                execution.assert_not_called()
    def test_execution_identity_cannot_be_another_protected_role(self):
        for role in ('human','repository','rollout'):
            self.write_recipe();self.controller_config['producers'].append({'id':role,'uid':self.recipe['execution_uid'],'role':role});self.write_controller()
            with self.assertRaisesRegex(ValueError,'identities overlap'):self.run_check()
        self.write_recipe();self.controller_config['worker_uids']=[];self.write_controller()
        with self.assertRaisesRegex(ValueError,'configured worker'):self.run_check()
    def test_controller_mandatory_checks_target_and_checker_role_are_bound(self):
        for field,value,message in [('required_checks',['package_integrity','missing_actual_check'],'mandatory checks'),('deployment_target','other','exact subject'),('recipe_sha256','a'*64,'socket/recipe'),('producers',[{'id':'operator','uid':self.config['producer_uid'],'role':'human'}],'configured checker')]:
            self.write_recipe();self.controller_config[field]=value;self.write_controller()
            with self.assertRaisesRegex(ValueError,message):self.run_check()
    def test_installed_helper_pin_is_checked_before_loading(self):
        self.assertEqual(producer.BEHAVIOR_SHA,behavior.digest(Path(behavior.__file__)))
        loaded=producer.load_behavior();self.assertTrue(callable(loaded.behavior_check))
        original=Path.read_bytes
        def changed(path):
            data=original(path)
            return data+b'\n' if path.name=='protected_behavior_v2.py' else data
        with patch.object(Path,'read_bytes',changed):
            with self.assertRaisesRegex(ValueError,'supervisor identity'):producer.load_behavior()
    def test_fixed_oracle_observes_exact_subject_not_exit_alone(self):
        w=self.work();r=self.run_check(w)
        self.assertEqual(r['subject'],self.subject);self.assertEqual(r['results'],{'package_integrity':True,'fixture_observation':True});self.assertTrue(r['cleanup_verified'])
        self.assertTrue((w/'behavior-intent.json').exists());self.assertTrue((w/'behavior-result.json').exists())
        self.assertTrue(all(p.poll() is not None for p in FixtureExecution.instances[-1].processes))
    def test_candidate_forged_positive_json_is_only_observation(self):
        self.candidate.write_text('print(\'{"passed":true,"cleanup_verified":true,"human_rollout":true}\')\n')
        result=self.run_check();self.assertFalse(result['results']['fixture_observation']);self.assertNotIn('human_rollout',result)
    def test_candidate_failure_cannot_be_overridden_by_oracle_true(self):
        self.candidate.write_text('import sys\nprint("observed-controller-result")\nsys.exit(7)\n')
        result=self.run_check();self.assertFalse(result['results']['fixture_observation'])
    def test_missing_and_duplicate_mandatory_oracles_refuse_before_execution(self):
        self.recipe['required_checks'].append('missing');self.write_recipe()
        with patch.object(behavior,'Execution') as e:
            with self.assertRaisesRegex(ValueError,'missing/duplicate'):self.run_check()
            e.assert_not_called()
        self.recipe['required_checks']=['fixture_observation'];self.recipe['steps']*=2;self.write_recipe()
        with self.assertRaisesRegex(ValueError,'missing/duplicate'):self.run_check()
    def test_wrong_target_artifact_and_recipe_refuse(self):
        self.subject['target_revision']='wrong'
        with self.assertRaisesRegex(ValueError,'target/policy'):self.run_check()
        self.subject['target_revision']='revision-1';self.subject['artifact']['package_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'copied package differs'):self.run_check()
        self.recipe_path.write_bytes(self.recipe_path.read_bytes()+b' ')
        with self.assertRaisesRegex(ValueError,'recipe differs'):self.run_check()
    def test_changed_oracle_and_writable_recipe_refuse(self):
        self.oracle.write_text('print("forged")\n')
        with self.assertRaisesRegex(ValueError,'oracle identity'):self.run_check()
        self.oracle.write_text('x');self.recipe_path.chmod(0o666)
        with self.assertRaisesRegex(ValueError,'protected|owner|mode'):self.run_check()
    def test_overlapping_identity_and_unsafe_check_path_refuse(self):
        self.recipe['execution_uid']=self.config['producer_uid'];self.write_recipe()
        with self.assertRaisesRegex(ValueError,'overlaps'):self.run_check()
        self.recipe['execution_uid']+=1;self.recipe['required_checks']=['../../escape'];self.write_recipe()
        with self.assertRaisesRegex(ValueError,'inventory'):self.run_check()
    def test_wrong_oracle_subject_cannot_emit_check(self):
        self.oracle.write_text(self.oracle.read_text().replace("'subject_sha256':c['subject_sha256']","'subject_sha256':'wrong'"));self.recipe['steps'][0]['oracle']['sha256']=behavior.digest(self.oracle);self.write_recipe()
        w=self.work()
        with self.assertRaisesRegex(ValueError,'exact-subject'):self.run_check(w)
        self.assertFalse((w/'behavior-result.json').exists())
    def test_timeout_and_output_overrun_leave_pending_without_check(self):
        for body,timeout,cap,exception in [('import time;time.sleep(2)',20,4096,TimeoutError),('print("x"*10000)',2000,64,ValueError)]:
            self.candidate.write_text(body);self.recipe['steps'][0].update(timeout_ms=timeout,output_bytes=cap);self.write_recipe();w=self.work()
            with self.assertRaises(exception):self.run_check(w)
            self.assertTrue((w/'behavior-intent.json').exists());self.assertFalse((w/'behavior-result.json').exists());self.assertFalse(FixtureExecution.instances[-1].released)
            self.assertTrue(all(p.poll() is not None for p in FixtureExecution.instances[-1].processes))
    def test_unknown_cleanup_cannot_create_positive_oracle_or_check(self):
        FixtureExecution.fail_cleanup=True;w=self.work()
        with self.assertRaisesRegex(ValueError,'cleanup unknown'):self.run_check(w)
        self.assertFalse((w/'fixture_observation.oracle-result.json').exists());self.assertFalse((w/'behavior-result.json').exists())
    def test_real_execution_refuses_missing_launch_authority(self):
        if os.geteuid()==0:self.skipTest('non-root refusal requires ordinary fixture UID')
        e=behavior.Execution(self.recipe,self.config)
        with self.assertRaisesRegex(ValueError,'launch authority'):e.admit(self.work(),'stable','a'*64)
        self.assertEqual(list(self.execution.iterdir()),[])
    def test_child_drops_groups_identity_before_candidate_exec(self):
        e=behavior.Execution(self.recipe,self.config);e.cg=self.root/'fake-cgroup';e.cg.mkdir();(e.cg/'cgroup.procs').write_text('');e.binding={'cgroup':str(e.cg),'cgroup_identity':[e.cg.stat().st_dev,e.cg.stat().st_ino]}
        calls=[]
        with patch.object(behavior.os,'setgroups',side_effect=lambda x:calls.append(('groups',x))),patch.object(behavior.os,'setgid',side_effect=lambda x:calls.append(('gid',x))),patch.object(behavior.os,'setuid',side_effect=lambda x:calls.append(('uid',x))),patch.object(behavior.os,'geteuid',return_value=self.recipe['execution_uid']),patch.object(behavior.os,'getegid',return_value=self.recipe['execution_gid']),patch.object(behavior.ctypes,'CDLL',return_value=types.SimpleNamespace(prctl=lambda *args:0)):
            e.child()
        self.assertEqual(calls,[('groups',[]),('gid',self.recipe['execution_gid']),('uid',self.recipe['execution_uid'])])
        self.assertEqual((e.cg/'cgroup.procs').read_text(),str(os.getpid()))
    def test_cleanup_rejects_replaced_domain_before_signaling(self):
        e=behavior.Execution(self.recipe,self.config);e.cg=self.root/'changed-domain';e.cg.mkdir();e.owns=True
        kill=e.cg/'cgroup.kill';kill.write_text('untouched')
        e.binding={'cgroup':str(e.cg),'cgroup_identity':[e.cg.stat().st_dev,e.cg.stat().st_ino+1]}
        with self.assertRaisesRegex(ValueError,'domain changed'):e.clean_group(e.cg)
        self.assertEqual(kill.read_text(),'untouched')
    def test_owned_direct_child_reaped_before_uid_audit_even_on_cleanup_error(self):
        e=behavior.Execution(self.recipe,self.config);seen=[]
        child=types.SimpleNamespace(wait=lambda **kw:seen.append('reaped'))
        with patch.object(e,'clean_group',side_effect=lambda group:seen.append('group')),patch.object(e,'idle_uid',side_effect=lambda:seen.append('uid')):
            e.finish_candidate(child)
        self.assertEqual(seen,['group','group','reaped','uid'])
        seen.clear()
        with patch.object(e,'clean_group',side_effect=ValueError('descendants remain')),patch.object(e,'idle_uid') as audit:
            with self.assertRaisesRegex(ValueError,'descendants remain'):e.finish_candidate(child)
            audit.assert_not_called()
        self.assertEqual(seen,['reaped'])
    def recovery_fixture(self):
        original=self.work();work=self.work()
        cg=self.root/'candidate-cgroup';cg.mkdir(mode=0o700);ocg=self.root/'oracle-cgroup';ocg.mkdir(mode=0o700)
        self.recipe.update(execution_cgroup=str(cg),oracle_cgroup=str(ocg));self.write_recipe()
        h=hashlib.sha256(behavior.canonical(self.subject)).hexdigest()
        behavior.saved(original/'behavior-intent.json',{'revision':1,'state':'pending','operation_id':'original','subject':self.subject,'subject_sha256':h,'recipe_sha256':self.config['recipe_sha256'],'controller_config_sha256':behavior.digest(self.recipe['controller_config'])})
        binding={'revision':1,'receipt':str(original),'recipe_sha256':self.config['recipe_sha256'],'cgroup':str(cg),'cgroup_identity':[cg.stat().st_dev,cg.stat().st_ino],'oracle_cgroup':str(ocg),'oracle_cgroup_identity':[ocg.stat().st_dev,ocg.stat().st_ino],'job':str(self.execution/'never-rerun')}
        behavior.saved(original/'execution-binding.json',binding)
        lock=self.execution/'checker.lock';lock.write_bytes(behavior.canonical(binding));lock.chmod(0o600)
        return original,work,binding,lock
    def test_recovery_only_cleans_bound_job_and_never_emits_or_reexecutes(self):
        original,work,binding,lock=self.recovery_fixture()
        with patch.object(behavior.Execution,'configured'),patch.object(behavior.Execution,'clean') as clean,patch.object(behavior.Execution,'spawn') as spawn:
            result=behavior.recover(self.config,self.recipe_path,original,work)
            clean.assert_called_once();spawn.assert_not_called()
        self.assertEqual(result['status'],'execution_stopped_outcome_unknown');self.assertFalse(result['check_emitted']);self.assertFalse(result['automatic_rerun'])
        self.assertEqual(lock.read_bytes(),b'');self.assertFalse((work/'request.json').exists())
    def test_recovery_does_not_clean_changed_domain_or_another_job(self):
        original,work,binding,lock=self.recovery_fixture();original_bytes=lock.read_bytes()
        lock.write_bytes(b'{"some":"other-job"}')
        with patch.object(behavior.Execution,'configured'),patch.object(behavior.Execution,'clean') as clean:
            with self.assertRaisesRegex(ValueError,'does not own'):behavior.recover(self.config,self.recipe_path,original,work)
            clean.assert_not_called()
        self.assertEqual(lock.read_bytes(),b'{"some":"other-job"}')
        lock.write_bytes(original_bytes)
        with patch.object(behavior.Execution,'configured'),patch.object(behavior.Execution,'clean',side_effect=ValueError('unknown cleanup')):
            with self.assertRaisesRegex(ValueError,'unknown cleanup'):behavior.recover(self.config,self.recipe_path,original,self.work())
        self.assertEqual(lock.read_bytes(),original_bytes)
        changed=dict(binding);changed['cgroup_identity'][1]+=1
        (original/'execution-binding.json').write_bytes(behavior.canonical(changed));lock.write_bytes(behavior.canonical(changed))
        with patch.object(behavior.Execution,'configured'),patch.object(behavior.Execution,'clean') as clean:
            with self.assertRaisesRegex(ValueError,'domain identity'):behavior.recover(self.config,self.recipe_path,original,self.work())
            clean.assert_not_called()
    def test_recovery_rejects_altered_subject_intent(self):
        original,work,_,lock=self.recovery_fixture();old=lock.read_bytes()
        p=original/'behavior-intent.json';intent=json.loads(p.read_text());intent['subject']['target_revision']='other';p.write_bytes(behavior.canonical(intent))
        with self.assertRaisesRegex(ValueError,'intent binding'):behavior.recover(self.config,self.recipe_path,original,work)
        self.assertEqual(lock.read_bytes(),old)
    def test_production_path_guard_rejects_alias_and_writable_ancestor(self):
        parent=self.root/'unsafe-parent';parent.mkdir(mode=0o777);parent.chmod(0o777);child=parent/'fixed.py';child.write_text('pass');child.chmod(0o600)
        with self.assertRaisesRegex(ValueError,'ancestor'):self.real_protected(child)
        alias=self.root/'alias';alias.symlink_to(child)
        with self.assertRaisesRegex(ValueError,'ancestor'):self.real_protected(alias)
        linked=self.root/'linked';os.link(child,linked)
        with self.assertRaisesRegex(ValueError,'single-link'):self.real_protected(linked)
    def test_candidate_spawn_never_inherits_descriptors_or_credential_environment(self):
        e=behavior.Execution(self.recipe,self.config);e.workspace=self.root
        env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'}
        with patch.object(behavior.subprocess,'Popen') as spawn:
            e.spawn(['/fixed/program'],env)
            options=spawn.call_args.kwargs
        self.assertTrue(options['close_fds']);self.assertNotIn('pass_fds',options);self.assertEqual(options['env'],env)
        self.assertEqual(options['stdin'],subprocess.DEVNULL);self.assertEqual(options['preexec_fn'],e.child)
    def test_pending_transport_exact_retry_does_not_execute_again_or_refresh(self):
        config_path=self.root/'producer.json';config_path.write_bytes(producer.canonical(self.config));config_path.chmod(0o600)
        subject_path=self.root/'subject.json';subject_path.write_bytes(producer.canonical(self.subject));subject_path.chmod(0o600)
        w=self.root/'transport-original';args=types.SimpleNamespace(mode='check-behavior',config=str(config_path),receipt=str(w),input=str(subject_path),package=str(self.package),recipe=str(self.recipe_path),operation_id='check-original')
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(behavior,'Execution',FixtureExecution),patch.object(producer,'exchange',side_effect=OSError('lost reply')):
            with self.assertRaisesRegex(OSError,'lost reply'):producer.run(args)
        original=json.loads((w/'request.json').read_text());count=len(FixtureExecution.instances)
        retry=types.SimpleNamespace(mode='retry',config=str(config_path),receipt=str(self.root/'transport-retry'),original_receipt=str(w),operation_id='check-original')
        with patch.object(producer,'exchange',return_value={'status':'ok','data':{}}) as exchange:
            self.assertEqual(producer.run(retry),0);self.assertEqual(exchange.call_args.args[1],original)
        self.assertEqual(len(FixtureExecution.instances),count)

if __name__=='__main__':unittest.main()
