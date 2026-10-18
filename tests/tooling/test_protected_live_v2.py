"""Real local observer/process control; fixture service is NOT a Borg release oracle.

Only the privileged UID/cgroup adapter is replaced. No accounts, cgroups,
provider calls or production protected Check are installed by these tests.
"""
from pathlib import Path
import hashlib, json, os, signal, subprocess, sys, time, unittest
from unittest.mock import patch
import protected_behavior_v2 as behavior
import protected_release_v2 as producer
import build_observer as fixed_build_observer
import test_protected_behavior_v2 as old_tests

class LiveExecution(old_tests.FixtureExecution):
    def __init__(self,*args):
        super().__init__(*args);self.candidates=[];self.oracles=[]
    def spawn(self,*args):
        p=super().spawn(*args);self.candidates.append(p);return p
    def spawn_observer(self,argv,work):
        p=subprocess.Popen(argv,cwd=work,env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'},stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=True,start_new_session=True)
        self.processes.append(p);self.oracles.append(p);return p
    def spawn_oracle(self,*args):
        p=super().spawn_oracle(*args);self.oracles.append(p);return p
    def clean_group(self,group):
        for p in self.candidates if group==self.cg else self.oracles:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            p.wait(timeout=5)
        if self.fail_cleanup:raise ValueError('fixture cleanup unknown')
    def clean(self):
        self.clean_group(self.cg);self.clean_group(self.ocg)
    def idle_uid(self):pass

class EvidenceExecution(LiveExecution):
    """Real local locks/children; synthetic cgroup files and no UID isolation."""
    configured=lambda self:None
    populated=behavior.Execution.populated
    current=behavior.Execution.current
    release=behavior.Execution.release
    def __init__(self,*args):
        super().__init__(*args)
        self.cg=Path(self.recipe['execution_cgroup']);self.ocg=Path(self.recipe['oracle_cgroup'])
    def idle_uid(self):
        if any(p.poll() is None for e in self.instances for p in e.candidates):
            raise ValueError('controlled execution identity is active')
    def admit(self,work,operation_id,subject_hash):
        import fcntl
        root=Path(self.recipe['execution_root'])
        self.lock=(root/'checker.lock').open('r+b')
        fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if self.lock.read():raise ValueError('unfinished fixture job')
        self.idle_uid()
        history=root/'operations';history.mkdir(exist_ok=True,mode=0o700)
        behavior.saved(history/(hashlib.sha256(operation_id.encode()).hexdigest()+'.json'),
                       {'operation_id':operation_id,'subject_sha256':subject_hash,'recipe_sha256':self.config['recipe_sha256'],'receipt':str(work)})
        self.job=root/('job-'+work.name);self.job.mkdir(mode=0o755)
        self.workspace=self.job/'workspace';self.workspace.mkdir(mode=0o700)
        self.binding={'revision':1,'receipt':str(work),'recipe_sha256':self.config['recipe_sha256'],
                      'cgroup':str(self.cg),'cgroup_identity':[self.cg.stat().st_dev,self.cg.stat().st_ino],
                      'oracle_cgroup':str(self.ocg),'oracle_cgroup_identity':[self.ocg.stat().st_dev,self.ocg.stat().st_ino],
                      'job':str(self.job)}
        self.lock.write(behavior.canonical(self.binding));self.lock.flush();os.fsync(self.lock.fileno())
        self.owns=True;behavior.saved(work/'execution-binding.json',self.binding)

PRELUDE=r"""import hashlib,json,pathlib,sys,time
c=json.loads(pathlib.Path(sys.argv[1]).read_text());seq=0
assert c['cleanup_verified'] is False
assert c['subject_sha256']==hashlib.sha256(json.dumps(c['subject'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
def ask(op,**fields):
 global seq
 seq+=1;print(json.dumps({'sequence':seq,'operation':op,**fields}),flush=True)
 r=json.loads(sys.stdin.readline());assert r['sequence']==seq;return r['result']
def finish(passed=True,obs=None):
 r=ask('finish',result={'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':passed,'observations':obs or ['Fixed protected fixture observer; no production behavior claim']})
 assert r['cleanup_verified'] is False and r['check_emitted'] is False
"""
DEFAULT=PRELUDE+r"""
h=ask('start',command='service')['handle']
while ask('poll',handle=h)['exit'] is None:time.sleep(.01)
finish()
"""

class LiveObserverTests(unittest.TestCase):
    setUp=old_tests.BehaviorProducerTests.setUp
    fixture_protected=old_tests.BehaviorProducerTests.fixture_protected
    write_recipe=old_tests.BehaviorProducerTests.write_recipe
    write_controller=old_tests.BehaviorProducerTests.write_controller
    work=old_tests.BehaviorProducerTests.work
    setup_reuse=old_tests.BehaviorProducerTests.setup_reuse
    reusable_run=old_tests.BehaviorProducerTests.reusable_run
    protected_peer=old_tests.BehaviorProducerTests.protected_peer
    def test_evidence_reuse_preserves_execution_time_and_fresh_attestation_end_to_end(self):
        self.setup_reuse();original,old=self.reusable_run()
        evidence=json.loads((original/'execution-evidence.json').read_bytes())
        job=Path(evidence['binding']['job']);prepared=job/'prepared';workspace=job/'workspace'
        journey=(prepared/'journey.json').read_bytes()
        projection=json.loads((prepared/'cargo-projection.json').read_bytes())
        source=json.loads((original/'behavior-source-manifest.json').read_bytes())
        self.assertEqual(source['subject'],self.subject);self.assertEqual(source['candidate'],self.ordinary_candidate)
        self.assertEqual(source['source_files'],evidence['manifest']['source']['files'])
        self.assertEqual(source['cargo_profile'],evidence['manifest']['profile'])
        self.assertNotEqual(projection['source_manifest_sha256'],projection['cache_baseline_manifest_sha256'])
        self.assertEqual(projection['source_manifest_sha256'],source['cargo_profile']['cargo_manifest_sha256'])
        self.assertEqual(projection['dependency_policy'],source['cargo_profile']['dependency_policy'])
        self.assertEqual(evidence['manifest']['source']['commit'],self.subject['artifact']['source_commit'])
        self.assertEqual(evidence['manifest']['source']['tree'],self.subject['artifact']['source_tree'])
        self.assertEqual(evidence['recipe']['preparation']['profile']['modules'],source['modules'])
        for check in ('selected_build','actual_candidate_journey'):
            live=json.loads((original/(check+'.live-evidence.json')).read_bytes())
            result=json.loads((original/(check+'.oracle-result.json')).read_bytes())
            self.assertEqual(live['subject'],self.subject);self.assertTrue(result['passed'])
            self.assertEqual(live['execution_binding_sha256'],behavior.digest(original/'execution-binding.json'))
            self.assertTrue(live['supervisor_transcript'][-1]['value']['cleanup_verified'])
            self.assertTrue(live['observer_result']['passed'])
            launches=[v['value'] for v in live['supervisor_transcript'] if v['kind']=='supervisor_launch']
            self.assertEqual(len(launches),1 if check=='selected_build' else 2)
            for launch in launches:self.assertEqual(launch['artifact_binary_sha256'],self.subject['artifact']['binary_sha256'])
            for output in live['outputs'].values():
                self.assertEqual(behavior.digest(output['path']),output['sha256'])
        # The controlled local service actually settled once across restart and
        # rejected a stale owner; this is not a deployed/provider behavior claim.
        state=json.loads((workspace/'fixture-effects.json').read_bytes())
        self.assertEqual(state,{'count':1,'ops':{'settled-original':{'status':'ok','count':1}}})
        self.assertTrue(all(p.poll() is not None for e in self.adapter.instances for p in e.processes))
        self.assertTrue(evidence['closure']['owned_job_closed'])
        for entries in (evidence['declared_inputs'],evidence['preparation']['prepared_inputs']):
            self.assertFalse(any(str(workspace) in name for name in entries))
        logs=json.loads((original/'build-observations.json').read_bytes())
        for row in logs['files'].values():
            self.assertEqual(behavior.digest(row['snapshot']),row['sha256'])
            self.assertEqual(Path(row['snapshot']).stat().st_mode&0o777,0o400)
        kept=(original/'execution-evidence.json').read_bytes()
        # Mutating disposable job output/cache cannot mutate protected snapshots.
        (workspace/'selected-build/build.log').write_text('mutable job output is not retained evidence')
        (Path(projection['destination'])/'job-cache-change').write_bytes(b'mutable')
        self.clock_ms=old['valid_until_ms']+5000
        with patch.object(behavior,'live_step',side_effect=AssertionError('second execution')) as execute:
            fresh,new=self.reusable_run(original)
            execute.assert_not_called()
        attestation=json.loads((fresh/'applicability.json').read_bytes())
        binding=json.loads((fresh/'evidence-binding.json').read_bytes())
        self.assertEqual((original/'execution-evidence.json').read_bytes(),kept)
        self.assertEqual((prepared/'journey.json').read_bytes(),journey)
        self.assertEqual(attestation['executed_at_ms'],evidence['executed_at_ms'])
        self.assertGreater(attestation['attested_at_ms'],old['valid_until_ms'])
        self.assertEqual(new['checked_at_ms'],attestation['attested_at_ms'])
        self.assertEqual(new['valid_until_ms']-new['checked_at_ms'],600000)
        self.assertEqual(binding['evidence'],attestation['evidence'])
        self.assertEqual(binding['request_sha256'],behavior.digest(fresh/'request.json'))
        for row in logs['files'].values():self.assertEqual(behavior.digest(row['snapshot']),row['sha256'])
    def setup_live(self,body=DEFAULT):
        self.observer=self.root/'observer.py';self.observer.write_text(body);self.observer.chmod(0o600)
        self.oracle.write_text(r"""import hashlib,json,pathlib,sys
c=json.loads(pathlib.Path(sys.argv[1]).read_text());p=pathlib.Path(c['live_evidence_path']);raw=p.read_bytes();e=json.loads(raw)
assert hashlib.sha256(raw).hexdigest()==c['live_evidence_sha256']
assert e['subject_sha256']==c['subject_sha256'] and e['recipe_sha256']==c['recipe_sha256']
assert c['cleanup_verified'] and e['cleanup_verified']
assert e['supervisor_transcript'][-1]['kind']=='supervisor_terminal'
assert e['supervisor_transcript'][-1]['value']['cleanup_verified']
print(json.dumps({'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':True,'observations':['Protected transcript exact subject plus measured terminal cleanup']}))
""")
        old=self.recipe['steps'][0]
        self.recipe['revision']=2
        self.recipe['steps']=[{'check':old['check'],'commands':{'service':old.get('candidate',old.get('commands',{}).get('service'))},'observer':{'path':str(self.observer),'sha256':behavior.digest(self.observer)},'oracle':{'path':str(self.oracle),'sha256':behavior.digest(self.oracle)},'timeout_ms':3000,'output_bytes':65536}]
        self.write_recipe();LiveExecution.instances=[];LiveExecution.fail_cleanup=False
    def run_live(self,work=None):
        work=work or self.work()
        with patch.object(behavior,'Execution',LiveExecution):return behavior.behavior_check(self.config,self.subject,self.package,self.recipe_path,work,'check-original')
    def observer_body(self,body):
        self.observer.write_text(body);self.recipe['steps'][0]['observer']['sha256']=behavior.digest(self.observer);self.write_recipe()
    def test_live_observer_then_measured_cleanup_then_exact_post_oracle(self):
        self.setup_live();w=self.work();r=self.run_live(w)
        self.assertTrue(r['results']['fixture_observation'])
        e=json.loads((w/'fixture_observation.live-evidence.json').read_text())
        self.assertEqual(e['subject'],self.subject);self.assertTrue(e['cleanup_verified'])
        launches=[x['value'] for x in e['supervisor_transcript'] if x['kind']=='supervisor_launch']
        self.assertEqual(len(launches),1);self.assertEqual(launches[0]['executable_sha256'],behavior.digest(Path(sys.executable).resolve()))
        self.assertTrue(all(p.poll() is not None for p in LiveExecution.instances[-1].processes))
    def test_fixed_build_observer_uses_supervisor_budget_and_cleanup_oracle(self):
        body=Path(fixed_build_observer.__file__).read_text()
        self.setup_live(body)
        step=self.recipe['steps'][0];step['commands']={'build':step['commands']['service']};step['timeout_ms']=1500
        self.write_recipe();w=self.work();started=time.monotonic_ns()
        # A context-supplied larger deadline cannot replace the supervisor value.
        original=behavior.live_step
        def forged_context(step,context,*args):
            return original(step,{**context,'live_control':{'deadline_monotonic_ns':10**30,'message_limit':10**9}},*args)
        with patch.object(behavior,'live_step',forged_context):r=self.run_live(w)
        self.assertTrue(r['results']['fixture_observation'])
        live=json.loads((w/'fixture_observation.live-input.json').read_text())
        self.assertEqual(live['live_control']['message_limit'],behavior.MAX_LIVE_MESSAGES)
        self.assertGreater(live['live_control']['deadline_monotonic_ns'],started)
        self.assertLessEqual(live['live_control']['deadline_monotonic_ns'],time.monotonic_ns()+1500000000)
        e=json.loads((w/'fixture_observation.live-evidence.json').read_text())
        messages=[x['value'] for x in e['supervisor_transcript'] if x['kind']=='observer_request']
        self.assertLessEqual(len(messages),behavior.MAX_LIVE_MESSAGES);self.assertTrue(e['cleanup_verified'])
        self.assertTrue(all(p.poll() is not None for p in LiveExecution.instances[-1].processes))
    def test_real_fixture_journey_restart_replay_and_stale_owner(self):
        self.candidate.write_text(r"""import json,pathlib,socket
root=pathlib.Path('.');state=root/'state.json'
d=json.loads(state.read_text()) if state.exists() else {'count':0,'generation':1,'ops':{}}
s=socket.socket();s.bind(('127.0.0.1',0));s.listen();(root/'port').write_text(str(s.getsockname()[1]))
while True:
 c,_=s.accept()
 with c:
  q=json.loads(c.makefile('rb').readline(4096));op=q['id']
  if op in d['ops']:r=d['ops'][op]
  elif q['generation']!=d['generation']:r={'status':'stale','count':d['count']}
  else:
   d['count']+=1;r={'status':'ok','count':d['count']};d['ops'][op]=r;state.write_text(json.dumps(d))
  c.sendall(json.dumps(r).encode()+b'\n')
""")
        body=PRELUDE+r"""
import socket
root=pathlib.Path(c['execution_workspace'])
def port():
 for _ in range(100):
  if (root/'port').exists():return int((root/'port').read_text())
  time.sleep(.01)
 raise AssertionError('no fixture server')
def request(p,identity,generation):
 with socket.create_connection(('127.0.0.1',p),timeout=1) as s:
  s.sendall(json.dumps({'id':identity,'generation':generation}).encode()+b'\n')
  return json.loads(s.makefile('rb').readline(4096))
h=ask('start',command='service')['handle'];p=port()
a=request(p,'settled-original',1);assert a=={'status':'ok','count':1}
r=ask('kill_leader',handle=h);assert r['leader_reaped'] and not r['descendant_cleanup_verified']
assert ask('stop_all')['candidate_group_clean'];(root/'port').unlink()
h=ask('start',command='service')['handle'];p=port()
assert request(p,'settled-original',1)==a
assert request(p,'stale',0)=={'status':'stale','count':1}
finish(obs=['Actual controlled fixture protocol response before restart','Same effect ID returned same original outcome after process restart','Stale generation refused; no extra effect; not Borg conformance'])
"""
        self.setup_live(body);w=self.work();r=self.run_live(w);self.assertTrue(r['results']['fixture_observation'])
        e=json.loads((w/'fixture_observation.live-evidence.json').read_text());self.assertEqual(e['supervisor_transcript'][-1]['value']['candidate_starts'],2)
    def test_build_output_is_compared_to_exact_candidate_not_installed_label(self):
        self.candidate.write_text("import os,pathlib\np=pathlib.Path(os.environ['BORG_ARTIFACT_BINARY']);pathlib.Path('built.bin').write_bytes(p.read_bytes())\n")
        body=PRELUDE+r"""
h=ask('start',command='service')['handle']
while ask('poll',handle=h)['exit'] is None:time.sleep(.01)
ask('stop_all')
b=pathlib.Path(c['execution_workspace'])/'built.bin'
finish(hashlib.sha256(b.read_bytes()).hexdigest()==c['subject']['artifact']['binary_sha256'],['Observed fixture copy exact candidate binary association, not actual compilation'])
"""
        self.setup_live(body);self.assertTrue(self.run_live()['results']['fixture_observation'])
        self.candidate.write_text("import pathlib\npathlib.Path('built.bin').write_bytes(b'old-installed-binary')\n")
        self.assertFalse(self.run_live()['results']['fixture_observation'])
    def test_candidate_forged_channel_and_positive_json_cannot_be_observer(self):
        self.candidate.write_text("print('{\"sequence\":1,\"operation\":\"finish\",\"passed\":true}')\n")
        self.setup_live(DEFAULT.replace('finish()','finish(False)'));w=self.work();r=self.run_live(w)
        self.assertFalse(r['results']['fixture_observation']);self.assertTrue((w/'fixture_observation.candidate-1.output').exists())
    def test_undeclared_command_argv_pid_and_sequence_refuse(self):
        for message in [{'sequence':1,'operation':'start','command':'unknown'},{'sequence':1,'operation':'start','command':'service','argv':['/bin/sh']},{'sequence':1,'operation':'kill_leader','pid':1},{'sequence':True,'operation':'stop_all'},{'sequence':2,'operation':'stop_all'}]:
            self.setup_live('import json\nprint('+repr(json.dumps(message))+',flush=True)\n');w=self.work()
            with self.assertRaises(ValueError):self.run_live(w)
            self.assertFalse((w/'behavior-result.json').exists())
    def test_wrong_subject_terminal_and_empty_execution_refuse(self):
        self.setup_live(PRELUDE+'finish()\n')
        with self.assertRaisesRegex(ValueError,'missing observable'):self.run_live()
        self.observer_body(DEFAULT.replace("finish()","c['subject_sha256']='wrong';finish()"))
        with self.assertRaisesRegex(ValueError,'subject differs'):self.run_live()
    def test_observer_eof_timeout_and_malformed_refuse_and_reap(self):
        for body,expected in [(PRELUDE+"ask('start',command='service')\n",ValueError),(PRELUDE+"ask('start',command='service');time.sleep(10)\n",TimeoutError),("print('not-json',flush=True)\n",ValueError)]:
            self.candidate.write_text('import time\ntime.sleep(10)\n');self.setup_live(body);self.recipe['steps'][0]['timeout_ms']=300;self.write_recipe();w=self.work()
            with self.assertRaises(expected):self.run_live(w)
            self.assertFalse((w/'behavior-result.json').exists());self.assertTrue(all(p.poll() is not None for p in LiveExecution.instances[-1].processes))
    def test_failed_group_cleanup_prevents_oracle_and_result(self):
        self.setup_live();LiveExecution.fail_cleanup=True;w=self.work()
        try:
            with self.assertRaisesRegex(ValueError,'cleanup unknown'):self.run_live(w)
        finally:LiveExecution.fail_cleanup=False
        self.assertFalse((w/'fixture_observation.oracle-result.json').exists());self.assertFalse((w/'behavior-result.json').exists())
    def test_terminal_later_command_or_garbage_refuses(self):
        for tail in ["ask('start',command='service')","print('garbage',flush=True)"]:
            self.setup_live(DEFAULT+'\n'+tail+'\n');w=self.work()
            with self.assertRaises(ValueError):self.run_live(w)
            self.assertFalse((w/'behavior-result.json').exists())
    def test_aggregate_output_and_launch_bound(self):
        self.candidate.write_text('print("x"*100000)\n');self.setup_live();self.recipe['steps'][0]['output_bytes']=4096;self.write_recipe()
        with self.assertRaisesRegex(ValueError,'output bound'):self.run_live()
        self.candidate.write_text('pass\n');self.setup_live(PRELUDE+"\nfor _ in range(17):ask('start',command='service')\nfinish()\n")
        with self.assertRaisesRegex(ValueError,'launch bound'):self.run_live()
    def test_observer_pin_and_recipe_revision_are_explicit(self):
        self.setup_live();self.observer.write_text('print("forged")\n')
        with self.assertRaisesRegex(ValueError,'observer identity'):self.run_live()
        self.setup_live();self.recipe['revision']=1;self.write_recipe()
        with self.assertRaisesRegex(ValueError,'step fields'):self.run_live()
    def test_observer_channel_is_private_and_candidate_has_no_descriptor_or_env(self):
        self.setup_live();e=behavior.Execution(self.recipe,self.config);e.workspace=self.root
        with patch.object(behavior.subprocess,'Popen') as spawn:
            e.spawn(['fixed'],{'PATH':'/usr/bin:/bin'});kwargs=spawn.call_args.kwargs
            self.assertIs(kwargs['stdin'],subprocess.DEVNULL);self.assertTrue(kwargs['close_fds']);self.assertNotIn('pass_fds',kwargs)
            e.spawn_observer(['fixed'],self.root);kwargs=spawn.call_args.kwargs
            self.assertIs(kwargs['stdin'],subprocess.PIPE);self.assertTrue(kwargs['close_fds']);self.assertEqual(set(kwargs['env']),{'PATH','LANG','HOME'})

    def test_unknown_owned_handle_and_nonzero_observer_exit_do_not_qualify(self):
        self.setup_live(PRELUDE+"ask('poll',handle=999)\n")
        with self.assertRaisesRegex(ValueError,'unknown owned'):self.run_live()
        self.observer_body(DEFAULT+'\nsys.exit(4)\n')
        with self.assertRaisesRegex(ValueError,'failed'):self.run_live()
    def test_frame_and_message_bounds(self):
        self.setup_live("print('x'*20000,flush=True)\n")
        with self.assertRaisesRegex(ValueError,'frame bound'):self.run_live()
        self.observer_body(PRELUDE+"\nfor _ in range(129):ask('stop_all')\n")
        with self.assertRaisesRegex(ValueError,'message bound'):self.run_live()
    def test_exited_owned_leader_is_not_signaled_again(self):
        self.setup_live(PRELUDE+"h=ask('start',command='service')['handle']\nwhile ask('poll',handle=h)['exit'] is None:time.sleep(.01)\nr=ask('kill_leader',handle=h);assert r['leader_reaped'] and r['exit']==0\nfinish()\n")
        original=subprocess.Popen.kill
        def reject_reaped(child):
            if child.poll() is not None:raise AssertionError('attempt to signal reused PID')
            return original(child)
        with patch.object(subprocess.Popen,'kill',reject_reaped):self.assertTrue(self.run_live()['results']['fixture_observation'])
    def test_live_false_cannot_be_overridden_and_exact_source_is_retained(self):
        self.setup_live(DEFAULT.replace('finish()','finish(False)'));w=self.work();r=self.run_live(w)
        self.assertFalse(r['results']['fixture_observation'])
        e=json.loads((w/'fixture_observation.live-evidence.json').read_text())
        self.assertEqual(e['subject']['artifact']['source_commit'],self.subject['artifact']['source_commit'])
        self.assertEqual(e['execution_binding_sha256'],behavior.digest(w/'execution-binding.json'))

    def test_selector_failure_cannot_leave_an_untracked_observer(self):
        self.setup_live()
        with patch.object(behavior.selectors,'DefaultSelector',side_effect=OSError('no selector capacity')):
            with self.assertRaisesRegex(OSError,'selector capacity'):self.run_live()
        self.assertEqual(LiveExecution.instances[-1].processes,[])
