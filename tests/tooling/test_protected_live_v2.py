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
