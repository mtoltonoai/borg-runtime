"""Producer mechanics with real bounded fixture processes, not account isolation.

Positive tests replace only the privileged cgroup/UID adapter. They never publish
production checks or claim that same-UID fixtures establish protected accounts.
"""
from pathlib import Path
import copy, hashlib, json, os, shutil, signal, subprocess, sys, time, types, unittest
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
        if p==self.root or self.root in p.parents or p==Path(behavior.__file__).absolute() or p==Path('/usr/bin/python3').resolve() or p in getattr(self,'reuse_system_readers',set()):
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
    def setup_reuse(self):
        """Real preparation/producer with pinned, explicitly controlled local peers."""
        import protected_preparation_v2 as prep
        import test_protected_preparation_v2 as preparation_tests
        import test_protected_live_v2 as live_tests
        # This worker's system binaries have a mapped owner. The same explicit
        # fixture path seam substitutes ownership, never hashes or production guards.
        self.reuse_system_readers={Path('/usr/bin/'+n).resolve() for n in ('git','gcc','ar','pkg-config')}
        self.addCleanup(lambda:sys.modules.__setitem__('package_v2',package_v2))
        base=self.case.commit
        with (self.case.repo/'src/main.rs').open('a') as f:f.write('// distinct candidate after the exact CAS predecessor\n')
        cargo=self.case.repo/'Cargo.toml';cargo.write_bytes(b'# exact candidate syntax, unchanged dependency policy\n'+cargo.read_bytes())
        self.case.commit=self.case.commit_all()
        built=self.case.build(self.root/'reuse-built');self.package=self.root/'reuse-built/borg-runtime.tar'
        manifest=package_v2.verify(self.package,built['package_sha256'],self.case.commit)
        self.subject['expected_base']=base
        self.subject['artifact']={'format':manifest['format'],'package_sha256':built['package_sha256'],
            'package_bytes':self.package.stat().st_size,'manifest_sha256':hashlib.sha256(package_v2.canonical(manifest)).hexdigest(),
            'source_commit':manifest['source']['commit'],'source_tree':manifest['source']['tree'],'object_format':manifest['source']['object_format'],
            'binary_sha256':manifest['files']['bin/borg-runtime']['sha256'],'binary_bytes':manifest['files']['bin/borg-runtime']['size'],'target':manifest['files']['bin/borg-runtime']['target']}
        self.ordinary_candidate=preparation_tests.candidate(self.subject['artifact'],self.subject['release_id'])
        profile_root=self.root/'reuse-profile';profile_root.mkdir(mode=0o700)
        self.recipe=preparation_tests.recipe(profile_root);self.recipe['revision']=4
        modules=Path(self.recipe['preparation']['path']).parent
        # Preserve production source/profile validation. Only the selected local
        # fixture launch/observer/oracle programs differ from deployment programs.
        build=r'''#!/usr/bin/python3
import hashlib,json,os,pathlib,sys
raw=pathlib.Path(sys.argv[1]).read_bytes();assert hashlib.sha256(raw).hexdigest()==sys.argv[2]
c=json.loads(raw);out=pathlib.Path(c['output']);out.mkdir()
binary=pathlib.Path(os.environ['BORG_ARTIFACT_BINARY']).read_bytes();(out/'copied.bin').write_bytes(binary)
logs=['receipt.json','manifest.json','build-invocation.json','build.log','sqlite/build-receipt.json','sqlite/compile.log','sqlite/archive.log','sqlite/identity-link.log','sqlite/identity.log','sqlite/pkg-config.log','sqlite/cargo-fixture/receipt.json','sqlite/cargo-fixture/command.log']
for name in logs:
 p=out/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('Controlled local copy/result log; no compiler or production claim.\n')
print('controlled selected build copied exact fixture executable',flush=True)
'''
        service=r'''#!/usr/bin/python3
import hashlib,json,pathlib,socket,sys
raw=pathlib.Path(sys.argv[1]).read_bytes();assert hashlib.sha256(raw).hexdigest()==sys.argv[2]
c=json.loads(raw);assert sys.argv[3]=='controller'
root=pathlib.Path(c['workspace']);state=root/'fixture-effects.json'
d=json.loads(state.read_text()) if state.exists() else {'count':0,'ops':{}}
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('127.0.0.1',c['controller_port']));s.listen()
while True:
 peer,_=s.accept()
 with peer:
  q=json.loads(peer.makefile('rb').readline(4096))
  if q['id'] in d['ops']:r=d['ops'][q['id']]
  elif q['generation']!=1:r={'status':'stale','count':d['count']}
  else:
   d['count']+=1;r={'status':'ok','count':d['count']};d['ops'][q['id']]=r;state.write_text(json.dumps(d))
  peer.sendall(json.dumps(r).encode()+b'\n')
'''
        observer=live_tests.PRELUDE+r'''
import socket
fixture=json.loads(pathlib.Path(c['commands']['controller']['arguments'][0]).read_bytes())
def request(identity,generation):
 end=time.monotonic()+2
 while True:
  try:
   with socket.create_connection(('127.0.0.1',fixture['controller_port']),timeout=.2) as s:
    s.sendall(json.dumps({'id':identity,'generation':generation}).encode()+b'\n')
    return json.loads(s.makefile('rb').readline(4096))
  except ConnectionRefusedError:
   if time.monotonic()>=end:raise
   time.sleep(.01)
h=ask('start',command='controller')['handle']
original=request('settled-original',1);assert original=={'status':'ok','count':1}
assert ask('kill_leader',handle=h)['leader_reaped'];ask('stop_all')
ask('start',command='controller')
assert request('settled-original',1)==original
assert request('stale',0)=={'status':'stale','count':1}
finish(obs=['Controlled local service restart preserved original effect; stale generation refused; not deployed Borg/provider behavior'])
'''
        oracle=r'''import hashlib,json,pathlib,sys
c=json.loads(pathlib.Path(sys.argv[1]).read_bytes());raw=pathlib.Path(c['live_evidence_path']).read_bytes();e=json.loads(raw)
assert hashlib.sha256(raw).hexdigest()==c['live_evidence_sha256']
assert e['subject_sha256']==c['subject_sha256'] and e['recipe_sha256']==c['recipe_sha256']
assert e['cleanup_verified'] and c['cleanup_verified'] and e['supervisor_transcript'][-1]['value']['cleanup_verified']
if c['check']=='selected_build':
 copied=pathlib.Path(c['execution_workspace'])/'selected-build/copied.bin'
 assert hashlib.sha256(copied.read_bytes()).hexdigest()==c['subject']['artifact']['binary_sha256']
print(json.dumps({'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':e['observer_result']['passed'],'observations':['Exact protected fixture transcript and local results after owned cleanup; not production compilation or provider qualification']}))
'''
        for name,body in [('fixed_build_launcher.py',build),('candidate_launcher.py',service),
                          ('journey_observer.py',observer),('build_oracle.py',oracle),('journey_oracle.py',oracle)]:
            path=modules/name;path.chmod(0o600);path.write_text(body);path.chmod(0o555 if 'launcher' in name else 0o444)
        pp=self.recipe['preparation']['profile']
        pp['modules']={name:behavior.digest(modules/name) for name in pp['modules']}
        limits=profile_root/'limits.json';limits.write_bytes(Path(pp['limits']['path']).read_bytes());limits.chmod(0o444)
        pp['limits']={'path':str(limits),'sha256':behavior.digest(limits)}
        # Private independent tool files allow actual toolchain-corruption tests.
        toolchain=profile_root/'toolchain';toolchain.mkdir(mode=0o755)
        for name in ('cargo','rustc','rustdoc'):
            shutil.copyfile(Path(pp['toolchain'])/name,toolchain/name);(toolchain/name).chmod(0o555)
        pp['toolchain']=str(toolchain)
        for step in self.recipe['steps']:
            step['timeout_ms']=5000;step['output_bytes']=1024*1024
            for command in step['commands'].values():command['sha256']=pp['modules'][Path(command['executable']).name]
            for role in ('observer','oracle'):step[role]['sha256']=pp['modules'][Path(step[role]['path']).name]
        installed={}
        for name,raw in [('installed_package',b'controlled installed predecessor bytes'),
                         ('installed_configuration',b'{"installed":"controlled configuration"}'),
                         ('installed_profile',b'{"limits":{"clock_uncertainty_ms":1000},"scope":"controlled local profile"}')]:
            path=profile_root/name;path.write_bytes(raw);path.chmod(0o400)
            installed[name]={'path':str(path),'sha256':behavior.digest(path)}
        self.subject['installed_package_sha256']=installed['installed_package']['sha256']
        state=profile_root/'state';state.write_bytes(b'controlled idle state\n');state.chmod(0o400)
        idle=profile_root/'idle.lock';idle.write_bytes(b'');idle.chmod(0o600)
        self.recipe['applicability']={'revision':1,**installed,'state_inputs':[{'path':str(state),'sha256':behavior.digest(state)}],'idle_locks':[str(idle)]}
        self.recipe.update(**{k:self.subject[k] for k in ('policy','deployment_target','target_revision','installed_package_sha256')},
                           execution_uid=os.geteuid()+1,execution_gid=os.getegid()+1,execution_root=str(self.execution))
        for key in ('execution_cgroup','oracle_cgroup'):
            group=profile_root/key;group.mkdir(mode=0o700)
            for name,raw in [('cgroup.events',b'populated 0\n'),('cgroup.procs',b''),('cgroup.kill',b'')]:
                (group/name).write_bytes(raw);(group/name).chmod(0o600)
            self.recipe[key]=str(group)
        (self.execution/'checker.lock').write_bytes(b'');(self.execution/'checker.lock').chmod(0o600)
        self.config['valid_for_ms']=600000
        self.write_recipe()
        self.bare=profile_root/'repository.git'
        prep.fixed_git(['clone','--bare','--no-hardlinks',str(self.case.repo),str(self.bare)],self.root,self.root)
        prep.fixed_git(['--git-dir='+str(self.bare),'update-ref',self.subject['target_ref'],base],self.root,self.root)
        self.controller_config['repository_path']=str(self.bare);self.write_controller()
        self.producer_config=self.root/'reuse-producer.json';self.producer_config.write_bytes(producer.canonical(self.config));self.producer_config.chmod(0o600)
        self.producer_input=self.root/'reuse-input.json'
        self.producer_input.write_bytes(producer.canonical({'subject':self.subject,'candidate':self.ordinary_candidate}))
        self.clock_ms=time.time_ns()//1_000_000
        self.peer_records={};self.peer_calls=[];self.actions=[];self.drop_reply=False;self.drop_before_commit=False
        self.adapter=live_tests.EvidenceExecution;self.adapter.instances=[];self.adapter.fail_cleanup=False
    def protected_peer(self,config,request):
        self.peer_calls.append(copy.deepcopy(request))
        command=request['command'];operation=command['operation']
        if operation=='view':
            return {'status':'ok','replayed':False,'data':{'candidate':copy.deepcopy(self.ordinary_candidate),'actions':copy.deepcopy(self.actions),'automatic_rollout':False}}
        if operation=='get_receipt':
            record=self.peer_records.get(command['input']['operation_id'])
            return {'status':'ok','replayed':False,'data':copy.deepcopy(record)} if record else {'status':'not_found','replayed':False,'data':None}
        if operation!='check':raise AssertionError('fixture forbids effects/authority: '+operation)
        if self.drop_before_commit:self.drop_before_commit=False;raise OSError('controlled unknown response before settlement')
        old=self.peer_records.get(request['operation_id'])
        if old:
            self.assertEqual(old['request'],request)
            return {**old['reply'],'replayed':True}
        reply={'status':'ok','replayed':False,'data':copy.deepcopy(command['input'])}
        self.peer_records[request['operation_id']]={'request':copy.deepcopy(request),'producer':copy.deepcopy(self.controller_config['producers'][0]),'reply':copy.deepcopy(reply)}
        if self.drop_reply:self.drop_reply=False;raise OSError('controlled lost response after settlement')
        return reply
    def reusable_run(self,original=None,operation_id=None,clock_ns=None):
        self.n+=1;out=self.root/('reuse-receipt-'+str(self.n))
        args=types.SimpleNamespace(mode='check-prepared' if original is None else 'attest-evidence',
            config=str(self.producer_config),input=str(self.producer_input),recipe=str(self.recipe_path),
            package=str(self.package if original is None else original/'behavior-package.snapshot'),
            receipt=str(out),operation_id=operation_id or ('execution-original' if original is None else 'attestation-'+str(self.n)),
            original_receipt=None if original is None else str(original))
        self.last_args=args;self.last_work=out
        epoch=self.clock_ms*1_000_000;tick=time.monotonic_ns()
        clock_ns=clock_ns if clock_ns is not None else lambda:epoch+time.monotonic_ns()-tick
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(behavior,'Execution',self.adapter),\
             patch.object(producer,'exchange',side_effect=self.protected_peer),patch.object(behavior.time,'time_ns',side_effect=clock_ns):
            self.assertEqual(producer.run(args),0)
        return out,json.loads((out/'request.json').read_bytes())['command']['input']
    def test_evidence_reuse_after_expiry_requires_fresh_applicability(self):
        self.setup_reuse();original,old=self.reusable_run()
        kept={name:(original/name).read_bytes() for name in ('request.json','receipt.json','execution-evidence.json','behavior-result.json','evidence-binding.json')}
        self.clock_ms=old['valid_until_ms']+1
        prepare=unittest.mock.Mock(side_effect=AssertionError('reuse regenerated job inputs'))
        load=behavior.load_preparer
        def retained_only(recipe):
            module=load(recipe);module.prepare=prepare;return module
        with patch.object(behavior,'load_preparer',side_effect=retained_only),\
             patch.object(behavior,'live_step',side_effect=AssertionError('reuse executed build/journey')) as execute,\
             patch.object(behavior,'oracle',side_effect=AssertionError('reuse ran oracle')) as oracle,\
             patch.object(self.adapter,'admit',side_effect=AssertionError('reuse admitted execution')) as admit:
            fresh,new=self.reusable_run(original)
        prepare.assert_not_called();execute.assert_not_called();oracle.assert_not_called();admit.assert_not_called()
        self.assertLess(old['valid_until_ms'],new['checked_at_ms'])
        self.assertEqual(new['valid_until_ms']-new['checked_at_ms'],600000)
        self.assertEqual(new['results'],old['results'])
        self.assertNotEqual((fresh/'request.json').read_bytes(),kept['request.json'])
        for name,raw in kept.items():self.assertEqual((original/name).read_bytes(),raw)
        a=json.loads((fresh/'applicability.json').read_bytes())
        self.assertEqual(a['executed_at_ms'],json.loads(kept['execution-evidence.json'])['executed_at_ms'])
        self.assertLessEqual(a['executed_at_ms'],old['checked_at_ms'])
        self.assertEqual(a['attested_at_ms'],new['checked_at_ms'])
        self.assertEqual(a['evidence']['sha256'],hashlib.sha256(kept['execution-evidence.json']).hexdigest())
        self.assertEqual(a['current']['actual_predecessor'],self.subject['expected_base'])
        self.assertEqual([q['command']['operation'] for q in self.peer_calls],['view','check','get_receipt','view','check'])
        binding=json.loads((fresh/'evidence-binding.json').read_bytes())
        self.assertEqual(binding['request_sha256'],hashlib.sha256((fresh/'request.json').read_bytes()).hexdigest())
        self.assertEqual(binding['operation_id'],json.loads((fresh/'request.json').read_bytes())['operation_id'])
    def test_evidence_reuse_rejects_invalid_original_or_changed_inputs(self):
        self.setup_reuse();original,_=self.reusable_run()
        evidence=json.loads((original/'execution-evidence.json').read_bytes());job=Path(evidence['binding']['job'])
        p=self.recipe['preparation']['profile'];modules=Path(self.recipe['preparation']['path']).parent
        families={
            'recipe':self.recipe_path,'producer_configuration':self.producer_config,
            'package':original/'behavior-package.snapshot','manifest':original/'package-manifest.json',
            'source_inventory_and_raw_profile':original/'behavior-source-manifest.json',
            'source_object':next((job/'prepared/source.git/objects').glob('*/*')),
            'complete_prepared_recipe':job/'prepared/build.json','fixture':job/'prepared/journey.json',
            'generated_limits':job/'prepared/controller-limits.json','limits_selection':Path(p['limits']['path']),
            'dependency_manifest':Path(p['cargo_manifest']['path']),
            'dependency_base':next(path for path in Path(p['cargo_home']).rglob('*') if path.is_file() and not path.is_symlink()),
            'dependency_archive':Path(p['archive']['path']),'toolchain':Path(p['toolchain'])/'rustc',
            'executable':job/'artifact-binary','result':original/'behavior-result.json',
            'oracle_result':original/'selected_build.oracle-result.json',
            'actual_log':original/'build-observations/0.snapshot',
            'closure':original/'execution-closure.json','ownership':original/'execution-binding.json',
            **{'code:'+name:modules/name for name in p['modules']}}
        with patch.object(behavior,'live_step',side_effect=AssertionError('invalid evidence executed')) as execute:
            for family,path in families.items():
                with self.subTest(family=family):
                    raw=path.read_bytes();mode=path.stat().st_mode&0o777
                    try:
                        path.chmod(0o600);path.write_bytes(raw+b'\nchanged')
                        with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
                        self.assertFalse((self.last_work/'request.json').exists())
                    finally:path.write_bytes(raw);path.chmod(mode)
            execute.assert_not_called()
        # A protected record with a self-consistent reference is still rejected
        # for its actual semantics, not merely because a serialization changed.
        paths=[original/n for n in ('execution-evidence.json','applicability.json','evidence-binding.json')]
        originals={path:path.read_bytes() for path in paths}
        changes={
            'unknown_version':lambda e:e.update(revision=99),
            'unknown_format':lambda e:e.update(format='foreign/1'),
            'failed':lambda e:e['execution_check']['results'].update(selected_build=False),
            'partial':lambda e:e['execution_check']['results'].pop('actual_candidate_journey'),
            'foreign_candidate':lambda e:e['candidate'].update(task_id='another-task'),
            'typed_candidate_identity':lambda e:e['candidate'].update(revision=True),
            'foreign_subject':lambda e:e['subject'].update(expected_base='0'*40),
            'foreign_receipt':lambda e:e.update(receipt=str(self.root/'other')),
            'unclosed':lambda e:e['closure'].update(owned_job_closed=False),
            'missing_provenance':lambda e:e.pop('prepared_members')}
        for name,change in changes.items():
            with self.subTest(original=name):
                try:
                    altered=copy.deepcopy(evidence);change(altered)
                    paths[0].chmod(0o600);paths[0].write_bytes(behavior.canonical(altered)+b'\n');paths[0].chmod(0o400)
                    attestation=json.loads(originals[paths[1]]);attestation['evidence']['sha256']=behavior.digest(paths[0])
                    paths[1].write_bytes(behavior.canonical(attestation)+b'\n')
                    binding=json.loads(originals[paths[2]]);binding['evidence']=attestation['evidence'];binding['attestation']['sha256']=behavior.digest(paths[1])
                    paths[2].write_bytes(behavior.canonical(binding)+b'\n')
                    with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
                    self.assertFalse((self.last_work/'request.json').exists())
                finally:
                    for path,raw in originals.items():path.chmod(0o600);path.write_bytes(raw)
                    paths[0].chmod(0o400)
        held=self.root/'held-evidence';paths[0].rename(held)
        try:
            with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
        finally:held.rename(paths[0])
        # System readers cannot be edited by this task. Inject a changed measured
        # identity at the existing read seam, while using the complete producer.
        real=behavior.member
        def changed_reader(path,*args,**kwargs):
            value=real(path,*args,**kwargs)
            return {**value,'sha256':'0'*64} if Path(path)==Path('/usr/bin/git') else value
        with patch.object(behavior,'member',side_effect=changed_reader):
            with self.assertRaisesRegex(ValueError,'input member changed'):self.reusable_run(original)
        # New-format private records cannot fall back to a legacy read when a
        # sidecar disappears, or when modes/aliases no longer protect it.
        sidecar=original/'evidence-binding.json';hold=self.root/'held-binding';sidecar.rename(hold)
        try:
            with self.assertRaisesRegex(ValueError,'incomplete original'):self.reusable_run(original)
        finally:hold.rename(sidecar)
        for name in ('request.json','applicability.json','evidence-binding.json','execution-evidence.json'):
            path=original/name;mode=path.stat().st_mode&0o777;path.chmod(0o644)
            try:
                with self.subTest(mode=name),self.assertRaisesRegex(ValueError,'private'):self.reusable_run(original)
            finally:path.chmod(mode)
        path=original/'applicability.json';hold=self.root/'held-applicability';path.rename(hold);path.symlink_to(hold)
        try:
            with self.assertRaises(ValueError):self.reusable_run(original)
        finally:path.unlink();hold.rename(path)
        peer=self.protected_peer;path=job/'prepared/journey.json';raw=path.read_bytes()
        def changed_during_view(config,request):
            response=peer(config,request)
            if request['command']['operation']=='view':
                path.chmod(0o600);path.write_bytes(raw+b'\nlate drift');path.chmod(0o444)
            return response
        self.protected_peer=changed_during_view
        try:
            with self.assertRaisesRegex(ValueError,'evidence changed during applicability'):self.reusable_run(original)
        finally:
            path.chmod(0o600);path.write_bytes(raw);path.chmod(0o444);self.protected_peer=peer
    def test_evidence_reuse_rechecks_current_target_roles_and_process_state(self):
        import fcntl
        import protected_preparation_v2 as prep
        self.setup_reuse();original,old=self.reusable_run();self.clock_ms=old['valid_until_ms']+1
        controller=Path(self.recipe['controller_config']);cc=controller.read_bytes()
        for name,change in {
            'target':lambda c:c.update(target_revision='changed'),
            'configuration':lambda c:c.update(max_check_age_ms=600001),
            'role_removed':lambda c:c.update(producers=[]),
            'role_identity':lambda c:c['producers'][0].update(id='another-checker'),
            'worker_role_overlap':lambda c:c['worker_uids'].append(self.config['producer_uid']),
            'mandatory_check':lambda c:c['required_checks'].append('new-required-check')}.items():
            with self.subTest(current=name):
                try:
                    value=json.loads(cc);change(value);controller.write_bytes(behavior.canonical(value))
                    with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
                finally:controller.write_bytes(cc)
        inp=self.producer_input.read_bytes()
        for key in ('expected_base','target_revision','deployment_target'):
            with self.subTest(subject=key):
                value=json.loads(inp);value['subject'][key]='b'*40 if key=='expected_base' else 'other-target'
                self.producer_input.write_bytes(behavior.canonical(value))
                try:
                    with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
                finally:self.producer_input.write_bytes(inp)
        prep.fixed_git(['--git-dir='+str(self.bare),'update-ref',self.subject['target_ref'],self.subject['artifact']['source_commit']],self.root,self.root)
        try:
            with self.assertRaisesRegex(ValueError,'base moved'):self.reusable_run(original)
        finally:prep.fixed_git(['--git-dir='+str(self.bare),'update-ref',self.subject['target_ref'],self.subject['expected_base']],self.root,self.root)
        for kind in ('installed_package','installed_configuration','installed_profile'):
            path=Path(self.recipe['applicability'][kind]['path']);raw=path.read_bytes();path.chmod(0o600);path.write_bytes(raw+b'changed')
            try:
                with self.subTest(installed=kind),self.assertRaises(ValueError):self.reusable_run(original)
            finally:path.write_bytes(raw);path.chmod(0o400)
        state=Path(self.recipe['applicability']['state_inputs'][0]['path']);state.chmod(0o600);raw=state.read_bytes();state.write_bytes(b'active')
        try:
            with self.assertRaises(ValueError):self.reusable_run(original)
        finally:state.write_bytes(raw);state.chmod(0o400)
        package=original/'behavior-package.snapshot';held=self.root/'unavailable-package';package.rename(held)
        try:
            with self.assertRaises((ValueError,OSError)):self.reusable_run(original)
        finally:held.rename(package)
        for state_name in ('started','unknown'):
            self.actions=[{'id':'prior-dispatch','kind':'rollout','subject':self.subject,'check_id':'original','grant_id':'separate-human-grant',
                           'state':state_name,'observation':None,'dispatch_until_ms':old['valid_until_ms']}]
            with self.subTest(dispatch=state_name),self.assertRaisesRegex(ValueError,'prior dispatch'):self.reusable_run(original)
        self.actions=[]
        claimed=copy.deepcopy(self.ordinary_candidate)
        self.ordinary_candidate['revision']=True
        try:
            with self.assertRaisesRegex(ValueError,'candidate/target state'):self.reusable_run(original)
        finally:self.ordinary_candidate=claimed
        with patch.object(self.adapter,'clean_group',side_effect=AssertionError('applicability must not clean a prior domain')) as clean:
            for key in ('execution_cgroup','oracle_cgroup'):
                events=Path(self.recipe[key])/'cgroup.events'
                for value in (b'populated 1\n',b'populated unknown\n'):
                    events.write_bytes(value)
                    try:
                        with self.subTest(group=key,value=value),self.assertRaises(ValueError):self.reusable_run(original)
                    finally:events.write_bytes(b'populated 0\n')
            for lock in [self.execution/'checker.lock',Path(self.recipe['applicability']['idle_locks'][0])]:
                lock.write_bytes(b'unfinished original identity')
                try:
                    with self.assertRaisesRegex(ValueError,'uncertain current lock'):self.reusable_run(original)
                    self.assertEqual(lock.read_bytes(),b'unfinished original identity')
                finally:lock.write_bytes(b'')
                with lock.open('r+b') as held:
                    fcntl.flock(held,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    with self.assertRaises(BlockingIOError):self.reusable_run(original)
            clean.assert_not_called()
        active=subprocess.Popen(['/usr/bin/python3','-I','-S','-B','-c','import time;time.sleep(30)'])
        owner=self.adapter.instances[0];owner.processes.append(active);owner.candidates.append(active)
        try:
            with self.assertRaisesRegex(ValueError,'identity is active'):self.reusable_run(original)
        finally:active.kill();active.wait(timeout=5);owner.processes.remove(active);owner.candidates.remove(active)
        with patch.object(self.adapter,'idle_uid',side_effect=ValueError('current process observation unknown')):
            with self.assertRaisesRegex(ValueError,'observation unknown'):self.reusable_run(original)
        self.assertFalse((self.last_work/'request.json').exists())
        t=self.clock_ms*1_000_000
        for clock in ([t,t-1_000_000],[t,t+5_000_000_000]):
            with self.subTest(clock=clock),self.assertRaisesRegex(ValueError,'clock uncertain'):
                self.reusable_run(original,clock_ns=clock)
            self.assertFalse((self.last_work/'request.json').exists())
        checks=sum(q['command']['operation']=='check' for q in self.peer_calls)
        # The whole uncertainty margin must fit before transport; no effect is
        # sent at the edge of the fresh attestation's original expiry.
        with self.assertRaisesRegex(ValueError,'expired or clock uncertain'):
            self.reusable_run(original,clock_ns=[t,t,t+600000*1_000_000])
        self.assertEqual(sum(q['command']['operation']=='check' for q in self.peer_calls),checks)
    def test_evidence_reuse_private_format_preserves_legacy_expiry(self):
        self.config['valid_for_ms']=600000;self.write_recipe()
        config=self.root/'legacy-config';config.write_bytes(producer.canonical(self.config));config.chmod(0o600)
        inp=self.root/'legacy-input';inp.write_bytes(producer.canonical(self.subject))
        original=self.root/'legacy-receipt'
        args=types.SimpleNamespace(mode='check-behavior',config=str(config),input=str(inp),recipe=str(self.recipe_path),
                                   package=str(self.package),receipt=str(original),operation_id='legacy-original')
        now=time.time_ns()//1_000_000
        with patch.object(producer,'load_behavior',return_value=behavior),patch.object(behavior,'Execution',FixtureExecution),\
             patch.object(producer,'exchange',return_value={'status':'ok'}),patch.object(behavior.time,'time_ns',return_value=now*1_000_000):
            self.assertEqual(producer.run(args),0)
        raw=(original/'request.json').read_bytes();request=json.loads(raw);check=request['command']['input']
        self.assertEqual(check['valid_until_ms'],now+600000)
        self.assertFalse((original/'execution-evidence.json').exists())
        with patch.object(behavior.time,'time_ns',return_value=(now+600001)*1_000_000),\
             patch.object(behavior,'live_step',side_effect=AssertionError('legacy evidence executed')) as execute:
            with self.assertRaisesRegex(ValueError,'legacy recipe'):
                behavior.attest_evidence(self.config,self.subject,None,original/'behavior-package.snapshot',self.recipe_path,original,self.work(),'new-attestation')
            execute.assert_not_called()
        retry=types.SimpleNamespace(mode='retry',config=str(config),receipt=str(self.root/'legacy-retry'),original_receipt=str(original),operation_id='legacy-original')
        with patch.object(producer,'exchange',return_value={'status':'stale_check','data':None,'replayed':False}) as exchange:
            self.assertEqual(producer.run(retry),2)
            self.assertEqual(exchange.call_args.args[1],request)
        self.assertEqual((original/'request.json').read_bytes(),raw)
        self.assertEqual(json.loads((self.root/'legacy-retry/request.json').read_bytes())['command']['input']['checked_at_ms'],now)
        behavior.saved(original/'evidence-binding.json',{'format':'unknown-private/99','revision':99})
        with patch.object(producer,'load_behavior',return_value=behavior):
            with self.assertRaisesRegex(ValueError,'fields/version'):producer.bound_original(original)
        self.recipe['revision']=99;self.write_recipe()
        with self.assertRaisesRegex(ValueError,'fields/revision'):self.run_check()
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
