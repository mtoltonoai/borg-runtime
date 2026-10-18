#!/usr/bin/env python3
"""Fixed protected-recipe supervisor; it does not select or activate a release policy.

Only installed, protected oracle programs can interpret observations as evidence.
Candidate commands run under the configured unprivileged execution identity and
in its preconfigured exclusive cgroup. They never receive producer descriptors.
"""
from __future__ import annotations
import contextlib, copy, ctypes, fcntl, hashlib, json, os, re, selectors, signal, stat, subprocess, time
from pathlib import Path

MAX_RECIPE=128*1024
MAX_OUTPUT=4*1024*1024
MAX_STEPS=32
MAX_LIVE_COMMANDS=8
MAX_LIVE_STARTS=16
MAX_LIVE_MESSAGES=128
MAX_LIVE_FRAME=16384
EVIDENCE_FORMAT='borg-protected-execution/1'
ATTESTATION_FORMAT='borg-protected-applicability/1'
ATTESTATION_MS=600000
MAX_EVIDENCE=16*1024*1024
MAX_MEMBERS=40000
MAX_MEMBER_BYTES=512*1024*1024
# These are append-only transport records, not part of the original execution.
TRANSPORT_MEMBERS={'execution-evidence.json','applicability.json','evidence-binding.json',
                   'request.json','receipt.json','applicability-view.json','reconciled-originals.json'}

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()

def protected(path, directory=False):
    p=Path(path)
    if not p.is_absolute() or '..' in p.parts:raise ValueError('protected path is not absolute/canonical')
    for q in (p,*p.parents):
        m=q.lstat()
        if stat.S_ISLNK(m.st_mode) or m.st_uid not in {0,os.geteuid()} or m.st_mode&0o022:
            raise ValueError('unprotected owner/mode/ancestor: '+str(q))
        if q!=p or directory:
            if not stat.S_ISDIR(m.st_mode):raise ValueError('protected directory required')
        elif not stat.S_ISREG(m.st_mode) or m.st_nlink!=1:raise ValueError('protected regular single-link file required')
    return p

def read_json(path, limit=MAX_RECIPE):
    import protected_release_v2 as producer
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        m=os.fstat(f.fileno())
        if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_size>limit:raise ValueError('bounded JSON input type/size')
        raw=f.read(limit+1)
    if len(raw)>limit:raise ValueError('bounded JSON input exceeded')
    return producer.strict(raw),raw

def saved(path,value):
    import protected_release_v2 as producer
    raw=canonical(value)+b'\n'
    if len(raw)>MAX_EVIDENCE:raise ValueError('protected record byte bound')
    with producer.private_create(path) as f:f.write(raw);f.flush();os.fsync(f.fileno())
    producer.sync_directory(Path(path).parent)

def integer(value,low,high):return type(value) is int and low<=value<=high

def load_recipe(path,config,subject):
    p=protected(path);r,raw=read_json(p)
    if hashlib.sha256(raw).hexdigest()!=config['recipe_sha256']:raise ValueError('recipe differs from selected digest')
    fields={'revision','supervisor_sha256','controller_config','execution_uid','execution_gid','execution_root','execution_cgroup','oracle_cgroup','policy','deployment_target','target_revision','installed_package_sha256','required_checks','steps'}
    if r.get('revision') in {3,4}:fields.add('preparation')
    if r.get('revision')==4:fields.add('applicability')
    if set(r)!=fields or type(r['revision']) is not int or r['revision'] not in {1,2,3,4}:raise ValueError('behavior recipe fields/revision')
    if r['supervisor_sha256']!=digest(protected(Path(__file__).absolute())):raise ValueError('supervisor source identity changed')
    if not integer(r['execution_uid'],1,2**31-1) or not integer(r['execution_gid'],1,2**31-1) or r['execution_uid'] in {config['producer_uid'],config['server_uid']}:
        raise ValueError('candidate identity overlaps protected authority')
    for k in ('policy','deployment_target','target_revision','installed_package_sha256'):
        if r[k]!=subject[k]:raise ValueError('behavior recipe exact target/policy mismatch')
    if r['policy']!=config['policy']:raise ValueError('producer policy mismatch')
    protected(r['execution_root'],directory=True)
    if r['execution_cgroup']==r['oracle_cgroup']:raise ValueError('candidate and oracle cleanup domains overlap')
    required=r['required_checks'];steps=r['steps']
    if not isinstance(required,list) or not 1<=len(required)<=MAX_STEPS or not all(isinstance(x,str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',x) for x in required) or len(set(required))!=len(required) or 'package_integrity' in required:raise ValueError('mandatory check inventory')
    if not isinstance(steps,list) or not 1<=len(steps)<=MAX_STEPS:raise ValueError('step count bound')
    actual=[]
    for s in steps:
        live=r['revision'] in {2,3,4} and 'observer' in s
        fields={'check','commands','observer','oracle','timeout_ms','output_bytes'} if live else {'check','candidate','oracle','timeout_ms','output_bytes'}
        if set(s)!=fields or s['check'] not in required:raise ValueError('step fields/check')
        if not integer(s['timeout_ms'],1,3600000) or not integer(s['output_bytes'],1,MAX_OUTPUT):raise ValueError('step resource bound')
        commands=s['commands'] if live else {'candidate':s['candidate']}
        if not isinstance(commands,dict) or not 1<=len(commands)<=MAX_LIVE_COMMANDS:raise ValueError('fixed command inventory bound')
        for name,c in commands.items():
            if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',name):raise ValueError('fixed command name')
            if not isinstance(c,dict) or set(c)!={'executable','sha256','arguments'}:raise ValueError('candidate fields')
            if c['executable']!='{artifact_binary}':
                q=protected(c['executable'])
                if digest(q)!=c['sha256']:raise ValueError('candidate launcher identity changed')
            elif c['sha256']!=subject['artifact']['binary_sha256']:raise ValueError('candidate executable subject mismatch')
            if not isinstance(c['arguments'],list) or len(c['arguments'])>64 or not all(isinstance(x,str) and len(x)<=4096 and '\x00' not in x for x in c['arguments']):raise ValueError('argument bound')
        for role in ('oracle','observer') if live else ('oracle',):
            o=s[role]
            if not isinstance(o,dict) or set(o)!={'path','sha256'}:raise ValueError('protected observer/oracle fields')
            q=protected(o['path'])
            if digest(q)!=o['sha256']:raise ValueError('protected '+role+' identity changed')
        actual.append(s['check'])
    if len(set(actual))!=len(actual) or set(actual)!=set(required):raise ValueError('missing/duplicate mandatory oracle')
    # Read the same protected controller role configuration, rather than trusting
    # the producer's own UID to describe every other protected role.
    cc,cc_raw=read_json(protected(r['controller_config']))
    if cc.get('socket')!=config['socket'] or cc.get('recipe_sha256')!=config['recipe_sha256']:raise ValueError('controller socket/recipe binding differs')
    for k in ('policy','repository','target_ref','deployment_target','target_revision','installed_package_sha256'):
        if cc.get(k)!=subject[k]:raise ValueError('controller exact subject binding differs')
    roles=cc.get('producers');workers=cc.get('worker_uids');checks=cc.get('required_checks')
    if not isinstance(roles,list) or not 1<=len(roles)<=1024 or not all(isinstance(x,dict) and set(x)=={'id','uid','role'} and integer(x['uid'],0,2**32-1) and x['role'] in {'checker','repository','human','rollout'} for x in roles):raise ValueError('controller producer inventory')
    ids=[x['uid'] for x in roles]
    role_ids=[(x['uid'],x['role']) for x in roles]
    names=[x['id'] for x in roles]
    if not all(isinstance(x,str) and x for x in names) or len(set(names))!=len(names) or len(set(role_ids))!=len(role_ids) or not isinstance(workers,list) or not all(integer(x,0,2**32-1) for x in workers) or any(x in workers for x in ids):raise ValueError('controller worker/producer identities overlap')
    if r['execution_uid'] not in workers or r['execution_uid'] in ids:raise ValueError('execution identity is not an unprivileged configured worker')
    if not any(x['uid']==config['producer_uid'] and x['role']=='checker' for x in roles):raise ValueError('producer is not the configured checker')
    if not isinstance(checks,list) or not checks or len(set(checks))!=len(checks) or set(checks)!=(set(required)|{'package_integrity'}):raise ValueError('mandatory checks differ from controller configuration')
    if not integer(cc.get('max_check_age_ms'),1,86400000) or config['valid_for_ms']>cc['max_check_age_ms']:raise ValueError('checker freshness exceeds controller bound')
    r['_controller_sha256']=hashlib.sha256(cc_raw).hexdigest()
    if r['revision'] in {3,4}:load_preparer(r).validate_profile(r['preparation'],r,protected)
    if r['revision']==4:
        # This private successor covers the complete selected prepared recipe.
        # Legacy recipes and an enlarged mandatory set need their own execution.
        if required!=['selected_build','actual_candidate_journey'] or config['valid_for_ms']!=ATTESTATION_MS:
            raise ValueError('reusable recipe requires exact checks and 600000ms applicability')
        applicability_inputs(r,subject)
    return r


def load_preparer(recipe):
    import importlib.util
    p=recipe['preparation']
    if not isinstance(p,dict) or set(p)!={'path','sha256','profile'}:raise ValueError('preparation shape')
    path=protected(p['path'])
    if path.name!='protected_preparation_v2.py' or digest(path)!=p['sha256']:raise ValueError('fixed preparer changed')
    spec=importlib.util.spec_from_file_location('protected_preparation_v2',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

class Execution:
    """Deployment adapter. No account, cgroup or permission provisioning here."""
    def __init__(self,recipe,config):
        self.recipe=recipe;self.config=config;self.cg=Path(recipe['execution_cgroup']);self.ocg=Path(recipe['oracle_cgroup']);self.lock=None;self.owns=False
    def configured(self):
        if os.geteuid()!=0 or self.config['producer_uid']!=0:
            raise ValueError('configured protected supervisor launch authority unavailable')
        for group in (self.cg,self.ocg):
            if not str(group).startswith('/sys/fs/cgroup/'):raise ValueError('existing cgroup-v2 path required')
            protected(group,directory=True)
            for n in ('cgroup.procs','cgroup.events','cgroup.kill'):protected(group/n)
        if self.cg in self.ocg.parents or self.ocg in self.cg.parents:raise ValueError('cleanup cgroups overlap')
    @contextlib.contextmanager
    def current(self):
        """Observe idle domains under the existing lock; never clean historical work."""
        self.configured()
        path=protected(Path(self.recipe['execution_root'])/'checker.lock')
        with idle_lock(path) as lock:
            if self.populated(self.cg) or self.populated(self.ocg):raise ValueError('current checker domains are active')
            self.idle_uid()
            groups={}
            for name,group in [('cgroup',self.cg),('oracle_cgroup',self.ocg)]:
                m=group.lstat();groups[name]={'path':str(group),'identity':[m.st_dev,m.st_ino]}
            yield {'lock':lock,'groups':groups,'execution_uid_idle':True}
            # A historical cleanup result never stands in for this observation.
            if self.populated(self.cg) or self.populated(self.ocg):raise ValueError('current checker domains became active')
            self.idle_uid()
    def admit(self,work,operation_id,subject_hash):
        self.configured()
        # A delegated writable ancestor would let the candidate escape cleanup.
        root=protected(self.recipe['execution_root'],directory=True)
        lockpath=root/'checker.lock'
        fd=os.open(lockpath,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        m=os.fstat(fd)
        if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_uid!=0 or m.st_mode&0o077:os.close(fd);raise ValueError('checker lock identity/mode')
        self.lock=os.fdopen(fd,'r+b');fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        prior=self.lock.read(MAX_RECIPE+1)
        if prior:raise ValueError('unfinished checker job; explicit recovery required')
        if self.populated(self.cg) or self.populated(self.ocg):raise ValueError('checker cgroups are not initially empty')
        self.idle_uid()
        history=root/'operations'
        if not history.exists():history.mkdir(mode=0o700)
        protected(history,directory=True)
        for index,_ in enumerate(history.iterdir()):
            if index>=10000:raise ValueError('retained checker operation capacity')
        marker=history/(hashlib.sha256(operation_id.encode()).hexdigest()+'.json')
        # An interrupted original job is never reexecuted merely because it did
        # not reach controller submission. Lookup/retry use its retained request.
        saved(marker,{'operation_id':operation_id,'subject_sha256':subject_hash,'recipe_sha256':self.config['recipe_sha256'],'receipt':str(work)})
        self.job=root/('job-'+work.name)
        self.job.mkdir(mode=0o755)
        self.workspace=self.job/'workspace';self.workspace.mkdir(mode=0o700)
        os.chown(self.workspace,self.recipe['execution_uid'],self.recipe['execution_gid'])
        self.binding={'revision':1,'receipt':str(work),'recipe_sha256':self.config['recipe_sha256'],'cgroup':str(self.cg),'cgroup_identity':[self.cg.stat().st_dev,self.cg.stat().st_ino],'oracle_cgroup':str(self.ocg),'oracle_cgroup_identity':[self.ocg.stat().st_dev,self.ocg.stat().st_ino],'job':str(self.job)}
        self.lock.write(canonical(self.binding));self.lock.flush();os.fsync(self.lock.fileno())
        self.owns=True
        saved(work/'execution-binding.json',self.binding)
    def idle_uid(self):
        # Refuse unrelated execution under this configured account; never kill
        # arbitrary same-UID processes outside the recorded cleanup domains.
        count=0
        with os.scandir('/proc') as entries:
            for entry in entries:
                if not entry.name.isdecimal():continue
                count+=1
                if count>65536:raise ValueError('process inventory bound')
                try:
                    with open('/proc/'+entry.name+'/status','rb') as f:raw=f.read(16385)
                except FileNotFoundError:continue
                if len(raw)>16384:raise ValueError('process status bound')
                lines=[line for line in raw.splitlines() if line.startswith(b'Uid:')]
                if len(lines)!=1:raise ValueError('process identity unavailable')
                if self.recipe['execution_uid'] in [int(n) for n in lines[0].split()[1:]]:raise ValueError('configured execution identity is already active outside cleaned domain')
    def populated(self,group):
        values=dict(x.split() for x in (group/'cgroup.events').read_text().splitlines())
        if values.get('populated') not in {'0','1'}:raise ValueError('unknown cgroup population')
        return values['populated']=='1'
    def bound_group(self,group):
        key='cgroup' if group==self.cg else 'oracle_cgroup' if group==self.ocg else None
        m=group.lstat()
        if key is None or not stat.S_ISDIR(m.st_mode) or self.binding[key]!=str(group) or self.binding[key+'_identity']!=[m.st_dev,m.st_ino]:raise ValueError('recorded cleanup domain changed')
    def child(self):
        # Only this trusted pre-exec function runs privileged. No shell/startup
        # script/candidate instruction runs before cgroup entry and identity drop.
        self.bound_group(self.cg)
        with (self.cg/'cgroup.procs').open('w') as f:f.write(str(os.getpid()))
        os.setgroups([]);os.setgid(self.recipe['execution_gid']);os.setuid(self.recipe['execution_uid'])
        libc=ctypes.CDLL(None,use_errno=True)
        if libc.prctl(38,1,0,0,0)!=0:raise OSError(ctypes.get_errno(),'no_new_privs failed')
        if os.geteuid()!=self.recipe['execution_uid'] or os.getegid()!=self.recipe['execution_gid']:raise ValueError('candidate identity drop failed')
    def spawn(self,argv,env):
        return subprocess.Popen(argv,cwd=self.workspace,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,close_fds=True,start_new_session=True,preexec_fn=self.child)
    def oracle_child(self):
        self.bound_group(self.ocg)
        with (self.ocg/'cgroup.procs').open('w') as f:f.write(str(os.getpid()))
    def spawn_oracle(self,argv,work):
        return subprocess.Popen(argv,cwd=work,env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'},stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,close_fds=True,start_new_session=True,preexec_fn=self.oracle_child)
    def spawn_observer(self,argv,work):
        # This private pipe belongs only to the pinned protected observer. All
        # candidate spawns close inherited FDs and have stdin=/dev/null.
        return subprocess.Popen(argv,cwd=work,env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'},stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=True,start_new_session=True,preexec_fn=self.oracle_child)
    def clean_group(self,group):
        if not self.owns:raise ValueError('cleanup lacks recorded job ownership')
        self.bound_group(group)
        with (group/'cgroup.kill').open('w') as f:f.write('1')
        end=time.monotonic()+5
        while self.populated(group):
            if time.monotonic()>=end:raise ValueError('candidate descendants remain; cleanup unknown')
            time.sleep(.02)
    def finish_candidate(self,process):
        try:self.clean_group(self.cg);self.clean_group(self.ocg)
        finally:process.wait(timeout=5)
        # A killed direct child may remain an owned zombie until wait reaps it.
        # Only audit residual execution-UID processes after that mandatory reap.
        self.idle_uid()
    def clean(self):
        self.clean_group(self.cg);self.clean_group(self.ocg);self.idle_uid()
    def release(self,clean):
        if self.lock:
            if clean and self.owns:self.lock.seek(0);self.lock.truncate();self.lock.flush();os.fsync(self.lock.fileno())
            self.lock.close();self.lock=None

def collect(process,cap,timeout_ms):
    output=bytearray();deadline=time.monotonic()+timeout_ms/1000
    selector=selectors.DefaultSelector();selector.register(process.stdout,selectors.EVENT_READ)
    try:
        while selector.get_map():
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('candidate/oracle deadline exceeded')
            for key,_ in selector.select(min(remaining,.1)):
                b=os.read(key.fd,min(65536,cap-len(output)+1))
                if not b:selector.unregister(key.fileobj);continue
                output.extend(b)
                if len(output)>cap:raise ValueError('candidate/oracle output bound exceeded')
        process.wait(timeout=max(.001,deadline-time.monotonic()))
        return process.returncode,bytes(output)
    finally:selector.close();process.stdout.close()

def oracle(step,context,work,execution):
    """Installed oracle reads private parent observations, never candidate JSON."""
    import protected_release_v2 as producer
    path=protected(step['oracle']['path'])
    if digest(path)!=step['oracle']['sha256']:raise ValueError('oracle changed before execution')
    inp=work/(step['check']+'.oracle-input.json');saved(inp,context)
    # The oracle is deliberately a protected checker program. Its reviewed source
    # must not execute candidate commands itself or give them checker authority.
    argv=['/usr/bin/python3','-I','-S','-B',str(path),str(inp)]
    p=execution.spawn_oracle(argv,work)
    try:code,raw=collect(p,MAX_RECIPE,step['timeout_ms'])
    finally:
        try:execution.clean_group(execution.ocg)
        finally:p.wait(timeout=5)
    if digest(path)!=step['oracle']['sha256']:raise ValueError('oracle changed during execution')
    if code!=0:raise ValueError('protected oracle failed')
    result=producer.strict(raw)
    if set(result)!={'revision','subject_sha256','recipe_sha256','check','passed','observations'} or result['revision']!=1 or result['subject_sha256']!=context['subject_sha256'] or result['recipe_sha256']!=context['recipe_sha256'] or result['check']!=step['check'] or type(result['passed']) is not bool:
        raise ValueError('oracle result exact-subject association differs')
    obs=result['observations']
    if not isinstance(obs,list) or not 1<=len(obs)<=128 or not all(isinstance(x,str) and 0<len(x)<=1024 for x in obs):raise ValueError('oracle lacks bounded observable acceptance')
    saved(work/(step['check']+'.oracle-result.json'),result)
    return result['passed']

def candidate_argv(command,executable,workspace,subject):
    argv=[str(executable) if command['executable']=='{artifact_binary}' else command['executable']]
    replacements={'{artifact_binary}':str(executable),'{workspace}':str(workspace),'{source_commit}':subject['artifact']['source_commit'],'{source_tree}':subject['artifact']['source_tree']}
    for value in command['arguments']:
        for a,b in replacements.items():value=value.replace(a,b)
        if '{' in value or '}' in value:raise ValueError('unknown recipe substitution')
        argv.append(value)
    expected=subject['artifact']['binary_sha256'] if command['executable']=='{artifact_binary}' else command['sha256']
    if digest(protected(argv[0]))!=expected:raise ValueError('candidate executable changed before launch')
    return argv,expected

def process_identity(process):
    # Inspect only this still-owned child, never an observer-supplied PID.
    try:
        with open('/proc/'+str(process.pid)+'/stat','rb') as f:raw=f.read(8193)
        if len(raw)>8192:raise ValueError('child identity bound')
        fields=raw.rsplit(b') ',1)[1].split();start=int(fields[19])
        image=digest('/proc/'+str(process.pid)+'/exe')
        return {'pid':process.pid,'start_ticks':start,'image_sha256':image,'observed':True}
    except (FileNotFoundError,ProcessLookupError):
        return {'pid':process.pid,'observed':False,'reason':'child exited before image observation'}


def live_step(step,context,work,execution,executable):
    """Closed local observer channel, not an executable/argv/target API.

    Observer assertions stay distinct from supervisor facts. No success is
    returned before both owned cleanup groups and every direct child are reaped.
    """
    import protected_release_v2 as producer
    path=protected(step['observer']['path'])
    if digest(path)!=step['observer']['sha256']:raise ValueError('observer changed before execution')
    # The protected supervisor owns this deadline and channel allowance. The
    # candidate cannot reset them, and observer startup consumes the same budget.
    deadline_ns=time.monotonic_ns()+step['timeout_ms']*1000000
    inp=work/(step['check']+'.live-input.json')
    saved(inp,{**context,'commands':step['commands'],'execution_workspace':str(execution.workspace),'artifact_binary':str(executable),'live_control':{'deadline_monotonic_ns':deadline_ns,'message_limit':MAX_LIVE_MESSAGES}})
    selector=selectors.DefaultSelector()
    try:observer=execution.spawn_observer(['/usr/bin/python3','-I','-S','-B',str(path),str(inp)],work)
    except BaseException:
        selector.close();raise
    children=[];outputs={};transcript=[];pending=bytearray();total=0;finished=None;clean=False
    end=deadline_ns/1000000000
    def record(kind,value):
        row={'kind':kind,'value':value}
        if sum(len(canonical(x)) for x in transcript)+len(canonical(row))>MAX_OUTPUT:raise ValueError('live transcript bound exceeded')
        transcript.append(row)
    def register(stream,kind):
        os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,kind)
    def cleanup_candidates():
        error=None
        try:execution.clean_group(execution.cg)
        except BaseException as e:error=e
        for child in children:
            try:child.wait(timeout=5)
            except BaseException as e:error=error or e
        if error:raise error
        execution.idle_uid()
        record('supervisor_cleanup',{'candidate_group_clean':True,'direct_children_reaped':True})
    def reply(sequence,value):
        raw=canonical({'sequence':sequence,'result':value})+b'\n'
        if len(raw)>4096:raise ValueError('live response bound')
        # The protected observer must consume each response; never block on it.
        if os.write(observer.stdin.fileno(),raw)!=len(raw):raise ValueError('live observer response incomplete')
        record('supervisor_response',{'sequence':sequence,'result':value})
    try:
        register(observer.stdout,'observer');register(observer.stderr,'observer_stderr');os.set_blocking(observer.stdin.fileno(),False)
        sequence=0
        while selector.get_map():
            left=end-time.monotonic()
            if left<=0:raise TimeoutError('live observer deadline exceeded')
            for key,_ in selector.select(min(left,.05)):
                raw=os.read(key.fd,min(65536,step['output_bytes']-total+1))
                if not raw:
                    selector.unregister(key.fileobj);continue
                total+=len(raw)
                if total>step['output_bytes']:raise ValueError('live aggregate output bound exceeded')
                kind=key.data
                if kind!='observer':
                    outputs.setdefault(kind,bytearray()).extend(raw);continue
                pending.extend(raw)
                while b'\n' in pending:
                    line,_,tail=pending.partition(b'\n');pending=bytearray(tail)
                    if len(line)>MAX_LIVE_FRAME:raise ValueError('live observer frame bound')
                    if finished is not None:raise ValueError('observer action after terminal result')
                    message=producer.strict(line);sequence+=1
                    if sequence>MAX_LIVE_MESSAGES:raise ValueError('live observer message bound')
                    if not isinstance(message,dict) or not integer(message.get('sequence'),1,MAX_LIVE_MESSAGES) or message['sequence']!=sequence:raise ValueError('live observer sequence differs')
                    op=message.get('operation');record('observer_request',message)
                    if op=='start' and set(message)=={'sequence','operation','command'}:
                        name=message['command']
                        if name not in step['commands'] or len(children)>=MAX_LIVE_STARTS:raise ValueError('undeclared command or launch bound')
                        argv,expected=candidate_argv(step['commands'][name],executable,execution.workspace,context['subject'])
                        env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':str(execution.workspace),'BORG_ARTIFACT_BINARY':str(executable)}
                        child=execution.spawn(argv,env);children.append(child);handle=len(children)
                        register(child.stdout,'candidate-'+str(handle))
                        record('supervisor_launch',{'handle':handle,'process_identity':process_identity(child),'pid':child.pid,'command':name,'argv':argv,'executable_sha256':expected,'artifact_binary_sha256':context['subject']['artifact']['binary_sha256']})
                        reply(sequence,{'handle':handle,'pid':child.pid,'descendant_cleanup_verified':False})
                    elif op in {'poll','kill_leader'} and set(message)=={'sequence','operation','handle'}:
                        handle=message['handle']
                        if not integer(handle,1,len(children)):raise ValueError('unknown owned process handle')
                        child=children[handle-1];code=child.poll()
                        if op=='kill_leader' and code is None:
                            # An owned unreaped child cannot have its PID reused.
                            child.kill();code=child.wait(timeout=min(5,max(.001,end-time.monotonic())))
                        reply(sequence,{'handle':handle,'exit':code,'leader_reaped':code is not None,'descendant_cleanup_verified':False})
                    elif op=='stop_all' and set(message)=={'sequence','operation'}:
                        cleanup_candidates();reply(sequence,{'candidate_group_clean':True,'direct_children_reaped':True})
                    elif op=='finish' and set(message)=={'sequence','operation','result'}:
                        result=message['result']
                        if not isinstance(result,dict) or set(result)!={'revision','subject_sha256','recipe_sha256','check','passed','observations'} or result['revision']!=1 or any(result[k]!=context[k] for k in ('subject_sha256','recipe_sha256','check')) or type(result['passed']) is not bool:raise ValueError('live result subject differs')
                        obs=result['observations']
                        if not children or not isinstance(obs,list) or not 1<=len(obs)<=128 or not all(isinstance(x,str) and 0<len(x)<=1024 for x in obs):raise ValueError('live result missing observable cases')
                        finished=result;reply(sequence,{'recorded':True,'cleanup_verified':False,'check_emitted':False})
                    else:raise ValueError('unsupported live observer request')
                if len(pending)>MAX_LIVE_FRAME:raise ValueError('live observer frame bound')
            # A long-running service need not exit on its own after observer done.
            if finished is not None and observer.poll() is not None:break
            if observer.poll() is not None and finished is None:raise ValueError('observer exited without terminal result')
        if pending or finished is None or observer.wait(timeout=max(.001,end-time.monotonic()))!=0:raise ValueError('observer incomplete or failed')
        cleanup_candidates()
        execution.clean_group(execution.ocg);observer.wait(timeout=5)
        # Capture all bytes left in killed/reaped process pipes; a terminal
        # observer cannot hide a later command by exiting before its pipe drains.
        while selector.get_map():
            for key,_ in selector.select(.05):
                raw=os.read(key.fd,min(65536,step['output_bytes']-total+1))
                if not raw:selector.unregister(key.fileobj);continue
                total+=len(raw)
                if total>step['output_bytes']:raise ValueError('live aggregate output bound exceeded')
                if key.data=='observer':raise ValueError('observer bytes after terminal result')
                outputs.setdefault(key.data,bytearray()).extend(raw)
            if time.monotonic()>end+10:raise TimeoutError('live pipe cleanup deadline exceeded')
        clean=True
        if digest(path)!=step['observer']['sha256']:raise ValueError('observer changed during execution')
        if digest(executable)!=context['subject']['artifact']['binary_sha256']:raise ValueError('artifact changed during live qualification')
        record('supervisor_terminal',{'cleanup_verified':True,'observer_exit':0,'candidate_starts':len(children)})
        output_records={}
        for name,raw in outputs.items():
            dest=work/(step['check']+'.'+name+'.output')
            with producer.private_create(dest) as f:f.write(raw);f.flush();os.fsync(f.fileno())
            output_records[name]={'path':str(dest),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
        data={**context,'observer_sha256':step['observer']['sha256'],'observer_result':finished,'supervisor_transcript':transcript,'outputs':output_records,'cleanup_verified':True}
        dest=work/(step['check']+'.live-evidence.json');saved(dest,data)
        return {'live_evidence_path':str(dest),'live_evidence_sha256':digest(dest),'live_observer_passed':finished['passed']}
    finally:
        # Every owned leader is reaped even if a group check or observer fails.
        try:
            if not clean:
                error=None
                for group in (execution.cg,execution.ocg):
                    try:execution.clean_group(group)
                    except BaseException as e:error=error or e
                for child in [*children,observer]:
                    try:child.wait(timeout=5)
                    except BaseException as e:error=error or e
                if error:raise error
                execution.idle_uid()
        finally:
            selector.close()
            for stream in [observer.stdin,observer.stdout,observer.stderr,*[c.stdout for c in children]]:
                if stream and not stream.closed:stream.close()


def fields(value,keys,label):
    if not isinstance(value,dict) or set(value)!=set(keys.split()):raise ValueError(label+' fields/version')

def exact(left,right):
    # Python container equality equates True, 1 and 1.0; exact JSON inputs do not.
    return canonical(left)==canonical(right)

def member(path,cap=MAX_MEMBER_BYTES,seal=False):
    """One protected no-follow read, bounded before and during hashing."""
    path=protected(path)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        before=os.fstat(f.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>cap:raise ValueError('evidence member type/bound')
        h=hashlib.sha256();total=0
        while True:
            raw=f.read(min(1024*1024,cap-total+1))
            if not raw:break
            total+=len(raw)
            if total>cap:raise ValueError('evidence member byte bound')
            h.update(raw)
        if seal:os.fsync(f.fileno())
        after=os.fstat(f.fileno())
        identity=lambda m:(m.st_dev,m.st_ino,m.st_mode,m.st_uid,m.st_gid,m.st_nlink,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
        if identity(before)!=identity(after) or identity(after)!=identity(path.lstat()) or total!=before.st_size:
            raise ValueError('evidence member changed while reading')
    return {'sha256':h.hexdigest(),'bytes':total,'mode':stat.S_IMODE(before.st_mode),'uid':before.st_uid,'gid':before.st_gid}

def inventory(root,exclude=(),seal=False):
    """Exact bounded membership, including empty directories; no mutable workspace."""
    import protected_release_v2 as producer
    root=protected(root,directory=True);pending=[root];m=root.lstat()
    result={'.':{'directory':True,'mode':stat.S_IMODE(m.st_mode),'uid':m.st_uid,'gid':m.st_gid}};total=0
    while pending:
        directory=pending.pop();protected(directory,directory=True)
        with os.scandir(directory) as entries:
            for entry in entries:
                path=Path(entry.path);name=path.relative_to(root).as_posix()
                if name in exclude:continue
                if len(result)>=MAX_MEMBERS:raise ValueError('evidence inventory entry bound')
                m=entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(m.st_mode):
                    protected(path,directory=True);pending.append(path)
                    result[name]={'directory':True,'mode':stat.S_IMODE(m.st_mode),'uid':m.st_uid,'gid':m.st_gid}
                else:
                    result[name]=member(path,seal=seal);total+=result[name]['bytes']
                    if total>2*1024*1024*1024:raise ValueError('evidence inventory byte bound')
        if seal:producer.sync_directory(directory)
    return dict(sorted(result.items()))

def pin(value):
    fields(value,'path sha256','protected input pin')
    if not isinstance(value['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',value['sha256']):raise ValueError('protected input digest')
    record=member(value['path'])
    if record['sha256']!=value['sha256']:raise ValueError('declared protected input changed')
    return record

def applicability_inputs(recipe,subject):
    a=recipe['applicability']
    fields(a,'revision installed_package installed_configuration installed_profile state_inputs idle_locks','applicability recipe')
    if type(a['revision']) is not int or a['revision']!=1:raise ValueError('unknown applicability recipe version')
    if not isinstance(a['state_inputs'],list) or len(a['state_inputs'])>MAX_STEPS or not isinstance(a['idle_locks'],list) or len(a['idle_locks'])>MAX_STEPS:
        raise ValueError('applicability inventory bound')
    pins=[a[k] for k in ('installed_package','installed_configuration','installed_profile')]+a['state_inputs']
    records={}
    for value in pins:
        record=pin(value)
        if value['path'] in records:raise ValueError('duplicate applicability input')
        records[value['path']]=record
    if a['installed_package']['sha256']!=subject['installed_package_sha256']:raise ValueError('installed predecessor package differs')
    if len(set(a['idle_locks']))!=len(a['idle_locks']):raise ValueError('duplicate applicability lock')
    for path in a['idle_locks']:protected(path)
    # Consume the already selected per-effect uncertainty allowance. This does
    # not install a profile, change that allowance or create another lifetime.
    profile,_=read_json(a['installed_profile']['path'],MAX_EVIDENCE)
    uncertainty=profile.get('limits',{}).get('clock_uncertainty_ms')
    if not integer(uncertainty,0,ATTESTATION_MS-1):raise ValueError('selected clock uncertainty unavailable')
    return records,uncertainty

@contextlib.contextmanager
def idle_lock(path):
    path=protected(path);fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'r+b') as f:
        m=os.fstat(f.fileno())
        if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_uid!=os.geteuid() or m.st_mode&0o077:
            raise ValueError('current lock owner/mode')
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if f.read(1):raise ValueError('unfinished or uncertain current lock; original reconciliation required')
        identity={'path':str(path),'identity':[m.st_dev,m.st_ino]}
        yield identity
        current=path.lstat()
        f.seek(0)
        if [current.st_dev,current.st_ino]!=identity['identity'] or f.read(1):raise ValueError('current lock changed')

def declared_inputs(recipe,config,recipe_path):
    import protected_release_v2 as producer
    p=recipe['preparation']['profile'];root=Path(recipe['preparation']['path']).parent
    paths={str(protected(recipe_path)),recipe['controller_config'],str(Path(__file__).absolute()),str(Path(producer.__file__).absolute())}
    paths.update(str(root/name) for name in p['modules'])
    paths.update(p[name]['path'] for name in ('archive','limits','cargo_manifest'))
    paths.update(str(Path(p['toolchain'])/name) for name in ('cargo','rustc','rustdoc'))
    paths.update(str(Path('/usr/bin/'+name).resolve(strict=True)) for name in ('python3','git','gcc','ar','pkg-config'))
    result={path:member(path) for path in sorted(paths)}
    if result[str(Path(producer.__file__).absolute())]['sha256']!=p['modules']['protected_release_v2.py']:
        raise ValueError('executing producer differs from complete helper inventory')
    if result[str(protected(recipe_path))]['sha256']!=config['recipe_sha256'] or result[recipe['controller_config']]['sha256']!=recipe['_controller_sha256']:
        raise ValueError('selected recipe/controller changed')
    return result

def prepared_associations(recipe,subject,candidate,work,binding,manifest,prepared,steps,seal=False):
    preparer=load_preparer(recipe)
    fields(prepared,'revision subject_sha256 candidate_sha256 recipe_sha256 prepared_inputs module_root modules source_manifest_sha256 source_files local_controlled_peers_only protected_check_emitted human_rollout_authority','prepared evidence')
    if (type(prepared['revision']) is not int or prepared['revision']!=1
        or prepared['subject_sha256']!=hashlib.sha256(canonical(subject)).hexdigest()
        or prepared['candidate_sha256']!=hashlib.sha256(canonical(candidate)).hexdigest()
        or prepared['recipe_sha256']!=digest(recipe_path_from_work(work))
        or prepared['modules']!=recipe['preparation']['profile']['modules']
        or prepared['module_root']!=str(Path(recipe['preparation']['path']).parent)
        or prepared['local_controlled_peers_only'] is not True or prepared['protected_check_emitted'] is not False
        or prepared['human_rollout_authority'] is not False):
        raise ValueError('prepared evidence exact association differs')
    root=Path(binding['job'])/'prepared'
    members=inventory(root,seal=seal)
    files={str(root/name):row['sha256'] for name,row in members.items() if 'sha256' in row}
    if files!=prepared['prepared_inputs']:raise ValueError('retained prepared input membership differs')
    # Verify the original members, never call prepare() or choose fresh ports,
    # timestamps, paths, cache projections or seed data.
    preparer.verify_prepared(recipe,prepared,work,protected)
    source,_=read_json(work/'behavior-source-manifest.json',MAX_EVIDENCE)
    fields(source,'subject candidate source_files cargo_profile modules','source provenance')
    if not exact(source,{'subject':subject,'candidate':candidate,'source_files':manifest['source']['files'],
                         'cargo_profile':manifest['profile'],'modules':prepared['modules']}) or prepared['source_files']!=len(source['source_files']):
        raise ValueError('source inventory/candidate build profile differs')
    package=preparer.load_package(Path(prepared['module_root']),recipe['preparation']['profile'],protected)
    package.validate_package_profile(manifest['profile'],manifest['source'],allow_legacy=False)
    replacements={'{prepared_build}':str(root/'build.json'),'{prepared_build_sha256}':digest(root/'build.json'),
                  '{prepared_journey}':str(root/'journey.json'),'{prepared_journey_sha256}':digest(root/'journey.json')}
    expected=copy.deepcopy(recipe['steps'])
    for step in expected:
        for command in step['commands'].values():
            command['arguments']=[replacements.get(arg,arg) for arg in command['arguments']]
    if not exact(steps,expected):raise ValueError('complete resolved recipe differs')
    return members

def recipe_path_from_work(work):
    return Path(work)/'behavior-recipe.json'

def retain_build_logs(work,execution):
    """Copy the fixed builder's bounded results after its owned processes stop.

    Candidate-written files remain observations. Only the preceding protected
    oracle decides the result; the mutable build/cache is never reused as proof.
    """
    import protected_release_v2 as producer
    root=execution.workspace/'selected-build';sqlite=root/'sqlite'
    paths=['receipt.json','manifest.json','build-invocation.json','build.log',
           'sqlite/build-receipt.json','sqlite/compile.log','sqlite/archive.log',
           'sqlite/identity-link.log','sqlite/identity.log','sqlite/pkg-config.log']
    cargos=[]
    with os.scandir(sqlite) as entries:
        for index,entry in enumerate(entries):
            if index>=MAX_MEMBERS:raise ValueError('build observation inventory bound')
            if entry.name.startswith('cargo-'):
                if not entry.is_dir(follow_symlinks=False):raise ValueError('build observation directory alias')
                cargos.append(entry.name)
    if len(cargos)!=1:raise ValueError('original build Cargo result missing/ambiguous')
    paths.extend('sqlite/'+cargos[0]+'/'+name for name in ('receipt.json','command.log'))
    dest=work/'build-observations';dest.mkdir(mode=0o700);records={}
    for index,name in enumerate(paths):
        path=root/name
        for parent in (path,*path.parents):
            if parent==execution.job:break
            if parent.is_symlink():raise ValueError('build observation alias')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        target=dest/(str(index)+'.snapshot')
        with os.fdopen(fd,'rb') as source,producer.private_create(target) as output:
            before=os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>MAX_OUTPUT:raise ValueError('build observation type/bound')
            raw=source.read(MAX_OUTPUT+1)
            if len(raw)!=before.st_size or len(raw)>MAX_OUTPUT:raise ValueError('build observation changed/bound')
            output.write(raw);output.flush();os.fsync(output.fileno())
        target.chmod(0o400)
        records[name]={'original_path':str(path),'snapshot':str(target),'sha256':digest(target),'bytes':len(raw)}
    saved(work/'build-observations.json',{'revision':1,'execution_binding_sha256':digest(work/'execution-binding.json'),
                                         'candidate_outputs_only':True,'files':records})
    producer.sync_directory(dest)

def retain_execution(config,subject,candidate,recipe,recipe_path,work,operation_id,manifest,prepared,steps,started,check):
    """Called only after successful execution and release of its original lock."""
    fields(check,'subject recipe_sha256 checked_at_ms valid_until_ms results cleanup_verified','execution check')
    if check['cleanup_verified'] is not True or set(check['results'])!={'package_integrity',*recipe['required_checks']} or any(v is not True for v in check['results'].values()):
        raise ValueError('failed or partial execution is not reusable')
    binding,_=read_json(protected(work/'execution-binding.json'))
    marker=Path(recipe['execution_root'])/'operations'/(hashlib.sha256(operation_id.encode()).hexdigest()+'.json')
    closure={'revision':1,'operation_id':operation_id,'binding_sha256':digest(work/'execution-binding.json'),
             'operation_marker_sha256':member(marker)['sha256'],'cleanup_verified':True,'owned_job_closed':True}
    saved(work/'execution-closure.json',closure)
    prepared_members=prepared_associations(recipe,subject,candidate,work,binding,manifest,prepared,steps,seal=True)
    for path in work.iterdir():
        if path.name not in TRANSPORT_MEMBERS:
            protected(path,directory=path.is_dir())
            if not path.is_dir():path.chmod(0o400)
    members=inventory(work,TRANSPORT_MEMBERS,seal=True)
    evidence={'format':EVIDENCE_FORMAT,'revision':1,'receipt':str(work),'operation_id':operation_id,
              'subject':subject,'candidate':candidate,'recipe':recipe,'producer_config':config,
              'started_at_ms':started,'executed_at_ms':check['checked_at_ms'],'execution_check':check,
              'manifest':manifest,'preparation':prepared,'steps':steps,'binding':binding,'closure':closure,
              'executable':member(Path(binding['job'])/'artifact-binary',seal=True),
              'members':members,'prepared_members':prepared_members,
              'declared_inputs':declared_inputs(recipe,config,recipe_path)}
    saved(work/'execution-evidence.json',evidence)
    (work/'execution-evidence.json').chmod(0o400)
    member(work/'execution-evidence.json',seal=True)
    return evidence

def verify_execution(config,subject,candidate,recipe,recipe_path,original):
    evidence,_=read_json(protected(original/'execution-evidence.json'),MAX_EVIDENCE)
    fields(evidence,'format revision receipt operation_id subject candidate recipe producer_config started_at_ms executed_at_ms execution_check manifest preparation steps binding closure executable members prepared_members declared_inputs','execution evidence')
    if evidence['format']!=EVIDENCE_FORMAT or type(evidence['revision']) is not int or evidence['revision']!=1 or recipe['revision']!=4:
        raise ValueError('legacy or unknown execution evidence is not reusable')
    if not exact([evidence[k] for k in ('receipt','subject','candidate','recipe','producer_config')],
                 [str(original),subject,candidate,recipe,config]):
        raise ValueError('foreign evidence or exact input/recipe scope changed')
    if not integer(evidence['started_at_ms'],1,2**63-1) or not integer(evidence['executed_at_ms'],evidence['started_at_ms'],2**63-1):
        raise ValueError('execution timestamps absent or invalid')
    check=evidence['execution_check']
    expected={'subject':subject,'recipe_sha256':config['recipe_sha256'],'checked_at_ms':evidence['executed_at_ms'],
              'valid_until_ms':evidence['executed_at_ms']+ATTESTATION_MS,
              'results':{'package_integrity':True,**{k:True for k in recipe['required_checks']}},'cleanup_verified':True}
    if canonical(check)!=canonical(expected):raise ValueError('failed/partial original execution')
    if inventory(original,TRANSPORT_MEMBERS)!=evidence['members'] or declared_inputs(recipe,config,recipe_path)!=evidence['declared_inputs']:
        raise ValueError('retained evidence or declared input member changed')
    binding,_=read_json(protected(original/'execution-binding.json'))
    fields(binding,'revision receipt recipe_sha256 cgroup cgroup_identity oracle_cgroup oracle_cgroup_identity job','execution binding')
    if (binding!=evidence['binding'] or binding['revision']!=1 or binding['receipt']!=str(original)
        or binding['recipe_sha256']!=config['recipe_sha256'] or Path(binding['job']).parent!=Path(recipe['execution_root'])
        or binding['cgroup']!=recipe['execution_cgroup'] or binding['oracle_cgroup']!=recipe['oracle_cgroup']):
        raise ValueError('foreign or incomplete execution binding')
    closure,_=read_json(protected(original/'execution-closure.json'))
    marker=Path(recipe['execution_root'])/'operations'/(hashlib.sha256(evidence['operation_id'].encode()).hexdigest()+'.json')
    marker_value,_=read_json(protected(marker))
    if marker_value!={'operation_id':evidence['operation_id'],'subject_sha256':hashlib.sha256(canonical(subject)).hexdigest(),
                      'recipe_sha256':config['recipe_sha256'],'receipt':str(original)}:raise ValueError('original operation marker differs')
    expected_closure={'revision':1,'operation_id':evidence['operation_id'],'binding_sha256':digest(original/'execution-binding.json'),
                      'operation_marker_sha256':member(marker)['sha256'],'cleanup_verified':True,'owned_job_closed':True}
    if canonical(closure)!=canonical(expected_closure) or closure!=evidence['closure']:raise ValueError('original owned execution is not closed')
    intent,_=read_json(protected(original/'behavior-intent.json'))
    result,_=read_json(protected(original/'behavior-result.json'))
    if (intent.get('operation_id')!=evidence['operation_id'] or intent.get('subject')!=subject
        or intent.get('recipe_sha256')!=config['recipe_sha256'] or intent.get('state')!='pending'
        or result.get('state')!='observed' or result.get('check')!=check or result.get('mandatory_checks_present') is not True):
        raise ValueError('original execution result/intent differs')
    import protected_release_v2 as producer
    mod=producer.load_verifier();manifest=mod.verify(original/'behavior-package.snapshot',subject['artifact']['package_sha256'],subject['artifact']['source_commit'])
    if not exact(manifest,evidence['manifest']):raise ValueError('retained package manifest differs')
    prepared,_=read_json(protected(original/'preparation.json'),MAX_EVIDENCE)
    if not exact(prepared,evidence['preparation']):raise ValueError('retained preparation differs')
    members=prepared_associations(recipe,subject,candidate,original,binding,manifest,prepared,evidence['steps'])
    if members!=evidence['prepared_members']:raise ValueError('retained prepared input bytes/ownership changed')
    executable=member(Path(binding['job'])/'artifact-binary')
    if executable!=evidence['executable'] or executable['sha256']!=subject['artifact']['binary_sha256']:raise ValueError('retained executable changed')
    return evidence

def current_facts(config,subject,candidate,recipe,operation_id,work,domains):
    import protected_release_v2 as producer
    cc,_=read_json(protected(recipe['controller_config']))
    if digest(recipe['controller_config'])!=recipe['_controller_sha256']:raise ValueError('current configuration changed')
    installed,uncertainty=applicability_inputs(recipe,subject)
    repo=protected(cc['repository_path'],directory=True);fixed=producer.load_verifier()
    ref=subject['target_ref'];base=subject['expected_base'];artifact=subject['artifact']
    if not isinstance(ref,str) or not re.fullmatch(r'refs/heads/[A-Za-z0-9_./-]+',ref) or '..' in ref or not isinstance(base,str) or not re.fullmatch('[0-9a-f]{'+str(len(artifact['source_commit']))+'}',base) or base==artifact['source_commit']:
        raise ValueError('exact ref/CAS predecessor required')
    if fixed.git(repo,'rev-parse','--is-bare-repository',limit=64).strip()!=b'true':raise ValueError('current protected repository must be bare')
    tree,fmt,_=fixed.immutable_tree(repo,artifact['source_commit'])
    if (tree,fmt)!=(artifact['source_tree'],artifact['object_format']):raise ValueError('current candidate source changed')
    head=fixed.git(repo,'rev-parse','--verify',ref+'^{commit}',limit=128).decode().strip()
    if head!=base:raise ValueError('current target/CAS base moved; no substitution')
    fixed.git(repo,'merge-base','--is-ancestor',base,artifact['source_commit'],limit=128)
    request={'revision':1,'operation_id':operation_id,'command':{'operation':'view','input':{'release_id':subject['release_id']}}}
    reply=producer.exchange(config,request)
    fields(reply,'status data replayed','current protected view')
    if reply['status']!='ok' or reply['replayed'] is not False:raise ValueError('current protected view unavailable')
    view=reply['data'];fields(view,'candidate actions automatic_rollout','current protected view data')
    if not exact(view['candidate'],candidate) or view['automatic_rollout'] is not False or not isinstance(view['actions'],list) or len(view['actions'])>10000:
        raise ValueError('current candidate/target state differs')
    for action in view['actions']:
        fields(action,'id kind subject check_id grant_id state observation dispatch_until_ms','current action')
        if not exact(action['subject'],subject) or action['kind'] not in {'merge','rollout'} or action['state'] not in {'prepared','applied','refused'}:
            raise ValueError('active, uncertain or foreign prior dispatch; reconcile original action')
    saved(work/'applicability-view.json',{'request':request,'reply':reply})
    return {'controller_config_sha256':recipe['_controller_sha256'],'installed_inputs':installed,
            'repository':str(repo),'actual_ref':ref,'actual_predecessor':head,'source_tree':tree,
            'domains':domains,'view_sha256':digest(work/'applicability-view.json')},uncertainty

def reconcile_attestations(config,recipe,subject,work,operation_id,initial):
    """Every prior same-scope request settles under its original identity."""
    import protected_release_v2 as producer
    history=protected(Path(recipe['execution_root'])/'operations',directory=True);records=[];record_bytes=2
    cc,_=read_json(protected(recipe['controller_config']))
    identity=next(p for p in cc['producers'] if p['uid']==config['producer_uid'] and p['role']=='checker')
    subject_hash=hashlib.sha256(canonical(subject)).hexdigest()
    for index,path in enumerate(history.iterdir()):
        if index>=10000:raise ValueError('retained checker operation capacity')
        marker,_=read_json(protected(path))
        fields(marker,'operation_id subject_sha256 recipe_sha256 receipt','original operation marker')
        if path.name!=hashlib.sha256(marker['operation_id'].encode()).hexdigest()+'.json':raise ValueError('original operation marker identity differs')
        if marker['operation_id']==operation_id:
            if initial and marker['receipt']==str(work):continue
            raise ValueError('operation already reserved; lookup/retry its exact original request')
        if marker['subject_sha256']!=subject_hash or marker['recipe_sha256']!=config['recipe_sha256']:continue
        old=protected(marker['receipt'],directory=True)
        prior,binding=producer.bound_original(old)
        if binding is None or prior['operation_id']!=marker['operation_id']:raise ValueError('original applicability request incomplete; reconcile original')
        lookup={'revision':1,'operation_id':operation_id,'command':{'operation':'get_receipt','input':{'operation_id':prior['operation_id']}}}
        reply=producer.exchange(config,lookup)
        record=producer.original_reply(reply,prior,identity)
        if record['reply'].get('status')!='ok' or record['reply'].get('data')!=prior['command']['input']:
            raise ValueError('original Check was not accepted; no renewed identity')
        row={'request':lookup,'reply':reply};record_bytes+=len(canonical(row))+1
        if record_bytes>MAX_EVIDENCE:raise ValueError('original reconciliation evidence byte bound')
        records.append(row)
    saved(work/'reconciled-originals.json',records)
    return history,identity

def attest_evidence(config,subject,candidate,package,recipe_path,original,work,operation_id,initial=False):
    """Fresh applicability, using only complete protected execution format 1."""
    original=protected(original,directory=True);protected(work,directory=True)
    if original.stat().st_mode&0o077 or work.stat().st_mode&0o077:raise ValueError('private evidence/attestation directory required')
    recipe=load_recipe(recipe_path,config,subject)
    if recipe['revision']!=4:raise ValueError('legacy recipe cannot reuse execution evidence')
    load_preparer(recipe).validate_candidate(candidate,subject)
    if not isinstance(operation_id,str) or not re.fullmatch(r'[A-Za-z0-9_.:/-]{1,240}',operation_id) or operation_id.startswith('finish:'):
        raise ValueError('applicability operation identity')
    if Path(package)!=original/'behavior-package.snapshot':raise ValueError('reuse requires the exact protected retained package')
    evidence_member=member(original/'execution-evidence.json')
    if evidence_member['mode']!=0o400:raise ValueError('immutable private execution evidence mode required')
    if not initial:
        import protected_release_v2 as producer
        _,binding=producer.bound_original(original)
        if binding is None:raise ValueError('legacy/partial receipt cannot be promoted')
        if binding['evidence']['path']!=str(original/'execution-evidence.json') or evidence_member['sha256']!=binding['evidence']['sha256']:
            raise ValueError('original execution evidence reference changed')
    evidence=verify_execution(config,subject,candidate,recipe,recipe_path,original)
    if not initial and binding['evidence']['operation_id']!=evidence['operation_id']:raise ValueError('original execution identity differs')
    reference={'path':str(original/'execution-evidence.json'),'sha256':evidence_member['sha256'],'operation_id':evidence['operation_id']}
    execution=Execution(recipe,config)
    with execution.current() as domains, contextlib.ExitStack() as locks:
        lock_records=[locks.enter_context(idle_lock(path)) for path in recipe['applicability']['idle_locks']]
        for key,row in domains['groups'].items():
            if [row['path'],row['identity']]!=[evidence['binding'][key],evidence['binding'][key+'_identity']]:
                raise ValueError('current process domain differs from original evidence')
        history,identity=reconcile_attestations(config,recipe,subject,work,operation_id,initial)
        start=time.time_ns()//1_000_000;tick=time.monotonic_ns()
        facts,uncertainty=current_facts(config,subject,candidate,recipe,operation_id,work,domains)
        facts['idle_locks']=lock_records
        # Recheck current declared bytes after the protected read. No target or
        # policy change is projected away to make old evidence look applicable.
        if load_recipe(recipe_path,config,subject)!=recipe or declared_inputs(recipe,config,recipe_path)!=evidence['declared_inputs']:
            raise ValueError('current declared input changed during applicability')
        if (member(original/'execution-evidence.json')!=evidence_member
            or inventory(original,TRANSPORT_MEMBERS)!=evidence['members']
            or inventory(Path(evidence['binding']['job'])/'prepared')!=evidence['prepared_members']):
            raise ValueError('retained evidence changed during applicability')
        import protected_release_v2 as producer
        head=producer.load_verifier().git(Path(facts['repository']),'rev-parse','--verify',subject['target_ref']+'^{commit}',limit=128).decode().strip()
        if head!=subject['expected_base']:raise ValueError('current target/CAS base moved during applicability')
        end=time.time_ns()//1_000_000;elapsed=(time.monotonic_ns()-tick)//1_000_000
        if start<evidence['executed_at_ms'] or end<start or abs(end-start-elapsed)>uncertainty+1 or elapsed+uncertainty>=ATTESTATION_MS:
            raise ValueError('applicability clock uncertain or observation expired')
        check={'subject':subject,'recipe_sha256':config['recipe_sha256'],'checked_at_ms':end,
               'valid_until_ms':end+ATTESTATION_MS,'results':evidence['execution_check']['results'],'cleanup_verified':True}
        attestation={'format':ATTESTATION_FORMAT,'revision':1,'operation_id':operation_id,'evidence':reference,
                     'producer':identity,'executed_at_ms':evidence['executed_at_ms'],'attested_at_ms':end,
                     'clock_uncertainty_ms':uncertainty,'check':check,'current':facts}
        if not initial:
            saved(history/(hashlib.sha256(operation_id.encode()).hexdigest()+'.json'),
                  {'operation_id':operation_id,'subject_sha256':hashlib.sha256(canonical(subject)).hexdigest(),
                   'recipe_sha256':config['recipe_sha256'],'receipt':str(work)})
        saved(work/'applicability.json',attestation)
    return check

def behavior_check(config,subject,package,recipe_path,work,operation_id,candidate=None):
    import protected_release_v2 as producer
    recipe=load_recipe(recipe_path,config,subject)
    reusable=recipe['revision']==4
    if (recipe['revision'] in {3,4})!=(candidate is not None):raise ValueError('prepared recipe requires exact ordinary candidate input')
    if candidate is not None:load_preparer(recipe).validate_candidate(candidate,subject)
    fixed=producer.load_verifier()
    if digest(protected(Path('/usr/bin/python3').resolve(strict=True)))!=fixed.READER_SHA['python_sha256']:raise ValueError('protected oracle interpreter differs from selected identity')
    if not isinstance(operation_id,str) or not re.fullmatch(r'[A-Za-z0-9_.:/-]{1,240}',operation_id):raise ValueError('behavior operation identity')
    protected(work,directory=True)
    started=time.time_ns()//1_000_000
    # Package verifier is the existing fixed producer; it binds every descriptor
    # field. Behavioral checks do not replace integrity or any human grant.
    package_result=producer.package_check(config,subject,package,work)
    subject_hash=hashlib.sha256(canonical(subject)).hexdigest()
    saved(work/'behavior-intent.json',{'revision':1,'operation_id':operation_id,'subject':subject,'subject_sha256':subject_hash,'recipe_sha256':config['recipe_sha256'],'controller_config_sha256':recipe['_controller_sha256'],'required_checks':recipe['required_checks'],'state':'pending'})
    execution=Execution(recipe,config);results={'package_integrity':True};clean=False;snap=work/'behavior-package.snapshot';retained=False
    try:
        execution.admit(work,operation_id,subject_hash)
        # A second bounded snapshot is independently checked against the same
        # exact package digest; no worker pathname is executed directly.
        mod=producer.load_verifier();cap=mod.MAX_BINARY+mod.MAX_MANIFEST+20480
        fd=os.open(package,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(fd,'rb') as f,producer.private_create(snap) as dst:
            info=os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size>cap:raise ValueError('behavior package type/bound')
            total=0
            while True:
                b=f.read(min(1024*1024,cap-total+1))
                if not b:break
                total+=len(b)
                if total>cap:raise ValueError('behavior package copy bound')
                dst.write(b)
            dst.flush();os.fsync(dst.fileno())
        manifest=mod.verify(snap,subject['artifact']['package_sha256'],subject['artifact']['source_commit'])
        executable=execution.job/'artifact-binary'
        with snap.open('rb') as f:
            f.seek(512);remaining=manifest['files']['bin/borg-runtime']['size']
            with producer.private_create(executable) as dst:
                while remaining:
                    b=f.read(min(remaining,1024*1024))
                    if not b:raise ValueError('truncated verified binary')
                    dst.write(b);remaining-=len(b)
                dst.flush();os.fsync(dst.fileno())
        executable.chmod(0o555)
        if digest(executable)!=subject['artifact']['binary_sha256']:raise ValueError('extracted binary differs')
        if reusable:
            snap.chmod(0o400)
            raw=read_json(protected(recipe_path))[1]
            if hashlib.sha256(raw).hexdigest()!=config['recipe_sha256']:raise ValueError('recipe changed before snapshot')
            with producer.private_create(recipe_path_from_work(work)) as f:f.write(raw);f.flush();os.fsync(f.fileno())
            saved(work/'package-manifest.json',manifest)
        else:snap.unlink()
        producer.sync_directory(work)
        steps=recipe['steps'];prepared=None;preparer=None
        if candidate is not None:
            preparer=load_preparer(recipe)
            steps,prepared=preparer.prepare(recipe,config,subject,candidate,work,execution,executable,protected)
        for index,step in enumerate(steps):
            if prepared is not None:preparer.verify_prepared(recipe,prepared,work,protected)
            if 'observer' in step:
                context={'revision':1,'subject':subject,'subject_sha256':subject_hash,'recipe_sha256':config['recipe_sha256'],'check':step['check'],'execution_workspace':str(execution.workspace),'artifact_binary':str(executable),'cleanup_verified':False,'execution_binding_sha256':digest(work/'execution-binding.json')}
                live=live_step(step,context,work,execution,executable)
                context.update(live,cleanup_verified=True)
                passed=oracle(step,context,work,execution)
                if digest(protected(live['live_evidence_path']))!=live['live_evidence_sha256']:raise ValueError('live evidence changed during adjudication')
                results[step['check']]=live['live_observer_passed'] and passed
                if reusable and step['check']=='selected_build' and results[step['check']]:
                    retain_build_logs(work,execution)
                continue
            command=step['candidate'];argv=[str(executable) if command['executable']=='{artifact_binary}' else command['executable']]
            substitutions={'{artifact_binary}':str(executable),'{workspace}':str(execution.workspace),'{source_commit}':subject['artifact']['source_commit'],'{source_tree}':subject['artifact']['source_tree']}
            for value in command['arguments']:
                for a,b in substitutions.items():value=value.replace(a,b)
                if '{' in value or '}' in value:raise ValueError('unknown recipe substitution')
                argv.append(value)
            if command['executable']!='{artifact_binary}' and digest(protected(command['executable']))!=command['sha256']:raise ValueError('launcher changed before execution')
            prefix=f'{index:02d}-{step["check"]}'
            saved(work/(prefix+'.intent.json'),{'state':'pending','argv':argv,'subject_sha256':subject_hash,'recipe_sha256':config['recipe_sha256']})
            env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':str(execution.workspace),'BORG_ARTIFACT_BINARY':str(executable)}
            p=execution.spawn(argv,env)
            try:exit_code,raw=collect(p,step['output_bytes'],step['timeout_ms'])
            finally:
                execution.finish_candidate(p)
            # Candidate bytes are retained as observations only. They are never
            # parsed into producer Check or used as a protected oracle result.
            with producer.private_create(work/(prefix+'.output')) as f:f.write(raw);f.flush();os.fsync(f.fileno())
            context={'revision':1,'subject':subject,'subject_sha256':subject_hash,'recipe_sha256':config['recipe_sha256'],'check':step['check'],'candidate_exit':exit_code,'output_sha256':hashlib.sha256(raw).hexdigest(),'output_path':str(work/(prefix+'.output')),'execution_workspace':str(execution.workspace),'artifact_binary':str(executable),'cleanup_verified':True}
            saved(work/(prefix+'.observed.json'),context)
            passed=oracle(step,context,work,execution)
            results[step['check']]=exit_code==0 and passed
            if digest(executable)!=subject['artifact']['binary_sha256']:raise ValueError('artifact changed during qualification')
        execution.clean();clean=True
        if prepared is not None:preparer.verify_prepared(recipe,prepared,work,protected)
        if hashlib.sha256(Path(recipe_path).read_bytes()).hexdigest()!=config['recipe_sha256']:raise ValueError('recipe changed during qualification')
        if digest(protected(recipe['controller_config']))!=recipe['_controller_sha256']:raise ValueError('controller role configuration changed during qualification')
        now=time.time_ns()//1_000_000
        check={'subject':subject,'recipe_sha256':config['recipe_sha256'],'checked_at_ms':now,'valid_until_ms':now+config['valid_for_ms'],'results':results,'cleanup_verified':True}
        saved(work/'behavior-result.json',{'state':'observed','check':check,'mandatory_checks_present':set(recipe['required_checks'])<=set(results),'execution_authority':'operator-configured; no rollout authority'})
        if not reusable:return check
        if any(value is not True for value in results.values()):raise ValueError('failed execution cannot produce reusable evidence')
        retained=True
    finally:
        # This file is solely this fresh receipt's private snapshot. Never leave
        # a successful cleanup claim after an unsuccessful snapshot removal.
        snapshot_error=None
        if snap.exists() and not retained:
            try:snap.unlink();producer.sync_directory(work)
            except Exception as error:clean=False;snapshot_error=error
        if execution.lock is not None and execution.owns and not clean:
            try:execution.clean()
            except Exception:pass
        execution.release(clean)
        if snapshot_error is not None:raise snapshot_error
    retain_execution(config,subject,candidate,recipe,recipe_path,work,operation_id,manifest,prepared,steps,started,check)
    return attest_evidence(config,subject,candidate,snap,recipe_path,work,work,operation_id,initial=True)

def recover(config,recipe_path,original,work):
    """Stop only the exact unfinished job; never rerun or manufacture a check."""
    original=protected(original,directory=True)
    protected(work,directory=True)
    intent,_=read_json(protected(original/'behavior-intent.json'))
    if intent.get('revision')!=1 or intent.get('state')!='pending' or intent.get('recipe_sha256')!=config['recipe_sha256'] or intent.get('subject_sha256')!=hashlib.sha256(canonical(intent.get('subject'))).hexdigest():raise ValueError('recovery intent binding differs')
    recipe=load_recipe(recipe_path,config,intent['subject'])
    if intent.get('controller_config_sha256')!=recipe['_controller_sha256']:raise ValueError('recovery controller configuration differs')
    binding,_=read_json(protected(original/'execution-binding.json'))
    e=Execution(recipe,config);e.configured();clean=False
    try:
        lockpath=protected(Path(recipe['execution_root'])/'checker.lock')
        e.lock=lockpath.open('r+b');fcntl.flock(e.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        retained=e.lock.read(MAX_RECIPE+1)
        if retained!=canonical(binding) or binding['receipt']!=str(original) or binding['recipe_sha256']!=config['recipe_sha256']:raise ValueError('recovery does not own the recorded job')
        for key,group in [('cgroup',e.cg),('oracle_cgroup',e.ocg)]:
            if binding[key]!=str(group) or binding[key+'_identity']!=[group.stat().st_dev,group.stat().st_ino]:raise ValueError('cleanup domain identity changed')
        e.binding=binding;e.owns=True;e.clean()
        result={'status':'execution_stopped_outcome_unknown','original_operation_id':intent['operation_id'],'original_receipt':str(original),'subject_sha256':intent['subject_sha256'],'recipe_sha256':config['recipe_sha256'],'cleanup_verified':True,'check_emitted':False,'automatic_rerun':False,'instruction':'If request.json exists, reconcile its original controller receipt; never replace or refresh it.'}
        saved(work/'recovery.json',result);clean=True;return result
    finally:e.release(clean)
