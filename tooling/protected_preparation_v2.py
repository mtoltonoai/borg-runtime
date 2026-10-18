#!/usr/bin/python3
"""Fixed per-candidate inputs for the protected recipe; no approval or rollout.

Only the recipe selects code, local readers, build profile and cleanup authority.
The ordinary candidate contributes immutable data, never an executable command.
"""
import copy, hashlib, importlib.util, json, os, posixpath, re, socket, stat, subprocess, sys, time, zlib
from pathlib import Path, PurePosixPath

MODULES = {'protected_preparation_v2.py', 'protected_behavior_v2.py',
 'protected_release_v2.py', 'package_v2.py', 'build_v2.py', 'candidate_binding.py',
 'fixed_build_launcher.py', 'build_observer.py', 'build_oracle.py',
 'candidate_launcher.py', 'journey_observer.py', 'journey_oracle.py', 'codex_protocol.py'}
CHECKS = ('selected_build', 'actual_candidate_journey')
TOKENS = {'{prepared_build}', '{prepared_build_sha256}', '{prepared_journey}', '{prepared_journey_sha256}'}
SOURCE_TEXT = 'fn main() { println!("{}", include_str!("../answer.txt").trim()); }\n#[test] fn corrected_source() { assert_eq!(include_str!("../answer.txt").trim(), "after"); }\n'

def canonical(v):
    return json.dumps(v, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024), b''): h.update(b)
    return h.hexdigest()

def read(path, cap=128*1024):
    import protected_release_v2 as producer
    with Path(path).open('rb') as f: raw=f.read(cap+1)
    if len(raw)>cap: raise ValueError('preparation JSON bound')
    return producer.strict(raw)

def write(path, data, mode=0o444):
    raw=canonical(data)+b'\n' if not isinstance(data, bytes) else data
    fd=os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, mode)
    with os.fdopen(fd,'wb') as f: f.write(raw); f.flush(); os.fsync(f.fileno())
    Path(path).chmod(mode)
    fd=os.open(Path(path).parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)


def validate_candidate(candidate, subject):
    if not isinstance(candidate,dict) or set(candidate)!={'id','task_id','task_revision','accepted_attempt_id','checkpoint','artifact','revision','qualification'}:
        raise ValueError('exact ordinary release candidate shape required')
    if candidate['id']!=subject.get('release_id') or candidate['artifact']!=subject.get('artifact') or candidate['revision']!=1:
        raise ValueError('ordinary candidate and protected subject differ')
    if candidate['qualification']!={'artifact':'claimed','repository_submission':'unconfigured','checks':'unconfigured','human_rollout':'unconfigured'}:
        raise ValueError('ordinary preparation cannot supply protected qualification')
    if type(candidate['task_revision']) is not int or candidate['task_revision']<1:
        raise ValueError('candidate task revision')
    for k in ('id','task_id','accepted_attempt_id'):
        if not isinstance(candidate[k],str) or not 0<len(candidate[k].encode())<=256 or any(ord(c)<32 or 127<=ord(c)<=159 for c in candidate[k]):raise ValueError('candidate identity')
    a=candidate['artifact'];cp=candidate['checkpoint']
    if not isinstance(cp,dict) or set(cp)!={'registration','repository','commit','remote_ref','operation_id'} or cp['commit']!=a.get('source_commit'):raise ValueError('candidate checkpoint source differs')
    registration=cp['registration']
    if not isinstance(registration,dict) or set(registration)!={'repository_id','registration_revision'}:raise ValueError('candidate checkpoint registration shape')
    for value in registration.values():
        if not isinstance(value,str) or not 0<len(value.encode())<=256 or any(ord(c)<32 or 127<=ord(c)<=159 for c in value):raise ValueError('candidate checkpoint registration identity')
    if len(canonical(candidate))>128*1024:raise ValueError('candidate bound')


def application_wire(profile):
    value=profile.get('application_wire_revision',8)
    if type(value) is not int or value not in (8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25):
        raise ValueError('explicit candidate application wire revision must be 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 or 25')
    return value


def validate_profile(preparation, recipe, protected):
    if set(preparation)!={'path','sha256','profile'}:raise ValueError('preparation fields')
    path=protected(preparation['path']);root=path.parent
    if path.name!='protected_preparation_v2.py' or sha(path)!=preparation['sha256']:raise ValueError('preparer identity')
    p=preparation['profile']
    fields={'revision','modules','archive','toolchain','cargo_home','cargo_manifest','limits'}
    if set(p) not in (fields,fields|{'application_wire_revision'}) or p['revision']!=2 or set(p['modules'])!=MODULES:
        raise ValueError('fixed preparation profile/inventory')
    application_wire(p)
    for name,expected in p['modules'].items():
        if not re.fullmatch('[0-9a-f]{64}',expected) or sha(protected(root/name))!=expected:raise ValueError('fixed preparation module changed: '+name)
    for name in ('fixed_build_launcher.py','candidate_launcher.py'):
        if not (root/name).stat().st_mode & 0o001:raise ValueError('fixed launcher is not executable by configured execution identity')
    if p['modules'][path.name]!=preparation['sha256'] or p['modules']['protected_behavior_v2.py']!=recipe['supervisor_sha256']:
        raise ValueError('preparer/supervisor inventory mismatch')
    if set(p['archive'])!={'path','sha256'}:raise ValueError('selected archive fields')
    archive=protected(p['archive']['path'])
    if sha(archive)!=p['archive']['sha256']:raise ValueError('selected archive changed')
    for name in ('toolchain','cargo_home'):protected(p[name],directory=True)
    cargo_manifest(p,protected)
    if not isinstance(p['limits'],dict) or set(p['limits'])!={'path','sha256'}:raise ValueError('selected runtime limits fields')
    limits_path=protected(p['limits']['path'])
    if not re.fullmatch('[0-9a-f]{64}',p['limits']['sha256']) or sha(limits_path)!=p['limits']['sha256']:raise ValueError('selected runtime limits changed')
    limits=read(limits_path)
    if not isinstance(limits,dict) or limits.get('schema_version')!=1:raise ValueError('selected runtime limits schema')
    bycheck={s['check']:s for s in recipe['steps']}
    if not set(CHECKS)<=set(bycheck) or [s['check'] for s in recipe['steps'][:2]]!=list(CHECKS):raise ValueError('fixed build then candidate journey required')
    build=bycheck[CHECKS[0]];journey=bycheck[CHECKS[1]]
    expected={'build':('fixed_build_launcher.py',['{prepared_build}','{prepared_build_sha256}'])}
    for name in ('controller','native1','coding1','coding2'):
        expected[name]=('candidate_launcher.py',['{prepared_journey}','{prepared_journey_sha256}',name])
    if set(build['commands'])!={'build'} or set(journey['commands'])!={'controller','native1','coding1','coding2'}:raise ValueError('fixed prepared command inventory')
    for step in (build,journey):
        for name,command in step['commands'].items():
            module,args=expected[name]
            if command!={'executable':str(root/module),'sha256':p['modules'][module],'arguments':args}:raise ValueError('prepared command differs from fixed recipe')
        for role,module in [('observer','build_observer.py' if step is build else 'journey_observer.py'),('oracle','build_oracle.py' if step is build else 'journey_oracle.py')]:
            if step[role]!={'path':str(root/module),'sha256':p['modules'][module]}:raise ValueError('prepared oracle/observer differs')
    return root


def load_package(root, profile, protected):
    path=root/'package_v2.py'
    if sha(path)!=profile['modules'][path.name]:raise ValueError('package reader changed')
    spec=importlib.util.spec_from_file_location('package_v2',path);m=importlib.util.module_from_spec(spec)
    sys.modules['package_v2']=m;spec.loader.exec_module(m)
    for path,key in ((Path('/usr/bin/git'),'git_sha256'),(Path('/usr/bin/python3').resolve(),'python_sha256')):
        if sha(protected(path))!=m.READER_SHA[key]:raise ValueError('selected source reader changed')
    for path in [*(Path(profile['toolchain'])/n for n in ('cargo','rustc','rustdoc')),Path('/usr/bin/gcc').resolve(),Path('/usr/bin/ar').resolve(),Path('/usr/bin/pkg-config').resolve()]:protected(path)
    if profile['archive']['sha256']!=m.SQLITE_ARCHIVE_SHA:raise ValueError('unselected build archive')
    m.verify_archive(Path(profile['archive']['path']));m.verify_tool_files(Path(profile['toolchain']))
    return m


def cargo_manifest(profile, protected):
    """Read only the pinned manifest; its historical source paths are never used."""
    pin=profile['cargo_manifest']
    if not isinstance(pin,dict) or set(pin)!={'path','sha256'} or not isinstance(pin['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',pin['sha256']):
        raise ValueError('selected Cargo manifest fields')
    path=protected(pin['path']);size=path.stat().st_size
    if sha(path)!=pin['sha256']:raise ValueError('selected Cargo manifest changed')
    # The protected content pin fixes the finite JSON byte bound before parsing.
    manifest=read(path,cap=size)
    if path.stat().st_size!=size or sha(path)!=pin['sha256']:raise ValueError('selected Cargo manifest changed while reading')
    fields={'files','symlinks','lock_sha256','manifest_sha256','git_revisions','registry_packages','scope'}
    if not isinstance(manifest,dict) or set(manifest)!=fields or not isinstance(manifest['files'],dict) or not isinstance(manifest['symlinks'],dict):
        raise ValueError('selected Cargo inventory fields')
    files={};links={};directories=set()
    def relative(name):
        if not isinstance(name,str) or '\x00' in name or str(PurePosixPath(name))!=name or '..' in PurePosixPath(name).parts:
            raise ValueError('Cargo inventory path')
        parts=PurePosixPath(name).parts
        if len(parts)<4 or parts[0]!='cargo' or tuple(parts[1:3]) not in {('registry','cache'),('registry','index'),('registry','src'),('git','db'),('git','checkouts')}:
            raise ValueError('unselected Cargo input path')
        return PurePosixPath(*parts[1:]).as_posix()
    for name,entry in manifest['files'].items():
        if not isinstance(name,str):raise ValueError('Cargo inventory path')
        if not name.startswith('cargo/'):continue
        rel=relative(name)
        if not isinstance(entry,dict) or set(entry)!={'mode','sha256','size','source'} or entry['mode'] not in {'0o644','0o755'} or type(entry['size']) is not int or entry['size']<0 or not isinstance(entry['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',entry['sha256']):
            raise ValueError('Cargo inventory file fields')
        files[rel]=entry
    for name,target in manifest['symlinks'].items():
        rel=relative(name)
        if not isinstance(target,str) or not target or '\x00' in target or PurePosixPath(target).is_absolute():raise ValueError('Cargo inventory link target')
        resolved=posixpath.normpath(posixpath.join(posixpath.dirname(rel),target))
        if not rel.startswith('git/checkouts/') or not resolved.startswith('git/checkouts/') or rel in files:raise ValueError('Cargo inventory link escape/overlap')
        links[rel]=target
    if not files:raise ValueError('empty selected Cargo inventory')
    for name in files.keys()|links.keys():
        directories.update(str(p) for p in PurePosixPath(name).parents if str(p)!='.')
    if directories & (files.keys()|links.keys()):raise ValueError('Cargo inventory path overlap')
    for name,target in links.items():
        resolved=posixpath.normpath(posixpath.join(posixpath.dirname(name),target))
        if resolved not in files and resolved not in directories:raise ValueError('Cargo inventory link target missing')
        if any(str(p) in links for p in [PurePosixPath(resolved),*PurePosixPath(resolved).parents]):raise ValueError('Cargo inventory link alias chain')
    return manifest,files,links,directories


def cargo_file(path, entry, protected, destination=None):
    """Bound every read by its pinned entry; never follow or create a hardlink."""
    protected(path)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as src:
        before=os.fstat(src.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size!=entry['size'] or stat.S_IMODE(before.st_mode)!=(int(entry['mode'],8)&0o555):
            raise ValueError('Cargo base file type/size/mode/link')
        dst=None
        try:
            if destination is not None:
                out=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                dst=os.fdopen(out,'wb')
                if (os.fstat(out).st_dev,os.fstat(out).st_ino)==(before.st_dev,before.st_ino):raise ValueError('Cargo projection aliases base')
            digest=hashlib.sha256();total=0
            while True:
                block=src.read(min(1024*1024,entry['size']-total+1))
                if not block:break
                total+=len(block)
                if total>entry['size']:raise ValueError('Cargo base file byte bound')
                digest.update(block)
                if dst is not None:dst.write(block)
            after=os.fstat(src.fileno())
            identity=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_mode,s.st_uid,s.st_gid,s.st_nlink)
            if total!=entry['size'] or digest.hexdigest()!=entry['sha256'] or identity(before)!=identity(after):raise ValueError('Cargo base file changed')
            if dst is not None:dst.flush();os.fsync(dst.fileno())
        finally:
            if dst is not None:dst.close()


def verify_cargo_base(profile, protected, selected=None):
    selected=cargo_manifest(profile,protected) if selected is None else selected
    manifest,files,links,directories=selected;base=protected(profile['cargo_home'],directory=True)
    expected=files.keys()|links.keys()|directories;seen=set();pending=[base]
    while pending:
        directory=pending.pop();protected(directory,directory=True)
        with os.scandir(directory) as children:
            for child in children:
                path=Path(child.path);rel=path.relative_to(base).as_posix()
                if rel not in expected or rel in seen or len(seen)>=len(expected):raise ValueError('Cargo base inventory membership/bound')
                seen.add(rel);info=child.stat(follow_symlinks=False)
                if rel in directories:
                    if not stat.S_ISDIR(info.st_mode):raise ValueError('Cargo base directory alias/type')
                    pending.append(path)
                elif rel in links:
                    if not stat.S_ISLNK(info.st_mode) or os.readlink(path)!=links[rel] or info.st_uid not in {0,os.geteuid()}:raise ValueError('Cargo base link changed')
                else:cargo_file(path,files[rel],protected)
    if seen!=expected:raise ValueError('Cargo base input missing')
    if sha(protected(profile['cargo_manifest']['path']))!=profile['cargo_manifest']['sha256']:raise ValueError('selected Cargo manifest changed')
    return selected


def project_cargo_cache(packager, profile, source, candidate_profile, execution, recipe, protected):
    # The pinned reader recomputes both the complete policy and exact source
    # association. Neither claimed policy digests nor cache hashes prove them.
    packager.validate_package_profile(candidate_profile,source,allow_legacy=False)
    selected=verify_cargo_base(profile,protected);manifest,files,links,directories=selected
    if manifest['manifest_sha256']!=packager.MANIFEST_SHA or manifest['lock_sha256']!=packager.LOCK_SHA:
        raise ValueError('Cargo cache baseline build profile differs')
    job=protected(execution.job,directory=True);workspace=Path(execution.workspace)
    if workspace!=job/'workspace' or workspace.is_symlink() or not workspace.is_dir() or workspace.resolve()!=workspace:raise ValueError('Cargo projection workspace identity')
    destination=workspace/'cargo-home';destination.mkdir(mode=0o700)
    # This happens before any candidate starts. Failure leaves only this fresh
    # job's quarantine; never remove/reuse another path or grant base writes.
    created=[destination]
    for name in sorted(directories,key=lambda n:(len(PurePosixPath(n).parts),n)):
        path=destination/name;path.mkdir(mode=0o700);created.append(path)
    base=Path(profile['cargo_home'])
    for name,entry in sorted(files.items()):
        path=destination/name;cargo_file(base/name,entry,protected,path)
        path.chmod(0o700 if int(entry['mode'],8)&0o111 else 0o600);created.append(path)
    for name,target in sorted(links.items()):
        path=destination/name;path.symlink_to(target);created.append(path)
    verify_cargo_base(profile,protected,selected)
    if os.geteuid()==0:
        for path in reversed(created):os.chown(path,recipe['execution_uid'],recipe['execution_gid'],follow_symlinks=False)
    # The mutable projection is never a protected input or shared cache.
    # Existing job/cgroup cleanup and retained-workspace policy stay unchanged.
    return {'revision':2,'manifest_sha256':profile['cargo_manifest']['sha256'],'base':str(base),'destination':str(destination),
            'files':len(files),'symlinks':len(links),'directories':len(directories),'bytes':sum(e['size'] for e in files.values()),
            'entry_bound':len(files)+len(links)+len(directories),'execution_uid':recipe['execution_uid'],'execution_gid':recipe['execution_gid'],
            'source_lock_sha256':candidate_profile['cargo_lock_sha256'],'source_manifest_sha256':candidate_profile['cargo_manifest_sha256'],
            'cache_baseline_lock_sha256':manifest['lock_sha256'],'cache_baseline_manifest_sha256':manifest['manifest_sha256'],
            'dependency_policy':candidate_profile['dependency_policy'],
            'scope':'Fresh mutable job-local cache copied from pinned immutable inputs; no shared-cache writes or reuse.'}


def snapshot_repository(packager, source, artifact, destination):
    """Copy only independently hashed reachable objects; never copy hooks/config."""
    if source.is_symlink() or source.resolve()!=source or not source.is_dir():raise ValueError('source repository alias/type')
    tree,fmt,entries=packager.immutable_tree(source,artifact['source_commit'])
    if tree!=artifact['source_tree'] or fmt!=artifact['object_format']:raise ValueError('source tree/format differs from candidate')
    destination.mkdir(mode=0o755);(destination/'objects').mkdir();(destination/'refs').mkdir()
    config=b'[core]\n\tbare = true\n\trepositoryformatversion = '+(b'1' if fmt=='sha256' else b'0')+b'\n'
    if fmt=='sha256':config+=b'[extensions]\n\tobjectformat = sha256\n'
    write(destination/'config',config);write(destination/'HEAD',b'ref: refs/heads/unused\n')
    objects={(artifact['source_commit'],'commit'),(tree,'tree')}
    raw=packager.git(source,'ls-tree','-r','-t','-z','--full-tree',artifact['source_commit'],limit=2*1024*1024)
    for item in raw.split(b'\0'):
        if item:
            meta,_=item.split(b'\t',1);_,kind,oid=meta.decode().split();objects.add((oid,kind))
    if len(objects)>2*packager.MAX_FILES+2:raise ValueError('source object count bound')
    total=0
    for oid,kind in sorted(objects):
        if kind not in {'commit','tree','blob'}:raise ValueError('source object kind')
        data=packager.git(source,'cat-file',kind,oid);total+=len(data)
        if len(data)>packager.MAX_SOURCE_FILE or total>packager.MAX_SOURCE+4*1024*1024:raise ValueError('source object byte bound')
        encoded=kind.encode()+b' '+str(len(data)).encode()+b'\0'+data
        if hashlib.new(fmt,encoded).hexdigest()!=oid:raise ValueError('source object identity changed')
        folder=destination/'objects'/oid[:2];folder.mkdir(exist_ok=True)
        write(folder/oid[2:],zlib.compress(encoded))
    # Validate exact candidate bytes against the selected complete policy,
    # only immutable Git objects and no candidate repository configuration.
    inventory={};cargo_inputs={}
    for name,mode,oid,size in entries:
        data=packager.git(destination,'cat-file','blob',oid)
        if len(data)!=size or hashlib.new(fmt,b'blob '+str(size).encode()+b'\0'+data).hexdigest()!=oid:raise ValueError('snapshot source blob changed')
        inventory[name]={'mode':mode,'git_blob':oid,'size':size,'sha256':hashlib.sha256(data).hexdigest()}
        if name in packager.CARGO_INPUTS:cargo_inputs[name]=data.decode('utf-8')
    source_record={'commit':artifact['source_commit'],'tree':tree,'object_format':fmt,'files':inventory}
    candidate_profile=packager.candidate_profile(cargo_inputs,source_record)
    if packager.immutable_tree(destination,artifact['source_commit'])[:2]!=(tree,fmt):raise ValueError('snapshot source verification failed')
    for p in sorted(destination.rglob('*'),reverse=True):
        if p.is_dir():p.chmod(0o555)
    destination.chmod(0o555)
    return source_record,candidate_profile


def fixed_git(args,cwd,home):
    env={'PATH':'/usr/bin:/bin','HOME':str(home),'LANG':'C','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_SYSTEM':'/dev/null','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_NO_LAZY_FETCH':'1','GIT_ALLOW_PROTOCOL':'file','GIT_AUTHOR_NAME':'Controlled Fixture','GIT_AUTHOR_EMAIL':'fixture@example.invalid','GIT_COMMITTER_NAME':'Controlled Fixture','GIT_COMMITTER_EMAIL':'fixture@example.invalid','GIT_AUTHOR_DATE':'2000-01-01T00:00:00Z','GIT_COMMITTER_DATE':'2000-01-01T00:00:00Z'}
    result=subprocess.run(['/usr/bin/git','-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false',*args],cwd=cwd,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
    if result.returncode or len(result.stdout)>4096 or len(result.stderr)>4096:raise ValueError('fixed fixture Git preparation failed')
    return result.stdout.decode().strip()


def prepare(recipe, config, subject, candidate, work, execution, executable, protected):
    validate_candidate(candidate,subject)
    root=validate_profile(recipe['preparation'],recipe,protected);p=recipe['preparation']['profile']
    package=load_package(root,p,protected)
    cc=read(protected(recipe['controller_config']))
    # Source location is existing operator-configured repository authority, not
    # a path supplied by a candidate or a newly selected remote repository.
    if cc['repository']!=subject['repository'] or cc['target_ref']!=subject['target_ref']:raise ValueError('source repository target differs')
    job=execution.job;inputs=job/'prepared';inputs.mkdir(mode=0o755)
    source=inputs/'source.git'
    source_record,candidate_profile=snapshot_repository(package,Path(cc['repository_path']),subject['artifact'],source)
    inventory=source_record['files']
    projection=project_cargo_cache(package,p,source_record,candidate_profile,execution,recipe,protected)
    write(inputs/'cargo-projection.json',projection)
    package_snapshot=work/'behavior-source-manifest.json'
    write(package_snapshot,{'subject':subject,'candidate':candidate,'source_files':inventory,'cargo_profile':candidate_profile,'modules':p['modules']},0o600)
    build={'revision':1,'packager':str(root/'package_v2.py'),'packager_sha256':p['modules']['package_v2.py'],'builder_sha256':p['modules']['build_v2.py'],'repo':str(source),'source_commit':subject['artifact']['source_commit'],'source_tree':subject['artifact']['source_tree'],'archive':p['archive']['path'],'toolchain':p['toolchain'],'cargo_home':projection['destination'],'output':str(execution.workspace/'selected-build'),'binding_module_sha256':p['modules']['candidate_binding.py']}
    buildpath=inputs/'build.json';write(buildpath,build)
    # Fixed controlled seed data, never candidate-provided privileged commands.
    seed=inputs/'seed';seed.mkdir();(seed/'src').mkdir();write(seed/'answer.txt',b'before\n');write(seed/'src/main.rs',SOURCE_TEXT.encode())
    fixed_git(['init'],seed,job);fixed_git(['add','.'],seed,job);fixed_git(['commit','-m','fixed controlled source'],seed,job)
    start=fixed_git(['rev-parse','HEAD'],seed,job)
    # The publication repository is execution-owned, not checker evidence.
    remote=execution.workspace/'journey-remote.git';fixed_git(['clone','--bare','--no-hardlinks',str(seed),str(remote)],job,job)
    seed_objects={(x.stat().st_dev,x.stat().st_ino) for x in (seed/'.git/objects').rglob('*') if x.is_file()}
    if any((x.stat().st_dev,x.stat().st_ino) in seed_objects or x.stat().st_nlink!=1 for x in (remote/'objects').rglob('*') if x.is_file()):raise ValueError('journey publication aliases immutable seed objects')
    if os.geteuid()==0:
        for path in [remote,*remote.rglob('*')]:os.chown(path,recipe['execution_uid'],recipe['execution_gid'])
    ports=[]
    while len(ports)<4:
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        if port not in ports:ports.append(port)
    cp,pp,proxy,gp=ports;workspace=execution.workspace
    fixture=inputs/'codex_protocol.py';write(fixture,(root/'codex_protocol.py').read_bytes(),0o555);write(fixture.with_suffix('.json'),{'port':gp})
    # A protected recipe selects the exact metadata. Only fixture role capacities
    # differ; each immutable file is included in the prepared input manifest.
    selected_limits=read(p['limits']['path']);limit_files={}
    if sha(protected(p['limits']['path']))!=p['limits']['sha256']:raise ValueError('selected runtime limits changed during preparation')
    for role,coding_capacity,native_capacity in [('controller',1,0),('coding',1,0),('native',0,1)]:
        policy=copy.deepcopy(selected_limits)
        policy['worker']['coding_capacity']=coding_capacity;policy['worker']['native_capacity']=native_capacity
        limit_files[role]=inputs/(role+'-limits.json');write(limit_files[role],policy)
    launches={'controller':['controller','--limits',str(limit_files['controller']),'--db',str(workspace/'journey.sqlite'),'--listen','127.0.0.1:'+str(cp),'--origin','http://127.0.0.1:'+str(cp)]}
    for name,kind,inc in [('native1','native','inc-1'),('coding1','coding','inc-1'),('coding2','coding','inc-2')]:
        args=['worker','--limits',str(limit_files[kind]),'--controller','http://127.0.0.1:'+str(proxy),'--id',kind+'-worker','--incarnation',inc,'--workspace-root',str(workspace/(kind+'-worker')),'--codex',str(fixture),'--coding-capacity','1' if kind=='coding' else '0','--native-capacity','1' if kind=='native' else '0']
        if kind=='native':args+=['--native-endpoint','http://127.0.0.1:'+str(pp)+'/v1/responses']
        launches[name]=args
    rustc=Path(p['toolchain'])/'rustc'
    journey={'binary':str(executable),'binary_sha256':subject['artifact']['binary_sha256'],'workspace':str(workspace),'launches':launches,'controller_port':cp,'peer_port':pp,'proxy_port':proxy,'gate_port':gp,'deadline_unix_ms':int(time.time()*1000)+3600000,'protocol_revision':application_wire(p),'task_defaults':selected_limits['task_defaults'],'selected_limits_sha256':p['limits']['sha256'],'repository':{'registration':{'repository_id':'journey-source','registration_revision':'1'},'url':str(remote),'start_commit':start,'checkpoint_remote':str(remote)},'repository_registration':{'repository_id':'journey-source','registration_revision':'1','display_name':'Fixed journey source','source_endpoint':str(remote),'checkpoint_endpoint':str(remote),'source_store_key':'journey-source.git','checkpoint_store_key':'journey-checkpoints.git'},'acceptance':[[str(rustc),'--test','src/main.rs','-o','acceptance'],['./acceptance'],[str(rustc),'src/main.rs','-o','demo']],'git_sha256':package.READER_SHA['git_sha256'],'expected_test_source':SOURCE_TEXT}
    journeypath=inputs/'journey.json';write(journeypath,journey)
    replacements={'{prepared_build}':str(buildpath),'{prepared_build_sha256}':sha(buildpath),'{prepared_journey}':str(journeypath),'{prepared_journey_sha256}':sha(journeypath)}
    steps=copy.deepcopy(recipe['steps'])
    for step in steps:
        for command in step.get('commands',{}).values():
            command['arguments']=[replacements.get(arg,arg) for arg in command['arguments']]
    # Freeze every input to candidate code and every imported oracle/reader.
    immutable={str(path):sha(path) for path in inputs.rglob('*') if path.is_file()}
    for path in sorted(inputs.rglob('*'),reverse=True):
        if path.is_dir():path.chmod(0o555)
        elif path.suffix!='.py':path.chmod(0o444)
    inputs.chmod(0o555)
    receipt={'revision':1,'subject_sha256':hashlib.sha256(canonical(subject)).hexdigest(),'candidate_sha256':hashlib.sha256(canonical(candidate)).hexdigest(),'recipe_sha256':config['recipe_sha256'],'prepared_inputs':immutable,'module_root':str(root),'modules':p['modules'],'source_manifest_sha256':sha(package_snapshot),'source_files':len(inventory),'local_controlled_peers_only':True,'protected_check_emitted':False,'human_rollout_authority':False}
    write(work/'preparation.json',receipt,0o600)
    return steps,receipt


def verify_prepared(recipe, receipt, work, protected):
    module_root=validate_profile(recipe['preparation'],recipe,protected)
    load_package(module_root,recipe['preparation']['profile'],protected)
    verify_cargo_base(recipe['preparation']['profile'],protected)
    if sha(work/'behavior-source-manifest.json')!=receipt['source_manifest_sha256']:raise ValueError('prepared source receipt changed')
    for path,expected in receipt['prepared_inputs'].items():
        if sha(protected(path))!=expected:raise ValueError('immutable prepared input changed')
    prepared=Path(next(path for path in receipt['prepared_inputs'] if path.endswith('/build.json'))).parent
    # Bound enumeration and never descend through an alias. Revision-4 evidence
    # additionally retains and compares the exact empty-directory inventory.
    expected=set(receipt['prepared_inputs'])
    if len(expected)>40000:raise ValueError('prepared input membership bound')
    pending=[prepared];actual=set();seen=set()
    while pending:
        directory=pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                path=Path(entry.path);name=str(path)
                if entry.is_symlink():raise ValueError('prepared input alias appeared')
                if name in seen or len(seen)>=40000:raise ValueError('prepared input membership bound')
                seen.add(name)
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False):actual.add(name)
                else:raise ValueError('prepared input membership type')
    if actual!=set(receipt['prepared_inputs']):raise ValueError('prepared input membership changed')
