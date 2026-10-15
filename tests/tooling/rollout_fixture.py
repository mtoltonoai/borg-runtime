"""Controlled service seam; real scratch package, journals and copied SQLite.

Not installed-role or real-systemd qualification. No subprocess/socket is used
unless integration_main explicitly connects to root's actual private controller.
"""
from pathlib import Path
import contextlib, copy, hashlib, importlib.util, json, os, shutil, sqlite3, tempfile, time
from unittest.mock import patch
import protected_rollout_v2 as adapter
import rollout_host_v2 as hostmod
import rollout_common_v2 as common
import package_v2 as package

MODULES={'adapter':'protected_rollout_v2.py','host':'rollout_host_v2.py','common':'rollout_common_v2.py','producer':'protected_release_v2.py','verifier':'package_v2.py'}

def pin(path):
    path=Path(path).resolve();return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def save(path,value):
    path=Path(path);path.write_bytes(common.canonical(value));path.chmod(0o600);return pin(path)

def fake_package(root):
    # Same deterministic archive/verifier; deliberately non-runnable ELF fixture.
    binary=root/'fake-binary';raw=bytearray(128);raw[:7]=b'\x7fELF\x02\x01\x01';raw[16:18]=(3).to_bytes(2,'little');raw[18:20]=(62).to_bytes(2,'little');binary.write_bytes(raw);binary.chmod(0o500)
    files={name:{'mode':'100644','git_blob':'a'*40,'size':1,'sha256':'2'*64} for name in ('Cargo.toml','Cargo.lock','src/main.rs','src/lib.rs','src/protocol.rs','src/controller/database.rs','src/worker/handoff.rs')}
    files['Cargo.toml']['sha256']=package.MANIFEST_SHA;files['Cargo.lock']['sha256']=package.LOCK_SHA
    build={'tools':{'sha256':package.TOOL_SHA,'versions':{'gcc':'13.3.0','gcc_target':'x86_64-linux-gnu','ar':'GNU ar 2.42','pkg-config':'1.8.1','cargo':'cargo 1.99.0 (fixture)','rustc':'rustc 1.99.0 (fixture)\nhost: '+package.TARGET}},'sqlite':{'identity':{'version':'3.54.0','source_id':package.SQLITE_SOURCE,'threadsafe':1,'api_armor':1,'foreign_keys':1},'source_sha256':package.SQLITE_FILES,'archive_sha256':package.SQLITE_ARCHIVE_SHA,'cflags':package.CFLAGS,'static_library_sha256':'1'*64}}
    m={'format':'borg-v2-release/1','source':{'commit':'a'*40,'tree':'b'*40,'object_format':'sha1','files':files},'profile':package.PROFILE,'recipe':{'packager_sha256':'3'*64,'build_helper_sha256':package.BUILDER_SHA,**package.READER_SHA,'environment':package.ENV_PROFILE},'build':build,'files':{'bin/borg-runtime':package.elf_binary(binary)}}
    path=root/'package.tar';package.package_tar(path,binary,m);path.chmod(0o600);return path

def artifact(path):
    p=Path(path);sha=package.digest(p);m=package.verify(p,sha)
    a={'format':m['format'],'package_sha256':sha,'package_bytes':p.stat().st_size,'manifest_sha256':common.digest(package.canonical(m)),'source_commit':m['source']['commit'],'source_tree':m['source']['tree'],'object_format':m['source']['object_format'],'binary_sha256':m['files']['bin/borg-runtime']['sha256'],'binary_bytes':m['files']['bin/borg-runtime']['size'],'target':m['files']['bin/borg-runtime']['target']}
    return a,m

def private_input_package(root,path,expected):
    # The historical test input is evidence, not an installed protected file.
    # Preserve it and verify a private exact copy against all descriptor fields.
    root=Path(root);root.mkdir(parents=True,exist_ok=True);root.chmod(0o700)
    source=Path(path)
    if not source.is_absolute() or source!=source.resolve(strict=True):raise ValueError('fixture package path alias')
    def identity():
        st=source.lstat()
        return {key:getattr(st,key) for key in ('st_dev','st_ino','st_mode','st_uid','st_gid','st_nlink','st_size','st_mtime_ns','st_ctime_ns')}
    before=identity()
    raw=common.read(source,expected['package_bytes'])
    if len(raw)!=expected['package_bytes'] or common.digest(raw)!=expected['package_sha256']:raise ValueError('fixture package descriptor bytes differ')
    after=identity()
    if after!=before:raise ValueError('fixture package changed during copy')
    dest=root/'verified-input-package.tar'
    common.write_new(dest,raw,0o600)
    actual,_=artifact(dest)
    if actual!=expected:raise ValueError('fixture package descriptor differs')
    if identity()!=before:raise ValueError('historical fixture package changed')
    save(root/'package-input.json',{'source':str(source),'before':before,'after':after,'copy':pin(dest),'artifact':actual,'source_unchanged':True})
    return dest

class Fixture:
    def __init__(self,root,package_path=None,subject=None,controller=None,producer=None):
        import _sqlite3
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.root.chmod(0o700)
        self.runtime_limits=common.strict((Path(adapter.__file__).parents[1]/'metadata/limits.json').read_bytes());self.runtime_limits['worker']['coding_capacity']=1;self.runtime_limits['worker']['native_capacity']=0
        self.effects=[];self.active={};self.invocations={};self.instances={};self.lost_phase=None
        package_path=Path(package_path) if package_path else fake_package(self.root)
        self.artifact,self.manifest=artifact(package_path)
        self.subject=subject or {'release_id':'release-test','artifact':self.artifact,'repository':'fixture-repo','target_ref':'refs/heads/main','expected_base':'0'*40,'policy':'policy','deployment_target':'fixture','target_revision':'one','installed_package_sha256':'1'*64}
        assert self.subject['artifact']==self.artifact
        self.controller=controller or {'socket':str(self.root/'private.sock'),'worker_uids':[9004],'producers':[{'id':'rollout','uid':os.geteuid(),'role':'rollout'}],'policy':self.subject['policy'],'recipe_sha256':'e'*64,'required_checks':['package'],'max_check_age_ms':60000,'repository':self.subject['repository'],'repository_path':str(self.root/'unused.git'),'target_ref':self.subject['target_ref'],'deployment_target':self.subject['deployment_target'],'target_revision':self.subject['target_revision'],'installed_package_sha256':self.subject['installed_package_sha256'],'max_records':1000}
        producer=producer or {'socket':self.controller['socket'],'server_uid':os.geteuid(),'producer_uid':os.geteuid(),'policy':self.subject['policy'],'recipe_sha256':self.controller['recipe_sha256'],'valid_for_ms':60000}
        controller_pin=save(self.root/'controller-config.json',self.controller);producer_pin=save(self.root/'producer.json',producer)
        installed=self.root/'installed';installed.mkdir(mode=0o700)
        modules={}
        for key,name in MODULES.items():
            to=installed/name;shutil.copyfile(Path(adapter.__file__).parent/name,to);to.chmod(0o500);modules[key]=pin(to)
        sqlite_library=next(line.split()[-1] for line in Path('/proc/self/maps').read_text().splitlines() if 'libsqlite3.so' in line and line.split()[-1].startswith('/'))
        limits={k:1048576 for k in common.LIMIT_FIELDS.split()}
        limits.update(clock_uncertainty_ms=0,module_bytes=16777216,package_bytes=536870912,io_chunk_bytes=65536,hosts=2,command_output_bytes=16777216,command_timeout_ms=30000,cleanup_timeout_ms=5000,cleanup_poll_ms=10,readiness_poll_ms=10,overall_timeout_ms=120000,transport_bytes=16777216,transport_timeout_ms=30000,state_entries=1000,state_bytes=16777216,api_pages=100,api_page_items=20,sqlite_tables=64,sqlite_rows=10000,sqlite_steps=1000)
        self.profile={'revision':1,'deployment_target':self.subject['deployment_target'],'target_revision':self.subject['target_revision'],'policy':self.subject['policy'],'producer':producer_pin,'controller_config':controller_pin,'python':pin('/proc/self/exe'),'transport_tools':{name:pin('/usr/bin/'+name) for name in ('ssh','sudo','env')},'sqlite_reader':{'extension':pin(_sqlite3.__file__),'library':pin(sqlite_library)},'state_root':str(self.root/'journal'),'limits':limits,'modules':modules,'formats':{'before':{'wire':8,'controller_schema':12,'worker_format':9},'after':{'wire':8,'controller_schema':12,'worker_format':9},'source_files':{k:self.manifest['source']['files'][k]['sha256'] for k in ('src/protocol.rs','src/controller/database.rs','src/worker/handoff.rs')}},'hosts':[]}
        Path(self.profile['state_root']).mkdir(mode=0o700)
        for name,role in [('controller','controller'),('worker','worker')]:
            root=self.root/name;root.mkdir(mode=0o700);state=root/'state';state.mkdir(mode=0o700);control=root/'control';control.mkdir(mode=0o700);release=root/'releases';release.mkdir(mode=0o700)
            policy=save(root/'policy.json',self.runtime_limits);save(state/'limits.json',self.runtime_limits);(state/'owner').write_bytes(b'fixed owner bytes')
            (state/'helpers').mkdir(mode=0o700);(state/'helpers'/'owner.binding').write_text('fixed-binding')
            if role=='controller':
                db=state/'tasks.sqlite';con=sqlite3.connect(db);con.execute('PRAGMA user_version=12');con.execute('CREATE TABLE evidence(id TEXT PRIMARY KEY,value TEXT)');con.execute("INSERT INTO evidence VALUES('original','unknown-outcome-retained')");con.commit();con.close();fmt=None
            else:
                db=None;fmt=state/'format.json';save(fmt,{'format_revision':9});(state/'commands').mkdir(mode=0o700);(state/'outbox').mkdir(mode=0o700);(state/'mutations').mkdir(mode=0o700)
            old={**self.artifact,'package_sha256':self.subject['installed_package_sha256'],'binary_sha256':'4'*64}
            unit=root/(name+'.service');unit.write_text('[Service]\nUser=fixture\nExecStart='+str(release/old['package_sha256']/'borg-runtime')+(' --incarnation ${INVOCATION_ID}' if role=='worker' else ' --protected-release-config '+controller_pin['path'])+' --limits '+policy['path']+'\n');unit.chmod(0o600)
            new=root/'new.service';new.write_bytes(unit.read_bytes().replace(old['package_sha256'].encode(),self.artifact['package_sha256'].encode()));new.chmod(0o600)
            h={'id':name,'role':role,'transport':{'kind':'local','ssh_config':None,'alias':None},'profile_path':str(self.root/'profile.json'),'state_root':str(control),'service':name+'.service','unit':pin(unit),'new_unit':pin(new),'release_root':str(release),'package':str(package_path),'old_artifact':old,'service_uid':os.geteuid(),'helper_uid':os.geteuid(),'incarnation':'systemd_invocation' if role=='worker' else None,'state':{'root':str(state),'format_file':str(fmt) if fmt else None,'database':str(db) if db else None,'policy':policy},'owner_lock':str(state/'owner'),'helpers':[str(state/'helpers')],'preserve_metadata':[str(state/'helpers'/'owner.binding')],'preserve_files':[],'api':{'host':'127.0.0.1','port':1,'worker_id':'worker' if role=='worker' else None},'systemctl':pin('/usr/bin/true')}
            self.profile['hosts'].append(h);self.active[name]=True;self.invocations[name]='old-invocation'
        self.profile_path=self.root/'profile.json';save(self.profile_path,self.profile);self.profile_sha=pin(self.profile_path)['sha256']
    def leaf_trust(self,path,directory=False):
        # Explicit test-only host ownership boundary. Private test tree may have
        # shared CI ancestors; all leaf type/link/mode and actual bytes retained.
        p=Path(path)
        if not p.is_absolute() or p!=p.resolve(strict=True):raise ValueError('fixture path alias')
        st=p.lstat()
        if directory:
            if not p.is_dir():raise ValueError('fixture directory')
        elif not p.is_file() or st.st_nlink!=1 or st.st_mode&0o022:raise ValueError('fixture file protection')
        return p
    @contextlib.contextmanager
    def seams(self):
        owner=self
        class ControlledHost(hostmod.Host):
            def service(self):
                return {'ActiveState':'active' if owner.active[self.h['id']] else 'inactive','SubState':'running' if owner.active[self.h['id']] else 'dead','MainPID':'42' if owner.active[self.h['id']] else '0','ControlGroup':'/fixture','FragmentPath':self.h['unit']['path'],'DropInPaths':'','NeedDaemonReload':'no','InvocationID':owner.invocations[self.h['id']],'User':'fixture'}
            def check_running(self,new):
                row=self.service();expected=self.h['new_unit'] if new else self.h['unit']
                if not owner.active[self.h['id']] or common.digest(Path(self.h['unit']['path']).read_bytes())!=expected['sha256']:raise ValueError('fixture running unit differs')
                if new:
                    binary=Path(self.h['release_root'])/self.s['artifact']['package_sha256']/'borg-runtime'
                    if common.digest(binary.read_bytes())!=self.s['artifact']['binary_sha256']:raise ValueError('fixture installed binary differs')
                return row
            def pids(self,row):return [42] if row['MainPID']!='0' else []
            def command(self,args):
                owner.effects.append([self.h['id'],*args])
                if args[0]=='stop':owner.active[self.h['id']]=False
                elif args[0]=='start':
                    owner.active[self.h['id']]=True;owner.invocations[self.h['id']]='new-invocation'
                    if self.h['role']=='worker':
                        q={'operation':'register_worker','input':{'operation_id':'new-register','worker':{'id':'worker','incarnation':'new-invocation','coding_capacity':1,'native_capacity':0}}}
                        outcome={'status':'ok','data':{'kind':'worker','value':q['input']['worker']}}
                        save(Path(self.h['state']['root'])/'mutations/registration.json',{'limits':owner.runtime_limits,'revision':3,'request':q,'outcome':outcome,'outcome_context':{'commit':{'controller_revision':8,'operation_id':'new-register'},'observed_controller_revision':8},'consumed':False,'recovery_epoch':0})
                elif args!=['daemon-reload']:raise AssertionError('unselected controlled service command')
                return b''
            def tasks(self):return {'task-original':{'id':'task-original','status':'failed','attempt':{'stopped':True,'uncertain':True},'checkpoint':{'registration':'unchanged'}}}
            def api(self,operation,inp):
                assert operation=='get_operation';j=common.strict((Path(self.h['state']['root'])/'mutations/registration.json').read_bytes())
                return {'request_id':'rollout-read','commit':j['outcome_context']['commit'],'observed_controller_revision':9,'status':'ok','data':{'kind':'operation','value':{'id':inp['operation_id'],'task_id':'','attempt_id':'','generation':0,'kind':'application_mutation','intended':j['request'],'state':'settled','outcome':j['outcome']}}}
        def transport(argv,limits,deadline_ms,data=b''):
            req=common.strict(data);hid=argv[argv.index('--host')+1];h=next(h for h in self.profile['hosts'] if h['id']==hid)
            result=hostmod.phase(self.profile,h,req)
            if self.lost_phase==[hid,req['phase']]:self.lost_phase=None;raise ValueError('controlled lost host ACK after original effect')
            return common.canonical(result)
        with contextlib.ExitStack() as stack:
            for c in (adapter.c,hostmod.c,common):stack.enter_context(patch.object(c,'protected',side_effect=self.leaf_trust))
            stack.enter_context(patch.object(hostmod,'Host',ControlledHost));stack.enter_context(patch.object(adapter.c,'run',side_effect=transport))
            yield
    def rollout(self,action='rollout'):
        return adapter.Rollout(self.profile,self.profile_path,self.profile_sha,16777216,self.subject,action)

class FakeProducer:
    def __init__(self,subject):
        self.action={'id':'rollout','kind':'rollout','subject':subject,'check_id':'check','grant_id':'grant','state':'prepared','observation':None,'dispatch_until_ms':None};self.receipts={};self.calls=[];self.fail=None
    def exchange(self,config,q,**kwargs):
        self.calls.append(copy.deepcopy(q));op=q['command']['operation'];id=q['operation_id']
        if op=='get_action':return {'status':'ok','data':copy.deepcopy(self.action),'replayed':False}
        if op=='get_receipt':return {'status':'ok','data':copy.deepcopy(self.receipts[q['command']['input']['operation_id']]),'replayed':False} if q['command']['input']['operation_id'] in self.receipts else {'status':'not_found','data':None,'replayed':False}
        if self.fail=='before':self.fail=None;raise ValueError('controlled loss before commit')
        if id in self.receipts:
            assert self.receipts[id]['request']==q;return {**copy.deepcopy(self.receipts[id]['reply']),'replayed':True}
        if op=='start_rollout':self.action.update(state='started',dispatch_until_ms=common.now_ms()+60000)
        elif op=='observe_rollout':self.action.update(state='applied' if q['command']['input']['observed'] else 'unknown',observation=q['command']['input']['installed_package_sha256'])
        else:raise AssertionError(op)
        reply={'status':'ok','data':copy.deepcopy(self.action),'replayed':False}
        self.receipts[id]={'producer':{'id':'rollout','uid':os.geteuid(),'role':'rollout'},'request':copy.deepcopy(q),'reply':copy.deepcopy(reply)}
        if self.fail=='after':self.fail=None;raise ValueError('controlled loss after commit')
        return reply

def integration_main():
    """Root-only actual protected Unix transport; fake service seam stays explicit."""
    import sys
    value=common.strict(Path(sys.argv[1]).read_bytes())
    package_path=private_input_package(Path(value['output']),value['package'],value['subject']['artifact'])
    f=Fixture(Path(value['output']),package_path,value['subject'],value['controller'],value['producer'])
    with f.seams():
        r=f.rollout(value['action_id']);real_exchange=r.producer.exchange
        loss=value['loss'];seen=[]
        def exchange(config,q,**kwargs):
            seen.append(copy.deepcopy(q))
            if q['command']['operation']=='start_rollout':
                if loss=='before':raise ValueError('controlled transport loss before send')
                reply=real_exchange(config,q,**kwargs)
                if loss=='after':raise ValueError('controlled transport loss after commit')
                return reply
            return real_exchange(config,q,**kwargs)
        r.producer.exchange=exchange
        first=r.execute('apply');effects=copy.deepcopy(f.effects)
        recovered=f.rollout(value['action_id']);second=recovered.execute('reconcile')
        observation={'status':'OBSERVED_NOT_ADJUDICATED','loss':loss,'first':first,'second':second,'effects':effects,'effects_after_reconcile':copy.deepcopy(f.effects),'requests':seen}
        save(Path(value['output'])/'integration-observation.json',observation)
        assert f.effects==effects,('recovery repeated service effect',observation)
        assert sum(q['command']['operation']=='start_rollout' for q in seen)==1,observation
        if loss=='none':
            assert first['status']==second['status']=='APPLIED',(first,second)
        elif loss in ('before','after'):
            assert first['status']==second['status']=='UNKNOWN',(first,second)
            assert effects==[],effects
        else:raise AssertionError('unknown fixture case')
        save(Path(value['output'])/'integration-result.json',{'status':'PASS_CONTROLLED_HOST_ACTUAL_PROTECTED_PROTOCOL','loss':loss,'first':first,'second':second,'effects':effects,'requests':seen,'no_effect_retry':True})

if __name__=='__main__':integration_main()
