#!/usr/bin/env python3
"""Fixed local host phases, invoked only through installed operator metadata.

No account/credential/cgroup setup, reset, migration, arbitrary command, rollback,
or uncertain phase replay. Systemd/HTTP are the explicit integration test seam.
"""
from __future__ import annotations
import argparse, contextlib, fcntl, hashlib, importlib.util, json, os, stat, struct, sys, tarfile, time
from pathlib import Path
_COMMON_SHA = '036041acb527b629e08f2f71749f6015b4ce8851cbba6a8fbceeec3688677868'

def _common():
    path=Path(__file__).with_name('rollout_common_v2.py'); raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=_COMMON_SHA: raise ValueError('installed rollout common module changed')
    m=importlib.util.module_from_spec(importlib.util.spec_from_file_location('rollout_common_v2',path));exec(compile(raw,str(path),'exec'),m.__dict__);return m
c=_common()


class Host:
    def __init__(self,profile,host,subject,request):
        c.fields(subject,'release_id artifact repository target_ref expected_base policy deployment_target target_revision installed_package_sha256','subject')
        for key in ('policy','deployment_target','target_revision'):
            if subject[key]!=profile[key]:raise ValueError('host subject/profile differs')
        if subject['installed_package_sha256']!=host['old_artifact']['package_sha256']:raise ValueError('host installed baseline differs')
        self.p=profile;self.h=host;self.s=subject;self.r=request;self.l=profile['limits']
        self.root=Path(host['state_root']);c.protected(self.root,directory=True)
        if os.geteuid()!=host['helper_uid']: raise ValueError('host helper UID differs')
        self.work=self.root/('action-'+c.digest(c.canonical({'action':request['action_id'],'subject':subject})))
    def command(self,args):
        c.pin(self.h['systemctl'],self.l['module_bytes'])
        deadline=self.r['deadline_ms']
        if args[0] in ('stop','start','daemon-reload'):deadline=min(deadline,self.r['dispatch_until_ms']-self.l['clock_uncertainty_ms'])
        return c.run([self.h['systemctl']['path'],*args],self.l,deadline)
    def service(self):
        names=['ActiveState','SubState','MainPID','ControlGroup','FragmentPath','DropInPaths','NeedDaemonReload','InvocationID','User']
        raw=self.command(['show',self.h['service'],'--property='+','.join(names)]).decode()
        values={}
        for line in raw.splitlines():
            k,sep,v=line.partition('=')
            if not sep or k not in names or k in values: raise ValueError('service observation shape')
            values[k]=v
        if set(values)!=set(names) or values['FragmentPath']!=self.h['unit']['path'] or values['DropInPaths'] or values['NeedDaemonReload']!='no': raise ValueError('service identity/drop-ins/reload changed')
        return values
    def pids(self,row):
        group=row['ControlGroup']
        if not group: return []
        if not group.startswith('/') or '..' in Path(group).parts: raise ValueError('service cgroup identity')
        root=Path('/sys/fs/cgroup')/group.lstrip('/');out=set();count=0
        for path,dirs,files in os.walk(root,followlinks=False):
            count+=len(dirs)+len(files)
            if count>self.l['state_entries']: raise ValueError('cgroup entry bound')
            if any((Path(path)/n).is_symlink() for n in dirs+files): raise ValueError('cgroup alias')
            if 'cgroup.procs' in files:
                raw=c.read(Path(path)/'cgroup.procs',self.l['command_output_bytes'])
                out.update(int(x) for x in raw.split())
        return sorted(out)
    def pid_binary(self,pid):
        path=Path('/proc')/str(pid)/'exe'
        with path.open('rb') as f:
            raw=f.read(self.l['package_bytes']+1)
        if len(raw)>self.l['package_bytes']: raise ValueError('running executable bound')
        return c.digest(raw),str(path.resolve())
    def check_running(self,new):
        row=self.service();a=self.s['artifact'] if new else self.h['old_artifact']
        unit=self.h['new_unit'] if new else self.h['unit']
        if c.digest(c.read(self.h['unit']['path'],self.l['module_bytes'],True))!=unit['sha256']: raise ValueError('current unit differs')
        if row['ActiveState']!='active' or row['SubState']!='running' or int(row['MainPID'])<=0: raise ValueError('service not running')
        installed=Path(self.h['release_root'])/a['package_sha256']
        if c.digest(c.read(installed/'manifest.json',self.l['module_bytes'],True))!=a['manifest_sha256']:raise ValueError('installed manifest differs')
        sha,path=self.pid_binary(int(row['MainPID']))
        if sha!=a['binary_sha256'] or path!=str(Path(self.h['release_root'])/a['package_sha256']/'borg-runtime'): raise ValueError('running package/binary differs')
        status=c.read(Path('/proc')/row['MainPID']/'status',self.l['command_output_bytes']).decode()
        uids=next(line.split()[1:] for line in status.splitlines() if line.startswith('Uid:'))
        if len(uids)!=4 or any(int(x)!=self.h['service_uid'] for x in uids): raise ValueError('service process UID differs')
        if ('--limits '+self.h['state']['policy']['path']).encode() not in c.read(self.h['unit']['path'],self.l['module_bytes'],True):raise ValueError('service runtime policy path differs')
        if self.h['role']=='worker' and b'--incarnation ${INVOCATION_ID}' not in c.read(self.h['unit']['path'],self.l['module_bytes'],True): raise ValueError('worker incarnation convention not present in pinned unit')
        if self.h['role']=='controller':
            flag=('--protected-release-config '+self.p['controller_config']['path']).encode()
            if flag not in c.read(self.h['unit']['path'],self.l['module_bytes'],True): raise ValueError('service protected configuration binding differs')
        if int(row['MainPID']) not in self.pids(row): raise ValueError('main process outside service group')
        return row
    def metadata(self):
        out={'pinned_file_hashes':{item['path']:c.digest(c.pin(item,self.l['module_bytes'])) for item in self.h['preserve_files']}}
        for path in self.h['preserve_metadata']:
            p=Path(path);m=p.lstat()
            if stat.S_ISLNK(m.st_mode): raise ValueError('preserved path alias')
            out[path]=[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode),m.st_size,m.st_mtime_ns]
        return out
    def state_files(self):
        root=Path(self.h['state']['root']);out={};total=0;count=0
        if root.is_symlink(): raise ValueError('state root alias')
        for path,dirs,files in os.walk(root,followlinks=False):
            count+=len(dirs)+len(files)
            if count>self.l['state_entries']: raise ValueError('state inventory entry bound')
            for name in sorted(dirs):
                p=Path(path)/name
                if p.is_symlink(): raise ValueError('state directory alias')
            for name in sorted(files):
                p=Path(path)/name;m=p.lstat()
                if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1: raise ValueError('state nonregular/aliased file')
                raw=c.read(p,self.l['state_bytes']);total+=len(raw)
                if total>self.l['state_bytes']: raise ValueError('state inventory byte bound')
                out[str(p.relative_to(root))]={'sha256':c.digest(raw),'identity':[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]}
        return out
    def compatibility(self):
        state=self.h['state'];expected=self.p['formats']['before']
        c.pin(state['policy'],self.l['state_bytes'])
        # The live main-file header can be stale while WAL is authoritative.
        # Controller startup + expected-wire reads prove the live format; a
        # stopped, locked full-sidecar copy is inspected before any install.
        if self.h['role']=='worker':
            if c.strict(c.read(state['format_file'],self.l['receipt_bytes']))!={'format_revision':expected['worker_format']}: raise ValueError('worker format differs; no reset or migration')
            if c.strict(c.read(Path(state['root'])/'limits.json',self.l['state_bytes']))!=c.strict(c.pin(state['policy'],self.l['state_bytes'])): raise ValueError('worker retained policy differs')
    def sqlite_snapshot(self,label):
        # Caller holds the existing owner lock after verified service shutdown.
        # Preserve exact DB/WAL/SHM bytes separately from the disposable reader.
        c.pin(self.p['sqlite_reader']['extension'],self.l['module_bytes'])
        c.pin(self.p['sqlite_reader']['library'],self.l['module_bytes'])
        import _sqlite3, sqlite3
        if Path(_sqlite3.__file__).resolve()!=Path(self.p['sqlite_reader']['extension']['path']): raise ValueError('SQLite reader extension differs')
        maps=c.read('/proc/self/maps',self.l['module_bytes']).decode()
        libraries={line.split()[-1] for line in maps.splitlines() if 'libsqlite3.so' in line and line.split()[-1].startswith('/')}
        if libraries!={self.p['sqlite_reader']['library']['path']}: raise ValueError('SQLite reader library differs')
        before=self.state_files();out=self.work/label;out.mkdir(mode=0o700);c.sync(self.work)
        evidence=out/'bytes';reader=out/'reader';evidence.mkdir(mode=0o700);reader.mkdir(mode=0o700)
        db=Path(self.h['state']['database'])
        if db.with_name(db.name+'-journal').exists(): raise ValueError('rollback-journal state outside WAL procedure')
        for suffix in ('','-wal','-shm'):
            path=Path(str(db)+suffix)
            if path.exists() or path.is_symlink():
                raw=c.read(path,self.l['state_bytes']);c.write_new(evidence/path.name,raw);c.write_new(reader/path.name,raw)
        end=min(self.r['deadline_ms'],c.now_ms()+self.l['command_timeout_ms'])
        conn=sqlite3.connect((reader/db.name).as_uri()+'?mode=ro',uri=True,timeout=max(0,end-c.now_ms())/1000)
        try:
            conn.set_progress_handler(lambda:int(c.now_ms()>=end),self.l['sqlite_steps'])
            conn.execute('PRAGMA query_only=ON')
            if conn.execute('PRAGMA quick_check').fetchall()!=[('ok',)]: raise ValueError('copied SQLite integrity failed')
            schema=conn.execute('PRAGMA user_version').fetchone()[0]
            if schema!=self.p['formats']['before']['controller_schema']: raise ValueError('stopped schema differs; no reset or migration')
            names=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchmany(self.l['sqlite_tables']+1)]
            if len(names)>self.l['sqlite_tables']: raise ValueError('copied SQLite table bound')
            tables={};total=0
            for name in names:
                quoted='"'+name.replace('"','""')+'"';columns=[r[1] for r in conn.execute('PRAGMA table_info('+quoted+')')]
                rows=conn.execute('SELECT * FROM '+quoted).fetchmany(self.l['sqlite_rows']+1)
                if len(rows)>self.l['sqlite_rows']: raise ValueError('copied SQLite row bound')
                rows=[[{'blob_hex':v.hex()} if isinstance(v,bytes) else v for v in row] for row in rows]
                tables[name]={'columns':columns,'rows':sorted(rows,key=c.canonical)}
                total+=len(c.canonical(tables[name]))
                if total>self.l['state_bytes']: raise ValueError('copied SQLite logical byte bound')
        finally: conn.close()
        if self.state_files()!=before: raise ValueError('stopped original state changed during copy')
        value={'schema':schema,'tables':tables,'physical':before};c.save(out/'snapshot.json',value,self.l['state_bytes']);return value
    def clean(self):
        for registry in self.h['helpers']:
            root=Path(registry);count=0
            if not root.exists(): raise ValueError('helper registry missing')
            for path,dirs,files in os.walk(root,followlinks=False):
                count+=len(dirs)+len(files)
                if count>self.l['state_entries']: raise ValueError('helper registry bound')
                if any((Path(path)/name).is_symlink() for name in dirs+files): raise ValueError('helper registry alias')
                if any(name.endswith('.json') and name[:-5].isdigit() for name in files): raise ValueError('unfinished helper ownership remains')
        if self.h['role']=='worker':
            root=Path(self.h['state']['root'])
            commands=list((root/'commands').glob('*.json'))
            if len(commands)>self.l['state_entries']: raise ValueError('worker command inventory bound')
            for p in commands:
                row=c.strict(c.read(p,self.l['state_bytes']))
                if row['command']['kind']=='run_attempt' and row['completed'] is not True: raise ValueError('unfinished worker command')
            for path in (root/'outbox',root/'outbox/acceptance'):
                if any(path.glob('*.json')): raise ValueError('pending worker report')
    @contextlib.contextmanager
    def owner(self):
        path=Path(self.h['owner_lock']);fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            m=os.fstat(fd)
            if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1: raise ValueError('owner lock type')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            if (m.st_dev,m.st_ino)!=(path.stat().st_dev,path.stat().st_ino): raise ValueError('owner lock replaced')
            yield
        finally: os.close(fd)
    def stopped(self):
        row=self.service()
        if row['ActiveState']!='inactive' or row['MainPID']!='0' or self.pids(row): raise ValueError('service/process group not verified stopped')
        self.clean();return row
    def api(self,operation,inp):
        a=self.h['api']
        if a['host'] not in ('127.0.0.1','::1'): raise ValueError('controller API must use existing loopback route')
        q={'protocol_revision':self.p['formats']['after']['wire'],'request_id':'rollout-read','operation':operation,'input':inp}
        reply=c.strict(c.http_read(a['host'],a['port'],c.canonical(q),self.l,self.r['deadline_ms']))
        if reply['request_id']!=q['request_id'] or reply['status']!='ok' or type(reply.get('observed_controller_revision')) is not int: raise ValueError('controller read refused')
        if operation=='get_operation' and reply['data']['kind']=='operation' and reply['data']['value']['kind']=='application_mutation':
            stamp=reply['commit'];c.fields(stamp,'operation_id controller_revision','original commit')
            if stamp['operation_id']!=inp['operation_id'] or type(stamp['controller_revision']) is not int or not 0<stamp['controller_revision']<=reply['observed_controller_revision']:raise ValueError('original mutation commit stamp differs')
        elif reply['commit'] is not None:raise ValueError('unexpected commit on pure read')
        return reply
    def tasks(self):
        after=None;tasks={}
        for _ in range(self.l['api_pages']):
            reply=self.api('list_tasks',{'after':after,'limit':self.l['api_page_items']});data=reply['data']
            if data['kind']!='tasks': raise ValueError('task projection differs')
            rows=data['value']
            if not rows: return tasks
            for t in rows:
                if t['id'] in tasks or t['status'] not in ('succeeded','failed','cancelled') or (t['attempt'] is not None and not t['attempt']['stopped']): raise ValueError('task not quiesced/unique')
                tasks[t['id']]=t
            after=rows[-1]['id']
        raise ValueError('task inventory page bound')
    def ready(self):
        end=min(self.r['deadline_ms'],c.now_ms()+self.l['command_timeout_ms'])
        while True:
            try:
                row=self.check_running(True)
                if self.h['role']=='controller': self.tasks()
                else: self.registration(row)
                return row
            except (ConnectionError,FileNotFoundError) as error: pass
            except ValueError as error:
                if str(error) not in ('service not running','new worker registration not committed'): raise
            if c.now_ms()>=end: raise ValueError('service readiness deadline; state requires reconciliation')
            time.sleep(min(self.l['readiness_poll_ms'],end-c.now_ms())/1000)
    def registration(self,row):
        path=Path(self.h['state']['root'])/'mutations/registration.json';j=c.strict(c.read(path,self.l['state_bytes']))
        c.fields(j,'limits revision request outcome outcome_context consumed recovery_epoch','registration journal')
        q=j['request'];w=q['input']['worker'];context=j['outcome_context']
        policy=c.strict(c.pin(self.h['state']['policy'],self.l['state_bytes']))
        if j['limits']!=policy or type(j['consumed']) is not bool or type(j['recovery_epoch']) is not int:raise ValueError('registration retained policy/shape differs')
        if w['coding_capacity']!=policy['worker']['coding_capacity'] or w['native_capacity']!=policy['worker']['native_capacity']:raise ValueError('registration capacity differs from retained policy')

        if j['revision']!=3 or q['operation']!='register_worker' or w['id']!=self.h['api']['worker_id'] or w['incarnation']!=row['InvocationID'] or j['outcome'] is None or j['outcome'].get('status')!='ok' or j['outcome_context'] is None: raise ValueError('new worker registration not committed')
        reply=self.api('get_operation',{'operation_id':q['input']['operation_id']});r=reply['data']
        c.fields(context,'commit observed_controller_revision','original registration context')
        if context['commit']!=reply['commit'] or not 0<context['commit']['controller_revision']<=context['observed_controller_revision']<=reply['observed_controller_revision']:raise ValueError('canonical registration commit differs')
        if j['outcome']!={'status':'ok','data':{'kind':'worker','value':w}}:raise ValueError('registered worker outcome differs')
        if r['kind']!='operation' or r['value']['intended']!=q or r['value']['outcome']!=j['outcome'] or r['value']['state']!='settled': raise ValueError('canonical registration differs')
        return {'request':q,'outcome':j['outcome'],'context':j['outcome_context']}
    def clock_now(self):
        if c.now_ms()+self.l['clock_uncertainty_ms']>=min(self.r['deadline_ms'],self.r['dispatch_until_ms']): raise ValueError('dispatch authorization expired before effect')
    def bound_now(self):
        # Every blocking observation precedes this last check at each effect.
        raw=c.read(self.r['profile_path'],self.r['profile_bytes'],True)
        if c.digest(raw)!=self.r['profile_sha256']: raise ValueError('target/profile changed before effect')
        for item in self.p['modules'].values():c.pin(item,self.l['module_bytes'])
        self.compatibility()
        for item in self.h['preserve_files']:c.pin(item,self.l['module_bytes'])
        if self.h['role']=='controller':c.pin(self.p['controller_config'],self.r['profile_bytes'])
        self.clock_now()
    def install(self):
        self.stopped()
        with self.owner():
            before=self.state_files();meta=self.metadata()
            stop=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))
            if before!=stop['state']: raise ValueError('runtime state changed since owned stop')
            if self.h['role']=='controller' and stop['snapshot']['schema']!=self.p['formats']['before']['controller_schema']: raise ValueError('no stopped schema proof')
            unit_stat=Path(self.h['unit']['path']).lstat();unit_identity=(unit_stat.st_dev,unit_stat.st_ino,unit_stat.st_uid,unit_stat.st_gid,stat.S_IMODE(unit_stat.st_mode))
            old=c.read(self.h['unit']['path'],self.l['module_bytes'],True);new=c.pin(self.h['new_unit'],self.l['module_bytes'])
            oldpath=str(Path(self.h['release_root'])/self.h['old_artifact']['package_sha256']/'borg-runtime').encode()
            newpath=str(Path(self.h['release_root'])/self.s['artifact']['package_sha256']/'borg-runtime').encode()
            if old.count(oldpath)!=1 or old.replace(oldpath,newpath)!=new: raise ValueError('unit change exceeds exact executable path')
            raw,m=c.verify_package(self.p,self.h,self.s);release=Path(self.h['release_root'])/self.s['artifact']['package_sha256']
            c.protected(release.parent,directory=True);self.bound_now()
            if release.exists(): raise ValueError('new release path already exists; reconcile instead of overwrite')
            self.clock_now();release.mkdir(mode=0o755);c.sync(release.parent)
            # Verified canonical tar has exactly these two members. No extractall.
            import io
            with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as tf:
                for name,target,mode in [('bin/borg-runtime','borg-runtime',0o555),('manifest.json','manifest.json',0o444)]:
                    body=tf.extractfile(name).read();self.clock_now();c.write_new(release/target,body,mode)
            c.sync(release);self.bound_now()
            temporary=Path(self.h['unit']['path']).with_name(self.h['service']+'.rollout-'+c.digest(self.r['action_id'].encode()))
            c.write_new(temporary,new,0o600)
            fd=os.open(temporary,os.O_WRONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                os.fchown(fd,unit_stat.st_uid,unit_stat.st_gid);os.fchmod(fd,stat.S_IMODE(unit_stat.st_mode));os.fsync(fd)
            finally:os.close(fd)
            c.sync(temporary.parent);self.bound_now()
            if c.read(self.h['unit']['path'],self.l['module_bytes'],True)!=old: raise ValueError('unit changed during install')
            latest=Path(self.h['unit']['path']).lstat()
            if (latest.st_dev,latest.st_ino,latest.st_uid,latest.st_gid,stat.S_IMODE(latest.st_mode))!=unit_identity:raise ValueError('unit metadata changed during install')
            self.clock_now();os.replace(temporary,self.h['unit']['path']);c.sync(temporary.parent)
            self.bound_now();self.command(['daemon-reload'])
            if self.state_files()!=before or self.metadata()!=meta: raise ValueError('install altered retained state/metadata')
            return {'state':before,'preserved_metadata':meta,'unit_sha256':c.digest(new),'artifact':self.s['artifact']}
    def execute(self,phase):
        self.compatibility();c.verify_package(self.p,self.h,self.s)
        if phase=='preflight':
            row=self.check_running(False);tasks=self.tasks() if self.h['role']=='controller' else None
            return {'service':row,'tasks':tasks,'metadata':self.metadata(),'formats':self.p['formats']['before']}
        if phase=='stop':
            row=self.check_running(False);self.bound_now();self.command(['stop',self.h['service']]);row=self.stopped()
            with self.owner():
                state=self.state_files();snapshot=self.sqlite_snapshot('stopped-copy') if self.h['role']=='controller' else None
            return {'service':row,'state':state,'metadata':self.metadata(),'snapshot':snapshot}
        if phase=='install': return self.install()
        if phase=='start':
            self.stopped()
            with self.owner():pass
            if c.digest(c.read(self.h['unit']['path'],self.l['module_bytes'],True))!=self.h['new_unit']['sha256']: raise ValueError('new unit differs')
            self.bound_now();self.command(['start',self.h['service']]);row=self.ready()
            if self.h['role']=='controller':
                self.tasks() # actual expected-wire read after canonical startup
                self.bound_now();self.command(['stop',self.h['service']]);self.stopped()
                with self.owner(): after=self.sqlite_snapshot('restart-copy')
                before=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))['snapshot']
                if (after['schema'],after['tables'])!=(before['schema'],before['tables']): raise ValueError('restart changed original logical controller state')
                self.bound_now();self.command(['start',self.h['service']]);row=self.ready();self.tasks()
            return {'service':row,'registration':self.registration(row) if self.h['role']=='worker' else None}
        if phase=='inspect':
            row=self.check_running(True)
            stop=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))
            preserved={}
            if self.h['role']=='worker':
                current=self.state_files()
                mutable={'mutations/registration.json','mutations/await.json','worker-owner.json'}
                for name,value in stop['state'].items():
                    if name not in mutable and current.get(name)!=value: raise ValueError('original worker history changed')
                    if name not in mutable: preserved[name]=value
            receipt_ids=[]
            if self.h['role']=='controller':
                table=stop['snapshot']['tables'].get('receipts',{'columns':[],'rows':[]})
                for values in table['rows']:
                    old=dict(zip(table['columns'],values));reply=self.api('get_operation',{'operation_id':old['id']});data=reply['data']
                    if reply['commit']!={'operation_id':old['id'],'controller_revision':old['controller_revision']}:raise ValueError('original mutation commit changed')
                    if data['kind']!='operation' or data['value']['intended']!=c.strict(old['request']) or data['value']['outcome']!=c.strict(old['outcome']) or data['value']['state']!='settled':raise ValueError('original controller mutation receipt changed')
                    receipt_ids.append(old['id'])
            proofs={}
            for phase in ('stop','install','start'):
                ip=self.work/(phase+'.intent.json');rp=self.work/(phase+'.result.json')
                proofs[phase]={'request':c.strict(c.read(ip,self.l['receipt_bytes'],True)) if ip.exists() else None,'result':c.strict(c.read(rp,self.l['receipt_bytes'],True)) if rp.exists() else None}
            return {'preserved_controller_receipts':receipt_ids,'preserved_worker_state':preserved,'phase_proofs':proofs,'service':row,'metadata':self.metadata(),'tasks':self.tasks() if self.h['role']=='controller' else None,
                    'registration':self.registration(row) if self.h['role']=='worker' else None,'artifact':self.s['artifact']}
        raise ValueError('unsupported fixed phase')


def phase(profile,host,request):
    c.fields(request,'revision action_id subject profile_path profile_sha256 profile_bytes phase dispatch_until_ms deadline_ms','host request')
    if request['revision']!=1:raise ValueError('host protocol revision')
    if request['phase'] not in ('preflight','stop','install','start','inspect'):raise ValueError('closed host phase')
    if not isinstance(request['action_id'],str) or not request['action_id']:raise ValueError('host action identity')
    for name in ('deadline_ms','profile_bytes'):c.positive(request[name],name)
    if type(request['dispatch_until_ms']) is not int or request['dispatch_until_ms']<0:raise ValueError('host dispatch deadline')
    h=Host(profile,host,request['subject'],request);bound=profile['limits']['receipt_bytes']
    with c.locked(h.root/'host.lock'):
        if not h.work.exists():h.work.mkdir(mode=0o700);c.sync(h.root)
        intent=h.work/(request['phase']+'.intent.json');result=h.work/(request['phase']+'.result.json')
        if request['phase'] in ('preflight','inspect'):return h.execute(request['phase'])
        if intent.exists():
            if c.strict(c.read(intent,bound,True))!=request:raise ValueError('phase identity conflict')
            if result.exists():return c.strict(c.read(result,bound,True))
            raise ValueError('possible original phase effect; read-only reconciliation required')
        c.save(intent,request,bound)
        value=h.execute(request['phase']);c.save(result,value,bound);return value


def main():
    p=argparse.ArgumentParser();p.add_argument('--profile',required=True);p.add_argument('--profile-sha256',required=True);p.add_argument('--profile-bytes',required=True,type=int);p.add_argument('--host',required=True);a=p.parse_args()
    profile=c.load_profile(a.profile,a.profile_sha256,a.profile_bytes)
    host=next(h for h in profile['hosts'] if h['id']==a.host)
    raw=sys.stdin.buffer.read(profile['limits']['transport_bytes']+1)
    if len(raw)>profile['limits']['transport_bytes']:raise ValueError('host request bound')
    req=c.strict(raw)
    if req['profile_path']!=a.profile or req['profile_sha256']!=a.profile_sha256 or req['profile_bytes']!=a.profile_bytes:raise ValueError('host profile binding')
    value=phase(profile,host,req);raw=c.canonical(value)
    if len(raw)>profile['limits']['transport_bytes']:raise ValueError('host reply bound')
    sys.stdout.buffer.write(raw+b'\n')
if __name__=='__main__':
    try:main()
    except Exception as e:print('protected rollout host: '+str(e),file=sys.stderr);sys.exit(1)
