#!/usr/bin/env python3
"""Operator-owned fixed rollout. Inactive without exact protected metadata.

`apply` sends one StartRollout and fixed host phases. `reconcile` is readback only:
it never repeats install/start/stop, even when a prior response was lost.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, shlex, stat, sys
from pathlib import Path
_COMMON_SHA = '436dd20bfb545383f27c941e48f4e131b0d60e006db0cf4d2c627e3e2cfa39f2'

def _common():
    p=Path(__file__).with_name('rollout_common_v2.py');raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=_COMMON_SHA:raise ValueError('installed rollout common changed')
    m=importlib.util.module_from_spec(importlib.util.spec_from_file_location('rollout_common_v2',p));exec(compile(raw,str(p),'exec'),m.__dict__);return m
c=_common()


def require_selected_reader(profile,role,executing_path):
    selected=profile['modules'][role]
    if (str(Path(executing_path).absolute())!=selected['path']
            or c.digest(c.read(executing_path,profile['limits']['module_bytes'],True))!=selected['sha256']):
        raise ValueError('unbound observation reader requires explicit binding')


def observation_arguments(args, scope):
    values=(args.observation_binding,args.observation_binding_sha256,args.observation_binding_bytes)
    if all(v is None for v in values):return None
    if any(v is None for v in values) or scope not in ('reconcile','inspect'):
        raise ValueError('observation binding requires readback-only scope')
    return dict(zip(('path','sha256','bytes'),values))


def validate_observation_binding(profile,path,sha,profile_bytes,subject,action_id,
                                 binding_pin,role,executing_path):
    """Select new observation code without replacing the original effect profile."""
    if role not in ('adapter','host'):raise ValueError('observation reader role')
    c.fields(binding_pin,'path sha256 bytes','observation binding pin')
    c.positive(binding_pin['bytes'],'observation binding byte bound')
    if binding_pin['bytes']>profile['limits']['module_bytes']:raise ValueError('observation binding byte bound')
    raw=c.pin({k:binding_pin[k] for k in ('path','sha256')},binding_pin['bytes'])
    if len(raw)!=binding_pin['bytes']:raise ValueError('observation binding length')
    value=c.strict(raw)
    c.fields(value,'revision kind action_id subject_sha256 original_intent_sha256 effect_profile original_modules observation_modules host_ids','observation binding')
    if type(value['revision']) is not int or value['revision']!=1 or value['kind']!='borg-v2-rollout-observation/1':raise ValueError('observation binding revision/kind')
    if value['action_id']!=action_id or value['subject_sha256']!=c.digest(c.canonical(subject)):
        raise ValueError('observation action/subject differs')
    if not c._hex(value['original_intent_sha256']):raise ValueError('observation original intent digest')
    effect={'path':str(path),'sha256':sha,'bytes':profile_bytes}
    if value['effect_profile']!=effect:raise ValueError('observation original profile differs')
    if c.load_profile(path,sha,profile_bytes)!=profile:raise ValueError('observation loaded profile differs')
    original={key:profile['modules'][key] for key in ('adapter','host','common')}
    if value['original_modules']!=original:raise ValueError('observation original modules differ')
    c.fields(value['observation_modules'],'adapter host','observation modules')
    if value['host_ids']!=sorted(h['id'] for h in profile['hosts']):raise ValueError('observation host set differs')
    for key,item in value['observation_modules'].items():
        c.fields(item,'path sha256','observation module')
        if item['path'] in {v['path'] for v in profile['modules'].values()}:
            raise ValueError('observation reader must preserve original modules')
        c.pin(item,profile['limits']['module_bytes'])
    selected=value['observation_modules'][role]
    if str(Path(executing_path).absolute())!=selected['path'] or c.digest(c.read(executing_path,profile['limits']['module_bytes'],True))!=selected['sha256']:
        raise ValueError('executing observation reader differs')
    if role=='adapter':
        work=Path(profile['state_root'])/('action-'+c.digest(action_id.encode()))
        intent=c.read(work/'intent.json',profile['limits']['receipt_bytes'],True)
        if c.digest(intent)!=value['original_intent_sha256']:raise ValueError('observation original intent differs')
        saved=c.strict(intent)
        if any(saved.get(k)!=v for k,v in {'revision':1,'action_id':action_id,'subject':subject,'profile_sha256':sha}.items()):
            raise ValueError('observation original intent binding differs')
    provenance={'binding':binding_pin,'action_id':action_id,'subject_sha256':value['subject_sha256'],
        'effect_profile':effect,'original_intent_sha256':value['original_intent_sha256'],
        'original_modules':original,'observation_modules':value['observation_modules']}
    return {'binding_pin':binding_pin,'descriptor':value,'provenance':provenance}


def verified_task_context(preflight_raw, result):
    preflight=c.strict(preflight_raw)
    if result['tasks']!=preflight['tasks']:raise ValueError('cleanup_complete_original_tasks_changed')
    return result['tasks'],{'original_preflight_sha256':c.digest(preflight_raw),
        'current_controller_proof_sha256':c.digest(c.canonical(result))}


def project_cleanup_authority(host, context):
    if context is None:raise ValueError('cleanup_controller_proof_required_first')
    tasks,binding=context;projected={}
    for task_id,task in tasks.items():
        attempt=task.get('attempt')
        if attempt is None or attempt.get('worker_id')!=host['api']['worker_id']:continue
        if task['id']!=task_id or task['status'] not in ('succeeded','failed','cancelled'):raise ValueError('cleanup_canonical_task_identity')
        result=task.get('result')
        projected[task_id]={'id':task_id,'session_id':task['session_id'],'assigned_agent_id':task['assigned_agent_id'],
            'status':task['status'],'input_sha256':c.digest(c.canonical(task['input'])),
            'attempt':attempt,'selected_checkpoint':task.get('selected_checkpoint'),
            'result_binding':None if result is None else {'attempt_id':result['attempt_id'],'checkpoint':result.get('checkpoint')},
            'task_sha256':c.digest(c.canonical(task))}
    return {'worker_id':host['api']['worker_id'],'workspace_root':str(Path(host['state']['root']).parent.parent),
        'tasks':projected,'binding':binding}


def saved_proof(path,bound,expected):
    """Recheck the exact durable private proof before publishing its reference."""
    path=c.protected(path)
    identity=lambda m:(m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_nlink,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
    before=path.lstat()
    if before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600 or before.st_size>bound:raise ValueError('saved_proof_metadata')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        if identity(os.fstat(fd))!=identity(before):raise ValueError('saved_proof_open_race')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(bound+1)
        if len(raw)>bound or len(raw)!=before.st_size or identity(os.fstat(fd))!=identity(before) or identity(path.lstat())!=identity(before):raise ValueError('saved_proof_changed')
    finally:os.close(fd)
    if raw!=c.canonical(expected):raise ValueError('saved_proof_content_changed')
    return {'path':str(path),'bytes':len(raw),'sha256':c.digest(raw)}


class Refused(ValueError):
    pass


class Rollout:
    def __init__(self,profile,path,sha,profile_bytes,subject,action_id,observation=None):
        self.p=profile;self.path=str(path);self.sha=sha;self.profile_bytes=profile_bytes;self.s=subject;self.action_id=action_id;self.l=profile['limits']
        c.fields(subject,'release_id artifact repository target_ref expected_base policy deployment_target target_revision installed_package_sha256','subject')
        for k in ('policy','deployment_target','target_revision'):
            if subject[k]!=profile[k]:raise ValueError('subject/profile '+k+' mismatch')
        self.producer=c.load_module('rollout_producer',profile['modules']['producer'],self.l['module_bytes'])
        raw=c.pin(profile['producer'],self.profile_bytes);self.config=c.strict(raw)
        c.fields(self.config,'socket server_uid producer_uid policy recipe_sha256 valid_for_ms','producer')
        self.producer_host=None
        if 'producer_transport' in profile:
            self.producer_host=next(h for h in profile['hosts'] if h['id']==profile['producer_transport']['host_id'])
        actual_uid=self.producer_host['helper_uid'] if self.producer_host else os.geteuid()
        if self.config['producer_uid']!=actual_uid or self.config['policy']!=subject['policy']:raise ValueError('producer UID/policy mismatch')
        self.controller=c.strict(c.pin(profile['controller_config'],self.profile_bytes))
        for k in ('socket','policy','recipe_sha256'):
            if self.controller[k]!=self.config[k]:raise ValueError('actual controller configuration differs')
        for k in ('repository','target_ref','deployment_target','target_revision','installed_package_sha256','policy'):
            if self.controller[k]!=subject[k]:raise ValueError('controller subject differs')
        if not any(x['uid']==actual_uid and x['role']=='rollout' for x in self.controller['producers']) or actual_uid in self.controller['worker_uids']:raise ValueError('configured rollout authority unavailable')
        self.producer_identity=next(x for x in self.controller['producers'] if x['uid']==actual_uid and x['role']=='rollout')
        self.work=Path(profile['state_root'])/('action-'+c.digest(action_id.encode()));self.original=None
        self.observation_pin=observation;self.observation=None
    def exchange(self,q):
        remaining=min(self.l['transport_timeout_ms'],self.end-c.now_ms())
        if remaining<=0:raise ValueError('operation deadline before protected transport')
        if self.producer_host is None:
            return self.producer.exchange(self.config,q,max_bytes=self.l['transport_bytes'],timeout_ms=remaining)
        raw=c.canonical(q)
        bound=self.l['transport_bytes']
        if len(raw)>bound:raise ValueError('remote producer request bound')
        h=self.producer_host;transport=self.p['producer_transport'];module=self.p['modules']['producer']
        c.pin(self.p['producer'],self.profile_bytes);c.pin(module,self.l['module_bytes'])
        c.pin(h['transport']['ssh_config'],self.profile_bytes)
        for item in self.p['transport_tools'].values():c.pin(item,self.l['module_bytes'])
        c.pin(self.p['python'],self.l['module_bytes'])
        remaining=min(remaining,self.end-c.now_ms())
        if remaining<=0:raise ValueError('operation deadline before remote producer')
        remote=[self.p['python']['path'],'-I','-S','-B',module['path'],'exchange-stdio',
                '--config',transport['config_path'],'--config-sha256',self.p['producer']['sha256'],
                '--module-sha256',module['sha256'],'--python-sha256',self.p['python']['sha256'],
                '--max-bytes',str(bound),'--timeout-ms',str(remaining)]
        argv=['/usr/bin/ssh','-F',h['transport']['ssh_config']['path'],'-T','-o','BatchMode=yes','-o','ControlMaster=no','-o','ControlPath=none',h['transport']['alias'],
              shlex.join(['/usr/bin/sudo','-n','/usr/bin/env','-i','PATH=/usr/bin:/bin',*remote])]
        limits={**self.l,'command_timeout_ms':min(self.l['command_timeout_ms'],remaining),'command_output_bytes':min(self.l['command_output_bytes'],bound)}
        reply=c.run(argv,limits,min(self.end,c.now_ms()+remaining),raw)
        if len(reply)>bound:raise ValueError('remote producer reply bound')
        return c.strict(reply)
    def read_action(self):
        q={'revision':1,'operation_id':self.action_id+':action-read','command':{'operation':'get_action','input':{'action_id':self.action_id}}}
        reply=self.exchange(q)
        if reply.get('status')!='ok':raise ValueError('current action read refused')
        a=reply['data']
        if a['id']!=self.action_id or a['kind']!='rollout' or a['subject']!=self.s:raise ValueError('current action binding differs')
        return a
    def mutation(self,name,command,recover=False):
        path=self.work/(name+'.request.json');replypath=self.work/(name+'.reply.json')
        q={'revision':1,'operation_id':self.action_id+':'+name,'command':command}
        if path.exists():
            if c.strict(c.read(path,self.l['receipt_bytes'],True))!=q:raise ValueError('original mutation body changed')
            lookup={'revision':1,'operation_id':self.action_id+':lookup:'+name,'command':{'operation':'get_receipt','input':{'operation_id':q['operation_id']}}}
            reply=self.exchange(lookup)
            if reply.get('status')=='not_found':raise ValueError('original mutation not committed; no automatic retry')
            if reply.get('status')!='ok':raise ValueError('original mutation lookup unavailable')
            record=reply['data']
            if record['request']!=q or record['producer']!=self.producer_identity:raise ValueError('original producer/request differs')
            # Lookup success is not admission success. Keep embedded refusal exact.
            result=record['reply']
        else:
            if recover:raise ValueError('no original mutation intent')
            c.save(path,q,self.l['receipt_bytes']);result=self.exchange(q)
        if not replypath.exists():c.save(replypath,result,self.l['receipt_bytes'])
        else:
            old=c.strict(c.read(replypath,self.l['receipt_bytes'],True))
            if {k:v for k,v in old.items() if k!='replayed'}!={k:v for k,v in result.items() if k!='replayed'}:raise ValueError('original mutation outcome changed')
        if result.get('status')!='ok':raise Refused('original protected mutation refused: '+str(result.get('reason',result.get('code','refused'))))
        return result['data']
    def original_start(self,recover=False):
        result=self.mutation('start',{'operation':'start_rollout','input':{'action_id':self.action_id,'subject':self.s}},recover)
        if result.get('state')!='started' or result.get('subject')!=self.s or result.get('id')!=self.action_id or result.get('kind')!='rollout' or type(result.get('dispatch_until_ms')) is not int:raise ValueError('original StartRollout admission invalid')
        self.original=result;return result
    def before_effect(self,controller_available=True):
        c.load_profile(self.path,self.sha,self.profile_bytes)
        c.pin(self.p['producer'],self.profile_bytes);c.pin(self.p['controller_config'],self.profile_bytes)
        if controller_available:
            current=self.read_action()
            if current['state']!='started' or any(current[k]!=self.original[k] for k in ('dispatch_until_ms','check_id','grant_id')):raise ValueError('current action is not originally Started')
        if c.now_ms()+self.l['clock_uncertainty_ms']>=min(self.end,self.original['dispatch_until_ms']):raise ValueError('original dispatch deadline expired')
    def host(self,h,phase,recovery=False):
        if self.observation_pin is not None:
            if phase!='inspect' or not recovery:raise ValueError('observation binding cannot dispatch an effect')
            self.observation=validate_observation_binding(self.p,self.path,self.sha,self.profile_bytes,
                self.s,self.action_id,self.observation_pin,'adapter',__file__)
        request={'revision':1,'action_id':self.action_id,'subject':self.s,'profile_path':h['profile_path'],'profile_sha256':self.sha,'profile_bytes':self.profile_bytes,'phase':phase,
                 'dispatch_until_ms':self.original['dispatch_until_ms'] if self.original else 0,'deadline_ms':self.end}
        if phase=='inspect' and h['role']=='worker':
            stop=c.strict(c.read(self.work/('stop-'+c.digest(h['id'].encode())+'.result.json'),self.l['receipt_bytes'],True))
            if stop.get('cleanup_records'):
                request['cleanup_authority']=project_cleanup_authority(h,getattr(self,'_cleanup_controller_context',None))
        key=phase+'-'+c.digest(h['id'].encode());path=self.work/(key+'.request.json');out=self.work/(key+'.result.json')
        if self.observation is not None:
            n=len(list(self.work.glob(key+'.readback-*.json')))
            if len(list(self.work.iterdir()))+2>self.l['state_entries']:raise ValueError('readback inventory bound')
            path=self.work/(key+'.readback-'+str(n)+'.request.json');out=self.work/(key+'.readback-'+str(n)+'.json')
        mutating=phase in ('stop','install','start')
        if mutating and path.exists():raise ValueError('possible host effect; read-only reconciliation required')
        if not path.exists():c.save(path,request,self.l['receipt_bytes'])
        helper=self.p['modules']['host'] if self.observation is None else self.observation['descriptor']['observation_modules']['host']
        argv=[self.p['python']['path'],'-I','-S','-B',helper['path'],'--profile',h['profile_path'],'--profile-sha256',self.sha,'--profile-bytes',str(self.profile_bytes),'--host',h['id']]
        if self.observation is not None:
            argv+=['--observation-binding',self.observation_pin['path'],'--observation-binding-sha256',self.observation_pin['sha256'],'--observation-binding-bytes',str(self.observation_pin['bytes'])]
        if h['transport']['kind']=='ssh':
            c.pin(h['transport']['ssh_config'],self.profile_bytes)
            argv=['/usr/bin/ssh','-F',h['transport']['ssh_config']['path'],'-T','-o','BatchMode=yes','-o','ControlMaster=no','-o','ControlPath=none',h['transport']['alias'],shlex.join(['/usr/bin/sudo','-n','/usr/bin/env','-i','PATH=/usr/bin:/bin',*argv])]
        request_raw=c.canonical(request)
        if len(request_raw)>self.l['transport_bytes']:raise ValueError('host request bound')
        raw=c.run(argv,self.l,self.end,request_raw)
        if len(raw)>self.l['transport_bytes']:raise ValueError('host response bound')
        result=c.strict(raw)
        if self.observation is not None and result.get('observation_reader')!=self.observation['provenance']:
            raise ValueError('host observation reader proof differs')
        if not out.exists():c.save(out,result,self.l['receipt_bytes'])
        elif recovery:
            n=len(list(self.work.glob(key+'.readback-*.json')))
            if len(list(self.work.iterdir()))>=self.l['state_entries']:raise ValueError('readback inventory bound')
            out=self.work/(key+'.readback-'+str(n)+'.json')
            c.save(out,result,self.l['receipt_bytes'])
        elif c.strict(c.read(out,self.l['receipt_bytes'],True))!=result:raise ValueError('host observation changed')
        if phase=='inspect':
            if not hasattr(self,'_full_proof_paths'):self._full_proof_paths={}
            self._full_proof_paths[h['id']]=out
        return result
    def final_readback(self,recovery=False):
        rows={};controller=next(h for h in self.p['hosts'] if h['role']=='controller')
        preflight_raw=c.read(self.work/('preflight-'+c.digest(controller['id'].encode())+'.result.json'),self.l['receipt_bytes'],True)
        before=c.strict(preflight_raw)
        self._cleanup_controller_context=None
        # Workers receive only authority projected from this same complete,
        # current controller proof, after exact original task-map equality.
        for h in [controller,*[h for h in self.p['hosts'] if h['role']=='worker']]:
            # Read original remote phase receipts, never repeat an effect to
            # resolve a lost transport acknowledgment.
            row=self.host(h,'inspect',recovery)
            for phase in ('stop','install','start'):
                key=phase+'-'+c.digest(h['id'].encode());qpath=self.work/(key+'.request.json');rpath=self.work/(key+'.result.json')
                proof=row['phase_proofs'][phase]
                if not qpath.exists() or proof['request']!=c.strict(c.read(qpath,self.l['receipt_bytes'],True)) or proof['result'] is None: raise ValueError('host phase completion unknown; no replay')
                if rpath.exists():
                    if c.strict(c.read(rpath,self.l['receipt_bytes'],True))!=proof['result']: raise ValueError('original host phase result changed')
                else:c.save(rpath,proof['result'],self.l['receipt_bytes'])
            prior=c.strict(c.read(self.work/('preflight-'+c.digest(h['id'].encode())+'.result.json'),self.l['receipt_bytes'],True))
            if row['artifact']!=self.s['artifact'] or row['metadata']!=prior['metadata']:raise ValueError('target artifact/preserved metadata differs')
            if h['role']=='controller' and row['tasks']!=before['tasks']:raise ValueError('complete original tasks changed')
            if h['role']=='controller':self._cleanup_controller_context=verified_task_context(preflight_raw,row)
            rows[h['id']]=row
        compact={}
        for host_id,row in rows.items():
            compact[host_id]={'full_proof':saved_proof(self._full_proof_paths[host_id],self.l['receipt_bytes'],row),
                'service':row['service'],'artifact':row['artifact'],'registration':row['registration'],
                'metadata_sha256':c.digest(c.canonical(row['metadata'])),
                'tasks_sha256':c.digest(c.canonical(row['tasks'])),
                'tasks_count':len(row['tasks']) if row['tasks'] is not None else None,
                'worker_preserved_records':len(row['preserved_worker_state']),
                'worker_preserved_sha256':c.digest(c.canonical(row['preserved_worker_state'])),
                'cleanup_observations_sha256':c.digest(c.canonical(row['cleanup_observations'])),
                'original_controller_receipts':len(row['preserved_controller_receipts']),
                'controller_receipt_proof':row.get('controller_receipt_proof'),
                'cleanup_capture_proof':row.get('cleanup_capture_proof'),
                'cleanup_transition_proof':row.get('cleanup_transition_proof'),
                'stop_marker_proof':row.get('stop_marker_proof'),
                'reconciliation_scope':row['reconciliation_scope']}
            if self.observation is not None:compact[host_id]['observation_reader']=row['observation_reader']
        if len(list(self.work.iterdir()))>=self.l['state_entries']:raise ValueError('proof reference inventory bound')
        index=len(list(self.work.glob('verified-host-proof-references-*.json')))
        c.save(self.work/('verified-host-proof-references-'+str(index)+'.json'),compact,self.l['receipt_bytes'])
        return compact
    def observe(self,observed):
        return self.mutation('observe-applied' if observed else 'observe-unknown',{'operation':'observe_rollout','input':{'action_id':self.action_id,'subject':self.s,'installed_package_sha256':self.s['artifact']['package_sha256'] if observed else 'unknown','observed':observed}})
    def execute(self,mode):
        if self.observation_pin is not None:
            if mode!='reconcile':raise ValueError('observation binding requires reconcile')
            self.observation=validate_observation_binding(self.p,self.path,self.sha,self.profile_bytes,
                self.s,self.action_id,self.observation_pin,'adapter',__file__)
        with c.locked(Path(self.p['state_root'])/'rollout.lock'):
            if not self.work.exists():self.work.mkdir(mode=0o700);c.sync(self.work.parent)
            intent=self.work/'intent.json'
            binding={'revision':1,'profile_sha256':self.sha,'subject':self.s,'action_id':self.action_id}
            if intent.exists():
                prior=c.strict(c.read(intent,self.l['receipt_bytes'],True))
                if any(prior[k]!=v for k,v in binding.items()):raise ValueError('retained rollout binding differs')
                if mode!='reconcile':raise ValueError('existing action requires read-only reconcile')
            else:
                if mode!='apply':raise ValueError('original action absent')
                prior={**binding,'started_at_ms':c.now_ms()};c.save(intent,prior,self.l['receipt_bytes'])
            # Reconciliation can observe after dispatch expiry, but is separately bounded.
            self.end=(c.now_ms() if mode=='reconcile' else prior['started_at_ms'])+self.l['overall_timeout_ms']
            result={'status':'UNKNOWN','action_id':self.action_id,'effect_retried':False,'automatic_rollback':False,'host_results':{}}
            if self.observation is not None:result['observation_reader']=self.observation['provenance']
            try:
                if mode=='apply':
                    current=self.read_action()
                    if current['state']!='prepared':raise ValueError('action is not freshly Prepared')
                    if c.per_host_forward(self.p):c.format_contract(self.p)
                    for h in self.p['hosts']:
                        # The protected Subject anchors the controller baseline;
                        # same-format workers retain their exact per-host baseline.
                        if (h['role']=='controller' or c.forward(self.p) and not c.per_host_forward(self.p)) and h['old_artifact']['package_sha256']!=self.s['installed_package_sha256']:raise ValueError('expected installed package differs across target')
                        self.host(h,'preflight')
                    self.original_start()
                    controller=next(h for h in self.p['hosts'] if h['role']=='controller');workers=[h for h in self.p['hosts'] if h['role']=='worker']
                    for h in workers:self.before_effect();self.host(h,'stop')
                    self.before_effect();self.host(controller,'stop')
                    self.before_effect(False);self.host(controller,'install')
                    self.before_effect(False);self.host(controller,'start')
                    for h in workers:
                        self.before_effect();self.host(h,'install');self.before_effect();self.host(h,'start')
                else:
                    self.original_start(True)
                    current=self.read_action()
                    if current['state'] not in ('started','unknown','applied'):raise ValueError('action cannot be observed')
                rows=self.final_readback(mode=='reconcile')
                observed=self.observe(True)
                if observed['state']!='applied':raise ValueError('rollout observation not applied')
                result.update(status='APPLIED',host_results=rows,observation=observed)
            except Exception as error:
                # Do not invent cleanup/absence from local transport termination.
                if isinstance(error,Refused) and self.original is None: result['status']='REFUSED_NO_HOST_EFFECT'
                result['error_detail']=c.error_record(error,mode);result['error']=result['error_detail']['code'];result['remote_effect_and_cleanup']='unverified unless retained host phase/readback proves it'
                if self.original is not None:
                    try:result['observation']=self.observe(False)
                    except Exception as e:result['observation_error']=c.error_record(e,mode)
            name='result-'+str(len(list(self.work.glob('result-*.json'))))+'.json'
            if len(list(self.work.iterdir()))>=self.l['state_entries']:raise ValueError('operation receipt inventory bound')
            c.save(self.work/name,result,self.l['receipt_bytes']);return result


def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['apply','reconcile']);p.add_argument('--profile',required=True);p.add_argument('--profile-sha256',required=True);p.add_argument('--profile-bytes',type=int,required=True);p.add_argument('--subject',required=True);p.add_argument('--action-id',required=True)
    p.add_argument('--observation-binding');p.add_argument('--observation-binding-sha256');p.add_argument('--observation-binding-bytes',type=int);a=p.parse_args()
    observation=observation_arguments(a,a.mode)
    profile=c.load_profile(a.profile,a.profile_sha256,a.profile_bytes);subject=c.strict(c.read(a.subject,profile['limits']['receipt_bytes'],True))
    if observation is None:require_selected_reader(profile,'adapter',__file__)
    value=Rollout(profile,a.profile,a.profile_sha256,a.profile_bytes,subject,a.action_id,observation).execute(a.mode)
    print(json.dumps(value,sort_keys=True));return 0 if value['status']=='APPLIED' else 2
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:print(json.dumps({'rollout_error':c.error_record(e,'startup')},sort_keys=True),file=sys.stderr);sys.exit(1)
