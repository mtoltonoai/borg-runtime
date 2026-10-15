#!/usr/bin/env python3
"""Operator-owned fixed rollout. Inactive without exact protected metadata.

`apply` sends one StartRollout and fixed host phases. `reconcile` is readback only:
it never repeats install/start/stop, even when a prior response was lost.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, shlex, sys
from pathlib import Path
_COMMON_SHA = '036041acb527b629e08f2f71749f6015b4ce8851cbba6a8fbceeec3688677868'

def _common():
    p=Path(__file__).with_name('rollout_common_v2.py');raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=_COMMON_SHA:raise ValueError('installed rollout common changed')
    m=importlib.util.module_from_spec(importlib.util.spec_from_file_location('rollout_common_v2',p));exec(compile(raw,str(p),'exec'),m.__dict__);return m
c=_common()


class Refused(ValueError):
    pass


class Rollout:
    def __init__(self,profile,path,sha,profile_bytes,subject,action_id):
        self.p=profile;self.path=str(path);self.sha=sha;self.profile_bytes=profile_bytes;self.s=subject;self.action_id=action_id;self.l=profile['limits']
        c.fields(subject,'release_id artifact repository target_ref expected_base policy deployment_target target_revision installed_package_sha256','subject')
        for k in ('policy','deployment_target','target_revision'):
            if subject[k]!=profile[k]:raise ValueError('subject/profile '+k+' mismatch')
        self.producer=c.load_module('rollout_producer',profile['modules']['producer'],self.l['module_bytes'])
        raw=c.pin(profile['producer'],self.profile_bytes);self.config=c.strict(raw)
        c.fields(self.config,'socket server_uid producer_uid policy recipe_sha256 valid_for_ms','producer')
        if self.config['producer_uid']!=os.geteuid() or self.config['policy']!=subject['policy']:raise ValueError('producer UID/policy mismatch')
        self.controller=c.strict(c.pin(profile['controller_config'],self.profile_bytes))
        for k in ('socket','policy','recipe_sha256'):
            if self.controller[k]!=self.config[k]:raise ValueError('actual controller configuration differs')
        for k in ('repository','target_ref','deployment_target','target_revision','installed_package_sha256','policy'):
            if self.controller[k]!=subject[k]:raise ValueError('controller subject differs')
        if not any(x['uid']==os.geteuid() and x['role']=='rollout' for x in self.controller['producers']) or os.geteuid() in self.controller['worker_uids']:raise ValueError('configured rollout authority unavailable')
        self.producer_identity=next(x for x in self.controller['producers'] if x['uid']==os.geteuid() and x['role']=='rollout')
        self.work=Path(profile['state_root'])/('action-'+c.digest(action_id.encode()));self.original=None
    def exchange(self,q):
        remaining=min(self.l['transport_timeout_ms'],self.end-c.now_ms())
        if remaining<=0:raise ValueError('operation deadline before protected transport')
        return self.producer.exchange(self.config,q,max_bytes=self.l['transport_bytes'],timeout_ms=remaining)
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
        request={'revision':1,'action_id':self.action_id,'subject':self.s,'profile_path':h['profile_path'],'profile_sha256':self.sha,'profile_bytes':self.profile_bytes,'phase':phase,
                 'dispatch_until_ms':self.original['dispatch_until_ms'] if self.original else 0,'deadline_ms':self.end}
        key=phase+'-'+c.digest(h['id'].encode());path=self.work/(key+'.request.json');out=self.work/(key+'.result.json')
        mutating=phase in ('stop','install','start')
        if mutating and path.exists():raise ValueError('possible host effect; read-only reconciliation required')
        if not path.exists():c.save(path,request,self.l['receipt_bytes'])
        helper=self.p['modules']['host'];argv=[self.p['python']['path'],'-I','-S','-B',helper['path'],'--profile',h['profile_path'],'--profile-sha256',self.sha,'--profile-bytes',str(self.profile_bytes),'--host',h['id']]
        if h['transport']['kind']=='ssh':
            c.pin(h['transport']['ssh_config'],self.profile_bytes)
            argv=['/usr/bin/ssh','-F',h['transport']['ssh_config']['path'],'-T','-o','BatchMode=yes','-o','ControlMaster=no','-o','ControlPath=none',h['transport']['alias'],shlex.join(['/usr/bin/sudo','-n','/usr/bin/env','-i','PATH=/usr/bin:/bin',*argv])]
        raw=c.run(argv,self.l,self.end,c.canonical(request))
        if len(raw)>self.l['transport_bytes']:raise ValueError('host response bound')
        result=c.strict(raw)
        if not out.exists():c.save(out,result,self.l['receipt_bytes'])
        elif recovery:
            n=len(list(self.work.glob(key+'.readback-*.json')))
            if len(list(self.work.iterdir()))>=self.l['state_entries']:raise ValueError('readback inventory bound')
            c.save(self.work/(key+'.readback-'+str(n)+'.json'),result,self.l['receipt_bytes'])
        elif c.strict(c.read(out,self.l['receipt_bytes'],True))!=result:raise ValueError('host observation changed')
        return result
    def final_readback(self,recovery=False):
        rows={};controller=next(h for h in self.p['hosts'] if h['role']=='controller')
        before=c.strict(c.read(self.work/('preflight-'+c.digest(controller['id'].encode())+'.result.json'),self.l['receipt_bytes'],True))
        for h in self.p['hosts']:
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
            rows[h['id']]=row
        return rows
    def observe(self,observed):
        return self.mutation('observe-applied' if observed else 'observe-unknown',{'operation':'observe_rollout','input':{'action_id':self.action_id,'subject':self.s,'installed_package_sha256':self.s['artifact']['package_sha256'] if observed else 'unknown','observed':observed}})
    def execute(self,mode):
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
            try:
                if mode=='apply':
                    current=self.read_action()
                    if current['state']!='prepared':raise ValueError('action is not freshly Prepared')
                    for h in self.p['hosts']:
                        if h['old_artifact']['package_sha256']!=self.s['installed_package_sha256']:raise ValueError('expected installed package differs across target')
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
                result['error']=str(error);result['remote_effect_and_cleanup']='unverified unless retained host phase/readback proves it'
                if self.original is not None:
                    try:result['observation']=self.observe(False)
                    except Exception as e:result['observation_error']=str(e)
            name='result-'+str(len(list(self.work.glob('result-*.json'))))+'.json'
            if len(list(self.work.iterdir()))>=self.l['state_entries']:raise ValueError('operation receipt inventory bound')
            c.save(self.work/name,result,self.l['receipt_bytes']);return result


def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['apply','reconcile']);p.add_argument('--profile',required=True);p.add_argument('--profile-sha256',required=True);p.add_argument('--profile-bytes',type=int,required=True);p.add_argument('--subject',required=True);p.add_argument('--action-id',required=True);a=p.parse_args()
    profile=c.load_profile(a.profile,a.profile_sha256,a.profile_bytes);subject=c.strict(c.read(a.subject,profile['limits']['receipt_bytes'],True))
    value=Rollout(profile,a.profile,a.profile_sha256,a.profile_bytes,subject,a.action_id).execute(a.mode)
    print(json.dumps(value,sort_keys=True));return 0 if value['status']=='APPLIED' else 2
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:print('protected rollout: '+str(e),file=sys.stderr);sys.exit(1)
