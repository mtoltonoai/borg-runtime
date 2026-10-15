"""Fixed post-cleanup adjudicator; never execute candidate or trust its PASS text."""
import hashlib,json,os,pathlib,re,selectors,subprocess,sys,time
def sha(raw):return hashlib.sha256(raw).hexdigest()
def adjudicate(c,evidence,raw_capture):
    if sha(json.dumps(c['subject'],sort_keys=True,separators=(',',':')).encode())!=c['subject_sha256']:raise ValueError('subject hash differs')
    if not c.get('cleanup_verified') or not evidence.get('cleanup_verified'):raise ValueError('whole-group cleanup absent')
    if any(evidence[k]!=c[k] for k in ('subject_sha256','recipe_sha256','check')):raise ValueError('subject mismatch')
    if evidence['supervisor_transcript'][-1]!={'kind':'supervisor_terminal','value':{'cleanup_verified':True,'observer_exit':0,'candidate_starts':6}}:raise ValueError('supervisor terminal/launch count differs')
    obs=evidence['observer_result']
    if not obs['passed'] or 'capture_sha256:'+sha(raw_capture) not in obs['observations']:raise ValueError('observer capture association differs')
    d=json.loads(raw_capture)
    if not d['passed'] or d['errors'] or d['subject_sha256']!=c['subject_sha256'] or d['recipe_sha256']!=c['recipe_sha256'] or d['fixture_sha256']!=c['fixture_sha256']:raise ValueError('journey did not pass')
    required={'direct_tool','delegation_restart','settlement_lost_ack','worker_loss_stale_owner','durable_restart'}
    if set(d['cases'])!=required:raise ValueError('mandatory case absent')
    cases=d['cases'];direct=cases['direct_tool']['task'];parent=cases['delegation_restart']['parent'];child=cases['delegation_restart']['child'];loss=cases['worker_loss_stale_owner']
    if len(d['model_requests'])!=4 or len(d['coding_starts'])!=3:raise ValueError('unexpected repeated execution')
    if any(t['status']!='succeeded' or t['result'] is None for t in (direct,parent,child)):raise ValueError('accepted journeys absent')
    direct_output=direct['result']['output']
    if direct_output!={'task_id':direct['id'],'agent_id':direct['assigned_agent_id'],'session_id':direct['session_id'],'attempt_id':direct['attempt']['id'],'generation':direct['attempt']['generation']}:raise ValueError('direct tool result differs')
    verify_collaboration(d)
    if len(child['result']['validation'])!=3 or not child['result']['checkpoint']:raise ValueError('coding checks/checkpoint absent')
    if parent['result']['output']!={'answer':'child_succeeded'}:raise ValueError('continuation absent')
    if loss['task']['result'] is not None or loss['task']['status']!='failed' or not loss['task']['attempt']['stopped'] or not loss['task']['attempt']['uncertain']:raise ValueError('worker loss hidden')
    if loss['stale_registration']['code']!='stale_incarnation' or loss['stale_report']['code']!='stale_incarnation' or loss['retry']['code']!='reconciliation_required':raise ValueError('stale owner admitted')
    if loss['replacement_task']['status']!='succeeded' or loss['replacement_task']['attempt']['worker_incarnation']!='inc-2' or loss['old_executor_state_before_final_cleanup'] not in (None,'Z'):raise ValueError('replacement admitted before accounting for old executor')
    settled=cases['settlement_lost_ack']
    if settled['exact_attempts']<1 or settled['operation']['state']!='settled' or cases['durable_restart']['operation']!=settled['operation']:raise ValueError('settlement/restart differs')
    if settled['exact_attempts']<2 and not any(x['reply']['status']=='ok' and x['request']['input']['operation_id']==settled['requests'][0]['request']['input']['settlement_id'] for x in settled['recovery_reads']):raise ValueError('worker did not resolve lost settlement reply')
    if settled['explicit_exact_replay']['status']!='ok' or settled['explicit_exact_replay']['data']!=settled['requests'][0]['reply']['data'] or settled['explicit_exact_replay']['commit']!=settled['requests'][0]['reply']['commit']:raise ValueError('exact settlement replay differs')
    for t in (direct,parent,child,loss['task'],loss['replacement_task']):
        after=cases['durable_restart']['tasks'][t['id']]
        if json.dumps(after,sort_keys=True,separators=(',',':'))!=json.dumps(t,sort_keys=True,separators=(',',':')):raise ValueError('restart terminal task changed')
    launches=[x['value'] for x in evidence['supervisor_transcript'] if x['kind']=='supervisor_launch']
    if [x['command'] for x in launches]!=['controller','native1','coding1','controller','coding2','controller']:raise ValueError('unexpected launch sequence')
    if any(x['artifact_binary_sha256']!=c['subject']['artifact']['binary_sha256'] for x in launches):raise ValueError('binary mismatch')
    return sorted(required)
def verify_collaboration(d):
    calls=d['mcp_calls'];cases=d['cases'];direct=cases['direct_tool']['task'];parent=cases['delegation_restart']['parent'];child=cases['delegation_restart']['child']
    if len(calls)!=2:raise ValueError('exact collaboration call count differs')
    for row,name,task,call_id in zip(calls,('collab_task_get','collab_task_delegate'),(direct,parent),('call_current_task','call_delegate')):
        q=row['request'];caller=row['caller'];params=q['params']
        if q['method']!='tools/call' or params['name']!=name or params['_meta']['borg']['logical_call_id']!=call_id:raise ValueError('canonical collaboration call differs')
        # Parent final attempt is a continuation; compare retained caller with
        # the call receipt subject, not a rewritten terminal attempt identity.
        bound=direct if name=='collab_task_get' else cases['delegation_restart']['waiting_parent']
        attempt=bound['attempt']
        expected_caller={'task_id':bound['id'],'attempt_id':attempt['id'],'generation':attempt['generation'],'worker_id':attempt['worker_id'],'incarnation':attempt['worker_incarnation']}
        if caller!=expected_caller or bound['session_id']!=task['session_id'] or attempt['session_id']!=bound['session_id']:raise ValueError('collaboration caller session differs')
        envelope=row['reply']
        if envelope['id']!=q['id'] or envelope['result']['isError']:raise ValueError('collaboration reply differs')
        content=envelope['result']['content']
        if len(content)!=1 or content[0]['type']!='text':raise ValueError('collaboration reply content differs')
        result=json.loads(content[0]['text']);reply=result['reply']
        if reply['status']!='ok' or reply['data']['kind']!='task' or not reply['commit']:raise ValueError('canonical committed task reply absent')
        if reply['commit']['operation_id']!=params['_meta']['borg']['operation_id']:raise ValueError('collaboration commit identity differs')
        target=reply['data']['value']
        if name=='collab_task_get':
            if target['id']!=direct['id'] or target['session_id']!=direct['session_id'] or target['attempt']['id']!=caller['attempt_id'] or target['attempt']['generation']!=caller['generation']:raise ValueError('direct tool association differs')
            original={'operation_id':params['_meta']['borg']['operation_id'],'logical_call_id':call_id,'outcome':{'status':'ok','data':reply['data']},'commit':reply['commit']}
            actual=json.loads(d['model_requests'][1]['input'][-1]['output'])
            if actual!=original or cases['direct_tool']['function_output']!=original:raise ValueError('original collaboration result changed before continuation')
            if result['requires_quiescence']:raise ValueError('direct read spuriously quiesced')
        else:
            if target['id']!=child['id'] or target['parent_task_id']!=parent['id'] or not result['requires_quiescence']:raise ValueError('delegation receipt differs')
    registration=child['input']['repository']['registration']
    entry=cases['durable_restart']['registration']
    if any(entry[k]!=registration[k] for k in ('repository_id','registration_revision')) or 'source_store_key' in entry or 'checkpoint_store_key' in entry:raise ValueError('repository registration replay differs')
    if direct['input']['definition']['id']!=direct['assigned_agent_id'] or any(t['session_id']!=t['attempt']['session_id'] for t in (direct,parent,child)):raise ValueError('canonical agent/session differs')
    if child['session_id']==parent['session_id']:raise ValueError('child session alias')

def main():
    c=json.loads(pathlib.Path(sys.argv[1]).read_bytes());raw=pathlib.Path(c['live_evidence_path']).read_bytes()
    if sha(raw)!=c['live_evidence_sha256']:raise ValueError('evidence changed')
    live=json.loads((pathlib.Path(sys.argv[1]).parent/(c['check']+'.live-input.json')).read_bytes())
    if any(live[k]!=c[k] for k in ('subject_sha256','recipe_sha256','check')):raise ValueError('live input association changed')
    args=live['commands']['controller']['arguments'];c['fixture_path']=args[0];c['fixture_sha256']=args[1]
    capture=(pathlib.Path(sys.argv[1]).parent/'journey-observations.json').read_bytes()
    cases=adjudicate(c,json.loads(raw),capture)
    fixture_raw=pathlib.Path(c['fixture_path']).read_bytes()
    if sha(fixture_raw)!=c['fixture_sha256']:raise ValueError('fixture input changed')
    fixture=json.loads(fixture_raw);d=json.loads(capture)
    for name,command in live['commands'].items():
        if command['arguments']!=[c['fixture_path'],c['fixture_sha256'],name]:raise ValueError('fixed launch arguments differ')
    repository=verify_published_source(fixture,d)
    path=pathlib.Path(sys.argv[1]).parent/'published-source-observation.json'
    path.write_text(json.dumps(repository,sort_keys=True,indent=2)+'\n');path.chmod(0o600)
    print(json.dumps({'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':True,'observations':['Actual candidate cases: '+','.join(cases),'Published exact checkpoint source independently read after cleanup: '+sha(path.read_bytes()),'Local controlled peers and measured final cleanup; no protected Check or cross-account qualification']}))

def verify_published_source(f,d):
    """Fixed Git readers after cleanup, never a candidate-supplied test command."""
    git=pathlib.Path('/usr/bin/git')
    if sha(git.read_bytes())!=f['git_sha256']:raise ValueError('selected Git reader changed')
    repo=pathlib.Path(f['repository']['checkpoint_remote'])
    if repo.is_symlink() or repo.resolve()!=repo:raise ValueError('repository alias')
    env={'PATH':'/usr/bin:/bin','HOME':'/nonexistent','LANG':'C','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_NO_REPLACE_OBJECTS':'1'}
    prefix=[str(git),'--no-pager','--git-dir',str(repo),'-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false']
    def read(args,expected_size=None):
        p=subprocess.Popen(prefix+args,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=True)
        selector=selectors.DefaultSelector();values={'out':bytearray(),'err':bytearray()};end=time.monotonic()+3
        try:
            for pipe,name in ((p.stdout,'out'),(p.stderr,'err')):
                os.set_blocking(pipe.fileno(),False);selector.register(pipe,selectors.EVENT_READ,name)
            while selector.get_map():
                if time.monotonic()>end:raise ValueError('fixed reader deadline')
                for key,_ in selector.select(.05):
                    value=os.read(key.fd,4097-len(values[key.data]))
                    if not value:selector.unregister(key.fileobj);continue
                    values[key.data].extend(value)
                    if len(values[key.data])>4096:raise ValueError('fixed reader output bound')
            if p.wait(timeout=max(.001,end-time.monotonic()))!=0:raise ValueError('fixed repository reader failed')
            value=bytes(values['out'])
            if expected_size is not None and len(value)!=expected_size:raise ValueError('object length changed')
            return value
        finally:
            if p.poll() is None:p.kill()
            p.wait(timeout=3);selector.close();p.stdout.close();p.stderr.close()
    results=[]
    for task in (d['cases']['delegation_restart']['child'],d['cases']['worker_loss_stale_owner']['replacement_task']):
        cp=task['result']['checkpoint'];commit=cp['commit'];ref=cp['remote_ref']
        accepted=task['input']['repository']
        if accepted!=f['repository']:raise ValueError('accepted repository source or registration differs')
        if cp['registration']!=f['repository']['registration'] or task['input']['repository']['registration']!=cp['registration'] or task['input']['repository']['checkpoint_remote']!=str(repo) or cp['repository']!=str(repo) or not re.fullmatch('[0-9a-f]{40}',commit) or not re.fullmatch('refs/heads/borg/tasks/[0-9a-f]+/[0-9a-f]+/[0-9a-f]+',ref):raise ValueError('checkpoint subject differs')
        actual=read(['rev-parse','--verify','--end-of-options',ref+'^{commit}']).decode().strip()
        if actual!=commit:raise ValueError('published reference does not identify accepted checkpoint')
        objects={}
        for name,expected in (('answer.txt',b'after\n'),('src/main.rs',f['expected_test_source'].encode())):
            spec=commit+':'+name
            if int(read(['cat-file','-s',spec]).decode())!=len(expected):raise ValueError('published source size differs')
            value=read(['cat-file','blob',spec],len(expected))
            if value!=expected:raise ValueError('published source content differs')
            objects[name]={'bytes':len(value),'sha256':sha(value)}
        results.append({'task_id':task['id'],'commit':commit,'remote_ref':ref,'objects':objects})
    return {'revision':1,'git_sha256':f['git_sha256'],'repository':str(repo),'checkpoints':results,'candidate_code_executed_by_oracle':False}
if __name__=='__main__':main()
