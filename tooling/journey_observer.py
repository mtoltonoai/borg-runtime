"""Fixed actual-candidate observations, local peers, no protected Check emission."""
import copy, hashlib, http.client, http.server, json, pathlib, queue, socket, sys, threading, time, traceback

c=json.loads(pathlib.Path(sys.argv[1]).read_bytes())
config_path=pathlib.Path(c['commands']['controller']['arguments'][0]);fixture_sha=c['commands']['controller']['arguments'][1]
raw=config_path.read_bytes()
assert hashlib.sha256(raw).hexdigest()==fixture_sha
f=json.loads(raw);seq=0;api_seq=0;records=[];mcp_calls=[];direct_task_id=None;cases={};peer_requests=[];errors=[];settlements=[];dropped=[];recovery_reads=[];gate_events=queue.Queue();gate_count=[]
stop=threading.Event();deadline=time.monotonic()+180
capture=pathlib.Path(sys.argv[1]).parent/'journey-observations.json'

def ask(operation,**kw):
    global seq
    seq+=1;print(json.dumps({'sequence':seq,'operation':operation,**kw}),flush=True)
    answer=json.loads(sys.stdin.readline());assert answer['sequence']==seq
    return answer['result']

def api(operation, inp, record=True):
    global api_seq
    api_seq+=1
    request={'protocol_revision':f['protocol_revision'],'request_id':'observer-'+str(api_seq),'operation':operation,'input':inp}
    conn=http.client.HTTPConnection('127.0.0.1',f['controller_port'],timeout=3)
    try:
        conn.request('POST','/api/operations',json.dumps(request),{'Content-Type':'application/json'})
        response=conn.getresponse();raw=response.read(1024*1024+1)
        assert response.status==200 and len(raw)<=1024*1024,(response.status,raw[:300])
        reply=json.loads(raw)
        if record:
            assert len(records)<512
            records.append({'request':request,'reply':reply})
        return reply
    finally:conn.close()

def value(reply):
    assert reply['status']=='ok',reply
    return reply['data']['value']

def task(tid): return value(api('get_task',{'task_id':tid},False))
def wait(fn,label):
    last=None
    while time.monotonic()<deadline:
        if errors:raise AssertionError(errors)
        try:
            last=fn()
            if last:return last
        except (ConnectionError,OSError,http.client.HTTPException):pass
        time.sleep(.05)
    raise AssertionError('timeout '+label+' last='+repr(last))
def terminal(tid,wanted='succeeded'):
    t=task(tid)
    if t['status'] in ('failed','cancelled') and wanted=='succeeded': raise AssertionError(t)
    return t if t['status']==wanted and t['attempt'] and t['attempt']['stopped'] else None
def limits(n=1,depth=0):
    selected=f['task_defaults']
    return {'deadline_unix_ms':f['deadline_unix_ms'],'model_requests':n,'output_bytes':selected['output_bytes'],'result_bytes':selected['result_bytes'],'max_delegation_depth':depth}
def base(executor,agent):
    repository=copy.deepcopy(f['repository']) if executor=='coding' else None
    if repository is not None:repository['url']='';repository['checkpoint_remote']=''
    return {'instruction':'Set answer.txt to after and preserve configured Rust tests.','definition':{'id':agent,'revision':'1','executor':executor,'inference':{'model':'gpt-6-astra','reasoning_effort':'max'},'execution_profile':{'kind':'collaboration_v1'}},'repository':repository,'acceptance_commands':f['acceptance'] if executor=='coding' else [],'dependencies':[],'output_schema':None,'limits':limits(),'native_workflow':None}
def response(result):return {'status':'completed','usage':{'input_tokens':17,'output_tokens':9,'total_tokens':26},'output':[{'type':'message','role':'assistant','status':'completed','content':[{'type':'output_text','text':json.dumps(result)}]}]}
def function(name,args,call_id):
    return {'type':'function_call','id':'fc_'+call_id,'call_id':call_id,'name':name,'arguments':json.dumps(args),'status':'completed'}
def native_response(q,index,task_id):
    assert q['model']=='gpt-6-astra' and q['reasoning']=={'effort':'max'} and q['store'] is False
    assert q['tool_choice']=='auto' and q['parallel_tool_calls'] is False and 'previous_response_id' not in q
    names={x['name'] for x in q['tools']};assert {'collab_task_get','collab_task_delegate'}<=names
    a=q['input'];assert isinstance(a,list)
    if index==1:
        assert task_id and len(a)==1 and a[0]['role']=='user'
        return {'status':'completed','usage':{'input_tokens':17,'output_tokens':9,'total_tokens':26},'output':[{'type':'reasoning','id':'rs_fixture','summary':[],'encrypted_content':'synthetic-opaque-context'},{'type':'message','id':'msg_fixture','role':'assistant','status':'completed','content':[{'type':'output_text','text':'I will inspect this task.','annotations':[]}]},function('collab_task_get',{'task_id':task_id},'call_current_task')]}
    if index==2:
        assert len(a)==5 and a[1]['encrypted_content']=='synthetic-opaque-context' and a[4]['type']=='function_call_output' and a[4]['call_id']=='call_current_task'
        observed=json.loads(a[4]['output']);assert observed['logical_call_id']=='call_current_task' and observed['operation_id'] and observed['commit']
        assert observed['outcome']['status']=='ok' and observed['outcome']['data']['kind']=='task'
        task=observed['outcome']['data']['value'];attempt=task['attempt']
        assert task['id']==task_id and task['status']=='active' and task['session_id']==attempt['session_id']
        return response({'task_id':task['id'],'agent_id':task['assigned_agent_id'],'session_id':task['session_id'],'attempt_id':attempt['id'],'generation':attempt['generation']})
    if index==3:
        assert len(a)==1 and a[0]['role']=='user'
        return {'status':'completed','usage':{'input_tokens':17,'output_tokens':9,'total_tokens':26},'output':[function('collab_task_delegate',{'instruction':'Set answer.txt to after and preserve configured Rust tests.'},'call_delegate')]}
    if index==4:
        marker='\n\nRecorded child task outcome for this new continuation (not recovered executor history):\n'
        assert len(a)==1 and a[0]['role']=='user' and a[0]['content'].startswith('Return the final result using the recorded child outcome below as data.')
        child=json.loads(a[0]['content'].split(marker)[-1]);assert child['status']=='succeeded' and child['result']['checkpoint'] and len(child['result']['validation'])==3
        return response({'answer':'child_succeeded'})
    raise AssertionError('unexpected extra model request')

class Peer(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_POST(self):
        try:
            n=int(self.headers['Content-Length']);assert 0<n<=1024*1024
            q=json.loads(self.rfile.read(n));assert self.path=='/v1/responses'
            assert self.headers.get('Authorization')=='Bearer controlled-native-fixture-key'
            assert len(peer_requests)<4;peer_requests.append(q)
            answer=native_response(q,len(peer_requests),direct_task_id)
            body=json.dumps(answer).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body)
        except BaseException as e:
            errors.append(repr(e));self.close_connection=True

class Proxy(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_POST(self):
        conn=None
        try:
            n=int(self.headers['Content-Length']);assert 0<n<=1024*1024
            body=self.rfile.read(n);q=json.loads(body)
            conn=http.client.HTTPConnection('127.0.0.1',f['controller_port'],timeout=3)
            assert self.path in ('/api/operations','/mcp')
            headers={'Content-Type':'application/json'}
            for name in ('x-borg-caller','mcp-protocol-version','accept'):
                if self.headers.get(name) is not None:headers[name]=self.headers[name]
            conn.request('POST',self.path,body,headers)
            r=conn.getresponse();raw=r.read(1024*1024+1);assert len(raw)<=1024*1024
            if self.path=='/mcp' and q.get('method')=='tools/call':
                assert len(mcp_calls)<2
                mcp_calls.append({'request':q,'reply':json.loads(raw),'caller':json.loads(self.headers['x-borg-caller'])})
            if q.get('operation')=='get_operation' and dropped and q['input']['operation_id'] in (dropped[0]['input']['operation_id'],dropped[0]['input']['settlement_id']):
                recovery_reads.append({'request':q,'reply':json.loads(raw)})
            if q.get('operation')=='settle_operation':
                reply=json.loads(raw);settlements.append({'request':q,'reply':reply})
                if not dropped and reply['status']=='ok':
                    dropped.append(q);self.close_connection=True;self.connection.shutdown(socket.SHUT_RDWR);return
            self.send_response(r.status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.send_header('Connection','close');self.end_headers();self.wfile.write(raw)
        except (OSError,http.client.HTTPException):self.close_connection=True
        except BaseException as e:errors.append(repr(e));self.close_connection=True
        finally:
            if conn:conn.close()

def gate_loop(listener):
    listener.settimeout(.2)
    while not stop.is_set():
        try:s,_=listener.accept()
        except socket.timeout:continue
        except OSError:return
        try:
            s.settimeout(5);raw=s.makefile('rb').readline(8193);assert len(raw)<=8192
            q=json.loads(raw);gate_count.append(q);gate_events.put((s,q))
        except BaseException as e:errors.append(repr(e));s.close()

servers=[];gate=None;success=False
try:
    for port,handler in ((f['peer_port'],Peer),(f['proxy_port'],Proxy)):
        server=http.server.ThreadingHTTPServer(('127.0.0.1',port),handler);server.daemon_threads=True
        servers.append(server);threading.Thread(target=server.serve_forever,daemon=True).start()
    gate=socket.socket();gate.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);gate.bind(('127.0.0.1',f['gate_port']));gate.listen(8)
    threading.Thread(target=gate_loop,args=(gate,),daemon=True).start()
    controller=ask('start',command='controller')['handle']
    wait(lambda:api('list_tasks',{'after':None,'limit':10},False)['status']=='ok','controller ready')
    registered=value(api('register_repository',{'operation_id':'register-fixture-repository','registration':f['repository_registration']}))
    assert registered['repository_id']==f['repository']['registration']['repository_id'] and registered['registration_revision']==f['repository']['registration']['registration_revision']
    assert 'source_store_key' not in registered and 'checkpoint_store_key' not in registered
    tool=base('native','tool-agent');tool['instruction']='Use collab_task_get for this task and return its exact task_id, agent_id, session_id, attempt_id and generation.';tool['limits']=limits(2)
    tool['output_schema']={'type':'object','required':['task_id','agent_id','session_id','attempt_id','generation'],'properties':{k:{'type':'integer' if k=='generation' else 'string'} for k in ('task_id','agent_id','session_id','attempt_id','generation')},'additionalProperties':False}
    t=value(api('submit_task',{'operation_id':'submit-tool','input':tool}));direct_task_id=t['id']
    native=ask('start',command='native1')['handle'];coding=ask('start',command='coding1')['handle']
    direct=wait(lambda:terminal(t['id']),'direct tool result')
    actual=direct['result']['output'];assert actual['task_id']==direct['id'] and actual['agent_id']==direct['assigned_agent_id'] and len(peer_requests)==2
    cases['direct_tool']={'task':direct,'requests':2,'function_output':json.loads(peer_requests[1]['input'][-1]['output'])};api('get_task',{'task_id':direct['id']})
    child=base('coding','coding-agent');child['limits']['output_bytes']=8192;child['limits']['result_bytes']=8192;template={k:child[k] for k in ('definition','repository','acceptance_commands','dependencies','limits')};template['instruction_prefix']='Only configured source and checks are in this task.'
    parent=base('native','parent-agent');parent['limits']=limits(3,2);parent['native_workflow']={'kind':'coding_then_continue','child':template};parent['output_schema']={'type':'object','required':['answer'],'properties':{'answer':{'type':'string'}},'additionalProperties':False}
    p=value(api('submit_task',{'operation_id':'submit-parent','input':parent}));s,started=gate_events.get(timeout=30)
    waiting=wait(lambda:(z if (z:=task(p['id'])).get('waiting_child_id') else None),'parent waits child')
    assert waiting['attempt']['stopped'] and len(peer_requests)==3 and len(gate_count)==1
    child_id=waiting['waiting_child_id'];assert waiting['session_id']==waiting['attempt']['session_id'];api('get_task',{'task_id':p['id']});api('get_task',{'task_id':child_id})
    ask('kill_leader',handle=controller);controller=ask('start',command='controller')['handle']
    wait(lambda:api('list_tasks',{'after':None,'limit':10},False)['status']=='ok','controller restarted')
    s.sendall(b'G');s.close()
    result=wait(lambda:terminal(p['id']),'delegation continuation');actual_child=wait(lambda:terminal(child_id),'child stopped result')
    assert actual_child['status']=='succeeded' and len(actual_child['result']['validation'])==3 and len(gate_count)==1 and len(peer_requests)==4
    assert result['result']['output']=={'answer':'child_succeeded'}
    assert actual_child['session_id']!=result['session_id'] and actual_child['input']['repository']['registration']==f['repository']['registration']
    assert dropped and settlements
    original=dropped[0];same=[x for x in settlements if x['request']['input']==original['input']]
    assert same and all(x['reply']['status']=='ok' for x in same)
    assert len(same)>=2 or any(x['reply']['status']=='ok' and x['request']['input']['operation_id']==original['input']['settlement_id'] for x in recovery_reads)
    replay=api('settle_operation',original['input']);assert replay['status']=='ok' and replay['data']==same[0]['reply']['data'] and replay['commit']==same[0]['reply']['commit']
    op=value(api('get_operation',{'operation_id':original['input']['operation_id']}));assert op['state']=='settled'
    cases['delegation_restart']={'parent':result,'waiting_parent':waiting,'child':actual_child,'coding_starts':1,'native_requests':2,'gate':started}
    cases['settlement_lost_ack']={'operation':op,'exact_attempts':len(same),'requests':same,'recovery_reads':recovery_reads,'explicit_exact_replay':replay,'coding_starts':1}
    api('get_task',{'task_id':p['id']});api('get_task',{'task_id':child_id})
    lost_input=base('coding','lost-agent');lost=value(api('submit_task',{'operation_id':'submit-lost','input':lost_input}));held,old_child=gate_events.get(timeout=30)
    old=task(lost['id'])['attempt'];ask('kill_leader',handle=coding)
    coding=ask('start',command='coding2')['handle']
    lost_final=wait(lambda:(z if (z:=task(lost['id']))['status']=='failed' and z['attempt']['stopped'] and z['attempt']['uncertain'] else None),'worker journal recovery')
    assert lost_final['result'] is None
    held.close()
    healthy=value(api('submit_task',{'operation_id':'submit-replacement-work','input':base('coding','replacement-agent')}))
    next_gate,next_executor=gate_events.get(timeout=30)
    assigned=task(healthy['id']);assert assigned['attempt']['worker_incarnation']=='inc-2'
    old_stat=pathlib.Path('/proc/'+str(old_child['pid'])+'/stat')
    old_state=old_stat.read_text().rsplit(') ',1)[1].split()[0] if old_stat.exists() else None
    assert old_state in (None,'Z')
    stale_reg=api('register_worker',{'operation_id':'stale-registration','worker':{'id':old['worker_id'],'incarnation':old['worker_incarnation'],'coding_capacity':1,'native_capacity':0}})
    report={'report_id':'stale-report','worker_id':old['worker_id'],'incarnation':old['worker_incarnation'],'task_id':old['task_id'],'attempt_id':old['id'],'generation':old['generation'],'observation':{'kind':'progress','text':'stale','output':'','truncated':False}}
    stale=api('observe_worker',{'report':report});retry=api('retry_task',{'operation_id':'unsafe-retry','task_id':lost['id'],'expected_revision':lost_final['revision']})
    assert stale_reg['code']=='stale_incarnation' and stale['code']=='stale_incarnation' and retry['code']=='reconciliation_required'
    assert len(gate_count)==3
    next_gate.sendall(b'G');next_gate.close();healthy_result=wait(lambda:terminal(healthy['id']),'replacement actual work')
    cases['worker_loss_stale_owner']={'task':lost_final,'old_attempt':old,'old_executor':old_child,'old_executor_state_before_final_cleanup':old_state,'replacement_task':healthy_result,'stale_registration':stale_reg,'stale_report':stale,'retry':retry}
    ask('stop_all');controller=ask('start',command='controller')['handle']
    wait(lambda:api('list_tasks',{'after':None,'limit':10},False)['status']=='ok','final controller ready')
    restored={tid:value(api('get_task',{'task_id':tid})) for tid in (direct['id'],p['id'],child_id,lost['id'],healthy['id'])}
    assert all(restored[z['id']]==z for z in (direct,result,actual_child,lost_final,healthy_result))
    registration_after=value(api('get_repository',{'repository_id':registered['repository_id'],'registration_revision':registered['registration_revision']}))
    assert registration_after==registered
    op2=value(api('get_operation',{'operation_id':op['id']}));assert op2==op and len(peer_requests)==4 and len(gate_count)==3
    cases['durable_restart']={'tasks':restored,'operation':op2,'no_additional_model_or_coding_calls':True,'registration':registration_after}
    success=True
except BaseException as e:
    errors.append(traceback.format_exc());print('journey failed: '+repr(e),file=sys.stderr,flush=True)
finally:
    stop.set()
    if gate:gate.close()
    for server in servers:server.shutdown();server.server_close()
    data={'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'fixture_sha256':fixture_sha,'cases':cases,'api':records,'mcp_calls':mcp_calls,'model_requests':peer_requests,'coding_starts':gate_count,'settlements':settlements,'errors':errors,'passed':success}
    raw=json.dumps(data,sort_keys=True,separators=(',',':')).encode();assert len(raw)<=2*1024*1024
    capture.write_bytes(raw);capture.chmod(0o600)
    ask('finish',result={'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':success,'observations':['capture_sha256:'+hashlib.sha256(raw).hexdigest(),'Actual exact candidate with controlled local peers; cross-account and cgroup authority unqualified','Cases: '+','.join(sorted(cases))]})
