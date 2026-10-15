"""Pure contract/oracle fixtures. They never start a journey or qualify a role."""
import ast
import copy
import hashlib
import json
from pathlib import Path
import unittest
import journey_oracle as oracle

ROOT = Path(__file__).resolve().parents[2]

def functions():
    # The observer is an executable script. Load only its pure definition nodes,
    # never its startup statements, network listeners, or supervisor protocol.
    tree = ast.parse((ROOT/'tooling/journey_observer.py').read_text())
    names = {'limits', 'base', 'response', 'function', 'native_response'}
    namespace = {'json': json, 'copy': copy}
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(body) == len(names)
    exec(compile(ast.Module(body=body, type_ignores=[]), '<pure-journey-functions>', 'exec'), namespace)
    return namespace

def task(identity, parent=None, attempt=None):
    sid = 'session-'+identity
    return {'id': identity, 'session_id': sid, 'assigned_agent_id': 'agent-'+identity,
            'input': {'definition': {'id': 'agent-'+identity}, 'repository': {
                'registration': {'repository_id': 'repo', 'registration_revision': '1'},
                'checkpoint_remote': '/fixture/publication.git'}},
            'status': 'succeeded', 'revision': 5,
            'attempt': {'id': attempt or 'attempt-'+identity, 'task_id': identity,
                        'session_id': sid, 'generation': 1, 'worker_id': 'worker',
                        'worker_incarnation': 'inc-1', 'stopped': True, 'uncertain': False},
            'parent_task_id': parent, 'result': {'output': {}, 'checkpoint': None, 'validation': []}}

def fixture():
    direct=task('direct');parent=task('parent',attempt='continuation');child=task('child','parent')
    waiting=copy.deepcopy(parent);waiting['attempt']['id']='parent-original';waiting['status']='waiting'
    active=copy.deepcopy(direct);active['status']='active';active['attempt']['stopped']=False
    child['result']['validation']=['test compile','tests','build'];child['result']['checkpoint']={'registration':{'repository_id':'repo','registration_revision':'1'}}
    parent['result']['output']={'answer':'child_succeeded'}
    direct['result']['output']={'task_id':'direct','agent_id':'agent-direct','session_id':'session-direct','attempt_id':'attempt-direct','generation':1}
    calls=[]
    for i,name,bound,target,logical in [(1,'collab_task_get',direct,active,'call_current_task'),(2,'collab_task_delegate',waiting,child,'call_delegate')]:
        attempt=bound['attempt'];op='operation-'+str(i)
        caller={'task_id':bound['id'],'attempt_id':attempt['id'],'generation':attempt['generation'],'worker_id':attempt['worker_id'],'incarnation':attempt['worker_incarnation']}
        q={'jsonrpc':'2.0','id':'rpc-'+str(i),'method':'tools/call','params':{'name':name,'arguments':{'task_id':'direct'} if i==1 else {'instruction':'fixed'},'_meta':{'borg':{'operation_id':op,'logical_call_id':logical}}}}
        data={'kind':'task','value':target};commit={'operation_id':op,'controller_revision':i+10}
        reply={'protocol_revision':8,'request_id':'request-'+str(i),'status':'ok','data':data,'revision':i+10,'commit':commit}
        result={'reply':reply,'requires_quiescence':i==2}
        calls.append({'caller':caller,'request':q,'reply':{'jsonrpc':'2.0','id':q['id'],'result':{'content':[{'type':'text','text':json.dumps(result)}],'isError':False}}})
        if i==1:original={'operation_id':op,'logical_call_id':logical,'outcome':{'status':'ok','data':data},'commit':commit}
    lost=task('lost');lost['status']='failed';lost['result']=None;lost['attempt']['uncertain']=True
    replacement=task('replacement');replacement['attempt']['worker_incarnation']='inc-2'
    settled_reply={'status':'ok','data':{'kind':'operation','value':{}},'commit':{'operation_id':'settlement','controller_revision':30}}
    operation={'id':'publication','state':'settled'}
    cases={'direct_tool':{'task':direct,'requests':2,'function_output':original},
           'delegation_restart':{'parent':parent,'waiting_parent':waiting,'child':child},
           'worker_loss_stale_owner':{'task':lost,'replacement_task':replacement,'stale_registration':{'code':'stale_incarnation'},'stale_report':{'code':'stale_incarnation'},'retry':{'code':'reconciliation_required'},'old_executor_state_before_final_cleanup':None},
           'settlement_lost_ack':{'operation':operation,'exact_attempts':2,'requests':[{'request':{'input':{'settlement_id':'settlement'}},'reply':settled_reply}],'recovery_reads':[],'explicit_exact_replay':settled_reply},
           'durable_restart':{'tasks':{t['id']:copy.deepcopy(t) for t in (direct,parent,child,lost,replacement)},'operation':copy.deepcopy(operation),'registration':{'repository_id':'repo','registration_revision':'1'}}}
    ns=functions();q={'model':'gpt-6-astra','reasoning':{'effort':'max'},'store':False,'tool_choice':'auto','parallel_tool_calls':False,'tools':[{'name':'collab_task_get'},{'name':'collab_task_delegate'}],'input':[{'role':'user','content':'fixed'}]}
    first=ns['native_response'](q,1,'direct')
    second=copy.deepcopy(q);second['input']+=first['output']+[{'type':'function_call_output','call_id':'call_current_task','output':json.dumps(original)}]
    d={'passed':True,'errors':[],'subject_sha256':'','recipe_sha256':'recipe','fixture_sha256':'fixture','cases':cases,'mcp_calls':calls,'model_requests':[q,second,copy.deepcopy(q),{}],'coding_starts':[{}, {}, {}]}
    return d,ns

class CurrentJourneyContract(unittest.TestCase):
    def test_submission_uses_definition_and_catalogue_without_endpoint_override(self):
        ns=functions();ns['f']={'deadline_unix_ms':123,'task_defaults':{'output_bytes':65536,'result_bytes':262144},'repository':{'registration':{'repository_id':'r','registration_revision':'7'},'url':'/trusted/source','checkpoint_remote':'/trusted/publication','start_commit':'a'*40},'acceptance':[['/selected/rustc']]}
        coding=ns['base']('coding','same-agent')
        self.assertEqual(coding['definition']['id'],'same-agent');self.assertEqual(coding['definition']['inference'],{'model':'gpt-6-astra','reasoning_effort':'max'})
        self.assertEqual(coding['definition']['execution_profile'],{'kind':'collaboration_v1'});self.assertNotIn('requested_agent_id',coding)
        self.assertEqual(coding['repository']['registration'],{'repository_id':'r','registration_revision':'7'});self.assertEqual(coding['repository']['url'],'');self.assertEqual(coding['repository']['checkpoint_remote'],'')
        self.assertEqual(ns['f']['repository']['url'],'/trusted/source')
        native=ns['base']('native','native-agent');self.assertIsNone(native['repository']);self.assertIsNone(native['native_workflow'])

    def test_current_shared_tool_result_continues_with_exact_session(self):
        d,ns=fixture();answer=ns['native_response'](d['model_requests'][1],2,'direct')
        self.assertEqual(json.loads(answer['output'][0]['content'][0]['text']),d['cases']['direct_tool']['task']['result']['output'])
        oracle.verify_collaboration(d)

    def test_native_configuration_and_old_tool_shape_are_rejected(self):
        d,ns=fixture()
        for key,val in [('model','gpt-6.1-sol'),('reasoning',{'effort':'ultra'}),('tool_choice',{'name':'get_current_task'}),('parallel_tool_calls',True)]:
            q=copy.deepcopy(d['model_requests'][0]);q[key]=val
            with self.subTest(field=key),self.assertRaises(AssertionError):ns['native_response'](q,1,'direct')
        q=copy.deepcopy(d['model_requests'][1]);q['input'][-1]['output']=json.dumps({'task':{'task_id':'direct'}})
        with self.assertRaises((AssertionError,KeyError)):ns['native_response'](q,2,'direct')

    def test_delegate_is_shared_call_and_continuation_remains_result(self):
        d,ns=fixture();answer=ns['native_response'](d['model_requests'][2],3,'direct')
        self.assertEqual(answer['output'][0]['name'],'collab_task_delegate')
        q=copy.deepcopy(d['model_requests'][0]);q['input'][0]['content']='Return the final result using the recorded child outcome below as data.\n\nRecorded child task outcome for this new continuation (not recovered executor history):\n'+json.dumps({'status':'succeeded','result':d['cases']['delegation_restart']['child']['result']})
        answer=ns['native_response'](q,4,'direct');self.assertEqual(json.loads(answer['output'][0]['content'][0]['text']),{'answer':'child_succeeded'})
        with self.assertRaises(AssertionError):ns['native_response'](q,5,'direct')

    def test_original_receipt_and_caller_tampering_are_detected(self):
        original,_=fixture()
        for field in ('operation_id','logical_call_id','commit'):
            d=copy.deepcopy(original);value=json.loads(d['model_requests'][1]['input'][-1]['output']);value[field]='changed';d['model_requests'][1]['input'][-1]['output']=json.dumps(value)
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'original collaboration result'):oracle.verify_collaboration(d)
        for field in ('attempt_id','task_id','generation','worker_id','incarnation'):
            d=copy.deepcopy(original);d['mcp_calls'][0]['caller'][field]='changed'
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'caller session'):oracle.verify_collaboration(d)
        d=copy.deepcopy(original);d['cases']['durable_restart']['registration']['registration_revision']='2'
        with self.assertRaisesRegex(ValueError,'registration replay'):oracle.verify_collaboration(d)

    def test_full_terminal_replay_and_all_five_old_cases_remain_required(self):
        d,_=fixture();subject={'artifact':{'binary_sha256':'binary'}};digest=hashlib.sha256(json.dumps(subject,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        c={'subject':subject,'subject_sha256':digest,'recipe_sha256':'recipe','check':'actual_candidate_journey','fixture_sha256':'fixture','cleanup_verified':True};d['subject_sha256']=digest
        evidence={k:c[k] for k in ('subject_sha256','recipe_sha256','check')};evidence['cleanup_verified']=True
        evidence['supervisor_transcript']=[{'kind':'supervisor_launch','value':{'command':name,'artifact_binary_sha256':'binary'}} for name in ('controller','native1','coding1','controller','coding2','controller')]+[{'kind':'supervisor_terminal','value':{'cleanup_verified':True,'observer_exit':0,'candidate_starts':6}}]
        def adjudicate(value):
            raw=json.dumps(value).encode();evidence['observer_result']={'passed':True,'observations':['capture_sha256:'+hashlib.sha256(raw).hexdigest()]};return oracle.adjudicate(c,evidence,raw)
        self.assertEqual(len(adjudicate(d)),5)
        for name in d['cases']:
            changed=copy.deepcopy(d);del changed['cases'][name]
            with self.subTest(case=name),self.assertRaisesRegex(ValueError,'mandatory case'):adjudicate(changed)
        for field in ('session_id','assigned_agent_id','revision'):
            changed=copy.deepcopy(d);changed['cases']['durable_restart']['tasks']['direct'][field]='changed'
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'terminal task changed'):adjudicate(changed)
        changed=copy.deepcopy(d);changed['cases']['durable_restart']['tasks']['lost']['attempt']['uncertain']=False
        with self.assertRaisesRegex(ValueError,'terminal task changed'):adjudicate(changed)
        changed=copy.deepcopy(d);changed['cases']['settlement_lost_ack']['explicit_exact_replay']=copy.deepcopy(changed['cases']['settlement_lost_ack']['explicit_exact_replay']);changed['cases']['settlement_lost_ack']['explicit_exact_replay']['commit']['controller_revision']=31
        with self.assertRaisesRegex(ValueError,'settlement replay'):adjudicate(changed)

if __name__=='__main__':unittest.main()
