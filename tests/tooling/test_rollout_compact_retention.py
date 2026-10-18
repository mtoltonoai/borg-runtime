"""Closed saved-receipt fixtures; no host, runtime, database or body recovery.

The real Python proof readers are exercised with synthetic stopped snapshots.
These cases do not qualify a deployed schema26 controller or grant retention.
"""
import copy
import json
import unittest
from unittest import mock

import rollout_common_v2 as common
import rollout_host_v2 as hostmod


def fixture():
    caller = {'task_id':'task-a','attempt_id':'attempt-a','generation':2,
              'worker_id':'worker-a','incarnation':'incarnation-a'}
    call = {'operation_id':'read-a','logical_call_id':'call-a','caller':caller,
            'tool':'collab_task_get','arguments':{'task_id':'task-a'}}
    evidence = {'kind':'task','outcome_bytes':8192,'outcome_sha256':'a'*64,
                'source':{'kind':'observation'}}
    policy = {'success_grace_ms':100,'failed_grace_ms':200,'cancelled_grace_ms':300,
              'idle_cache_grace_ms':0,'batch_entries':8,'filesystem_entries_per_yield':16,
              'operation_timeout_ms':1000,'retry_delay_ms':10,'record_bytes':1048576}
    plan = {'operation_id':'release-a','caller':caller,'command_id':'run-a','session_id':'session-a',
            'terminal_revision':7,'first_safe_operation_id':'safe-a','policy_revision':'historical-policy',
            'policy':policy,'custody':{'kind':'native_journal','manifest_sha256':'b'*64,'bytes':9000,'files':1},
            'custody_operation_id':'custody-a','reads':[{'operation_id':'read-a',
                'commit':{'operation_id':'read-a','controller_revision':10},'evidence':evidence}]}
    result = {'attempt_id':'attempt-a','released':['read-a'],'body_bytes_before':8192,'body_bytes_after':500,
              'first_safe_operation_id':'safe-a','first_safe_ms':1000,'eligible_at_ms':1100}
    attempt = {'id':'attempt-a','session_id':'session-a','task_id':'task-a','generation':2,
               'worker_id':'worker-a','worker_incarnation':'incarnation-a','phase':'stopped',
               'deadline_unix_ms':900,'stopped':True,'uncertain':False}
    task = {'id':'task-a','session_id':'session-a','revision':7,'status':'succeeded',
            'attempt':attempt,'pending_assignment':None,'waiting_child_id':None,
            'input':{'definition':{'executor':'native'}}}
    compact = {'committed_read_reference':{'evidence':evidence,'release_operation_id':'release-a',
                'attempt_id':'attempt-a','session_id':'session-a'}}
    first = dict(plan, operation_id='safe-a', first_safe_operation_id=None)
    custody = dict(first, operation_id='custody-a')
    report = dict(caller, report_id='custody-a', observation={'kind':'read_transcript_custody','proof':custody})
    values = {
        'read-a':({'operation':'call_collaboration','input':{'call':call}},compact,10),
        'custody-a':({'operation':'observe_worker','input':{'report':report}},
                     {'status':'ok','data':{'kind':'task','value':task}},20),
        'safe-a':({'operation':'release_terminal_reads','input':first},
                  {'status':'ok','data':{'kind':'terminal_read_release','value':dict(result,released=[],body_bytes_before=0,body_bytes_after=0)}},30),
        'release-a':({'operation':'release_terminal_reads','input':plan},
                     {'status':'ok','data':{'kind':'terminal_read_release','value':result}},40),
    }
    rows = [[key,common.canonical(request).decode(),common.canonical(outcome).decode(),revision]
            for key,(request,outcome,revision) in values.items()]
    snapshot = {'schema':26,'tables':{'receipts':{'columns':['id','request','outcome','controller_revision'],'rows':rows},
                  'external_operations':{'columns':['id','body'],'rows':[]}}}
    profile = {'formats':{'before':{'wire':25,'controller_schema':26,'worker_format':20},
                          'after':{'wire':25,'controller_schema':26,'worker_format':20},'source_files':{}},
               'limits':{'sqlite_rows':100,'state_entries':100}}
    manifest = {'source':{'files':{name:{'sha256':sha} for name,sha in common.COMPACT_RECEIPT_SOURCES.items()}}}
    record = {'id':'read-a','task_id':'task-a','attempt_id':'attempt-a','generation':2,
              'kind':'committed_read_reference','intended':values['read-a'][0],
              'state':'original_read_body_unavailable','outcome':compact}
    return copy.deepcopy((profile,snapshot,manifest,record))


def change_row(snapshot, identity, column, change):
    table = snapshot['tables']['receipts']; index = table['columns'].index(column)
    row = next(row for row in table['rows'] if row[0] == identity)
    value = json.loads(row[index]) if column in ('request','outcome') else row[index]
    value = change(value)
    row[index] = common.canonical(value).decode() if column in ('request','outcome') else value


def changed(value, path, replacement):
    result = copy.deepcopy(value); node = result
    for key in path[:-1]: node = node[key]
    node[path[-1]] = replacement
    return result


class CompactReceiptTests(unittest.TestCase):
    def setUp(self):
        for target in ('subprocess.Popen','socket.socket'):
            guard = mock.patch(target, side_effect=AssertionError('no process or network in proof fixture'))
            guard.start(); self.addCleanup(guard.stop)

    def plan(self, profile, snapshot, manifest):
        return common.controller_receipt_plan(profile,snapshot,copy.deepcopy(snapshot),manifest=manifest)

    def test_already_compact_original_keeps_exact_request_commit_and_removed_body_evidence(self):
        profile,snapshot,manifest,record = fixture(); before = copy.deepcopy(snapshot)
        rows = self.plan(profile,snapshot,manifest)
        selected = next(v for v in rows if v['id']=='read-a')
        self.assertEqual(selected['record'],record)
        self.assertEqual(selected['namespace'],'committed_read_reference')
        self.assertEqual(selected['commit'],{'operation_id':'read-a','controller_revision':10})
        reply = {'status':'ok','data':{'kind':'operation','value':record},'commit':selected['commit'],
                 'observed_controller_revision':40}
        self.assertEqual(common.verify_controller_lookup(selected,reply),'committed_read_reference')
        self.assertEqual(snapshot,before)
        self.assertEqual(len(rows),4)
        self.assertTrue(all(v['namespace']=='application_mutation' for v in rows if v['id']!='read-a'))

    def test_compact_requires_each_authenticated_producer_and_exact_format(self):
        profile,snapshot,manifest,_ = fixture()
        for path in common.COMPACT_RECEIPT_SOURCES:
            for kind in ('missing','different'):
                wrong=copy.deepcopy(manifest)
                if kind=='missing': del wrong['source']['files'][path]
                else: wrong['source']['files'][path]['sha256']='0'*64
                with self.subTest(path=path,kind=kind), self.assertRaisesRegex(ValueError,'authenticated_sources'):
                    self.plan(profile,snapshot,wrong)
        with self.assertRaisesRegex(ValueError,'authenticated_sources'): self.plan(profile,snapshot,None)
        for key in ('wire','controller_schema','worker_format'):
            wrong=copy.deepcopy(profile);wrong['formats']['after'][key]-=1
            with self.subTest(format=key),self.assertRaises(ValueError): self.plan(wrong,snapshot,manifest)
        wrong=copy.deepcopy(snapshot);wrong['schema']=25
        oldprofile=copy.deepcopy(profile)
        oldprofile['formats']['before']['controller_schema']=25
        oldprofile['formats']['after']['controller_schema']=25
        with self.assertRaisesRegex(ValueError,'compact_original_schema'): self.plan(oldprofile,wrong,manifest)

    def test_each_original_causal_receipt_is_required(self):
        profile,snapshot,manifest,_=fixture()
        for identity in ('custody-a','safe-a','release-a'):
            bad=copy.deepcopy(snapshot)
            bad['tables']['receipts']['rows']=[r for r in bad['tables']['receipts']['rows'] if r[0]!=identity]
            with self.subTest(identity=identity),self.assertRaisesRegex(ValueError,'causal_receipt_missing'):
                self.plan(profile,bad,manifest)

    def test_causal_identity_policy_custody_and_first_safe_are_not_replaceable(self):
        cases = [
            ('read-a','request',['input','call','operation_id'],'other'),
            ('read-a','request',['input','call','caller','attempt_id'],'other'),
            ('read-a','request',['input','call','tool'],'collab_task_cancel'),
            ('read-a','outcome',['committed_read_reference','session_id'],'other'),
            ('read-a','outcome',['committed_read_reference','evidence','outcome_sha256'],'c'*64),
            ('read-a','outcome',['committed_read_reference','evidence','source'],{'kind':'unknown'}),
            ('release-a','request',['input','policy_revision'],'new-policy'),
            ('release-a','request',['input','policy','success_grace_ms'],101),
            ('release-a','request',['input','custody','manifest_sha256'],'c'*64),
            ('release-a','request',['input','reads',0,'commit','controller_revision'],11),
            ('release-a','outcome',['data','value','released'],[]),
            ('release-a','outcome',['data','value','eligible_at_ms'],1101),
            ('safe-a','outcome',['data','value','body_bytes_before'],1),
            ('safe-a','outcome',['data','value','released'],['read-a']),
            ('custody-a','request',['input','report','worker_id'],'other'),
            ('custody-a','request',['input','report','observation','proof','command_id'],'other'),
            ('custody-a','outcome',['data','value','revision'],8),
            ('custody-a','outcome',['data','value','attempt','uncertain'],True),
            ('custody-a','outcome',['data','value','attempt','stopped'],False),
            ('custody-a','outcome',['data','value','status'],'active'),
            ('custody-a','outcome',['data','value','input','definition','executor'],'coding'),
        ]
        for identity,column,path,replacement in cases:
            profile,snapshot,manifest,_=fixture()
            change_row(snapshot,identity,column,lambda v:changed(v,path,replacement))
            with self.subTest(identity=identity,path=path),self.assertRaises(ValueError):
                self.plan(profile,snapshot,manifest)

    def test_causal_revision_order_and_boolean_numbers_refuse(self):
        for identity,value in [('read-a',40),('custody-a',30),('safe-a',40),('release-a',10),('custody-a',True)]:
            profile,snapshot,manifest,_=fixture()
            change_row(snapshot,identity,'controller_revision',lambda _:value)
            with self.subTest(identity=identity,value=value),self.assertRaises(ValueError):
                self.plan(profile,snapshot,manifest)

    def test_live_inline_to_compact_transition_is_not_snapshot_preservation(self):
        profile,compact,manifest,record=fixture(); inline=copy.deepcopy(compact)
        change_row(inline,'read-a','outcome',lambda _:{'status':'ok','data':{'kind':'task','value':{'id':'task-a','original':'retained body'}}})
        with self.assertRaisesRegex(ValueError,'restart changed original logical controller state'):
            common.controller_receipt_plan(profile,inline,compact,manifest=manifest)
        legacy=next(v for v in self.plan(profile,inline,manifest) if v['id']=='read-a')
        reply={'status':'ok','data':{'kind':'operation','value':record},'commit':legacy['commit'],'observed_controller_revision':40}
        with self.assertRaisesRegex(ValueError,'live_receipt_record_changed'):
            common.verify_controller_lookup(legacy,reply)

    def test_live_compact_lookup_requires_exact_record_and_original_commit(self):
        profile,snapshot,manifest,record=fixture()
        selected=next(v for v in self.plan(profile,snapshot,manifest) if v['id']=='read-a')
        reply={'status':'ok','data':{'kind':'operation','value':record},'commit':selected['commit'],'observed_controller_revision':40}
        for path,value in [(['commit'],None),(['commit','controller_revision'],40),
                (['data','value','generation'],3),(['data','value','state'],'settled'),
                (['data','value','outcome','committed_read_reference','evidence','source'],{'kind':'unknown'}),
                (['observed_controller_revision'],9)]:
            with self.subTest(path=path),self.assertRaises(ValueError):
                common.verify_controller_lookup(selected,changed(reply,path,value))

    def test_retained_page_and_list_projection_is_joined_without_body_substitution(self):
        # Real Rust owns nested DTO validation. Python must preserve every saved
        # source field across the three causal plans and the returned record.
        page={'subject':{'kind':'task','task_id':'task-a','session_id':'session-a','part':'record'},
              'identity':{'source':{'kind':'task','task_id':'task-a','session_id':'session-a','part':'record','revision':'7'},
                          'digest':'sha256:'+'d'*64},
              'summary':{'kind':'task','status':'succeeded','parts':['record']},
              'encoding':'canonical_json_utf8','offset':0,'returned_bytes':4096,'total_bytes':4096,
              'complete':True,'next_cursor':None,'retained_truncation':False}
        for source in ({'kind':'page','page':page},
                       {'kind':'read_list','items':[],'complete':True,'next_cursor':None}):
            profile,snapshot,manifest,_=fixture()
            for identity,path in [('read-a',['committed_read_reference','evidence','source']),
                                  ('release-a',['input','reads',0,'evidence','source']),
                                  ('safe-a',['input','reads',0,'evidence','source']),
                                  ('custody-a',['input','report','observation','proof','reads',0,'evidence','source'])]:
                column='outcome' if identity=='read-a' else 'request'
                change_row(snapshot,identity,column,lambda v:changed(v,path,source))
            selected=next(v for v in self.plan(profile,snapshot,manifest) if v['id']=='read-a')
            self.assertEqual(selected['record']['outcome']['committed_read_reference']['evidence']['source'],source)
            bad=copy.deepcopy(snapshot)
            change_row(bad,'release-a','request',lambda v:changed(v,['input','reads',0,'evidence','source'],{'kind':'observation'}))
            with self.assertRaisesRegex(ValueError,'original_evidence'): self.plan(profile,bad,manifest)

    def test_external_namespace_priority_preserves_raw_compact_receipt(self):
        profile,snapshot,manifest,_=fixture()
        external={'id':'read-a','task_id':'task-a','attempt_id':'attempt-a','generation':2,
                  'kind':'checkpoint','intended':{'remote_ref':'original'},'state':'uncertain','outcome':None}
        snapshot['tables']['external_operations']['rows']=[['read-a',common.canonical(external).decode()]]
        selected=next(v for v in self.plan(profile,snapshot,manifest) if v['id']=='read-a')
        self.assertEqual(selected,{'id':'read-a','namespace':'external','record':external,'commit':None})
        self.assertIn('committed_read_reference',snapshot['tables']['receipts']['rows'][0][2])

    def test_current19_original_lookup_remains_legacy_without_new_source_authority(self):
        profile,snapshot,_,_=fixture()
        profile['formats']['before']=profile['formats']['after']={'wire':24,'controller_schema':25,'worker_format':19}
        snapshot['schema']=25
        snapshot['tables']['receipts']['rows']=[['old','{"operation":"old","input":{}}','{"status":"ok","data":{"kind":"unit"}}',7]]
        selected=self.plan(profile,snapshot,None)[0]
        self.assertEqual(selected,{'id':'old','namespace':'application_mutation',
            'record':{'id':'old','task_id':'','attempt_id':'','generation':0,'kind':'application_mutation',
                      'intended':{'operation':'old','input':{}},'state':'settled',
                      'outcome':{'status':'ok','data':{'kind':'unit'}}},
            'commit':{'operation_id':'old','controller_revision':7}})

    def test_host_api_accepts_compact_commit_only_with_selected_authenticated_sources(self):
        profile,_,manifest,record=fixture()
        host=hostmod.Host.__new__(hostmod.Host)
        host.p=profile;host.h={'api':{'host':'127.0.0.1','port':12345}}
        host.l={};host.r={'deadline_ms':10000};host.after=True;host._verified_manifest=manifest
        response={'request_id':'rollout-read','status':'ok','data':{'kind':'operation','value':record},
                  'commit':{'operation_id':'read-a','controller_revision':10},'observed_controller_revision':40}
        with mock.patch.object(hostmod.c,'http_read',return_value=common.canonical(response)) as http:
            self.assertEqual(host.api('get_operation',{'operation_id':'read-a'}),response)
            self.assertEqual(http.call_count,1)
            del host._verified_manifest
            with self.assertRaisesRegex(ValueError,'authenticated_sources'):
                host.api('get_operation',{'operation_id':'read-a'})
            host._verified_manifest=manifest
            bad=copy.deepcopy(response);bad['commit']['operation_id']='other'
            http.return_value=common.canonical(bad)
            with self.assertRaisesRegex(ValueError,'original mutation commit stamp differs'):
                host.api('get_operation',{'operation_id':'read-a'})

    def test_current19_host_lookup_still_needs_only_legacy_commit_contract(self):
        host=hostmod.Host.__new__(hostmod.Host)
        host.p={'formats':{'after':{'wire':24,'controller_schema':25,'worker_format':19}}}
        host.h={'api':{'host':'127.0.0.1','port':12345}}
        host.l={};host.r={'deadline_ms':10000};host.after=True
        response={'request_id':'rollout-read','status':'ok',
                  'data':{'kind':'operation','value':{'kind':'application_mutation'}},
                  'commit':{'operation_id':'original','controller_revision':7},'observed_controller_revision':9}
        with mock.patch.object(hostmod.c,'http_read',return_value=common.canonical(response)) as http:
            self.assertEqual(host.api('get_operation',{'operation_id':'original'}),response)
            bad=copy.deepcopy(response);bad['data']['value']['kind']='checkpoint'
            http.return_value=common.canonical(bad)
            with self.assertRaisesRegex(ValueError,'unexpected commit on pure read'):
                host.api('get_operation',{'operation_id':'original'})

    def test_original_cleanup_read_sidecars_remain_strict_even_same_byte_inode_change(self):
        host=hostmod.Host.__new__(hostmod.Host)
        host.p={'formats':{'after':{'wire':25,'controller_schema':26,'worker_format':20}}}
        host.h={'role':'worker'}
        # No before typed body was captured. A name/hash is not authority to
        # reconstruct an old plan or accept a mutable-directory exception.
        for name in ('cleanup-reads/00.json','cleanup-reads/custody-00.json',
                     'cleanup-reads/safe-00.json','cleanup-reads/release-00.json'):
            old={'sha256':'a'*64,'identity':[1,2,1000,1000,384]}
            stop={'state':{name:old}}
            with mock.patch.object(host,'api',side_effect=AssertionError('no API proof for missing old body')):
                self.assertEqual(host.worker_history(stop,{name:copy.deepcopy(old)}),({name:old},{}))
                for variant in ('inode','content','missing'):
                    current={name:copy.deepcopy(old)}
                    if variant=='inode':current[name]['identity'][1]+=1
                    elif variant=='content':current[name]['sha256']='b'*64
                    else:current={}
                    with self.subTest(name=name,variant=variant),self.assertRaisesRegex(ValueError,'original_worker_history_changed'):
                        host.worker_history(stop,current)
