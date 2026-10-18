"""Pure local scratch tests. No commands, service calls, sockets or provider use."""
import copy, os, signal, sqlite3, stat, struct, tempfile, unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
import rollout_common_v2 as common
import protected_rollout_v2 as adapter
import rollout_host_v2 as hostmod
import protected_release_v2 as producer
from rollout_fixture import Fixture, FakeProducer, save, error_cause

class RolloutTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='borg-rollout-pure-');self.addCleanup(self.temp.cleanup)
        self.f=Fixture(Path(self.temp.name));self.peer=FakeProducer(self.f.subject)
        self.stack=self.f.seams();self.stack.__enter__();self.addCleanup(self.stack.__exit__,None,None,None)
        self.no_process=patch('subprocess.Popen',side_effect=AssertionError('pure test must not start process'));self.no_process.start();self.addCleanup(self.no_process.stop)
        self.no_socket=patch('socket.socket',side_effect=AssertionError('pure test must not connect'));self.no_socket.start();self.addCleanup(self.no_socket.stop)
    def runner(self):
        r=self.f.rollout();r.producer.exchange=self.peer.exchange;return r
    def assert_error_message(self,result,message):
        self.assertEqual(error_cause(result)['message_sha256'],common.digest(message.encode()))
        self.assertEqual(result['error'],result['error_detail']['code'])
    def full_host_proof(self,result,host_id):
        reference=result['host_results'][host_id]['full_proof'];path=Path(reference['path'])
        raw=path.read_bytes();self.assertEqual(len(raw),reference['bytes'])
        self.assertEqual(common.digest(raw),reference['sha256'])
        self.assertTrue(path.is_relative_to(Path(self.f.profile['state_root'])))
        return common.strict(raw)
    def remote_runner(self):
        h=next(h for h in self.f.profile['hosts'] if h['role']=='controller')
        ssh=self.f.root/'existing-ssh-config';ssh.write_text('Host selected-controller\n');ssh.chmod(0o600)
        h['transport']={'kind':'ssh','ssh_config':{'path':str(ssh),'sha256':common.digest(ssh.read_bytes())},'alias':'selected-controller'}
        h['helper_uid']=0
        self.f.profile['producer_transport']={'kind':'ssh','host_id':h['id'],'config_path':'/etc/operator/producer.json'}
        config=common.strict(Path(self.f.profile['producer']['path']).read_bytes());config['producer_uid']=0
        self.f.profile['producer']=save(Path(self.f.profile['producer']['path']),config)
        self.f.controller['producers']=[{'id':'rollout','uid':0,'role':'rollout'}]
        self.f.profile['controller_config']=save(Path(self.f.profile['controller_config']['path']),self.f.controller)
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        r=self.f.rollout();r.end=common.now_ms()+60000
        return r
    def test_remote_producer_uses_only_selected_controller_route_and_pins(self):
        import shlex
        r=self.remote_runner();q={'revision':1,'operation_id':'exact','command':{'operation':'get_action','input':{'action_id':'rollout'}}}
        reply={'status':'ok','data':self.peer.action,'replayed':False}
        with patch.object(adapter.c,'run',return_value=common.canonical(reply)) as run:
            self.assertEqual(r.exchange(q),reply)
        argv,limits,deadline,data=run.call_args.args
        self.assertEqual(argv[:2],['/usr/bin/ssh','-F']);self.assertEqual(argv[-2],'selected-controller')
        remote=shlex.split(argv[-1]);self.assertEqual(remote[:5],['/usr/bin/sudo','-n','/usr/bin/env','-i','PATH=/usr/bin:/bin'])
        self.assertIn('exchange-stdio',remote)
        self.assertEqual(remote[remote.index('--config')+1],'/etc/operator/producer.json')
        self.assertEqual(remote[remote.index('--config-sha256')+1],self.f.profile['producer']['sha256'])
        self.assertEqual(remote[remote.index('--module-sha256')+1],self.f.profile['modules']['producer']['sha256'])
        self.assertEqual(data,common.canonical(q));self.assertEqual(r.producer_identity['uid'],0)
        self.assertEqual(limits['command_output_bytes'],min(r.l['command_output_bytes'],r.l['transport_bytes']));self.assertLessEqual(deadline,r.end)
        self.assertEqual(self.f.effects,[])
    def test_remote_transport_preserves_explicit_bounds_above_local_defaults(self):
        import shlex
        r=self.remote_runner()
        r.l.update(transport_bytes=producer.MAX*2,transport_timeout_ms=120000,
                   command_output_bytes=producer.MAX*3,command_timeout_ms=150000)
        r.end=201000
        q={'revision':1,'operation_id':'original-op','command':{'operation':'get_receipt','input':{'operation_id':'prior-op'}}}
        reply={'status':'ok','data':{'retained':'x'*(producer.MAX+1)},'replayed':False}
        self.assertGreater(len(common.canonical(reply)),producer.MAX)
        with patch.object(adapter.c,'now_ms',return_value=1000),patch.object(adapter.c,'run',return_value=common.canonical(reply)) as run:
            self.assertEqual(r.exchange(q),reply)
        argv,limits,deadline,data=run.call_args.args
        remote=shlex.split(argv[-1])
        self.assertEqual(int(remote[remote.index('--max-bytes')+1]),producer.MAX*2)
        self.assertEqual(int(remote[remote.index('--timeout-ms')+1]),120000)
        self.assertEqual(limits['command_timeout_ms'],120000)
        self.assertEqual(limits['command_output_bytes'],producer.MAX*2)
        self.assertEqual(deadline,121000);self.assertEqual(data,common.canonical(q))
        self.assertEqual(self.f.effects,[])
    def test_remote_producer_refuses_worker_or_new_route_and_changed_pin(self):
        r=self.remote_runner()
        for field,value in [('host_id','worker'),('kind','arbitrary'),('config_path','relative')]:
            profile=copy.deepcopy(self.f.profile);profile['producer_transport'][field]=value
            with self.assertRaises(ValueError):common.validate(profile)
        profile=copy.deepcopy(self.f.profile);profile['producer_transport']['arbitrary_command']='true'
        with self.assertRaises(ValueError):common.validate(profile)
        self.f.controller['worker_uids'].append(0);self.f.profile['controller_config']=save(Path(self.f.profile['controller_config']['path']),self.f.controller)
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        with self.assertRaisesRegex(ValueError,'rollout authority unavailable'):self.f.rollout()
        Path(self.f.profile['producer']['path']).write_bytes(b'{}')
        with patch.object(adapter.c,'run') as run:
            with self.assertRaisesRegex(ValueError,'pinned input changed'):r.exchange({'query':'never sent'})
            run.assert_not_called()
    def test_remote_lost_start_ack_looks_up_exact_receipt_without_second_send(self):
        r=self.remote_runner();r.work.mkdir();self.peer.fail='after'
        def transport(argv,limits,deadline,data):
            q=common.strict(data)
            try:return common.canonical(self.peer.exchange(r.config,q))
            finally:
                for record in self.peer.receipts.values():record['producer']=r.producer_identity
        with patch.object(adapter.c,'run',side_effect=transport):
            with self.assertRaisesRegex(ValueError,'loss after commit'):r.original_start()
            r.original_start(True)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
        self.assertEqual(self.peer.calls[-1]['command']['operation'],'get_receipt')
        self.assertEqual(r.original['state'],'started');self.assertEqual(self.f.effects,[])

    def test_real_package_files_phases_and_full_reconcile(self):
        unit_metadata={h['id']:(Path(h['unit']['path']).stat().st_uid,Path(h['unit']['path']).stat().st_gid,Path(h['unit']['path']).stat().st_mode) for h in self.f.profile['hosts']}
        r=self.runner();result=r.execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        self.assertTrue(all(self.f.active.values()));before=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(before,self.f.effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
        for h in self.f.profile['hosts']:
            st=Path(h['unit']['path']).stat();self.assertEqual((st.st_uid,st.st_gid,st.st_mode),unit_metadata[h['id']])
            installed=Path(h['release_root'])/self.f.artifact['package_sha256']
            self.assertEqual(common.digest((installed/'borg-runtime').read_bytes()),self.f.artifact['binary_sha256'])
            self.assertEqual((Path(h['state']['root'])/'helpers/owner.binding').read_bytes(),b'fixed-binding')
    def test_lost_start_before_commit_no_host_mutation_or_retry(self):
        self.peer.fail='before';a=self.runner().execute('apply');self.assertEqual(a['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        b=self.runner().execute('reconcile');self.assertEqual(b['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        self.assert_error_message(b,'original mutation not committed; no automatic retry');self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
    def test_lost_start_after_commit_only_recovers_original(self):
        self.peer.fail='after';self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        r=self.runner().execute('reconcile');self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
    def test_recorded_refusal_not_outer_lookup_success(self):
        r=self.runner();r.work.mkdir();r.end=common.now_ms()+60000
        q={'revision':1,'operation_id':'rollout:start','command':{'operation':'start_rollout','input':{'action_id':'rollout','subject':self.f.subject}}}
        save(r.work/'start.request.json',q);self.peer.receipts[q['operation_id']]={'producer':{'id':'rollout','uid':os.geteuid(),'role':'rollout'},'request':q,'reply':{'status':'stale_check','data':None,'replayed':False}}
        with self.assertRaises(adapter.Refused):r.original_start(True)
        self.assertEqual(self.f.effects,[])
    def test_changed_original_body_is_not_replayed(self):
        self.peer.fail='after';r=self.runner();r.execute('apply');path=r.work/'start.request.json';v=common.strict(path.read_bytes());v['command']['input']['subject']['policy']='other';save(path,v)
        result=self.runner().execute('reconcile');self.assert_error_message(result,'original mutation body changed');self.assertEqual(self.f.effects,[])
    def test_expired_original_start_does_not_authorize_new_effect(self):
        r=self.runner();r.work.mkdir();r.end=common.now_ms()+60000;r.original_start();r.original['dispatch_until_ms']=1;self.peer.action['dispatch_until_ms']=1
        with self.assertRaisesRegex(ValueError,'deadline expired'):r.before_effect()
        self.assertEqual(self.f.effects,[])
    def test_current_unknown_or_applied_cannot_resume_historical_started(self):
        r=self.runner();r.work.mkdir();r.end=common.now_ms()+60000;r.original_start()
        for state in ('unknown','applied'):
            self.peer.action['state']=state
            with self.assertRaisesRegex(ValueError,'not originally Started'):r.before_effect()
        self.assertEqual(self.f.effects,[])
    def test_loss_after_last_host_start_readonly_reconciles_all_receipts(self):
        self.f.lost_phase=['worker','start'];first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN',first)
        before=copy.deepcopy(self.f.effects);second=self.runner().execute('reconcile');self.assertEqual(second['status'],'APPLIED',second);self.assertEqual(self.f.effects,before)
        self.assertIn('rollout:observe-unknown',self.peer.receipts);self.assertIn('rollout:observe-applied',self.peer.receipts)
    def test_partial_host_install_does_not_resume_remaining_hosts(self):
        self.f.lost_phase=['controller','install'];first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN')
        before=copy.deepcopy(self.f.effects);second=self.runner().execute('reconcile');self.assertEqual(second['status'],'UNKNOWN');self.assertEqual(self.f.effects,before)
        self.assertFalse(self.f.active['controller']);self.assertFalse(self.f.active['worker'])
    def test_all_ten_artifact_fields_are_checked(self):
        for field in self.f.artifact:
            with self.subTest(field=field):
                subject=copy.deepcopy(self.f.subject);v=subject['artifact'][field];subject['artifact'][field]=v+1 if isinstance(v,int) else v+'x'
                with self.assertRaises((ValueError,TypeError)):common.verify_package(self.f.profile,self.f.profile['hosts'][0],subject)
    def test_formats_refuse_before_effect(self):
        self.f.profile['formats']['after']['worker_format']=8
        with self.assertRaisesRegex(ValueError,'incompatible'):common.validate(self.f.profile)
        self.assertEqual(self.f.effects,[])
    def test_forward_transition_preserves_originals_and_reconciles_without_effect_replay(self):
        self.f.forward_transition()
        worker=self.f.profile['hosts'][1];history=Path(worker['state']['root'])/'retained-journal.json'
        history.write_bytes(b'{"unknown_original_effect":"must remain unchanged"}')
        marker=Path(worker['state']['format_file']);old_marker=marker.read_bytes();old_inode=marker.stat().st_ino
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(common.strict(old_marker),{'format_revision':9})
        self.assertEqual(common.strict(marker.read_bytes()),{'format_revision':12});self.assertNotEqual(marker.stat().st_ino,old_inode)
        self.assertEqual(history.read_bytes(),b'{"unknown_original_effect":"must remain unchanged"}')
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        self.assertEqual(self.f.task_phases[0],False);self.assertTrue(all(self.f.task_phases[1:]))
        before=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED')
        self.assertEqual(self.f.effects,before)
        host=self.f.profile['hosts'][0]
        captures=list(Path(host['state_root']).glob('action-*/stopped-copy/bytes/tasks.sqlite'))
        self.assertEqual(len(captures),1)
        con=sqlite3.connect(captures[0]);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],12);con.close()
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
    def test_forward_transition_requires_exact_tuple_and_closed_evidence(self):
        self.f.forward_transition();common.validate(self.f.profile)
        for side,key,value in [('before','wire',12),('before','controller_schema',13),('before','worker_format',8),('after','wire',13),('after','controller_schema',15),('after','worker_format',11)]:
            with self.subTest(side=side,key=key):
                p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward']
        with self.assertRaisesRegex(ValueError,'qualified forward'):common.validate(p)
        for key in ('controller','worker','retained_state'):
            p=copy.deepcopy(self.f.profile);del p['formats']['forward']['evidence'][key]
            with self.assertRaisesRegex(ValueError,'qualification evidence'):common.validate(p)
        self.assertEqual(self.f.effects,[])
    def test_forward_qualification_or_source_drift_refuses_before_start(self):
        self.f.forward_transition()
        Path(self.f.profile['formats']['forward']['evidence']['retained_state']['path']).write_bytes(b'changed')
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'protected pinned input changed')
        self.assertEqual(self.f.effects,[]);self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
    def test_forward_package_and_migration_source_are_exact(self):
        self.f.forward_transition();h=self.f.profile['hosts'][0]
        for key in ('before_package_sha256','after_package_sha256'):
            p=copy.deepcopy(self.f.profile);p['formats']['forward'][key]='f'*64
            with self.assertRaisesRegex(ValueError,'package binding'):common.verify_package(p,h,self.f.subject)
        for key in common.FORWARD_SOURCES:
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][key]='f'*64
            with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,h,self.f.subject)
        p=copy.deepcopy(self.f.profile);del p['formats']['source_files']['src/controller/schema14-from13.sql']
        with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
        self.assertEqual(self.f.effects,[])
    def test_phase_wire_selects_old_preflight_and_new_restart_without_fallback(self):
        self.f.forward_transition();original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,14]);self.assertEqual(self.f.effects,[])
    def test_forward_controller_rejects_any_original_or_extra_state_change(self):
        self.f.forward_transition();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        root=Path(self.f.profile['hosts'][0]['state_root']);work=next(root.glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        cases=[]
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten original outcome';cases.append(v)
        v=copy.deepcopy(after);v['tables']['sessions']['rows'][0][-1]='invented delivery';cases.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][-1]='invented audit';cases.append(v)
        v=copy.deepcopy(after);v['tables']['sessions']['rows']=[];cases.append(v)
        v=copy.deepcopy(after);v['tables']['workflow_events']['rows']=[[1,0,'new activity','[]']];cases.append(v)
        v=copy.deepcopy(after);v['tables']['unexpected']={'columns':[],'rows':[]};cases.append(v)
        v=copy.deepcopy(after);v['tables']['charter_sessions']['columns'].append('extra');cases.append(v)
        v=copy.deepcopy(after);v['tables']['settings']['rows']=[r for r in v['tables']['settings']['rows'] if r[0]!='workflow_revision'];cases.append(v)
        v=copy.deepcopy(after);v['schema']=15;cases.append(v)
        for value in cases:
            with self.subTest(value=cases.index(value)):
                with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,value)
    def test_forward_actual_phase_stops_before_workers_on_rewritten_original(self):
        self.f.forward_transition();startup=self.f.on_start
        def corrupt(host):
            startup(host)
            if host['role']=='controller':
                con=sqlite3.connect(host['state']['database']);con.execute("UPDATE receipts SET outcome='changed'");con.commit();con.close()
        self.f.on_start=corrupt
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'restart changed original logical controller state or allowed additive delta')
        self.assertFalse(self.f.active['controller']);self.assertFalse(self.f.active['worker'])
        self.assertFalse(any(row[:2]==['worker','start'] for row in self.f.effects))
    def test_forward_crash_after_migration_never_rolls_back_or_repeats_start(self):
        self.f.forward_transition();startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled crash after committed forward marker')
        self.f.on_start=crash
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(effects,self.f.effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],16);con.close()
        self.assertFalse(result['automatic_rollback']);self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_forward_lost_worker_start_ack_only_reads_original_completion(self):
        self.f.forward_transition();self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN')
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(effects,self.f.effects)
    def test_forward_expiry_after_committed_migration_does_not_restore_or_restart(self):
        self.f.forward_transition();startup=self.f.on_start;clock=hostmod.c.now_ms;expired=[False]
        def expire(host):
            startup(host)
            if host['role']=='controller':expired[0]=True
        self.f.on_start=expire
        with patch.object(hostmod.c,'now_ms',side_effect=lambda:clock()+1000000 if expired[0] else clock()):
            result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'dispatch authorization expired before effect');self.assertFalse(result['automatic_rollback'])
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],16);con.close()
        self.assertEqual(sum(row[:2]==['controller','start'] for row in self.f.effects),1)
        self.assertFalse(any(row[:2]==['worker','start'] for row in self.f.effects))
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(effects,self.f.effects)
    def test_forward_premature_worker_marker_refuses_before_any_effect(self):
        self.f.forward_transition();save(Path(self.f.profile['hosts'][1]['state']['format_file']),{'format_revision':12})
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase');self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
    def test_forward_marker_exception_never_hides_changed_original_journal(self):
        self.f.forward_transition();h=self.f.profile['hosts'][1];path=Path(h['state']['root'])/'history.json';path.write_bytes(b'original unknown effect')
        self.f.lost_phase=['worker','start'];self.runner().execute('apply');path.write_bytes(b'tampered')
        effects=copy.deepcopy(self.f.effects);result=self.runner().execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'original_worker_history_changed');self.assertEqual(effects,self.f.effects)
    def test_forward_worker_waits_for_exact_delayed_marker_without_another_start(self):
        self.f.forward_transition();startup=self.f.on_start;worker=self.f.profile['hosts'][1];sleeps=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def progress(delay):
            sleeps.append(delay);self.assertGreater(delay,0)
            self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':9})
            startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=progress):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(sleeps),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_forward_worker_nontransition_exhausts_existing_readiness_bound(self):
        self.f.forward_transition();startup=self.f.on_start
        def wrong(host):
            if host['role']=='controller':startup(host)
        self.f.on_start=wrong
        clock=hostmod.c.now_ms;offset=[0];sleeps=[]
        def advance(delay):sleeps.append(delay);offset[0]+=self.f.profile['limits']['command_timeout_ms']
        with patch.object(hostmod.c,'now_ms',side_effect=lambda:clock()+offset[0]),patch.object(hostmod.time,'sleep',side_effect=advance):
            result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'service readiness deadline; state requires reconciliation');self.assertEqual(len(sleeps),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':9})
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_forward_worker_unknown_marker_refuses_without_readiness_poll(self):
        self.f.forward_transition();startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':13}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unknown format cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_forward_worker_marker_exception_does_not_allow_mode_or_journal_changes(self):
        self.f.forward_transition();startup=self.f.on_start
        def changed(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).chmod(0o644)
        self.f.on_start=changed
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker marker ownership/mode changed')
    def test_lossless_forward_preserves_originals_and_adds_only_empty_output_table(self):
        self.f.forward_transition(lossless=True)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],17)
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':13})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_lossless_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(lossless=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','controller_schema',16),('before','worker_format',12),('after','wire',14),('after','controller_schema',16),('after','worker_format',12)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=common.FORWARD_KIND
        with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.LOSSLESS_SOURCES-common.FORWARD_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_lossless_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(lossless=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'lossless-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,15]);self.assertEqual(self.f.effects,[])
    def test_lossless_forward_refuses_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(lossless=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.FORWARD_KIND;old['formats']['after']=dict(common.FORWARD_AFTER)
        older=copy.deepcopy(after);older['schema']=16
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_lossless_forward_crash_after_commit_retains17_without_effect_retry(self):
        self.f.forward_transition(lossless=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled lossless migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],17);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_lossless_forward_lost_ack_reconciles_original_marker13_completion(self):
        self.f.forward_transition(lossless=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':13})
    def test_lossless_forward_rejects_previous_target_marker12_without_polling(self):
        self.f.forward_transition(lossless=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':12}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_lossless_forward_waits_for_original9_to13_admission_once(self):
        self.f.forward_transition(lossless=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_diagnostic_forward_preserves_originals_and_adds_only_empty_output_and_credit_tables(self):
        self.f.forward_transition(diagnostic=True)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],18)
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertNotIn('native_request_diagnostic_credits',before['tables'])
        self.assertEqual(after['tables']['native_request_diagnostic_credits'],{'columns':['attempt_id','request_index','reservation_id','diagnostic_id','consumed'],'rows':[]})
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':14})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_diagnostic_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(diagnostic=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','controller_schema',16),('before','worker_format',12),('after','wire',17),('after','controller_schema',17),('after','worker_format',13)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=common.FORWARD_KIND
        with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.DIAGNOSTIC_SOURCES-common.LOSSLESS_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_diagnostic_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(diagnostic=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'lossless-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,16]);self.assertEqual(self.f.effects,[])
    def test_diagnostic_forward_refuses_credit_and_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(diagnostic=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['rows']=[['attempt',1,'reservation','diagnostic',0]];values.append(v)
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['columns'][-1]='state';values.append(v)
        v=copy.deepcopy(after);del v['tables']['native_request_diagnostic_credits'];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.LOSSLESS_KIND;old['formats']['after']=dict(common.LOSSLESS_AFTER)
        older=copy.deepcopy(after);older['schema']=17
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_diagnostic_forward_crash_after_commit_retains18_without_effect_retry(self):
        self.f.forward_transition(diagnostic=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled diagnostic migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],18);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_diagnostic_forward_lost_ack_reconciles_original_marker14_completion(self):
        self.f.forward_transition(diagnostic=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':14})
    def test_diagnostic_forward_rejects_previous_target_marker13_without_polling(self):
        self.f.forward_transition(diagnostic=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':13}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_diagnostic_forward_waits_for_original9_to14_admission_once(self):
        self.f.forward_transition(diagnostic=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_charter_adoption_forward_preserves_originals_and_adds_only_empty_output_and_credit_tables(self):
        self.f.forward_transition(charter_adoption=True)
        db=self.f.profile['hosts'][0]['state']['database']
        con=sqlite3.connect(db)
        con.execute("INSERT INTO agents VALUES('agent','original-selection')")
        con.execute("INSERT INTO definitions VALUES('agent','original-revision','original-definition-bytes')")
        con.commit();con.close()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],19)
        self.assertEqual(after['tables']['agents'],before['tables']['agents'])
        self.assertEqual(after['tables']['definitions'],before['tables']['definitions'])
        self.assertEqual(after['tables']['governance_configurations']['rows'],[])
        self.assertEqual(after['tables']['governance_state']['rows'],[])
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertNotIn('native_request_diagnostic_credits',before['tables'])
        self.assertEqual(after['tables']['native_request_diagnostic_credits'],{'columns':['attempt_id','request_index','reservation_id','diagnostic_id','consumed'],'rows':[]})
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':14})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_charter_adoption_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(charter_adoption=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','controller_schema',16),('before','worker_format',12),('after','wire',18),('after','controller_schema',17),('after','worker_format',13)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for kind in (common.FORWARD_KIND,common.LOSSLESS_KIND,common.DIAGNOSTIC_KIND):
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=kind
            with self.subTest(kind=kind),self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.CHARTER_ADOPTION_SOURCES-common.DIAGNOSTIC_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_charter_adoption_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(charter_adoption=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'charter-adoption-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,17]);self.assertEqual(self.f.effects,[])
    def test_charter_adoption_forward_refuses_credit_and_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(charter_adoption=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['rows']=[['attempt',1,'reservation','diagnostic',0]];values.append(v)
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['columns'][-1]='state';values.append(v)
        v=copy.deepcopy(after);del v['tables']['native_request_diagnostic_credits'];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.LOSSLESS_KIND;old['formats']['after']=dict(common.LOSSLESS_AFTER)
        older=copy.deepcopy(after);older['schema']=17
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_charter_adoption_forward_crash_after_commit_retains19_without_effect_retry(self):
        self.f.forward_transition(charter_adoption=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled charter-adoption migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],19);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_charter_adoption_forward_lost_ack_reconciles_original_marker14_completion(self):
        self.f.forward_transition(charter_adoption=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':14})
    def test_charter_adoption_forward_rejects_previous_target_marker13_without_polling(self):
        self.f.forward_transition(charter_adoption=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':13}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_charter_adoption_forward_waits_for_original9_to14_admission_once(self):
        self.f.forward_transition(charter_adoption=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_memory_notice_removal_forward_preserves_originals_and_adds_only_empty_output_and_credit_tables(self):
        self.f.forward_transition(memory_notice_removal=True)
        db=self.f.profile['hosts'][0]['state']['database']
        con=sqlite3.connect(db)
        con.execute("INSERT INTO agents VALUES('agent','original-selection')")
        con.execute("INSERT INTO definitions VALUES('agent','original-revision','original-definition-bytes')")
        con.commit();con.close()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],19)
        self.assertEqual(after['tables']['agents'],before['tables']['agents'])
        self.assertEqual(after['tables']['definitions'],before['tables']['definitions'])
        self.assertEqual(after['tables']['governance_configurations']['rows'],[])
        self.assertEqual(after['tables']['governance_state']['rows'],[])
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertNotIn('native_request_diagnostic_credits',before['tables'])
        self.assertEqual(after['tables']['native_request_diagnostic_credits'],{'columns':['attempt_id','request_index','reservation_id','diagnostic_id','consumed'],'rows':[]})
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':14})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_memory_notice_removal_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(memory_notice_removal=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','wire',12),('before','controller_schema',16),('before','worker_format',12),('after','wire',17),('after','wire',19),('after','controller_schema',17),('after','worker_format',13)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for kind in (common.FORWARD_KIND,common.LOSSLESS_KIND,common.DIAGNOSTIC_KIND,common.CHARTER_ADOPTION_KIND):
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=kind
            with self.subTest(kind=kind),self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.MEMORY_NOTICE_REMOVAL_SOURCES-common.CHARTER_ADOPTION_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_memory_notice_removal_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(memory_notice_removal=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'memory-notice-removal-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,18]);self.assertEqual(self.f.effects,[])
    def test_memory_notice_removal_forward_refuses_credit_and_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(memory_notice_removal=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['rows']=[['attempt',1,'reservation','diagnostic',0]];values.append(v)
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['columns'][-1]='state';values.append(v)
        v=copy.deepcopy(after);del v['tables']['native_request_diagnostic_credits'];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.LOSSLESS_KIND;old['formats']['after']=dict(common.LOSSLESS_AFTER)
        older=copy.deepcopy(after);older['schema']=17
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_memory_notice_removal_forward_crash_after_commit_retains19_without_effect_retry(self):
        self.f.forward_transition(memory_notice_removal=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled memory-notice-removal migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],19);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_memory_notice_removal_forward_lost_ack_reconciles_original_marker14_completion(self):
        self.f.forward_transition(memory_notice_removal=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':14})
    def test_memory_notice_removal_forward_rejects_previous_target_marker13_without_polling(self):
        self.f.forward_transition(memory_notice_removal=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':13}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_memory_notice_removal_forward_waits_for_original9_to14_admission_once(self):
        self.f.forward_transition(memory_notice_removal=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_pr_publication_forward_preserves_originals_and_adds_only_empty_output_and_credit_tables(self):
        self.f.forward_transition(pr_publication=True)
        db=self.f.profile['hosts'][0]['state']['database']
        con=sqlite3.connect(db)
        con.execute("INSERT INTO agents VALUES('agent','original-selection')")
        con.execute("INSERT INTO definitions VALUES('agent','original-revision','original-definition-bytes')")
        con.commit();con.close()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],20)
        self.assertEqual(after['tables']['agents'],before['tables']['agents'])
        self.assertEqual(after['tables']['definitions'],before['tables']['definitions'])
        self.assertEqual(after['tables']['governance_configurations']['rows'],[])
        self.assertEqual(after['tables']['governance_state']['rows'],[])
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertNotIn('native_request_diagnostic_credits',before['tables'])
        self.assertEqual(after['tables']['native_request_diagnostic_credits'],{'columns':['attempt_id','request_index','reservation_id','diagnostic_id','consumed'],'rows':[]})
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':15})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_pr_publication_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(pr_publication=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','wire',12),('before','controller_schema',16),('before','worker_format',12),('after','wire',18),('after','wire',20),('after','controller_schema',19),('after','worker_format',14)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for kind in (common.FORWARD_KIND,common.LOSSLESS_KIND,common.DIAGNOSTIC_KIND,common.CHARTER_ADOPTION_KIND,common.MEMORY_NOTICE_REMOVAL_KIND):
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=kind
            with self.subTest(kind=kind),self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.PR_PUBLICATION_SOURCES-common.MEMORY_NOTICE_REMOVAL_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_pr_publication_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(pr_publication=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'PR-publication-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,19]);self.assertEqual(self.f.effects,[])
    def test_pr_publication_forward_refuses_credit_and_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(pr_publication=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['rows']=[['attempt',1,'reservation','diagnostic',0]];values.append(v)
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['columns'][-1]='state';values.append(v)
        v=copy.deepcopy(after);del v['tables']['native_request_diagnostic_credits'];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.LOSSLESS_KIND;old['formats']['after']=dict(common.LOSSLESS_AFTER)
        older=copy.deepcopy(after);older['schema']=17
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_pr_publication_forward_crash_after_commit_retains20_without_effect_retry(self):
        self.f.forward_transition(pr_publication=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled PR-publication migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],20);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_pr_publication_forward_lost_ack_reconciles_original_marker15_completion(self):
        self.f.forward_transition(pr_publication=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':15})
    def test_pr_publication_forward_rejects_previous_target_marker14_without_polling(self):
        self.f.forward_transition(pr_publication=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':14}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_pr_publication_forward_waits_for_original9_to15_admission_once(self):
        self.f.forward_transition(pr_publication=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_pr_forward_refuses_old_worker_pr_namespace_before_effects(self):
        self.f.forward_transition(pr_publication=True)
        worker=self.f.profile['hosts'][1]
        root=Path(worker['state']['root'])/'pull-requests';root.mkdir(mode=0o700)
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'installed worker9 pull-requests namespace is not absent')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_instruction_source_forward_preserves_originals_and_adds_only_empty_output_and_credit_tables(self):
        self.f.forward_transition(instruction_source=True)
        db=self.f.profile['hosts'][0]['state']['database']
        con=sqlite3.connect(db)
        con.execute("INSERT INTO agents VALUES('agent','original-selection')")
        con.execute("INSERT INTO definitions VALUES('agent','original-revision','original-definition-bytes')")
        con.commit();con.close()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller,worker=self.f.profile['hosts']
        work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual(before['schema'],12);self.assertEqual(after['schema'],21)
        self.assertEqual(after['tables']['agents'],before['tables']['agents'])
        self.assertEqual(after['tables']['definitions'],before['tables']['definitions'])
        self.assertEqual(after['tables']['governance_configurations']['rows'],[])
        self.assertEqual(after['tables']['governance_state']['rows'],[])
        self.assertNotIn('charter_output_chunks',before['tables'])
        self.assertNotIn('native_request_diagnostic_credits',before['tables'])
        self.assertEqual(after['tables']['native_request_diagnostic_credits'],{'columns':['attempt_id','request_index','reservation_id','diagnostic_id','consumed'],'rows':[]})
        self.assertEqual(after['tables']['charter_output_chunks'],{'columns':['request_id','chunk_index','operation_id','reference','body'],'rows':[]})
        self.assertEqual(after['tables']['receipts']['rows'],[row+[None] for row in before['tables']['receipts']['rows']])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':16})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
    def test_instruction_source_forward_requires_its_exact_contract_and_all_new_source_bindings(self):
        self.f.forward_transition(instruction_source=True);common.validate(self.f.profile);host=self.f.profile['hosts'][0]
        for side,key,value in [('before','wire',12),('before','controller_schema',16),('before','worker_format',12),('after','wire',19),('after','wire',21),('after','controller_schema',20),('after','worker_format',15)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value
            with self.subTest(side=side,key=key):
                with self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for kind in (common.FORWARD_KIND,common.LOSSLESS_KIND,common.DIAGNOSTIC_KIND,common.CHARTER_ADOPTION_KIND,common.MEMORY_NOTICE_REMOVAL_KIND,common.PR_PUBLICATION_KIND):
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=kind
            with self.subTest(kind=kind),self.assertRaisesRegex(ValueError,'unsupported'):common.validate(p)
        for name in common.INSTRUCTION_SOURCE_SOURCES-common.PR_PUBLICATION_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name]
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError,'source inventory'):common.validate(p)
            p=copy.deepcopy(self.f.profile);p['formats']['source_files'][name]='f'*64
            with self.subTest(changed=name):
                with self.assertRaisesRegex(ValueError,'source binding'):common.verify_package(p,host,self.f.subject)
        self.assertEqual(self.f.effects,[])
    def test_instruction_source_forward_reads_exact_phase_wire_without_fallback(self):
        self.f.forward_transition(instruction_source=True);original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'instruction-source-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[11,20]);self.assertEqual(self.f.effects,[])
    def test_instruction_source_forward_refuses_credit_and_chunk_rows_shape_or_original_record_changes(self):
        self.f.forward_transition(instruction_source=True);self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        values=[]
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['rows']=[['attempt',1,'reservation','diagnostic',0]];values.append(v)
        v=copy.deepcopy(after);v['tables']['native_request_diagnostic_credits']['columns'][-1]='state';values.append(v)
        v=copy.deepcopy(after);del v['tables']['native_request_diagnostic_credits'];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['rows']=[['request',0,'invented','{}',{'blob':'00'}]];values.append(v)
        v=copy.deepcopy(after);v['tables']['charter_output_chunks']['columns'][-1]='summary';values.append(v)
        v=copy.deepcopy(after);del v['tables']['charter_output_chunks'];values.append(v)
        v=copy.deepcopy(after);v['tables']['receipts']['rows'][0][2]='rewritten';values.append(v)
        for v in values:
            with self.assertRaises(ValueError):common.controller_retained(self.f.profile,before,v)
        old=copy.deepcopy(self.f.profile);old['formats']['forward']['kind']=common.LOSSLESS_KIND;old['formats']['after']=dict(common.LOSSLESS_AFTER)
        older=copy.deepcopy(after);older['schema']=17
        with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(old,before,older)
    def test_instruction_source_forward_crash_after_commit_retains21_without_effect_retry(self):
        self.f.forward_transition(instruction_source=True);startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled instruction-source migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],21);self.assertEqual(con.execute('SELECT count(*) FROM charter_output_chunks').fetchone()[0],0);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))
    def test_instruction_source_forward_lost_ack_reconciles_original_marker16_completion(self):
        self.f.forward_transition(instruction_source=True);self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':16})
    def test_instruction_source_forward_rejects_previous_target_marker15_without_polling(self):
        self.f.forward_transition(instruction_source=True);startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':15}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')
    def test_instruction_source_forward_waits_for_original9_to16_admission_once(self):
        self.f.forward_transition(instruction_source=True);startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_instruction_source_forward_refuses_old_worker_pr_namespace_before_effects(self):
        self.f.forward_transition(instruction_source=True)
        worker=self.f.profile['hosts'][1]
        root=Path(worker['state']['root'])/'pull-requests';root.mkdir(mode=0o700)
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'installed worker9 pull-requests namespace is not absent')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_pr_worker_namespace_is_in_full_inventory_and_original_history_check(self):
        worker=self.f.profile['hosts'][1]
        # Same-format15 controlled state, not an installed9 migration fixture.
        self.f.profile['formats']['before']=dict(common.PR_PUBLICATION_AFTER)
        self.f.profile['formats']['after']=dict(common.PR_PUBLICATION_AFTER)
        save(Path(worker['state']['format_file']),{'format_revision':15})
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);con.execute('PRAGMA user_version=20');con.close()
        root=Path(worker['state']['root'])/'pull-requests';root.mkdir(mode=0o700)
        phase=root/'original.json';phase.write_bytes(b'original unresolved PR phase')
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN')
        work=next(Path(worker['state_root']).glob('action-*'))
        before=common.strict((work/'stop.result.json').read_bytes())['state']
        self.assertEqual(before['pull-requests/original.json']['sha256'],common.digest(phase.read_bytes()))
        phase.write_bytes(b'changed unresolved PR phase')
        effects=copy.deepcopy(self.f.effects);result=self.runner().execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'original_worker_history_changed')
        self.assertEqual(self.f.effects,effects)

    def codex_arg0_state(self,instruction_source=False):
        """Original completed Coding command plus real inert scratch links."""
        self.f.forward_transition(pr_publication=not instruction_source,instruction_source=instruction_source)
        worker=self.f.profile['hosts'][1];root=Path(worker['state']['root'])
        attempt_id='attempt-original-coding-1';command_id='run-'+attempt_id
        definition={'id':'original-agent','revision':'one','executor':'coding'}
        command={'kind':'run_attempt','command_id':command_id,
            'session':{'id':'original-session','agent_id':definition['id'],'definition':definition,'task_id':'original-task'},
            'attempt':{'id':attempt_id,'session_id':'original-session','task_id':'original-task','generation':1,
                'worker_id':worker['id'],'worker_incarnation':'old-invocation','phase':'assigned',
                'deadline_unix_ms':60000,'stopped':False,'uncertain':False},
            'task':{'instruction':'Controlled retained coding command','definition':definition,'repository':None,
                'acceptance_commands':[],'output_schema':None,'dependencies':[],
                'limits':{'deadline_unix_ms':60000,'model_requests':1,'output_bytes':1024,'result_bytes':2048,'max_delegation_depth':0}},
            'checkpoint':None,'continuation':None}
        receipt=root/'commands'/(command_id.encode().hex()+'.json')
        save(receipt,{'command':command,'completed':True,'effects_started':True})
        directory=root
        for name in ('codex',attempt_id.encode().hex(),'tmp','arg0-linux','codex-arg0Ab12Cd'):
            directory=directory/name;directory.mkdir(mode=0o700)
        link=directory/'apply_patch';target='/controlled/absent-original-codex'
        link.symlink_to(target)
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'codex-link-proof','deadline_ms':common.now_ms()+60000})
        return host,root,receipt,link,target

    def test_pr_codex_arg0_links_preserve_exact_bytes_metadata_and_original_history(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        links=[link]
        for name in ('applypatch','codex-linux-sandbox'):
            item=link.with_name(name);item.symlink_to(target);links.append(item)
        rows=host.state_files()
        for item in links:
            m=item.lstat();raw=os.fsencode(target)
            self.assertEqual(rows[str(item.relative_to(root))],{'sha256':common.digest(raw),
                'identity':[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)],
                'kind':'codex_arg0_symlink','bytes':len(raw),'mtime_ns':m.st_mtime_ns,'ctime_ns':m.st_ctime_ns})
        self.assertEqual(rows,host.state_files())
        self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED')
        self.assertEqual(effects,self.f.effects)
        for item in links:
            self.assertEqual(os.readlink(item),target)
            self.assertEqual(host.state_files()[str(item.relative_to(root))],rows[str(item.relative_to(root))])

    def test_instruction_source_codex_arg0_links_preserve_exact_bytes_metadata_and_original_history(self):
        host,root,receipt,link,target=self.codex_arg0_state(instruction_source=True)
        links=[link]
        for name in ('applypatch','codex-linux-sandbox'):
            item=link.with_name(name);item.symlink_to(target);links.append(item)
        rows=host.state_files()
        for item in links:
            m=item.lstat();raw=os.fsencode(target)
            self.assertEqual(rows[str(item.relative_to(root))],{'sha256':common.digest(raw),
                'identity':[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)],
                'kind':'codex_arg0_symlink','bytes':len(raw),'mtime_ns':m.st_mtime_ns,'ctime_ns':m.st_ctime_ns})
        self.assertEqual(rows,host.state_files())
        self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED')
        self.assertEqual(effects,self.f.effects)
        for item in links:
            self.assertEqual(os.readlink(item),target)
            self.assertEqual(host.state_files()[str(item.relative_to(root))],rows[str(item.relative_to(root))])

    def test_pr_codex_arg0_escaping_target_is_hashed_without_opening(self):
        host,root,receipt,link,_=self.codex_arg0_state()
        target=self.f.root/'outside-worker-state';target.write_bytes(b'never read or execute this target')
        link.unlink();link.symlink_to(target)
        opened=os.open
        def guarded(path,*args,**kwargs):
            if not isinstance(path,int) and os.fsdecode(path) in (str(target),str(link),link.name):
                raise AssertionError('symlink or escaped target opened')
            return opened(path,*args,**kwargs)
        with patch.object(hostmod.os,'open',side_effect=guarded):rows=host.state_files()
        self.assertEqual(rows[str(link.relative_to(root))]['sha256'],common.digest(os.fsencode(str(target))))
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_retargeted_link_cannot_pass_original_history(self):
        host,root,receipt,link,_=self.codex_arg0_state()
        self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        effects=copy.deepcopy(self.f.effects)
        link.unlink();link.symlink_to('/different/inert-target')
        result=self.runner().execute('reconcile')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'original_worker_history_changed')
        self.assertEqual(self.f.effects,effects)

    def test_pr_codex_arg0_replacement_during_read_refuses(self):
        host,root,receipt,link,target=self.codex_arg0_state();readlink=os.readlink;changed=[]
        def race(path,*args,**kwargs):
            raw=readlink(path,*args,**kwargs)
            if os.fsdecode(path)==link.name and not changed:
                link.unlink();link.symlink_to(target+'-changed');changed.append(True)
            return raw
        with patch.object(hostmod.os,'readlink',side_effect=race):
            with self.assertRaisesRegex(ValueError,'changed'):host.state_files()
        self.assertEqual(changed,[True]);self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_replacement_after_read_refuses(self):
        host,root,receipt,link,target=self.codex_arg0_state();strict=hostmod.c.strict;changed=[]
        def race(raw):
            value=strict(raw)
            if isinstance(value,dict) and value.get('command',{}).get('kind')=='run_attempt' and not changed:
                link.unlink();link.symlink_to(target+'-changed');changed.append(True)
            return value
        with patch.object(hostmod.c,'strict',side_effect=race):
            with self.assertRaisesRegex(ValueError,'changed'):host.state_files()
        self.assertEqual(changed,[True]);self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_requires_original_completed_coding_attempt_association(self):
        host,root,receipt,link,target=self.codex_arg0_state();original=common.strict(receipt.read_bytes())
        cases=[('incomplete',lambda row:row.update(completed=False)),
            ('native',lambda row:row['command']['task']['definition'].update(executor='native')),
            ('other-attempt',lambda row:row['command']['attempt'].update(id='different-attempt')),
            ('not-run',lambda row:row['command'].update(kind='stop_attempt'))]
        for label,mutate in cases:
            with self.subTest(label=label):
                row=copy.deepcopy(original);mutate(row);save(receipt,row)
                with self.assertRaisesRegex(ValueError,'completed original Coding command'):host.state_files()
                save(receipt,original)
        wrong=receipt.with_name('wrong-command-name.json');receipt.rename(wrong)
        with self.assertRaisesRegex(ValueError,'command filename differs'):host.state_files()
        wrong.rename(receipt);receipt.unlink()
        with self.assertRaisesRegex(ValueError,'completed original Coding command'):host.state_files()
        save(receipt,original);self.assertIn(str(link.relative_to(root)),host.state_files())
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_scope_never_extends_to_other_transition_or_controller(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        transition=self.f.profile['formats']['forward'];kind=transition['kind']
        transition['kind']=common.MEMORY_NOTICE_REMOVAL_KIND
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        transition['kind']=kind;host.h['role']='controller'
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        host.h['role']='worker';del self.f.profile['formats']['forward']
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_unknown_sibling_or_suffix_and_special_file_refuse(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        sibling=link.with_name('unrecognized-helper');sibling.symlink_to(target)
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        sibling.unlink();directory=link.parent;wrong=directory.with_name('codex-arg0Ab12CdX');directory.rename(wrong)
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        wrong.rename(directory)
        fifo=root/'unexpected.fifo';os.mkfifo(fifo,0o600)
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        fifo.unlink();alias=root/'unexpected-directory';alias.symlink_to(directory,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_parent_authority_and_link_count_remain_strict(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        link.parent.chmod(0o777)
        with self.assertRaisesRegex(ValueError,'parent ownership'):host.state_files()
        link.parent.chmod(0o700);uid=host.h['service_uid'];host.h['service_uid']=uid+1
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        host.h['service_uid']=uid
        alias=link.with_name('applypatch');os.link(link,alias,follow_symlinks=False)
        self.assertEqual(link.lstat().st_nlink,2)
        with self.assertRaisesRegex(ValueError,'nonregular/aliased'):host.state_files()
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_inventory_bounds_include_link_bytes_and_deadline(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        size=sum(p.lstat().st_size for p in root.rglob('*') if not p.is_dir())
        previous=host.l['state_bytes'];host.l['state_bytes']=size-1
        with self.assertRaisesRegex(ValueError,'byte bound'):host.state_files()
        host.l['state_bytes']=previous;host.r['deadline_ms']=0
        with patch.object(hostmod.os,'readlink',side_effect=AssertionError('expired inventory must not read link')) as readlink:
            with self.assertRaisesRegex(ValueError,'deadline'):host.state_files()
            readlink.assert_not_called()
        self.assertEqual(self.f.effects,[])

    def test_pr_codex_arg0_preservation_does_not_discharge_process_cleanup(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        self.assertIn(str(link.relative_to(root)),host.state_files())
        record=Path(host.h['helpers'][0])/'4242.json';save(record,{'original_process':'still requires cleanup'})
        before=record.read_bytes()
        with self.assertRaisesRegex(ValueError,'unfinished helper ownership remains'):host.clean()
        self.assertEqual(record.read_bytes(),before);self.assertEqual(os.readlink(link),target)
        self.assertEqual(self.f.effects,[])

    def opaque_stream_probe(self,path,chunk,on_read=None):
        """Instrument only the selected real file descriptor, not command JSON."""
        original=os.fdopen;identity=path.stat();reads=[]
        case=self
        class Stream:
            def __init__(self,stream):self.stream=stream
            def __enter__(self):self.stream.__enter__();return self
            def __exit__(self,*args):return self.stream.__exit__(*args)
            def read(self,size=-1):
                case.assertGreater(size,0);case.assertLessEqual(size,chunk)
                raw=self.stream.read(size);reads.append((size,len(raw)))
                if on_read:on_read(len(reads),raw)
                return raw
        def opened(fd,*args,**kwargs):
            m=os.fstat(fd);stream=original(fd,*args,**kwargs)
            return Stream(stream) if (m.st_dev,m.st_ino)==(identity.st_dev,identity.st_ino) else stream
        return opened,reads

    def test_opaque_state_multichunk_hash_and_original_command_decoding(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        chunk=host.l['io_chunk_bytes'];raw=(b'opaque\x00retained-history\xff'*(chunk//10+1))[:chunk*2+17]
        path=root/'opaque-history.bin';path.write_bytes(raw);before=path.stat();command=receipt.read_bytes()
        opened,reads=self.opaque_stream_probe(path,chunk);strict=hostmod.c.strict;decoded=[]
        def record_decode(data):decoded.append(data);return strict(data)
        with patch.object(hostmod.os,'fdopen',side_effect=opened),patch.object(hostmod.c,'strict',side_effect=record_decode):
            rows=host.state_files()
        self.assertGreaterEqual(sum(n>0 for _,n in reads),3)
        self.assertEqual(sum(n for _,n in reads),len(raw));self.assertIn(command,decoded)
        self.assertEqual(rows[path.name],{'sha256':common.digest(raw),
            'identity':[before.st_dev,before.st_ino,before.st_uid,before.st_gid,stat.S_IMODE(before.st_mode)]})
        self.assertEqual(rows[str(receipt.relative_to(root))]['sha256'],common.digest(command))
        self.assertIn(str(link.relative_to(root)),rows);self.assertEqual(receipt.read_bytes(),command)
        self.assertEqual(self.f.effects,[])

    def test_opaque_state_deadline_is_checked_between_chunks(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        chunk=host.l['io_chunk_bytes'];path=root/'opaque-history.bin';path.write_bytes(b'x'*(chunk*3+1))
        clock=[1000];host.r['deadline_ms']=2000
        def expire(index,raw):
            if index==1:clock[0]=2000
        opened,reads=self.opaque_stream_probe(path,chunk,expire)
        with patch.object(hostmod.c,'now_ms',side_effect=lambda:clock[0]),patch.object(hostmod.os,'fdopen',side_effect=opened):
            with self.assertRaisesRegex(ValueError,'deadline'):host.state_files()
        self.assertEqual(len(reads),1);self.assertEqual(self.f.effects,[])

    def test_opaque_state_mutation_or_growth_during_chunk_refuses(self):
        host,root,receipt,link,target=self.codex_arg0_state()
        chunk=host.l['io_chunk_bytes'];path=root/'opaque-history.bin';raw=b'x'*(chunk*3+1)
        for mode in ('mutate','grow'):
            with self.subTest(mode=mode):
                path.write_bytes(raw)
                def change(index,data):
                    if index!=1:return
                    with path.open('r+b' if mode=='mutate' else 'ab') as stream:stream.write(b'changed')
                opened,reads=self.opaque_stream_probe(path,chunk,change)
                with patch.object(hostmod.os,'fdopen',side_effect=opened):
                    with self.assertRaisesRegex(ValueError,'changed|grew'):host.state_files()
                self.assertGreaterEqual(len(reads),1);self.assertEqual(self.f.effects,[])

    def test_opaque_state_aggregate_budget_refuses_next_file_before_open(self):
        # Isolate two ordinary files so either enumeration order proves the
        # remaining aggregate allowance, rather than only a per-file limit.
        worker=copy.deepcopy(self.f.profile['hosts'][1]);root=Path(worker['state']['root'])/'opaque-only';root.mkdir(mode=0o700)
        worker['state']['root']=str(root)
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'opaque-budget','deadline_ms':common.now_ms()+60000})
        chunk=host.l['io_chunk_bytes'];raw=b'x'*(chunk+1)
        for name in ('a.bin','b.bin'):(root/name).write_bytes(raw)
        host.l['state_bytes']=len(raw)*2-1
        original=os.open;opened=[];root_identity=root.stat()
        def record_open(path,*args,**kwargs):
            if path in ('a.bin','b.bin') and 'dir_fd' in kwargs:
                m=os.fstat(kwargs['dir_fd'])
                if (m.st_dev,m.st_ino)==(root_identity.st_dev,root_identity.st_ino):opened.append(path)
            return original(path,*args,**kwargs)
        with patch.object(hostmod.os,'open',side_effect=record_open):
            with self.assertRaisesRegex(ValueError,'byte bound'):host.state_files()
        self.assertEqual(len(opened),1);self.assertEqual(self.f.effects,[])

    def test_stopped_forward_worker_never_starts_or_stops_old_binary(self):
        h=self.f.stopped_worker();original=Path(h['owner_lock']).stat()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        self.assertFalse(any(row[:2]==['worker','stop'] for row in self.f.effects))
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
        final=Path(h['owner_lock']).stat();self.assertEqual((original.st_dev,original.st_ino),(final.st_dev,final.st_ino))
        self.assertEqual(common.strict(Path(h['state']['format_file']).read_bytes()),{'format_revision':14})
        before=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(before,self.f.effects)
    def retained_startup_pair(self,h):
        path=Path(h['state']['root'])/'limits.json';before=path.read_bytes()
        h['state']['retained_policy']={'path':str(path),'sha256':common.digest(before)}
        selected=copy.deepcopy(self.f.runtime_limits);selected['revision']='current-cleanup-only';selected['retention']['worker']['idle_cache_grace_ms']+=1
        h['state']['policy']=save(Path(h['state']['policy']['path']),selected)
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        return path,before,selected
    def test_stopped_worker_preserves_distinct_retained_policy_and_new_registration(self):
        h=self.f.stopped_worker();path,before,selected=self.retained_startup_pair(h)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(path.read_bytes(),before)
        journal=common.strict((path.parent/'mutations/registration.json').read_bytes())
        self.assertEqual(journal['limits'],self.f.runtime_limits);self.assertNotEqual(journal['limits'],selected)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
    def test_retained_policy_pair_refuses_unpinned_or_execution_policy_changes(self):
        h=self.f.stopped_worker();path,before,selected=self.retained_startup_pair(h)
        host=hostmod.Host(self.f.profile,h,self.f.subject,{'action_id':'policy','deadline_ms':common.now_ms()+60000})
        self.assertEqual(host.retained_policy(),self.f.runtime_limits)
        pin=h['state'].pop('retained_policy')
        with self.assertRaisesRegex(ValueError,'retained policy differs'):host.retained_policy()
        h['state']['retained_policy']=pin
        changed=copy.deepcopy(selected);changed['transport']['body_bytes']+=1
        h['state']['policy']=save(Path(h['state']['policy']['path']),changed)
        with self.assertRaisesRegex(ValueError,'execution policy differs'):host.retained_policy()
        h['state']['policy']=save(Path(h['state']['policy']['path']),selected)
        path.write_bytes(before+b' ')
        with self.assertRaisesRegex(ValueError,'pin changed'):host.retained_policy()
        self.assertEqual(self.f.effects,[])
    def test_retained_policy_pin_cannot_select_another_root_or_controller(self):
        h=self.f.stopped_worker();self.retained_startup_pair(h)
        p=copy.deepcopy(self.f.profile);p['hosts'][1]['state']['retained_policy']['path']='/other/limits.json'
        with self.assertRaisesRegex(ValueError,'policy path'):common.validate(p)
        p=copy.deepcopy(self.f.profile);p['hosts'][0]['state']['retained_policy']=h['state']['retained_policy']
        with self.assertRaisesRegex(ValueError,'policy path'):common.validate(p)

    def test_stopped_forward_worker_requires_explicit_exact_profile(self):
        h=self.f.stopped_worker();proof=h.pop('stopped_predecessor')
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
        h['stopped_predecessor']=proof
        for field,value in [('MainPID','42'),('ActiveState','active'),('SubState','failed')]:
            p=copy.deepcopy(self.f.profile);p['hosts'][1]['stopped_predecessor']['service'][field]=value
            with self.assertRaises(ValueError):common.validate(p)
        p=copy.deepcopy(self.f.profile);p['hosts'][0]['stopped_predecessor']=proof
        with self.assertRaisesRegex(ValueError,'forward worker'):common.validate(p)
    def test_stopped_forward_worker_refuses_owner_lock_without_mutation(self):
        import fcntl
        h=self.f.stopped_worker()
        with Path(h['owner_lock']).open('r+b') as f:
            fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
            result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
    def test_stopped_forward_worker_never_treats_failed_traversal_as_empty(self):
        h=self.f.stopped_worker();original=hostmod.Host.__mro__[1];scandir=os.scandir
        host=hostmod.Host(self.f.profile,h,self.f.subject,{'action_id':'proof','deadline_ms':common.now_ms()+60000})
        for kind,target in [('cgroup',Path('/sys/fs/cgroup/fixture')),
                            ('helpers',Path(h['helpers'][0])),('state',Path(h['state']['root']))]:
            for error in (PermissionError,FileNotFoundError):
                with self.subTest(kind=kind,error=error.__name__):
                    def unreadable(path):
                        if isinstance(path,int):
                            m=os.fstat(path);wanted=target.lstat()
                            matches=(m.st_dev,m.st_ino)==(wanted.st_dev,wanted.st_ino)
                        else:matches=Path(path)==target
                        if matches:raise error('controlled inventory unavailable')
                        return scandir(path)
                    pids=original.pids if kind=='cgroup' else lambda host,row: []
                    with patch.object(hostmod.Host,'pids',pids),patch.object(hostmod.os,'scandir',side_effect=unreadable):
                        with host.owner():
                            with self.assertRaisesRegex(error,'controlled inventory unavailable'):
                                host.check_stopped_predecessor()
                    self.assertEqual(self.f.effects,[])
        # The existing explicit inactive-service absence remains distinct from
        # a nonempty cgroup path that cannot actually be inventoried.
        self.assertEqual(original.pids(host,{'ControlGroup':''}),[])
    def test_stopped_forward_worker_refuses_live_group_or_pending_work(self):
        h=self.f.stopped_worker()
        with patch.object(hostmod.Host,'pids',return_value=[4242]):
            result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'service/process group not verified stopped');self.assertEqual(self.f.effects,[])
        save(Path(h['state']['root'])/'outbox/pending.json',{'original':'unknown'})
        # A new action cannot turn the original pending report into a clean stop.
        r=self.f.rollout('other');r.producer.exchange=self.peer.exchange
        with self.assertRaisesRegex(ValueError,'pending worker report'):
            host=hostmod.Host(self.f.profile,h,self.f.subject,{'action_id':'proof','deadline_ms':common.now_ms()+60000})
            with host.owner():host.check_stopped_predecessor()
    def test_stopped_forward_worker_refuses_retained_or_owner_identity_drift(self):
        h=self.f.stopped_worker();host=hostmod.Host(self.f.profile,h,self.f.subject,{'action_id':'proof','deadline_ms':common.now_ms()+60000})
        owner=Path(h['owner_lock']);original=owner.read_bytes();owner.write_bytes(original+b'drift')
        with host.owner():
            with self.assertRaisesRegex(ValueError,'retained state changed'):host.check_stopped_predecessor()
        owner.write_bytes(original);h['stopped_predecessor']['owner_identity'][1]+=1
        with host.owner():
            with self.assertRaisesRegex(ValueError,'owner changed'):host.check_stopped_predecessor()
        self.assertEqual(self.f.effects,[])
    def test_stopped_forward_worker_refuses_service_race_during_state_read(self):
        h=self.f.stopped_worker();original=hostmod.Host.state_files
        def race(host):
            value=original(host)
            if host.h['id']=='worker':self.f.active['worker']=True
            return value
        with patch.object(hostmod.Host,'state_files',race):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(self.f.effects,[])
    def test_stopped_forward_worker_refuses_changed_predecessor_binary(self):
        h=self.f.stopped_worker();p=Path(h['release_root'])/h['old_artifact']['package_sha256']/'borg-runtime';p.chmod(0o600);p.write_bytes(b'changed')
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'stopped predecessor binary changed');self.assertEqual(self.f.effects,[])
    def test_stopped_forward_worker_lost_stop_receipt_never_replays_phase(self):
        self.f.stopped_worker();self.f.lost_phase=['worker','stop']
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'controlled lost host ACK after original effect');effects=copy.deepcopy(self.f.effects)
        worker=self.f.profile['hosts'][1];self.assertEqual(len(list(Path(worker['state_root']).glob('action-*/stop.result.json'))),1)
        self.assertFalse(any(row[0]=='worker' for row in effects))
        self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(effects,self.f.effects)

    def test_metadata_unknown_fields_and_missing_bounds_refuse(self):
        v=copy.deepcopy(self.f.profile);v['fallback']=True
        with self.assertRaises(ValueError):common.validate(v)
        v=copy.deepcopy(self.f.profile);del v['limits']['cleanup_timeout_ms']
        with self.assertRaises(ValueError):common.validate(v)
    def test_profile_changed_after_wait_refuses(self):
        r=self.runner();r.work.mkdir();r.end=common.now_ms()+60000;r.original_start();self.f.profile_path.write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError,'profile changed'):r.before_effect()
    def test_exact_unit_change_only(self):
        h=self.f.profile['hosts'][0];new=Path(h['new_unit']['path']);new.write_bytes(new.read_bytes()+b'Environment=NEW_GRANT=yes\n');h['new_unit']['sha256']=common.digest(new.read_bytes());save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        r=self.runner().execute('apply');self.assertEqual(r['status'],'UNKNOWN');self.assert_error_message(r,'unit change exceeds exact executable path');self.assertFalse(any(x[1]=='daemon-reload' for x in self.f.effects))
    def test_retained_worker_policy_drift_refuses(self):
        h=self.f.profile['hosts'][1];save(Path(h['state']['root'])/'limits.json',{'other':'policy'})
        r=self.runner().execute('apply');self.assertEqual(r['status'],'UNKNOWN');self.assert_error_message(r,'worker retained policy differs');self.assertEqual(self.f.effects,[])
    def test_original_worker_history_tamper_prevents_applied(self):
        h=self.f.profile['hosts'][1];path=Path(h['state']['root'])/'history.json';path.write_bytes(b'original')
        self.f.lost_phase=['worker','start'];self.runner().execute('apply');path.write_bytes(b'tampered')
        result=self.runner().execute('reconcile');self.assertEqual(result['status'],'UNKNOWN');self.assertEqual(error_cause(result)['code'],'original_worker_history_changed')
    def test_expiry_during_systemctl_pin_never_launches(self):
        h=self.f.profile['hosts'][0];r={'action_id':'expired','subject':self.f.subject,'deadline_ms':3000,'dispatch_until_ms':2000}
        # The actual command method, not the controlled service override.
        original=hostmod.Host.__mro__[1];host=original(self.f.profile,h,self.f.subject,r);clock=[1000]
        def pinned(*args):clock[0]=2001;return b''
        with patch.object(hostmod.c,'pin',side_effect=pinned),patch.object(hostmod.c,'now_ms',side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(ValueError,'deadline before command'):host.command(['stop',h['service']])
        self.assertEqual(self.f.effects,[])
    def test_expiry_during_last_unit_read_prevents_replace(self):
        h=self.f.profile['hosts'][0];unit=Path(h['unit']['path']);original=unit.read_bytes();read=hostmod.c.read;clock=hostmod.c.now_ms;expired=[False]
        def delayed(path,*args,**kwargs):
            result=read(path,*args,**kwargs)
            if Path(path)==unit and list(unit.parent.glob(h['service']+'.rollout-*')):expired[0]=True
            return result
        with patch.object(hostmod.c,'read',side_effect=delayed),patch.object(hostmod.c,'now_ms',side_effect=lambda:clock()+1000000 if expired[0] else clock()):
            result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'dispatch authorization expired before effect');self.assertEqual(unit.read_bytes(),original)
        self.assertFalse(any(row[1]=='daemon-reload' for row in self.f.effects))
    def test_exact_receipt_bound_roundtrip_and_refusal(self):
        path=self.f.root/'exact.json';value={'retained':'same'};cap=len(common.canonical(value))
        common.save(path,value,cap);self.assertEqual(common.strict(common.read(path,cap)),value)
        other=self.f.root/'too-small.json'
        with self.assertRaisesRegex(ValueError,'receipt byte bound'):common.save(other,value,cap-1)
        self.assertFalse(other.exists())
    def test_symlink_and_hardlink_inputs_refused(self):
        p=self.f.root/'regular';p.write_bytes(b'x');p.chmod(0o600);alias=self.f.root/'alias';alias.symlink_to(p)
        with self.assertRaises((ValueError,OSError)):common.read(alias,20)
        link=self.f.root/'hard';os.link(p,link)
        with self.assertRaises(ValueError):common.read(p,20)
    def test_wal_schema_copy_observes_wal_and_preserves_original_bytes(self):
        h=self.f.profile['hosts'][0];db=Path(h['state']['database']);con=sqlite3.connect(db);con.execute('PRAGMA journal_mode=WAL');con.execute('PRAGMA wal_autocheckpoint=0');con.execute('PRAGMA user_version=13');con.commit()
        self.addCleanup(con.close)
        # Deliberately non-quiesced writer only creates this pure input fixture;
        # the production caller must hold owner lock with the service stopped.
        self.assertEqual(int.from_bytes(db.read_bytes()[60:64],'big'),12)
        r={'action_id':'wal','subject':self.f.subject,'deadline_ms':common.now_ms()+60000};host=hostmod.Host(self.f.profile,h,self.f.subject,r);host.work.mkdir();before=host.state_files()
        with self.assertRaisesRegex(ValueError,'stopped schema differs'):host.sqlite_snapshot('capture')
        self.assertEqual(before,host.state_files())

    def mixed_worker_baseline(self):
        # Controlled service observations, real per-host unit/package/state bytes.
        # The protected controller subject remains unchanged.
        worker=self.f.profile['hosts'][1]
        previous=worker['old_artifact']['package_sha256']
        worker['old_artifact']=dict(worker['old_artifact'],package_sha256='7'*64)
        unit=Path(worker['unit']['path'])
        unit.write_bytes(unit.read_bytes().replace(previous.encode(),b'7'*64))
        worker['unit']['sha256']=common.digest(unit.read_bytes())
        save(self.f.profile_path,self.f.profile)
        self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        return worker

    def test_mixed_same_format_keeps_controller_subject_and_each_host_history(self):
        worker=self.mixed_worker_baseline()
        subject=copy.deepcopy(self.f.subject)
        original=Path(worker['state']['root'])/'retained-original.json'
        original.write_bytes(b'{"retained_original":"unchanged"}')
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(self.f.subject,subject)
        self.assertEqual(original.read_bytes(),b'{"retained_original":"unchanged"}')
        self.assertEqual(self.f.effects,[['worker','stop','worker.service'],
            ['controller','stop','controller.service'],['controller','daemon-reload'],
            ['controller','start','controller.service'],
            ['controller','stop','controller.service'],['controller','start','controller.service'],
            ['worker','daemon-reload'],
            ['worker','start','worker.service']])
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED')
        self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_mixed_same_format_refuses_wrong_controller_baseline_before_start(self):
        self.mixed_worker_baseline()
        self.f.profile['hosts'][0]['old_artifact']['package_sha256']='8'*64
        save(self.f.profile_path,self.f.profile)
        self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN')
        self.assert_error_message(result,'expected installed package differs across target')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_mixed_worker_helper_refuses_subject_not_bound_to_controller(self):
        worker=self.mixed_worker_baseline()
        subject=copy.deepcopy(self.f.subject)
        subject['installed_package_sha256']=worker['old_artifact']['package_sha256']
        with self.assertRaisesRegex(ValueError,'host controller baseline differs'):
            hostmod.Host(self.f.profile,worker,subject,{'action_id':'wrong-controller'})
        self.assertEqual(self.f.effects,[])

    def test_mixed_baseline_does_not_relax_forward_before_package(self):
        self.f.forward_transition()
        worker=self.mixed_worker_baseline()
        with self.assertRaisesRegex(ValueError,'host installed baseline differs'):
            hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'wrong-forward'})
        result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN')
        self.assert_error_message(result,'expected installed package differs across target')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_mixed_worker_real_running_check_retains_exact_artifact_unit_and_uid(self):
        worker=self.mixed_worker_baseline()
        old=worker['old_artifact'];release=Path(worker['release_root'])/old['package_sha256']
        release.mkdir(mode=0o700)
        manifest=common.canonical({'controlled_original':'worker'})
        (release/'manifest.json').write_bytes(manifest)
        (release/'manifest.json').chmod(0o600)
        old['manifest_sha256']=common.digest(manifest)
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'exact-worker'})
        # Exercise the production checker with only procfs/service observations
        # controlled; retain the actual unit and manifest byte/hash reads.
        check=hostmod.Host.__mro__[1].check_running
        read=hostmod.c.read
        def proc_read(path,*args,**kwargs):
            if str(path)=='/proc/42/status':return ('Uid:\t'+(' '.join([str(os.geteuid())]*4))+'\n').encode()
            return read(path,*args,**kwargs)
        valid=(old['binary_sha256'],str(release/'borg-runtime'))
        with patch.object(hostmod.c,'read',side_effect=proc_read),patch.object(host,'pid_binary',return_value=valid) as image:
            self.assertEqual(check(host,False)['MainPID'],'42')
            for invalid in [('9'*64,valid[1]),(valid[0],str(Path(worker['release_root'])/self.f.subject['installed_package_sha256']/'borg-runtime'))]:
                image.return_value=invalid
                with self.assertRaisesRegex(ValueError,'running package/binary differs'):check(host,False)
            image.return_value=valid
            (release/'manifest.json').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'installed manifest differs'):check(host,False)
            (release/'manifest.json').write_bytes(manifest)
            unit=Path(worker['unit']['path']);unit.write_bytes(unit.read_bytes()+b'# changed\n')
            with self.assertRaisesRegex(ValueError,'current unit differs'):check(host,False)
        self.assertEqual(self.f.effects,[])

    def test_mixed_profile_change_after_start_refuses_before_next_effect(self):
        self.mixed_worker_baseline()
        runner=self.runner();runner.work.mkdir();runner.end=common.now_ms()+60000
        runner.original_start()
        changed=copy.deepcopy(self.f.profile)
        changed['hosts'][1]['old_artifact']['package_sha256']='8'*64
        save(self.f.profile_path,changed)
        with self.assertRaisesRegex(ValueError,'rollout profile changed'):runner.before_effect()
        self.assertEqual(self.f.effects,[])

    def test_supplied_turn_forward_preserves_all_schema21_rows_and_owned_journals(self):
        self.f.supplied_turn_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        root=Path(worker['state']['root']);kept={}
        for name in ('pull-requests/pr.json','codex/61/supplied-charter/outer-0/intent.json','charter-output/original.json'):
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'{"original":"retained"}')
            kept[path]=path.read_bytes()
        subject=copy.deepcopy(self.f.subject)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller=self.f.profile['hosts'][0];work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual((before['schema'],after['schema']),(21,22))
        self.assertEqual(after['tables'],before['tables'])
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':17})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        self.assertEqual(self.f.subject,subject)
        self.assertEqual({p:p.read_bytes() for p in kept},kept)
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED')
        self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_supplied_turn_forward_requires_exact_before_hosts_and_version_source_inventory(self):
        self.f.supplied_turn_transition();common.validate(self.f.profile)
        variants=[]
        for side,key,value in [('before','wire',19),('before','controller_schema',20),('before','worker_format',15),
                               ('after','wire',20),('after','controller_schema',21),('after','worker_format',16)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value;variants.append(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker'];variants.append(p)
        for key,value in [('formats',dict(common.SUPPLIED_TURN_BEFORE,worker_format=15)),('artifact',{}),('unit',{})]:
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['before_hosts']['worker'][key]=value;variants.append(p)
        for name in common.SUPPLIED_TURN_BEFORE_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker']['source_files'][name];variants.append(p)
        for name in common.SUPPLIED_TURN_SOURCES-common.INSTRUCTION_SOURCE_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name];variants.append(p)
        for p in variants:
            with self.subTest(formats=p['formats']),self.assertRaises(ValueError):common.validate(p)
        self.assertEqual(self.f.effects,[])

    def test_supplied_turn_forward_installed_manifest_joins_all_host_source_fields(self):
        self.f.supplied_turn_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        old=worker['old_artifact'];path=Path(worker['release_root'])/old['package_sha256']/'manifest.json'
        raw=path.read_bytes();common.verify_before_manifest(self.f.profile,worker,raw)
        for key in ('commit','tree','object_format'):
            m=common.strict(raw);m['source'][key]='foreign'
            p=copy.deepcopy(self.f.profile);h=p['hosts'][1];changed=common.canonical(m)
            h['old_artifact']['manifest_sha256']=common.digest(changed)
            p['formats']['forward']['before_hosts']['worker']['artifact']=copy.deepcopy(h['old_artifact'])
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'artifact/source identity'):
                common.verify_before_manifest(p,h,changed)
        for name in common.SUPPLIED_TURN_BEFORE_SOURCES:
            m=common.strict(raw);m['source']['files'][name]['sha256']='f'*64
            p=copy.deepcopy(self.f.profile);h=p['hosts'][1];changed=common.canonical(m)
            h['old_artifact']['manifest_sha256']=common.digest(changed)
            p['formats']['forward']['before_hosts']['worker']['artifact']=copy.deepcopy(h['old_artifact'])
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'baseline source binding'):
                common.verify_before_manifest(p,h,changed)
        self.assertEqual(self.f.effects,[])

    def test_supplied_turn_forward_real_running_check_uses_predecessor_manifest_binding(self):
        self.f.supplied_turn_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        old=worker['old_artifact'];release=Path(worker['release_root'])/old['package_sha256']
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'supplied-running'})
        check=hostmod.Host.__mro__[1].check_running;read=hostmod.c.read
        def proc_read(path,*args,**kwargs):
            if str(path)=='/proc/42/status':return ('Uid:\t'+(' '.join([str(os.geteuid())]*4))+'\n').encode()
            return read(path,*args,**kwargs)
        with patch.object(hostmod.c,'read',side_effect=proc_read),patch.object(host,'pid_binary',return_value=(old['binary_sha256'],str(release/'borg-runtime'))):
            self.assertEqual(check(host,False)['MainPID'],'42')
            proof=self.f.profile['formats']['forward']['before_hosts']['worker']
            proof['source_files']['src/protocol.rs']='f'*64
            with self.assertRaisesRegex(ValueError,'baseline source binding'):check(host,False)
        self.assertEqual(self.f.effects,[])

    def test_supplied_turn_forward_wrong_controller_or_unbound_mixed_worker_refuses_before_start(self):
        self.f.supplied_turn_transition();self.mixed_worker_baseline()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        self.assert_error_message(result,'supplied forward exact host binding')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
        self.f.bind_supplied_predecessors()
        subject=copy.deepcopy(self.f.subject);subject['installed_package_sha256']='9'*64
        with self.assertRaisesRegex(ValueError,'host controller baseline differs'):
            hostmod.Host(self.f.profile,self.f.profile['hosts'][1],subject,{'action_id':'wrong-controller'})

    def test_supplied_turn_forward_reads_wire20_then21_without_fallback(self):
        self.f.supplied_turn_transition();original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'supplied-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[20,21]);self.assertEqual(self.f.effects,[])

    def test_supplied_turn_forward_refuses_any_retained_table_row_or_shape_change(self):
        self.f.supplied_turn_transition();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes());after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        changed=[]
        p=copy.deepcopy(after);p['tables']['receipts']['rows'][0][2]='rewritten';changed.append(p)
        p=copy.deepcopy(after);p['tables']['charter_requests']['rows']=[['fabricated']];changed.append(p)
        p=copy.deepcopy(after);del p['tables']['charter_output_chunks'];changed.append(p)
        p=copy.deepcopy(after);p['tables']['new_table']={'columns':[],'rows':[]};changed.append(p)
        for p in changed:
            with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(self.f.profile,before,p)

    def test_supplied_turn_forward_crash_after_commit_retains22_without_effect_retry(self):
        self.f.supplied_turn_transition();startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled supplied-turn migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database']);self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],22);con.close()
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))

    def test_supplied_turn_forward_lost_ack_reconciles_original_marker17_completion(self):
        self.f.supplied_turn_transition();self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(self.f.profile['hosts'][1]['state']['format_file']).read_bytes()),{'format_revision':17})

    def test_supplied_turn_forward_rejects_unselected_marker15_without_polling(self):
        self.f.supplied_turn_transition();startup=self.f.on_start
        def wrong(host):
            startup(host)
            if host['role']=='worker':Path(host['state']['format_file']).write_bytes(common.canonical({'format_revision':15}))
        self.f.on_start=wrong
        with patch.object(hostmod.time,'sleep',side_effect=AssertionError('unselected marker cannot poll')):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'UNKNOWN');self.assert_error_message(result,'worker format differs from exact phase')

    def test_supplied_turn_forward_waits_for_original16_to17_admission_once(self):
        self.f.supplied_turn_transition();startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)

    def test_supplied_turn_stopped16_predecessor_requires_exact_manifest_and_resumes_once(self):
        self.f.supplied_turn_transition();h=self.mixed_worker_baseline();self.f.active[h['id']]=False
        old=h['old_artifact'];binary=b'controlled exact stopped worker16 image'
        old['binary_sha256']=common.digest(binary);old['binary_bytes']=len(binary)
        self.f.bind_supplied_predecessors()
        release=Path(h['release_root'])/old['package_sha256'];common.write_new(release/'borg-runtime',binary,0o500)
        request={'action_id':'stopped16-proof','deadline_ms':common.now_ms()+60000}
        host=hostmod.Host(self.f.profile,h,self.f.subject,request)
        m=Path(h['owner_lock']).lstat()
        h['stopped_predecessor']={'service':host.service(),
            'owner_identity':[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)],
            'state_sha256':common.digest(common.canonical(host.state_files()))}
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        manifest=release/'manifest.json';raw=manifest.read_bytes()
        with host.owner():
            self.assertEqual(host.check_stopped_predecessor()['MainPID'],'0')
            manifest.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'stopped predecessor manifest changed'):host.check_stopped_predecessor()
            manifest.write_bytes(raw)
        self.assertEqual(self.f.effects,[])
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(sum(row[:2]==['worker','stop'] for row in self.f.effects),0)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
        self.assertEqual(common.strict(Path(h['state']['format_file']).read_bytes()),{'format_revision':17})

    def test_agent_directory_forward_preserves_schema21_rows_and_owned_journals(self):
        self.f.agent_directory_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        formats=self.f.profile['formats']
        self.assertEqual(formats['forward']['kind'],common.AGENT_DIRECTORY_KIND)
        self.assertEqual(formats['before'],{'wire':20,'controller_schema':21,'worker_format':16})
        self.assertEqual(formats['after'],{'wire':22,'controller_schema':23,'worker_format':17})
        self.assertEqual(formats['after'],common.AGENT_DIRECTORY_AFTER)
        self.assertNotEqual(worker['old_artifact']['package_sha256'],self.f.subject['installed_package_sha256'])
        root=Path(worker['state']['root']);kept={}
        for name in ('pull-requests/pr.json','codex/61/supplied-charter/outer-0/intent.json','charter-output/original.json'):
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'{"original":"retained"}')
            kept[path]=path.read_bytes()
        subject=copy.deepcopy(self.f.subject)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        controller=self.f.profile['hosts'][0];work=next(Path(controller['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual((before['schema'],after['schema']),(21,23))
        self.assertNotIn('agent_retirements',before['tables'])
        self.assertEqual(after['tables'],dict(before['tables'],agent_retirements={
            'columns':common.AGENT_RETIREMENT_COLUMNS.split(),'rows':[]}))
        con=sqlite3.connect(controller['state']['database'])
        try:
            self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],23)
            self.assertEqual([row[1] for row in con.execute('PRAGMA table_info(agent_retirements)')],common.AGENT_RETIREMENT_COLUMNS.split())
            self.assertEqual(con.execute('SELECT * FROM agent_retirements').fetchall(),[])
        finally:con.close()
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':17})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        self.assertEqual(self.f.subject,subject);self.assertEqual({p:p.read_bytes() for p in kept},kept)
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_agent_directory_forward_requires_exact_before_hosts_and_new_producer(self):
        self.f.agent_directory_transition();self.mixed_worker_baseline()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        self.assert_error_message(result,'supplied forward exact host binding')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
        self.f.bind_supplied_predecessors();common.validate(self.f.profile)
        variants=[]
        for side,key,value in [('before','wire',21),('before','controller_schema',22),('before','worker_format',17),
                               ('after','wire',21),('after','controller_schema',22),('after','worker_format',16)]:
            p=copy.deepcopy(self.f.profile);p['formats'][side][key]=value;variants.append(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker'];variants.append(p)
        for key,value in [('formats',dict(common.AGENT_DIRECTORY_BEFORE,worker_format=15)),('artifact',{}),('unit',{})]:
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['before_hosts']['worker'][key]=value;variants.append(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker']['source_files']['src/protocol.rs'];variants.append(p)
        new_sources=common.AGENT_DIRECTORY_SOURCES-common.SUPPLIED_TURN_SOURCES
        self.assertTrue(new_sources)
        for name in new_sources:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name];variants.append(p)
        for p in variants:
            with self.subTest(formats=p['formats']),self.assertRaises(ValueError):common.validate(p)
        self.assertEqual(self.f.effects,[])

    def test_agent_directory_forward_refuses_wrong_nonempty_or_additional_tables_and_rewrites(self):
        self.f.agent_directory_transition();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        common.controller_retained(self.f.profile,before,after)
        changed=[]
        p=copy.deepcopy(after);p['tables']['agent_retirements']['columns']=['wrong'];changed.append(p)
        p=copy.deepcopy(after);p['tables']['agent_retirements']['rows']=[['fabricated']*len(common.AGENT_RETIREMENT_COLUMNS.split())];changed.append(p)
        p=copy.deepcopy(after);del p['tables']['agent_retirements'];changed.append(p)
        p=copy.deepcopy(after);p['tables']['new_table']={'columns':[],'rows':[]};changed.append(p)
        p=copy.deepcopy(after);p['tables']['receipts']['rows'][0][2]='rewritten';changed.append(p)
        p=copy.deepcopy(after);del p['tables']['charter_output_chunks'];changed.append(p)
        for index,p in enumerate(changed):
            with self.subTest(delta=index),self.assertRaisesRegex(ValueError,'logical controller state'):
                common.controller_retained(self.f.profile,before,p)

    def test_agent_directory_forward_crash_after_commit_retains23_without_effect_retry(self):
        self.f.agent_directory_transition();startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled agent-directory migration committed before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database'])
        try:
            self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],23)
            self.assertEqual([row[1] for row in con.execute('PRAGMA table_info(agent_retirements)')],common.AGENT_RETIREMENT_COLUMNS.split())
            self.assertEqual(con.execute('SELECT * FROM agent_retirements').fetchall(),[])
        finally:con.close()
        self.assertEqual(sum(row[:2]==['controller','start'] for row in effects),1)
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))

    def test_agent_directory_forward_reads_wire20_then22_without_fallback(self):
        self.f.agent_directory_transition();original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'agent-directory-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[20,22]);self.assertEqual(self.f.effects,[])

    def test_agent_directory_forward_lost_ack_reconciles_original_marker17_completion(self):
        self.f.agent_directory_transition();self.f.lost_phase=['worker','start']
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN')
        effects=copy.deepcopy(self.f.effects);worker=self.f.profile['hosts'][1]
        work=next(Path(worker['state_root']).glob('action-*'))
        retained={name:(work/name).read_bytes() for name in ('start.intent.json','start.result.json')}
        result=self.runner().execute('reconcile');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(self.f.effects,effects)
        self.assertEqual({name:(work/name).read_bytes() for name in retained},retained)
        proof=self.full_host_proof(result,'worker')['phase_proofs']['start']
        self.assertEqual(proof,{'request':common.strict(retained['start.intent.json']),'result':common.strict(retained['start.result.json'])})
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':17})

    def test_composed_forward_preserves_schema24_rows_and_original_worker_journals(self):
        self.f.composed_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        formats=self.f.profile['formats']
        self.assertEqual(formats['before'],{'wire':23,'controller_schema':24,'worker_format':18})
        self.assertEqual(formats['after'],{'wire':24,'controller_schema':25,'worker_format':19})
        self.assertEqual(formats['forward']['kind'],common.COMPOSED_KIND)
        self.assertNotEqual(worker['old_artifact']['package_sha256'],self.f.subject['installed_package_sha256'])
        root=Path(worker['state']['root']);kept={}
        for name in ('pull-requests/original.json','codex/61/completion/input.json','codex/61/supplied-charter/outer-0/intent.json'):
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'{"original":"retained"}')
            kept[path]=path.read_bytes()
        subject=copy.deepcopy(self.f.subject)
        result=self.runner().execute('apply');self.assertEqual(result['status'],'APPLIED',result)
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        self.assertEqual((before['schema'],after['schema']),(24,25))
        self.assertEqual(after['tables'],before['tables'])
        self.assertEqual(before['tables']['session_inputs']['rows'][0][2],'retained accepted input bytes')
        self.assertEqual(before['tables']['task_assignments']['rows'][0][-1],'retained pending handoff bytes')
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':19})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        self.assertEqual(self.f.subject,subject);self.assertEqual({p:p.read_bytes() for p in kept},kept)
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_composed_forward_requires_exact_tuple_each_host_and_complete_source_inventory(self):
        self.f.composed_transition();self.mixed_worker_baseline()
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        self.assert_error_message(result,'supplied forward exact host binding')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))
        self.f.bind_supplied_predecessors();common.validate(self.f.profile)
        variants=[]
        for side in ('before','after'):
            for key in ('wire','controller_schema','worker_format'):
                for delta in (-1,1):
                    p=copy.deepcopy(self.f.profile);p['formats'][side][key]+=delta;variants.append(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward'];variants.append(p)
        p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']=common.AGENT_DIRECTORY_KIND;variants.append(p)
        p=copy.deepcopy(self.f.profile);p['formats']['forward']['kind']='unreviewed';variants.append(p)
        p=copy.deepcopy(self.f.profile);p['formats']['forward']['after_package_sha256']=p['formats']['forward']['before_package_sha256'];variants.append(p)
        p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker'];variants.append(p)
        for key,value in [('formats',dict(common.SUPPLIED_TURN_BEFORE)),('artifact',{}),('unit',{})]:
            p=copy.deepcopy(self.f.profile);p['formats']['forward']['before_hosts']['worker'][key]=value;variants.append(p)
        for name in common.SUPPLIED_TURN_BEFORE_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['forward']['before_hosts']['worker']['source_files'][name];variants.append(p)
        for name in common.COMPOSED_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name];variants.append(p)
        p=copy.deepcopy(self.f.profile);p['formats']['source_files']['foreign.rs']='f'*64;variants.append(p)
        for index,p in enumerate(variants):
            with self.subTest(variant=index),self.assertRaises(ValueError):common.validate(p)
        self.assertEqual(self.f.effects,[])

    def test_composed_forward_verifies_each_installed_manifest_and_selected_package_sources(self):
        self.f.composed_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        release=Path(worker['release_root'])/worker['old_artifact']['package_sha256']
        raw=(release/'manifest.json').read_bytes();common.verify_before_manifest(self.f.profile,worker,raw)
        for name in common.SUPPLIED_TURN_BEFORE_SOURCES:
            p=copy.deepcopy(self.f.profile);h=p['hosts'][1]
            p['formats']['forward']['before_hosts']['worker']['source_files'][name]='f'*64
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'baseline source binding'):
                common.verify_before_manifest(p,h,raw)
        p=copy.deepcopy(self.f.profile);p['formats']['source_files']['src/worker/codex_retry.rs']='f'*64
        save(self.f.profile_path,p);self.f.profile=p;self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_composed_forward_refuses_all_retained_table_row_shape_and_schema_changes(self):
        self.f.composed_transition();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
        work=next(Path(self.f.profile['hosts'][0]['state_root']).glob('action-*'))
        before=common.strict((work/'stopped-copy/snapshot.json').read_bytes())
        after=common.strict((work/'restart-copy/snapshot.json').read_bytes())
        for table in ('receipts','sessions','session_inputs','task_assignments'):
            p=copy.deepcopy(after);p['tables'][table]['rows'][0][-1]='rewritten'
            with self.subTest(table=table),self.assertRaisesRegex(ValueError,'logical controller state'):
                common.controller_retained(self.f.profile,before,p)
        variants=[]
        p=copy.deepcopy(after);del p['tables']['agent_retirements'];variants.append(p)
        p=copy.deepcopy(after);p['tables']['agent_retirements']['columns']=['wrong'];variants.append(p)
        p=copy.deepcopy(after);p['tables']['new_table']={'columns':[],'rows':[]};variants.append(p)
        for p in variants:
            with self.assertRaisesRegex(ValueError,'logical controller state'):common.controller_retained(self.f.profile,before,p)
        p=copy.deepcopy(after);p['schema']=24
        with self.assertRaisesRegex(ValueError,'schema differs'):common.controller_retained(self.f.profile,before,p)

    def test_composed_forward_crash_after_marker_commit_never_restarts_or_rolls_back(self):
        self.f.composed_transition();startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled schema25 commit before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database'])
        try:self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],25)
        finally:con.close()
        self.assertEqual(sum(row[:2]==['controller','start'] for row in effects),1)
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))

    def test_composed_forward_reads_only_wire23_then24(self):
        self.f.composed_transition();original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'composed-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[23,24]);self.assertEqual(self.f.effects,[])

    def test_composed_forward_lost_worker_ack_uses_original_marker19_completion(self):
        self.f.composed_transition();self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        worker=self.f.profile['hosts'][1];work=next(Path(worker['state_root']).glob('action-*'))
        retained={name:(work/name).read_bytes() for name in ('start.intent.json','start.result.json')}
        result=self.runner().execute('reconcile');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(self.f.effects,effects);self.assertEqual({name:(work/name).read_bytes() for name in retained},retained)
        self.assertEqual(self.full_host_proof(result,'worker')['phase_proofs']['start'],{'request':common.strict(retained['start.intent.json']),'result':common.strict(retained['start.result.json'])})
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':19})

    def test_composed_forward_waits_only_for_original18_and_rejects_other_markers(self):
        self.f.composed_transition();startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'wrong-marker','deadline_ms':common.now_ms()+60000});host.after=True
        for marker in (17,20):
            save(Path(worker['state']['format_file']),{'format_revision':marker})
            with self.subTest(marker=marker),self.assertRaisesRegex(ValueError,'worker format differs from exact phase'):
                host.compatibility(pending_forward=True)

class ProcessCleanupTests(unittest.TestCase):
    def test_leader_exit_still_kills_owned_group_and_closes_streams(self):
        # No process is spawned. Model a reaped leader with surviving group.
        p=MagicMock();p.pid=43210;p.returncode=0
        for f in (p.stdin,p.stdout,p.stderr):f.closed=False
        sel=MagicMock();sel.get_map.return_value={}
        calls=[];reaped=[False]
        p.wait.side_effect=lambda **kw:reaped.__setitem__(0,True)
        def kill(pid,sig):
            if sig: self.assertFalse(reaped[0],'signal after reap')
            calls.append((pid,sig))
            if sig==0:raise ProcessLookupError()
        limits={'command_timeout_ms':1000,'cleanup_timeout_ms':1000,'cleanup_poll_ms':1,'io_chunk_bytes':100,'command_output_bytes':100}
        with patch.object(common.subprocess,'Popen',return_value=p),patch.object(common.selectors,'DefaultSelector',return_value=sel),patch.object(common.os,'set_blocking'),patch.object(common.os,'killpg',side_effect=kill),patch.object(common.os,'waitid',return_value=type('Status',(),{'si_code':os.CLD_EXITED,'si_status':0})()):
            self.assertEqual(common.run(['fixture'],limits,common.now_ms()+1000),b'')
        self.assertIn((43210,signal.SIGKILL),calls)
        for f in (p.stdin,p.stdout,p.stderr):self.assertTrue(f.close.called)

class DeadlineTests(unittest.TestCase):
    def test_private_trickling_frame_uses_original_absolute_deadline(self):
        peer=MagicMock();peer.__enter__.return_value=peer
        peer.getsockopt.return_value=struct.pack('3i',1,os.geteuid(),1)
        clock=[0.0];wire=bytearray(struct.pack('!I',12)+b'{}'+b' '*10)
        def recv(n):clock[0]+=0.4;return bytes([wire.pop(0)])
        peer.recv.side_effect=recv
        with patch.object(producer.socket,'socket',return_value=peer),patch.object(producer.time,'monotonic',side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(TimeoutError,'absolute deadline'):
                producer.exchange({'socket':'/not-opened','server_uid':os.geteuid()},{'request':'fixture'},max_bytes=1000,timeout_ms=1000)
        self.assertEqual(peer.recv.call_count,3);self.assertTrue(peer.__exit__.called)
    def test_http_trickling_headers_do_not_reset_deadline(self):
        peer=MagicMock();peer.__enter__.return_value=peer;clock=[0.0]
        def recv(n):clock[0]+=0.4;return b'H'
        peer.recv.side_effect=recv
        limits={'transport_timeout_ms':1000,'transport_bytes':1000,'io_chunk_bytes':100}
        with patch.object(common.socket,'create_connection',return_value=peer),patch.object(common.time,'monotonic',side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(TimeoutError,'absolute deadline'):common.http_read('127.0.0.1',1,b'{}',limits,common.now_ms()+1000)
        self.assertEqual(peer.recv.call_count,3);self.assertTrue(peer.__exit__.called)
    def test_http_complete_wire_retains_exact_body(self):
        peer=MagicMock();peer.__enter__.return_value=peer;peer.recv.return_value=b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}'
        with patch.object(common.socket,'create_connection',return_value=peer):
            self.assertEqual(common.http_read('127.0.0.1',17,b'{}',{'transport_timeout_ms':1000,'transport_bytes':1000,'io_chunk_bytes':100},common.now_ms()+1000),b'{}')
        self.assertIn(b'Host: 127.0.0.1:17',peer.sendall.call_args.args[0])
    def test_post_spawn_selector_setup_failure_cleans_group(self):
        p=MagicMock();p.pid=43210;p.returncode=0
        for f in (p.stdin,p.stdout,p.stderr):f.closed=False
        sel=MagicMock();sel.register.side_effect=OSError('fixture FD exhaustion')
        calls=[]
        def kill(pid,sig):
            calls.append((pid,sig))
            if sig==0:raise ProcessLookupError()
        limits={'command_timeout_ms':1000,'cleanup_timeout_ms':1000,'cleanup_poll_ms':1,'io_chunk_bytes':100,'command_output_bytes':100}
        with patch.object(common.subprocess,'Popen',return_value=p),patch.object(common.selectors,'DefaultSelector',return_value=sel),patch.object(common.os,'set_blocking'),patch.object(common.os,'killpg',side_effect=kill):
            with self.assertRaisesRegex(OSError,'FD exhaustion'):common.run(['fixture'],limits,common.now_ms()+1000)
        self.assertIn((43210,signal.SIGKILL),calls)
        for f in (p.stdin,p.stdout,p.stderr):self.assertTrue(f.close.called)

if __name__=='__main__':unittest.main()
