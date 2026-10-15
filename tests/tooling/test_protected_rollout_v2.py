"""Pure local scratch tests. No commands, service calls, sockets or provider use."""
import copy, os, signal, sqlite3, struct, tempfile, unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
import rollout_common_v2 as common
import protected_rollout_v2 as adapter
import rollout_host_v2 as hostmod
import protected_release_v2 as producer
from rollout_fixture import Fixture, FakeProducer, save

class RolloutTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='borg-rollout-pure-');self.addCleanup(self.temp.cleanup)
        self.f=Fixture(Path(self.temp.name));self.peer=FakeProducer(self.f.subject)
        self.stack=self.f.seams();self.stack.__enter__();self.addCleanup(self.stack.__exit__,None,None,None)
        self.no_process=patch('subprocess.Popen',side_effect=AssertionError('pure test must not start process'));self.no_process.start();self.addCleanup(self.no_process.stop)
        self.no_socket=patch('socket.socket',side_effect=AssertionError('pure test must not connect'));self.no_socket.start();self.addCleanup(self.no_socket.stop)
    def runner(self):
        r=self.f.rollout();r.producer.exchange=self.peer.exchange;return r
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
        self.assertIn('not committed',b['error']);self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)
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
        result=self.runner().execute('reconcile');self.assertIn('body changed',result['error']);self.assertEqual(self.f.effects,[])
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
        r=self.runner().execute('apply');self.assertEqual(r['status'],'UNKNOWN');self.assertIn('exceeds exact',r['error']);self.assertFalse(any(x[1]=='daemon-reload' for x in self.f.effects))
    def test_retained_worker_policy_drift_refuses(self):
        h=self.f.profile['hosts'][1];save(Path(h['state']['root'])/'limits.json',{'other':'policy'})
        r=self.runner().execute('apply');self.assertEqual(r['status'],'UNKNOWN');self.assertIn('retained policy',r['error']);self.assertEqual(self.f.effects,[])
    def test_original_worker_history_tamper_prevents_applied(self):
        h=self.f.profile['hosts'][1];path=Path(h['state']['root'])/'history.json';path.write_bytes(b'original')
        self.f.lost_phase=['worker','start'];self.runner().execute('apply');path.write_bytes(b'tampered')
        result=self.runner().execute('reconcile');self.assertEqual(result['status'],'UNKNOWN');self.assertIn('history changed',result['error'])
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
        self.assertEqual(result['status'],'UNKNOWN');self.assertIn('expired',result['error']);self.assertEqual(unit.read_bytes(),original)
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
