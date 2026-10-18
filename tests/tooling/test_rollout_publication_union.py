"""Exact publication-union rollout boundary, using existing controlled seams.

No installed role, real runtime, service, network or provider qualification.
Historical recovered25/26/20 fixture bodies and source families stay unchanged.
The package is the existing non-runnable ELF fixture with explicit union source
membership; startup below changes only real scratch SQL/worker markers.
"""
import copy
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import protected_preparation_v2 as prep
import protected_rollout_v2 as adapter
import rollout_common_v2 as common
import rollout_host_v2 as hostmod
import rollout_fixture as fixturemod
import test_protected_rollout_v2 as legacy_rollout
import test_rollout_compact_retention as compact
import test_rollout_current_format as current_format
import test_rollout_charter_journals as journals
import test_rollout_retained_holds as holds
import test_rollout_codex_retained as codex_retained
from test_rollout_reconciliation import NoExternalEffects

save=fixturemod.save


def select_union(host):
    host.p['formats']={'before':dict(common.PUBLICATION_UNION_AFTER),
                       'after':dict(common.PUBLICATION_UNION_AFTER),'source_files':{}}
    host._verified_manifest={'source':{'files':{name:{'sha256':sha}
        for name,sha in hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES.items()}}}


class PublicationUnionRolloutTests(unittest.TestCase):
    # Reuse helpers only: inheriting RolloutTests would silently rerun its suite.
    runner=legacy_rollout.RolloutTests.runner
    full_host_proof=legacy_rollout.RolloutTests.full_host_proof
    assert_error_message=legacy_rollout.RolloutTests.assert_error_message
    mixed_worker_baseline=legacy_rollout.RolloutTests.mixed_worker_baseline

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='borg-publication-union-')
        self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name);package_root=root/'package';package_root.mkdir()
        original=fixturemod.fake_package(package_root)
        _,manifest=fixturemod.artifact(original)
        for name in common.PUBLICATION_UNION_SOURCES:
            manifest['source']['files'].setdefault(name,{'mode':'100644','git_blob':'a'*40,'size':1,'sha256':'2'*64})
        union=package_root/'union.tar'
        fixturemod.package.package_tar(union,package_root/'fake-binary',manifest);union.chmod(0o600)
        self.f=fixturemod.Fixture(root/'fixture',package_path=union)
        self.peer=fixturemod.FakeProducer(self.f.subject)
        self.stack=self.f.seams();self.stack.__enter__()
        self.addCleanup(self.stack.__exit__,None,None,None)
        for name in ('subprocess.Popen','socket.socket'):
            guard=patch(name,side_effect=AssertionError('controlled union fixture has no external effects'))
            guard.start();self.addCleanup(guard.stop)

    def union_transition(self):
        # This helper establishes the real retained25/19 state through the
        # existing fixture. It does NOT execute its historical26/20 startup.
        self.f.recovered_transition()
        f=self.f.profile['formats']
        f['before']=dict(common.PUBLICATION_UNION_BEFORE)
        f['after']=dict(common.PUBLICATION_UNION_AFTER)
        f['forward']['kind']=common.PUBLICATION_UNION_KIND
        f['source_files']={n:self.f.manifest['source']['files'][n]['sha256'] for n in common.PUBLICATION_UNION_SOURCES}
        self.f.bind_supplied_predecessors()
        def startup(host):
            if host['role']=='controller':
                con=sqlite3.connect(host['state']['database'])
                try:
                    schema=con.execute('PRAGMA user_version').fetchone()[0]
                    self.assertIn(schema,(25,28))
                    # Host intentionally restarts the controller after its
                    # stopped restart-copy. Only the original25 open migrates;
                    # the second, already-current28 open performs no rewrite.
                    if schema==25:con.executescript('BEGIN;PRAGMA user_version=28;COMMIT;')
                    self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],28)
                finally:con.close()
            else:
                path=Path(host['state']['format_file']);temporary=path.with_name('union-marker')
                self.assertEqual(common.strict(path.read_bytes()),{'format_revision':19})
                save(temporary,{'format_revision':22});os.replace(temporary,path);common.sync(path.parent)
        self.f.on_start=startup
        save(self.f.profile_path,self.f.profile);self.f.profile_sha=fixturemod.pin(self.f.profile_path)['sha256']
        self.assertTrue(common.publication_union_forward(self.f.profile))

    def test_publication_union_preserves_schema25_rows_and_original_worker_journals(self):
        self.union_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        formats=self.f.profile['formats']
        self.assertEqual(formats['before'],{'wire':24,'controller_schema':25,'worker_format':19})
        self.assertEqual(formats['after'],{'wire':27,'controller_schema':28,'worker_format':22})
        self.assertEqual(formats['forward']['kind'],common.PUBLICATION_UNION_KIND)
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
        self.assertEqual((before['schema'],after['schema']),(25,28))
        self.assertEqual(after['tables'],before['tables'])
        self.assertEqual(before['tables']['session_inputs']['rows'][0][2],'retained accepted input bytes')
        self.assertEqual(before['tables']['task_assignments']['rows'][0][-1],'retained pending handoff bytes')
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':22})
        self.assertEqual(self.full_host_proof(result,'controller')['preserved_controller_receipts'],['original'])
        self.assertEqual(self.f.subject,subject);self.assertEqual({p:p.read_bytes() for p in kept},kept)
        effects=copy.deepcopy(self.f.effects)
        self.assertEqual(self.runner().execute('reconcile')['status'],'APPLIED');self.assertEqual(self.f.effects,effects)
        self.assertEqual(sum(q['command']['operation']=='start_rollout' for q in self.peer.calls),1)

    def test_publication_union_requires_exact_tuple_each_host_and_complete_source_inventory(self):
        self.union_transition();self.mixed_worker_baseline()
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
        for name in common.PUBLICATION_UNION_SOURCES:
            p=copy.deepcopy(self.f.profile);del p['formats']['source_files'][name];variants.append(p)
        p=copy.deepcopy(self.f.profile);p['formats']['source_files']['foreign.rs']='f'*64;variants.append(p)
        for index,p in enumerate(variants):
            with self.subTest(variant=index),self.assertRaises(ValueError):common.validate(p)
        self.assertEqual(self.f.effects,[])

    def test_publication_union_verifies_each_installed_manifest_and_selected_package_sources(self):
        self.union_transition();worker=self.mixed_worker_baseline();self.f.bind_supplied_predecessors()
        release=Path(worker['release_root'])/worker['old_artifact']['package_sha256']
        raw=(release/'manifest.json').read_bytes();common.verify_before_manifest(self.f.profile,worker,raw)
        for name in common.SUPPLIED_TURN_BEFORE_SOURCES:
            p=copy.deepcopy(self.f.profile);h=p['hosts'][1]
            p['formats']['forward']['before_hosts']['worker']['source_files'][name]='f'*64
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'baseline source binding'):
                common.verify_before_manifest(p,h,raw)
        p=copy.deepcopy(self.f.profile);p['formats']['source_files']['src/worker/read_retention.rs']='f'*64
        save(self.f.profile_path,p);self.f.profile=p;self.f.profile_sha=common.digest(self.f.profile_path.read_bytes())
        result=self.runner().execute('apply');self.assertEqual(result['status'],'UNKNOWN')
        self.assertEqual(self.f.effects,[])
        self.assertFalse(any(q['command']['operation']=='start_rollout' for q in self.peer.calls))

    def test_publication_union_refuses_all_retained_table_row_shape_and_schema_changes(self):
        self.union_transition();self.assertEqual(self.runner().execute('apply')['status'],'APPLIED')
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
        p=copy.deepcopy(after);p['schema']=25
        with self.assertRaisesRegex(ValueError,'schema differs'):common.controller_retained(self.f.profile,before,p)

    def test_publication_union_crash_after_marker_commit_never_restarts_or_rolls_back(self):
        self.union_transition();startup=self.f.on_start
        def crash(host):
            startup(host)
            if host['role']=='controller':raise ValueError('controlled schema28 commit before crash')
        self.f.on_start=crash
        first=self.runner().execute('apply');self.assertEqual(first['status'],'UNKNOWN');self.assertFalse(first['automatic_rollback'])
        effects=copy.deepcopy(self.f.effects);self.assertEqual(self.runner().execute('reconcile')['status'],'UNKNOWN');self.assertEqual(self.f.effects,effects)
        con=sqlite3.connect(self.f.profile['hosts'][0]['state']['database'])
        try:self.assertEqual(con.execute('PRAGMA user_version').fetchone()[0],28)
        finally:con.close()
        self.assertEqual(sum(row[:2]==['controller','start'] for row in effects),1)
        self.assertFalse(any(row[:2]==['worker','start'] for row in effects))

    def test_publication_union_reads_only_wire24_then27(self):
        self.union_transition();original=hostmod.Host.__mro__[1]
        h=self.f.profile['hosts'][0];host=original(self.f.profile,h,self.f.subject,{'action_id':'recovered-wire','deadline_ms':common.now_ms()+60000})
        seen=[]
        def reply(address,port,raw,*rest):
            q=common.strict(raw);seen.append(q['protocol_revision'])
            return common.canonical({'request_id':q['request_id'],'status':'ok','observed_controller_revision':1,'commit':None,'data':{'kind':'tasks','value':[]}})
        with patch.object(hostmod.c,'http_read',side_effect=reply):
            host.api('list_tasks',{});host.after=True;host.api('list_tasks',{})
        self.assertEqual(seen,[24,27]);self.assertEqual(self.f.effects,[])

    def test_publication_union_lost_worker_ack_uses_original_marker22_completion(self):
        self.union_transition();self.f.lost_phase=['worker','start']
        self.assertEqual(self.runner().execute('apply')['status'],'UNKNOWN');effects=copy.deepcopy(self.f.effects)
        worker=self.f.profile['hosts'][1];work=next(Path(worker['state_root']).glob('action-*'))
        retained={name:(work/name).read_bytes() for name in ('start.intent.json','start.result.json')}
        result=self.runner().execute('reconcile');self.assertEqual(result['status'],'APPLIED',result)
        self.assertEqual(self.f.effects,effects);self.assertEqual({name:(work/name).read_bytes() for name in retained},retained)
        self.assertEqual(self.full_host_proof(result,'worker')['phase_proofs']['start'],{'request':common.strict(retained['start.intent.json']),'result':common.strict(retained['start.result.json'])})
        self.assertEqual(sum(row[:2]==['worker','start'] for row in effects),1)
        self.assertEqual(common.strict(Path(worker['state']['format_file']).read_bytes()),{'format_revision':22})

    def test_publication_union_waits_only_for_original19_and_rejects_other_markers(self):
        self.union_transition();startup=self.f.on_start;worker=self.f.profile['hosts'][1];waits=[]
        def delayed(host):
            if host['role']=='controller':startup(host)
        def advance(delay):waits.append(delay);startup(worker)
        self.f.on_start=delayed
        with patch.object(hostmod.time,'sleep',side_effect=advance):result=self.runner().execute('apply')
        self.assertEqual(result['status'],'APPLIED',result);self.assertEqual(len(waits),1)
        self.assertEqual(sum(row[:2]==['worker','start'] for row in self.f.effects),1)
        host=hostmod.Host(self.f.profile,worker,self.f.subject,{'action_id':'wrong-marker','deadline_ms':common.now_ms()+60000});host.after=True
        for marker in (18,20,21,23):
            save(Path(worker['state']['format_file']),{'format_revision':marker})
            with self.subTest(marker=marker),self.assertRaisesRegex(ValueError,'worker format differs from exact phase'):
                host.compatibility(pending_forward=True)

class PublicationUnionObservationTests(NoExternalEffects):
    def test_current22_marker_requires_complete_authenticated_family_and_exact_bytes(self):
        f=current_format.CurrentFormatObservationTests()
        f.FORMATS=hostmod._PUBLICATION_UNION_CODEX_HOME_FORMATS
        f.FAMILY=hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES
        f.setUp();self.addCleanup(f.doCleanups)
        self.assertTrue(f.host.codex_home_scope())
        preserved,observed=f.history()
        self.assertEqual(preserved,{})
        proof=observed['format.json'];self.assertEqual(proof['format_revision'],22)
        self.assertTrue(proof['original_bytes_preserved'] and proof['noninode_identity_preserved'])
        self.assertFalse(proof['actual_writer_attributed'] or proof['service_effects'])
        for name in f.FAMILY:
            f.host._verified_manifest=copy.deepcopy(f.manifest)
            del f.host._verified_manifest['source']['files'][name]
            with self.subTest(source=name),self.assertRaises(ValueError):f.history()
        f.host._verified_manifest=copy.deepcopy(f.manifest)
        for side in ('before','after'):
            for value in (19,20,21,23):
                f.host.p=copy.deepcopy(f.profile);f.host.p['formats'][side]['worker_format']=value
                with self.subTest(side=side,marker=value),self.assertRaises(ValueError):f.history()
        f.host.p=copy.deepcopy(f.profile)
        f.new['sha256']='0'*64
        with self.assertRaises(ValueError):f.history()
        f.new['sha256']=f.old['sha256'];f.new['identity'][2]+=1
        with self.assertRaises(ValueError):f.history()

    def test_current22_charter_manifest_and_consumed_journal_keep_original_bytes(self):
        for kind in ('reply','supplied_reply','compaction_reply','output_chunk'):
            with self.subTest(kind=kind),journals.fixture('supplied_reply' if kind=='output_chunk' else kind) as f:
                select_union(f.host)
                name,value=journals.original_journal(f,kind)
                before=journals.baseline(f,name,value)
                # Hold both original inodes so replacement cannot reuse them.
                fds=[os.open(f.root/n,os.O_RDONLY|os.O_NOFOLLOW) for n in (name,f.name)]
                try:
                    journals.replace_journal(f,name)
                    f.replace_manifest()
                    preserved,observed=f.history()
                    self.assertEqual(set(observed),{name,f.name})
                    self.assertEqual(preserved,{n:v for n,v in f.before.items() if n not in (name,f.name)})
                    proof=f.host._charter_manifests.proof()
                    self.assertEqual(proof['source_files'],hostmod._PUBLICATION_UNION_CHARTER_MANIFEST_SOURCES)
                    self.assertEqual(proof['journal_source_files'],hostmod._PUBLICATION_UNION_CHARTER_JOURNAL_SOURCES)
                    row=proof['journal_observations'][common.digest(name.encode())]
                    self.assertEqual(row['original_request_sha256'],common.digest(common.canonical(value['request'])))
                    self.assertEqual(row['original_context_sha256'],common.digest(common.canonical(value['outcome_context'])))
                    self.assertTrue(row['original_content_exact']);self.assertFalse(row['new_operation_authority'])
                    self.assertEqual(f.stop,before);f.host.api.assert_not_called()
                    bad=copy.deepcopy(value);bad['consumed']=False
                    journals.replace_journal(f,name,bad)
                    del f.host._charter_manifests  # new inventory, no reuse of prior observation cache
                    with self.assertRaises(ValueError):f.history()
                finally:
                    for fd in fds:os.close(fd)
        for name in hostmod._PUBLICATION_UNION_CHARTER_JOURNAL_SOURCES:
            with self.subTest(missing=name),journals.fixture('supplied_reply') as f:
                select_union(f.host);path,value=journals.original_journal(f,'supplied_reply')
                journals.baseline(f,path,value);journals.replace_journal(f,path)
                del f.host._verified_manifest['source']['files'][name]
                with self.assertRaises(ValueError):f.history()
                f.host.api.assert_not_called()

    def test_current22_hold_clocks_do_not_start_cleanup_or_change_material(self):
        for reason in ('uncertain_effect','canonical_binding','local_recovery'):
            with self.subTest(reason=reason),fixturemod.cleanup_fixture() as f:
                f.record=holds.hold(f.record,reason);f.write(f.name,f.record);f.refresh()
                select_union(f.host)
                before=copy.deepcopy(f.stop);after=holds.tick(f.record);f.write(f.name,after)
                current=f.host.state_files()
                proof=f.host.cleanup_clock_observation(f.name,f.before[f.name],current[f.name],f.stop['cleanup_records'])
                self.assertEqual(proof['original'],f.before[f.name]);self.assertEqual(proof['observed'],current[f.name])
                self.assertEqual(proof['next_due_ms_after'],after['last_observed_ms']+after['policy']['retry_delay_ms'])
                self.assertEqual(f.stop,before)
                self.assertEqual({n:v for n,v in current.items() if n!=f.name},
                                 {n:v for n,v in f.before.items() if n!=f.name})
                for label,change in (
                    ('material',lambda x:x.update(checkpoint=None)),
                    ('command',lambda x:x.update(command_id='foreign')),
                    ('start',lambda x:x['worktrees'][0].update(started=True)),
                    ('cache',lambda x:x.update(cache_started=True)),
                    ('clock',lambda x:x.update(last_observed_ms=1)),
                    ('deadline',lambda x:x.update(next_due_ms=x['next_due_ms']+1))):
                    bad=copy.deepcopy(after);change(bad);self.assertNotEqual(bad,after)
                    f.write(f.name,bad);changed=f.host.state_files()
                    with self.subTest(change=label),self.assertRaises(ValueError):
                        f.host.cleanup_clock_observation(f.name,f.before[f.name],changed[f.name],f.stop['cleanup_records'])
        # Exact selected family is required for the added hold reasons.
        for name in ('src/worker/cleanup.rs','src/worker/process.rs','src/worker/codex.rs'):
            with self.subTest(missing=name),fixturemod.cleanup_fixture() as f:
                f.record=holds.hold(f.record);f.write(f.name,f.record);f.refresh();select_union(f.host)
                del f.host._verified_manifest['source']['files'][name]
                f.write(f.name,holds.tick(f.record));current=f.host.state_files()
                with self.assertRaises(ValueError):f.host.cleanup_clock_observation(f.name,f.before[f.name],current[f.name],f.stop['cleanup_records'])

    def test_current28_compact_receipts_preserve_exact_causal_records_and_26_history(self):
        profile,snapshot,_,record=compact.fixture()
        profile['formats']['before']=dict(common.PUBLICATION_UNION_AFTER)
        profile['formats']['after']=dict(common.PUBLICATION_UNION_AFTER)
        manifest={'source':{'files':{n:{'sha256':v} for n,v in common.PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES.items()}}}
        # Direct decoder vocabulary test, not an admitted26-to28 rollout profile.
        # common.validate is deliberately not called here; the full selected
        # route is separately tested as exact24/25/19-to27/28/22 above.
        for schema in (26,28):
            with self.subTest(schema=schema):
                original=copy.deepcopy(snapshot);original['schema']=schema
                profile['formats']['before']['controller_schema']=schema
                restarted=copy.deepcopy(original);restarted['schema']=28
                before=copy.deepcopy(original)
                rows=common.controller_receipt_plan(profile,original,restarted,manifest=manifest)
                selected=next(v for v in rows if v['id']=='read-a')
                self.assertEqual(selected['record'],record);self.assertEqual(len(rows),4)
                reply={'status':'ok','data':{'kind':'operation','value':record},'commit':selected['commit'],'observed_controller_revision':40}
                self.assertEqual(common.verify_controller_lookup(selected,reply),'committed_read_reference')
                self.assertEqual(original,before)
        snapshot['schema']=28;profile['formats']['before']['controller_schema']=28
        for identity in ('custody-a','safe-a','release-a'):
            wrong=copy.deepcopy(snapshot)
            wrong['tables']['receipts']['rows']=[r for r in wrong['tables']['receipts']['rows'] if r[0]!=identity]
            with self.subTest(missing=identity),self.assertRaises(ValueError):
                common.controller_receipt_plan(profile,wrong,copy.deepcopy(wrong),manifest=manifest)
        for name in common.PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES:
            wrong=copy.deepcopy(manifest);wrong['source']['files'][name]['sha256']='0'*64
            with self.subTest(source=name),self.assertRaises(ValueError):
                common.controller_receipt_plan(profile,snapshot,copy.deepcopy(snapshot),manifest=wrong)

    def test_compact_union_does_not_authorize_publication27_or_pre25_marker_carriers(self):
        profile,snapshot,old_manifest,_=compact.fixture()
        old_profile=copy.deepcopy(profile)
        manifest={'source':{'files':{n:{'sha256':v} for n,v in common.PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES.items()}}}
        profile['formats']['before']=dict(common.PUBLICATION_UNION_AFTER)
        profile['formats']['after']=dict(common.PUBLICATION_UNION_AFTER)
        for schema in (25,27,29,True):
            wrong=copy.deepcopy(snapshot);wrong['schema']=schema
            profile['formats']['before']['controller_schema']=schema
            restarted=copy.deepcopy(wrong);restarted['schema']=28
            with self.subTest(schema=schema),self.assertRaisesRegex(ValueError,'compact_original_schema'):
                common.controller_receipt_plan(profile,wrong,restarted,manifest=manifest)
        wrong=copy.deepcopy(snapshot);wrong['schema']=28
        old_profile['formats']['before']['controller_schema']=28
        restarted=copy.deepcopy(wrong);restarted['schema']=26
        with self.assertRaisesRegex(ValueError,'compact_original_schema'):
            common.controller_receipt_plan(old_profile,wrong,restarted,manifest=old_manifest)
        original=copy.deepcopy(snapshot);original['schema']=25
        profile['formats']['before']=dict(common.PUBLICATION_UNION_BEFORE)
        with self.assertRaisesRegex(ValueError,'compact_original_schema'):
            common.controller_receipt_plan(profile,original,wrong,manifest=manifest)
        old_profile['formats']['before']['controller_schema']=26
        historical=common.controller_receipt_plan(old_profile,snapshot,copy.deepcopy(snapshot),manifest=old_manifest)
        self.assertEqual(len(historical),4)

    def test_current22_codex_local_recovery_retains_home_without_cleanup_authority(self):
        helper=codex_retained.CodexRetainedTests()
        helper.FAMILY=hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES
        helper.FORMATS=hostmod._PUBLICATION_UNION_CODEX_HOME_FORMATS
        with helper.fixture('local_recovery') as f:
            before=copy.deepcopy(f.stop)
            with patch.object(hostmod,'verify_task',side_effect=AssertionError('no canonical settlement')):
                preserved,changes=f.check()
            self.assertEqual(set(changes),{f.name});self.assertEqual(f.stop,before)
            proof=f.host._codex_home_proof
            self.assertEqual(proof['retained_source_files'],hostmod._PUBLICATION_UNION_CODEX_RETAINED_SOURCES)
            row=proof['retained_attempts'][f.key]
            self.assertEqual(row['current_reason'],'local_recovery')
            self.assertTrue(row['current_not_started'] and row['current_not_done'] and row['original_home_file_rows_exact'])
            for field in ('old_progress_body_available','canonical_settlement_claimed','reclamation_claimed'):
                self.assertFalse(row[field])
            home={n:v for n,v in before['state'].items() if n.startswith(f.base+'/')}
            self.assertTrue(home);self.assertEqual({n:f.current[n] for n in home},home)
            self.assertTrue(set(home).issubset(preserved));f.host.api.assert_not_called()
        for change in ('start','done','archive','home','command','clock','source'):
            with self.subTest(change=change),helper.fixture('local_recovery') as f:
                if change=='start':f.record['started']=True
                elif change=='done':f.record['done']=True
                elif change=='archive':f.record['archive']={'invented':'copyout'}
                elif change=='home':(f.root/f.base/'plugins/cache/vendor.db').write_bytes(b'changed')
                elif change=='command':
                    command=common.strict((f.root/f.f.command).read_bytes())
                    command['command']['attempt']['session_id']='foreign';f.f.write(f.f.command,command)
                elif change=='clock':f.record['next_due_ms']+=1
                else:del f.host._verified_manifest['source']['files']['src/worker/cleanup_plugins.rs']
                f.write_progress()
                with self.assertRaises(ValueError):f.check()
                f.host.api.assert_not_called()

    def test_both_real_loaders_bind_the_exact_common_bytes_and_refuse_drift(self):
        path=Path(common.__file__);raw=path.read_bytes();expected=common.digest(raw)
        self.assertEqual(adapter._COMMON_SHA,expected);self.assertEqual(hostmod._COMMON_SHA,expected)
        root=path.parent.parent
        self.assertEqual(len(hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES),76)
        self.assertIn(b'pub const WIRE_REVISION: u32 = 27;', (root/'src/protocol.rs').read_bytes())
        for family in (hostmod._PUBLICATION_UNION_CODEX_HOME_SOURCES,
                       hostmod._PUBLICATION_UNION_CODEX_RETAINED_SOURCES,
                       hostmod._PUBLICATION_UNION_CHARTER_MANIFEST_SOURCES,
                       hostmod._PUBLICATION_UNION_CHARTER_JOURNAL_SOURCES,
                       common.PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES):
            for name,sha in family.items():
                with self.subTest(source=name):self.assertEqual(common.digest((root/name).read_bytes()),sha)
        for module in (adapter,hostmod):
            self.assertEqual(module._common().PUBLICATION_UNION_AFTER,common.PUBLICATION_UNION_AFTER)
            with self.subTest(loader=module.__name__),patch.object(Path,'read_bytes',return_value=raw+b'\n'),self.assertRaisesRegex(ValueError,'common.*changed'):
                module._common()


# Import as a module: no imported/inherited TestCase adds hidden selections.
# Preparation may invoke its existing private Git fixture; it never launches
# the generated runtime/build recipe or contacts an installed service.
import test_protected_preparation_v2 as preparation_fixture

class PublicationUnionPreparationTests(unittest.TestCase):
    def test_exact_wire27_is_bound_into_real_prepared_journey_without_authority(self):
        private=Path(tempfile.mkdtemp(prefix='borg-union-preparation-'))
        with patch.object(preparation_fixture,'TEST_ROOT',private), \
                patch.object(preparation_fixture,'_SOURCE',None):
            f=preparation_fixture.Preparation()
            try:
                f.setUp()
                f.r['preparation']['profile']['application_wire_revision']=27
                before=copy.deepcopy(f.r);_,receipt=f.execute()
                self.assertEqual(f.r,before)
                journey=f.job/'prepared/journey.json'
                self.assertEqual(common.strict(journey.read_bytes())['protocol_revision'],27)
                self.assertEqual(receipt['prepared_inputs'][str(journey)],prep.sha(journey))
                prep.verify_prepared(f.r,receipt,f.work,preparation_fixture.local)
                self.assertFalse(receipt['protected_check_emitted'] or receipt['human_rollout_authority'])
            finally:
                try:f.doCleanups()
                finally:preparation_fixture.tearDownModule()

    def test_wire27_is_explicit_and_26_unknown_and_nonintegers_remain_refused(self):
        self.assertEqual(prep.application_wire({}),8)
        for wire in (8,24,25,27):self.assertEqual(prep.application_wire({'application_wire_revision':wire}),wire)
        for wire in (26,28,True,False,None,'27',27.0):
            with self.subTest(wire=wire),self.assertRaises(ValueError):prep.application_wire({'application_wire_revision':wire})
