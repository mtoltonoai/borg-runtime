"""Unchanged hold observations and the inspect-only reader entrypoint.

Local controlled files only. These tests grant no cleanup or task authority.
"""
import copy, io, json, sys, unittest
from types import SimpleNamespace
from unittest import mock
import rollout_host_v2 as host
from rollout_fixture import cleanup_fixture
from test_rollout_reconciliation import NoExternalEffects, transition_fixture
C=host.c


def hold(value,reason='uncertain_effect',status=None):
    value=copy.deepcopy(value)
    value.update(reason=reason,status=status,first_safe_ms=None,
                 next_due_ms=value['last_observed_ms']+value['policy']['retry_delay_ms'])
    return value


def tick(value):
    value=copy.deepcopy(value);value['last_observed_ms']+=600000
    value['next_due_ms']=value['last_observed_ms']+value['policy']['retry_delay_ms']
    return value


def select(h):
    h.p['formats']['source_files'].update({'src/worker/cleanup.rs':host._RETAINED_HOLD_SOURCE_SHA,
                                         'src/worker/process.rs':host._PROCESS_SOURCE_SHA})


class RetainedHoldTests(NoExternalEffects):
    def test_two_exact_reasons_keep_status_and_every_nonclock_field(self):
        with cleanup_fixture() as f:
            for reason,status in [('uncertain_effect',None),('canonical_binding',None),('canonical_binding','failed')]:
                with self.subTest(reason=reason,status=status):
                    before=hold(f.record,reason,status);after=tick(before)
                    proof=host.cleanup_clocks(before,after,retained_holds=True)
                    self.assertEqual(proof['next_due_ms_after'],after['last_observed_ms']+before['policy']['retry_delay_ms'])
                    self.assertEqual(before['status'],after['status']);self.assertNotIn('transition',proof)
                    self.assertEqual(set(proof),{'last_observed_ms_before','last_observed_ms_after','next_due_ms_before','next_due_ms_after','unchanged_record_sha256'})
                    with self.assertRaisesRegex(ValueError,'outside observation-only'):
                        host.cleanup_clocks(before,after)

    def test_captured_hold_tick_preserves_command_and_never_uses_settled_task_authority(self):
        for reason in ('uncertain_effect','canonical_binding'):
            with self.subTest(reason=reason),transition_fixture('clock') as x:
                x.original=hold(x.original,reason);x.observed=tick(x.original)
                x.task.update(status='failed',selected_checkpoint=None)
                x.task['attempt']['uncertain']=True
                x.baseline();x.install();select(x.host)
                with mock.patch.object(host,'validate_transition',side_effect=AssertionError('hold cannot reclaim')):
                    current,preserved,observed=x.observe()
                self.assertEqual(preserved[x.f.command],x.before[x.f.command])
                self.assertNotIn('transition',observed[x.f.name]);self.assertEqual(x.hook.proof()['records'],0)
                self.assertEqual(x.capture.proof()['records'],1)
                self.assertEqual(C.strict(x.host.cleanup_bytes(x.f.name,current[x.f.name],1048576)),x.observed)
                with self.assertRaisesRegex(ValueError,'canonical_binding'):
                    host.verify_task(C,x.command,x.task,'agent-lab-2')

    def test_unselected_producer_process_or_missing_pin_refuses(self):
        for path in ('src/worker/cleanup.rs','src/worker/process.rs'):
            for value in (None,'0'*64):
                with self.subTest(path=path,value=value),cleanup_fixture() as f:
                    f.record=hold(f.record);f.write(f.name,f.record);f.refresh();select(f.host)
                    if value is None:f.host.p['formats']['source_files'].pop(path)
                    else:f.host.p['formats']['source_files'][path]=value
                    f.write(f.name,tick(f.record));current=f.host.state_files()
                    with self.assertRaisesRegex(ValueError,'outside observation-only'):
                        f.host.cleanup_clock_observation(f.name,f.before[f.name],current[f.name],f.stop['cleanup_records'])

    def test_all_material_changes_and_backward_clock_refuse(self):
        with cleanup_fixture() as f:
            before=hold(f.record)
            changes={
                'reason':lambda x:x.update(reason='canonical_binding'),
                'status':lambda x:x.update(status='failed'),
                'checkpoint':lambda x:x.update(checkpoint=None),
                'policy':lambda x:x['policy'].update(retry_delay_ms=60001),
                'attempt':lambda x:x.update(attempt_id='other'),
                'command':lambda x:x.update(command_id='other'),
                'proof':lambda x:x['worktrees'][0].update(proof={'changed':True}),
                'removal':lambda x:x['worktrees'][0].update(started=True),
                'cache':lambda x:x.update(cache_started=True),
                'cache_safe':lambda x:x.update(cache_safe_ms=1),
                'backward':lambda x:x.update(last_observed_ms=before['last_observed_ms']-1),
            }
            for name,change in changes.items():
                with self.subTest(name=name):
                    after=tick(before);change(after)
                    with self.assertRaisesRegex(ValueError,'exceeds observation clocks'):
                        host.cleanup_clocks(before,after,retained_holds=True)

    def test_retained_scope_and_exact_deadlines_refuse_nonproducer_shapes(self):
        with cleanup_fixture() as f:
            changes=[lambda x:x.update(first_safe_ms=1),lambda x:x.update(disposition='uncertain'),
                     lambda x:x.update(cache_started=True),lambda x:x.update(cache_done=True),
                     lambda x:x['worktrees'][0].update(started=True),lambda x:x['worktrees'][0].update(done=True)]
            for change in changes:
                before=hold(f.record);change(before)
                with self.assertRaisesRegex(ValueError,'retained hold scope'):
                    host.cleanup_clocks(before,tick(before),retained_holds=True)
            for side in ('before','after'):
                before=hold(f.record);after=tick(before)
                (before if side=='before' else after)['next_due_ms']-=1
                with self.assertRaisesRegex(ValueError,'retained hold deadline'):
                    host.cleanup_clocks(before,after,retained_holds=True)
            before=hold(f.record);after=tick(before);after['last_observed_ms']=2**64-1;after['next_due_ms']=2**64
            with self.assertRaisesRegex(ValueError,'clock overflow'):
                host.cleanup_clocks(before,after,retained_holds=True)

    def test_cache_safe_is_preserved_not_fabricated_and_other_reasons_stay_closed(self):
        with cleanup_fixture() as f:
            before=hold(f.record);before['cache_safe_ms']=7
            host.cleanup_clocks(before,tick(before),retained_holds=True)
            for reason in ('canonical_unavailable','filesystem','clock','not_terminal','active','checkpoint','source_or_path','complete'):
                before=hold(f.record,reason)
                with self.subTest(reason=reason),self.assertRaisesRegex(ValueError,'outside observation-only'):
                    host.cleanup_clocks(before,tick(before),retained_holds=True)


    def select_recovered(self,h):
        h.p['formats']={'before':dict(host._RECOVERED_CODEX_HOME_FORMATS),
                        'after':dict(host._RECOVERED_CODEX_HOME_FORMATS),'source_files':{}}
        h._verified_manifest={'source':{'files':{name:{'sha256':digest}
            for name,digest in host._RECOVERED_CODEX_HOME_SOURCES.items()}}}

    def test_selected20_workspace_hold_ticks_keep_original_material_and_no_reclaim(self):
        # This is cleanup/, not the independent cleanup-codex/ sidecar reader.
        for reason in ('uncertain_effect','canonical_binding'):
            with self.subTest(reason=reason),transition_fixture('clock') as x:
                x.original=hold(x.original,reason);x.observed=tick(x.original)
                x.task.update(status='failed',selected_checkpoint=None)
                x.task['attempt']['uncertain']=True
                x.baseline();x.install();self.select_recovered(x.host)
                original=copy.deepcopy(x.original)
                with mock.patch.object(host,'validate_transition',side_effect=AssertionError('hold cannot reclaim')):
                    current,preserved,observed=x.observe()
                self.assertEqual(preserved[x.f.command],x.before[x.f.command])
                row=observed[x.f.name]
                self.assertEqual(set(row),{'original','observed','last_observed_ms_before','last_observed_ms_after',
                    'next_due_ms_before','next_due_ms_after','unchanged_record_sha256'})
                self.assertEqual(row['original'],x.before[x.f.name])
                self.assertEqual(row['observed'],current[x.f.name])
                self.assertEqual(row['last_observed_ms_before'],original['last_observed_ms'])
                self.assertEqual(row['next_due_ms_after'],x.observed['last_observed_ms']+original['policy']['retry_delay_ms'])
                self.assertEqual(x.hook.proof()['records'],0)
                self.assertEqual(x.capture.proof()['records'],1)
                self.assertEqual(C.strict(x.host.cleanup_bytes(x.f.name,current[x.f.name],1048576)),x.observed)
                self.assertEqual(x.original,original)

    def test_selected20_workspace_holds_refuse_missing_mixed_or_unselected_writer(self):
        for change in ('missing_cleanup','changed_cleanup','missing_process','changed_process',
                       'mixed19_cleanup','historical_family','unrelated_family_pin'):
            with self.subTest(change=change),cleanup_fixture() as f:
                f.record=hold(f.record);f.write(f.name,f.record);f.refresh()
                self.select_recovered(f.host)
                # Exact historical profile pins cannot rescue a failed20 family.
                select(f.host)
                files=f.host._verified_manifest['source']['files']
                if change.startswith('missing_'):del files['src/worker/'+change[8:]+'.rs']
                elif change.startswith('changed_'):files['src/worker/'+change[8:]+'.rs']['sha256']='0'*64
                elif change=='mixed19_cleanup':files['src/worker/cleanup.rs']['sha256']=host._RETAINED_HOLD_SOURCE_SHA
                elif change=='historical_family':
                    f.host._verified_manifest={'source':{'files':{n:{'sha256':v} for n,v in host._COMPOSED_CODEX_HOME_SOURCES.items()}}}
                else:del files['src/worker/read_retention.rs']
                f.write(f.name,tick(f.record));current=f.host.state_files()
                with self.assertRaisesRegex(ValueError,'outside observation-only'):
                    f.host.cleanup_clock_observation(f.name,f.before[f.name],current[f.name],f.stop['cleanup_records'])

    def test_selected20_workspace_hold_material_and_backward_clock_still_refuse(self):
        changes=[lambda x:x.update(reason='canonical_binding'),lambda x:x.update(status='failed'),
                 lambda x:x.update(last_observed_ms=1),lambda x:x.update(cache_started=True)]
        for index,change in enumerate(changes):
            with self.subTest(change=index),cleanup_fixture() as f:
                f.record=hold(f.record);f.write(f.name,f.record);f.refresh();self.select_recovered(f.host)
                after=tick(f.record);change(after);f.write(f.name,after);current=f.host.state_files()
                with self.assertRaises(ValueError):
                    f.host.cleanup_clock_observation(f.name,f.before[f.name],current[f.name],f.stop['cleanup_records'])


class HostObservationEntryTests(NoExternalEffects):
    def test_partial_binding_and_every_noninspect_phase_refuse_before_read_or_dispatch(self):
        profile={'limits':{'module_bytes':1000}}
        with mock.patch.object(C,'pin',side_effect=AssertionError('must refuse before reading')):
            for phase in ('preflight','stop','install','start','unknown',None):
                with self.subTest(phase=phase),self.assertRaisesRegex(ValueError,'inspect-only'):
                    host.observation_reader(profile,{'phase':phase},('/binding','a'*64,100))
            for values in ((None,'a'*64,100),('/binding',None,100),('/binding','a'*64,None)):
                with self.assertRaisesRegex(ValueError,'inspect-only'):
                    host.observation_reader(profile,{'phase':'inspect'},values)
            profile['modules']={'host':{'path':'/old/host','sha256':'a'*64}}
            with self.assertRaisesRegex(ValueError,'requires explicit binding'):
                host.observation_reader(profile,{'phase':'start'},(None,None,None))
            profile['modules']['host']={'path':host.__file__,'sha256':C.digest(b'current-selected-host')}
            with mock.patch.object(C,'read',return_value=b'current-selected-host'):
                self.assertIsNone(host.observation_reader(profile,{'phase':'start'},(None,None,None)))
            with mock.patch.object(C,'read',return_value=b'changed-host'),self.assertRaisesRegex(ValueError,'requires explicit binding'):
                host.observation_reader(profile,{'phase':'inspect'},(None,None,None))

    def test_descriptor_bound_before_module_load_and_exact_validator_operands(self):
        profile={'limits':{'module_bytes':1000}}
        req={'phase':'inspect','profile_path':'/original/profile','profile_sha256':'b'*64,'profile_bytes':200,
             'subject':{'fixed':'subject'},'action_id':'original-action'}
        modules={'adapter':{'path':'/new/adapter','sha256':'c'*64},'host':{'path':'/new/host','sha256':'d'*64}}
        raw=C.canonical({'observation_modules':modules});binding=('/binding','a'*64,len(raw))
        validator=mock.Mock(return_value={'provenance':{'checked':True}})
        with mock.patch.object(C,'pin',return_value=raw) as pin, mock.patch.object(C,'load_module',return_value=SimpleNamespace(validate_observation_binding=validator)) as load:
            actual=host.observation_reader(profile,req,binding)
            self.assertEqual(actual,{'provenance':{'checked':True}})
            pin.assert_called_once_with({'path':'/binding','sha256':'a'*64},len(raw))
            load.assert_called_once_with('rollout_observation_adapter',modules['adapter'],1000)
            validator.assert_called_once_with(profile,'/original/profile','b'*64,200,req['subject'],'original-action',dict(zip(('path','sha256','bytes'),binding)),'host',host.__file__)
        with mock.patch.object(C,'load_module',side_effect=AssertionError('must refuse before module load')):
            with self.assertRaisesRegex(ValueError,'byte bound'):host.observation_reader(profile,req,('/binding','a'*64,1001))
            with mock.patch.object(C,'pin',return_value=raw+b' '),self.assertRaisesRegex(ValueError,'length'):
                host.observation_reader(profile,req,binding)

    def test_main_passes_original_request_and_attaches_only_reader_provenance(self):
        req={'phase':'inspect','profile_path':'/profile','profile_sha256':'a'*64,'profile_bytes':200}
        profile={'limits':{'transport_bytes':4096},'hosts':[{'id':'fixed-host'}]}
        inp=SimpleNamespace(buffer=io.BytesIO(C.canonical(req)));out=SimpleNamespace(buffer=io.BytesIO())
        argv=['host','--profile','/profile','--profile-sha256','a'*64,'--profile-bytes','200','--host','fixed-host',
              '--observation-binding','/binding','--observation-binding-sha256','b'*64,'--observation-binding-bytes','100']
        provenance={'original':'unchanged','reader':'pinned'}
        with mock.patch.object(sys,'argv',argv),mock.patch.object(sys,'stdin',inp),mock.patch.object(sys,'stdout',out),mock.patch.object(C,'load_profile',return_value=profile),mock.patch.object(host,'observation_reader',return_value={'provenance':provenance}) as reader,mock.patch.object(host,'phase',return_value={'reconciliation_scope':{'host_effect_repeated':False}}) as phase:
            host.main()
        phase.assert_called_once_with(profile,profile['hosts'][0],req)
        reader.assert_called_once_with(profile,req,('/binding','b'*64,100))
        self.assertEqual(json.loads(out.buffer.getvalue()),{'reconciliation_scope':{'host_effect_repeated':False},'observation_reader':provenance})
