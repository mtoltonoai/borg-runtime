"""Pure protected-observer clock/protocol tests; no process or provider calls."""
import copy,io,json,unittest
from unittest.mock import patch
import build_observer as observer


class Clock:
    def __init__(self):self.now=0;self.sleeps=[]
    def monotonic_ns(self):return self.now
    def sleep(self,seconds):
        assert seconds>=0
        self.sleeps.append(seconds);self.now+=round(seconds*1000000000)


class BuildObserverBudgetTests(unittest.TestCase):
    def context(self,deadline_ms=210000,messages=128):
        return {'commands':{'build':{}},'cleanup_verified':False,'subject_sha256':'subject','recipe_sha256':'recipe','check':'selected_build','live_control':{'deadline_monotonic_ns':deadline_ms*1000000,'message_limit':messages}}
    def run_observation(self,deadline_ms=210000,exit_ms=None,code=0,reaped=True,messages=128,start_delay_ms=0):
        clock=Clock();requests=[]
        def ask(op,**fields):
            requests.append({'operation':op,**copy.deepcopy(fields),'time_ns':clock.now})
            if op=='start':
                self.assertEqual(fields,{'command':'build'});clock.now+=start_delay_ms*1000000
                return {'handle':1}
            if op=='poll':
                self.assertEqual(fields,{'handle':1});done=exit_ms is not None and clock.now>=exit_ms*1000000
                return {'exit':code if done else None,'leader_reaped':reaped if done else False}
            self.assertEqual(op,'finish')
            return {'recorded':True,'cleanup_verified':False,'check_emitted':False}
        with patch.object(observer.time,'monotonic_ns',clock.monotonic_ns),patch.object(observer.time,'sleep',clock.sleep):
            reply=observer.observe(self.context(deadline_ms,messages),ask)
        self.assertFalse(reply['cleanup_verified']);self.assertFalse(reply['check_emitted'])
        self.assertLessEqual(len(requests),messages)
        return clock,requests
    def test_measured_build_duration_fits_trusted_210_second_stage(self):
        clock,requests=self.run_observation(exit_ms=123567)
        terminal=requests[-1]['result'];self.assertTrue(terminal['passed'])
        self.assertGreater(clock.now,120000000000);self.assertLess(clock.now,210000000000)
        self.assertEqual(terminal['subject_sha256'],'subject');self.assertEqual(terminal['recipe_sha256'],'recipe')
    def test_nonterminal_build_uses_finite_messages_and_reserves_terminal_interval(self):
        clock,requests=self.run_observation()
        self.assertEqual(len(requests),128);self.assertEqual(sum(x['operation']=='poll' for x in requests),126)
        self.assertFalse(requests[-1]['result']['passed'])
        interval=210000000000//127
        self.assertGreaterEqual(210000000000-clock.now,interval)
    def test_short_configured_budget_is_not_replaced_by_old_timeout(self):
        clock,requests=self.run_observation(deadline_ms=300,exit_ms=350)
        self.assertFalse(requests[-1]['result']['passed']);self.assertLess(clock.now,300000000)
        self.assertTrue(self.run_observation(deadline_ms=300,exit_ms=150)[1][-1]['result']['passed'])
    def test_exit_failure_and_unreaped_success_remain_failures(self):
        for code,reaped in [(1,True),(0,False),(-9,True)]:
            with self.subTest(code=code,reaped=reaped):
                self.assertFalse(self.run_observation(exit_ms=0,code=code,reaped=reaped)[1][-1]['result']['passed'])
    def test_start_latency_consumes_same_deadline_without_refresh(self):
        clock,requests=self.run_observation(deadline_ms=1000,start_delay_ms=1001,exit_ms=0)
        self.assertEqual([x['operation'] for x in requests],['start','finish'])
        self.assertFalse(requests[-1]['result']['passed']);self.assertEqual(clock.now,1001000000)
    def test_missing_invalid_or_expired_control_refuses_before_start(self):
        for control in [None,{}, {'deadline_monotonic_ns':0,'message_limit':128},{'deadline_monotonic_ns':100,'message_limit':2},{'deadline_monotonic_ns':True,'message_limit':128},{'deadline_monotonic_ns':100,'message_limit':True}]:
            with self.subTest(control=control):
                c=self.context();c['live_control']=control;calls=[]
                with patch.object(observer.time,'monotonic_ns',return_value=0):
                    with self.assertRaises((AssertionError,TypeError,KeyError)):observer.observe(c,lambda *a,**k:calls.append(a))
                self.assertEqual(calls,[])
    def test_main_preserves_sequence_exact_protocol_and_bounded_terminal_reply(self):
        clock=Clock();c=self.context(deadline_ms=1000,messages=3)
        responses='\n'.join(json.dumps(x) for x in [{'sequence':1,'result':{'handle':1}},{'sequence':2,'result':{'exit':0,'leader_reaped':True}},{'sequence':3,'result':{'recorded':True,'cleanup_verified':False,'check_emitted':False}}])+'\n'
        output=io.StringIO()
        with patch.object(observer.sys,'argv',['build_observer.py','input.json']),patch.object(observer.pathlib.Path,'read_bytes',return_value=json.dumps(c).encode()),patch.object(observer.sys,'stdin',io.StringIO(responses)),patch.object(observer.sys,'stdout',output),patch.object(observer.time,'monotonic_ns',clock.monotonic_ns),patch.object(observer.time,'sleep',clock.sleep):observer.main()
        requests=[json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([x['sequence'] for x in requests],[1,2,3]);self.assertEqual([x['operation'] for x in requests],['start','poll','finish']);self.assertTrue(requests[-1]['result']['passed'])
    def test_mismatched_supervisor_sequence_refuses(self):
        c=self.context();output=io.StringIO()
        with patch.object(observer.sys,'argv',['build_observer.py','input.json']),patch.object(observer.pathlib.Path,'read_bytes',return_value=json.dumps(c).encode()),patch.object(observer.sys,'stdin',io.StringIO('{"sequence":2,"result":{"handle":1}}\n')),patch.object(observer.sys,'stdout',output),patch.object(observer.time,'monotonic_ns',return_value=0):
            with self.assertRaises(AssertionError):observer.main()
        self.assertEqual(len(output.getvalue().splitlines()),1)


if __name__=='__main__':unittest.main()
