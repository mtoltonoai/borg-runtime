"""Fixed supervised build stage; successful exit is only a measured input."""
import json,pathlib,sys,time


def observe(c,ask):
    assert set(c['commands'])=={'build'} and not c['cleanup_verified']
    control=c['live_control']
    assert set(control)=={'deadline_monotonic_ns','message_limit'}
    end=control['deadline_monotonic_ns'];messages=control['message_limit']
    assert type(end) is int and type(messages) is int and messages>=3
    assert time.monotonic_ns()<end,'configured build observation budget expired'
    h=ask('start',command='build')['handle']
    start=time.monotonic_ns();polls=messages-2
    # Reserve start/finish messages and one final polling interval for terminal
    # handoff. There is no second timeout independent of the protected stage.
    interval=(end-start)//(polls+1)
    outcome=None
    if interval>0:
        for index in range(1,polls+1):
            remaining=start+index*interval-time.monotonic_ns()
            if remaining>0:time.sleep(remaining/1000000000)
            if time.monotonic_ns()>=end:break
            outcome=ask('poll',handle=h)
            if outcome['exit'] is not None:break
    passed=outcome is not None and outcome['exit']==0 and outcome['leader_reaped'] and time.monotonic_ns()<end
    return ask('finish',result={'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':passed,'observations':['Fixed supervisor observed build exit; exact artifact/source and cleanup still require post-run adjudication']})


def main():
    c=json.loads(pathlib.Path(sys.argv[1]).read_bytes());seq=0
    def ask(op,**fields):
        nonlocal seq
        seq+=1
        assert seq<=c['live_control']['message_limit'],'configured live message allowance exhausted'
        print(json.dumps({'sequence':seq,'operation':op,**fields}),flush=True)
        r=json.loads(sys.stdin.readline());assert r['sequence']==seq;return r['result']
    observe(c,ask)


if __name__=='__main__':main()
