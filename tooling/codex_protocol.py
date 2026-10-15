#!/usr/bin/python3
"""Controlled stdio protocol peer. No provider, Codex runtime, or network egress.
A loopback connection lets the Rust test observe each real executor process and
release its one turn; it does not stand in for worker/controller stop evidence.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

config = json.loads(Path(__file__).with_suffix('.json').read_text())
thread = 'controlled-thread-' + str(os.getpid())
turn = 'controlled-turn-' + str(os.getpid())

def send(value):
    print(json.dumps(value), flush=True)

for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        send({'id': request['id'], 'result': {'userAgent': 'controlled-recovery-fixture'}})
    elif method == 'command/exec':
        assert request['params']['command'] == ['/usr/bin/true']
        outcome = subprocess.run(['/usr/bin/true'], check=False)
        send({'id': request['id'], 'result': {'exitCode': outcome.returncode, 'stdout': '', 'stderr': ''}})
    elif method == 'thread/start':
        params=request['params']
        assert params['model']=='gpt-6-astra' and params['config']['model_reasoning_effort']=='max'
        assert params['approvalPolicy']=='never' and params['sandbox']=='workspace-write'
        servers=params['config']['mcp_servers'];assert set(servers)=={'borg_collaboration'}
        assert servers['borg_collaboration']['command'] and servers['borg_collaboration']['args']
        send({'id': request['id'], 'result': {'thread': {'id': thread}, 'model':params['model'], 'reasoningEffort':params['config']['model_reasoning_effort']}})
    elif method == 'turn/start':
        assert request['params']['model']=='gpt-6-astra' and request['params']['effort']=='max'
        send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress'}}})
        observation = {'pid': os.getpid(), 'cwd': os.getcwd(), 'before': Path('answer.txt').read_text(), 'thread': thread, 'turn': turn}
        with socket.create_connection(('127.0.0.1', config['port']), timeout=20) as gate:
            gate.sendall((json.dumps(observation) + '\n').encode())
            if gate.recv(1) != b'G':
                sys.exit(4)
        Path('answer.txt').write_text('after\n')
        send({'method': 'item/completed', 'params': {'item': {'type': 'agentMessage', 'text': 'controlled source correction'}}})
        send({'method': 'turn/completed', 'params': {'threadId': thread, 'turn': {'id': turn, 'status': 'completed'}}})
    elif method == 'turn/interrupt':
        send({'id': request['id'], 'result': {}})
    elif 'id' in request:
        send({'id': request['id'], 'error': {'code': -32601, 'message': 'unsupported fixture request'}})
