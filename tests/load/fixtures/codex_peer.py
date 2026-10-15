#!/usr/bin/python3
"""Controlled stdio peer only. No Codex runtime or provider calls."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

config = json.loads(Path(__file__).with_suffix('.json').read_text())
assert os.environ.get('OPENAI_API_KEY') == 'local-load-synthetic-key'
assert all(k not in os.environ for k in ['CODEX_API_KEY', 'GH_TOKEN', 'GITHUB_TOKEN'])
thread = 'load-thread-' + str(os.getpid())
turn = 'load-turn-' + str(os.getpid())

def send(value):
    print(json.dumps(value), flush=True)

def record(value):
    target = Path(config['evidence']) / ('turn-' + str(os.getpid()) + '.json')
    temporary = target.with_suffix('.pending')
    temporary.write_text(json.dumps(value, sort_keys=True))
    temporary.replace(target)

for line in sys.stdin:
    assert len(line) <= 1024 * 1024
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        send({'id': request['id'], 'result': {'userAgent': 'controlled-local-load-peer'}})
    elif method == 'command/exec':
        assert request['params']['command'] == ['/usr/bin/true']
        outcome = subprocess.run(['/usr/bin/true'], check=False)
        send({'id': request['id'], 'result': {'exitCode': outcome.returncode, 'stdout': '', 'stderr': ''}})
    elif method == 'thread/start':
        assert request['params']['model'] == 'gpt-6-astra' and request['params']['config']['model_reasoning_effort'] == 'max'
        send({'id': request['id'], 'result': {'thread': {'id': thread}, 'model': 'gpt-6-astra', 'reasoningEffort': 'max'}})
    elif method == 'turn/start':
        assert request['params']['model'] == 'gpt-6-astra' and request['params']['effort'] == 'max'
        send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress'}}})
        observed = {'pid': os.getpid(), 'cwd': os.getcwd(), 'thread': thread, 'turn': turn,
                    'before': Path('answer.txt').read_text(), 'started_unix_ns': time.time_ns(),
                    'started_monotonic_ns': time.monotonic_ns(),
                    'auto_released': Path(config['release']).exists()}
        record(observed)
        with socket.create_connection(('127.0.0.1', config['port']), timeout=310) as gate:
            gate.sendall((json.dumps(observed) + '\n').encode())
            if not observed['auto_released'] and gate.recv(1) != b'G':
                sys.exit(4)
        Path('answer.txt').write_text('after\n')
        observed['finished_unix_ns'] = time.time_ns()
        observed['finished_monotonic_ns'] = time.monotonic_ns()
        record(observed)
        send({'method': 'item/completed', 'params': {'item': {'type': 'agentMessage', 'text': 'controlled source correction'}}})
        send({'method': 'turn/completed', 'params': {'threadId': thread, 'turn': {'id': turn, 'status': 'completed'}}})
    elif method == 'turn/interrupt':
        send({'id': request['id'], 'result': {}})
    elif 'id' in request:
        send({'id': request['id'], 'error': {'code': -32601, 'message': 'unsupported controlled request'}})
