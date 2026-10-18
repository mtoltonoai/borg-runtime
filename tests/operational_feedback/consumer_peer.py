#!/usr/bin/python3
"""Controlled app-server peer; no Codex, provider, credentials or remote access.

The loopback gate separates an observed progress event from the final message,
so the actual worker's truncation can be checked before acceptance overwrites
recent_output. It is a test synchronization point, not a workload delay.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

config = json.loads(Path(__file__).with_suffix('.json').read_text())
thread = 'consumer-thread-' + str(os.getpid())
turn = 'consumer-turn-' + str(os.getpid())


def send(value):
    print(json.dumps(value), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        send({'id': request['id'], 'result': {'userAgent': 'controlled-consumer-peer'}})
    elif method == 'command/exec':
        assert request['params']['command'] == ['/usr/bin/true']
        result = subprocess.run(['/usr/bin/true'], check=False, timeout=5)
        send({'id': request['id'], 'result': {'exitCode': result.returncode, 'stdout': '', 'stderr': ''}})
    elif method == 'thread/start':
        assert request['params']['model'] == 'gpt-6-astra'
        assert request['params']['config']['model_reasoning_effort'] == 'max'
        send({'id': request['id'], 'result': {'thread': {'id': thread}, 'model': 'gpt-6-astra', 'reasoningEffort': 'max'}})
    elif method == 'turn/start':
        assert request['params']['model'] == 'gpt-6-astra'
        assert request['params']['effort'] == 'max'
        send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress'}}})
        send({'method': 'item/agentMessage/delta', 'params': {'delta': 'X' * 48}})
        with socket.create_connection(('127.0.0.1', config['port']), timeout=30) as gate:
            gate.sendall((json.dumps({'pid': os.getpid(), 'cwd': os.getcwd(), 'before': Path('answer.txt').read_text(), 'thread': thread, 'turn': turn}) + '\n').encode())
            if gate.recv(1) != b'G':
                sys.exit(4)
        Path('answer.txt').write_text('after\n')
        send({'method': 'thread/tokenUsage/updated', 'params': {'threadId': thread, 'turnId': turn, 'tokenUsage': {'total': {'inputTokens': 5, 'outputTokens': 2}}}})
        send({'method': 'item/completed', 'params': {'item': {'type': 'agentMessage', 'text': '\u00e9' * 24}}})
        send({'method': 'turn/completed', 'params': {'threadId': thread, 'turn': {'id': turn, 'status': 'completed'}}})
    elif method == 'turn/interrupt':
        send({'id': request['id'], 'result': {}})
    elif 'id' in request:
        send({'id': request['id'], 'error': {'code': -32601, 'message': 'unsupported controlled request'}})
