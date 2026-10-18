#!/usr/bin/python3
"""Controlled app-server peer that launches the actual task-scoped Borg MCP bridge.
Only loopback fixture gates are used. No Codex, provider or remote execution.
"""
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time

config = json.loads(Path(__file__).with_suffix('.json').read_text())
thread = 'recovery-thread-' + str(os.getpid())
turn = 'recovery-turn-' + str(os.getpid())
bridge = None
bridge_config = None
sequence = 0


def stopped(signum, frame):
    raise SystemExit(128 + signum)


for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGALRM):
    signal.signal(sig, stopped)
signal.alarm(180)


def send(value):
    print(json.dumps(value), flush=True)


def rpc(method, params):
    global sequence
    sequence += 1
    frame = (json.dumps({'jsonrpc': '2.0', 'id': sequence,
                         'method': method, 'params': params}) + '\n').encode()
    assert len(frame) <= bridge_config['limits']['frame_bytes']
    bridge.stdin.write(frame)
    bridge.stdin.flush()
    deadline = time.monotonic() + 90
    raw = bytearray()
    while not raw.endswith(b'\n'):
        assert len(raw) < bridge_config['limits']['frame_bytes']
        remaining = deadline - time.monotonic()
        assert remaining > 0 and select.select([bridge.stdout], [], [], remaining)[0]
        byte = os.read(bridge.stdout.fileno(), 1)
        assert byte
        raw += byte
    reply = json.loads(raw)
    assert reply['id'] == sequence and 'error' not in reply
    return reply['result']


def gate(mode, result=None):
    observation = {'pid': os.getpid(), 'bridge_pid': bridge.pid,
                   'cwd': os.getcwd(), 'before': Path('answer.txt').read_text(),
                   'thread': thread, 'turn': turn, 'mode': mode, 'result': result}
    with socket.create_connection(('127.0.0.1', config['port']), timeout=90) as stream:
        stream.sendall((json.dumps(observation) + '\n').encode())
        if stream.recv(1) != b'G':
            raise SystemExit(4)


try:
    for line in sys.stdin:
        request = json.loads(line)
        method = request.get('method')
        if method == 'initialize':
            send({'id': request['id'], 'result': {'userAgent': 'controlled-retained-call-peer'}})
        elif method == 'command/exec':
            assert request['params']['command'] == ['/usr/bin/true']
            result = subprocess.run(['/usr/bin/true'], check=False, timeout=5)
            send({'id': request['id'], 'result': {'exitCode': result.returncode, 'stdout': '', 'stderr': ''}})
        elif method == 'thread/start':
            assert request['params']['model'] == 'gpt-6-astra'
            assert request['params']['config']['model_reasoning_effort'] == 'max'
            server = request['params']['config']['mcp_servers']['borg_collaboration']
            args = server['args']
            assert args[0] == '__collaboration_mcp'
            bridge_config = json.loads(Path(args[args.index('--config') + 1]).read_text())
            bridge = subprocess.Popen([server['command'], *args], stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=sys.stderr, bufsize=0)
            version = config['mcp_version']
            assert rpc('initialize', {'protocolVersion': version, 'capabilities': {},
                       'clientInfo': {'name': 'controlled-recovery-peer', 'version': '1'}})['protocolVersion'] == version
            send({'id': request['id'], 'result': {'thread': {'id': thread}, 'model': 'gpt-6-astra', 'reasoningEffort': 'max'}})
        elif method == 'turn/start':
            assert request['params']['model'] == 'gpt-6-astra' and request['params']['effort'] == 'max'
            text = json.dumps(request['params']['input'])
            mode = next(name for name in ('LOSS_ONCE', 'READ_ONCE', 'HEALTHY') if name in text)
            send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress'}}})
            send({'method': 'item/agentMessage/delta', 'params': {'delta': 'Controlled bridge ready'}})
            result = None
            if mode == 'LOSS_ONCE':
                gate(mode)
                tool, args = 'collab_memory_set', {'key': 'recovery-proof', 'expected_revision': None, 'value': {'write': 'once'}}
            elif mode == 'READ_ONCE':
                tool, args = 'collab_memory_get', {'key': 'recovery-proof'}
            else:
                tool = None
            if tool:
                body = rpc('tools/call', {'name': tool, 'arguments': args,
                           '_meta': {'callId': 'original-write' if mode == 'LOSS_ONCE' else 'later-read',
                                     'threadId': thread, 'sessionId': thread}})
                assert body['isError'] is False
                result = json.loads(body['content'][0]['text'])
                rpc('ping', {})  # delivery marker is durable before the gate
                if mode == 'LOSS_ONCE':
                    raise AssertionError('held original result unexpectedly reached lost executor')
            gate(mode, result)
            Path('answer.txt').write_text('after\n')
            send({'method': 'item/completed', 'params': {'item': {'type': 'agentMessage', 'text': 'Controlled recovery source accepted'}}})
            send({'method': 'turn/completed', 'params': {'threadId': thread, 'turn': {'id': turn, 'status': 'completed'}}})
        elif method == 'thread/inject_items':
            assert request['params']['threadId'] == thread
            send({'id': request['id'], 'result': {}})
        elif method == 'turn/interrupt':
            send({'id': request['id'], 'result': {}})
        elif 'id' in request:
            send({'id': request['id'], 'error': {'code': -32601, 'message': 'unsupported controlled request'}})
finally:
    if bridge is not None:
        bridge.stdin.close()
        try:
            bridge.wait(timeout=3)
        except subprocess.TimeoutExpired:
            bridge.terminate()
            try:
                bridge.wait(timeout=3)
            except subprocess.TimeoutExpired:
                bridge.kill()
                bridge.wait(timeout=3)
