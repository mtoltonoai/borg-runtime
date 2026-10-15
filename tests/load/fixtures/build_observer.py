#!/usr/bin/python3
"""Observe a real rustc child; no permits, serialization or synthetic build."""
import json
import errno
import os
from pathlib import Path
import subprocess
import sys
import time

config = json.loads(Path(__file__).with_suffix('.json').read_text())
assert len(sys.argv) == 2 and sys.argv[1] in ('test', 'binary')
args = [config['rustc']]
if sys.argv[1] == 'test':
    args += ['--test']
args += ['src/main.rs', '-o', 'acceptance' if sys.argv[1] == 'test' else 'demo']
assert all(k not in os.environ for k in ['OPENAI_API_KEY', 'CODEX_API_KEY', 'GH_TOKEN', 'GITHUB_TOKEN'])
started = time.time_ns()
started_monotonic = time.monotonic_ns()
child = subprocess.Popen(args)
record = {'wrapper_pid': os.getpid(), 'pid': child.pid, 'cwd': os.getcwd(), 'kind': sys.argv[1],
          'argv': args, 'spawn_before_unix_ns': started, 'spawn_returned_unix_ns': time.time_ns(),
          'spawn_before_monotonic_ns': started_monotonic, 'spawn_returned_monotonic_ns': time.monotonic_ns(),
          'first_live_unix_ns': None, 'last_live_unix_ns': None,
          'first_live_monotonic_ns': None, 'last_live_monotonic_ns': None, 'start_ticks': None,
          'unobserved_samples': 0, 'sample_diagnostics': []}
path = Path(config['evidence']) / ('build-' + str(os.getpid()) + '.json')

def save():
    temporary = path.with_suffix('.pending')
    temporary.write_text(json.dumps(record, sort_keys=True))
    temporary.replace(path)

save()
try:
    while child.poll() is None:
        try:
            fields = Path('/proc/' + str(child.pid) + '/stat').read_text().rsplit(') ', 1)[1].split()
            if fields[0] not in ('Z', 'X'):
                stamp = time.time_ns()
                monotonic = time.monotonic_ns()
                ticks = int(fields[19])
                assert record['start_ticks'] in (None, ticks)
                record['start_ticks'] = ticks
                if record['first_live_unix_ns'] is None:
                    record['first_live_unix_ns'] = stamp
                    record['first_live_monotonic_ns'] = monotonic
                record['last_live_unix_ns'] = stamp
                record['last_live_monotonic_ns'] = monotonic
        except (OSError, IndexError, ValueError) as error:
            if isinstance(error, OSError) and error.errno not in (errno.ENOENT, errno.ESRCH):
                raise
            record['unobserved_samples'] += 1
            if len(record['sample_diagnostics']) < 4:
                record['sample_diagnostics'].append('process exited or stat sample incomplete; no live sample inferred')
        time.sleep(.005)
    record['exit'] = child.wait()
finally:
    if child.poll() is None:
        child.kill()
        child.wait(timeout=5)
        record['exit'] = child.returncode
    record['wait_returned_unix_ns'] = time.time_ns()
    record['wait_returned_monotonic_ns'] = time.monotonic_ns()
    save()
sys.exit(record['exit'])
