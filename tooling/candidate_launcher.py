#!/usr/bin/python3
"""Fixed local qualification launcher. Synthetic loopback credential only."""
import hashlib, json, os, pathlib, sys
raw=pathlib.Path(sys.argv[1]).read_bytes()
if hashlib.sha256(raw).hexdigest()!=sys.argv[2]:raise SystemExit('fixed launch inputs changed')
c=json.loads(raw);name=sys.argv[3]
if name not in c['launches']:raise SystemExit('undeclared fixed launch')
p=pathlib.Path(c['binary'])
if hashlib.sha256(p.read_bytes()).hexdigest()!=c['binary_sha256']:raise SystemExit('candidate changed')
argv=c['launches'][name]
env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':c['workspace']}
if name.startswith('native'):env['OPENAI_API_KEY']='controlled-native-fixture-key'
os.execve(str(p),[str(p),*argv],env)
