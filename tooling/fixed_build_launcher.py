#!/usr/bin/python3
"""Only the selected offline packager and an exact immutable source commit."""
import hashlib,json,os,pathlib,sys
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
raw=pathlib.Path(sys.argv[1]).read_bytes()
if len(raw)>16384 or hashlib.sha256(raw).hexdigest()!=sys.argv[2]:raise SystemExit('build configuration changed')
c=json.loads(raw)
if set(c)!={'revision','packager','packager_sha256','builder_sha256','repo','source_commit','source_tree','archive','toolchain','cargo_home','output','binding_module_sha256'} or c['revision']!=1:raise SystemExit('fixed build configuration shape')
packager=pathlib.Path(c['packager'])
if sha(packager)!=c['packager_sha256'] or sha(packager.with_name('build_v2.py'))!=c['builder_sha256']:raise SystemExit('fixed selected build tooling differs')
if pathlib.Path(c['output']).exists():raise SystemExit('build output must be fresh')
argv=['/usr/bin/python3','-I','-S','-B',str(packager),'build','--repo',c['repo'],'--commit',c['source_commit'],'--output',c['output'],'--archive',c['archive'],'--toolchain-dir',c['toolchain'],'--cargo-home',c['cargo_home']]
os.execve(argv[0],argv,{'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'})
