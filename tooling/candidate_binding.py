"""Fixed source/build/package association. Historical build is not a protected Check."""
from pathlib import Path
import hashlib, json, os, stat, tarfile
import package_v2

def sha(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def canonical(obj): return json.dumps(obj,sort_keys=True,separators=(',',':')).encode()

def bind(root, destination):
    """Snapshot once, then verify, extract and measure only the private snapshot."""
    root=Path(root); destination=Path(destination); destination.mkdir(mode=0o700)
    completion=json.loads((root/'completion.json').read_bytes())
    receipt=json.loads((root/'package/receipt.json').read_bytes())
    invocation=json.loads((root/'package/build-invocation.json').read_bytes())
    if completion['build_exit']!=0 or completion['verify_exit']!=0 or receipt['status']!='passed' or invocation['exit']!=0: raise ValueError('build did not pass')
    for rel,h in completion['evidence'].items():
        if sha(root/rel)!=h: raise ValueError('historical build input differs: '+rel)
    package=root/'package/borg-runtime.tar'; fd=os.open(package,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f, (destination/'package.tar').open('xb') as out:
        info=os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size!=completion['package']['bytes'] or info.st_size>256*1024*1024: raise ValueError('package input bound')
        total=0
        for chunk in iter(lambda:f.read(1024*1024),b''):
            total+=len(chunk)
            if total>info.st_size: raise ValueError('package changed during snapshot')
            out.write(chunk)
        if total!=info.st_size: raise ValueError('package truncated')
        out.flush();os.fsync(out.fileno())
    snapshot=destination/'package.tar'
    m=package_v2.verify(snapshot,completion['package']['sha256'],completion['source_commit'])
    if sha(root/'package/manifest.json')!=completion['manifest_sha256'] or m!=json.loads((root/'package/manifest.json').read_bytes()): raise ValueError('manifest mismatch')
    for name,item in m['source']['files'].items():
        if sha(root/'package/source'/name)!=item['sha256']: raise ValueError('build source differs: '+name)
    if (receipt['source_tree']!=m['source']['tree'] or receipt['binary']!=m['files']['bin/borg-runtime'] or receipt['package_sha256']!=completion['package']['sha256']): raise ValueError('build receipt subject differs')
    expected=['/usr/bin/python3','-I','-S','-B',str(root/'package/recipe/build_v2.py'),'--root',str(root/'package/source'),'--build-dir',str(root/'package/sqlite'),'--archive',receipt['archive'],'--toolchain-dir',receipt['toolchain'],'--offline','--cargo','build','--release']
    if invocation['command']!=expected or invocation['environment']['CARGO_NET_OFFLINE']!='true': raise ValueError('fixed build command differs')
    if sha(root/'package/recipe/build_v2.py')!=m['profile']['build_helper_sha256']: raise ValueError('builder changed')
    binary=destination/'borg-runtime'
    with tarfile.open(snapshot,'r:') as tf, binary.open('xb') as f: f.write(tf.extractfile('bin/borg-runtime').read())
    binary.chmod(0o555)
    if sha(binary)!=m['files']['bin/borg-runtime']['sha256']: raise ValueError('extracted binary differs')
    result={'revision':1,'package_sha256':sha(snapshot),'manifest_sha256':completion['manifest_sha256'],'source_commit':m['source']['commit'],'source_tree':m['source']['tree'],'binary_sha256':sha(binary),'source_files_verified':len(m['source']['files']),'build_recipe_sha256':m['profile']['build_helper_sha256'],'build_invocation_sha256':sha(root/'package/build-invocation.json'),'build_receipt_sha256':sha(root/'package/receipt.json'),'existing_build_exit':0,'new_build_executed':False,'protected_build_qualification':False,'binary':str(binary),'rustc':str(Path(receipt['toolchain'])/'rustc'),'rustc_sha256':m['build']['tools']['sha256']['rustc']}
    if sha(result['rustc'])!=result['rustc_sha256']: raise ValueError('selected compiler differs')
    (destination/'binding.json').write_bytes(canonical(result)+b'\n')
    return result

def supervised_build_adjudication(context, evidence, binding):
    """For the fixed build stage: exit alone cannot attest a candidate build.

    The caller supplies the supervisor-owned transcript after whole-group cleanup.
    This is separate from bind(), which preserves an existing ordinary build.
    """
    if not context.get('cleanup_verified') or not evidence.get('cleanup_verified'): raise ValueError('build cleanup unproven')
    if evidence.get('subject_sha256')!=context['subject_sha256'] or evidence.get('recipe_sha256')!=context['recipe_sha256']: raise ValueError('build subject differs')
    launches=[x['value'] for x in evidence['supervisor_transcript'] if x['kind']=='supervisor_launch']
    if len(launches)!=1 or launches[0]['command']!='build': raise ValueError('fixed build not observed')
    if launches[0].get('argv')!=context['fixed_build_argv'] or launches[0].get('executable_sha256')!=context['fixed_build_executable_sha256']: raise ValueError('unreviewed build command')
    exits=[x['value']['result'] for x in evidence['supervisor_transcript'] if x['kind']=='supervisor_response' and 'exit' in x['value']['result']]
    if not exits or exits[-1].get('exit')!=0 or not exits[-1].get('leader_reaped'): raise ValueError('build exit not observed')
    artifact=context['subject']['artifact']
    for field in ('package_sha256','manifest_sha256','source_commit','source_tree','binary_sha256'):
        if artifact[field]!=binding[field]: raise ValueError('built artifact differs: '+field)
    return True
