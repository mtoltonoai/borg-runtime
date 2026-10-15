"""Read only a fixed selected build's output after measured whole-group cleanup."""
import hashlib,importlib.util,json,pathlib,sys
def sha(p):
    h=hashlib.sha256()
    with pathlib.Path(p).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()
def load(name,path,expected):
    if sha(path)!=expected:raise ValueError('fixed oracle dependency changed')
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m
c=json.loads(pathlib.Path(sys.argv[1]).read_bytes());work=pathlib.Path(sys.argv[1]).parent
e=pathlib.Path(c['live_evidence_path'])
if sha(e)!=c['live_evidence_sha256']:raise ValueError('build evidence changed')
evidence=json.loads(e.read_bytes());live=json.loads((work/(c['check']+'.live-input.json')).read_bytes())
if any(live[k]!=c[k] for k in ('subject_sha256','recipe_sha256','check')) or set(live['commands'])!={'build'}:raise ValueError('build input differs')
command=live['commands']['build'];path,expected=command['arguments'];raw=pathlib.Path(path).read_bytes()
if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('build configuration changed')
config=json.loads(raw)
packager=load('package_v2',pathlib.Path(config['packager']),config['packager_sha256'])
binding=load('candidate_binding',pathlib.Path(__file__).with_name('candidate_binding.py'),config['binding_module_sha256'])
package=pathlib.Path(config['output'])/'borg-runtime.tar';artifact=c['subject']['artifact']
# Whole-group cleanup precedes every read; the fixed verifier rehashes all bytes.
if not c['cleanup_verified'] or not evidence['cleanup_verified']:raise ValueError('build cleanup absent')
manifest=packager.verify(package,artifact['package_sha256'],artifact['source_commit'])
built={'format':manifest['format'],'package_sha256':sha(package),'package_bytes':package.stat().st_size,'manifest_sha256':hashlib.sha256(packager.canonical(manifest)).hexdigest(),'source_commit':manifest['source']['commit'],'source_tree':manifest['source']['tree'],'object_format':manifest['source']['object_format'],'binary_sha256':manifest['files']['bin/borg-runtime']['sha256'],'binary_bytes':manifest['files']['bin/borg-runtime']['size'],'target':manifest['files']['bin/borg-runtime']['target']}
if built!=artifact:raise ValueError('complete rebuilt artifact differs from exact release subject')
c['fixed_build_argv']=[command['executable'],*command['arguments']];c['fixed_build_executable_sha256']=command['sha256']
binding.supervised_build_adjudication(c,evidence,built)
if built['source_commit']!=config['source_commit'] or built['source_tree']!=config['source_tree']:raise ValueError('build source target changed')
print(json.dumps({'revision':1,'subject_sha256':c['subject_sha256'],'recipe_sha256':c['recipe_sha256'],'check':c['check'],'passed':True,'observations':['Fixed selected supervised build and exact source/package/binary binding after cleanup; no rollout authority']}))
