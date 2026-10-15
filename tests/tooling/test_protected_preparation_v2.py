"""Preparation tests are private local fixtures, never protected account proof."""
import copy, hashlib, json, os, shutil, subprocess, tempfile, types, unittest
from pathlib import Path
from unittest.mock import patch, Mock
import protected_preparation_v2 as prep
import protected_behavior_v2 as behavior
import protected_release_v2 as producer
import package_v2 as package

ROOT=Path(__file__).resolve().parents[2]/'tooling'
TEST_ROOT=Path(tempfile.mkdtemp(prefix='borg-protected-preparation-tests-'))
_SOURCE=None
def source_repository():
    global _SOURCE
    if _SOURCE is None:
        _SOURCE=TEST_ROOT/'fixture-source';_SOURCE.mkdir();(_SOURCE/'src').mkdir()
        for name in ('Cargo.toml','Cargo.lock'):(_SOURCE/name).write_bytes((ROOT.parent/name).read_bytes())
        (_SOURCE/'src/main.rs').write_text('fn main() {}\n');(_SOURCE/'src/lib.rs').write_text('// controlled selected-profile source\n')
        prep.fixed_git(['init'],_SOURCE,TEST_ROOT);prep.fixed_git(['add','.'],_SOURCE,TEST_ROOT);prep.fixed_git(['commit','-m','controlled source'],_SOURCE,TEST_ROOT)
    return _SOURCE

def local(path,directory=False):
    p=Path(path)
    if p.is_symlink() or not (p.is_dir() if directory else p.is_file()):raise ValueError('fixture path')
    return p

def save(path,v):path.write_bytes(prep.canonical(v)+b'\n')

def artifact():
    repo=source_repository();commit=prep.fixed_git(['rev-parse','HEAD'],repo,TEST_ROOT);tree,fmt,_=package.immutable_tree(repo,commit)
    # These are typed test claims, never a verified package or protected Check.
    return {'format':'borg-v2-release/1','package_sha256':'1'*64,'package_bytes':10240,'manifest_sha256':'2'*64,'source_commit':commit,'source_tree':tree,'object_format':fmt,'binary_sha256':'3'*64,'binary_bytes':128,'target':package.TARGET}

def candidate(a=None,identity='release-one'):
    a=artifact() if a is None else a
    return {'id':identity,'task_id':'task-one','task_revision':3,'accepted_attempt_id':'attempt-one','checkpoint':{'registration':{'repository_id':'repo','registration_revision':'1'},'commit':a['source_commit'],'repository':'local','remote_ref':'refs/heads/published','operation_id':'checkpoint-one'},'artifact':a,'revision':1,'qualification':{'artifact':'claimed','repository_submission':'unconfigured','checks':'unconfigured','human_rollout':'unconfigured'}}

def subject(c):return {'release_id':c['id'],'artifact':c['artifact'],'repository':'repo','target_ref':'refs/heads/main','expected_base':'a'*40,'policy':'fixed-policy','deployment_target':'private-target','target_revision':'revision-one','installed_package_sha256':'1'*64}

def cargo_fixture(base):
    cache=base/'immutable-cargo';cache.mkdir()
    files={}
    for name,data in [('registry/cache/fixture/sample.crate',b'controlled archive input'),('registry/index/fixture/.cache/sample',b'controlled index input'),('git/checkouts/fixture/abc/README',b'controlled Git input')]:
        path=cache/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data);path.chmod(0o444)
        files['cargo/'+name]={'mode':'0o644','size':len(data),'sha256':prep.sha(path),'source':'/unread/historical-origin'}
    link='git/checkouts/fixture/abc/subdir/README';(cache/link).parent.mkdir();(cache/link).symlink_to('../README')
    manifest={'files':files,'symlinks':{'cargo/'+link:'../README'},'lock_sha256':package.LOCK_SHA,'manifest_sha256':package.MANIFEST_SHA,'git_revisions':[],'registry_packages':[],'scope':'Private copy fixture; not real Cargo execution.'}
    path=base/'cargo-manifest.json';save(path,manifest);path.chmod(0o444)
    return {'cargo_home':str(cache),'cargo_manifest':{'path':str(path),'sha256':prep.sha(path)}}

def recipe(base):
    mods={n:prep.sha(ROOT/n) for n in prep.MODULES}
    pp={'path':str(ROOT/'protected_preparation_v2.py'),'sha256':mods['protected_preparation_v2.py'],'profile':{'revision':2,'limits':{'path':str(ROOT.parent/'metadata/limits.json'),'sha256':prep.sha(ROOT.parent/'metadata/limits.json')},'modules':mods,'archive':{'path':'/home/dev-user/borg-runtime/.build/sqlite-amalgamation-3540000.zip','sha256':package.SQLITE_ARCHIVE_SHA},'toolchain':'/home/dev-user/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin','cargo_home':'/home/dev-user/.cargo'}}
    pp['profile'].update(cargo_fixture(base))
    steps=[]
    for name,commands,observer,oracle in [('selected_build',{'build':{'executable':str(ROOT/'fixed_build_launcher.py'),'sha256':mods['fixed_build_launcher.py'],'arguments':['{prepared_build}','{prepared_build_sha256}']}},'build_observer.py','build_oracle.py'),('actual_candidate_journey',{n:{'executable':str(ROOT/'candidate_launcher.py'),'sha256':mods['candidate_launcher.py'],'arguments':['{prepared_journey}','{prepared_journey_sha256}',n]} for n in ('controller','native1','coding1','coding2')},'journey_observer.py','journey_oracle.py')]:
        steps.append({'check':name,'commands':commands,'observer':{'path':str(ROOT/observer),'sha256':mods[observer]},'oracle':{'path':str(ROOT/oracle),'sha256':mods[oracle]},'timeout_ms':210000,'output_bytes':1024*1024})
    return {'revision':3,'supervisor_sha256':mods['protected_behavior_v2.py'],'controller_config':str(base/'controller.json'),'execution_uid':2000,'execution_gid':2000,'execution_root':str(base),'execution_cgroup':'/uninstalled/candidate','oracle_cgroup':'/uninstalled/observer','policy':'fixed-policy','deployment_target':'private-target','target_revision':'revision-one','installed_package_sha256':'1'*64,'required_checks':list(prep.CHECKS),'steps':steps,'preparation':pp}

class Preparation(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='case-',dir=TEST_ROOT));self.c=candidate();self.s=subject(self.c);self.r=recipe(self.root)
        self.work=self.root/'receipt';self.work.mkdir();self.job=self.root/'job';self.job.mkdir();self.ws=self.job/'workspace';self.ws.mkdir()
        self.e=types.SimpleNamespace(job=self.job,workspace=self.ws)
        self.binary=self.job/'binary';self.binary.write_bytes(b'controlled binary metadata fixture')
        save(Path(self.r['controller_config']),{'repository':'repo','target_ref':'refs/heads/main','repository_path':str(source_repository())})
        self.config={'recipe_sha256':'2'*64}
    def execute(self):return prep.prepare(self.r,self.config,self.s,self.c,self.work,self.e,self.binary,local)
    def test_two_distinct_candidates_without_retained_receipt_api(self):
        for identity in ('release-one','release-other'):
            c=candidate(identity=identity);prep.validate_candidate(c,subject(c))
        c=candidate();c['artifact']=copy.deepcopy(c['artifact']);c['artifact']['source_commit']='b'*40;c['checkpoint']['commit']='b'*40
        prep.validate_candidate(c,subject(c))
    def test_candidate_subject_fields_exact(self):
        for field,value in [('id','other'),('revision',2),('task_revision',0),('accepted_attempt_id','bad\nidentity')]:
            c=copy.deepcopy(self.c);c[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):prep.validate_candidate(c,self.s)
    def test_no_ordinary_positive_check_claim(self):
        self.c['qualification']['checks']='passed'
        with self.assertRaisesRegex(ValueError,'protected qualification'):prep.validate_candidate(self.c,self.s)
    def test_checkpoint_source_mismatch(self):
        self.c['checkpoint']['commit']='b'*40
        with self.assertRaisesRegex(ValueError,'checkpoint'):prep.validate_candidate(self.c,self.s)
    def test_checkpoint_registration_is_closed_and_required(self):
        for registration in (None,{}, {'repository_id':'repo'}, {'repository_id':'repo','registration_revision':'1','source_store_key':'secret'}, {'repository_id':'repo','registration_revision':''}):
            c=copy.deepcopy(self.c);c['checkpoint']['registration']=registration
            with self.subTest(registration=registration),self.assertRaisesRegex(ValueError,'registration'):prep.validate_candidate(c,self.s)
        c=copy.deepcopy(self.c);del c['checkpoint']['registration']
        with self.assertRaisesRegex(ValueError,'checkpoint'):prep.validate_candidate(c,self.s)
    def test_metadata_profile_refuses_legacy_missing_and_changed_policy(self):
        for field,value in [('revision',1),('limits',{'path':str(ROOT.parent/'metadata/limits.json'),'sha256':'0'*64})]:
            r=copy.deepcopy(self.r);r['preparation']['profile'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):prep.validate_profile(r['preparation'],r,local)
        r=copy.deepcopy(self.r);del r['preparation']['profile']['limits']
        with self.assertRaises(ValueError):prep.validate_profile(r['preparation'],r,local)
    def test_unknown_candidate_command_is_refused(self):
        self.c['command']=['sh','-c','PASS']
        with self.assertRaisesRegex(ValueError,'shape'):prep.validate_candidate(self.c,self.s)
    def test_missing_import_pin(self):
        del self.r['preparation']['profile']['modules']['candidate_binding.py']
        with self.assertRaisesRegex(ValueError,'inventory'):prep.validate_profile(self.r['preparation'],self.r,local)
    def test_changed_each_import_refused(self):
        for n in prep.MODULES:
            old=self.r['preparation']['profile']['modules'][n];self.r['preparation']['profile']['modules'][n]='0'*64
            with self.subTest(name=n),self.assertRaises(ValueError):prep.validate_profile(self.r['preparation'],self.r,local)
            self.r['preparation']['profile']['modules'][n]=old
    def test_recipe_cannot_insert_dynamic_commands(self):
        self.r['steps'][1]['commands']['controller']['arguments']=['/untrusted/other']
        with self.assertRaisesRegex(ValueError,'fixed recipe'):prep.validate_profile(self.r['preparation'],self.r,local)
    def test_recipe_cannot_skip_build(self):
        self.r['steps'].reverse()
        with self.assertRaisesRegex(ValueError,'build then'):prep.validate_profile(self.r['preparation'],self.r,local)
    def test_stale_archive_profile(self):
        self.r['preparation']['profile']['archive']['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'archive'):prep.validate_profile(self.r['preparation'],self.r,local)
    def test_actual_git_snapshot_and_generated_inputs(self):
        before=copy.deepcopy(self.r);steps,receipt=self.execute()
        self.assertEqual(self.r,before);self.assertEqual([s['check'] for s in steps],list(prep.CHECKS))
        self.assertNotIn('{prepared_',json.dumps(steps));self.assertEqual(receipt['subject_sha256'],hashlib.sha256(prep.canonical(self.s)).hexdigest())
        prep.verify_prepared(self.r,receipt,self.work,local)
        build=json.loads((self.job/'prepared/build.json').read_bytes());journey=json.loads((self.job/'prepared/journey.json').read_bytes())
        self.assertEqual(build['cargo_home'],str(self.ws/'cargo-home'))
        self.assertNotEqual(build['cargo_home'],self.r['preparation']['profile']['cargo_home'])
        projection=json.loads((self.job/'prepared/cargo-projection.json').read_bytes())
        self.assertEqual(projection['destination'],build['cargo_home'])
        self.assertIn(str(self.job/'prepared/cargo-projection.json'),receipt['prepared_inputs'])
        self.assertEqual(build['source_commit'],self.s['artifact']['source_commit']);self.assertEqual(journey['binary'],str(self.binary))
        self.assertEqual(package.immutable_tree(Path(build['repo']),build['source_commit'])[:2],(self.s['artifact']['source_tree'],'sha1'))
        self.assertEqual(journey['repository']['url'],str(self.job/'prepared/seed'))
        self.assertEqual(journey['protocol_revision'],8)
        selected=json.loads((ROOT.parent/'metadata/limits.json').read_bytes())
        self.assertEqual(journey['selected_limits_sha256'],prep.sha(ROOT.parent/'metadata/limits.json'))
        self.assertEqual(journey['task_defaults'],selected['task_defaults'])
        self.assertEqual(journey['repository']['registration'],{'repository_id':'journey-source','registration_revision':'1'})
        self.assertEqual(journey['repository_registration']['source_endpoint'],journey['repository']['url'])
        self.assertEqual(journey['repository_registration']['checkpoint_endpoint'],journey['repository']['checkpoint_remote'])
        for name,args in journey['launches'].items():
            self.assertNotIn('--model',args);self.assertNotIn('--repository-read-map',args)
            metadata_path=Path(args[args.index('--limits')+1]);actual=json.loads(metadata_path.read_bytes())
            expected=copy.deepcopy(selected);expected['worker']['coding_capacity']=0 if name=='native1' else 1;expected['worker']['native_capacity']=1 if name=='native1' else 0
            self.assertEqual(actual,expected);self.assertEqual(receipt['prepared_inputs'][str(metadata_path)],prep.sha(metadata_path))
            if name!='controller':
                self.assertEqual(int(args[args.index('--coding-capacity')+1]),actual['worker']['coding_capacity'])
                self.assertEqual(int(args[args.index('--native-capacity')+1]),actual['worker']['native_capacity'])
        self.assertEqual(journey['acceptance'][0][0],str(Path(self.r['preparation']['profile']['toolchain'])/'rustc'))
        self.assertFalse(receipt['protected_check_emitted']);self.assertFalse(receipt['human_rollout_authority'])
        self.assertFalse((Path(build['repo'])/'hooks').exists());self.assertFalse((Path(build['repo'])/'objects/info/alternates').exists())
        seed=self.job/'prepared/seed/.git/objects';remote=self.ws/'journey-remote.git/objects'
        seed_ids={(p.stat().st_dev,p.stat().st_ino) for p in seed.rglob('*') if p.is_file()}
        self.assertFalse(seed_ids & {(p.stat().st_dev,p.stat().st_ino) for p in remote.rglob('*') if p.is_file()})
        self.assertTrue(all(p.stat().st_nlink==1 and p.stat().st_uid==os.geteuid() for p in seed.rglob('*') if p.is_file()))
        # Candidate-owned publication mutation cannot rewrite the prepared seed.
        before={str(p):prep.sha(p) for p in seed.rglob('*') if p.is_file()}
        target=next(p for p in remote.rglob('*') if p.is_file());target.chmod(0o600);target.write_bytes(b'publication mutation')
        self.assertEqual(before,{str(p):prep.sha(p) for p in seed.rglob('*') if p.is_file()})
    def test_source_tree_mismatch_before_any_build(self):
        self.c['artifact']['source_tree']='0'*40
        with self.assertRaisesRegex(ValueError,'tree/format'):self.execute()
        self.assertFalse((self.ws/'selected-build').exists())
    def test_missing_source_commit(self):
        self.c['artifact']['source_commit']='0'*40;self.c['checkpoint']['commit']='0'*40
        with self.assertRaises(ValueError):self.execute()
    def test_wrong_repository_target(self):
        self.s['target_ref']='refs/heads/other'
        with self.assertRaisesRegex(ValueError,'repository target'):self.execute()
    def test_source_symlink_repository_refused(self):
        alias=self.root/'alias';alias.symlink_to(source_repository(),target_is_directory=True)
        save(Path(self.r['controller_config']),{'repository':'repo','target_ref':'refs/heads/main','repository_path':str(alias)})
        # Explicit repository-root alias check is part of source admission.
        with self.assertRaises(ValueError):self.execute()
    def test_prepared_input_change_refused(self):
        _,receipt=self.execute();p=self.job/'prepared/journey.json';p.chmod(0o600);p.write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError,'prepared input changed'):prep.verify_prepared(self.r,receipt,self.work,local)
    def test_prepared_extra_file_refused(self):
        _,receipt=self.execute();root=self.job/'prepared';root.chmod(0o700);(root/'extra').write_text('extra')
        with self.assertRaisesRegex(ValueError,'membership'):prep.verify_prepared(self.r,receipt,self.work,local)
    def test_prepared_extra_directory_alias_refused(self):
        _,receipt=self.execute();root=self.job/'prepared';root.chmod(0o700);(root/'alias').symlink_to(self.ws,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'alias appeared'):prep.verify_prepared(self.r,receipt,self.work,local)
    def test_selected_source_reader_protection_is_required(self):
        seen=[]
        def guard(path,directory=False):
            seen.append(str(path))
            if str(path)=='/usr/bin/git':raise ValueError('reader has writable ancestor')
            return local(path,directory)
        with self.assertRaisesRegex(ValueError,'writable ancestor'):prep.load_package(ROOT,self.r['preparation']['profile'],guard)
        self.assertIn('/usr/bin/git',seen)
    def test_prepared_source_receipt_change_refused(self):
        _,receipt=self.execute();(self.work/'behavior-source-manifest.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'source receipt'):prep.verify_prepared(self.r,receipt,self.work,local)

class CargoProjection(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='cargo-projection-',dir=TEST_ROOT));self.profile=cargo_fixture(self.root)
        self.base=Path(self.profile['cargo_home']);self.manifest=Path(self.profile['cargo_manifest']['path'])
        self.inventory={name:{'sha256':value} for name,value in [('Cargo.lock',package.LOCK_SHA),('Cargo.toml',package.MANIFEST_SHA)]}
        self.recipe={'execution_uid':os.geteuid(),'execution_gid':os.getegid()}
    def execution(self,name='job'):
        job=self.root/name;job.mkdir();workspace=job/'workspace';workspace.mkdir()
        return types.SimpleNamespace(job=job,workspace=workspace)
    def project(self,e=None):
        return prep.project_cargo_cache(self.profile,self.inventory,e or self.execution(),self.recipe,local)
    def manifest_change(self,change):
        value=json.loads(self.manifest.read_bytes());change(value);self.manifest.chmod(0o600);save(self.manifest,value);self.manifest.chmod(0o444)
        self.profile['cargo_manifest']['sha256']=prep.sha(self.manifest)
    def test_exact_copy_is_separate_owned_and_mutable_without_base_change(self):
        e=self.execution();before={str(p):prep.sha(p) for p in self.base.rglob('*') if p.is_file() and not p.is_symlink()}
        result=self.project(e);cache=Path(result['destination'])
        self.assertEqual(cache,e.workspace/'cargo-home');self.assertEqual(result['files'],3);self.assertEqual(result['symlinks'],1)
        self.assertEqual(result['bytes'],sum(p.stat().st_size for p in self.base.rglob('*') if p.is_file() and not p.is_symlink()))
        self.assertEqual(result['entry_bound'],len(list(cache.rglob('*'))))
        for p in self.base.rglob('*'):
            copied=cache/p.relative_to(self.base)
            self.assertEqual(copied.lstat().st_uid,self.recipe['execution_uid'])
            if p.is_symlink():self.assertEqual(os.readlink(copied),os.readlink(p))
            elif p.is_file():
                self.assertEqual(prep.sha(copied),prep.sha(p));self.assertEqual(copied.stat().st_nlink,1)
                self.assertNotEqual((copied.stat().st_dev,copied.stat().st_ino),(p.stat().st_dev,p.stat().st_ino))
        (cache/'registry/cache/fixture/sample.crate').write_bytes(b'job-local mutation')
        (cache/'.package-cache').touch();(cache/'.global-cache').write_bytes(b'disposable bookkeeping')
        self.assertEqual(before,{str(p):prep.sha(p) for p in self.base.rglob('*') if p.is_file() and not p.is_symlink()})
        prep.verify_cargo_base(self.profile,local)
    def test_two_jobs_never_share_copies_or_reuse_destination(self):
        a=self.execution('a');b=self.execution('b');ra=self.project(a);rb=self.project(b)
        name='registry/cache/fixture/sample.crate';pa=Path(ra['destination'])/name;pb=Path(rb['destination'])/name
        pa.write_bytes(b'first job only');self.assertNotEqual(pa.read_bytes(),pb.read_bytes())
        with self.assertRaises(FileExistsError):self.project(a)
    def test_missing_and_extra_base_entries_refused_before_projection(self):
        target=self.base/'registry/cache/fixture/sample.crate';data=target.read_bytes();target.unlink();e=self.execution()
        with self.assertRaisesRegex(ValueError,'missing'):self.project(e)
        self.assertFalse((e.workspace/'cargo-home').exists())
        target.write_bytes(data);target.chmod(0o444);(self.base/'credentials.toml').write_text('not selected')
        with self.assertRaisesRegex(ValueError,'membership'):self.project(e)
    def test_hash_size_mode_and_hardlink_tampering_refused(self):
        target=self.base/'registry/cache/fixture/sample.crate';original=target.read_bytes()
        target.chmod(0o644)
        with self.assertRaisesRegex(ValueError,'mode'):prep.verify_cargo_base(self.profile,local)
        target.write_bytes(b'x'*len(original));target.chmod(0o444)
        with self.assertRaisesRegex(ValueError,'changed'):prep.verify_cargo_base(self.profile,local)
        target.chmod(0o644);target.write_bytes(original+b'x');target.chmod(0o444)
        with self.assertRaisesRegex(ValueError,'size'):prep.verify_cargo_base(self.profile,local)
        target.chmod(0o644);target.write_bytes(original);target.chmod(0o444);os.link(target,self.root/'outside-hardlink')
        with self.assertRaisesRegex(ValueError,'link'):prep.verify_cargo_base(self.profile,local)
    def test_unselected_root_config_refused(self):
        self.manifest_change(lambda m:m['files'].update({'cargo/config.toml':next(iter(m['files'].values()))}))
        with self.assertRaisesRegex(ValueError,'unselected Cargo input'):prep.cargo_manifest(self.profile,local)
    def test_changed_pin_or_selected_lock_refuses_before_copy(self):
        self.profile['cargo_manifest']['sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'manifest changed'):self.project()
        self.profile['cargo_manifest']['sha256']=prep.sha(self.manifest);self.inventory['Cargo.lock']['sha256']='f'*64
        e=self.execution('second')
        with self.assertRaisesRegex(ValueError,'build profile'):self.project(e)
        self.assertFalse((e.workspace/'cargo-home').exists())
    def test_symlink_target_and_parent_alias_refused(self):
        link=self.base/'git/checkouts/fixture/abc/subdir/README';link.unlink();link.symlink_to('/etc/passwd')
        with self.assertRaisesRegex(ValueError,'link changed'):prep.verify_cargo_base(self.profile,local)
        link.unlink();link.symlink_to('../README')
        registry=self.base/'registry';registry.rename(self.root/'outside-registry');registry.symlink_to(self.root/'outside-registry',target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'directory alias'):prep.verify_cargo_base(self.profile,local)
    def test_manifest_relative_escape_refused(self):
        self.manifest_change(lambda m:m['symlinks'].update({'cargo/git/checkouts/fixture/abc/subdir/README':'../../../../../escape'}))
        with self.assertRaisesRegex(ValueError,'escape'):prep.cargo_manifest(self.profile,local)
    def test_invalid_manifest_byte_bounds_refused(self):
        for value in (-1,True,'12'):
            self.manifest_change(lambda m:m['files'][next(iter(m['files']))].update({'size':value}))
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'file fields'):prep.cargo_manifest(self.profile,local)
    def test_per_entry_protection_and_post_copy_base_verification(self):
        target=self.base/'registry/cache/fixture/sample.crate';seen=[]
        def guard(path,directory=False):
            seen.append(str(path))
            if Path(path)==target:raise ValueError('protected base file denied')
            return local(path,directory)
        with self.assertRaisesRegex(ValueError,'protected base file denied'):prep.verify_cargo_base(self.profile,guard)
        self.assertIn(str(target),seen)
        original=prep.cargo_file;copied=[]
        def changed_after_copy(path,entry,protected,destination=None):
            original(path,entry,protected,destination)
            if destination is not None:
                copied.append(str(destination))
                if Path(path)==target:target.chmod(0o644);target.write_bytes(b'x'*entry['size']);target.chmod(0o444)
        e=self.execution()
        with patch.object(prep,'cargo_file',side_effect=changed_after_copy),self.assertRaisesRegex(ValueError,'changed'):self.project(e)
        self.assertTrue(copied);self.assertTrue((e.workspace/'cargo-home').is_dir())
        # Failed partial output stays inside this job; it is never reused/deleted
        # through an untrusted path or labeled a successful prepared cache.
    def test_projection_workspace_alias_is_refused(self):
        e=self.execution();e.workspace.rmdir();e.workspace.symlink_to(self.base,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'workspace identity'):self.project(e)


class RecipeAndSourceAdmission(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='admission-',dir=TEST_ROOT))
        self.c=candidate();self.s=subject(self.c);self.r=recipe(self.root)
        save(self.root/'recipe.json',self.r)
        self.config={'socket':'/fixed/socket','producer_uid':os.geteuid(),'server_uid':3000,'policy':'fixed-policy','recipe_sha256':prep.sha(self.root/'recipe.json'),'valid_for_ms':1000}
        self.cc={**{k:self.s[k] for k in ('policy','repository','target_ref','deployment_target','target_revision','installed_package_sha256')},'repository_path':str(source_repository()),'socket':self.config['socket'],'recipe_sha256':self.config['recipe_sha256'],'required_checks':['package_integrity',*prep.CHECKS],'max_check_age_ms':1000,'worker_uids':[2000],'producers':[{'id':'checker','uid':os.geteuid(),'role':'checker'},{'id':'human','uid':2001,'role':'human'},{'id':'repository','uid':2002,'role':'repository'},{'id':'rollout','uid':2003,'role':'rollout'}]}
        save(self.root/'controller.json',self.cc)
    def test_fixed_recipe3_loads_without_candidate_specific_recipe_hash(self):
        with patch.object(behavior,'protected',local):
            first=behavior.load_recipe(self.root/'recipe.json',self.config,self.s)
            later=copy.deepcopy(self.s);later['release_id']='second';later['artifact']['source_commit']='b'*40
            second=behavior.load_recipe(self.root/'recipe.json',self.config,later)
        self.assertEqual(first,second)
    def test_all_protected_target_fields_refuse_mismatch(self):
        for k in ('policy','repository','target_ref','deployment_target','target_revision','installed_package_sha256'):
            s=copy.deepcopy(self.s);s[k]='other'
            with self.subTest(field=k),patch.object(behavior,'protected',local),self.assertRaises(ValueError):behavior.load_recipe(self.root/'recipe.json',self.config,s)
    def test_missing_actual_candidate_refused_before_package_or_execution(self):
        work=self.root/'work';work.mkdir()
        with patch.object(behavior,'protected',local),patch.object(producer,'package_check') as verify:
            with self.assertRaisesRegex(ValueError,'exact ordinary candidate'):behavior.behavior_check(self.config,self.s,'/missing',self.root/'recipe.json',work,'operation')
            verify.assert_not_called()
    def test_selected_checks_cannot_be_removed_by_preparation(self):
        self.cc['required_checks'].append('another_required_case');save(self.root/'controller.json',self.cc)
        with patch.object(behavior,'protected',local),self.assertRaisesRegex(ValueError,'mandatory checks'):behavior.load_recipe(self.root/'recipe.json',self.config,self.s)
    def test_real_distinct_commits_snapshot_and_tampered_profile(self):
        repo=self.root/'repo';repo.mkdir();(repo/'src').mkdir()
        for name in ('Cargo.toml','Cargo.lock'):(repo/name).write_bytes((ROOT.parent/name).read_bytes())
        (repo/'src/main.rs').write_text('fn main() {}\n');(repo/'src/lib.rs').write_text('// candidate one\n')
        prep.fixed_git(['init'],repo,self.root)
        commits=[]
        for label in ('one','two'):
            (repo/'src/lib.rs').write_text('// candidate '+label+'\n');prep.fixed_git(['add','.'],repo,self.root);prep.fixed_git(['commit','-m',label],repo,self.root)
            commit=prep.fixed_git(['rev-parse','HEAD'],repo,self.root);tree,fmt,_=package.immutable_tree(repo,commit)
            a={**artifact(),'source_commit':commit,'source_tree':tree,'object_format':fmt};out=self.root/('snapshot-'+label)
            inventory=prep.snapshot_repository(package,repo,a,out);commits.append(commit)
            self.assertEqual(inventory['src/lib.rs']['sha256'],hashlib.sha256(('// candidate '+label+'\n').encode()).hexdigest())
        self.assertNotEqual(*commits)
        (repo/'Cargo.toml').write_text('[package]\nname="unselected"\n');prep.fixed_git(['add','.'],repo,self.root);prep.fixed_git(['commit','-m','unselected'],repo,self.root)
        commit=prep.fixed_git(['rev-parse','HEAD'],repo,self.root);tree,fmt,_=package.immutable_tree(repo,commit)
        with self.assertRaisesRegex(ValueError,'build profile'):prep.snapshot_repository(package,repo,{**artifact(),'source_commit':commit,'source_tree':tree,'object_format':fmt},self.root/'bad-profile')


class ProducerJoin(unittest.TestCase):
    def args(self,root,body):
        c={'socket':'/uninstalled/socket','server_uid':0,'producer_uid':os.geteuid(),'policy':'policy','recipe_sha256':'0'*64,'valid_for_ms':1000}
        save(root/'config',c);(root/'config').chmod(0o600);save(root/'input',body)
        return types.SimpleNamespace(mode='check-prepared',config=str(root/'config'),input=str(root/'input'),package='/candidate/package',recipe='/fixed/recipe',receipt=str(root/'receipt'),operation_id='one',original_receipt=None)
    def run_fixture(self,body,side_effect=None):
        root=Path(tempfile.mkdtemp(prefix='producer-',dir=TEST_ROOT));args=self.args(root,body)
        supervisor=Mock();supervisor.behavior_check.side_effect=side_effect
        supervisor.behavior_check.return_value={'subject':body.get('subject'),'cleanup_verified':True,'results':{'selected_build':True}}
        ex=Mock(return_value={'status':'ok'})
        with patch.object(producer,'load_behavior',return_value=supervisor),patch.object(producer,'exchange',ex):
            if side_effect:
                with self.assertRaises(ValueError):producer.run(args)
            else:self.assertEqual(producer.run(args),0)
        return root,supervisor,ex
    def test_producer_forwards_exact_ordinary_candidate_and_subject(self):
        c=candidate();body={'candidate':c,'subject':subject(c)}
        root,supervisor,exchange=self.run_fixture(body)
        args=supervisor.behavior_check.call_args.args
        self.assertEqual(args[1],body['subject']);self.assertEqual(args[-1],c)
        req=json.loads((root/'receipt/request.json').read_bytes())
        self.assertEqual(req['command']['operation'],'check');self.assertEqual(req['command']['input']['subject'],body['subject'])
        self.assertEqual(exchange.call_count,1)
    def test_cleanup_failure_never_reaches_protected_socket(self):
        c=candidate();root,supervisor,exchange=self.run_fixture({'candidate':c,'subject':subject(c)},ValueError('cleanup unknown'))
        self.assertEqual(exchange.call_count,0);self.assertFalse((root/'receipt/request.json').exists())
    def test_malformed_candidate_input_refuses_before_execution(self):
        root=Path(tempfile.mkdtemp(prefix='producer-',dir=TEST_ROOT));args=self.args(root,{'subject':subject(candidate()),'candidate':candidate(),'command':'grant'})
        supervisor=Mock()
        with patch.object(producer,'load_behavior',return_value=supervisor),patch.object(producer,'exchange') as exchange:
            with self.assertRaisesRegex(ValueError,'subject and ordinary'):producer.run(args)
            supervisor.behavior_check.assert_not_called();exchange.assert_not_called()

def tearDownModule():
    for p in sorted(TEST_ROOT.rglob('*'),reverse=True):
        if not p.is_symlink():
            try:p.chmod(0o700 if p.is_dir() else 0o600)
            except FileNotFoundError:pass
    shutil.rmtree(TEST_ROOT)

if __name__=='__main__':unittest.main()
