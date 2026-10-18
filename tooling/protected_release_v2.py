#!/usr/bin/env python3
"""Restricted local release producer, without deployment or automatic rollout.

Package checking never executes candidate code. Behavioral checking uses a pinned
protected supervisor, separately configured execution identity and fixed oracles.
Missing mandatory results are never assumed and candidate output is not evidence.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, select, socket, stat, struct, sys, time
from pathlib import Path
MAX=128*1024
VERIFIER_SHA="512296740c56d216f8dd2dbe61a05c3aef3749f7aba2630c4ab916ffec02a05b"
BEHAVIOR_SHA='755590b533ea2591d17d770bb50f9a8c0b0d499598d39cbae6511247f78ee03b'
def canonical(v):return json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()
def strict(data):
    def pairs(xs):
        d={}
        for k,v in xs:
            if k in d:raise ValueError("duplicate JSON field")
            d[k]=v
        return d
    return json.loads(data,object_pairs_hook=pairs,parse_constant=lambda v:(_ for _ in ()).throw(ValueError("nonfinite JSON")))
def protected_file(path):
    p=Path(path).absolute()
    for q in (p,*p.parents):
        m=q.lstat()
        if stat.S_ISLNK(m.st_mode):raise ValueError("aliased protected input")
    m=p.stat()
    if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_uid not in {0,os.geteuid()} or m.st_mode&0o022:raise ValueError("protected input owner/mode mismatch")
    return p
def load_config(path):
    p=protected_file(path);fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        m=os.fstat(f.fileno())
        if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_size>MAX:raise ValueError('configuration type/bound')
        raw=f.read(MAX+1)
    if len(raw)>MAX:raise ValueError("configuration bound")
    return decode_config(raw)
def decode_config(raw):
    c=strict(raw)
    if set(c)!={"socket","server_uid","producer_uid","policy","recipe_sha256","valid_for_ms"}:raise ValueError("configuration fields")
    if (type(c['producer_uid']) is not int or c['producer_uid']!=os.geteuid()
        or type(c['server_uid']) is not int or not 0<=c['server_uid']<2**32
        or type(c['valid_for_ms']) is not int or not 0<c['valid_for_ms']<=86400000):raise ValueError("producer identity/interval")
    return c
def exchange(c,request,*,max_bytes=MAX,timeout_ms=65000):
    data=canonical(request)
    if type(max_bytes) is not int or max_bytes<=0 or type(timeout_ms) is not int or timeout_ms<=0:raise ValueError("transport metadata bounds")
    if len(data)>max_bytes:raise ValueError("request bound")
    end=time.monotonic()+timeout_ms/1000
    def remaining():
        left=end-time.monotonic()
        if left<=0:raise TimeoutError('protected exchange absolute deadline; reconcile original identity')
        return left
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as peer:
        peer.settimeout(remaining());peer.connect(c['socket'])
        _,uid,_=struct.unpack('3i',peer.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
        if uid!=c['server_uid']:raise ValueError("wrong kernel controller identity")
        peer.settimeout(remaining());peer.sendall(struct.pack('!I',len(data))+data)
        def read(n):
            out=bytearray()
            while len(out)<n:
                peer.settimeout(remaining());b=peer.recv(n-len(out))
                if not b:raise ValueError("reply lost; reconcile original identity")
                out.extend(b)
            return bytes(out)
        n=struct.unpack('!I',read(4))[0]
        if n>max_bytes:raise ValueError("reply bound")
        return strict(read(n))
OPERATOR_COMMANDS={'grant','prepare','execute_merge','reconcile_merge','start_rollout','observe_rollout','get_action','get_receipt','view'}
def stream_bytes(fd,bound,end,data=None):
    """One bounded stdin/stdout exchange under the original absolute deadline."""
    original=os.get_blocking(fd);os.set_blocking(fd,False);out=bytearray();offset=0
    try:
        while data is None or offset<len(data):
            left=end-time.monotonic()
            if left<=0:raise TimeoutError('producer stdio deadline; reconcile original identity')
            ready=select.select([fd] if data is None else [],[] if data is None else [fd],[],left)
            if not any(ready):raise TimeoutError('producer stdio deadline; reconcile original identity')
            try:
                if data is None:
                    block=os.read(fd,min(4096,bound+1-len(out)))
                    if not block:return bytes(out)
                    out.extend(block)
                    if len(out)>bound:raise ValueError('producer stdio request bound')
                else:
                    written=os.write(fd,data[offset:offset+4096])
                    if written<=0:raise ValueError('producer stdio reply lost; reconcile original identity')
                    offset+=written
            except BlockingIOError:continue
        return None
    finally:os.set_blocking(fd,original)
def exchange_stdio(args,input_fd=0,output_fd=1):
    if type(args.max_bytes) is not int or args.max_bytes<=0 or type(args.timeout_ms) is not int or args.timeout_ms<=0:raise ValueError('producer stdio bounds')
    end=time.monotonic()+args.timeout_ms/1000
    module=protected_file(Path(__file__).absolute())
    if hashlib.sha256(module.read_bytes()).hexdigest()!=args.module_sha256:raise ValueError('remote producer module differs')
    if hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest()!=args.python_sha256:raise ValueError('remote Python differs')
    with protected_file(args.config).open('rb') as source:raw=source.read(MAX+1)
    if len(raw)>MAX or hashlib.sha256(raw).hexdigest()!=args.config_sha256:raise ValueError('remote producer configuration differs')
    config=decode_config(raw)
    request=strict(stream_bytes(input_fd,args.max_bytes,end))
    if not isinstance(request,dict) or set(request)!={'revision','operation_id','command'} or request['revision']!=1 or not isinstance(request['operation_id'],str) or not request['operation_id']:raise ValueError('producer stdio request schema')
    command=request['command']
    if not isinstance(command,dict) or command.get('operation') not in OPERATOR_COMMANDS:raise ValueError('unsupported producer command; checks are produced by the fixed checker')
    remaining=int((end-time.monotonic())*1000)
    if remaining<=0:raise TimeoutError('producer stdio deadline; reconcile original identity')
    reply=exchange(config,request,max_bytes=args.max_bytes,timeout_ms=remaining)
    raw=canonical(reply)
    if len(raw)>args.max_bytes:raise ValueError('producer stdio reply bound')
    stream_bytes(output_fd,args.max_bytes,end,raw)
    return 0

def private_create(path):
    return os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb')
def sync_directory(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)
def load_verifier():
    path=protected_file(Path(__file__).with_name('package_v2.py'))
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=VERIFIER_SHA:raise ValueError('installed verifier identity changed')
    mod=importlib.util.module_from_spec(importlib.util.spec_from_file_location('protected_package_verifier',path));exec(compile(raw,str(path),'exec'),mod.__dict__)
    return mod
def load_behavior():
    path=protected_file(Path(__file__).with_name('protected_behavior_v2.py'))
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BEHAVIOR_SHA:raise ValueError('installed behavior supervisor identity changed')
    # Isolated Python startup deliberately omits the script directory. Bind only
    # this already-running producer module, not an ambient import search path.
    sys.modules.setdefault('protected_release_v2',sys.modules[__name__])
    mod=importlib.util.module_from_spec(importlib.util.spec_from_file_location('protected_behavior_supervisor',path));exec(compile(raw,str(path),'exec'),mod.__dict__)
    return mod
def package_check(c,subject,package,work):
    mod=load_verifier();a=subject['artifact'];snapshot=work/'package.snapshot';cap=mod.MAX_BINARY+mod.MAX_MANIFEST+20480
    # The untrusted caller path is opened once. Verification and descriptor
    # derivation operate only on this private immutable checker copy.
    try:
        fd=os.open(package,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(fd,'rb') as source:
            info=os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size>cap:raise ValueError('package input type/bound')
            total=0;digest=hashlib.sha256()
            with private_create(snapshot) as retained:
                while True:
                    block=source.read(min(1024*1024,cap-total+1))
                    if not block:break
                    total+=len(block)
                    if total>cap:raise ValueError('package input exceeds copy bound')
                    retained.write(block);digest.update(block)
                retained.flush();os.fsync(retained.fileno())
        snapshot.chmod(0o400);sync_directory(work)
        if digest.hexdigest()!=a['package_sha256']:raise ValueError('copied package differs from exact subject')
        m=mod.verify(snapshot,a['package_sha256'],a['source_commit'])
        actual={'format':m['format'],'package_sha256':a['package_sha256'],'package_bytes':total,
                'manifest_sha256':hashlib.sha256(mod.canonical(m)).hexdigest(),'source_commit':m['source']['commit'],'source_tree':m['source']['tree'],
                'object_format':m['source']['object_format'],'binary_sha256':m['files']['bin/borg-runtime']['sha256'],'binary_bytes':m['files']['bin/borg-runtime']['size'],'target':m['files']['bin/borg-runtime']['target']}
        if actual!=a or subject['policy']!=c['policy']:raise ValueError('exact artifact/policy mismatch')
    finally:
        if snapshot.exists():snapshot.unlink()
        sync_directory(work)
    # No candidate executable or compiler/test child was started. Cleanup covers
    # this fixed in-process verification and its private copied input only.
    now=time.time_ns()//1_000_000
    return {'subject':subject,'recipe_sha256':c['recipe_sha256'],'checked_at_ms':now,'valid_until_ms':now+c['valid_for_ms'],
            'results':{'package_integrity':True},'cleanup_verified':True}

BINDING_FORMAT='borg-protected-check-binding/1'
def bound_original(original):
    """Reconciliation preserves the original request even after its Check expires.

    No execution member is refreshed here. A new-format sidecar is mandatory
    when new-format evidence/attestation exists; absence is never a legacy adapter.
    """
    original=Path(original)
    p=protected_file(original/'request.json')
    fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        if os.fstat(f.fileno()).st_size>MAX:raise ValueError('original request bound')
        raw=f.read(MAX+1)
    if len(raw)>MAX:raise ValueError('original request bound')
    prior=strict(raw)
    if not isinstance(prior,dict) or type(prior.get('revision')) is not int or prior['revision']!=1 or set(prior)!={'revision','operation_id','command'}:
        raise ValueError('original request schema')
    sidecar=original/'evidence-binding.json'
    if not sidecar.exists() and not sidecar.is_symlink():
        if any((original/n).exists() or (original/n).is_symlink() for n in ('execution-evidence.json','applicability.json')):
            raise ValueError('incomplete original evidence binding; no legacy promotion')
        return prior,None
    helper=load_behavior();helper.protected(original,directory=True)
    if original.stat().st_mode&0o077:raise ValueError('original request/sidecars must remain private')
    binding,_=helper.read_json(helper.protected(sidecar))
    helper.fields(binding,'format revision operation_id request_sha256 evidence attestation producer','original evidence binding')
    if binding['format']!=BINDING_FORMAT or type(binding['revision']) is not int or binding['revision']!=1:
        raise ValueError('unknown private evidence binding version')
    if any(helper.member(original/name)['mode']&0o077 for name in ('request.json','applicability.json','evidence-binding.json')):
        raise ValueError('original request/sidecars must remain private')
    if binding['operation_id']!=prior['operation_id'] or binding['request_sha256']!=hashlib.sha256(raw).hexdigest() or raw!=canonical(prior):
        raise ValueError('original request digest/operation binding differs')
    for name in ('evidence','attestation'):
        helper.fields(binding[name],'path sha256'+(' operation_id' if name=='evidence' else ''),'original '+name+' reference')
        sha=binding[name]['sha256'];path=binding[name]['path']
        if not isinstance(sha,str) or len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):raise ValueError('original reference digest')
        if not isinstance(path,str) or not Path(path).is_absolute() or '..' in Path(path).parts:raise ValueError('original reference path')
    ap=original/'applicability.json'
    if binding['attestation']['path']!=str(ap) or helper.member(ap)['sha256']!=binding['attestation']['sha256']:
        raise ValueError('original applicability bytes differ')
    attestation,_=helper.read_json(helper.protected(ap),helper.MAX_EVIDENCE)
    helper.fields(attestation,'format revision operation_id evidence producer executed_at_ms attested_at_ms clock_uncertainty_ms check current','original applicability')
    if (attestation['format']!=helper.ATTESTATION_FORMAT or type(attestation['revision']) is not int or attestation['revision']!=1
        or attestation['operation_id']!=prior['operation_id'] or not helper.exact(attestation['evidence'],binding['evidence'])
        or not helper.exact(attestation['producer'],binding['producer']) or not helper.exact(prior['command'],{'operation':'check','input':attestation['check']})):
        raise ValueError('original applicability/request association differs')
    helper.fields(binding['producer'],'id uid role','original checker identity')
    if binding['producer']['role']!='checker':raise ValueError('original producer is not a checker')
    check=attestation['check']
    helper.fields(check,'subject recipe_sha256 checked_at_ms valid_until_ms results cleanup_verified','original Check')
    if (not helper.integer(attestation['executed_at_ms'],1,2**63-1)
        or not helper.integer(attestation['attested_at_ms'],attestation['executed_at_ms'],2**63-1)
        or not helper.integer(check['checked_at_ms'],1,2**63-1)
        or not helper.integer(check['valid_until_ms'],1,2**63-1)
        or not helper.integer(attestation['clock_uncertainty_ms'],0,helper.ATTESTATION_MS-1)
        or check['checked_at_ms']!=attestation['attested_at_ms']
        or check['valid_until_ms']!=check['checked_at_ms']+helper.ATTESTATION_MS):
        raise ValueError('original execution/attestation times differ')
    return prior,binding

def original_reply(reply,prior,identity):
    if not isinstance(reply,dict) or set(reply)!={'status','data','replayed'} or reply.get('status')!='ok' or type(reply['replayed']) is not bool:
        raise ValueError('original receipt unavailable; reconcile original identity')
    record=reply.get('data')
    if (not isinstance(record,dict) or set(record)!={'request','producer','reply'} or canonical(record.get('request'))!=canonical(prior) or canonical(record.get('producer'))!=canonical(identity)
        or not isinstance(record.get('reply'),dict) or set(record['reply'])!={'status','data','replayed'} or type(record['reply']['replayed']) is not bool):
        raise ValueError('original protected receipt binding differs')
    if (prior['command'].get('operation')=='check' and record['reply']['status']=='ok'
        and canonical(record['reply']['data'])!=canonical(prior['command']['input'])):
        raise ValueError('original protected Check reply differs')
    return record

def bind_applicability(out,request):
    helper=load_behavior()
    path=helper.protected(out/'applicability.json')
    attestation,_=helper.read_json(path,helper.MAX_EVIDENCE)
    if attestation['operation_id']!=request['operation_id'] or not helper.exact(request['command'],{'operation':'check','input':attestation['check']}):
        raise ValueError('applicability does not bind the original Check')
    value={'format':BINDING_FORMAT,'revision':1,'operation_id':request['operation_id'],
           'request_sha256':hashlib.sha256((out/'request.json').read_bytes()).hexdigest(),
           'evidence':attestation['evidence'],'attestation':{'path':str(path),'sha256':helper.digest(path)},
           'producer':attestation['producer']}
    helper.saved(out/'evidence-binding.json',value)
    bound_original(out)
    check=attestation['check'];now=time.time_ns()//1_000_000
    if now<check['checked_at_ms'] or now+attestation['clock_uncertainty_ms']>=check['valid_until_ms']:
        raise ValueError('fresh applicability expired or clock uncertain before transport')
    return value

def run(args):
    c=load_config(args.config);out=Path(args.receipt);out.mkdir(mode=0o700)
    if args.mode=='recover-behavior':
        load_behavior().protected(Path(args.config).absolute())
        load_behavior().recover(c,args.recipe,Path(args.original_receipt),out)
        return 2 # cleanup observation is not a successful check or retry
    if args.mode in {'retry','lookup'}:
        prior,binding=bound_original(Path(args.original_receipt))
        if binding is not None and binding['producer']['uid']!=c['producer_uid']:raise ValueError('original checker identity differs')
        if args.mode=='retry':
            if args.operation_id!=prior['operation_id']:raise ValueError('retry cannot replace the original operation identity')
            request=prior
        else:request={'revision':1,'operation_id':args.operation_id,'command':{'operation':'get_receipt','input':{'operation_id':prior['operation_id']}}}
    else:
        with Path(args.input).open('rb') as f:raw=f.read(MAX+1)
        if len(raw)>MAX:raise ValueError('input bound')
        value=strict(raw)
        if args.mode=='check-package':command={'operation':'check','input':package_check(c,value,args.package,out)}
        elif args.mode in {'check-behavior','check-prepared','attest-evidence'}:
            supervisor=load_behavior();supervisor.protected(Path(args.config).absolute())
            if args.mode in {'check-prepared','attest-evidence'}:
                if not isinstance(value,dict) or set(value)!={'subject','candidate'}:raise ValueError('prepared input requires subject and ordinary candidate only')
                if args.mode=='attest-evidence':
                    checked=supervisor.attest_evidence(c,value['subject'],value['candidate'],args.package,args.recipe,Path(args.original_receipt),out,args.operation_id)
                else:checked=supervisor.behavior_check(c,value['subject'],args.package,args.recipe,out,args.operation_id,value['candidate'])
                command={'operation':'check','input':checked}
            else:command={'operation':'check','input':supervisor.behavior_check(c,value,args.package,args.recipe,out,args.operation_id)}
        else:
            command=value
            if command.get('operation') not in OPERATOR_COMMANDS:raise ValueError('unsupported producer command; checks are produced by the fixed checker')
        request={'revision':1,'operation_id':args.operation_id,'command':command}
    p=out/'request.json'
    with private_create(p) as f:f.write(canonical(request));f.flush();os.fsync(f.fileno())
    sync_directory(out)
    fresh_binding=bind_applicability(out,request) if (out/'applicability.json').exists() else None
    receipt={'status':'uncertain','request_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'transport':'kernel-peer-uid-unix','automatic_rollout':False}
    try:
        reply=exchange(c,request)
        receipt['reply']=reply
        if fresh_binding is not None or (args.mode in {'retry','lookup'} and binding is not None):
            if not isinstance(reply,dict) or set(reply)!={'status','data','replayed'} or type(reply['replayed']) is not bool or not isinstance(reply['status'],str):
                raise ValueError('original protected response schema differs; reconcile original request')
        if args.mode=='lookup' and reply.get('status')=='ok':
            record=reply.get('data',{})
            if binding is not None:
                record=original_reply(reply,prior,binding['producer'])
                receipt['status']='recorded' if record['reply'].get('status')=='ok' else 'refused'
            elif record.get('request')!=prior or record.get('producer',{}).get('uid')!=c['producer_uid']:raise ValueError('original protected receipt binding differs')
        if args.mode=='retry' and binding is not None and reply.get('status')=='ok' and canonical(reply.get('data'))!=canonical(prior['command']['input']):
            raise ValueError('original Check retry reply differs; reconcile original request')
        if fresh_binding is not None and reply.get('status')=='ok' and (canonical(reply.get('data'))!=canonical(request['command']['input']) or reply.get('replayed') is not False):
            raise ValueError('new attestation reply differs; reconcile original request')
        if receipt['status']=='uncertain':receipt['status']='recorded' if reply.get('status')=='ok' else 'refused'
    finally:
        with private_create(out/'receipt.json') as f:f.write(canonical(receipt)+b'\n');f.flush();os.fsync(f.fileno())
        sync_directory(out)
    return 0 if receipt['status']=='recorded' else 2
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['check-package','check-behavior','check-prepared','attest-evidence','recover-behavior','request','lookup','retry','exchange-stdio']);p.add_argument('--config',required=True);p.add_argument('--input');p.add_argument('--original-receipt');p.add_argument('--operation-id');p.add_argument('--receipt');p.add_argument('--package');p.add_argument('--recipe');p.add_argument('--config-sha256');p.add_argument('--module-sha256');p.add_argument('--python-sha256');p.add_argument('--max-bytes',type=int);p.add_argument('--timeout-ms',type=int);a=p.parse_args()
    try:
        if a.mode=='exchange-stdio':sys.exit(exchange_stdio(a))
        if not a.operation_id or not a.receipt:raise ValueError('operation identity and receipt directory required')
        if a.mode in {'lookup','retry','recover-behavior','attest-evidence'} and not a.original_receipt:raise ValueError('original receipt required')
        if a.mode in {'check-package','check-behavior','check-prepared','attest-evidence','request'} and not a.input:raise ValueError('input required')
        if a.mode in {'check-package','check-behavior','check-prepared','attest-evidence'} and not a.package:raise ValueError('package required')
        if a.mode in {'check-behavior','check-prepared','recover-behavior','attest-evidence'} and not a.recipe:raise ValueError('protected behavior recipe required')
        sys.exit(run(a))
    except Exception as e:print('protected release: '+str(e),file=sys.stderr);sys.exit(1)
