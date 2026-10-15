"""Fixed rollout file/process primitives. Policy bounds are explicit metadata."""
from __future__ import annotations
import contextlib, fcntl, hashlib, importlib.util, json, os, selectors, signal, socket, stat, subprocess, time
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def strict(raw):
    def pairs(items):
        value = {}
        for k, v in items:
            if k in value: raise ValueError('duplicate JSON field')
            value[k] = v
        return value
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def fields(value, expected, label):
    if not isinstance(value, dict) or set(value) != set(expected.split()): raise ValueError(label + ' fields')


def positive(value, label):
    if type(value) is not int or value <= 0: raise ValueError(label)


def digest(raw): return hashlib.sha256(raw).hexdigest()
def now_ms(): return time.time_ns() // 1_000_000


def protected(path, directory=False):
    p = Path(path)
    if not p.is_absolute() or '..' in p.parts or p != p.resolve(strict=True): raise ValueError('protected path alias')
    for q in (p, *p.parents):
        m = q.lstat()
        if stat.S_ISLNK(m.st_mode) or m.st_uid not in {0, os.geteuid()} or m.st_mode & 0o022:
            raise ValueError('protected path ownership/mode')
    m = p.lstat()
    if directory:
        if not stat.S_ISDIR(m.st_mode): raise ValueError('protected directory type')
    elif not stat.S_ISREG(m.st_mode) or m.st_nlink != 1: raise ValueError('protected file type/link')
    return p


def read(path, bound, trusted=False):
    positive(bound, 'read byte bound')
    p = protected(path) if trusted else Path(path)
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as f:
        m = os.fstat(f.fileno())
        if not stat.S_ISREG(m.st_mode) or m.st_nlink != 1 or m.st_size > bound: raise ValueError('file type/link/byte bound')
        raw = f.read(bound + 1)
        if len(raw) > bound: raise ValueError('file grew beyond byte bound')
        return raw


def sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)


def write_new(path, raw, mode=0o600):
    p = Path(path)
    with os.fdopen(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode), 'wb') as f:
        f.write(raw); f.flush(); os.fchmod(f.fileno(), mode); os.fsync(f.fileno())
    sync(p.parent)


def save(path, value, bound):
    raw = canonical(value)
    if len(raw) > bound: raise ValueError('receipt byte bound')
    write_new(path, raw)


@contextlib.contextmanager
def locked(path):
    p = Path(path); fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        m = os.fstat(fd)
        if not stat.S_ISREG(m.st_mode) or m.st_nlink != 1 or m.st_uid != os.geteuid() or m.st_mode & 0o077: raise ValueError('lock identity/mode')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (p.stat().st_dev, p.stat().st_ino) != (m.st_dev, m.st_ino): raise ValueError('replaced lock')
        yield
    finally: os.close(fd)


def pin(item, bound):
    fields(item, 'path sha256', 'pin')
    raw = read(item['path'], bound, trusted=True)
    if digest(raw) != item['sha256']: raise ValueError('protected pinned input changed')
    return raw


def load_module(name, item, bound):
    raw = pin(item, bound)
    module = importlib.util.module_from_spec(importlib.util.spec_from_file_location(name, item['path']))
    exec(compile(raw, item['path'], 'exec'), module.__dict__)
    return module


def run(argv, limits, deadline_ms, data=b''):
    """Bound local transport, retain unknown remote outcomes; never retry."""
    remaining = min(limits['command_timeout_ms'], deadline_ms - now_ms())
    if remaining <= 0: raise ValueError('operation deadline before command')
    end = time.monotonic() + remaining / 1000
    p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         start_new_session=True, env={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent'})
    output = bytearray(); errors = bytearray(); sel = None
    streams = {p.stdout: output, p.stderr: errors}; position = 0
    owned = True
    try:
        sel = selectors.DefaultSelector()
        for f in streams: os.set_blocking(f.fileno(), False); sel.register(f, selectors.EVENT_READ)
        os.set_blocking(p.stdin.fileno(), False)
        if data: sel.register(p.stdin, selectors.EVENT_WRITE)
        else: p.stdin.close()
        while sel.get_map():
            left = end - time.monotonic()
            if left <= 0: raise TimeoutError('bounded command deadline; effect may be unknown')
            for key, mask in sel.select(left):
                f = key.fileobj
                if mask & selectors.EVENT_WRITE:
                    try: position += os.write(f.fileno(), data[position:position + limits['io_chunk_bytes']])
                    except BlockingIOError: continue
                    if position == len(data): sel.unregister(f); f.close()
                else:
                    try: block = os.read(f.fileno(), limits['io_chunk_bytes'])
                    except BlockingIOError: continue
                    if not block: sel.unregister(f); f.close(); continue
                    streams[f].extend(block)
                    if len(output) + len(errors) > limits['command_output_bytes']: raise ValueError('command output bound; effect may be unknown')
        # WNOWAIT pins the owned leader PID/PGID until our final group signal.
        # Popen.poll/wait must not reap it before that signal.
        while True:
            try: status=os.waitid(os.P_PID,p.pid,os.WEXITED|os.WNOWAIT|os.WNOHANG)
            except ChildProcessError:
                owned=False;raise ValueError('owned leader already reaped; cleanup identity unverified')
            if status is not None:break
            left=end-time.monotonic()
            if left<=0:raise TimeoutError('bounded command exit deadline')
            time.sleep(min(limits['cleanup_poll_ms']/1000,left))
        if status.si_code!=os.CLD_EXITED or status.si_status!=0:raise ValueError('fixed command failed: exit '+str(status.si_status))
    finally:
        try:
            if sel is not None:sel.close()
        finally:
            try:
                if not owned:raise ValueError('local command cleanup unverified; remote outcome unknown')
                # Leader remains ours even if it exited; descendants may retain
                # pipes. Never send a signal after the following reap.
                try:os.killpg(p.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                end_cleanup=time.monotonic()+limits['cleanup_timeout_ms']/1000
                try:p.wait(timeout=max(0,end_cleanup-time.monotonic()))
                except subprocess.TimeoutExpired:raise ValueError('local command cleanup unverified; remote outcome unknown')
                while True:
                    try:os.killpg(p.pid,0) # observation only after reap
                    except ProcessLookupError:break
                    if time.monotonic()>=end_cleanup:raise ValueError('owned group cleanup unverified; remote outcome unknown')
                    time.sleep(min(limits['cleanup_poll_ms']/1000,max(0,end_cleanup-time.monotonic())))
            finally:
                for f in (p.stdin,p.stdout,p.stderr):
                    if not f.closed:
                        try:f.close()
                        except OSError:pass
    return bytes(output)


def http_read(host,port,data,limits,deadline_ms):
    """Fixed loopback POST with an absolute deadline and bounded complete frame.

    The selected Borg server returns Content-Length for its fixed JSON body.
    Transfer coding, duplicate lengths and extra response bytes refuse.
    """
    if host not in ('127.0.0.1','::1'):raise ValueError('non-loopback control read')
    end=time.monotonic()+min(limits['transport_timeout_ms'],deadline_ms-now_ms())/1000
    def left():
        value=end-time.monotonic()
        if value<=0:raise TimeoutError('canonical HTTP absolute deadline')
        return value
    if len(data)>limits['transport_bytes']:raise ValueError('canonical request byte bound')
    host_header=('['+host+']' if ':' in host else host)+':'+str(port)
    request=b'POST /api/operations HTTP/1.1\r\nHost: '+host_header.encode()+b'\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: '+str(len(data)).encode()+b'\r\n\r\n'+data
    with socket.create_connection((host,port),timeout=left()) as peer:
        peer.settimeout(left());peer.sendall(request);raw=bytearray();header_end=None;length=None
        while True:
            peer.settimeout(left());chunk=peer.recv(min(limits['io_chunk_bytes'],limits['transport_bytes']-len(raw)+1))
            if not chunk:raise ValueError('truncated canonical HTTP frame')
            raw.extend(chunk)
            if len(raw)>limits['transport_bytes']:raise ValueError('canonical HTTP frame byte bound')
            if header_end is None and b'\r\n\r\n' in raw:
                header_end=raw.index(b'\r\n\r\n')+4;lines=bytes(raw[:header_end-4]).split(b'\r\n');headers={}
                if lines[0]!=b'HTTP/1.1 200 OK':raise ValueError('canonical HTTP status differs')
                for line in lines[1:]:
                    key,sep,value=line.partition(b':');key=key.lower();value=value.strip()
                    if not sep or key in headers:raise ValueError('canonical HTTP header shape')
                    headers[key]=value
                if b'transfer-encoding' in headers or not headers.get(b'content-length',b'').isdigit():raise ValueError('canonical HTTP length required')
                length=int(headers[b'content-length'])
                if header_end+length>limits['transport_bytes']:raise ValueError('canonical HTTP response length bound')
            if header_end is not None and len(raw)>=header_end+length:
                if len(raw)!=header_end+length:raise ValueError('extra canonical HTTP frame bytes')
                left();return bytes(raw[header_end:])


LIMIT_FIELDS = 'receipt_bytes module_bytes package_bytes io_chunk_bytes hosts command_output_bytes command_timeout_ms cleanup_timeout_ms cleanup_poll_ms overall_timeout_ms transport_bytes transport_timeout_ms state_entries state_bytes api_pages api_page_items sqlite_tables sqlite_rows sqlite_steps readiness_poll_ms clock_uncertainty_ms'
PROFILE_FIELDS = 'revision deployment_target target_revision policy producer controller_config python transport_tools sqlite_reader state_root limits modules formats hosts'
HOST_FIELDS = 'id role transport profile_path state_root service unit new_unit release_root package old_artifact service_uid helper_uid incarnation state owner_lock helpers preserve_metadata preserve_files api systemctl'


def validate(profile):
    fields(profile, PROFILE_FIELDS, 'rollout profile')
    if profile['revision'] != 1: raise ValueError('unsupported rollout profile')
    fields(profile['sqlite_reader'], 'extension library', 'SQLite reader pins')
    fields(profile['limits'], LIMIT_FIELDS, 'limits')
    for name, value in profile['limits'].items():
        if name=='clock_uncertainty_ms':
            if type(value) is not int or value<0:raise ValueError('limits.clock_uncertainty_ms')
        else:positive(value, 'limits.' + name)
    fields(profile['transport_tools'],'ssh sudo env','transport tools')
    for name,item in profile['transport_tools'].items():
        fields(item,'path sha256','transport tool')
        if item['path']!='/usr/bin/'+name:raise ValueError('fixed transport tool path')
    fields(profile['formats'], 'before after source_files', 'formats')
    for name in ('before','after'):
        fields(profile['formats'][name], 'wire controller_schema worker_format', 'format tuple')
        for value in profile['formats'][name].values(): positive(value, 'format value')
    if profile['formats']['before'] != profile['formats']['after']: raise ValueError('incompatible state formats: no reset or migration')
    fields(profile['modules'], 'producer verifier common host adapter', 'module inventory')
    if not isinstance(profile['hosts'],list) or not 0<len(profile['hosts'])<=profile['limits']['hosts']: raise ValueError('host count bound')
    ids=set(); controllers=[]
    for h in profile['hosts']:
        fields(h, HOST_FIELDS, 'host')
        if not isinstance(h['id'],str) or not h['id'] or h['id'] in ids: raise ValueError('host identity')
        ids.add(h['id'])
        if h['role'] not in ('controller','worker'): raise ValueError('host role')
        if h['role']=='controller': controllers.append(h)
        if not isinstance(h['service'],str) or '/' in h['service'] or not h['service'].endswith('.service') or h['service'].startswith('-'): raise ValueError('service identity')
        fields(h['transport'], 'kind ssh_config alias', 'transport')
        if h['transport']['kind'] not in ('local','ssh'): raise ValueError('transport kind')
        if h['transport']['kind']=='local' and (h['transport']['ssh_config'] is not None or h['transport']['alias'] is not None): raise ValueError('local transport fields')
        if h['transport']['kind']=='ssh' and (not isinstance(h['transport']['alias'],str) or not h['transport']['alias'] or h['transport']['alias'].startswith('-')): raise ValueError('ssh alias')
        fields(h['state'], 'root format_file database policy', 'state')
        fields(h['api'], 'host port worker_id', 'host API')
        if h['api']['host'] not in ('127.0.0.1','::1') or type(h['api']['port']) is not int or not 0<h['api']['port']<65536: raise ValueError('host API endpoint')
        for k in ('helper_uid','service_uid'):
            if type(h[k]) is not int or h[k]<0: raise ValueError('host UID')
        if h['incarnation'] != ('systemd_invocation' if h['role']=='worker' else None): raise ValueError('unsupported incarnation convention')
        if h['role']=='worker' and (not h['api']['worker_id'] or h['state']['database'] is not None): raise ValueError('worker state/API binding')
        if h['role']=='controller' and (h['api']['worker_id'] is not None or h['state']['format_file'] is not None): raise ValueError('controller state/API binding')
        for k in ('profile_path','state_root','release_root','owner_lock'):
            if not Path(h[k]).is_absolute() or '..' in Path(h[k]).parts: raise ValueError('absolute host path required')
        if Path(h['state_root']) == Path(h['state']['root']) or Path(h['state']['root']) in Path(h['state_root']).parents: raise ValueError('rollout journal must be outside runtime state')
        if not isinstance(h['preserve_files'],list):raise ValueError('preserved file inventory')
        for item in h['preserve_files']:fields(item,'path sha256','preserved file pin')
        if not isinstance(h['helpers'],list) or not isinstance(h['preserve_metadata'],list) or len(h['helpers'])+len(h['preserve_metadata'])>profile['limits']['state_entries']: raise ValueError('host preserved paths')
    if len(controllers)!=1: raise ValueError('one controller target required')
    return profile


def load_profile(path, expected_sha, profile_bytes):
    raw=read(path,profile_bytes,trusted=True)
    if digest(raw)!=expected_sha: raise ValueError('rollout profile changed')
    p=validate(strict(raw))
    for item in p['modules'].values(): pin(item,p['limits']['module_bytes'])
    for item in p['transport_tools'].values(): pin(item,p['limits']['module_bytes'])
    pin(p['python'],p['limits']['module_bytes'])
    if digest(Path('/proc/self/exe').read_bytes())!=p['python']['sha256']: raise ValueError('selected Python identity differs')
    protected(p['state_root'],directory=True)
    return p


def verify_package(profile, host, subject):
    limits=profile['limits']; package=Path(host['package'])
    raw=read(package,limits['package_bytes'],trusted=True)
    a=subject['artifact']
    if digest(raw)!=a['package_sha256'] or len(raw)!=a['package_bytes']: raise ValueError('exact package differs')
    verifier=load_module('rollout_fixed_verifier',profile['modules']['verifier'],limits['module_bytes'])
    m=verifier.verify(package,a['package_sha256'],a['source_commit'])
    actual={'format':m['format'],'package_sha256':digest(raw),'package_bytes':len(raw),'manifest_sha256':digest(verifier.canonical(m)),
            'source_commit':m['source']['commit'],'source_tree':m['source']['tree'],'object_format':m['source']['object_format'],
            'binary_sha256':m['files']['bin/borg-runtime']['sha256'],'binary_bytes':m['files']['bin/borg-runtime']['size'],'target':m['files']['bin/borg-runtime']['target']}
    if actual!=a: raise ValueError('all ten artifact fields differ')
    for name,sha in profile['formats']['source_files'].items():
        if m['source']['files'].get(name,{}).get('sha256')!=sha: raise ValueError('package format-source binding differs')
    if set(profile['formats']['source_files'])!={'src/protocol.rs','src/controller/database.rs','src/worker/handoff.rs'}: raise ValueError('format source inventory')
    return raw,m
