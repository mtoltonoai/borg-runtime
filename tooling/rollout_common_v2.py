"""Fixed rollout file/process primitives. Policy bounds are explicit metadata."""
from __future__ import annotations
import contextlib, fcntl, hashlib, importlib.util, json, os, re, selectors, signal, socket, stat, subprocess, time
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


_ERROR_PHASES = {'preflight', 'stop', 'install', 'start', 'inspect', 'apply', 'reconcile', 'transport', 'startup'}
_GUARD_CODES = {'state_directory_changed', 'original_worker_history_changed'}
_ERROR_CODES = _GUARD_CODES | {'guard_refused', 'fixed_command_failed'}
_ERROR_SITES = {'state_files.initial_directory', 'state_files.during_read',
    'state_files.final_directory', 'state_files.final_root',
    'worker_history.original_row', 'cleanup_observation.required_inputs'}
_ERROR_CATEGORIES = {'root', 'other', 'cleanup', 'mutations', 'processes', 'commands',
    'stops', 'codex', 'publications', 'pull-requests', 'launches', 'reports', 'checkpoints'}
_IDENTITY_FIELDS = ('device', 'inode', 'uid', 'gid', 'mode', 'nlink', 'bytes', 'mtime_ns', 'ctime_ns')


def _hex(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def _numbers(value, count):
    return isinstance(value, list) and len(value) == count and all(type(x) is int and x >= 0 for x in value)


def _error_details(value):
    """Only closed, non-content metadata can leave a failed host guard."""
    fields(value, 'kind site category relative_sha256 depth changed_fields expected observed', 'guard detail')
    if (value['kind'] not in ('directory', 'history') or value['site'] not in _ERROR_SITES
            or value['category'] not in _ERROR_CATEGORIES or not _hex(value['relative_sha256'])
            or type(value['depth']) is not int or value['depth'] < 0
            or not isinstance(value['changed_fields'], list)
            or len(set(value['changed_fields'])) != len(value['changed_fields'])
            or any(x not in (*_IDENTITY_FIELDS, 'sha256', 'identity', 'kind', 'present') for x in value['changed_fields'])):
        raise ValueError('guard_detail_shape')
    for row in (value['expected'], value['observed']):
        if value['kind'] == 'directory':
            if not _numbers(row, 9): raise ValueError('guard_directory_identity')
        elif row is not None:
            if not isinstance(row, dict) or not {'sha256', 'identity'} <= set(row) or set(row) - {'sha256', 'identity', 'kind', 'bytes', 'mtime_ns', 'ctime_ns'}:
                raise ValueError('guard_history_identity')
            if not _hex(row['sha256']) or not _numbers(row['identity'], 5): raise ValueError('guard_history_identity')
            if 'kind' in row and row['kind'] != 'codex_arg0_symlink': raise ValueError('guard_history_kind')
            if any(type(row[k]) is not int or row[k] < 0 for k in ('bytes', 'mtime_ns', 'ctime_ns') if k in row): raise ValueError('guard_history_metadata')
    return value


class GuardFailure(ValueError):
    def __init__(self, code, details=None):
        if code not in _GUARD_CODES:
            raise ValueError('guard_error_code')
        self.code = code
        self.details = None if details is None else _error_details(details)
        super().__init__(code)


def _closed_command(value, depth):
    fields(value, 'wait_code wait_status stdout_bytes stdout_sha256 stderr_bytes stderr_sha256' + (' host_error' if 'host_error' in value else '') + (' process' if 'process' in value else ''), 'command error')
    if (any(type(value[k]) is not int or value[k] < 0 for k in ('wait_code','wait_status','stdout_bytes','stderr_bytes'))
            or not _hex(value['stdout_sha256']) or not _hex(value['stderr_sha256'])):
        raise ValueError('command_error_shape')
    if 'process' in value:
        process=value['process']
        fields(process,'leader_pid leader_reaped group_empty input_bytes input_sent_bytes input_closed_early','command process')
        if (any(type(process[k]) is not int or process[k]<0 for k in ('leader_pid','input_bytes','input_sent_bytes'))
                or process['leader_pid']==0 or process['input_sent_bytes']>process['input_bytes']
                or any(type(process[k]) is not bool for k in ('leader_reaped','group_empty','input_closed_early'))):
            raise ValueError('command_process_shape')
    if 'host_error' in value:
        if depth >= 2:raise ValueError('command_error_depth')
        _closed_error(value['host_error'],depth+1)
    return value


def _closed_error(value, depth=0):
    fields(value, 'phase type code message_sha256 errno' + (' details' if 'details' in value else '') + (' command' if 'command' in value else ''), 'host error')
    if (value['phase'] not in _ERROR_PHASES or type(value['type']) is not str
            or re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,95}', value['type']) is None
            or type(value['code']) is not str or value['code'] not in _ERROR_CODES
            or not _hex(value['message_sha256'])
            or value['errno'] is not None and (type(value['errno']) is not int or value['errno'] < 0)):
        raise ValueError('host_error_shape')
    if 'details' in value: _error_details(value['details'])
    if 'command' in value: _closed_command(value['command'],depth)
    return value


class CommandFailure(ValueError):
    def __init__(self, status, output, errors, process=None):
        self.command = {'wait_code': status.si_code, 'wait_status': status.si_status,
            'stdout_bytes': len(output), 'stdout_sha256': digest(output),
            'stderr_bytes': len(errors), 'stderr_sha256': digest(errors)}
        if process is not None:self.command['process']=process
        # Large/private command output is never decoded or forwarded as an
        # error. Only the host's small closed metadata envelope is recognized.
        if len(output) <= 65536:
            try:
                envelope = strict(output)
                fields(envelope, 'host_error', 'host error envelope')
                self.command['host_error'] = _closed_error(envelope['host_error'])
            except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
                pass
        super().__init__('fixed_command_failed')


def error_record(error, phase):
    """Keep actionable closed diagnostics without exporting exception text."""
    message = str(error)
    code = error.code if isinstance(error, GuardFailure) else 'guard_refused'
    kind = type(error).__name__
    row = {'phase': phase if phase in _ERROR_PHASES else 'startup',
        'type': kind if re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,95}', kind) else 'Exception',
        'code': 'fixed_command_failed' if isinstance(error, CommandFailure) else code,
        'message_sha256': digest(message.encode()), 'errno': getattr(error, 'errno', None)}
    if isinstance(error, GuardFailure) and error.details is not None: row['details'] = error.details
    if isinstance(error, CommandFailure): row['command'] = error.command
    return row


def controller_receipt_rows(snapshot, name, bound):
    table = snapshot['tables'][name]
    columns, rows = table['columns'], table['rows']
    if (not isinstance(columns, list) or not all(type(k) is str for k in columns)
            or len(set(columns)) != len(columns) or 'id' not in columns
            or not isinstance(rows, list) or len(rows) > bound):
        raise ValueError('receipt_table_shape_or_bound')
    result = {}
    for values in rows:
        if not isinstance(values, list) or len(values) != len(columns): raise ValueError('receipt_row_shape')
        row = dict(zip(columns, values))
        if type(row['id']) is not str or not row['id'] or row['id'] in result: raise ValueError('receipt_row_identity')
        result[row['id']] = row
    return result


# Immutable e20519 decoder and causal receipt producer family.
COMPACT_RECEIPT_SOURCES = {'src/controller/application_schema.rs': 'f177e8ca7839e0f9e5130ee0a50ebe7eee78d30c7979e8d930dc9fb10ab7f783', 'src/controller/database.rs': '7a9d59f9b32775d75f754b192fbbd0853ac28da1970b9d42a9f8c849fc3dfb40', 'src/controller/metadata.rs': '126d141f05cf1d74de85008b08b19a79bdc90025c3335f79dd33eab7d42ad532', 'src/controller/operations/effects.rs': 'a818afb64d82048e563599159a7c982e200bd43437c00249d057c4316322e88a', 'src/controller/operations/reports.rs': '299c8630b98b053a6e50c33a313e4e78026d3cbc760326943769446869c909c7', 'src/controller/read_retention.rs': '0def3401d029f1b14eab602b3d3f6520862da8ddb7a4022967030fce0d1b72f7', 'src/limits/retention.rs': 'd69614214efd380d62c7a6f5970a46f484b03c54943d8e18dcb0e7b4b972835d', 'src/mcp/mod.rs': '734eb4884aacd34acfb0ea14817d92a9241d11e59c59e4a200529cb2ab4f4d1e', 'src/protocol.rs': '9a04ea9917f388f10f055c1a341872f632da8626abf09092b5182e5edcd58fb6', 'src/read_page.rs': 'd386b2f8a310788e2bfe116878f47892382b5360ea89ca5a713754430c6e9a54', 'src/read_retention.rs': 'c1a069d41f3ff8d133b7929680d2b3f094e4d07a4331e6784427c64d9cc6a811', 'src/repository_view.rs': '08c9a14c273502223bed3888292e38d468280a6474169b00f33838f4ea2bb949'}


# Exact current union decoder; the historical recovered family stays intact.
PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES = {
    'src/controller/application_schema.rs': '5797006d361ff3370e64f371ec871e2f8226cd0d091bfd42a7226923c3090c45',
    'src/controller/database.rs': '28e2667b764edb51cfe57c1d56acdf3fbc34ec589e9b257b64564aec62ffc83e',
    'src/controller/metadata.rs': '0c2edca4afa187de2867b538ce7c15fcf69f6ba948467810356ca572a5e930b2',
    'src/controller/operations/effects.rs': '9d7647d45f99ebb346a37f2bec0150ec87532f2454d3bd686df3f875d610846d',
    'src/controller/operations/reports.rs': '488115cf01161e2ab5ec1295fa9af2118b6c02ed294088557a24fbbe2bcbb9a7',
    'src/controller/read_retention.rs': '22b2eeb67dfb7402a72a97dd1eef9a81d891aea1f2da6446e65d421ee63b155a',
    'src/limits/retention.rs': 'd69614214efd380d62c7a6f5970a46f484b03c54943d8e18dcb0e7b4b972835d',
    'src/mcp/mod.rs': '6c1efc59c9cc296c240c7a2b7cece9efb3ae597d8a74b4af960538b84e4bc6d7',
    'src/protocol.rs': '1cf2097fe4268ecf3d9c654d8dce4f8cbaaa50402661405cf892978519e0fc34',
    'src/read_page.rs': 'd386b2f8a310788e2bfe116878f47892382b5360ea89ca5a713754430c6e9a54',
    'src/read_retention.rs': '297a634ddbc1b1f41f83a8fdc55bf8d93c40660ebdec9df80cb0a5858278aac6',
    'src/repository_view.rs': '08c9a14c273502223bed3888292e38d468280a6474169b00f33838f4ea2bb949',
}
COMPACT_READ_KINDS = {'collab_read_continue': 'page', 'collab_task_get': 'task', 'collab_memory_get': 'memory', 'collab_agent_get': 'agent', 'collab_tasks_list': 'list', 'collab_agents_list': 'list', 'collab_task_releases_list': 'list', 'collab_repository_tree': 'repository', 'collab_repository_read': 'repository', 'collab_repository_search': 'repository', 'collab_repository_diff': 'repository', 'collab_repository_history': 'repository', 'collab_document_get': 'document', 'collab_documents_list': 'document', 'collab_document_versions_list': 'document', 'collab_comments_list': 'document', 'collab_question_get': 'question', 'collab_questions_list': 'question', 'collab_question_answers': 'question', 'collab_question_actions': 'question', 'collab_decisions_list': 'question', 'collab_review_get': 'review', 'collab_reviews_list': 'review', 'collab_review_target_get': 'review', 'collab_task_coordination_get': 'work', 'collab_task_waits': 'work', 'collab_limits_get': 'collaboration', 'collab_session_get': 'collaboration', 'collab_sessions_list': 'collaboration', 'collab_definition_get': 'collaboration', 'collab_memory_list': 'collaboration', 'collab_task_history_list': 'collaboration', 'collab_workers_list': 'collaboration', 'collab_worker_get': 'collaboration', 'collab_machines_list': 'collaboration', 'collab_machine_get': 'collaboration', 'collab_task_attempts_list': 'collaboration', 'collab_task_checkpoints_list': 'collaboration', 'collab_release_get': 'collaboration', 'collab_release_checks_list': 'collaboration', 'collab_repositories_list': 'collaboration', 'collab_repository_get': 'collaboration'}


def compact_receipt_sources(profile, manifest):
    """Exact selected decoder/provenance producers, authenticated by Host.execute.

    Format numbers alone do not admit a new receipt representation. This reader
    does not enable retention or reconstruct the removed original body.
    """
    after = profile.get('formats', {}).get('after')
    if after == {'wire':25,'controller_schema':26,'worker_format':20}:
        required = COMPACT_RECEIPT_SOURCES
    elif after == {'wire':27,'controller_schema':28,'worker_format':22}:
        required = PUBLICATION_UNION_COMPACT_RECEIPT_SOURCES
    else:
        raise ValueError('compact_receipt_format')
    files = manifest.get('source', {}).get('files', {}) if isinstance(manifest, dict) else {}
    if any(not isinstance(files.get(name), dict) or files[name].get('sha256') != sha
           for name, sha in required.items()):
        raise ValueError('compact_receipt_authenticated_sources')
    return dict(required)


def _compact_uint(value, label, maximum=(1 << 64) - 1, positive_only=False):
    if type(value) is not int or not (int(positive_only) <= value <= maximum):
        raise ValueError('compact_' + label)
    return value


def _compact_text(value, label):
    if type(value) is not str or not value: raise ValueError('compact_' + label)
    return value


def _compact_receipt_record(identity, old, receipts):
    """Join immutable saved receipts; the exact live runtime validates typed DTOs.

    The nested typed source projection is preserved byte-for-value throughout
    the evidence/release/live-record joins. Its Rust decoder remains the one
    semantic authority; Python does not introduce another pagination decoder.
    """
    same = lambda left, right: canonical(left) == canonical(right)
    def receipt(key, operation, data_kind):
        row = receipts.get(key)
        if not isinstance(row, dict): raise ValueError('compact_causal_receipt_missing')
        revision = _compact_uint(row.get('controller_revision'), 'receipt_revision', (1 << 63)-1, True)
        request, outcome = strict(row['request']), strict(row['outcome'])
        fields(request, 'operation input', 'compact causal request')
        fields(outcome, 'status data', 'compact causal outcome')
        fields(outcome['data'], 'kind value', 'compact causal data')
        if request['operation'] != operation or outcome['status'] != 'ok' or outcome['data']['kind'] != data_kind:
            raise ValueError('compact_causal_receipt_kind')
        return request['input'], outcome['data']['value'], revision

    operation, outcome = strict(old['request']), strict(old['outcome'])
    fields(operation, 'operation input', 'compact original request')
    fields(operation['input'], 'call', 'compact original input')
    if operation['operation'] != 'call_collaboration': raise ValueError('compact_original_not_read')
    call = operation['input']['call']
    fields(call, 'operation_id logical_call_id caller tool arguments', 'compact invocation')
    caller = call['caller']
    fields(caller, 'task_id attempt_id generation worker_id incarnation', 'compact caller')
    for key in ('task_id','attempt_id','worker_id','incarnation'): _compact_text(caller[key], key)
    _compact_uint(caller['generation'], 'generation', positive_only=True)
    _compact_text(call['logical_call_id'], 'logical_call_id')
    if call['operation_id'] != identity: raise ValueError('compact_original_identity')
    fields(outcome, 'committed_read_reference', 'compact outcome')
    compact = outcome['committed_read_reference']
    fields(compact, 'evidence release_operation_id attempt_id session_id', 'compact reference')
    evidence = compact['evidence']
    fields(evidence, 'kind outcome_bytes outcome_sha256 source', 'compact evidence')
    if (COMPACT_READ_KINDS.get(call['tool']) != evidence['kind'] or not _hex(evidence['outcome_sha256'])
            or not isinstance(evidence['source'], dict) or not isinstance(call['arguments'], dict)):
        raise ValueError('compact_read_classification_or_digest')
    _compact_uint(evidence['outcome_bytes'], 'outcome_bytes', (1 << 63)-1, True)
    source_fields = {'observation':'kind','page':'kind page','read_list':'kind items complete next_cursor',
                     'repository':'kind source file','document':'kind version'}.get(evidence['source'].get('kind'))
    if source_fields is None: raise ValueError('compact_source_kind')
    fields(evidence['source'], source_fields, 'compact source')
    _compact_text(compact['session_id'], 'session_id')
    if compact['attempt_id'] != caller['attempt_id']: raise ValueError('compact_attempt')
    release_id = _compact_text(compact['release_operation_id'], 'release_id')
    release, result, release_revision = receipt(release_id, 'release_terminal_reads', 'terminal_read_release')
    fields(release, 'operation_id caller command_id session_id terminal_revision first_safe_operation_id policy_revision policy custody custody_operation_id reads', 'compact release')
    fields(result, 'attempt_id released body_bytes_before body_bytes_after first_safe_operation_id first_safe_ms eligible_at_ms', 'compact release result')
    original_revision = _compact_uint(old['controller_revision'], 'original_revision', (1 << 63)-1, True)
    if (release['operation_id'] != release_id or not same(release['caller'], caller) or release['session_id'] != compact['session_id']
            or release_revision <= original_revision or result['attempt_id'] != caller['attempt_id']):
        raise ValueError('compact_release_identity_or_order')
    _compact_text(release['command_id'], 'command_id')
    _compact_text(release['policy_revision'], 'policy_revision')
    _compact_uint(release['terminal_revision'], 'terminal_revision', (1 << 63)-1, True)
    policy = release['policy']
    fields(policy, 'success_grace_ms failed_grace_ms cancelled_grace_ms idle_cache_grace_ms batch_entries filesystem_entries_per_yield operation_timeout_ms retry_delay_ms record_bytes', 'compact historical policy')
    for key, value in policy.items():
        _compact_uint(value, 'policy_' + key, (1 << 63)-1,
                      key in ('batch_entries','filesystem_entries_per_yield','operation_timeout_ms','retry_delay_ms','record_bytes'))
    custody = release['custody']
    fields(custody, 'kind manifest_sha256 bytes files', 'compact custody')
    if custody['kind'] not in ('native_journal','coding_copy') or not _hex(custody['manifest_sha256']):
        raise ValueError('compact_custody_shape')
    for key in ('bytes','files'): _compact_uint(custody[key], 'custody_' + key, positive_only=True)
    reads = release['reads']
    if not isinstance(reads, list) or not 0 < len(reads) <= policy['batch_entries']:
        raise ValueError('compact_release_batch')
    selected = {}
    for item in reads:
        fields(item, 'operation_id commit evidence', 'compact selected read')
        key = _compact_text(item['operation_id'], 'read_id')
        fields(item['commit'], 'operation_id controller_revision', 'compact selected commit')
        _compact_uint(item['commit']['controller_revision'], 'read_revision', (1 << 63)-1, True)
        if key in selected or key == release_id or item['commit']['operation_id'] != key:
            raise ValueError('compact_release_selection')
        selected[key] = item
    if not same(selected.get(identity), {'operation_id':identity,
            'commit':{'operation_id':identity,'controller_revision':original_revision},'evidence':evidence}):
        raise ValueError('compact_original_evidence')
    released = result['released']
    if (not isinstance(released,list) or any(type(v) is not str for v in released)
            or len(set(released)) != len(released) or identity not in released or not set(released) <= set(selected)):
        raise ValueError('compact_released_membership')
    before_bytes = _compact_uint(result['body_bytes_before'], 'before_bytes', (1 << 63)-1, True)
    after_bytes = _compact_uint(result['body_bytes_after'], 'after_bytes', (1 << 63)-1, True)
    if before_bytes <= after_bytes: raise ValueError('compact_release_size')
    first_id = _compact_text(release['first_safe_operation_id'], 'first_safe_id')
    first_request, first_result, first_revision = receipt(first_id, 'release_terminal_reads', 'terminal_read_release')
    expected_first = dict(release, operation_id=first_id, first_safe_operation_id=None)
    expected_result = dict(result, released=[], body_bytes_before=0, body_bytes_after=0)
    if (not same(first_request, expected_first) or not same(first_result, expected_result)
            or result['first_safe_operation_id'] != first_id or first_revision >= release_revision):
        raise ValueError('compact_first_safe_binding_or_order')
    custody_id = _compact_text(release['custody_operation_id'], 'custody_id')
    report_input, task, custody_revision = receipt(custody_id, 'observe_worker', 'task')
    fields(report_input, 'report', 'compact report input')
    report = report_input['report']
    fields(report, 'report_id worker_id incarnation task_id attempt_id generation observation', 'compact report')
    fields(report['observation'], 'kind proof', 'compact custody observation')
    expected_custody = dict(expected_first, operation_id=custody_id)
    if (report['report_id'] != custody_id or not same(report['observation'], {'kind':'read_transcript_custody','proof':expected_custody})
            or custody_revision >= first_revision
            or any(not same(report[key], caller[key]) for key in ('worker_id','incarnation','task_id','attempt_id','generation'))):
        raise ValueError('compact_custody_binding_or_order')
    if (not isinstance(task,dict) or task.get('id') != caller['task_id'] or task.get('session_id') != release['session_id']
            or not same(task.get('revision'), release['terminal_revision']) or task.get('pending_assignment') is not None
            or task.get('waiting_child_id') is not None): raise ValueError('compact_terminal_task')
    attempt = task.get('attempt')
    if (not isinstance(attempt,dict) or attempt.get('id') != caller['attempt_id']
            or attempt.get('task_id') != caller['task_id'] or attempt.get('session_id') != release['session_id']
            or not same(attempt.get('generation'), caller['generation']) or attempt.get('worker_id') != caller['worker_id']
            or attempt.get('worker_incarnation') != caller['incarnation'] or attempt.get('phase') != 'stopped'
            or attempt.get('stopped') is not True or attempt.get('uncertain') is not False):
        raise ValueError('compact_terminal_attempt')
    grace_field = {'succeeded':'success_grace_ms','failed':'failed_grace_ms','cancelled':'cancelled_grace_ms'}.get(task.get('status'))
    executor = task.get('input',{}).get('definition',{}).get('executor')
    if grace_field is None or {'native':'native_journal','coding':'coding_copy'}.get(executor) != custody['kind']:
        raise ValueError('compact_terminal_executor_or_status')
    first_ms = _compact_uint(result['first_safe_ms'], 'first_safe_ms')
    due_ms = _compact_uint(result['eligible_at_ms'], 'eligible_at_ms')
    if first_ms + policy[grace_field] != due_ms: raise ValueError('compact_original_grace')
    # No original body is synthesized or hashed here. Rust validates the saved
    # source projection and causal provenance again for the exact live lookup.
    return {'id':identity,'task_id':caller['task_id'],'attempt_id':caller['attempt_id'],
        'generation':caller['generation'],'kind':'committed_read_reference','intended':operation,
        'state':'original_read_body_unavailable','outcome':outcome}


def controller_receipt_plan(profile, before, restart, *, manifest=None):
    """Keep raw migration proof separate from the live API's selected namespace.

    GetOperation selects external_operations before application receipts. A
    same-ID external record has no application commit stamp; neither fabricating
    one nor omitting the hidden raw receipt is a preservation proof.
    """
    controller_retained(profile, before, restart)
    limits = profile['limits']
    receipts = controller_receipt_rows(before, 'receipts', limits['sqlite_rows'])
    external = controller_receipt_rows(restart, 'external_operations', limits['sqlite_rows'])
    plan = []
    for operation_id, old in receipts.items():
        if (type(old.get('request')) is not str or type(old.get('outcome')) is not str
                or type(old.get('controller_revision')) is not int or old['controller_revision'] <= 0):
            raise ValueError('original_receipt_shape')
        request, outcome = strict(old['request']), strict(old['outcome'])
        compact_record = None
        if isinstance(outcome, dict) and 'committed_read_reference' in outcome:
            allowed_schemas = (26,28) if profile.get('formats', {}).get('after') == {'wire':27,'controller_schema':28,'worker_format':22} else (26,)
            if before['schema'] not in allowed_schemas: raise ValueError('compact_original_schema')
            compact_receipt_sources(profile, manifest)
            compact_record = _compact_receipt_record(operation_id, old, receipts)
        if operation_id in external: continue
        if compact_record is not None:
            plan.append({'id':operation_id,'namespace':'committed_read_reference','record':compact_record,
                'commit':{'operation_id':operation_id,'controller_revision':old['controller_revision']}})
            continue
        task_id = ''
        if outcome.get('status') == 'ok' and outcome.get('data', {}).get('kind') == 'task':
            task_id = outcome['data']['value']['id']
            if type(task_id) is not str: raise ValueError('original_task_identity')
        record = {'id': operation_id, 'task_id': task_id, 'attempt_id': '', 'generation': 0,
            'kind': 'application_mutation', 'intended': request, 'state': 'settled', 'outcome': outcome}
        plan.append({'id': operation_id, 'namespace': 'application_mutation', 'record': record,
            'commit': {'operation_id': operation_id, 'controller_revision': old['controller_revision']}})
    for operation_id, old in external.items():
        if type(old.get('body')) is not str: raise ValueError('external_record_shape')
        record = strict(old['body'])
        if (not isinstance(record, dict) or set(record) != {
                'id', 'task_id', 'attempt_id', 'generation', 'kind', 'intended', 'state', 'outcome'}
                or record['id'] != operation_id
                or record['kind'] not in ('checkpoint', 'pull_request_head', 'pull_request', 'merge')):
            raise ValueError('external_record_identity_or_kind')
        plan.append({'id': operation_id, 'namespace': 'external', 'record': record, 'commit': None})
    if len(plan) > limits['state_entries']: raise ValueError('receipt_plan_bound')
    return plan


def verify_controller_lookup(expected, reply):
    observed = reply.get('observed_controller_revision')
    if (reply.get('status') != 'ok' or type(observed) is not int or observed < 0
            or 'commit' not in reply or canonical(reply['commit']) != canonical(expected['commit'])):
        raise ValueError('live_receipt_commit_or_status')
    if expected['commit'] is not None and observed < expected['commit']['controller_revision']:
        raise ValueError('live_receipt_revision')
    if canonical(reply.get('data')) != canonical({'kind': 'operation', 'value': expected['record']}):
        raise ValueError('live_receipt_record_changed')
    return expected['namespace']


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
    input_closed=False;failure=None
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
                    except BrokenPipeError:
                        # An early child exit is still a failed original
                        # command. Drain bounded output/status before cleanup
                        # rather than losing its nonsecret failure evidence.
                        input_closed=True;sel.unregister(f);f.close();continue
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
        if status.si_code!=os.CLD_EXITED or status.si_status!=0 or input_closed:
            failure=CommandFailure(status,output,errors,{'leader_pid':p.pid,'leader_reaped':False,'group_empty':False,
                'input_bytes':len(data),'input_sent_bytes':position,'input_closed_early':input_closed})
            raise failure
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
                if failure is not None:
                    failure.command['process']['leader_reaped']=True
                    failure.command['process']['group_empty']=True
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
FORMAT_SOURCES = {'src/protocol.rs','src/controller/database.rs','src/worker/handoff.rs'}
FORWARD_SOURCES = FORMAT_SOURCES | {'src/controller/application_schema.rs','src/controller/workflow_schema.rs',
    'src/controller/schema13-from12.sql','src/controller/schema14-from13.sql','src/worker/mod.rs'}
FORWARD_KIND = 'validated-controller12-worker9-to-controller16-worker12/1'
FORWARD_BEFORE = {'wire':11,'controller_schema':12,'worker_format':9}
FORWARD_AFTER = {'wire':14,'controller_schema':16,'worker_format':12}
LOSSLESS_KIND = 'validated-controller12-worker9-to-controller17-worker13/1'
LOSSLESS_AFTER = {'wire':15,'controller_schema':17,'worker_format':13}
LOSSLESS_SOURCES = FORWARD_SOURCES | {'src/controller/schema17-from16.sql','src/charter.rs',
    'src/controller/charters.rs','src/controller/charter_outputs.rs','src/worker/charter.rs',
    'src/worker/charter_output.rs','src/worker/mutations.rs'}


DIAGNOSTIC_KIND = 'validated-controller12-worker9-to-controller18-worker14/1'
DIAGNOSTIC_AFTER = {'wire':16,'controller_schema':18,'worker_format':14}
DIAGNOSTIC_SOURCES = LOSSLESS_SOURCES | {'src/controller/schema18-from17.sql',
    'src/controller/native_request_diagnostics.rs','src/controller/operations/reports.rs',
    'src/native_loop_contract.rs','src/native/diagnostic.rs','src/worker/native_loop.rs'}


# Schema19 is the receipt/audit vocabulary barrier for operator charter
# publication/adoption. It adds no SQL tables beyond the schema18 contract.
CHARTER_ADOPTION_KIND = 'validated-controller12-worker9-to-controller19-worker14/1'
CHARTER_ADOPTION_AFTER = {'wire':17,'controller_schema':19,'worker_format':14}
CHARTER_ADOPTION_SOURCES = DIAGNOSTIC_SOURCES | {'src/controller/charter_definitions.rs',
    'src/governance.rs','src/controller/governance.rs','src/controller/governance_history.rs',
    'src/controller/governance_validation.rs'}


# Wire18 disables fresh automatic memory notifications while preserving their
# historical receipt vocabulary. Storage/helper formats are unchanged; this is
# a distinct forward-only contract, not permission to select an older binary.
MEMORY_NOTICE_REMOVAL_KIND = 'validated-controller12-worker9-to-wire18-controller19-worker14/1'
MEMORY_NOTICE_REMOVAL_AFTER = {'wire':18,'controller_schema':19,'worker_format':14}
MEMORY_NOTICE_REMOVAL_SOURCES = CHARTER_ADOPTION_SOURCES | {'src/controller/memory_notifications.rs',
    'src/native/loop_step.rs','src/worker/memory_notice.rs','src/worker/codex.rs',
    'src/worker/codex_mcp.rs'}


# Schema20 fences typed requested PR effects/results without adding SQL tables.
# Worker15 admits its original pull-requests phase records; older captured
# worker roots do not qualify this new namespace or reader automatically.
PR_PUBLICATION_KIND = 'validated-controller12-worker9-to-wire19-controller20-worker15/1'
PR_PUBLICATION_AFTER = {'wire':19,'controller_schema':20,'worker_format':15}
PR_PUBLICATION_SOURCES = MEMORY_NOTICE_REMOVAL_SOURCES | {'src/pull_request.rs',
    'src/controller/pull_request.rs','src/controller/operations/effects.rs',
    'src/controller/operations/reports.rs','src/controller/repositories.rs',
    'src/worker/pull_request.rs','src/worker/pull_request_provider.rs',
    'src/worker/mutations.rs','src/worker/cleanup.rs'}


# Schema21 and worker16 fence accepted instruction-source vocabulary. The
# controller keeps the same additive SQL tables; original rows stay exact.
# This separate contract requires current source and retained-reader evidence.
INSTRUCTION_SOURCE_KIND = 'validated-controller12-worker9-to-wire20-controller21-worker16/1'
INSTRUCTION_SOURCE_AFTER = {'wire':20,'controller_schema':21,'worker_format':16}
INSTRUCTION_SOURCE_SOURCES = PR_PUBLICATION_SOURCES | {'src/delegation.rs',
    'src/execution_profile.rs','src/repository_view.rs','src/controller/operations.rs',
    'src/controller/collaboration_calls.rs','src/controller/repository_queries.rs',
    'src/mcp/mod.rs','src/mcp/client.rs'}


# This new boundary starts from an explicitly qualified wire20/schema21/worker16
# host set. Historical forward kinds retain their homogeneous schema12 baseline.
# Schema22 changes only the strict supplied-turn record vocabulary; retained SQL
# rows and the worker's original journals remain byte-preserved.
SUPPLIED_TURN_KIND = 'validated-controller21-worker16-to-wire21-controller22-worker17/1'
SUPPLIED_TURN_BEFORE = {'wire':20,'controller_schema':21,'worker_format':16}
SUPPLIED_TURN_AFTER = {'wire':21,'controller_schema':22,'worker_format':17}
SUPPLIED_TURN_BEFORE_SOURCES = FORMAT_SOURCES | {'src/controller/application_schema.rs'}
SUPPLIED_TURN_SOURCES = INSTRUCTION_SOURCE_SOURCES | {'src/controller/charters_supplied.rs',
    'src/controller/charters_supplied_turns.rs','src/worker/codex_charter.rs',
    'src/worker/codex_charter_turns.rs'}


# Agent-directory writes add one empty retirement table and a new strict wire
# vocabulary. This separate kind retains the same exact deployed20/21/16
# predecessor set. The already-qualified supplied-turn kind remains unchanged.
AGENT_DIRECTORY_KIND = 'validated-controller21-worker16-to-wire22-controller23-worker17/1'
AGENT_DIRECTORY_BEFORE = {'wire':20,'controller_schema':21,'worker_format':16}
AGENT_DIRECTORY_AFTER = {'wire':22,'controller_schema':23,'worker_format':17}
AGENT_DIRECTORY_SOURCES = SUPPLIED_TURN_SOURCES | {'src/controller/agents.rs',
    'src/controller/schema23-from22.sql'}
AGENT_RETIREMENT_COLUMNS = 'id definition_revision operation_id'


# Coordinated wire23/schema24/worker18 to wire24/schema25/worker19.
# The validated runtime changes only the controller schema marker and worker
# outer marker. Existing SQL tables/rows and inner worker records stay intact.
# Source membership binds the selected package; it does not create permission
# to rewrite retained state or bypass runtime typed admission.
COMPOSED_KIND = 'validated-controller24-worker18-to-wire24-controller25-worker19/1'
COMPOSED_BEFORE = {'wire':23,'controller_schema':24,'worker_format':18}
COMPOSED_AFTER = {'wire':24,'controller_schema':25,'worker_format':19}
COMPOSED_SOURCES = AGENT_DIRECTORY_SOURCES | {
    'src/controller/compatibility.rs',
    'src/controller/governance_completion.rs',
    'src/controller/governance_completion_actions.rs',
    'src/controller/identity_administration.rs',
    'src/controller/machine_resources.rs',
    'src/controller/metadata.rs',
    'src/controller/operations/supplied_outer.rs',
    'src/controller/schema24-from23.sql',
    'src/controller/sessions.rs',
    'src/controller/task_handoffs.rs',
    'src/controller/task_submission.rs',
    'src/governance_completion.rs',
    'src/limits/identity.rs',
    'src/limits/mod.rs',
    'src/limits/worker.rs',
    'src/machine_resources.rs',
    'src/protocol_identity.rs',
    'src/supplied_outer.rs',
    'src/worker/codex_retry.rs',
    'src/worker/completion.rs',
    'src/worker/machine_resources.rs',
    'src/worker/process.rs',
}


# This distinct boundary selects the recovered identity/read vocabulary. The
# runtime validates retained records before changing schema25/worker19 markers;
# this route permits no table/row or original worker-record rewriting.
RECOVERED_KIND = 'validated-controller25-worker19-to-wire25-controller26-worker20/1'
RECOVERED_BEFORE = {'wire':24,'controller_schema':25,'worker_format':19}
RECOVERED_AFTER = {'wire':25,'controller_schema':26,'worker_format':20}
RECOVERED_SOURCES = COMPOSED_SOURCES | {
    'src/controller/identity_limit_fields.rs',
    'src/controller/read_pages.rs',
    'src/controller/read_pagination.rs',
    'src/controller/read_retention.rs',
    'src/protocol_identity_limit_fields.rs',
    'src/protocol_read_pagination.rs',
    'src/read_page.rs',
    'src/read_retention.rs',
    'src/mcp/read_recovery.rs',
    'src/worker/identity_limit_fields.rs',
    'src/worker/read_pagination.rs',
    'src/worker/read_retention.rs',
    'src/worker/cleanup_reads.rs',
}


# The current union admits both schema26 read/identity and schema27 publication
# vocabulary. Only the actual installed24/25/19 predecessor is selected here;
# the runtime still owns all typed admission and marker writes. No SQL table,
# row, worker journal, or original effect rewrite is permitted by this route.
PUBLICATION_UNION_KIND = 'validated-controller25-worker19-to-wire27-controller28-worker22/1'
PUBLICATION_UNION_BEFORE = {'wire':24,'controller_schema':25,'worker_format':19}
PUBLICATION_UNION_AFTER = {'wire':27,'controller_schema':28,'worker_format':22}
PUBLICATION_UNION_SOURCES = RECOVERED_SOURCES | {
    'src/mcp/publication.rs',
    'src/controller/publication_requests.rs',
    'src/worker/publication_requests.rs',
    'src/controller/credits.rs',
}

def publication_union_forward(profile):
    transition=forward(profile)
    return transition is not None and transition.get('kind')==PUBLICATION_UNION_KIND



def recovered_forward(profile):
    transition=forward(profile)
    return transition is not None and transition.get('kind')==RECOVERED_KIND


def composed_forward(profile):
    transition=forward(profile)
    return transition is not None and transition.get('kind')==COMPOSED_KIND


def directory_forward(profile):
    transition=forward(profile)
    return transition is not None and transition.get('kind')==AGENT_DIRECTORY_KIND


def per_host_forward(profile):
    return supplied_forward(profile) or directory_forward(profile) or composed_forward(profile) or recovered_forward(profile) or publication_union_forward(profile)


def supplied_forward(profile):
    transition=forward(profile)
    return transition is not None and transition.get('kind')==SUPPLIED_TURN_KIND


def supplied_before_hosts(profile):
    transition=forward(profile);hosts=profile['hosts'];proofs=transition['before_hosts']
    before=PUBLICATION_UNION_BEFORE if publication_union_forward(profile) else RECOVERED_BEFORE if recovered_forward(profile) else COMPOSED_BEFORE if composed_forward(profile) else SUPPLIED_TURN_BEFORE
    if (not isinstance(proofs,dict) or not isinstance(hosts,list)
            or len({h['id'] for h in hosts})!=len(hosts)
            or set(proofs)!={h['id'] for h in hosts}):
        raise ValueError('supplied forward before-host inventory')
    controllers=[h for h in hosts if h['role']=='controller']
    if len(controllers)!=1 or transition['before_package_sha256']!=controllers[0]['old_artifact']['package_sha256']:
        raise ValueError('supplied forward controller baseline')
    for h in hosts:
        proof=proofs[h['id']]
        fields(proof,'formats artifact unit source_files','supplied forward before-host proof')
        if (canonical(proof['formats'])!=canonical(before)
                or canonical(proof['artifact'])!=canonical(h['old_artifact'])
                or canonical(proof['unit'])!=canonical(h['unit'])):
            raise ValueError('supplied forward exact host binding')
        fields(proof['artifact'],'format package_sha256 package_bytes manifest_sha256 source_commit source_tree object_format binary_sha256 binary_bytes target','supplied forward artifact')
        fields(proof['unit'],'path sha256','supplied forward unit')
        if (not isinstance(proof['source_files'],dict)
                or set(proof['source_files'])!=SUPPLIED_TURN_BEFORE_SOURCES
                or any(not _hex(v) for v in proof['source_files'].values())):
            raise ValueError('supplied forward baseline source inventory')
        if (not _hex(proof['artifact']['package_sha256'])
                or proof['artifact']['package_sha256']==transition['after_package_sha256']):
            raise ValueError('supplied forward predecessor package identity')


def verify_before_manifest(profile,host,raw):
    if not per_host_forward(profile):return
    format_contract(profile)
    old=host['old_artifact'];m=strict(raw)
    if digest(raw)!=old['manifest_sha256']:raise ValueError('supplied forward installed manifest identity')
    source=m.get('source',{});binary=m.get('files',{}).get('bin/borg-runtime',{})
    observed={'format':m.get('format'),'source_commit':source.get('commit'),
        'source_tree':source.get('tree'),'object_format':source.get('object_format'),
        'binary_sha256':binary.get('sha256'),'binary_bytes':binary.get('size'),'target':binary.get('target')}
    if canonical(observed)!=canonical({k:old[k] for k in observed}):
        raise ValueError('supplied forward installed artifact/source identity')
    for name,sha in forward(profile)['before_hosts'][host['id']]['source_files'].items():
        if source.get('files',{}).get(name,{}).get('sha256')!=sha:
            raise ValueError('supplied forward installed baseline source binding')


def forward(profile):
    return profile['formats'].get('forward')


def format_sources(profile):
    transition=forward(profile)
    if transition is None:return FORMAT_SOURCES
    return PUBLICATION_UNION_SOURCES if publication_union_forward(profile) else RECOVERED_SOURCES if recovered_forward(profile) else COMPOSED_SOURCES if composed_forward(profile) else AGENT_DIRECTORY_SOURCES if transition['kind']==AGENT_DIRECTORY_KIND else SUPPLIED_TURN_SOURCES if transition['kind']==SUPPLIED_TURN_KIND else INSTRUCTION_SOURCE_SOURCES if transition['kind']==INSTRUCTION_SOURCE_KIND else PR_PUBLICATION_SOURCES if transition['kind']==PR_PUBLICATION_KIND else MEMORY_NOTICE_REMOVAL_SOURCES if transition['kind']==MEMORY_NOTICE_REMOVAL_KIND else CHARTER_ADOPTION_SOURCES if transition['kind']==CHARTER_ADOPTION_KIND else DIAGNOSTIC_SOURCES if transition['kind']==DIAGNOSTIC_KIND else LOSSLESS_SOURCES if transition['kind']==LOSSLESS_KIND else FORWARD_SOURCES


def format_contract(profile):
    f=profile['formats'];transition=forward(profile)
    if transition is None:
        if 'forward' in f: raise ValueError('forward transition cannot be null')
        if f['before']!=f['after']: raise ValueError('incompatible state formats: qualified forward transition required')
        return
    fields(transition,'kind before_package_sha256 after_package_sha256 evidence'+(' before_hosts' if per_host_forward(profile) else ''),'forward transition')
    expected=PUBLICATION_UNION_AFTER if publication_union_forward(profile) else RECOVERED_AFTER if recovered_forward(profile) else COMPOSED_AFTER if composed_forward(profile) else AGENT_DIRECTORY_AFTER if directory_forward(profile) else SUPPLIED_TURN_AFTER if supplied_forward(profile) else FORWARD_AFTER if transition['kind']==FORWARD_KIND else LOSSLESS_AFTER if transition['kind']==LOSSLESS_KIND else DIAGNOSTIC_AFTER if transition['kind']==DIAGNOSTIC_KIND else CHARTER_ADOPTION_AFTER if transition['kind']==CHARTER_ADOPTION_KIND else MEMORY_NOTICE_REMOVAL_AFTER if transition['kind']==MEMORY_NOTICE_REMOVAL_KIND else PR_PUBLICATION_AFTER if transition['kind']==PR_PUBLICATION_KIND else INSTRUCTION_SOURCE_AFTER if transition['kind']==INSTRUCTION_SOURCE_KIND else None
    before=PUBLICATION_UNION_BEFORE if publication_union_forward(profile) else RECOVERED_BEFORE if recovered_forward(profile) else COMPOSED_BEFORE if composed_forward(profile) else AGENT_DIRECTORY_BEFORE if directory_forward(profile) else SUPPLIED_TURN_BEFORE if supplied_forward(profile) else FORWARD_BEFORE
    if expected is None or canonical(f['before'])!=canonical(before) or canonical(f['after'])!=canonical(expected):
        raise ValueError('incompatible or unsupported forward transition')
    for key in ('before_package_sha256','after_package_sha256'):
        value=transition[key]
        if not isinstance(value,str) or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('forward package identity')
    if transition['before_package_sha256']==transition['after_package_sha256']: raise ValueError('forward package identities must differ')
    fields(transition['evidence'],'controller worker retained_state','forward qualification evidence')
    for item in transition['evidence'].values(): fields(item,'path sha256','forward qualification pin')
    if set(f['source_files'])!=format_sources(profile): raise ValueError('forward migration source inventory')
    if per_host_forward(profile):supplied_before_hosts(profile)


def forward_evidence(profile):
    # These are the existing reviewed qualification records selected by the
    # protected profile, not a new authority, executable, or generic PASS parser.
    transition=forward(profile)
    if transition:
        for item in transition['evidence'].values(): pin(item,profile['limits']['receipt_bytes'])


# Exact additive table/column contract of schema13-from12.sql and
# schema14-from13.sql. The package binds those source bytes; runtime startup
# remains the sole schema validator/migrator. No SQL is run on original state here.
FORWARD_TABLES = {
    'workflow_questions':'id revision created_product_revision recipient status record_kind record_id body',
    'workflow_answers':'id question_id product_revision body',
    'workflow_question_actions':'question_id question_revision product_revision body',
    'workflow_documents':'id revision created_product_revision body',
    'workflow_document_versions':'document_id version body',
    'workflow_comments':'id record_kind record_id created_product_revision body',
    'workflow_reviews':'id revision created_product_revision reviewer status body',
    'workflow_review_decisions':'id review_id body',
    'workflow_review_withdrawals':'review_id body',
    'workflow_task_state':'task_id revision body',
    'workflow_waits':'id task_id resolver_kind resolver_id closed body',
    'workflow_wait_closures':'wait_id body',
    'workflow_wait_consumptions':'wait_id task_id attempt_id body input_ack',
    'workflow_events':'product_revision ordinal body targets',
    'charter_sessions':'id revision body',
    'charter_session_versions':'id revision body operation_id',
    'charter_requests':'id session_id attempt_id request_index kind body',
    'charter_replies':'id request_id body',
    'charter_closures':'request_id body',
    'charter_compaction_results':'request_id body',
    'governance_rules':'id revision body operation_id',
    'governance_configurations':'revision body operation_id qualification',
    'governance_state':'singleton body',
    'governance_holds':'scope body',
    'native_request_coordination':'kind key body',
}
LOSSLESS_TABLES = {'charter_output_chunks':'request_id chunk_index operation_id reference body'}
DIAGNOSTIC_TABLES = {'native_request_diagnostic_credits':'attempt_id request_index reservation_id diagnostic_id consumed'}


def controller_retained(profile,before,after):
    if per_host_forward(profile):format_contract(profile)
    if before['schema']!=profile['formats']['before']['controller_schema'] or after['schema']!=profile['formats']['after']['controller_schema']:
        raise ValueError('controller transition schema differs')
    expected=strict(canonical(before['tables']))
    if forward(profile) and not per_host_forward(profile):
        kind=forward(profile)['kind']
        tables={**FORWARD_TABLES,**(LOSSLESS_TABLES if kind in (LOSSLESS_KIND,DIAGNOSTIC_KIND,CHARTER_ADOPTION_KIND,MEMORY_NOTICE_REMOVAL_KIND,PR_PUBLICATION_KIND,INSTRUCTION_SOURCE_KIND) else {}),
                **(DIAGNOSTIC_TABLES if kind in (DIAGNOSTIC_KIND,CHARTER_ADOPTION_KIND,MEMORY_NOTICE_REMOVAL_KIND,PR_PUBLICATION_KIND,INSTRUCTION_SOURCE_KIND) else {})}
        if set(expected)&set(tables):raise ValueError('historical schema contains forward tables')
        for table,column in [('sessions','workflow_delivery'),('receipts','governance')]:
            original=expected[table]
            if column in original['columns']:raise ValueError('historical schema contains forward column')
            original['columns'].append(column)
            original['rows']=[row+[None] for row in original['rows']]
        settings=expected['settings']
        if settings['columns']!=['name','value'] or any(row[0]=='workflow_revision' for row in settings['rows']):
            raise ValueError('historical workflow revision differs')
        settings['rows'].append(['workflow_revision','0'])
        settings['rows'].sort(key=canonical)
        for table,columns in tables.items():expected[table]={'columns':columns.split(),'rows':[]}
    if directory_forward(profile):
        if 'agent_retirements' in expected:raise ValueError('predecessor already contains agent retirements')
        expected['agent_retirements']={'columns':AGENT_RETIREMENT_COLUMNS.split(),'rows':[]}
    if after['tables']!=expected: raise ValueError('restart changed original logical controller state or allowed additive delta')


def validate(profile):
    fields(profile, PROFILE_FIELDS+(' producer_transport' if 'producer_transport' in profile else ''), 'rollout profile')
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
    fields(profile['formats'], 'before after source_files'+(' forward' if 'forward' in profile['formats'] else ''), 'formats')
    for name in ('before','after'):
        fields(profile['formats'][name], 'wire controller_schema worker_format', 'format tuple')
        for value in profile['formats'][name].values(): positive(value, 'format value')
    format_contract(profile)
    fields(profile['modules'], 'producer verifier common host adapter', 'module inventory')
    if not isinstance(profile['hosts'],list) or not 0<len(profile['hosts'])<=profile['limits']['hosts']: raise ValueError('host count bound')
    ids=set(); controllers=[]
    for h in profile['hosts']:
        fields(h, HOST_FIELDS+(' stopped_predecessor' if 'stopped_predecessor' in h else ''), 'host')
        if 'stopped_predecessor' in h:
            proof=h['stopped_predecessor']
            if h['role']!='worker' or not forward(profile):raise ValueError('stopped predecessor requires an explicit forward worker')
            fields(proof,'service owner_identity state_sha256','stopped predecessor')
            fields(proof['service'],'ActiveState SubState MainPID ControlGroup FragmentPath DropInPaths NeedDaemonReload InvocationID User','stopped service')
            if any(not isinstance(v,str) for v in proof['service'].values()):raise ValueError('stopped service value')
            if (proof['service']['ActiveState'],proof['service']['SubState'],proof['service']['MainPID'])!=('inactive','dead','0'):raise ValueError('stopped predecessor state')
            if proof['service']['FragmentPath']!=h['unit']['path'] or proof['service']['DropInPaths'] or proof['service']['NeedDaemonReload']!='no':raise ValueError('stopped predecessor unit')
            identity=proof['owner_identity']
            if not isinstance(identity,list) or len(identity)!=5 or any(type(v) is not int or v<0 for v in identity):raise ValueError('stopped owner identity')
            sha=proof['state_sha256']
            if not isinstance(sha,str) or len(sha)!=64 or any(v not in '0123456789abcdef' for v in sha):raise ValueError('stopped state digest')
        if not isinstance(h['id'],str) or not h['id'] or h['id'] in ids: raise ValueError('host identity')
        ids.add(h['id'])
        if h['role'] not in ('controller','worker'): raise ValueError('host role')
        if h['role']=='controller': controllers.append(h)
        if not isinstance(h['service'],str) or '/' in h['service'] or not h['service'].endswith('.service') or h['service'].startswith('-'): raise ValueError('service identity')
        fields(h['transport'], 'kind ssh_config alias', 'transport')
        if h['transport']['kind'] not in ('local','ssh'): raise ValueError('transport kind')
        if h['transport']['kind']=='local' and (h['transport']['ssh_config'] is not None or h['transport']['alias'] is not None): raise ValueError('local transport fields')
        if h['transport']['kind']=='ssh' and (not isinstance(h['transport']['alias'],str) or not h['transport']['alias'] or h['transport']['alias'].startswith('-')): raise ValueError('ssh alias')
        fields(h['state'], 'root format_file database policy'+(' retained_policy' if 'retained_policy' in h['state'] else ''), 'state')
        if 'retained_policy' in h['state']:
            fields(h['state']['retained_policy'],'path sha256','retained worker policy')
            if h['role']!='worker' or Path(h['state']['retained_policy']['path'])!=Path(h['state']['root'])/'limits.json':raise ValueError('retained worker policy path')
        fields(h['api'], 'host port worker_id', 'host API')
        if h['api']['host'] not in ('127.0.0.1','::1') or type(h['api']['port']) is not int or not 0<h['api']['port']<65536: raise ValueError('host API endpoint')
        for k in ('helper_uid','service_uid'):
            if type(h[k]) is not int or h[k]<0: raise ValueError('host UID')
        if h['incarnation'] != ('systemd_invocation' if h['role']=='worker' else None): raise ValueError('unsupported incarnation convention')
        if h['role']=='worker' and (not h['api']['worker_id'] or h['state']['database'] is not None): raise ValueError('worker state/API binding')
        if forward(profile) and h['role']=='worker' and Path(h['state']['format_file'])!=Path(h['state']['root'])/'format.json':raise ValueError('forward worker marker path')
        if h['role']=='controller' and (h['api']['worker_id'] is not None or h['state']['format_file'] is not None): raise ValueError('controller state/API binding')
        for k in ('profile_path','state_root','release_root','owner_lock'):
            if not Path(h[k]).is_absolute() or '..' in Path(h[k]).parts: raise ValueError('absolute host path required')
        if Path(h['state_root']) == Path(h['state']['root']) or Path(h['state']['root']) in Path(h['state_root']).parents: raise ValueError('rollout journal must be outside runtime state')
        if not isinstance(h['preserve_files'],list):raise ValueError('preserved file inventory')
        for item in h['preserve_files']:fields(item,'path sha256','preserved file pin')
        if not isinstance(h['helpers'],list) or not isinstance(h['preserve_metadata'],list) or len(h['helpers'])+len(h['preserve_metadata'])>profile['limits']['state_entries']: raise ValueError('host preserved paths')
    if len(controllers)!=1: raise ValueError('one controller target required')
    if 'producer_transport' in profile:
        transport=profile['producer_transport'];fields(transport,'kind host_id config_path','producer transport')
        host=controllers[0]
        if transport['kind']!='ssh' or transport['host_id']!=host['id'] or host['transport']['kind']!='ssh':raise ValueError('producer transport must use the configured controller SSH route')
        path=Path(transport['config_path'])
        if not path.is_absolute() or '..' in path.parts:raise ValueError('absolute remote producer configuration required')
    return profile


def load_profile(path, expected_sha, profile_bytes):
    raw=read(path,profile_bytes,trusted=True)
    if digest(raw)!=expected_sha: raise ValueError('rollout profile changed')
    p=validate(strict(raw))
    forward_evidence(p)
    for item in p['modules'].values(): pin(item,p['limits']['module_bytes'])
    for item in p['transport_tools'].values(): pin(item,p['limits']['module_bytes'])
    pin(p['python'],p['limits']['module_bytes'])
    if digest(Path('/proc/self/exe').read_bytes())!=p['python']['sha256']: raise ValueError('selected Python identity differs')
    protected(p['state_root'],directory=True)
    return p


def verify_package(profile, host, subject):
    format_contract(profile);forward_evidence(profile)
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
    if set(profile['formats']['source_files'])!=format_sources(profile): raise ValueError('format source inventory')
    if forward(profile):
        transition=forward(profile)
        if (transition['after_package_sha256']!=a['package_sha256']
                or transition['before_package_sha256']!=subject['installed_package_sha256']
                or not per_host_forward(profile) and transition['before_package_sha256']!=host['old_artifact']['package_sha256']):
            raise ValueError('qualified forward package binding differs')
    return raw,m
