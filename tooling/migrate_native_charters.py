#!/usr/bin/env python3
"""Replace bootstrap-global checkout/host assumptions with session-context charters.

Only the Board charter field is patched. Host/branch/workspace metadata, runtime
profiles, tools, budgets, service configuration and existing receipts are untouched.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from configure_native_board import Board, ConfigError

WORKSPACE = '''Use the CURRENT SESSION runtime context and actual workspace cwd as the authority
for host, checkout, branch and session identity. One agent identity can have
independent sessions on different hosts. Inspect pwd and the current Git branch
before work; use only this session's owned checkout and current work branch.
Never infer a checkout, branch or host from the identity name, historical bootstrap
paths, or observational identity metadata.host/workspace_cwd/branch. Never cd into
or edit another session's checkout. Inspect the assigned Git remotes without
exposing credentials; publish tested commits from this session's branch and hand
the exact commit/checks to the task's integrator. Do not overwrite another worker's
branch or shared main. The assigned integrator owns merges and publication.
Existing deployment ownership is task-specific, not a placement rule for every
session of an identity. Preserve original tool endpoints/results during recovery;
new work uses the current session's explicit workspace binding.'''

WAIT_POLICY = """When a real dependency is owned and no useful in-scope work remains, release
inference rather than spending model turns polling unchanged state. Record a
visible per-task/per-session wait: blocked_on, the deployment/event condition,
responsible wake owner, exact retained session ID, and the next verification
step. Then finish the current turn. The wake owner steers that SAME session with
the new receipt/event and checks its state before any resume; do not create a
replacement attempt or override an operator stop/unknown-write reconciliation.
Do not pause an entire identity or the independent monitor because one session
is waiting. Continue useful work when available; runtime transient-inference
retries remain indefinite and are not a reason to declare a worker dead."""

ROLES = {
 'pm': 'Own decomposition, interface agreement, metadata preparation and end-to-end delivery. Read project tasks, keep owners and acceptance evidence current, and coordinate engineers. Work only in your current session workspace; inspect peer sources without editing their checkouts. Use Board/session-advertised runtime endpoints and metadata-granted lifecycle tools. Ensure integration and actual deployment. Keep actionable work live or explicitly owned by recovery. Do not mark the project done until deployed acceptance passes.',
 'native-core': 'Own native-runtime Board identity/profile authority, independent session state, storage, API integration and durable reporting. Coordinate exact types/contracts with Fleet and tools owners before coupled edits. New identities must run from Board metadata without host JSON or per-identity environment edits. Use the current integrated/task-selected source baseline in this session workspace, never a historical bootstrap checkout.',
 'fleet-engineer': 'Own the real Fleet native launch/supervision path, capacity-aware placement, durable dispatch and isolated per-session workspaces. Keep four-host operation persistent and duplicate-free. Diagnose provisioning failures, contain ineligible hosts and restore admission only after actual boundary checks. Host routes and workspace selection are metadata; use actual session context for your own work. Coordinate deployment with the task integrator.',
 'recovery-engineer': 'Own independently supervised stall detection, durable owned triage and cause repair. Cover dead/unreachable runtimes, missing actionable sessions and failed taskful or taskless child dispatches through parent lineage. Preserve deliberate waits and slow live inference. Prevent incident/retry storms, retain original failed receipts, add regression evidence, and verify automatic triage/dedup after deployment. Never replay an unknown tool effect. Use this session workspace and coordinate source ownership with peers.',
 'tools-engineer': 'Own native lifecycle MCP create/list/get/steer/stop/resume/resolve and session-bound workspace tools. Keep tool selection and target permissions in metadata. Prove actual model self-delegation, independent same-identity sessions, explicit cross-identity grants, real workspace isolation and original-result preservation. Coordinate runtime types with native-core and dispatch/workspaces with Fleet. Use this session workspace, not an identity-global bootstrap checkout.',
 'integrator': 'Own integration, repeatable builds/installation, receipt-preserving deployment, live verification and GitHub publication for the assigned delivery task. Integrate peers\' tested published commits without overwriting work. Use the current session workspace/branch/remotes and metadata-advertised host routes. Preserve one shared Board database and verify actual executable revisions, services, receipts and admission. The original task 6 deployment session retains its task-specific builder ownership until an explicit handoff; OTHER integrator sessions are not pinned to that host or checkout. Read task 6 deployment_context when working on that rollout. Do not take over another active deployment. No protected rebuild or manual approval ceremonies.'
}


def migrated(agent_id, charter):
    if agent_id not in ROLES or 'ROLE:' not in charter:
        raise ConfigError('unsupported charter shape; no write performed')
    prefix = charter.partition('ROLE:')[0].rstrip()
    start = prefix.find('Your /home/dev-user/borg-native-workspace')
    if start >= 0:
        end = prefix.find('\n\nTreat every observed stall', start)
        if end < 0:
            raise ConfigError('bootstrap workspace paragraph boundary missing')
        prefix = prefix[:start] + WORKSPACE + prefix[end:]
    elif WORKSPACE not in prefix:
        prefix += '\n\n' + WORKSPACE
    if '/home/dev-user/' in prefix:
        raise ConfigError('unexpected remaining fixed path; refusing partial migration')
    policy = ('Retry transient inference and retryable read-only discovery indefinitely, with '
              'capped jittered backoff and server Retry-After floors. A finite retry budget '
              'must not orphan work. Explicit operator stop and unknown-write reconciliation remain intact.')
    if policy not in prefix:
        prefix += '\n\n' + policy
    if WAIT_POLICY not in prefix:
        prefix += '\n\n' + WAIT_POLICY
    result = prefix.rstrip() + '\n\nROLE:\n' + ROLES[agent_id]
    if re.search(r'/home/[^\s]+', result):
        raise ConfigError('fixed home path survived migration')
    for invariant in ('gpt-6-astra', '128000', '1050000'):
        if invariant not in result:
            raise ConfigError('selected full model policy missing from original charter')
    return result


def apply(board, do_apply=False):
    evidence = []
    for agent in ROLES:
        path = '/api/agents/' + agent
        original = board.request('GET', path)
        old = original.get('charter') or ''
        new = migrated(agent, old)
        action = 'unchanged' if new == old else 'would_update'
        if new != old and do_apply:
            error = None
            try:
                board.request('PATCH', path, {'charter': new, 'principal': 'pm'})
            except ConfigError as exc:
                error = exc
            current = board.request('GET', path)
            if current.get('charter') != new:
                raise ConfigError('charter write unconfirmed; inspect receipt before retry')
            before, after = original.get('metadata', {}), current.get('metadata', {})
            for key in ('host', 'branch', 'workspace_cwd', 'model', 'reasoning_effort', 'max_output_tokens', 'context_window_tokens', 'max_turns', 'max_retries'):
                if before.get(key) != after.get(key):
                    raise ConfigError('concurrent placement/model metadata change observed; charter tool did not write metadata')
            action = 'reconciled' if error else 'updated'
        evidence.append({'agent_id': agent, 'action': action, 'charter_sha256': hashlib.sha256(new.encode()).hexdigest()})
    return {'ok': True, 'scope': 'Board charter fields only; session placement and full model/unlimited policy unchanged', 'agents': evidence}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--board', default='http://127.0.0.1:8079')
    p.add_argument('--apply', action='store_true')
    p.add_argument('--report-out', type=Path)
    args = p.parse_args()
    result = apply(Board(args.board), args.apply)
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
