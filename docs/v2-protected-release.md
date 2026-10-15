# Protected release qualification

This local implementation adds a restricted producer endpoint, durable release-stage
records, a fixed package-check producer, local Git compare-and-swap, and exact
artifact/target human authorization. It does not configure a production checker,
repository provider, deployment target or host accounts. The [rollout adapter](v2-protected-rollout.md) implements the host-effect path but does not install its protected operator profile or grant access.

Ordinary `PrepareRelease` remains a claim. Its candidate body, revision and
original reply stay unchanged. `GetProtectedRelease` returns a separate read-only
view of recorded checks, grants and current actions. Presence of a record is not
current eligibility. The browser has no protected mutation controls.

## Accounts and configuration

The controller accepts `--protected-release-config /absolute/operator/config.json`.
Without this option, protected operations remain unavailable. The configuration
is closed JSON corresponding to `protected_release::Config`: socket path; actual
worker UIDs; distinct checker, repository, human and rollout producer UIDs/IDs;
policy and recipe digest; mandatory check names and finite freshness bound;
canonical repository identity and local bare Git directory; exact target ref;
deployment target/revision and current installed package digest; retained-record
capacity. A missing role cannot produce that role's receipt. No default accepting
producer or target is supplied.

The operator creates a protected socket directory and read-protected configuration
outside worker write access. The runtime refuses aliased or broadly writable
configuration, worker-owned configuration/socket parent, overlapping worker and
producer identities, and an existing socket. It does not unlink a stale socket.
The kernel peer UID selects a configured role before request decoding; caller JSON
cannot select a role. Socket mode is0660. Actual group/ACL routing and account
separation are deployment inputs. The local same-UID tests qualify protocol and
refusal behavior, not cross-account protection. No account or permission is
created by this implementation.

A framed JSON request has private `revision:1`, stable `operation_id`, and typed
`command`. Frame, connection, exchange, subprocess and operation-identity bounds come from the selected `interface.protected_*` metadata. The shipped seed uses 128KiB request/reply frames, 16 concurrent handlers, a 60-second exchange bound and 240-byte public operation IDs. These are metadata values, not fixed implementation limits. `finish:` is reserved for controller adapter settlement.
The internal start, finish and observation operations are rejected on the socket.

## Exact subjects and stages

A subject names the existing release candidate and every `ReleaseArtifact` field,
canonical source repository, exact source commit/tree, configured target branch,
expected old branch commit, policy, deployment target/revision and expected
installed package. Compiler target is not deployment target.

A checker record binds that subject, selected recipe, observed check results,
cleanup, observation time and expiration. Failed or incomplete checks remain
visible but cannot authorize action. A human grant binds the same subject and a
finite expiration. Only the configured human UID can grant; human authority
cannot replace checks. Current roles, policy, candidate, freshness and target are
checked at preparation and again at dispatch admission.

Prepare records a merge or rollout action. Merge execution durably admits the
original action before external work and returns that original admission receipt
on the first response and every exact retry. It does not substitute a later effect
observation into that receipt. `GetAction` provides current effect state.
`ReconcileMerge` performs bounded readback and records its own immutable outcome;
retrying its ID returns that original observation even if later observations
change current state. An operation ID cannot change input or producer. Lost
transport outcomes require the original receipt/action lookup, not a fresh action.

The repository adapter supports a configured local bare Git repository and exact
fast-forward only. It verifies current base, ancestor relation and source tree,
then executes `update-ref` with the original expected-old commit. It never silently
refreshes the base. It disables hooks, replacement objects, remote protocols,
global/system config and lazy fetch. Dispatch authorization is checked again
immediately before CAS. Reconciliation reads the exact operation/base/source
reflog witness; a missing witness is unknown, not proof of nonexecution. It never
reissues CAS to resolve uncertainty. A successful historical witness does not
claim the branch is still at that commit. Repository-side target protection is
still required; ordinary worker push rights must not bypass it. No PR provider or
non-fast-forward integration policy is selected.

Rollout preparation and `StartRollout` require current checks plus the exact human
grant. The returned dispatch deadline is the earliest applicable expiry. A future
operator-owned rollout adapter must recheck target/current installed identity and
honor this deadline immediately before its effect. This batch does not run such
an adapter. `ObserveRollout` records matching installed-artifact readback or
unknown outcome, retaining an already dispatched effect even after a grant
expires. Merge never launches rollout. New actions require new eligibility;
historical observations grant nothing.

Prepared/started actions reserve terminal receipt capacity so unrelated producer
traffic cannot consume their final observation slot. Stage response allocation
is checked incrementally. Store mutation and receipt commit share the controller's
single SQLite owner; repository processes run outside transactions.

## Fixed package producer and operator client

Install exact `tooling/protected_release_v2.py` and `tooling/package_v2.py` together
under a protected path. The producer embeds the existing verifier digest. Its
configuration contains only `socket`, `server_uid`, `producer_uid`, `policy`,
`recipe_sha256`, and `valid_for_ms`. The client checks the server's kernel UID.
It runs with the selected Python interpreter and no site customization:

```sh
/usr/bin/python3 -I -S -B tooling/protected_release_v2.py check-package \
  --config /protected/checker.json --input /protected/subject.json \
  --package /retained/borg-runtime.tar --operation-id STABLE_CHECK_ID \
  --receipt /protected/fresh-check-receipt
```

The producer first copies the bounded input once into its protected receipt directory, then runs only the fixed existing package verifier over that copy and compares
all actual descriptor fields. It emits only `package_integrity:true`. It never
executes a package binary or candidate-writable compiler/test script as a checker,
and never manufactures behavior-check results. The separate `check-behavior` mode
described below joins an installed protected recipe to isolated candidate execution
and independently protected result interpretation. Configuring only package
integrity still does not establish full release acceptance.

`request` sends the typed grant/prepare/execute/reconcile/start/observe/read commands
from `--input`. It cannot submit checker assertions or internal operations.
`lookup --original-receipt DIR` reads and verifies the original committed receipt;
`retry --original-receipt DIR` explicitly resends the exact retained request without
recomputing the check, changing its identity or refreshing any subject. All modes
require a fresh `--receipt` directory. Retained request and receipt files are explicitly private0600, independent of umask. Requests are synced before transport; lost
responses remain uncertain. No automatic retry or deployment is performed.

## Protected behavioral producer

`check-behavior` uses the same subject, immutable controller Check, socket roles and
human grant rules. Install the producer, its digest-pinned
`protected_behavior_v2.py`, the unchanged verifier and separately reviewed oracle
programs under protected paths. The behavioral helper adds no runtime dependency,
public mutation, schema or wire change. It does not run a merge or rollout.

```sh
/usr/bin/python3 -I -S -B /protected/tooling/protected_release_v2.py check-behavior \
  --config /protected/checker.json --recipe /protected/behavior-recipe.json \
  --input /protected/subject.json --package /retained/borg-runtime.tar \
  --operation-id STABLE_CHECK_ID --receipt /protected/fresh-behavior-receipt
```

The closed recipe has revision1, the supervisor source digest, the protected
controller-configuration path, dedicated execution UID/GID, protected execution root, preconfigured exclusive candidate/oracle cgroup
paths, exact policy/deployment target/revision/installed-package identity, required
check names and bounded steps. Each step identifies a fixed candidate launcher
and SHA256 (or the exact artifact binary), bounded arguments, a separately fixed
oracle path and SHA256, deadline and output-byte bound. Every required check has
exactly one step. `package_integrity` is reserved for the existing verifier.
Only artifact binary, workspace, source commit and source tree substitutions are
accepted. The recipe digest is the controller's configured recipe identity; an
agent cannot replace it by choosing a file or check name. The producer reads the
existing controller configuration, verifies its exact socket/policy/recipe/target,
requires exactly its mandatory checks including package integrity, and binds the
observed configuration bytes through completion. The execution UID must belong to
its worker set and cannot equal any checker/repository/human/rollout producer UID
or the server UID. The caller must be its configured checker. The operator must
supply the actual current controller configuration, not a guessed parallel file;
this client does not invent an API attesting a running controller's configuration.

The current local launcher requires an operator-configured privileged supervisor
and a distinct unprivileged execution UID/GID. It does not create either account,
change host permissions or create cgroups. All recipe, launcher, oracle, helper,
execution-root and interpreter paths require protected owners and ancestors; a
candidate-writable parent is refused. The execution account must initially have
no processes; the exclusive cgroups must be empty. Both cleanup-domain inode
identities are retained before launch. Candidate admission enters its cgroup,
clears supplementary groups, drops identity and sets no-new-privileges before
executing any candidate command. Children receive only the explicit minimal
environment, no inherited descriptors and no checker credentials. The candidate
cannot write its root-owned verified artifact, recipe, oracle or receipt paths.
Actual filesystem/IPC/service permissions must also prevent this execution
account from bypassing containment through an external launcher; account names
and cgroup paths alone do not prove that host boundary.

Candidate exit and output are bounded observations, never Check payloads.
After the process group and every descendant in its cgroup have stopped, a
separately protected oracle interprets the private observation bound to the exact
subject, recipe, check and artifact. That trusted oracle is installed source, not
candidate source or a candidate-generated result file. It runs with checker
permissions and therefore requires review of its own dependencies and behavior:
it must not execute candidate code with its authority, accept a candidate's
self-reported pass, grant rollout authority, or substitute another artifact.
Its response has exact subject/recipe/check identities, an explicit result and
bounded actual observations; an unknown, failed or mismatched result is not
success. Candidate failure remains failed even if the oracle reports true.
The oracle has its own recorded cleanup domain. Cleanup uncertainty emits no
Check. A valid failed check can be recorded truthfully and remains ineligible.

The supervisor stores a pending intent before launch and a stable operation
marker under its protected execution root. A marker is never automatically
removed, so repeating the same check identity cannot run candidate code again.
The retained marker inventory is bounded to10,000; exhausting it refuses new
execution and requires an explicit retention disposition. There is no automatic
eviction or identity reuse. A lost response after submission uses the existing
`lookup` or exact `retry` path without refreshing the check time. A crash between
execution and request persistence leaves an unknown outcome, not permission to
rerun or invent a check.

`recover-behavior --original-receipt DIR --recipe FILE` takes the same producer
configuration and a fresh receipt directory. It checks the retained pending
subject, recipe, job lock and both cleanup-domain inode identities, then stops
only that recorded unfinished job. It never cleans another job, reexecutes a
candidate or creates a Check. A successful cleanup returns exit2 with
`execution_stopped_outcome_unknown`; the original controller receipt still needs
reconciliation if a request was persisted. Unknown cleanup retains the lock.
Interrupted admission before a complete binding, a changed cleanup domain or
lost journal is refused for explicit operator reconciliation. Never clear such
a lock merely to retry a new check.

The implementation does not select a production mandatory recipe. The applicable
v2 release checks still include the supported task journey, controller-restart
and worker-loss recovery, stale-owner rejection, no repeated settled external
effect, and review of uncovered critical paths. The recipe must bind their actual
exercised sources and artifact to protected oracles with real acceptance facts;
ordinary candidate test output cannot satisfy these by assertion. Mandatory build
validation likewise needs the selected toolchain/recipe and source identity, with
candidate compilation outside evidence authority. Missing configured recipes or
host protections remain blockers, rather than synthetic successful checks.

Local qualification uses actual bounded subprocesses with a clearly substituted
same-UID process adapter and fixed fixture oracle; it validates orchestration,
negative results, receipt retry and cleanup/recovery refusal. It does not establish
production account separation, actual cgroup-v2 authority, all descendant behavior
on a deployment host or any of the full-system release criteria above. The real
launcher separately refuses the ordinary local UID, and tests retain that refusal.

## Compatibility and installation proposal

The following wire 4/schema 8 procedure and its explicit 7-to-8 migration flag describe the earlier protected-release executable and retained qualification only. They are not startup options or migration instructions for the current metadata-limits representation. Current wire 8/schema 12/worker format 9 and worker policy-binding refusal are described in the [limits guide](v2-limits.md); no new state replacement or downgrade is authorized here.

The public worker protocol remains wire4 and worker journal6; new ordinary
operations are bounded read projections only: `ListAgents`, `GetAgent`,
`ListAgentTasks`, `GetProtectedRelease`. Older clients can continue existing
operations. New readers require the updated server; an old server rejects unknown
operations. Agents are existing durable identities, distinct from templates and
worker hosts. Task pages use current assigned identity; submitted definition
revisions and historical requested identities remain unchanged.

Controller schema8 adds only `protected_receipts` and `protected_actions`. Existing
schema7 is refused by default. Explicit `--migrate-release-schema 7-to-8` first
validates the existing store/identity/helper binding and reconciles the existing
owner, then creates both tables and changes the version in one SQLite transaction.
Failure before commit rolls back both tables and version. Existing task, receipt,
candidate and settings bytes are not rewritten. Unsupported/corrupt input is
refused. Downgrade is not automatic: a schema7 executable refuses schema8.

Before an actual upgrade, stop the original owner and preserve a consistent copy
of the database, its exact sidecar set and owned helper/lock/worker state. Review
the exact target executable and migration flag; do not initialize a new empty
controller or overlay fixture state onto real history.

A separate private qualification in
`artifacts/v2/build-60156282/schema7-rollback-qualification-8w1hoiw0/receipt.json`
used the retained schema7 executable and the protected-release batch's schema8
executable. It preserved accepted, queued and failed/uncertain task records,
receipts and a claimed release candidate through explicit upgrade and stopped
rollback. The captured nonempty WAL was restored with the database at its original
path, preserving the original database, registry and lock inodes without editing
`owner.binding`. A relocated ordinary copy was refused. The qualification admits
no new work after upgrade and before rollback; restoring that pre-upgrade snapshot
after newer commits would lose history and is not qualified. It does not qualify
power-loss recovery, interrupted restoration, worker-state restoration or live
host cutover. A later final package needs its own exact input binding; this receipt
does not claim that an untested executable ran. Live upgrade and rollback/cutover
remain separate operator actions.

## Qualification limits

Tests use the real SQLite owner, local Git, kernel Unix credentials and the exact
fixed verifier. The package fixture has ELF-shaped bytes behind a mocked compiler
boundary; it is not a runnable release. Controlled worker reports and synthetic
role UIDs exercise acceptance/refusal, not independent protected compilation or
production account isolation. Actual branch rules, mandatory behavior recipe,
producer/build accounts and filesystem access, and actual human/rollout target
configuration remain unconfigured. Those inputs cannot be inferred from a green
local test or ordinary task output.


## Protected live observation

Recipe revision2 adds a live step without changing public controller operations,
roles, checks, grants or rollout rules. Revision1 post-run steps remain supported.
A live step has `check`, `commands`, `observer`, `oracle`, `timeout_ms` and
`output_bytes`. Commands are at most eight fixed named executable/hash/argument
records with the existing substitutions. The observer and final oracle are
separately protected, pinned programs. The configuration still binds the exact
recipe, required checks, source artifact, policy, target and actual producer roles.
A new recipe identity requires deliberately matching installed configuration;
this code does not install or activate a recipe.

The supervisor starts the fixed observer before candidate work. Its private
standard-input/output channel accepts only sequential bounded JSON requests:
`start` for a declared command; `poll` or `kill_leader` for a supervisor-issued
handle; `stop_all`; and one `finish` result bound to the exact subject/recipe/check.
No executable, arguments, environment, PID, target or path can be supplied through
this channel. There are at most16 starts,128 messages and16KiB per message within
one step deadline and aggregate output bound. The fixed observer source is trusted
code, not a candidate-supplied test program. It must use this dropping launch path
for candidate execution and never execute candidate instructions as the checker.

Candidate processes receive no observer pipe or protected descriptors and run in
the existing execution identity/cgroup. Polling and killing an owned leader do not
prove descendant cleanup. The supervisor reaps its exact owned children before
identity reuse; only whole-group drain plus all owned child waits establish
cleanup. EOF, timeout, malformed or excess messages, unknown handles, output
exhaustion and any post-terminal request refuse qualification and trigger cleanup.
A terminal observer reply explicitly says no Check has been emitted and cleanup
is not yet verified. The supervisor kills/drains both recorded groups and reaps
all owned leaders even when observer processing fails. Unknown cleanup remains
nonpassing and retains the original operation for explicit cleanup recovery.

The private evidence distinguishes observer assertions from supervisor-measured
launch, executable hash, available process image/starttime, exit and cleanup facts.
A very short-lived process can exit before image observation; this is recorded as
unavailable, not invented. The final oracle receives the protected transcript hash
only after cleanup. Both the observer result and final oracle must pass; a false
live observation cannot be overridden. Candidate output is retained separately as
untrusted observation. Recipe, observer, exact candidate and live evidence hashes
are checked across the run; checks still bind the unchanged target configuration.

This supplies the bounded live-control and evidence seam, not all mandatory
production journey oracles. Local tests use actual private processes and a
controlled fixture service to exercise restart, original-effect replay and stale
refusal; they are not full Borg runtime or supplied-provider qualification. A
fixture copying bytes tests exact build-output association, not compilation.
Independently installed fixed build/journey scenarios must exercise the exact
candidate binary, with current task/tool/continuation behavior and recovery.
Historical installed-runtime journeys and a child-produced artifact are separate
subjects. A different binary or a candidate's own PASS record cannot qualify the
new candidate. Actual dedicated identities, cgroup/resource configuration, access
and protected ancestors remain deployment inputs; same-UID fixtures do not prove
that boundary. Human artifact/target rollout authority remains separate.

## Reusable candidate preparation

[Prepared candidate checks](v2-protected-preparation.md) supplies the fixed
revision3 preparation join for these build and journey steps. It accepts the
ordinary candidate and exact protected subject, snapshots the configured source
commit, pins the helper/reader inventory, and generates read-only per-job inputs
without changing the configured recipe digest. The full built artifact must
match that candidate before journey qualification. Existing additional required
checks, configured protected identities and final human rollout authority remain
separate. The local generated build/journey receipt is not protected-host proof.
