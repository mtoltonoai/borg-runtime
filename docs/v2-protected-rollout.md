# Fixed operator rollout adapter

`tooling/protected_rollout_v2.py` implements the existing private
StartRollout/ObserveRollout contract. It does not prepare a grant, Check, merge,
account, cgroup, SSH route, repository grant, or target. There is no default
installed target. Ordinary task/MCP input cannot select its host paths.

The adapter is inactive without a protected operator profile, a matching
controller protected-release configuration, an existing Prepared rollout action,
a fresh exact-source Check and human grant, and already-installed helper/tools
and package on every configured host. The installed rollout principal and host
helper invocation path must be inaccessible to task/worker identities. A same-UID
fixture does not establish that protection. The package/target is the complete
existing Subject; all ten ReleaseArtifact fields must match the verified archive.

## Invocation

Use the pinned canonical selected Python executable with isolated startup:

```
python3 -I -S -B protected_rollout_v2.py apply \
  --profile /operator/rollout.json --profile-sha256 <sha256> \
  --profile-bytes <metadata-byte-bound> --subject /operator/subject.json \
  --action-id <existing-prepared-action-id>
```

`reconcile` uses the same command with its mode changed. It only queries original
receipts and current host state. It never resumes a stop, install or start. A
missing receipt does not authorize another mutation with a fresh identity.

The profile is closed revision 1 JSON. Every policy bound is required under
`limits`; there is no implicit rollout policy or numeric default. Test fixture
values are test inputs, not selected production capacity. Existing package
verifier representation/profile rules remain unchanged.

Top-level fields: `revision`, `deployment_target`, `target_revision`, `policy`,
`producer`, `controller_config`, `python`, `transport_tools`, `sqlite_reader`,
`state_root`, `limits`, `modules`, `formats`, `hosts`. The optional
`producer_transport` selects the fixed SSH join described below.

* A file pin is exactly `{path, sha256}`. Use canonical absolute paths without
  symlinks, protected owners/modes and regular single-link files. The Python pin
  must match the running interpreter. Module pins are exactly `producer`,
  `verifier`, `common`, `host`, `adapter`. The common module also has an embedded
  hash in both entrypoints. `transport_tools` pins the fixed `/usr/bin/ssh`,
  `/usr/bin/sudo` and `/usr/bin/env` paths. The SQLite reader pins the selected
  existing stdlib `_sqlite3` extension and its actually mapped SQLite library;
  no alternate engine or acquired package is selected.
* `producer` pins the existing producer configuration. `controller_config` pins
  the actual configuration named in the controller's unchanged unit. Policy,
  recipe, target, repository, installed digest and kernel UID role must agree.
* `formats` contains `before`, `after` and `source_files`. Both format tuples
  contain `wire`, `controller_schema`, `worker_format`. Without `forward`, they
  must be equal and the source inventory is exactly `src/protocol.rs`,
  `src/controller/database.rs`, `src/worker/handoff.rs`. The two explicit
  forward transitions below have larger source inventories. Every other tuple
  difference refuses; there is no version-range inference or reset fallback.
* Exactly one controller and all intended workers are listed in `hosts`, with
  unique IDs. Each host has `id`, `role`, `transport`, `profile_path`,
  `state_root`, `service`, `unit`, `new_unit`, `release_root`, `package`,
  `old_artifact`, `service_uid`, `helper_uid`, `incarnation`, `state`,
  `owner_lock`, `helpers`, `preserve_metadata`, `preserve_files`, `api`,
  `systemctl`. The state root for rollout receipts is separate from runtime
  state. `state` is `{root,format_file,database,policy}`; controller format_file
  is null, worker database is null. Policy is a file pin, and workers must retain
  that exact JSON in their existing limits.json.
* `transport` is `{kind,ssh_config,alias}`: local has null SSH fields; SSH uses
  only the pinned already-provisioned configuration and named host. There is no
  route discovery, transfer, credential installation or access change. Identical
  profile/module locations must already exist on each selected host. The fixed
  remote command invokes the selected helper via existing noninteractive sudo.
* `api` is `{host,port,worker_id}` on an existing numeric loopback endpoint.
  Controller worker_id is null. Worker API traffic uses the already-provisioned
  loopback controller route. No new listener or tunnel is created.
* Worker incarnation is explicitly `systemd_invocation` and the pinned unit must
  contain `--incarnation ${INVOCATION_ID}`; controller incarnation is null. It is
  not assumed for other launch conventions. New registration must have the real
  revision-3 journal, exact retained runtime policy and capacities, original
  MutationCommit context, canonical settled GetOperation and current InvocationID.
* `preserve_files` pins nonsecret resource profiles, repository maps and other
  public operational inputs. `preserve_metadata` records only lstat metadata of
  credential/environment files or preserved paths, never their contents. No
  account, credential, resource profile, workspace or repository is rewritten.

Required limits: `receipt_bytes`, `module_bytes`, `package_bytes`,
`io_chunk_bytes`, `hosts`, `command_output_bytes`, `command_timeout_ms`,
`cleanup_timeout_ms`, `cleanup_poll_ms`, `overall_timeout_ms`, `transport_bytes`,
`transport_timeout_ms`, `state_entries`, `state_bytes`, `api_pages`,
`api_page_items`, `sqlite_tables`, `sqlite_rows`, `sqlite_steps`,
`readiness_poll_ms`, `clock_uncertainty_ms`. All are positive integers except the
clock uncertainty may be zero for a same-clock fixture. The operator must supply
a justified clock bound for real hosts; a profile value alone does not establish
clock synchronization. Authorization checks subtract that uncertainty from the
original absolute dispatch deadline.

### Supported forward transitions

The optional closed `formats.forward` object starts only from
`{wire:11,controller_schema:12,worker_format:9}`. Each kind selects one exact
destination; it is not a minimum version or permission to chain transitions.

| `kind` | Exact destination |
| --- | --- |
| `validated-controller12-worker9-to-controller16-worker12/1` | `{wire:14,controller_schema:16,worker_format:12}` |
| `validated-controller12-worker9-to-controller17-worker13/1` | `{wire:15,controller_schema:17,worker_format:13}` |

The closed object's fields are:

* `kind`: exactly one of the two values above.
* `before_package_sha256` and `after_package_sha256`: distinct exact package
  hashes matching every old host artifact and the full selected Subject.
* `evidence`: exactly `controller`, `worker`, `retained_state`, each a file pin
  for the reviewed qualification records. All three are bounded, protected
  inputs checked before host effects. They are evidence selected by the profile;
  they do not confer authority or replace actual state/readback guards.

For the schema16 transition `source_files` contains exactly the three files above plus
`src/controller/application_schema.rs`, `src/controller/workflow_schema.rs`,
`src/controller/schema13-from12.sql`, `src/controller/schema14-from13.sql`, and
`src/worker/mod.rs`. The schema17 transition requires those eight files plus
`src/controller/schema17-from16.sql`, `src/charter.rs`,
`src/controller/charters.rs`, `src/controller/charter_outputs.rs`,
`src/worker/charter.rs`, `src/worker/charter_output.rs`, and
`src/worker/mutations.rs`. Every digest must match the verified candidate package.
Qualification records must cover the selected destination and its original
state readers; earlier schema16/worker12 evidence is not evidence for the new
output representation.
The adapter does not execute these sources or migrate original state itself.
The selected runtime performs its existing validated migration under its owner
lock. Worker `format_file` must be the original state's `format.json`.

The stopped controller comparison permits only the exact 25 empty tables from
the two schema deltas, NULL `sessions.workflow_delivery` and
`receipts.governance` columns on every original row, and the initial
`workflow_revision=0` setting. Every original table, column and row remains
equal. The schema17 transition additionally requires exactly one empty
`charter_output_chunks` table with columns `request_id`, `chunk_index`,
`operation_id`, `reference`, `body`. A populated table, an omitted table or a
different column set refuses. The controller marker must become the selected
16 or 17. Worker startup must produce the paired marker 12 or 13 and a
fresh canonical registration. Only that marker's inode and contents may change
in addition to the existing registration/await/owner files; its device,
ownership and mode remain equal, and all other original history stays exact.
The new runtime's bounded `charter-output` admission is part of its own startup;
the adapter does not create, repair, truncate or translate output records.
Uncertain original effects remain uncertain; completed command bookkeeping
does not authorize replay.

Migration commits are forward barriers. A crash, expired dispatch or lost
acknowledgment after migration may leave newer state even when rollout is
`UNKNOWN`. Retained original bytes are evidence, not an automatic restore or
permission to run an old binary on newer state. Read-only reconciliation must
prove the original phase completed; it cannot retry migration or host effects.

## Effects and retained evidence

Before Start, each service must match its old package/unit/UID and compatible
runtime policy. The controller must return the expected wire revision and only
terminal, stopped Task objects, with bounded pagination. Operator quiescence
must prevent new submissions during cutover; a detected change refuses rather
than silently cancelling work.

The adapter journals the exact Start request before sending it. A successful
receipt lookup is examined for its embedded original outcome. A historical
Started receipt is not renewed authorization: the adapter also observes the
current action and keeps the original dispatch_until_ms. Every mutating host
command checks this deadline after blocking preparation and immediately before
launch. Unit replacement checks after the last old-unit read. Socket and local
process waits use finite absolute deadlines.

Workers stop first, then the controller. Each stopped host must have inactive
service, MainPID zero, empty service cgroup, no helper records, no unfinished
commands/pending reports, and an exclusive lock on the original owner inode.
The original DB, WAL and SHM are captured byte-for-byte under that lock. Only a
separate disposable copied-state reader is opened by SQLite in query-only mode;
its authoritative user_version can differ from a stale main-file header. The
original store is never opened by this reader. Every original table/row is
retained in the bounded logical snapshot.

Installation creates a new release directory exclusively, manually extracts the
two verified archive members, fsyncs files and parent directories, and replaces
only the exact executable path in the unit, preserving its original owner, group
and permissions (including a private 0600 unit). Existing release paths refuse.
Runtime state bytes and preserved metadata must remain unchanged through install.
The controller starts, answers canonical reads using the selected new wire,
stops for the exact logical-store comparison (including only the declared
additions for the supported transition), then starts again before workers.
Preflight uses the old wire without fallback. This is one journaled host phase;
an ambiguous acknowledgment never repeats it. Worker startup requires an actual
canonical fresh registration and retains old history. Final readback compares all
Task objects, original ordinary mutation receipts, installed binary/manifest/unit,
profile/format, preserved files and every configured host's phase receipts.

Each host records an immutable phase intent before effects and a result after
verification. A lost host acknowledgment is reconciled from these exact original
receipts plus current readback. Intent without completion stays unknown. An
observed failure and a later verified success use distinct stable observation
operation IDs; neither is a retry of a host effect. ObserveRollout(true) is sent
only after all configured hosts prove the complete target. ObserveRollout is not
itself an execution verifier; these adapter oracles supply the evidence.

Local command leaders remain unreaped until the last owned process-group signal,
then cleanup only observes closure. Leader exit alone does not prove descendants
stopped. A cleanup deadline marks uncertainty. Closing a local SSH process never
proves a remote effect stopped. There is no automatic rollback, stale-owner
removal, or reset.

## Qualification scope

Pure stdlib tests use scratch package files, fsync, phase journals and SQLite
copies with a controlled service boundary; they do not execute services,
executors or providers. Controlled clocks cover expiry during pins and unit
reads, trickling transport replies, and identity-safe descendant cleanup. Tests
also distinguish lost Start before/after commit, original refusal, changed input,
current Applied/Unknown, partial progress, exact receipt bounds and read-only
recovery of a lost final host acknowledgment.

Forward-transition tests use the actual additive SQL files on scratch schema12
stores and a controlled runtime-start seam. They discriminate rewritten
original receipts, unexpected rows/columns, missing or changed qualification
pins, incorrect format/package/source identity, wrong worker markers, and
post-migration crash/expiry/lost-ACK handling. The schema17 cases also require
the new chunk table to remain empty and distinguish the paired marker13 from
the previous target's marker12. They do not claim execution of
the runtime migrators or migration of any installed store; those are separate
source-bound runtime and retained-state qualifications.

`actual_rollout_adapter_original_receipts_and_no_effect_retry` additionally uses
the actual Rust controller/SQLite/private Unix transport and then reopens that
controller. Host systemd/SSH operations remain controlled, including a separate
scratch service-state DB. It is not a real host rollout, installed protected UID
proof, or candidate source/build/model qualification. The existing historical
accepted package supplied by BORG_TEST_PACKAGE and BORG_TEST_PACKAGE_DESCRIPTOR
is a protocol fixture; its bytes are not reported as a newly built current runtime.

An actual configured host qualification still requires the exact installation,
roles, existing transport/access, the selected supported baseline, finite metadata,
clock bound and a fresh approved Check/action/human grant. This source batch
creates none of those prerequisites.

## Optional remote producer on the existing controller host

Without `producer_transport`, the existing same-host Unix producer path is
unchanged. To use the already configured controller SSH route, add exactly:

```
"producer_transport": {
  "kind": "ssh",
  "host_id": "<existing controller host ID>",
  "config_path": "/absolute/installed/producer.json"
}
```

No independent alias, SSH options or executable may be added here. The host must
be the profile's single controller and already use SSH transport. Its helper UID
is the producer UID; that UID still needs an explicit rollout role and cannot be
a worker. The remote config bytes must match the pinned local producer copy.
The Python and producer-module locations and bytes are already selected by the
profile. The fixed command uses the existing SSH configuration and sudo route,
then the producer's bounded `exchange-stdio` operation. No route is provisioned.

The controller's actual private-socket checks remain authoritative. Original
StartRollout intent is synced locally before the exchange. A lost reply triggers
only original receipt/action readback; reconciliation never resends StartRollout
or resumes host installation. All exact-subject, fresh-check, human-grant and
host-readback conditions remain unchanged. Pure transport tests are not evidence
of a currently installed route or a completed rollout.
