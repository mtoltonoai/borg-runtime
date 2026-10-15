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
`state_root`, `limits`, `modules`, `formats`, `hosts`.

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
  contain `wire`, `controller_schema`, `worker_format` and must be equal. The
  package must contain the pinned `src/protocol.rs`,
  `src/controller/database.rs`, `src/worker/handoff.rs`. A different deployed
  format refuses: there is no reset, migration or compatibility fallback.
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
The controller starts, answers canonical reads, stops for an exact logical-store
comparison, then starts again before workers. This is one journaled host phase;
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

`actual_rollout_adapter_original_receipts_and_no_effect_retry` additionally uses
the actual Rust controller/SQLite/private Unix transport and then reopens that
controller. Host systemd/SSH operations remain controlled, including a separate
scratch service-state DB. It is not a real host rollout, installed protected UID
proof, or candidate source/build/model qualification. The existing historical
accepted package supplied by BORG_TEST_PACKAGE and BORG_TEST_PACKAGE_DESCRIPTOR
is a protocol fixture; its bytes are not reported as a newly built current runtime.

An actual configured host qualification still requires the exact installation,
roles, existing transport/access, same-format deployed baseline, finite metadata,
clock bound and a fresh approved Check/action/human grant. This source batch
creates none of those prerequisites.
