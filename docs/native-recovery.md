# Independent native recovery

`tooling/native_recovery.py` runs as **one independently supervised controller**
on the shared Board host. Fleet workers and native engines run separately on
all four labs. The monitor never stops or resumes an original native session,
never resolves a tool result, and never retries an original tool mutation.
It creates owned diagnosis/repair tasks and uses Fleet's durable dispatch API to
start independent triage. A source-level fix and regression, not a one-time wake,
finish a repair.

## Observation and authority

* Board `/api/agents?verbose=true` supplies full desired identity state, including
  explicit stand-down. `lifecycle_intent`, retirement, pauses and Board task
  dependencies are respected; presence alone is not execution coverage.
* Board `/api/tasks?project_id=N` supplies task authority. This REST list is not
  paged. Agent and Fleet dispatch lists **are** paged to exhaustion.
* `/api/fleet/hosts` and `/api/fleet/dispatches` supply current placement and
  dispatch state. Runtime URLs include `/api`. Terminal historical dispatch URLs
  and inactive drained hosts are not resurrected as live endpoint obligations.
* Native `GET /api/sessions` supplies live `driver_running`, status, phase,
  generation, step and `last_progress_at`. Heartbeat/phase timestamps do not
  count as useful progress when `last_progress_at` exists. For failed/uncertain
  sessions, `GET /api/sessions/{id}/tools` supplies original receipt statuses;
  tool arguments and result bodies are never copied into incident descriptions.
* Runtime absence is scoped to **current task/dispatch ownership**. An unknown
  endpoint for a task prevents claiming that task is missing; an unrelated
  retired or drained endpoint cannot indefinitely hide an orphan elsewhere.
  Explicit unscoped bootstrap endpoints remain conservative until adopted.
* A slow live inference/stream or indefinite retry is not process death. After
  the configurable notice age it may get a **diagnostic** incident, not a stop
  or restart. HTTP unreachability is reported as unreachability, never as proven
  process death. Triage verifies process/supervisor facts.
* Explicitly stopped sessions and verified, reasoned waits are visible. Unknown
  tool effects always require receipt reconciliation, including triage tools.

Runtime process death, failed inference, lost drivers/stopped progress, stalled
provisioning, actionable tasks with no associated live session, unowned tasks,
and missing Fleet execution capacity all have owned recovery paths. Native's
transient inference and read-only discovery retries are unlimited; the monitor
also retries observation and idempotent control-plane operations indefinitely.
It uses capped exponential jitter and honors a server Retry-After floor even
when it exceeds the local cap. A finite retry count never makes work terminal.

## Durable incidents and no duplicate dispatch

The local SQLite journal uses WAL, `synchronous=FULL`, and an OS process lock.
Keep it on persistent local storage; do not run several controllers with separate
journals. It is an observation/outbox journal, not a second task authority.

An incident key is deterministic for `(project, fault kind, subject)`. Board
`external_link {source:"borg-native-recovery-v1", external_id:incident_key}`
deduplicates repair-task creation even if a response is lost or the monitor dies
before saving its receipt. The exact HTTP method/path/body is committed **before
first transmission**, and retries use that frozen intent even if ownership or
policy changes. Comments use the same external-link mechanism. Fleet creation
uses `request_id="task:{repair_id}:attempt:1"`, identical to automatic worker
enqueue; monitor does not change `native_attempt` or relocate an uncertain
session. A repeated response cannot create a second diagnostic session.

Source tasks receive `metadata.recovery_task_id`. Repair tasks carry immutable
`metadata.recovery` (incident key, source, kind, session, depth), safe evidence,
and `recovery_monitor_state`. A failed triage path gets at most one deduplicated
fallback-owner control-path repair; a broken fallback is a visible owned
external dependency, not recursive session spawning. Intentional triage stops
are retained. Saturated capacity is recorded visibly while the same request
continues retrying; unknown sessions are not killed to free slots.

Cleared symptoms do not automatically close repair tasks. The repair owner
records `metadata.recovery_resolution` with nonempty `cause`, `fix` (tested
commit/deployment), and `regression`, verifies the symptom is gone, then marks
the task done. Premature closure is reopened, keyed by the observed Board
`updated_at` closure generation, so a second closure or recurrence is not hidden
by an old outbox receipt. Deliberate cancellation remains an explicit owner
choice.

## Configuration and metadata policy

Copy `config/recovery.example.json` outside the checkout. It holds Board access,
persistent journal location, project IDs, polling/HTTP deadlines and optional
`board_headers_env` / `runtime_headers_env` header-to-environment-variable maps.
Credentials belong in the service environment, never in Board metadata or Git.
The `revision` field should name the exact deployed commit.

Operational policy is merged from config defaults, then
`project.metadata.recovery`. Per-identity and per-task `metadata.recovery`
override task classification thresholds and monitoring eligibility. The project
selects `triage_agent` and `fallback_agent` for its controller. Relevant keys:

* `enabled`, `triage_agent`, `fallback_agent`
* `missing_session_grace_seconds`, `runtime_failure_grace_seconds`,
  `runtime_failure_observations`, `dispatch_grace_seconds`
* `progress_stall_seconds`, `live_wait_notice_seconds` (0 disables notices),
  `triage_grace_seconds`, `retry_initial_seconds`, `retry_cap_seconds`
* `maintenance:{reason,until}` with a finite RFC3339 deadline; expired,
  reasonless or malformed holds do not silently suppress recovery.
* On identities/tasks/sessions, `wait:{verified:true,reason,until}` is a visible
  verified external wait. Arbitrary stale progress text is not a verified wait.

Use project maintenance for coordinated rollout. It suspends new monitor effects
without deleting incidents/receipts. Fleet `enabled:false` drains placements;
it is not a license to cancel a running tool. Always let the integrator preserve
active bootstrap tool results before a runtime/Board rollout.

Normally `extra_runtimes` is empty after Fleet adoption. A bootstrap entry can be
`{host_id,runtime_url,task_ids:[...],agent_ids:[...]}`; without task/identity scope,
its unknown state conservatively affects all task absence checks. Remove an
entry when its sessions have been adopted or deliberately retired.

The deployed native inference metadata is `max_retries:null`,
`retry_initial_ms:5000`, `retry_max_ms:60000`, `retry_jitter_percent:20`.
`4294967295` was only a bridge in old bootstrap host JSON. Do not change the
selected model, effort, output allowance, context window or turn limit.

## Persistent deployment

The integrator owns rollout and the active release path. No bootstrap service
needs to be restarted merely to install this independent monitor.

```sh
install -d -m 700 ~/.config/borg ~/.local/share/borg/state ~/.config/systemd/user
install -m 600 config/recovery.example.json ~/.config/borg/recovery.json
# Set board URL, revision and durable paths for this installation.
install -m 644 config/systemd/borg-recovery.service ~/.config/systemd/user/
# Unit template expects ~/.local/share/borg/current to be the integrated release;
# adjust ExecStart to the actual release path through a normal unit override.
systemctl --user daemon-reload
systemctl --user enable --now borg-recovery.service
systemctl --user status borg-recovery.service
journalctl --user -u borg-recovery.service -n 60 --no-pager
```

Enable user-service persistence/lingering through the deployment's existing
service setup. The unit is not `PartOf` or `Requires` the engine it monitors:
engine failure must not silence recovery. `Restart=always` and disabled restart
rate limiting restart a dead monitor; its durable outbox prevents duplicate
Board/Fleet effects. A crashed monitor must be diagnosed too, not repeatedly
woken without repair.

`--once` is for a single deployment/test pass; it returns nonzero if observations
fail. Normal daemon mode keeps retrying. `--status` reads local durable incident
and health state without dispatching. Board service identity
`native-recovery-monitor` publishes heartbeat, deployed revision, incident/retry
counts and bounded visible waits. A Board outage leaves a durable local error
and journald status rather than a false healthy heartbeat.

## Tests and live drills

```sh
python3 -m unittest discover -s tests -p 'test_native_recovery.py' -v
BORG_TEST_BOARD_BINARY=/path/to/integrated/task-board \
  python3 -m unittest discover -s tests -p 'test_native_recovery_board.py' -v
BORG_TEST_BOARD_BINARY=/path/to/integrated/task-board \
  cargo test --locked --test board_content_recovery -- --ignored
```

The first suite kills a real observation process, exercises HTTP response loss,
monitor restart, duplicate-controller exclusion, >40 failed attempts then
success, unknown effects, pauses/stand-down, scoped completeness, repeated
closure/recurrence, frozen request intent after reassignment, and triage-storm
prevention. The second starts an isolated **real Board with Fleet registry** and
checks all monitor REST writes/reads, real placement/dispatch dedup, source links
and lost-create receipt recovery. The third runs actual Board MCP through the
native durable loop for the technical-text/application-error incident.

After deployment, inject faults only into disposable verification sessions or a
separate test runtime endpoint; never kill an engineer's in-flight tool. Capture
before/after task, session, dispatch, journal and completed-tool-result digests.
Prove owned incident creation, triage dispatch, original receipt preservation,
durable source/config fix plus regression, monitor restart dedup, and eventual
repair closure. An HTTP-health recovery or scheduled task alone is not evidence
that an engineer actually repaired the underlying cause.
