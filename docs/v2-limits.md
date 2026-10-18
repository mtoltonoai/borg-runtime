# Runtime Limit Metadata

Borg selects operational limits from [metadata/limits.json](../metadata/limits.json). The JSON is explicit seed data, with `schema_version` 1 and a named global `revision`. `--limits /absolute/path/limits.json` selects another complete file at process startup. Without that argument the executable loads its shipped seed. Global service metadata has no hot reload. The separately configured definition-update policy permits bounded agent-definition limit changes through `collab_definition_update` and the trusted-host browser/API settings operation; these use the same canonical controller checks and receipt. They select a new definition revision for future submissions and do not rewrite already accepted task snapshots or worker startup policy.

The closed schema rejects missing and unknown policy fields. Validation checks positive finite ranges, addressable encodings and relationships between consumers; it never clamps an incompatible value. A metadata change does not change a fixed wire representation or grant provider quota, repository access, host resource controls or protected-release authority.

## Scope And Acceptance

| Metadata | Consumer and lifetime |
|---|---|
| `runtime`, `controller`, `transport`, `worker`, `product`, `repository`, `interface` | Service operational policy selected at startup; task-bound operations use applicable policy from their retained full global snapshot. Worker process/recovery policy remains bound to its own startup state. |
| `retention.worker` | Optional current worker authority for disposable workspace/cache cleanup. Historical absence disables deletion; it does not reinterpret accepted execution settings. |
| `task_defaults` | Initial producer values for new submissions; existing explicit task limits retain their meaning. |
| `TaskInput.limits` | Accepted explicit deadline, model-request allowance, output bytes, complete-result bytes and maximum delegation depth. |
| `execution.native`, `coding`, `collaboration`, `memory_notice` | Global execution defaults, resolved once during task admission. |
| `Definition.limit_overrides` | Optional complete execution-group replacements bound to that immutable agent definition revision. |
| `TaskInput.resolved_limits` | Server-owned accepted snapshot containing the exact global metadata, global and definition revisions, and resolved execution values. |
| `constraints` | Compiled representation facts and separately reported external limits; unknown external values remain `null`. |

In the wire18 successor, memory-notification limit fields remain in retained
metadata and its exact accepted snapshots for decoding and identity preservation.
They do not enable automatic memory notifications or model-context injection.
Explicit memory operations and the separate workflow/charter input policies
remain active; retirement does not rewrite historical metadata.

Submission cannot assert its own accepted snapshot. Reusing an agent definition identity/revision with different policy refuses. Restart, exact receipt replay and changed defaults retain the original accepted input. New tasks can resolve against a later global revision. `collab_limits_get` takes no arguments and returns current global metadata separately from the current task's accepted snapshot; it cannot select another task or change policy. `collab_task_get` includes the explicit `TaskInput.limits`. The browser task detail and `/limits?task_id=<task-id>` display those accepted bounds and metadata revisions; `/limits` labels current global defaults separately. Reading either surface does not update the accepted task.

Execution limits are independent of inference selection. Native model POSTs retain their acknowledged request accounting. One Codex root turn can contain opaque inner model/tool calls; moving limits into metadata does not establish an enforced count or a known provider context/rate limit. An explicit unknown is not unlimited capacity.

## HTTP Policy And Representation

The HTTP/1 parser uses `transport.http_header_count` directly; the shipped seed selects 100 headers. The independent `transport.http_header_bytes` and header-read deadline still apply. The count is not a hidden parser default.

The pinned Hyper 1.11.1 / http 1.5.0 header-map representation supports at most 24,576 headers. `constraints.http1_maximum_header_count` must report that exact compiled fact; it is not a policy knob. A configured header count above it refuses validation. The minimum HTTP buffer and JSON-container capacity are likewise representation checks. Changing metadata cannot enlarge those dependency representations, and external provider/context/inner-Codex ceilings remain unknown unless separately established.

Header deadline expiry emits the content-free operator diagnostic `http_connection_refused: transport.http_header_timeout_ms`. It occurs before the request service and does not add a structured HTTP refusal body or change parsing, status codes, closure or bounds. The [focused receipt](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-19/receipt.json) records three parser and five schema tests passing. All [four real HTTP adapter regressions](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-20/receipt.json) also pass.

Exact header count/byte and regex compilation-size attribution remain gaps. Hyper merges count and buffer failures, and its public size predicate also covers URI representation overflow; the diagnostic therefore says `request_head_limit_cause_unavailable`. The schema validator erases compiled-size and syntax causes, so `native_schema_invalid_or_unsupported` remains generic. These bounds are still enforced. [Pinned API evidence](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/metadata-refusal-diagnostics-03/BOUNDARIES.md) identifies the typed dependency information needed to close those gaps without guessing.

## Retained Worker State

The worker holds its exclusive owner lock while loading or creating its private `limits.json`. A first empty execution root records the exact selected metadata. A nonempty root without the binding refuses rather than applying current defaults to historical evidence. Retained binding files must satisfy the existing type, ownership, mode and no-alias checks.

Once startup validation and retained binding succeed, prior process cleanup and local execution-evidence checks use the retained policy. A selected-policy difference outside the optional retention block refuses registration and new work without rewriting that binding. A changed retention block may carry its own new global revision label while every other retained field stays exact; a revision-only change still refuses. Cleanup records its selected policy/revision separately and never replaces the retained execution binding. Earlier validation, cleanup or evidence failures may refuse first. This path does not complete pending remote recovery or flush the outbox; normal durable recovery resumes under matching policy. It is a compatibility refusal, not a hot-change or migration mechanism. Task execution separately uses its accepted execution snapshot; worker defaults cannot silently replace it.

The current worker format marker is 9. Codex collaboration journal revision 2 additionally binds global, task collaboration and worker policy. Native helper request/reply framing is revision 6; its closed bound-source discriminator preserves the limiting metadata field in helper refusal diagnostics. Mutation and acceptance journals use revision 3. Exact original occurrences and outcomes remain replayed under their original bindings. Journal admission reserves complete bounded outcome space before sending; a bound refusal is not a successful mutation or a new retry identity. Memory-boundary indices are checked against exact retained journal membership; there is no separate 1024-entry reader quota.

## Representation And Rollout

The automatic-cleanup successor uses private wire 11 and controller schema 12. The installed maximum-context package uses wire 10; earlier wire-9 and wire-8 subjects remain historical. Earlier installed wire-8 packages remain historical subjects until a separately verified replacement; this source document does not claim they implement definition updates. Unsupported wire/database/journal representations refuse without conversion. The worker outer marker alone does not make old state compatible with the required policy and inner-journal records. No schema migration, downgrade, automatic reset or state overlay is provided by this change.

Any stopped-state replacement remains a separately reviewed operational action with exact package/source, state and preservation bindings. This document does not authorize it. Existing source repositories, checkpoints, credentials and historical evidence are not reset by loading limit metadata.

## Evidence

The [metadata-limits integration batch](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/) retains source manifests and actual compiler/test receipts. Source checks cover closed metadata, exact task snapshots, changed defaults, explicit agent overrides, journal reservation before dispatch, retained worker policy, and memory-boundary membership beyond the former 1024 guard. Final aggregate and deployed qualification remain bound to their own receipts; historical test counts and old package identities are not carried forward.

## Maximum context metadata

Context defaults remain metadata, not consumer-local policy constants. The selected model's documented context window is 1,050,000 tokens. The current seed sets `constraints.provider_model_context_tokens` to that known external fact and `execution.coding.model_context_window` to the explicit per-task Codex window. The model-support validator enforces the selected model's external ceiling; it does not supply an omitted default. Historical absent coding windows remain unspecified and serialize without a new field.

The four native byte carriers `context_bytes`, `request_bytes`, `helper_value_bytes` and `response_wire_bytes` use 4,294,967,295, the existing unsigned32-bit framing ceiling. Bytes and tokens are different units. Actual received chunks drive fallible allocation; a declared maximum does not allocate its full value. Output, per-event/input/result limits, deadlines and host controls remain separate. No full-window workload or exact internal compaction threshold is established by setting these values.

Global defaults resolve with complete agent override groups at task admission. Existing heads with smaller retained overrides require canonical definition-limit updates for future submissions; changing a file or submitting another immutable revision does not update a selected head. The twelve allowed fields are the earlier eight plus native request/helper/response carriers and the coding context window. A change from an unspecified executor window is recorded as `previous_unspecified`, not a comparison against zero. Accepted tasks and original outcomes retain their previous metadata.

The shipping controller profile is projected from its actual retained metadata, preserving native capacity 256, coding capacity 8 and unrelated operational values. Worker startup metadata remains unchanged. Installation, effective executor readback and fresh task results require their own source-bound evidence; these documented source selections are not an installed-state claim.


## Automatic Disposable Cleanup

The `retention.worker` object selects successful, failed, cancelled and unused-cache grace periods, batch size, filesystem yield size, operation timeout, retry delay and cleanup-record byte bound. The initial defaults are immediate successful-workspace eligibility, 24 hours for failed or cancelled workspaces and seven idle days for unused reconstructible repository caches. Safety holds override all timers. A missing policy disables cleanup, including when a historical task snapshot contains different settings.

Workers keep task/attempt/generation, process quiescence, publication and acknowledgment checks separate from elapsed time. Successful-workspace removal requires acknowledged source and no uncommitted nonignored edits. Failed or cancelled dirty workspaces can expire only after the configured grace and with no unresolved work depending on them. The grace begins from a durably recorded safe observation, not mtime. Changing task ownership or retry generation invalidates old eligibility; a current-task mismatch is conservatively retained unless historical eligibility is independently established.

Worktrees share repository objects only within their worker's registered repository cache. Cleanup holds a cache while a live worktree or local recovery dependency needs it. An acknowledged external checkpoint alone does not pin this reconstructible copy. Removal verifies owned filesystem identity and resumes incomplete work after restart without retargeting replacement paths. Bounded batches and due timers allow cleanup to continue after an acknowledgment or recovery hold clears, without a new task submission.

The initial increment retains task/result/history/checkpoint references, audit data, operation receipts and recovery journals. It reclaims disposable working copies and build outputs, not the central repository or durable task database. Total database growth, disk quotas and durable-record retirement are separate gaps. Cleanup evidence reports reclaimed, retained or uncertain resources and the selected policy revision. Source integration, actual deletion tests and installed activation each have their own receipts.
