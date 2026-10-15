# Runtime Limit Metadata

Borg selects operational limits from [metadata/limits.json](../metadata/limits.json). The JSON is explicit seed data, with `schema_version` 1 and a named global `revision`. `--limits /absolute/path/limits.json` selects another complete file at process startup. Without that argument the executable loads its shipped seed. There is no hot reload or MCP settings-write operation.

The closed schema rejects missing and unknown policy fields. Validation checks positive finite ranges, addressable encodings and relationships between consumers; it never clamps an incompatible value. A metadata change does not change a fixed wire representation or grant provider quota, repository access, host resource controls or protected-release authority.

## Scope And Acceptance

| Metadata | Consumer and lifetime |
|---|---|
| `runtime`, `controller`, `transport`, `worker`, `product`, `repository`, `interface` | Service operational policy selected at startup; task-bound operations use applicable policy from their retained full global snapshot. Worker process/recovery policy remains bound to its own startup state. |
| `task_defaults` | Initial producer values for new submissions; existing explicit task limits retain their meaning. |
| `TaskInput.limits` | Accepted explicit deadline, model-request allowance, output bytes, complete-result bytes and maximum delegation depth. |
| `execution.native`, `coding`, `collaboration`, `memory_notice` | Global execution defaults, resolved once during task admission. |
| `Definition.limit_overrides` | Optional complete execution-group replacements bound to that immutable agent definition revision. |
| `TaskInput.resolved_limits` | Server-owned accepted snapshot containing the exact global metadata, global and definition revisions, and resolved execution values. |
| `constraints` | Compiled representation facts and separately reported external limits; unknown external values remain `null`. |

Submission cannot assert its own accepted snapshot. Reusing an agent definition identity/revision with different policy refuses. Restart, exact receipt replay and changed defaults retain the original accepted input. New tasks can resolve against a later global revision. `collab_limits_get` takes no arguments and returns current global metadata separately from the current task's accepted snapshot; it cannot select another task or change policy. `collab_task_get` includes the explicit `TaskInput.limits`. The browser task detail and `/limits?task_id=<task-id>` display those accepted bounds and metadata revisions; `/limits` labels current global defaults separately. Reading either surface does not update the accepted task.

Execution limits are independent of inference selection. Native model POSTs retain their acknowledged request accounting. One Codex root turn can contain opaque inner model/tool calls; moving limits into metadata does not establish an enforced count or a known provider context/rate limit. An explicit unknown is not unlimited capacity.

## HTTP Policy And Representation

The HTTP/1 parser uses `transport.http_header_count` directly; the shipped seed selects 100 headers. The independent `transport.http_header_bytes` and header-read deadline still apply. The count is not a hidden parser default.

The pinned Hyper 1.11.1 / http 1.5.0 header-map representation supports at most 24,576 headers. `constraints.http1_maximum_header_count` must report that exact compiled fact; it is not a policy knob. A configured header count above it refuses validation. The minimum HTTP buffer and JSON-container capacity are likewise representation checks. Changing metadata cannot enlarge those dependency representations, and external provider/context/inner-Codex ceilings remain unknown unless separately established.

Header deadline expiry emits the content-free operator diagnostic `http_connection_refused: transport.http_header_timeout_ms`. It occurs before the request service and does not add a structured HTTP refusal body or change parsing, status codes, closure or bounds. The [focused receipt](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-19/receipt.json) records three parser and five schema tests passing. All [four real HTTP adapter regressions](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-20/receipt.json) also pass.

Exact header count/byte and regex compilation-size attribution remain gaps. Hyper merges count and buffer failures, and its public size predicate also covers URI representation overflow; the diagnostic therefore says `request_head_limit_cause_unavailable`. The schema validator erases compiled-size and syntax causes, so `native_schema_invalid_or_unsupported` remains generic. These bounds are still enforced. [Pinned API evidence](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/metadata-refusal-diagnostics-03/BOUNDARIES.md) identifies the typed dependency information needed to close those gaps without guessing.

## Retained Worker State

The worker holds its exclusive owner lock while loading or creating its private `limits.json`. A first empty execution root records the exact selected metadata. A nonempty root without the binding refuses rather than applying current defaults to historical evidence. Retained binding files must satisfy the existing type, ownership, mode and no-alias checks.

Once startup validation and retained binding succeed, prior process cleanup and local execution-evidence checks use the retained policy. Any selected-policy difference, including a revision-only change, refuses registration and new work without rewriting that binding. Earlier validation, cleanup or evidence failures may refuse first. This path does not complete pending remote recovery or flush the outbox; normal durable recovery resumes under matching policy. It is a compatibility refusal, not a hot-change or migration mechanism. Task execution separately uses its accepted execution snapshot; worker defaults cannot silently replace it.

The current worker format marker is 9. Codex collaboration journal revision 2 additionally binds global, task collaboration and worker policy. Native helper request/reply framing is revision 6; its closed bound-source discriminator preserves the limiting metadata field in helper refusal diagnostics. Mutation and acceptance journals use revision 3. Exact original occurrences and outcomes remain replayed under their original bindings. Journal admission reserves complete bounded outcome space before sending; a bound refusal is not a successful mutation or a new retry identity. Memory-boundary indices are checked against exact retained journal membership; there is no separate 1024-entry reader quota.

## Representation And Rollout

This source uses private wire 8 and controller schema 12. Unsupported wire/database/journal representations refuse without conversion. The worker outer marker alone does not make old state compatible with the required policy and inner-journal records. No schema migration, downgrade, automatic reset or state overlay is provided by this change.

Any stopped-state replacement remains a separately reviewed operational action with exact package/source, state and preservation bindings. This document does not authorize it. Existing source repositories, checkpoints, credentials and historical evidence are not reset by loading limit metadata.

## Evidence

The [metadata-limits integration batch](../artifacts/v2/build-60156282/metadata-limits-v41pfogc/) retains source manifests and actual compiler/test receipts. Source checks cover closed metadata, exact task snapshots, changed defaults, explicit agent overrides, journal reservation before dispatch, retained worker policy, and memory-boundary membership beyond the former 1024 guard. Final aggregate and deployed qualification remain bound to their own receipts; historical test counts and old package identities are not carried forward.
