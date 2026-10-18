# MCP repository publication

This change is a descendant of runtime `af5ce1151014581e7264c52b29ccce1886ae5aa7`.
Its instruction source is registered `borg-spec` revision `1`,
`git://127.0.0.1:19418/borg.git`, immutable commit
`50f03d0cc7e004dd2bfc0c500f6fab4b68f2af09`. Source identities include `AGENTS.md`,
`constitution.md`, all `spec/core/` inputs, and the v2 implementation, protocol,
review packet, dependency profile, execution work orders and collaboration MCP
proposal. Relative specification paths resolve in that source. The real
`spec-lock.json` and 58-file `spec-snapshot/` remain unchanged.

## Contract and authority

Both executors consume the canonical catalogue and controller admission for:

- `collab_repository_publish(source_task_id, expected_source_revision,
  checkpoint_operation_id, repository_id, registration_revision, target_revision)`
- `collab_pull_request_open(publication_id, title, body)`
- `collab_publication_get(publication_id)`

An immutable `Definition.execution_profile` of `collaboration_v2` can carry
`publication`, a closed version-1 selection. Its `version` is the JSON string
`"1"`; repository and target revisions are exact strings; `actions` is a
nonempty unique list drawn from `publish_branch` and `open_pull_request`;
`branch_namespace` is a validated Git branch prefix without `refs/heads/`.
Historical absence remains absent. Explicit null, unknown versions/actions and
unknown fields refuse. Absence hides all three tools and a forged direct
mutation refuses. Repository reads and catalogue visibility grant no write
capability. No new manager policy, repository-management or protected authority
is introduced. The charter manager can carry this selection only through its
existing fully validated definition operation.

The controller copies the target from the immutable registration. The model
cannot supply a remote, API endpoint, base branch, provider repository identity,
credential selector or branch suffix. The suffix binds a digest of the original
requester, accepted source and target. A requesting Native task retains its real
Native input; only the publication command materializes the checkpoint in an
owned validation worktree. It starts no Coding executor for that purpose.

Source admission reuses the canonical successful-result occurrence and original
acknowledged checkpoint validation. It records the accepting attempt separately
from the original checkpoint publisher, then binds both separately from the
current requesting task/session/attempt/generation/worker incarnation. The
source must be an immutable accepted prerequisite or a completed owned child.
Source tasks remain succeeded, and their accepted input and session are not
changed. There is no additional review or human gate for ordinary publication.
Actual reviewer decisions remain a separate requirement of later release work.

Exact original receipt replay precedes fresh validation. Reusing an operation
identity with different input refuses. An acknowledgment reports admission with
`pending`, not a created branch or PR. A new status occurrence reports bounded
`pending`, `completed`, `refused` or `unknown` facts. A completed branch carries
its exact ref/commit proof; a completed PR carries the original provider ID,
number, URL, head and observed base evidence. Replaying an earlier status read
returns its earlier observation. Only the trusted adapter receives the selected
API token; child/model environments and public projections receive no token.
Configuration errors and known never-dispatched closure use closed refusal
reasons. The existing trusted-worker filesystem/account limitation remains:
these tools do not establish containment against arbitrary same-account code.

## Durable execution and recovery

Admission atomically records the original MCP receipt, one typed
`PublishRepository` command and its original prepared effect. Existing command
rows reserve delivery and acknowledgment capacity; the original external effect
reserves settlement capacity. Complete command, journal, tool-result and maximum
provider-proof envelopes are checked before commit. Metadata supplies all
bounds. Native publication admission also checks the complete selected catalogue
and helper validation carrier against the accepted Native limits.

The command uses the existing worker command journal, owned process groups,
Git cache, validation worktree, mutation-settlement journal and PR `Effect`
representation. Legacy requested Coding completion and the new command share
head dispatch, PR creation, effect validation and original readback. Legacy
`Binding` fields retain their source-attempt meanings. New effects additionally
carry a typed requester/source binding; old fields are not repurposed.

The worker persists intent and possible-dispatch facts before network writes.
Git uses the existing empty-lease compare-and-create and exact ref readback.
Unknown push results read the same ref and commit. PR creation uses one original
marker and the existing configured provider adapter. A lost create response or
restart reads that marker and never creates again. Empty, conflicting or
unavailable readback remains unknown. A cancelled or stale requester cannot
start a fresh effect; a possibly dispatched effect can still retain and settle
its original facts. Process termination alone is not proof of remote absence.
Worker replacement reconciles the original command before registration or
reasoning can resume. A retired phase verifies the original canonical effect
and repairs only its local completion flag after a lost local acknowledgment.

This uses existing controller tables and fields. The new worker phase directory
is `publication-requests`, with strict version-1 records bound to exact retained
commands. Missing, aliased, changed or unsupported records refuse; they are not
reconstructed as authorization to send. Publication status reads remain outside
the closed automatic-read retry/retention classification. Neither a read-only
annotation nor `changed=false` makes publication intent, failed/unknown effects
or authority compactable.

## Format and composition handoff

| Carrier | Installed af5 | Recovered current | Captured publication | Selected union |
|---|---:|---:|---:|---:|
| Application wire | 24 | 25 | 26 | 27 |
| Controller schema | 25 | 26 | 27 | 28 |
| Worker state | 19 | 20 | 21 | 22 |
| Native helper frame | 8 | 8 | 8 | 8 |

The selected union uses fresh coordinated markers 27/28/22. Historical controller
schemas 12 through27 and worker formats9 through21 are inspected before an owned
atomic marker transition, without rewriting original rows or journals. Schema26
and worker20 admit identity-limit fields, paginated reads and retained-read
references; standalone schema27 and worker21 admit publication instead. Only the
union admits both vocabularies together. Neither predecessor marker may hide the
other branch's fields, commands, original invocations or outcomes. Wire peers
still require the current exact revision; historical storage admission is not a
claim of wire compatibility. Package format and Native helper frame are unchanged.

The native helper frame and serialized `NativeLoopProfile`/`ValidationInput`
fields remain unchanged. The additional tool schemas are values in the existing
carrier; actual Native consumer execution exercises that carrier. This is not a
protected package, installed-state or rollback qualification claim. Those
producers need their own source/artifact-bound compatibility handoff before a
release can use these markers.

The union must be qualified against both predecessor shapes. It preserves `metadata::RetainedOutcome::Inline` versus
`ReadReference`, original request/commit/release provenance checks in
`read_retained`, `Retained::lookup` and `Data::OriginalReadBodyUnavailable`.
External-operation lookup must remain first in `effects::GetOperation`. The
publication inventory scanner uses the existing
validated `scan_inline` behavior only for compact read inventory, without
admitting arbitrary compact bodies to writes or control paths. Preserve exact
original CallBody/OutcomeBody, caller/source/operation/commit, delivery/consumption,
ReadEvidence digests and custody/first-safe/release proof. A compact reference is
not absence, a reconstructed old response, a fresh read or a successful effect.
Keep the recovered cleanup-read journals, stopped worker rows, strict
missing/changed-content refusal, retention policy and old-reader rejection.
No af5 or captured historical test result qualifies this union. The recovered
workspace has no authored checkpoint; qualification of the composed source is
required independently.

Collision points are the shared protocol/profile catalogue, controller schema,
original effect settlement, publication inventories and worker marker/journal
admission. This is a descendant patch, not a wholesale replacement of recovered
files. Task19's protected Python/docs scope is untouched and is not a prerequisite.

## Local qualification and evidence

The seven task-declared argv vectors are retained in the qualification evidence.
They prepare/recheck the staged SQLite 3.54.0 archive offline, compile every
ordinary Rust test target, inventory and serially execute the exact `publication`,
controller `publication_mcp`, recovery `publication_mcp` and legacy recovery
`pull_request_publication` filters, then check the complete base diff. Inventory
must be nonempty and unique, and execution accounting must cover the inventory.
The exact selected Cargo path and binary hash are checked by each filter command.
No filters are passed to `build_v2.py`.

`.build/publication-validation/` retains original failures, subsequent command
logs, exact final source/file hashes and the final results handoff. The prescribed
list/test logs also remain under `.build/sqlite-publication/`. Existing recovery
fixtures retain source/image SHA-256 inventories, actual controller/worker logs,
local Git material, process outcomes and controlled provider observations beneath
their reported test artifact directory. These are test-only repositories,
registrations, definitions and synthetic credentials. Nothing is pushed to a
live branch, and no live PR, merge, protected orchestration, installation or
deployment is performed. Worker-owned source checkpointing remains the task's
completion path.

The controller cases exercise admission/parity, exact replay after selected-head
changes and cancellation, source/target/action/envelope refusals, pending versus
unknown versus exact settled proof, exhausted receipt reservations, and physical
SQLite reopen/migration/unsupported-reader refusal. Recovery cases exercise
actual Native and Coding shared consumers, real local Git, dropped Git and PR
acknowledgments, worker incarnation replacement and missing credential refusals.
The existing legacy PR recovery filter remains required. A green empty filter,
isolated mocked state machine or source-only association is insufficient.

## Proposed release-engineer data

The following complete definition is a proposal only. It has no publication
selection and is not registered or activated by this task. Its source and exact
clause identities are reusable charter data, not an authority grant. The Native
override retains the selected metadata group with explicit catalogue capacity;
69 functions describes this branch's V2 publication catalogue without manager
capabilities. Its schema/node allowances reuse the existing ordinary Native
fixture profile. A future composed catalogue and actual output/charter schema
must be checked again, not assumed to fit these numbers.

```json
{
  "id": "release-engineer",
  "revision": "publication-proposed-1",
  "executor": "native",
  "inference": {
    "model": "gpt-6-astra",
    "reasoning_effort": "max"
  },
  "execution_profile": {
    "kind": "collaboration_v2"
  },
  "limit_overrides": {
    "native": {
      "context_bytes": 4294967295,
      "input_bytes": 1048576,
      "output_bytes": 1048576,
      "result_bytes": 262144,
      "schema_bytes": 262144,
      "json_depth": 24,
      "json_nodes": 65536,
      "schema_expansion": 512,
      "validation_work": 8388608,
      "regex_bytes": 65536,
      "regex_dfa_bytes": 65536,
      "request_bytes": 4294967295,
      "response_wire_bytes": 4294967295,
      "helper_value_bytes": 4294967295,
      "max_output_tokens": 16384,
      "function_count": 70,
      "function_name_bytes": 64,
      "function_description_bytes": 4096,
      "call_id_bytes": 256,
      "function_calls_per_response": 1,
      "memory_notice_bytes": 16384,
      "endpoint_bytes": 4096,
      "api_key_env_bytes": 128,
      "api_key_bytes": 8192,
      "http_connect_timeout_ms": 10000,
      "error_body_bytes": 8192,
      "error_read_timeout_ms": 250,
      "decode_check_bytes": 1024,
      "helper_error_bytes": 128,
      "task_tool_observation_bytes": 4096,
      "summary_bytes": 8192,
      "decision_envelope_bytes": 256,
      "continuation_envelope_bytes": 1024,
      "task_tool_identity_bytes": 256
    },
    "coding": null,
    "collaboration": null,
    "memory_notice": null
  },
  "charter": {
    "kind": "inline",
    "source": {
      "files": [
        {
          "file": "roles/release-engineer.md",
          "text": "# Release Engineer\n\n## Purpose\n\nThe agent MUST select publication source from a succeeded accepted dependency or completed owned child.\n\n## Evidence\n\nThe agent MUST distinguish publication admission from completed external effects.\n\n## Recovery\n\nThe agent MUST reconcile an unknown publication using its original operation identity.\n\n## Scope\n\nThe agent MUST keep repository publication separate from protected release orchestration and deployment.\n\n## Review\n\nThe agent MUST bind any separately authorized post-review release orchestration to its actual recorded reviewer decision.\n"
        }
      ],
      "clauses": [
        {
          "file": "roles/release-engineer.md",
          "section": "Purpose",
          "sentence": "The agent MUST select publication source from a succeeded accepted dependency or completed owned child."
        },
        {
          "file": "roles/release-engineer.md",
          "section": "Evidence",
          "sentence": "The agent MUST distinguish publication admission from completed external effects."
        },
        {
          "file": "roles/release-engineer.md",
          "section": "Recovery",
          "sentence": "The agent MUST reconcile an unknown publication using its original operation identity."
        },
        {
          "file": "roles/release-engineer.md",
          "section": "Scope",
          "sentence": "The agent MUST keep repository publication separate from protected release orchestration and deployment."
        },
        {
          "file": "roles/release-engineer.md",
          "section": "Review",
          "sentence": "The agent MUST bind any separately authorized post-review release orchestration to its actual recorded reviewer decision."
        }
      ]
    }
  }
}
```

After the mechanism and actual target access are independently qualified, the
existing charter manager can create the intended full immutable definition with
`execution_profile.publication` populated from the exact registered repository
and target revisions, selected actions and namespace. In the saved observation,
`runtime` revision `1` has no PR target; it cannot supply a real target revision
for that proposed selection. Do not invent one or mutate registration as part
of this task. A later qualified registration/target is a concrete prerequisite
for activating the release-engineer capability. Native read bindings remain
read-only, and this proposal assigns no protected producer role or grant.
