# Borg Runtime v2 And Duvet Cap

## Runtime Repository

The runtime repository is [mtoltonoai/borg-runtime](https://github.com/mtoltonoai/borg-runtime).
It contains maintained source, tests, tooling, build manifests, operator metadata
defaults and the unchanged historical specification lock and its copied inputs.
The authored current specification remains in the separate Borg specification
repository under `spec/impl/v2`.

Build caches, installed dependency binaries, generated planning and execution
evidence, and reset receipts remain local. The evidence links below identify
individual local runs; they are not bundled in a fresh Git clone and do not
establish that the latest tree has passed every release check. Existing deployed
repository registrations and accepted checkpoint identities retain their
recorded endpoints until an explicit catalogue revision changes new work.

The previous implementation, tests, copied execution sources and build artifacts
were deleted in the implementation reset requested on 2026-10-13. Duvet Cap,
specification inputs and Git metadata were retained; no implementation archive
was created. The user subsequently authorized the new v2 build from its completed
[build packet](../borg/spec/impl/v2/review-packet.md). Source is being implemented
in four independent modules under src/, with one package/integrator. Historical
prototype results do not qualify this new implementation.

## Current v2 Build And Evidence

The [latest local integration](artifacts/v2/build-60156282/metadata-limits-v41pfogc/protected-rollout-integration-01/run-03/receipt.json) passes all 17 protected-controller cases and 26 affected rollout-tooling cases. It exercises the real controller, SQLite and private producer socket, with controlled host/service effects. Original receipt reconciliation makes no repeated service effect. The earlier 257-test tooling suite remains a separate preceding-source run. The [memory batch-identity correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/memory-batch-identity-sizing-01/run-01/receipt.json) passes 27 affected cases after an old-source regression reproduced rejection of a valid notice. The shared formatter preserves the existing 66-byte batch identity and wire encoding. These local passes do not establish installed host/account/cgroup rollout or Codex internal-tool memory delivery.

The maintained source includes stable agent identities, distinct concurrent
sessions, shared agent memory, profile-bound model settings, the common
collaboration MCP surface, scoped repository reads and live browser views. The
[limits integration receipt](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-12/receipt.json)
records the earlier 577-case guarded union, not a new single all-pass full run.
The [native continuation correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-14/receipt.json)
then passed all 317 affected library, native and collaboration cases. It accepts
documented optional reasoning content in the internal model continuation and
excludes reasoning items from public progress after the original credential and
size checks. The [diagnostic successor](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-15/receipt.json)
passed 40 native library cases, including configured-connect-timeout versus OS
failure discrimination. These test sets overlap and must not be added together.
Before this latest local integration, maintained Rust/test source was combined21 plus the [header diagnostic successor](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-22/receipt.json) and the [bounded memory-notice correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-23/receipt.json), with the separately recorded release-helper and protected-journey compatibility corrections. The [repository catalogue integration](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-19/receipt.json) adds durable multi-repository registrations, immutable task/child revision bindings, separate source/checkpoint destinations and shared MCP/browser catalogue views. The broad regression recorded 605 passes and four fixture failures; [the affected rerun](artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-16/receipt.json) passes all four corrected cases, 21 MCP cases and 13 native-collaboration cases. These sets overlap; this is not a new single full all-pass run. Earlier failed receipts remain unchanged.

Current source uses wire 8, database schema 12, worker state format 9, cache identity 2 and publication phase 2. No existing deployed state was reset, migrated or retargeted by this source integration. The local multi-repository browser/native journey below retains its recorded source identity; transition of existing deployed state remains separate work. Header deadline expiry now names `transport.http_header_timeout_ms` in a content-free operator log; HTTP responses and parsing are unchanged. The [three parser and five schema tests](artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-19/receipt.json) pass. All [four real HTTP adapter regressions](artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-20/receipt.json) also pass. Header count/byte refusals and erased regex compilation-size errors still lack exact cause attribution.

Memory notification consumers are integrated for native independently required
requests and Codex pre-turn or controlled Borg MCP-result boundaries. The
[joined Codex normal-path receipt](artifacts/v2/build-60156282/memory-notification-consumers-01/joined-codex-normal-02/receipt.json)
uses the actual controller, worker, MCP bridge and pinned Codex with a controlled
local model peer. It verifies the notice, original tool result, canonical memory
reread and controller restart. The
[independent semantic review](artifacts/v2/build-60156282/memory-notification-consumers-01/review/final-semantic-review-01.json)
preserves the negative lost-ACK evidence and separate restart suffix. Arbitrary
active-turn injection can cause an extra model request; built-in shell and outer
asynchronous tool boundaries remain partially implemented and unqualified.
These checks do not establish live-model notification behavior.

The [memory scan-budget regression](artifacts/v2/build-60156282/metadata-limits-v41pfogc/memory-notice-exhaustion-01/negative-01/receipt.json) reproduced a premature no-notice result in the old implementation. The corrected source passes [24 affected tests](artifacts/v2/build-60156282/metadata-limits-v41pfogc/memory-notice-exhaustion-02/run-01/receipt.json): 13 Host cases, 9 controller cases, one controlled native notice journey and one Codex boundary cancellation case. An exhausted empty-prefix scan now refuses the next boundary with its exact metadata field unless the acknowledged sequence bounds prove drainage. Durable prefix and remaining notices survive; this is bounded refusal and recovery, not arbitrary-history eventual-delivery proof or closure of the separate Codex inner-tool gap.

The [repository negative-test run](artifacts/v2/build-60156282/metadata-limits-v41pfogc/repository-read-negatives-01/run-02/receipt.json) passes all 18 repository-query cases on its preceding exact source. Its new private-Git fixture proves external-diff and text-conversion helpers can execute, then verifies that all five canonical repository queries leave them inert and preserve every bare-repository file byte and mode. Other installed hook/filter settings are hostile inputs, not individually proven trigger paths. This does not establish concurrent hostile same-UID isolation. The memory and repository runs have separate captured source identities and are not one full-suite run.

The [preceding release package](artifacts/v2/build-60156282/template-combined-jg_5xbhp/current-release-02/completion.json)
and [verified deployment recovery](artifacts/v2/build-60156282/session-reset-lab2-start05-cgtnmb3j/actual-recovery-completion.json)
retain their original wire-6 source and executable identities. They are not
deployment evidence for the newly integrated metadata-limits source. The
separately authorized
[repository read-only grant](artifacts/v2/build-60156282/session-rollout-readonly-j8jy0r_h/repository-read-grant-completion.json)
is applied on agent-lab: the controller can read existing repository content and
has no write access. Future published checkpoint readability still needs testing.

The [current native association snapshot](artifacts/v2/build-60156282/repository-current-implementation-report-09/completion.json) captures maintained source at 2026-10-15 13:07:26 UTC: 165/240 implementation, 154/240 test and 154/240 paired associations. All prior 223 requirements remain; 17 clauses specify the already requested metadata-driven repository behavior. Normal/strict exits are 0/1 for missing coverage, with zero partial, unmatched or stale references. All 17 repository clauses now have scoped exact test associations. The [nine-test catalogue run](artifacts/v2/build-60156282/metadata-limits-v41pfogc/test-17/receipt.json) includes actual local permission denial before and after registration, unchanged ownership/modes, and the same accepted task reading successfully after fixture-only restoration. This is not remote ACL or credential-grant qualification. Missing associations are not missing-feature counts. The [reviewed association correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/checkpoint-26/integration.json) adds 20 exact implementation links and 17 test links over existing behavior. Its only new test logic verifies that two delivered memory-notice batches leave exactly the two explicitly submitted sessions; the [actual affected test passes](artifacts/v2/build-60156282/metadata-limits-v41pfogc/checkpoint-26/test-01/receipt.json). The subsequent [native boundary qualification](artifacts/v2/build-60156282/metadata-limits-v41pfogc/checkpoint-27/test-01/receipt.json) passes all 15 native-collaboration cases, including two new tests: native failure does not launch a configured coding executor, and a pending memory notice preserves the held original MCP result before delivery. These add two exact TEST associations. They use controlled local peers, not live providers or Codex internal tools. No production behavior changed. The native selector remains 135 source/test files. Seven rollout Python producer/test files are separately hash-bound supplemental inputs, not additional native coverage.

Separate [240/240 planning](artifacts/v2/build-60156282/metadata-limits-v41pfogc/repository-catalogue-01/authoring-validation/final-01/validation.json) passes normal/strict 0/0 and deterministic graph reproduction. The existing 5 capabilities, 12 milestones and their approval/readiness values are unchanged; execution-plan return 1 retains explicit readiness blockers with no stale outputs. Neither report establishes deployment or whole-system acceptance. [Traceability details](docs/v2-traceability.md) retain historical reports and source bindings.

The [current repository/native provider journey](artifacts/v2/build-60156282/metadata-limits-v41pfogc/repository-native-browser-actual-01/receipt.json) passed from 09:44:29 through 09:44:50 UTC on 2026-10-15, using combined19. The browser registered two synthetic local repositories and submitted a native task selecting one exact registration revision and commit. Two actual Astra/max requests surrounded one committed `collab_repository_read`; the model returned the selected repository's opaque marker, exact source and original receipt commit. Neither marker was supplied in the instruction, definition or output schema. The complete settled task, registration operations, tool receipt and result survived a fresh controller process. Both provider responses completed with HTTP 200; controller/worker/restarted-controller exits were zero, browser close completed, the listener closed and source/fixture guards passed. This does not qualify deployed-state transition, access-grant invariance, remote Git, Codex, coding, protected release or capacity.

The [current live Codex coding test](artifacts/v2/build-60156282/metadata-limits-v41pfogc/live-coding-current-01/completion.json) passed from 10:04:22 through 10:05:07 UTC on 2026-10-15. Actual pinned Codex with Astra/max edited the single required Rust line; the worker ran all 3 declared absolute compiler/test/build commands, acknowledged the exact registered checkpoint and retained the entire Task unchanged across a fresh controller process. The source test remained unchanged. Observed processes and the listener closed without forced cleanup. Codex's summary reported that rustc was unavailable through its ordinary PATH, so this does not claim model-driven build invocation. The independent worker checks passed. This is a tiny local fixture, not a Borg self-change, protected Check/merge, cgroup qualification or rollout.

The [current release-tool correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-release-tool-01/receipt.json) accepts wire 8 and binds the accepted immutable repository registration plus checkpoint destination. Saved-request retry/reconciliation and claimed/unconfigured qualification remain unchanged. All 19 targeted cases and the [full 214-test tooling suite](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-release-tool-01/full-tooling-receipt.json) pass. Protected journey/recipe compatibility and actual protected authority remain separate work.

The [coding prompt correction](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-integration-21/receipt.json) now supplies exact accepted build/test argument vectors to Codex without changing them or the independent checkpoint validation. Its 105 worker-library cases and 23 worker integration cases pass across the recorded ordinary and host contexts. One original nested-sandbox Codex preflight failure is preserved; the same no-model case passed in the established host context without changing socket permissions. This is command visibility and regression evidence, not proof that Codex ran a build.

The [live Borg-source journey](artifacts/v2/build-60156282/metadata-limits-v41pfogc/live-borg-self-development-01/completion.json) now passes on combined21: actual pinned Codex with Astra/max edits one title in a private copy of Borg, the worker independently builds and passes all 33 declared product checks, acknowledges checkpoint af339b765e1a3a45475afcfce5f5b1800d65a11e and preserves the complete accepted Task across restart. The model reports running the supplied build commands; exact inner Codex command accounting is not exposed. The [package](artifacts/v2/build-60156282/metadata-limits-v41pfogc/live-borg-self-development-01/package-01/receipt.json) and [ordinary preparation/reconciliation](artifacts/v2/build-60156282/metadata-limits-v41pfogc/ordinary-self-candidate-preparation-04/run-01/receipt.json) preserve that accepted source and result. The earlier ordinary-consumer harness failure remains recorded, not relabeled.

The [current protected journey compatibility update](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-protected-journey-01/receipt.json) binds current repository registration, metadata, shared MCP calls and exact result/session replay. Its 36 targeted tests and historical full 222-test tooling run pass. The [observer correction and current 231-test tooling run](artifacts/v2/build-60156282/metadata-limits-v41pfogc/maintained-build-observer-01/full-tooling-receipt.json) use the configured stage deadline without a shorter independent poll cutoff. The [actual accepted candidate's local checks](artifacts/v2/build-60156282/metadata-limits-v41pfogc/protected-local-current-run-preparation-02/run-01/result.json) independently rebuild the identical package and pass all five controlled journey cases, including original tool continuation, coding-child restart, lost acknowledgment, uncertain worker loss and terminal replay. Source guards and final cleanup pass. These checks use a same-account development adapter and controlled model/executor peers; they do not qualify installed protected accounts/cgroups, emit a protected Check, merge or roll out. [Current combined evidence](artifacts/v2/build-60156282/metadata-limits-v41pfogc/self-development-checkpoint-22/completion.json) keeps those limits explicit.

The [earlier native provider success](artifacts/v2/build-60156282/metadata-limits-v41pfogc/native-provider-actual-03/receipt.json)
ran from 08:43:54 through 08:44:12 UTC on 2026-10-15. A browser-created task made
two actual Astra/max model requests around a committed `collab_limits_get` call,
returned an accepted result, and retained the exact result, original tool receipt,
completion receipt and accepted metadata after a fresh controller restart.
Controller and worker processes exited cleanly, the listener closed and all source
and executable guards passed. No Codex, coding, deployment or capacity result is
inferred. Both earlier failed attempts remain preserved with their original
uncertain outcomes and request debits. The runtime image is combined14; the later
combined15 library-only diagnostic change is separately tested above. This earlier receipt remains historical rather than being rebound to combined19.

The [recorded self-delivery qualification](artifacts/v2/build-60156282/self-delivery-u_45m87n/completion.json)
uses an actual worker to edit a private copy of Borg source through a controlled
executor, build it, run the product tests from its acknowledged checkpoint and
return the accepted result. Two clean release builds of that exact commit produce
identical packages. The delivered executable serves the change, preserves the
accepted task/candidate across restart and reconciles preparation. This is a
controlled local source/worker/package join, not real Codex or protected CI.

Unsupported or corrupt SQLite startup now preserves existing WAL/SHM state.
The real historical-schema package is refused without rewriting its files.
Rollback-journal roots and incompatible empty-WAL modes refuse before recovery
can alter them. No migration or backup behavior is added.

Credential-bearing repository URLs now refuse before task/receipt persistence
and before standalone Git effects. Named authenticated proxy values use bounded
exact/decoded-value report guards; unsafe executor IDs refuse before owned journal
writes. The [credential-path receipt](artifacts/v2/build-60156282/credential-paths-j56zefql/completion.json)
retains the actual prior synthetic leak and corrected no-write result. These
finite checks do not qualify unknown transformed secrets or opaque executor state.

Known missing or partial controller storage now refuses before SQLite can create
a replacement or alter surviving sidecars. The [storage-loss receipt](artifacts/v2/build-60156282/store-loss-_h9dqdpk/completion.json)
includes the reproduced failure, fixed process probe and fresh-controller refusal
to recreate tasks or execute old worker journals. This adds no backup or restore
mechanism. The preceding 256/8 load remains evidence for its recorded executable.

Committed mutation replies now retain their original operation identity and
controller revision separately from the current observed revision. The
[reply-context receipt](artifacts/v2/build-60156282/reply-metadata-qrc806qw/completion.json)
binds wire4/schema7/worker-format5, 137 tooling tests, actual publication-before-intent
negative cases, the current preparation-tool join and controlled 256/8 restart/replay.
Uncommitted delegation refusals retain the original pending request. Large
receipts must fit both replay and original lookup before acceptance. Earlier
representations refuse unchanged; no retained data migration is performed.

Registration, command acknowledgment/admission and each external-effect settlement
observation now use durable mutation identities with exact recorded replay. Workers
journal request/response consumption and recheck current authority before effects.
The [preceding mutation replay receipt](artifacts/v2/build-60156282/mutation-replay-j8fewqvc/completion.json)
binds its historical wire3/schema6/worker-format4 checks, 134 tooling tests and a
256-native/8-coding controlled-load restart/replay run. Old private representations
refuse without rewriting; no existing data migration or deployment is performed.

The current application has controller, worker, native executor and browser
product modules. [Build instructions](docs/v2-build.md) prepare the exact SQLite
library and selected Rust dependencies without changing global configuration.
The [serde_json selection receipt](artifacts/v2/build-60156282/serde-json-alternative/completion.json)
records the available 1.0.151 pin and its actual numeric-path regression checks;
it does not claim to repair that release's upstream f32 comparison defect.

The [preceding local suite receipt](artifacts/v2/build-60156282/self-delivery-u_45m87n/completion.json)
records one coherent aggregate run of 267 passing Rust tests. Prior startup and
fixture failures remain recorded with their original source and outcomes. The documented
regression profile serializes libtest cases that otherwise compete for the same
bounded validator pool; application concurrency and test-internal actors remain
concurrent. A joined native,
coding-child and native-continuation test uses controlled model/executor peers
with actual application processes, SQLite, HTTP, Git and Rust checks. It survives
a lost delegation acknowledgment and controller restart with one child and one
accepted parent result. Separate hard-loss cases stop actual surviving native validators after worker and
controller loss before readiness or replacement admission. Interrupted provider
outcomes and request reservations remain unresolved. These local controlled peers
do not qualify real-provider delegation. Live-model tests and the host-context Codex
probe were excluded. The separate historical
[coding journey](artifacts/v2/build-60156282/coding-e2e-02/run.log)
passed on its recorded executable: a real Codex task edited synthetic Rust,
passed three checks, published an acknowledged local Git checkpoint and retained
the accepted result across controller restart. The
[browser form receipt on its earlier executable](artifacts/v2/build-60156282/delegation-68z1p__z/ui-browser-7JAcYt/receipt.json)
covers three native-form groups with no workers or model calls. These are distinct subjects, not a claim
that the current application has completed every v2 requirement.

The [earlier local mixed-load receipt](artifacts/v2/build-60156282/local-load-4bjl85jp/completion.json)
records four passing controlled-peer runs on its source: 32/2, 64/4, 128/8 and 256/8 native
requests/coding turns. The last run accepted 256 native and nine coding tasks,
with eight coding turns active together and sampled compiler overlap of eight.
Each coding task built, tested and published an actual private Git checkpoint;
all native requests remained held through that work. Exact replay, controller
and worker restart, source guards and cleanup passed. The
[load guide](docs/v2-local-load.md) explains the evidence and limits. These are
one-host component checks with a tiny Rust fixture, not supplied-Codex,
live-provider, sustained-load or four-host qualification. The recovery
batch reran 256/8 successfully on its recorded executable: 265 accepted tasks,
nine coding completions, sampled compiler overlap of seven, exact replay/restart
and cleanup. The earlier four ramps retain their original source identity.

The [initial artifact preparation receipt](artifacts/v2/build-60156282/release-artifact-uu1xhxx9/completion.json)
records two clean offline release builds of its earlier source with identical package bytes and a real
packaged-executable HTTP/task/restart smoke test. The
[packaging guide](docs/v2-package.md) describes exact-source preparation and
verification against an independently retained digest. Its 122 tooling tests
passed. The executable is not a provisioned cluster or protected release approval;
no model, worker or real browser was used in the packaging smoke test.

The [release workflow receipt on its recorded source](artifacts/v2/build-60156282/release-workflow-3w6mtgmr/completion.json)
binds an actual new source package to a controlled accepted coding task and real
local Git checkpoint publication. Package verification, candidate registration,
forced controller restart, exact reconciliation and the read-only browser views
passed. All 133 tooling tests passed. The [preparation guide](docs/v2-release-workflow.md)
describes these commands. Candidate metadata remains a claim; protected checks,
repository submission and human rollout authority remain unconfigured. Controlled
worker reports in this test do not establish a real Codex coding journey.

The current source checks opened kernel mount identities before SQLite access,
rejects unsupported or aliased storage without initialization, and preserves
existing controller locks. Actual ext4 acceptance, refusal and restart checks are
in the current suite receipt. Worker report/outbox checks now withhold structured
results and identities containing configured exact credential values, and redact
diagnostic strings. Synthetic credential tests preserve result and replay
semantics; they do not qualify unknown/transformed secrets or opaque executor state.

Current recovery follows retained delegation lineage when cancelling outstanding
descendants and computes queued reasons from the actual admission checks. Typed
native outcome observations keep lost/partial provider responses uncertain.
Completed results refused because of validation infrastructure are reconciled
from their immutable original receipt, with fresh acceptance checks and no model
rerun. Pending uncertainty and acceptance records are drained before replacement
worker registration; failure to persist uncertainty withholds later terminal
reports. These cases include actual local HTTP, SQLite and worker restarts.

The [historical native Duvet traceability report](artifacts/v2/build-60156282/self-delivery-u_45m87n/implementation-report-final/coverage.json)
captured 100 of 162 requirements with implementation citations and 92 with paired
test citations after semantic review of all 162 exact obligations. Strict checking remains
unsatisfied. These are annotation counts, not functional-completion percentages;
the report's copied input tree is its exact subject.
The [requirement audit](docs/v2-traceability.md) records scope and remaining gaps.
Its validation found a reconnect outbox race: active reporting and registration
could drain the same file concurrently. Both paths now use the existing mutex.
The controlled regression reproduces the old error and passes with the fix.
The same corrected executable passes the 256-native/8-coding controlled-load
restart/replay check; previous load receipts retain their original identities.

The preceding mutation/recovery receipts used wire revision 4, database schema 7
and worker state format 6. Unsupported older state was refused without rewriting it;
historical test roots remain historical. A durable controller-registry binding
and explicit worker native-tracking records govern cleanup; empty new directories
do not prove that untracked prior execution stopped. No existing state was migrated.

Live supplied-executor/delegation qualification, historical usage reconciliation, protected
release integration,
host resource enforcement and four-host load qualification remain incomplete.
The [selected worker-wide resource direction](../borg/spec/impl/v2/proposals/worker-resource-envelope.md)
uses one cgroup v2 per worker, ordinary startup and exhaustion/recovery qualification.
The [read-only worker verifier](docs/v2-worker-resources.md) now checks an explicit
operator profile before new work while preserving cleanup on refusal. Exact host
limits and separate quota work remain; no finite host controls or quota backend
are configured or qualified by this implementation.
The historical browser-created coding/Codex attempt was
[denied before execution](artifacts/v2/build-60156282/product/browser-coding-execution-blocked.json).
The newer native direct-tool browser journey below was allowed and executed.
Each receipt retains its own source and executable bindings; later source changes
do not silently update earlier evidence.

Shared and v1 authored specifications, proposals, decisions and review metadata
remain in the separate [specifications repository](../borg/spec/impl/v1/README.md).
Deletion does not change their requirements, selections or recorded approval states.

## Retained Files

- `tooling/`: six Duvet Cap modules and their documentation.
- `tests/tooling/`: six Python test suites.
- `.tools/`: installed Duvet 0.4.3 and its installation manifests.
- `spec-lock.json` and `spec-snapshot/`: the unchanged historical target and 58 copied files.
- `planning-inputs/current/`: the separate 59-file shared-source evaluation snapshot already targeted by current capability declarations.
- `docs/retrospective/`: small historical measurement and research documents, without implementation source or binaries.

The historical target is
`540189023a5d5b1f5c6baab0d1bbd8145d05511ac2178bc42c6bb119b5954d04`.
The separate current planning target is
`ae8c0fca8fe823f5bb53585dc12bd54c20058072de70f8510810f70e5f6867b1`.
The latter is an evaluation snapshot, not a replacement runtime lock or a claim
that a Git commit contains those source bytes.

## Checks

```sh
PYTHONPATH=tooling python3 -B -m unittest discover -s tests/tooling -p 'test_*.py' -v
python3 -B tooling/planning_check.py --root planning-inputs/current --main-spec ../borg/spec/impl/v1/implementation.md
```

Use the native run directory printed by the second command for the graph and
execution-plan commands in [the tooling guide](tooling/README.md). Reports are
regenerated after the reset and are bound to current source and tool hashes.
Planning associations do not demonstrate implementation or executed conformance.
No original gate snapshot baseline is created.

## Historical References

Older artifact paths cited by the authored decision records or retrospective
notes can refer to evidence intentionally deleted in this reset. Those citations
remain historical; no retained note or new report substitutes for the deleted
implementation evidence. The [reset receipt](reset-receipts/2026-10-13/completion.json) records the removed directory roots
and verifies retained inputs. Current build progress and dependency qualification are recorded under artifacts/v2/.

## Current Native Model And Direct-Tool Journey

The [native direct-tool batch](artifacts/v2/build-60156282/native-task-tool-jg6xkae0/completion.json) adds the configured `inspect_current_task_then_continue` workflow. The selected model requests `get_current_task`; the native Rust worker reads its own committed controller task, durably acknowledges the exact observation, then sends the second native model request. Both requests have cumulative durable reservations and fresh ownership checks after schema preflight. The final value still passes the original task schema and controller acceptance. There is no general tool catalogue or native shell.

The [actual live browser receipt](artifacts/v2/build-60156282/native-task-tool-jg6xkae0/live-native-browser-01/receipt.json) passed on2026-10-14 at16:33UTC in8.341seconds. It used `gpt-6.1-sol` for two real requests, with coding capacity zero and no Codex execution. The model's task identity, status and observation revision matched the exact committed tool receipt. The browser displayed the accepted result before and after controller restart. All processes exited0, the listener closed, and no owned helper records or processes remained. This establishes the bounded read-only native tool path, not arbitrary tool execution or the full deployment target.

The [component receipt](artifacts/v2/build-60156282/native-task-tool-jg6xkae0/component-completion.json) binds253 passing affected tests and25 explicitly carried unchanged passing tests, for278 distinct local test cases. Earlier compile/test failures remain recorded; this is not a claim of one new all-green aggregate invocation. Native Duvet now finds103/165 implementation and95/165 paired test associations (normal0/strict1); planning is165/165 (normal/strict0/0). Three native-tool obligations were added without retiring any of the earlier162.

The separate [measured coverage receipt](artifacts/v2/build-60156282/measured-coverage-s03exewg/completion.json) applies to the frozen pre-native-tool source. It measures85.245percent runtime-image line coverage, with explicit instrumentation failures/profile-loss limits and a critical-path review. It does not establish coverage of the new tool workflow or impose a percentage release gate.

The [preceding native release-package qualification](artifacts/v2/build-60156282/native-release-clywwq3s/completion.json) builds its recorded native-tool source with the maintained offline release recipe. The23,040,000-byte package verifies against its independently retained digest and private source commit. Its extracted release executable passed the real browser/native model-tool-model journey and controller restart in10.115seconds, with two requests and clean shutdown. This qualifies that source's release-profile helper path; it is not a protected release approval or deployment.

## Current Acceptance And Publication Recovery

The [acceptance-order batch](artifacts/v2/build-60156282/acceptance-order-elh7sdet/completion.json) adds actual validator-capacity/reconciliation, reconciliation-commit-failure, child-before-parent-stop, over-budget usage and Git-publication-failure cases. The Git case found a real defect: a tracked failed publication was also classified as an unknown executor outcome, so settling the original publication could not unblock retry. A private failure type now distinguishes those outcomes. Independent executor/provider uncertainty and unverified cleanup remain blocking; controller settlement rules and wire/storage formats are unchanged.

The batch has passing evidence for 215 distinct affected cases across library, controller, worker and recovery targets. One failed aggregate invocation and its corrected exact unit rerun are retained; this is not a new whole-suite pass. Independent source and evidence review passed. The newly packaged release executable also passed the exact Git failure, controller restart, original-ref readback/settlement and fresh-attempt case in 3.850 seconds. The executor is controlled and the Git repository is local; the explicit readback helper is linked into the test driver. No new model request or deployment was needed for this recovery correction.

Fresh native Duvet reporting remains 103/165 implementation and 95/165 paired test associations, with no partial or unmatched references (normal exit0, strict exit1). The unchanged specification and planning tools retain 165/165 planning associations under their exact prior bindings. The prior live native journey and measured line coverage retain their original source identities; neither is relabeled as a new run of this correction.

## Initial Model Budget Deferral

The [selected v2 policy](../borg/spec/impl/v2/implementation.md#initial-model-budget-policy) defers fine-grained cost/token and hidden supplied-executor request-budget enforcement. New browser submissions use the maximum supported request allowance; the budget selector is removed. Omitted coding-child allowances leave the parent's two native request reservations. Existing explicit inputs, immutable accepted tasks, native debits/child reservations, observed usage, deadlines, output/result bounds and host/provider limits retain their meanings. This removes the budget-policy release blocker without claiming that hidden Codex requests are enforced. [Source, validation and planning evidence](artifacts/v2/build-60156282/budget-deferral-r6rrqtoe/completion.json) identify the actual successor; older reports keep their original scope.

## Checkpoint Publication Crash Recovery

The [checkpoint recovery receipt](artifacts/v2/build-60156282/checkpoint-crash-recovery-_cw2bqua/completion.json) closes the reproduced gap between real Git publication and settlement-journal creation. A bounded durable phase binds the actual acknowledged executor association/completion and original accepted publication. Restart inspects the same ref and settles that original operation without rerunning coding or blindly pushing. A durable possible-send marker prevents an empty readback from being treated as not-applied after a remote receiver may have begun. Independent unknown executor effects remain blocked.

The final source passes123 affected tests, with107 unchanged controller tests carried from the preceding run:230 distinct affected passes, not a fresh full-suite claim. Eight crash-recovery functions cover16 scenarios, plus a new readback unit case and expanded old-format preservation. The packaged release passes the existing real Git failure/restart/readback case and an actual format5 refusal. Debug-only crash cuts do not claim release-crash or real-provider evidence. Worker format6 preserves/refuses earlier roots; wire4/database7 are unchanged. Current planning remains168/168; native annotations are104/168 implementation and96/168 paired, with strict implementation checking still unsatisfied.
