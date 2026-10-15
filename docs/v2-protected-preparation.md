# Prepared candidate checks

This tooling joins the ordinary immutable release candidate to the existing
protected producer. It does not change the ordinary API, release record, database
schema, protected roles, or artifact/target human rollout authorization.
Installing a recipe and qualifying its actual accounts,
paths and cgroups remain deployment prerequisites.

## Fixed recipe, variable candidate data

`protected_release_v2.py check-prepared` accepts the existing producer options
(`--config`, `--input`, `--package`, `--recipe`, `--operation-id`, `--receipt`).
Its bounded input has exactly `subject` and `candidate`. `candidate` is the
ordinary `ReleaseCandidate` returned by successful preparation or its original
receipt lookup. The checkpoint includes its exact public repository ID and
registration revision; a legacy checkpoint without that binding is refused.
`subject` is the existing protected `Subject`, including the
exact artifact, source, repository/merge base and deployment target. The original
candidate remains Claimed/Unconfigured and is never rewritten. These input bytes
are data: the protected controller still independently checks its durable
candidate on Check admission. Preparation metadata does not grant authority.

Recipe revision 3 keeps a stable operator-configured digest. It adds one fixed
`preparation` entry with `path`, `sha256`, and a `profile`. The profile contains
revision 2, the exact module-name/hash inventory, already selected build
archive, toolchain and immutable Cargo-home base paths,
`cargo_manifest: {path, sha256}` for the exact selected dependency inventory,
and `limits: {path, sha256}` for the protected runtime metadata JSON.
Preparation profile revision 1 and a profile missing the Cargo inventory are
refused.
The generated controller and coding metadata select capacity 1/0; native
metadata selects 0/1. All other values are copied exactly from that pinned
file. The candidate runtime validates each file; the preparer supplies no
policy fallback. Updating the installed fixed module/profile digests requires
the existing explicit operator recipe update, never a candidate-driven repin. Generated metadata files and their CLI paths are immutable
prepared inputs. The preparer is protected source, pinned
before import. All imported checker modules and selected source readers are
verified, including the packager, builder, post-cleanup oracles, fixed launchers,
controlled peers, Git, Python and selected compiler toolchain. No input selects
an executable, import path, environment, remote or role.

The existing protected controller configuration supplies `repository_path`.
Preparation reads only the candidate's exact full immutable commit, verifies
object identities and the selected manifest/lock, then constructs a fresh
read-only object repository. It copies no hooks, alternates, mutable refs or
source configuration. This exact-source snapshot is candidate-readable, not
candidate-writable. The original source is neither checked out nor changed.

The selected Cargo base is immutable. Before a build starts, preparation verifies
its complete pinned inventory against the candidate manifest and lock, then
copies that inventory into a fresh job-local Cargo home. Regular files are
independent copies; only declared internal dependency symlinks are retained.
No ambient credentials or top-level Cargo configuration are copied. The base is
verified before and after copying and again during final prepared-input checks.
Only the new job-local copy is assigned to the execution identity, so Cargo can
materialize already-selected crate archives without write access to the shared
base. The mutable copy is never reused by another job or treated as protected
evidence. Existing cleanup terminates processes; retained job/cache files are
not claimed to have been deleted. This mechanism adds no filesystem or network
sandbox.

Two fixed live steps come first: `selected_build`, then
`actual_candidate_journey`. Existing other required recipe checks remain present;
this join cannot reduce the controller's required check set. The four token
arguments `{prepared_build}`, `{prepared_build_sha256}`, `{prepared_journey}`,
`{prepared_journey_sha256}` are expanded only by the pinned preparer. Fixed
launchers receive the generated input path and its digest. The recipe itself is
not regenerated or silently repinned for each candidate.

## Execution and evidence

The build runs the selected offline packager under the existing unprivileged
execution identity. After whole-group cleanup the pinned oracle verifies the
built package and all ten ReleaseArtifact fields against the requested subject.
A different build artifact fails this subject; it cannot inherit the candidate's
checks. Successful exit or candidate-written PASS text is insufficient.

The fixed journey uses the exact candidate binary and private controlled local
peers. Fresh fixture catalogue registration binds the existing seed and
publication endpoints; submitted coding tasks carry only its ID/revision and
start commit, with empty endpoint fields for canonical resolution. Registration
creates no repository access grant or acquisition. No repository-read store root
is configured by this journey. Definitions select Astra/max and collaboration
explicitly. Wire revision 8 uses `collab_task_get` with exact original result
continuation and configured `collab_task_delegate`, including canonical
agent/session associations. The controlled Codex protocol peer verifies the
selected effective model/effort and task-scoped MCP configuration; it is not
live Codex qualification. It observes coding-child delegation,
controller restart, lost settlement acknowledgment, worker loss, stale-owner
refusal, recovery restrictions, and complete terminal Task replay. Published
checkpoint contents are independently read after cleanup. It uses synthetic
local credentials only; it does not call a provider or infer real-provider,
browser, scale, host isolation, or complete release readiness.

Generated seed inputs and publication storage are distinct. The publication
repository is copied without hardlinks before its ownership is assigned to the
execution identity. Immutable input membership/hashes and module/profile pins
are rechecked around execution. Candidate work has no checker receipt or socket
FDs and no human/repository/rollout authority.

The existing owned-operation history, lock, exact cgroup identities, descendant
cleanup, recovery and original-request lookup/retry rules remain. Interrupted
jobs are not automatically rerun with a new Check identity. A malformed input,
source/profile drift, unknown process outcome or unverified cleanup cannot emit
a positive Check. A successful final Check still does not merge or roll out.

## Qualification boundary

Private tests exercise preparation and producer routing with explicit test
adapters. The actual local generated build/journey result is labeled by its exact
candidate and source hashes. It does not use the protected UID/cgroup adapter and
never submits a Check. Prior journey08 remains historical evidence for its own
candidate and old job; its PASS is not imported into a new release.

Required deployment inputs are concrete existing inputs: the real immutable
controller startup configuration; separately configured checker and execution
identities with no overlapping producer role; installed protected recipe/modules
and selected readable build tools/cache/archive and protected runtime metadata; an
exact local repository;
protected job/receipt directories; and exclusive preconfigured candidate/observer
cgroups with qualified cleanup. This batch provisions none of these. Other
applicable release checks and the final exact-artifact/target human grant remain
separate from this component.
