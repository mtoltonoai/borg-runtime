# Duvet Cap Workspace

Before work, read AGENTS.md and constitution.md from the explicitly identified
Borg specification source, including the further required inputs they identify.
Use complete source content supplied in the accepted task instruction or retained
charter, or an explicitly available read-only source identified by the task or
coordinator. Retain its repository/revision and file identities; resolve relative
specification links within that source, not within this runtime checkout.

The coordinator's local authoring layout uses ../borg, but a registered worker
checkout does not include or imply that sibling. Coding repository MCP reads
select the task's accepted start or checkpoint source. An explicitly accepted
`instruction_source` additionally exposes `source="instructions"` for that exact
registered revision and commit; use it for required specification inputs. An
absent binding does not add that selector. Listing another repository in the
catalogue does not let a task read it. If required current
specification content is unavailable or incomplete, report the exact missing
input and do not proceed with work that depends on it. Do not silently skip the
instructions, substitute historical spec-snapshot content, infer a new read
binding, or create a checkout, mount or access grant to make the path exist.

Shared and v1 authored specifications remain in the Borg specification source
under spec/impl/v1. Borg is specifications-only.
This repository now retains Duvet Cap, its tests, installed Duvet and specification
inputs. The previous implementation and its generated build/test artifacts were
deleted at the user's request on 2026-10-13. Do not restore or resume implementation
unless the user explicitly instructs it.

The user has now instructed proceeding with the completed v2 build packet. New
v2 source and tests are authorized under src/ and tests/. The coordinator owns
shared manifests/protocol/entry points; four component owners have disjoint
controller, worker, native and product modules. Read spec/impl/v2/ in the identified
specification source for current requirements and the whole-batch work orders.
Do not restore old code.

Preserve the real spec-lock.json and all 58 copied spec-snapshot files. Current
planning uses the separate artifact evaluation snapshot; do not silently retarget
the real lock. Preserve unrelated work and Git history.

The maintained modules are capability_dag.py, citation_inventory.py,
duvet_native.py, execution_plan.py, planning_check.py and spec_set.py. The
citation_inventory module remains a source/path/hash utility dependency; native
Duvet supplies the planning requirement catalog and coverage.

For tooling changes, run:

    PYTHONPATH=tooling /usr/bin/python3 -B -m unittest discover -s tests/tooling -p 'test_*.py' -v

Use the selected /usr/bin/python3 interpreter: the package checker verifies its
exact identity. The coordination virtualenv's python3 is a different profile.

Use actual native Duvet reporting and deterministic graph/execution checks for
reporting changes. Keep planning, implementation/test citations, execution
readiness and behavioral conformance distinct. Never infer adoption from document
presentation or invent a gate baseline. The original baseline remains absent.

Keep this standalone host separate from Fleet/Agent Lab and other hosts. No
access/credential changes, publication, deployment, commit or push is implied by
specification or planning work. Follow the user's current authorization for
changes; retain separate historical decision provenance.

On this coordinator host, use the locally selected ordinary qualification
workflow and adopted standing retention policy described in `docs/v2-build.md`.
After tests and any required review finish, its mandatory completion step
automatically retires eligible test executable payloads and records their
retirement; do not add a per-payload approval or indefinite historical-image
hold. Preserve source/build identities, hashes, logs and results. Any temporary
payload hold must name its active consumer or candidate/deployed/rollback use,
owner and reference. This is local developer artifact handling, not worker
product cleanup or package retention. Other Borg checkouts need not resolve
this coordinator's artifact paths.
