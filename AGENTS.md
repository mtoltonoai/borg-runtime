# Duvet Cap Workspace

Read ../borg/AGENTS.md and ../borg/constitution.md before work. Shared and v1
authored specifications remain in ../borg/spec/impl/v1. Borg is specifications-only.
This repository now retains Duvet Cap, its tests, installed Duvet and specification
inputs. The previous implementation and its generated build/test artifacts were
deleted at the user's request on 2026-10-13. Do not restore or resume implementation
unless the user explicitly instructs it.

The user has now instructed proceeding with the completed v2 build packet. New
v2 source and tests are authorized under src/ and tests/. The coordinator owns
shared manifests/protocol/entry points; four component owners have disjoint
controller, worker, native and product modules. Read ../borg/spec/impl/v2/ for
current requirements and the whole-batch work orders. Do not restore old code.

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
