# Duvet Cap

Duvet Cap reads capability and milestone declarations from the linked v1
specification set. Native Duvet 0.4.3 supplies the requirement/citation graph and
coverage. The thin layer validates exact references, schema, IDs, explicit
prerequisites, readiness and cycles, then generates deterministic graph and phase
views. Report-local native array positions are not persistent identities.

The six maintained modules are `capability_dag`, `execution_plan`,
`planning_check`, `spec_set`, `duvet_native` and `citation_inventory`. The last
provides existing snapshot/path/hash checks; its legacy standalone inventory is
not the planning catalog. Prototype-only `development_check`, its test suite and
`traceability-review.json` were removed with the implementation.

## Validation

From the repository root:

```sh
PYTHONPATH=tooling python3 -B -m unittest discover -s tests/tooling -p 'test_*.py' -v
python3 -B tooling/planning_check.py --root planning-inputs/current --main-spec ../borg/spec/impl/v1/implementation.md
```

For the printed native run directory, generate views with:

```sh
python3 -B tooling/capability_dag.py --root planning-inputs/current --main-spec ../borg/spec/impl/v1/implementation.md --source ../borg/spec/impl/v1/implementation-capabilities.md --duvet-metrics <run-directory>/planning-metrics.json --output-dir <run-directory>/dag
python3 -B tooling/execution_plan.py --root planning-inputs/current --main-spec ../borg/spec/impl/v1/implementation.md --duvet-metrics <run-directory>/planning-metrics.json --output-dir <run-directory>/execution
```

Repeat either view command with `--check` to verify deterministic output. A plan
can return exit 1 for approval/readiness blockers while `stale_outputs` is empty;
that is not a reproducibility failure. Exit 2 means invalid inputs/tool failure.

## Source And Review Boundaries

The default main specification is sibling `borg/spec/impl/v1/implementation.md`;
`--main-spec` supports explicit isolated subjects. Typed capability/phase links
remain within that implementation root. Review metadata and its consumed
provenance are hash-bound separately from the three normative document roles.
Missing, stale, unknown or conflicting metadata is rejected. No declaration is
approved merely because its wording is normative.

The unchanged root lock targets historical copied requirements. The explicit
current evaluation root retains the current authored declaration target without
changing that real lock. Reports bind source paths, all producing tools, source
bytes and review metadata, so historical or relocated outputs are not silently
accepted as current proof. No report authorizes execution or promotion.

Capability Source quotations are references to native requirements, not a second
authored normative inventory. Native planning completeness does not establish
implementation, test execution or trusted conformance. Runtime sources and their
historical generated evidence were deliberately removed in the implementation
reset; only fresh Duvet Cap validation is maintained here.
