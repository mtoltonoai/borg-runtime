#!/usr/bin/env python3
"""Duvet Cap phase views: explicit declarations and reported readiness, never execution."""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
from pathlib import Path
import re
import sys
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))
import capability_dag as dag
import spec_set

ID = re.compile(r"[A-Z][A-Z0-9_]*\Z")


def block(text: str, kind: str, required: bool) -> list[dict]:
    blocks = spec_set.document_parts(text)[1]
    if any(language.startswith("duvet-cap-") and language not in {"duvet-cap-stages", "duvet-cap-milestones"} for language, _ in blocks):
        raise ValueError("unknown or malformed Duvet Cap declaration fence")
    matches = [value for language, value in blocks if language == "duvet-cap-" + kind]
    if not matches and not required:
        return []
    if len(matches) != 1:
        raise ValueError("expected exactly one closed " + kind + " declaration block")
    data = tomllib.loads(matches[0])
    if set(data) != {kind} or not isinstance(data[kind], list):
        raise ValueError("expected only " + kind + " array")
    return data[kind]


def records(rows: list[dict], required: set[str], optional: set[str]) -> list[dict]:
    ids = set()
    result = []
    for original in rows:
        if not isinstance(original, dict) or required - original.keys() or original.keys() - required - optional:
            raise ValueError("missing or unknown declaration fields")
        row = dict(original)
        identity = row.get("id")
        if not isinstance(identity, str) or not ID.fullmatch(identity) or identity in ids:
            raise ValueError("missing, invalid or duplicate ID")
        ids.add(identity)
        row.setdefault("approval", "proposed")
        row.setdefault("approval_ref", "")
        if not isinstance(row["approval"], str) or row["approval"] not in {"approved", "proposed"} or not isinstance(row["approval_ref"], str):
            raise ValueError("invalid approval record")
        result.append(row)
    return sorted(result, key=lambda row: row["id"])


def strings(value, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(v, str) or not v for v in value) or len(set(value)) != len(value):
        raise ValueError("invalid or duplicate " + label)
    return sorted(value)


def ownership_paths(value) -> list[str]:
    paths = strings(value, "ownership paths")
    for name in paths:
        if name == "." or any(c in name for c in "*?[]") or name.startswith("/") or ".." in Path(name).parts or "\\" in name or str(Path(name)) != name:
            raise ValueError("ownership paths must be canonical relative paths")
    return paths


def reported_ownership(value) -> list[dict]:
    """Validate reported source responsibility, not identity or execution authority."""
    if not isinstance(value, list) or not value:
        raise ValueError("ownership must be a nonempty list")
    result, seen = [], set()
    for record in value:
        if (not isinstance(record, dict) or set(record) != {"owner", "role", "paths", "evidence"}
                or not isinstance(record["owner"], str) or not record["owner"].strip()
                or not isinstance(record["role"], str) or record["role"] not in {"author", "integrator"}
                or not isinstance(record["evidence"], str) or not record["evidence"].strip()):
            raise ValueError("invalid reported ownership record")
        identity = (record["owner"], record["role"])
        paths = ownership_paths(record["paths"])
        if not paths or identity in seen:
            raise ValueError("empty ownership paths or duplicate owner/role")
        seen.add(identity)
        result.append(dict(record, paths=paths))
    return sorted(result, key=lambda record: (record["owner"], record["role"]))


def parse_stages(text: str) -> list[dict]:
    rows = records(block(text, "stages", True), {"id", "order", "heading"}, {"approval", "approval_ref"})
    headings = re.findall(r"^### (.+)$", "\n".join(line for _, line in spec_set.active_lines(text)), re.M)
    orders = set()
    for row in rows:
        if type(row["order"]) is not int or row["order"] < 1 or row["order"] in orders:
            raise ValueError("invalid or duplicate stage order")
        if not isinstance(row["heading"], str) or headings.count(row["heading"]) != 1:
            raise ValueError("stage heading missing or ambiguous")
        orders.add(row["order"])
    return sorted(rows, key=lambda row: (row["order"], row["id"]))


def validate_milestones(rows: list[dict]) -> list[dict]:
    required = {"id", "capability", "stage", "title", "acceptance", "provides", "requires", "gates"}
    rows = records(rows, required, {"approval", "approval_ref", "owns", "scope"})
    for row in rows:
        for key in ("capability", "stage"):
            if not isinstance(row[key], str) or not ID.fullmatch(row[key]):
                raise ValueError("invalid " + key + " reference")
        for key in ("title", "acceptance"):
            if not isinstance(row[key], str) or not row[key].strip():
                raise ValueError("missing " + key)
        row["provides"] = strings(row["provides"], "interface IDs")
        if any(not ID.fullmatch(v) for v in row["provides"]):
            raise ValueError("invalid interface ID")
        row["requires"] = strings(row["requires"], "required interfaces")
        for ref in row["requires"]:
            parts = ref.split(":")
            if len(parts) != 2 or any(not ID.fullmatch(v) for v in parts):
                raise ValueError("invalid required interface reference")
        row["owns"] = ownership_paths(row.get("owns", []))
        row.setdefault("scope", "integration")
        if not isinstance(row["scope"], str) or row["scope"] not in {"integration", "interface"}:
            raise ValueError("invalid milestone scope")
        if not isinstance(row["gates"], list) or not row["gates"]:
            raise ValueError("missing acceptance/evidence gates")
        gates, seen = [], set()
        for gate in row["gates"]:
            if (not isinstance(gate, dict) or set(gate) != {"id", "expectation"}
                    or not isinstance(gate["id"], str) or not ID.fullmatch(gate["id"])
                    or gate["id"] in seen or not isinstance(gate["expectation"], str) or not gate["expectation"].strip()):
                raise ValueError("invalid or duplicate gate")
            seen.add(gate["id"])
            gates.append(dict(gate))
        row["gates"] = sorted(gates, key=lambda gate: gate["id"])
    return rows


def parse_milestones(text: str) -> list[dict]:
    return validate_milestones(block(text, "milestones", False))


def approved(row: dict) -> bool:
    return row.get("approval") == "approved" and bool(row.get("approval_ref", "").strip())


def overlap(left: list[str], right: list[str]) -> bool:
    return any(a == b or a.startswith(b + "/") or b.startswith(a + "/") for a in left for b in right)


def build_plan(capabilities: list[dict], stages: list[dict], milestones: list[dict], progress=None, draft=False) -> dict:
    milestones = validate_milestones(milestones)
    caps = {row["id"]: row for row in capabilities}
    phases = {row["id"]: row for row in stages}
    if len(caps) != len(capabilities) or len(phases) != len(stages):
        raise ValueError("duplicate capability or stage ID")
    dag.dependency_order(capabilities)
    by_id = {row["id"]: row for row in milestones}
    parents = {}
    for row in milestones:
        if row["capability"] not in caps or row["stage"] not in phases:
            raise ValueError("unknown capability or stage reference")
        parents[row["id"]] = set()
        for ref in row["requires"]:
            parent, interface = ref.split(":")
            if parent not in by_id or interface not in by_id[parent]["provides"]:
                raise ValueError("unknown prerequisite milestone or interface reference")
            parents[row["id"]].add(parent)
    pending = {key: set(value) for key, value in parents.items()}
    heap = [key for key, value in pending.items() if not value]
    heapq.heapify(heap)
    order = []
    while heap:
        item = heapq.heappop(heap)
        order.append(item)
        for other in sorted(pending):
            if item in pending[other]:
                pending[other].remove(item)
                if not pending[other]:
                    heapq.heappush(heap, other)
    if len(order) != len(milestones):
        raise ValueError("milestone prerequisite cycle")
    progress = {} if progress is None else progress
    if not isinstance(progress, dict) or progress.keys() - by_id.keys():
        raise ValueError("unknown milestone in progress overlay")
    ownership = {}
    effective_owns = {identity: row["owns"] for identity, row in by_id.items()}
    for identity, value in progress.items():
        if not isinstance(value, dict) or value.keys() - {"interfaces", "gates", "assignment", "ownership"}:
            raise ValueError("unknown progress fields")
        for kind, allowed, statuses in (("interfaces", set(by_id[identity]["provides"]), {"ready", "not_ready", "unknown"}),
                                        ("gates", {g["id"] for g in by_id[identity]["gates"]}, {"pass", "fail", "unknown"})):
            facts = value.get(kind, {})
            if not isinstance(facts, dict) or facts.keys() - allowed:
                raise ValueError("unknown " + kind + " progress reference")
            for fact in facts.values():
                if (not isinstance(fact, dict) or fact.keys() - {"status", "evidence"}
                        or not isinstance(fact.get("status"), str) or fact.get("status") not in statuses or not isinstance(fact.get("evidence", ""), str)):
                    raise ValueError("invalid progress status/evidence")
        if "assignment" in value and not isinstance(value["assignment"], str):
            raise ValueError("invalid worker assignment")
        if "ownership" in value:
            ownership[identity] = reported_ownership(value["ownership"])
            effective_owns[identity] = sorted(set(by_id[identity]["owns"]).union(
                path for record in ownership[identity] for path in record["paths"]))
    blockers = []
    if not capabilities:
        blockers.append("missing_approved_capability_declarations")
    if not milestones:
        blockers.append("missing_approved_milestone_declarations")
    if draft:
        blockers.append("unapproved_draft_view")
    views, ancestors = {}, {}
    for identity in order:
        row = by_id[identity]
        blocked = []
        for label, declaration in (("milestone", row), ("capability", caps[row["capability"]]), ("stage", phases[row["stage"]])):
            if not approved(declaration):
                blocked.append(label + "_approval_missing")
        if draft:
            blocked.append("unapproved_draft_view")
        if not effective_owns[identity]:
            blocked.append("ownership_boundary_missing")
        ancestors[identity] = set(parents[identity])
        for parent in parents[identity]:
            ancestors[identity].update(ancestors[parent])
        bound = {by_id[parent]["capability"] for parent in ancestors[identity]}
        unbound = sorted(set(caps[row["capability"]]["prerequisites"]) - bound)
        if row["scope"] == "integration":
            blocked += ["capability_prerequisite_interface_undeclared:" + cap for cap in unbound]
        for ref in row["requires"]:
            parent, interface = ref.split(":")
            fact = progress.get(parent, {}).get("interfaces", {}).get(interface, {})
            if views[parent]["readiness_blockers"]:
                blocked.append("prerequisite_not_ready:" + ref)
            elif fact.get("status") != "ready" or not fact.get("evidence", "").strip():
                blocked.append("interface_readiness_evidence_missing:" + ref)
        gates, incomplete = [], []
        for gate in row["gates"]:
            fact = progress.get(identity, {}).get("gates", {}).get(gate["id"], {})
            status, evidence = fact.get("status", "missing"), fact.get("evidence", "")
            gates.append(dict(gate, status=status, evidence=evidence))
            if status != "pass" or not evidence.strip():
                incomplete.append("gate_" + (status if status != "pass" else "evidence_missing") + ":" + gate["id"])
        complete = not blocked and not incomplete
        views[identity] = dict(row, ready=not blocked and not complete, complete=complete,
                               readiness_blockers=sorted(blocked), completion_blockers=sorted(incomplete), gates=gates,
                               deferred_capability_prerequisites=unbound if row["scope"] == "interface" else [],
                               assignment=progress.get(identity, {}).get("assignment", ""))
        if identity in ownership:
            views[identity].update(ownership=ownership[identity], effective_owns=effective_owns[identity])
        blockers += [identity + ":" + reason for reason in blocked]
    ready = sorted(key for key, view in views.items() if view["ready"])
    parallel = []
    for identity in ready:
        for group in parallel:
            if all(not overlap(effective_owns[identity], effective_owns[other]) for other in group):
                group.append(identity)
                break
        else:
            parallel.append([identity])
    stage_views = []
    for stage in sorted(stages, key=lambda row: (row["order"], row["id"])):
        ids = sorted(m["id"] for m in milestones if m["stage"] == stage["id"])
        reasons = [] if ids else ["milestone_declarations_missing"]
        if not approved(stage):
            reasons.append("stage_approval_missing")
        stage_views.append(dict(stage, milestones=ids, ready=[mid for mid in ready if mid in ids], blockers=reasons))
    return {"kind": "unapproved-draft" if draft else "authoritative-specification-set",
            "execution_authorized": False, "promotion_allowed": False,
            "evidence_basis": "Reported local interface/gate receipts; planner does not verify behavior or trusted conformance.",
            "dependency_order": order, "ready": ready, "parallel_ready_sets": parallel,
            "blockers": sorted(set(blockers)), "milestones": [views[mid] for mid in sorted(views)], "stages": stage_views}


def render(plan: dict, provenance: dict) -> dict[str, str]:
    value = dict(plan, provenance=provenance)
    lines = ["# Generated Phase Execution Plan", "", "Generated view only; not authorization to execute or promote.", "",
             "Source kind: " + plan["kind"], "", "## Approval And Readiness Blockers", ""]
    lines += ["- " + item for item in plan["blockers"]] or ["No declaration/readiness blockers recorded."]
    lines += ["", "## Stage Views", "", "| Stage | Milestones | Ready | Blockers |", "|---|---|---|---|"]
    for stage in plan["stages"]:
        lines.append(f'| {stage["id"]}: {stage["heading"]} | {", ".join(stage["milestones"]) or "none"} | {", ".join(stage["ready"]) or "none"} | {", ".join(stage["blockers"]) or "none"} |')
    lines += ["", "Dependency order: " + (", ".join(plan["dependency_order"]) or "none"), "",
              "Parallel-ready sets: " + (json.dumps(plan["parallel_ready_sets"]) if plan["parallel_ready_sets"] else "none"), "",
              "## Acceptance And Evidence Gates", ""]
    for milestone in plan["milestones"]:
        lines += ["### " + milestone["id"], "", milestone["acceptance"], ""]
        lines += [f'- {gate["id"]}: {gate["expectation"]} Status: {gate["status"]}; evidence: {gate["evidence"] or "missing"}.' for gate in milestone["gates"]]
        lines += [""]
    return {"execution-plan.json": json.dumps(value, sort_keys=True, indent=2) + "\n",
            "execution-plan.md": "\n".join(lines) + "\n"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--main-spec", type=Path, required=True)
    parser.add_argument("--duvet-metrics", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--progress", type=Path)
    parser.add_argument("--proposal-capabilities", type=Path)
    parser.add_argument("--proposal-milestones", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        draft = bool(args.proposal_capabilities or args.proposal_milestones)
        if draft and args.progress:
            raise ValueError("draft views do not accept progress overlays")
        bundle = spec_set.load_reviewed(args.main_spec)
        canonical_capabilities, canonical_stages, canonical_milestones = spec_set.reviewed_declarations(bundle)
        source = args.proposal_capabilities or Path(bundle["paths"]["capabilities"])
        milestone_source = args.proposal_milestones or Path(bundle["paths"]["phases"])
        captured = {}
        def read(path):
            path = path.resolve()
            if path not in captured:
                captured[path] = path.read_bytes()
            return captured[path]
        generator_paths = [Path(__file__).resolve(), Path(dag.__file__).resolve(),
                           Path(dag.native.__file__).resolve(), Path(spec_set.__file__).resolve(),
                           Path(dag.inventory.__file__).resolve()]
        generator = {"schema_version": 1, "python_version": sys.version,
                     "sources": {str(path): dag.digest(read(path)) for path in sorted(generator_paths)}}
        source_text = read(source).decode("utf-8")
        phases_text = read(Path(bundle["paths"]["phases"])).decode("utf-8")
        milestone_text = read(milestone_source).decode("utf-8")
        capabilities, stages, milestones = canonical_capabilities, canonical_stages, canonical_milestones
        if draft:
            capabilities = spec_set.without_review(dag.parse(source_text))
            stages = spec_set.without_review(parse_stages(phases_text))
            milestones = spec_set.without_review(parse_milestones(milestone_text))
        report, pins = dag.load_duvet(args.root.resolve(), args.duvet_metrics, bundle=bundle, source=source)
        if pins.get("spec_set_digest") != bundle["digest"] or pins["planning_source_sha256"] != dag.digest(source_text.encode()):
            raise ValueError("native planning report does not pin this specification set/source")
        if pins.get("proposal") != bool(args.proposal_capabilities):
            raise ValueError("native report capability source kind differs")
        traceability = dag.native.planning_traceability(capabilities, report, pins["duvet_json_sha256"], draft=pins["proposal"])
        progress = {}
        provenance = {"generator": generator, "spec_set": bundle, "native_duvet": pins, "native_metrics": traceability["metrics"],
                      "milestone_source": str(milestone_source.resolve()),
                      "milestone_source_sha256": dag.digest(read(milestone_source))}
        if args.progress:
            overlay = json.loads(read(args.progress))
            if set(overlay) != {"schema_version", "spec_set_digest", "milestones"} or overlay["schema_version"] != 1 or overlay["spec_set_digest"] != bundle["digest"]:
                raise ValueError("invalid or stale progress overlay")
            progress = overlay["milestones"]
            provenance["progress_sha256"] = dag.digest(read(args.progress))
        plan = build_plan(capabilities, stages, milestones, progress, draft)
        spec_set.verify(bundle)
        if any(path.read_bytes() != data for path, data in captured.items()):
            raise ValueError("execution-plan input changed during generation")
        outputs = render(plan, provenance)
        stale = dag.write_outputs(args.output_dir, outputs, args.check)
        print(json.dumps({"kind": plan["kind"], "stages": len(stages), "milestones": len(milestones),
                          "ready": plan["ready"], "blockers": plan["blockers"], "stale_outputs": stale if args.check else []}, sort_keys=True))
        return 1 if plan["blockers"] or (args.check and stale) else 0
    except (ValueError, OSError, KeyError) as error:
        print("Execution-plan generation failed:", error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
