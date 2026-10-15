"""Thin adapter over pinned Duvet 0.4.3 JSON, not a normative-text extractor."""
from __future__ import annotations


def identity(row: dict) -> tuple[str, str, str]:
    return row["target_path"], row["target_section"], " ".join(row["comment"].split())


def requirement_index(report: dict) -> dict:
    result = {}
    for index, row in enumerate(report["annotations"]):
        if row.get("type") != "SPEC":
            continue
        key = identity(row)
        if key in result:
            raise ValueError("ambiguous identity in native Duvet extraction")
        result[key] = index
    return result


def validate_references(nodes: list[dict], report: dict) -> dict:
    """Resolve authored exact references anew, never persist a positional ID."""
    requirements = requirement_index(report)
    resolved = {}
    for node in nodes:
        resolved[node["id"]] = []
        for ref in node["requirements"]:
            key = ref["file"], ref["section"], " ".join(ref["sentence"].split())
            if key not in requirements:
                raise ValueError(node["id"] + ": unresolved exact original requirement: " + repr(key))
            resolved[node["id"]].append((ref, requirements[key]))
    return resolved


def planning_metrics(report: dict) -> dict:
    """Count only native SPEC citation byte coverage; no custom mapping count."""
    specs, covered, partial, linked = [], [], [], set()
    citation_ids = set()
    for index, row in enumerate(report["annotations"]):
        kind = row.get("type", "CITATION")  # Native 0.4.3 omits the default type.
        if kind == "SPEC":
            specs.append(index)
            status = report["statuses"].get(str(index), {})
            size, count = status.get("spec", 0), status.get("citation", 0)
            if size <= 0 or count > size:
                raise ValueError("invalid native SPEC coverage size")
            if count == size:
                covered.append(index)
            elif count:
                partial.append(index)
            linked.update(status.get("related", []))
        elif kind == "CITATION" and row["source"] == "generated/planning-citations.txt":
            citation_ids.add(index)
        else:
            raise ValueError("non-planning annotation in isolated report")
    if not specs:
        raise ValueError("native report has no requirements")
    unmatched = sorted(citation_ids - linked)
    return {"basis": "native Duvet SPEC statuses: citation bytes equal spec bytes",
            "total": len(specs), "covered": len(covered),
            "unmapped": len(specs) - len(covered) - len(partial), "partial": len(partial),
            "missing_full_association": len(specs) - len(covered),
            "citation_occurrences": len(citation_ids), "unmatched": len(unmatched),
            "covered_spec_indices": covered, "partial_spec_indices": partial,
            "unmapped_spec_indices": sorted(set(specs) - set(covered) - set(partial)),
            "unmatched_citation_indices": unmatched}



def citation_reason(capability: str, draft: bool = True) -> str:
    return f"Capability {capability}; " + ("unapproved " if draft else "") + "planning association only."


def planning_traceability(nodes: list[dict], report: dict, report_sha256: str, draft: bool = True) -> dict:
    resolved = validate_references(nodes, report)
    links = []
    for capability, refs in sorted(resolved.items()):
        for ref, spec_index in refs:
            related = report["statuses"].get(str(spec_index), {}).get("related", [])
            citations = []
            for index in related:
                annotation = report["annotations"][index]
                if (annotation.get("type", "CITATION") == "CITATION"
                        and annotation.get("source") == "generated/planning-citations.txt"
                        and annotation.get("comment") == citation_reason(capability, draft)):
                    citations.append(index)
            if not citations:
                raise ValueError(capability + ": no native planning citation link for " + repr(ref))
            links.append({"capability_id": capability, "requirement": ref,
                          "native_report_sha256": report_sha256,
                          "report_local_spec_index": spec_index,
                          "report_local_citation_indices": sorted(citations)})
    return {"basis": "Native Duvet SPEC statuses and related citation graph",
            "native_report_sha256": report_sha256,
            "index_scope": "All array indices are local offsets in this exact report hash, not persistent identities.",
            "metrics": planning_metrics(report), "links": links,
            "runtime_evidence": False}
