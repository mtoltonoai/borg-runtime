#!/usr/bin/env python3
"""Extract a flat capability DAG from one Markdown declaration source.

Duvet supplies the pinned requirement extraction. This tool parses explicit
capabilities/edges; it infers neither from prose and maintains no requirements DB.
Generated files are outputs only. Validation is not architecture approval or
behavioral conformance evidence.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import heapq
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import citation_inventory as inventory
import duvet_native as native
import spec_set

ID = re.compile(r"[A-Z][A-Z0-9_]*\Z")
FIELDS = {"ID", "Title", "Prerequisites", "Deliverable", "Acceptance"}
REPEATED = {"Source", "Dependency"}
OPTIONAL = {"Approval", "Approval-Ref"}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


active_lines = spec_set.active_lines


def parse(text: str) -> list[dict]:
    nodes, fields, sources, edges = [], {}, [], []

    def finish() -> None:
        if not (fields or sources or edges):
            return
        missing = FIELDS - fields.keys()
        if missing:
            raise ValueError("missing fields: " + ", ".join(sorted(missing)))
        identity = fields["ID"]
        if not ID.fullmatch(identity):
            raise ValueError("invalid capability ID: " + identity)
        prerequisites = [] if fields["Prerequisites"] == "none" else [v.strip() for v in fields["Prerequisites"].split(",")]
        if any(not ID.fullmatch(v) for v in prerequisites) or len(set(prerequisites)) != len(prerequisites):
            raise ValueError(identity + ": invalid or duplicate prerequisite")
        if not sources:
            raise ValueError(identity + ": missing original source references")
        dependencies = {}
        for value in edges:
            parts = [part.strip() for part in value.split(" | ", 2)]
            if len(parts) != 3 or not parts[2] or parts[1] not in {"source-constraint", "inferred"}:
                raise ValueError(identity + ": invalid dependency provenance")
            parent, basis, reason = parts
            if parent in dependencies:
                raise ValueError(identity + ": duplicate dependency explanation")
            dependencies[parent] = {"prerequisite": parent, "basis": basis, "rationale": reason}
        if set(dependencies) != set(prerequisites):
            raise ValueError(identity + ": dependency explanations must match explicit prerequisites")
        refs = []
        for value in sources:
            address, separator, sentence = value.partition(" | ")
            file, marker, section = address.partition("#")
            if not separator or not marker or not file or not section or not sentence:
                raise ValueError(identity + ": invalid source reference")
            refs.append({"file": file, "section": section,
                         "sentence": inventory.canonical_sentence(sentence)})
        if len({(r['file'], r['section'], r['sentence']) for r in refs}) != len(refs):
            raise ValueError(identity + ": duplicate source reference")
        if fields.get("Approval", "proposed") not in {"approved", "proposed"}:
            raise ValueError(identity + ": invalid approval status")
        nodes.append({"id": identity, "title": fields["Title"], "deliverable": fields["Deliverable"],
                      "approval": fields.get("Approval", "proposed"), "approval_ref": fields.get("Approval-Ref", ""),
                      "acceptance": fields["Acceptance"], "prerequisites": sorted(prerequisites),
                      "dependencies": [dependencies[k] for k in sorted(dependencies)],
                      "requirements": sorted(refs, key=lambda r: (r['file'], r['section'], r['sentence']))})
        fields.clear(); sources.clear(); edges.clear()

    for number, line in active_lines(text):
        if line == "---":
            finish()
            continue
        key, separator, value = line.partition(":")
        if separator and key in FIELDS | REPEATED | OPTIONAL:
            value = value.strip()
            if not value:
                raise ValueError(f"line {number}: empty {key}")
            if key != "ID" and "ID" not in fields:
                raise ValueError(f"line {number}: missing ID before {key}")
            if key in FIELDS | OPTIONAL:
                if key in fields:
                    raise ValueError(f"line {number}: duplicate {key} or missing record separator")
                fields[key] = value
            elif key == "Source":
                sources.append(value)
            else:
                edges.append(value)
        elif fields and line:
            raise ValueError(f"line {number}: unsupported field/prose inside capability: {line}")
    finish()
    ids = [n['id'] for n in nodes]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate capability ID")
    return sorted(nodes, key=lambda n: n['id'])


def dependency_order(nodes: list[dict]) -> list[str]:
    by_id = {n['id']: n for n in nodes}
    remaining = {n['id']: set(n['prerequisites']) for n in nodes}
    if any(parent not in by_id for parents in remaining.values() for parent in parents):
        raise ValueError("unknown prerequisite")
    ready = [key for key, parents in remaining.items() if not parents]
    heapq.heapify(ready)
    result = []
    while ready:
        key = heapq.heappop(ready)
        result.append(key)
        for other in sorted(remaining):
            if key in remaining[other]:
                remaining[other].remove(key)
                if not remaining[other]:
                    heapq.heappush(ready, other)
    if len(result) != len(nodes):
        raise ValueError("dependency cycle")
    return result




def planning_tools() -> dict:
    directory = Path(__file__).resolve().parent
    return {str(directory / name): digest((directory / name).read_bytes()) for name in
            ('planning_check.py', 'capability_dag.py', 'execution_plan.py',
             'spec_set.py', 'duvet_native.py', 'citation_inventory.py')}


def load_duvet(root: Path, metrics_path: Path, *, bundle=None, source=None) -> tuple[dict, dict]:
    lock, _ = inventory.verify_snapshot(root)
    metrics = json.loads(metrics_path.read_text())
    if bundle is not None:
        spec_set.verify_report_context(metrics, bundle, source, planning_tools())
    if metrics.get('runtime_evidence') is not False or metrics.get('duvet_version') != '0.4.3':
        raise ValueError('DAG traceability requires an isolated native planning report from Duvet 0.4.3')
    report_path = Path(metrics['report_json'])
    stage = Path(metrics['staging_root'])
    for name, expected in lock['files'].items():
        if digest((stage / name).read_bytes()) != expected:
            raise ValueError('Duvet input differs from pinned source: ' + name)
    selection = lock['files']['.duvet/config.toml']
    if metrics['original_config_sha256'] != selection:
        raise ValueError('Duvet selection differs from pinned selection')
    if not metrics['commands'] or metrics['commands'][0]['exit_code'] != 0:
        raise ValueError('native Duvet report did not succeed')
    raw = report_path.read_bytes()
    if digest(raw) != metrics.get('report_json_sha256'):
        raise ValueError('Duvet report differs from its producing run hash; generate fresh native metrics')
    report = json.loads(raw)
    if 'spec_set' in metrics:
        spec_set.verify(metrics['spec_set'])
    pins = {'source_commit': lock['commit'], 'snapshot_sha256': lock['snapshot_sha256'],
            'selection_sha256': selection, 'duvet_version': metrics['duvet_version'],
            'duvet_json': str(report_path), 'duvet_json_sha256': digest(raw),
            'planning_source_sha256': metrics['source_sha256']}
    if 'spec_set' in metrics:
        pins['spec_set_digest'] = metrics['spec_set']['digest']
        pins['proposal'] = metrics['proposal']
    return report, pins


def render(source_text: str, source_name: str, kind: str, report: dict, pins: dict, reviewed_nodes=None) -> dict[str, str]:
    nodes = parse(source_text) if reviewed_nodes is None else reviewed_nodes
    order = dependency_order(nodes)
    visible = '\n'.join(line for _, line in active_lines(source_text))
    for label, key in [('Source snapshot', 'snapshot_sha256'), ('Source selection digest', 'selection_sha256')]:
        values = re.findall(r'^' + label + r': ([0-9a-f]{64})$', visible, re.M)
        if nodes and values != [pins[key]]:
            raise ValueError('missing or mismatched explicit ' + label)
    if digest(source_text.encode()) != pins['planning_source_sha256']:
        raise ValueError('declaration source differs from native planning report input')
    traceability = native.planning_traceability(nodes, report, pins['duvet_json_sha256'], draft=pins.get('proposal', True))
    metrics = traceability['metrics']
    graph = {'source': {'path': source_name, 'sha256': digest(source_text.encode()), 'kind': kind},
             'pins': pins, 'nodes': nodes, 'dependency_order': order,
             'edge_semantics': 'Explicit prerequisite IDs and authored per-edge rationales; semantic adequacy remains a review obligation. No semantic extraction or architecture approval is claimed.',
             'native_traceability': traceability}
    provenance = Counter(edge['basis'] for n in nodes for edge in n['dependencies'])
    count = sum(len(n['prerequisites']) for n in nodes)
    lines = ['# Generated Top-Level Capability DAG', '', 'Generated output only. Edit the authored declaration source, never this file.', '',
             f'Source: `{source_name}` ({kind}); SHA-256 `{graph["source"]["sha256"]}`.', '',
             f'{len(nodes)} capabilities; {count} prerequisite edges. Edge declarations: {dict(sorted(provenance.items()))}.', '',
             'Node grouping and edge rationale require review; the tool does not infer dependencies from Duvet or prose.', '']
    if nodes:
        lines += ['```mermaid', 'flowchart TD']
        for node in nodes:
            title = node['title'].replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
            lines.append(f'  {node["id"]}["{node["id"]}: {title}"]')
        for node in nodes:
            lines.extend(f'  {parent} --> {node["id"]}' for parent in node['prerequisites'])
        lines += ['```', '']
    else:
        lines += ['No capability declarations are present. An empty graph does not approve or complete a decomposition.', '']
    lines += ['Dependency order (stable-ID tie breaking): ' + (', '.join(order) or '(empty)'), '',
              f'Native Duvet planning associations: {metrics["covered"]}/{metrics["total"]}; {metrics["unmapped"]} unmapped; {metrics["partial"]} partial.', '',
              'Traceability uses native Duvet citation links. Original file/section/sentence identities and stable capability IDs are resolved for this report; array indices are only offsets within its pinned hash. Planning associations are not runtime implementation/test or execution evidence.']
    return {'implementation-dependency-dag.json': json.dumps(graph, indent=2, sort_keys=True) + '\n',
            'implementation-dependency-dag.md': '\n'.join(lines) + '\n',
            'dependency-order.txt': '\n'.join(order) + ('\n' if order else '')}


def write_outputs(output: Path, results: dict[str, str], check: bool) -> list[str]:
    stale = [name for name, value in results.items() if not (output / name).is_file() or (output / name).read_bytes() != value.encode('utf-8')]
    if not check:
        output.mkdir(parents=True, exist_ok=True)
        for name, value in results.items():
            (output / name).write_bytes(value.encode('utf-8'))
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--duvet-metrics', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--proposal', action='store_true', help='label an external unapproved declaration draft')
    parser.add_argument('--check', action='store_true', help='compare generated output without writing')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--main-spec', type=Path, help='authored implementation main spec; defaults to sibling borg/spec/impl/v1/implementation.md')
    args = parser.parse_args()
    try:
        root, source = args.root.resolve(), args.source.resolve()
        lock, _ = inventory.verify_snapshot(root)
        bundle = spec_set.load_reviewed(spec_set.implementation_main(root, args.main_spec))
        canonical_nodes, _, _ = spec_set.reviewed_declarations(bundle)
        source = spec_set.declaration_source(source, bundle, args.proposal)
        report, pins = load_duvet(root, args.duvet_metrics, bundle=bundle, source=source)
        if pins.get('spec_set_digest') != bundle['digest'] or pins.get('proposal') != args.proposal:
            raise ValueError('native planning report does not match linked set/source kind')
        source_text = source.read_bytes().decode('utf-8')
        nodes = spec_set.without_review(parse(source_text)) if args.proposal else canonical_nodes
        results = render(source_text, str(source), 'proposal' if args.proposal else 'implementation-spec', report, pins, nodes)
        spec_set.verify(bundle)
        stale = write_outputs(args.output_dir, results, args.check)
        graph = json.loads(results['implementation-dependency-dag.json'])
        print(json.dumps({'source_kind': graph['source']['kind'], 'capabilities': len(graph['nodes']),
                          'edges': sum(len(n['prerequisites']) for n in graph['nodes']),
                          'native_planning_covered': graph['native_traceability']['metrics']['covered'],
                          'duvet_requirements': graph['native_traceability']['metrics']['total'],
                          'unmapped_requirements': graph['native_traceability']['metrics']['unmapped'],
                          'stale_outputs': stale if args.check else []}, sort_keys=True))
        return 1 if args.check and stale else 0
    except (ValueError, OSError, KeyError) as error:
        print('Capability DAG validation failed:', error, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
