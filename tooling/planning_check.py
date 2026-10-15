#!/usr/bin/env python3
"""Generate planning citations and run isolated native Duvet; never runtime evidence."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))
import capability_dag as dag
import citation_inventory as inventory
import duvet_native as native
import spec_set


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def citations(nodes: list[dict], draft: bool = True) -> str:
    lines = ["Generated planning associations only; not runtime implementation/test evidence.", ""]
    for node in sorted(nodes, key=lambda n: n["id"]):
        for ref in sorted(node["requirements"], key=lambda r: (r["file"], r["section"], r["sentence"])):
            inventory.relative_path(ref["file"])
            lines += [f'//= {ref["file"]}#{ref["section"]}',
                      '//= reason=' + native.citation_reason(node["id"], draft),
                      f'//# {ref["sentence"]}', ""]
    return "\n".join(lines)


def planning_config(original: dict) -> str:
    lines = [f"'$schema' = {json.dumps(original['$schema'])}", '',
             '[[source]]', 'pattern = "generated/planning-citations.txt"',
             'type = "implementation"', '']
    for spec in original["specification"]:
        if set(spec) != {"source", "format"} or spec["format"] != "markdown":
            raise ValueError("unsupported pinned specification block")
        lines += ['[[specification]]']
        lines += [f'{key} = {json.dumps(spec[key])}' for key in ("source", "format")]
        lines += ['']
    lines += ['[report.json]', 'enabled = true', 'path = "reports/planning.json"', '',
              '[report.html]', 'enabled = true', 'path = "reports/planning.html"', '',
              '[report.snapshot]', 'enabled = false', '']
    rendered = "\n".join(lines)
    assert tomllib.loads(rendered)["specification"] == original["specification"]
    return rendered


native_metrics = native.planning_metrics


def check(root: Path, source: Path, output: Path, proposal: bool = False, main_spec: Path | None = None) -> dict:
    lock, original = inventory.verify_snapshot(root)
    bundle = spec_set.load_reviewed(spec_set.implementation_main(root, main_spec))
    canonical_nodes, _, _ = spec_set.reviewed_declarations(bundle)
    source = spec_set.declaration_source(source, bundle, proposal)
    source_text = source.read_bytes().decode('utf-8')
    nodes = spec_set.without_review(dag.parse(source_text)) if proposal else canonical_nodes
    dag.dependency_order(nodes)  # Validate existing declarations, never infer edges.
    visible = "\n".join(line for _, line in dag.active_lines(source_text))
    for label, expected in [("Source snapshot", lock["snapshot_sha256"]),
                            ("Source selection digest", lock["files"][".duvet/config.toml"])]:
        if nodes and re.findall(r"^" + label + r": ([0-9a-f]{64})$", visible, re.M) != [expected]:
            raise ValueError("declaration pin mismatch: " + label)
    executable = root / ".tools/bin/duvet"
    install = root / ".tools/.crates.toml"
    versions = [key.split()[1] for key, bins in tomllib.loads(install.read_text())["v1"].items()
                if key.startswith("duvet ") and "duvet" in bins]
    if versions != ["0.4.3"]:
        raise ValueError("expected installed Duvet 0.4.3")
    inputs = [source, root / "spec-lock.json", executable, install,
              Path(__file__).resolve(), Path(dag.__file__), Path(inventory.__file__), Path(native.__file__),
              Path(spec_set.__file__), Path(__file__).resolve().parent / 'execution_plan.py']
    inputs += spec_set.input_paths(bundle)
    inputs += sorted((Path(__file__).resolve().parents[1] / 'tests/tooling').glob('test_*.py'))
    implementation_spec = Path(bundle['paths']['main'])
    inputs.append(implementation_spec)
    before = {str(p): dag.digest(p.read_bytes()) for p in inputs}
    producing_tools = dag.planning_tools()
    write_json(output / "input-manifest.json", before)
    stage = output / "duvet-root"
    for name in lock["files"]:
        target = stage / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / "spec-snapshot" / name, target)
    (output / "declarations.md").write_bytes(source_text.encode('utf-8'))
    generated = stage / "generated/planning-citations.txt"
    generated.parent.mkdir()
    generated.write_text(citations(nodes, proposal))
    config = stage / ".duvet/planning.toml"
    config.write_text(planning_config(original))
    commands = []
    for ci in ("false", "true"):
        command = [str(executable), "report", "--config-path", ".duvet/planning.toml",
                   "--ci", ci, "--require-citations", "true", "--require-tests", "false"]
        run = subprocess.run(command, cwd=stage, capture_output=True, text=True, timeout=60)
        log = output / f"duvet-ci-{ci}.log"
        log.write_text(run.stdout + run.stderr)
        commands.append({"command": command, "cwd": str(stage), "exit_code": run.returncode,
                         "log": str(log), "log_sha256": dag.digest(log.read_bytes())})
        write_json(output / "native-commands.json", commands)
        if ci == "false" and run.returncode:
            raise ValueError("native report failed; see " + str(log))
    report_path = stage / "reports/planning.json"
    report = json.loads(report_path.read_text())
    if set(report["specifications"]) != {s["source"] for s in original["specification"]}:
        raise ValueError("native report selection differs from pinned original")
    metrics = native_metrics(report)
    traceability = native.planning_traceability(nodes, report, dag.digest(report_path.read_bytes()), draft=proposal)
    write_json(output / "planning-traceability.json", traceability)
    occurrences = sum(len(n["requirements"]) for n in nodes)
    if metrics["citation_occurrences"] != occurrences:
        raise ValueError("native citation count differs from generated occurrences")
    if before != {str(p): dag.digest(p.read_bytes()) for p in inputs}:
        raise ValueError("inputs changed during report run")
    inventory.verify_snapshot(root)
    for name, expected in lock["files"].items():
        if dag.digest((stage / name).read_bytes()) != expected:
            raise ValueError("staged original input changed: " + name)
    expected_exit = 1 if metrics["missing_full_association"] or metrics["unmatched"] else 0
    if commands[-1]["exit_code"] != expected_exit:
        raise ValueError("unexpected native CI result; inspect native logs")
    spec_set.verify(bundle)
    if producing_tools != dag.planning_tools():
        raise ValueError('planning tooling changed during report run')
    result = {"run_id": output.name, "kind": "unapproved draft planning associations" if proposal else "authoritative-set planning associations",
              "proposal": proposal, "spec_set": bundle, "planning_tools": producing_tools,
              "runtime_evidence": False, "promotion_allowed": False,
              "source": str(source), "source_sha256": dag.digest(source_text.encode()),
              "implementation_spec_sha256": before[str(implementation_spec)],
              "source_copy": str(output / "declarations.md"), "duvet_version": versions[0],
              "duvet_binary_sha256": before[str(executable)],
              "snapshot_sha256": lock["snapshot_sha256"],
              "original_config_sha256": lock["files"][".duvet/config.toml"],
              "planning_config": str(config), "planning_config_sha256": dag.digest(config.read_bytes()),
              "generated_citations": str(generated), "generated_citations_sha256": dag.digest(generated.read_bytes()),
              "report_json": str(report_path), "report_json_sha256": dag.digest(report_path.read_bytes()),
              "staging_root": str(stage), "traceability_json": str(output / "planning-traceability.json"),
              "native_annotation_index_scope": "Offsets in report_json_sha256 only; never persistent identities.",
              "report_html": str(report_path.with_suffix(".html")),
              "input_manifest_sha256": dag.digest((output / "input-manifest.json").read_bytes()),
              "selected_specifications": len(original["specification"]),
              "capabilities": len(nodes), "edges": sum(len(n["prerequisites"]) for n in nodes),
              "metrics": metrics, "commands": commands,
              "duplicates": "Preserve each capability/reference occurrence and reason; count each native SPEC once.",
              "exact_reference_validation": "passed; whitespace folded, no wording substitutions",
              "limitations": ["Planning association, not DAG approval or runtime conformance.",
                              "The native inventory retains the known descriptive constitution keyword blockquote."]}
    write_json(output / "planning-metrics.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source", type=Path, help="defaults to main spec, resolving its authoritative capability link")
    parser.add_argument('--main-spec', type=Path, help='authored implementation main spec; defaults to sibling borg/spec/impl/v1/implementation.md')
    parser.add_argument("--proposal", action="store_true", help="explicitly label an external draft source as unapproved")
    args = parser.parse_args()
    root = args.root.resolve()
    parent = root / "artifacts/planning"
    parent.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    print("Planning artifacts:", output, flush=True)
    try:
        lock, _ = inventory.verify_snapshot(root)
        main_spec = spec_set.implementation_main(root, args.main_spec)
        source = args.source or main_spec
        result = check(root, source.resolve(), output, args.proposal, main_spec)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        write_json(output / "failure.json", {"error": str(error), "runtime_evidence": False})
        print(error, file=sys.stderr)
        return 2
    print(json.dumps({k: v for k, v in result["metrics"].items() if not k.endswith("indices")}, indent=2))
    return result["commands"][-1]["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
