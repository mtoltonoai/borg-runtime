"""Resolve only the two explicitly typed authoritative links in the main spec."""
import hashlib
import json
from pathlib import Path
import re


def document_parts(text: str) -> tuple[list[tuple[int, str]], list[tuple[str, str]]]:
    """Visible top-level lines and fences, excluding comments and outer examples."""
    lines, blocks = [], []
    fence = None
    content = []
    comment = False
    for number, original in enumerate(text.splitlines(), 1):
        if fence is not None:
            delimiter, length, language = fence
            if re.fullmatch(r" {0,3}" + re.escape(delimiter) + "{" + str(length) + r",}\s*", original):
                blocks.append((language, "\n".join(content)))
                fence, content = None, []
            else:
                content.append(original)
            continue
        if not comment and (original.startswith("    ") or original.startswith("\t")):
            continue
        line, visible = original, ""
        while line:
            if comment:
                _, end, line = line.partition("-->")
                if not end:
                    line = ""
                else:
                    comment = False
            else:
                before, start, line = line.partition("<!--")
                visible += before
                if start:
                    comment = True
                else:
                    line = ""
        opening = re.match(r" {0,3}(`{3,}|~{3,})(.*)$", visible)
        if opening:
            delimiter = opening[1]
            language = opening[2].strip() if not visible[0].isspace() else ""
            fence = delimiter[0], len(delimiter), language
            continue
        if visible and visible[0].isspace():
            continue
        lines.append((number, visible.rstrip()))
    if fence is not None:
        raise ValueError("unterminated code fence")
    if comment:
        raise ValueError("unterminated HTML comment")
    return lines, blocks


def active_lines(text: str):
    yield from document_parts(text)[0]


def load(main: Path) -> dict:
    main = main.resolve()
    text = "\n".join(line for _, line in active_lines(main.read_bytes().decode("utf-8")))
    paths = {"main": main}
    for role, label in (("capabilities", "Implementation Capability Specification"),
                        ("phases", "Phased Implementation Specification")):
        links = re.findall(r"\[" + re.escape(label) + r"\]\(([^)]+)\)", text)
        if len(links) != 1:
            raise ValueError("expected one authoritative link for " + role)
        link = links[0]
        if ":" in link or "#" in link or Path(link).is_absolute():
            raise ValueError("authoritative link must be a local relative document")
        target = (main.parent / link).resolve()
        if not target.is_relative_to(main.parent) or not target.is_file():
            raise ValueError("authoritative link missing or outside implementation specification root")
        if target in paths.values():
            raise ValueError("authoritative document roles must be distinct")
        paths[role] = target
    hashes = {role: hashlib.sha256(path.read_bytes()).hexdigest() for role, path in paths.items()}
    digest = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"paths": {role: str(path) for role, path in paths.items()}, "hashes": hashes, "digest": digest}


def declaration_source(source: Path, bundle: dict, proposal: bool = False) -> Path:
    source = source.resolve()
    if source == Path(bundle["paths"]["main"]):
        source = Path(bundle["paths"]["capabilities"])
    if source != Path(bundle["paths"]["capabilities"]) and not proposal:
        raise ValueError("external declaration source requires --proposal")
    return source


def verify(bundle: dict) -> None:
    current = load(Path(bundle["paths"]["main"]))
    if "review_metadata" in bundle:
        current = bind_review(current)
    if current != bundle:
        raise ValueError("linked specification or review inputs, roles or digest changed")


def implementation_main(root: Path, explicit: Path | None = None) -> Path:
    """The shared-spec lock does not choose an implementation's authored spec."""
    return (explicit if explicit is not None else
            root.parent / 'borg/spec/impl/v1/implementation.md').resolve()


def verify_report_context(metrics: dict, bundle: dict, source: Path, tools: dict) -> None:
    """Content equality alone cannot relocate a report or its producing tools."""
    recorded = metrics.get('spec_set', {}).get('paths', {})
    expected = {role: str(Path(path).resolve()) for role, path in bundle['paths'].items()}
    actual = {role: str(Path(path).resolve()) for role, path in recorded.items()}
    if actual != expected or Path(metrics.get('source', '')).resolve() != source.resolve():
        raise ValueError('native planning report belongs to different specification/source paths')
    if metrics.get('spec_set', {}).get('review_inputs', {}) != bundle.get('review_inputs', {}):
        raise ValueError('native planning report has different or stale review metadata inputs')
    if metrics.get('planning_tools') != tools:
        raise ValueError('native planning report does not bind the current tooling paths and bytes')


# Review authorization is a separate input, never a fourth normative role.
REVIEW_FILE = 'implementation-review.json'
REVIEW_GROUPS = ('capabilities', 'stages', 'milestones')


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate review metadata key: ' + key)
        result[key] = value
    return result


def bind_review(bundle: dict) -> dict:
    """Bind explicit review state to exact normative bytes and local provenance."""
    root = Path(bundle['paths']['main']).parent
    path = root / REVIEW_FILE
    if path.is_symlink() or not path.is_file():
        raise ValueError('missing or nonlocal review metadata: ' + str(path))
    raw = path.read_bytes()
    data = json.loads(raw, object_pairs_hook=_unique_object)
    required = {'schema_version', 'source_hashes', 'provenance', *REVIEW_GROUPS}
    if not isinstance(data, dict) or set(data) != required or type(data['schema_version']) is not int or data['schema_version'] != 1:
        raise ValueError('invalid review metadata schema')
    if data['source_hashes'] != bundle['hashes']:
        raise ValueError('review metadata binds stale normative source hashes')
    if not isinstance(data['provenance'], dict) or not data['provenance']:
        raise ValueError('missing review provenance')
    inputs = {str(path): hashlib.sha256(raw).hexdigest()}
    for name, expected in data['provenance'].items():
        target = root / name
        if (not isinstance(name, str) or not isinstance(expected, str)
                or not re.fullmatch(r'[0-9a-f]{64}', expected)
                or Path(name).is_absolute() or '..' in Path(name).parts
                or '\\' in name or str(Path(name)) != name or target.is_symlink()
                or not target.resolve().is_relative_to(root)
                or target.resolve() in {Path(p) for p in bundle['paths'].values()} | {path}
                or not target.is_file()):
            raise ValueError('invalid or escaping review provenance path')
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError('review provenance hash changed: ' + name)
        inputs[str(target.resolve())] = actual
    for group in REVIEW_GROUPS:
        entries = data[group]
        if not isinstance(entries, dict):
            raise ValueError('invalid review status group: ' + group)
        for identity, row in entries.items():
            if (not re.fullmatch(r'[A-Z][A-Z0-9_]*', identity)
                    or not isinstance(row, dict) or set(row) != {'approval', 'approval_ref'}
                    or not isinstance(row['approval'], str) or row['approval'] not in {'approved', 'proposed'}
                    or not isinstance(row['approval_ref'], str)):
                raise ValueError('invalid review status: ' + group + ':' + identity)
            if row['approval'] == 'proposed':
                if row['approval_ref']:
                    raise ValueError('conflicting proposed approval reference')
                continue
            name, mark, anchor = row['approval_ref'].partition('#')
            if not mark or not anchor or name not in data['provenance']:
                raise ValueError('approved review entry lacks bound provenance reference')
            headings = re.findall(r'^#{1,6}\s+(.+?)\s*$', (root / name).read_text(), re.M)
            anchors = [re.sub(r'\s', '-', re.sub(r'[^\w\s-]', '', t.lower())) for t in headings]
            if anchors.count(anchor) != 1:
                raise ValueError('review provenance anchor does not resolve uniquely')
    result = dict(bundle, review_metadata=data, review_inputs=inputs,
                  normative_digest=bundle['digest'])
    binding = {'normative': bundle['hashes'], 'review': inputs[str(path)],
               'provenance': data['provenance']}
    result['digest'] = hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return result


def load_reviewed(main: Path) -> dict:
    return bind_review(load(main))


def input_paths(bundle: dict) -> list[Path]:
    return [Path(p) for p in [*bundle['paths'].values(), *bundle.get('review_inputs', {})]]


def apply_review(rows: list[dict], group: str, bundle: dict) -> list[dict]:
    entries = bundle['review_metadata'][group]
    identities = [row['id'] for row in rows]
    if len(set(identities)) != len(identities) or set(entries) != set(identities):
        raise ValueError('review metadata IDs differ from ' + group + ' declarations')
    return [dict(row, **entries[row['id']]) for row in rows]


def reviewed_declarations(bundle: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """Apply a closed-world external review map; inline status cannot compete."""
    # Local imports avoid coupling basic role routing to declaration parsers.
    import capability_dag as dag
    import execution_plan as ep
    cap = Path(bundle['paths']['capabilities']).read_text()
    phase = Path(bundle['paths']['phases']).read_text()
    if any(line.partition(':')[0] in {'Approval', 'Approval-Ref'} for _, line in active_lines(cap)):
        raise ValueError('inline capability review status conflicts with external metadata')
    for kind, required in [('stages', True), ('milestones', False)]:
        for row in ep.block(phase, kind, required):
            if not isinstance(row, dict):
                raise ValueError('invalid declaration record')
            if {'approval', 'approval_ref'} & row.keys():
                raise ValueError('inline ' + kind + ' review status conflicts with external metadata')
    result = (apply_review(dag.parse(cap), 'capabilities', bundle),
              apply_review(ep.parse_stages(phase), 'stages', bundle),
              apply_review(ep.parse_milestones(phase), 'milestones', bundle))
    verify(bundle)
    return result


def without_review(rows: list[dict]) -> list[dict]:
    """An external draft never inherits canonical authorization by matching IDs."""
    return [dict(row, approval='proposed', approval_ref='') for row in rows]
