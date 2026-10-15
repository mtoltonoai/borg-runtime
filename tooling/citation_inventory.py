#!/usr/bin/env python3
"""Check the pinned specification and inventory local Duvet-style citations.

This is a presence diagnostic, not Duvet, a behavioral coverage check, or a
promotion gate. It cannot witness a cited test's execution. Strict mode always
refuses promotion until an independent complete gate exists.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tomllib


KEYWORD = re.compile(r"\b(?:MUST NOT|MUST|SHOULD NOT|SHOULD|MAY)\b")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
MARKER = re.compile(r"^\s*//([=#])\s?(.*)$")


class InventoryError(ValueError):
    """The pinned inputs cannot be evaluated reliably."""


def canonical_sentence(value: str) -> str:
    """Fold line wrapping and spacing; preserve words, case, and punctuation."""
    return " ".join(value.split())


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest_digest(files: dict[str, str]) -> str:
    return digest(json.dumps(files, sort_keys=True, separators=(",", ":")).encode())


def relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or str(path) != value
        or "\\" in value
    ):
        raise InventoryError(f"noncanonical relative path: {value!r}")
    return value


def verify_snapshot(root: Path) -> tuple[dict, dict]:
    lock_path = root / "spec-lock.json"
    try:
        lock = json.loads(lock_path.read_text())
    except (OSError, ValueError) as error:
        raise InventoryError(f"cannot read {lock_path}: {error}") from error
    if not isinstance(lock, dict) or not isinstance(lock.get("files"), dict):
        raise InventoryError("spec-lock.json must contain a files manifest")
    files = lock["files"]
    if not files:
        raise InventoryError("spec-lock.json has an empty files manifest")
    for name, expected in files.items():
        relative_path(name)
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise InventoryError(f"invalid SHA256 for snapshot path {name}")
    if manifest_digest(files) != lock.get("snapshot_sha256"):
        raise InventoryError("spec-lock.json snapshot_sha256 does not match its manifest")
    snapshot = root / "spec-snapshot"
    if not snapshot.is_dir() or snapshot.is_symlink():
        raise InventoryError("spec-snapshot must be a real directory")
    actual = set()
    for path in snapshot.rglob("*"):
        if path.is_symlink():
            raise InventoryError(f"snapshot contains a symlink: {path.relative_to(snapshot)}")
        if path.is_file():
            actual.add(path.relative_to(snapshot).as_posix())
    missing = sorted(set(files) - actual)
    extra = sorted(actual - set(files))
    if missing or extra:
        raise InventoryError(f"snapshot file set differs: missing={missing}, extra={extra}")
    for name, expected in sorted(files.items()):
        if digest((snapshot / name).read_bytes()) != expected:
            raise InventoryError(f"snapshot bytes changed: {name}")
    config_path = ".duvet/config.toml"
    if config_path not in files:
        raise InventoryError("gate configuration is not in the pinned manifest")
    try:
        config = tomllib.loads((snapshot / config_path).read_text())
    except (OSError, ValueError) as error:
        raise InventoryError(f"cannot read pinned gate configuration: {error}") from error
    return lock, config


@dataclass(frozen=True)
class Requirement:
    file: str
    section: str
    sentence: str
    keyword: str
    line: int

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.file, self.section, self.sentence


def heading_slug(heading: str) -> str:
    """Duvet 0.4.3's slug form for the specification's ASCII headings.

    Punctuation separates words: "Tenant's" becomes "tenant-s", not "tenants".
    This remains a local parser; compare its extraction with actual Duvet.
    """
    return re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")


def extract_requirements(path: Path, source: str) -> list[Requirement]:
    requirements = []
    slugs: set[str] = set()
    section = ""
    paragraph: list[str] = []
    paragraph_line = 0
    fenced = False

    def flush() -> None:
        if not paragraph:
            return
        sentence = canonical_sentence(" ".join(paragraph))
        paragraph.clear()
        keywords = KEYWORD.findall(sentence)
        if not keywords:
            return
        if not section:
            raise InventoryError(f"{source}:{paragraph_line}: requirement has no heading")
        if len(keywords) != 1:
            raise InventoryError(
                f"{source}:{paragraph_line}: expected one key word per normative paragraph"
            )
        if not sentence.endswith("."):
            raise InventoryError(f"{source}:{paragraph_line}: normative paragraph is not a sentence")
        requirements.append(Requirement(source, section, sentence, keywords[0], paragraph_line))

    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if line.lstrip().startswith(("```", "~~~")):
            flush()
            fenced = not fenced
            continue
        if fenced:
            continue
        match = HEADING.match(line)
        if match:
            flush()
            base = heading_slug(match[1])
            section = base
            suffix = 0
            while section in slugs:
                suffix += 1
                section = f"{base}-{suffix}"
            slugs.add(section)
        elif not line.strip() or line.lstrip().startswith(">"):
            flush()
        else:
            if not paragraph:
                paragraph_line = line_number
            paragraph.append(line)
    flush()
    return requirements


def selected_requirements(root: Path, lock: dict, config: dict) -> list[Requirement]:
    specifications = config.get("specification", [])
    if not specifications:
        raise InventoryError("pinned gate configuration selects no specifications")
    selected = set()
    requirements = []
    for item in specifications:
        source = relative_path(item.get("source", ""))
        if item.get("format") != "markdown":
            raise InventoryError(f"selected specification is not Markdown: {source}")
        if source in selected:
            raise InventoryError(f"specification selected twice: {source}")
        if source not in lock["files"]:
            raise InventoryError(f"selected specification not pinned: {source}")
        selected.add(source)
        requirements.extend(extract_requirements(root / "spec-snapshot" / source, source))
    identities = [requirement.identity for requirement in requirements]
    if len(set(identities)) != len(identities):
        raise InventoryError("duplicate file/section/sentence requirement identity")
    return requirements


def read_citations(path: Path, root: Path) -> tuple[list[dict], list[dict]]:
    citations = []
    errors = []
    current: dict | None = None
    location = path.relative_to(root).as_posix()

    def flush() -> None:
        nonlocal current
        if current is None:
            return
        current["sentence"] = canonical_sentence(" ".join(current.pop("quote")))
        if not current["sentence"]:
            errors.append({"location": current["location"], "reason": "citation has no quoted sentence"})
        else:
            citations.append(current)
        current = None

    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        match = MARKER.match(line)
        if match is None:
            flush()
            continue
        marker, value = match.groups()
        value = value.strip()
        if marker == "=" and value.startswith("type="):
            if current is None:
                errors.append({"location": f"{location}:{line_number}", "reason": "type marker without target"})
            elif value != "type=test":
                errors.append({"location": f"{location}:{line_number}", "reason": f"unsupported citation metadata: {value}"})
            else:
                current["kind"] = value.removeprefix("type=")
            continue
        if marker == "=":
            flush()
            current = {
                "target": value,
                "kind": "implementation",
                "location": f"{location}:{line_number}",
                "quote": [],
            }
        elif current is None:
            errors.append({"location": f"{location}:{line_number}", "reason": "quoted sentence without target"})
        else:
            current["quote"].append(value)
    flush()
    return citations, errors


def inventory(root: Path) -> dict:
    root = root.resolve()
    lock, config = verify_snapshot(root)
    requirements = selected_requirements(root, lock, config)
    by_identity = {requirement.identity: requirement for requirement in requirements}
    selected = {item["source"] for item in config["specification"]}
    source_files: set[Path] = set()
    # Use the pinned source selection, plus conventional standalone root src/tests.
    patterns = [item["pattern"] for item in config.get("source", [])]
    patterns.extend(["src/**/*.rs", "tests/**/*.rs"])
    for pattern in patterns:
        if PurePosixPath(pattern).is_absolute() or ".." in PurePosixPath(pattern).parts:
            raise InventoryError(f"source glob escapes implementation root: {pattern}")
        source_files.update(path for path in root.glob(pattern) if path.is_file())
    matches: dict[tuple, dict[str, list]] = defaultdict(
        lambda: {"implementation": [], "test": []}
    )
    errors = []
    citations = []
    for path in sorted(source_files):
        if not path.resolve().is_relative_to(root) or path.is_symlink():
            raise InventoryError(f"source file escapes implementation root: {path}")
        found, malformed = read_citations(path, root)
        citations.extend(found)
        errors.extend(malformed)
    for citation in citations:
        target = citation["target"]
        if target.count("#") != 1:
            errors.append({"location": citation["location"], "reason": f"invalid target: {target}"})
            continue
        source, section = target.split("#", 1)
        # The source identity stays relative to the original specification root.
        try:
            relative_path(source)
        except InventoryError as error:
            errors.append({"location": citation["location"], "reason": str(error)})
            continue
        if source not in selected:
            errors.append({"location": citation["location"], "reason": f"unselected specification: {source}"})
            continue
        identity = source, section, citation["sentence"]
        if identity not in by_identity:
            errors.append({"location": citation["location"], "reason": f"stale file/section/sentence citation: {target}", "sentence": citation["sentence"]})
            continue
        matches[identity][citation["kind"]].append(citation["location"])
    rows = []
    per_file = {}
    for source in sorted(selected):
        per_file[source] = {"total": 0, "absolute": 0, "paired_presence": 0, "absolute_presence_gaps": 0}
    for requirement in requirements:
        locations = matches[requirement.identity]
        paired = bool(locations["implementation"] and locations["test"])
        absolute = requirement.keyword.startswith("MUST")
        counts = per_file[requirement.file]
        counts["total"] += 1
        counts["absolute"] += absolute
        counts["paired_presence"] += paired
        counts["absolute_presence_gaps"] += absolute and not paired
        rows.append({
            "file": requirement.file,
            "section": requirement.section,
            "sentence": requirement.sentence,
            "keyword": requirement.keyword,
            "specification_line": requirement.line,
            "implementation_citations": locations["implementation"],
            "test_citations": locations["test"],
            "paired_presence": paired,
            "execution_witnessed": False,
            "governing_classification": "unreviewed",
        })
    totals = {key: sum(counts[key] for counts in per_file.values()) for key in next(iter(per_file.values()))}
    blockers = [
        "Full Duvet extraction and conformance gate have not been run by this tool.",
        "Cited test execution and behavior are unwitnessed by this inventory.",
        "A trusted promotion gate is not implemented.",
        "Governing requirements have not been classified or reviewed; none are excluded.",
    ]
    if totals["absolute_presence_gaps"]:
        blockers.append(f"{totals['absolute_presence_gaps']} absolute requirements lack paired citation presence.")
    if errors:
        blockers.append(f"{len(errors)} malformed or stale citations.")
    return {
        "schema_version": 1,
        "diagnostic": "Local citation presence only; not full Duvet or promotion evidence.",
        "snapshot_sha256": lock["snapshot_sha256"],
        "specification_commit": lock.get("commit"),
        "gate_configuration_sha256": lock["files"][".duvet/config.toml"],
        "snapshot_verified": True,
        "selected_specifications": sorted(selected),
        "source_patterns": patterns,
        "source_files": [path.relative_to(root).as_posix() for path in sorted(source_files)],
        "totals": totals,
        "per_file": per_file,
        "citation_errors": errors,
        "requirements": rows,
        "promotion_allowed": False,
        "promotion_blockers": blockers,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--json", type=Path, help="write the full diagnostic report to this file")
    parser.add_argument("--strict", action="store_true", help="fail closed for promotion (always blocked by this diagnostic)")
    args = parser.parse_args(argv)
    try:
        report = inventory(args.root)
    except (InventoryError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"Inventory input error: {error}", file=sys.stderr)
        return 2
    if args.json:
        args.json.write_text(json.dumps(report, indent=2) + "\n")
    print(report["diagnostic"])
    print(f"Verified snapshot: {report['snapshot_sha256']}")
    print(f"Selected normative files: {len(report['selected_specifications'])}")
    totals = report["totals"]
    print(f"Requirements: {totals['total']}; absolute: {totals['absolute']}")
    print(f"Paired citation presence: {totals['paired_presence']}; absolute presence gaps: {totals['absolute_presence_gaps']}")
    for error in report["citation_errors"]:
        print(f"{error['location']}: {error['reason']}", file=sys.stderr)
    print("Promotion: BLOCKED")
    for blocker in report["promotion_blockers"]:
        print(f"- {blocker}")
    return 1 if args.strict or report["citation_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
