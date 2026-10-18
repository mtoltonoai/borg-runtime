#!/usr/bin/env python3
"""Build/verify a deterministic v2 release package; never merge, deploy or approve."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib

MANIFEST_SHA = "20f1acdf87b07698a2881653d76c579e81c3c98eacbfdd735e161a20bb80cc1d"
LOCK_SHA = "df189e6ea91a5e05839f2773611a59830bafc0114f13a06cbc8359b44195c28b"
BUILDER_SHA = "a6c3f47c946bb96f121d617f7c141e7513f597d63556bb56f3c439989332172c"
SQLITE_SHA3 = "7b670a62fdfbd672b75fef004cb703c8a3e87d3a5cc7d675b4a08337004a2d93"
SQLITE_SOURCE = "2026-10-09 15:46:58 be8d059e9a49089ab2dce5ed26dd87aaf598fdc5fbe0b107c0dd758464bcd5e3"
MANAGED_CONFIG_SHA = "24095bd6315880162c8f660917100ac36638cb73778dd9ec6f1c9026ea87b178"
SQLITE_ARCHIVE_SHA = "68e913b0fe8ec6b4e5f391c594a28872884e80d6cd042a1e50c957be46b5aa4d"
SQLITE_FILES = {"sqlite3.c": "2da161e310ef0aba3465f22a4d12b8a43723236f0942fa76e8aac523c3317243",
                "sqlite3.h": "cd20a2877bc82b94e36c1093b727dd87cfa3c1d581aac6e240925535cbd90166",
                "sqlite3ext.h": "48963abfefb41ac329ef9d706acb5eadba952a99a748285c9ded6b5e0942721b"}
CFLAGS = ["-O2", "-fPIC", "-std=c11", "-DSQLITE_THREADSAFE=1", "-DSQLITE_DEFAULT_FOREIGN_KEYS=1", "-DSQLITE_ENABLE_API_ARMOR"]
TOOL_SHA = {"cargo": "e951141cc55a6cd7b9876d187bd30a3720e6086b99413af95e3b8de1cdd72f14",
            "rustc": "f3834d26669b03f6855fa54bd2381443123e860167bc0c7a8dd137e5cf6e5e4f",
            "rustdoc": "a1321a60578a6b2e3e0ff428201b8618bbd9a9fbcd475c0d0f151bbf17cb0a9e",
            "gcc": "1b99826121ae6682a634e5efe09bd3e3df58ce58e0b28f849114ab5b89139c26",
            "ar": "6452af2eea333b8c65e1adb92964fc8f97863ab003fa13f9d12bff5345cd7dbe",
            "pkg-config": "7e8152db1b0c7e49d47b4b82a70dc9997b557951c5d4d938b0e7f208ce9eb10b"}
READER_SHA = {"git_sha256": "2a8c18fbf43da9f692d75474c72bea9dfd796c260b0f3dfe456376abc3bbd668",
              "python_sha256": "e50d468e8b0adfb05733f5b87b3cff34829c4a8c1aea50c865aa8bdfe4bb150f"}
ENV_PROFILE = {"locale": "C", "timezone": "UTC", "cargo_incremental": "0", "network": "offline",
               "path_normalization": "borg-v2-build-paths/1"}
TARGET = "x86_64-unknown-linux-gnu"
# Immediately preceding profile, retained exactly for legacy package reads.
# New writes use candidate_profile(), never this raw-byte baseline directly.
PROFILE = {"cargo_manifest_sha256": MANIFEST_SHA, "cargo_lock_sha256": LOCK_SHA,
           "build_helper_sha256": BUILDER_SHA, "cargo_arguments": ["build", "--locked", "--offline", "--release"],
           "target": TARGET, "sqlite_version": "3.54.0", "sqlite_archive_sha3_256": SQLITE_SHA3,
           "sqlite_source_id": SQLITE_SOURCE, "managed_cargo_source_config_sha256": MANAGED_CONFIG_SHA}
DEPENDENCY_POLICY = {"revision": 1,
                     "manifest_parsed_sha256": "e8e9e8b21f515d93e39b295738e7b857dcc88df12614db3f17484bc8128ac1bd",
                     "lock_parsed_sha256": "fe872ec79ea98442f4369331cdf1ba643ec06648cd3afc3d4efcf4c07898704f"}
CARGO_INPUTS = {"Cargo.toml": ("cargo_manifest_sha256", "manifest_parsed_sha256"),
                "Cargo.lock": ("cargo_lock_sha256", "lock_parsed_sha256")}
MAX_FILES, MAX_SOURCE_FILE, MAX_SOURCE = 10_000, 8 * 1024 * 1024, 128 * 1024 * 1024
MAX_BINARY, MAX_MANIFEST = 256 * 1024 * 1024, 4 * 1024 * 1024
# Reserve and enforce room for the trusted builder's finite metadata before
# executing it. Candidate strings can expand substantially when JSON-escaped.
BUILD_METADATA_RESERVE = 16 * 1024
HEX40, HEX64 = re.compile(r"[0-9a-f]{40}\Z"), re.compile(r"[0-9a-f]{64}\Z")
PACKAGE_FILES = ("bin/borg-runtime", "manifest.json")


def digest(path: Path, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode()


def parsed_toml(text: str) -> bytes:
    """Canonical complete typed value, not a dependency subset or Python ==."""
    if type(text) is not str:
        raise ValueError("Cargo TOML input must be an exact UTF-8 string")
    value = tomllib.loads(text)

    def supported(item):
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("unsupported Cargo TOML key type")
                supported(child)
        elif type(item) is list:
            for child in item:
                supported(child)
        elif type(item) not in (str, bool, int):
            raise ValueError("unsupported Cargo TOML value type")

    supported(value)
    return canonical(value)


def validate_source(source: dict, expected_commit: str | None = None) -> None:
    if type(source) is not dict or set(source) != {"commit", "tree", "object_format", "files"}:
        raise ValueError("invalid source identity")
    fmt = source["object_format"]
    pattern = {"sha1": HEX40, "sha256": HEX64}.get(fmt) if type(fmt) is str else None
    if (pattern is None or any(type(source[k]) is not str or not pattern.fullmatch(source[k]) for k in ("commit", "tree"))
            or (expected_commit is not None and source["commit"] != expected_commit)):
        raise ValueError("source identity mismatch")
    inventory = source["files"]
    if (type(inventory) is not dict or not 4 <= len(inventory) <= MAX_FILES
            or not {"Cargo.toml", "Cargo.lock", "src/main.rs", "src/lib.rs"} <= inventory.keys()):
        raise ValueError("source inventory bound/membership")
    total = 0
    for name, entry in inventory.items():
        if type(name) is not str:
            raise ValueError("invalid source inventory name")
        source_policy(name)
        if (type(entry) is not dict or set(entry) != {"mode", "git_blob", "size", "sha256"}
                or entry["mode"] not in ("100644", "100755")
                or type(entry["git_blob"]) is not str or not pattern.fullmatch(entry["git_blob"])
                or type(entry["sha256"]) is not str or not HEX64.fullmatch(entry["sha256"])
                or type(entry["size"]) is not int or not 0 <= entry["size"] <= MAX_SOURCE_FILE):
            raise ValueError("invalid source inventory entry")
        total += entry["size"]
    if total > MAX_SOURCE:
        raise ValueError("source inventory exceeds bound")


def candidate_profile(inputs: dict, source: dict) -> dict:
    """Bind captured exact Cargo text to immutable source and trusted policy."""
    validate_source(source)
    if type(inputs) is not dict or set(inputs) != set(CARGO_INPUTS):
        raise ValueError("manifest profile Cargo input membership")
    profile = {**PROFILE, "revision": 2, "dependency_policy": dict(DEPENDENCY_POLICY), "cargo_inputs": dict(inputs)}
    total = 0
    for name, (raw_key, policy_key) in CARGO_INPUTS.items():
        text = inputs[name]
        if type(text) is not str or len(text) > MAX_SOURCE_FILE:
            raise ValueError("manifest profile Cargo input type/bound")
        raw = text.encode("utf-8")
        total += len(raw)
        if len(raw) > MAX_SOURCE_FILE or total > MAX_SOURCE:
            raise ValueError("manifest profile Cargo input byte bound")
        entry = source["files"][name]
        raw_hash = hashlib.sha256(raw).hexdigest()
        blob = hashlib.new(source["object_format"], b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        if entry["size"] != len(raw) or entry["sha256"] != raw_hash or entry["git_blob"] != blob:
            raise ValueError("manifest profile source manifest/lock build profile binding differs: " + name)
        if hashlib.sha256(parsed_toml(text)).hexdigest() != DEPENDENCY_POLICY[policy_key]:
            raise ValueError("source manifest/lock build profile dependency policy differs: " + name)
        profile[raw_key] = raw_hash
    return profile


def validate_package_profile(profile: dict, source: dict, *, allow_legacy: bool = True) -> None:
    if type(profile) is not dict:
        raise ValueError("manifest profile shape mismatch")
    # Explicit, exact legacy branch: no older profile or candidate-supplied
    # policy is an allowlist. Identical legacy rebuilds use their old recipe.
    if allow_legacy and canonical(profile) == canonical(PROFILE):
        if any(source["files"][name]["sha256"] != PROFILE[keys[0]] for name, keys in CARGO_INPUTS.items()):
            raise ValueError("source profile mismatch")
        return
    if (set(profile) != set(PROFILE) | {"revision", "dependency_policy", "cargo_inputs"}
            or type(profile["revision"]) is not int or profile["revision"] != 2
            or type(profile["dependency_policy"]) is not dict
            or canonical(profile["dependency_policy"]) != canonical(DEPENDENCY_POLICY)):
        raise ValueError("manifest profile revision/policy mismatch")
    expected = candidate_profile(profile["cargo_inputs"], source)
    if canonical(profile) != canonical(expected):
        raise ValueError("manifest profile exact input/recipe mismatch")


def write_json(path: Path, value) -> None:
    path.write_bytes(canonical(value))


def checked_path(path: Path) -> Path:
    path = path.absolute()
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", str(path)) or ".." in path.parts:
        raise ValueError("path must be absolute and contain no shell/build metacharacters")
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("symlink path refused")
    return path


def safe_name(name: str) -> None:
    p = PurePosixPath(name)
    if (not name or p.is_absolute() or any(x in ("", ".", "..", ".git") for x in name.split("/"))
            or "\\" in name or any(ord(c) < 32 or ord(c) > 126 for c in name)):
        raise ValueError("unsafe source/member name")


def source_policy(name: str) -> None:
    safe_name(name)
    p = PurePosixPath(name)
    if (".cargo" in p.parts or p.name in (".gitmodules", ".gitattributes", "rust-toolchain", "rust-toolchain.toml", "build.rs")
            or p.parts[0] in ("target", ".build") or name.startswith("src/bin/")
            or (p.name == "Cargo.toml" and name != "Cargo.toml")):
        raise ValueError("unselected source build/configuration input: " + name)


def git(repo: Path, *args: str, limit: int = MAX_SOURCE_FILE) -> bytes:
    git_dir = repo / ".git" if (repo / ".git").exists() else repo
    if not git_dir.is_dir() or git_dir.is_symlink() or not (git_dir / "objects").is_dir():
        raise ValueError("requires an ordinary local Git directory, not a linked worktree")
    env = {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C", "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0",
           "GIT_NO_LAZY_FETCH": "1", "GIT_ALLOW_PROTOCOL": ""}
    command = ["/usr/bin/git", "--no-replace-objects", "--git-dir=" + str(git_dir),
               "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", *args]
    # Git receives no network operation, checkout/filter/hook, shell or source
    # command. A bounded spool avoids loading arbitrary repository output in RAM.
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(command, env=env, stdout=output, stderr=subprocess.DEVNULL, timeout=60)
        if result.returncode or output.tell() > limit:
            raise ValueError("Git read failed or exceeded bound")
        output.seek(0)
        return output.read(limit + 1)


def immutable_tree(repo: Path, commit: str) -> tuple[str, str, list[tuple[str, str, str, int]]]:
    if not (HEX40.fullmatch(commit) or HEX64.fullmatch(commit)):
        raise ValueError("source requires a full lowercase Git commit, never a branch/tag")
    config = git(repo, "config", "--local", "--null", "--list", limit=256 * 1024)
    keys = [item.split(b"\n", 1)[0].lower() for item in config.split(b"\0") if item]
    git_dir = repo / ".git" if (repo / ".git").exists() else repo
    if (any(k == b"extensions.partialclone" or k.endswith(b".promisor") or k.startswith((b"include.", b"includeif.")) for k in keys)
            or list((git_dir / "objects/pack").glob("*.promisor")) or (git_dir / "objects/info/alternates").exists()):
        raise ValueError("promisor/partial clone, included config or object alternates refused")
    object_format = git(repo, "rev-parse", "--show-object-format", limit=64).decode().strip()
    pattern = {"sha1": HEX40, "sha256": HEX64}.get(object_format)
    if pattern is None or not pattern.fullmatch(commit):
        raise ValueError("commit length differs from Git object format")
    def verify_object(kind: str, oid: str) -> None:
        size = int(git(repo, "cat-file", "-s", oid, limit=64))
        if not 0 <= size <= MAX_SOURCE_FILE:
            raise ValueError("Git object exceeds bound")
        data = git(repo, "cat-file", kind, oid)
        identity = hashlib.new(object_format, kind.encode() + b" " + str(size).encode() + b"\0" + data).hexdigest()
        if len(data) != size or identity != oid:
            raise ValueError("Git object bytes differ from requested immutable identity")
    verify_object("commit", commit)
    resolved = git(repo, "rev-parse", "--verify", commit + "^{commit}").decode().strip()
    if resolved != commit:
        raise ValueError("source commit identity differs")
    tree = git(repo, "rev-parse", "--verify", commit + "^{tree}").decode().strip()
    if not pattern.fullmatch(tree):
        raise ValueError("unsupported Git tree identity")
    verify_object("tree", tree)
    entries = []
    total = 0
    verified_trees = {tree}
    raw = git(repo, "ls-tree", "-r", "-t", "-z", "--full-tree", commit, limit=2 * 1024 * 1024)
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode("ascii").split()
        name = name.decode("utf-8")
        source_policy(name)
        if mode == "040000" and kind == "tree" and pattern.fullmatch(oid):
            if oid not in verified_trees:
                if len(verified_trees) >= MAX_FILES:
                    raise ValueError("Git tree inventory exceeds bound")
                verify_object("tree", oid)
                verified_trees.add(oid)
            continue
        if mode not in ("100644", "100755") or kind != "blob" or not pattern.fullmatch(oid):
            raise ValueError("symlink, submodule or unsupported source entry")
        size = int(git(repo, "cat-file", "-s", oid, limit=64))
        total += size
        if size < 0 or size > MAX_SOURCE_FILE or total > MAX_SOURCE or len(entries) >= MAX_FILES:
            raise ValueError("source inventory exceeds bound")
        entries.append((name, mode, oid, size))
    if len({e[0] for e in entries}) != len(entries):
        raise ValueError("duplicate source path")
    if not {"Cargo.toml", "Cargo.lock", "src/main.rs", "src/lib.rs"} <= {e[0] for e in entries}:
        raise ValueError("selected application source is incomplete")
    return tree, object_format, sorted(entries)


def materialize(repo: Path, commit: str, checkout: Path) -> tuple[dict, dict]:
    tree, object_format, entries = immutable_tree(repo, commit)
    checkout.mkdir()
    inventory = {}
    cargo_inputs = {}
    for name, mode, oid, size in entries:
        data = git(repo, "cat-file", "blob", oid)
        if len(data) != size or hashlib.new(object_format, b"blob " + str(size).encode() + b"\0" + data).hexdigest() != oid:
            raise ValueError("Git blob bytes differ from immutable inventory")
        path = checkout / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as f:
            f.write(data)
        path.chmod(int(mode[-3:], 8))
        inventory[name] = {"mode": mode, "git_blob": oid, "size": size, "sha256": hashlib.sha256(data).hexdigest()}
        if name in CARGO_INPUTS:
            cargo_inputs[name] = data.decode("utf-8")
    source = {"commit": commit, "tree": tree, "object_format": object_format, "files": inventory}
    profile = candidate_profile(cargo_inputs, source)
    check_checkout(checkout, inventory)
    return source, profile


def check_checkout(root: Path, inventory: dict) -> None:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("source checkout root changed")
    actual = set()
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs:
            if (Path(base) / name).is_symlink():
                raise ValueError("source directory symlink appeared")
        for name in files:
            path = Path(base) / name
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if relative not in inventory or not stat.S_ISREG(info.st_mode):
                raise ValueError("unexpected source file/type")
            expected = inventory[relative]
            if (info.st_size != expected["size"] or stat.S_IMODE(info.st_mode) != int(expected["mode"][-3:], 8)
                    or digest(path) != expected["sha256"]):
                raise ValueError("source checkout drift: " + relative)
            actual.add(relative)
    if actual != set(inventory):
        raise ValueError("source inventory changed")


def reject_configs(checkout: Path, cargo_home: Path) -> list[Path]:
    paths = [cargo_home / "config", cargo_home / "config.toml"]
    for parent in (checkout, *checkout.parents):
        paths.extend([parent / ".cargo/config", parent / ".cargo/config.toml"])
    for path in paths:
        if path == Path("/.cargo/config.toml"):
            if path.is_symlink() or not path.is_file() or digest(path) != MANAGED_CONFIG_SHA:
                raise ValueError("managed Cargo policy configuration differs")
            continue
        if path.exists() or path.is_symlink():
            raise ValueError("unselected Cargo configuration: " + str(path))
    return paths


def validate_build_metadata(recipe: dict, built: dict) -> None:
    if (not isinstance(recipe, dict) or set(recipe) != {"packager_sha256", "build_helper_sha256", "git_sha256", "python_sha256", "environment"}
            or not HEX64.fullmatch(recipe["packager_sha256"]) or recipe["build_helper_sha256"] != BUILDER_SHA
            or any(recipe[k] != v for k, v in READER_SHA.items()) or recipe["environment"] != ENV_PROFILE):
        raise ValueError("unselected package recipe identity")
    if not isinstance(built, dict) or set(built) != {"tools", "sqlite"} or set(built["tools"]) != {"sha256", "versions"} or built["tools"]["sha256"] != TOOL_SHA:
        raise ValueError("unselected build tool identities")
    v = built["tools"]["versions"]
    if (not isinstance(v, dict) or set(v) != {"gcc", "gcc_target", "ar", "pkg-config", "cargo", "rustc"}
            or not all(isinstance(value, str) for value in v.values())
            or v["gcc"] != "13.3.0" or v["gcc_target"] != "x86_64-linux-gnu" or not v["ar"].endswith(" 2.42")
            or v["pkg-config"] != "1.8.1" or not v["cargo"].startswith("cargo 1.99.0 ")
            or not v["rustc"].startswith("rustc 1.99.0 ") or "host: " + TARGET not in v["rustc"]):
        raise ValueError("unselected tool versions/target")
    s = built["sqlite"]
    if (set(s) != {"identity", "source_sha256", "archive_sha256", "cflags", "static_library_sha256"}
            or s["identity"] != {"version": "3.54.0", "source_id": SQLITE_SOURCE, "threadsafe": 1, "api_armor": 1, "foreign_keys": 1}
            or s["source_sha256"] != SQLITE_FILES or s["archive_sha256"] != SQLITE_ARCHIVE_SHA or s["cflags"] != CFLAGS
            or not HEX64.fullmatch(s["static_library_sha256"])):
        raise ValueError("unselected SQLite identity/source/recipe")


def verify_archive(path: Path) -> None:
    if not path.is_file() or path.stat().st_size > 16 * 1024 * 1024 or digest(path, "sha3_256") != SQLITE_SHA3:
        raise ValueError("selected offline SQLite archive is missing or mismatched")


def verify_tool_files(toolchain: Path) -> None:
    paths = {"cargo": toolchain / "cargo", "rustc": toolchain / "rustc", "rustdoc": toolchain / "rustdoc",
             "gcc": Path("/usr/bin/gcc"), "ar": Path("/usr/bin/ar"), "pkg-config": Path("/usr/bin/pkg-config")}
    if {name: digest(path) for name, path in paths.items()} != TOOL_SHA:
        raise ValueError("installed tools differ from selected build identities")


def run_builder(checkout: Path, output: Path, builder: Path, archive: Path, toolchain: Path, cargo_home: Path,
                source: dict) -> dict:
    env = {"PATH": str(toolchain) + ":/usr/bin:/bin", "HOME": str(output / "home"), "LC_ALL": "C", "TZ": "UTC",
           "CARGO_HOME": str(cargo_home), "CARGO_TARGET_DIR": str(output / "target"), "CARGO_NET_OFFLINE": "true",
           "CARGO_INCREMENTAL": "0"}
    (output / "home").mkdir()
    command = ["/usr/bin/python3", "-I", "-S", "-B", str(builder), "--root", str(checkout),
               "--build-dir", str(output / "sqlite"), "--archive", str(archive),
               "--toolchain-dir", str(toolchain), "--offline", "--cargo", "build", "--release", "--reproducible-paths"]
    invocation = {"command": command, "environment": env}
    with (output / "build.log").open("xb") as log:
        child = subprocess.Popen(command, env=env, cwd=output, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        invocation["pid"] = child.pid
        try:
            invocation["exit"] = child.wait()
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGTERM)
                child.wait(timeout=5)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=10)
            invocation["exit"] = child.returncode
            raise
        finally:
            write_json(output / "build-invocation.json", invocation)
    if invocation["exit"] != 0:
        raise ValueError("trusted build failed; retained build.log and invocation")
    sqlite = json.loads((output / "sqlite/build-receipt.json").read_text())
    cargos = sorted((output / "sqlite").glob("cargo-*/receipt.json"))
    if len(cargos) != 1:
        raise ValueError("expected exactly one fresh Cargo receipt")
    cargo = json.loads(cargos[0].read_text())
    if (sqlite.get("status") != "passed" or cargo.get("status") != "passed"
            or cargo.get("manifest_sha256") != source["files"]["Cargo.toml"]["sha256"]
            or cargo.get("lock_sha256") != source["files"]["Cargo.lock"]["sha256"]
            or cargo.get("sqlite_receipt_sha256") != digest(output / "sqlite/build-receipt.json")):
        raise ValueError("build receipt association mismatch")
    if (cargo.get("path_normalization") != ENV_PROFILE["path_normalization"]
            or set(cargo.get("path_environment", {})) != {"CARGO_ENCODED_RUSTFLAGS", "CFLAGS", "CXXFLAGS"}):
        raise ValueError("build path-normalization receipt missing or incompatible")
    if sqlite["profile"]["builder_sha256"] != BUILDER_SHA or sqlite["profile"]["archive_sha3_256"] != SQLITE_SHA3:
        raise ValueError("SQLite recipe identity mismatch")
    if sqlite["identity"] != {"version": "3.54.0", "source_id": SQLITE_SOURCE, "threadsafe": 1, "api_armor": 1, "foreign_keys": 1}:
        raise ValueError("SQLite linked identity mismatch")
    commands = cargo.get("commands", [])
    if (len(commands) != 1 or commands[0].get("exit") != 0
            or commands[0].get("command") != [sqlite["tools"]["paths"]["cargo"], *PROFILE["cargo_arguments"]]
            or commands[0].get("cwd") != str(checkout)
            or digest(Path(commands[0]["log"])) != commands[0].get("log_sha256")):
        raise ValueError("Cargo command/result association mismatch")
    for name, expected in sqlite["outputs"].items():
        safe_name(name)
        path = checked_path(output / "sqlite" / name)
        if not path.is_file() or digest(path) != expected:
            raise ValueError("SQLite output drift")
    tools = sqlite["tools"]
    for name, path in tools["paths"].items():
        if digest(Path(path)) != tools["sha256"][name]:
            raise ValueError("build tool drift")
    if "host: " + TARGET not in tools["versions"]["rustc"]:
        raise ValueError("conflicting compiler target")
    return {"tools": {"sha256": tools["sha256"], "versions": tools["versions"]},
            "sqlite": {"identity": sqlite["identity"], "source_sha256": sqlite["source_input_sha256"],
                       "archive_sha256": sqlite["archive_sha256"], "cflags": sqlite["profile"]["cflags"],
                       "static_library_sha256": sqlite["outputs"]["libsqlite3.a"]}}


def elf_binary(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or not (info.st_mode & 0o111) or not 20 <= info.st_size <= MAX_BINARY:
        raise ValueError("release executable missing, symlinked or outside bound")
    with path.open("rb") as f:
        header = f.read(20)
    check_elf(header)
    return {"sha256": digest(path), "size": info.st_size, "mode": "0555", "target": TARGET}


def check_elf(header: bytes) -> None:
    if (header[:7] != b"\x7fELF\x02\x01\x01" or int.from_bytes(header[16:18], "little") not in (2, 3)
            or int.from_bytes(header[18:20], "little") != 62):
        raise ValueError("binary is not the selected x86_64 Linux ELF executable")


def tar_header(name: str, size: int, mode: int) -> bytes:
    info = tarfile.TarInfo(name)
    info.size, info.mode, info.mtime = size, mode, 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info.tobuf(format=tarfile.USTAR_FORMAT)


def package_tar(path: Path, binary: Path, manifest: dict) -> None:
    encoded = canonical(manifest)
    if len(encoded) > MAX_MANIFEST:
        raise ValueError("manifest bound exceeded")
    with path.open("xb") as out:
        for name, source, size, mode in [
            (PACKAGE_FILES[0], binary.open("rb"), binary.stat().st_size, 0o555),
            (PACKAGE_FILES[1], io.BytesIO(encoded), len(encoded), 0o444),
        ]:
            with source:
                out.write(tar_header(name, size, mode))
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    out.write(chunk)
                out.write(b"\0" * ((-size) % 512))
        # POSIX end markers plus deterministic 10240-byte record padding.
        out.write(b"\0" * (1024 + (-(out.tell() + 1024)) % 10240))


def strict_json(data: bytes):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError("duplicate JSON field")
            out[key] = value
        return out
    def reject(value):
        raise ValueError("nonfinite JSON refused")
    return json.loads(data, object_pairs_hook=pairs, parse_constant=reject)


def verify(path: Path, expected_sha256: str, expected_commit: str | None = None) -> dict:
    path = checked_path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BINARY + MAX_MANIFEST + 20_480:
        raise ValueError("package bound exceeded")
    if not HEX64.fullmatch(expected_sha256) or digest(path) != expected_sha256:
        raise ValueError("package differs from caller-supplied SHA-256")
    actual_binary = None
    manifest = None
    with path.open("rb") as f:
        for name, mode, cap in [(PACKAGE_FILES[0], 0o555, MAX_BINARY), (PACKAGE_FILES[1], 0o444, MAX_MANIFEST)]:
            header = f.read(512)
            info = tarfile.TarInfo.frombuf(header, "ascii", "strict")
            if (info.name != name or info.type != tarfile.REGTYPE or info.size < 0 or info.size > cap
                    or header != tar_header(name, info.size, mode)):
                raise ValueError("noncanonical package inventory/header")
            h = hashlib.sha256()
            remaining = info.size
            data = bytearray() if name == PACKAGE_FILES[1] else None
            first = b""
            while remaining:
                block = f.read(min(1024 * 1024, remaining))
                if not block:
                    raise ValueError("truncated package member")
                if not first:
                    first = block[:20]
                h.update(block)
                if data is not None:
                    data.extend(block)
                remaining -= len(block)
            padding = (-info.size) % 512
            if f.read(padding) != b"\0" * padding:
                raise ValueError("nonzero member padding")
            if data is None:
                check_elf(first)
                actual_binary = {"sha256": h.hexdigest(), "size": info.size, "mode": "0555", "target": TARGET}
            else:
                manifest = strict_json(bytes(data))
                if canonical(manifest) != bytes(data):
                    raise ValueError("noncanonical manifest")
        tail = f.read(20_481)
        required = 1024 + (-(f.tell() - len(tail) + 1024)) % 10240
        if tail != b"\0" * required or f.read(1):
            raise ValueError("extra/truncated package contents")
    if (not isinstance(manifest, dict) or set(manifest) != {"format", "source", "profile", "recipe", "build", "files"}
            or manifest["format"] != "borg-v2-release/1"
            or manifest["files"] != {PACKAGE_FILES[0]: actual_binary}):
        raise ValueError("manifest profile/inventory mismatch")
    source = manifest["source"]
    validate_build_metadata(manifest["recipe"], manifest["build"])
    validate_source(source, expected_commit)
    validate_package_profile(manifest["profile"], source)
    return manifest


#= spec/impl/v2/implementation.md#changes-can-be-prepared-without-per-change-human-approval
## Change preparation MUST produce a deployable artifact linked to its source revision.
# Scope: exact immutable checkout + selected build + deterministic executable package;
# no test acceptance, protected-evidence, merge, deployment or human-approval authority.
def build(repo: Path, commit: str, output: Path, archive: Path, toolchain: Path, cargo_home: Path) -> dict:
    repo, output, archive, toolchain, cargo_home = map(checked_path, (repo, output, archive, toolchain, cargo_home))
    output.mkdir(mode=0o700)  # Existing output, even empty, is never reused.
    receipt = {"status": "failed", "started_unix_ns": time.time_ns(), "source_repo": str(repo), "commit": commit,
               "output": str(output), "archive": str(archive), "toolchain": str(toolchain), "cargo_home": str(cargo_home)}
    package = output / "borg-runtime.tar"
    try:
        if digest(Path("/proc/self/exe")) != READER_SHA["python_sha256"]:
            raise ValueError("packager must run under the selected /usr/bin/python3 binary")
        if digest(Path("/usr/bin/git")) != READER_SHA["git_sha256"] or digest(Path("/usr/bin/python3")) != READER_SHA["python_sha256"]:
            raise ValueError("selected reader/builder interpreter identity mismatch")
        builder = checked_path(Path(__file__).absolute().with_name("build_v2.py"))
        if digest(builder) != BUILDER_SHA:
            raise ValueError("installed build helper differs from selected trusted recipe")
        verify_archive(archive)
        verify_tool_files(toolchain)
        recipe_dir = output / "recipe"
        recipe_dir.mkdir()
        retained_builder = recipe_dir / "build_v2.py"
        retained_builder.write_bytes(builder.read_bytes())
        retained_builder.chmod(0o500)
        if digest(retained_builder) != BUILDER_SHA:
            raise ValueError("retained builder copy mismatch")
        packager_hash = digest(Path(__file__))
        reader_tools = {"git_sha256": digest(Path("/usr/bin/git")), "python_sha256": digest(Path("/usr/bin/python3"))}
        archive_hash = digest(archive)
        source, profile = materialize(repo, commit, output / "source")
        recipe = {"packager_sha256": packager_hash, "build_helper_sha256": BUILDER_SHA,
                  **reader_tools, "environment": ENV_PROFILE}
        # Bound the complete candidate-controlled encoding plus a separately
        # enforced builder envelope before invoking any trusted build command.
        envelope = {"format": "borg-v2-release/1", "source": source, "profile": profile, "recipe": recipe,
                    "build": {}, "files": {PACKAGE_FILES[0]: {"sha256": "0" * 64, "size": MAX_BINARY,
                                                              "mode": "0555", "target": TARGET}}}
        if len(canonical(envelope)) + BUILD_METADATA_RESERVE > MAX_MANIFEST:
            raise ValueError("embedded manifest bound exceeded before builder")
        reject_configs(output / "source", cargo_home)
        built = run_builder(output / "source", output, retained_builder, archive, toolchain, cargo_home, source)
        if len(canonical(built)) > BUILD_METADATA_RESERVE:
            raise ValueError("build metadata exceeds reserved manifest bound")
        check_checkout(output / "source", source["files"])
        reject_configs(output / "source", cargo_home)
        if (digest(builder) != BUILDER_SHA or digest(retained_builder) != BUILDER_SHA
                or digest(Path(__file__)) != packager_hash or digest(archive) != archive_hash):
            raise ValueError("recipe/archive input drift")
        if reader_tools != {"git_sha256": digest(Path("/usr/bin/git")), "python_sha256": digest(Path("/usr/bin/python3"))}:
            raise ValueError("package reader/interpreter identity drift")
        if immutable_tree(repo, commit)[0] != source["tree"]:
            raise ValueError("source tree identity changed")
        binary = checked_path(output / "target/release/borg-runtime")
        binary_info = elf_binary(binary)
        manifest = {"format": "borg-v2-release/1", "source": source, "profile": profile,
                    "recipe": recipe,
                    "build": built, "files": {PACKAGE_FILES[0]: binary_info}}
        write_json(output / "manifest.json", manifest)
        package_tar(package, binary, manifest)
        if elf_binary(binary) != binary_info:
            raise ValueError("release binary drift during packaging")
        package_hash = digest(package)
        verify(package, package_hash, commit)
        receipt.update(status="passed", package_sha256=package_hash, package_size=package.stat().st_size,
                       manifest_sha256=digest(output / "manifest.json"), binary=binary_info, source_tree=source["tree"])
        return receipt
    except BaseException as error:
        receipt["error"] = str(error)
        # A failed archive, if any, is retained but never named as a passed output.
        raise
    finally:
        receipt["finished_unix_ns"] = time.time_ns()
        write_json(output / "receipt.json", receipt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    create = commands.add_parser("build")
    create.add_argument("--repo", type=Path, required=True)
    create.add_argument("--commit", required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--archive", type=Path, required=True)
    create.add_argument("--toolchain-dir", type=Path, required=True)
    create.add_argument("--cargo-home", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("package", type=Path)
    check.add_argument("--expect-sha256", required=True)
    check.add_argument("--expect-commit")
    args = parser.parse_args()
    try:
        if args.action == "build":
            result = build(args.repo, args.commit, args.output, args.archive, args.toolchain_dir, args.cargo_home)
            print(json.dumps({k: result[k] for k in ("status", "commit", "package_sha256", "package_size")}, sort_keys=True))
        else:
            manifest = verify(args.package, args.expect_sha256, args.expect_commit)
            print(json.dumps({"status": "verified", "commit": manifest["source"]["commit"], "package_sha256": args.expect_sha256}, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, TypeError, RecursionError, tarfile.TarError, subprocess.SubprocessError) as error:
        print("Package preparation/verification failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
