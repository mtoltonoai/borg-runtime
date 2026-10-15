#!/usr/bin/env python3
"""Prepare the selected private SQLite library and optionally run locked Cargo."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile

VERSION = "3.54.0"
ARCHIVE = "sqlite-amalgamation-3540000.zip"
PREFIX = "sqlite-amalgamation-3540000"
URL = "https://www.sqlite.org/2026/" + ARCHIVE
SHA3 = "7b670a62fdfbd672b75fef004cb703c8a3e87d3a5cc7d675b4a08337004a2d93"
SOURCE_ID = "2026-10-09 15:46:58 be8d059e9a49089ab2dce5ed26dd87aaf598fdc5fbe0b107c0dd758464bcd5e3"
SOURCE_FILES = ("sqlite3.c", "sqlite3.h", "sqlite3ext.h")
CFLAGS = ["-O2", "-fPIC", "-std=c11", "-DSQLITE_THREADSAFE=1", "-DSQLITE_DEFAULT_FOREIGN_KEYS=1", "-DSQLITE_ENABLE_API_ARMOR"]
TOOLCHAIN_HASHES = {
    "rustc": "f3834d26669b03f6855fa54bd2381443123e860167bc0c7a8dd137e5cf6e5e4f",
    "cargo": "e951141cc55a6cd7b9876d187bd30a3720e6086b99413af95e3b8de1cdd72f14",
}
MAX_ARCHIVE = 16 * 1024 * 1024
MAX_EXPANDED = 32 * 1024 * 1024
BUILD_OVERRIDES = ("CC", "CXX", "AR", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTC_BOOTSTRAP", "CFLAGS", "CPPFLAGS", "CXXFLAGS", "LDFLAGS", "LIBRARY_PATH", "CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "LD_PRELOAD", "LD_LIBRARY_PATH", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")


def digest(path: Path, algorithm: str = "sha256") -> str:
    result = hashlib.new(algorithm)
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def checked_path(path: Path) -> Path:
    path = path.absolute()
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", str(path)) or ".." in path.parts:
        raise ValueError("build paths must be absolute/canonical and contain no spaces or shell/pkg-config metacharacters")
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("symlink is not an accepted build/input path: " + str(path))
    return path


def inspect_tools(toolchain: Path) -> dict:
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("this selected build profile requires x86_64 Linux")
    if sys.version_info < (3, 11):
        raise ValueError("Python 3.11 or later is required")
    paths = {"gcc": Path("/usr/bin/gcc"), "ar": Path("/usr/bin/ar"), "pkg-config": Path("/usr/bin/pkg-config"),
             "cargo": toolchain / "cargo", "rustc": toolchain / "rustc", "rustdoc": toolchain / "rustdoc"}
    for name, path in paths.items():
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError("required existing tool is unavailable: " + str(path))
        if name in TOOLCHAIN_HASHES and digest(path) != TOOLCHAIN_HASHES[name]:
            raise ValueError("tool differs from selected binary: " + name)
    def query(name, *arguments):
        return subprocess.run([str(paths[name]), *arguments], check=True, capture_output=True, text=True).stdout.strip()
    versions = {"gcc": query("gcc", "-dumpfullversion"), "gcc_target": query("gcc", "-dumpmachine"),
                "ar": query("ar", "--version").splitlines()[0], "pkg-config": query("pkg-config", "--version"),
                "cargo": query("cargo", "--version"), "rustc": query("rustc", "-vV")}
    if versions["gcc"] != "13.3.0" or versions["gcc_target"] != "x86_64-linux-gnu" or not versions["ar"].endswith(" 2.42") or versions["pkg-config"] != "1.8.1":
        raise ValueError("native tools differ from the selected GCC13.3.0/Binutils2.42/pkgconf1.8.1 profile")
    return {"paths": {k: str(v) for k, v in paths.items()}, "versions": versions,
            "sha256": {k: digest(v) for k, v in paths.items()}}


class OfficialRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, newurl):
        destination = urllib.parse.urlparse(newurl)
        if destination.scheme != "https" or destination.netloc not in ("sqlite.org", "www.sqlite.org"):
            raise ValueError("refusing SQLite download redirect outside its official HTTPS origin")
        return super().redirect_request(request, file, code, message, headers, newurl)


def verify_archive(path: Path) -> None:
    if not path.is_file() or path.stat().st_size > MAX_ARCHIVE or digest(path, "sha3_256") != SHA3:
        raise ValueError("SQLite archive does not match the selected SHA3-256: " + str(path))


def acquire(path: Path, offline: bool) -> str:
    if path.exists():
        verify_archive(path)
        return "reused verified local archive"
    if offline:
        raise ValueError("offline archive is missing: " + str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".sqlite-download-", dir=path.parent)
    temporary = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as output, urllib.request.build_opener(OfficialRedirects()).open(URL, timeout=60) as response:
            total = 0
            while block := response.read(1024 * 1024):
                total += len(block)
                if total > MAX_ARCHIVE:
                    raise ValueError("SQLite download exceeded the archive bound")
                output.write(block)
        verify_archive(temporary)
        if path.exists():
            verify_archive(path)
        else:
            temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return "acquired and verified official archive"


def extract(archive: Path, build: Path) -> None:
    expected = {PREFIX + "/", *(PREFIX + "/" + name for name in (*SOURCE_FILES, "shell.c"))}
    with zipfile.ZipFile(archive) as source:
        members = source.infolist()
        if len(members) != len(expected) or {m.filename for m in members} != expected:
            raise ValueError("unexpected SQLite archive members")
        if sum(m.file_size for m in members) > MAX_EXPANDED:
            raise ValueError("SQLite archive expands beyond the selected source bound")
        for item in members:
            name = PurePosixPath(item.filename)
            kind = stat.S_IFMT(item.external_attr >> 16)
            if name.is_absolute() or ".." in name.parts or "\\" in item.filename or item.flag_bits & 1 or kind not in (0, stat.S_IFREG, stat.S_IFDIR):
                raise ValueError("unsafe archive member: " + item.filename)
        for name in SOURCE_FILES:
            # Do not use extractall, honor archive permissions, or extract shell.c.
            (build / name).write_bytes(source.read(PREFIX + "/" + name))


def environment(build: Path, tools: dict) -> dict:
    return {"SQLITE3_LIB_DIR": str(build), "SQLITE3_INCLUDE_DIR": str(build), "SQLITE3_STATIC": "1",
            "PKG_CONFIG_LIBDIR": str(build / "pkgconfig"), "PKG_CONFIG_PATH": str(build / "pkgconfig"),
            "RUSTC": tools["paths"]["rustc"], "RUSTDOC": tools["paths"]["rustdoc"]}


PROBE = r'''#include "sqlite3.h"
#include <stdio.h>
int main(void) {
    printf("{\"version\":\"%s\",\"source_id\":\"%s\",\"threadsafe\":%d,\"api_armor\":%d,\"foreign_keys\":%d}\n",
        sqlite3_libversion(), sqlite3_sourceid(), sqlite3_threadsafe(),
        sqlite3_compileoption_used("ENABLE_API_ARMOR"), sqlite3_compileoption_used("DEFAULT_FOREIGN_KEYS"));
    return 0;
}
'''


def run(command: list[str], cwd: Path, env: dict, log: Path, commands: list) -> str:
    with log.open("w") as output:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
    commands.append({"command": command, "cwd": str(cwd), "exit": result.returncode,
                     "log": str(log), "log_sha256": digest(log)})
    if result.returncode:
        raise ValueError(f"command failed ({result.returncode}); see {log}")
    return log.read_text()


def prepare(build: Path, archive: Path, tools: dict, acquisition: str) -> dict:
    receipt_path = build / "build-receipt.json"
    profile = {"sqlite": VERSION, "source_id": SOURCE_ID, "archive_sha3_256": SHA3, "cflags": CFLAGS,
               "tool_sha256": tools["sha256"], "builder_sha256": digest(Path(__file__)), "build_dir": str(build)}
    if build.exists() and any(build.iterdir()):
        if not receipt_path.is_file():
            raise ValueError("nonempty build directory has no matching receipt; use a fresh --build-dir")
        receipt = json.loads(receipt_path.read_text())
        if receipt.get("profile") != profile or receipt.get("status") != "passed":
            raise ValueError("existing build receipt differs; use a fresh --build-dir")
        required = {*SOURCE_FILES, "sqlite3.o", "libsqlite3.a", "pkgconfig/sqlite3.pc", "env.sh", "identity.c", "identity-probe"}
        if set(receipt.get("outputs", {})) != required:
            raise ValueError("existing receipt has an incomplete output inventory")
        for name, expected in receipt["outputs"].items():
            path = checked_path(build / name)
            if not path.is_file() or digest(path) != expected:
                raise ValueError("existing build output changed: " + name)
        return receipt
    build.mkdir(parents=True, exist_ok=True)
    receipt = {"status": "building", "profile": profile, "tools": tools, "archive": str(archive),
               "archive_sha256": digest(archive), "acquisition": acquisition, "commands": [], "outputs": {}}
    env = os.environ.copy()
    env.update(environment(build, tools))
    try:
        extract(archive, build)
        receipt["source_input_sha256"] = {name: digest(build / name) for name in SOURCE_FILES}
        run([tools["paths"]["gcc"], *CFLAGS, "-c", "sqlite3.c", "-o", "sqlite3.o"], build, env, build / "compile.log", receipt["commands"])
        run([tools["paths"]["ar"], "rcsD", "libsqlite3.a", "sqlite3.o"], build, env, build / "archive.log", receipt["commands"])
        (build / "pkgconfig").mkdir()
        (build / "pkgconfig/sqlite3.pc").write_text(f"prefix={build}\nlibdir=${{prefix}}\nincludedir=${{prefix}}\nName: SQLite\nDescription: Borg v2 selected private SQLite\nVersion: {VERSION}\nLibs: -L${{libdir}} -lsqlite3\nLibs.private: -lm -ldl -lpthread\nCflags: -I${{includedir}}\n")
        (build / "env.sh").write_text("# Per-command environment only; no global configuration.\n" + "".join(f"export {key}={shlex.quote(value)}\n" for key, value in environment(build, tools).items()))
        (build / "identity.c").write_text(PROBE)
        run([tools["paths"]["gcc"], "-std=c11", "identity.c", str(build / "libsqlite3.a"), "-lm", "-ldl", "-lpthread", "-o", "identity-probe"], build, env, build / "identity-link.log", receipt["commands"])
        identity = json.loads(run([str(build / "identity-probe")], build, env, build / "identity.log", receipt["commands"]))
        if identity != {"version": VERSION, "source_id": SOURCE_ID, "threadsafe": 1, "api_armor": 1, "foreign_keys": 1}:
            raise ValueError("linked SQLite identity/options differ from selected source")
        flags = run([tools["paths"]["pkg-config"], "--static", "--libs", "sqlite3"], build, env, build / "pkg-config.log", receipt["commands"])
        if shlex.split(flags) != ["-L" + str(build), "-lsqlite3", "-lm", "-ldl", "-lpthread"]:
            raise ValueError("pkg-config did not select the exact private static link flags")
        verify_archive(archive)
        if receipt["source_input_sha256"] != {name: digest(build / name) for name in SOURCE_FILES}:
            raise ValueError("SQLite source changed during compilation")
        if tools["sha256"] != {name: digest(Path(path)) for name, path in tools["paths"].items()}:
            raise ValueError("selected tool changed during compilation")
        if digest(Path(__file__)) != profile["builder_sha256"]:
            raise ValueError("build tool source changed during compilation")
        receipt["identity"] = identity
        outputs = [*SOURCE_FILES, "sqlite3.o", "libsqlite3.a", "pkgconfig/sqlite3.pc", "env.sh", "identity.c", "identity-probe"]
        receipt["outputs"] = {name: digest(build / name) for name in outputs}
        receipt["status"] = "passed"
    except Exception as error:
        receipt["status"] = "failed"
        receipt["error"] = str(error)
        raise
    finally:
        write_json(receipt_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--build-dir", type=Path, help="fresh output directory, or an intact matching tool-created build")
    parser.add_argument("--archive", type=Path, help="reuse this exact archive; acquire at this path only if missing")
    parser.add_argument("--toolchain-dir", type=Path, default=Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))) / "toolchains/stable-x86_64-unknown-linux-gnu/bin")
    parser.add_argument("--offline", action="store_true", help="never download; also pass --offline to optional Cargo")
    parser.add_argument("--cargo", choices=("build", "check", "test"), help="optional ordinary whole-package Cargo command")
    parser.add_argument("--release", action="store_true", help="use Cargo's release profile")
    args = parser.parse_args()
    try:
        root = checked_path(args.root)
        build = checked_path(args.build_dir or root / ".build/sqlite")
        archive = checked_path(args.archive or root / ".build" / ARCHIVE)
        if any(os.environ.get(name) for name in BUILD_OVERRIDES):
            raise ValueError("unreviewed compiler/linker environment override; use a clean build shell")
        tools = inspect_tools(args.toolchain_dir.absolute())
        acquisition = acquire(archive, args.offline)
        receipt = prepare(build, archive, tools, acquisition)
        print("SQLite build receipt:", build / "build-receipt.json", flush=True)
        print("Per-command environment:", build / "env.sh", flush=True)
        if args.cargo:
            if not (root / "Cargo.toml").is_file() or not (root / "Cargo.lock").is_file():
                raise ValueError("optional Cargo requires the checkout's Cargo.toml and Cargo.lock")
            manifest_before = digest(root / "Cargo.toml")
            lock_before = digest(root / "Cargo.lock")
            source_paths = sorted({*list((root / "src").rglob("*.rs")), *list((root / "tests").rglob("*.rs")), *([root / "build.rs"] if (root / "build.rs").is_file() else [])})
            source_before = {str(path): digest(path) for path in source_paths}
            command = [tools["paths"]["cargo"], args.cargo, "--locked"]
            if args.offline:
                command.append("--offline")
            if args.release:
                command.append("--release")
            output = Path(tempfile.mkdtemp(prefix="cargo-", dir=build))
            commands = []
            cargo_receipt = {"sqlite_receipt_sha256": digest(build / "build-receipt.json"), "manifest_sha256": manifest_before, "lock_sha256": lock_before, "source_sha256": source_before, "commands": commands}
            env = os.environ.copy()
            env.update(environment(build, tools))
            try:
                run(command, root, env, output / "command.log", commands)
                if digest(root / "Cargo.toml") != manifest_before or digest(root / "Cargo.lock") != lock_before:
                    raise ValueError("Cargo manifest/lock changed during the locked command")
                if source_before != {str(path): digest(path) for path in source_paths}:
                    raise ValueError("Rust sources changed during the Cargo command")
                cargo_receipt["status"] = "passed"
            except Exception as error:
                cargo_receipt.update(status="failed", error=str(error))
                raise
            finally:
                write_json(output / "receipt.json", cargo_receipt)
                print("Cargo receipt:", output / "receipt.json", flush=True)
        return 0
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, zipfile.BadZipFile) as error:
        print("Build preparation failed:", error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
