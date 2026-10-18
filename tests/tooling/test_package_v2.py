"""Package mechanics with real private Git and a mocked expensive build boundary.

These tests do not compile Rust/SQLite or qualify the fake ELF as runnable. Root's
actual two-build and packaged-controller acceptance is separate evidence.
"""
from pathlib import Path
import copy
import datetime
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

import package_v2 as package
import build_v2 as builder
from selected_package_fixture import historical_build_files, selected_build_files


ROOT = Path(__file__).resolve().parents[2]


class ReproduciblePathTests(unittest.TestCase):
    def settings(self, prefix):
        return builder.reproducible_environment(
            Path(prefix) / "source", Path(prefix) / "sqlite",
            {"paths": {"rustc": prefix + "/toolchain/bin/rustc"}},
            {"CARGO_HOME": prefix + "/cargo", "CARGO_TARGET_DIR": prefix + "/target"})

    def test_distinct_roots_map_all_compiler_roles_to_the_same_paths(self):
        # This tests flag construction, not binary reproducibility. The real
        # paired offline package build is a separately retained qualification.
        for role in ("source", "sqlite", "cargo", "target", "toolchain"):
            results = []
            for prefix in ("/tmp/borg-short", "/tmp/borg-longer-separate-root"):
                values = self.settings(prefix)
                physical = prefix + "/" + role + "/nested/original.rs"
                rust = values["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")
                c = values["CFLAGS"].split()
                for flags, kind in ((rust, "--remap-path-prefix="), (c, "-ffile-prefix-map=")):
                    matches = [flag.removeprefix(kind).split("=", 1) for flag in flags
                               if physical.startswith(flag.removeprefix(kind).split("=", 1)[0])]
                    self.assertTrue(matches)
                    source, target = matches[-1]
                    results.append(target + physical[len(source):])
                self.assertEqual(values["CFLAGS"], values["CXXFLAGS"])
            self.assertEqual(results, ["/borg-build/" + role + "/nested/original.rs"] * 4)

    def test_missing_or_ambiguous_roles_and_external_overrides_refuse(self):
        tools = {"paths": {"rustc": "/tmp/selected/toolchain/bin/rustc"}}
        selected = {"CARGO_HOME": "/tmp/selected/cargo", "CARGO_TARGET_DIR": "/tmp/selected/target"}
        for key in selected:
            with self.subTest(missing=key), self.assertRaisesRegex(ValueError, "explicit Cargo"):
                builder.reproducible_environment(Path("/tmp/selected/source"), Path("/tmp/selected/sqlite"),
                                                tools, {k: v for k, v in selected.items() if k != key})
        with self.assertRaisesRegex(ValueError, "overlap"):
            builder.reproducible_environment(Path(selected["CARGO_HOME"]), Path("/tmp/selected/sqlite"), tools, selected)
        for name in builder.BUILD_OVERRIDES:
            with self.subTest(override=name), self.assertRaisesRegex(ValueError, "overrides"):
                builder.reproducible_environment(Path("/tmp/selected/source"), Path("/tmp/selected/sqlite"),
                                                tools, {**selected, name: "unselected"})

    def test_package_pins_the_selected_normalization_and_builder(self):
        self.assertEqual(package.ENV_PROFILE["path_normalization"], builder.PATH_NORMALIZATION)
        self.assertEqual(package.BUILDER_SHA, package.digest(ROOT / "tooling/build_v2.py"))

    def test_both_release_consumers_pin_the_selected_verifier(self):
        import prepare_v2_release
        import protected_release_v2
        selected = package.digest(ROOT / "tooling/package_v2.py")
        self.assertEqual(prepare_v2_release.VERIFIER_SHA, selected)
        self.assertEqual(protected_release_v2.VERIFIER_SHA, selected)


def fake_elf():
    data = bytearray(128)
    data[:7] = b"\x7fELF\x02\x01\x01"
    data[16:18] = (3).to_bytes(2, "little")
    data[18:20] = (62).to_bytes(2, "little")
    return bytes(data)


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="borg-package-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repository"
        self.repo.mkdir()
        self.git("init", "--quiet")
        self.git("config", "user.email", "package-test@example.invalid")
        self.git("config", "user.name", "Package fixture")
        for name, raw in selected_build_files().items():
            (self.repo / name).write_bytes(raw)
        # Compilation remains mocked. Exact selected Cargo bytes make the
        # fixture usable by a separately loaded, unmodified package verifier.
        (self.repo / "src").mkdir()
        (self.repo / "src/main.rs").write_text("fn main() {}\n")
        (self.repo / "src/lib.rs").write_text("// private unit fixture\n")
        self.commit = self.commit_all()
        self.archive = self.root / "sqlite.zip"
        self.archive.write_bytes(b"controlled archive at mocked qualification boundary")
        self.cache = self.root / "cargo-cache"
        self.cache.mkdir()
        self.tools = self.root / "tools"
        self.tools.mkdir()

    def git(self, *args):
        env = {"PATH": "/usr/bin:/bin", "HOME": str(self.root), "LC_ALL": "C", "GIT_CONFIG_NOSYSTEM": "1",
               "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"}
        return subprocess.run(["/usr/bin/git", "-C", str(self.repo), *args], env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout

    def commit_all(self):
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "private package fixture")
        return self.git("rev-parse", "HEAD").decode().strip()

    def fake_build(self, checkout, output, builder, archive, toolchain, cargo_home, source=None):
        self.assertEqual(package.digest(builder), package.BUILDER_SHA)
        self.assertNotEqual(builder, checkout / "tooling/build_v2.py")
        self.assertEqual(archive, self.archive)
        self.assertEqual(toolchain, self.tools)
        self.assertEqual(cargo_home, self.cache)
        binary = output / "target/release/borg-runtime"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(fake_elf())
        binary.chmod(0o755)
        (output / "build.log").write_text("controlled build-boundary fixture, not Rust execution\n")
        return {"tools": {"sha256": package.TOOL_SHA, "versions": {
                    "gcc": "13.3.0", "gcc_target": "x86_64-linux-gnu", "ar": "GNU ar 2.42", "pkg-config": "1.8.1",
                    "cargo": "cargo 1.99.0 (controlled fixture)", "rustc": "rustc 1.99.0 (controlled fixture)\nhost: " + package.TARGET}},
                "sqlite": {"identity": {"version":"3.54.0", "source_id": package.SQLITE_SOURCE, "threadsafe":1, "api_armor":1, "foreign_keys":1},
                           "source_sha256": package.SQLITE_FILES, "archive_sha256": package.SQLITE_ARCHIVE_SHA,
                           "cflags": package.CFLAGS, "static_library_sha256": "1" * 64}}

    def build(self, output, effect=None):
        with patch.object(package, "verify_archive") as archive_check, patch.object(package, "verify_tool_files"), patch.object(package, "run_builder", side_effect=effect or self.fake_build):
            result = package.build(self.repo, self.commit, output, self.archive, self.tools, self.cache)
            archive_check.assert_called_once_with(self.archive)
            return result

    #= spec/impl/v2/implementation.md#changes-can-be-prepared-without-per-change-human-approval
    #= type=test
    ## Change preparation MUST produce a deployable artifact linked to its source revision.
    # Scope: real immutable Git input/package verification with a mocked build;
    # runnable artifact qualification is the separate actual root build/consumer.
    def test_complete_deterministic_package_and_external_identity_verification(self):
        first = self.build(self.root / "one")
        second = self.build(self.root / "two")
        self.assertEqual(first["package_sha256"], second["package_sha256"])
        self.assertEqual((self.root / "one/borg-runtime.tar").read_bytes(), (self.root / "two/borg-runtime.tar").read_bytes())
        manifest = package.verify(self.root / "one/borg-runtime.tar", first["package_sha256"], self.commit)
        self.assertEqual(manifest["source"]["commit"], self.commit)
        self.assertEqual(manifest["source"]["tree"], self.git("rev-parse", "HEAD^{tree}").decode().strip())
        self.assertEqual(set(manifest["files"]), {"bin/borg-runtime"})
        encoded = package.canonical(manifest)
        self.assertNotIn(str(self.root).encode(), encoded)
        self.assertNotIn(b"started_unix_ns", encoded)
        self.assertNotEqual(first["output"], second["output"])
        self.assertEqual(json.loads((self.root / "one/receipt.json").read_text())["status"], "passed")

    def test_dirty_working_tree_cannot_replace_selected_commit(self):
        (self.repo / "src/main.rs").write_text("malicious dirty bytes\n")
        (self.repo / "untracked.rs").write_text("not selected\n")
        result = self.build(self.root / "out")
        manifest = package.verify(self.root / "out/borg-runtime.tar", result["package_sha256"], self.commit)
        self.assertEqual((self.root / "out/source/src/main.rs").read_text(), "fn main() {}\n")
        self.assertNotIn("untracked.rs", manifest["source"]["files"])
        with self.assertRaisesRegex(ValueError, "full lowercase"):
            package.immutable_tree(self.repo, "HEAD")

    def test_manifest_and_lock_change_refuse_before_builder(self):
        for name in ("Cargo.toml", "Cargo.lock"):
            original = (self.repo / name).read_bytes()
            # Syntax-only comments are admitted by revision 2; a new parsed
            # field still changes the complete selected policy and refuses.
            (self.repo / name).write_bytes(b"unselected = true\n" + original)
            self.commit = self.commit_all()
            with patch.object(package, "verify_archive"), patch.object(package, "verify_tool_files"), patch.object(package, "run_builder") as builder:
                out = self.root / name.replace(".", "-")
                with self.assertRaisesRegex(ValueError, "manifest/lock"):
                    package.build(self.repo, self.commit, out, self.archive, self.tools, self.cache)
                builder.assert_not_called()
                self.assertEqual(json.loads((out / "receipt.json").read_text())["status"], "failed")
            (self.repo / name).write_bytes(original)

    def test_selected_profile_matches_exact_current_cargo_inputs(self):
        actual = {name: (ROOT / name).read_bytes() for name in ("Cargo.toml", "Cargo.lock")}
        self.assertEqual(actual, selected_build_files())
        self.assertEqual(package.PROFILE["cargo_manifest_sha256"], hashlib.sha256(actual["Cargo.toml"]).hexdigest())
        self.assertEqual(package.PROFILE["cargo_lock_sha256"], hashlib.sha256(actual["Cargo.lock"]).hexdigest())

    def test_historical_profile_is_not_silently_admitted_by_current_reader(self):
        # Historical artifacts retain their original reader/recipe; selecting
        # current exact Cargo inputs must not turn this reader into a fallback.
        historical = historical_build_files()
        self.assertNotEqual(historical, selected_build_files())
        for name, raw in historical.items():
            (self.repo / name).write_bytes(raw)
        self.commit = self.commit_all()
        out = self.root / "unselected-historical-profile"
        with patch.object(package, "verify_archive"), patch.object(package, "verify_tool_files"), patch.object(package, "run_builder") as run:
            with self.assertRaisesRegex(ValueError, "manifest/lock"):
                package.build(self.repo, self.commit, out, self.archive, self.tools, self.cache)
            run.assert_not_called()
        self.assertEqual(json.loads((out / "receipt.json").read_text())["status"], "failed")
        self.assertFalse((out / "borg-runtime.tar").exists())

    def test_unselected_feature_change_refuses_before_builder(self):
        path = self.repo / "Cargo.toml"
        original = path.read_text()
        self.assertEqual(original.count('features = ["rustls", "http2"]'), 1)
        path.write_text(original.replace('features = ["rustls", "http2"]',
                                         'features = ["rustls", "http2", "gzip"]'))
        self.commit = self.commit_all()
        out = self.root / "unselected-feature"
        with patch.object(package, "verify_archive"), patch.object(package, "verify_tool_files"), patch.object(package, "run_builder") as run:
            with self.assertRaisesRegex(ValueError, "manifest/lock"):
                package.build(self.repo, self.commit, out, self.archive, self.tools, self.cache)
            run.assert_not_called()
        self.assertFalse((out / "borg-runtime.tar").exists())

    def test_current_reader_refuses_historical_profile_in_canonical_package(self):
        result = self.build(self.root / "good-current")
        manifest = package.verify(self.root / "good-current/borg-runtime.tar", result["package_sha256"], self.commit)
        historical = historical_build_files()
        for name, raw in historical.items():
            entry = manifest["source"]["files"][name]
            entry.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                         git_blob=hashlib.sha1(b"blob "+str(len(raw)).encode()+b"\0"+raw).hexdigest())
        manifest["profile"]["cargo_manifest_sha256"] = hashlib.sha256(historical["Cargo.toml"]).hexdigest()
        manifest["profile"]["cargo_lock_sha256"] = hashlib.sha256(historical["Cargo.lock"]).hexdigest()
        out = self.root / "historical-profile.tar"
        package.package_tar(out, self.root / "good-current/target/release/borg-runtime", manifest)
        with self.assertRaisesRegex(ValueError, "manifest profile"):
            package.verify(out, package.digest(out), self.commit)

    def test_symlinks_submodules_and_config_inputs_refuse(self):
        (self.repo / "link").symlink_to("src/main.rs")
        bad = self.commit_all()
        with self.assertRaisesRegex(ValueError, "symlink"):
            package.immutable_tree(self.repo, bad)
        (self.repo / "link").unlink()
        self.git("add", "-u")
        self.git("update-index", "--add", "--cacheinfo", "160000," + self.commit + ",nested")
        self.git("commit", "--quiet", "-m", "controlled gitlink")
        bad = self.git("rev-parse", "HEAD").decode().strip()
        with self.assertRaisesRegex(ValueError, "submodule"):
            package.immutable_tree(self.repo, bad)
        for path in ["../escape", "/absolute", "a/../../b", "a\\b", "a/.git/config", ".cargo/config.toml", "build.rs", "src/bin/extra.rs", "nested/Cargo.toml", "rust-toolchain.toml", "target/release/borg-runtime"]:
            with self.subTest(path=path), self.assertRaises(ValueError):
                package.source_policy(path)

    def test_source_selected_builder_is_never_executed(self):
        (self.repo / "tooling").mkdir()
        (self.repo / "tooling/build_v2.py").write_text("raise RuntimeError('must never execute source builder')\n")
        self.commit = self.commit_all()
        self.build(self.root / "out")

    def test_stale_output_not_overwritten_and_builder_failure_retained(self):
        stale = self.root / "stale"
        stale.mkdir()
        (stale / "sentinel").write_text("preserve")
        with self.assertRaises(FileExistsError):
            self.build(stale)
        self.assertEqual((stale / "sentinel").read_text(), "preserve")
        def failed(checkout, output, *args):
            (output / "build.log").write_text("controlled failed compiler boundary")
            raise ValueError("controlled build failure")
        with self.assertRaisesRegex(ValueError, "controlled build failure"):
            self.build(self.root / "failed", failed)
        receipt = json.loads((self.root / "failed/receipt.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertNotIn("package_sha256", receipt)
        self.assertTrue((self.root / "failed/build.log").exists())
        self.assertFalse((self.root / "failed/borg-runtime.tar").exists())

    def test_source_mutation_and_unselected_config_appearance_refuse(self):
        def mutate(checkout, output, *args):
            result = self.fake_build(checkout, output, *args)
            (checkout / "src/main.rs").write_text("changed while compiling\n")
            return result
        with self.assertRaisesRegex(ValueError, "checkout drift"):
            self.build(self.root / "drift", mutate)
        (self.cache / "config.toml").write_text("[build]\nrustflags=['--cfg=surprise']\n")
        with self.assertRaisesRegex(ValueError, "Cargo configuration"):
            self.build(self.root / "config")

    def test_binary_symlink_and_wrong_target_refuse(self):
        def symlink(checkout, output, *args):
            result = self.fake_build(checkout, output, *args)
            binary = output / "target/release/borg-runtime"
            binary.rename(output / "other")
            binary.symlink_to(output / "other")
            return result
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.build(self.root / "link-output", symlink)
        def wrong_target(checkout, output, *args):
            result = self.fake_build(checkout, output, *args)
            binary = output / "target/release/borg-runtime"
            data = bytearray(binary.read_bytes())
            data[18:20] = (183).to_bytes(2, "little")
            binary.write_bytes(data)
            return result
        with self.assertRaisesRegex(ValueError, "selected x86_64"):
            self.build(self.root / "target-output", wrong_target)

    def test_tamper_traversal_extra_inventory_and_truncation_refuse_without_extract(self):
        receipt = self.build(self.root / "good")
        source = self.root / "good/borg-runtime.tar"
        original = source.read_bytes()
        variants = []
        changed = bytearray(original)
        changed[512 + 30] ^= 1
        variants.append(bytes(changed))
        variants.append(package.tar_header("../escape", 128, 0o555) + original[512:])
        variants.append(original + b"unexpected suffix")
        variants.append(original[:-1])
        for index, data in enumerate(variants):
            path = self.root / f"bad-{index}.tar"
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                package.verify(path, receipt["package_sha256"], self.commit)
            with self.assertRaises(ValueError):
                package.verify(path, hashlib.sha256(data).hexdigest(), self.commit)
        self.assertFalse((self.root / "escape").exists())

    def test_self_consistent_replacement_requires_new_external_digest_and_expected_commit(self):
        receipt = self.build(self.root / "good")
        manifest = json.loads((self.root / "good/manifest.json").read_text())
        manifest["source"]["commit"] = "e" * 40
        forged = self.root / "replacement.tar"
        package.package_tar(forged, self.root / "good/target/release/borg-runtime", manifest)
        with self.assertRaisesRegex(ValueError, "caller-supplied"):
            package.verify(forged, receipt["package_sha256"], self.commit)
        with self.assertRaisesRegex(ValueError, "source identity"):
            package.verify(forged, package.digest(forged), self.commit)

    def test_archive_and_trusted_builder_pin_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "SQLite archive"):
            package.verify_archive(self.archive)
        with patch.object(package, "BUILDER_SHA", "f" * 64), self.assertRaisesRegex(ValueError, "trusted recipe"):
            package.build(self.repo, self.commit, self.root / "recipe-refused", self.archive, self.tools, self.cache)

    def test_sha256_git_source_uses_actual_object_format(self):
        self.repo = self.root / "sha256-repository"
        self.repo.mkdir()
        self.git("init", "--quiet", "--object-format=sha256")
        self.git("config", "user.email", "package-test@example.invalid")
        self.git("config", "user.name", "Package fixture")
        for name, raw in selected_build_files().items():
            (self.repo / name).write_bytes(raw)
        (self.repo / "src").mkdir()
        (self.repo / "src/main.rs").write_text("fn main() {}\n")
        (self.repo / "src/lib.rs").write_text("// sha256 fixture\n")
        self.commit = self.commit_all()
        self.assertEqual(len(self.commit), 64)
        result = self.build(self.root / "sha256-output")
        manifest = package.verify(self.root / "sha256-output/borg-runtime.tar", result["package_sha256"], self.commit)
        self.assertEqual(manifest["source"]["object_format"], "sha256")
        self.assertEqual(len(manifest["source"]["tree"]), 64)

    def test_promisor_and_partial_clone_configs_refuse_without_fetch(self):
        self.git("config", "remote.fixture.promisor", "true")
        self.git("config", "remote.fixture.url", "https://example.invalid/must-not-contact")
        with self.assertRaisesRegex(ValueError, "promisor/partial"):
            package.immutable_tree(self.repo, self.commit)

    def test_finite_recipe_fields_and_fifo_refuse_even_with_new_expected_digest(self):
        result = self.build(self.root / "good")
        manifest = json.loads((self.root / "good/manifest.json").read_text())
        manifest["build"]["tools"]["sha256"]["cargo"] = "f" * 64
        path = self.root / "wrong-recipe.tar"
        package.package_tar(path, self.root / "good/target/release/borg-runtime", manifest)
        with self.assertRaisesRegex(ValueError, "tool identities"):
            package.verify(path, package.digest(path), self.commit)
        fifo = self.root / "pipe"
        os.mkfifo(fifo)
        with patch.object(package, "digest", side_effect=AssertionError("must not read FIFO")), self.assertRaisesRegex(ValueError, "package bound"):
            package.verify(fifo, "0" * 64)

    def test_corrupt_commit_and_tree_object_identities_refuse(self):
        original = package.git
        for kind in ("commit", "tree"):
            def altered(repo, *args, **kwargs):
                data = original(repo, *args, **kwargs)
                if args[:2] == ("cat-file", kind):
                    return bytes([data[0] ^ 1]) + data[1:]
                return data
            with self.subTest(kind=kind), patch.object(package, "git", side_effect=altered), self.assertRaisesRegex(ValueError, "object bytes"):
                package.immutable_tree(self.repo, self.commit)

    def refuse_cargo(self, name, raw, label):
        for path, original in selected_build_files().items():
            (self.repo / path).write_bytes(raw if path == name else original)
        self.commit = self.commit_all()
        output = self.root / label
        with patch.object(package, "verify_archive"), patch.object(package, "verify_tool_files"), patch.object(package, "run_builder") as run:
            with self.assertRaises((ValueError, RecursionError)):
                package.build(self.repo, self.commit, output, self.archive, self.tools, self.cache)
            run.assert_not_called()
        self.assertFalse((output / "borg-runtime.tar").exists())
        self.assertEqual(json.loads((output / "receipt.json").read_bytes())["status"], "failed")

    def test_candidate_toml_syntax_changes_keep_policy_and_bind_exact_source(self):
        selected = selected_build_files()
        self.assertEqual(len(package.parsed_toml(selected["Cargo.toml"].decode())), 1760)
        self.assertEqual(len(package.parsed_toml(selected["Cargo.lock"].decode())), 57914)
        results = []
        for index in range(2):
            manifest = selected["Cargo.toml"].decode().replace('edition = "2024"', "edition = '2024'")
            # Table/key order, comments, literal spelling and line endings only.
            if index:
                manifest = "\n\n".join(reversed(manifest.strip().split("\n\n"))) + "\n"
            inputs = {"Cargo.toml": (f"# candidate {index} \u2603\n" + manifest).encode(),
                      "Cargo.lock": (f"# candidate {index}\n" + selected["Cargo.lock"].decode().replace("version = 4", "version = 0x4", 1)).replace("\n", "\r\n").encode()}
            for name, raw in inputs.items():
                (self.repo / name).write_bytes(raw)
                self.assertEqual(package.parsed_toml(raw.decode()), package.parsed_toml(selected[name].decode()))
            self.commit = self.commit_all()
            # Dirty policy-changing text must never replace the committed input.
            (self.repo / "Cargo.toml").write_text('[package]\nname = "dirty"\n')
            output = self.root / f"syntax-{index}"
            def controlled(checkout, *args):
                for name, raw in inputs.items():
                    self.assertEqual((checkout / name).read_bytes(), raw)
                return self.fake_build(checkout, *args)
            result = self.build(output, controlled)
            value = package.verify(output / "borg-runtime.tar", result["package_sha256"], self.commit)
            self.assertEqual(value["profile"]["revision"], 2)
            self.assertEqual(value["profile"]["dependency_policy"], package.DEPENDENCY_POLICY)
            for name, (key, _) in package.CARGO_INPUTS.items():
                raw = inputs[name]; entry = value["source"]["files"][name]
                self.assertEqual(value["profile"]["cargo_inputs"][name].encode(), raw)
                self.assertEqual(value["profile"][key], hashlib.sha256(raw).hexdigest())
                self.assertEqual(entry["sha256"], value["profile"][key])
                self.assertEqual(entry["size"], len(raw))
                self.assertEqual(entry["git_blob"], hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest())
                self.assertNotEqual(value["profile"][key], package.PROFILE[key])
            results.append((output, result, value))
        self.assertNotEqual(results[0][2]["source"]["commit"], results[1][2]["source"]["commit"])
        for key, _ in package.CARGO_INPUTS.values():
            self.assertNotEqual(results[0][2]["profile"][key], results[1][2]["profile"][key])
        shutil.rmtree(self.repo)
        for output, result, value in results:
            shutil.rmtree(output / "source")
            with patch.object(package, "git", side_effect=AssertionError("package-only verification cannot read Git")):
                self.assertEqual(package.verify(output / "borg-runtime.tar", result["package_sha256"], value["source"]["commit"]), value)

    def test_candidate_manifest_semantics_change_refuses_before_builder(self):
        raw = selected_build_files()["Cargo.toml"]
        changes = [
            (b'version = "0.2.0"', b'version = "0.2.1"'),
            (b'=0.13.5', b'=0.13.6'),
            (b'https://github.com/camshaft/bach', b'https://example.invalid/bach'),
            (b'default-features = false', b'default-features = true'),
            (b'default-features = false', b'default-features = 0'),
            (b'["rustls", "http2"]', b'["rustls", "http2", "gzip"]'),
            (b'["rustls", "http2"]', b'["http2", "rustls"]'),
            (b'[package]', b'[package]\nbuild = "different.rs"'),
            (b'httpdate = "=1.0.3"\n', b''),
            (b'[lints.rust]', b'[profile.release]\nopt-level = 1\n\n[lints.rust]'),
        ]
        for index, (before, after) in enumerate(changes):
            self.assertIn(before, raw)
            with self.subTest(change=before):
                self.refuse_cargo("Cargo.toml", raw.replace(before, after, 1), f"manifest-semantic-{index}")

    def test_candidate_lock_semantics_change_refuses_before_builder(self):
        raw = selected_build_files()["Cargo.lock"]
        checksum = re.search(rb'checksum = "([0-9a-f]+)"', raw).group(1)
        version = re.search(rb'version = "([^"]+)"', raw).group(1)
        edge = re.search(rb'dependencies = \[\n "([^"]+)"', raw).group(1)
        parts = raw.split(b"[[package]]")
        changes = [raw.replace(b"version = 4", b"version = 3", 1),
                   raw.replace(b"version = 4", b"version = true", 1),
                   raw.replace(b'version = "' + version + b'"', b'version = "0.0.0"', 1),
                   raw.replace(checksum, b"0" * len(checksum), 1),
                   raw.replace(b"registry+https://github.com/rust-lang/crates.io-index", b"registry+https://example.invalid/index", 1),
                   raw.replace(b' "' + edge + b'"', b' "unselected-edge"', 1),
                   b"[[package]]".join([parts[0], parts[2], parts[1], *parts[3:]])]
        for index, changed in enumerate(changes):
            with self.subTest(index=index):
                self.refuse_cargo("Cargo.lock", changed, f"lock-semantic-{index}")

    def test_candidate_input_caps_types_and_embedded_hashes_fail_closed(self):
        raw = selected_build_files()["Cargo.toml"]
        invalid = [b"[broken", b"same=1\nsame=1\n", b"bad = 1.5\n", b"bad = nan\n", b"bad = inf\n",
                   b"bad = 2026-10-18\n", b"bad = 12:34:56\n", b"bad = 2026-10-18T12:34:56Z\n",
                   b"bad = [1, {nested = 0.5}]\n", b"bad = null\n", b"\xff\n"]
        for index, value in enumerate(invalid):
            with self.subTest(input=value):
                self.refuse_cargo("Cargo.toml", value + raw, f"invalid-toml-{index}")
        class CustomInt(int):
            pass
        class CustomStr(str):
            pass
        for value in (None, 1.0, float('nan'), datetime.date(2026, 10, 18), (1,), CustomInt(1), CustomStr("x"), {1: "x"}):
            with self.subTest(type=type(value)), patch.object(package.tomllib, "loads", return_value={"nested": [value]}), self.assertRaises(ValueError):
                package.parsed_toml("unused")
        # Real bounded Git admission and escaped manifest preflight both occur
        # before the mocked build boundary; no dependency acquisition is used.
        for cap, value in [("MAX_SOURCE_FILE", 1024), ("MAX_SOURCE", 1024), ("MAX_MANIFEST", 200_000)]:
            text = b"#" + ("\u2603" * 40_000).encode() + b"\n" + raw if cap == "MAX_MANIFEST" else raw + b"\n# " + cap.encode() + b"\n"
            with self.subTest(cap=cap), patch.object(package, cap, value):
                self.refuse_cargo("Cargo.toml", text, "cap-" + cap)
        for name, value in selected_build_files().items():
            (self.repo / name).write_bytes(value)
        self.commit = self.commit_all()
        result = self.build(self.root / "tamper-good")
        original = package.verify(self.root / "tamper-good/borg-runtime.tar", result["package_sha256"])
        cases = []
        def change(fn):
            value = copy.deepcopy(original); fn(value); cases.append(value)
        change(lambda m: m["profile"]["cargo_inputs"].update({"Cargo.toml": m["profile"]["cargo_inputs"]["Cargo.toml"] + "# altered\n"}))
        for name, (key, policy_key) in package.CARGO_INPUTS.items():
            change(lambda m: m["profile"].update({key: "f" * 64}))
            change(lambda m: m["source"]["files"][name].update({"sha256": "f" * 64}))
            change(lambda m: m["source"]["files"][name].update({"size": m["source"]["files"][name]["size"] + 1}))
            change(lambda m: m["source"]["files"][name].update({"git_blob": "f" * 40}))
            change(lambda m: m["profile"]["dependency_policy"].update({policy_key: "f" * 64}))
        for revision in (1, 3, True, 2.0, "2"):
            change(lambda m: m["profile"].update({"revision": revision}))
        change(lambda m: m["profile"].update({"extra": True}))
        change(lambda m: m["profile"].pop("target"))
        change(lambda m: m["profile"]["cargo_inputs"].pop("Cargo.lock"))
        change(lambda m: m["profile"]["cargo_inputs"].update({"extra": ""}))
        for value in (None, False, 17, [], {}):
            change(lambda m: m["profile"]["cargo_inputs"].update({"Cargo.toml": value}))
        change(lambda m: m["profile"]["dependency_policy"].update({"revision": True}))
        change(lambda m: m["profile"]["dependency_policy"].update({"extra": 1}))
        change(lambda m: m["source"]["files"]["Cargo.toml"].update({"size": True}))
        change(lambda m: m["source"].update({"object_format": "sha256"}))
        for claim_changed_policy in (False, True):
            def rebind(m):
                text = m["profile"]["cargo_inputs"]["Cargo.toml"].replace('version = "0.2.0"', 'version = "9.0.0"')
                data = text.encode(); h = hashlib.sha256(data).hexdigest()
                m["profile"]["cargo_inputs"]["Cargo.toml"] = text
                m["profile"]["cargo_manifest_sha256"] = h
                m["source"]["files"]["Cargo.toml"].update(size=len(data), sha256=h, git_blob=hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest())
                if claim_changed_policy:
                    m["profile"]["dependency_policy"]["manifest_parsed_sha256"] = hashlib.sha256(package.parsed_toml(text)).hexdigest()
            change(rebind)
        for index, value in enumerate(cases):
            path = self.root / f"tamper-{index}.tar"
            package.package_tar(path, self.root / "tamper-good/target/release/borg-runtime", value)
            with self.subTest(tamper=index), self.assertRaises((ValueError, RecursionError)):
                package.verify(path, package.digest(path), self.commit)

    def test_candidate_build_receipt_hashes_must_match_immutable_source(self):
        for name, raw in selected_build_files().items():
            (self.repo / name).write_bytes(b"# receipt candidate\n" + raw)
        self.commit = self.commit_all()
        checkout = self.root / "receipt-source"
        source, profile = package.materialize(self.repo, self.commit, checkout)
        paths = {}; hashes = {}
        for name in package.TOOL_SHA:
            p = self.tools / name; p.write_text("controlled tool identity " + name)
            paths[name] = str(p); hashes[name] = package.digest(p)
        for index, wrong in enumerate((None, "manifest_sha256", "lock_sha256")):
            output = self.root / f"receipt-{index}"; output.mkdir()
            sql = output / "sqlite"; sql.mkdir(); (sql / "cargo-fixture").mkdir()
            (sql / "libsqlite3.a").write_bytes(b"controlled library")
            log = output / "cargo.log"; log.write_bytes(b"controlled command result")
            sqlite = {"status": "passed", "profile": {"builder_sha256": package.BUILDER_SHA, "archive_sha3_256": package.SQLITE_SHA3, "cflags": package.CFLAGS},
                      "identity": {"version": "3.54.0", "source_id": package.SQLITE_SOURCE, "threadsafe": 1, "api_armor": 1, "foreign_keys": 1},
                      "tools": {"paths": paths, "sha256": hashes, "versions": {"rustc": "rustc 1.99.0\nhost: " + package.TARGET}},
                      "outputs": {"libsqlite3.a": package.digest(sql / "libsqlite3.a")}, "source_input_sha256": package.SQLITE_FILES, "archive_sha256": package.SQLITE_ARCHIVE_SHA}
            package.write_json(sql / "build-receipt.json", sqlite)
            cargo = {"status": "passed", "manifest_sha256": profile["cargo_manifest_sha256"], "lock_sha256": profile["cargo_lock_sha256"],
                     "sqlite_receipt_sha256": package.digest(sql / "build-receipt.json"), "path_normalization": package.ENV_PROFILE["path_normalization"],
                     "path_environment": {k: "controlled" for k in ("CARGO_ENCODED_RUSTFLAGS", "CFLAGS", "CXXFLAGS")},
                     "commands": [{"exit": 0, "command": [paths["cargo"], *package.PROFILE["cargo_arguments"]], "cwd": str(checkout), "log": str(log), "log_sha256": package.digest(log)}]}
            if wrong:
                cargo[wrong] = package.MANIFEST_SHA if wrong == "manifest_sha256" else package.LOCK_SHA
            package.write_json(sql / "cargo-fixture/receipt.json", cargo)
            process = types.SimpleNamespace(pid=123, wait=lambda: 0, returncode=0)
            with self.subTest(wrong=wrong), patch.object(package.subprocess, "Popen", return_value=process) as spawn:
                args = (checkout, output, ROOT / "tooling/build_v2.py", self.archive, self.tools, self.cache, source)
                if wrong:
                    with self.assertRaisesRegex(ValueError, "build receipt association"):
                        package.run_builder(*args)
                else:
                    self.assertEqual(package.run_builder(*args)["sqlite"]["source_sha256"], package.SQLITE_FILES)
                spawn.assert_called_once()

    def test_legacy_profile_reader_is_explicit_and_new_profile_is_distinct(self):
        # Exercise the actual immediately selected prior recipe in a private
        # fixture. Both builds mock compilation; neither is a release PASS.
        old = subprocess.check_output(["/usr/bin/git", "show", "447d49bf549c8d93f7964de643f38776a4e17f6c:tooling/package_v2.py"], cwd=ROOT)
        root = self.root / "legacy-recipe"; root.mkdir()
        (root / "package_v2.py").write_bytes(old)
        (root / "build_v2.py").write_bytes((ROOT / "tooling/build_v2.py").read_bytes())
        spec = importlib.util.spec_from_file_location("legacy_package_recipe", root / "package_v2.py")
        legacy = importlib.util.module_from_spec(spec); spec.loader.exec_module(legacy)
        with patch.object(legacy, "verify_archive"), patch.object(legacy, "verify_tool_files"), patch.object(legacy, "run_builder", side_effect=self.fake_build):
            result = legacy.build(self.repo, self.commit, self.root / "legacy", self.archive, self.tools, self.cache)
        path = self.root / "legacy/borg-runtime.tar"; before = path.read_bytes()
        value = legacy.verify(path, result["package_sha256"], self.commit)
        self.assertEqual(package.verify(path, result["package_sha256"], self.commit), value)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(value["profile"], package.PROFILE)
        self.assertNotIn("revision", value["profile"])
        self.assertEqual(value["recipe"]["packager_sha256"], hashlib.sha256(old).hexdigest())
        current = self.build(self.root / "current-profile")
        new_path = self.root / "current-profile/borg-runtime.tar"
        new = package.verify(new_path, current["package_sha256"], self.commit)
        self.assertEqual(new["profile"]["revision"], 2)
        self.assertNotEqual(current["package_sha256"], result["package_sha256"])
        with self.assertRaisesRegex(ValueError, "manifest profile"):
            legacy.verify(new_path, current["package_sha256"], self.commit)
        for index, mutate in enumerate((lambda p: p.update(revision=1), lambda p: p.update(revision=2), lambda p: p.update(cargo_lock_sha256="a" * 64))):
            bad = copy.deepcopy(value); mutate(bad["profile"])
            forged = self.root / f"legacy-invalid-{index}.tar"
            package.package_tar(forged, self.root / "legacy/target/release/borg-runtime", bad)
            with self.assertRaisesRegex(ValueError, "manifest profile"):
                package.verify(forged, package.digest(forged), self.commit)


if __name__ == "__main__":
    unittest.main()
