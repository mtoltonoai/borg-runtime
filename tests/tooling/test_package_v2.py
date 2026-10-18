"""Package mechanics with real private Git and a mocked expensive build boundary.

These tests do not compile Rust/SQLite or qualify the fake ELF as runnable. Root's
actual two-build and packaged-controller acceptance is separate evidence.
"""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import tempfile
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

    def fake_build(self, checkout, output, builder, archive, toolchain, cargo_home):
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
            (self.repo / name).write_bytes(original + b"\n# new profile\n")
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


if __name__ == "__main__":
    unittest.main()
