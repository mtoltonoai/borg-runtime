# Preparing and verifying a v2 release artifact

`tooling/package_v2.py` packages the selected Borg executable from a full immutable
local Git commit. It installs nothing, acquires nothing and makes no provider,
deployment, merge or approval request. A successful package is a build result
linked to source; it does not establish applicable test acceptance or authorize
rollout. Final rollout still needs its separate artifact/target-bound checks and
human authorization.

Use the maintained packaging tool, not a tool chosen by the source checkout:

```sh
/usr/bin/python3 -I -S -B /home/dev-user/borg-runtime/tooling/package_v2.py build \
  --repo /absolute/local/repository \
  --commit FULL_IMMUTABLE_GIT_COMMIT \
  --output /absolute/fresh/output \
  --archive /home/dev-user/borg-runtime/.build/sqlite-amalgamation-3540000.zip \
  --toolchain-dir /home/dev-user/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin \
  --cargo-home /home/dev-user/.cargo
```

The output path must not exist, even as an empty directory. A failed attempt
retains its receipt, checkout and any logs/partial outputs; use a different fresh
output for retry. The tool does not delete or reuse failed evidence. Paths with
symlink ancestors, whitespace or build metacharacters are refused by the selected
recipe. The local repository can be an ordinary working repository or a bare
repository, but not a linked Git worktree. Dirty/untracked files do not enter the
package: only the exact committed tree is materialized.

Git SHA-1 and SHA-256 object formats are supported using the repository's actual
finite object format. Branches, tags, abbreviated hashes and conflicting
identities are refused. The tool independently hashes raw commit, reachable tree
and blob objects with the appropriate Git object header. Git supplies tree
linkage and parsing; no custom Git tree parser is introduced. Reads cannot use a
transport. Promisor/partial clones, object alternates, config includes, symlinks,
submodules, source Cargo configuration/toolchain overrides, build scripts,
additional package manifests, implicit extra binaries and tracked target/.build
outputs are refused. No checkout filters or source hooks are run.

The selected recipe pins the current Cargo manifest and lock, the installed
`build_v2.py` digest, Rust/Cargo/rustdoc and existing native tool hashes/versions,
and SQLite 3.54.0 archive/source identities. It preserves the exact existing
managed Cargo source-policy configuration at `/.cargo/config.toml`; it neither
disables that policy nor replaces the registry. Other Cargo configuration is
refused. A changed profile requires a deliberate recipe update and qualification,
not a caller-supplied alternative builder, feature set, target or dependency.
The actual running packager interpreter is checked through `/proc/self/exe`
against the selected system Python digest before any build. Recording the fixed
child-builder interpreter is therefore not a claim about a different producer.

The trusted helper is copied and checked before invocation with:

```text
build_v2.py --root CHECKOUT --build-dir FRESH_SQLITE --archive EXISTING_ARCHIVE
  --toolchain-dir SELECTED_TOOLCHAIN --offline --cargo build --release
```

Its existing locked/offline build and SQLite identity probe remain the actual
build boundary. The packager supplies an isolated HOME, explicit existing Cargo
cache and fresh target directory, a fixed locale/timezone and no inherited
compiler/linker overrides. It does not introduce path-remapping or new compiler
flags. Complete source inventory, recipe/archive/tool identities, selected
configuration and the release binary are checked for drift. The helper's Cargo
receipt must match the exact command, checkout, lock, manifest and SQLite receipt.

The resulting uncompressed `borg-runtime.tar` contains exactly:

* `bin/borg-runtime`, mode0555;
* `manifest.json`, mode0444.

The canonical manifest contains source commit/tree/object format and source file
inventory; selected profile; packager/build-helper/tool identities; SQLite source,
flags and static-library identity; and executable size/digest/target. Tar entries
have fixed order, zero timestamp/UID/GID and empty owner names, canonical USTAR
headers and deterministic zero padding. Execution timestamps, absolute working
paths and command logs remain in the external `receipt.json` and build receipts.
SQLite's upstream source timestamp is part of its fixed release identity.

Verify against an independently retained expected digest, preferably also the
expected source commit:

```sh
/usr/bin/python3 -I -S -B /home/dev-user/borg-runtime/tooling/package_v2.py verify \
  /absolute/output/borg-runtime.tar \
  --expect-sha256 EXPECTED_PACKAGE_SHA256 \
  --expect-commit EXPECTED_FULL_GIT_COMMIT
```

Verification requires a bounded regular file before hashing. It reads fixed tar
headers and bounded member bytes without extracting anything. Traversal/link
headers, extra members, noncanonical metadata/padding/JSON, duplicate JSON keys,
truncation, mismatched source/profile/recipe/tool/SQLite identities and binary
tampering are refused. Internal checksums do not establish identity by themselves:
a self-consistent replacement must still match the caller's separately supplied
outer digest and optional commit. Neither those supplied values nor same-UID
directories are protected evidence, signatures or approval authority.

This is the x86_64 GNU/Linux application executable, not a self-contained cluster
image. Its selected SQLite library is linked statically, but the executable can
depend on compatible installed GNU/Linux runtime libraries. Worker use also
requires the separately configured Codex executable, Git and SSH as applicable,
repository access, provider configuration and credentials. None are packaged,
provisioned or granted here. The package does not prove complete build hermeticity
or independently requalify every cached dependency/sysroot file; the exact lock
and selected existing toolchain/build evidence retain their own scope.

The Python unit tests use actual private Git repositories and a mocked expensive
build/tool/archive boundary with an ELF-shaped fixture. They prove packaging/refusal and
verification mechanics, not that fixture's executability. Actual two fresh
release builds, byte-for-byte reproduction and a controller launched from the
verified packaged binary are separate retained acceptance. A differing binary
must be investigated before changing path/compiler flags or claiming
reproducibility. No unit-only result is a deployable-runtime or rollout claim.

## Recorded Native Release Qualification

The [native release qualification receipt](../artifacts/v2/build-60156282/native-release-clywwq3s/completion.json) packages its recorded source without a production code or dependency change in that batch. A private qualification repository contains62 byte-identical source/test inputs at commit92df18896e121e72f8296f01bb5578ecf380aa22. This is a recorded fixture identity, not a commit or publication in the maintained runtime repository. The ordinary packager performed one fresh locked/offline release build in80.401seconds and its maintained verifier returned exit0. Bitwise reproduction was not rerun in that batch; earlier reproducibility receipts retain their own source identity.

Package digest:025b6ec9e429f78a38d212a75c2578907ca1f8c982bce0c94ebfeab8df402194, size23,040,000bytes. The extracted executable digest is33553dd7a2f0d89ded7dfd74861f2158b37b4e930ba1d98a5bfdf507ebdfa7f2. Its bytes match both the built image and the verified package member. The [live release-profile browser test](../artifacts/v2/build-60156282/native-release-clywwq3s/live-native-release-01/receipt.json) used two actual gpt-6.1-sol requests with the read-only get_current_task tool, accepted the exact observed result, and recovered that result after controller restart. No Codex, synthetic model peer, deployment or protected approval was substituted. All three processes exited0, the listener closed, and no owned helper records or processes remained.

This is the first real-provider qualification of this source's packaged release executable. It does not establish all release error paths, four-host workload capacity, live coding/submission, repository/CI protections or final rollout authority. The existing normal-test, Duvet and planning evidence is reused only for its unchanged source inputs and stated scope.

## Recorded Publication Recovery Qualification

The [recovery correction receipt](../artifacts/v2/build-60156282/acceptance-order-elh7sdet/completion.json) binds a successor package to the narrow worker failure-classification fix. Its private fixture commit is `7a81ee027f6e10f794e8a2887d30c6b771cb348c`, with 67 byte-identical source/test/fixture files. The maintained offline packager and verifier passed with the unchanged dependency profile. Package SHA256 is `fb1ea13b29a4b45e7edf490800c5cbf26c34d7307ffd0eb1f6a2a98d1d7f790b`; executable SHA256 is `2ff1ecc18deb862042e5341875acc9ed9eb64cd831b79700c40be5c1428945f1`.

The existing recovery test driver used `BORG_RECOVERY_BINARY` to launch the verified release image for every actual controller and worker. The [actual run](../artifacts/v2/build-60156282/acceptance-order-elh7sdet/release-git-recovery-01/receipt.json) passed in 3.850 seconds: failed local Git publication, restart, exact original-ref readback and not-applied settlement, then a successful new attempt from the acknowledged checkpoint. The explicit readback helper is linked current test-library code, and the executor is a controlled fixture. This is not a lost Git acknowledgment test, new live-model run, double-build reproducibility claim or protected release approval. Earlier model and reproducibility receipts remain historical evidence for their recorded inputs.

## Current Worker Resource Verification Package

The [worker-envelope implementation receipt](../artifacts/v2/build-60156282/worker-envelope-implementation-e42rg9kw/completion.json) binds the optional operator profile and its startup/admission checks to a new offline package. The private qualification commit is `16118c760f5ed2bcfdeb50382b627b3453f1feb4`; it contains 72 exact source/test/fixture files. Package SHA256 is `25811b0822d57480d71abb2fc3bccda30f56864c777f986fdf02424d1fd14b39`; executable SHA256 is `07cfd6ba269ad2abba9a02ff8599ed85af0482d05a5867a17c0f8bbfb53fb031`. The maintained package verifier passed against those independently retained subjects.

The [actual packaged CLI checks](../artifacts/v2/build-60156282/worker-envelope-implementation-e42rg9kw/release-profile-refusal-01/receipt.json) refuse malformed profiles, an ordinary-directory cgroup impostor and this host's unlimited cgroup controls. All return the expected failure before a controller connection, and all processes are reaped. This qualifies negative startup behavior in the release build. It does not establish a positive finite host envelope, descendant inheritance, exhaustion recovery, four-host capacity or rollout authority. No provider call or host control change was performed. Earlier live-model and recovery runs retain their own package/source identities.

## Checkpoint Recovery Release Package

The [checkpoint recovery package](../artifacts/v2/build-60156282/checkpoint-crash-recovery-_cw2bqua/completion.json) contains74 exact source/test/fixture files at private qualification commit `c07788104e5aca596ef5d03169061b09f787bb62`. The maintained offline builder and verifier passed. Package SHA256 is `f3602f68de6a7970e6fdbb1ce6f1cfad22cb3a266cd79d2d88b42a9ccda6754a`; executable SHA256 is `d2b543f98cdde4ccf3b7444b641c3bcd48738cb131fca7831db02aa97e23ef8a`. This is a source-bound private fixture identity, not a maintained-runtime Git commit.

The verified package passes the actual existing Git failure/restart/readback test using the current retained test driver, and refuses a synthetic format5 worker root through an actual release-CLI probe without changing its bytes, mode, inode or modification time. It does not execute the debug-only crash cuts or establish remote-provider/cluster capacity. Earlier packages and live-model results retain their original subjects. Deployment state is tracked separately from this release qualification.
