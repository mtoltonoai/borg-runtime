# Building v2 from a fresh checkout

`tooling/build_v2.py` prepares the selected SQLite 3.54.0 static library from the
pinned official source archive. It uses existing system Python, GCC 13.3.0,
Binutils 2.42, pkgconf 1.8.1 and the exact selected Rust/Cargo 1.99 binaries. It
installs nothing and changes no global configuration. The selected inputs are
specified in the separate Borg specification repository's
`spec/impl/v2/dependency-profile.md`; the build tool carries their exact pins and
does not require a sibling specification checkout. Resolve any required source
instructions as described in the runtime repository's `AGENTS.md`.

The runtime checkout must contain its ordinary `Cargo.lock`. Use the existing
managed Cargo registry and its normal configured access; this tool does not
replace the registry or create credentials. The original `spec-lock.json` and
copied `spec-snapshot/` are not touched.

From the runtime checkout, build the complete application:

```sh
python3 -B tooling/build_v2.py --cargo build --release
```

The default archive location is `.build/sqlite-amalgamation-3540000.zip`. A
matching local archive is reused. Otherwise the tool downloads the exact official
HTTPS archive and verifies its pinned SHA3-256 before extracting anything. It
extracts only `sqlite3.c`, `sqlite3.h` and `sqlite3ext.h`; it does not extract or
build the supplied command-line shell. There is no system-SQLite fallback.

For an offline machine with the selected archive and Cargo dependencies already
available:

```sh
python3 -B tooling/build_v2.py \
  --archive /absolute/path/sqlite-amalgamation-3540000.zip \
  --build-dir .build/sqlite-repro \
  --offline --cargo build --release
```

Omit `--cargo` to prepare/check only SQLite, without compiling the Rust runtime.
`--cargo check` and `--cargo test` are also supported, always with `--locked`.
`--offline` applies to both archive acquisition and Cargo. This tool does not
silently fetch missing dependencies in offline mode.

The output contains `env.sh`, `pkgconfig/sqlite3.pc`, the static library and a
`build-receipt.json`. The receipt binds the archive, three extracted source files,
actual tool binaries/versions, exact compiler flags, command logs and output
hashes. A small C probe links the private archive by its explicit pathname and
checks the exact SQLite version/source identity, threading and selected compile
options. These checks establish the prepared C library, not the eventual Rust
binary's link or the application's durability guarantees.

To run another ordinary Cargo command after preparing the library:

```sh
. .build/sqlite-repro/env.sh
cargo test --locked --offline --test controller
```

Source the environment only in that command shell. It supplies the private
SQLite library/include/pkg-config paths and selected Rust compiler; it does not
modify a login profile, service or Cargo registry. The crate build script's
`bundled_bindings` feature refers to its pregenerated Rust bindings, not to a
bundled SQLite engine. The actual Rust link, used APIs and transaction/restart
behavior still need their tests.

For the complete local regression profile without the separate installed-Codex
probe or opt-in live journeys:

```sh
cargo test --locked --offline --no-fail-fast -- \
  --test-threads=1 --skip actual_pinned_codex_handshake_without_a_model_turn
```

The native helper-lifecycle tests deliberately hold the same process-wide pool
of four validator slots while exercising short execution deadlines. Running
unrelated test cases concurrently can expire a fixture before its required live
helper has started. Serial libtest scheduling preserves those deadlines and the
live-child/cleanup assertions. It does not serialize the application or the
concurrent actors inside each test. Individual targets without this shared
fixture constraint can run with libtest's ordinary parallel scheduling. The
[explicit mixed-load check](v2-local-load.md) separately exercises application
concurrency. No compiler, runtime limit or global configuration is changed by
the regression command.

Build directories and archive paths must contain no whitespace, symlinks or
shell/pkg-config metacharacters. Existing nonempty output directories are accepted
only when their tool-created receipt, build profile and output hashes match.
The earlier manually prepared `.build/sqlite` may have no such receipt: use a
fresh directory such as `.build/sqlite-repro` instead of overwriting it. Failed
preparations retain their logs and failure receipt; rerun into a fresh directory.
Known compiler/linker override environment variables cause refusal rather than
silently changing the profile. Another installed location for the exact selected
Rust/Cargo binaries can be passed with `--toolchain-dir`. By default the tool
uses `RUSTUP_HOME/toolchains/stable-x86_64-unknown-linux-gnu/bin` when
`RUSTUP_HOME` is set, otherwise the same path under `~/.rustup`. This preserves
the selected toolchain when a worker has a fresh private `HOME`; the worker
already passes its existing `RUSTUP_HOME`. Exact binary hash checks still apply.

The optional Cargo command gets its own log and receipt in a fresh `cargo-*`
subdirectory. Source changes, provider requests, browser acceptance and rollout
are separate operations. A local successful build does not authorize deployment,
prove the four-host capacity target, or replace the protected checker and final
human rollout approval required by the implementation specification.

## Local qualification test-image lifetime

On the coordinator host, the selected local packet is
`artifacts/v2/parallel-integration-o1p7h0uc/release/test-image-retention-lifecycle-01/ordinary-workflow-successor.json`.
Its pinned `ordinary_workflow.py` delegates compilation and tests to the existing
runner with the same profile and bounds. The selected root adoption receipt
binds the standing policy; this local artifact path is not a dependency for
workers or other checkouts.

Use the owned compiler test output directly instead of making an always-retained
copy. After tests and any required review finish, invoke the workflow's mandatory
`finish` command. It automatically retires eligible test payloads under the
standing policy, with no new per-payload approval. Keep source/build identities,
hashes, logs, results and retirement receipts. Historical evidence checks accept
explicit retirement records for released test payloads; execution still requires
the exact live image.

Keep a payload only for a named active consumer or pending candidate, deployed
or rollback use, with an owner and reference that can release the hold. Runtime
and package images are outside this test-payload finalizer. Reconstructible
compiler caches remain a separate bounded local resource policy. This workflow
does not change worker product cleanup, runtime features or package policy.

## Candidate Cargo source and selected dependency policy

New packages keep the `borg-v2-release/1` container and its two members, and write
package profile revision 2. The profile embeds the exact UTF-8 `Cargo.toml` and
`Cargo.lock` strings, their actual raw SHA-256 hashes, and dependency policy
revision 1. Admission parses the complete TOML values with the standard library
`tomllib`, permits only dictionaries, lists, strings, booleans and integers, and
compares the canonical JSON digests to the trusted selected policy. Canonical
JSON sorts keys, uses compact separators and ASCII escapes, rejects nonfinite
numbers, and ends with one newline. Array order and value types remain significant.

Comments, whitespace, key ordering and equivalent TOML literals can change the
candidate bytes while keeping this policy. A dependency, feature, checksum,
source, build setting or any other parsed-value change refuses before the trusted
builder runs. Existing source/configuration restrictions, raw source bounds,
offline inputs and build/tool pins still apply. The complete embedded manifest
has the existing size cap; admission reserves a bounded envelope for trusted
build metadata before execution and enforces that envelope on the returned metadata.

The source inventory binds each captured Cargo file's raw hash, byte length and
Git blob under the recorded object format. The builder receipt must report those
same candidate raw hashes. Package-only verification recomputes the complete
policy, raw hashes, sizes and blob identities from the embedded strings without
extracting files, reading a source checkout, executing candidate code or fetching
dependencies. A candidate's claimed semantic digest is insufficient.

Protected preparation captures Cargo bytes through immutable Git reads and uses
the same validator. The approved cache manifest, membership and data pins stay
unchanged: its manifest/lock hashes identify the selected cache baseline. The
projection receipt's `source_manifest_sha256` and `source_lock_sha256` identify
the actual candidate; `cache_baseline_manifest_sha256` and
`cache_baseline_lock_sha256` separately identify the approved baseline. Each job
gets a fresh independent mutable copy with all existing protection, alias,
membership and post-copy base checks. A missing or mismatched approved cache is
an explicit failure. There is no offline-build network fallback.

The new reader also has one explicit exact branch for the immediately preceding
package profile. Other historical profiles remain refused. Existing legacy
packages and receipts are unchanged; reproducing their exact bytes requires
their original pinned recipe. A revision-2 build is a distinct artifact and is
not claimed byte-identical to a legacy package. Local fixture ELF and mocked-build
tests establish tooling behavior only; actual release execution, protected checks,
independent review and rollout remain separate evidence.
