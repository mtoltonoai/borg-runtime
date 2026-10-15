# Local Mixed-Load Check

This check exercises the real controller, worker, HTTP transport, SQLite state,
Git checkpoints and Rust compiler on one host. A local HTTP peer substitutes for
model inference; a controlled stdio peer substitutes for the supplied Codex
executor. Only a synthetic credential reaches those peers. This is an owned
component test, not live-provider, Codex, four-host or production qualification.

## Run

From the runtime repository, use the selected build environment and a fresh,
absolute evidence directory:

```sh
source .build/env.sh
BORG_LOAD_NATIVE=32 BORG_LOAD_CODING=2 BORG_LOAD_DIR=/absolute/fresh/case \
  cargo test --offline --locked --test load -- --ignored --nocapture
```

The closed native counts are 32, 64, 128 and 256. Coding capacity is 2, 4 or 8.
The directory must not already exist. The ignored test runs only when explicitly
requested. It creates private synthetic Git repositories; it does not change the
maintained repository's history or contact a remote Git service.

## What Is Exercised

The peer receives and holds every complete native request before coding work
starts. All native responses remain held while coding tasks edit a tiny Rust
fixture, compile its tests, run them, compile a binary and publish actual Git
checkpoints. One extra coding task must remain queued without an attempt until a
completed task releases capacity. Every result goes through the real worker and
controller; the test does not inject successful result reports.

After native responses are released, every task must finish successfully with a
stopped, non-uncertain attempt. Repeating the original submission operations and
restarting the controller and worker must preserve the exact accepted tasks and
checkpoints without another native request or coding turn.

The scenario has a 20-minute overall bound and five-minute phase bounds. Failure
retains task, outbox and progress evidence. Cleanup is separately bounded and
records owned process closure and registry reconciliation.

## Interpret The Evidence

`result.json`, successful Cargo exit, unchanged before/after input hashes and
successful `cleanup.json` are required together. The intermediate `summary.json`
alone is not a pass. Input pins include the runtime and load-test executables,
compiler, fixture scripts, Cargo inputs and runtime/test source.

Coding capacity measures simultaneously held controlled turns. Sampled compiler
overlap is a separate measurement; eight active coding tasks do not establish
eight simultaneous compiler children. The tiny Rust fixture is not representative
of every repository build. `latency_to_final_observation_ms` is submission age at
the final cohort observation, not per-task completion latency or a service SLO.

The check does not establish real provider quotas, representative inference,
supplied Codex behavior, CPU/memory/disk enforcement, sustained mixed-load
capacity, cancellation latency at scale, storage-loss recovery or four-host
operation. Successful local concurrency is one input to those later checks.

The [current run receipt](../artifacts/v2/build-60156282/task-recovery-124tfa07/completion.json)
records the 256/8 regression after the recovery changes, with exact source and
result bindings. The [earlier four-step receipt](../artifacts/v2/build-60156282/local-load-4bjl85jp/completion.json)
retains its own earlier executable and source.
