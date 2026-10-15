# Preparing a claimed release candidate

`tooling/prepare_v2_release.py` joins the existing package verifier to the ordinary
controller preparation API. It creates no resource, deployment target, permission
or approval mechanism. It does not merge, publish or deploy. The controller still
performs its current-state and idempotency checks in its own transaction; the
tool's package checks and HTTP calls run outside that transaction.

Prepare from an actual succeeded coding task and an independently supplied
package digest/immutable source commit:

```sh
/usr/bin/python3 -I -S -B tooling/prepare_v2_release.py prepare \
  --controller http://127.0.0.1:8100 \
  --task-id TASK_ID --operation-id STABLE_PREPARATION_OPERATION \
  --package /absolute/path/borg-runtime.tar \
  --expect-sha256 EXPECTED_PACKAGE_SHA256 --expect-commit EXPECTED_GIT_COMMIT \
  --output /absolute/fresh/preparation-receipt
```

The existing `package_v2.verify` is loaded only after its maintained source digest
matches the selected verifier. Verification must pass before the first network
call. The tool fetches the current task and requires coding/succeeded, its exact
accepted stopped/non-uncertain attempt, equal accepted/selected checkpoints and
the package's exact source commit. It sends the complete checkpoint and observed
task revision to `PrepareRelease`. A concurrent task change remains the server's
conditional-write responsibility; the tool never refreshes a stale mutation into
a different request automatically.

`ReleaseArtifact` is constructed from the verified manifest and actual package:
format, package digest/size, canonical manifest digest, source commit/tree/object
format, binary digest/size and compiler target. The compiler target is not a
deployment target. The stable operation ID comes from the caller. There are no
acceptance, protected-check, repository-approval or rollout fields in the request.

Before mutation, the tool syncs the exact canonical request and accepted task
snapshot into a fresh receipt directory. It retains source/tool pins, original
external digests, request/response bodies and hashes, bounded call outcomes and
the returned candidate. An existing output directory is never overwritten. These
are ordinary same-UID records, not tamper-proof or independently protected proof.

Only literal loopback HTTP origins are accepted, with no credentials, hostname
lookup, alternate path, query or fragment. `http.client` connects directly and
does not honor proxy variables, redirect responses, cookies or authentication
configuration. The existing controller emits bounded JSON with Content-Length;
chunked/compressed/unknown-length replies are refused. Requests/replies are
limited to 1 MiB, returned header data to 16 KiB, connection establishment to ten
seconds and connected HTTP exchange to twenty-five seconds. Loopback access
remains the trusted-host/SSH-forwarding deployment assumption, not caller
authentication or hostile-user isolation supplied by this tool.

A lost or malformed mutation reply is uncertain, even if the server may already
have committed. There is no automatic resend. Reconcile the original operation:

```sh
/usr/bin/python3 -I -S -B tooling/prepare_v2_release.py reconcile \
  --controller http://127.0.0.1:8100 \
  --original-receipt /absolute/fresh/preparation-receipt \
  --expect-sha256 EXPECTED_PACKAGE_SHA256 --expect-commit EXPECTED_GIT_COMMIT \
  --output /absolute/fresh/lookup-receipt
```

Reconciliation verifies the original request, accepted-task snapshot and package
again, then sends only `GetOperation`. A settled record must carry the exact
original `PrepareRelease` operation and matching candidate. A missing record is
reported as `not_observed`, not as proof that an in-flight original can never
commit. It never causes an automatic mutation.

An explicit `retry` subcommand accepts the same arguments as `reconcile` and
resends the identical original request, including operation ID, task revision,
checkpoint and artifact. It performs no new task lookup and cannot silently
select a different source/revision. The controller's exact-input idempotency
semantics decide that retry. Keep the original receipt/package available through
unknown outcomes; do not create a new operation just to resolve a lost reply.

Successful output is only an ordinary candidate with artifact `claimed` and
repository submission, protected checks and human rollout all `unconfigured`.
Responses that assert stronger statuses, different source/artifact/attempt
bindings or unrecognized shapes are refused, leaving a sent mutation uncertain.
Package consistency is not independent checker evidence. The protected release
integration and actual artifact/target-bound human rollout remain separate and
are not implemented by this adapter.

Exit0 means a matching claimed candidate was returned or reconciled. Exit2 means
an explicit non-success outcome such as refusal, pending, uncertain or not
observed. Exit1 means local/transport/response validation failed; inspect the
fresh receipt, whose `mutation_attempted` and status preserve uncertainty. No
exit code means permission to deploy.

The Python tests use a real loopback HTTP peer and the actual package verifier
over an ELF-shaped unit fixture. They exercise binding, bad packages before HTTP,
malicious approval/status claims, lost replies, exact retry/lookup, mismatched
originals, fresh receipt preservation and response bounds. They do not compile
Rust or prove a real controller accepted the fixture. The real packaged binary
plus controller restart smoke has its own retained execution evidence.

## Current Local Self-Source Qualification

The [worker-owned delivery receipt](../artifacts/v2/build-60156282/self-delivery-u_45m87n/completion.json) exercises this adapter with an accepted result actually produced by the maintained worker. A controlled stdio executor edits a private copy of the real Borg source; real Cargo build/product checks and Git checkpoint publication precede acceptance. The exact accepted commit is packaged twice with identical bytes, served by the delivered executable, prepared and reconciled across restart. The private fixture commits do not establish a canonical repository remote, protected CI or rollout authority. The selected statuses remain claimed/unconfigured.
