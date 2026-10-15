# Contract: The Signed Assertion

> **CONTRACT.** This document pins the statement of identity the platform presents to every service it
> calls on a session's behalf: tool servers, hook destinations, the credential broker, and, where they exist,
> definition sources and the workspace service. These parties cannot read the durable store, so they take
> who is calling and what the call may do from the assertion. It is versioned and changed only by the coordinated act the constitution's Protected Guarantees describe. Its requirements realize
> [Protected Guarantee "Identity Comes From Authentication"](../../constitution.md) and trace to
> [overview section 7](../overview.md) and [overview section 5](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The signature scheme, the key custody, and the lifetime are declared defaults, not requirements.

## Purpose And Scope

A service the platform calls must scope, dedupe, and fence what it does to the session that called it,
without trusting an argument the model wrote. The assertion is a short-lived, signed, call-bound
statement derived from the session's grants at the moment of the call. It is a capability-style token used only at trust boundaries: it is never the authoritative record, which is the grant data in the durable store, and it is never stored by the platform beyond the call it was issued for.

## Claims

### An Assertion Identifies The Call It Is Bound To

An assertion MUST carry the call id of the one call it was issued for.

An assertion MUST carry the audience that identifies the one service it is addressed to.

An assertion MUST carry a digest of the request it is bound to: the method, the target as sent, and the body's digest.

An assertion for a streamed body MUST carry the digest of the body that the caller declared before the body was sent.

A call the platform makes on its own behalf, with no session, MUST carry a call id that can never equal a session's call id and is unique per calling actor within the receipt retention.

### An Assertion Identifies The Actor

An assertion MUST carry the tenant the session belongs to.

An assertion MUST carry the session's principal.

An assertion MUST carry the session's on-behalf-of chain in order from the outermost delegator to the
session.

An assertion MUST carry the session id.

An assertion MUST carry the session's incarnation as a claim separate from the session id.

An assertion issued for a fork MUST identify the fork rather than its parent.

An assertion for a sub-session or a fork MAY identify the parent session.

Each entry of the chain MUST carry its relation, delegated or attested, and its attester.

A chain MUST have at most the configured number of entries.

An assertion presented by a node that forwards another caller's call MUST carry that caller's verified principal and chain unchanged in a forwarded claim.

### An Assertion Carries What The Call May Do

An assertion MUST carry the scopes that apply to the call, derived from the session's grants at the moment
of issue.

An assertion MUST carry the session's lease generation.

An assertion MUST carry an expiry no later than the configured assertion lifetime after issue.

An assertion MUST NOT carry a credential, a secret, or transcript content.

An assertion MUST identify the grant from which its scopes were derived, so that the service's audit record identifies the grant.

An assertion MUST carry its issue time.

An assertion MUST carry its issuer.

## Signature And Keys

### The Signature Is Asymmetric

An assertion MUST be signed with an asymmetric scheme, so that a verifier holding the verification key cannot issue an assertion.

Verifying an assertion MUST NOT require a call to the key service.

An assertion MUST carry its claims only as the signed payload, never as a second structured copy beside the signature.

An assertion MUST have exactly one encoding.

### Keys Are Published And Rotated Without A Gap

The platform MUST publish the set of verification keys at a location every verifier can fetch.

A rotation MUST keep the previous signing key verifiable until the last assertion signed under it has
expired.

A rotation MUST NOT fail a call that was in flight when it began.

A signing key MUST be published to verifiers before the first assertion signed under it.

A signing key MUST sign nothing until the configured publication lead after its publication has passed.

A verifier MUST read the key set only through a path that authenticates the verifier, never through an unauthenticated route.

Each key id MUST carry the id of the lineage that signs under it, so that a verifier can bound which lineage's keys it trusts.

A verifier MUST bound how often it fetches a key lineage it has not seen.

A verifier MUST refresh its cached key set within the configured age and drop any key the set no longer lists.

A verifier that holds no verification key MUST refuse to start.

A refresh that fails or reads no key MUST keep the key set read last while that set is within the configured age.

A failed refresh MUST be published as an event.

A refresh MUST replace the key set only when every configured key source was read.

## Verification

### A Verifier Refuses What Does Not Match

A verifier MUST refuse an assertion whose expiry has passed.

A verifier MUST refuse an assertion whose audience is not the verifier.

A verifier MUST refuse an assertion presented for a call id other than the one it carries.

A verifier MUST refuse an assertion signed by a key outside the published set.

A verifier MUST refuse an assertion that carries a tenant other than the one the call is addressed to.

A verifier that cannot fetch the key set MUST fail closed.

A verifier MUST check the signature over the bytes it received before it decodes any claim.

A verifier MUST act only on claims decoded from the signed payload.

A verifier MUST accept exactly one signature scheme.

A verifier MUST refuse an assertion that declares no signature, another signature scheme, or another token type.

A verifier MUST refuse an assertion in any encoding other than the one the contract declares.

A verifier MUST refuse an assertion whose issue time, less the configured clock-skew allowance, is later than the verifier's current time.

A verifier MUST refuse an assertion whose lifetime exceeds the configured maximum.

A verifier MUST refuse a call whose presented lease generation differs from the one the assertion carries.

A verifier MUST refuse a chain longer than the configured bound.

A verifier MUST trust a forwarded claim only from a node signing under the lineage of the partition owner the call is addressed to.

A verifier of a streamed call MUST run every check other than the body comparison before it reads the body.

A verifier of a streamed call MUST compare the streamed bytes with the declared digest at stream end.

A verifier of a streamed call MUST commit nothing on a digest mismatch.

Every refusal MUST be published as an event carrying one reason class from a closed set and never the token.

A signing lineage a tenant has never seen MUST raise exactly one event across every node and restart.

### The Assertion Scopes The Call

A service MUST verify every assertion it receives itself rather than accept a verification performed by another component on the request's path.

A service MUST scope everything a call does to the assertion's tenant, session, principal chain, and
scopes before the service's own authorization runs.

A service MUST take the acting identity of a call from the assertion rather than from an argument or a
header the caller controls.

A service MUST refuse a call that has no receipt and whose lease generation is older than the highest it has recorded for that session.

A refusal for a stale lease generation MUST be distinct from an authorization failure.

A refusal for a missing scope MUST be distinct from an authentication failure and from a fence refusal.

A service MUST decide, record, and attribute a forwarded call on the forwarded principal and chain rather than on the identity of the node that forwarded it.

The identity a forwarding node presents MUST hold only the scope of the kind of hop it makes.

A reconcile of the caller's own call MUST require no scope beyond a valid assertion for the session.

A close of the caller's own call MUST require no scope beyond a valid assertion for the session.

An assertion for a close or a reconcile of a call that an ended incarnation left open MUST carry an ended claim.

A service MUST accept an assertion that carries an ended claim only for a close or a reconcile of a call the ended incarnation left open.

## Where An Assertion Is Presented

### Every Outbound Call Carries One

Every tool call the platform makes MUST carry an assertion addressed to the tool server.

Where a definition controller exists, every read it makes MUST carry an assertion that identifies the binding's tenant, or, where the source cannot verify an assertion, a credential the binding identifies by reference.

Every credential lease the platform requests MUST carry an assertion addressed to the credential broker.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed verifiers, or else carry an explicit
version increment.

A change to this contract that is not additive with respect to deployed verifiers MUST carry a stated
migration path.

Each field's encoding MUST be declared by this contract rather than chosen per value.

An integer field that may exceed the declared exact-integer bound MUST be carried as a decimal string.
