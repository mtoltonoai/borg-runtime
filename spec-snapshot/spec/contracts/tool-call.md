# Contract: The Tool Call

> **CONTRACT.** This document pins what passes between the platform and a tool server: the spec and
> semantics a server declares, the call the platform makes, and the guards a server honors so that a
> call is a durable operation whoever makes it. It is honored across releases by the platform, by the
> services the platform's operators run, and, where they honor an idempotency key, by remote targets. Its
> requirements realize [Platform Principle P6](../../constitution.md) and trace to
> [overview section 5](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The tool protocol and the result bounds are declared defaults.

## Purpose And Scope

Tools are services. A tool's spec comes from its server, its semantics default to the server's
annotations, and a call carries the identity of its caller and a key that makes it safe to redo. This
contract fixes those shapes; how the platform resolves, gates, and commits a call is the tools
capability.

## The Spec

### The Server Declares The Spec

A tool's spec MUST consist of a name, a description, and an input schema, declared by the server that
provides it.

A server MUST list its tools on request.

A server MUST announce a change to its list of tools.

The platform MUST treat a spec as untrusted input, since its description enters the model's context.

A tool's spec MAY declare the scope a caller holds to call it.

A tool's spec MAY declare the operational limits a caller respects, such as what the tool cannot do, its expected duration, and its rate bound.

A tool's spec MAY declare the inline bound of its results, which a definition can only lower.

### The Server Declares The Semantics

A tool's semantics MUST include whether it is idempotent, whether it has side effects, its timeout, its
retry policy, and whether it is cancellable.

A tool MAY declare a reconcile binding that reports whether an in-flight effect took effect.

A tool's semantics MUST default to the server's declared annotations.

A definition MUST only make a tool's semantics more conservative than the server declared.

A tool that is not declared cancellable MUST be treated as not cancellable.

A tool's semantics MUST state whether a failed attempt may be removed from the model's view once a retry succeeds.

### The Spec Declares Call Types And Credentials

A spec MAY declare call-type tags that hook policies are written against.

A spec MUST request any credential it needs by credential reference rather than by value.

## The Call

### Every Call Carries Its Identity

Every call MUST carry a call id.

Every call MUST carry the caller's assertion, addressed to the server.

Every call MUST carry the caller's lease generation.

Every call MUST carry the caller's distributed trace context.

A tool server the operators run MUST continue the caller's trace under a span of its own and carry its trace id on every event it publishes for the call.

### The Call Id Is The Dedupe Key End To End

The call id MUST be given to the executor and to any target system that accepts an idempotency key.

A re-issued side-effecting call that follows an unknown outcome MUST reuse the original call id.

A call a service makes on a session's behalf from inside a call MUST carry a call id derived from the outer call's id.

## Server Guards

### A Service Dedupes By Call Id

A service the platform's operators run MUST keep a receipt keyed by tenant, session id, and call id.

A service that receives a call id it holds a receipt for under the same request digest MUST return the recorded outcome without running the call again.

A receipt for a call whose effect takes place outside the service's store MUST be admitted before the effect and settled after it.

A receipt for a call whose effect takes place inside the service's store MUST commit with the effect.

A service MUST establish the caller of every request, the handshake included, before any method runs.

A service MUST refuse a call whose caller lacks the tool's declared scope with a refusal that identifies the scope and is distinct from an authentication failure.

A service MUST keep every receipt for at least the configured call deadline upper bound plus the configured outcome-unknown window after it writes the receipt, measured on its own clock.

A service MUST refuse to start with a receipt retention below that sum.

An operation MUST decide every refusal before its first write.

A refusal decided before admission MUST record nothing and leave the fence unchanged.

A refusal the effect itself returns after admission MUST be settled as the call's outcome and replayed on retry.

A receipt MUST be settled at most once.

A call whose id has a receipt under a different request digest MUST be refused with a final error and receive nothing from the receipt.

A service the platform's operators run MUST answer a close for a call id with the call's final state: closed, in flight, or settled.

A close MUST be idempotent.

A service that receives a close for a call id it holds no receipt for MUST write a closed receipt, so that a later admission of that call id is refused.

A close of a call in flight MUST end the call.

A close or a reconcile MUST compare its target call id with the assertion's call claim before it reads or writes anything.

### A Service Answers Reconcile

A service with a reconcile binding MUST answer, for a call id, one of never admitted, still running, interrupted, settled, or closed, with the outcome where there is one.

A repeat of a call and a reconcile of it MUST NOT disagree.

### A Service Fences A Superseded Owner

A service the platform's operators run MUST refuse a call that has no receipt and whose lease generation is older than the highest it has recorded for that session.

The refusal of a superseded owner MUST be distinct from any other error.

A remote target outside the operators' control MAY be guarded only by the call id.

A call that has a receipt MUST be answered from the receipt whatever its lease generation.

A service MUST fence a close by lease generation as it fences an admission.

A service MUST NOT fence a reconcile.

A service that runs as more than one instance MUST fence by the highest lease generation any of its instances has recorded for the session.

A service MUST apply the fence check to a request it fences but records no receipt for.

## Results

### A Result Is Bounded In The Transcript

A result MUST reach the transcript as a bounded view whose bounds come from the tool spec.

The full output of a call MUST be stored once, so that a session can page through it.

A result MAY carry image, audio, and binary content, each typed by its media type and bounded by the configured size.

A result's structured content MUST be checked against the tool's output schema where the server declares one.

A server MAY return a result over its declared inline bound as a reference with a bounded preview and the result's shape.

A reference returned for a result MUST also appear in the result's plain text.

A result part the platform does not model MUST reach the model as a textual reference carrying its address, name, and size rather than be dropped.

A tool server MAY deliver a result larger than the payload bound by staging it through the API as an attachment that its result event refers to.

A tool server's right to stage an attachment MUST be bound to one open dispatch to that server, identified by the session and the call id in fields the server signs.

The platform MUST check that the dispatch is open to that server before it accepts any byte of the staged output.

The platform's answer to that check MUST reveal only whether the call is open to that server.

A staged output MUST be usable only by the result of the dispatch that admitted it.

A tool server MUST be allowed to send into a session only if that session has dispatched to the server, on the server's own topic and at a bounded priority.

### A Tool Error Is A Result, Not A Failure

An error a tool returns MUST reach the model as an error result rather than as a failed turn.

A blocked tool result MUST reach the model as an error result that states the call completed and its result was withheld, with the reason.

A deferred tool call MUST return an immediate result to the model that carries the decision id and the deadline.

A re-issue of a held side-effecting call with the same tool and the same arguments MUST return the same pending result and decision id rather than a second defer.

A deferred call that is dispatched MUST run under its original call id.

### An Interrupted Side Effect Reports Unknown

A side-effecting call interrupted before its outcome was recorded MUST return an unknown outcome that
states what was sent and when.

### A Long Call May Finish Later

A call MAY return before its work completes and deliver its result later as an inbox event.

A call's horizon MUST be its deadline plus the configured outcome-unknown window.

A late result MUST identify the call id of the dispatch it answers.

A late result MUST be retried under one stable id.

Retries of a late result MUST end when the platform acknowledges it.

Delivery of a late result MUST end at the first of the end of the call's session, the platform's refusal of the server's sends, or the call's horizon.

A final result that arrives after the horizon MUST be dropped with an audit event.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed tool servers, or else carry an
explicit version increment.

A change to this contract that is not additive with respect to deployed tool servers MUST carry a stated
migration path.

A party MUST carry a content part it does not recognize through unchanged rather than reject the message.
