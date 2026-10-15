# Contract: The Decision Hook

> **CONTRACT.** This document pins the interface between the platform and an external decider: a service
> the platform asks, at one of its hook points, for a decision it then enforces. Stop admission, a decision service
> called over the network, and, where the human-approvals outcome is adopted, a human approval routed
> through an adapter are all instances.
> It is honored across releases by the platform and by every adapter that answers it. Its requirements
> realize [Core Principle VII](../../constitution.md) and [Protected Guarantee "Agents Never
> Approve"](../../constitution.md) and trace to [overview section 6](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The deadline bounds and the request size bound are declared defaults.

## Purpose And Scope

An external decider is declared by a definition, never by a model, and points at a destination the
tenant registered. Unlike an event hook, it decides, so it has a deadline and a failure outcome. This
contract fixes what the platform sends, what the decider answers, and what each party guarantees. The
pipelines that order deciders and the semantics of each verdict inside the session are the governance
capability.

## Declaration

### A Decision Hook Is Declared By Configuration

A decision hook MUST be declared by the session's definition rather than by the model.

A decision hook MUST point at a destination registered with the tenant and referenced by name.

A decision hook MUST declare a deadline within the configured bounds.

A decision hook MUST declare a failure outcome, which defaults to block.

### Latency-Sensitive Hooks Admit An External Decider Only As A Defer

An external decider on the model-output, tool-call, or tool-result hook MUST run only as a defer.

An external decider on the stop-request hook MAY run synchronously.

## The Request

### The Request Identifies Itself

A decision request MUST carry the contract version.

A decision request MUST carry a decision id that is the same when the same question is asked again.

A decision request MUST carry the tenant and the session, or the definition or tenant scope of an operation that has no session.

A decision request MUST carry the hook point and the call-type tags that apply.

A decision request MUST carry the name of the decider being asked.

### The Request Identifies The Actors

A decision request MUST carry the session's principal and on-behalf-of chain.

A decision request acting on another principal's event MUST also carry the requester's principal and
chain.

### The Request Carries The Subject

A decision request MUST carry the subject as the deciders earlier in the pipeline left it.

A decision request MUST carry the session's usage.

A decision request MUST truncate content past the configured size bound with an explicit marker.

A decision request on the model-output hook MAY include the tool results the output cites.

A decision request for a configuration change MUST carry the configuration as it would be stored and the principal proposing it.

### Each Hook Adds Its Own Fields

A decision request at the model-request hook MUST carry the cause of the turn and the session's budget view.

A decision request at the tool-call hook MUST carry the digest of the spec the model saw and the call's side-effect and idempotency semantics.

A decision request at the tool-result hook MUST state whether the result is an error and whether it is a call's result, a tool spec at load, or an outcome-unknown result.

A decision request at the notification-arrival hook MUST carry the event's kind, priority, recorded publisher and chain, access scope, and attachment references.

A decision request at the control-operation hook MUST carry the caller as its principal together with the operation's kind, target scope, and tier.

A decision request for a subscribe or an unsubscribe MUST carry the principal, the tenant, the subscriber, the topic group, and the topic.

A decision request at the compaction hook MUST carry the summary the compaction proposes and the cut point it covers.

A decision request at the notification-arrival hook MUST carry a bounded activity descriptor: the session's phase, the work invested in the current step as tokens and time, the expected time to the next turn boundary, and the session's priority.

A decision request MUST NOT carry session context beyond that descriptor.

### The Request Is Attributable

The platform MAY sign a decision request under its own identity so the destination can verify the
question is the platform's.

### A Retry Reuses The Decision Id

A decision request that fails in transport or with a server-side failure MAY be retried under the same decision id within the decider's deadline.

A decision request the destination refused, or answered with a body that is no verdict, MUST NOT be retried.

## The Response

### A Decision Is One Of A Closed Set

A decision MUST be exactly one of allow, flag, block, defer, or transform.

A block MUST carry a reason written for the model.

A block's reason MUST NOT repeat the content it rejects.

A transform MUST carry the patch to apply as redacted spans or set fields.

A defer MUST carry the principals allowed to resolve it, a deadline, and an outcome on timeout.

A redaction MUST identify the payload field it applies to.

A redaction marker MUST reveal nothing about the content it replaced.

A redaction span outside its field MUST be treated as the decider's failure.

A defer MUST carry the subject as the whole pipeline left it, so that the resolver decides on what will run.

A defer MUST carry each deferring decider's reason as that decider gave it.

### Each Decision Is Legal Only At Stated Hooks

A defer MUST NOT be returned at the model-request, notification-arrival, or stop-request hooks.

A defer at the model-output hook MUST apply only to a tool-use block.

A transform MUST NOT be returned at the control-operation or stop-request hooks.

A transform that sets fields MUST set only fields in the closed set its hook offers.

### A Block Takes Precedence Over A Defer

A block by any decider in a pipeline MUST take precedence over a defer returned by another decider.

### No Verdict Is The Failure Outcome

A decider that gives no verdict within its deadline MUST be treated as having answered its declared
failure outcome.

A response that is not a verdict of the closed set MUST be treated as having answered the failure
outcome.

A decision returned at run time that is not legal at its hook MUST be treated as that decider's failure.

### A Decider Is Idempotent By Decision Id

A decider asked the same decision id again MUST answer the same verdict or report the verdict already
given.

## Stop Admission

### The Stop Hook Asks Whether Work Remains

A stop-admission request MUST identify the session whose stop is proposed.

A stop-admission block MUST carry a reason that states the open work that keeps the session running.

A stop-admission decider MUST allow a stop for a session whose definition's lifecycle intent, where one exists, is paused or retired.

A stop-admission request MUST carry the definition the session runs, whether the session is a root session or a child, and the count of consecutive blocked stops.

A stop-admission answer MUST be allow or block.

## Resolution Of A Defer

### Only A Listed Principal Resolves

A defer MUST be resolved only by an authenticated principal on its allowed list.

A defer MUST be resolved at most once.

A defer past its deadline MUST take its timeout outcome.

The resolution of a defer MUST be exactly one of allow, flag, or block.

A defer's timeout outcome MUST default to block.

A defer scoped to an approval call type MUST NOT list an agent or a model judge among its resolvers.

A defer configuration whose resolvers include no eligible principal MUST be refused at validation.

An approval defer that reaches its deadline with no eligible resolver MUST take block as its timeout outcome.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed deciders, or else carry an explicit
version increment.

A change to this contract that is not additive with respect to deployed deciders MUST carry a stated
migration path.
