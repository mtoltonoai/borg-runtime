# Outcome: Declarative Session Management

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome adopted, alone (declarative) or together with the imperative outcome (both); they add to the core and never replace it. Requirements trace to
> [overview section 10](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The source interface and the definition's shape are pinned by
> the [definition-source contract](../../contracts/definition-source.md).

## Purpose And Scope

A definition is the desired configuration of one agent identity, and under this outcome the platform runs
root sessions against whatever a tenant's sources serve. This document fixes how a source is bound, the
controller's behavior, how a version is adopted at turn boundaries by cohort, and what happens when a
source is unreachable or serves an invalid version. The rules that hold under every outcome, including
that a configuration change applies at the next turn boundary and that a change to the cached prefix
preserves the cache, are in [core/sessions-and-partitions.md](../../core/sessions-and-partitions.md).
This document also applies under the outcome that runs both paths; the requirements that apply only when
the declarative path is the sole path are in [declarative-only.md](declarative-only.md).

## Binding A Source

### Binding Is Governance-Tier

Binding, changing, or removing a definition source MUST be a governance-tier operation.

### Bindings Carry The Tenant

Every definition source binding MUST belong to one tenant.

A controller's read of a source MUST be authenticated by the platform's signed assertion bound to the binding's tenant and source, or, where the source cannot verify an assertion, by a credential the binding identifies by reference.

The tenant of a definition, an instance, or a cached read MUST come from the binding rather than from
the payload.

A binding MUST NOT hold a standing source credential where its source accepts the platform's assertion.

A binding MUST be identified by an id that identifies the tenant's relationship with a source and stays the same across a credential rotation, an endpoint change, a removal and re-binding of the same source, and a restore.

A binding id MUST NOT be reused for a different source after the binding is deleted.

A definition name and its binding id together MUST fit within the configured bound on a principal id.

A binding's upper bound MUST bound the number of definitions it serves, the models, tool servers, decider modules, hook destinations, observation queues, and key labels its definitions may use, and the budgets, deadlines, priority, instance caps, and schedules they may set.

The API MUST accept a read of a binding that returns the record and its digest.

A registration of a binding whose source address falls in a refused range MUST be refused.

### Removing A Binding Pauses And Deleting It Ends

Removing a binding MUST pause the sessions of the definitions it served, keeping their state and inboxes.

Removing a binding MUST block the definition-scoped defers its controller holds, with an event.

Removing a binding MUST keep the binding id, its definitions' identities, and its paused sessions.

Re-binding a source under its binding id MUST resume its sessions once their effective lifecycle is run.

Deleting a binding MUST be a governance-tier operation distinct from removing it.

Deleting a binding MUST end every session of the binding as a definition-scope stop does.

A deleted binding's accepted state MUST be freed only after its sessions have ended.

A deletion of a binding MUST carry the digest of the binding record the caller last read.

A deletion of a binding MUST be refused as a conflict when the record has changed.

## The Controller

### One Controller Per Binding

The platform MUST run one controller per source binding.

A controller MUST read the whole index on connect, after a resume failure, and when a held name may fit under the binding's upper bound.

A controller MUST validate each new version against the schema and the tenant's upper bounds.

A controller MUST pass each new version through the input deciders and the control-operation hook.

A controller MUST keep an accepted version under its digest.

A controller MUST keep the previous version when it rejects a new one.

A controller MUST take every other change from the change feed in revision order.

Admission MUST check each model, tool server, decider module, key label, hook destination, and observation queue a version references against both the tenant's registry and the binding's upper bound.

A definition that would take a binding past its definition count MUST be held as a source fault, with the definitions already accepted never displaced.

A version's reference to another definition MUST stay inside the tenant.

### Admission Is All Or Nothing

A version MUST be admitted in full or rejected in full, with no field applied in part.

A value outside an upper bound MUST be rejected at admission rather than clamped.

A version on which a decider gave no verdict within its deadline MUST be left pending for retry, neither accepted nor recorded as rejected.

A transform verdict at admission MUST count as a block.

Admission MUST run only the tenant's pipeline.

A newer version of a definition MUST supersede an admission in progress for an older one without recording a rejection.

Content served again under a new version number MUST be admitted afresh at the upper bounds and deciders then in force.

### A Changed Bound Re-Runs Admission

When a binding's upper bound or the tenant's input deciders change, the controller MUST re-run the upper-bound and input-decider checks over every accepted version.

An accepted version that no longer passes MUST be withdrawn, so that no session adopts it.

The sessions of a withdrawn version MUST adopt another accepted version that passes, or pause.

A later re-check that an accepted version passes MUST reinstate it.

A narrowed upper bound on tool servers, models, or budgets MUST take effect at dispatch, at the model-request hook, and at the budget deciders without waiting for the source.

A re-check that waits on an unanswered decider past the configured alert delay MUST raise an alert identifying the narrowing it delays.

### Accepted Versions Are Tenant Content The Platform Holds

The platform MUST store each accepted version and its components as tenant content owned by the binding.

A session MUST read its configuration from the platform's stored copy rather than from the source.

A turn MUST NOT wait on a call to a definition source.

A version's stored content MUST stay available while any session pins the version.

### The Desired Set Is Materialized

A controller MUST materialize the desired set of sessions its definitions call for.

### Each Partition Reconciles Its Own Sessions

Each partition MUST reconcile its own sessions against the desired set the definition controllers materialize.

The platform MUST NOT require a global session reconciler.

### A Widening Version Defers

A controller MUST refuse a widening version unless the binding is trusted for that class of change, or
defer it to the approver the tenant's governance designates.

## Adoption

### Sessions Pin A Digest And Adopt At Turn Boundaries

A session MUST pin a definition version by its digest.

A session MUST adopt a newly accepted version at a turn boundary.

A session's configuration MUST be the definition version it pins, composed with the tenant's defaults within the binding's upper bound.

A field a version sets MUST replace the tenant's default within the binding's upper bound.

The tenant's required deciders and the binding's upper bounds MUST only narrow what a version may do.

A tool or server removed by an accepted version MUST be refused at dispatch to every session whose desired version lacks it, from the moment the version is accepted.

A tool or server added by an accepted version MUST become callable only once the session adopts the version.

The dispatch refusal of a tool or server removed by an accepted version MUST follow the session's desired version, so that a halted rollout lifts it for sessions that never adopted.

### Rollout Is By Cohort

A version marked for a fraction MUST be adopted by a deterministic cohort of that fraction of the definition's sessions.

The platform MUST be able to halt a rollout.

The platform MUST NOT advance a rollout past the fraction a version was marked for.

A proposed change MUST run on a cohort against its parent before it is adopted by every session.

A definition's first accepted version MUST become its base whatever fraction it was marked for.

A halted version MUST remain pinned by the sessions already on it.

A halted version MUST be adopted by no further session.

The platform MUST halt a rollout when the candidate cohort's rate of quarantines, invalid-request blocks, or decider blocks exceeds the base cohort's by the configured margin.

### Versions Form A Chain And Can Be Compared

A version MUST identify its parent, so that a change can run as an experiment against its parent.

An experiment MUST report its significance against the parent on the usage stream.

### Gate Specs And Versions Are Addressed By Digest

A gate spec, a definition, and a change MUST be addressed by digest.

A gate spec, a definition, and a change MUST be audited by events.

A gate spec, a definition, and a change MUST be admitted at the control-operation hook.

A definition version's content id MUST equal the digest its source serves for it.

## Instancing And Lifecycle

### Instancing Is Singleton Or Keyed

A singleton definition MUST have one session.

Under this outcome a keyed instance's template MUST be identified by its binding and its definition.

A keyed instance's status MUST be reported through events and event hooks rather than written back into a source.

A singleton definition's session id MUST be derived from its tenant, its binding, its instancing form, and its name, so that a rebound binding resumes the same session and no instance key can reproduce the id.

The API MUST accept a send to a singleton definition by its name, so that a caller never derives a session id.

A send to a definition keyed by requester MUST NOT carry a key.

A send to a definition keyed by a publisher's key MUST carry the key.

### Lifecycle Follows The Source

A definition's sessions MUST be ended only by a retirement its source serves that has applied, by a deletion of its binding, or by an emergency stop.

A served retirement that takes the binding's count of retired definitions past the configured fraction MUST be held rather than applied.

A held retirement MUST read as a pause to the definition's sessions, keeping their state and inboxes.

The count of retired definitions MUST be cumulative from the binding's first index read or its last confirm.

A governance-tier confirm that identifies the held set by its digest MUST release the hold and apply the retirements.

A confirm whose digest does not match the held set MUST be refused.

An entry that serves a held name again as run or paused MUST lift that name's hold.

A held retirement MUST be visible through an event carrying the held set's digest, through the definition's status, and through the reason on the paused event.

### Served Lifecycle And Emergency Controls

A resume control MUST NOT override a pause or a retirement the source serves.

A resume control MUST NOT lift the pause that removing a binding placed.

### A Handback Drops Only What Was Pending At The Pause

A controller MUST refuse a handback that fails its form and bounds check, with an event, and drop nothing.

A taken-back event MUST be dropped only while the session is paused and only if the event was pending when the pause committed.

An id or range outside what was pending at the pause MUST be refused with an audit event and stay pending.

A handback MUST NOT list a defer's resolution or a deferred call's outcome.

A dropped event MUST be skipped when the inbox is applied, with its id joining the duplicate guard.

A drop MUST be audited per event id.

A handback MUST stay within the configured bounds on event ids and on dropped-count ranges.

### A Definition's Status Is Readable

The API MUST expose, per definition, the accepted versions, the rejected digests with their reasons, the sessions per version, the instance count, and any held retirement with its digest.

## Standing Schedules

### Schedules Come From The Definition

A standing schedule MUST become a timer the definition's sessions can list but not cancel.

A standing schedule MUST be created when an instance starts.

A standing schedule MUST end when its instance ends.

A keyed definition's standing schedule MUST carry a guard.

## Failure Handling For Sources

### A Definition Source That Is Unreachable

An unreachable definition source MUST change nothing about the sessions it serves.

An unreachable source MUST be retried with backoff and jitter.

The platform MUST raise one alert per unreachable source rather than one per session.

### A Definition That Is Invalid Or Blocked

A definition version that fails validation or is blocked at the hook MUST be rejected by its digest.

A rejected definition version MUST NOT be retried.

A rejected definition version MUST produce an event carrying the reason.

A body that does not hash to its digest MUST be treated as a source fault retried with backoff rather than as a rejection.

A body that hashes to its digest but carries a version number other than its entry's MUST be treated as a source fault with one alert and no retry.

### A Definition Missing From Its Index

A definition missing from its source's index MUST be left unchanged rather than retired.

### A Source Fault Changes Nothing

An index that is empty while the binding holds accepted definitions MUST be treated as a source fault that holds every dropped definition as missing.

Removals that exceed the binding's configured fraction of its definitions within the configured window MUST be treated as a source fault that holds the removed definitions as missing.

A revision lower than the applied revision MUST be treated as a source fault from which nothing applies.

A source fault MUST raise one alert per binding per fault.

A source fault MUST NOT change any session's pinned version or lifecycle.

## Simulation

### Scenarios For This Outcome

A scenario MUST check that every pinned version was accepted by its binding when it was pinned.

A scenario MUST check that each digest is decided at most once per binding.

A scenario MUST check that no source fault changes a session's pinned version or lifecycle.

A scenario MUST check that no definition is retired except by a served retirement that applied or was confirmed, an emergency control, or a binding deletion.

A scenario MUST check that a pinned version changes only at a turn boundary or on load.

A scenario MUST check, with two tenants whose sources serve the same binding and definition names, that no definition, instance, cache entry, or status crosses between them.

A scenario MUST check that a session drops only taken-back events that were pending at its pause.
