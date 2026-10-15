# Outcome: Build The Collaboration Platform

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome, the built platform, adopted; they add to the core and never replace it. This document defines the collaboration platform where people and agents work together, and its role toward the agent platform as a tool server, as the adapter that gives meaning to events, and, where the declarative alternative for root sessions is adopted, as a definition source. It is a product built beside the platform and must not contradict the constitution. Requirements realize [Core Principle I](../../../constitution.md) and [Platform Principle P4](../../../constitution.md) and trace to [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The store, the live-update transport, and the identity providers are declared defaults.

## Purpose And Scope

The collaboration platform is where work is organized: boards, documents, chat, a wiki, and questions anyone can pose. Toward the agent platform it is a client and an adapter, never a client of the durable store. This outcome fixes the properties a valid implementation has, independent of the store or transport an implementation uses.

## Scale And Shape

### It Runs On Many Servers With No Node State

The service MUST hold no state that cannot be lost with a node.

The service MUST scale to the agent platform's session count and to the configured number of viewers.

### Changes Within A Tenant Are Ordered By One Revision

Changes within one tenant MUST be ordered by one gapless revision in commit order.

A tenant MUST have one writer of its revision.

A change MUST NOT be ordered across tenants.

### A Durable Store Behind It

The service MUST keep its durable state in a store that survives the loss of a node.

The service MUST refuse a content id a client supplies that differs from the one the service computed.

Pruning document versions MUST NOT remove a current, an approved, or a pinned version, or one that a served definition version references.

A document version number MUST NOT be reused.

A document MUST be tenant content held under the tenant's keys.

## Identity And Lifecycle

### Identity Comes Only From Authentication

The acting identity of a call to the collaboration platform MUST come from authentication rather than from an argument.

A write MUST record the authenticated principal as its author.

An identity argument MUST be accepted only when it equals the authenticated caller.

A refusal of a mismatched identity argument MUST be published as an event.

Authorization MUST fail closed.

At most one runtime MUST be able to act as a given agent at any moment.

The service MUST attest a person toward the agent platform only from the signed sign-in claims presented on the request, never from a stored copy.

Each external bridge or ingest service MUST act under a service principal of its own.

Re-ingesting an external record MUST resolve to the existing record for the same external link rather than create a second one.

### The Operator Team Is Closed

A membership that would resolve a non-person into the operator team MUST be refused.

The operator team, and any team nested in it, MUST be changed only by an operator.

A write that would remove the last operator MUST be refused.

Every change to the operator team MUST be published as an event to the operators.

A recovery entry for the operator team MUST be one-time, add-only, and applied once.

### Lifecycle Intent Is The Only Pause Path

An agent's run, paused, or retired intent MUST be the only way to pause its sessions.

A pause requested through any path MUST take effect by setting that intent.

A check MUST report any agent whose intent and reported state differ.

Recording an agent's retirement MUST, in the same transaction, unassign its open tasks, cancel the questions routed to it, and reroute the answers to the questions it asked to each record's current owner.

Pausing an agent MUST NOT change its tasks or its questions.

Where the declarative alternative for root sessions is adopted, the service MUST mark an agent retired only when the platform reports the retirement applied.

### A Question Outlives Its Asker

An open question MUST remain cancellable or supersedable by a principal other than its asker, so that a retired asker leaves no question that cannot be closed.

A cancel or supersede MUST record the acting principal and a reason.

A cancel or supersede MUST keep the original asker as the question's poser.

A supersede by a principal other than the asker MUST pose the replacement under the acting principal and link the old question to the new one.

When a record's owner changes, its open questions MUST stay open.

### A Decision Is Recorded Under The Principal That Made It

A question MUST be answered, and its answer superseded or withdrawn, only by the principal it is routed to, from an authenticated session.

An answer MUST record the authenticated principal and the authentication path it came through.

An answer whose author is not an authenticated person MUST NOT be presented as an operator's answer.

An imported answer MUST record its import path.

A document's operator review MUST be decided only by an operator from an authenticated session.

A gated action MUST proceed only on a recorded positive go-ahead whose typed fields match the action.

A denial, an absent block, or a cancelled, withdrawn, moot, expired, or imported answer MUST NOT count as a go-ahead.

A go-ahead MUST bind to one principal, so that a reassigned gated task needs a new go-ahead.

A go-ahead posed with an expiry later than its control's longest expiry MUST be refused.

An answered question MUST NOT be cancelled or superseded.

A moot mark on an answered question MUST record the acting principal and the answer that made it moot.

A moot mark MUST remove an answer's effect and never create one.

A posted comment, a question's typed fields, and a recorded answer MUST be immutable.

A question routed to a team MUST be answerable by any member of the team under that member's own identity.

The first recorded answer to a team-routed question MUST resolve its waits.

A blocking question MUST take no default answer.

A question's prompt MUST state the recommended option.

An answer MUST be delivered to the asker if the asker is active, and otherwise to the current owner of the record.

A question routed to an operator MUST ask exactly one decision.

An option offered to an operator MUST NOT describe a step the operator would take by hand.

A gate on starting a task MUST cover the task's descendants, including those created or reparented later.

A gate MUST be removed only by an operator's own write.

## Its Relation To The Agent Platform

### It Is A Client And An Adapter, Not A Store Client

The service MUST reach the agent platform through the platform's API rather than through the durable store.

### It Publishes Every Record Change

Every change to a record MUST be published to that record's topic.

A published event's id MUST derive from the service's own ordered revision, so that the platform applies each change exactly once.

A record's move into a scope MUST publish to the destination scope's topic as a change of that scope.

An event payload MUST identify each record it refers to by both its cited reference and its durable id, fixed at commit.

An event payload MUST carry the fields that changed, cut to the payload bound with a truncated flag.

A watch event MUST be scoped to the watcher alone.

### Every Record Has A Durable Id Beside Its Cited Reference

Every record MUST carry a durable id unique across tenants beside the reference people cite.

A reference people cite MUST resolve only within the caller's tenant.

### It Serves Definitions

Where the declarative alternative for root sessions is adopted, the service MUST serve agent definitions as a definition source over the definition-source contract.

Where the declarative alternative for root sessions is adopted, the service MUST compose a definition version only from approved document versions.

Where the declarative alternative for root sessions is adopted, the service MUST serve a retired intent only for an explicit, recorded retirement, never inferred from a missing record or a failed read.

Where the declarative alternative for root sessions is adopted, the composer MUST NOT issue a new version when the composed body is unchanged apart from its version field.

An agent's definition identity MUST NOT be reassigned to another agent.

### It Manages Subscriptions And Answers Hooks

The service MUST subscribe an agent's session to a task when it assigns the task.

The service MUST unsubscribe an agent's session from a task when the work is done.

The service MUST answer a stop-admission request according to whether the agent has open, unblocked work.

The service MUST receive the agent platform's session status hooks rather than have agents report their own status.

The service MUST broker a short-lived tail token rather than relay a session's tail itself.

A task MUST count as open work for its assignee only when it is not started or in progress, is not archived, has no blocked or set-aside ancestor, and has no open child.

The service MUST have a recovery action for every blocked, quarantined, and failed state the status-hook contract can report.

Alarming a person MUST NOT be the only recovery action for any such state.

The service MUST obtain a new tail token at every connect and resume of a viewer's tail.

The service MUST NOT log or store a tail token.

### It Keeps No Delivery Mechanism Of Its Own

The service MUST NOT keep an agent inbox, a notification poll, or a liveness watchdog of its own.

The service MUST keep only the notifications addressed to a person.

The service MUST publish each new notification to that person's own topic.

A change MUST be addressed to a person only when the change identifies that person directly, recorded at commit.

### The Decision Queue Is Complete By Construction

The decision queue MUST include every open decision routed to its viewer directly or through a team, and every document in the viewer's operator review.

A decision owed to a person MUST NOT reach that person only through a message or a pointer outside the queue.

Withdrawing a document from operator review MUST record the acting principal and a reason.

Withdrawing a document from operator review MUST publish an event to every operator.

Archiving or deprecating a document in operator review MUST be refused while its review is open.

## Coordination

### Coordination Logic Lives In The Collaboration Platform

Fanning work out, collecting results, looping with a cap, and re-delegating MUST be implemented in the collaboration platform as tasks and assignments rather than as a layer in the agent platform.

### A Block Is A Wait On A Resolver

A task MUST be blocked only while it waits on a task, a question, a document's operator review, or a review record.

A task that has an open wait MUST be blocked.

A wait on an agent, a team, or an external party MUST be refused, so that nothing waits on a principal that may vanish.

A task's status MUST be one of a closed set.

A resolver MUST end the waits on it in the transaction that resolves it, whatever its outcome.

A task whose last wait ends MUST return to the status it held before its first wait, with its assignee.

The event that unblocks a task MUST carry the outcome of each wait that ended.

A wait that would close a cycle through waits and parent links MUST be refused.

A wait on a resolver that is already closed MUST be refused.

A task MUST be at most the configured number of levels below its top-level ancestor.

A task MUST be set aside only by an operator's own write or by a matching go-ahead.

A task MUST be exempt from monitoring exactly while it is set aside.

### Housekeeping Runs From Deadlines

An automatic archive MUST be restorable.

An automatic archive MUST be published as an event.

A task that an open wait identifies as its resolver MUST NOT be archived automatically.

A project's refused statuses, required metadata, default assignee, and dwell deadline MUST be tenant configuration enforced at every write.

A task that exceeds its project's dwell deadline MUST produce an overdue event to the project's coordinators.

A task's close event MUST carry the task's pickup, waiting, and blocked durations.

## Extension

### Block Types Share One Document Model

