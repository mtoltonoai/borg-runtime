# Core: Events And Subscriptions

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines publish, subscribe, and send; fan-out to inboxes; how a notification's priority decides whether it interrupts; why subscriptions are the platform's own; and how people's pages subscribe as viewers. Requirements realize [Core Principle II](../../constitution.md) and [Platform Principle P4](../../constitution.md) and trace to [overview section 4](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The envelope's fields are pinned by the [event-envelope contract](../contracts/event-envelope.md). The transaction key bound is a declared default.

## Purpose And Scope

Events are how anything reaches a session, and subscriptions are how a session declares what reaches it.
This document fixes the primitives, the ordering and fan-out guarantees, the interruption rule, and the
viewer path for people. The inbox application that makes delivery exactly once is in
spec/core/sessions-and-partitions.md. The attribution of every delivered event to its publisher's authenticated principal and chain and to its causal parent is pinned by spec/contracts/event-envelope.md. The refusal of an inbox entry whose publisher's tenant differs from the receiving session's, and its audit event, are in spec/core/tenancy-and-confidentiality.md.

## Primitives

### Three Primitives, Opaque To The Core

The platform MUST provide publish, subscribe, and unsubscribe as primitives.

The platform MUST provide send to one session or to a keyed template with its key.

The core does not interpret a topic or a payload; Platform Principle P4 in constitution.md states it.

A subscriber MUST be a session or a viewer.

### Subscriptions Come From Definitions, Sessions, And Grants

A standing subscription MUST come from the session's definition.

A session MUST be able to subscribe and unsubscribe itself through native tools.

A principal other than the session MUST manage the session's subscriptions only through a control
operation that passes the hook under a grant.

### A Topic Belongs To A Topic Group

Every topic MUST belong to exactly one topic group held by its tenant.

A topic MUST be addressed by its topic group and its topic name.

A topic MUST carry no policy record of its own.

A topic group MUST carry the ordered deciders asked before a subscriber is added to any of its topics.

A topic group MUST carry the ordered deciders asked before a subscriber is removed from any of its topics.

A subscribe MUST pass its group's subscribe deciders before the subscriber is recorded.

A subscribe that a group's deciders deny MUST be refused with the decider's reason.

A subscribe that a group's deciders deny MUST record no subscriber.

A group's deciders MUST be asked when a subscribe or an unsubscribe is made, so that a later change to the group leaves existing subscriptions as they stand.

Who may publish to a topic MUST be decided by a publish gate of the topic's group, separate from its subscribe deciders.

A publish, a subscribe, or an unsubscribe addressed to a topic of a group the tenant does not hold MUST be refused.

A topic group MAY carry a retention bound and a default event-kind filter that its topics inherit.

Creating, replacing, or removing a topic group MUST be a control operation that passes the control-operation hook.

Removing a topic group MUST reclaim its topics together with their subscriptions.

A tenant MUST be able to list its topic groups and the topics of each.

A late event addressed to a topic that is idle MUST re-create the topic rather than be refused.

### A Supervisor Session Observes Another Session

A session holding a read grant on another session MUST be able to receive that session's gated activity as inbox events as the activity occurs.

A supervisor session MUST NOT slow the session it observes.

A supervisor session MUST see only output that has passed the observed session's deciders.

## Fan-Out

### Publish Latency Is Independent Of Subscribers

A publish MUST complete once the event is appended to its topic, before fan-out to subscribers.

Fan-out MUST deliver to subscribers' inboxes and doorbells in batches within the store's configured transaction key bound.

The latency of a publish MUST be independent of the number of subscribers to its topic.

A topic's queue and subscriber index MUST be owned by the partition that owns the topic's key, under that partition's fence.

Fan-out of a topic MUST NOT delay the owning partition's application of its sessions' inboxes.

A slow or frozen subscriber MUST delay no other subscriber's deliveries.

A topic MUST accept subscribers beyond one fan-out batch's bound by adding shards rather than by refusing the subscribe.

A topic whose subscriber index exceeds the configured count MUST split into topic shards, each with its own queue and index.

A publish MUST write a bounded number of keys per topic shard.

### Each Subscriber Receives In Order

Each subscriber MUST receive a topic's events in the order they were published.

Each notification MUST carry an id the subscriber can dedupe on.

Where a topic's source assigns no position, the topic's single writer MUST assign a monotonic position at append.

### A Topic's Queue Is Bounded

A topic's queue MUST keep an event as long as its retention has not elapsed.

A topic's queue MUST be trimmed of the events every subscriber has passed once their retention has elapsed.

A topic's queue MUST be bounded by its reader with a drop marker per subscriber rather than by blocking a publisher.

A re-sync from a position older than the topic's retention MUST report the range as unrecoverable rather than skip it silently.

A restore that cannot preserve a topic's positions MUST deliver the unrecoverable range to each subscriber as a dropped range rather than replay it.

### Indexes Are Kept In Both Directions

The platform MUST keep an index from each topic to its subscribers.

The platform MUST keep an index from each session to its subscriptions.

A subscription MUST be held by its subscriber as a durable record independent of whether the topic is resident.

## Priority And Interruption

### The Session Decides Whether A Notification Interrupts

A notification whose priority exceeds the session's current interrupt threshold MUST interrupt the work in progress.

A notification whose priority does not exceed the threshold MUST wait for the next turn boundary.

### The Threshold Is Dynamic And Configured

The inputs from which the interrupt threshold is computed within a step depend on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

The threshold function MUST be configuration.

An idle session MUST wake for any notification.

A session MUST be able to raise or lower its own interrupt threshold through a native tool.

A transform at the notification-arrival hook MAY set whether the notification interrupts now or waits for the turn boundary.

The computed interrupt threshold MUST decide delivery unless a transform set the delivery field.

When no notification-arrival decider is configured, the partition owner MUST compute the interrupt threshold from the session's state without building a decision request.

### Interrupting A Turn Cancels Only The Model

What an interruption cancels at the model depends on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

A running side-effecting tool MUST NOT be cancelled by a notification.

The result of a tool that was running when a notification arrived MUST be recorded before the
notification is delivered.

### Digests Are A Subscription Option

A subscription MAY ask for its events as a digest rather than one at a time.

A subscription MAY declare a delivery mode and a maximum hold.

A session that holds an entry for its next wake MUST set a deadline at the entry's maximum hold.

## Subscriptions Are The Platform's Own

### Ordering And State Are Kept In One Store

The inbox append and the doorbell write commit in one transaction of the durable store, as spec/core/sessions-and-partitions.md specifies.

The platform MUST NOT rely on an external queue for per-session ordering or for exactly-once
application.

### An External Queue Is At Most An Ingress Buffer

An external queue or topic MAY buffer publishes outside the platform for publishers that require independence from the cluster's availability.

An ingress buffer MUST be drained by the platform's ingress into its own topics.

## Viewers

### A Viewer Is One Page's Connection

A viewer MUST be one live connection from one person's page, opened with a short-lived viewer token for
one person in one tenant.

A viewer MUST subscribe and unsubscribe on its connection as its page changes.

A viewer's subscriptions MUST NOT outlive its connection.

A reconnecting page MUST send its subscription set again.

### No Change Is Missed And No History Is Replayed

A viewer that subscribes to a topic MUST NOT miss a change made between the page loading its view and
the subscription taking effect.

A viewer MUST NOT receive history.

A page that needs current state MUST load it from the adapter rather than from the viewer stream.

### Small And Lossy By Design

A viewer MUST receive change notices bounded by the publisher's payload bound rather than records.

A slow viewer, or a burst on a busy topic, MUST collapse to one reload notice per topic.

A viewer MUST NOT slow a publisher, a session, or another viewer.

### Only What Its Person May Read

A viewer subscription MUST pass a read check against the topic's access scope when it is made.

A viewer subscription MUST end when its person's access is revoked.

A viewer's access MUST be decided again at every open and every resume.

A viewer's stream MUST end at its credential's expiry.

### Person-Addressed Routing Is The Adapter's

What is addressed to a person MUST be published by the adapter to that person's own topic, which the
person's pages subscribe to.
