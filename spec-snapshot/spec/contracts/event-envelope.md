# Contract: The Event Envelope

> **CONTRACT.** This document pins the shape of an event as a publisher submits it to the platform through publish or send, and as the platform delivers it to a subscriber. Adapters publish under it, sessions
> and viewers receive under it, and it is honored across releases. Its requirements realize
> [Platform Principle P4](../../constitution.md) and [Protected Guarantee "Scopes Are Recorded From The
> First Event"](../../constitution.md) and trace to [overview section 4](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The payload bound and the dedupe window are declared defaults.

## Purpose And Scope

The core never interprets a topic or a payload. What it needs from an event is addressing, ordering,
deduplication, priority, confidentiality, and attribution, and those are the fields this contract pins.
Everything else belongs to the adapter.

## Addressing

### An Event Carries Its Destination

A published event MUST be addressed to a topic.

A sent event MUST be addressed to a session, or to a keyed template together with its key.

A topic MUST be scoped within the publisher's tenant.

## Identity And Deduplication

### Every Event Has A Dedupe Id

Every event MUST carry an id that dedupes retries.

The platform MUST apply an event with an id it has already applied for the same destination at most
once.

A publisher that publishes from an ordered log SHOULD derive each event's id from its position in the
log, so that a retry is a duplicate by construction.

A publish or send id MUST be deduplicated per publisher principal and destination, so that the same id reused toward another destination is a fresh event.

### A Topic Delivery Carries Its Position

Every delivery of a topic's event to a subscriber MUST carry the event's position on the topic together with the lineage epoch the position belongs to.

A subscriber MUST NOT compare topic positions across lineage epochs.

## Priority

### Every Event Has A Priority

Every event MUST carry a priority.

The core MUST NOT interpret a priority beyond ordering.

## Confidentiality

### Every Event Carries Its Scope

Every event MUST carry the access scope its source allowed.

## Attribution

### The Publisher Is Recorded, Never Read

The platform MUST record on every delivered event its publisher's authenticated principal and chain.

The platform MUST NOT take a publisher's identity from the payload.

Every delivered event MUST identify its causal parent event.

## Payload

### The Payload Is Opaque And Bounded

The core MUST NOT interpret a payload.

A payload MUST NOT exceed the configured payload bound.

Content larger than the payload bound MUST be carried by reference.

### An Event May Carry A Kind

An event MAY carry a kind, an opaque label the core compares only for equality.

### An Inbound Event Never Dispatches A Tool

An inbound event MUST NOT carry a tool call for the core to dispatch.

## Attachments

### Attachments Are Carried Beside The Payload

An event MAY carry attachments, each a content type and a blob reference.

The platform MUST render an attachment into a turn as a content block without reading the payload.

An attachment whose bytes are unavailable when its event applies MUST render as unavailable with a reason.

An event whose attachment is unavailable MUST still apply.

A publish or a send MUST refer to a staged upload only when the publisher's principal and chain both equal the provenance recorded on the upload.

A publish whose attachments cannot all be resolved MUST append nothing.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed publishers and subscribers, or else
carry an explicit version increment.

A change to this contract that is not additive with respect to deployed publishers and subscribers MUST
carry a stated migration path.

Each field's encoding MUST be declared by this contract rather than chosen per value.

An integer field that may exceed the declared exact-integer bound MUST be carried as a decimal string.
