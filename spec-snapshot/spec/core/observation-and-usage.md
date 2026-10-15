# Core: Observation And Usage

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines what the platform records about its own behavior: a usage record per unit of work, observation records that point at transcript ranges, and the loop by which those outputs feed back as configuration. Requirements realize [Platform Principle P7](../../constitution.md) and [Core Principle III](../../constitution.md) and trace to [overview section 13](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The usage record's fields are pinned by the [usage-record contract](../contracts/usage-record.md); the observation triggers and queues are declared defaults.

## Purpose And Scope

The platform's own outputs are data. This document fixes the usage stream that measures cost against
cause, the observation records that let a reader see what a session did without copying its transcript,
and the loop that turns both into configuration changes. How a proposed change becomes a new definition version depends on who owns the list of root sessions, a fixed default (imperative) recorded in [spec/record.md](../record.md) part A whose declarative alternative is an entry of [spec/decisions/deferred.md](../decisions/deferred.md); the requirements under the default and under each alternative are in spec/decisions/session-management/.

## Usage

### One Record Per Unit Of Work

Exactly one usage record exists per model attempt, tool call, and decider call, and its fields, which include the record's tenant and the session's principal chain and never transcript content, are pinned by spec/contracts/usage-record.md.

### Versions Compare On One Stream

A model attempt's record MUST carry the definition digest the session ran, so that versions compare on
the same stream.

Budget deciders MUST read the usage stream.

Metering MUST read the same usage stream rather than a separate one.

## Observation

### A Session Publishes Observation Records

A session MUST publish an observation record to a queue when it crosses a size threshold, and when it ends, compacts, or is asked to.

An observation record MUST carry the session and identity, the reason, the transcript range, the size in
bytes and tokens, the model and configuration version, and timestamps.

An observation record MUST point at the transcript rather than copy it.

An observation record MUST point at the same frozen blobs the transcript ranges refer to.

An episode MUST be recorded as an observation record that points at the transcript range of its attempts rather than copying them.

An episode's event MUST carry the episode's id and its range reference.

An episode's event MUST NOT carry content.

A label MUST reference content rather than copy it.

### Reading An Observed Range Is Granted And Audited

A principal reading a range an observation record points at MUST hold a read grant on the session.

Every read of an observed range MUST be audited.

A reader MUST pass the access-scope check for the range it reads.

An observation record's pin MUST cover only the transcript ranges its records cite, never reasoning content, a stored tool output, a frozen session, or an audit copy.

A grant to read episodes MUST be scoped to episode records in the reader's tenant rather than to transcripts in general.

A copy of observed content held by a consumer MUST become unreadable within the configured bound when the key protecting the source content is destroyed.

A label derived from observed content, such as a verdict, a reason, or a decider id, MAY outlive the content.

A consumer whose observed range is no longer readable MUST record the range as unreadable rather than substitute content.

An episode kept longer than its transcript MUST be transferred to a tenant-owned copy before the key protecting its range is destroyed.

### Triggers And Queues Are Configuration

The triggers for an observation record and its target queue MUST be configuration.

### Dependency Measurements Are Published

A dependency client's own measurements MUST be published through the node's metrics rather than dropped.

## The Improvement Loop

### Improvement Runs Through The Platform's Own Primitives

An observer MUST propose a change as a new definition version through the change control that governs the
definition rather than by writing configuration directly.

The signals by which a change is evaluated MUST come from the usage and observation streams.

A proposed change MAY be evaluated on experiment forks of recorded sessions before it runs on a cohort.

