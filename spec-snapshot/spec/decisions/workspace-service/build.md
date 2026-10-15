# Outcome: Build The Workspace Service

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome, the workspace service, adopted; they add to the core and never replace it. This document defines the service that provides a session with disposable workspaces for shell, files, and builds: a tool server listed in the definition, with the session's identity on every call, a workspace's contents as a value, and no standing credentials inside. It is a product built beside the platform. Requirements realize [Boundary "The Platform Hosts No Programs For Its Tenants"](../../../constitution.md), [Protected Guarantee "Secrets Never Reach The Model Or The Records"](../../../constitution.md), and [Protected Guarantee "The Tenant Is The Boundary"](../../../constitution.md) and trace to [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The substrate, the snapshot store, and the credential protocol are declared defaults.

## Purpose And Scope

Coding roles require an execution environment, and the platform hosts none. This outcome fixes the service's guarantees: it is a tool server like any other, a workspace is disposable because its contents are a value, credentials are per call, and isolation holds across tenants. How the service provisions hosts is outside this specification.

## A Service Listed In The Definition

### It Is A Tool Server, Not Part Of The Platform

The service MUST be called as a tool server listed in an agent's definition.

Every call to the service MUST carry the session's identity.

The platform MUST contain no logic specific to what the service does beyond calling its tools.

As a tool server, the service is bound by spec/contracts/tool-call.md and spec/contracts/signed-assertion.md, which pin assertion checks, lease generations, and call receipts.

The service MUST be reachable only from the platform's nodes.

### Operations

The service MUST let a session acquire a workspace, operate on its files, submit a build from it, and release it.

A workspace MUST be addressed by a handle that is valid only for the session that holds its lease.

A workspace lease MUST belong to the service rather than to the platform.

A workspace lease MUST expire unless it is used, so that the platform needs no eviction hook.

Acquiring a workspace MUST carry the scope the task's grant allows.

The scope of an acquire MUST default to read-only.

A running command MUST count as use of the workspace's lease.

An acquire of a workspace name on which the session already holds a lease MUST return that handle and change nothing.

An acquire MUST NOT fail because the host that last held the workspace is unavailable.

A live workspace MUST be held by exactly one session.

A lineage MUST belong to the session that started it.

A new incarnation of a session MUST end every lease the session's older incarnations held.

A side effect of an older session incarnation MUST NOT be admitted once the service has recorded a newer one.

A handle, a lineage, a snapshot, or a receipt that its caller is not entitled to MUST answer as if it did not exist.

An expired handle MUST fail with a distinct error that identifies the workspace's last snapshot.

A call's ending MUST be one of a closed set.

A call's ending MUST state by itself whether the call's effects inside the workspace exist.

The service MUST NOT repeat on its own a command that may have acted outside the workspace.

A reconcile answer for a workspace call MUST identify the snapshot that followed the effect.

An implicit cancel MUST be recorded when it is decided.

Recovery MUST cancel rather than reattach to a command whose record is marked cancelled or whose lease has ended.

A call that its host never received MUST settle as interrupted at the current head.

A call that its host never received MUST NOT be sent to the host late.

A host MUST end a call no later than the lesser of the message's transit time and twice the clock-skew bound after its deadline.

A call's spooled output MUST NOT be cut while the call can still write.

A late result MUST attach at most the configured tail of each output stream and state how many bytes it left out.

### A Host Is Acquired Only For Work That Needs It

A workspace MAY exist without a host, as a stored tree whose builds run on a build service.

Acquiring a host for a workspace MUST be a gated action that keeps the workspace's handle and lineage.

A workspace's stored tree MUST carry over onto a host when the host is acquired and back when the host is released.

A host acquired for a workspace MUST be released when the work that needed it ends.

## A Workspace's Contents Are A Value

### A Snapshot Follows Every Changing Call

The service MUST snapshot a workspace's contents, content-addressed, after every call that changed them.

A session MUST be able to continue from its last snapshot on another host after its host is lost.

A snapshot MUST chain to the previous one, so that it stores only what changed.

Calls held open on a workspace at the same time MUST form one batch covered by one snapshot.

A restored tree MUST have the same content hash as its snapshot.

A call that has returned as running MUST be outside every batch.

A lost host MUST lose at most the calls in flight and the calls whose covering snapshot had not finished uploading.

A snapshot MUST cover the whole workspace, including every repository it holds.

A snapshot MUST store working-tree bytes as they are, applying no conversion or filter other than large-file retrieval.

A snapshot MUST become durable only when its head commit succeeds against the lineage's current head, the live workspace incarnation, and the lineage's writer generation.

A snapshot MUST NOT have two successors.

A restored tree MUST be verified under the rules recorded in the snapshot.

A restore whose verification fails MUST be retried on another host.

A restore whose verification fails after the configured retries MUST fail rather than hand over the tree.

A snapshot id in a result or an event MUST be an opaque id that reveals nothing about the snapshot's content.

A lineage MUST outlive its session for the tenant's retention of workspace lineages.

### Swap, Fork, And Rollback

A swap MUST restore a workspace's snapshot onto any host.

A fork MUST start a new lineage from a snapshot.

A fork MUST pin the snapshot it starts from.

A rollback MUST make an earlier snapshot the head as a new snapshot, so that a lineage only grows.

A planned move MUST run between calls, after the last snapshot's upload.

A planned move MUST NOT serve a call on the new host before the head is restored there.

A host lost after a planned move's restore has started MUST produce no offline event.

A rollback MUST be refused while any call that changes the workspace is in flight.

Every call on a workspace other than a reconcile or a close MUST be refused while its rollback is in flight.

A rollback MUST start only when no read of the workspace is being served.

A workspace whose rollback check failed or whose hand-over did not finish MUST NOT be used again.

A restore of any snapshot MUST read at most the configured number of blobs, whatever the lineage's length or fork depth.

Deleting a lineage MUST NOT make a live fork of it unrestorable.

A sub-session that needs its parent's workspace MUST take a snapshot fork rather than share the parent's live workspace.

A fork that needs a workspace MUST take a snapshot fork from the service.

### Offline Is An Event

A lost host MUST produce one event per lost workspace into the holding session, published through the platform's API, carrying the handle, the reason, the last durable snapshot or none, and the ids of the calls whose effects were discarded.

The service MUST NOT restore a workspace after an unplanned loss without the session acquiring it again.

A lineage left with no durable snapshot MUST end, freeing its name.

Deleting a held workspace's lineage MUST end its lease offline with an event that carries that reason and no snapshot.

A command the session was told was interrupted MUST have stopped, even on a host cut off from the service.

A host MUST stop a lineage's commands when its host lease for the lineage ends unrenewed.

A host lease MUST be renewed only by the lineage's writer.

The writer MUST commit the next renewal of a host lease only after the host acknowledged the last.

The service MUST declare a host lost only after its lease plus the margin since the last renewal it holds, or when the substrate reports the host stopped.

## No Standing Credentials

### Credentials Are Per Call

A workspace-local endpoint MUST serve each call's credentials only while that call runs.

A call's processes MUST be terminated when the call ends, across any restart of the host agent.

The host's own metadata endpoint MUST be unreachable from a tool.

Credentials MUST come from the credential broker in exchange for the call's assertion.

A log, event, or saved output MUST NOT carry a secret or an endpoint token.

A tool identity MUST be unable to start a process that outlives or escapes its call.

A call whose supervisor is lost MUST end as stopped by the host.

A call's credentials MUST reach only that call's own processes.

A credential token MUST grant nothing once its call has ended.

A credential helper MUST fail rather than prompt.

The service's own helpers MUST NOT place a credential in a file, an environment variable, or a command line.

A served credential's claimed expiry MUST NOT exceed the configured upper bound, whatever the workspace type configures.

A host's store credential MUST allow only conditional creates of the exact objects the host may write, and never a replace.

A host MUST hold only the data keys of the lineages it serves and the sources it has pinned, unwrapped off the host.

A host MUST NOT hold a branch key or an owner key.

Every credential the service acts with MUST carry the operator as its source identity.

A credential the service acts with MUST allow a write only for a call that writes.

The service MUST hold no credential for an operator beyond the configured margin before its expiry.

## Isolation

### A Workspace Is Isolated And Bounded

A host MUST run one tenant's workspaces only.

A tool MUST run unprivileged under resource limits.

All egress MUST leave through one point the service controls.

A build cache MUST be shared within a tenant.

A build cache MUST NOT be shared across tenants.

A reachable host MUST destroy a deleted lineage's copies on acknowledgment of the discard.

An unreachable host MUST destroy a deleted lineage's copies when its lease, margin, and skew bound have passed.

The service's own tooling MUST run nothing a session can configure in a repository or in a home directory.

A restore or a rollback MUST hand the tree over through a surface the tool identity cannot write.

A tool process MUST NOT hold the host's store credential.

A workspace's processes MUST see no other workspace's processes.

The snapshot process MUST read every file under the workspace root whatever its modes, and nothing outside it by path or by handle.

A tool process MUST see only the system and toolchain read-only, its own workspace read-write, and the paths declared visible at acquire.

The egress point MUST admit no route to a source that carries a domain except through the credential helper, so that the session's domain set records every entry.

Every storage and key operation the service makes MUST run under a credential scoped to one tenant's prefix and key.

A build cache entry MUST be keyed by the content hash of its inputs, so that one session cannot corrupt another's builds.

A build cache entry MUST be served only to a session whose clearance covers the domain the entry came from.

A build cache entry MUST record the principal that wrote it.

Every backend endpoint the service calls MUST come from its configuration.

The service MUST use an unencrypted transport only to an endpoint its configuration marks for it, and only where the request carries no secret.

Every backend call MUST be bounded in time and in size.

A session's use of the service MUST NOT be able to degrade another session's.

A host MUST NOT be reachable inbound from outside the service.

### Placement And Clearance

A workspace type MUST carry its network placement, so that a tool needing a particular network runs on a workspace placed there.

A session MUST be able to hold several workspaces at once, each with its own handle and its own snapshot lineage.

A session's own workspaces MAY reach each other.

A workspace MUST NOT reach another session's workspaces.

Acquiring a workspace for a repository MUST require the session's clearance for the repository's domain.

A session MUST keep a domain set that only grows, recording the domain of every repository that enters any of its workspaces.

A lineage MUST carry its session's domain set.

A fork MUST add its source session's domain set to its own.

A continuation, restore, or fork MUST be refused for a session whose clearance does not cover the lineage's domain set.

A credential request for a repository outside the session's clearance MUST be refused at the time of the request, with an event.

A workspace type MUST carry text through its tools unchanged, including characters outside the basic character set.

### A Host Keeps Its Promises

A host MUST record each promise it makes before it answers the message that caused it.

A host MUST treat a reboot or a damaged promise record as a lost host.

A host that cannot record a promise MUST answer the message with a retryable error and make no new promise.

A host that starts a new epoch MUST quarantine the previous epoch's workspace copies rather than delete them at once.

A host that meets a record format it does not know MUST stop with an alarm rather than start a new epoch.

A lineage MUST have one writer instance at a time, fenced by a writer generation checked in the service's records before every host operation.

A destructive host operation MUST identify the workspace incarnation it targets and the lease it acts under.

A host MUST refuse a destructive operation that identifies another incarnation or lease.

A host MUST refuse as never received a message whose admission time plus the retention bound and the skew bound has passed.

### Hosts Report Health And A Lost Host Frees Its Leases

A host MUST report its health on registration and on each heartbeat.

An acquire MUST prefer a healthy host.

An acquire that falls back to a degraded host MUST state the degradation as a warning on the result rather than block.

A host that misses heartbeats past the configured stale window MUST be removed from the pool and its leases freed.

## Tools That Need No Workspace

### A Workspace-Free Tool Is One Call To A Service

The service MUST serve a tool that is one call to an already-scaled service without provisioning a host.

The service MUST hold no state for such a tool beyond held credentials and the fence.

A session MUST receive compute of its own only when its task must check out, build, or run code.

### The Service Holds The Credentials For A Workspace-Free Tool

A credential for a tool that needs no workspace MUST be held and used by the service alone, so that no model-written code or tool process holds it.

Every call to such a tool MUST act with credentials issued for the operator the call acts for.

## Stored Values

### A Stored Value Belongs To One Session

The service MUST keep for a session a value that is referenced by name, versioned, and immutable, each version recording what produced it.

A stored value MUST be readable only by its session, or by a session acting for the same operator that holds the value's clearances through an explicit share.

A stored value's id MUST be opaque.

Every write of a stored value MUST pass the structured-secret detector before storage, with a match replaced by a marker that identifies the detector.

## Observability

### Every Served Call Is An Event

The service MUST publish an event for every tool call it serves, carrying the operator, the tool, and the outcome, never content.

The service MUST publish an event for every call it makes to a backing service, carrying the service, the operation, the operator, and the status.
