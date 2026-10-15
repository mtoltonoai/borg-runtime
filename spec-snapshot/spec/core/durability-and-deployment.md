# Core: Durability And Deployment

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines how a session survives a crash, a move, a deploy, and a
> lost cluster: re-drive from committed state, an epoch fence so a replaced cluster cannot write back,
> deployment as a cluster type, and the alarms and drills that make a restore trustworthy. Requirements
> realize [Platform Principle P5](../../constitution.md), [Platform Principle P6](../../constitution.md),
> and [Core Principle III](../../constitution.md) and trace to [overview section 14](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The deployment control plane and the epoch form are declared
> defaults.

## Purpose And Scope

A session's state is durable, and all work is re-drivable from it. This document fixes what that
guarantee requires of deploys and of cluster loss: an epoch fence so a replaced cluster cannot write back,
deployment through the operating team's own control plane, and drilled restores. How the store survives losing every node is a fixed default, self-durable, recorded in [spec/record.md](../record.md) part A; its alternative is the entry "A Recovery Store Outside The Cluster" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under the default and under the alternative, including the source a new cluster restores from, are in spec/decisions/durability/.

## Re-Drive Is The Basis

### Work Resumes From Committed State

The next owner's resumption of each session from its committed cursors is specified in spec/core/failure-handling.md.

An interrupted model attempt MUST be run again as a new attempt with an attempt id of its own.

An interrupted tool call MUST follow its semantics on recovery.

Format evolution, so that two forms of a durable format are readable during a rollout, is pinned by spec/contracts/stored-form.md.

## Cluster Replacement

### Audit And Usage Leave Through Their Own Sink

Audit events and usage records leave the session store for a sink with its own retention: for audit events as spec/core/identity-delegation-and-secrets.md requires, and for usage records as spec/contracts/usage-record.md pins.

A drain MUST deliver every pending audit event and usage record to that sink before the final records are written.

### Stage Resources Outlive Every Cluster

The blob store, the tenant root keys, the signing roots, the restore source, and the frontend's stable name MUST survive the replacement or loss of any cluster.

The frontend's access log MUST survive the deletion of any deployment that declares it.

A cluster MUST NOT be created in a stage that lacks any resource its sessions depend on.

A blob's storage location, an owner id, and a published name MUST NOT encode the cluster, node, or partition that wrote them.

### The Epoch Fence

A new cluster MUST increment an epoch that every write is conditioned on.

A cluster whose epoch is superseded MUST fail its next write.

A cluster whose epoch is superseded MUST stop its partitions.

The epoch increment MUST be conditional, so that two new clusters cannot both become the owning cluster.

## Drain And Restore

### A Drain Is Orderly

On a drain, the frontend MUST refuse every state-changing request other than an emergency control with a retryable status, so that a retried request is deduplicated by its id.

On a drain, a partition MUST apply what is pending.

A partition MUST freeze after it has applied what is pending.

On a drain, an in-flight model turn MUST be assigned the drain deadline.

A model turn still in flight at the drain deadline MUST be recorded as interrupted.

On a drain, final records MUST be written before storage terminates.

A drain MUST be reversible before the switch to the new cluster, so that the draining cluster resumes serving when the replacement is not accepted.

A drain MUST NOT complete before every emergency control in force is held by the restore source.

### A Restore Resumes Every Session

A new cluster MUST restore every session from the restore source the adopted durability outcome provides.

A restored session MUST resolve its keys when it next wakes, at a paced rate.

A restored recurring timer's missed occurrences MUST follow the missed-occurrence rule at a paced rate.

A restore MUST start the cluster's partitions only once every record is written back.

Every partition MUST open within the configured bound of the restore's open point.

A restore MUST apply the tenant's emergency controls before any session resumes.

A restore MUST apply a tenant's authority records before any of the tenant's sessions resumes.

A restore MUST bring a tenant back whole: its configuration, its session index, and its sessions.

A pin whose own expiry had passed at the drain MUST keep that expiry.

A restored session MUST receive an event carrying the time of the record it was restored from.

A restore MUST NOT cause an identifier issued before the restore to be issued again.

A consumer of a change feed whose position is inside the range a restore lost MUST reset and record a gap rather than replay.

The dedupe record of a state-changing request other than a publish or a send MUST survive a cluster replacement, so that a retry against the next cluster is answered from the record.

A partition whose restore fails MUST be published as an event with its reason.

A restore failure of one partition MUST NOT stop the node from starting the partitions it restored.

## Deployment

### The Platform Deploys As Its Own Cluster Type

A platform cluster MUST NOT share a cluster with another workload, so that another workload cannot deprive sessions of resources.

A deploy that replaces a cluster MUST be a drain, a checkpoint, a new cluster, and a restore, in that
order.

A deploy that rolls a node's image in place MUST transfer that node's partitions under the fence, so
that no session is paused.

A cluster that holds sessions MUST NOT be deleted before its drain has completed.

What a node runs MUST be pinned by digest.

### A Deployment Completes On Readiness

A deployment MUST count a node as deployed only after the node's readiness check passes.

A node whose readiness check does not pass within the configured bound MUST fail its deployment rather than complete it.

### A Record Variant Is Readable Before It Is Written

A new variant of a durable record MUST NOT be written before every reader of that record kind runs code that reads it.

A deployment MUST NOT roll back below the version that reads a record variant while a record of that variant exists.

A reader of many records MUST NOT fail the whole read because one record is of a kind it does not know.

### Every Connection Between Nodes Is Authenticated

Every connection between the platform's nodes authenticates both peers under a credential scoped to one cluster and rotated, as spec/core/shared-framework.md specifies.

## Operability

### A Node Runs Only The Platform's Process

A platform node MUST run only the platform's own process.

The platform's process MUST run unprivileged and isolated from the host's own process, network, and filesystem namespaces.

A platform node MUST accept no command and no interactive session from any principal.

A node image MUST NOT include an agent that accepts configuration or commands from outside the deployment control plane.

A change to a platform node outside the deployment control plane MUST be prevented by permission rather than detected after the fact.

Code a tenant supplies MUST NOT be able to reach the host's metadata endpoint or the node's own credentials.

A component MUST bind no network listener beyond those its design declares by port.

An administrative listener MUST bind only the local interface unless a declared peer calls it.

### A Node Starts Only On A Checked Configuration

A node MUST refuse to start on a configuration that fails its schema check, carries a field it does not know, selects an unauthenticated transport, or relies on a default for a network-facing setting.

### A Node Reports Its Own Health

A node MUST report its own health to the deployment control plane rather than rely on a check that a
crash-looping process passes.

A node MUST withdraw readiness before it drains.

A node's readiness MUST reflect the node's own ability to reach each dependency a request needs, so that traffic is routed away from that node alone.

A reachability probe MUST exercise the dependency's request path rather than only open a connection, so that an authorization or credential failure is detected.

A change in a node's reported reachability of a dependency MUST be published as an event.

A cluster MUST be reported ready only when every partition is assigned and a canary session has completed a round trip within the configured bound.

The deployment control plane MUST treat a cluster whose health reports have stopped for longer than the configured bound as unhealthy.

Traffic MUST NOT be moved to a cluster that is not reported ready and healthy.

### Alarms Derive From The Platform's Own Events

Alarms and dashboards MUST derive from the platform's own metric definitions.

Every alarm MUST have a runbook.

An alarm on a gauge that must stay at a value MUST treat missing data as breaching.

A node MUST export its resident memory and its allocator's gauges in every stage.

Every alarm MUST have fired once, in a drill or a synthetic test, before its stage carries production work.

### Restores Are Drilled

A restore MUST be drilled in a pre-production stage every release, since every deploy and every crash
depends on it.

A restore MUST be drilled into a different failure domain from the one the lost cluster occupied.

### Platform Limits Are Runtime Settings

A limit on the platform's own operation, such as a cap, a cadence, a timeout, or a pool size, MUST be a runtime setting rather than a constant fixed in code.

A platform operator MUST be able to change a runtime setting through the API without a redeploy or a restart.

A setting that takes effect only when a node starts MUST be node configuration rather than a runtime setting.

A change to a runtime setting MUST be published as an event identifying the operator and the setting.

### Least Privilege From The First Release

A component MUST hold only the permissions that its specified behavior needs.
