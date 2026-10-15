# Deferred Decisions

> **DEFERRED DECISIONS.** The register of decisions deliberately left open. Each entry states what is deferred, the property of the core that keeps it possible, and the concrete situation that reopens it. This document is descriptive and carries no requirements. It is the register that [overview section 19](../overview.md) summarizes. It complements the two decision documents, [tenancy-scope.md](tenancy-scope.md) and [execution.md](execution.md), whose method is in [README.md](README.md), and it holds the alternatives to the six fixed defaults that [record.md](../record.md) part A lists.

## The Rule

Two decisions are settled with the operator before building: tenancy scope, which is irreversible, and execution. Every other choice is either a fixed default recorded in [record.md](../record.md) part A or an entry here, decided when use demands it and kept possible by a property the core requires. Four rules follow.

- No question is asked of the operator about a deferred item before its trigger has occurred. The questions in the decision documents cover the two decisions only.
- Reopening an item follows the method in [README.md](README.md): the implementer answers from the environment first, states the decision and its outcomes, asks about the operator's situation in at most four independent questions, recommends with the margin and what would change it, confirms, and records the outcome in [record.md](../record.md).
- Before its trigger occurs, nothing is built for an item beyond the invariant listed with it, and that invariant is not weakened, because it is what keeps the item possible.
- An entry that ends with a line "Requirements when reopened" holds the alternatives to a fixed default. The files it lists are normative, are in the tree, and are commented out of the gate configuration. Adopting one means removing the comment markers from its blocks in `.duvet/config.toml`, adding them to the blocks of the files the "replaces" clause lists, and writing the change in the fixed-defaults table of [record.md](../record.md) part A.

## The Register

### Fair Share Between Tenants Under Contention

- Deferred: dividing shared capacity between tenants when they contend for it, beyond the per-tenant caps that every deployment configures.
- Kept possible by: every request, record, and usage record carries its tenant, and the per-tenant caps are counted across the cluster, so a fair-share rule is a policy over counts the platform already keeps rather than a new enforcement path; the scheduler leases quota in shares with reservations per priority class.
- Reopened when: two tenants run sessions on one cluster and one tenant's load is measured to delay another's work.

### Metering And Billing

- Deferred: attributing the cost of model calls, tool calls, and decider calls to a tenant, and charging for it.
- Kept possible by: the usage stream: one usage record per model attempt, tool call, and decider call, carrying the tenant, the principal chain, the session and its parent, the cause, the outcome, cache reads and writes, prompt size, and definition digest.
- Reopened when: a tenant is to be charged for its use, or a cost must be attributed to a tenant for the operating organization's accounting.

### Reclamation Of Unreferenced Blobs

- Deferred: reclaiming the bytes of blobs that no owner or pin references, to reduce storage cost.
- Kept possible by: every blob records its owner and its pins from its first write, and nothing an owner or pin references is reclaimed, so a collector has the records it needs and can never remove a referenced blob.
- Reopened when: the storage cost of unreferenced bytes is measured and exceeds what the operator accepts.

### In-Place Patching Of A Running Cluster

- Deferred: replacing a cluster's software without draining its sessions to a new cluster.
- Kept possible by: every session is restorable from its committed state, so a drain to a new cluster and a restore there is the deployment path, and nothing a session holds in memory is needed to resume it.
- Reopened when: the time a drain-and-restore deployment makes sessions unavailable, or the frequency of deployments, exceeds what the operator accepts.

### More Identity Providers And Unattended Agent Identities

- Deferred: accepting people and services authenticated by an identity provider other than those the platform accepts, and an unattended identity per agent at the identity provider with a second trusted issuer.
- Kept possible by: a principal id carries its identity provider, so adding a provider adds data rather than changing the ids of existing principals; broker backends sit behind one interface, and a verifier admits a second issuer under the checks it already runs.
- Reopened when: a tenant's people or services authenticate with a provider the platform does not accept, or the identity provider offers an unattended identity per agent.

### A Tenant's Own Root Key

- Deferred: a tenant holding its own root key in its own account of the key service rather than one the service holds for it.
- Kept possible by: the envelope design, under which each tenant's keys derive from a root key dedicated to that tenant, whether its own or one the service holds for it, and a node decrypts only under keys in its trusted key list.
- Reopened when: a tenant requires custody of its root key.

### Stronger Isolation Of Tenant Code

- Deferred: running a tenant's deciders with more separation than the sandbox gives, such as compute that no other tenant's code shares.
- Kept possible by: the sandbox is the only place tenant-supplied code runs: deciders are the only tenant code the platform runs, and a sandboxed program decider is deterministic and bounded in time and memory, behind the decider interface.
- Reopened when: a tenant requires, by policy or by contract, that its deciders run on compute no other tenant's code shares, or a fault is found that the sandbox's bounds do not contain.

### Cross-Tenant Collaboration

- Deferred: people or agents of two tenants working on one task, session, or document.
- Kept possible by: explicit shares as grants: content reaches another tenant only when it is published on purpose and never through a derived store, a blob is shared only by an explicit pin with a holder and an expiry, and delegated authority is data evaluated at the hooks, so a share across tenants would be a grant rather than a new path.
- Reopened when: two tenants ask to work on the same task, or an agent of one tenant must read a record of another.

### Streaming Ingress And Request-Reply Between Sessions

- Deferred: a continuous stream of external events into sessions at a rate that one webhook publish per event does not carry, and a session asking another session a question and receiving the reply correlated to its request.
- Kept possible by: topics and call ids on sends: every event has a topic, a send is addressed to one session or to a keyed definition with its key, and a call id is the dedupe key end to end, so a stream is a topic with a publisher and a reply is a send carrying the call id of the request.
- Reopened when: a tenant has an event source whose rate one publish per event cannot carry, or a session needs a reply from another session rather than an event on a topic.

### Signed Definitions

- Deferred: verifying the author of a definition version cryptographically rather than trusting the source's binding.
- Kept possible by: the attested author recorded with each version, and the binding that carries the tenant and authenticates under the platform's assertion, so a signature would be checked against the recorded author.
- Reopened when: a source outside the tenant's own change control serves definitions, or a tenant requires that a version's authorship be verifiable independently of the binding.

### Serving External Customers

- Deferred: running agents for customers outside the operating organization.
- Kept possible by: the tenant boundary and what it carries: the tenant on every record, keys derived per tenant with deletion by key destruction, provider-qualified principals that admit a customer's identity provider, and the usage stream that metering and billing build on.
- Reopened when: a customer outside the operating organization asks to run agents on the platform and the organization decides to offer it. This is a product decision.

### Tool-Call Extensions: Server Requests And Stored Outputs By Reference

- Deferred: a tool server asking the platform for a model completion or for a question to a person during a call; and passing a platform-stored output by reference as a tool-call argument, resolved at dispatch, capped, and recorded by digest.
- Kept possible by: a request the platform does not support is refused with a protocol-level error, so a server learns the refusal rather than failing silently; and the full output of every tool call is stored once under its call id, so a reference has one thing to resolve to.
- Reopened when: a tool server a tenant needs requires either request; or an agent must compute over an output a server other than the workspace service produced.

### Control-Plane Resize Or Reset, And Compliance Reporting On Nodes

- Deferred: what a resize instruction and a reset instruction from the deployment control plane mean for a platform cluster; and whether a node runs an organization's host compliance agent, which carries a path to run code on the node.
- Kept possible by: a cluster acknowledges no control-plane instruction it has not acted on, so an undecided instruction stays pending rather than being dropped; and a node's configuration is written by the deployment and a node accepts no command path into a process that holds tenant plaintext, so the default when the reporting is reopened is no such path, recorded as a compliance exception.
- Reopened when: the control plane sends either instruction to a platform cluster; or an organization requires that reporting.

### Owner Granularity For Deletion

- Deferred: destroying the bytes of one record, or one catalog entry's artifacts, alone, where a product writes many records under one owner.
- Kept possible by: owners and pins are recorded from the first write, every catalog entry carries its tenant and its version, and each product chooses its owner granularity, so a product can move to one owner per record.
- Reopened when: a retention rule or a request requires destroying one record's bytes, or tenants write their own catalog entries.

### Keyed Instances Shared Across Publishers Or Started By A Sub-Session

- Deferred: whether several publishers may share one keyed instance; and whether a keyed instance started by a sub-session of another definition is bounded in configuration by its parent, or only in authority. The default kept is one instance per publisher-scoped key.
- Kept possible by: an instance key is the pair of the publisher's authenticated principal and the key it supplied, so no publisher reaches another publisher's instance; and a keyed instance acts for its parent as requester with intersected grants.
- Reopened when: a second publisher asks to reach an existing instance; or the first keyed instance is started by a sub-session.

### Memory: Its Store And Its Access

- Deferred: whether the store that holds an agent's memories is a component of the platform or a separate service behind a tool server; and how a session reads, writes, and recalls a memory.
- Kept possible by: a memory is keyed by agent identity and persists across every session of that identity; a memory is recordable by a mechanism that observes the session and not only by a tool call; a memory recorded by observation carries the access scope of the transcript range it came from; and the loss of one host destroys no memory, so either store and any access path keeps the same record.
- Reopened when: a tenant's definition requires memory across sessions beyond the key-value slot, through a tool that writes it or a transcript pipeline that records it.

### The Subject A Schema Decider Validates At A Configuration Hook

- Deferred: whether a schema decider at the control-operation hook validates the configuration as it would be stored after the change, or only the fields the change carries.
- Kept possible by: a decision request for a configuration change carries as its subject the configuration as it would be stored with its proposing principal, within the decision-request size bound, so a validator of the whole record and a validator of the change both have what they read.
- Reopened when: a tenant's schema decider denies a configuration change for a field the change did not touch, or a tenant's configuration record approaches the decision-request size bound.

### Per-Tenant Separation Of Shared Platform Identities

- Deferred: binding a signing lineage to the tenants it signs for, giving the on-behalf-of exchange a client identity or a verified tenant claim per tenant, moving program workers to a credential-free runner that serves one tenant each, and binding a membership record to the node that published it.
- Kept possible by: a verifier refuses an assertion whose tenant differs from the call's; the exchange is bound to the tenant the assertion carries; a program holds no credentials; and a membership record that fails to decode is reported as corrupt with an event rather than acted on.
- Reopened when: a deployment serves a second tenant, or the network between nodes is not trusted.

### A Collaboration Product: An Adapted Tool Or A Built Platform

- Deferred: a product in which people assign work to agents, change what an agent is configured to do, are notified when an agent needs a person, and watch what agents are doing: either an adapter that connects a tool the tenant already uses (a ticket tracker, a board, or a chat system) and publishes each record change as an event, or the collaboration platform built beside the agent platform, with boards, documents, chat, a wiki, questions, and the one queue in which everything that needs a person waits. The fixed default is none: the tenant's own systems assign work, change configuration, and read status through the frontend, webhooks, event hooks, and tails.
- Kept possible by: everything that calls the platform comes through the frontend as an authenticated principal and passes the control-operation hook; an external system publishes through a webhook mapped to a principal; a session's lifecycle and status events are delivered to the destinations its definition specifies, so no agent spends turns reporting its own status; a person watches a session through a tail under a read grant and follows records through a viewer that receives change notices and never history; whether a session may stop is decided at the stop hook; what is addressed to a person is published to that person's own topic by an adapter outside the core; and the core keeps no domain vocabulary, so a board, a task, or a document is a service's record that the platform never interprets.
- Reopened when: people assign work to agents in a tool they already use, that tool notifies other systems as each of its records changes, and people would keep using it (the adapted tool); or tens of people or more direct agents, an agent's standing configuration is documents that many people write and that are reviewed and approved before they take effect, or many people watch live activity at once and no tool they use can present it (the built platform). A system of the tenant's that assigns work but cannot call an API or receive events also reopens it, because a person would otherwise relay work and status between systems by hand.
- Requirements when reopened: spec/decisions/collaboration-platform/adapt.md (the adapted tool) or spec/decisions/collaboration-platform/build.md (the built platform); replaces: none.

### Declarative Root Sessions And A Reference Definition Source

- Deferred: the platform owning the list of root sessions that should be running: the tenant serves definitions from a source under its own change control; the platform's controllers read the source, validate and govern each version, materialize the desired set, and reconcile every partition's root sessions against it; and sessions adopt new versions at turn boundaries by cohort. This applies either to every root session (declarative) or to standing agents only, with imperative creation for root sessions started for one task (both). Also deferred: a definition source provided with the platform that serves definitions from a repository. The fixed default is imperative: a system of the tenant's creates a root session with its configuration through the API, reconfigures it, ends it, and reacts to its lifecycle events, and that system is the controller of the list.
- Kept possible by: a tenant's principal can create a root session and end it; every session records its origin; a parent supervises its children and a template its instances; whether a session that should be running is running is decided by a mechanism, never by a person checking; lifecycle events reach the owner of the list without the owner polling; a session's configuration changes only at a turn boundary; a change to the cached prefix is made through the provider's cache-preserving path; and the definition-source contract, under which any service that serves an index at a rising revision, immutable versions by digest, and a change feed is a source, so a tenant with definitions in a repository can serve the contract itself.
- Reopened when: no system of the tenant's owns the list of what should be running, because people start and stop agents by hand or nothing does, so the list must move to the platform; or an agent's configuration lives in a reviewed system that can serve it over the network and changes while agents are running; or a standing set of agents and root sessions started for one task coexist and the ad hoc root sessions have no definition (both); or a tenant whose definitions live in a repository has no source that serves the contract (the reference source, a product decision). A tenant system that cannot receive lifecycle events also reopens it, because the imperative default is then untenable.
- Requirements when reopened: spec/decisions/session-management/declarative.md and spec/decisions/session-management/declarative-only.md (declarative), or spec/decisions/session-management/imperative.md, spec/decisions/session-management/declarative.md, and spec/decisions/session-management/both.md (both); replaces: spec/decisions/session-management/imperative.md and spec/decisions/session-management/imperative-only.md (declarative), or spec/decisions/session-management/imperative-only.md (both).

### Where Agents Run Code: Existing Runners Or The Workspace Service

- Deferred: an execution environment for the shell commands, file edits, and builds an agent asks for: either runners the tenant already operates (a build service, a container service, or a pool of hosts) placed behind a tool server (existing), or the workspace service: disposable workspaces for shell, files, and builds, whose contents are an immutable value captured as a content-addressed snapshot after every call that changed them, so that a lost host loses at most the call in flight (build). Also deferred, once repository content reaches sessions: governing content that enters a session through a tool result or a forked transcript rather than through a workspace. The fixed default is none: agents run no code of their own, every tool they call is a service, and the specification adds no requirements about execution environments.
- Kept possible by: the platform hosts no programs for its tenants, so anything that runs code for an agent is a tool server listed in the definition and called from the partition owner's node with the session's assertion on every call; every dispatch carries the caller's lease generation, so a superseded owner cannot act; a tool spec identifies a credential by reference, and the credential broker leases a short-lived credential for one call and delivers it only to the executor; every outbound call leaves through the egress point, which admits only the destinations the binding lists; a service called on a session's behalf may exchange the call's assertion for credentials of its own at the broker; and every event carries its access scope.
- Reopened when: any agent runs a shell command or a build. A few agents at a time, on runners the tenant operates that receive credentials for each job and keep none between jobs, that run only work from teams allowed to see each other's data, and whose loss in the middle of a task is acceptable, selects the existing runners. Tens of agents or more running code at once, work in progress that must continue on another host after its host is lost, hosts that keep credentials at rest or are shared with teams that may not see each other's data, or no runner at all that could run an agent's commands, selects the workspace service. A tool result that carries repository content reopens the governance of content that enters a session outside a workspace.
- Requirements when reopened: spec/decisions/workspace-service/existing.md (existing runners) or spec/decisions/workspace-service/build.md (the workspace service); replaces: none.

### Human Approvals And Record-Based Autonomy

- Deferred: a standing approval path for the decisions that reach a person, in which a request is batched with an evidence summary, never goes to the operator who directed the work, approvers are measured by time to approve and by approval rate, and seeded known-bad requests reach approvers at a configured rate so that an approver who is not reviewing is detectable (human approvals); and, in addition to it, a temporal policy that widens what a role may do as its record of changes without rollback grows and narrows it after a rollback (record-based autonomy). The fixed default is checkers-only: authority is widened only by a sound checker running a gate spec that was approved once, the platform runs no approval path of its own, and a decision that no mechanism can decide is blocked with its reason and recorded as a candidate for a new checker.
- Kept possible by: agents never approve; authority is widened only by a sound checker or by a person the tenant's governance identifies; a decision reaches a person only when no mechanism can decide it; every decision is an event; and an approval path adds to the checkers and removes nothing from them.
- Reopened when: a person approves tens or more of an agent's risky actions on a typical day, or the tenant's policy reserves classes of action for a person's decision (a production release, spending above an amount, a change to access) and agents reach them daily or more often (human approvals); and, in addition, the operator wants a role's approvals to relax as its record of changes without rollback grows, without a person reviewing its history, and the deployment or review system records each revert of an agent's change against the change it undoes (record-based autonomy). A revert that is not recorded against its change keeps record-based autonomy closed, because a policy that could widen but never narrow is a widening change without evidence.
- Requirements when reopened: spec/decisions/authority-widening/human-approvals.md (human approvals), or spec/decisions/authority-widening/human-approvals.md and spec/decisions/authority-widening/record-based-autonomy.md (human approvals with record-based autonomy); replaces: spec/decisions/authority-widening/checkers-only.md.

### A Recovery Store Outside The Cluster

- Deferred: a recovery store outside the cluster that holds what a new cluster needs to resume every session: state and cursors, lease generation, timers, subscriptions, the key-value slot, pending inbox events, in-flight dispatch records, and the names of frozen blobs, each record an envelope under the tenant's keys, written at each freeze and at shutdown, conditioned on the epoch, and reached through one narrow interface with a simulator that nothing else in the platform reads. The fixed default is self-durable: the durable store keeps every committed write on storage that outlives its nodes, and a new cluster restores every session from the store itself or from its backup, which is the only restore source.
- Kept possible by: every part of a session's state is in the durable store; work is re-driven from committed state; every write is conditioned on an epoch, so a replaced cluster cannot write back; a new cluster restores every session from its restore source; and restores are drilled.
- Reopened when: a drill in which every node of the store's cluster is lost at the same moment shows that committed writes do not survive it; or the store keeps data only up to a scheduled backup whose interval is longer than the loss of recent work the operator accepts, or whose restore takes longer than the unavailability of sessions the operator accepts after a cluster is replaced. The freeze interval is then declared within the accepted loss window. When nothing survives and no loss of committed work is acceptable, the recovery store is adopted with the shortest freeze interval the store sustains, and the item is reopened again when a store that keeps committed writes through the loss of every node is available, with a full-cluster restart drilled before the recovery store is removed.
- Requirements when reopened: spec/decisions/durability/recovery-store.md; replaces: spec/decisions/durability/self-durable.md.

### The Specification As A Product

- Deferred: the specification as a product beside the platform: a requirement is an entity with a stable id that survives an edit, a move, a split, and a merge; a document is a versioned tree of typed blocks from which the requirement text is generated; a version moves from Draft through Preview and Target to Green by mechanism, when the trusted reports of every bound repository show no failures; publishing a version is governed, a widening change defers to an approver, and an author cannot approve a version that weakens a requirement; and the change gate decides on an evidence record from a trusted runner and never admits a change on the traceability check alone. The fixed default is gate-only: the requirement gate that every build runs is the whole of conformance checking.
- Kept possible by: every requirement binds to a citation that detects its violation; coverage needs both an implementation citation and a test citation; a cited test is witnessed by execution; a build with an uncovered mandatory requirement is not promoted; a requirement's identity is its quoted sentence; a gate run evaluates against an immutable snapshot; and the evidence-record contract gives evidence a form a mechanism decides on.
- Reopened when: a second team cites the specification from a separately built repository that releases on its own schedule, or a repository that the editing team cannot review cites it; or a designated approver outside the editing team must agree before a mandatory requirement is weakened or an exception is granted; or machine-checkable evidence beyond the gate's report (mutation-testing results, proofs, witnessed test runs) exists and a person reads it. The product needs a place to run, the collaboration product of its own entry under either outcome and workspaces for the trusted runner, so that entry is reopened with it or before it; without a trusted runner the default stays, because the product decides only on a trusted runner's record.
- Requirements when reopened: spec/decisions/conformance-product/product.md; replaces: none.
