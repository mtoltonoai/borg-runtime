# Glossary

> **What this document is.** The controlled vocabulary for the whole specification. Every term used
> normatively in the constitution, the contracts, the core specifications, and the outcome specifications is
> defined here once, so the specs use words consistently. This document is descriptive, not normative: it
> carries no requirements. The declared defaults that realize a term are recorded in [record.md](./record.md),
> never here. A term that belongs to one outcome of a decision says so at the end of its definition. A term that belongs to one outcome of the two decisions says "under the X outcome of decision Y". For the six fixed defaults (where people and agents work together, who owns the list of root sessions, where agents run code, how authority is widened, how the store survives losing every node, and how conformance is checked), a term that belongs to the default says "under the fixed default X", and a term that belongs to an alternative says "under the X alternative of the deferred entry T", where T is the entry in spec/decisions/deferred.md whose last line lists the files that hold the requirements; the default kept and its alternatives are listed in spec/record.md part A.

---

## The platform and its parties

- **Platform**: the multi-tenant agent harness this specification defines: it runs sessions, delivers
  events, calls models, dispatches tools, governs at fixed hook points, and persists state, for tenants
  who supply everything else as configuration and as services.
- **Core**: the part of the platform that executes and enforces. It decides no policy that configuration can supply: policies, prompts, tools, models, deciders, thresholds, and triggers are data it reads.
- **Tenant**: the trust and data boundary: everyone who could ever be allowed to see a piece of data,
  together with their agents, sessions, keys, and grants. A team or organization that onboards is a
  tenant; a project is not, and organizes work inside one.
- **Operator**: a person who directs agents' work and on whose behalf sessions may act. Under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform", an operator is a person who resolves into the tenant's operator team, writing from an authenticated session.
- **Person**: a human principal, authenticated by the people identity provider.
- **Service**: a non-human principal reached over the network and authenticated by request signing or
  by the platform's assertion: tool servers, definition sources, hook destinations, sibling products.
- **Agent**: an identity whose behavior a definition describes and whose sessions the platform runs. An agent is a definition principal, never a person or a service, whatever starts its sessions; a person who starts one appears in the chain.
- **Agent identity**: the durable record of one agent, which outlives any one of its sessions; every session carries the identity it runs as, and one identity may have several sessions at once.
- **Node**: one process of the platform on one host. A **cluster** is the set of nodes and durable-store
  replicas serving one stage. A **cell** is an independent cluster behind a tenant router. A stored value of the workspace service is not called a cell in this specification.
- **Stage**: a deployment environment with its own cluster and resources, from development through
  production; under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform", each platform stage pairs with a
  stage of the collaboration platform.
- **Shared framework**: the library of generic components that the platform and any product built beside
  it (a collaboration platform, a workspace service) build on; a generic mechanism any of them needs is built
  there rather than in a product.
- **Platform operator**: a principal that holds a configured operator role of the platform's own operating account and is admitted to the operations that act on a tenant as a resource (setting its caps, reading its usage); the set of operator roles is configuration, never a whole account. Distinct from an operator, who directs agents' work.

## Sessions, partitions, and the durable store

- **Session**: one agent identity's continuous transcript and state for its lifetime, however long.
  Which root sessions exist is decided by their owner through the fixed default for who owns the list of root sessions (imperative) or an adopted alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source", never by the core; sub-sessions, forks, and keyed instances are created by their
  parent or their template.
- **Transcript**: the ordered record of a session's turns; the model's view of it is derived from the
  durable log and never edited in place.
- **Session state**: the durable record of a session: pinned definition version, phase, cursors, lease
  generation, timers, subscriptions, key-value slot, pending inbox, in-flight dispatch records, and the
  names of its frozen ranges.
- **Durable store**: the replicated, transactional store holding every session's state and inbox, with
  atomic multi-key commits, read-validation of selected keys, and a watch that wakes a reader when a key's
  version advances. Anything in a node's memory is a cache of it.
- **Partition**: the unit of ownership: a fixed slice of the session space owned by exactly one node at
  a time, which is the single writer of its sessions' state. A session's partition is a function of its
  id alone.
- **Partition space**: the set of partitions, sized by a configured number of bits of the session hash,
  grown by splitting each partition into exactly two children.
- **Actor**: a unit of state and behavior that exactly one partition owns at a time and that handles its messages in order under that partition's fence; sessions and fan-out actors are actors of the platform's partitioned actor set.
- **Partitioned actor set**: the shared-framework component that hosts a set of actors across a fixed set of partitions: one coordinator per set assigns the partitions to nodes by a placement policy, each partition's ownership is fenced by generation, and a node's partitions can be drained.
- **Coordinator**: the one component per partitioned set that assigns partitions to nodes from a
  consistent view of membership, by a pluggable **placement policy** under which a node's arrival or
  departure moves only the partitions it gains or gives up. It is off the critical path.
- **Generation**: the increasing instance number a partition owner commits under; every commit checks
  it, so a superseded owner's next commit fails.
- **Fence**: any check of a generation, epoch, or lease generation that stops a superseded actor from taking effect, so that correctness does not depend on the superseded actor detecting that it was superseded.
- **Lease**: a time-bounded claim that lapses unless renewed.
- **Doorbell**: a per-partition key to which a publisher appends a session id in the same transaction
  as the inbox append, so the partition's watch wakes for exactly the sessions with work.
- **Inbox**: a session's own ordered queue of events, applied in order under the session's **cursor**,
  the position below which everything has been applied.
- **Watermark**: the doorbell position below which every session listed in the doorbell has applied every event.
- **Residency**: whether a session's resident form is loaded on its owner: a session loads when it has work, stays while busy or recently active, and is **evicted** lowest priority first.
- **Frozen**: the state of a transcript range, or a whole idle session, whose bytes have moved to the blob store and been replaced by a pointer. The **transcript head** is the bounded unfrozen tail.
- **Index element**: a frozen session's pointer and timers, stored inside its partition's index key
  rather than as keys of their own.
- **Keyed instance**: a session created from a keyed template by the first event addressed to its key, and
  identified by its tenant, its template, its instancing form, and its publisher-scoped instance key. A keyed instance of a definition is called an instance; a template's instance cap bounds how many exist.
- **Instance key**: the publisher-scoped key that, with the template and the instancing form, identifies a keyed instance: the pair of the publisher's authenticated principal and the key it supplied, so that no publisher reaches another publisher's instance.
- **Origin**: what a session records about the party responsible for its existence: a caller, a
  definition, a keyed template, or a parent session.
- **Root session**: a session whose origin is a caller or a definition, as opposed to a child or an
  instance. "Creating a root session" covers both a caller's call through the API and, under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source", the platform materializing a root session from a
  definition a source serves.
- **Platform-managed session**: a sub-session, a fork, or a keyed instance, owned by its parent or its
  template under every outcome.
- **Keyed template**: a configuration plus a key scheme, from a definition or from a caller's
  registration, from which the first event addressed to a key creates an instance.
- **Supervisor session**: a session that holds a read grant on another session and receives that session's gated activity as inbox events as the activity occurs, without slowing the observed session and seeing only output that has passed the observed session's deciders.
- **Memory**: a record an agent keeps across its sessions, keyed by its agent identity; it can be written by the agent through a tool call or recorded by a mechanism that observes the session, and a memory recorded by observation carries the access scope of the transcript range it came from.
- **Transcript pipeline**: a mechanism that reads a session's gated activity as it is published and records values derived from it, such as memories, without a turn of the observed session.
- **Owner of a root session**: the party responsible for deciding whether the session should exist: the
  caller's principal or the definition's controller.
- **Lifecycle event**: a status event the platform delivers about a session: started, adopted a version,
  woken, idle, blocked, waiting on a defer, stopped, failed, quarantined, or out of budget.
- **Incarnation**: one life of a session id, from its creation to its end, counted from one and rising by one each time the id starts again; carried in call ids, in the assertion as a claim of its own, and in blob ownership, so that a service can refuse a dead incarnation's late calls.
- **Tombstone**: the record an ended session whose id can recur leaves behind, carrying its last lease generation and tail position, so that the next incarnation starts above both.
- **Pending-close record**: the record an ending incarnation leaves when it still has tool calls open, from which a close and a reconcile are resent for each call while its outcome is unknown and its cutoff has not passed.
- **Close**: the message by which the platform tells a tool server that it abandons a call id, so that the server refuses a later admission of that id and reports the call's outcome if it ran.
- **Turn boundary**: the point before a turn's model request is built: at the start of a run, or once every result of the previous turn has committed, and never between a tool call and its result. Adoption of a configuration change, the compaction swap, a served pause, and events that waited below the interrupt threshold take effect there.
- **Safe point**: the first point in a step at which an emergency control or an interruption takes effect: a model attempt in flight is abandoned, and a dispatched tool call is left to finish under the fence.
- **Run**: the sequence of turns from the wake that takes a session out of idle and ending when it returns to idle or blocks.
- **Position**: an event's place on its topic, or an entry's place in a session's tail, carried with the lineage epoch it belongs to and compared only within that epoch.
- **Forwarded call**: a call that the node that authenticated a caller makes to the owner of another partition on the caller's behalf, carrying the caller's verified principal and chain unchanged in a claim of its own, the **forwarded claim**; the owner decides on that pair, never on the forwarding node's authority.

## Events and delivery

- **Event**: an opaque payload with a topic, a priority, a dedupe id, an access scope, and the publisher identity the platform recorded.
- **Topic**: an opaque name, scoped within a tenant, that events are published to.
- **Topic group**: the tenant-held group every topic belongs to, by which a topic is addressed together with its name; the group carries the ordered deciders asked before a subscriber is added to or removed from any of its topics, a publish gate, and the retention bound and default event-kind filter its topics inherit, and a topic carries no policy record of its own. The collaboration platform's term for it is a channel group.
- **Topic shard**: one of the queues and subscriber indexes a topic splits into once its subscribers exceed one fan-out batch's bound; a subscriber stays in one shard.
- **Publish**: appending an event to a topic for fan-out to every subscriber. **Send**: delivering an
  event to one session, or to a keyed template with its key.
- **Subscription**: the record that a subscriber receives a topic's events: standing (from a
  definition), dynamic (managed on a session's behalf), or viewer (following a page).
- **Subscriber**: a session or a viewer.
- **Fan-out**: delivery of one event to every subscriber's inbox and doorbell, in batches bounded by the
  store's transaction size.
- **Notification**: an event as it arrives at a session, compared with the session's **interrupt threshold**: the dynamic priority a notification must exceed to interrupt work in progress rather than wait for the next turn boundary.
- **Priority**: a number the core carries on sessions, events, timers, and work; it orders everything
  that queues. Classes with names are an adapter convention.
- **Causal parent**: the event that caused another, identified on every delivered event.
- **Access scope**: the readers an event's source allowed, carried on the event and propagated as a
  union onto everything derived from it.
- **Dropped-count marker**: what an inbox keeps, per topic, in place of events discarded past its cap,
  so nothing is dropped silently.
- **Viewer**: one live connection from one person's page, opened with a short-lived **viewer token**;
  it subscribes to topics as the page changes and receives **change notices**, small bounded payloads, a
  burst of which collapses to one reload notice per topic.
- **Tail**: a live stream of one session's gated activity, read from the transcript extended with tail-only entries (gated output, tool calls, bounded scrubbed results, decisions, lifecycle, usage) and addressed by positions that survive compaction, freezing, an owner move, and a planned restore; a **tail token** admits one viewer to one session's tail, opens one stream, and ends it at expiry.
- **Ingress buffer**: a queue outside the platform that buffers publishes while the cluster is unreachable.
- **Staged upload**: bytes an uploader placed under a tenant-owned holder through the API, referable only by a publish or send of the same principal and chain within a window measured on the owner's clock, and expiring with the holder's pin.
- **Event kind**: the member of the closed set of lifecycle and status events a hook delivers (started, adopted a version, woken, idle, blocked, waiting on a defer, stopped, failed, quarantined, out of budget); a subscription may filter by it.

## Turns, models, and context

- **Turn**: one model request and its reply, with the tool calls the reply makes. A **step** is the unit
  of durable progress within it. A **model attempt** is one request, identified by an attempt id that is
  its idempotency key.
- **Provider**: the party whose model answers, through a **model endpoint**; each provider has a wire format the platform renders to and decodes from.
- **Capacity source**: an account, a region, and a model endpoint reached through a role. A tenant's record configures its capacity sources; a request for a tenant with none configured is refused.
- **Scheduler**: the per-node component every model call passes through: it admits by priority then
  deadline, adapts concurrency to throttles and latency, and routes across capacity sources.
- **Latency class**: interactive, background, or batch (slower and lower-cost, result as an inbox event).
- **Quota share**: a node's leased portion of a model's quota, rebalanced by demand, with reservations
  per priority class.
- **Prompt cache**: the provider's cache of a request prefix. The **cached prefix** is the leading part
  of a request the cache can serve; the **cache lifetime** is how long it survives idleness; a **refresh
  read** keeps it warm across an expected short gap.
- **Breakpoint**: a marker where a cached prefix may be split; the platform places them at segment ends
  so client and provider cache boundaries coincide.
- **Segment**: one part of a rendered model request, rendered as a unit from its content in the provider's wire format; a cache breakpoint is placed only at a segment boundary, and turn metrics split by segment kind.
- **Render version**: the version of the rendering that produced a segment; a rendered segment depends only on its content, the provider's wire format, and the render version, so that it is cached by content and shared across sessions.
- **Compaction**: replacing the transcript before a cut point with a summary while keeping the tail
  verbatim; **background** when run as a separate request, or **inline** when the provider returns it beside a reply, swapped in at a turn boundary either way.
- **Keep set**: the parts of a session's context that a compaction keeps verbatim after the cut, beside the recent tail: plans, open items, unresolved defects, constraints, decisions, the identifiers the work depends on, and the session's pending timers with their notes.
- **Soft and hard thresholds**: the context sizes at which a background compaction starts and at which
  compaction becomes a precondition of the next turn.
- **Rollback**: removing failed attempts from the model's view only; the durable log keeps them.
- **Trimming**: evicting stale tool results from the model's view behind a placeholder the model can
  resolve by call id.
- **Provider state**: opaque provider-bound content (reasoning, compaction items) the platform
  round-trips unmodified.
- **Charter**: the prompt components of a definition that state an agent's identity and purpose.
- **Cohort**: the fraction of a definition's sessions that adopt a new version first (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Harness**: a program of the operator's that runs an agent's model loop: it builds model requests, reads
  replies, calls tools, and keeps its own context between requests. Distinct from the platform, which under
  the direct outcome of decision execution runs that loop itself.
- **Step boundary (under hosted harness)**: the point at which a harness process has no model call and no tool
  call in flight, commits its working state through the platform in the stored form, and receives the inbox
  events that arrived; the platform schedules, delivers, and persists at step boundaries and treats the
  harness as opaque between them (under the hosted-harness outcome of decision execution).

## Tools and dispatch

- **Tool**: a function a model may call; its **spec** (name, description, schema) comes from the server
  that provides it, and its **semantics** (idempotent, side effects, timeout, retry, cancellable,
  reconcile binding) default to the server's annotations and can only be made more conservative.
- **Tool server**: a service that lists tools and answers calls over the tool protocol.
- **Loaded definition**: the definition version as a session loads it; in particular the list of tool servers and tools its sessions may call.
- **Call type**: a tag a tool spec declares (for example a board comment, or a definition write) that
  hook policies are written against, so the core keeps no domain vocabulary.
- **Dispatch**: resolving a tool call against the loaded definition, passing the tool-call hook, recording it, leasing
  credentials, running it, passing the tool-result hook, and committing the result.
- **Call id**: the dedupe key of a tool call end to end, given to the executor and to any target that
  honors one.
- **Dispatch record**: the durable record of a call, committed before the call is sent.
- **Outcome unknown**: the result returned to the model for a side-effecting call interrupted before its
  outcome was recorded.
- **Reconcile**: asking a tool server whether a call id's effect took effect.
- **Native tool**: a tool the platform implements itself because it acts on the platform's own state.
- **Sub-session**: a session started by another session, narrowed by the call, whose lifecycle returns
  to its parent's inbox. A **fork** is a sub-session that starts from its parent's transcript up to the
  fork point.
- **Egress point**: the single path a party's outbound calls leave through. The platform's egress point admits only destinations registered with the tenant; a tool server such as the workspace service has an egress point of its own for the processes it runs.
- **Workspace**: a disposable environment for shell, files, and builds, provided by the workspace service as a
  tool server listed in the definition (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Runner**: a host or service the tenant already operates that runs a tool's command or build, placed
  behind a tool server so that every call carries the session's identity (under the existing alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Non-final result**: a tool's answer that its work continues, which keeps the dispatch record open and matchable by call id while the final result has not arrived and the call's horizon has not passed.
- **Call horizon and cutoff**: a call's first dispatch deadline plus the outcome-unknown window, after which a service may drop its receipt; the **cutoff** is the horizon less a margin for the assertion lifetime and clock skew, and the platform presents the call id to no service after it.
- **Step-up**: a tool server's refusal for insufficient scope, delivered to the model as a request that the operator whose authority the session would use widen the grant; the answer is recorded as a narrow, short-lived grant.
- **Listing**: a tool server's list of tools as a node holds it, refreshed on an interval or on the server's announcement; a step whose server fails to list runs with the last listing.
- **Experiment fork**: a fork a principal with a grant takes from a session at any step of its transcript, whose every tool call passes the tool-call hook and then goes to the experiment's interceptor rather than to the tool's server; it never acts on an external system, is readable as a tail by the experiment's principal, and is discarded when the experiment ends.

## Governance

- **Hook point**: one of the fixed places at which the core evaluates policy before acting: model request, model
  output, tool call, tool result, notification arrival, compaction, stop request, control operation.
- **Decision request**: what the core builds at a hook: principal and chain, hook and call types, the
  content, and the session's usage, with a size bound; at the notification-arrival hook it also carries a bounded **activity descriptor** (the session's phase, the work invested in the current step, the expected time to the next turn boundary, and the session's priority) and no wider session context.
- **Decider**: a component that maps a decision request to a decision. Kinds: pattern, schema (validates the subject against a declared schema in process), classifier,
  program (sandboxed), policy (declarative and analyzable), judge (a model call), session decider (an ephemeral session started to answer one request, a checker that includes a model), external (a service or
  a person).
- **Pipeline**: the ordered deciders a definition lists per hook; low-cost deterministic ones first, and a
  block stops evaluation of the rest.
- **Decision (of a decider)**: the verdict a decider returns at a hook point: allow, flag (proceed with an
  event), block (with a reason), defer (hold the call while a decision is awaited), or transform (proceed
  with a patch). Distinct from a decision of the specification, defined under Decisions.
- **Defer**: a held call awaiting a decision, with a decision id, the principals allowed to resolve
  it, a deadline, and an outcome on timeout.
- **Stop admission**: the stop-request hook's decision of whether a session may stop or has remaining work.
- **Shadow mode**: a decider evaluating and recording without enforcing, so its readiness to enforce is
  measured.
- **Episode**: a recorded fail-retry-pass sequence, a shadow result, or an operator override, which
  feeds back as training labels.
- **Widening change**: a configuration change that increases what a session may do or weakens a check;
  **narrowing** decreases it; **neutral** does neither.
- **Governance tier**: the class of operations that change policy, grants, source bindings, and the
  decider catalog; a tier above it changes governance policy itself.
- **Bootstrap bundle**: the trust root a tenant is created with: default policies, the identity adapter's authority,
  first grants, catalog pins, and source bindings with their upper bounds. It also carries the tenant's governance-policy set, the grants for the emergency controls, the principals allowed to invoke the emergency override with their quorum and the override's expiry, the call-type tags of the never-automate set, and the tenant's defaults for the definition fields a version omits.
- **Emergency override**: a fully audited path, checked by the core itself, that replaces a policy that blocks governance from operating.
- **Gate spec**: an approved statement that a class of change may proceed when specified evidence holds, run where the agent cannot access it.
- **Evidence**: a checker's own record (proved, refuted, or unknown, with bounds and who ran it), never
  an agent's account of it.
- **Sound checker**: a checker that includes no model, the only kind that may widen authority.
- **Approver**: the person the tenant's governance designates to approve a widening change to policy, grants,
  or a gate spec; under the human-approvals alternative of the deferred entry "Human Approvals And Record-Based Autonomy", also the person a
  batched request that no mechanism can decide is routed to, never the operator who directed the work.
- **Seeded request**: a known-bad request sent to approvers at a configured rate, so that an approver who
  approves everything is detected (under the human-approvals alternative of the deferred entry "Human Approvals And Record-Based Autonomy").
- **Record-based autonomy**: a temporal policy that widens what a role may do as its record of changes without rollback grows and narrows it after a rollback (under the record-based-autonomy alternative of the deferred entry "Human Approvals And Record-Based Autonomy").
- **Substrate check**: a check the core runs at a hook as its own mechanism rather than as policy; no tenant or definition configuration can remove or reorder it, it never runs in shadow, and it always fails closed.
- **Subject**: what a decider is asked about: a tool call's arguments, a stop's principal, a configuration change as it would be stored with its proposing principal, or a subscribe.
- **Cost class**: the grouping of decider kinds by evaluation cost that orders a pipeline.
- **Blocked cycle**: an attempt that took no effect because its output or every tool call it made was blocked; consecutive blocked cycles per session are capped across turns.
- **Authored-for set**: every human principal in a session's chain and the requester, none of whom may resolve a defer on that session's change.
- **State slot**: a program decider's per-session state, stored with the session's state and committed with the transition the hook gates.
- **Effective lifecycle**: the most restrictive of a session's served lifecycle, the emergency controls in force at its scopes, and any quarantine the platform placed.
- **Stop request**: a turn that ends with the provider's end-of-turn signal, no tool call, and no unapplied inbox event, so the session would go idle; decided at the stop-request hook.
- **End**: the primitive that ends a session: its final observation is published, its transcript frozen, and its subscriptions and timers released.

## Identity and delegation

- **Principal**: the identity an action is attributed to: a person, a service, a definition (identified by
  its tenant and its name, and by its source under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source"),
  or a session. Ids carry their identity provider and their tenant.
- **Chain**: the on-behalf-of sequence a session acts under (operator, concierge, sub-agent), carried
  with every call and event. Each entry carries its relation, delegated (the party passed its authority) or attested (a service vouched for the party), and its attester; a session acts for a person only through delegated entries, and an attested entry enters a caller only through a verifier.
- **Requester**: the principal whose event a shared session is acting on; the **effective authority** is
  the intersection of the session's grants and the requester's.
- **Grant**: a record of delegated authority: who delegates, to which session or definition, which
  actions and resources, its expiry, conditions, and tenant. A sub-session's grants are references to
  its parent's.
- **Upper bound**: the maximum of what a role, binding, or definition may ever be granted, including its maximum priority.
- **Platform-level grant**: a grant held by the platform's own operator principal rather than by a tenant's
  principal, covering the operations above any one tenant, such as registering a tenant (under the
  registration outcome of decision tenancy-scope).
- **Assertion**: a short-lived signed statement of a session's principal, chain, tenant, scopes, lease
  generation, and call id, bound to one call and audience, presented at trust boundaries.
- **Credential broker**: the service that exchanges an assertion and a credential reference for a
  short-lived credential scoped to the principal, chain, and target, delivered only to the executor.
- **Credential reference**: the name by which a tool spec requests a credential without holding it.
- **Identity adapter**: the higher-level component that maps external identities to principals and
  writes grants.
- **Attribution**: the operator identity carried into every call a session makes for them, so the
  external audit trail identifies the operator. The operator carried into a target's audit trail is the outermost delegator of the session's chain.
- **Calling actor**: the platform component on whose behalf the platform makes a call of its own, with no session; such a call carries a call id that can never equal a session's call id, unique per calling actor within the receipt retention, and its audit record identifies the calling actor beside the call id.
- **Clearance**: a grant that lists the domains a session may see; a repository carries a **domain**. A session keeps a domain set that only grows, recording the domain of every repository that enters any of its workspaces; a lineage carries its session's set, and a continuation, restore, or fork is refused when the session's clearance does not cover it (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Human-only action**: an action the tenant's policy reserves for a person; a session reaching one does not perform it, and what becomes of the decision depends on the fixed default for how authority is widened (checkers-only) or on an adopted alternative of the deferred entry "Human Approvals And Record-Based Autonomy".
- **Self-approval risk**: the risk that the operator who directed agent-authored work also approves it.
- **Consent**: an operator's recorded agreement that a session use the operator's own authority for an action, which reaches that operator; distinct from an approval of an agent-authored change, which never goes to the operator who directed the work (under the human-approvals alternative of the deferred entry "Human Approvals And Record-Based Autonomy").
- **Scope (authorization)**: the unit of authority a grant confers and a tool requires, carried in the assertion and used by the broker to select a credential profile; distinct from an access scope on an event.

## Tenancy, keys, and storage

- **Root key**: a tenant's key in the key service (the tenant's own, or one the service holds for it)
  from which **branch keys** are derived per epoch and random **data keys** per resource.
- **Envelope**: a resource's ciphertext together with its wrapped keys and authenticated data binding it
  to its owner and range.
- **Trusted key list**: the node's configured set of permitted root keys; a stored value never determines a key.
- **Decrypt cache**: the per-node cache of plaintext keys whose lifetime is the revocation window.
- **Blob store**: the owner-scoped store of immutable, encrypted blobs addressed by the hash of their stored
  bytes, backed by an object store with a node-local cache.
- **Owner**: the one session, tenant-owned holder, or product service that wrote a blob; the owner's writer, under the owner's generation, is the only writer of the blob's pins, transfers, and key deletion. A **pin** is an explicit shared reference with a holder and an expiry.
- **Holder class**: the class of holder a pin records beside its scope and its own expiry, written by the owner's writer in the commit that gives out the reference; the classes are the implementer's to define, and none admits another tenant.
- **Decider catalog**: a tenant's versioned entries for the deciders its definitions may reference, each written by a governance-tier operation and pinned by version, declaring the decider's kind, its artifact by content id, the hooks and call-type scope it may run at, the decision set it can return, what part of the request it reads, and whether it is stateless, stateful, or temporal; beside it, the platform's own pins for substrate checks. Code, models, and weights enter the platform only as catalog entries; a definition carries data and may only narrow an entry.
- **Retention**: tenant configuration, per kind of data, for how long it is kept; deletion is key
  destruction.
- **Usage record**: one record per model attempt, tool call, or decider call, carrying tenant, chain,
  session, cause, outcome, cache reads and writes, prompt size, and definition digest.
- **Observation record**: a pointer to a transcript range published to a queue when a session crosses a
  threshold, with who, why, and how much.
- **Audit event**: a record of a decision or an issued credential, holding ids and decisions but never
  content, kept apart from session data.
- **Owner key**: the one key, wrapped under the tenant's branch key, under which an owner's data keys are wrapped; its key object is the only stored copy, so destroying it deletes the owner's content.
- **Release location**: a store that holds platform-published content and no tenant data, from which a tenant's own writer copies that content, verified against the content ids fixed in the platform's reviewed build.
- **Content id**: the hash of the bytes a content's consumers can compute and verify: the canonical plaintext for a kind a party outside the platform computes (a definition version, a prompt component, a document version, a catalog artifact), and the stored ciphertext for every private kind; it resolves only within its tenant.
- **Cited reference**: the short readable form people write for a record; it resolves only within the caller's tenant and is never reissued.
- **Durable id**: the self-describing id of a record or a blob, unique across tenants, carrying its kind code (an entity id) or its content-type code and hash function (a content id), with one canonical text form whose prefix shows the kind; codes come from one append-only registry and are never reused.

## Definitions and sources

- **Definition**: the desired configuration of one agent identity: prompt components, model and
  parameters, tool servers, deciders, budgets, deadlines, priority, standing subscriptions and schedules,
  compaction, cache, observation settings, hook destinations, lifecycle, and instancing.
- **Version**: an immutable definition addressed by its digest; versions form a parent chain.
- **Definition source**: a service bound to one tenant that serves definitions over the definition
  contract: an index at a rising revision, versions by digest, and a change feed (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Binding**: the governed association of a tenant with a source, with its credential and upper bounds
  (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Controller**: the platform's per-binding component that reads the index, validates versions, passes
  them through governance, and materializes the desired set (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Desired set**: the sessions a tenant's definitions call for, which partitions reconcile against (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Lifecycle intent**: a definition's run, paused, or retired state, as served by its source (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Instancing**: singleton, or keyed by requester or by a publisher's key.
- **Adoption**: a session switching to a newly accepted version at its next turn boundary (under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Standing schedule**: a definition's recurring timer, created with each instance.
- **Session policy**: a tenant-owned record, referenced by name, of settings a session inherits; a session's configuration lists policies, whose settings apply in list order with the session's own fields last, and a change to a policy applies to each listing session at its next turn boundary (under the fixed default imperative or the both alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Handback**: the list, served by a definition source in the index entry of a paused singleton, of the event ids and dropped ranges it has taken back since the pause; cumulative, and applied by the session's partition only to events pending at the pause (under the declarative or both alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Source fault**: a served index, feed, entry, or body that breaks the definition-source contract or a binding's guard; a fault changes no session and raises one alert per binding (under the declarative or both alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Held retirement**: a served retirement past the binding's fraction, which the platform applies as a pause pending a governance-tier confirm of the held set by its digest (under the declarative or both alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").
- **Accepted state**: a binding's record of each definition's base version, candidate version, last rejection, and status, from which partitions reconcile (under the declarative or both alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source").

## Timers

- **Timer**: a deferred event to its own session's inbox, durable, with a name unique within the session,
  fired exactly once per **occurrence** by the session's partition.
- **Guard**: a condition (held events on topics, silence on topics, a slot value, or one decision hook)
  that must hold for a fire to wake the model.
- **Flex**: how late an occurrence may be delivered so that it shares a wake.
- **Deadline heap**: a partition's one ordered set of its sessions' earliest durable deadlines.
- **Empty wake**: a wake in which the model made no side-effecting call, send, or publish.
- **Wake budget**: the cap on timer-caused empty wakes per session, counted over a configured window that opens at the session's first counted wake; at the cap the fire waits in the inbox and the session stays idle rather than blocked.
- **Stale limit**: the time past its due time after which a timer's late occurrence is discarded with an event.

## The frontend

- **Frontend**: the one authenticated API through which anything calls the platform,
  scoped to a tenant and passing the control-operation hook.
- **Control operation**: start, adopt, pause, stop, resume, quarantine, reconfigure, or redirect a session;
  internal primitives, exposed externally only as emergency controls.
- **Emergency controls**: pause, stop, quarantine, and resume per session, definition, or tenant,
  working when every service outside the platform is unreachable.
- **Webhook**: an authenticated inbound endpoint, mapped to a principal, through which an external
  system publishes.
- **Event hook**: an outbound delivery of a session's lifecycle and status events to a destination its
  definition specifies, from the session's durable outbox.
- **Hook destination**: a per-tenant registered endpoint referenced by name.
- **Attachment**: a content type and blob reference carried beside a payload, rendered into a turn
  without the core reading the payload.
- **Cap (quota)**: a per-tenant bound the frontend holds a tenant to on a shared cluster: sessions, concurrent model steps, each session's queue depth, message rate, and topic groups. A cap is a runtime setting a platform operator writes on the tenant's record, never a constant in code; no cap applies to a tenant unless one is set; usage against every capped quantity is counted and published whether or not a cap is in force.
- **Runtime setting**: a limit on the platform's own operation (a cap, a cadence, a timeout, a pool size) that a platform operator changes through the API without a redeploy or a restart and that a node applies without restarting; a setting that takes effect only when a node starts is node configuration instead.

## Failure, durability, and operations

- **Idempotency key**: the identifier every side effect carries so it can be redone safely.
- **Quarantine**: setting aside a session whose state cannot be decoded, or an event that fails repeatedly,
  so that the partition continues.
- **Re-drive**: the next owner continuing a step from the committed state after a crash or move.
- **Recovery store**: a store outside the cluster, adopted under the recovery-store alternative of the deferred entry "A Recovery Store Outside The Cluster", that holds what a new cluster needs to resume sessions.
- **Epoch**: the increasing number a lineage of clusters commits under, so a replaced cluster cannot
  write back.
- **Drain**: the orderly stop of a node or cluster: readiness withdrawn, pending work applied, sessions
  frozen, final records written.
- **Restore**: a new cluster taking over a lineage from backup, or from the recovery store (under the recovery-store alternative of the deferred entry "A Recovery Store Outside The Cluster").
- **Health signal**: a process's own report that it may be sent requests.
- **Deployment control plane**: the system that creates, patches, replaces, and deletes clusters.
- **Lineage (cluster)**: the succession of clusters that commit under one epoch sequence and sign under one signing root; a cluster that replaces another continues its lineage. A **lineage id** is a lineage's public identifier, carried in each signing key's identifier so a verifier can bound which lineage's keys it trusts; it reveals nothing about the tenant or the deployment's purpose.
- **Stage resources**: the resources of a stage that outlive any cluster: the blob store, the tenant root keys, the signing roots, the recovery store, and the frontend's stable name and access log.
- **Open point**: the instant a restore has written back every record and starts the cluster's partitions; the reference time for every restored pin, and the time a restored session learns from the event it receives.
- **Capture**: a periodic write of a partition's or a tenant's records to the recovery store or a backup, from which a restore reads; distinct from a workspace snapshot.
- **Reachability probe**: a node's periodic exercise of a dependency's request path, not only a connection, whose result feeds the node's own readiness.
- **Set aside**: the state of a queued message, a timer fire, or a schedule element whose stored form cannot be decoded or unsealed and that is recorded with an event rather than allowed to stall its queue; the messages behind it proceed. On the collaboration platform, also a task status that an operator's own write sets, which removes the task and its descendants from open work (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").

## Simulation and conformance

- **Simulator**: a substitute for an external dependency under deterministic simulation, injecting the
  faults the real dependency exhibits. The **entropy source** supplies inputs and faults; the
  **simulation framework** explores interleavings.
- **Contract test**: one set of expectations run against a simulator and, through a **probe**, against the real dependency, so that the simulator cannot diverge from it.
- **Scenario**: a simulated run asserting an invariant; a **property test** generates sequences and
  checks a reference model.
- **Citation**: a marker in code or test that identifies the requirement it satisfies by its identity.
- **Requirement gate**: the check that every requirement is cited by an implementation and a test that
  exercises it. **Execution witness**: proof that the cited test and code ran.
- **Trusted runner**: a runner, outside any agent's reach, that executes a checker and signs the evidence
  record of what it ran; a gate decides on that record and never on an agent's account of it.

## Durable formats

- **Tag**: the identifier an evolvable durable format gives each field and each variant, never reused for another meaning after the field or variant is removed; a decoder preserves a field whose tag it does not recognize through a read and a rewrite.
- **Fallback variant**: the variant an enumeration in a durable format declares that a reader reads an unknown variant as, chosen so that a reader can hold it without acting on it wrongly; an enumeration may declare that it has none.

## Decisions

- **Decision (of the specification)**: a choice where valid solutions differ and the operator's situation
  decides which is right; each has a document under spec/decisions/ and two to four outcomes. Distinct from
  a decision of a decider, defined under Governance.
- **Outcome**: one resolution of a decision, with its own file of requirements that apply only when the
  outcome is adopted.
- **Default outcome**: the outcome a build proceeds on when the operator has not answered within the wait
  period; it is the assumption behind each question's recommended option. It applies to the specification's own decisions; under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform", a blocking question posed on the collaboration platform takes no default and states its recommended option instead.
- **Wait period**: the stated time the implementer waits for the operator's answers to a batch of questions
  before proceeding on the default outcome; the irreversible decision, tenancy-scope, has no wait period and blocks until answered.
- **Screener question**: a question about the operator's situation whose answer rules outcomes out;
  screeners are asked first, together, and independently of each other.
- **Effect**: the one-line statement on each option of a question of what choosing that option rules out
  or favors.
- **Recommended option**: the one option per question marked as the asker's assumption, taken as the
  answer when none is given; it never describes a person doing by hand what a mechanism should do.
- **Decision record**: the file spec/record.md: the outcomes adopted, the answers given, the declared
  defaults, and the gate selection.
- **Implementer-decides list**: what the building agent settles without asking: the names of resources
  and fields, the layout, the libraries, numeric defaults inside declared bounds, and the order of work.
- **Imperative management**: the fixed default for who owns the list of root sessions, in which a system of the tenant's
  owns the list of root sessions through the API and reacts to lifecycle events.
- **Declarative management**: the alternative held by the deferred entry "Declarative Root Sessions And A Reference Definition Source", in which the platform owns the
  list of root sessions by reconciling it against definitions a tenant's source serves.
- **Execution (decision)**: the decision of how a session's model turns run: the platform makes the model
  calls itself, or it hosts and supervises a harness the operator already runs and that harness makes them.
- **Direct execution**: the outcome of decision execution in which the platform owns the model loop: it builds
  each request from the durable log, schedules and routes the call, gates each completed block, dispatches
  tool calls, compacts, and manages the prompt cache.
- **Hosted harness**: the outcome of decision execution in which the platform runs the operator's harness as a
  session's executor, one supervised process per session on a host the tenant operates, and keeps every
  invariant at the boundary the harness crosses: process lifecycle, the stored form, the credential broker,
  the egress point, dispatch, the inbox, and the API.
- **Deferred decision**: a decision that is asked only when its trigger occurs, kept possible by an
  invariant in the core; the register is spec/decisions/deferred.md.

## The collaboration platform (deferred alternative; spec/decisions/collaboration-platform/)

- **Collaboration platform**: where people and agents work together, with boards, documents, chat, a wiki,
  and questions; toward the platform it is a tool server and an adapter, and a definition source under the declarative alternative of the deferred entry "Declarative Root Sessions And A Reference Definition Source" (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Task**, **document**, **question**, **channel**: the collaboration platform's records; every change
  to one publishes to that record's topic (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Composer**: the only writer of definition versions in the collaboration platform, composing a definition version from approved document versions (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Decision queue**: the one queue in which everything that needs a person waits (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform"). It is computed per viewer from every open decision routed to the viewer directly or through a team, and every document in the viewer's operator review, blocking first and then oldest, rather than kept as a stored list.
- **Revision**: the collaboration platform's gapless per-tenant counter that orders its event log (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Wait**: a task's dependency on a resolver (a task, a question, a document's operator review, or a review record) whose resolution ends it; a task is blocked only while it has a wait, and a wait on an agent, a team, or an external party is refused (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Go-ahead**: a recorded positive answer with typed fields (control, scope, seat, expiry) that a gated action requires. **Gate (collaboration platform)**: a control on a task's transition that requires a go-ahead; a gate on starting work covers descendants (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Moot**: a mark on an answered question that removes its answer's effect without creating one (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Operator team**: the tenant's governed team whose members are operators; only persons resolve into it, only an operator changes it, and a write that would remove the last operator is refused (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Project coordinator**: a principal holding the coordinator right on a project, which may cancel or supersede its open questions and add gates, and to which a project's overdue events are delivered; distinct from the partition coordinator (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").
- **Open work**: the tasks whose assignee may not stop: todo or in progress, not archived, with no blocked or set-aside ancestor and no open child (under the build alternative of the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform").

## The workspace service (deferred alternative; spec/decisions/workspace-service/)

- **Handle**: the name by which a session refers to a workspace it holds under a lease (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Snapshot**: a content-addressed capture of a workspace's contents after a call that changed them;
  snapshots form a **lineage**, and a **swap** restores one onto any host (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Per-call credentials**: credentials served to a call's processes only while the call runs (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Workspace type**: an image and a network **placement**, registered under one name, that a workspace is
  provisioned with (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Stored value**: a named, versioned, immutable value the workspace service keeps for one session, each version recording what produced it; distinct from the session's key-value slot (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Workspace lease**: the service's time-bounded claim on a workspace by a session, ended after the idle period with a running command counting as use; distinct from the session's lease generation that the assertion carries (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Workspace incarnation**: one materialization of a workspace on a host, counted so that a snapshot commits only against the live incarnation and a lost host's late writes are refused; distinct from the session incarnation (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Writer generation**: the counter under which a lineage's one writer instance acts, checked in the service's records before every host operation; distinct from the session's lease generation, the session incarnation, and the workspace incarnation (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Promise (workspace host)**: a host's recorded commitment to the service, made in answer to a message and written before the answer is sent; a host that reboots or whose promise record is damaged is treated as a lost host, and a host that cannot record a promise answers with a retryable error and makes no new promise (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").
- **Interrupted (workspace)** and **stopped by the host**: two of the closed set of a call's endings; an interrupted call's host was lost, so its effects inside the workspace are gone, while a call the host ended by itself keeps its effects in the covering snapshot (under the build alternative of the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service").

## Specifications and the change gate (deferred alternative; spec/decisions/conformance-product/)

- **Specification**: a document whose requirements are entities that code and tests cite one by one (under the product alternative of the deferred entry "The Specification As A Product").
- **Requirement block**: a typed block with a stable id, level, polarity, statement, conditions,
  parameters, relations, and rationale, from which the requirement's text is generated (under the product alternative of the deferred entry "The Specification As A Product").
- **Pin**: the exported form of a specification version a repository commits and cites against (under the product alternative of the deferred entry "The Specification As A Product").
- **Trusted run**: the requirement gate run by the pipeline with the base commit's configuration (under the product alternative of the deferred entry "The Specification As A Product").
- **Failure**: one requirement-level gap the gate reports: unimplemented, untested, not executed,
  stale, orphaned (under the product alternative of the deferred entry "The Specification As A Product").
- **Exception**: a recorded, reasoned decision not to implement a requirement in one repository (under the product alternative of the deferred entry "The Specification As A Product").
- **Lifecycle (specification)**: Draft, Preview, Target, Green, Superseded, Abandoned (under the product alternative of the deferred entry "The Specification As A Product").
