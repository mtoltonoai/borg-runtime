# The Decision Record

> **What this document is.** The record of this deployment's choices: which outcome each decision adopted and on what answers (part A), the declared defaults that the requirements leave to configuration (part B), and the rule for which files the gate lists (part C). A value here is configuration, not a requirement. The constitution, the contracts, the core specifications, and the outcome specifications mention no engine, store, provider, library, numeric width, or path; where behavior depends on one, the requirement says "the configured X" or "the declared default", and the value is recorded here. This document is descriptive: it carries no requirements. Changing a value here is a configuration change, not a specification change, unless a requirement's wording depends on it.
>
> These are the defaults a conforming realization adopts; an operator confirms or adjusts them as [decisions/README.md](./decisions/README.md) states: the two decisions by their questions, the fixed defaults by the deferred register's triggers, and the declared defaults by reading part B and adjusting any value in one reply. A deployment records the concrete products that back each category; this document lists categories and open standards, not a vendor or an account.

---

## Decisions Adopted

The two decisions are described in `spec/decisions/`, and the method by which an operator settles them is in [decisions/README.md](./decisions/README.md). Each row below carries the decision's default outcome, assumed in the absence of an operator's answers to the decision's questions; a row changes when an operator confirms or adjusts it.

| Decision | Outcome adopted | Files listed in the gate | Answers | Confirmed by | Date |
|---|---|---|---|---|---|
| [tenancy-scope](./decisions/tenancy-scope.md) | one-tenant | `spec/decisions/tenancy-scope/one-tenant.md` | defaults assumed | unconfirmed by an operator | unset |
| [execution](./decisions/execution.md) | direct | `spec/decisions/execution/direct.md`, `spec/decisions/execution/direct-model-turns-and-scheduling.md`, `spec/decisions/execution/direct-context-compaction-and-cache.md` | defaults assumed | unconfirmed by an operator | unset |

When an operator confirms or adjusts a decision, replace its row: the outcome adopted, the files that outcome lists (as its decision document's Recording section states them), the answers given (question number and option), who confirmed, and the date.

### Fixed Defaults

Six choices are fixed defaults rather than decisions, and no question is asked about them. Each has one default outcome whose requirements are in the gate; its alternatives are held by one entry of [decisions/deferred.md](./decisions/deferred.md), whose trigger reopens it, and that entry's last line lists the files that enter the gate and the files they replace.

| Fixed default | Default outcome | Files in the gate | Deferred entry that reopens it |
|---|---|---|---|
| Where people and agents work together | none: the tenant's own systems assign work, change configuration, and read status through the frontend, webhooks, event hooks, and tails (no additional requirements) | none | A Collaboration Product: An Adapted Tool Or A Built Platform |
| Who owns the list of root sessions that should be running | imperative: a system of the tenant's creates a root session with its configuration through the API, reconfigures it, ends it, and reacts to its lifecycle events | `spec/decisions/session-management/imperative.md`, `spec/decisions/session-management/imperative-only.md` | Declarative Root Sessions And A Reference Definition Source |
| Where agents run code | none: agents run no code of their own, and every tool they call is a service (no additional requirements) | none | Where Agents Run Code: Existing Runners Or The Workspace Service |
| How authority is widened | checkers-only: authority is widened only by a sound checker running a gate spec approved once, and a decision no mechanism can decide is blocked with its reason and recorded as a candidate for a new checker | `spec/decisions/authority-widening/checkers-only.md` | Human Approvals And Record-Based Autonomy |
| How the store survives losing every node | self-durable: the durable store keeps every committed write on storage that outlives its nodes, and a new cluster restores every session from the store or its backup | `spec/decisions/durability/self-durable.md` | A Recovery Store Outside The Cluster |
| How conformance is checked | gate-only: the requirement gate that every build runs is the whole of conformance checking (no additional requirements) | none | The Specification As A Product |

When a trigger occurs and an alternative is adopted, replace its row: the alternative adopted, its files, who confirmed, and the date. The declared defaults in part B are confirmed by the step [decisions/README.md](./decisions/README.md) describes under "Confirming The Declared Defaults"; until then each carries the value assumed for the situation its parenthesis states.

---

## Declared Defaults

A default whose parenthesis begins "depends on" states the situational fact its value rests on; the value carried here is the one assumed for the situation the record describes, and an operator's adjustment at the confirmation step replaces it, marked with who confirmed it and the date. A default marked "applies only ..." is in force only when part A adopts the outcome, or the deferred register's alternative, that it refers to.

### Durable substrate

- **The durable store** is a transactional key-value store with durable actors: atomic multi-key commits, read-validation of chosen keys, and a watch that wakes a reader when a key's version advances.
- **The shared-framework components** (the partitioned actor set, reads at the woken version, the deadline queue, the per-node simulated wall clock, envelope encryption, the blob store, the change feed, the live-stream hub, rate limiting, caller authentication, and the write-ahead log) are built against that store and shared across the products.
- **Object store** behind the blob store and, when the workspace service of the deferred register is adopted, the snapshot store: an object store with a node-local cache; which object store is an assumption to confirm.
- **Partition bit count** k = 12 (4,096 partitions) by default, node configuration, split by doubling (depends on how many root sessions and instances run at once at peak, and on how soon after first use the population reaches that peak).
- **Transaction key bound** 100 keys per transaction, advisory; it follows from the store's own limit, and the confirmation step checks that fan-out at peak stays within it.
- **Per-session key count** two keys for an active session (the inbox queue; and state, config, and cursor together); a frozen session is an element of its partition's index key (depends on how many sessions run at once at peak; an assumption to confirm).
- **Transcript head bound** 64 KiB, chosen to match the inbox entry size; the store serves values to 128 KiB without a cost step, and the whole state value stays under 120 KiB: the 64 KiB head, an 8 KiB list of tail-only entries, a 4 KiB budget for decider state slots, and the rest for frozen-range pointers and in-flight records.
- **Inbox cap** 10,000 pending events for a session that is not draining, a high value that bounds a session that never drains rather than normal operation; past it the inbox keeps a dropped-count marker per topic (depends on how many sessions run at once at peak; an assumption to confirm).
- **The recovery store** (applies only when the recovery store of the deferred register is adopted) is a wide-column store attached to the cluster, carrying sessions across a cluster replacement; its **write interval**, within which a dirty timer change reaches it, is 60 seconds (depends on how much recent work may be lost after a crash; outcome-bound).
- **Partition hash** the leading 64 bits of a collision-resistant hash over the session id's binary form, read as a big-endian integer, the partition being its top k bits; the hash is part of the durable format, so changing it is a migration.
- **Capacity step threshold** the alarm threshold on which a capacity step is triggered: about one hundred partitions per node, so that k = 12 serves up to about 40 nodes and the space splits to k + 1 when the node count passes that (depends on how many sessions run at once at peak).
- **Index element bound** near 100 KB in the worst case (timers at their limits about 97 KB, a frozen pointer about 1.5 KB) and near 115 KB for a session whose state cannot be decoded; a tombstone is under 128 bytes.
- **Assignment repair grace** 10 seconds before a node starts a partition assigned to it that is not running, fenced on the generation it observed.
- **Coordinator hold** the coordinator holds assignments when the membership view has lost more than one half of the current owners; the bound on the hold is an assumption to confirm.
- **Store watch** returns empty at 45 seconds and never misses a write that came first; a doorbell watch is kept alive within a 10-second idle bound.
- **Recovery-store item bound** (applies only when the recovery store of the deferred register is adopted) the store's own item size limit, about 400 kilobytes; a larger record spans several items or is written to the blob store with the item keeping a pointer (outcome-bound).
- **Restore open bound** 1 hour: every partition opens within it of the open point, and no restored pin gains a holder or a duration beyond it.
- **Recovery store protection** (applies only when the recovery store of the deferred register is adopted) point-in-time recovery kept for 35 days; the store is protected against deletion other than its planned removal (outcome-bound).
- **Values left to measurement** (assumptions to confirm) the attempt count at which an event quarantines its session, the freeze threshold, the warm window, the close resend backoff (bounded by the call's cutoff), the cap on appends one transition makes to other sessions, the decision-request size bound per hook, the deadline per decider kind and per decider cost class, and the episode retention period.

### Models and the inference layer

- **Providers** the inference layer renders to two provider wire formats behind one provider-neutral transcript, reached through a model-inference service (depends on which providers serve models).
- **Capacity sources** the tenant's own capacity sources, configured on the tenant's record; the platform holds no model credentials, and a request for a tenant with no configured source is refused (operator question 1 pending).
- **Provider endpoints and limits** the model endpoints of each capacity source, and each provider's per-connection concurrency limit and request body cap: assumptions to confirm (depend on which providers serve models).
- **Transport** a multiplexed, connection-oriented protocol with bearer-token authentication by default, request bodies streamed, no request or response compression on the native routes.
- **Compaction method** the provider's native compaction, with the platform's own summarizer as a fallback.
- **Prompt-cache lifetimes** a short and a long lifetime where the provider offers them; breakpoints at the provider's cap, placed at segment ends (depends on the prompt-cache lifetimes the providers serving models offer).
- **Compaction thresholds** the hard threshold is 80 percent of the model's context window; the soft threshold starts at a quarter of the window and is confirmed by measuring quality on a cohort (depends on the context windows the providers serving models offer).
- **Model catalog** a per-model table of the facts a request depends on (context window, maximum output, and whether the model supports tool use, streaming, reasoning, prompt caching, and image input), kept per region for every model the platform routes to, so a configuration a model cannot run is refused when submitted; how the table is produced is an assumption to confirm.
- **Connection pool to a model endpoint** one connection of 128 streams per provider per node by default.
- **Per-tenant provider cache** 1,024 tenants per node, evicting the entry used least recently.
- **Refused-role retry bound** 60 seconds before a tenant-supplied role that could not be assumed is tried again, unless the tenant's configuration changes first.

### Keys and encryption

- **Cipher modes** an authenticated cipher with 256-bit keys for values, and a deterministic, nonce-misuse-resistant authenticated cipher for immutable blobs.
- **Key hierarchy** a root key per tenant in a managed key service (the tenant's own, or one the service holds for it), branch keys per tenant per epoch under a service-held root, and a random data key per session.
- **Hash** a collision-resistant hash over stored bytes identifies a blob and keys content.
- **Decrypt-cache lifetime** five minutes, which is the revocation window.
- **Compression scheme** for frozen transcript ranges and recovery records: a block-compression frame format whose decoder is a published standard, so that the encoder can change without a format change; the encoder is the standard one or the platform's own, which passes encrypted reasoning bytes through as literals; a reader accepts the uncompressed form as well.
- **Epoch length** of a branch key: an assumption to confirm.
- **Key-service client** a pool idle timeout under the service's idle-close window, a new connection before its per-connection request cap, a connect timeout under a second, a hedge in the tens of milliseconds, and a deadline of one to two seconds; keys resolve when a session's doorbell wakes it.
- **Pins** a pin recorded with a reference bridges 7 days; a controller's pin on a definition version lapses 1 hour after the version is neither base nor candidate; a pin element holds at most 100 entries and an observation pin at most 1,500 blob names; a lapsed pin answers not found. When the workspace service of the deferred register is adopted, the service's pins on a source expire at 7 days and renew at half (outcome-bound).
- **Blob segments** 8 MiB; a blob larger than one segment is segments each addressed by its own bytes plus an index whose name is the blob's name.
- **Content ids** the plaintext digest for a definition version, a prompt component, a document version, and a catalog artifact; the ciphertext digest for every other kind; an entity id is 20 bytes and 33 characters of text, a content id at most 39 bytes and 64 characters, with the type letter the fourth character of the text.
- **Root key destruction waiting window** the time a tenant's root key destruction waits before it takes effect: an assumption to confirm.
- **Plaintext copy destruction bound** the time within which every plaintext copy a host holds is destroyed after its key; an assumption to confirm, and when the workspace service of the deferred register is adopted the host's own bound is the host lease (outcome-bound).

### Identity

- **People** authenticate through the people identity provider; **services** through request signing; **agents** through the platform's signed assertion.
- **The assertion** is a short-lived asymmetrically signed token valid for at most 300 seconds, carrying its issue time and its expiry in whole seconds; a verifier allows 60 seconds of clock skew on the issue time and none on the expiry, so a request is admissible for at most 360 seconds after issue; a chain has at most 3 entries and a principal id at most 64 bytes; the signing key is an asymmetric key held in the managed key service, a node's signing key rotates at least every 6 hours, and verifiers read the public half at start and on a refresh interval rather than per call.
- **Acting for operators** goes through an on-behalf-of exchange where a target system accepts it, and as the platform's own identity otherwise.
- **The operator identity** is carried into a role session in a colon-free form of at most 64 characters, so the external audit trail identifies the operator.
- **Verification key set** published per lineage; a verifier keeps a cached set no older than 60 seconds and drops any key the set no longer lists; a lineage not seen before is loaded at most once per second across all lineages; a new signing key is readable at the published location for a 90-second publication lead before it signs; a key-set object not rewritten for 7 days expires; a lineage id is 1 to 63 lowercase letters, digits, and hyphens, starting with a letter and not ending with a hyphen; a refresh that fails keeps the set read last.
- **Credential profiles** a bounded set selected by the call's scope, read-only and read-write; the default is read-only; a served credential's claimed expiry is about one minute, refreshed during a long call.
- **Broker refresh margin** an upstream credential held per tenant, operator, destination, and scope set is replaced five minutes before its expiry.
- **Store connection credential** 15 minutes; connections are retired on the same schedule, so revoking the role ends every connection within that bound.
- **Peer credential rotation interval** the interval at which the credential a node presents to its peers is rotated: an assumption to confirm.
- **Definition identity** a name of lowercase letters, digits, dot, underscore, and hyphen that starts with a letter or digit; a binding id of lowercase letters, digits, and hyphens; the two together at most 52 bytes.
- **Platform operator roles** the configured set of roles of the platform's operating account admitted to operator operations; no whole account is admitted.

### Governance

- **Decider engines** pattern deciders on a linear-time regular-expression engine; a classifier as a small cross-encoder without an accelerator behind a deterministic pre-filter; program deciders as sandboxed, fuel- and memory-bounded modules; policy deciders as a declarative, analyzable policy engine, with a temporal extension that applies only when record-based autonomy of the deferred register is adopted; a judge through the inference layer; and an external decider over the network.
- **Decider hosting** the builtin deciders and the policy engine run behind the same decider interface as every other decider; whether they run as sandboxed modules or as part of the platform's own code is an assumption to confirm.
- **Builtin deciders** structured-secret detection, a check for characters outside the basic character set and for emphasis, typed-reference checks, and an attribution check.
- **Decision-request size bound, per-decider cut-denies, and retry budgets** are per-session configuration with platform defaults.
- **Hook deadline bounds** the time a decider pipeline may take at each hook and the longest a defer is held: assumptions to confirm.
- **Decision-request retries** at most two, with a short backoff, inside the decider's deadline, for transport and server-side failures only.
- **Program decider state slots** at most 4 KiB per session in total, apart from the transcript head.
- **Blocked cycles** an in-loop retry budget of 3 attempts per output, the first included; a cap of 5 consecutive blocked cycles per session across turns.
- **Open defers per session** 8; a new defer past it is a block.
- **Emergency override** a quorum of one operator by default; a policy replacement made by override expires after 24 hours unless re-made through the governance-policy tier; the window within which each invoking principal confirms the same replacement by digest is an assumption to confirm.
- **Never-automate list, default** changing a version set, changing a pipeline, deploying, automated remediation of security findings, and creating or deleting a package or an application.
- **Stop admission** at most 20 consecutive blocked stops per session; an external stop-admission decider's deadline of 1 second; a decision latency of 20 milliseconds at the 99th percentile.
- **Step-up surface** (applies only when the built collaboration platform of the deferred register is adopted) a step-up question is posed on the collaboration platform and routed to the operator whose authority the session would use (outcome-bound).
- **Step-up grant lifetime** the lifetime of the narrow grant an operator's step-up answer becomes: an assumption to confirm.

### Timers

- **Minimum intervals** an unguarded recurring timer at most every 15 minutes; a guarded one every 5 minutes with a decision hook, and every minute with only local conditions (depends on whether the providers offer a prompt cache and for how long, because an unguarded fire is a model call whose cost depends on whether the cache is warm).
- **Per-session caps** 32 timers, 8 of them recurring; 8 schedules per definition; 4 timers per other owner and 16 from other owners in total; a name at most 64 bytes; a note at most 1 KiB; at most 4 guard conditions with at most 1 hook, a hook argument at most 256 bytes, 8 topics per condition, a slot prefix or key at most 64 bytes and a value at most 128 bytes, and a guard at most 512 bytes in total (depends on how many sessions run at once at peak).
- **Clock-skew bound** a step over 1 second is an event; a disagreement with the store's commit timestamps over 30 seconds holds a session's timer fires (depends on the clock skew between nodes the deployment tolerates); the per-owner clock disagreement bound is at most half the assertion clock-skew allowance.
- **Backoff after empty wakes** begins after 2 consecutive empty wakes, doubles the effective interval, and is capped at the lesser of 8 times the interval and 24 hours, never below the scheduled interval.
- **Wake budget** 24 timer-caused empty wakes per session per 24-hour window.
- **Resolution and lead time** timers resolve to 1 second and calendar schedules to 1 minute; a first occurrence lies within 400 days.
- **Flex** 1 minute for a once timer, a tenth of the interval up to 15 minutes for an interval schedule, 5 minutes for a calendar schedule.
- **Stale limit** none by default.
- **Guard hook** a deadline of 2 seconds; asked again at most once per 5 minutes.

### Failure handling

- **Retry budgets, backoff schedules, and attempt counts** per failure class, with platform defaults: assumptions to confirm.
- **Output-format retry attempts** the number of times a model reply that fails its output format is requested again: an assumption to confirm.

### Events and the frontend

- **Event payload bound** 64 KiB per inbox entry, attachment references counted inside it and the platform's own fields added on top, whatever producer wrote the entry; **dedupe window** 24 hours from the first attempt, per publisher principal and destination, equal to the outcome-unknown window, with a 64-id duplicate guard per session as a second check; a caller's first-attempt time may lead the node's clock by at most 60 seconds.
- **Frontend transport, request caps, and trace format** a multiplexed, connection-oriented protocol, per-request body and rate caps, and the trace context carried on every reply: assumptions to confirm.
- **Event-hook retry schedule and signing keys** a backoff with jitter up to a bounded number of attempts, and a per-tenant asymmetric signing key with rotation: assumptions to confirm.
- **Attachments** at most 20 per event and 16 MiB in total; one upload at most 16 MiB; a topic publish keeps a 7-day pin on its attachments as the bridge to every receiver's copy.
- **Staged uploads** a staged upload may be referred to within 24 hours of its commit and its pin lasts 7 days; a tenant holds at most 1,000 staged uploads by default and 5,000 at most; a node takes at most 32 uploads in flight; a slot is counted for 7 days and 15 minutes, with the owner committing within 12 minutes of the reservation; staging is a data-plane operation at tenant scope with its own size bound and rate class. Tool servers' staged outputs are counted apart: 1,000 by default and 5,000 at most per tenant; a server's staged copy lives 1 hour and its result may refer to it within 30 minutes of the commit.
- **Topics and subscriptions** a topic name at most 64 bytes, scoped to its tenant; a tenant id at most 64 bytes; at most 128 subscriptions per session; a fan-out transaction delivers to at most 32 subscribers within the transaction key bound; a topic splits into shards once it holds more than 60 subscribers; a topic group name is between 1 and 32 characters, and a group name and a topic name together are at most 63 bytes.
- **Topic retention** how long a topic keeps an event for a subscriber that has not passed it, sized against the pause and quarantine limits: an assumption to confirm.
- **Tail and viewer tokens** a tail token and an assertion live at most 300 seconds; a tail ends at its token's expiry with an end event; tail-only entries occupy at most 8 KiB in the state value beside the head.
- **Frontend connection lifetime and access log** a connection lifetime of 10 minutes, below which every long-poll bound and keepalive interval on the frontend's path stays; an access log of each request's authenticated principal, method, path, and response code, retained at least 365 days.
- **Webhooks** a dedupe window of 24 hours by default and at most 7 days; at most 10,000 live delivery records per endpoint by default and 100,000 at most; at most 400,000 per tenant; at most 16 endpoints per tenant.
- **Session listing page** 50 sessions by default, 200 at most.
- **Per-tenant caps** none in force by default for sessions at once, concurrent model steps, each session's queue depth, message rate, and topic groups; when set, the message rate defaults to 120 a minute; each cap is a per-tenant runtime setting written by a platform operator, counted across the cluster, and takes effect on every node from the tenant's next request; usage against every capped quantity is published whether or not a cap is in force, so that the cap values recorded here are chosen from published usage.

### Tools

- **Tool protocol** the protocol over which a tool server lists its tools with their annotations and answers calls; a server's entry may state the protocol revision, with negotiation as the default. **Result bounds** an inline result bound of 16 KiB, which a definition may only lower, a preview of at most 2 KiB on a result returned by reference, and the full output stored once and paged.
- **Tool-server retries and deadlines** three attempts with a doubling backoff from 200 milliseconds for a connection or listing request, within a call deadline of 60 seconds by default and a listing deadline of 10 seconds; a per-server setting overrides them, and one attempt turns retries off; longer work returns its result later as an inbox event.
- **Tool listing lifetime** 15 minutes unless the server announces a change; a failed listing backs the server off from 10 seconds, doubling, to at most 10 minutes.
- **Call guards** a call deadline upper bound of 4 hours; an outcome-unknown window of 24 hours; a service keeps every receipt for at least their sum, 28 hours, and 7 days by default; a cutoff margin of 420 seconds (the 300-second assertion lifetime plus twice the 60-second skew allowance); both bounds are platform constants rather than per-tenant configuration.
- **Dispatch record caps** 16 open dispatch records per session; 16 outcome-unknown records per session; 32 tool servers a session may have dispatched to at once.

### Retention and loss window

- **Retention periods** transcripts for the session's lifetime and 90 days after it ends; audit events and usage records for 13 months, in sinks of their own apart from session data; deletion is key destruction; a tenant overrides each default per kind (depends on how long each kind of data must be kept; assumptions to confirm).
- **Freeze interval** 1 minute; a crash loses at most what the last freeze did not cover (depends on how much recent work may be lost after a crash).
- **Usage-record delivery interval** 1 minute from a partition to the usage sink (depends on how much recent work may be lost after a crash).
- **Observation triggers and queues** a record is published when a session crosses a size threshold, ends, compacts, or is asked to; the threshold and the target queue are assumptions to confirm.
- **Usage and audit sinks** sinks of their own apart from session data; which sinks is an assumption to confirm.
- **Workspace lineages and snapshots** (applies only when the workspace service of the deferred register is adopted) a retention kind of their own, kept for the tenant's configured period after the session ends (outcome-bound).
- **Observed-content copy bound** the time within which a copy made of observed content becomes unreadable once its source is deleted: an assumption to confirm.

### Scale and deployment

- **Scale target** hundreds of thousands to millions of sessions, hundreds of operators across dozens of projects, with thousands of viewers (depends on how many root sessions and instances run at once at peak and how many people watch live activity at once).
- **The deployment control plane** operates the platform as its own cluster type, never sharing a cluster with another workload.
- **Epoch form** the increasing number a lineage of clusters commits under: an assumption to confirm.
- **The first tenant** (applies only under the one-tenant outcome of the tenancy-scope decision) is bootstrapped with a stage's first cluster rather than registered; its **identifier** is an assumption to confirm (outcome-bound).
- **Stages** development, two pre-production stages, and production; when the built collaboration platform of the deferred register is adopted, each is paired with a stage of the collaboration platform.
- **Cluster health** a node reports its health to the deployment control plane every 10 seconds; a cluster whose reports have stopped for 30 seconds is unhealthy; a create is bounded by a 60-minute stabilization timeout and retried at most 10 times one minute apart; readiness is every partition assigned plus a canary round trip within the configured bound.
- **Node readiness bound** the time within which a node of a deployment becomes ready: an assumption to confirm; the 60-minute stabilization timeout above is the nearest existing value.
- **Canary round-trip bound** the time within which the canary round trip completes for a cluster to be reported ready: an assumption to confirm.
- **Reachability probe** a cadence of 10 seconds and a bound of 5 seconds.
- **Drain deadline** the bound on the wait for requests and steps in flight during a drain: node configuration, an assumption to confirm.

### Definitions and sources

Applies only when the deferred item "Declarative Root Sessions And A Reference Definition Source" in [decisions/deferred.md](./decisions/deferred.md) is adopted.

- **Canonical serialization and digest** as the Contracts section below states them; **long-poll bound** the longest a change-feed request is held, below the connection lifetime of any intermediary between the source and the platform: an assumption to confirm.
- **Bounds** at most 10,000 definitions per binding, which is also the default, a higher value refused; a source guard holds when 20 percent of a binding's definitions are removed within a sliding 1-hour window reset by a full index read, or retired cumulatively since the last confirm; a source outage alerts after 60 seconds; source backoff is full jitter from 1 second doubling to 300 seconds; a rollout fraction is an integer in parts per million; a handback carries at most as many event ids as the inbox cap and at most 145 dropped-count ranges, and a lower value needs a contract version increment; a version body is at most 256 KiB with at most 64 components, each at most 1 MiB and at most 4 MiB in all; an instance key is at most 256 bytes.
- **Rollout halt margin** the margin by which the candidate cohort's quarantine or block rate may exceed the base cohort's before the platform halts the rollout: an assumption to confirm.

### The collaboration platform

Applies only when the built collaboration platform of the deferred item "A Collaboration Product: An Adapted Tool Or A Built Platform" in [decisions/deferred.md](./decisions/deferred.md) is adopted.

- **Store** a relational database that runs on many servers; **live updates** an outbox at a gapless per-tenant revision with a doorbell and a server-push event stream; **deployment** a container service behind a load balancer with its own pipeline; **search** the database's own full-text and trigram matching; **identity providers** the people identity provider for people and request signing for services.
- **Concurrent viewers** the number of viewers the collaboration platform serves at once: thousands; an assumption to confirm (depends on how many people watch live activity at once; outcome-bound).
- **Tasks** a task is at most 8 levels below its top-level ancestor; a done task is archived after 7 days untouched and a todo aged out after 14 days in the projects the tenant chooses, never a set-aside task; the intake dwell deadline is about 120 seconds (outcome-bound).
- **Document versions** every version is kept by default; a tenant that prunes keeps every current, approved, pinned, and definition-referenced version and the most recent ones younger than a configured age (outcome-bound).

### The workspace service

Applies only when the workspace service of the deferred item "Where Agents Run Code: Existing Runners Or The Workspace Service" in [decisions/deferred.md](./decisions/deferred.md) is adopted.

- **Substrate** a host substrate that runs tools unprivileged; keeps them off the host's metadata endpoint; confines egress to one point; terminates a call's processes when the call ends; creates process identities without a person at the host; keeps a call's process tree across an agent restart; exposes a boot id; keeps its clock within the skew bound after a suspend; gives the agent local storage that tool identities cannot fill; isolates each workspace's processes in a namespace of their own; and authenticates the host agent with a per-host credential.
- **Snapshot store** content-addressed packs chained to the previous snapshot, kept in the object store above; **credential protocol** a per-call container-credentials endpoint with short claimed expiries.
- **Leases and deadlines** a workspace idle period of 30 minutes per type with a running command counting as use; a linger of 10 minutes after release; a host lease of 5 minutes per lineage, under which a service outage sure to stop no command is 80 seconds and a silent host is found within about 8 minutes; a host stale window of 90 seconds; a hold window of 30 seconds (at most 120, lowerable to zero); a call deadline of 30 minutes (at most 4 hours); acquire 300 seconds; reconcile and close 10 seconds; cancel and reads 30 seconds; write, edit, and search 60 seconds; rollback and release 120 seconds; each per workspace type.
- **Remote deadline late bound** how late a deadline a remote host enforces may take effect after its due time: an assumption to confirm (outcome-bound).
- **Output and snapshots** an output cap of 64 MiB per stream with a replay retention of 15 minutes; a late result attaches at most the last 8 MiB of each stream, 16 MiB per event, and states how many bytes it left out; a restore reads at most 20 blobs, with a cumulative pack every 12 snapshots and a base move once the cumulative pack passes a quarter of the base; snapshot and restore run within 20 percent of the raw version-control steps on the same substrate.
- **Restore verification retries** 1: a restore whose verification fails is retried once on another host, then fails (outcome-bound).
- **Hosts and credentials** 4 workspaces per host by default; a host's store credential is per lineage and lives 15 minutes; a served credential's claimed expiry is about one minute, refreshed during a long call, under an upper bound the service fixes.
- **Programs and stored values** per run by default 5 seconds of processor time, 256 MiB of memory, 16 output values, 50 nested calls, and 64 MiB of nested answers; the maximum is 60 seconds and 1 GiB; a definition may only lower them; two concurrent runs per session; a stored value version is at most 64 MiB.

### The requirement gate

- **The requirement-gate tool** extracts every key-word sentence and maps it to the code and test that cite it, exporting a stable, content-addressed form that citations resolve against.
- **Export format** (applies only when the specification as a product of the deferred register is adopted) the exported, content-addressed form of a specification version that each repository commits and cites against: an assumption to confirm (outcome-bound).

### Simulation

- **The simulation framework** drives every component deterministically; **the entropy source** supplies inputs and faults; a contract test pairs each dependency's simulator with a probe against the real dependency.
- **Seeded interleavings** 1,000 iterations per case; 100 for flood cases; each case states its count.

### Contracts

- **Canonical serialization** a profile of the standard canonical form of the text interchange format: object fields ordered by the bytes of their names, applied recursively; only the quotation mark, the reverse solidus, and the control characters below 0x20 escaped, with the short two-character escapes where they exist and lowercase four-digit escapes otherwise, and every other character, including the delete character, the line and paragraph separators, and the solidus, written raw; integers only, with no floating-point value and no null (a field is present or omitted); lowercase booleans; opaque bytes as padded base-64 text, an embedded digest as the base-64 of its raw bytes and never a display rendering; a defined order for every array, with index entries ordered by the raw bytes of the definition name.
- **Digest** a 256-bit collision-resistant hash over the canonical bytes, computed once by the producer.
- **Integer fields** each declared as a number bounded at 2 to the 53rd power minus 1, refused above it on encode and decode, or as a decimal string; source-chosen counters (revisions, version numbers, lease generations) and any 64-bit generation are decimal strings; times are whole seconds.

---

## The Gate Selection

The gate configuration is `.duvet/config.toml`. Its requirement side is maintained by hand and lists exactly these files: `constitution.md`, every file under `spec/contracts/`, every file under `spec/core/`, the outcome files of the outcomes part A adopts for the two decisions, and the files of the six fixed defaults, and no other. Every other outcome file is present in the configuration with a leading `# ` on each line of its block, so that the full set of outcome files is visible and a selection is changed by moving comment markers rather than by writing paths. The descriptive files (this record, the overview, the glossary, the traceability map, the readmes, the decision documents, and the deferred register) carry no requirements and are not listed. The source side is the globs of whatever implementation cites the requirements.

The selection this record states, which the configuration carries:

- Active (32 files): `constitution.md`; the nine contracts `spec/contracts/signed-assertion.md`, `spec/contracts/definition-source.md`, `spec/contracts/decision-hook.md`, `spec/contracts/event-hook.md`, `spec/contracts/event-envelope.md`, `spec/contracts/tool-call.md`, `spec/contracts/usage-record.md`, `spec/contracts/evidence-record.md`, and `spec/contracts/stored-form.md`; the fourteen core files `spec/core/sessions-and-partitions.md`, `spec/core/events-and-subscriptions.md`, `spec/core/failure-handling.md`, `spec/core/tools-and-dispatch.md`, `spec/core/governance.md`, `spec/core/identity-delegation-and-secrets.md`, `spec/core/tenancy-and-confidentiality.md`, `spec/core/encryption-and-blobs.md`, `spec/core/timers.md`, `spec/core/the-frontend.md`, `spec/core/observation-and-usage.md`, `spec/core/durability-and-deployment.md`, `spec/core/simulation-and-conformance.md`, and `spec/core/shared-framework.md`; the adopted outcome files of the two decisions `spec/decisions/execution/direct.md`, `spec/decisions/execution/direct-model-turns-and-scheduling.md`, `spec/decisions/execution/direct-context-compaction-and-cache.md`, and `spec/decisions/tenancy-scope/one-tenant.md`; and the files of the fixed defaults `spec/decisions/session-management/imperative.md`, `spec/decisions/session-management/imperative-only.md`, `spec/decisions/authority-widening/checkers-only.md`, and `spec/decisions/durability/self-durable.md`.
- Commented out (14 files): `spec/decisions/execution/hosted-harness.md`, `spec/decisions/tenancy-scope/registration.md`, `spec/decisions/tenancy-scope/cells.md`, `spec/decisions/collaboration-platform/adapt.md`, `spec/decisions/collaboration-platform/build.md`, `spec/decisions/session-management/declarative.md`, `spec/decisions/session-management/declarative-only.md`, `spec/decisions/session-management/both.md`, `spec/decisions/workspace-service/existing.md`, `spec/decisions/workspace-service/build.md`, `spec/decisions/authority-widening/human-approvals.md`, `spec/decisions/authority-widening/record-based-autonomy.md`, `spec/decisions/durability/recovery-store.md`, and `spec/decisions/conformance-product/product.md`.

To change a selection: for one of the two decisions, settle it by its document's method and write the new row in part A; for a fixed default, reopen it only on the trigger its entry in [decisions/deferred.md](./decisions/deferred.md) states, settle it by the same method, and write the new row in the fixed-defaults table. In the configuration, remove the leading `# ` from each line of the newly adopted files' blocks and add it to each line of the blocks of the files they replace; run the gate. Citations in the code that quote a requirement of a file no longer listed then fail as stale, and the list of those failures is the work the change creates. An outcome that has no file (none, gate-only) changes nothing in the configuration; a declared default changes nothing in the configuration either, because it is a value in part B rather than a file.
