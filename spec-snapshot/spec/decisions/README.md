# The Decisions

This directory holds the two decisions a valid solution settles with the operator before building, one register of decisions that are deferred, and one directory per decision holding the requirements that apply only under each of its outcomes. The core under [`../core/`](../core) holds what every valid solution has; a decision is where two valid solutions differ and the operator holds the one fact that settles it. Every other choice is a fixed default: its requirements are in the gate, its alternatives are in the tree with their requirements commented out of the gate, and the register of deferred decisions states the situation that reopens it.

The two decisions:

1. [tenancy-scope](tenancy-scope.md): how many tenants, and when. Outcomes: one-tenant (default), registration, cells. Requirements per outcome under [`tenancy-scope/`](tenancy-scope).
2. [execution](execution.md): how a session's model turns are executed. Outcomes: direct (default), with its requirements in [execution/direct.md](execution/direct.md), [execution/direct-model-turns-and-scheduling.md](execution/direct-model-turns-and-scheduling.md), and [execution/direct-context-compaction-and-cache.md](execution/direct-context-compaction-and-cache.md); hosted-harness, with its requirements in [execution/hosted-harness.md](execution/hosted-harness.md).

## Purpose

An agent starting from no context builds the system from this specification by reading the core, settling the two decisions with the operator by the method below, and confirming the declared defaults. The steps are:

1. Read [`../../constitution.md`](../../constitution.md), every file under [`../core/`](../core), and every file under [`../contracts/`](../contracts).
2. Settle the two decisions in the order given below: self-answer from what is already known; pose the remaining screeners in one batch; state what is ruled out; recommend; confirm; record the outcome in [`../record.md`](../record.md) part A; set the gate selection.
3. Confirm the declared defaults in [`../record.md`](../record.md) part B, as the section "Confirming The Declared Defaults" states.
4. Implement the core, the contracts, and the adopted outcome files; cite each requirement from code and from a test; run the requirement gate.
5. Reopen a deferred decision only when its trigger occurs.

A decision document is descriptive: it carries no requirement. It guides the implementer and the operator to one outcome, and the requirements under that outcome are in the outcome files the decision record lists.

## The Method

Each decision document follows one method, condensed here from the operator's runbook.

- **Self-answer first.** Before asking anything, the implementer answers from what the environment and the earlier decisions already show. A question whose answer is already known is not asked.
- **Frame the decision.** State the decision in one sentence and the two to four candidate outcomes, with what each is wanted for and avoided for. The decision documents do this already.
- **Ask about the situation, never about a mechanism.** Every question asks about the operator's situation in past or present specifics ("Where does an agent's configuration live today, and who changes it?"), never which mechanism or feature to use.
- **Screeners first, together, at most four, independent.** The questions whose answers rule outcomes out are asked first, in one batch of at most four, each answerable without the others. A question that depends on an earlier answer is marked "Ask only if ..." and is asked only after its guard is answered.
- **Options are exhaustive and exclusive.** Each question has two to four options that cover every situation and overlap in none. Exactly one option is marked "(recommended)": it is the assumption behind the decision's default outcome and the answer assumed when the operator does not answer within the wait period, and it never describes a person doing by hand what a mechanism should do. Each option carries one line "Effect:" stating what it rules out or favors.
- **Non-blocking, with a labelled default and a wait period.** The implementer poses the batch, states the wait period, and proceeds on the default outcome when the period ends without an answer, marking the record unconfirmed; an operator's confirmation replaces the mark. A decision whose document says it blocks waits for the answers instead: tenancy scope blocks, because the tenant boundary is a security property and the cluster's creation path, key hierarchy, and routing are built to the adopted outcome.
- **Show what each answer rules out.** After the answers, the implementer states which outcomes are ruled out and which remain.
- **Recommend with the margin.** The implementer states how each remaining outcome aligns with the answers, what separates the winner from the runner-up, and what would change the recommendation.
- **Confirm with exactly three options.** Adopt (recommended), adjust (the operator states the outcome and the answer to change), need more detail (the operator states the question), each with its effect.
- **Record.** The frame, the answers, the outcome, and the date go into the record, and the gate selection is set.
- **Never return the decision unresolved.** The implementer arrives with a recommendation and asks for a confirm; it does not ask the operator to choose among the outcomes unaided.
- **Budget.** At most five questions in two rounds per decision: the screeners in the first round, the guarded follow-ups in the second.

## The Order Of Decisions

1. [tenancy-scope](tenancy-scope.md): how many tenants, and when. First, and blocking, because it is irreversible and is settled before any key hierarchy or store layout is implemented.
2. [execution](execution.md): how a session's model turns are executed. Second, because what runs a turn is built on the tenant boundary the first decision fixes.

The screeners of the two decisions are independent of each other, so the implementer poses both batches in one message to the operator and waits for the tenancy-scope answers before building; the execution decision proceeds as its document states. Each decision's own guarded follow-ups, if any, form the second round.

## The Register Of Deferred Decisions

[deferred.md](deferred.md) lists the decisions deliberately left open, each with the invariant in the core that keeps it possible and the concrete situation that reopens it. Six of its entries are the alternatives to the fixed defaults in [`../record.md`](../record.md) part A: a collaboration product, declarative root sessions, an execution environment for agents that run code, human approvals and record-based autonomy, a recovery store, and the specification as a product. A deferred decision is reopened only by its trigger: the implementer poses no question about it, and the operator is not asked to settle it before the trigger occurs. When a trigger occurs, the item is settled by the method above and its entry's last line states which files enter the gate and which they replace.

## Confirming The Declared Defaults

[`../record.md`](../record.md) part B holds every value the requirements leave to configuration, each with the situational fact it depends on. The implementer reads part B, states in one message each value it will build to, grouped as part B groups them, and the operator adjusts any value in one reply; a value the operator does not adjust is confirmed as it stands. No question is asked about a declared default unless the value cannot be defaulted: a value that a requirement makes mandatory and that nothing in the environment or the record supplies. Several values follow from the durable store the implementer selects rather than from the operator: the transaction key bound is the store's own advisory bound; the transcript head bound, the state value bound, and the index element bound follow from the value size the store serves efficiently; the subscription cap per session follows from the state value bound; and the element count at which a key is sharded follows from the cost of moving one element. The implementer checks that fan-out at the stated peak stays within those bounds and reports the result with the values. An operator's adjustment replaces the entry's value and is marked with who confirmed it and the date, as part A rows are.

## What The Implementer Decides Without Asking

In general, the implementer decides without asking:

- names: of API resources, identifiers, topics, files, and modules;
- file and storage layout: keys, tables, directories, and packing;
- libraries, frameworks, and language-level choices, within the declared defaults;
- the exact value of any declared default inside the bound its requirement states, and the measurement that settles it (for example the soft compaction threshold, which is fixed by measuring cost against the context window), and the order in which the defaults are measured and tuned;
- the order of work;
- anything that is feasibility rather than a requirement: how a requirement is met, which algorithm meets it, and how it is tested.

The implementer also decides the following, grouped by subject. Each is a mechanism, or a choice between mechanisms, under which the requirements hold either way. Items marked with the name of a deferred item apply only when that item is reopened and adopted.

Models, compaction, frozen ranges, and the prompt cache:

- whether a finished compaction result is recorded durably before its swap or recomputed after a crash;
- whether a frozen range's reasoning content is stored apart from its visible transcript so that a read under a read grant does not fetch it;
- how the entries a tail carries beyond the transcript are stored relative to the transcript head bound and frozen with it, provided a tail position inside a frozen range stays resolvable;
- the order in which a definition's prompt components are rendered, favoring the most widely shared first so that the cached prefix is shared across sessions;
- how a model change by a transform is accounted in the cache-write measures;
- how reasoning bound to a previous model is dropped or neutralized on a model switch so that the next request is valid;
- whether a session's configuration may select among the capacity sources the tenant's configuration offers;
- the node's cache of transcript blocks, within the rule that any tier outliving the process holds ciphertext only;
- the admission-control algorithm of the direct scheduler (additive increase with multiplicative decrease is the recommended shape), provided a throttle narrows admission rather than fails a session inside its deadline;
- how model quota is shared across nodes (a coordinator leasing shares is recommended), provided shares rebalance by demand, a share is reservable per priority class, and no coordination call sits on the critical path of a model call;
- connection pooling to model endpoints and dead-connection detection, provided the provider's per-connection concurrency limit is enforced before sending;
- the in-memory cache key for rendered segments, provided a rendered segment is never shared across tenants and dropping the cache loses nothing.

Sessions, partitions, and the durable store:

- whether removing a session that is running a step is refused without a force flag, and whether a forced removal waits for the step in flight;
- whether an alert is raised when a template's instance count approaches its cap, and at what fraction;
- how sessions blocked on a tenant's configuration are indexed so that a configuration write wakes only them;
- whether placement weights nodes by a declared capacity and how a planned drain is expressed to the placement policy;
- whether and how a partition's doorbell is sharded, how the shard count is recorded, and how a change of shard count is migrated;
- how a partition's index, and any other key whose element count would grow without bound, is sharded, and at what element count a shard splits;
- how a reloaded or restored session's inbox cursor is initialized so that no entry is skipped or applied twice;
- the duplicate guard by which a session skips an event id it has already applied or dropped, its size, and how it travels with a frozen session;
- the pace at which a stop at template or tenant scope ends non-resident sessions, and how a wait on a transcript position from an earlier compaction count is answered;
- the durable-store key layout, including whether every key begins with its tenant so that a store credential can be scoped by prefix;
- whether the partition of a record whose identifier a caller chooses is derived under a cluster-local secret, and how such records are routed again after a restore;
- whether an emergency stop of a session whose state cannot be decoded keeps an audit copy of its state bytes, and for how long;
- the transaction key bound, the transcript head bound, the state value bound, the index element bound, and the subscription cap per session, from the store's own limits;
- the split schedule for the partition space, as long as a split is by doubling and happens before the median partition's active load exceeds what one node serves, and whether a split is made in place or through a replacing partitioned set.

Drains, restores, nodes, and operation:

- how live streams are ended on a drain, how the live-stream hub's bounded retention is measured, and whether the retry value viewers receive is spread over a configured range;
- how a pin's remaining time is counted across a drain and a restore, within the restore open bound and never reviving a pin that had expired at the drain;
- how a restore records the last restored position of the ended epoch so that a change-feed consumer can detect a lost range;
- whether a deployment's permissions are checked against the resource types it creates before it runs or verified by the deployment's own failure and rollback;
- how an unhealthy node is recovered through the deployment control plane, by restart or by replacement;
- a node's readiness state machine, including whether a node that began draining returns to ready when a drain is reversed;
- how a capacity step is triggered (an alarm on a declared threshold, a schedule, or a forecast), provided the threshold is a declared value;
- how the emergency controls in force are recorded and how nodes learn of a change to them;
- the eviction policy of bounded per-tenant caches;
- the failure detector and the grace by which membership liveness is judged, given that correctness rests on the fence rather than on the detector;
- whether one generation type serves the store's fence, the assertion, and every service's guards.

Keys, blobs, and pins:

- the key under which an inbox entry written by a principal other than the session is sealed (a per-tenant publish key or the recipient session's key), within the envelope requirements and without a key-service call per target;
- how a node's use of a tenant's root key is granted, for example per cluster role by an identity that only the deployment control plane's own service can assume;
- whether a blob write runs outside the owner's fence, since a write creates no reference;
- the granularity at which a session's content is keyed for retention (a key period or a key per blob), provided retention deletes by key destruction, a pinned blob is transferred or its pin honored before its key is destroyed, a frozen session is re-sealed before its key is destroyed, and retention steps are paced;
- whether a blob is stored in independently verifiable segments so that a ranged read verifies without reading the whole blob;
- how a pin's commit checks the source's generation, retention, and deletion state (a conditional transaction on the source's records);
- how a pin's holder keeps the pin alive while it uses it (a renewable hold record that lapses a configured time after its last renewal, or renewal of the pin itself), including when a receiving partition records its hold and how a stager treats a pin whose holds it cannot read;
- a pin's holder classes (the receiving partition, the tenant's partitions, a product's service), provided no class admits another tenant;
- how a write checks for an existing object (a conditional write rather than a listing);
- the decrypt-cache lifetime, which is the revocation window the tenancy requirements call for, and the key-service client's timeouts;
- whether a connection to the key service is held open between calls;
- whether a direct send's attachment pin is shorter than a topic publish's, within the declared pin duration.

Events, topics, and subscriptions:

- whether a topic is created implicitly by the first publish or subscribe addressed to it or by a separate operation;
- where a subscription's event-kind filter is applied, provided every event still reaches every subscriber whose filter admits it exactly once;
- when an ended session's subscriptions are removed from the topics' indexes (at the end or when a delivery finds the session gone);
- when a dedupe record is discarded, no earlier than the end of the dedupe window;
- how a publisher recovers an unknown commit result before it retries; reading back what the commit wrote is the recommended way;
- the live-stream hub's buffer sizes and the fan-out batch size within the transaction key bound, and the at-rest cost of an idle topic within the declared targets.

Governance and deciders:

- whether validation warns when the cost order reverses the declared order of two deciders with overlapping scope;
- whether a configuration can give a distinct pipeline per tool at the tool-call hook or deciders select by call-type tag;
- the default when a decider's configuration does not state whether a truncation denies (a declared default of record.md if the operator sets it);
- whether a decider's declared decision set is checked against its hook when a configuration is validated; at run time a decision that is not legal at its hook is the decider's failure;
- whether a tool call is dispatched when its own block passes the model-output hook or only after every block of the response has passed;
- how several defers on one subject, or a deny whose policies identify several approvers, are resolved (one at a time as a chain under distinct decision ids, or as one defer with several resolvers), provided a block or a timeout anywhere ends in block and the model receives one result;
- whether a session blocked at the consecutive-blocked-cycles cap re-attempts automatically after a cooldown or waits for a resume, and whether a resume waives the cooldown;
- whether a stop-admission decider's scope distinguishes root sessions from children, and the default scope of an external stop-admission decider;
- whether a decider may be enforced without a shadow period by a governance-policy-tier write with a stated reason;
- whether a judge's or an external decider's decision is cached by content;
- how a model change is classified (an ordered list of model tiers held by the tenant's governance, with an unlisted model ranking above every tier), within the rule that an uncovered field is widening;
- whether a decider's configuration can be sealed and held by reference, resolved only where the decider runs and kept out of model context, reasons, events, and logs;
- the number of state slots a program decider has per session and whether they are shared across the hooks it runs at;
- whether a new version of a program decider inherits its predecessor's state slot and whether a fork copies its parent's state slots;
- whether a policy decider evaluates the tenant's and the definition's policies as one set or in sequence;
- at which layers, beyond the access check, the exclusion of reasoning, stored tool output, frozen sessions, and audit copies from an observation read is enforced.

Identity, credentials, and the frontend's path:

- whether the broker issues a delegated credential from a bounded set of predefined profiles selected by the call's scope or from a policy computed per call;
- how the broker's credential material is isolated from other principals;
- whether the broker caches an exchanged credential across an operator's sessions, within the one-call lease of every delivered credential, and by what margin before expiry it replaces a cached one;
- the custody of a lineage's signing root; the recommended custody keeps the root, and the authority to let a node sign under it, outside the deployment of the cluster that signs, in the deployment control plane's own identity;
- how key publication is confirmed before a key signs; reading the set back through the path verifiers use is recommended;
- how a change-feed client authenticates each request without holding a standing credential;
- how an ingress proxy's forwarded identity is restricted: by listener, by source address range, by per-request target, and by network reachability of the frontend port;
- whether a component in front of the frontend terminates connections, verifies request signatures, and keeps the access log, as long as the access-log requirements hold.

Timers and tools:

- where within their flex windows coalesced fires are placed, how skipped occurrences and failing guard destinations are coalesced into events, the minimum recheck interval of a waited fire's guard hook, and the backoff curve of a recurring timer's interval after empty wakes;
- how same-named tools of two servers are disambiguated, whether a tool-server entry may set per-server retries and backoff and a per-server listing refresh age, and the backoff curve of a failing listing;
- whether a declared tool timeout above the upper bound is reduced to the bound or refused at dispatch;
- how a session tracks the servers to which it has an open dispatch;
- from which node a tool call is issued, provided its dispatch record is committed by the partition owner under its fence and the call leaves through the egress point;
- whether a call's trace context is forwarded to a backing service that accepts one.

Shared framework components and identifiers:

- the deadline queue's interface beyond insert, cancel, and next-deadline, such as re-arm by key;
- the change feed's wait interface, including deadlines and returns with no change;
- the order in which rate-limited requests that wait are served, and how runs of throttle refusals are coalesced into events;
- the precedence among incoming trace header formats;
- the sort order of ids, whether a content-type code carries the schema's version, whether durable-store keys are built from an id's binary or text form, and how an id code from a private-use range is handled when ids cross deployments;
- whether keyed actors are hosted by a virtual-actor host inside the partition, and how such actors receive work and propose writes;
- the transaction-passing interface by which a component joins its caller's commit.

Simulation and testing:

- the iteration counts of seeded interleaving scenarios, and whether changes are checked by mutation testing, on what schedule, and how surviving mutants are recorded.

Root sessions, definitions, and sources:

- how the desired set and templates are stored, and the controller's polling interval within the declared bound (declarative root sessions);
- cohort hashing; a hash of the session id, so that the same sessions lead every rollout, is recommended (declarative root sessions);
- the test by which a candidate version is compared with its parent before a rollout widens (declarative root sessions);
- the conformance suite's fixed vectors for the definition-source encoding: each escaped character class, a character written raw, an integer above the number bound, and the bound of a number field (declarative root sessions);
- how two bindings of one tenant keep their definition names distinct and in what order name checks run; a declared, non-overlapping name prefix per binding, checked in a fixed order so that a rejected name carries one reason, is recommended (declarative root sessions);
- the default end policy of a keyed instance whose definition states none, within the declared idle bound.

People and agents working together (a collaboration product):

- the names and shapes of the records, their topics, and the events a change publishes; the store, the live-update transport, and the identity providers, within the declared defaults; the adapter's or the product's storage layout, and how it derives event ids from its revision;
- how a tail is presented in a page and how a tail token is brokered;
- the document format the composer reads and the definition schema it emits;
- how an agent's work moves to the platform from any runtime that acted as the agent before, with at most one runtime able to act as a given agent at any moment;
- the order in which boards, documents, chat, the wiki, and questions are built;
- whether a dashboard block's live query runs as the viewer through an on-behalf-of exchange or as a read-only service identity, and how its results are cached; a viewer without access to the data sees none of it either way;
- whether an access-change event on a private project narrows its access scope to that project;
- the rate classes per principal kind, the burst and in-flight bounds, the commit-latency and propagation targets, the request body bound, the document version size bound, and the long-poll hold, as long as a refusal over a bound states the bound;
- whether an assignment is refused when the task's repository domain is outside the assignee's clearance, or the refusal is left to the workspace acquire, which requires the clearance under every outcome;
- which principals besides the asker may cancel or supersede an open question; the record's current owner and a coordinator of the record's project are recommended; and how a posted question that asks more than one decision is corrected; cancelling it and posing it again is recommended;
- how a write transaction stays free of calls to other services; no network call other than to the store is recommended;
- when an agent's definition identity is reserved; at registration, before any version is composed, is recommended;
- whether the decision queue is computed from the open decisions or kept as a stored list, provided it is complete;
- the service's deployment shape and where documents are stored, provided no state is lost with a node and a document is tenant content under the tenant's keys;
- whether code reviews, notebooks, and dashboards share the typed-block document model.

Where agents run code (an execution environment):

- the host substrate, the snapshot store, and the credential protocol, within the declared defaults; the handle format and the lease length, inside the declared bound;
- the snapshot pack format, how a snapshot chains to the previous one, and where packs are kept; what a snapshot captures of a repository beyond the working tree and what it leaves out, and what a rollback's result says about files left on the host (capturing the staged tree, references, branches, tags, remotes, upstreams, files outside every repository, and large-file content, leaving out ignored files, the build cache, and any home directory, and stating the left-in-place files on the rollback result, is recommended);
- how a snapshot's operation id is derived; deriving it from the lineage, the predecessor head, and the workspace incarnation is recommended, so that a retried upload is a duplicate by construction;
- how a fork shares its source's blobs and how a long lineage keeps a restore within the configured blob bound; copying no blob, moving the base forward, and cumulative packs are recommended;
- which workspace types exist, their images, and their network placements; which languages agent-written programs may use, keeping run records language-neutral;
- how the build cache is keyed within a tenant, and which principals' builds may write the shared build cache: builds of reviewed commits only, entries scoped per principal, or outputs checked by rebuilding;
- whether base packs are deduplicated inside a tenant, once a measurement of stored bases against distinct bases shows the saving pays for the shared deletion;
- the size of the pool of hosts provisioned ahead of demand, its growth and shrink rules, and its claim-latency target, under the cost upper bound the operator sets, with cost per workspace-hour reported;
- whether the service is split into a plane for attributed calls that need no host and a plane for isolated compute;
- how a capacity reservation that no call follows lapses; after the assertion lifetime plus the clock-skew bound is recommended;
- how a call's supervisor is identified; a process group and a control socket rather than a process id is recommended;
- the process layout on a host (one privileged host agent, supervisors, snapshot and upload processes), provided a tool process can reach no process outside its workspace and holds no host credential;
- the direction of the host-to-service connection (host-initiated is recommended), provided a host exposes no inbound endpoint a tool could reach;
- the order in which acquire, run, snapshot, swap, fork, and rollback are built.

Authority widening and checkers:

- which sound checkers exist for each class of change (tests, deterministic simulation, proofs, canaries with rollback), and how a gate spec is stored, versioned, and kept where the agent cannot reach it; the checkers are built first, because every outcome needs them;
- the layout of an evidence record within the evidence-record contract, and where evidence is retained;
- how a defer is presented to the person who resolves it, and how its deadline and its outcome on timeout are configured;
- the width of the grant written when an operator's session starts; reads only is the recommended default;
- the batching window, the layout of the evidence summary, the rate of seeded requests within its declared bound, and the queue the requests wait in (the collaboration product's decision queue where that item is adopted, or an external decider) (human approvals);
- how the temporal policy is expressed as a declarative decider, how a role's record is stored, and the schedule by which authority widens inside the role's upper bound (record-based autonomy).

Durability and the restore source:

- the store's backup schedule and restore procedure within the declared loss window, and how a backup is verified;
- the epoch's representation, a declared default, and where its conditional increment is recorded; the epoch fence and the drain are built first, because every restore source needs them;
- how a drain is sequenced across partitions, and the drain deadline's default within its declared bound;
- the drill's procedure and the stage it runs in, beyond the requirement that it runs every release in a pre-production stage;
- which deployment control plane deploys the platform, provided a deploy is a drain, a checkpoint, a new cluster, and a restore, and no lifecycle operation runs without a session-safe procedure;
- the build and test discipline of the main line, provided no stage deploys a build the gate has not passed;
- the record's encoding within the envelope, the narrow interface and its simulator, the default freeze interval within its declared bound, and how the interval for dirty timer changes is configured (a recovery store);
- when an unreferenced part of a recovery-store record is reclaimed, after a configured grace period being recommended, and whether a node restores partitions from the recovery store when it takes ownership of each or when it starts, at ownership being recommended so that a restore costs only the partitions the node owns (a recovery store).

Conformance and the requirement gate:

- the gate tool and its extraction format, which are declared defaults, and where its reports are published;
- how a citation is written in each language the code uses, as long as it carries the file, the section, and the quoted sentence;
- the layout of the extracted snapshot the gate evaluates against, and how the snapshot is content-addressed;
- which pipeline stage runs the gate and how a refused promotion is reported;
- whether an approval of a specification version covers its requirements or its bytes, recording the requirement diff between versions either way;
- the block schema's field names, the id format, the export format each repository commits, the tracking system that holds the Target ticket, the threshold at which the gate's shadow record lets it begin blocking, and the order in which the lifecycle stages are built (the specification as a product).

A question is asked only when a fact that only the operator holds changes which requirements apply.

Four things are never the implementer's to decide, whatever the decision: a grant on a resource the operator owns, an account-wide policy, the onboarding of the platform with an identity provider, and any request to another team. The implementer prepares each as a task for the operator, with the change written out, and continues on the work that does not depend on it.

## The Record And The Gate Selection

[`../record.md`](../record.md) is the decision record for a deployment. Part A records, for each of the two decisions, the outcome adopted, the answers given, and the date, marked unconfirmed while the outcome rests on the labelled default, and then the fixed defaults with the deferred entry that reopens each; part B records the declared defaults with their confirmations; part C states the gate selection. The gate configuration, `.duvet/config.toml`, lists the constitution, the contracts, every core file, and only the adopted and default outcome files; the other outcomes' files stay in the tree, commented out, so changing an outcome is editing the record and the configuration. An outcome with no file adds nothing to the core, and the record lists it as "no additional requirements".
