# Decision: Execution

> **DECISION.** How a session's model turns run: the platform makes the model calls itself, or the platform hosts and supervises an agent harness the operator already runs, and that harness makes the model calls. The invariants that hold under every outcome are in [core/sessions-and-partitions.md](../core/sessions-and-partitions.md), [core/tools-and-dispatch.md](../core/tools-and-dispatch.md), [core/identity-delegation-and-secrets.md](../core/identity-delegation-and-secrets.md), [core/observation-and-usage.md](../core/observation-and-usage.md), [core/events-and-subscriptions.md](../core/events-and-subscriptions.md), [core/governance.md](../core/governance.md), and [core/the-frontend.md](../core/the-frontend.md): every tool call passes dispatch; one usage record exists per model attempt; the tail shows every session's gated activity; pause, stop, and quarantine take effect without the executor's cooperation; durable state is the stored form and a crash loses at most the step in progress; credentials are leased per call and never held. This document guides the implementer and the operator to one outcome. The requirements under each outcome are in [execution/direct.md](execution/direct.md), [execution/direct-model-turns-and-scheduling.md](execution/direct-model-turns-and-scheduling.md), and [execution/direct-context-compaction-and-cache.md](execution/direct-context-compaction-and-cache.md) for direct execution, and in [execution/hosted-harness.md](execution/hosted-harness.md) for the hosted harness; the decision record lists exactly the adopted outcome's files in the gate configuration. The decision traces to [overview section 18](../overview.md).

## The Decision

How a session's model turns run: the platform makes the model calls itself (direct, the default), or the platform hosts and supervises an agent harness the operator already runs, and that harness makes the model calls (hosted harness). The specification cannot settle it because only the operator knows whether their agents already run inside a harness they intend to keep, and whether that harness can be configured to take its credentials by lease, to send every tool call through one tool server, and to report its usage.

## The Outcomes

- **Direct** (default): the platform owns the loop. It builds every model request from the durable log, schedules and routes the call, gates each completed block of the reply, dispatches the tool calls, compacts the transcript, and manages the prompt cache. Every session is a transcript the platform can read, fork, and compact, and every requirement about scheduling, caching, and interruption applies within the turn. Wanted when agents are defined as configuration (prompts, tools, policies) rather than as a program, or when the operator's existing harness cannot be made to take credentials by lease, route tool calls through one server, and report usage. Avoided when the operator has a harness whose behavior within a turn is itself the product and must be kept, because the platform would then run one loop and the harness another.
- **Hosted harness**: the platform runs the operator's harness as the session's executor, one supervised process per session on a host the tenant operates, and keeps every invariant by owning the boundary the harness crosses: process lifecycle, the stored form, the credential broker, the egress point, dispatch, the inbox, and the API. Compaction, caching, and the order of requests within a turn are the harness's; the platform schedules at the step level and bounds the harness by capacity leases and egress rate caps. Model output is gated where it leaves the harness rather than per block inside it. Wanted when the operator's agents already run in a harness that will remain, and that harness can take its credentials by lease, send every tool call through one tool server, and report its transcript and usage. Avoided when no such harness exists, because the boundary would be built with nothing to put inside it, and when the harness cannot be constrained to the egress point and dispatch, because the invariants would then be unverifiable.

## What The Implementer Decides Without Asking

- Under direct: the scheduler's internal structure, the render cache's layout, the compaction summarizer's prompt, and the cache lifetime heuristics inside the declared bounds.
- Under hosted harness: the host service's interface for starting, signaling, and ending a process; the shape of the working-state record inside the stored form; how the step boundary is signaled; the grace period and capacity lease values inside the declared bounds; and the simulated harness's behavior.
- Under both: the names of API resources and fields, the layout of the code, the libraries used, and the order in which the pieces are built.

## Questions For The Operator

Ask 1 and 2 together; 3 is guarded on the answer to 1. They do not block: after the wait period the implementer proceeds on the recommended options, which assume direct execution, because the choice is reversible while no session has run under it. The implementer first answers from the environment what it can, such as whether a harness program exists in the operator's repositories and how its tool calls are routed, and asks only what remains.

1. How do your agents run today?
   - As configuration: prompts, tool lists, and policies that a runtime of ours or of a vendor's executes (recommended). Effect: favors direct; there is no harness to host.
   - Inside a harness program of ours that we intend to keep and develop. Effect: favors hosted harness, subject to 2 and 3.
   - Inside a harness program we intend to replace. Effect: favors direct; the harness's behavior does not need to survive.
   - Not yet; nothing runs. Effect: favors direct; the platform's loop is the first one.
2. Where do your agents' model and tool credentials come from today?
   - They are issued per call by a service, or we can change the harness to take them that way (recommended). Effect: no change; both outcomes remain open.
   - They are configured on the host or in the program and we cannot change how the harness takes them. Effect: rules out hosted harness; a harness that holds a long-lived credential cannot be supervised by lease.
   - We do not know, or the harness is a vendor's and we cannot inspect it. Effect: favors direct until the harness's credential path is known.
3. (Ask only if 1 is a harness program you intend to keep.) How does your harness reach its tools and report what it did?
   - Through one configurable tool server address, and it emits its transcript and usage, or we can change it to (recommended). Effect: no change; both outcomes remain open.
   - Its tools are built in or it reaches them directly, and we cannot change that. Effect: rules out hosted harness; every tool call must pass dispatch.
   - It reaches tools through one server but reports nothing. Effect: hosted harness remains open; usage comes from the egress point and the transcript from the tool calls and outputs the platform sees, but the tail is sparser than under direct.

## Recommending

Direct when 1 shows agents as configuration, nothing running, or a harness to be replaced, and whenever 2 or 3 rules the hosted harness out. Hosted harness when 1 shows a harness the operator intends to keep and 2 and 3 show, or allow the operator to make, a harness that takes credentials by lease and reaches tools through one server. What separates direct from hosted harness is one fact: whether a harness the operator will keep already exists and can be constrained at its boundary. What would change the recommendation: a harness that cannot be changed to take credentials by lease or to route tool calls through one server makes hosted harness impossible; an operator whose harness is the product, with behavior within a turn that direct execution would replace, makes direct a loss of that behavior and favors hosted harness even at the cost of sparser gating. Confirm: adopt (recommended), adjust (state the outcome and the answer you would change), need more detail (state the question).

## Recording

Write the outcome, the answers, and the date in [record.md](../record.md); for direct list `spec/decisions/execution/direct.md`, `spec/decisions/execution/direct-model-turns-and-scheduling.md`, and `spec/decisions/execution/direct-context-compaction-and-cache.md` in `.duvet/config.toml`, for hosted harness list `spec/decisions/execution/hosted-harness.md`, and comment out the other outcome's files.
