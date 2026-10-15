# Traceability

> **What this document is.** The bidirectional map between the normative documents and the architecture they realize. Every normative document (the constitution, the nine contracts, the fourteen core files, and every outcome file under spec/decisions/) traces to a section of [overview.md](./overview.md), the authority on intent, and every overview section is served by at least one document. This document is descriptive; it carries no requirements. It exists so that a change to intent and a change to a requirement can each be checked against the other.
>
> The decision documents (spec/decisions/tenancy-scope.md and spec/decisions/execution.md), the register of deferred decisions (spec/decisions/deferred.md), and the decision record (spec/record.md) are descriptive: they appear in the second table where they serve a section and do not appear in the first. An outcome file is listed whether or not the record adopts it, because what a document realizes does not depend on whether a deployment selected it; the gate configuration lists the adopted outcomes' files and the fixed defaults' files. The outcome files of a fixed default trace to the core section they refine and to section 19, where the default and its deferred alternatives are described.

## Normative document to the overview sections it realizes

| Normative document | Realizes overview sections |
|---|---|
| `constitution.md` | 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 15, 19 |
| `spec/contracts/signed-assertion.md` | 5, 7 |
| `spec/contracts/definition-source.md` | 10 |
| `spec/contracts/decision-hook.md` | 6 |
| `spec/contracts/event-hook.md` | 12 |
| `spec/contracts/event-envelope.md` | 4 |
| `spec/contracts/tool-call.md` | 5 |
| `spec/contracts/usage-record.md` | 13 |
| `spec/contracts/evidence-record.md` | 6, 19 |
| `spec/contracts/stored-form.md` | 9 |
| `spec/core/sessions-and-partitions.md` | 3, 4, 10 |
| `spec/core/events-and-subscriptions.md` | 4 |
| `spec/core/tools-and-dispatch.md` | 5 |
| `spec/core/governance.md` | 6 |
| `spec/core/identity-delegation-and-secrets.md` | 7 |
| `spec/core/tenancy-and-confidentiality.md` | 8 |
| `spec/core/encryption-and-blobs.md` | 9 |
| `spec/core/timers.md` | 11 |
| `spec/core/the-frontend.md` | 12 |
| `spec/core/observation-and-usage.md` | 13 |
| `spec/core/failure-handling.md` | 14 |
| `spec/core/durability-and-deployment.md` | 14 |
| `spec/core/simulation-and-conformance.md` | 15 |
| `spec/core/shared-framework.md` | 16 |
| `spec/decisions/tenancy-scope/one-tenant.md` | 8, 17 |
| `spec/decisions/tenancy-scope/registration.md` | 8, 17 |
| `spec/decisions/tenancy-scope/cells.md` | 8, 17 |
| `spec/decisions/execution/direct.md` | 18 |
| `spec/decisions/execution/direct-model-turns-and-scheduling.md` | 18 |
| `spec/decisions/execution/direct-context-compaction-and-cache.md` | 18 |
| `spec/decisions/execution/hosted-harness.md` | 18 |
| `spec/decisions/collaboration-platform/adapt.md` | 19 |
| `spec/decisions/collaboration-platform/build.md` | 19 |
| `spec/decisions/session-management/imperative.md` | 10, 19 |
| `spec/decisions/session-management/imperative-only.md` | 10, 19 |
| `spec/decisions/session-management/declarative.md` | 10, 19 |
| `spec/decisions/session-management/declarative-only.md` | 10, 19 |
| `spec/decisions/session-management/both.md` | 10, 19 |
| `spec/decisions/workspace-service/existing.md` | 19 |
| `spec/decisions/workspace-service/build.md` | 19 |
| `spec/decisions/authority-widening/checkers-only.md` | 6, 19 |
| `spec/decisions/authority-widening/human-approvals.md` | 6, 19 |
| `spec/decisions/authority-widening/record-based-autonomy.md` | 6, 19 |
| `spec/decisions/durability/self-durable.md` | 14, 19 |
| `spec/decisions/durability/recovery-store.md` | 14, 19 |
| `spec/decisions/conformance-product/product.md` | 15, 19 |

## Overview section to the documents that serve it

Paths in this table are relative to `spec/`, except `constitution.md`, which is at the repository root. A document marked "descriptive" carries no requirements.

| Overview section | Served by |
|---|---|
| 1. The central principle | constitution.md |
| 2. Why the platform exists | constitution.md |
| 3. Sessions, partitions, and the durable store | core/sessions-and-partitions.md; constitution.md |
| 4. Events, inboxes, and subscriptions | core/events-and-subscriptions.md; core/sessions-and-partitions.md; contracts/event-envelope.md; constitution.md |
| 5. Tools are services | core/tools-and-dispatch.md; contracts/tool-call.md; contracts/signed-assertion.md; constitution.md |
| 6. Governance is one mechanism | core/governance.md; contracts/decision-hook.md; contracts/evidence-record.md; constitution.md; decisions/authority-widening/checkers-only.md; decisions/authority-widening/human-approvals.md; decisions/authority-widening/record-based-autonomy.md |
| 7. Identity, delegation, and secrets | core/identity-delegation-and-secrets.md; contracts/signed-assertion.md; constitution.md |
| 8. Tenancy and confidentiality | core/tenancy-and-confidentiality.md; constitution.md; decisions/tenancy-scope.md (descriptive); decisions/tenancy-scope/one-tenant.md; decisions/tenancy-scope/registration.md; decisions/tenancy-scope/cells.md |
| 9. Encryption at rest and the blob store | core/encryption-and-blobs.md; contracts/stored-form.md; constitution.md |
| 10. Definitions: what configures an agent | core/sessions-and-partitions.md; contracts/definition-source.md; decisions/session-management/imperative.md; decisions/session-management/imperative-only.md; decisions/session-management/declarative.md; decisions/session-management/declarative-only.md; decisions/session-management/both.md |
| 11. Timers | core/timers.md; constitution.md |
| 12. The frontend | core/the-frontend.md; contracts/event-hook.md |
| 13. Observation, usage, and the improvement loop | core/observation-and-usage.md; contracts/usage-record.md; constitution.md |
| 14. Failure handling, durability, and deployment | core/failure-handling.md; core/durability-and-deployment.md; constitution.md; decisions/durability/self-durable.md; decisions/durability/recovery-store.md |
| 15. Deterministic simulation and conformance | core/simulation-and-conformance.md; constitution.md; decisions/conformance-product/product.md |
| 16. Shared framework components | core/shared-framework.md |
| 17. How many tenants, and when | decisions/tenancy-scope.md (descriptive); decisions/tenancy-scope/one-tenant.md; decisions/tenancy-scope/registration.md; decisions/tenancy-scope/cells.md; record.md (descriptive, part A) |
| 18. How a session's model turns run | decisions/execution.md (descriptive); decisions/execution/direct.md; decisions/execution/direct-model-turns-and-scheduling.md; decisions/execution/direct-context-compaction-and-cache.md; decisions/execution/hosted-harness.md; record.md (descriptive, part A) |
| 19. Fixed defaults and deferred decisions | decisions/deferred.md (descriptive); record.md (descriptive, parts A and B); constitution.md; contracts/evidence-record.md; decisions/collaboration-platform/adapt.md; decisions/collaboration-platform/build.md; decisions/session-management/imperative.md; decisions/session-management/imperative-only.md; decisions/session-management/declarative.md; decisions/session-management/declarative-only.md; decisions/session-management/both.md; decisions/workspace-service/existing.md; decisions/workspace-service/build.md; decisions/authority-widening/checkers-only.md; decisions/authority-widening/human-approvals.md; decisions/authority-widening/record-based-autonomy.md; decisions/durability/self-durable.md; decisions/durability/recovery-store.md; decisions/conformance-product/product.md |
| 20. Rationale | (descriptive; no normative file; the reasons for the invariants of part 1) |
