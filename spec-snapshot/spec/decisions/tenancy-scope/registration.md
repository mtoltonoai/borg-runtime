# Outcome: Tenant Registration

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the registration outcome of decision [tenancy-scope](../tenancy-scope.md); they add to the core and never replace it. This document defines how a tenant is registered through the API: who may register one, what exists before its first durable record, and how it is identified. Requirements trace to [overview section 8](../../overview.md) and [overview section 17](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome more than one tenant exists and a path to add a tenant is part of the API. Registration creates the tenant's trust root before any of its data is written, so that every record of the new tenant is under the tenant's own key and every grant traces to its bootstrap bundle. The tenant boundary that holds under every outcome is in [core/tenancy-and-confidentiality.md](../../core/tenancy-and-confidentiality.md).

## Registering A Tenant

### Registration Is A Governance-Tier Operation

The API MUST accept registration of a tenant by a principal holding a platform-level grant.

Registration of a tenant MUST be a governance-tier operation.

### A Tenant's Trust Root Exists Before Its First Record

A registered tenant's root key MUST be created before its first durable record is written.

### Registration Is An Event

A tenant's registration MUST be published as an event.

## Tenant Ids

### A Tenant Id Is Unique And Encodes No Location

A tenant MUST be identified by an id that no other tenant has.

A tenant id MUST NOT encode where the tenant's data is held.
