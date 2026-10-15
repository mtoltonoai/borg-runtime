# Outcome: One Tenant

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the one-tenant
> outcome of decision [tenancy-scope](../tenancy-scope.md); they add to the core and never replace it.
> Requirements trace to [overview section 8](../../overview.md) and
> [overview section 17](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The first tenant's name is a declared default.

## Purpose And Scope

Under this outcome the platform serves one tenant, created with the first cluster of a stage, and offers
no path to register another. The core requires every record to carry its tenant from the start, so a
second tenant can be added without rewriting records; that requirement and the rest of the tenant
boundary are in [core/tenancy-and-confidentiality.md](../../core/tenancy-and-confidentiality.md).

## The First Tenant

### The First Tenant Is Bootstrapped

The first tenant MUST be bootstrapped with a stage's first cluster rather than configured.

The platform MUST NOT provide a path to register a tenant.
