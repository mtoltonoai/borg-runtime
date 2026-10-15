# Decision: Tenancy Scope

> **DECISION.** How many tenants the platform serves, and when a path to register a tenant exists. The invariants that hold under every outcome are in [core/tenancy-and-confidentiality.md](../core/tenancy-and-confidentiality.md): every record carries its tenant from the start; the tenant is the trust and data boundary; ids are unique across tenants and encode no location; every tenant's keys derive from a root key dedicated to it. This document guides the implementer and the operator to one outcome. The requirements under each outcome are in [tenancy-scope/one-tenant.md](tenancy-scope/one-tenant.md), [tenancy-scope/registration.md](tenancy-scope/registration.md), and [tenancy-scope/cells.md](tenancy-scope/cells.md); the decision record lists exactly the adopted outcome's files in the gate configuration. The decision traces to [overview sections 8 and 17](../overview.md).

## The Decision

How many tenants the platform serves, and when registration exists. The specification cannot settle it because only the operator knows which teams will run agents within the next two quarters and what isolation any of them is required, by policy or by contract, to have.

## The Outcomes

- **One-tenant** (default): the one tenant is created with a stage's first cluster, its bootstrap bundle installed as part of the cluster's creation rather than configured afterward, and the platform has no path for registering a tenant. Every multi-tenant invariant still holds: every record carries its tenant, and simulation runs two tenants and asserts that nothing crosses between them. Wanted when one team, or one organization that is one trust boundary, runs agents for the next two quarters and no second tenant is known. Avoided when a second tenant is known, because a tenant creation path would then be built twice.
- **Registration**: a governed call creates a tenant on a running cluster: it derives the tenant's root key, installs its bootstrap bundle, and records the tenant before any other record of that tenant exists. Tenants share the cluster, separated by the tenant on every record, by grants that do not extend beyond the tenant, and by keys derived from a root key dedicated to each. Wanted when a second tenant is known within two quarters, or when creating a tenant must not require a change to the cluster. Avoided when only one tenant is expected, because a path that no second tenant exercises runs untested, and when any tenant requires isolation beyond a key of its own.
- **Cells**: a tenant is registered and placed in a cell, an independent cluster behind a tenant router, which shares no node, store, or key cache with another cell. Ids encode no location, so a tenant can be moved between cells without renaming anything. Wanted when a tenant is required, by policy or contract, to keep its data off hosts and stores another tenant uses, or when load exceeds what one cluster serves. Avoided when no tenant requires it, because the router and the per-cell operation of every component multiply the operating cost before use demands it.

## What The Implementer Decides Without Asking

- Under one-tenant: the tenant's name, a declared default in record.md, and how its bootstrap bundle is packaged with the cluster.
- The representation of the tenant field on records, keys, topics, and ids, and how an id's uniqueness across tenants is produced.
- The concrete derivation of branch keys and data keys from the tenant's root key within the envelope design.
- How simulation scenarios create and exercise two tenants.
- Under registration: the shape of the registration call, how the new tenant's bootstrap bundle is supplied, and how the registering role is granted.
- Under cells: how the router's table is stored and replicated, how a cell is identified, and the order in which cells are deployed.

## Questions For The Operator

Ask all three together. They block: the implementer waits for the answers rather than proceeding on the recommended options after the wait period, because the tenant boundary is a security property and the cluster's creation path, key hierarchy, and routing are built to the adopted outcome. The implementer first answers from the environment what it can, such as which teams have asked to run agents and who operates the clusters, and asks only what remains.

1. How many teams will run agents on the platform in the next two quarters?
   - One team, and no other has asked (recommended). Effect: favors one-tenant.
   - One team now; others are expected, but none has asked or has a date. Effect: favors one-tenant; the deferred register's trigger, a second tenant is known, reopens this decision.
   - Two or more, and they are known. Effect: rules out one-tenant; favors registration, or cells when 2 shows that one of them requires isolation beyond a key of its own.
2. What isolation does any of those teams require for its data today, by policy or by contract?
   - None beyond encryption under a key of its own and the access checks the platform enforces; its data may be stored and processed beside another team's (recommended). Effect: no cell is needed; favors one-tenant or registration as 1 decides.
   - Its own root key, held in its own account of the key service, with shared hosts and stores acceptable. Effect: no cell is needed; favors one-tenant or registration as 1 decides; this answer is the trigger of the deferred item "a tenant's own key and capacity".
   - Its data must not be stored or processed on hosts or stores another team uses. Effect: rules out registration on a shared cluster for that team; favors cells when 1 shows more than one team, and is satisfied by one-tenant while that team is the only one.
3. Who would create a new tenant, and could that person deploy a change to the cluster?
   - Nobody beyond the team operating the platform, and only for the one tenant (recommended). Effect: favors one-tenant, under which the tenant is created with the cluster.
   - The team operating the platform, through a governed call when a new team is known, without a deploy. Effect: rules out one-tenant; favors registration.
   - A person outside the operating team, such as an onboarding role of the operating organization or the new team's own administrator. Effect: rules out one-tenant; favors registration with the registering role granted rather than built in, and cells when 2 requires it.

## Recommending

One-tenant when 1 shows one team and no known second, 2 shows no requirement beyond a key of its own, and 3 shows that nobody needs to create a tenant beyond the first. Registration when 1 shows a known second team or 3 shows that someone must create a tenant without a deploy, and 2 shows no team that must keep its data off shared hosts and stores. Cells when 2 shows such a team while 1 shows more than one team, or when the declared defaults in spec/record.md part B show load beyond one cluster. What separates one-tenant from registration is one fact: whether a second tenant is known. What separates registration from cells is one fact: whether any tenant requires isolation beyond a key of its own. What would change the recommendation: a contract or regulation requiring separate hosts makes a shared cluster impossible for that tenant; a second team asking with a date turns one-tenant into registration; an onboarding role outside the operating team rules out one-tenant regardless of the count. Confirm: adopt (recommended), adjust (state the outcome and the answer you would change), need more detail (state the question).

## Recording

Write the outcome, the answers, and the date in [record.md](../record.md); list the adopted outcome's file (one-tenant.md, registration.md, or cells.md) in `.duvet/config.toml` and comment out the other outcomes' files.
