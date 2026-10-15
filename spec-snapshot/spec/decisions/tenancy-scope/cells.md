# Outcome: Cells

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the cells outcome of decision [tenancy-scope](../tenancy-scope.md); they add to the core and never replace it. This document defines a cell as an independent cluster holding a subset of the tenants, how a tenant is assigned and routed to its cell, and how a tenant moves between cells. Requirements trace to [overview section 8](../../overview.md) and [overview section 17](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome the platform runs as several independent clusters, called cells, each holding a subset of the tenants, so that a failure or a limit in one cell reaches only the tenants assigned to it. A tenant router in front of the cells sends each call to the cell that holds its tenant. The tenant boundary that holds under every outcome is in [core/tenancy-and-confidentiality.md](../../core/tenancy-and-confidentiality.md).

## A Cell Is An Independent Cluster

### A Cell Has Its Own Stores And Key Roots

A cell MUST be an independent cluster with its own durable store, its own key service roots, and its own blob store.

### A Tenant Is Assigned To One Cell

A tenant MUST be assigned to exactly one cell.

A tenant router MUST direct every call for a tenant to that tenant's cell.

A cell MUST hold no record of a tenant assigned to another cell.

## Ids And Migration

### Ids Stay Unique Across Cells

An id MUST be unique across cells.

### Moving A Tenant Loses No Event

Moving a tenant between cells MUST be an operation with a defined migration procedure.

A migration of a tenant between cells MUST lose no event.
