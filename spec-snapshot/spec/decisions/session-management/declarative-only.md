# Outcome: Declarative Session Management Only

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../deferred.md) is reopened and the declarative outcome adopted alone; they add to the core and never replace it. Requirements trace to [overview section 10](../../overview.md) and
> [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading.

## Purpose And Scope

Under the declarative outcome alone, the definitions a tenant's sources serve are the only way a root
session comes to exist, so the API refuses to write configuration. This document holds the requirements
that separate the declarative outcome from the outcome that runs both paths;
[declarative.md](declarative.md) holds the requirements the two share.

## What The API Refuses

### Configuration Is Never Written Through The API

A session's configuration MUST NOT be written through the API.

The desired set of sessions MUST NOT be written through the API.

A budget cap or a standing subscription MUST NOT be written through the API.

The platform MUST NOT create a root session except from a definition a source serves.
