# Outcome: Imperative Only

> **OUTCOME SPECIFICATION.** These requirements apply when the decision record ([spec/record.md](../../record.md) part A) keeps the fixed default imperative for who owns the list of root sessions; they add to the core and never replace it. This document states what a build with no declarative path excludes; it is adopted together with [imperative.md](./imperative.md) and never together with the declarative requirements. Requirements trace to [overview section 10](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under the imperative outcome the platform has no definition sources, no controllers, and no desired set. Every root session exists because a caller asked for it, and the caller's system is the only party that decides whether it keeps existing. The requirements below state that exclusivity, so that a build cannot add a second, ungoverned path to a root session by accident. When both paths are wanted, the decision record adopts the both outcome instead, whose rules are in [both.md](./both.md).

## Exclusivity

### The Platform Creates No Root Session On Its Own

The platform MUST NOT create a root session except at a caller's request.

The platform MUST NOT read agent configuration from any source other than a caller's request.

The platform MUST NOT keep a desired set of root sessions.

A keyed instance MUST be created only from a template a caller registered.
