# Outcome: Existing Runners Behind A Tool Server

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Where Agents Run Code: Existing Runners Or The Workspace Service" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome, the existing runners, adopted; they add to the core and never replace it. This document defines what a tool server placed in front of runners the tenant operates does: the session's identity on every call, credentials per call and none standing, and isolation between tenants. Requirements trace to [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome agents run code on runners the tenant operates: hosts or services that execute commands and builds. The platform hosts no program for a tenant, so a session reaches a runner only through a tool server the tenant builds or configures in front of the runners. The requirements below fix what that tool server guarantees; how it provisions and schedules runners is outside this specification.

## A Tool Server Listed In The Definition

### Every Call Carries The Session's Identity

The tool server MUST be called through the definition's list of tool servers rather than through any path built into the platform.

Every call to the tool server MUST carry the session's identity.

### Each Call Is Verified And Runs At Most Once

The tool server MUST verify each call's assertion and its lease generation.

The tool server MUST run a call id it holds a receipt for at most once.

## No Standing Credentials

### Credentials Are Per Call

Credentials MUST come from the credential broker in exchange for the call's assertion.

Credentials MUST be served to a call's processes only while the call runs.

A host MUST hold no standing credential.

A log, event, or saved output MUST NOT carry a secret or an endpoint token.

Every credential the tool server acts with MUST carry the operator as its source identity.

A credential the tool server acts with MUST allow a write only for a call that writes.

## Isolation

### A Host Runs One Tenant

A host MUST run one tenant's workloads only.

A tool MUST run unprivileged under resource limits.

All egress MUST leave through one point the tool server controls.
