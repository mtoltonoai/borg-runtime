# Outcome: Adapt An Existing Collaboration Tool

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "A Collaboration Product: An Adapted Tool Or A Built Platform" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome, the adapted tool, adopted; they add to the core and never replace it. This document defines what the adapter between the tenant's own collaboration tool and the platform does: identity, publishing record changes, subscriptions, stop admission, status, the tail, and, where the declarative alternative for root sessions is adopted, serving definitions. Requirements trace to [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome an adapter connects the tenant's own collaboration tool to the platform. The adapter is a client of the platform's API and gives the tool's records their meaning as events, so that the platform stays generic over the domain. The requirements below fix what the adapter does; how the tool stores its records and what it shows its users are outside this specification.

## Identity

### Identity Comes Only From Authentication

The adapter MUST take the acting identity of a call from authentication rather than from an argument.

The adapter MUST refuse a call whose identity argument differs from the authenticated caller.

## Publishing

### The Adapter Reaches The Platform Through Its API

The adapter MUST reach the platform through the platform's API rather than through the durable store.

### Each Record Change Is Published Exactly Once

The adapter MUST publish each change to a record to that record's topic.

A published event's id MUST derive from the tool's own ordered revision, so that the platform applies each change exactly once.

## Subscriptions And Admission

### Assignment Subscribes The Session

The adapter MUST subscribe an agent's session to a task when the task is assigned.

The adapter MUST unsubscribe an agent's session from a task when the work is done.

### Stop Admission Follows Open Work

The adapter MUST answer a stop-admission request according to whether the agent has open, unblocked work.

## Status And The Tail

### Status Comes From The Platform

The adapter MUST receive the platform's session status hooks rather than have the agent report its own status.

### A Tail Token Is Brokered

The adapter MUST broker a short-lived tail token rather than relay a session's tail.

The adapter MUST obtain a new tail token at every connect and resume of a viewer's tail.

The adapter MUST NOT log or store a tail token.

### Every Reported State Has A Recovery Action

The adapter MUST have a recovery action for every blocked, quarantined, and failed state the status-hook contract can report.

Alarming a person MUST NOT be the only recovery action for any such state.

## Delivery Mechanisms

### The Adapter Keeps No Delivery Mechanism Of Its Own

The adapter MUST NOT keep an agent inbox, a notification poll, or a liveness watchdog of its own.

## Definitions

### Definitions Are Served Under The Declarative Outcome

Where the declarative alternative for root sessions is adopted, the adapter MUST serve agent definitions over the definition-source contract.
