# Outcome: Self-Durable Store

> **OUTCOME SPECIFICATION.** These requirements apply when the decision record ([spec/record.md](../../record.md) part A) keeps the fixed default self-durable for how the store survives losing every node; they add to the core and never replace it. This document defines what holds when the durable store is itself the mechanism by which sessions survive the loss of every node, with no recovery store beside it. Requirements trace to [overview section 14](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome the durable store survives the loss of every node at once, so nothing outside the cluster is needed to resume sessions after a full-cluster restart. The store's own write-ahead log, snapshots, and index are the durability mechanism, and a new cluster is restored from the store's own backup. The re-drive, drain, and restore invariants that hold under every outcome are in [core/durability-and-deployment.md](../../core/durability-and-deployment.md).

## The Store Is The Durability Mechanism

### The Store Survives The Loss Of Every Node

The durable store MUST survive the loss of every node at once without losing committed state.

The durable store's own write-ahead log, snapshot, and index MUST be the durability mechanism.

## Backup And Restore

### A New Cluster Restores From The Store's Backup

A new cluster MUST restore every session from the durable store's own backup.

A procedure for backing up the durable store and restoring a cluster from the backup MUST be defined.

### A Full-Cluster Restart Is Drilled

A full-cluster restart MUST be drilled in a pre-production stage every release.

## Drain And Crash

### An Orderly Drain Loses Nothing

An orderly drain MUST lose nothing.

### A Crash Loses No Committed State

A crash MUST lose no committed state.
