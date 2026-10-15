# Contract: The Stored Form

> **CONTRACT.** This document pins the form in which a tenant's data is stored: the envelope every durable
> value and blob is wrapped in, how a blob is addressed, and how formats evolve. Data written under any earlier form of a format must be
> readable, deletable, and recoverable by every later build, so this is an irreversible choice decided before
> the first durable record. Its requirements realize [Core Principle V](../../constitution.md) and
> [Protected Guarantee "The Tenant Is The Boundary"](../../constitution.md) and trace to
> [overview section 9](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The cipher modes, the hash function, the compression, and the epoch length are declared
> defaults.

## Purpose And Scope

Two properties make deletion and recovery mechanical: every ciphertext carries what is needed to decrypt
it given the tenant's root key, and nothing is readable under any other key. This contract fixes the envelope, the part of the key hierarchy the stored form represents, blob addressing, the fields that are stored unencrypted, and how a durable format evolves.

## The Key Hierarchy

### Keys Derive From The Tenant's Root

Every stored value of a tenant's content MUST be encrypted under a data key derived from that tenant's root key.

A branch key MUST be derived per tenant per epoch under a service-held root, or per resource under a
tenant-held root.

A data key MUST be random per resource.

An event appended to a session's inbox by a principal other than the session MUST be encrypted under a key the publisher can use without holding the session's data key.

### A Resource Carries Its Own Wrapped Keys

A stored resource MUST carry its own wrapped keys, so that any node can read it with at most one call to
the key service.

A long-lived resource's data key MUST be re-wrappable under a new epoch's branch key without a call to
the key service.

A backup or point-in-time copy of any store MUST NOT retain key material that still opens after the owning key is destroyed.

### The Tenant Is In The Encryption Context

Every envelope's encryption context MUST carry the tenant.

## The Envelope

### Authenticated Data Binds The Envelope

Every envelope MUST carry authenticated data that binds it to its tenant, its owner, and its range.

A ciphertext copied to another owner or range MUST fail to decrypt.

A segment of a blob MUST have authenticated data that binds its owner and its position within the blob.

An inbox entry's authenticated data MUST bind it to its session id and its event id.

A session's stored state MUST carry a header that decodes independently of its body and carries the format identity, the session's lease generation, and its owner identity.

The state header MUST be bound to the body as authenticated data.

### Values And Blobs Use Authenticated Encryption

A stored value MUST be encrypted under an authenticated mode.

An immutable blob MUST be encrypted under a deterministic authenticated mode, so that a retried write
produces the same bytes under the same name.

## Blobs

### A Blob Is Addressed By Its Stored Bytes

A blob MUST be addressed by the hash of its stored ciphertext.

A blob's name MUST reveal nothing about its plaintext.

A store or cache MUST be able to verify a blob's integrity from its name without holding any key.

### A Blob Carries Its Key Chain

A blob's header MUST carry its wrapped key chain, so that the blob store and the key service alone can
recover a session's history.

### A Blob Is Immutable And Owned

A blob MUST NOT change once written.

A blob MUST be stored under its tenant and its owner.

### Frozen Ranges Are Compressed Before Encryption

A frozen transcript range MUST be compressed before it is encrypted.

A compressed range MUST be written in a format whose decoder is a published standard, so that the encoder can change without a format change.

## What Is Stored Unencrypted

### Scheduling Fields Are Readable Without Keys

A timer's timing fields are operational metadata rather than tenant content, so the obligation to encrypt a tenant's content does not apply to them.

A timer's timing fields MUST be stored unencrypted, so that a partition schedules and recovers without the session's keys.

A timer's name, note, and comparison values MUST be encrypted under the session's keys.

A tenant's content MUST be sealed by the writing client before it is written, so that the durable store never holds it in plaintext.

The values stored unencrypted MUST be only the platform's own operational metadata that this contract enumerates.

## Format Evolution

### Every Durable Format Is Self-Describing

Every evolvable durable format MUST identify each field by a tag.

A tag given to a field or a variant MUST NOT be reused for another meaning after the field or variant is removed.

A decoder MUST preserve a field whose tag it does not recognize through a read and a rewrite.

A field added to a durable format MUST carry the value a reader that lacks the field falls back to.

An enumeration in a durable format MUST declare a fallback variant that a reader reads an unknown variant as, or declare that it has none.

An enumeration whose variants cannot be held safely MUST declare no fallback, so that an unknown variant fails the decode and the value is held unchanged.

A variant MUST be removed only where every record holding it can be held as the fallback.

Every component of a durable format that may gain a field MUST itself be tagged, including the payload of a variant and the stored form of a resource's wrapped keys.

A record at rest MUST remain readable by every later build without being rewritten.

Two consecutive forms of a durable format MUST both be readable during a rollout.

A record MUST remain readable under the stored format, the encryption domain, and the key generation it was sealed with after any of them changes.

A re-seal of a record under a new binding MUST commit under the record's partition fence, so that the owner's own commits cannot interleave with it.

The hash that maps a session id to its partition MUST be part of the durable format, so that changing it is a migration.

A value that exceeds its bound MUST be rejected as an error rather than truncated.

A length that is not the minimal encoding of its value MUST be rejected, so that every value has exactly one encoding.

A change to a field's bound MUST be a format change.

A stored envelope's format identity MUST be independent of the shape of the value it carries, so that a change to the carried value invalidates no stored envelope.

### A Decoder Reports Every Corrupt Value

A decoder MUST distinguish an absent value, a valid value, and a corrupt value.

A decoder MUST NOT overwrite a value it could not decode.

A plaintext value found where a sealed value is due MUST be treated as undecodable rather than read.

A decoder MUST allocate no more than the remaining input can hold.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to already-written data, or else carry an
explicit version increment.

A change to this contract that is not additive with respect to already-written data MUST carry a stated
migration path.
