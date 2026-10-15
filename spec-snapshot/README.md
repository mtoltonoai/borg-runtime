# The Specification

This repository holds the requirements for a multi-tenant agent platform and the products built beside it, written to describe the shape of a valid solution independent of any implementation. The code that implements it is kept in other repositories; that code cites these requirements, and where the two disagree, the specification is the authority unless it is deliberately changed.

The specification is an invariant core plus guided decisions: one constitution, the contracts, the core every valid solution has, two guided decisions and six fixed defaults with the requirements of each outcome in a file of its own, a register of deferred decisions, a decision record, and a requirement gate that lists exactly the adopted outcomes and the fixed defaults. An agent working here starts at [AGENTS.md](AGENTS.md).

## The Documents

- **[constitution.md](constitution.md)**: the fixed invariants, as normative requirements, with the key words defined. Every other specification inherits from it and must not contradict it.
- **[spec/overview.md](spec/overview.md)**: the architecture and the intent every requirement traces to, in three parts: the core concepts, the decisions, and what is deferred and the rationale for the invariants. Descriptive.
- **[spec/glossary.md](spec/glossary.md)**: the controlled vocabulary, including the decision vocabulary and the session-origin terms. Descriptive.
- **[spec/record.md](spec/record.md)**: the decision record for a deployment: which outcome each decision adopted and on what answers, the declared defaults that the requirements leave to configuration, and the rule for the gate selection. Descriptive.
- **[spec/core/](spec/core/)**: the behavior every valid solution has, under every outcome of every decision, one normative file per area; fourteen files.
- **[spec/decisions/](spec/decisions/)**: the guided decisions and the fixed defaults. [spec/decisions/README.md](spec/decisions/README.md) holds the method, the order in which the two decisions are settled, how the declared defaults are confirmed, and what the implementer decides alone; [spec/decisions/deferred.md](spec/decisions/deferred.md) is the register of decisions deliberately left open, each with the property that keeps it possible and the trigger that reopens it, including the alternatives to the six fixed defaults; one document per decision ([spec/decisions/tenancy-scope.md](spec/decisions/tenancy-scope.md) and [spec/decisions/execution.md](spec/decisions/execution.md)) states the decision, its outcomes, the questions for the operator, and how to recommend and record; and one directory per decision or fixed default holds a normative file per outcome with the requirements that apply only under that outcome.
- **[spec/contracts/](spec/contracts/)**: the interfaces independently deployed parties honor across versions: the signed assertion, the definition source, the decision hook, the event hook, the event envelope, the tool call, the usage record, the evidence record, and the stored form. A change to one is additive or carries a version increment and a migration path.
- **[spec/traceability.md](spec/traceability.md)**: the bidirectional map between every normative file and the overview sections it realizes. Descriptive.
- **[AGENTS.md](AGENTS.md)**: the entry point for an agent that builds from this specification or edits it.

## The Requirement Model

Every normative statement is a single sentence that uses one of the five key words the constitution defines, written in capitals, exactly once, and carries exactly one obligation, under a stable heading. A requirement's identity is the tuple of its file, its section, and its quoted sentence. There are no separate ids, so editing a sentence flags every citation that no longer matches it. The descriptive files (the overview, the glossary, the record, the readmes, the decision documents, the deferred register, and the traceability map) carry no requirements and are not extracted.

## Building From The Specification

1. Read [constitution.md](constitution.md) and every file under [spec/core/](spec/core/). Together they are what every valid solution has.
2. Settle the two decisions, tenancy scope and then execution, with the operator and by the method [spec/decisions/README.md](spec/decisions/README.md) states. First answer from what the environment already tells you. State each decision in one sentence and its two to four outcomes. Pose the remaining screening questions of both decisions in one batch, each about the operator's situation in past or present specifics rather than about mechanisms, each with two to four exhaustive options, exactly one of them recommended, and one line of effect per option. Ask a guarded follow-up only after its guard is answered. State what the answers rule out and what remains. Recommend one outcome per decision, with what separates it from the runner-up and what would change it. Close with a confirm question of exactly three options: adopt (recommended), adjust, need more detail. Tenancy scope waits for an answer, because its choice cannot be changed once data has been written under it; execution proceeds as its document states. Never return a decision to the operator without a recommendation.
3. Confirm the declared defaults: read [spec/record.md](spec/record.md) part B, state in one message each value you will build to, and let the operator adjust any value in one reply. Ask no question about a value that can be defaulted. The six fixed defaults in part A need no step: no question is asked about them, and each is reopened only on the trigger its entry in [spec/decisions/deferred.md](spec/decisions/deferred.md) states.
4. Write [spec/record.md](spec/record.md): the outcome, the answers, who confirmed, and the date for each decision in part A, and the confirmed values in part B.
5. Set the gate selection in [.duvet/config.toml](.duvet/config.toml): the constitution, every contract, every core file, the files of the adopted outcomes, and the files of the fixed defaults are active; every other outcome file is present and commented out.
6. Implement the core, the contracts, the adopted outcomes, and the fixed defaults. Cite each requirement from the code that implements it and from the test that exercises it.
7. Run the gate; a build in which a mandatory requirement lacks either citation is not promoted.
8. Reopen an item in [spec/decisions/deferred.md](spec/decisions/deferred.md) only when its trigger occurs.

## Editing Rules

1. **Atomic key-word sentences under stable headings.** Each normative sentence carries one obligation and one key word; a sentence that bundles two obligations is split into two; each sentence is its own paragraph under a heading in title case that does not change.
2. **Stay standalone.** No normative sentence mentions a concrete engine, store, provider, library, numeric width, or path. Write "the durable store", "a provider", "the configured bound". A concrete choice goes in spec/record.md.
3. **Write literally.** No idioms, no figurative language, no personification of systems, no colloquialisms. No proper nouns and no product, vendor, or technology names: write the category. The verb "name" is not used to mean carry, identify, specify, state, list, record, address, or refer to (write "carry its tenant", "identifies its parent", "addressed by its digest"); the noun stays. Only characters from the basic seven-bit character set: a colon or a new sentence replaces a dash, the word "section" replaces its sign, and quotes are straight.
4. **Use the glossary.** Add a term there before using it normatively, and use only the current form of a renamed term.
5. **Descriptive versus normative.** The overview, the glossary, the record, the readmes, the decision documents, the deferred register, and the traceability map are descriptive and are not listed in the gate; the constitution, the contracts, the core files, and the outcome files are normative, and every one of them is extracted.
6. **Contracts change only under discipline.** A change to spec/contracts/ is additive, or carries a version increment and a migration path.
7. **Traceability is bidirectional.** Every normative file maps to an overview section, and every overview section is served by at least one normative file.
8. **A governing requirement binds to the change process.** A requirement about how the specification or its configuration may change binds to the change-process check rather than to a runtime citation, and is not counted as gating for a build.

## The Gate

The gate is configured in [.duvet/config.toml](.duvet/config.toml). The requirement side lists the normative files, maintained by hand, each block with `format = "markdown"` set; the code side is the globs of the code that cites them, which lives in the implementing repositories. A citation is two markers in the code: the first, `//= <file>#<section>`, gives the file and the section of the requirement, and the second, `//# <exact sentence>`, quotes the requirement's sentence; the gate matches the quoted text against the section, so a reworded requirement invalidates every stale citation. A requirement is covered only when it has both an implementation citation and a test citation that exercises the behavior. The rules the gate enforces are stated normatively in [spec/core/simulation-and-conformance.md](spec/core/simulation-and-conformance.md); whether conformance is checked beyond the gate is a fixed default (gate-only) whose alternative is the entry "The Specification As A Product" in [spec/decisions/deferred.md](spec/decisions/deferred.md).

```sh
# extract the requirements of one file (the output directory must end with a slash)
duvet extract -f markdown -o /tmp/extract/ ./spec/core/governance.md

# run the full requirement gate against the selection in .duvet/config.toml
duvet report
```

## Status

The specification has 46 normative files: the constitution, 9 contracts, 14 core files, and 22 outcome files. The default gate selection lists 32 of them: the constitution, the contracts, the core files, the 4 outcome files of the two decisions' default outcomes, and the 4 files of the fixed defaults; every normative file is extracted by the gate. Both decisions are at their default outcome, assumed and unconfirmed by an operator, and the declared defaults are unconfirmed until the confirmation step; the record says so row by row.
