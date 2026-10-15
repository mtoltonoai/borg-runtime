# Agent Entry Point

This file orients an agent that builds from this specification or edits it. Read it first, then [constitution.md](constitution.md), then every file under [spec/core/](spec/core/).

## What This Repository Is

This repository is the specification of a multi-tenant agent platform and the products built beside it. It describes the shape of a valid solution and nothing about any implementation; code that implements it is kept in other repositories, cites the requirements, and is checked against them by a requirement gate.

The normative files are of four kinds. [constitution.md](constitution.md) holds the invariants and defines the key words. [spec/contracts/](spec/contracts/) holds the interfaces that independently deployed parties honor across versions. [spec/core/](spec/core/) holds the behavior every valid solution has, under every outcome of every decision. [spec/decisions/](spec/decisions/) holds the two decision documents, the register of deferred decisions, and one directory per decision or fixed default, each directory with one normative file per outcome carrying the requirements that apply only under that outcome.

The descriptive files carry no requirements: [spec/overview.md](spec/overview.md) is the intent every requirement traces to; [spec/glossary.md](spec/glossary.md) is the controlled vocabulary; [spec/record.md](spec/record.md) is the decision record, which says which outcome each decision adopted, the declared defaults, and the rule for the gate selection; [spec/decisions/README.md](spec/decisions/README.md) is the method, the order of the two decisions, how the declared defaults are confirmed, and what the implementer decides alone; [spec/decisions/deferred.md](spec/decisions/deferred.md) is the register of decisions deliberately left open, including the alternatives to the six fixed defaults; the two decision documents, [spec/decisions/tenancy-scope.md](spec/decisions/tenancy-scope.md) and [spec/decisions/execution.md](spec/decisions/execution.md), guide each decision; and [spec/traceability.md](spec/traceability.md) maps every normative file to the overview sections it realizes.

Two facts apply throughout. A requirement is identified by its file, its section, and its exact sentence, so changing a sentence's wording invalidates every citation of it. A requirement is written for every valid solution, so it mentions no concrete engine, store, provider, library, numeric width, or path; a concrete value is configuration, recorded in spec/record.md.

## Building From The Specification

1. Read [constitution.md](constitution.md) and every file under [spec/core/](spec/core/). Together they are what every valid solution has; nothing in a decision weakens them.
2. Settle the two decisions with the operator, tenancy scope and then execution, by the method below. Each decision document states the decision, its outcomes with one marked as the default, what you decide without asking, the questions to ask, how to recommend, and how to record. The six fixed defaults need no step: no question is asked about them, and each is reopened only on the trigger its entry in [spec/decisions/deferred.md](spec/decisions/deferred.md) states.
3. Confirm the declared defaults: read [spec/record.md](spec/record.md) part B, state in one message each value you will build to, grouped as part B groups them, and let the operator adjust any value in one reply. Ask no question about a value that can be defaulted.
4. Write [spec/record.md](spec/record.md): for each decision, the outcome, the answers, who confirmed, and the date in part A; the confirmed values in part B.
5. Set the gate selection in [.duvet/config.toml](.duvet/config.toml): the constitution, every contract, every core file, the files of the adopted outcomes, and the files of the fixed defaults are active; every other outcome file is present with a leading comment marker on each line of its block.
6. Implement the core, the contracts, the adopted outcomes, and the fixed defaults. Cite each requirement from the code that implements it and from the test that exercises it, with the two markers the gate reads.
7. Run the gate. A build in which a mandatory requirement lacks an implementation citation or a test citation is not promoted.
8. Reopen an item in [spec/decisions/deferred.md](spec/decisions/deferred.md) only when its trigger occurs.

### The Method, Condensed

- Understand the situation, form a recommendation, and ask for a confirm. Never return the decision to the operator without a recommendation.
- Answer first from what the environment and the earlier answers already tell you; ask only what remains.
- State the decision in one sentence and list its two to four candidate outcomes.
- Ask about the operator's situation in past or present specifics ("Where does an agent's configuration live today, and who changes it?"), never about mechanisms ("which configuration key?").
- Ask the screening questions first, together, independent of one another, at most four. Each has two to four options that are exhaustive and exclusive; each option carries one line "Effect: ..." stating what it rules out or favors; exactly one option is marked "(recommended)", which is the answer assumed when the operator does not answer within the wait period, and it is never a description of a person doing by hand what a mechanism should do.
- Ask a follow-up marked "Ask only if ..." only after its guard is answered.
- After the answers, state what is ruled out and what remains.
- Recommend: how each outcome aligns with the answers, what separates the winner from the runner-up, and what would change the recommendation.
- Close with a confirm question of exactly three options: adopt (recommended), adjust, need more detail.
- Record the frame, the answers, and the outcome. At most five questions in two rounds per decision.
- A decision whose gated action is reversible proceeds on its recommended defaults after the wait period; tenancy scope waits for an answer, because its choice cannot be changed once data has been written under it, and execution proceeds as its document states.

## Editing The Specification

1. **Atomic key-word sentences under stable headings.** Each normative sentence uses one of the five key words the constitution defines, written in capitals, exactly once, and carries one obligation; a sentence that bundles two obligations is split into two; each sentence is its own paragraph under a heading in title case that does not change. Do not write a key word in capitals in a heading or in descriptive prose, because the gate extracts every sentence that contains one.
2. **Stay standalone.** No normative sentence mentions a concrete engine, store, provider, library, numeric width, or path. Write "the durable store", "a provider", "the configured bound". A concrete choice goes in spec/record.md.
3. **Write literally.** No idioms, no figurative language, no personification of systems, no colloquialisms. No proper nouns and no product, vendor, or technology names: write the category. The verb "name" is not used to mean carry, identify, specify, state, list, record, address, or refer to (write "carry its tenant", "identifies its parent", "addressed by its digest"); the noun stays. Only characters from the basic seven-bit character set: a colon or a new sentence replaces a dash, the word "section" replaces its sign, and quotes are straight.
4. **Use the glossary.** Add a term there before using it normatively, and use only the current form of a renamed term.
5. **Descriptive versus normative.** The overview, the glossary, the record, the readmes, the decision documents, the deferred register, and the traceability map are descriptive and are not listed in the gate; the constitution, the contracts, the core files, and the outcome files are normative, and every one of them is extracted. A normative file's header states that the key words are normative with the meaning the constitution gives them, that each requirement is one self-contained sentence with one obligation under a stable heading, and which of its values are declared defaults.
6. **Contracts change only under discipline.** A change to spec/contracts/ is additive, or carries a version increment and a migration path.
7. **Traceability is bidirectional.** Every normative file maps to an overview section, and every overview section is served by at least one normative file; update spec/traceability.md with the change.
8. **A governing requirement binds to the change process.** A requirement about how the specification or its configuration may change binds to the change-process check rather than to a runtime citation, and is not counted as gating for a build.
9. **The record and the gate selection agree.** When an outcome changes, the row in spec/record.md part A and the comment markers in .duvet/config.toml change together.

## Before You Finish

Extract every normative file you touched and confirm that the extraction succeeds; read your text once more for figurative language, for the verb "name", for proper nouns and technology names, and for characters outside the basic set; and confirm that the record and the gate selection still agree. The commands:

```sh
# extract the requirements of one file (the output directory must end with a slash)
duvet extract -f markdown -o /tmp/extract/ ./spec/core/governance.md

# run the full requirement gate against the selection in .duvet/config.toml
duvet report
```
