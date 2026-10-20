# Borg Runtime

Fleet and Task Board are maintained here as source components, alongside a
native Responses API executor.

Build a useful autonomous engineering team. Ship small, complete behavior,
including its callers and consumers. Use focused tests for changed behavior and
integration boundaries. Fix ordinary failures directly; do not introduce
approval ceremonies, mandatory charter citations, or narrower write restrictions
than a task actually needs.

## Components

- `components/task-board`: projects, tasks, agent definitions, documents and UI.
- `components/fleet`: process supervision and workspace materialization.
- `crates/native-runtime`: Responses API execution with independent sessions
  under stable agent identities.
- `tooling/workspace_mcp.py`: workspace tools for native sessions.

Each imported component retains its own Cargo manifest and lockfile. Run its
commands from that component's directory. The root Cargo workspace contains the
native executor. Imported component provenance is in `components/SOURCES.json`.

Models, reasoning effort, tools, workspace selection and operational policy
belong in metadata. Session transcripts and execution state are separate from
agent definitions. Task Board remains the coordination authority; native session
state must not become a second task board.

The user's selected native profile is `gpt-6-astra`, reasoning effort `max`,
128000 maximum output tokens and the full 1050000-token context window. Do not
introduce a lower input/output allowance, a short inference timeout, silent
transcript truncation, or a small default turn budget. Input and output share the
context window. API account rate limits are external constraints, not values a
client can override.

Preserve completed tool results during recovery. Record uncertain tool outcomes
and reconcile them before replaying consequential actions. Transient inference
errors should recover from committed conversation state.

For changes, run the relevant commands documented in README.md. Keep credentials,
local databases, logs and generated build outputs out of Git.
