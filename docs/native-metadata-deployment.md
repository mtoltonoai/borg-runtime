# Native Board metadata deployment

The Board is the runtime profile/tool authority. This is the repeatable data
update used for project 1, not an approval gate or a replacement host config.
Integrator still owns activation; Fleet owns reachable runtime origins, REST
`/api` URLs, tunnels, capacity and session workspace provisioning.

`config/native-board-profiles.json` prepares the six existing identities and
seven roles (`pm`, `native-core`, `fleet-engineer`, `tools-engineer`,
`recovery-engineer`, `integrator`, and reusable `native-engineer`).

```sh
# Inspect desired changes (GET only):
python3 tooling/configure_native_board.py
# Apply directly; only unequal entries are updated, each followed by read-back:
python3 tooling/configure_native_board.py --apply \
  --report-out .runtime/acceptance/metadata-applied.json
# Independent effective-role/agent read-back (GET only):
python3 tooling/configure_native_board.py --verify \
  --report-out .runtime/acceptance/metadata-verified.json
```

Use `--board` for the shared Board URL, `--actor` for attribution, and optional
`--token-env` for an existing credential reference. No credential values are
stored or printed. Equal entries are skipped so repeating the command does not
create configuration-change storms. An interrupted mutation is **not** blindly
reissued: read-back must establish its exact value. Unknown/unconfirmed receipts
are explicit errors. Unexpected enabled grants and agent overrides remain
visible, rather than silently widening access or being overwritten.

## Exact profile

The role `runtime/profile` entry and the selected existing identities retain:

- model `gpt-6-astra`, reasoning effort `max`;
- 128000 maximum output tokens; full 1050000 shared context; `max_turns: null`;
- `max_retries: null`, initial delay 5000ms, local backoff cap 60000ms, jitter 20%;
- existing Responses API URL and `OPENAI_API_KEY` **environment reference**, not
  the separate Codex login or a per-identity credential.

Core honors server Retry-After/rate-reset floors even above the local backoff
cap. Transient inference and retryable read-only discovery never terminate due
to exhaustion. Explicit stop and unknown-write reconciliation are preserved.

Core precedence is defaults, effective role/agent `runtime/profile`, recognized
flat identity metadata, then `metadata.native`. Existing chosen flat limits are
kept during migration. A new identity can inherit everything with only a role,
charter and `metadata.harness: native`.

## Exact tool grants

Each role has stable entries `board`, `workspace`, `native`, `web`:

- `board`: MCP `http://127.0.0.1:8079/mcp`. **No per-identity X-Fleet-Agent
  environment mapping.** Core derives `bound_agent_id` from the durable native
  session, overrides stale host attribution, and retains it on tool intents.
- `workspace`: MCP `http://127.0.0.1:8791/mcp/{session_id}` with four workspace
  methods. Core substitutes an explicit Fleet/bootstrap placement binding for
  future authorized calls; it never reroutes original committed tool receipts.
- `native`: MCP `{runtime_url}/mcp/sessions/{session_id}`, where runtime_url is
  the **origin**, not Fleet's REST URL ending in `/api`. Method allowlist is
  `session_create`, `session_list`, `session_get`, `session_steer`, `session_stop`,
  `session_resume`, `session_resolve_tool`.
- `web`: provider tool `{"type":"web_search"}`.

Only the PM role's native grant explicitly carries `allowed_agent_ids: ["*"]`:
it can create/control configured existing or newly registered identities.
Engineer/integrator roles omit that field and remain own-identity-only. The
lifecycle adapter binds the actual caller from its durable session; target-agent
permission never changes caller attribution. Revoking the grant must take effect
on later lifecycle calls. The reusable `native-engineer` role has no cross-agent
grant.

The migration also retires the duplicate `web_search` entry introduced during
parallel metadata preparation, but only when its payload exactly matches that
recorded provider alias. It removes the native-core retry-only agent override
only when it matches its recorded predecessor; the role supplies the full chosen
profile. Neither migration silently removes unrelated/concurrently changed
configuration.

## New identity without host edits

For an acceptance identity, register an ordinary Board agent with a charter and
metadata such as:

```json
{
  "agent_id": "native-acceptance",
  "display_name": "Native acceptance engineer",
  "charter": "Execute only the assigned acceptance task; preserve receipts and report evidence.",
  "metadata": {"role": "native-engineer", "harness": "native"}
}
```

No `BORG_AGENT_NATIVE_ACCEPTANCE` environment variable or host identity JSON
entry is needed. Do not override `bound_agent_id`; the runtime owns it. Fleet
uses its configured repository/ref unless the identity/task explicitly supplies
`metadata.workspace: {"repo": "...", "ref": "..."}`. Do not use observational
`metadata.host` as placement. Assign a task or use the PM's lifecycle grant to
request a session through Fleet.

After integrated activation, verify the new identity in each runtime's effective
profile list, its correctly attributed Board tool result, parent/child lifecycle
receipts and per-session workspace effects. Board metadata read-back alone is
preparation evidence, not proof of those live behaviors.

## Regression tests

```sh
python3 -m unittest discover -s tests -p test_configure_native_board.py -v
```

Tests cover no-write planning, exact effective config, repeated apply without
writes, full limit enforcement, explicit cross-identity grant, alias cleanup,
concurrent-config protection, and an actual HTTP server that commits an update
then loses the reply (one mutation, reconciled read-back).
