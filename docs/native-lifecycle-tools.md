# Native lifecycle and session workspace tools

Native lifecycle MCP is served by the native runtime at
`POST /mcp/sessions/{caller_session_id}`. It implements stateless Streamable HTTP
with JSON responses, initialization, notifications, discovery and tool calls.
The ordinary runtime bearer policy also protects this endpoint. Browser origins
are rejected; unauthenticated loopback service requires a loopback Host header.
The URL binds the caller's durable session. Tool arguments cannot change its
agent identity. Target controls and reads default to the same identity; explicit
metadata grants can authorize named targets or any newly defined identity.
This is a trusted service boundary, not a shell security sandbox.

## Metadata is the selection and permission authority

Grant an effective Board `tool` configuration entry (agent or role inheritance):

```json
{
  "id": "native",
  "enabled": true,
  "payload": {
    "kind": "mcp",
    "name": "native",
    "url": "{runtime_url}/mcp/sessions/{session_id}",
    "tools_allowlist": [
      "session_create", "session_list", "session_get", "session_steer",
      "session_stop", "session_resume", "session_resolve_tool"
    ],
    "allowed_agent_ids": []
  }
}
```

The Board write tool uses `entry_id` for the entry's `id`. The runtime resolves
session URL templates before discovery and persists the resolved tool-server
configuration with each committed tool intent. The lifecycle service rechecks
the current metadata on every request. An omitted allowlist grants the catalog;
an empty allowlist grants none. Removing or disabling the effective grant
removes access. The provider sees names such as `native__session_create`.

`allowed_agent_ids` is an additional target permission on the native MCP grant.
Omitted or `[]` means own identity only. A list such as `["native-core",
"tools-engineer"]` additionally permits those exact targets; `["*"]` is the
explicit PM grant for any existing or newly metadata-defined identity. Own
identity is always included. `session_create` and `session_list` accept an
optional target `agent_id`; other controls resolve the target's actual owner and
check this grant. None of this changes the caller's attribution. Revocation is
checked on every call, not only when the model discovered its catalog.

Core binds `X-Fleet-Agent` from the session's durable agent ID for every MCP
endpoint, including Board aliases. A trusted `bound_agent_id` is persisted with
tool intents and scoped into the transport catalog cache. A legacy
`headers_env.X-Fleet-Agent` is bypassed when bound: no model `register_agent`
call or per-agent host environment variable is needed. Secret authorization
headers can still use their existing environment references.

`{runtime_url}` expands to the runtime service origin/base (an advertised `/api`
suffix is removed before adding MCP routes). Template expansion runs after
workspace/native placement overrides so a literal `{session_id}` cannot become
an endless discovery 404.

## Tools and durable results

| MCP tool | Arguments | Behavior |
| --- | --- | --- |
| `session_create` | `input`, optional target `agent_id`, `task_id`, `request_id` | Creates a distinct concurrent authorized session via Fleet in Board mode. A task association does not reassign or complete the Board task. |
| `session_list` | optional target `agent_id`, `offset`, `limit` (1–500) | Lists the selected authorized identity's local sessions and cross-host Fleet dispatches, with a next offset. |
| `session_get` | `session_id`, optional `include_transcript`, `include_tools`, `after_sequence`, `limit` (1–1000) | Reads status/placement and optional original tool results or bounded transcript. A queued dispatch can have no runtime session yet. |
| `session_steer` | `session_id`, `text` | Durably appends input; a stopped session stays stopped until resumed. |
| `session_stop` | `session_id`, optional `cancel` | By default, drains the active tool receipt before stopping. Immediate cancellation can leave an unknown outcome and is refused for the caller's own active tool. |
| `session_resume` | `session_id` | Continues from committed transcript; refuses unknown tool outcomes. |
| `session_resolve_tool` | `session_id`, `call_id`, `result` (required, may be null) | Records the verified original outcome without executing the tool again. An identical committed resolution is idempotent; a conflicting one is rejected. |

A create receipt includes `session_id`, `agent_id`, `parent_session_id`, the
namespaced `request_id`, and either `dispatch` or the local `session`. Fleet
persists `metadata.parent_session_id` too. The optional explicit request key is
scoped to the caller session. Otherwise the native tool operation identity is
used. An exact retry of that intent returns its original session; changed intent
conflicts. In local JSON-only mode a deterministic UUID provides equivalent
create idempotency, but Fleet provisioning is not available. Production Board
mode always uses the Fleet broker; it does not silently create a local session
when capacity or the Board is unavailable.

All lifecycle tool results include MCP text content, `structuredContent`, and
`isError`. Pre-dispatch validation, permission and known state conflicts produce
truthful failed tool results. The HTTP client never follows redirects or retries
mutating requests. A lost or malformed response after a remote mutation leaves
an **unknown outcome**, not a fabricated failure or success. Inspect the original
session/dispatch/tool record and external effect, then resolve its verified
result. Never use a new create key to bypass an uncertain old operation.

## Fleet broker and native HTTP boundary

Board-mode create calls `POST /api/fleet/dispatches` with
`{request_id, agent_id, task_id, input, metadata}`. Fleet owns atomic host
reservation, capacity, the session UUID, and provisioning. The endpoint returns
the durable dispatch immediately; provisioning may still be pending. Reads use
Board's `agent_id`, `session_id`, `offset`, and `limit` filters, and enforce the
target-permission check again. Remote controls use the dispatch's `runtime_url`.
That URL must identify the same host from every runtime (host-specific SSH
loopback forwards are valid; one ambiguous loopback URL for all hosts is not).

Fleet creates the runtime session with `POST /api/sessions`:

```json
{
  "agent_id": "engineer",
  "task": "task_5",
  "input": "Perform the assigned work.",
  "session_id": "a2cab3d7-b560-4319-b161-ad3987de362a",
  "host_id": "agent-lab-4",
  "workspace_cwd": "/fleet/workspaces/a2cab3d7-b560-4319-b161-ad3987de362a",
  "workspace_mcp_url": "http://127.0.0.1:8791/mcp/{session_id}",
  "lifecycle_mcp_url": "http://127.0.0.1:8787/mcp/sessions/{session_id}",
  "generation": 1,
  "metadata": {"dispatch_id": 42}
}
```

`request_id` is a compatibility alias for `session_id` on the native HTTP route,
not a second idempotency field; supplying both is rejected. Fleet uses
`session_id`. Session placement is durable and immutable, not an identity-wide
workspace pointer. Unknown fields are rejected. Existing get/list/transcript/
steer/stop/resume/resolve-tool routes remain available. Transcript HTTP reads can
specify `after_sequence` and `limit`; omitting bounds preserves the legacy API.

`POST /api/sessions/{id}/bind` explicitly adopts a drained bootstrap session
using `SessionOptions` fields and a nondecreasing generation. Core rejects
active drivers and unknown tool outcomes; original transcript and completed
receipts remain intact. This privileged HTTP placement operation is not exposed
as a model lifecycle tool. A normal create never changes existing placement.

`POST /api/sessions/{id}/drain` requests a receipt-preserving session stop.
`POST /api/quiesce` closes runtime admission and requests a safe turn boundary;
it is a request, not proof all in-flight work has drained. Inspect state before
stopping the process. `POST /api/unquiesce` reopens admission and drives Ready
sessions, without resuming intentionally stopped sessions. The public `/health`
reports `host_id`, build `revision`, process-instance `instance_id`, `ready`, and
`draining`. Build with `BORG_BUILD_REV` for an immutable revision claim; release
checks should corroborate it with executable hashes.

## Persistent shared workspace service

Run independently of Fleet so a Fleet restart cannot kill an active shell or
lose its result:

```sh
mkdir -p /fleet/workspaces
python3 tooling/workspace_mcp.py --session-root /fleet/workspaces --port 8791
```

Fleet provisions `/fleet/workspaces/<session UUID>` before starting a session.
`/mcp/<session UUID>` accepts only canonical UUIDs naming existing real
subdirectories. Missing workspaces are not auto-created. Symlinks, traversal,
aliases, query strings, and cross-session model-selected roots are refused. Each
request opens and pins its directory inode, including shell cwd, so replacing
the pathname cannot redirect an already-bound request. Filesystem/cwd isolation
does not make trusted shell commands a security sandbox.

Legacy `--root WORKSPACE` at `/mcp` is unchanged for bootstrap compatibility.
The two root modes are mutually exclusive. `/health` returns `mode`,
`instance_id`, `revision`, and service status without allocating a workspace.
Non-loopback binding requires the configured bearer token environment variable.
For deployment, drain native tool receipts before replacing the workspace
service; never restart the bootstrap helper underneath an active tool.

## Checks

```sh
python3 -m unittest discover -s tests -v
cargo test --locked -p native-runtime --test lifecycle
cargo test --locked -p native-runtime
```

The lifecycle suite uses the real native HTTP/MCP client/server and durable
runtime, deterministic model peers, and the actual Python workspace process.
It covers same-identity self-delegation/concurrency, exact create idempotency,
metadata permission changes, default cross-identity denial, explicit target and
wildcard grants with hot revocation, controls and bounded reads,
self-stop receipt preservation, unknown-effect resolution without replay, and
per-session filesystem/tool endpoint isolation. Deployed multi-host acceptance
is a separate live check, not inferred from a unit test.

## Live model delegation evidence

After Fleet is deployed, give a fresh metadata-defined identity a parent task
that writes `native-lifecycle-proof.json` with `{role:"parent",session_id,token}`,
creates exactly one same-identity child through `native__session_create` with a
stable request key, and asks that child to write the same relative filename with
`role:"child"` in its own worktree. Both should use `workspace_write`; the parent
checks that its marker is unchanged after the child completes. Do not create a
new dispatch to recover an uncertain response; inspect the original request.

The read-only collector joins the original native operation, Fleet request,
child UUID, actual workspace markers, and observed concurrent drivers:

```sh
python3 tooling/native_lifecycle_proof.py \
  --request-id STABLE_PARENT_FLEET_REQUEST --token PUBLIC_PROOF_TOKEN \
  --trace .runtime/lifecycle-proof.jsonl --watch-seconds 240
```

Rerunning only performs reads and preserves overlap samples in the trace. It
never enqueues, steers, resumes or resolves a session. It refuses to infer
concurrent execution from timestamps alone, or pass based only on model claims.
A waiting/blocked/failed proof exits 2 and retains evidence; this is not an
inference deadline or an instruction to restart a slow live session.
