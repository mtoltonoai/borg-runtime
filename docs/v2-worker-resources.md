# Worker resource verification

The worker can verify an already configured cgroup v2 before advertising capacity
or starting work. It does not create groups, move processes, change kernel limits
or install services. The operator establishes the worker's group through ordinary
host startup before children launch. Disk quotas remain separate work.

Pass the operator-owned profile to the ordinary worker command:

```sh
borg-runtime worker \
  --controller http://127.0.0.1:8100 \
  --id worker-a --incarnation UNIQUE_INCARNATION \
  --workspace-root /configured/worker-root \
  --codex /configured/pinned-codex \
  --resource-profile /configured/worker-profile.json \
  --coding-capacity 1 --native-capacity 1
```

These paths and the incarnation are placeholders, not provisioned targets. The
controller URL is local to the worker, including when an existing authenticated
SSH forward carries its traffic. The small capacities are for initial bring-up,
not evidence of the selected four-host workload. Existing executor/provider
configuration remains required; this option creates no credentials or access.

The profile's sole authored field definition is in the
[v2 interface](../../borg/spec/impl/v2/protocol.md#worker-resource-profile).
It supplies an absolute cgroup filesystem directory and exact finite CPU
quota/period, memory, swap and process ceilings. No numeric production defaults
are selected. Zero swap is valid; unlimited controls are not accepted as a finite
configured profile. The bounded JSON input rejects missing, duplicate, unknown,
invalid and overflowing fields.

The worker loads the profile once. It verifies its actual unified membership,
the opened directory's cgroup2 mount identity, normal domain type and exact leaf
controls. It does not assume a fixed mount location. Aliased directories,
ordinary files presented as controls, unavailable metadata and mismatches refuse.
Ancestor controls can reduce capacity further; matching leaf values proves no
throughput or headroom guarantee. This is a trusted-host check, not protection
against a privileged actor changing the kernel or moving descendants later.

Checks gate registration, work-producing command/delegation recovery and final
launch after awaited currentness checks. Existing exclusive worker ownership and
prior-process reconciliation happen before profile refusal. Refusal retains
journals and permits bounded delivery/reconciliation of retained reports; it does
not create new work. Existing shutdown and process ownership remain the
termination mechanism. No polling resource manager or per-task reservation is
introduced.

Without a profile, the worker explicitly reports unqualified development mode.
That mode preserves earlier local tests; it does not satisfy production resource
qualification. An invalid configured profile never falls back to that mode.

The [implementation receipt](../artifacts/v2/build-60156282/worker-envelope-implementation-e42rg9kw/completion.json)
separates local tests from real host enforcement. Actual finite-limit descendant
membership, CPU pressure, memory/PID exhaustion and recovery remain to test on
configured hosts. In particular, PID exhaustion can prevent the existing
`/bin/kill` helper from spawning during cold cleanup. Generous limits do not prove
that recovery path; the host supervisor and original process evidence need actual
qualification. No automatic retry may convert unknown execution or external
effects into a claimed stopped or settled outcome.
