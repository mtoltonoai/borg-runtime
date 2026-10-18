# Host resource observations

`controller` and `worker` accept `--host-observation-profile /absolute/profile.json`.
This versioned operator profile is separate from accepted task limits. Omission
disables collection and yields an empty Machines list. The example in
`metadata/machine-resources.example.json` is a template: select literal existing
local storage directories, configured machine/worker identities, and the actual
SHA-256 of the existing `/usr/bin/stat` binary before use. Its zero hash deliberately
makes disk measurements unavailable. Nothing installs a helper or creates scopes.

Each machine has one collector and one report flight with one replaceable latest
sample. Reporting is independent of task permits, command awaits, command journals
and capacity registration. Shutdown stops collection and joins the owned helper
and report before completion. An unconfirmed helper reap holds cleanup; it never
permits a new helper to overlap an unknown predecessor. Controller observations
are collected outside the SQL owner. Reports replace only ephemeral memory and
consume no mutation receipt, task revision or durable history budget.

CPU uses two bounded `/proc/stat` reads in the same boot and online-core scope.
Guest counters are already included in user/nice and are not added again. Busy is
total minus idle and iowait. Used core-equivalents equal busy/total times online
logical cores; percent is busy/total times100. First samples, counter resets, boot
or core changes and invalid intervals are explicit warming/unavailable values.
Memory is MemTotal minus MemAvailable, with checked KiB-to-byte conversion.

Disk is filesystem space, not recursive directory size or drive health. The fixed
hash-checked helper runs without a shell, with only configured directory arguments,
`--` and `LC_ALL=C`; bounded output, timeout and owned cleanup apply. Directory and
mount identities must match before/after. Supported local filesystem families are
ext4, xfs, btrfs and overlay. Shared filesystem identities are deduplicated and
carry all configured public scope labels. Total, used, free and service-available
bytes remain distinct. Missing helpers, inaccessible paths and failures yield
explicit unavailable values, never fabricated zeros or old successful metrics.

A report is scoped by configured machine role and current registered worker
incarnation. Sequence conflicts/older samples are refused; exact duplicates do not
renew age. Epoch ordering uses kernel boot identity and process-start ticks, not
wall time. A later same-boot worker process can restart sequence numbering under
the existing incarnation; delayed earlier processes cannot overwrite it. A reboot
requires ordinary worker-incarnation replacement/reconciliation. Equal start ticks
cannot establish newer authority and are refused on sequence conflict. Public
projections expose an epoch digest rather than kernel boot/process-start details.

Receiver monotonic age alone determines freshness. Producer wall time is
informational. Newer unavailable metrics replace successful ones; missing reports
age to stale. Controller restart yields unobserved until a new report arrives.
Existing MCP read receipts keep their original projection/age on replay.

The shared typed Machines list/detail projection serves native and Coding MCP
`collab_machines_list` / `collab_machine_get`, and `/machines`. These are read-only
catalogue tools with existing bounded page/cursor semantics. Machines is linked
from navigation and Workers; measured usage is explicitly separate from registered
capacity, occupancy and admitted cgroup limits. Browser refresh uses the existing
bounded fetch lifecycle without task-change events. Local age includes conservative
in-flight time, continues during disconnection, and does not bypass retry backoff.
Only the observations section is replaced, preserving unsent form input.

This independent branch selects wire24/schema25/worker19 from base23/24/18.
Schema25 is a vocabulary-only marker over the exact schema24 SQL: latest samples
are not durable. Startup validates typed historical controller receipts, native
journals, Coding journals and saved carriers before any marker upgrade or replay.
Historical row/journal bytes stay unchanged; unknown future markers are refused.
Other concurrent branches claiming these revisions require explicit composition
and final source-bound tooling/package qualification before any rollout.

The tests use local controlled peers/clocks/fixtures and actual Node builtins for
the browser script. They do not establish an installed four-host deployment.
