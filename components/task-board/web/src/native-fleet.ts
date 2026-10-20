// Native Fleet's Board projection, not requests to a browser's localhost runtime.
// Agent identity and session identity intentionally remain separate rows.
export interface NativeHost {
  host_id: string
  capacity: number
  active: number
  enabled: boolean
  runtime_ready: boolean
  revision: string
  ttl_seconds: number
  last_seen: number
  fresh: boolean
}

export interface NativeRuntimeProjection {
  phase?: string
  step?: number
  last_progress_at?: number
  updated_at?: number
  driver_active?: boolean
}

export interface NativeDispatch {
  id: number
  request_id: string
  session_id: string
  agent_id: string
  task_id: number | null
  host_id: string
  workspace_cwd: string
  generation: number
  version: number
  status: string
  progress: string | null
  error: string | null
  updated_at: number
  created_at: number
  metadata?: {
    runtime?: NativeRuntimeProjection
    observed_at?: number
    observation_error_at?: number
    fleet_revision?: string
  }
}

export interface NativeFleetSnapshot {
  hosts: NativeHost[]
  dispatches: NativeDispatch[]
  observedAt: number
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Malformed native Fleet row')
  return value as Record<string, unknown>
}

function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error('Native Fleet API did not return a JSON array')
  return value
}

function host(value: unknown): NativeHost {
  const row = object(value)
  for (const key of ['host_id', 'revision']) {
    if (typeof row[key] !== 'string') throw new Error(`Missing native host ${key}`)
  }
  for (const key of ['capacity', 'active', 'ttl_seconds', 'last_seen']) {
    if (typeof row[key] !== 'number' || !Number.isFinite(row[key]) || row[key] < 0) throw new Error(`Invalid native host ${key}`)
  }
  for (const key of ['enabled', 'runtime_ready', 'fresh']) {
    if (typeof row[key] !== 'boolean') throw new Error(`Missing native host ${key}`)
  }
  if ((row.last_seen as number) > 8_640_000_000_000) throw new Error('Invalid native host timestamp')
  return row as unknown as NativeHost
}

function dispatch(value: unknown): NativeDispatch {
  const row = object(value)
  for (const key of ['id', 'generation', 'version', 'updated_at', 'created_at']) {
    if (typeof row[key] !== 'number' || !Number.isFinite(row[key]) || row[key] < 0) throw new Error(`Invalid native dispatch ${key}`)
  }
  for (const key of ['request_id', 'session_id', 'agent_id', 'host_id', 'workspace_cwd', 'status']) {
    if (typeof row[key] !== 'string' || !row[key]) throw new Error(`Missing native dispatch ${key}`)
  }
  for (const key of ['progress', 'error']) {
    if (row[key] != null && typeof row[key] !== 'string') throw new Error(`Invalid native dispatch ${key}`)
  }
  if (row.task_id != null && (typeof row.task_id !== 'number' || row.task_id <= 0)) throw new Error('Invalid native dispatch task_id')
  if (row.metadata != null) {
    const metadata = object(row.metadata)
    if (metadata.runtime != null) {
      const runtime = object(metadata.runtime)
      if (runtime.phase != null && typeof runtime.phase !== 'string') throw new Error('Invalid native runtime phase')
      if (runtime.driver_active != null && typeof runtime.driver_active !== 'boolean') throw new Error('Invalid native runtime driver observation')
      for (const key of ['step', 'last_progress_at', 'updated_at']) {
        if (runtime[key] != null && (typeof runtime[key] !== 'number' || !Number.isFinite(runtime[key]))) throw new Error(`Invalid native runtime ${key}`)
      }
    }
  }
  if ((row.updated_at as number) > 8_640_000_000_000) throw new Error('Invalid native dispatch timestamp')
  return row as unknown as NativeDispatch
}

// The API defaults to 100 rows. Read every page or a busy fleet's history/counts
// would silently disappear. Detect ignored offset/duplicate rows rather than loop.
export async function loadNativeFleet(get: (path: string) => Promise<unknown>): Promise<NativeFleetSnapshot> {
  const hostsPromise = get('/fleet/hosts').then((value) => array(value).map(host))
  const dispatchesPromise = (async () => {
    const result: NativeDispatch[] = []
    const seen = new Set<number>()
    const limit = 1000
    for (let offset = 0; ; offset += limit) {
      const page = array(await get(`/fleet/dispatches?limit=${limit}&offset=${offset}`)).map(dispatch)
      for (const row of page) {
        if (seen.has(row.id)) throw new Error('Native dispatch pagination repeated a row; retrying a coherent observation')
        seen.add(row.id)
        result.push(row)
      }
      if (page.length < limit) return result
    }
  })()
  const [hosts, dispatches] = await Promise.all([hostsPromise, dispatchesPromise])
  if (new Set(hosts.map((row) => row.host_id)).size !== hosts.length) throw new Error('Duplicate native host records')
  return { hosts, dispatches, observedAt: Date.now() }
}

const terminal = new Set(['completed', 'failed', 'stopped', 'cancelled'])
export function occupiesCapacity(status: string): boolean {
  return !terminal.has(status)
}

export function hostState(host: NativeHost, nowSeconds = Date.now() / 1000): string {
  if (!host.enabled || host.capacity === 0) return 'Draining'
  if (!host.runtime_ready) return 'Runtime unavailable'
  // The server's fresh flag is a snapshot. Recompute age while showing cached data.
  if (!host.fresh || nowSeconds - host.last_seen > host.ttl_seconds || nowSeconds < host.last_seen - 30) return 'Stale heartbeat'
  return host.active >= host.capacity ? 'At capacity' : 'Ready'
}

export function statusLabel(status: string): string {
  const labels: Record<string, string> = {
    reserved: 'Reserved', provisioning: 'Provisioning workspace', launching: 'Launching',
    ready: 'Ready', running: 'Running', retrying: 'Retrying', waiting: 'Waiting',
    stopping: 'Stopping', stopped: 'Stopped', completed: 'Completed', failed: 'Failed',
    needs_resolution: 'Needs tool reconciliation', cancelled: 'Cancelled',
  }
  return labels[status] ?? `Unknown status: ${status}`
}

export function filterDispatches(rows: NativeDispatch[], filters: { host?: string; agent?: string; session?: string; attention?: boolean }): NativeDispatch[] {
  return rows.filter((row) => (!filters.host || row.host_id === filters.host)
    && (!filters.agent || row.agent_id === filters.agent)
    && (!filters.session || row.session_id === filters.session)
    && (!filters.attention || row.status === 'failed' || row.status === 'needs_resolution'))
    .sort((left, right) => right.updated_at - left.updated_at || right.id - left.id)
}
