import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { filterDispatches, hostState, occupiesCapacity, statusLabel } from './native-fleet'
import { refreshNativeFleet, useNativeFleet } from './resources'

const linkClass = 'text-sky-700 dark:text-sky-400 underline decoration-dotted underline-offset-2'
const selectClass = 'rounded border border-[var(--color-border)] bg-[var(--color-panel)] px-2 py-1 text-xs'

function age(seconds: number, now: number): string {
  const delta = Math.floor(now - seconds)
  if (delta < -30) return 'clock skew'
  if (delta < 60) return `${Math.max(delta, 0)}s ago`
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`
  return `${Math.floor(delta / 3600)}h ago`
}

function tone(status: string): string {
  if (status === 'failed' || status === 'needs_resolution') return 'bg-red-500/15 text-red-800 dark:text-red-300'
  if (status === 'retrying') return 'bg-amber-500/15 text-amber-800 dark:text-amber-300'
  return 'bg-sky-500/10 text-sky-800 dark:text-sky-300'
}

export default function NativeFleet() {
  const { data, error, loading } = useNativeFleet()
  const [now, setNow] = useState(Date.now() / 1000)
  const [params, setParams] = useSearchParams()
  useEffect(() => {
    const timer = window.setInterval(() => {
      setNow(Date.now() / 1000)
      if (document.visibilityState !== 'hidden') refreshNativeFleet()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [])

  const hosts = data?.hosts ?? []
  const dispatches = data?.dispatches ?? []
  const filtered = filterDispatches(dispatches, {
    host: params.get('host') ?? '', agent: params.get('agent') ?? '',
    session: params.get('session') ?? '', attention: params.get('attention') === '1',
  })
  const pages = Math.max(1, Math.ceil(filtered.length / 50))
  const page = Math.min(pages, Math.max(1, Math.floor(Number(params.get('page'))) || 1))
  const visible = filtered.slice((page - 1) * 50, page * 50)
  const agents = [...new Set(dispatches.map((row) => row.agent_id))].sort()
  const active = dispatches.filter((row) => occupiesCapacity(row.status)).length
  const retrying = dispatches.filter((row) => row.status === 'retrying').length
  const attention = dispatches.filter((row) => row.status === 'failed' || row.status === 'needs_resolution').length
  const updateFilter = (key: string, value: string) => {
    const next = new URLSearchParams(params)
    if (value) next.set(key, value)
    else next.delete(key)
    if (key !== 'page') next.delete('page')
    setParams(next)
  }

  return (
    <main className="min-h-0 flex-1 overflow-y-auto p-5" data-testid="native-fleet">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h1 className="text-base font-semibold">Native Fleet and sessions</h1>
        <button type="button" className={selectClass} onClick={refreshNativeFleet} disabled={loading}>
          {loading ? 'Refreshing...' : 'Refresh now'}
        </button>
      </div>
      <p className="mb-4 max-w-3xl text-xs text-[var(--color-muted)]">
        Shared Board observations. One identity can have multiple independent sessions. Retrying inference is live work;
        an old progress timestamp alone is not proof of a dead worker. Unknown tool effects require reconciliation, not replay.
      </p>
      {error && (
        <div role="alert" className="mb-4 rounded border border-amber-500/50 bg-amber-500/10 p-3 text-sm">
          Native Fleet data unavailable: {error.message}. {data ? 'Showing the last observation, not current health.' : 'No verified host or session state.'} Retrying automatically.
        </div>
      )}
      {!data && loading && <p role="status" className="text-sm">Loading native host and session observations...</p>}
      {data && <>
        <div className="mb-4 flex flex-wrap gap-4 text-sm" aria-label="Native fleet totals">
          <span><strong>{hosts.length}</strong> registered hosts</span>
          <span><strong>{active}</strong> reserved or active sessions</span>
          <span><strong>{retrying}</strong> retrying</span>
          <span><strong>{attention}</strong> failed or unresolved</span>
          <span className="text-xs text-[var(--color-muted)]">Observed {age(data.observedAt / 1000, now)}</span>
        </div>
        <section aria-label="Native hosts" className="mb-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {hosts.map((host) => {
            const state = hostState(host, now)
            const healthy = state === 'Ready' || state === 'At capacity'
            return <article key={host.host_id} className="rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] p-3" data-host={host.host_id}>
              <h2 className="mb-2 text-sm font-semibold"><Link className={linkClass} to={`/native?host=${encodeURIComponent(host.host_id)}`}>{host.host_id}</Link></h2>
              <p className={`mb-2 text-xs ${healthy ? 'text-emerald-700 dark:text-emerald-300' : 'text-amber-800 dark:text-amber-300'}`}>{state}</p>
              <dl className="space-y-1 text-xs">
                <div><dt className="inline text-[var(--color-muted)]">Capacity: </dt><dd className="inline">{host.active} / {host.capacity} occupied</dd></div>
                <div><dt className="inline text-[var(--color-muted)]">Heartbeat: </dt><dd className="inline"><time title={new Date(host.last_seen * 1000).toISOString()}>{age(host.last_seen, now)}</time> (TTL {host.ttl_seconds}s)</dd></div>
                <div><dt className="inline text-[var(--color-muted)]">Deployed revision: </dt><dd className="inline"><code title={host.revision}>{host.revision ? host.revision.slice(0, 12) : 'Unreported'}</code></dd></div>
              </dl>
            </article>
          })}
          {hosts.length === 0 && <p className="text-sm text-[var(--color-muted)]">No native hosts registered. Worker deployment is not demonstrated.</p>}
        </section>
        <section aria-label="Native sessions">
          <h2 className="mb-3 text-sm font-semibold">Independent sessions ({filtered.length} shown by filter / {dispatches.length} total)</h2>
          <div className="mb-3 flex flex-wrap items-center gap-3">
            <label className="text-xs">Identity <select className={`ml-1 ${selectClass}`} value={params.get('agent') ?? ''} onChange={(event) => updateFilter('agent', event.target.value)}>
              <option value="">All identities</option>{agents.map((agent) => <option key={agent}>{agent}</option>)}
            </select></label>
            <label className="text-xs">Host <select className={`ml-1 ${selectClass}`} value={params.get('host') ?? ''} onChange={(event) => updateFilter('host', event.target.value)}>
              <option value="">All hosts</option>{hosts.map((host) => <option key={host.host_id}>{host.host_id}</option>)}
            </select></label>
            <label className="text-xs"><input type="checkbox" checked={params.get('attention') === '1'} onChange={(event) => updateFilter('attention', event.target.checked ? '1' : '')} /> Failed / unresolved only</label>
            {(params.size > 0) && <button type="button" className={linkClass + ' text-xs'} onClick={() => setParams({})}>Clear filters</button>}
          </div>
          <ul className="space-y-3">
            {visible.map((row) => <li key={row.id} id={`session-${row.session_id}`} data-session={row.session_id} className="rounded-lg border border-[var(--color-border)] bg-[var(--color-panel)] p-3">
              <div className="mb-2 flex flex-wrap items-center gap-2 text-xs">
                <span className={`rounded-full px-2 py-0.5 font-medium ${tone(row.status)}`}>{statusLabel(row.status)}</span>
                <Link className={linkClass} to={`/agents/${encodeURIComponent(row.agent_id)}`}>{row.agent_id}</Link>
                <span>on {row.host_id}</span>
                {row.task_id != null ? <Link className={linkClass} to={`/tasks/${row.task_id}`}>task_{row.task_id}</Link> : <span className="text-[var(--color-muted)]">Independent request (no task)</span>}
                <span className="ml-auto text-[var(--color-muted)]">Updated {age(row.updated_at, now)}</span>
              </div>
              <div className="break-all text-xs"><span className="text-[var(--color-muted)]">Session </span><Link className={linkClass + ' font-mono'} to={`/native?session=${encodeURIComponent(row.session_id)}`}>{row.session_id}</Link></div>
              <p className="mt-2 break-words whitespace-pre-wrap text-sm">{row.progress || 'No progress detail reported yet.'}</p>
              <dl className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-[var(--color-muted)]" aria-label="Runtime progress">
                <div><dt className="inline">Phase: </dt><dd className="inline">{row.metadata?.runtime?.phase ?? 'Unreported'}</dd></div>
                <div><dt className="inline">Step: </dt><dd className="inline">{row.metadata?.runtime?.step ?? 'Unreported'}</dd></div>
                <div><dt className="inline">Driver at observation: </dt><dd className="inline">{row.metadata?.runtime?.driver_active === true ? 'Active' : row.metadata?.runtime?.driver_active === false ? 'Inactive' : 'Unreported'}</dd></div>
                {typeof row.metadata?.runtime?.last_progress_at === 'number' && <div><dt className="inline">Engine progress: </dt><dd className="inline">{age(row.metadata.runtime.last_progress_at > 100_000_000_000 ? row.metadata.runtime.last_progress_at / 1000 : row.metadata.runtime.last_progress_at, now)}</dd></div>}
              </dl>
              {row.metadata?.observation_error_at != null && <p className="mt-2 text-xs text-amber-800 dark:text-amber-300">Runtime observation failed; the status above is the last known state, not proof of liveness.</p>}
              {row.error && <p className="mt-2 break-words whitespace-pre-wrap text-xs text-red-800 dark:text-red-300" role="note">Failure: {row.error}</p>}
              <details className="mt-2 text-xs text-[var(--color-muted)]">
                <summary className="cursor-pointer">Dispatch and workspace binding</summary>
                <dl className="mt-2 space-y-1 break-all">
                  <div><dt className="inline">Request: </dt><dd className="inline font-mono">{row.request_id}</dd></div>
                  <div><dt className="inline">Workspace: </dt><dd className="inline font-mono">{row.workspace_cwd}</dd></div>
                  <div><dt className="inline">Generation / report version: </dt><dd className="inline">{row.generation} / {row.version}</dd></div>
                  <div><dt className="inline">Reported: </dt><dd className="inline">{new Date(row.updated_at * 1000).toISOString()}</dd></div>
                </dl>
              </details>
            </li>)}
          </ul>
          {filtered.length === 0 && <p className="text-sm text-[var(--color-muted)]">No session dispatches match. This is not evidence that unregistered runtime sessions do not exist.</p>}
          {pages > 1 && <nav aria-label="Session pages" className="mt-3 flex items-center gap-3 text-xs">
            <button type="button" className={selectClass} disabled={page <= 1} onClick={() => updateFilter('page', String(page - 1))}>Previous</button>
            <span>Page {page} / {pages}</span>
            <button type="button" className={selectClass} disabled={page >= pages} onClick={() => updateFilter('page', String(page + 1))}>Next</button>
          </nav>}
        </section>
      </>}
    </main>
  )
}
