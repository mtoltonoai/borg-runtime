import assert from 'node:assert/strict'
import test from 'node:test'
import { filterDispatches, hostState, loadNativeFleet, occupiesCapacity, statusLabel } from '../src/native-fleet.ts'

const now = 1792464000
const host = { host_id: 'agent-lab', capacity: 4, active: 2, enabled: true, runtime_ready: true,
  revision: 'a'.repeat(40), last_seen: now, ttl_seconds: 60, fresh: true }
const row = (id = 1, extra = {}) => ({ id, request_id: `request:${id}`, session_id: `session-${id}`,
  agent_id: 'engineer', task_id: id, host_id: 'agent-lab', workspace_cwd: `/work/session-${id}`,
  generation: 1, version: 2, status: 'running', progress: 'Waiting for model', error: null,
  created_at: now - 10, updated_at: now, ...extra })

test('freshness is recomputed as cached heartbeats age, never frozen green', () => {
  assert.equal(hostState(host, now), 'Ready')
  assert.equal(hostState(host, now + 61), 'Stale heartbeat')
  assert.equal(hostState(host, now - 31), 'Stale heartbeat')
  assert.equal(hostState({ ...host, runtime_ready: false }, now), 'Runtime unavailable')
  assert.equal(hostState({ ...host, enabled: false }, now), 'Draining')
  assert.equal(hostState({ ...host, capacity: 0 }, now), 'Draining')
  assert.equal(hostState({ ...host, active: 4 }, now), 'At capacity')
})

test('independent same-identity sessions remain separate; retry is not failed', () => {
  const rows = [row(1, { status: 'retrying' }), row(2, { status: 'needs_resolution' }), row(3, { agent_id: 'other' })]
  assert.equal(filterDispatches(rows, { agent: 'engineer' }).length, 2)
  assert.deepEqual(filterDispatches(rows, { attention: true }).map((r) => r.id), [2])
  assert.deepEqual(filterDispatches(rows, { session: 'session-1' }).map((r) => r.id), [1])
  assert.equal(filterDispatches(rows, { host: 'missing' }).length, 0)
  assert.equal(occupiesCapacity('retrying'), true)
  assert.equal(occupiesCapacity('needs_resolution'), true)
  assert.equal(occupiesCapacity('failed'), false)
  assert.equal(statusLabel('needs_resolution'), 'Needs tool reconciliation')
  assert.match(statusLabel('new-state'), /Unknown status/)
})

test('pages beyond the 100-row default and preserves every independent session', async () => {
  const calls = []
  const result = await loadNativeFleet(async (path) => {
    calls.push(path)
    if (path === '/fleet/hosts') return [host]
    if (path.endsWith('offset=0')) return Array.from({ length: 1000 }, (_, index) => row(index + 1))
    if (path.endsWith('offset=1000')) return [row(1001)]
    throw new Error('unexpected path')
  })
  assert.equal(result.dispatches.length, 1001)
  assert.equal(result.dispatches.at(-1).session_id, 'session-1001')
  assert.equal(calls.length, 3)
  assert.equal(result.hosts[0].revision, 'a'.repeat(40))
})

test('ignored pagination and malformed success responses fail visibly', async () => {
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host]
    : Array.from({ length: 1000 }, (_, index) => row(index + 1))), /pagination repeated/)
  await assert.rejects(loadNativeFleet(async () => '<html>not a registry</html>'), /JSON array/)
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [{ ...host, runtime_ready: undefined }] : []), /runtime_ready/)
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host, host] : []), /Duplicate native host/)
})

test('API failure is not converted to an empty successful fleet', async () => {
  await assert.rejects(loadNativeFleet(async () => { throw new Error('HTTP 503') }), /HTTP 503/)
})

test('optional new progress fields do not hide bootstrap records', async () => {
  const result = await loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host] : [row()])
  assert.equal(result.dispatches[0].metadata, undefined)
  assert.equal(result.dispatches[0].progress, 'Waiting for model')
})

test('invalid timestamps fail before rendering date formatting', async () => {
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [{ ...host, last_seen: 1e30 }] : []), /timestamp/)
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host] : [row(1, { updated_at: NaN })]), /updated_at/)
})


test('malformed optional runtime projection becomes a visible API error, not a render crash', async () => {
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host]
    : [row(1, { metadata: { runtime: { phase: { invalid: true } } } })]), /runtime phase/)
  await assert.rejects(loadNativeFleet(async (path) => path === '/fleet/hosts' ? [host]
    : [row(1, { metadata: { runtime: { driver_active: 'yes' } } })]), /driver observation/)
})
