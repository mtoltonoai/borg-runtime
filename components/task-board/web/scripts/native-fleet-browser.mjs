#!/usr/bin/env node
// Dependency-free Chromium/CDP integration test of the actual built Board UI.
// Serve controlled real HTTP fixtures under /board (not browser localhost native APIs).
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { readFile, mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { dirname, extname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const binary = process.env.CHROMIUM_BIN
if (!binary) throw new Error('Set CHROMIUM_BIN to an installed Chromium/headless-shell binary')
const dist = fileURLToPath(new URL('../dist/', import.meta.url))
const profile = await mkdtemp(join(tmpdir(), 'native-board-ui-'))
const now = Math.floor(Date.now() / 1000)
const revision = 'a'.repeat(40)
const hosts = Array.from({ length: 4 }, (_, i) => ({ host_id: i ? `agent-lab-${i + 1}` : 'agent-lab',
  capacity: 4, active: 1, enabled: i !== 3, runtime_ready: i !== 1, fresh: i !== 2,
  revision, last_seen: i === 2 ? now - 70 : now, ttl_seconds: 60 }))
const dispatch = (id, status) => ({ id, request_id: `task:${id}:attempt:1`, session_id: `11111111-1111-4111-8111-${String(id).padStart(12, '0')}`,
  agent_id: 'engineer', task_id: id, host_id: 'agent-lab', workspace_cwd: `/sessions/${id}`,
  generation: 1, version: 2, status, progress: status === 'retrying' ? 'Retrying inference after provider rate limit' : '<img src=x onerror="window.injected=true">',
  error: status === 'needs_resolution' ? 'Unknown tool result; reconciliation required' : null,
  created_at: now - 20, updated_at: now,
  metadata: { runtime: { phase: 'awaiting-model', step: 42, driver_active: true, last_progress_at: (now - 300) * 1000 } } })
const dispatches = [dispatch(1, 'retrying'), dispatch(2, 'needs_resolution')]
let unavailable = false
const paths = []
const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost')
    paths.push(url.pathname + url.search)
    const path = url.pathname.replace(/^\/board/, '')
    if (path === '/api/stream') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      res.write(': fixture\n\n')
      return
    }
    if (path.startsWith('/api/')) {
      res.setHeader('Content-Type', 'application/json')
      if (unavailable && path.startsWith('/api/fleet/')) {
        res.writeHead(503)
        res.end(JSON.stringify({ error: 'Fixture Board unavailable' }))
        return
      }
      let value = []
      if (path === '/api/fleet/hosts') value = hosts
      if (path === '/api/fleet/dispatches') value = dispatches
      if (path === '/api/projects') value = [{ id: 1, name: 'Native delivery', status: 'active', task_counts: {} }]
      if (path === '/api/agents') value = [{ id: 'engineer', display_name: 'Engineer', status: 'busy', metadata: {} }]
      if (path === '/api/system/link-rules') value = { link_rules: [] }
      if (path === '/api/crash-reports') value = { task_id: 999, created: false }
      res.end(JSON.stringify(value))
      return
    }
    if (path.startsWith('/assets/')) {
      const target = resolve(dist, '.' + path)
      if (!target.startsWith(resolve(dist) + '/')) throw new Error('path outside assets')
      const types = { '.js': 'text/javascript', '.css': 'text/css', '.woff2': 'font/woff2', '.svg': 'image/svg+xml' }
      res.setHeader('Content-Type', types[extname(path)] ?? 'application/octet-stream')
      res.end(await readFile(target))
      return
    }
    res.setHeader('Content-Type', 'text/html')
    res.end((await readFile(join(dist, 'index.html'), 'utf8')).replace('<head>', '<head><base href="/board/">'))
  } catch {
    res.writeHead(500)
    res.end('Fixture request failed')
  }
})
server.listen(0, '127.0.0.1')
await once(server, 'listening')
const port = server.address().port
const child = spawn(binary, ['--no-sandbox', '--headless', '--disable-gpu', '--remote-debugging-address=127.0.0.1', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] })
let stderr = ''
child.stderr.on('data', (chunk) => { stderr = (stderr + chunk).slice(-4000) })
let socket
const pending = new Map()
let nextId = 0
const exceptions = []
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
async function until(fn, label, timeout = 15000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    const value = await fn()
    if (value) return value
    await sleep(100)
  }
  throw new Error(`Timed out: ${label}`)
}
function call(method, params = {}) {
  const id = ++nextId
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)) }, 10000)
    pending.set(id, { resolve, reject, timer })
    socket.send(JSON.stringify({ id, method, params }))
  })
}
async function evaluate(expression) {
  const response = await call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true })
  if (response.exceptionDetails) throw new Error(response.exceptionDetails.text)
  return response.result.value
}
try {
  const debugPort = await until(async () => {
    if (child.exitCode != null) throw new Error('Chromium exited before startup: ' + stderr)
    try { return Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]) } catch { return null }
  }, 'Chromium startup')
  const target = await (await fetch(`http://127.0.0.1:${debugPort}/json/new?about:blank`, { method: 'PUT', signal: AbortSignal.timeout(10000) })).json()
  socket = new WebSocket(target.webSocketDebuggerUrl)
  socket.addEventListener('message', ({ data }) => {
    const value = JSON.parse(data)
    if (value.method === 'Runtime.exceptionThrown') exceptions.push(value.params.exceptionDetails.text)
    const handler = pending.get(value.id)
    if (handler) {
      clearTimeout(handler.timer)
      pending.delete(value.id)
      if (value.error) handler.reject(new Error(value.error.message))
      else handler.resolve(value.result)
    }
  })
  await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', reject, { once: true }) })
  await call('Runtime.enable')
  await call('Page.enable')
  await call('Emulation.setDeviceMetricsOverride', { width: 1280, height: 960, deviceScaleFactor: 1, mobile: false })
  await call('Page.navigate', { url: `http://127.0.0.1:${port}/board/native?agent=engineer` })
  await until(() => evaluate('document.querySelectorAll("[data-session]").length === 2'), 'two independent same-identity rows')
  const rendered = await evaluate(`({ text: document.querySelector('[data-testid="native-fleet"]').innerText,
    links: [...document.querySelectorAll('[data-testid="native-fleet"] a')].map(a => a.getAttribute('href')),
    hosts: document.querySelectorAll('[data-host]').length, injected: window.injected === true })`)
  for (const text of ['Retrying', 'Needs tool reconciliation', 'Stale heartbeat', 'Runtime unavailable', 'Draining',
    'awaiting-model', '42', '5m ago', revision.slice(0, 12)]) assert.ok(rendered.text.includes(text), `missing ${text}`)
  assert.equal(rendered.hosts, 4)
  assert.equal(rendered.injected, false)
  assert.ok(rendered.links.includes('/board/agents/engineer'))
  assert.ok(rendered.links.includes('/board/tasks/1'))
  assert.ok(paths.includes('/board/api/fleet/dispatches?limit=1000&offset=0'))
  await evaluate('document.querySelector("input[type=checkbox]").click()')
  await until(() => evaluate('document.querySelectorAll("[data-session]").length === 1'), 'attention filter')
  assert.match(await evaluate('document.querySelector("[data-session]").innerText'), /Needs tool reconciliation/)
  unavailable = true
  await evaluate('[...document.querySelectorAll("button")].find(b => b.textContent === "Refresh now").click()')
  await until(() => evaluate('document.querySelector("[role=alert]")?.textContent.includes("last observation")'), 'honest stale/error state')
  assert.equal(await evaluate('document.querySelectorAll("[data-session]").length'), 1)
  unavailable = false
  await until(() => evaluate('!document.querySelector("[role=alert]") && !!document.querySelector("[data-session]")'), 'automatic API recovery')
  await call('Emulation.setDeviceMetricsOverride', { width: 420, height: 900, deviceScaleFactor: 1, mobile: true })
  await sleep(200)
  assert.ok(await evaluate('document.documentElement.scrollWidth <= window.innerWidth + 2'), 'mobile horizontal overflow')
  if (process.env.NATIVE_UI_SCREENSHOT) {
    const image = await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false })
    await mkdir(dirname(process.env.NATIVE_UI_SCREENSHOT), { recursive: true })
    await writeFile(process.env.NATIVE_UI_SCREENSHOT, Buffer.from(image.data, 'base64'))
  }
  assert.deepEqual(exceptions, [])
  console.log(JSON.stringify({ ok: true, cases: ['actual-built-ui', 'proxy-subpath-links', 'same-identity-sessions',
    'retry-and-unknown-states', 'real-progress-vs-observation', 'stale-host', 'text-escaping',
    'filter', 'outage-last-good-data', 'automatic-recovery', 'mobile-layout'], api_requests: paths.filter(p => p.includes('/api/fleet/')).length }))
} finally {
  socket?.close()
  for (const { timer } of pending.values()) clearTimeout(timer)
  child.kill('SIGTERM')
  server.closeAllConnections()
  await new Promise((resolve) => server.close(resolve))
  await rm(profile, { recursive: true, force: true })
}
