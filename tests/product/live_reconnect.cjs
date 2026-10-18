'use strict';

// Coordination test for the actual browser script, using only Node builtins.
// It does not qualify browser HTML parsing, networking or a controller restart.
// Fetch replies and timer advancement are explicit; no browser or service runs.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { TextDecoder, TextEncoder } = require('node:util');

const sourcePath = path.resolve(process.argv[2] || path.join(__dirname, '../../src/product/live.js'));
const source = fs.readFileSync(sourcePath, 'utf8');
const sourceHash = createHash('sha256').update(source).digest('hex');
const settle = () => new Promise(resolve => setImmediate(resolve));
const route = '/agents/reconnect-agent/settings';
const oldHtml = '<title>Settings global-1</title><main data-live-refresh="true" data-current-revision="global-1"><input value="server-global-1"></main>';
const newHtml = '<title>Settings global-2</title><main data-live-refresh="true" data-current-revision="global-2"><input value="server-global-2"></main>';

function response(value) {
  const bytes = new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value));
  let read = false;
  return {
    ok: true,
    status: 200,
    body: { getReader: () => ({
      async read() {
        if (read) return { done: true };
        read = true;
        return { done: false, value: bytes };
      },
      async cancel() {},
    }) },
  };
}

async function scenario(kind) {
  const frozen = kind === 'frozen';
  const status = { textContent: '' };
  const documentEvents = new Map(), windowEvents = new Map(), timers = new Map();
  const requests = [];
  let nextTimer = 0, inFlight = null, main;
  const refresh = { hidden: true, addEventListener() {} };
  const empty = { hidden: false };
  const feed = {
    children: [],
    prepend(node) { this.children.unshift(node); },
    replaceChildren() { this.children = []; },
    get lastElementChild() {
      return { remove: () => this.children.pop() };
    },
  };
  function makeMain(revision, liveRefresh = 'true') {
    return {
      dataset: { liveRefresh, currentRevision: revision },
      inputValue: `server-${revision}`,
      querySelector(selector) {
        assert.equal(selector, 'script,iframe,object,embed');
        return null;
      },
      replaceWith(next) { assert.equal(main, this); main = next; },
    };
  }
  main = makeMain(frozen ? 'unresolved-original-operation' : 'global-1', frozen ? 'false' : 'true');
  const originalFrozenMain = main;
  let stateNotifications = 0;
  const document = {
    hidden: false,
    title: 'Settings global-1',
    activeElement: { closest: () => null },
    body: { dataset: { runtimeLimits: JSON.stringify({
      cursor_max: '9223372036854775807',
      product: {
        response_bytes: 65536, page_items: 4,
        live: {
          request_timeout_ms: 10000, catchup_pages_per_batch: 2,
          retained_activity_items: 4, max_active_fetches: 2,
          retry_initial_ms: 11, retry_multiplier: 2, retry_max_ms: 44,
          retry_limit: null,
        },
      },
    }) } },
    getElementById(id) {
      const nodes = { 'live-status': status, 'activity-items': feed, 'live-refresh': refresh, 'activity-empty': empty };
      assert(Object.hasOwn(nodes, id), `unexpected DOM identity: ${id}`);
      return nodes[id];
    },
    querySelector(selector) { assert.equal(selector, 'main'); return main; },
    addEventListener(name, callback) { documentEvents.set(name, callback); },
    dispatchEvent(event) { assert.equal(event.type, "borg:state-changed"); stateNotifications++; return true; },
    createElement(tag) {
      assert(['li', 'a', 'small'].includes(tag));
      return { tag, children: [], append(...nodes) { this.children.push(...nodes); } };
    },
    importNode(node, deep) { assert.equal(deep, true); return makeMain(node.dataset.currentRevision, node.dataset.liveRefresh); },
  };
  // Exact known fixtures only: this stub is not an HTML parser or DOM framework.
  class DOMParser {
    parseFromString(html, type) {
      assert.equal(type, 'text/html');
      assert([oldHtml, newHtml].includes(html), 'unexpected markup fixture');
      const revision = html === oldHtml ? 'global-1' : 'global-2';
      return {
        title: `Settings ${revision}`,
        querySelector(selector) { assert.equal(selector, 'main'); return makeMain(revision); },
      };
    }
  }
  function fetch(url, options) {
    assert.equal(inFlight, null, 'the script must keep this scenario sequential');
    assert.equal(options.method, 'GET', 'reconnect must never replay a mutation');
    assert.equal(options.credentials, 'same-origin');
    assert.equal(options.redirect, 'error');
    requests.push(url);
    return new Promise((resolve, reject) => {
      const finish = (callback, value) => {
        options.signal.removeEventListener('abort', aborted);
        inFlight = null;
        callback(value);
      };
      const aborted = () => finish(reject, new Error('aborted'));
      inFlight = {
        url,
        reply: value => finish(resolve, response(value)),
        fail: () => finish(reject, new TypeError('observed transport disconnection')),
      };
      options.signal.addEventListener('abort', aborted, { once: true });
    });
  }
  const context = vm.createContext({
    document,
    window: { addEventListener(name, callback) { windowEvents.set(name, callback); } },
    location: { pathname: route, search: '' },
    DOMParser, fetch, Event, AbortController, TextDecoder, Uint8Array,
    setTimeout(callback, delay) { const id = ++nextTimer; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
  });
  async function reply(url, value) {
    assert(inFlight, `expected pending fetch ${url}`);
    assert.equal(inFlight.url, url);
    inFlight.reply(value);
    await settle();
  }
  const quiet = { revision: '41', changed: false, reset: false };
  try {
    vm.runInContext(source, context, { filename: sourcePath, timeout: 1000 });
    await reply('/ui/changes?after=0', { ...quiet, changed: true });
    await reply('/ui/activity', {
      items: [{ sequence: '7', task_id: 'task-retained', task_revision: '1', kind: 'task_submitted' }],
      cursor: '7', has_more: false,
    });
    if (!frozen) await reply(route, oldHtml);
    assert.equal(inFlight.url, '/ui/changes?after=41');
    assert.equal(status.textContent, 'Live');
    assert.equal(feed.children.length, 1);
    const retainedActivity = feed.children[0];
    const before = main;
    if (kind === 'dirty') {
      main.inputValue = 'unsent operator value';
      documentEvents.get('input')({ target: { closest(selector) { assert.equal(selector, 'main form'); return {}; } } });
    }

    inFlight.fail();
    await settle();
    assert.equal(status.textContent, 'Updates disconnected · reconnecting');
    assert.equal(inFlight, null);
    assert.equal(timers.size, 1, 'only the reconnect timer may remain');
    const [timerId, retry] = [...timers.entries()][0];
    assert.equal(retry.delay, 11);
    timers.delete(timerId);
    retry.callback();
    await settle();

    // A restarted controller can expose new current metadata while retaining
    // this exact durable revision. No new activity or fake mutation wakes it.
    await reply('/ui/changes?after=41', quiet);
    await reply('/ui/activity?after=7', { items: [], cursor: '7', has_more: false });
    if (kind === 'clean') {
      await reply(route, newHtml);
      assert.notEqual(main, before);
      assert.equal(main.dataset.currentRevision, 'global-2');
      assert.equal(main.inputValue, 'server-global-2');
      assert.equal(document.title, 'Settings global-2');
      assert.equal(refresh.hidden, true);
      assert.equal(status.textContent, 'Live');
    } else if (kind === 'dirty') {
      assert.equal(main, before);
      assert.equal(main.inputValue, 'unsent operator value');
      assert.equal(main.dataset.currentRevision, 'global-1');
      assert.equal(refresh.hidden, false);
      assert.equal(status.textContent, 'Live activity · page update ready');
    } else {
      assert.equal(main, originalFrozenMain);
      assert.equal(main.dataset.liveRefresh, 'false');
      assert.equal(main.dataset.currentRevision, 'unresolved-original-operation');
      assert.equal(refresh.hidden, true);
      assert.equal(status.textContent, 'Live');
    }
    assert.equal(feed.children.length, 1);
    assert.equal(feed.children[0], retainedActivity, 'reconnect must retain activity without reset or duplication');
    assert.deepEqual(requests.filter(url => url.startsWith('/ui/activity')), ['/ui/activity', '/ui/activity?after=7']);
    assert.equal(requests.filter(url => url === route).length, kind === 'clean' ? 2 : frozen ? 0 : 1);

    assert.equal(stateNotifications, 2, "initial catch-up and reconnect each notify retained views");
    // Equal-revision polling after recovery must not become periodic full loads.
    const fetches = requests.length;
    await reply('/ui/changes?after=41', quiet);
    assert.equal(requests.length, fetches + 1);
    assert.equal(stateNotifications, 2, "unchanged polling must not repeatedly notify views");
    assert.equal(inFlight.url, '/ui/changes?after=41');
    assert.equal(main.dataset.currentRevision, kind === 'clean' ? 'global-2' : frozen ? 'unresolved-original-operation' : 'global-1');
  } finally {
    const pagehide = windowEvents.get('pagehide');
    if (pagehide) pagehide();
    await settle();
    assert.equal(inFlight, null, 'pagehide must release the pending fetch');
    assert.equal(timers.size, 0, 'pagehide must clear timers');
  }
}

(async () => {
  const results = [];
  for (const kind of ['clean', 'dirty', 'frozen']) {
    try {
      await scenario(kind);
      results.push({ case: kind, status: 'passed' });
    } catch (error) {
      results.push({ case: kind, status: 'failed', error: error.stack || String(error) });
    }
  }
  console.log(JSON.stringify({ source: sourcePath, source_sha256: sourceHash, results }, null, 2));
  if (results.some(result => result.status !== 'passed')) process.exitCode = 1;
})().catch(error => { console.error(error); process.exitCode = 1; });
