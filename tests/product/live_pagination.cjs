'use strict';

// One coordination scenario for actual live.js, using the existing Node VM style.
// This does not qualify browser parsing, real networking, or a controller restart.
// --negative-early-ack mutates only an owned temporary source copy and should fail.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { TextDecoder, TextEncoder } = require('node:util');

const sourcePath = path.resolve(process.argv[2] || path.join(__dirname, '../../src/product/live.js'));
const mode = process.argv[3] || 'current';
assert(['current', '--negative-early-ack'].includes(mode), 'unknown test mode');
assert(process.argv.length <= 4, 'unexpected test arguments');
const sourceBytes = fs.readFileSync(sourcePath);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const sourceHash = hash(sourceBytes);
const settle = () => new Promise(resolve => setImmediate(resolve));
const route = '/agents/pagination-agent/settings';
const oldHtml = '<title>Settings 41</title><main data-live-refresh="true" data-current-revision="41"></main>';
const newHtml = '<title>Settings 42</title><main data-live-refresh="true" data-current-revision="42"></main>';
const acknowledgement = 'if(!more){revision=pendingRevision;pendingRevision=null;reconnectPending=false;document.dispatchEvent(new Event("borg:state-changed"));}';
const prematureAcknowledgement = 'if(true){revision=pendingRevision;pendingRevision=null;reconnectPending=false;document.dispatchEvent(new Event("borg:state-changed"));}';

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

async function scenario(source, filename, facts) {
  const status = { textContent: '' };
  const documentEvents = new Map(), windowEvents = new Map(), refreshEvents = new Map(), timers = new Map();
  const requests = [];
  facts.requests = requests;
  let nextTimer = 0, inFlight = null, main;
  const refresh = { hidden: true, addEventListener(name, callback) { refreshEvents.set(name, callback); } };
  const empty = { hidden: false };
  const feed = {
    children: [],
    prepend(node) { this.children.unshift(node); },
    replaceChildren() { this.children = []; },
    get lastElementChild() { return { remove: () => this.children.pop() }; },
  };
  function makeMain(revision) {
    return {
      dataset: { liveRefresh: 'true', currentRevision: revision },
      querySelector(selector) {
        assert.equal(selector, 'script,iframe,object,embed');
        return null;
      },
      replaceWith(next) { assert.equal(main, this); main = next; },
    };
  }
  main = makeMain('41');
  let stateNotifications = 0;
  const document = {
    hidden: false,
    title: 'Settings 41',
    activeElement: { closest: () => null },
    body: { dataset: { runtimeLimits: JSON.stringify({
      cursor_max: '9223372036854775807',
      product: {
        response_bytes: 65536, page_items: 1,
        live: {
          request_timeout_ms: 10000, catchup_pages_per_batch: 1,
          retained_activity_items: 2, max_active_fetches: 1,
          retry_initial_ms: 11, retry_multiplier: 2, retry_max_ms: 44,
          retry_limit: 1,
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
    importNode(node, deep) { assert.equal(deep, true); return makeMain(node.dataset.currentRevision); },
  };
  // Exact fixture mapping only, matching live_reconnect.cjs; not an HTML parser.
  class DOMParser {
    parseFromString(html, type) {
      assert.equal(type, 'text/html');
      assert([oldHtml, newHtml].includes(html), 'unexpected markup fixture');
      const revision = html === oldHtml ? '41' : '42';
      return {
        title: `Settings ${revision}`,
        querySelector(selector) { assert.equal(selector, 'main'); return makeMain(revision); },
      };
    }
  }
  function fetch(url, options) {
    assert.equal(inFlight, null, 'live requests must remain sequential');
    assert.equal(options.method, 'GET', 'catch-up and explicit retry must never repeat a mutation');
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
        fail: () => finish(reject, new TypeError('activity page transport failure')),
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
  const item = sequence => ({ sequence, task_id: 'task-retained', task_revision: sequence, kind: 'task_progress' });
  const events = () => feed.children.map(node => {
    const marker = node.children.find(child => child.tag === 'small').textContent;
    const match = / · event ([0-9]+)$/.exec(marker);
    assert(match, 'activity must retain its durable event identity');
    return match[1];
  });
  const noEarlyAcknowledgement = () => { assert.equal(stateNotifications, 1, "only complete catch-up notifies projection consumers"); return assert.equal(
    requests.includes('/ui/changes?after=42'), false,
    'revision 42 must not be acknowledged before the final activity page',
  ); };
  try {
    vm.runInContext(source, context, { filename, timeout: 1000 });
    await reply('/ui/changes?after=0', { revision: '41', changed: true, reset: false });
    await reply('/ui/activity', { items: [item('7')], cursor: '7', has_more: false });
    await reply(route, oldHtml);
    assert.equal(inFlight.url, '/ui/changes?after=41');
    assert.equal(status.textContent, 'Live');
    assert.deepEqual(events(), ['7']);
    const retainedSeven = feed.children[0];

    await reply('/ui/changes?after=41', { revision: '42', changed: true, reset: false });
    await reply('/ui/activity?after=7', { items: [item('8')], cursor: '8', has_more: true });
    await reply(route, newHtml);
    noEarlyAcknowledgement();
    assert.equal(inFlight.url, '/ui/activity?after=8', 'unfinished catch-up must continue from its last accepted page');
    assert.deepEqual(events(), ['8', '7']);
    const retainedEight = feed.children[0];
    assert.equal(feed.children[1], retainedSeven);

    inFlight.fail();
    await settle();
    noEarlyAcknowledgement();
    assert.equal(status.textContent, 'Updates disconnected · reconnecting');
    assert.equal(inFlight, null);
    assert.equal(timers.size, 1, 'the first failure permits exactly one retry timer');
    const [timerId, retry] = [...timers.entries()][0];
    assert.equal(retry.delay, 11);
    timers.delete(timerId);
    retry.callback();
    await settle();
    assert.equal(inFlight.url, '/ui/activity?after=8', 'automatic retry must preserve the accepted cursor');
    assert.equal(feed.children[0], retainedEight);
    assert.equal(feed.children[1], retainedSeven);

    inFlight.fail();
    await settle();
    noEarlyAcknowledgement();
    assert.equal(status.textContent, 'Updates disconnected · retry limit reached');
    assert.equal(refresh.hidden, false);
    assert.equal(inFlight, null);
    assert.equal(timers.size, 0, 'exhausted retry budget must leave no automatic retry or request timer');
    const stoppedRequests = requests.length;
    await settle();
    assert.equal(requests.length, stoppedRequests, 'exhaustion must not start another fetch');
    assert.deepEqual(events(), ['8', '7']);
    assert.equal(feed.children[0], retainedEight);
    assert.equal(feed.children[1], retainedSeven);

    assert.equal(typeof refreshEvents.get('click'), 'function');
    refreshEvents.get('click')();
    await settle();
    // The existing explicit control refreshes the view before resuming catch-up.
    await reply(route, newHtml);
    noEarlyAcknowledgement();
    assert.equal(inFlight.url, '/ui/activity?after=8', 'explicit retry must resume the unfinished cursor');
    await reply('/ui/activity?after=8', { items: [item('9')], cursor: '9', has_more: false });
    noEarlyAcknowledgement();
    await reply(route, newHtml);
    assert.equal(inFlight.url, '/ui/changes?after=42', 'acknowledge only after the final activity page and view update');
    assert.equal(stateNotifications, 2, "one notification per fully caught-up revision");
    assert.equal(status.textContent, 'Live');
    assert.equal(refresh.hidden, true);
    assert.deepEqual(events(), ['9', '8'], 'newest-first ordering and retained-item bound must both hold');
    assert.equal(feed.children[1], retainedEight, 'accepted page 8 must not be reset, duplicated or recreated');
    assert(!feed.children.includes(retainedSeven), 'only the oldest retained event is removed');
    assert.deepEqual(requests.filter(url => url.startsWith('/ui/activity')), [
      '/ui/activity', '/ui/activity?after=7',
      '/ui/activity?after=8', '/ui/activity?after=8', '/ui/activity?after=8',
    ]);
    assert.deepEqual(requests.filter(url => url.startsWith('/ui/changes')), [
      '/ui/changes?after=0', '/ui/changes?after=41', '/ui/changes?after=42',
    ]);
    facts.final_events = events();
    facts.retry_limit = 1;
    facts.automatic_retries = 1;
    facts.explicit_retries = 1;
    facts.acknowledged_revision = '42';
  } finally {
    const pagehide = windowEvents.get('pagehide');
    if (pagehide) pagehide();
    await settle();
    assert.equal(inFlight, null, 'pagehide must release the pending fetch');
    assert.equal(timers.size, 0, 'pagehide must clear every timer');
    facts.pagehide_cleanup = { pending_fetch: false, timers: 0 };
  }
}

(async () => {
  let temporaryRoot = null;
  const result = { case: 'paginated_catchup_finite_retry_and_explicit_resume', status: 'running' };
  const report = { schema: 'borg-live-pagination-vm/1', mode, source: sourcePath, source_sha256: sourceHash, results: [result] };
  try {
    let executedPath = sourcePath;
    if (mode === '--negative-early-ack') {
      const text = sourceBytes.toString('utf8');
      assert.equal(text.split(acknowledgement).length, 2, 'negative control requires exactly one known acknowledgement guard');
      temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'borg-ui16-early-ack-'));
      executedPath = path.join(temporaryRoot, 'live.js');
      fs.writeFileSync(executedPath, text.replace(acknowledgement, prematureAcknowledgement), { flag: 'wx' });
      report.negative_control = 'Remove only the has_more guard before acknowledging pendingRevision; expected outcome is failed/exit 1 at the premature revision-42 acknowledgement oracle.';
    }
    const executedSource = fs.readFileSync(executedPath);
    report.executed_source = executedPath;
    report.executed_source_sha256 = hash(executedSource);
    await scenario(executedSource.toString('utf8'), executedPath, result);
    result.status = 'passed';
  } catch (error) {
    result.status = 'failed';
    result.error = error.stack || String(error);
  } finally {
    try {
      assert.equal(hash(fs.readFileSync(sourcePath)), sourceHash, 'original source must remain unchanged');
      report.original_source_unchanged = true;
    } catch (error) {
      result.status = 'failed';
      report.source_check_error = error.stack || String(error);
    }
    try {
      if (temporaryRoot) fs.rmSync(temporaryRoot, { recursive: true, force: false });
      report.temporary_source_removed = temporaryRoot === null || !fs.existsSync(temporaryRoot);
      assert(report.temporary_source_removed);
    } catch (error) {
      result.status = 'failed';
      report.cleanup_error = error.stack || String(error);
    }
  }
  console.log(JSON.stringify(report, null, 2));
  if (result.status !== 'passed') process.exitCode = 1;
})().catch(error => { console.error(error); process.exitCode = 1; });
