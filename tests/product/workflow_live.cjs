'use strict';

// Coordination checks for the supplied browser script using Node builtins only.
// Fetch, timers and the DOM are explicit fixtures. These checks do not qualify
// real browser parsing, networking, a controller restart, or provider execution.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { TextDecoder, TextEncoder } = require('node:util');

assert(process.argv.length <= 3, 'expected at most one live.js source argument');
const sourcePath = path.resolve(process.argv[2] || path.join(__dirname, '../../src/product/live.js'));
const sourceBytes = fs.readFileSync(sourcePath);
const source = sourceBytes.toString('utf8');
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const sourceHash = hash(sourceBytes);
const settle = () => new Promise(resolve => setImmediate(resolve));
const route = '/agents/workflow-live-fixture/settings';
const taskEndpoint = '/ui/activity';
const workflowEndpoint = '/ui/workflow-activity';
const R1 = '9007199254740993', R2 = '9007199254740994', R3 = '9007199254740995';
const database = 'workflow-live-database';
const documentId = 'document /<em>';
const principal = () => ({ kind: 'trusted_host_operator' });
const text = value => ({ text: value, truncated: false });
const version = number => ({ document_id: documentId, version: number, content_id: `content-${number}` });
const eventId = (revision, ordinal = '1') => ({ database_id: database, product_revision: revision, ordinal });

// The opaque cursor preserves canonical integer tokens. Never pass its u64
// product_revision through a JavaScript Number or stringify a rounded Number.
function cursor(id) {
  return `{"database_id":${JSON.stringify(id.database_id)},"product_revision":${id.product_revision},"ordinal":${id.ordinal}}`;
}
const taskUrl = after => taskEndpoint + (after === null ? '' : '?after=' + encodeURIComponent(after));
const workflowUrl = after => workflowEndpoint + (after === null ? '' : '?cursor=' + encodeURIComponent(cursor(after)));
const changesUrl = after => '/ui/changes?after=' + encodeURIComponent(after);
const notice = (revision, changed = true, reset = false) => ({ revision, changed, reset });
const taskItem = sequence => ({ sequence, task_id: 'task-fixture', task_revision: sequence, kind: 'task_progress' });
const taskPage = (items, through, hasMore = false) => ({ items, cursor: through, has_more: hasMore });
const workflowPage = (items, through, after, hasMore = false) => ({
  items, through, cursor: after === null ? null : cursor(after), has_more: hasMore,
});

// Full typed activity fixtures follow collaboration/workflow.rs, with every
// projected numeric field represented as a decimal string.
function activity(id, domain = 'document', change = 'created') {
  let subject, detail;
  if (domain === 'document') {
    subject = { kind: 'document', document_id: documentId };
    detail = change === 'version_added'
      ? { kind: 'document', change: { kind: change, previous: version('1'), version: version('2'), title: text('Updated title'), body: text('Updated body') } }
      : { kind: 'document', change: { kind: 'created', version: version('1'), responsible_owner: principal(), title: text('<img src=x onerror=alert(1)>'), body: text('Document body') } };
  } else if (domain === 'question') {
    subject = { kind: 'question', question_id: 'question-fixture' };
    detail = { kind: 'question', change: { kind: 'posed', linked_record: { kind: 'document', document_id: documentId }, recipient: principal(), prompt: text('Which option?'), recommended_option_id: 'option-one' } };
  } else if (domain === 'review') {
    subject = { kind: 'review', review_id: 'review-fixture' };
    detail = { kind: 'review', change: { kind: 'requested', target: { kind: 'document_version', version: version('2') }, reviewer: principal(), summary: text('Review this version') } };
  } else {
    assert.equal(domain, 'wait');
    subject = { kind: 'task', task_id: 'task-fixture' };
    detail = { kind: 'wait', change: { kind: 'added', wait: { task_id: 'task-fixture', wait_id: 'wait-fixture' }, resolver: { kind: 'review', review_id: 'review-fixture' } } };
  }
  return {
    id, subject, subject_revision: '1',
    actor: { principal: principal(), provenance: { kind: 'trusted_host', path: 'browser' } },
    detail, truncated: false,
  };
}

function response(value, counters) {
  const bytes = new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value));
  let read = false;
  return { ok: true, status: 200, body: { getReader: () => ({
    async read() {
      if (read) return { done: true };
      read = true;
      return { done: false, value: bytes };
    },
    async cancel() { counters.reader_cancels++; },
  }) } };
}

function harness(options = {}) {
  const workflowEnabled = options.workflow !== false;
  const status = { textContent: '' };
  const documentEvents = new Map(), windowEvents = new Map(), refreshEvents = new Map(), timers = new Map();
  const requests = [], opaqueCursors = new Set(), markup = new Map();
  const counters = { reader_cancels: 0 };
  let nextTimer = 0, inFlight = null, main;
  const refresh = { hidden: true, addEventListener(name, callback) { refreshEvents.set(name, callback); } };
  const activityEmpty = { hidden: false }, workflowEmpty = { hidden: false }, workflowSection = { hidden: true };
  function feed() {
    return {
      children: [],
      prepend(node) { this.children.unshift(node); },
      replaceChildren() { this.children = []; },
      get lastElementChild() { return { remove: () => this.children.pop() }; },
    };
  }
  const taskFeed = feed(), workflowFeed = feed();
  function makeMain(revision) {
    return {
      dataset: { liveRefresh: 'true', currentRevision: revision },
      inputValue: `server-${revision}`,
      querySelector(selector) { assert.equal(selector, 'script,iframe,object,embed'); return null; },
      replaceWith(next) { assert.equal(main, this); main = next; },
    };
  }
  main = makeMain(R1);
  function html(revision) {
    const value = `<title>Settings ${revision}</title><main data-live-refresh="true" data-current-revision="${revision}"><input value="server-${revision}"></main>`;
    markup.set(value, revision);
    return value;
  }
  const metadata = {
    cursor_max: '9223372036854775807',
    product: {
      response_bytes: 65536, page_items: options.pageItems || 1,
      live: {
        request_timeout_ms: 10000, catchup_pages_per_batch: 1,
        retained_activity_items: options.retained || 2, max_active_fetches: 1,
        retry_initial_ms: 11, retry_multiplier: 2, retry_max_ms: 44, retry_limit: 1,
      },
    },
  };
  if (workflowEnabled) {
    metadata.workflow = { page_items: options.workflowPageItems || options.pageItems || 1, page_bytes: options.workflowPageBytes || 16384 };
    metadata.workflow_cursor_bytes = options.workflowCursorBytes || 2048;
    metadata.workflow_identity_bytes = 256;
  }
  const nodes = { 'live-status': status, 'activity-items': taskFeed, 'live-refresh': refresh, 'activity-empty': activityEmpty };
  if (workflowEnabled) Object.assign(nodes, {
    'workflow-activity': workflowSection, 'workflow-items': workflowFeed, 'workflow-empty': workflowEmpty,
  });
  const document = {
    hidden: false, title: `Settings ${R1}`, activeElement: { closest: () => null },
    body: { dataset: { runtimeLimits: JSON.stringify(metadata) } },
    getElementById(id) {
      assert(Object.hasOwn(nodes, id), `unexpected DOM lookup${workflowEnabled ? '' : ' without workflow metadata'}: ${id}`);
      return nodes[id];
    },
    querySelector(selector) { assert.equal(selector, 'main'); return main; },
    addEventListener(name, callback) { documentEvents.set(name, callback); },
    dispatchEvent(event) { assert.equal(event.type, "borg:state-changed"); return true; },
    createElement(tag) {
      assert(['li', 'a', 'small'].includes(tag), `unexpected activity element: ${tag}`);
      return {
        tag, children: [], append(...children) { this.children.push(...children); },
        set innerHTML(_) { assert.fail('activity data must never use innerHTML'); },
        insertAdjacentHTML() { assert.fail('activity data must never insert HTML'); },
      };
    },
    importNode(node, deep) { assert.equal(deep, true); return makeMain(node.dataset.currentRevision); },
  };
  class DOMParser {
    parseFromString(value, type) {
      assert.equal(type, 'text/html');
      assert(markup.has(value), 'DOMParser accepts exact registered fixtures only');
      const revision = markup.get(value);
      return { title: `Settings ${revision}`, querySelector(selector) { assert.equal(selector, 'main'); return makeMain(revision); } };
    }
  }
  function fetch(url, request) {
    assert.equal(inFlight, null, 'both activity streams must keep the one-request capacity');
    assert.equal(request.method, 'GET', 'catch-up must never replay a mutation');
    assert.equal(request.credentials, 'same-origin');
    assert.equal(request.cache, 'no-store');
    assert.equal(request.redirect, 'error');
    requests.push(url);
    return new Promise((resolve, reject) => {
      let finished = false;
      const finish = (callback, value) => {
        assert.equal(finished, false, 'fixture request settled twice');
        finished = true;
        request.signal.removeEventListener('abort', aborted);
        inFlight = null;
        callback(value);
      };
      const aborted = () => finish(reject, new Error('aborted'));
      inFlight = {
        url,
        reply: value => finish(resolve, response(value, counters)),
        fail: () => finish(reject, new TypeError('workflow fixture transport disconnection')),
      };
      request.signal.addEventListener('abort', aborted, { once: true });
    });
  }
  const context = vm.createContext({
    document, window: { addEventListener(name, callback) { windowEvents.set(name, callback); } },
    location: { pathname: route, search: '' }, DOMParser, fetch, Event, AbortController, TextDecoder, TextEncoder, Uint8Array,
    JSON: {
      parse(value, ...args) {
        assert(!opaqueCursors.has(value), 'workflow cursor must stay opaque; JSON.parse would round its u64 tokens');
        return JSON.parse(value, ...args);
      },
      stringify: JSON.stringify,
    },
    setTimeout(callback, delay) { const id = ++nextTimer; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
  });
  const api = {
    requests, counters, metadata, taskFeed, workflowFeed, workflowSection, workflowEmpty, status, refresh, document, html,
    get main() { return main; },
    get pendingUrl() { return inFlight && inFlight.url; },
    start() { vm.runInContext(source, context, { filename: sourcePath, timeout: 1000 }); },
    async reply(url, value) {
      assert(inFlight, `expected pending fetch ${url}`);
      assert.equal(inFlight.url, url);
      if (url.startsWith(workflowEndpoint) && value.cursor !== null) opaqueCursors.add(value.cursor);
      inFlight.reply(value);
      await settle();
    },
    async fail() {
      assert(inFlight, 'expected a request to fail');
      inFlight.fail();
      await settle();
      assert.equal(inFlight, null);
    },
    async retry() {
      assert.equal(timers.size, 1, 'only the retry timer may remain');
      const [id, timer] = [...timers.entries()][0];
      assert.equal(timer.delay, 11);
      timers.delete(id);
      timer.callback();
      await settle();
    },
    dirty(value = 'unsent operator value') {
      main.inputValue = value;
      documentEvents.get('input')({ target: { closest(selector) { assert.equal(selector, 'main form'); return {}; } } });
    },
    notAcknowledged(revision) {
      assert.equal(requests.includes(changesUrl(revision)), false, `revision ${revision} acknowledged before both streams completed`);
    },
    async cleanup() {
      const pagehide = windowEvents.get('pagehide');
      if (pagehide) pagehide();
      await settle();
      assert.equal(inFlight, null, 'pagehide must abort the outstanding request');
      assert.equal(timers.size, 0, 'pagehide must clear every request and retry timer');
    },
  };
  return api;
}

function markers(feed, workflow = true) {
  return feed.children.map(node => {
    const small = node.children.find(child => child.tag === 'small');
    assert(small, 'activity row must have a text identity marker');
    const match = (workflow ? / · event ([0-9]+:[0-9]+)$/ : / · event ([0-9]+)$/).exec(small.textContent);
    assert(match, `invalid activity marker: ${small.textContent}`);
    return match[1];
  });
}
function link(node) {
  const value = node.children.find(child => child.tag === 'a');
  assert(value, 'activity row must have a typed link');
  return value;
}
async function boot(h, workflow = true, controllerRevision = R1) {
  h.start();
  await h.reply(changesUrl('0'), notice(controllerRevision));
  await h.reply(taskUrl(null), taskPage([taskItem('7')], '7'));
  if (workflow) {
    const first = eventId(R1);
    await h.reply(workflowUrl(null), workflowPage([activity(first)], first, first));
    assert.equal(h.workflowSection.hidden, false);
    assert.equal(h.workflowEmpty.hidden, true);
    assert.deepEqual(markers(h.workflowFeed), [`${R1}:1`]);
    assert.equal(link(h.workflowFeed.children[0]).href, '/documents/' + encodeURIComponent(documentId));
    assert.equal(link(h.workflowFeed.children[0]).textContent, 'document created');
  }
  await h.reply(route, h.html(controllerRevision));
  assert.equal(h.pendingUrl, changesUrl(controllerRevision));
}

async function rejectWorkflowPage(h, page) {
  await boot(h);
  const retained = h.workflowFeed.children[0], original = h.main;
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8'));
  await h.reply(workflowUrl(eventId(R1)), page);
  assert.equal(h.pendingUrl, null, 'rejected workflow page must stop the batch before refreshing or acknowledging');
  h.notAcknowledged(R2);
  assert.equal(h.status.textContent, 'Updates disconnected · reconnecting');
  assert.deepEqual(markers(h.workflowFeed), [`${R1}:1`]);
  assert.equal(h.workflowFeed.children[0], retained, 'rejected page must leave the retained row untouched');
  assert.equal(h.main, original);
  assert.deepEqual(markers(h.taskFeed, false), ['8', '7']);
}

const cases = [];
function test(name, options, body) { cases.push({ name, options, body }); }

test('independent_large_cursors_and_unequal_multipage_streams', {}, async h => {
  // Controller revisions, task sequences and workflow positions are separate.
  const C1 = '41', C2 = '42', C3 = '43';
  await boot(h, true, C1);
  const first = eventId(R1), second = eventId(R2);
  await h.reply(changesUrl(C1), notice(C2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8', true));
  // Task has_more must not short-circuit the workflow request in this batch.
  await h.reply(workflowUrl(first), workflowPage([activity(second, 'document', 'version_added')], second, second));
  assert.equal(link(h.workflowFeed.children[0]).textContent, 'document version added');
  const retainedSecond = h.workflowFeed.children[0];
  await h.reply(route, h.html(C2));
  h.notAcknowledged(C2);
  await h.reply(taskUrl('8'), taskPage([taskItem('9')], '9'));
  await h.reply(workflowUrl(second), workflowPage([], second, second));
  await h.reply(route, h.html(C2));
  assert.equal(h.pendingUrl, changesUrl(C2));
  assert.equal(h.workflowFeed.children[0], retainedSecond);

  const third = eventId(R3, '1'), fourth = eventId(R3, '2'), fifth = eventId(R3, '3');
  await h.reply(changesUrl(C2), notice(C3));
  await h.reply(taskUrl('9'), taskPage([], '9'));
  // Snapshot head is ahead of the current page. Resume at cursor, not through.
  await h.reply(workflowUrl(second), workflowPage([activity(third, 'question')], fifth, third, true));
  assert.equal(link(h.workflowFeed.children[0]).href, '/questions/question-fixture');
  assert.equal(link(h.workflowFeed.children[0]).textContent, 'question posed');
  await h.reply(route, h.html(C3));
  h.notAcknowledged(C3);
  await h.reply(taskUrl('9'), taskPage([], '9'));
  await h.reply(workflowUrl(third), workflowPage([activity(fourth, 'review')], fifth, fourth, true));
  assert.equal(link(h.workflowFeed.children[0]).href, '/reviews/review-fixture');
  assert.equal(link(h.workflowFeed.children[0]).textContent, 'review requested');
  const retainedFourth = h.workflowFeed.children[0];
  await h.reply(route, h.html(C3));
  h.notAcknowledged(C3);
  await h.reply(taskUrl('9'), taskPage([], '9'));
  await h.reply(workflowUrl(fourth), workflowPage([activity(fifth, 'wait')], fifth, fifth));
  assert.equal(link(h.workflowFeed.children[0]).href, '/tasks/task-fixture/history');
  assert.equal(link(h.workflowFeed.children[0]).textContent, 'wait added');
  h.notAcknowledged(C3);
  await h.reply(route, h.html(C3));
  assert.equal(h.pendingUrl, changesUrl(C3));
  assert.deepEqual(markers(h.taskFeed, false), ['9', '8']);
  assert.deepEqual(markers(h.workflowFeed), [`${R3}:3`, `${R3}:2`]);
  assert.equal(h.workflowFeed.children[1], retainedFourth, 'retention must not recreate accepted rows');
  assert.deepEqual(h.requests.filter(url => url.startsWith(workflowEndpoint)), [
    workflowUrl(null), workflowUrl(first), workflowUrl(second), workflowUrl(second), workflowUrl(third), workflowUrl(fourth),
  ]);
  return { controller_revision: C3, task_cursor: '9', workflow_position: `${R3}:3`, opaque_cursor: cursor(fifth), retained_per_stream: 2 };
});

test('mixed_task_changes_and_comments_preserve_exact_subjects_and_advance', { pageItems: 6, retained: 7 }, async h => {
  await boot(h);
  const taskSubject = { kind: 'task', task_id: 'task-fixture' };
  const rows = ['owner_changed', 'closed'].map((kind, index) => {
    const row = activity(eventId(R2, String(index + 1)));
    row.subject = taskSubject;
    row.detail = { kind: 'task', change: kind === 'owner_changed'
      ? { kind, previous: principal(), current: { kind: 'agent', agent_id: 'agent-next' } }
      : { kind, resolution: { task_id: 'task-fixture', terminal_task_revision: '2', status: 'cancelled', accepted_result: null, summary: 'Closed without an accepted result' } } };
    return row;
  });
  for (const [index, kind] of ['task', 'question', 'review', 'document'].entries()) {
    const row = activity(eventId(R2, String(index + 3)));
    row.subject = { kind, [kind + '_id']: kind + '-fixture' };
    row.detail = { kind: 'document', change: { kind: 'comment_added', comment_id: 'comment-' + index, body: text('<script>literal comment</script>') } };
    rows.push(row);
  }
  const through = rows.at(-1).id;
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([], '7'));
  await h.reply(workflowUrl(eventId(R1)), workflowPage(rows, through, through));
  const fresh = h.workflowFeed.children.slice(0, 6);
  assert.deepEqual(fresh.map(node => link(node).href), [
    '/documents/document-fixture', '/reviews/review-fixture', '/questions/question-fixture',
    '/tasks/task-fixture/history', '/tasks/task-fixture/history', '/tasks/task-fixture/history',
  ]);
  assert.deepEqual(fresh.map(node => link(node).textContent), [
    'document comment added', 'document comment added', 'document comment added',
    'document comment added', 'task closed', 'task owner changed',
  ]);
  h.notAcknowledged(R2);
  await h.reply(route, h.html(R2));
  assert.equal(h.pendingUrl, changesUrl(R2));
  assert.deepEqual(markers(h.workflowFeed).slice(0, 6), ['6','5','4','3','2','1'].map(ordinal => R2 + ':' + ordinal));
  return { accepted_exact_subject_combinations: 6, workflow_cursor: cursor(through), controller_revision: R2 };
});

for (const [name, domain, change, subject] of [
  ['task_change_on_document_is_rejected', 'task', 'owner_changed', { kind: 'document', document_id: 'document-fixture' }],
  ['noncomment_document_change_on_task_is_rejected', 'document', 'created', { kind: 'task', task_id: 'task-fixture' }],
]) {
  test(name, {}, async h => {
    const row = activity(eventId(R2));
    row.subject = subject;
    row.detail = { kind: domain, change: { kind: change } };
    await rejectWorkflowPage(h, workflowPage([row], row.id, row.id));
    return { rejected_domain: domain, rejected_change: change, subject_kind: subject.kind };
  });
}

test('workflow_failure_retains_accepted_task_page_and_retries_each_cursor', {}, async h => {
  await boot(h);
  const first = eventId(R1), second = eventId(R2);
  const retainedWorkflow = h.workflowFeed.children[0];
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8'));
  const retainedTask = h.taskFeed.children[0];
  assert.equal(h.pendingUrl, workflowUrl(first));
  await h.fail();
  h.notAcknowledged(R2);
  assert.equal(h.status.textContent, 'Updates disconnected · reconnecting');
  assert.equal(h.workflowFeed.children[0], retainedWorkflow);
  await h.retry();
  await h.reply(taskUrl('8'), taskPage([], '8'));
  await h.reply(workflowUrl(first), workflowPage([activity(second)], second, second));
  await h.reply(route, h.html(R2));
  assert.equal(h.pendingUrl, changesUrl(R2));
  assert.deepEqual(markers(h.taskFeed, false), ['8', '7']);
  assert.equal(h.taskFeed.children[0], retainedTask);
  assert.equal(h.workflowFeed.children[1], retainedWorkflow);
  assert.deepEqual(h.requests.filter(url => url.startsWith(workflowEndpoint)), [workflowUrl(null), workflowUrl(first), workflowUrl(first)]);
  return { accepted_task_cursor_after_retry: '8', duplicate_rows: 0 };
});

test('dirty_form_and_same_revision_reconnect_preserve_both_feeds', {}, async h => {
  await boot(h);
  const first = eventId(R1), second = eventId(R2), original = h.main;
  h.dirty();
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8'));
  await h.reply(workflowUrl(first), workflowPage([activity(second)], second, second));
  assert.equal(h.pendingUrl, changesUrl(R2), 'dirty-form deferral may acknowledge fully caught-up activity');
  assert.equal(h.main, original);
  assert.equal(h.main.inputValue, 'unsent operator value');
  assert.equal(h.refresh.hidden, false);
  const retainedTask = h.taskFeed.children[0], retainedWorkflow = h.workflowFeed.children[0];
  await h.fail();
  await h.retry();
  await h.reply(changesUrl(R2), notice(R2, false));
  await h.reply(taskUrl('8'), taskPage([], '8'));
  await h.reply(workflowUrl(second), workflowPage([], second, second));
  assert.equal(h.pendingUrl, changesUrl(R2));
  assert.equal(h.main, original);
  assert.equal(h.main.inputValue, 'unsent operator value');
  assert.equal(h.status.textContent, 'Live activity · page update ready');
  assert.equal(h.taskFeed.children[0], retainedTask);
  assert.equal(h.workflowFeed.children[0], retainedWorkflow);
  const count = h.requests.length;
  await h.reply(changesUrl(R2), notice(R2, false));
  assert.equal(h.requests.length, count + 1, 'quiet recovered polling must not repeatedly reload either stream');
  assert.equal(h.requests.filter(url => url === route).length, 1, 'dirty updates must not fetch replacement HTML');
  return { unsent_input_preserved: true, same_revision_reconnect: true };
});

test('edit_started_during_html_request_prevents_replacement', {}, async h => {
  await boot(h);
  const original = h.main, first = eventId(R1), second = eventId(R2);
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8'));
  await h.reply(workflowUrl(first), workflowPage([activity(second)], second, second));
  assert.equal(h.pendingUrl, route);
  h.dirty('typed while HTML was in flight');
  await h.reply(route, h.html(R2));
  assert.equal(h.main, original);
  assert.equal(h.main.inputValue, 'typed while HTML was in flight');
  assert.equal(h.refresh.hidden, false);
  assert.equal(h.pendingUrl, changesUrl(R2));
  assert.deepEqual(markers(h.workflowFeed), [`${R2}:1`, `${R1}:1`]);
  return { in_flight_edit_preserved: true };
});

test('clean_same_revision_reconnect_refreshes_once_without_cursor_reset', {}, async h => {
  await boot(h);
  const first = eventId(R1), original = h.main, retainedWorkflow = h.workflowFeed.children[0];
  await h.fail();
  await h.retry();
  await h.reply(changesUrl(R1), notice(R1, false));
  await h.reply(taskUrl('7'), taskPage([], '7'));
  await h.reply(workflowUrl(first), workflowPage([], first, first));
  await h.reply(route, h.html('reconnected-metadata'));
  assert.notEqual(h.main, original);
  assert.equal(h.main.dataset.currentRevision, 'reconnected-metadata');
  assert.equal(h.workflowFeed.children[0], retainedWorkflow);
  assert.equal(h.pendingUrl, changesUrl(R1));
  const count = h.requests.length;
  await h.reply(changesUrl(R1), notice(R1, false));
  assert.equal(h.requests.length, count + 1);
  assert.equal(h.requests.filter(url => url === route).length, 2);
  return { metadata_refreshes_after_reconnect: 1, retained_workflow_cursor: cursor(first) };
});

test('malformed_later_workflow_row_is_atomic_and_does_not_advance_cursor', { pageItems: 2, retained: 3 }, async h => {
  await boot(h);
  const first = eventId(R1), second = eventId(R2, '1'), third = eventId(R2, '2');
  const retainedWorkflow = h.workflowFeed.children[0];
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('7'), taskPage([taskItem('8')], '8'));
  const malformed = activity(third, 'question');
  malformed.subject_revision = 'not-a-decimal';
  await h.reply(workflowUrl(first), workflowPage([activity(second), malformed], third, third));
  assert.equal(h.pendingUrl, null, 'invalid page must enter bounded retry, not acknowledge the revision');
  h.notAcknowledged(R2);
  assert.deepEqual(markers(h.workflowFeed), [`${R1}:1`], 'a valid prefix of an invalid page must not render');
  assert.equal(h.workflowFeed.children[0], retainedWorkflow);
  assert.deepEqual(markers(h.taskFeed, false), ['8', '7'], 'a rejected workflow page must not roll back the other stream');
  await h.retry();
  await h.reply(taskUrl('8'), taskPage([], '8'));
  await h.reply(workflowUrl(first), workflowPage([activity(second), activity(third, 'question')], third, third));
  await h.reply(route, h.html(R2));
  assert.equal(h.pendingUrl, changesUrl(R2));
  assert.deepEqual(markers(h.workflowFeed), [`${R2}:2`, `${R2}:1`, `${R1}:1`]);
  assert.equal(h.workflowFeed.children[2], retainedWorkflow);
  return { rejected_page_rendered_rows: 0, retry_cursor: cursor(first) };
});

for (const bound of [
  { name: 'workflow_item_bound_tighter_than_product', pageItems: 2, workflowPageItems: 1 },
  { name: 'product_item_bound_tighter_than_workflow', pageItems: 1, workflowPageItems: 2 },
]) {
  test(bound.name, bound, async h => {
    const second = eventId(R2, '1'), third = eventId(R2, '2');
    await rejectWorkflowPage(h, workflowPage([activity(second), activity(third, 'question')], third, third));
    return { product_page_items: bound.pageItems, workflow_page_items: bound.workflowPageItems, rejected_items: 2 };
  });
}

test('workflow_byte_bound_is_tighter_than_product_response_bound', { workflowPageBytes: 4096 }, async h => {
  const second = eventId(R2), item = activity(second);
  item.detail.change.body = text('é'.repeat(h.metadata.workflow.page_bytes));
  const page = workflowPage([item], second, second);
  const bytes = new TextEncoder().encode(JSON.stringify(page)).byteLength;
  assert(bytes > h.metadata.workflow.page_bytes);
  assert(bytes < h.metadata.product.response_bytes, 'fixture must isolate the tighter workflow byte limit');
  await rejectWorkflowPage(h, page);
  assert.equal(h.counters.reader_cancels, 1, 'oversized streamed workflow body must cancel its reader');
  return { response_bytes: bytes, workflow_byte_limit: h.metadata.workflow.page_bytes, reader_cancels: 1 };
});

test('opaque_cursor_one_byte_above_metadata_bound_is_rejected', {
  workflowCursorBytes: Buffer.byteLength(cursor(eventId(R1)), 'utf8'),
}, async h => {
  const second = eventId(R2, '10');
  const page = workflowPage([activity(second)], second, second);
  assert.equal(Buffer.byteLength(page.cursor, 'utf8'), h.metadata.workflow_cursor_bytes + 1);
  await rejectWorkflowPage(h, page);
  return { cursor_bytes: Buffer.byteLength(page.cursor, 'utf8'), cursor_byte_limit: h.metadata.workflow_cursor_bytes };
});

test('workflow_tail_cannot_skip_ahead_to_an_unscanned_snapshot_head', {}, async h => {
  const second = eventId(R2, '1'), unseen = eventId(R2, '2');
  await rejectWorkflowPage(h, workflowPage([activity(second)], unseen, unseen));
  return { returned_position: `${R2}:1`, unscanned_head: `${R2}:2`, premature_tail_rejected: true };
});

test('reset_clears_both_cursors_and_pagehide_aborts_workflow_fetch', {}, async h => {
  await boot(h);
  await h.reply(changesUrl(R1), notice(R2, false, true));
  assert.equal(h.taskFeed.children.length, 0);
  assert.equal(h.workflowFeed.children.length, 0);
  await h.reply(taskUrl(null), taskPage([taskItem('9')], '9'));
  assert.equal(h.pendingUrl, workflowUrl(null), 'reset must not send the previous opaque cursor');
  h.notAcknowledged(R2);
  await h.cleanup();
  return { reset_streams: 2, workflow_request_aborted: true };
});

test('empty_workflow_history_keeps_null_cursor', {}, async h => {
  h.start();
  await h.reply(changesUrl('0'), notice(R1));
  await h.reply(taskUrl(null), taskPage([], '0'));
  await h.reply(workflowUrl(null), workflowPage([], null, null));
  await h.reply(route, h.html(R1));
  assert.equal(h.workflowEmpty.hidden, false);
  assert.equal(h.workflowFeed.children.length, 0);
  await h.reply(changesUrl(R1), notice(R2));
  await h.reply(taskUrl('0'), taskPage([], '0'));
  assert.equal(h.pendingUrl, workflowUrl(null), 'empty history must not invent a cursor');
  return { empty_through: null, empty_cursor: null };
});

test('workflow_metadata_absent_preserves_existing_dom_and_requests', { workflow: false }, async h => {
  await boot(h, false);
  const count = h.requests.length;
  await h.reply(changesUrl(R1), notice(R1, false));
  assert.equal(h.requests.length, count + 1);
  assert.equal(h.requests.some(url => url.startsWith(workflowEndpoint)), false);
  assert.equal(h.status.textContent, 'Live');
  return { workflow_requests: 0, optional_workflow_dom_lookups: 0 };
});

(async () => {
  const report = {
    schema: 'borg-workflow-live-vm/1', source: sourcePath, source_sha256: sourceHash,
    scope: 'Explicit Node VM DOM, fetch and timer fixtures only; no real browser, network, controller restart or provider.',
    results: [],
  };
  for (const entry of cases) {
    const h = harness(entry.options), result = { case: entry.name, status: 'running' };
    report.results.push(result);
    try {
      result.facts = await entry.body(h);
      result.status = 'passed';
    } catch (error) {
      result.status = 'failed';
      result.error = error.stack || String(error);
      result.pending_url = h.pendingUrl;
      result.live_status = h.status.textContent;
      result.requests = h.requests;
    } finally {
      try {
        await h.cleanup();
        result.pagehide_cleanup = { pending_fetch: false, timers: 0 };
      } catch (error) {
        result.status = 'failed';
        result.cleanup_error = error.stack || String(error);
      }
    }
  }
  report.original_source_unchanged = hash(fs.readFileSync(sourcePath)) === sourceHash;
  console.log(JSON.stringify(report, null, 2));
  if (!report.original_source_unchanged || report.results.some(result => result.status !== 'passed')) process.exitCode = 1;
})().catch(error => { console.error(error); process.exitCode = 1; });
