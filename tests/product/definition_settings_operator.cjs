// Controlled DOM/transport only; Rust tests exercise the actual local HTTP API.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { TextEncoder, TextDecoder } = require('node:util');
const source = fs.readFileSync(process.argv[2] || path.resolve(__dirname, '../../src/product/definition_settings.js'), 'utf8');
function response(value) {
  const raw = typeof value === 'string' ? value : JSON.stringify(value);
  let read = false;
  return {ok: true, status: 200, body: {getReader() {return {
    async read() {if (read) return {done: true}; read = true; return {done: false, value: new TextEncoder().encode(raw)};},
    async cancel() {}
  };}}};
}
function reply(overrides = {}) {
  return {request_id: 'request-1', status: 'ok', commit: {operation_id: 'original-1', controller_revision: 17},
    observed_controller_revision: 17, data: {kind: 'definition_update', value: {}}, ...overrides};
}
function fixture(send, edits = {}, options = {}) {
  const cfg = {wire: 25, body_bytes: 262144, identity_bytes: 256, request_timeout_ms: 30000,
    maximum_value: '18446744073709551615', agent_id: 'subject', fields: ['coding.frame_bytes', 'collaboration.journal_bytes'], ...options};
  const values = {operation_id: 'original-1', expected_revision: 'old-head', expected_global_revision: 'old-global',
    expected_policy_revision: 'old-policy', 'coding.frame_bytes': '9007199254740993', 'collaboration.journal_bytes': '', ...edits};
  const inputs = Object.entries(values).map(([name, value]) => ({name, value, disabled: false}));
  const note = {textContent: ''}, button = {disabled: true}, lookup = {hidden: true, href: ''};
  const handlers = {}, calls = [], navigation = [], urls = [], timers = new Map(); let timer = 0;
  const form = {dataset: {settings: JSON.stringify(cfg)},
    querySelector(selector) {return {'.settings-status': note, 'button[type="submit"]': button, '.settings-lookup': lookup}[selector];},
    querySelectorAll(selector) {if (selector === 'input[name]') return inputs; if (selector === 'input,button') return [...inputs, button]; throw new Error(selector);},
    addEventListener(name, callback) {handlers[name] = callback;}};
  const context = {TextEncoder, TextDecoder, Uint8Array, URL, AbortController, console,
    document: {getElementById(id) {assert.equal(id, 'definition-settings-form'); return form;}},
    crypto: {randomUUID() {return 'request-1';}},
    location: {href: 'http://local/agents/subject/settings?task_id=kept', assign(url) {navigation.push(url);}},
    history: {replaceState(_state, _title, url) {urls.push(url.href);}},
    setTimeout(callback) {timers.set(++timer, callback); return timer;}, clearTimeout(id) {timers.delete(id);},
    fetch: async (url, init) => {calls.push({url, init}); return send(url, init);}};
  vm.runInNewContext(source, context, {filename: 'definition_settings.js'});
  return {form, inputs, note, button, lookup, calls, navigation, urls, timers,
    submit: () => handlers.submit({preventDefault() {}})};
}
(async () => {
  let count = 0;
  async function check(name, run) {await run(); console.log('PASS ' + name); count++;}
  await check('exact large numeric lexeme, blank omission and original CAS', async () => {
    const f = fixture(async () => response(reply())); await f.submit();
    assert.equal(f.calls.length, 1); const {url, init} = f.calls[0];
    assert.equal(url, '/api/operations'); assert.equal(init.credentials, 'same-origin'); assert.equal(init.redirect, 'error');
    assert.match(init.body, /"coding.frame_bytes":9007199254740993/); assert.doesNotMatch(init.body, /"collaboration.journal_bytes"/);
    const raw = JSON.parse(init.body); assert.equal(raw.operation, 'update_definition');
    assert.equal(raw.input.update.expected_revision, 'old-head'); assert.equal(raw.input.update.expected_global_revision, 'old-global');
    assert.equal(raw.input.update.expected_policy_revision, 'old-policy');
    assert.equal(f.form.dataset.originalRequest, init.body); assert.deepEqual(f.navigation, ['/operations/original-1']);
    assert.match(f.urls[0], /task_id=kept&original_operation=original-1/); assert(f.inputs.every(input => input.disabled));
    await f.submit(); assert.equal(f.calls.length, 1);
  });
  await check('invalid numeric/unknown/overflow inputs never dispatch', async () => {
    for (const edits of [{'coding.frame_bytes': '-1'}, {'coding.frame_bytes': '1.5'}, {'coding.frame_bytes': '18446744073709551616'}, {host_admin: 'true'}]) {
      const f = fixture(async () => {throw new Error('unexpected send');}, edits); await f.submit();
      assert.equal(f.calls.length, 0); assert.match(f.note.textContent, /no request sent/); assert.equal(f.form.dataset.originalRequest, undefined);
    }
  });
  await check('second submit cannot overlap original pending request', async () => {
    let resolve; const pending = new Promise(r => {resolve = r;}); const f = fixture(() => pending);
    const first = f.submit(); await f.submit(); assert.equal(f.calls.length, 1);
    f.inputs[0].value = 'replacement'; resolve(response(reply())); await first;
    assert.match(f.form.dataset.originalRequest, /"operation_id":"original-1"/); assert.equal(f.calls.length, 1);
  });
  await check('lost reply keeps original identity and cannot resend', async () => {
    const f = fixture(async () => {throw new Error('lost original response');}); await f.submit(); await f.submit();
    assert.equal(f.calls.length, 1); assert.equal(f.lookup.href, '/operations/original-1'); assert.equal(f.lookup.hidden, false);
    assert.equal(f.navigation.length, 0); assert.match(f.note.textContent, /no second request/); assert.match(f.urls[0], /original_operation=original-1/);
  });
  await check('malformed mismatched pending and uncommitted responses stay unresolved', async () => {
    for (const value of ['{bad', reply({request_id: 'wrong'}), reply({commit: null}), reply({status: 'pending'}),
      reply({data: {kind: 'task'}}), reply({commit: {operation_id: 'wrong', controller_revision: 17}}),
      reply({commit: {operation_id: 'original-1', controller_revision: 9007199254740992}})]) {
      const f = fixture(async () => response(value)); await f.submit();
      assert.match(f.note.textContent, /Outcome unresolved/); assert.equal(f.navigation.length, 0);
      await f.submit(); assert.equal(f.calls.length, 1);
    }
  });
  await check('committed refusal reads original receipt without replacement', async () => {
    const f = fixture(async () => response(reply({status: 'refused', code: 'configuration_conflict', message: 'stale'})));
    await f.submit(); assert.deepEqual(f.navigation, ['/operations/original-1']); assert.equal(f.calls.length, 1);
  });
  await check('timeout abort closes request and never retries', async () => {
    const f = fixture((_url, init) => new Promise((_resolve, reject) => init.signal.addEventListener('abort', () => reject(new Error('timeout')))));
    const first = f.submit(); assert.equal(f.timers.size, 1); [...f.timers.values()][0](); await first;
    assert.match(f.note.textContent, /Outcome unresolved/); assert.equal(f.timers.size, 0);
    await f.submit(); assert.equal(f.calls.length, 1);
  });
  await check('request and reply bounds retain no-send or unknown distinction', async () => {
    const small = fixture(async () => {throw new Error('unexpected');}, {}, {body_bytes: 10}); await small.submit();
    assert.equal(small.calls.length, 0); assert.match(small.note.textContent, /no request sent/);
    const large = fixture(async () => response('x'.repeat(4096)), {}, {body_bytes: 1024}); await large.submit();
    assert.equal(large.calls.length, 1); assert.match(large.note.textContent, /Reply exceeds transport.body_bytes/); assert.equal(large.navigation.length, 0);
  });
  console.log(JSON.stringify({scope: 'controlled DOM and transport; no live browser, host or provider', passed: count}));
})().catch(error => {console.error(error); process.exitCode = 1;});
