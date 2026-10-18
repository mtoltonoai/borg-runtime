// The same operator API and original-request discipline as governance controls.
// The ordinary Browser POST remains a non-operator invocation.
(() => {
  "use strict";
  const form = document.getElementById("definition-settings-form");
  if (!form) return;
  const cfg = JSON.parse(form.dataset.settings), note = form.querySelector(".settings-status");
  const button = form.querySelector('button[type="submit"]'), lookup = form.querySelector(".settings-lookup");
  const bytes = value => new TextEncoder().encode(value).byteLength;
  const identity = value => typeof value === "string" && value.length > 0 && bytes(value) <= cfg.identity_bytes && !/[\u0000-\u001f\u007f]/.test(value);
  const message = error => error instanceof Error ? error.message : "Original operation unavailable";
  function request() {
    const names = ["operation_id", "expected_revision", "expected_global_revision", "expected_policy_revision"];
    const fields = new Map();
    for (const input of form.querySelectorAll("input[name]")) {
      if (fields.has(input.name) || (!names.includes(input.name) && !cfg.fields.includes(input.name))) throw new Error("Unknown or duplicate settings field");
      fields.set(input.name, input.value);
    }
    for (const name of names) if (!identity(fields.get(name))) throw new Error("Invalid original settings identity");
    if (!identity(cfg.agent_id)) throw new Error("Invalid selected agent identity");
    const changes = [];
    for (const name of cfg.fields) {
      const value = fields.get(name);
      if (value === undefined || value === "") continue;
      if (!/^[0-9]+$/.test(value)) throw new Error(name + " must be an integer");
      const exact = BigInt(value);
      if (exact > BigInt(cfg.maximum_value)) throw new Error(name + " exceeds the supported integer representation");
      changes.push(JSON.stringify(name) + ":" + exact.toString());
    }
    const requestId = crypto.randomUUID();
    if (!identity(requestId)) throw new Error("Invalid request identity");
    const original = Object.fromEntries(names.map(name => [name, fields.get(name)]));
    const update = JSON.stringify({agent_id: cfg.agent_id, expected_revision: original.expected_revision,
      expected_global_revision: original.expected_global_revision, expected_policy_revision: original.expected_policy_revision});
    // Numeric lexemes stay exact above 2^53; they never pass through Number.
    const raw = '{"protocol_revision":' + cfg.wire + ',"request_id":' + JSON.stringify(requestId) +
      ',"operation":"update_definition","input":{"operation_id":' + JSON.stringify(original.operation_id) +
      ',"update":' + update.slice(0, -1) + ',"changes":{' + changes.join(",") + '}}}}';
    if (bytes(raw) > cfg.body_bytes) throw new Error("Request exceeds transport.body_bytes");
    return {raw, id: original.operation_id, requestId};
  }
  async function send(raw, requestId) {
    const abort = new AbortController(), timer = setTimeout(() => abort.abort(), cfg.request_timeout_ms);
    try {
      const response = await fetch("/api/operations", {method: "POST", credentials: "same-origin", cache: "no-store", redirect: "error",
        headers: {"Content-Type": "application/json"}, body: raw, signal: abort.signal});
      if (!response.ok) throw new Error("Operator API unavailable or refused (" + response.status + ")");
      const reader = response.body.getReader(), chunks = []; let size = 0;
      for (;;) {
        const item = await reader.read(); if (item.done) break;
        size += item.value.byteLength;
        if (size > cfg.body_bytes) { await reader.cancel(); throw new Error("Reply exceeds transport.body_bytes"); }
        chunks.push(item.value);
      }
      const joined = new Uint8Array(size); let offset = 0;
      for (const chunk of chunks) { joined.set(chunk, offset); offset += chunk.length; }
      const reply = JSON.parse(new TextDecoder("utf-8", {fatal: true}).decode(joined));
      if (reply.request_id !== requestId) throw new Error("Mismatched reply identity");
      return reply;
    } finally { clearTimeout(timer); }
  }
  button.disabled = false;
  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (form.dataset.originalRequest) { note.textContent = "This original request was already sent or may have been sent. Read its receipt; no second request was issued."; return; }
    let original;
    try { original = request(); } catch (error) { note.textContent = message(error) + "; no request sent"; return; }
    form.dataset.originalRequest = original.raw;
    for (const input of form.querySelectorAll("input,button")) input.disabled = true;
    lookup.hidden = false; lookup.href = "/operations/" + encodeURIComponent(original.id);
    note.textContent = "Outcome unresolved. Preserve original operation " + original.id + "; no automatic retry.";
    try {
      const address = new URL(location.href); address.searchParams.set("original_operation", original.id);
      history.replaceState(null, "", address);
      const reply = await send(original.raw, original.requestId), commit = reply.commit;
      if (!commit || commit.operation_id !== original.id || !Number.isSafeInteger(commit.controller_revision) || commit.controller_revision <= 0 ||
          !Number.isSafeInteger(reply.observed_controller_revision) || reply.observed_controller_revision < commit.controller_revision) throw new Error("No matching committed receipt");
      if (!(reply.status === "refused" || (reply.status === "ok" && reply.data?.kind === "definition_update"))) throw new Error("No terminal settings outcome");
      // The existing read-only page validates the original record and renders its
      // exact success/refusal. No updated form or replacement operation is sent.
      location.assign(lookup.href);
    } catch (error) { note.textContent = "Outcome unresolved: " + message(error) + ". Read original operation " + original.id + "; no automatic retry."; }
  });
})();
