(() => {
  "use strict";
  const root = document.getElementById("governance-root");
  if (!root) return;
  const cfg = JSON.parse(root.dataset.governance), status = document.getElementById("governance-status");
  const bytes = value => new TextEncoder().encode(value).byteLength;
  const text = (tag, value, parent) => { const node = document.createElement(tag); node.textContent = value; parent.append(node); return node; };
  const safeNumber = value => Number.isSafeInteger(value) && value >= 0;
  const identity = value => typeof value === "string" && value.length > 0 && bytes(value) <= cfg.identity_bytes;
  const exactInteger = value => {
    if (!safeNumber(value)) throw new Error("Numeric record cannot be displayed exactly; use the original canonical record");
    return String(value);
  };
  function terms(parent, entries) {
    const list = document.createElement("dl"); parent.append(list);
    for (const [label,value] of entries) { text("dt",label,list); text("dd",String(value),list); }
  }
  function section(id, title) { const node = document.getElementById(id); node.replaceChildren(); text("h2",title,node); return node; }
  function link(parent,label,url) { const a=text("a",label,parent); a.href=url; return a; }
  function message(error) { return error instanceof Error ? error.message : "Recorded view unavailable"; }
  async function send(request) {
    const raw = typeof request === "string" ? request : JSON.stringify(request);
    if (bytes(raw) > cfg.body_bytes) throw new Error("Request exceeds transport.body_bytes; no request sent");
    const abort = new AbortController(), timer = setTimeout(() => abort.abort(),cfg.request_timeout_ms);
    try {
      const response = await fetch("/api/operations",{method:"POST",credentials:"same-origin",cache:"no-store",redirect:"error",headers:{"Content-Type":"application/json"},body:raw,signal:abort.signal});
      if (!response.ok) throw new Error(`Operator API unavailable or refused (${response.status}); reconcile the original operation`);
      const reader=response.body.getReader(), chunks=[]; let size=0;
      for (;;) { const item=await reader.read(); if(item.done) break; size+=item.value.byteLength; if(size>cfg.body_bytes){await reader.cancel();throw new Error("Reply exceeds transport.body_bytes; reconcile original operation");} chunks.push(item.value); }
      const joined=new Uint8Array(size);let offset=0;for(const chunk of chunks){joined.set(chunk,offset);offset+=chunk.length;}
      const reply=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(joined));
      const expected=JSON.parse(raw).request_id;
      if(reply.request_id!==expected) throw new Error("Mismatched reply identity; reconcile original operation");
      return reply;
    } finally { clearTimeout(timer); }
  }
  function request(operation,input) {
    const request_id=crypto.randomUUID();
    if(!identity(request_id)) throw new Error("Request identity exceeds accepted identity bound");
    return {protocol_revision:cfg.wire,request_id,operation,input};
  }
  async function query(query) {
    const r=await send(request("query_governance",{query}));
    if(r.status!=="ok") throw new Error(`${r.code || r.status || "unknown"}: ${r.message || "Governance read unavailable"}`);
    if(r.data?.kind!=="governance") throw new Error("Unexpected governance result");
    return r.data.value;
  }
  function actor(parent, value) {
    if(!value?.principal || !value?.provenance) throw new Error("Missing recorded actor provenance");
    terms(parent,[["Recorded principal",value.principal.kind === "trusted_host_operator" ? "Trusted host operator (not an authenticated person)" : `Agent ${value.principal.agent_id || "unknown"}`],
      ["Actual invocation provenance",JSON.stringify(value.provenance)]]);
  }
  function decision(record, expectedId, point, taskId) {
    if(record?.id?.operation_id!==expectedId || record.id.point!==point) throw new Error("Decision identity differs from the selected original");
    if(taskId && (record.detail?.kind!=="completion" || record.detail.decision?.subject?.task_id!==taskId)) throw new Error("Decision does not belong to the selected task");
    const node=section("governance-decision","Recorded decision");
    terms(node,[["Original operation",record.id.operation_id],["Decision point",record.id.point],["Decision mode",record.mode],
      ["Rule",record.rule.id],["Exact rule revision",record.rule.revision],["Governance configuration revision",record.configuration.governance_revision],
      ["Global configuration revision",record.configuration.global_revision],["Accepted task configuration revision",record.configuration.accepted_global_revision ?? "Not task-bound"],
      ["Definition update policy revision",record.configuration.definition_update_policy_revision ?? "Not applicable"],
      ["Actual recorded outcome",JSON.stringify(record.actual_outcome)],["Original controller revision",exactInteger(record.controller_revision)]]);
    actor(node,record.actor);
    if(record.detail.kind==="completion") {
      const detail=record.detail.decision;
      terms(node,[["Task",detail.subject.task_id],["Attempt",detail.subject.attempt_id],["Remaining-work disposition",JSON.stringify(detail.disposition)],
        ["Blocked continuation count",exactInteger(detail.consecutive_count)]]);
      text("h3","Recorded remaining-work reasons",node);
      if(!Array.isArray(detail.reasons)) throw new Error("Invalid recorded reasons");
      if(!detail.reasons.length) text("p","No remaining-work reason was recorded.",node);
      else { const list=document.createElement("ul");node.append(list);for(const reason of detail.reasons) text("li",JSON.stringify(reason),list); }
    } else text("pre",JSON.stringify(record.detail,null,2),node);
    link(node,"Read original operation", "/operations/"+encodeURIComponent(expectedId));
  }
  async function readDecision(id,point,taskId) {
    const result=await query({kind:"decision",decision:{operation_id:id,point}});
    if(result.kind!=="decision") throw new Error("Unexpected decision projection");
    decision(result.value,id,point,taskId);
  }
  let refreshing=false, refreshAgain=false;
  async function refresh() {
    if(refreshing){refreshAgain=true;return;}
    refreshing=true;
    document.getElementById("governance-refresh").disabled=true;
    status.textContent="Reading recorded governance";
    const failures=[];
    try {
      const selected=await query({kind:"state"});
      if(selected.kind!=="state") throw new Error("Unexpected selection projection");
      const node=section("governance-selection","Selected configuration");
      terms(node,[["Selected governance revision",selected.value.selected_revision]]);
      if(selected.value.temporary_override) {
        const v=selected.value.temporary_override;
        terms(node,[["Original override operation",v.operation_id],["Prior policy",v.prior_revision],["Replacement policy",v.replacement_revision],["Recorded expiry (Unix ms)",exactInteger(v.expires_at_ms)],["Reason",v.reason]]);actor(node,v.actor);
        text("p","This is recorded state. Expiry, admission, and recovery are checked by the controller, not the browser clock.",node);
      }
      const c=await query({kind:"configuration",revision:selected.value.selected_revision});
      if(c.kind!=="configuration" || c.value.revision!==selected.value.selected_revision) throw new Error("Configuration does not match the selected exact revision");
      terms(node,[["Global metadata revision",c.value.global_revision],["Completion rule",c.value.completion.id+" @ "+c.value.completion.revision],["Definition rule",c.value.definition_limits.id+" @ "+c.value.definition_limits.revision]]);
    } catch(e) { failures.push(message(e)); }
    const selectedDecision=cfg.decision_id;
    // Never leave a previously loaded decision looking current after a failed
    // read or after the task no longer retains an assessment.
    text("p", "Decision view has not been loaded.", section("governance-decision","Recorded decision"));
    if(cfg.task_id) {
      try {
        const r=await send(request("get_task",{task_id:cfg.task_id}));
        if(r.status!=="ok" || r.data?.kind!=="task" || r.data.value.id!==cfg.task_id) throw new Error("Task view is unavailable");
        const task=r.data.value,node=section("governance-task","Task execution state");
        terms(node,[["Task",task.id],["Revision",exactInteger(task.revision)],["Status",task.status],["Execution blocker",task.blocked_reason ?? "No current blocker"],
          ["Physical termination",task.attempt ? (task.attempt.stopped ? "Confirmed in canonical task" : "Not confirmed") : "No current attempt"],["Unresolved effects",task.attempt?.uncertain ? "Yes; recovery is still required" : "No uncertainty reported"]]);
        link(node,"Open task", "/tasks/"+encodeURIComponent(task.id));
        const stop=root.querySelector('[data-control="stop"]');
        if(!stop.dataset.originalRequest && !stop.dataset.revisionInitialized && !stop.dataset.revisionEdited) {
          stop.elements.expected_revision.value=String(task.revision);
          stop.dataset.revisionInitialized="true";
        }
      } catch(e) { failures.push(message(e)); }
    }
    if(selectedDecision) try { await readDecision(selectedDecision,cfg.point,cfg.task_id); } catch(e) { failures.push(message(e)); }
    else if(cfg.task_id) {
      try {
        const result=await query({kind:"task_completion",task_id:cfg.task_id});
        if(result.kind!=="task_completion" || result.value?.task_id!==cfg.task_id) throw new Error("Unexpected task completion projection");
        if(result.value.decision===null) text("p","No completion decision is retained for this task.",section("governance-decision","Recorded decision"));
        else {
          const record=result.value.decision;
          if(!identity(record?.id?.operation_id)) throw new Error("Missing original completion decision identity");
          decision(record,record.id.operation_id,"completion",cfg.task_id);
        }
      } catch(e) { failures.push(message(e)); }
    } else text("p","No decision selected.",section("governance-decision","Recorded decision"));
    status.textContent=failures.length ? "Recorded views incomplete: "+failures.join("; ")+". Emergency controls remain separate." : "Recorded state loaded. Refresh reads only; it does not retry controls.";
    refreshing=false;document.getElementById("governance-refresh").disabled=false;
    if(refreshAgain){refreshAgain=false;void refresh();}
  }
  function controlRequest(form) {
    const value=name=>form.elements[name]?.value ?? "", id=value("operation_id"), kind=form.dataset.control;
    if(!identity(id)) throw new Error("Invalid original operation ID");
    let mutation={kind};
    if(kind!=="reconcile_expiry") mutation.reason=value("reason");
    if(kind==="pause" || kind==="release_hold") {
      mutation.scope=value("scope")==="all_tasks" ? {kind:"all_tasks"} : {kind:"task",task_id:value("task_id")};
      if(mutation.scope.kind==="task" && !identity(mutation.scope.task_id)) throw new Error("A task identity is required for this scope");
    }
    if(kind==="stop") {
      if(!identity(value("task_id"))) throw new Error("A task identity is required");
      mutation.task_id=value("task_id");
      const revision=value("expected_revision");
      if(!/^[1-9][0-9]*$/.test(revision) || BigInt(revision)>18446744073709551615n) throw new Error("Exact positive task revision required");
      mutation.expected_revision="EXACT_REVISION";
      // Preserve the u64 lexeme without a lossy JavaScript Number conversion.
      return JSON.stringify(request("mutate_governance",{operation_id:id,mutation})).replace('"expected_revision":"EXACT_REVISION"','"expected_revision":'+revision);
    }
    if(kind==="release_hold" || kind==="reconcile_expiry") {
      if(!identity(value("expected_operation_id"))) throw new Error("Original hold/override identity is required");
      mutation.expected_operation_id=value("expected_operation_id");
    }
    if(!["pause","release_hold","reconcile_expiry"].includes(kind)) throw new Error("Unsupported control");
    return JSON.stringify(request("mutate_governance",{operation_id:id,mutation}));
  }
  for(const form of root.querySelectorAll("form.governance-control")) {
    if(form.elements.expected_revision) form.elements.expected_revision.addEventListener("input",()=>{form.dataset.revisionEdited="true";});
    form.querySelector('button[type="submit"]').disabled=false;
    form.addEventListener("submit",async event=>{
      event.preventDefault();
      const note=form.querySelector(".control-status"),button=form.querySelector('button[type="submit"]');
      if(form.dataset.originalRequest) { note.textContent="This original request was already sent or may have been sent. Read its receipt; no second request was issued.";return; }
      let raw;
      try { raw=controlRequest(form); } catch(e) { note.textContent=message(e)+"; no request sent";return; }
      const original=JSON.parse(raw).input, id=original.operation_id, mutation=original.mutation;
      form.dataset.originalRequest=raw;
      for(const input of form.querySelectorAll("input,select,button")) input.disabled=true;
      const lookup=form.querySelector(".control-lookup");lookup.hidden=false;lookup.href="/operations/"+encodeURIComponent(id);
      const address=new URL(location.href);address.searchParams.set("original_control",id);history.replaceState(null,"",address);
      note.textContent="Outcome unresolved. Preserve original operation "+id+"; no automatic retry.";
      try {
        const r=await send(raw), outcome=r;
        if(!r.commit || r.commit.operation_id!==id || !safeNumber(r.commit.controller_revision) || r.commit.controller_revision===0
          || !safeNumber(r.observed_controller_revision) || r.observed_controller_revision<r.commit.controller_revision) throw new Error("No matching committed receipt");
        if(outcome?.status==="refused") { note.textContent="Recorded refusal: "+outcome.code+": "+outcome.message+". Original operation "+id;return; }
        if(outcome?.status!=="ok" || outcome.data?.kind!=="governance") throw new Error("No terminal governance result");
        const value=outcome.data.value,kind=form.dataset.control;
        if(!((kind==="pause" && value.kind==="hold" && value.value.operation_id===id && JSON.stringify(value.value.scope)===JSON.stringify(mutation.scope) && value.value.reason===mutation.reason && value.value.actor?.principal?.kind==="trusted_host_operator" && value.value.actor?.provenance?.kind==="trusted_host" && value.value.actor.provenance.path==="operator_api")
          ||(kind==="release_hold" && value.kind==="released" && JSON.stringify(value.value)===JSON.stringify(mutation.scope))||(kind==="stop" && value.kind==="stop_requested" && value.value.task_id===mutation.task_id)
          ||(kind==="reconcile_expiry" && value.kind==="state" && value.value.temporary_override===null))) throw new Error("Result does not match original control");
        note.textContent="Control intent recorded. This does not prove work stopped or resumed. Original operation "+id;
        try { await readDecision(id,"emergency_control",null); }
        catch(e) { note.textContent += ". Recorded decision view unavailable: "+message(e); }
      } catch(e) { note.textContent="Outcome unresolved: "+message(e)+". Read original operation "+id+"; no automatic retry."; }
    });
  }
  if(cfg.original_control) {
    const note=text("p","A prior control identity was retained across navigation; read its original receipt before another action: ",root);
    link(note,cfg.original_control,"/operations/"+encodeURIComponent(cfg.original_control));
  }
  document.getElementById("governance-refresh").addEventListener("click",()=>void refresh());
  document.addEventListener("borg:state-changed",()=>void refresh());
  void refresh();
})();
