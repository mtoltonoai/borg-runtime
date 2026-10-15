(() => {
  "use strict";
  const status = document.getElementById("live-status"), feed = document.getElementById("activity-items"), refresh = document.getElementById("live-refresh");
  if (!status || !feed || !refresh) return;
  let metadata;
  try { metadata = JSON.parse(document.body.dataset.runtimeLimits); }
  catch { status.textContent = "Live updates unavailable: limits metadata is missing"; return; }
  const policy = metadata.product, live = policy?.live;
  const positive = value => Number.isSafeInteger(value) && value > 0;
  if (!live || ![policy.response_bytes,policy.page_items,live.request_timeout_ms,live.catchup_pages_per_batch,live.retained_activity_items,live.max_active_fetches,live.retry_initial_ms,live.retry_multiplier,live.retry_max_ms].every(positive)
      || live.retry_initial_ms > live.retry_max_ms || (live.retry_limit !== null && (!Number.isSafeInteger(live.retry_limit) || live.retry_limit < 0))
      || typeof metadata.cursor_max !== "string" || !/^[1-9][0-9]*$/.test(metadata.cursor_max)) {
    status.textContent = "Live updates unavailable: invalid limits metadata"; return;
  }
  const active = new Set();
  const abortActive = () => { for (const request of active) request.abort(); };
  let retryDelay = live.retry_initial_ms;
  let revision = "0", cursor = null, dirty = false, pending = false, running = false, stopped = false, failures = 0, timer = null, forceRefresh = false, wake = false, editVersion = 0, pendingRevision = null;
  const decimal = value => typeof value === "string" && value.length <= metadata.cursor_max.length && /^(0|[1-9][0-9]*)$/.test(value) && BigInt(value) <= BigInt(metadata.cursor_max);
  function state(text) { status.textContent = text; }
  async function bounded(url, html = false) {
    if(active.size >= live.max_active_fetches) throw new Error("Live request capacity reached");
    const request = new AbortController(); active.add(request);
    const deadline = setTimeout(() => request.abort(), live.request_timeout_ms);
    try {
      const response = await fetch(url, {method:"GET", credentials:"same-origin", cache:"no-store", redirect:"error", signal:request.signal});
      if (!response.ok) throw new Error(`Request refused (${response.status})`);
      const reader = response.body.getReader(); const chunks = []; let bytes = 0;
      for (;;) { const item = await reader.read(); if (item.done) break; bytes += item.value.byteLength; if (bytes > policy.response_bytes) { await reader.cancel(); throw new Error("Response exceeds bound"); } chunks.push(item.value); }
      const joined = new Uint8Array(bytes); let offset=0; for(const item of chunks){joined.set(item,offset);offset+=item.length;}
      const text = new TextDecoder("utf-8",{fatal:true}).decode(joined);
      return html ? text : JSON.parse(text);
    } finally { clearTimeout(deadline); active.delete(request); }
  }
  async function activity() {
    let more = false;
    for (let page=0; page<live.catchup_pages_per_batch; page++) {
      const value = await bounded("/ui/activity" + (cursor === null ? "" : "?after=" + encodeURIComponent(cursor)));
      if (!decimal(value.cursor) || !Array.isArray(value.items) || value.items.length > policy.page_items || typeof value.has_more !== "boolean") throw new Error("Invalid activity page");
      let prior = cursor;
      for (const item of value.items) {
        if (!decimal(item.sequence) || typeof item.task_id !== "string" || typeof item.kind !== "string" || typeof item.task_revision !== "string" || (prior !== null && BigInt(item.sequence)<=BigInt(prior))) throw new Error("Invalid activity order");
        prior = item.sequence;
      }
      if (prior !== null && BigInt(value.cursor)<BigInt(prior)) throw new Error("Activity cursor moved backwards");
      if(value.has_more && value.cursor === cursor) throw new Error("Activity page made no progress");
      for (const item of value.items) {
        const li=document.createElement("li"), a=document.createElement("a"), small=document.createElement("small");
        a.href="/tasks/"+encodeURIComponent(item.task_id)+"/history"; a.textContent=item.kind.replaceAll("_"," ");
        small.textContent=item.task_id+" · revision "+item.task_revision+" · event "+item.sequence;
        li.append(a,small); feed.prepend(li);
      }
      while(feed.children.length>live.retained_activity_items) feed.lastElementChild.remove();
      cursor=value.cursor; more=value.has_more;
      document.getElementById("activity-empty").hidden=feed.children.length!==0;
      if(!more) break;
    }
    return more;
  }
  function editing() { return dirty || document.activeElement?.closest("main input,main textarea,main select") !== null; }
  async function updatePage(force=false) {
    const old=document.querySelector("main");
    if(!old || old.dataset.liveRefresh!=="true") return;
    if(!force && editing()){pending=true;refresh.hidden=false;return;}
    const capturedEdits=editVersion, capturedFocus=document.activeElement;
    const html=await bounded(location.pathname+location.search,true);
    const parsed=new DOMParser().parseFromString(html,"text/html"), next=parsed.querySelector("main");
    if(!next || next.dataset.liveRefresh!=="true") throw new Error("Unexpected live view");
    // Same-origin server rendering escapes record data. Do not import scripts,
    // document headers or any executable content from the response.
    if(next.querySelector("script,iframe,object,embed")) throw new Error("Unexpected active view content");
    const newlyFocused=document.activeElement!==capturedFocus && document.activeElement?.closest("main input,main textarea,main select");
    if(document.querySelector("main")!==old || editVersion!==capturedEdits || newlyFocused || (!force && editing())){pending=true;refresh.hidden=false;return;}
    old.replaceWith(document.importNode(next,true)); document.title=parsed.title;
    dirty=false;pending=false;refresh.hidden=true;
  }
  async function run() {
    if(running || stopped || document.hidden) return; running=true;
    try {
      if(forceRefresh){forceRefresh=false;await updatePage(true);}
      while(!stopped && !document.hidden) {
        if(pendingRevision !== null) {
          const more=await activity();
          await updatePage();
          if(!more){revision=pendingRevision;pendingRevision=null;}
          failures=0;retryDelay=live.retry_initial_ms;state(pending?"Live activity · page update ready":"Live");
          continue;
        }
        const value=await bounded("/ui/changes?after="+encodeURIComponent(revision));
        if(!decimal(value.revision) || typeof value.reset!=="boolean" || typeof value.changed!=="boolean") throw new Error("Invalid change revision");
        if(value.reset){cursor=null;feed.replaceChildren();}
        // A notification is acknowledged only after durable catch-up and view
        // refresh (or intentional dirty-form deferral). Keep work across failures.
        if(value.changed || value.reset || value.revision!==revision || cursor===null) pendingRevision=value.revision;
        else {failures=0;retryDelay=live.retry_initial_ms;state(pending?"Live activity · page update ready":"Live");}
      }
    } catch(error) {
      if(!stopped && !document.hidden && !forceRefresh && !wake){
        failures++;
        if(live.retry_limit !== null && failures > live.retry_limit){state("Updates disconnected · retry limit reached");refresh.hidden=false;}
        else {state("Updates disconnected · reconnecting");timer=setTimeout(()=>{timer=null;run();},retryDelay);retryDelay=Math.min(live.retry_max_ms,retryDelay*live.retry_multiplier);}
      }
    } finally { running=false; if(!stopped && !document.hidden && (forceRefresh || wake)){wake=false;clearTimeout(timer);timer=setTimeout(()=>{timer=null;run();},0);} }
  }
  document.addEventListener("input",event=>{if(event.target.closest("main form")){dirty=true;editVersion++;}});
  refresh.addEventListener("click",()=>{failures=0;retryDelay=live.retry_initial_ms;forceRefresh=true;abortActive();if(!running)run();});
  document.addEventListener("visibilitychange",()=>{if(document.hidden){abortActive();clearTimeout(timer);timer=null;state("Updates paused while hidden");}else{state("Reconnecting");wake=true;if(!running){wake=false;run();}}});
  window.addEventListener("pagehide",()=>{stopped=true;abortActive();clearTimeout(timer);});
  window.addEventListener("pageshow",event=>{if(event.persisted){stopped=false;wake=true;state("Reconnecting");if(!running){wake=false;run();}}});
  run();
})();
