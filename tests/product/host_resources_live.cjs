'use strict';
// Exercises the actual live.js with the existing Node VM/explicit-fetch style.
// The tiny DOM below recognizes only fixed fixture nodes; it is not a browser.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { TextEncoder, TextDecoder } = require('node:util');
const sourcePath = process.argv[2] || 'src/product/live.js';
const source = fs.readFileSync(sourcePath, 'utf8');
const settle = () => new Promise(resolve => setImmediate(resolve));
(async () => {
  let now = 0, nextTimer = 0, flight = null, maxFlight = 0, section;
  const timers = new Map(), events = new Map(), windowEvents = new Map(), requests = [];
  const status = {textContent:''}, refresh = {hidden:true, addEventListener(){}}, empty={hidden:true};
  const feed={children:[], replaceChildren(){this.children=[];}, prepend(v){this.children.unshift(v);}};
  function observation(sequence, age=0) {
    const ageField={textContent:String(age)}, freshness={textContent:'Fresh'}, notice={textContent:'As of read'};
    const row={dataset:{machineAgeMs:String(age),machineStaleAfterMs:'150',machineFreshness:'fresh'},querySelector(s){return {'[data-machine-age]':ageField,'[data-machine-age-status]':freshness}[s]||null;}};
    return {sequence,row,ageField,freshness,notice,
      querySelectorAll(s){assert.equal(s,'[data-machine-row]');return [row];},
      querySelector(s){if(s==='[data-machine-connection]')return notice;assert.equal(s,'script,iframe,object,embed,form,input,textarea,select');return null;},
      replaceWith(next){assert.equal(section,this);section=next;}};
  }
  section=observation(1);
  const main={dataset:{liveRefresh:'true',machineRefreshMs:'50'},inputValue:'original',querySelector(s){if(s==='#machine-resources')return section;assert.equal(s,'script,iframe,object,embed');return null;},replaceWith(){throw Error('must preserve dirty main');}};
  const document={hidden:false,title:'Machines',activeElement:{closest(){return null;}},
    body:{dataset:{runtimeLimits:JSON.stringify({cursor_max:'9223372036854775807',product:{response_bytes:65536,page_items:4,live:{request_timeout_ms:1000,catchup_pages_per_batch:2,retained_activity_items:4,max_active_fetches:1,retry_initial_ms:11,retry_multiplier:2,retry_max_ms:44,retry_limit:null}}})}},
    getElementById(id){return {'live-status':status,'activity-items':feed,'live-refresh':refresh,'activity-empty':empty}[id]||null;},
    querySelector(s){assert.equal(s,'main');return main;}, addEventListener(k,v){events.set(k,v);},dispatchEvent(){return true;},
    importNode(node,deep){assert.equal(deep,true);return observation(node.sequence,Number(node.row.dataset.machineAgeMs));},
    createElement(){throw Error('no activity expected');}};
  class DOMParser {parseFromString(html,type){assert.equal(type,'text/html');const data=JSON.parse(html);return {title:'Machines',querySelector(s){assert.equal(s,'main');return {querySelector(s){assert.equal(s,'#machine-resources');return observation(data.sequence,data.age);}};}};}}
  function fetch(url,options){
    assert.equal(options.method,'GET');assert.equal(options.credentials,'same-origin');assert.equal(options.redirect,'error');
    assert.equal(flight,null,'no overlapping observation/activity fetch');maxFlight=Math.max(maxFlight,1);requests.push(url);
    return new Promise((resolve,reject)=>{
      const end=(fn,value)=>{options.signal.removeEventListener('abort',abort);flight=null;fn(value);};
      const abort=()=>end(reject,Error('aborted'));
      flight={url,fail(){end(reject,Error('offline'));},reply(value){const data=new TextEncoder().encode(typeof value==='string'?value:JSON.stringify(value));let read=false;
        end(resolve,{ok:true,body:{getReader(){return {async read(){if(read)return {done:true};read=true;return {done:false,value:data};},async cancel(){}};}}});}};
      options.signal.addEventListener('abort',abort,{once:true});
    });
  }
  const context=vm.createContext({document,window:{addEventListener(k,v){windowEvents.set(k,v);}},location:{pathname:'/machines',search:''},DOMParser,fetch,Event,AbortController,TextDecoder,Uint8Array,performance:{now:()=>now},
    setTimeout(callback,delay){const id=++nextTimer;timers.set(id,{callback,delay});return id;},clearTimeout(id){timers.delete(id);}});
  async function reply(url,value){assert.equal(flight?.url,url);flight.reply(value);await settle();}
  async function fire(delay){const entry=[...timers].find(([,t])=>t.delay===delay);assert(entry,`missing ${delay}ms timer`);timers.delete(entry[0]);entry[1].callback();await settle();}
  vm.runInContext(source,context,{filename:sourcePath,timeout:1000});
  // Mark actual unsent operator input before initial activity reconciliation.
  main.inputValue='unsent operator draft';events.get('input')({target:{closest:()=>({})}});
  await reply('/ui/changes?after=0',{revision:'41',changed:true,reset:false});
  await reply('/ui/activity',{items:[],cursor:'0',has_more:false});
  assert.equal(flight.url,'/ui/changes?after=41');
  const beforeMain=main;
  // No task change or event: a sample timer interrupts only the read-only wait.
  now=50;await fire(50);assert.equal(flight,null);await fire(0);
  assert.equal(flight.url,'/machines');
  // Another timer while this page is stalled cannot overlap/cancel its fetch.
  now=100;await fire(50);assert.equal(flight.url,'/machines');
  await reply('/machines',JSON.stringify({sequence:2,age:0}));
  assert.equal(section.sequence,2);assert.equal(section.ageField.textContent,'50','page transfer latency contributes to age');assert.equal(main,beforeMain);assert.equal(main.inputValue,'unsent operator draft');
  // A pending tick never becomes a concurrent request or an unbounded backlog.
  assert.equal(flight.url,'/ui/changes?after=41');
  now=150;await fire(50);await fire(0);assert.equal(flight.url,'/machines');
  flight.fail();await settle();assert.match(section.notice.textContent,/unavailable/);
  assert.equal(main.inputValue,'unsent operator draft');
  now=260;await fire(50);assert.equal(section.row.dataset.machineFreshness,'stale');assert.match(section.freshness.textContent,/Stale/);
  assert.equal(section.ageField.textContent,'210','in-flight time counts and age advances with no response/event');
  assert.equal(flight,null,'sample timer must not bypass the existing reconnect backoff');
  await fire(11);
  // Reconnect replaces only the observation section and leaves task revision41.
  assert.equal(flight.url,'/machines');await reply('/machines',JSON.stringify({sequence:4,age:0}));
  assert.equal(section.sequence,4);assert.equal(section.row.dataset.machineFreshness,'fresh');assert.equal(main.inputValue,'unsent operator draft');
  await reply('/ui/changes?after=41',{revision:'41',changed:false,reset:false});
  await reply('/ui/activity?after=0',{items:[],cursor:'0',has_more:false});
  assert.equal(flight.url,'/ui/changes?after=41');
  document.hidden=true;events.get('visibilitychange')();await settle();assert.equal(flight,null);assert.equal(timers.size,0);
  now=500;document.hidden=false;events.get('visibilitychange')();await settle();assert.equal(section.row.dataset.machineFreshness,'stale');assert.equal(flight.url,'/machines');
  windowEvents.get('pagehide')();await settle();assert.equal(flight,null);assert.equal(timers.size,0);
  assert.equal(maxFlight,1);assert.equal(main.inputValue,'unsent operator draft');
  console.log(JSON.stringify({status:'PASS',source:sourcePath,sha256:createHash('sha256').update(source).digest('hex'),cases:['sample-only refresh','bounded single in-flight','stale without events','unavailable reconnect','dirty form preserved','hidden and pagehide cleanup'],requests},null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
