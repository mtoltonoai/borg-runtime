//! Lossless carriage of one already received output, using only the existing
//! private worker state and canonical operation journal. No provider replay.
use super::*;
use crate::charter::{CharterOutputChunk,CharterOutputReference};
use aws_lc_rs::digest;
use std::io::{Read,Write};
use std::os::unix::fs::{OpenOptionsExt,MetadataExt};

#[derive(Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    revision:u32,
    caller:crate::mcp::Caller,
    operation_id:String,
    observation:CharterObservation,
}
fn hex(bytes:&[u8])->String {const D:&[u8;16]=b"0123456789abcdef";let mut s=String::with_capacity(bytes.len()*2);for b in bytes{s.push(D[(b>>4)as usize]as char);s.push(D[(b&15)as usize]as char);}s}
fn sha(bytes:&[u8])->String {hex(digest::digest(&digest::SHA256,bytes).as_ref())}
fn request(observation:&CharterObservation)->Result<(&str,&CharterReplyObservation),String>{match observation{
    CharterObservation::SuppliedReply{turn_id,reply,..}|CharterObservation::Reply{turn_id,reply}=>Ok((turn_id,reply)),
    CharterObservation::CompactionReply{run_id,reply,..}=>Ok((run_id,reply)),
    _=>Err("original output manifest is not a reply".into()),
}}
fn reply_mut(observation:&mut CharterObservation)->Result<&mut CharterReplyObservation,String>{match observation{
    CharterObservation::SuppliedReply{reply,..}|CharterObservation::Reply{reply,..}|CharterObservation::CompactionReply{reply,..}=>Ok(reply),
    _=>Err("original output manifest is not a reply".into()),
}}
fn root(state:&Path)->PathBuf {state.join("charter-output")}
fn directory(state:&Path,id:&str)->PathBuf{root(state).join(sha(id.as_bytes()))}
fn chunk_path(dir:&Path,index:u32)->PathBuf{dir.join(format!("{index:010}.chunk"))}
fn chunk_operation(reference:&CharterOutputReference,index:u32)->String{format!("charter-output-{}-{index}",sha(reference.request_id.as_bytes()))}
fn bound(limits:&RuntimeLimits)->Result<usize,String>{
    let l=limits.charter.as_ref().ok_or("original output requires accepted charter policy")?;
    // A conservative envelope partition, not an accepted output maximum. Exact
    // journal/body/receipt bounds are checked before any chunk is sent.
    let pending=limits.worker.mutation_record_bytes.checked_sub(limits.transport.body_bytes)
        .ok_or("original output journal cannot reserve original outcome")?;
    // A chunk's original lookup carries one two-hex-digit encoding per byte.
    // Reserve half that outcome for fixed identities/receipt framing; the exact
    // typed preflight below remains authoritative for every concrete operation.
    let size=(l.record_bytes.min(limits.transport.body_bytes).min(pending)/16)
        .min(limits.controller.outcome_bytes/4);
    if size==0{return Err("original output chunk envelope cannot fit".into())}
    Ok(size.min(u32::MAX as usize))
}
fn read(path:&Path,max:usize)->Result<Vec<u8>,String>{
    let file=std::fs::OpenOptions::new().read(true).custom_flags(0x20000|0x800).open(path).map_err(|_|"original output file missing")?;
    let m=file.metadata().map_err(|e|e.to_string())?;
    let uid=std::fs::metadata("/proc/self").map_err(|e|e.to_string())?.uid();
    if !m.is_file()||m.uid()!=uid||m.len()>max as u64{return Err("original output file type/owner/bound differs".into())}
    let mut bytes=Vec::new();file.take(max as u64+1).read_to_end(&mut bytes).map_err(|e|e.to_string())?;
    if bytes.len()>max{return Err("original output file exceeds bound".into())}Ok(bytes)
}
fn sync(dir:&Path)->Result<(),String>{std::fs::File::open(dir).and_then(|v|v.sync_all()).map_err(|e|e.to_string())}
fn sync_ancestry(state:&Path,dir:&Path)->Result<(),String>{
    // mkdir/chmod establishes privacy, not durable parent entries. Sync child
    // contents then each newly allocated directory link before any outbound ACK.
    sync(dir)?;
    #[cfg(test)] tests::sync_cut(1)?;
    sync(&root(state))?;
    #[cfg(test)] tests::sync_cut(2)?;
    sync(state)
}
fn put_raw(path:&Path,bytes:&[u8])->Result<(),String>{
    if path.exists(){if read(path,bytes.len())?!=bytes{return Err("original output chunk changed".into())}std::fs::File::open(path).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;return sync(path.parent().ok_or("output parent absent")?)}
    let temp=path.with_extension("pending");
    let mut file=std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).custom_flags(0x20000).open(&temp).map_err(|e|e.to_string())?;
    file.write_all(bytes).and_then(|_|file.sync_all()).map_err(|e|e.to_string())?;
    std::fs::rename(&temp,path).map_err(|e|e.to_string())?;sync(path.parent().ok_or("output parent absent")?)
}
struct Counter {digest:digest::Context,bytes:u64,max:u64}
impl Write for Counter {
    fn write(&mut self,bytes:&[u8])->std::io::Result<usize>{self.bytes=self.bytes.checked_add(bytes.len()as u64).filter(|n|*n<=self.max).ok_or_else(||std::io::Error::other("original output exceeds accepted response bound"))?;self.digest.update(bytes);Ok(bytes.len())}
    fn flush(&mut self)->std::io::Result<()>{Ok(())}
}
struct Pieces<'a>{dir:&'a Path,size:usize,index:u32,bytes:Vec<u8>}
impl Pieces<'_>{
    fn save(&mut self)->std::io::Result<()>{if self.bytes.is_empty(){return Ok(())}put_raw(&chunk_path(self.dir,self.index),&self.bytes).map_err(std::io::Error::other)?;self.index=self.index.checked_add(1).ok_or_else(||std::io::Error::other("original output index exhausted"))?;self.bytes.clear();Ok(())}
}
impl Write for Pieces<'_>{
    fn write(&mut self,mut bytes:&[u8])->std::io::Result<usize>{let n=bytes.len();while !bytes.is_empty(){let take=(self.size-self.bytes.len()).min(bytes.len());self.bytes.extend_from_slice(&bytes[..take]);bytes=&bytes[take..];if self.bytes.len()==self.size{self.save()?;}}Ok(n)}
    fn flush(&mut self)->std::io::Result<()>{Ok(())}
}
fn operation(manifest:&Manifest,observation:CharterObservation,id:String)->Operation{Operation::ObserveCharter{operation_id:id,caller:manifest.caller.clone(),observation}}
fn check_manifest(manifest:&Manifest,limits:&RuntimeLimits,maximum:usize)->Result<(),String>{
    if manifest.revision!=1{return Err("unsupported original output manifest".into())}
    let (id,reply)=request(&manifest.observation)?;
    if id.is_empty()||manifest.operation_id!=format!("{id}-reply")||reply.output.is_some(){return Err("original output manifest identity or inline body differs".into())}
    let l=limits.charter.as_ref().ok_or("original output policy absent")?;
    bounded_json_size(manifest,l.record_bytes)?;
    if let Some(reference)=&reply.output_reference{
        reference.validate(maximum,bound(limits)?)?;
        if reference.request_id!=id||reference.chunks as usize>l.records{return Err("original output manifest extent differs".into())}
    }
    mutations::prepare(operation(manifest,manifest.observation.clone(),manifest.operation_id.clone()),0,limits)?;
    Ok(())
}
/// The original Value remains unchanged while its deterministic bytes are
/// streamed twice: once for extent/digest, once to bounded immutable files.
/// Manifest intent precedes chunk writes, so interrupted retention is visible.
pub(super) fn retain(state:&Path,limits:&RuntimeLimits,maximum:usize,caller:crate::mcp::Caller,
    operation_id:String,mut observation:CharterObservation)->Result<Manifest,String>{
    let id=request(&observation)?.0.to_owned();
    let reply=reply_mut(&mut observation)?;
    if reply.output_reference.is_some(){return Err("producer supplied an invented output reference".into())}
    let credentials=configured_credentials(limits)?;
    let output=reply.output.take();
    if let Some(value)=&output{
        if value_contains_credential(value,&credentials,&limits.worker)?{return Err("original output contains configured credential".into())}
        let mut count=Counter{digest:digest::Context::new(&digest::SHA256),bytes:0,max:maximum as u64};
        serde_json::to_writer(&mut count,value).map_err(|e|e.to_string())?;
        let chunk_bytes=u32::try_from(bound(limits)?).map_err(|_|"original output chunk size overflow")?;
        let chunks=u32::try_from(count.bytes.div_ceil(u64::from(chunk_bytes))).map_err(|_|"original output chunk count overflow")?;
        reply.output_reference=Some(CharterOutputReference{request_id:id,bytes:count.bytes,sha256:hex(count.digest.finish().as_ref()),chunk_bytes,chunks});
    }
    let manifest=Manifest{revision:1,caller,operation_id,observation};check_manifest(&manifest,limits,maximum)?;
    // Reserve the largest concrete chunk operation and complete result before
    // beginning storage. No provider/network call occurs in this function.
    if let Some(reference)=&request(&manifest.observation)?.1.output_reference{
        let chunk=CharterOutputChunk{reference:reference.clone(),index:reference.chunks-1,data:"0".repeat(reference.chunk_bytes as usize*2)};
        bounded_json_size(&chunk,limits.charter.as_ref().unwrap().record_bytes)?;
        mutations::prepare(operation(&manifest,CharterObservation::OutputChunk{chunk},chunk_operation(reference,reference.chunks-1)),0,limits)?;
    }
    let dir=directory(state,&manifest.operation_id);process::private_directory(&root(state))?;process::private_directory(&dir)?;
    let path=dir.join("manifest.json");
    if path.exists(){let old:Manifest=serde_json::from_slice(&read(&path,limits.charter.as_ref().unwrap().record_bytes)?).map_err(|_|"corrupt original output manifest")?;if old!=manifest{return Err("original output occurrence changed".into())}}
    atomic_json(&path,&manifest)?;
    if let Some(value)=output{
        let reference=request(&manifest.observation)?.1.output_reference.as_ref().unwrap();
        let mut pieces=Pieces{dir:&dir,size:reference.chunk_bytes as usize,index:0,bytes:Vec::with_capacity(reference.chunk_bytes as usize)};
        serde_json::to_writer(&mut pieces,&value).map_err(|e|e.to_string())?;pieces.save().map_err(|e|e.to_string())?;
        if pieces.index!=reference.chunks{return Err("original output encoder extent changed".into())}
    }
    verify_files(state,limits,maximum,&manifest)?;sync_ancestry(state,&dir)?;Ok(manifest)
}
fn verify_files(state:&Path,limits:&RuntimeLimits,maximum:usize,manifest:&Manifest)->Result<(),String>{
    check_manifest(manifest,limits,maximum)?;
    let dir=directory(state,&manifest.operation_id);
    let reference=request(&manifest.observation)?.1.output_reference.as_ref();
    let expected=reference.map_or(0,|r|r.chunks as usize);
    let mut seen=0usize;
    for entry in std::fs::read_dir(&dir).map_err(|e|e.to_string())?{
        let entry=entry.map_err(|e|e.to_string())?;let name=entry.file_name();
        if name=="manifest.json"{continue}
        seen=seen.checked_add(1).filter(|n|*n<=expected).ok_or("original output file inventory differs")?;
        let text=name.to_str().ok_or("original output file name invalid")?;
        let index=text.strip_suffix(".chunk").and_then(|s|s.parse::<u32>().ok()).ok_or("interrupted or unexpected original output file")?;
        if index>=expected as u32||entry.path()!=chunk_path(&dir,index){return Err("original output file order or identity differs".into())}
    }
    if seen!=expected{return Err("original output incomplete; provider replay forbidden".into())}
    if let Some(reference)=reference{
        let mut digest=digest::Context::new(&digest::SHA256);let mut total=0u64;
        for index in 0..reference.chunks{
            let bytes=read(&chunk_path(&dir,index),reference.chunk_bytes as usize)?;
            let size=(reference.bytes-u64::from(index)*u64::from(reference.chunk_bytes)).min(u64::from(reference.chunk_bytes));
            if bytes.len()as u64!=size{return Err("original output chunk length differs".into())}
            digest.update(&bytes);total=total.checked_add(bytes.len()as u64).ok_or("original output byte count overflow")?;
        }
        if total!=reference.bytes||hex(digest.finish().as_ref())!=reference.sha256{return Err("original output digest differs".into())}
    }Ok(())
}
/// Use the exact successful mutation and original GetOperation shapes. The
/// controller independently enforces these same ceilings; a journal fitting its
/// own disk envelope alone does not establish recoverable receipt carriage.
fn chunk_frames(manifest:&Manifest,chunk:&CharterOutputChunk,ack:&crate::charter::CharterOutputChunkReceipt)->Result<[Reply;2],String>{
    let id=chunk_operation(&chunk.reference,chunk.index);
    let request=operation(manifest,CharterObservation::OutputChunk{chunk:chunk.clone()},id.clone());
    let outcome=Outcome::Ok{data:Data::Charter(CharterOutcome::OutputChunk(ack.clone()))};
    let lookup=Outcome::Ok{data:Data::Operation(OperationRecord{id:id.clone(),task_id:String::new(),attempt_id:String::new(),generation:0,kind:"application_mutation".into(),intended:serde_json::to_value(&request).map_err(|e|e.to_string())?,state:"settled".into(),outcome:Some(serde_json::to_value(&outcome).map_err(|e|e.to_string())?)})};
    Ok([outcome,lookup].map(|outcome|Reply{request_id:String::new(),outcome,commit:Some(MutationCommit{operation_id:id.clone(),controller_revision:i64::MAX as u64}),observed_controller_revision:Some(i64::MAX as u64)}))
}
fn chunk_preflight(limits:&RuntimeLimits,manifest:&Manifest,chunk:&CharterOutputChunk,ack:&crate::charter::CharterOutputChunkReceipt)->Result<(),String>{
    for frame in chunk_frames(manifest,chunk,ack)?{
        bounded_json_size(&frame.outcome,limits.controller.outcome_bytes).map_err(|_|"original output receipt/lookup exceeds controller.outcome_bytes")?;
        let size=bounded_json_size(&frame,limits.transport.body_bytes)?;
        if limits.transport.identity_bytes.checked_mul(2).and_then(|padding|size.checked_add(padding)).is_none_or(|size|size>limits.transport.body_bytes){return Err("original output complete reply exceeds transport.body_bytes".into())}
    }
    mutations::prepare(operation(manifest,CharterObservation::OutputChunk{chunk:chunk.clone()},chunk_operation(&chunk.reference,chunk.index)),0,limits)?;
    Ok(())
}
pub(super) async fn upload(client:&ApiClient,state:&Path,limits:&RuntimeLimits,maximum:usize,manifest:&Manifest)->Result<CharterOutcome,String>{
    verify_files(state,limits,maximum,manifest)?;
    // Re-establish manifest durability before authorizing any outbound record.
    let dir=directory(state,&manifest.operation_id);atomic_json(&dir.join("manifest.json"),manifest)?;
    sync_ancestry(state,&dir)?;
    if let Some(reference)=&request(&manifest.observation)?.1.output_reference{
        for index in 0..reference.chunks{
            let bytes=read(&chunk_path(&dir,index),reference.chunk_bytes as usize)?;
            let expected=crate::charter::CharterOutputChunkReceipt{reference:reference.clone(),index,sha256:sha(&bytes),bytes:u32::try_from(bytes.len()).map_err(|_|"output chunk count overflow")?};
            let chunk=CharterOutputChunk{reference:reference.clone(),index,data:hex(&bytes)};
            chunk_preflight(limits,manifest,&chunk,&expected)?;
            match mutate_original(client,state,limits,manifest.caller.clone(),chunk_operation(reference,index),CharterObservation::OutputChunk{chunk}).await?{
                CharterOutcome::OutputChunk(actual) if actual==expected=>{},
                _=>return Err("original output chunk acknowledgment differs".into()),
            }
        }
    }
    let result=mutate_original(client,state,limits,manifest.caller.clone(),manifest.operation_id.clone(),manifest.observation.clone()).await?;
    let (id,original)=request(&manifest.observation)?;
    let reply=match &result{CharterOutcome::Reply(v)=>v,CharterOutcome::Compaction(v)=>v.reply.as_ref().ok_or("original compaction reply absent")?,_=>return Err("original reply outcome differs".into())};
    if reply.turn_id!=id||reply.reply_id!=manifest.operation_id||reply.observation!=*original{return Err("original reply acknowledgment association differs".into())}
    Ok(result)
}
/// Local validation happens before the owned outer marker transition and any
/// outbound recovery. Missing chunks remain an explicit refusal, never a reset.
pub(in crate::worker) fn admit(state:&Path,limits:&RuntimeLimits,worker:&str)->Result<(),String>{
    let root=root(state);if !root.exists(){return Ok(())}
    let format:serde_json::Value=serde_json::from_slice(&read(&state.join("format.json"),limits.worker.state_format_bytes)?).map_err(|_|"invalid output owner marker")?;
    if !matches!(format.get("format_revision").and_then(|v|v.as_u64()),Some(13 | 14 | 15 | 16 | 17 | 18))
        && std::fs::read_dir(&root).map_err(|e|e.to_string())?.next().is_some(){return Err("new output records under historical worker marker".into())}
    for (manifest,original,maximum) in inventory(state,limits,worker)?{
        if matches!(&manifest.observation,CharterObservation::SuppliedReply{..})
            &&!matches!(format.get("format_revision").and_then(|v|v.as_u64()),Some(17 | 18)){return Err("supplied original output requires worker17".into());}
        verify_files(state,&original,maximum,&manifest)?;
    }Ok(())
}
fn inventory(state:&Path,limits:&RuntimeLimits,worker:&str)->Result<Vec<(Manifest,RuntimeLimits,usize)>,String>{
    let commands=command_receipts(state,limits)?;
    let mut runs=Vec::new();let mut maximum=0usize;let mut maximum_records=0usize;
    for (path,receipt) in &commands{
        let WorkerCommand::RunAttempt{command_id,attempt,task,..}=&receipt.command else{continue};
        if path!=&state.join("commands").join(format!("{}.json",component(command_id)))||attempt.worker_id!=worker{return Err("original output command identity differs".into())}
        task.execution_limits()?;
        let accepted=task.resolved_limits.as_ref().ok_or("output command policy absent")?.global_metadata.as_ref();
        maximum=maximum.max(accepted.charter.as_ref().map_or(0,|l|l.record_bytes));
        maximum_records=maximum_records.max(accepted.charter.as_ref().map_or(0,|l|l.records));
        let maximum=if task.definition.executor==ExecutorKind::Coding {
            crate::charter::supplied_output_bytes(task.execution_limits()?.coding.frame_bytes,accepted.charter.as_ref().map_or(0,|l|l.reply_items))?
        }else{task.execution_limits()?.native.canonical_output_bytes()?};
        runs.push((attempt,accepted,maximum));
    }
    let mut entries=Vec::new();
    for e in std::fs::read_dir(root(state)).map_err(|e|e.to_string())?{if entries.len()>=maximum_records{return Err("original output inventory exceeds accepted charter.records".into())}let e=e.map_err(|e|e.to_string())?;if !e.file_type().map_err(|e|e.to_string())?.is_dir(){return Err("original output directory type differs".into())}entries.push(e.path());}
    entries.sort();let mut manifests=Vec::new();
    for dir in entries{
        let manifest:Manifest=serde_json::from_slice(&read(&dir.join("manifest.json"),maximum)?).map_err(|_|"incomplete or corrupt original output manifest")?;
        if dir!=directory(state,&manifest.operation_id){return Err("original output directory identity differs".into())}
        let mut matches=runs.iter().filter(|(a,_,_)|caller(a)==manifest.caller);
        let (_,accepted,maximum)=matches.next().ok_or("original output accepted command missing")?;
        if matches.next().is_some(){return Err("original output accepted command ambiguous".into())}
        manifests.push((manifest,(*accepted).clone(),*maximum));
    }Ok(manifests)
}
/// Recovery only resumes immutable audit storage. The exact retained RunAttempt
/// supplies policy/caller; a manifest never selects its own authority. No Begin,
/// provider call, tool dispatch, task mutation, or replacement request occurs.
pub(in crate::worker) async fn recover(client:&ApiClient,state:&Path,url:&str,worker:&str)->Result<(),String>{
    let root=root(state);if !root.exists(){return Ok(())}
    for (manifest,limits,maximum) in inventory(state,client.limits(),worker)?{
        let original=ApiClient::new(url,Arc::new(limits.clone()))?;
        upload(&original,state,&limits,maximum,&manifest).await?;
    }Ok(())
}

#[cfg(test)]
#[path="charter_output_tests.rs"]
mod tests;
