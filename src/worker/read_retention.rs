//! Custody of terminal read originals under the existing worker owner.
//! Native's consumed mutation journal is its unique retained transcript: this
//! module reads it, but never replaces it or archives provider request history.
use super::*;
use crate::read_retention::{ReadEvidence, ReleaseItem, TranscriptCustody};

pub(super) fn native_reads(
    state: &Path, input: &TaskInput, attempt: &Attempt,
    policy: &crate::limits::WorkerRetentionLimits, after: Option<&str>,
) -> Result<(Vec<ReleaseItem>, TranscriptCustody), String> {
    let metadata = &input.resolved_limits.as_ref().ok_or("accepted metadata absent")?.global_metadata;
    if !handoff::admits_read_references(state, &metadata.worker)? {
        return Err("terminal read custody requires worker20".into());
    }
    let directory = state.join("collaboration").join(component(&attempt.id));
    let bound = (input.limits.model_requests as usize)
        .checked_mul(input.execution_limits()?.native.function_calls_per_response)
        .ok_or("native call inventory bound overflow")?;
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err("native read inventory unavailable".into()),
    };
    let deadline = process::now_ms().checked_add(policy.operation_timeout_ms)
        .ok_or("cleanup deadline overflow")?;
    let mut selected = BTreeMap::new();
    if let Some(entries) = entries {
        for (index, entry) in entries.enumerate() {
            if process::now_ms() >= deadline { return Err("cleanup operation deadline".into()); }
            if index >= bound { return Err("native call inventory bound".into()); }
            let path = entry.map_err(|_| "native read inventory unavailable")?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return Err("native call entry differs".into());
            }
            let bytes = cleanup::read_record_bytes(&path, metadata.worker.mutation_record_bytes)?
                .ok_or("native original journal missing")?;
            let journal = mutations::decode(&bytes, metadata)?;
            let Operation::CallCollaboration { call } = &journal.request else {
                return Err("native original journal operation differs".into());
            };
            if call.caller != collaboration_caller(attempt) || !journal.consumed {
                return Err("native original body still has a consumer".into());
            }
            let outcome = journal.outcome.as_ref().ok_or("native original outcome unresolved")?;
            let Ok(evidence) = ReadEvidence::from_outcome(call, outcome) else { continue };
            let context = journal.outcome_context.as_ref().ok_or("native original context absent")?;
            let result = crate::mcp::result(call, Reply { request_id:"retained-native-read".into(),
                commit:Some(context.commit.clone()), observed_controller_revision:Some(context.observed_controller_revision), outcome:outcome.clone() })?;
            if result.requires_quiescence { return Err("native original read still requires quiescence".into()); }
            if after.is_some_and(|id| call.operation_id.as_str() <= id) { continue; }
            let item = ReleaseItem { operation_id:call.operation_id.clone(),
                commit:context.commit.clone(), evidence };
            let source = (path.file_name().ok_or("native journal filename")?.to_string_lossy().into_owned(),
                bytes.len() as u64, crate::read_retention::digest(&bytes));
            if selected.insert(call.operation_id.clone(), (item, source)).is_some() {
                return Err("native original call duplicated".into());
            }
        }
    }
    let selected = selected.into_values().take(policy.batch_entries).collect::<Vec<_>>();
    let sources = selected.iter().map(|(_, source)| source).collect::<Vec<_>>();
    let bytes = sources.iter().try_fold(0u64, |sum, source| sum.checked_add(source.1))
        .ok_or("native custody byte count overflow")?;
    let manifest_sha256 = crate::read_retention::digest(
        &serde_json::to_vec(&sources).map_err(|_| "native custody encoding")?);
    let custody = TranscriptCustody::NativeJournal { manifest_sha256, bytes, files:sources.len() as u64 };
    Ok((selected.into_iter().map(|(item, _)| item).collect(), custody))
}

/// Old outer markers cannot admit compact inner carriers, even if there are no
/// currently active tasks. This runs before marker rewrite or report replay.
pub(super) fn admit(state:&Path,limits:&RuntimeLimits)->Result<(),String> {
    if handoff::admits_read_references(state,&limits.worker)? {return Ok(())}
    let outbox=match std::fs::read_dir(state.join("outbox")) {
        Ok(entries)=>Some(entries),Err(e) if e.kind()==std::io::ErrorKind::NotFound=>None,
        Err(_)=>return Err("read custody outbox unavailable".into()),
    };
    for (index,entry) in outbox.into_iter().flatten().enumerate() {
        if index>=limits.worker.outbox_records {return Err("read custody outbox inventory bound".into());}
        let path=entry.map_err(|_|"read custody outbox unavailable")?.path();
        if path.extension().is_some_and(|v|v=="json"||v=="refused")
            && matches!(read_report(&path,limits)?.observation,Observation::ReadTranscriptCustody{..}) {
            return Err("original read custody report requires worker20".into());
        }
    }
    for (_,receipt) in command_receipts(state,limits)? {
        let WorkerCommand::RunAttempt{attempt,task,..}=receipt.command else {continue};
        let root=state.join("collaboration").join(component(&attempt.id));
        let max=(task.limits.model_requests as usize).checked_mul(task.execution_limits()?.native.function_calls_per_response).ok_or("read admission count overflow")?;
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind()==std::io::ErrorKind::NotFound => None,
            Err(_) => return Err("read admission inventory unavailable".into()),
        };
        if let Some(entries)=entries {
            for (index,entry) in entries.enumerate() {
                if index>=max {return Err("read admission inventory bound".into())}
                let value:serde_json::Value=cleanup::read_record(&entry.map_err(|_|"read admission inventory")?.path(),limits.worker.mutation_record_bytes)?.ok_or("read admission missing")?;
                if value.get("native_read_reference").is_some() {return Err("native read reference requires worker20".into())}
            }
        }
        let path=codex::state_path(&state.join("codex"),&attempt.id).join("collaboration-codex/calls.json");
        if let Some(value)=cleanup::read_record::<serde_json::Value>(&path,task.execution_limits()?.collaboration.journal_bytes)? {
            if value.get("revision").and_then(|v|v.as_u64())==Some(3)
                ||value.get("entries").and_then(|v|v.as_array()).is_some_and(|v|v.iter().any(|v|v.get("outcome").and_then(|v|v.get("committed_read_reference")).is_some())) {
                return Err("Coding read reference requires worker20".into());
            }
        }
    }
    let root=state.join("cleanup-reads");
    match std::fs::read_dir(root) {
        Ok(mut entries)=>if entries.next().is_some(){Err("terminal read release requires worker20".into())}else{Ok(())},
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>Ok(()),Err(_)=>Err("read release admission unavailable".into()),
    }
}
