//! Bounded pieces of one original provider output, in the caller's existing
//! controller transaction. A partial upload is neither a reply nor authority.
use super::*;
use aws_lc_rs::digest;
use rusqlite::limits::Limit;
use std::io::{self, Read};

const TABLE: &str = "charter_output_chunks";

fn present(tx: &StoreContext<'_>) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [TABLE], |r| r.get(0),
    )?)
}

fn digest_hex(bytes: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, bytes).as_ref())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}

fn lower_hex(value: &str) -> bool {
    value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn unhex(value: &str, bytes: usize) -> Result<Vec<u8>> {
    if bytes.checked_mul(2) != Some(value.len()) || !lower_hex(value) {
        return refuse("invalid_input", "output chunk requires exact lowercase hex bytes");
    }
    let nibble = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
    Ok(value.as_bytes().chunks_exact(2)
        .map(|p| (nibble(p[0]) << 4) | nibble(p[1])).collect())
}

fn chunk_length(reference: &CharterOutputReference, index: u32) -> Result<usize> {
    if reference.bytes == 0 || reference.chunk_bytes == 0 || reference.chunks == 0
        || index >= reference.chunks
    {
        return refuse("invalid_input", "invalid output chunk extent");
    }
    let size = u64::from(reference.chunk_bytes);
    let expected = reference.bytes / size + u64::from(reference.bytes % size != 0);
    if expected != u64::from(reference.chunks) {
        return refuse("invalid_input", "output chunk count differs from exact extent");
    }
    let offset = u64::from(index).checked_mul(size)
        .ok_or_else(|| corrupt("output chunk offset overflow"))?;
    let remaining = reference.bytes.checked_sub(offset)
        .ok_or_else(|| corrupt("output chunk offset exceeds original extent"))?;
    usize::try_from(remaining.min(size))
        .map_err(|_| corrupt("output chunk length exceeds host representation"))
}

/// Return the immutable accepted task policy, never a current replacement for
/// that policy. Current reader bounds are checked independently below.
fn accepted(tx: &StoreContext<'_>, request: &RequestRecord) -> Result<Task> {
    if matches!(request, RequestRecord::SuppliedContext(_)) {
        return refuse("invalid_input", "supplied context has no native provider output");
    }
    let task = historical(tx, request.caller(), request.session())?;
    if task.input.definition.executor != if matches!(request, RequestRecord::SuppliedTurn(_)) { ExecutorKind::Coding } else { ExecutorKind::Native } {
        return refuse("source_mismatch", "output chunk requires original native occurrence");
    }
    let selected = task.input.resolved_limits.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted policy absent"))?;
    selected.validate_for(&task.input).map_err(Fault::Store)?;
    if selected.global_metadata.charter.is_none() {
        return refuse("not_configured", "original task has no accepted charter policy");
    }
    Ok(task)
}

fn reference_bounds(
    tx: &StoreContext<'_>, request: &RequestRecord, reference: &CharterOutputReference,
    l: &CharterLimits, task: &Task,
) -> Result<()> {
    identity(tx, &reference.request_id)?;
    let selected = task.input.resolved_limits.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted policy absent"))?;
    let original = selected.global_metadata.charter.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted charter policy absent"))?;
    let actual_sqlite = usize::try_from(tx.limit(Limit::SQLITE_LIMIT_LENGTH)?)
        .map_err(|_| corrupt("invalid SQLite length limit"))?;
    let chunk_maximum = l.record_bytes.min(original.record_bytes)
        .min(tx.limits.transport.body_bytes).min(selected.global_metadata.transport.body_bytes)
        .min(actual_sqlite);
    reference.validate(if matches!(request, RequestRecord::SuppliedTurn(_)) { supplied_output_bytes(selected.values.coding.frame_bytes, original.reply_items).map_err(Fault::Store)? } else { selected.values.native.canonical_output_bytes().map_err(Fault::Store)? }, chunk_maximum)
        .map_err(|_| Fault::Refused("input_bound", "output reference violates original or current storage bounds".into()))?;
    if reference.request_id != request.id()
        || !valid_id(&selected.global_metadata, &reference.request_id)
        || u64::from(reference.chunks) > l.records as u64
        || u64::from(reference.chunks) > original.records as u64
    {
        return refuse("input_bound", "output reference differs from original occurrence or bounds");
    }
    chunk_length(reference, 0)?;
    for bound in [l.record_bytes, original.record_bytes,
        tx.limits.transport.body_bytes, selected.global_metadata.transport.body_bytes]
    {
        bounded(reference, bound)?;
    }
    Ok(())
}

fn chunk_receipt(chunk: &CharterOutputChunk, bytes: &[u8]) -> Result<CharterOutputChunkReceipt> {
    Ok(CharterOutputChunkReceipt {
        reference: chunk.reference.clone(), index: chunk.index,
        sha256: digest_hex(bytes), bytes: u32::try_from(bytes.len())
            .map_err(|_| corrupt("output chunk exceeds receipt representation"))?,
    })
}

/// Bound the real operation, original receipt lookup, and chunk query envelope,
/// not merely the raw BLOB. Hex expansion is measured by the existing encoder.
fn envelope_bounds(
    tx: &StoreContext<'_>, operation_id: &str, caller: &Caller,
    chunk: &CharterOutputChunk, result: &CharterOutputChunkReceipt,
    l: &CharterLimits, task: &Task, current_receipt_checked: bool,
) -> Result<()> {
    let selected = task.input.resolved_limits.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted policy absent"))?;
    let original = selected.global_metadata.charter.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted charter policy absent"))?;
    if !valid_id(&selected.global_metadata, operation_id) {
        return refuse("invalid_input", "output chunk operation exceeds accepted identity bound");
    }
    caller.validate(&selected.global_metadata.transport)
        .map_err(|_| corrupt("output chunk caller violates original accepted identity bounds"))?;
    bounded(chunk, l.record_bytes)?;
    bounded(chunk, original.record_bytes)?;
    let operation = Operation::ObserveCharter {
        operation_id: operation_id.into(), caller: caller.clone(),
        observation: CharterObservation::OutputChunk { chunk: chunk.clone() },
    };
    let outcome = Outcome::Ok { data: Data::Charter(CharterOutcome::OutputChunk(result.clone())) };
    let query = CharterQueryResult::OutputChunk(chunk.clone());
    // Both policies are mandatory. Identical immutable values have the same
    // encoded predicate; do not serialize a large piece a second time merely
    // because the accepted snapshot is a separately owned copy.
    let accepted=selected.global_metadata.as_ref();
    for limits in std::iter::once(tx.limits).chain((accepted!=tx.limits).then_some(accepted)) {
        bounded(&operation, limits.transport.body_bytes)?;
        // checked_chunk obtained this exact request/outcome from metadata::read,
        // which already enforces receipt_bound under this same current policy.
        // Fresh input and every distinct accepted policy still check it here.
        if !current_receipt_checked || limits!=tx.limits {
            metadata::receipt_bound(limits, &operation, &outcome)?;
        }
        bounded(&query, limits.transport.body_bytes)?;
    }
    Ok(())
}

fn varint_bytes(mut value: u64) -> u64 {
    let mut size = 1;
    while value > 127 && size < 9 { value >>= 7; size += 1; }
    size
}

/// SQLite's limit applies to the complete encoded row, including serial-type
/// headers. These are the five concrete columns of charter_output_chunks.
fn sqlite_row_bytes(request_id: &str, index: u32, operation_id: &str, reference: &str, body: usize) -> Result<u64> {
    let integer_bytes: u64 = match index {
        0 | 1 => 0, 2..=127 => 1, 128..=32767 => 2,
        32768..=8_388_607 => 3, 8_388_608..=2_147_483_647 => 4, _ => 6,
    };
    let mut data = integer_bytes;
    let mut serials: u64 = 1; // serial type of any represented u32 integer
    for (length, text) in [(request_id.len(), true), (operation_id.len(), true),
        (reference.len(), true), (body, false)]
    {
        let length = u64::try_from(length).map_err(|_| corrupt("output row size overflow"))?;
        data = data.checked_add(length).ok_or_else(|| corrupt("output row size overflow"))?;
        let serial = length.checked_mul(2).and_then(|v| v.checked_add(if text { 13 } else { 12 }))
            .ok_or_else(|| corrupt("output row serial type overflow"))?;
        serials += varint_bytes(serial);
    }
    let mut header = serials + 1;
    loop {
        let next = serials + varint_bytes(header);
        if next == header { break; }
        header = next;
    }
    data.checked_add(header).ok_or_else(|| corrupt("output row size overflow"))
}

fn sqlite_bound(tx: &StoreContext<'_>, request_id: &str, index: u32, operation_id: &str, reference: &str, body: usize) -> Result<()> {
    let actual = u64::try_from(tx.limit(Limit::SQLITE_LIMIT_LENGTH)?)
        .map_err(|_| corrupt("invalid SQLite length limit"))?;
    if sqlite_row_bytes(request_id, index, operation_id, reference, body)? > actual {
        return refuse("input_bound", "output chunk row exceeds actual SQLite length limit");
    }
    Ok(())
}

struct StoredChunk {
    operation_id: String,
    reference: CharterOutputReference,
    body: Vec<u8>,
}

/// Every body is byte-prefixed in SQL before materialization; original lengths
/// and SQL types are checked as well, so NUL/truncation cannot hide corruption.
fn read_chunk(tx: &StoreContext<'_>, request_id: &str, index: u32, l: &CharterLimits) -> Result<StoredChunk> {
    let id_bound = sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?;
    let body_bound = sql_bound(l.record_bytes.saturating_add(1))?;
    let row = tx.query_row(
        "SELECT typeof(chunk_index),typeof(operation_id),typeof(reference),typeof(body),\
         length(CAST(operation_id AS BLOB)),length(CAST(reference AS BLOB)),length(CAST(body AS BLOB)),\
         CAST(substr(CAST(operation_id AS BLOB),1,?3) AS TEXT),\
         CAST(substr(CAST(reference AS BLOB),1,?4) AS TEXT),substr(CAST(body AS BLOB),1,?4)\
         FROM charter_output_chunks WHERE request_id=?1 AND chunk_index=?2",
        params![request_id, i64::from(index), id_bound, body_bound],
        |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?, r.get::<_,String>(3)?,
            r.get::<_,i64>(4)?, r.get::<_,i64>(5)?, r.get::<_,i64>(6)?,
            r.get::<_,String>(7)?, r.get::<_,String>(8)?, r.get::<_,Vec<u8>>(9)?)),
    ).optional()?.ok_or_else(|| corrupt("original output chunk is missing"))?;
    let (index_type, operation_type, reference_type, body_type, a, b, c, operation_id, raw, body) = row;
    if index_type != "integer" || operation_type != "text" || reference_type != "text" || body_type != "blob"
        || unsigned(a)? > tx.limits.transport.identity_bytes as u64
        || unsigned(b)? > l.record_bytes as u64 || unsigned(c)? > l.record_bytes as u64
        || unsigned(a)? != operation_id.len() as u64 || unsigned(b)? != raw.len() as u64
        || unsigned(c)? != body.len() as u64
    {
        return Err(corrupt("stored output chunk type or byte bound differs"));
    }
    identity(tx, &operation_id).map_err(|_| corrupt("stored output operation identity differs"))?;
    let reference: CharterOutputReference = stored(&raw, l.record_bytes)?;
    if reference.request_id != request_id || body.len() != chunk_length(&reference, index)? {
        return Err(corrupt("stored output chunk extent differs"));
    }
    sqlite_bound(tx, request_id, index, &operation_id, &raw, body.len())?;
    Ok(StoredChunk { operation_id, reference, body })
}

fn checked_chunk(
    tx: &StoreContext<'_>, request: &RequestRecord, reference: &CharterOutputReference,
    index: u32, l: &CharterLimits, task: &Task,
) -> Result<(StoredChunk, u64)> {
    let stored = read_chunk(tx, request.id(), index, l)?;
    if stored.reference != *reference {
        return Err(corrupt("original output chunks have different references"));
    }
    let original = receipt(tx, &stored.operation_id)?;
    let Operation::ObserveCharter { operation_id, caller, observation: CharterObservation::OutputChunk { chunk } } = &original.request else {
        return Err(corrupt("output chunk lacks its exact original operation"));
    };
    if operation_id != &stored.operation_id || caller != request.caller()
        || chunk.reference != *reference || chunk.index != index
        || chunk.data != hex(&stored.body)
        || charter_outcome(&original)? != &CharterOutcome::OutputChunk(chunk_receipt(chunk, &stored.body)?)
    {
        return Err(corrupt("output chunk differs from its original receipt"));
    }
    envelope_bounds(tx, operation_id, caller, chunk, &chunk_receipt(chunk, &stored.body)?, l, task, true)?;
    if original.commit.controller_revision <= receipt(tx, request.id())?.commit.controller_revision {
        return Err(corrupt("output chunk preceded its original request"));
    }
    Ok((stored, original.commit.controller_revision))
}

/// A bounded count also proves zero-based contiguous indices: the primary key
/// is unique, and count/min/max must agree. No configured-count scan is emitted.
fn count(tx: &StoreContext<'_>, request_id: &str, l: &CharterLimits) -> Result<u32> {
    let limit = sql_bound(l.records.checked_add(1).ok_or_else(|| corrupt("output count bound overflow"))?)?;
    let (n, first, last): (i64, Option<i64>, Option<i64>) = tx.query_row(
        "SELECT count(*),min(chunk_index),max(chunk_index) FROM\
         (SELECT chunk_index FROM charter_output_chunks WHERE request_id=?1 ORDER BY chunk_index LIMIT ?2)",
        params![request_id, limit], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let n = unsigned(n)?;
    if n > l.records as u64 || (n != 0 && (first != Some(0) || last != Some(integer(n - 1)?))) {
        return Err(corrupt("output chunk membership is not an exact contiguous prefix"));
    }
    u32::try_from(n).map_err(|_| corrupt("output chunk count exceeds representation"))
}

/// Read only this bounded sealed row; do not call parent reply(), which invokes
/// verify() and would recursively re-enter this module.
fn sealed(tx: &StoreContext<'_>, request: &RequestRecord, l: &CharterLimits) -> Result<Option<(CharterReplyRecord, u64)>> {
    let Some(raw) = raw_body(tx,
        "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE request_id=?1",
        request.id(), l.record_bytes)? else { return Ok(None) };
    let value: CharterReplyRecord = stored(&raw, l.record_bytes)?;
    let id: String = tx.query_row(
        "SELECT CAST(substr(CAST(id AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE request_id=?1",
        params![request.id(), sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?], |r| r.get(0),
    )?;
    if value.reply_id != id || value.turn_id != request.id() || value.effective != request.source().reference
        || value.audit != audit(request.source(), &value.observation.declaration)
        || value.observation.output.is_some() || value.observation.output_reference.is_none()
    {
        return Err(corrupt("output chunks lack their exact sealed reference reply"));
    }
    observation(tx, &value.observation, l)?;
    let original = receipt(tx, &value.reply_id)?;
    let Operation::ObserveCharter { caller, observation, .. } = &original.request else {
        return Err(corrupt("sealed output reply original operation differs"));
    };
    let matches = match (request, observation, charter_outcome(&original)?) {
        (RequestRecord::SuppliedTurn(_), CharterObservation::SuppliedReply { turn_id, terminal, reply }, CharterOutcome::Reply(record)) => {
            supplied_turns::validate_terminal(tx, request.id(), terminal, l)?;
            turn_id == request.id() && reply == &value.observation && record == &value
        },
        (RequestRecord::Turn(_), CharterObservation::Reply { turn_id, reply }, CharterOutcome::Reply(record)) =>
            turn_id == request.id() && reply == &value.observation && record == &value,
        (RequestRecord::Compaction(_), CharterObservation::CompactionReply { run_id, reply, summary }, CharterOutcome::Compaction(record)) =>
            run_id == request.id() && reply == &value.observation && record.run_id == request.id()
                && record.reply.as_ref() == Some(&value) && &record.summary == summary,
        _ => false,
    };
    if !matches || caller != request.caller()
        || original.commit.controller_revision <= receipt(tx, request.id())?.commit.controller_revision
    {
        return Err(corrupt("sealed output differs from its original reply receipt"));
    }
    Ok(Some((value, original.commit.controller_revision)))
}

pub(super) fn put(ctx: &Context<'_, '_>, caller: &Caller, chunk: &CharterOutputChunk) -> Result<CharterOutputChunkReceipt> {
    identity(ctx.tx, ctx.operation_id)?;
    let request = request(ctx.tx, &chunk.reference.request_id, ctx.limits)?;
    if caller != request.caller() { return refuse("caller_mismatch", "output chunk belongs to another original caller"); }
    let task = accepted(ctx.tx, &request)?;
    reference_bounds(ctx.tx, &request, &chunk.reference, ctx.limits, &task)?;
    validate_request_receipt(ctx.tx, &request)?;
    if request_rejection(ctx.tx, &request, ctx.limits)?.is_some() {
        return refuse("operation_conflict", "confirmed rejected request has no provider output");
    }
    let has_reply: bool = ctx.tx.query_row("SELECT EXISTS(SELECT 1 FROM charter_replies WHERE request_id=?1)", [request.id()], |r| r.get(0))?;
    if has_reply { return refuse("operation_conflict", "sealed original output cannot acquire another chunk"); }
    let n = count(ctx.tx, request.id(), ctx.limits)?;
    if n != chunk.index { return refuse("operation_conflict", "output chunk is not the next original index"); }
    let raw = encode(&chunk.reference)?;
    let different: bool = ctx.tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM charter_output_chunks WHERE request_id=?1 AND (typeof(reference)!='text' OR reference!=?2))",
        params![request.id(), raw], |r| r.get(0),
    )?;
    if different { return refuse("operation_conflict", "output upload cannot replace its original reference"); }
    if n > 0 { checked_chunk(ctx.tx, &request, &chunk.reference, n - 1, ctx.limits, &task)?; }
    // Refuse an oversized encoded chunk before allocating its decoded bytes.
    bounded(chunk, ctx.limits.record_bytes)?;
    let selected = task.input.resolved_limits.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted policy absent"))?;
    bounded(chunk, selected.global_metadata.charter.as_ref()
        .ok_or_else(|| corrupt("output chunk accepted charter policy absent"))?.record_bytes)?;
    let body = unhex(&chunk.data, chunk_length(&chunk.reference, chunk.index)?)?;
    let result = chunk_receipt(chunk, &body)?;
    envelope_bounds(ctx.tx, ctx.operation_id, caller, chunk, &result, ctx.limits, &task, false)?;
    sqlite_bound(ctx.tx, request.id(), chunk.index, ctx.operation_id, &raw, body.len())?;
    capacity(ctx.tx, ctx.limits, 1)?;
    let duplicate: bool = ctx.tx.query_row("SELECT EXISTS(SELECT 1 FROM charter_output_chunks WHERE operation_id=?1)", [ctx.operation_id], |r| r.get(0))?;
    if duplicate { return refuse("operation_conflict", "output chunk operation already owns another row"); }
    ctx.tx.execute(
        "INSERT INTO charter_output_chunks(request_id,chunk_index,operation_id,reference,body) VALUES(?1,?2,?3,?4,?5)",
        params![request.id(), i64::from(chunk.index), ctx.operation_id, raw, body],
    )?;
    if chunk.index+1==chunk.reference.chunks {
        // A malformed complete upload must not become retained state that
        // startup cannot read. Failure rolls back only this final candidate;
        // committed predecessors and the original refusal remain unchanged.
        verify_inner(ctx.tx,&request,&chunk.reference,ctx.limits,Some((ctx.operation_id,chunk)))
            .map_err(|_|Fault::Refused("invalid_input","complete original output JSON/digest differs".into()))?;
    }
    Ok(result)
}

/// IgnoredAny does not validate ignored string UTF-8 and uses a nesting scratch
/// vector. Enforce UTF-8 across piece boundaries and the original accepted depth
/// before giving bytes to it. No string value, property, or output is retained.
#[derive(Default)]
struct StreamGuard {
    tail: [u8; 4], tail_len: usize,
    depth: usize, in_string: bool, escaped: bool,
}

impl StreamGuard {
    fn accept(&mut self, bytes: &[u8], maximum_depth: usize) -> Result<()> {
        let mut offset = 0;
        while self.tail_len != 0 && offset < bytes.len() {
            if self.tail_len == self.tail.len() { return Err(corrupt("original output has invalid UTF-8")); }
            self.tail[self.tail_len] = bytes[offset];
            self.tail_len += 1; offset += 1;
            match std::str::from_utf8(&self.tail[..self.tail_len]) {
                Ok(_) => self.tail_len = 0,
                Err(error) if error.error_len().is_none() => {},
                Err(_) => return Err(corrupt("original output has invalid UTF-8")),
            }
        }
        if offset < bytes.len() {
            if let Err(error) = std::str::from_utf8(&bytes[offset..]) {
                if error.error_len().is_some() { return Err(corrupt("original output has invalid UTF-8")); }
                let tail = &bytes[offset + error.valid_up_to()..];
                if tail.len() >= self.tail.len() { return Err(corrupt("original output has invalid UTF-8")); }
                self.tail[..tail.len()].copy_from_slice(tail); self.tail_len = tail.len();
            }
        }
        for byte in bytes {
            if self.in_string {
                if self.escaped { self.escaped = false; }
                else if *byte == b'\\' { self.escaped = true; }
                else if *byte == b'"' { self.in_string = false; }
            } else {
                match byte {
                    b'"' => self.in_string = true,
                    b'[' | b'{' => {
                        self.depth = self.depth.checked_add(1).filter(|n| *n <= maximum_depth)
                            .ok_or_else(|| corrupt("original output exceeds accepted JSON depth"))?;
                    },
                    b']' | b'}' => self.depth = self.depth.checked_sub(1)
                        .ok_or_else(|| corrupt("original output JSON closure differs"))?,
                    _ => {},
                }
            }
        }
        Ok(())
    }

    fn complete(&self) -> bool {
        self.tail_len == 0 && self.depth == 0 && !self.in_string && !self.escaped
    }
}

/// Value serialization has finite numbers and unique object keys. Validate
/// those properties without allocating string values. Key fingerprints are
/// bounded by the already accepted json_nodes; the JSON parser below remains
/// responsible for the complete grammar. String escapes are the canonical
/// serde_json Value encoding, so identical decoded keys have identical bytes.
#[derive(Default)]
struct ValueGuard {
    stack:Vec<Option<BTreeSet<String>>>,key_due:Vec<bool>,nodes:usize,
    in_string:bool,key:bool,key_hash:Option<digest::Context>,escape:u8,
    unicode:u16,unicode_digits:u8,number:Vec<u8>,primitive:bool,
}
impl ValueGuard {
    fn node(&mut self,max:usize)->Result<()>{self.nodes=self.nodes.checked_add(1).filter(|n|*n<=max).ok_or_else(||corrupt("original output exceeds accepted JSON node bound"))?;Ok(())}
    fn finish_number(&mut self)->Result<()>{if !self.number.is_empty(){
        let number:serde_json::Number=serde_json::from_slice(&self.number).map_err(|_|corrupt("original output number is not a finite JSON Value"))?;
        if number.to_string().as_bytes()!=self.number{return Err(corrupt("original output number is not the original Value encoding"));}self.number.clear();
    }Ok(())}
    fn accept(&mut self,bytes:&[u8],max_nodes:usize)->Result<()>{for &b in bytes{
        if self.in_string {
            if let Some(hash)=&mut self.key_hash{hash.update(&[b]);}
            if self.escape==2 {
                let nibble=match b{b'0'..=b'9'=>b-b'0',b'a'..=b'f'=>b-b'a'+10,_=>return Err(corrupt("noncanonical original string escape"))};
                self.unicode=(self.unicode<<4)|u16::from(nibble);self.unicode_digits+=1;
                if self.unicode_digits==4 {if self.unicode>0x1f||matches!(self.unicode,8|9|10|12|13){return Err(corrupt("noncanonical original string escape"));}self.escape=0;}
                continue;
            }
            if self.escape==1 {match b{b'u'=>{self.escape=2;self.unicode=0;self.unicode_digits=0;},b'"'|b'\\'|b'b'|b'f'|b'n'|b'r'|b't'=>self.escape=0,_=>return Err(corrupt("noncanonical original string escape"))}continue;}
            match b {
                b'\\'=>self.escape=1,
                b'"'=>{self.in_string=false;if self.key{let h=hex(self.key_hash.take().expect("key digest").finish().as_ref());let keys=self.stack.last_mut().and_then(Option::as_mut).ok_or_else(||corrupt("original key outside object"))?;if !keys.insert(h){return Err(corrupt("duplicate original output object key"));}*self.key_due.last_mut().unwrap()=false;}},
                0..=31=>return Err(corrupt("unescaped original string control")),_=>{},
            }continue;
        }
        if !self.number.is_empty(){if b.is_ascii_digit()||matches!(b,b'e'|b'E'|b'+'|b'-'|b'.'){
            // serde_json uses at most 24 bytes for its finite f64 formatter,
            // and at most 20 for i64/u64. This bounds representation, not output.
            if self.number.len()>=32{return Err(corrupt("number cannot be a serialized JSON Value"));}self.number.push(b);continue;
        }self.finish_number()?;}
        if b.is_ascii_whitespace(){continue;}
        match b {
            b'"'=>{self.node(max_nodes)?;self.in_string=true;self.primitive=false;self.key=self.stack.last().is_some_and(Option::is_some)&&self.key_due.last()==Some(&true);if self.key{self.key_hash=Some(digest::Context::new(&digest::SHA256));}},
            b'{'=>{self.node(max_nodes)?;self.stack.push(Some(BTreeSet::new()));self.key_due.push(true);self.primitive=false;},
            b'['=>{self.node(max_nodes)?;self.stack.push(None);self.key_due.push(false);self.primitive=false;},
            b'}'|b']'=>{self.stack.pop().ok_or_else(||corrupt("original container closure differs"))?;self.key_due.pop();self.primitive=false;},
            b','=>{if self.stack.last().is_some_and(Option::is_some){*self.key_due.last_mut().unwrap()=true;}self.primitive=false;},
            b':'=>self.primitive=false,
            b'-'|b'0'..=b'9'=>{self.node(max_nodes)?;self.number.push(b);self.primitive=false;},
            _=>{if !self.primitive{self.node(max_nodes)?;self.primitive=true;}},
        }
    }Ok(())}
    fn complete(&mut self)->Result<()>{self.finish_number()?;if self.in_string||self.escape!=0||!self.stack.is_empty(){return Err(corrupt("incomplete original output Value"));}Ok(())}
}

struct ChunkReader<'a, 'db> {
    tx: &'a StoreContext<'db>, request: &'a RequestRecord,
    reference: &'a CharterOutputReference, limits: &'a CharterLimits, task: &'a Task,
    next: u32, body: Vec<u8>, offset: usize, total: u64,
    digest: digest::Context, last_revision: u64, failure: Option<Fault>,
    guard: StreamGuard, value_guard:ValueGuard, maximum_depth: usize, maximum_nodes:usize, pending:Option<(&'a str,&'a CharterOutputChunk)>,
}

impl Read for ChunkReader<'_, '_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() { return Ok(0); }
        if self.failure.is_some() { return Err(io::Error::other("invalid original output chunk")); }
        if self.offset == self.body.len() {
            if self.next == self.reference.chunks { return Ok(0); }
            // Only the final candidate is not committed yet. It is read from
            // this action's inserted row and compared with that exact input;
            // every predecessor still requires its immutable original receipt.
            let result=if let Some((operation_id,candidate))=self.pending.filter(|(_,v)|v.index==self.next) {
                read_chunk(self.tx,self.request.id(),self.next,self.limits).and_then(|v|{
                    if v.operation_id!=operation_id||v.reference!=candidate.reference||hex(&v.body)!=candidate.data{return Err(corrupt("pending output chunk differs from original action"));}
                    Ok((v,self.last_revision.checked_add(1).ok_or_else(||corrupt("output chronology exhausted"))?))
                })
            }else{checked_chunk(self.tx, self.request, self.reference, self.next, self.limits, self.task)};
            let (chunk, revision) = match result {
                Ok(value) => value,
                Err(failure) => { self.failure = Some(failure); return Err(io::Error::other("invalid original output chunk")); }
            };
            if revision <= self.last_revision {
                self.failure = Some(corrupt("output chunks do not follow original commit order"));
                return Err(io::Error::other("invalid original output commit order"));
            }
            if let Err(failure) = self.guard.accept(&chunk.body, self.maximum_depth).and_then(|_|self.value_guard.accept(&chunk.body,self.maximum_nodes)) {
                self.failure = Some(failure);
                return Err(io::Error::other("invalid original output encoding or depth"));
            }
            self.last_revision = revision;
            self.total = match self.total.checked_add(chunk.body.len() as u64) {
                Some(value) if value <= self.reference.bytes => value,
                _ => { self.failure = Some(corrupt("output chunk total exceeds original extent")); return Err(io::Error::other("invalid original output extent")); }
            };
            self.digest.update(&chunk.body);
            self.body = chunk.body;
            self.offset = 0;
            self.next += 1;
        }
        let n = buffer.len().min(self.body.len() - self.offset);
        buffer[..n].copy_from_slice(&self.body[self.offset..self.offset + n]);
        self.offset += n;
        Ok(n)
    }
}

/// Bounded memory: IgnoredAny consumes one JSON value directly from piece rows;
/// it never constructs the original Value or assembles an oversized SQL value.
pub(super) fn verify(tx: &StoreContext<'_>, request: &RequestRecord, reference: &CharterOutputReference, l: &CharterLimits) -> Result<()> {
    verify_inner(tx,request,reference,l,None)
}
fn verify_inner(tx:&StoreContext<'_>,request:&RequestRecord,reference:&CharterOutputReference,l:&CharterLimits,pending:Option<(&str,&CharterOutputChunk)>)->Result<()> {
    let task = accepted(tx, request)?;
    reference_bounds(tx, request, reference, l, &task)?;
    validate_request_receipt(tx, request)?;
    if request_rejection(tx, request, l)?.is_some() { return Err(corrupt("rejected request has staged output")); }
    if count(tx, request.id(), l)? != reference.chunks { return Err(corrupt("original output chunk membership is incomplete")); }
    let mut reader = ChunkReader { tx, request, reference, limits: l, task: &task,
        next: 0, body: Vec::new(), offset: 0, total: 0, digest: digest::Context::new(&digest::SHA256),
        last_revision: receipt(tx, request.id())?.commit.controller_revision, failure: None,
        guard: StreamGuard::default(),value_guard:ValueGuard::default(),maximum_nodes:task.input.execution_limits().map_err(Fault::Store)?.native.json_nodes,
        maximum_depth: task.input.execution_limits().map_err(Fault::Store)?.native.json_depth,pending };
    let parsed = {
        // serde_json's I/O decoder consumes individual bytes. Buffer those
        // reads under the original accepted decode-check allowance so the full
        // row/receipt reader is entered per block, never once per output byte.
        let capacity=task.input.execution_limits().map_err(Fault::Store)?.native.decode_check_bytes
            .min(reference.chunk_bytes as usize);
        let buffered=std::io::BufReader::with_capacity(capacity,&mut reader);
        let mut decoder = serde_json::Deserializer::from_reader(buffered);
        serde::de::IgnoredAny::deserialize(&mut decoder).and_then(|_| decoder.end())
    };
    if let Some(failure) = reader.failure.take() { return Err(failure); }
    if parsed.is_err() { return Err(corrupt("original output chunks are not one complete JSON value")); }
    reader.value_guard.complete()?;
    if !reader.guard.complete() || reader.total != reference.bytes || reader.next != reference.chunks
        || reader.offset != reader.body.len() || hex(reader.digest.finish().as_ref()) != reference.sha256
    {
        return Err(corrupt("original output digest or complete byte extent differs"));
    }
    // The parent may read a just-inserted reply inside its original transaction,
    // before that operation's receipt is inserted. Receipt/seal chronology is a
    // startup/query obligation below, not a prerequisite for this byte proof.
    Ok(())
}

pub(super) fn query(tx: &StoreContext<'_>, request_id: &str, index: u32, l: &CharterLimits) -> Result<CharterOutputChunk> {
    let request = request(tx, request_id, l)?;
    validate_request_receipt(tx, &request)?;
    let (value, revision) = sealed(tx, &request, l)?
        .ok_or_else(|| Fault::Refused("not_found", "original output is not sealed".into()))?;
    let reference = value.observation.output_reference.as_ref()
        .ok_or_else(|| corrupt("sealed output reference absent"))?;
    let task = accepted(tx, &request)?;
    reference_bounds(tx, &request, reference, l, &task)?;
    chunk_length(reference, index)?;
    if count(tx, request_id, l)? != reference.chunks { return Err(corrupt("sealed output membership is incomplete")); }
    if reference.chunks > 1 {
        let (_, last_revision) = checked_chunk(tx, &request, reference, reference.chunks - 1, l, &task)?;
        if last_revision >= revision { return Err(corrupt("output seal preceded its last original chunk")); }
    }
    let (chunk, original_revision) = checked_chunk(tx, &request, reference, index, l, &task)?;
    if original_revision >= revision { return Err(corrupt("output chunk did not precede its sealed reply")); }
    Ok(CharterOutputChunk { reference: reference.clone(), index, data: hex(&chunk.body) })
}

pub(super) fn validate_existing(tx: &StoreContext<'_>, l: &CharterLimits) -> Result<()> {
    // Historical schemas have no chunk table. A parent reference verification
    // still refuses a reference without its rows; migration creates no evidence.
    if !present(tx)? { return Ok(()); }
    let limit = sql_bound(l.records.checked_add(1).ok_or_else(|| corrupt("output inventory bound overflow"))?)?;
    let mut q = tx.prepare(
        "SELECT length(CAST(request_id AS BLOB)),CAST(substr(CAST(request_id AS BLOB),1,?1) AS TEXT),typeof(request_id)\
         FROM charter_output_chunks ORDER BY request_id,chunk_index LIMIT ?2",
    )?;
    let mut rows = q.query(params![sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?, limit])?;
    let mut ids = BTreeSet::new(); let mut n = 0usize;
    while let Some(row) = rows.next()? {
        n += 1;
        if n > l.records { return Err(corrupt("retained output chunks exceed configured inventory")); }
        let id: String = row.get(1)?;
        if row.get::<_,String>(2)? != "text" || unsigned(row.get(0)?)? != id.len() as u64 {
            return Err(corrupt("stored output request identity exceeds bound"));
        }
        identity(tx, &id).map_err(|_| corrupt("stored output request identity differs"))?;
        ids.insert(id);
    }
    drop(rows); drop(q);
    for id in ids {
        let request = request(tx, &id, l)?;
        validate_request_receipt(tx, &request)?;
        let task = accepted(tx, &request)?;
        let reference = read_chunk(tx, &id, 0, l)?.reference;
        reference_bounds(tx, &request, &reference, l, &task)?;
        if request_rejection(tx, &request, l)?.is_some() { return Err(corrupt("rejected request has retained output chunks")); }
        let n = count(tx, &id, l)?;
        if n > reference.chunks { return Err(corrupt("retained output has extra chunks")); }
        if n == reference.chunks {
            verify(tx, &request, &reference, l)?;
            if let Some((value, revision)) = sealed(tx, &request, l)? {
                let (_, last_revision) = checked_chunk(tx, &request, &reference, n - 1, l, &task)?;
                if value.observation.output_reference.as_ref() != Some(&reference) || revision <= last_revision {
                    return Err(corrupt("sealed output does not follow its exact original chunk receipts"));
                }
            }
        }
        else {
            if sealed(tx, &request, l)?.is_some() { return Err(corrupt("partial upload was sealed as a reply")); }
            let mut prior = receipt(tx, &id)?.commit.controller_revision;
            for index in 0..n {
                let (_, revision) = checked_chunk(tx, &request, &reference, index, l, &task)?;
                if revision <= prior { return Err(corrupt("partial output chunks have invalid original commit order")); }
                prior = revision;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_original_membership(tx: &StoreContext<'_>, original: &metadata::Recorded, l: &CharterLimits) -> Result<()> {
    let Operation::ObserveCharter { operation_id, caller, observation: CharterObservation::OutputChunk { chunk } } = &original.request else { return Ok(()) };
    if matches!(&original.outcome, Outcome::Refused { .. }) { return Ok(()); }
    let request = request(tx, &chunk.reference.request_id, l)?;
    validate_request_receipt(tx, &request)?;
    let task = accepted(tx, &request)?;
    reference_bounds(tx, &request, &chunk.reference, l, &task)?;
    let (row, revision) = checked_chunk(tx, &request, &chunk.reference, chunk.index, l, &task)?;
    if operation_id != &original.commit.operation_id || row.operation_id != *operation_id
        || caller != request.caller() || revision != original.commit.controller_revision
        || charter_outcome(original)? != &CharterOutcome::OutputChunk(chunk_receipt(chunk, &row.body)?)
    {
        return Err(corrupt("successful original output chunk receipt lost its exact row"));
    }
    Ok(())
}

/// A committed staged prefix reserves this occurrence's representation. It may
/// only be sealed by that exact complete reference, never an unrelated inline
/// or absent output that would strand committed pieces on restart.
pub(super) fn verify_representation(tx:&StoreContext<'_>,r:&RequestRecord,reference:Option<&CharterOutputReference>,l:&CharterLimits)->Result<()> {
    if let Some(reference)=reference {return verify(tx,r,reference,l)}
    if present(tx)? && count(tx,r.id(),l)?!=0 {return refuse("operation_conflict","staged original output requires its exact complete reference");}
    Ok(())
}
