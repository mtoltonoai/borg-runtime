//! Actual Controller receipts and SQLite reopen. The output is controlled fixture
//! data; this does not claim a provider response or a multi-gigabyte load run.
use super::*;

fn output_config() -> ControllerConfig {
    let mut cfg = charter_config();
    // Each encoded piece remains below the existing 1 MiB transport/receipt
    // limits. The original provider carrier and task final limit are unchanged.
    Arc::make_mut(&mut cfg.limits).charter.as_mut().unwrap().record_bytes = 512 * 1024;
    cfg
}

fn output_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}

fn output_unhex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value.as_bytes().chunks_exact(2).map(|p| {
        let nibble = |b: u8| match b {
            b'0'..=b'9' => b - b'0', b'a'..=b'f' => b - b'a' + 10,
            _ => panic!("noncanonical chunk hex"),
        };
        (nibble(p[0]) << 4) | nibble(p[1])
    }).collect()
}

fn output_sha(bytes: &[u8]) -> String {
    output_hex(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}

fn output_reference(id: &str, bytes: &[u8], size: u32) -> CharterOutputReference {
    CharterOutputReference { request_id: id.into(), bytes: bytes.len() as u64,
        sha256: output_sha(bytes), chunk_bytes: size,
        chunks: u32::try_from(bytes.len().div_ceil(size as usize)).unwrap() }
}

fn piece(reference: &CharterOutputReference, bytes: &[u8], index: u32) -> CharterOutputChunk {
    let start = index as usize * reference.chunk_bytes as usize;
    let end = bytes.len().min(start + reference.chunk_bytes as usize);
    CharterOutputChunk { reference: reference.clone(), index, data: output_hex(&bytes[start..end]) }
}

fn piece_operation(who: &Caller, label: &str, chunk: CharterOutputChunk) -> Operation {
    Operation::ObserveCharter { operation_id: format!("{label}-{}", chunk.index),
        caller: who.clone(), observation: CharterObservation::OutputChunk { chunk } }
}

async fn upload_from(c: &Controller, who: &Caller, label: &str, reference: &CharterOutputReference, bytes: &[u8], start: u32) {
    for index in start..reference.chunks {
        let chunk = piece(reference, bytes, index);
        let actual = exchange(c, piece_operation(who, label, chunk.clone())).await;
        let CharterOutcome::OutputChunk(saved) = outcome(&actual) else { panic!("not a chunk receipt") };
        let decoded = output_unhex(&chunk.data);
        assert_eq!(saved, CharterOutputChunkReceipt { reference: reference.clone(), index,
            sha256: output_sha(&decoded), bytes: decoded.len() as u32 });
        assert!(actual.commit.is_some());
    }
}

fn referenced(reference: &CharterOutputReference) -> CharterReplyObservation {
    let mut value = reply(None, &[]);
    value.output = None;
    value.output_reference = Some(reference.clone());
    value
}

async fn query_bytes(c: &Controller, reference: &CharterOutputReference) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..reference.chunks {
        let CharterQueryResult::OutputChunk(chunk) = query(c, CharterQuery::OutputChunk {
            request_id: reference.request_id.clone(), index,
        }).await else { panic!("not an output chunk") };
        assert_eq!(chunk.reference, *reference); assert_eq!(chunk.index, index);
        let raw = output_unhex(&chunk.data);
        if index + 1 < reference.chunks { assert_eq!(raw.len(), reference.chunk_bytes as usize); }
        bytes.extend_from_slice(&raw);
    }
    assert_eq!(bytes.len() as u64, reference.bytes);
    assert_eq!(output_sha(&bytes), reference.sha256);
    bytes
}

#[tokio::test]
async fn charter_original_output_above_eight_mib_retains_exact_chunks_and_small_original_receipt() {
    let p = path(); let cfg = output_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (t, a, who) = setup(&c, "large-original-output").await;
    let boundary = begin(&c, &a, &who, 1, "large-output-turn").await;
    // Includes multibyte UTF-8 and escaping; piece boundaries are raw byte
    // boundaries and need not coincide with Unicode scalars or JSON tokens.
    let output = json!([{"id":"actual-item","type":"message","content":[{
        "type":"output_text","text":"𝄞\\\"\n".repeat(900_000)
    }]}]);
    let bytes = serde_json::to_vec(&output).unwrap();
    assert!(bytes.len() > 8 * 1024 * 1024);
    assert!(bytes.len() > t.input.limits.output_bytes);
    assert!(bytes.len() > cfg.limits.charter.as_ref().unwrap().record_bytes);
    let reference = output_reference(&boundary.turn_id, &bytes, 128 * 1024);
    upload_from(&c, &who, "large-piece", &reference, &bytes, 0).await;
    let original_piece = piece_operation(&who, "large-piece", piece(&reference, &bytes, 0));
    let first = exchange(&c, original_piece.clone()).await;
    let repeated = exchange(&c, original_piece.clone()).await;
    assert_eq!(first.outcome, repeated.outcome); assert_eq!(first.commit, repeated.commit);
    assert_eq!(code(call(&c, Operation::QueryCharter { query: CharterQuery::OutputChunk {
        request_id: boundary.turn_id.clone(), index: 0,
    }}).await), "not_found"); // Staged pieces are not a reply.
    let observation = referenced(&reference);
    let original = Operation::ObserveCharter { operation_id: "large-output-seal".into(), caller: who.clone(),
        observation: CharterObservation::Reply { turn_id: boundary.turn_id.clone(), reply: observation.clone() } };
    let sealed = exchange(&c, original.clone()).await;
    let CharterOutcome::Reply(saved) = outcome(&sealed) else { panic!() };
    assert_eq!(saved.observation, observation);
    assert_eq!(saved.audit.findings, vec![CharterAuditFinding::MissingMetadata]);
    assert!(saved.observation.output.is_none());
    assert!(serde_json::to_vec(&sealed).unwrap().len() < 8192);
    let lookup = exchange(&c, Operation::GetOperation { operation_id: "large-output-seal".into() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = &lookup.outcome else { panic!() };
    assert_eq!(record.intended, serde_json::to_value(&original).unwrap());
    assert_eq!(record.outcome.as_ref(), Some(&serde_json::to_value(&sealed.outcome).unwrap()));
    assert_eq!(lookup.commit, sealed.commit);
    assert!(serde_json::to_vec(&lookup).unwrap().len() < 16_384);
    assert_eq!(query_bytes(&c, &reference).await, bytes);
    assert_eq!(get(&c, &t.id).await.input, t.input);
    drop(c); controller_owner_released(&p).await;

    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = exchange(&c, original).await;
    assert_eq!(replay.outcome, sealed.outcome); assert_eq!(replay.commit, sealed.commit);
    let replay_piece = exchange(&c, original_piece).await;
    assert_eq!(replay_piece.outcome, first.outcome); assert_eq!(replay_piece.commit, first.commit);
    let reconstructed = query_bytes(&c, &reference).await;
    assert_eq!(reconstructed, bytes);
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&reconstructed).unwrap(), output);
    assert_eq!(get(&c, &t.id).await.input, t.input);
}

#[tokio::test]
async fn charter_output_prefix_reopens_but_wrong_caller_order_reference_and_duplicate_identity_do_not_replace_it() {
    let p = path(); let cfg = output_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (_, a, who) = setup(&c, "piece-identities").await;
    let boundary = begin(&c, &a, &who, 1, "piece-identities-turn").await;
    let bytes = serde_json::to_vec(&json!([{"text":"λ original retained text"}])).unwrap();
    let reference = output_reference(&boundary.turn_id, &bytes, 7);
    assert!(reference.chunks > 2);
    let first_chunk = piece(&reference, &bytes, 0);
    assert_eq!(code(exchange(&c, piece_operation(&who, "out-of-order", piece(&reference, &bytes, 1))).await.outcome), "operation_conflict");
    let mut foreign = who.clone(); foreign.generation += 1;
    assert_eq!(code(exchange(&c, piece_operation(&foreign, "foreign", first_chunk.clone())).await.outcome), "caller_mismatch");
    let mut malformed = first_chunk.clone(); malformed.data.push('0');
    assert_eq!(code(exchange(&c, piece_operation(&who, "malformed-hex", malformed)).await.outcome), "invalid_input");
    let original = piece_operation(&who, "original-piece", first_chunk.clone());
    let committed = exchange(&c, original.clone()).await; outcome(&committed);
    assert_eq!(code(exchange(&c, piece_operation(&who, "other-operation", first_chunk)).await.outcome), "operation_conflict");
    let mut different = piece(&reference, &bytes, 1); different.reference.sha256 = "0".repeat(64);
    assert_eq!(code(exchange(&c, piece_operation(&who, "different-reference", different)).await.outcome), "operation_conflict");
    for (suffix,output) in [("inline",Some(json!(["different"]))),("absent",None)] {
        let mut unrelated=reply(None,&[]);unrelated.output=output;
        assert_eq!(code(observe(&c,&who,&format!("prefix-{suffix}"),CharterObservation::Reply{turn_id:boundary.turn_id.clone(),reply:unrelated}).await.outcome),"operation_conflict");
    }
    assert!(turn(&c, &boundary.turn_id).await.reply.is_none());
    drop(c); controller_owner_released(&p).await;

    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(exchange(&c, original.clone()).await.commit, committed.commit);
    assert_eq!(code(call(&c, Operation::QueryCharter { query: CharterQuery::OutputChunk {
        request_id: boundary.turn_id.clone(), index: 0,
    }}).await), "not_found");
    upload_from(&c, &who, "original-piece", &reference, &bytes, 1).await;
    for (suffix,output) in [("inline",Some(json!(["different"]))),("absent",None)] {
        let mut unrelated=reply(None,&[]);unrelated.output=output;
        assert_eq!(code(observe(&c,&who,&format!("complete-{suffix}"),CharterObservation::Reply{turn_id:boundary.turn_id.clone(),reply:unrelated}).await.outcome),"operation_conflict");
    }
    let sealed = observe(&c, &who, "identities-seal", CharterObservation::Reply {
        turn_id: boundary.turn_id.clone(), reply: referenced(&reference),
    }).await; outcome(&sealed);
    assert_eq!(query_bytes(&c, &reference).await, bytes);
    assert_eq!(exchange(&c, original).await.commit, committed.commit);
    assert_eq!(code(exchange(&c, piece_operation(&who, "post-seal", piece(&reference, &bytes, 0))).await.outcome), "operation_conflict");
}

#[tokio::test]
async fn charter_output_missing_tampered_rebound_or_deleted_membership_refuses_reopen_without_repair() {
    for defect in 0..4 {
        let p = path(); let cfg = output_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (_, a, who) = setup(&c, "output-corruption").await;
        let boundary = begin(&c, &a, &who, 1, "output-corruption-turn").await;
        let bytes = serde_json::to_vec(&json!([{"original":"unchanged"}])).unwrap();
        let reference = output_reference(&boundary.turn_id, &bytes, 5);
        upload_from(&c, &who, "corruption-piece", &reference, &bytes, 0).await;
        let seal = Operation::ObserveCharter { operation_id: "corruption-seal".into(), caller: who.clone(),
            observation: CharterObservation::Reply { turn_id: boundary.turn_id.clone(), reply: referenced(&reference) } };
        let original = exchange(&c, seal.clone()).await; outcome(&original);
        drop(c); controller_owner_released(&p).await;
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        assert_eq!(exchange(&c, seal).await.commit, original.commit);
        assert_eq!(query_bytes(&c, &reference).await, bytes);
        drop(c); controller_owner_released(&p).await;

        let sql = rusqlite::Connection::open(&p).unwrap();
        match defect {
            0 => { assert_eq!(sql.execute("DELETE FROM charter_output_chunks WHERE request_id=?1 AND chunk_index=1", [&boundary.turn_id]).unwrap(), 1); },
            1 => { assert_eq!(sql.execute("UPDATE charter_output_chunks SET body=?1 WHERE request_id=?2 AND chunk_index=1", rusqlite::params![vec![b'x'; 5], boundary.turn_id]).unwrap(), 1); },
            2 => {
                let mut changed = reference.clone(); changed.sha256 = "f".repeat(64);
                assert_eq!(sql.execute("UPDATE charter_output_chunks SET reference=?1 WHERE request_id=?2 AND chunk_index=1",
                    rusqlite::params![serde_json::to_string(&changed).unwrap(), boundary.turn_id]).unwrap(), 1);
            },
            _ => {
                // Even erasing all surviving rows and the seal cannot erase the
                // successful original chunk/reply receipts' reverse membership.
                assert_eq!(sql.execute("DELETE FROM charter_output_chunks WHERE request_id=?1", [&boundary.turn_id]).unwrap(), reference.chunks as usize);
                assert_eq!(sql.execute("DELETE FROM charter_replies WHERE request_id=?1", [&boundary.turn_id]).unwrap(), 1);
            },
        }
        drop(sql); let before = stored_bytes(&p);
        assert!(Controller::open_with_config(&p, cfg).is_err(), "defect {defect}");
        assert_eq!(stored_bytes(&p), before, "startup rewrote corrupt original output {defect}");
    }
}

#[tokio::test]
async fn charter_output_compaction_seals_in_original_transaction_without_turn_or_tool_effects() {
    let p = path(); let cfg = output_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (t, _, who, run) = begun_compaction(&c, "chunked-compaction").await;
    let bytes = serde_json::to_vec(&json!([{"summary":"exact original λ"}])).unwrap();
    let reference = output_reference(&run.run_id, &bytes, 3);
    upload_from(&c, &who, "compaction-piece", &reference, &bytes, 0).await;
    let original = Operation::ObserveCharter { operation_id: "compaction-piece-seal".into(), caller: who.clone(),
        observation: CharterObservation::CompactionReply { run_id: run.run_id.clone(), reply: referenced(&reference), summary: Some("exact original λ".into()) } };
    let committed = exchange(&c, original.clone()).await;
    let CharterOutcome::Compaction(record) = outcome(&committed) else { panic!() };
    assert_eq!(record.run_id, run.run_id);
    assert_eq!(record.reply.as_ref().unwrap().observation.output_reference.as_ref(), Some(&reference));
    assert_eq!(record.disposition, Some(CompactionDisposition::CurrentSourceSummary));
    assert_eq!(record.summary.as_deref(), Some("exact original λ"));
    assert!(record.reply.as_ref().unwrap().observation.tool_call_ids.is_empty());
    assert!(session(&c, &t.session_id).await.session.compaction_in_flight.is_none());
    assert_eq!(query_bytes(&c, &reference).await, bytes);
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = exchange(&c, original).await;
    assert_eq!(replay.outcome, committed.outcome); assert_eq!(replay.commit, committed.commit);
    assert_eq!(query_bytes(&c, &reference).await, bytes);
}

#[tokio::test]
async fn charter_output_invalid_complete_json_digest_and_utf8_never_seal_as_original_reply() {
    for (label, bytes, wrong_digest) in [
        ("partial-json", br#"[{"unfinished":"#.to_vec(), false),
        ("two-values", b"null null".to_vec(), false),
        ("bad-utf8", vec![b'"', 0xff, b'"'], false),
        ("wrong-total-digest", b"null".to_vec(), true),
        ("nonfinite", b"1e999".to_vec(),false),
        ("duplicate-key", br#"{"a":1,"a":2}"#.to_vec(),false),
        ("high-surrogate", br#""\uD800""#.to_vec(),false),
        ("low-surrogate", br#""\uDC00""#.to_vec(),false),
    ] {
        let p=path();let cfg=output_config();let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (_, a, who) = setup(&c, label).await;
        let boundary = begin(&c, &a, &who, 1, &format!("{label}-turn")).await;
        let mut reference = output_reference(&boundary.turn_id, &bytes, 2);
        if wrong_digest { reference.sha256 = "0".repeat(64); }
        for index in 0..reference.chunks-1{outcome(&exchange(&c,piece_operation(&who,label,piece(&reference,&bytes,index))).await);}
        let final_operation=piece_operation(&who,label,piece(&reference,&bytes,reference.chunks-1));
        let failed=exchange(&c,final_operation.clone()).await;assert_eq!(code(failed.outcome.clone()),"invalid_input");
        assert_eq!(exchange(&c,final_operation.clone()).await.commit,failed.commit);
        assert!(turn(&c, &boundary.turn_id).await.reply.is_none());
        drop(c);controller_owner_released(&p).await;
        let c=Controller::open_with_config(&p,cfg).unwrap();
        assert_eq!(exchange(&c,final_operation).await.outcome,failed.outcome);
        assert!(turn(&c,&boundary.turn_id).await.reply.is_none());
        assert_eq!(code(call(&c, Operation::QueryCharter { query: CharterQuery::OutputChunk {
            request_id: boundary.turn_id.clone(), index: 0,
        }}).await), "not_found");
    }
}

#[tokio::test]
async fn canonical_output_expansion_uses_accepted_extent_and_replays_after_reopen() {
    let p=path();let mut cfg=output_config();
    let l=Arc::make_mut(&mut cfg.limits);let n=&mut l.execution.native;
    n.response_wire_bytes=65_536;n.helper_value_bytes=65_536;n.context_bytes=65_536;n.output_bytes=16_384;n.json_nodes=4096;
    l.validate().unwrap();
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,a,who)=setup(&c,"canonical-expansion").await;
    let b=begin(&c,&a,&who,1,"canonical-expansion-turn").await;
    let template=include_str!("../../src/worker/testdata/canonical-output-expansion.json");
    let wire=template.replace("__PADDING__",&" ".repeat(65_536-template.len()+11));
    let raw:serde_json::Value=serde_json::from_str(&wire).unwrap();let bytes=serde_json::to_vec(&raw["output"]).unwrap();
    assert_eq!(wire.len(),65_536);assert!(bytes.len()>wire.len());
    let reference=output_reference(&b.turn_id,&bytes,4096);
    assert!(reference.validate(65_536,4096).is_err());
    let mut beyond=reference.clone();beyond.bytes=(65_536+24*4096+1)as u64;beyond.chunks=u32::try_from(beyond.bytes.div_ceil(4096)).unwrap();
    let over=CharterOutputChunk{reference:beyond,index:0,data:output_hex(&bytes[..4096])};
    assert_eq!(code(exchange(&c,piece_operation(&who,"canonical-over",over)).await.outcome),"input_bound");
    assert!(turn(&c,&b.turn_id).await.reply.is_none());
    upload_from(&c,&who,"canonical-piece",&reference,&bytes,0).await;
    let original=Operation::ObserveCharter{operation_id:"canonical-seal".into(),caller:who.clone(),observation:CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:referenced(&reference)}};
    let sealed=exchange(&c,original.clone()).await;outcome(&sealed);assert_eq!(query_bytes(&c,&reference).await,bytes);
    assert_eq!(get(&c,&t.id).await.input,t.input);
    drop(c);controller_owner_released(&p).await;
    // Current native settings do not replace the accepted task's B or N.
    let now=Arc::make_mut(&mut cfg.limits);now.revision.push_str("-current");now.execution.native.json_nodes=8;
    let c=Controller::open_with_config(&p,cfg).unwrap();
    let replay=exchange(&c,original).await;assert_eq!(replay.outcome,sealed.outcome);assert_eq!(replay.commit,sealed.commit);
    assert_eq!(query_bytes(&c,&reference).await,bytes);assert_eq!(get(&c,&t.id).await.input,t.input);
}
