//! Source-level unit discriminators. Controller ingress/replay uses the separate
//! real Controller integration target; these do not simulate provider success.
use super::*;
use rusqlite::Connection;
fn limits()->crate::limits::RuntimeLimits {
    let mut l=crate::limits::RuntimeLimits::seed().unwrap();
    l.charter=Some(CharterLimits{record_bytes:65_536,source_bytes:16_384,files:4,clauses:32,
        metadata_bytes:8192,references:32,reply_items:32,tool_calls:8,reference_depth:4,
        records:1024,page_items:4,page_bytes:65_536});l
}
fn source()->CharterSource {
    CharterSource{files:vec![CharterFile{file:"charter.md".into(),text:"# Purpose\n\nThe agent MUST preserve exact accepted identity.\n".into()}],clauses:vec![CharterClauseRef{file:"charter.md".into(),section:"Purpose".into(),sentence:"The agent MUST preserve exact accepted identity.".into()}]}
}
fn resolved()->ResolvedCharter {ResolvedCharter{reference:CharterSourceRef{definition_id:"agent".into(),definition_revision:"one".into()},source:source()}}
#[test]
fn charter_exact_source_index_refuses_missing_changed_ambiguous_and_overbound_source() {
    let c=Connection::open_in_memory().unwrap();let l=limits();let tx=StoreContext::new(&c,&l);let p=l.charter.as_ref().unwrap();
    validate_source(&tx,&source(),p).unwrap();
    for defect in 0..6 {
        let mut s=source();match defect {
            0=>s.clauses[0].sentence.push('!'),
            1=>s.files[0].text.push_str("\nAnother requirement MUST remain indexed.\n"),
            2=>s.files[0].text.push_str("\n# Purpose\n\nDuplicate heading.\n"),
            3=>s.clauses.push(s.clauses[0].clone()),
            4=>s.files[0].text="x".repeat(p.source_bytes+1),
            _=>s.files[0].text.push_str("\n```text\nHidden MUST not become inferred source.\n```\n"),
        }assert!(validate_source(&tx,&s,p).is_err(),"defect {defect}");
    }
}
#[test]
fn charter_reference_audit_preserves_absence_and_invalid_declarations_without_inference() {
    let s=resolved();
    assert_eq!(audit(&s,&None).findings,vec![CharterAuditFinding::MissingMetadata]);
    let wrong=serde_json::json!("the model supplied a string");
    assert_eq!(audit(&s,&Some(wrong.clone())).findings,vec![CharterAuditFinding::MalformedMetadata]);
    let mut declaration=DeclaredCharterReferences{revision:s.reference.clone(),clauses:s.source.clauses.clone()};
    assert!(audit(&s,&Some(serde_json::to_value(&declaration).unwrap())).findings.is_empty());
    declaration.revision.definition_revision="other".into();declaration.clauses.push(declaration.clauses[0].clone());declaration.clauses[0].sentence.push('!');
    assert_eq!(audit(&s,&Some(serde_json::to_value(&declaration).unwrap())).findings,vec![CharterAuditFinding::RevisionMismatch,CharterAuditFinding::UnknownClause{index:0}]);
    declaration.clauses=vec![s.source.clauses[0].clone();2];
    assert!(audit(&s,&Some(serde_json::to_value(declaration).unwrap())).findings.contains(&CharterAuditFinding::DuplicateClause{index:1}));
    assert!(audit(&s,&Some(serde_json::json!({"revision":s.reference,"clauses":[]}))).findings.contains(&CharterAuditFinding::EmptyApplicabilityUnspecified));
    let p=limits();let c=Connection::open_in_memory().unwrap();let tx=StoreContext::new(&c,&p);
    let reply=CharterReplyObservation{output_reference:None,output:Some(serde_json::json!([{"id":"actual-item","type":"message","content":[{"type":"output_text","text":"original controlled output"}]}])),response_id:None,output_item_ids:vec![],tool_call_ids:vec![],declaration:Some(wrong)};
    observation(&tx,&reply,p.charter.as_ref().unwrap()).unwrap(); // invalid references are admitted as bounded audit data
    let mut missing=reply.clone();missing.output=None;observation(&tx,&missing,p.charter.as_ref().unwrap()).unwrap();
    let mut explicit_null=missing.clone();explicit_null.output=Some(serde_json::Value::Null);
    assert_ne!(serde_json::to_value(&missing).unwrap(),serde_json::to_value(&explicit_null).unwrap());
    explicit_null.declaration=Some(serde_json::Value::Null);
    let decoded:CharterReplyObservation=serde_json::from_value(serde_json::to_value(&explicit_null).unwrap()).unwrap();assert_eq!(decoded,explicit_null);
    assert_eq!(audit(&resolved(),&decoded.declaration).findings,vec![CharterAuditFinding::MalformedMetadata]);
    assert!(reply.response_id.is_none());assert_eq!(reply.declaration,Some(serde_json::json!("the model supplied a string")));
}
#[test]
fn charter_original_optional_definition_bytes_and_closed_metadata_shapes() {
    let raw=r#"{"id":"legacy","revision":"one","executor":"native"}"#;
    let d:Definition=serde_json::from_str(raw).unwrap();assert!(d.charter.is_none());assert_eq!(serde_json::to_string(&d).unwrap(),raw);
    let value=serde_json::to_value(&d).unwrap();assert!(value.get("charter").is_none());
    let value=serde_json::to_value(&d).unwrap();let round:Definition=serde_json::from_value(value.clone()).unwrap();assert_eq!(serde_json::to_value(round).unwrap(),value);
    assert!(serde_json::from_value::<DeclaredCharterReferences>(serde_json::json!({"revision":resolved().reference,"clauses":[],"force_reset":true})).is_err());
}
#[test]
fn charter_bounded_materialization_refuses_prefix_oversize_before_json_decoding() {
    let c=Connection::open_in_memory().unwrap();c.execute_batch(include_str!("charters.sql")).unwrap();
    let p=limits();let tx=StoreContext::new(&c,&p);let maximum=p.charter.as_ref().unwrap().record_bytes;
    c.execute("INSERT INTO charter_replies(id,request_id,body) VALUES('reply','request',?1)",[format!("{}\\0{}","x".repeat(maximum),"y".repeat(maximum))]).unwrap();
    let raw=raw_body(&tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE request_id=?1","request",maximum).unwrap().unwrap();
    assert_eq!(raw.len(),maximum+1);assert!(matches!(stored::<CharterReplyRecord>(&raw,maximum),Err(Fault::Store(message)) if message.contains("exceeds bound")));
    assert_eq!(c.query_row("SELECT length(CAST(body AS BLOB)) FROM charter_replies",[],|r|r.get::<_,i64>(0)).unwrap(),i64::try_from(maximum*2+2).unwrap());
}
#[test]
fn charter_rejection_record_is_distinct_and_preserves_existing_summary_bytes() {
    let old=CompactionResult{summary:Some("actual accepted summary".into()),disposition:CompactionDisposition::CurrentSourceSummary};
    let raw=encode(&old).unwrap();let decoded:CompactionResultRecord=decode(&raw).unwrap();assert_eq!(encode(&decoded).unwrap(),raw);
    assert!(matches!(decoded,CompactionResultRecord::Summary(_)));
    let rejected=CompactionResultRecord::Rejected(CompactionRejection{operation_id:"exact-reconciliation".into(),report_id:"exact-original-rejection".into()});
    let value=serde_json::to_value(&rejected).unwrap();assert!(value.get("summary").is_none());assert!(value.get("disposition").is_none());
    assert_eq!(decode::<CompactionResultRecord>(&encode(&rejected).unwrap()).unwrap(),rejected);
    let mixed=serde_json::json!({"summary":null,"disposition":"no_accepted_summary","operation_id":"mixed","report_id":"mixed"});
    assert!(serde_json::from_value::<CompactionResultRecord>(mixed).is_err());
}
