use super::*;
fn capture()->Capture{
    let source=ResolvedCharter{reference:CharterSourceRef{definition_id:"agent".into(),definition_revision:"v1".into()},source:CharterSource{files:vec![],clauses:vec![]}};
    Capture{boundary:CharterSuppliedBoundary{turn_id:"original".into(),delivery_id:"initial".into(),session_id:"session".into(),
        caller:crate::mcp::Caller{task_id:"task".into(),attempt_id:"attempt".into(),generation:1,worker_id:"worker".into(),incarnation:"one".into()},outer_index:1,effective:source,request_id:10,thread_id:"thread".into(),output_schema:supplied_result_schema()},
        turn_id:"public-turn".into(),items:vec![],frame_bytes:1024}
}
fn item(id:&str,phase:Option<&str>,text:Value)->Value{
    let mut item=json!({"type":"agentMessage","id":id,"text":serde_json::to_string(&text).unwrap()});
    if let Some(phase)=phase{item["phase"]=json!(phase);}item
}
fn terminal(items:Vec<Value>)->Value{json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"public-turn","status":"completed","items":items}}})}
#[test]
fn supplied_projection_retains_genuine_final_declaration_and_separate_result(){
    let c=capture();let declaration=json!({"revision":{"definition_id":"agent","definition_revision":"v1"},"clauses":[]});
    let t=terminal(vec![item("comment",Some("commentary"),json!({"charter_references":"not-final"})),item("final",Some("final_answer"),json!({"result":"original text","charter_references":declaration}))]);
    let (actual,ids,result,conflicts)=c.projection(&t);assert!(conflicts.is_empty());assert_eq!(actual,Some(declaration));assert_eq!(result.as_deref(),Some("original text"));assert_eq!(ids,vec!["comment","final"]);
    assert_eq!(t["params"]["turn"]["items"][0]["phase"],"commentary");
}
#[test]
fn supplied_projection_unknown_missing_null_malformed_and_multiple_are_not_inferred(){
    let c=capture();
    for phase in [None,Some("commentary"),Some("partial_answer")]{let t=terminal(vec![item("item",phase,json!({"result":"x","charter_references":{"genuine":"but unknown scope"}}))]);assert_eq!(c.projection(&t).0,None);}
    assert_eq!(c.projection(&terminal(vec![item("item",Some("final_answer"),json!({"result":"x"}))])).0,None);
    for value in [Value::Null,json!("malformed"),json!({"wrong":"shape"})]{assert_eq!(c.projection(&terminal(vec![item("item",Some("final_answer"),json!({"result":"x","charter_references":value}))])).0,Some(value));}
    let t=terminal(vec![item("a",Some("final_answer"),json!({"result":"a","charter_references":"first"})),item("b",Some("final_answer"),json!({"result":"b","charter_references":"second"}))]);
    let (refs,_,result,_)=c.projection(&t);assert_eq!(refs,Some(json!(["first","second"])));assert!(result.is_none());
    let mut unknown=t;unknown["params"]["turn"]["itemsView"]=json!("summary");assert_eq!(c.projection(&unknown).0,None);
}
#[test]
fn supplied_new_contract_leaves_historical_native_and_initial_observation_bytes_unchanged(){
    let native=json!({"kind":"reply","turn_id":"turn","reply":{"output":null,"response_id":null,"output_item_ids":[],"tool_call_ids":[],"declaration":null}});
    let value:CharterObservation=serde_json::from_value(native.clone()).unwrap();assert_eq!(serde_json::to_value(value).unwrap(),native);
    let initial=json!({"kind":"ack_supplied_context","delivery_id":"initial","request_id":2,"response":{"thread":{"id":"original"}}});
    let value:CharterObservation=serde_json::from_value(initial.clone()).unwrap();assert_eq!(serde_json::to_value(value).unwrap(),initial);
    assert_eq!(supplied_output_bytes(usize::MAX,1).unwrap(),isize::MAX as usize);
}

// The installed maximum metadata must admit actual small public frames while
// retaining the same item-count and serialized-size checks as ordinary caps.
#[test]
fn maximum_frame_policy_keeps_small_public_items_and_terminals_bounded(){
    let limits:crate::limits::RuntimeLimits=serde_json::from_str(include_str!("../../tests/fixtures/maximum-session-controller-metadata-04.json")).unwrap();
    assert!(limits.execution.coding.frame_bytes.checked_mul(25).is_none());
    let (_cancel,cancelled)=tokio::sync::watch::channel(false);
    let mut control=crate::worker::test_control(u64::MAX,cancelled);
    control.runtime_limits=std::sync::Arc::new(limits);
    let mut c=capture();c.frame_bytes=control.runtime_limits.execution.coding.frame_bytes;
    let final_item=item("maximum-small",Some("final_answer"),json!({"result":"kept","charter_references":null}));
    let message=json!({"method":"item/completed","params":{"threadId":"thread","turnId":"public-turn","item":final_item}});
    c.item(&message,&control).unwrap();
    c.item(&message,&control).unwrap();
    assert_eq!(c.items,vec![message]);
    let terminal=terminal(vec![final_item]);
    bounded_json_size(&terminal,supplied_frame_bytes(c.frame_bytes)).unwrap();
    let maximum=supplied_output_bytes(c.frame_bytes,control.runtime_limits.charter.as_ref().unwrap().reply_items).unwrap();
    assert_eq!(maximum,isize::MAX as usize);
    bounded_json_size(&json!({"items":c.items,"terminal":terminal}),maximum).unwrap();
    assert_eq!(c.projection(&terminal).2.as_deref(),Some("kept"));
}

#[test]
fn ordinary_public_frame_and_item_count_limits_still_refuse(){
    let limits:crate::limits::RuntimeLimits=serde_json::from_str(include_str!("../../tests/fixtures/maximum-session-controller-metadata-04.json")).unwrap();
    let (_cancel,cancelled)=tokio::sync::watch::channel(false);
    let mut control=crate::worker::test_control(u64::MAX,cancelled);
    control.runtime_limits=std::sync::Arc::new(limits);
    std::sync::Arc::make_mut(&mut control.runtime_limits).charter.as_mut().unwrap().reply_items=1;
    let message=json!({"method":"item/completed","params":{"threadId":"thread","turnId":"public-turn","item":item("first",Some("final_answer"),json!({"result":"kept"}))}});
    let mut c=capture();c.frame_bytes=1;
    assert!(c.item(&message,&control).is_err());assert!(c.items.is_empty());
    assert!(bounded_json_size(&terminal(vec![]),supplied_frame_bytes(c.frame_bytes)).is_err());
    c.frame_bytes=1024;c.item(&message,&control).unwrap();
    let mut another=message.clone();another["params"]["item"]["id"]=json!("second");
    assert_eq!(c.item(&another,&control).unwrap_err(),"supplied public items exceed accepted charter.reply_items");
    assert_eq!(c.items,vec![message]);
    assert_eq!(supplied_frame_bytes(1024),25_600);
    assert_eq!(supplied_output_bytes(1024,1).unwrap(),51_266);
}

#[test]
fn supplied_completed_final_survives_empty_unloaded_summary_views_and_conflicts_stay_explicit(){
    let mut c=capture();let genuine=item("original",Some("final_answer"),json!({"result":"kept final","charter_references":null}));
    c.items.push(json!({"method":"item/completed","params":{"threadId":"thread","turnId":"public-turn","item":genuine}}));
    for view in [None,Some("full"),Some("notLoaded"),Some("summary")] {
        let mut t=terminal(vec![]);if let Some(view)=view{t["params"]["turn"]["itemsView"]=json!(view);}
        let (refs,ids,result,conflicts)=c.projection(&t);assert_eq!(refs,Some(Value::Null));assert_eq!(ids,vec!["original"]);
        assert_eq!(result.as_deref(),Some("kept final"));assert!(conflicts.is_empty());
    }
    let same=terminal(vec![genuine.clone()]);assert!(c.projection(&same).3.is_empty());assert_eq!(c.projection(&same).1.len(),1);
    let changed=terminal(vec![item("original",Some("final_answer"),json!({"result":"changed","charter_references":"different"}))]);
    let (_,ids,result,conflicts)=c.projection(&changed);assert_eq!(ids,vec!["original"]);assert_eq!(conflicts,vec!["original"]);assert!(result.is_none());
    assert_eq!(c.items[0]["params"]["item"],genuine);
}
