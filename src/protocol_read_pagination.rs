//! Typed pagination vocabulary only. User strings and opaque JSON are not markers.
use super::*;
pub fn requires_read_pagination(op:&Operation)->bool {
    match op {
        Operation::QueryCollaboration {query:crate::collaboration::Query::Read(_)}=>true,
        Operation::QueryCollaboration {query:crate::collaboration::Query::Repository {request:crate::repository_view::Request::ReadPage {..},..}}=>true,
        Operation::CallCollaboration {call}=>call_requires_read_pagination(call),
        _=>false,
    }
}
pub fn call_requires_read_pagination(call:&crate::mcp::Invocation)->bool {
    if call.tool=="collab_read_continue" {return true;}
    if crate::read_page::ReadTool::parse(&call.tool).is_none() {return false;}
    call.arguments.get("max_bytes").is_some()
        || (matches!(call.tool.as_str(),"collab_task_get"|"collab_document_get"|"collab_repository_read")
            && call.arguments.get("cursor").is_some())
        || (matches!(call.tool.as_str(),"collab_task_get"|"collab_document_get")
            && call.arguments.get("part").is_some())
        || (call.tool=="collab_task_get" && call.arguments.get("session_id").is_some())
}
pub fn outcome_requires_read_pagination(outcome:&Outcome)->bool {
    let Outcome::Ok {data}=outcome else{return false;};
    data_requires_read_pagination(data)
}
fn repository(value:&crate::repository_view::Output)->bool {
    matches!(value.data,crate::repository_view::Data::ReadPage(_)|crate::repository_view::Data::FileSlice {..})
}
fn data_requires_read_pagination(data:&Data)->bool {
    use crate::collaboration::QueryResult;
    match data {
        Data::ReadPage(_)|Data::ReadList(_)=>true,
        Data::Repository(value)=>repository(value),
        Data::Collaboration(QueryResult::Read(_))=>true,
        Data::Collaboration(QueryResult::Repository(value))=>repository(value),
        Data::Collaboration(QueryResult::Call(record))=>call_requires_read_pagination(&record.invocation)
            || outcome_requires_read_pagination(&record.result.reply.outcome),
        Data::Operation(record) if record.kind=="application_mutation"=>
            serde_json::from_value::<Operation>(record.intended.clone()).is_ok_and(|o|requires_read_pagination(&o))
            || record.outcome.as_ref().is_some_and(|v|serde_json::from_value::<Outcome>(v.clone()).is_ok_and(|o|outcome_requires_read_pagination(&o))),
        _=>false,
    }
}
