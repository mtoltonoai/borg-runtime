//! Original successful read observations, distinct from an absent call or an
//! ordinary Outcome. Digests describe removed bytes; they do not recreate them.
use crate::{mcp::{CallResult, Caller, Invocation}, protocol::*};
use serde::{Deserialize, Serialize};

pub const UNAVAILABLE: &str = "original_read_body_unavailable";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadKind { Task, Memory, Agent, List, Repository, Collaboration, Document, Question, Review, Work, Page }

// A closed tool classification AND a typed canonical result are required.
// In particular changed=false, an empty result and a no-op write grant nothing.
pub fn read_kind(call: &Invocation, outcome: &Outcome) -> Option<ReadKind> {
    use crate::collaboration::QueryResult as Q;
    use ReadKind as K;
    let Outcome::Ok { data } = outcome else { return None };
    if matches!(data,Data::ReadPage(_)) {
        return if call.tool=="collab_read_continue" {Some(K::Page)} else {kind_for_tool(&call.tool)};
    }
    if matches!(data,Data::ReadList(_)) && call.tool!="collab_read_continue" {return kind_for_tool(&call.tool);}
    match (call.tool.as_str(), data) {
        ("collab_task_get", Data::Task(_)) => Some(K::Task),
        ("collab_memory_get", Data::Memory(_)) => Some(K::Memory),
        ("collab_agent_get", Data::Agent(_)) => Some(K::Agent),
        ("collab_tasks_list" | "collab_agents_list" | "collab_task_releases_list", Data::CollaborationPage(_)) => Some(K::List),
        ("collab_release_get", Data::Release(_)) => Some(K::Collaboration),
        ("collab_repository_tree", Data::Repository(v)) if matches!(v.data, crate::repository_view::Data::Tree { .. }) => Some(K::Repository),
        ("collab_repository_read", Data::Repository(v)) if matches!(v.data, crate::repository_view::Data::File { .. }) => Some(K::Repository),
        ("collab_repository_search", Data::Repository(v)) if matches!(v.data, crate::repository_view::Data::Search { .. }) => Some(K::Repository),
        ("collab_repository_diff", Data::Repository(v)) if matches!(v.data, crate::repository_view::Data::Diff { .. }) => Some(K::Repository),
        ("collab_repository_history", Data::Repository(v)) if matches!(v.data, crate::repository_view::Data::History { .. }) => Some(K::Repository),
        ("collab_limits_get", Data::Collaboration(Q::Limits { .. }))
        | ("collab_session_get", Data::Collaboration(Q::Session(_)))
        | ("collab_sessions_list", Data::Collaboration(Q::Sessions(_)))
        | ("collab_definition_get", Data::Collaboration(Q::Definition(_)))
        | ("collab_memory_list", Data::Collaboration(Q::MemoryKeys(_)))
        | ("collab_task_history_list", Data::Collaboration(Q::TaskHistory(_)))
        | ("collab_workers_list", Data::Collaboration(Q::Workers(_)))
        | ("collab_worker_get", Data::Collaboration(Q::Worker(_)))
        | ("collab_machines_list", Data::Collaboration(Q::Machines(_)))
        | ("collab_machine_get", Data::Collaboration(Q::Machine(_)))
        | ("collab_task_attempts_list", Data::Collaboration(Q::TaskAttempts(_)))
        | ("collab_task_checkpoints_list", Data::Collaboration(Q::TaskCheckpoints(_)))
        | ("collab_release_checks_list", Data::Collaboration(Q::ReleaseChecks(_)))
        | ("collab_repositories_list", Data::Collaboration(Q::Repositories(_)))
        | ("collab_repository_get", Data::Collaboration(Q::RepositoryRegistration(_))) => Some(K::Collaboration),
        ("collab_document_get", Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Document(_)))
        | ("collab_documents_list", Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Documents(_)))
        | ("collab_document_versions_list", Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Versions(_)))
        | ("collab_comments_list", Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Comments(_))) => Some(K::Document),
        (tool, Data::QuestionQuery(value)) if matches!((tool, value.as_ref()),
            ("collab_question_get", crate::collaboration::questions::QuestionQueryResult::Question(_))
            | ("collab_questions_list", crate::collaboration::questions::QuestionQueryResult::Questions(_))
            | ("collab_question_answers", crate::collaboration::questions::QuestionQueryResult::Answers(_))
            | ("collab_question_actions", crate::collaboration::questions::QuestionQueryResult::Actions(_))
            | ("collab_decisions_list", crate::collaboration::questions::QuestionQueryResult::DecisionQueue(_))) => Some(K::Question),
        (tool, Data::ReviewQuery(value)) if matches!((tool, value.as_ref()),
            ("collab_review_get", crate::collaboration::documents::ReviewQueryOutcome::Review(_))
            | ("collab_reviews_list", crate::collaboration::documents::ReviewQueryOutcome::Reviews(_))
            | ("collab_review_target_get", crate::collaboration::documents::ReviewQueryOutcome::AcceptedTarget(_))) => Some(K::Review),
        ("collab_task_coordination_get", Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(_)))
        | ("collab_task_waits", Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Waits(_))) => Some(K::Work),
        _ => None,
    }
}
fn kind_for_tool(tool: &str) -> Option<ReadKind> {
    use ReadKind as K;
    Some(match tool {
        "collab_read_continue" => K::Page,
        "collab_task_get" => K::Task, "collab_memory_get" => K::Memory, "collab_agent_get" => K::Agent,
        "collab_tasks_list" | "collab_agents_list" | "collab_task_releases_list" => K::List,
        "collab_repository_tree" | "collab_repository_read" | "collab_repository_search" | "collab_repository_diff" | "collab_repository_history" => K::Repository,
        "collab_document_get" | "collab_documents_list" | "collab_document_versions_list" | "collab_comments_list" => K::Document,
        "collab_question_get" | "collab_questions_list" | "collab_question_answers" | "collab_question_actions" | "collab_decisions_list" => K::Question,
        "collab_review_get" | "collab_reviews_list" | "collab_review_target_get" => K::Review,
        "collab_task_coordination_get" | "collab_task_waits" => K::Work,
        "collab_limits_get" | "collab_session_get" | "collab_sessions_list" | "collab_definition_get"
        | "collab_memory_list" | "collab_task_history_list" | "collab_workers_list" | "collab_worker_get"
        | "collab_machines_list" | "collab_machine_get" | "collab_task_attempts_list" | "collab_task_checkpoints_list"
        | "collab_release_get" | "collab_release_checks_list" | "collab_repositories_list" | "collab_repository_get" => K::Collaboration,
        _ => return None,
    })
}
pub fn digest(bytes: &[u8]) -> String {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
pub fn valid_digest(value: &str) -> bool { value.len() == 64 && value.bytes().all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)) }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceProjection {
    Observation,
    Page { page: PageProjection },
    ReadList { items: Vec<ListItemProjection>, complete: bool, next_cursor: Option<String> },
    Repository { source: crate::repository_view::Source, file: Option<FileProjection> },
    Document { version: crate::collaboration::workflow::DocumentVersionRef },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileProjection { pub path: String, pub object: String, pub start_line: usize, pub total_lines: usize, pub next_line: Option<usize> }


/// Exact typed pagination metadata, with only content removed. Cursor and
/// original selection remain observations; neither grants a new read.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PageProjection {
    pub subject: crate::read_page::Selection,
    pub identity: crate::read_page::Identity,
    pub summary: crate::read_page::Summary,
    pub encoding: crate::read_page::Encoding,
    pub offset: usize, pub returned_bytes: usize, pub total_bytes: usize,
    pub complete: bool, pub next_cursor: Option<String>, pub retained_truncation: Option<bool>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
pub enum ListItemProjection {
    Complete { bytes: usize, sha256: String },
    Partial { content_complete: bool, page: PageProjection },
}
impl PageProjection {
    pub fn from_page(p:&crate::read_page::ReadPage)->Self {
        Self {subject:p.subject.clone(),identity:p.identity.clone(),summary:p.summary.clone(),encoding:p.encoding,
            offset:p.offset,returned_bytes:p.returned_bytes,total_bytes:p.total_bytes,complete:p.complete,
            next_cursor:p.next_cursor.clone(),retained_truncation:p.retained_truncation}
    }
    pub fn with_content(&self,content:String)->crate::read_page::ReadPage {
        crate::read_page::ReadPage {subject:self.subject.clone(),identity:self.identity.clone(),summary:self.summary.clone(),
            encoding:self.encoding,offset:self.offset,returned_bytes:self.returned_bytes,total_bytes:self.total_bytes,
            content,complete:self.complete,next_cursor:self.next_cursor.clone(),retained_truncation:self.retained_truncation}
    }
    fn validate(&self,call:&Invocation)->Result<(),String> {
        use crate::read_page::{Selection as S,Source as I,Summary as U,Cursor,Encoding as E};
        let end=self.offset.checked_add(self.returned_bytes).ok_or("page range overflow")?;
        if end>self.total_bytes || self.complete!=(end==self.total_bytes) || self.complete!=self.next_cursor.is_none()
            || (self.returned_bytes==0 && self.total_bytes!=0)
            || !self.identity.digest.strip_prefix("sha256:").is_some_and(valid_digest) {
            return Err("retained page range or integrity differs".into());
        }
        match (&self.subject,&self.identity.source,&self.summary) {
            (S::Task{task_id,session_id,part},I::Task{task_id:id,session_id:session,part:p,..},U::Task{..})
                if task_id==id && session_id.as_ref()==Some(session) && part==p => (),
            (S::Document{document_id,version,part},I::Document{version:v,part:p,record_revision},U::Document{..})
                if document_id==&v.document_id && version.is_none_or(|n|n==v.version) && part==p
                && (version.is_some() || record_revision.is_some()) => (),
            (S::Repository{task_id,start_line,max_lines,..},I::Repository{registration,commit,blob,..},
                U::Repository{start_line:start,max_lines:lines,..})
                if task_id==&call.caller.task_id && start_line==start && max_lines==lines
                && !registration.repository_id.is_empty() && !registration.registration_revision.is_empty()
                && !commit.is_empty() && !blob.is_empty() && self.encoding==E::Utf8 => (),
            (S::Public{tool,caller_task_id,..},I::Public{tool:t,..},U::Public{tool:u})
                if tool==t && tool==u && caller_task_id.as_deref()==Some(call.caller.task_id.as_str())
                && self.encoding==E::CanonicalJsonUtf8 => (),
            _=>return Err("retained page typed source differs".into()),
        }
        if let Some(raw)=&self.next_cursor {
            let next=Cursor::decode(raw,raw.len()).map_err(|e|e.message)?;
            if next.selection!=self.subject || next.identity!=self.identity || next.encoding!=self.encoding || next.offset!=end {
                return Err("retained next-content cursor differs".into());
            }
        }
        if call.tool=="collab_read_continue" {
            let raw=call.arguments.get("cursor").and_then(|v|v.as_str()).ok_or("original page cursor absent")?;
            let c=Cursor::decode(raw,raw.len()).map_err(|e|e.message)?;
            if c.selection!=self.subject || c.identity!=self.identity || c.encoding!=self.encoding || c.offset!=self.offset {
                return Err("original content cursor differs".into());
            }
            return Ok(());
        }
        let mut args=call.arguments.clone();
        let options=crate::mcp::take_read_options(&call.tool,&mut args)?;
        if let Some(raw)=options.cursor {
            let c=Cursor::decode(&raw,raw.len()).map_err(|e|e.message)?;
            if c.selection!=self.subject || c.identity!=self.identity || c.encoding!=self.encoding || c.offset!=self.offset {
                return Err("original explicit page cursor differs".into());
            }
        } else if self.offset!=0 {return Err("first page has nonzero offset".into());}
        match &self.subject {
            S::Task{task_id,session_id,part} if call.tool=="collab_task_get"
                && args.get("task_id").and_then(|v|v.as_str())==Some(task_id.as_str())
                && options.session_id.as_ref().is_none_or(|id|Some(id)==session_id.as_ref())
                && options.task_part.unwrap_or_default()==*part => (),
            S::Document{document_id,version,part} if call.tool=="collab_document_get"
                && args.get("document_id").and_then(|v|v.as_str())==Some(document_id.as_str())
                && args.get("version").and_then(|v|v.as_u64()).is_none_or(|n|Some(n)==*version)
                && options.document_part.unwrap_or_default()==*part => (),
            S::Repository{source,path,start_line,max_lines,..} if call.tool=="collab_repository_read"
                && args.get("source")==Some(&serde_json::to_value(source).map_err(|_|"source encoding")?)
                && args.get("path").and_then(|v|v.as_str())==Some(path.as_str())
                && args.get("start_line").and_then(|v|v.as_u64())==Some(*start_line as u64)
                && args.get("max_lines").and_then(|v|v.as_u64())==Some(*max_lines as u64) => (),
            S::Public{tool,arguments,item,..} if tool.name()==call.tool => {
                if *item {
                    let n=args.get("limit").and_then(|v|v.as_u64()).ok_or("partial list limit absent")?;
                    let selected=arguments.get("limit").and_then(|v|v.as_u64()).ok_or("partial item limit absent")?;
                    if selected!=1 || n<selected {return Err("partial item query differs".into());}
                    args["limit"]=serde_json::json!(selected);
                }
                if &args!=arguments {return Err("original public query differs".into());}
            }
            _=>return Err("original page selection differs".into()),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidence {
    pub kind: ReadKind,
    pub outcome_bytes: usize,
    pub outcome_sha256: String,
    pub source: SourceProjection,
}
impl ReadEvidence {
    pub fn from_outcome(call: &Invocation, outcome: &Outcome) -> Result<Self, String> {
        let kind = read_kind(call, outcome).ok_or("not an allowlisted successful canonical read")?;
        let bytes = serde_json::to_vec(outcome).map_err(|_| "read outcome encoding")?;
        let source = match outcome {
            Outcome::Ok {data:Data::ReadPage(page)} => {
                if page.content.len()!=page.returned_bytes {return Err("page content size differs".into());}
                SourceProjection::Page {page:PageProjection::from_page(page)}
            },
            Outcome::Ok {data:Data::ReadList(list)} => SourceProjection::ReadList {
                items:list.items.iter().map(|item|match item {
                    crate::read_page::PagedItem::Complete{value}=>{
                        let bytes=serde_json::to_vec(value).map_err(|_|"list item encoding")?;
                        Ok(ListItemProjection::Complete{bytes:bytes.len(),sha256:digest(&bytes)})
                    }
                    crate::read_page::PagedItem::Partial{content_complete,page}=>{
                        if page.returned_bytes!=page.content.len() {return Err("partial item size differs");}
                        Ok(ListItemProjection::Partial{content_complete:*content_complete,page:PageProjection::from_page(page)})
                    }
                }).collect::<Result<Vec<_>,&str>>()?,complete:list.complete,next_cursor:list.next_cursor.clone()
            },

            Outcome::Ok { data: Data::Repository(v) } => SourceProjection::Repository {
                source: v.source.clone(), file: match &v.data {
                    crate::repository_view::Data::File { path, object, start_line, total_lines, next_line, .. } => Some(FileProjection { path:path.clone(), object:object.clone(), start_line:*start_line, total_lines:*total_lines, next_line:*next_line }),
                    _ => None,
                },
            },
            Outcome::Ok { data: Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Document(v)) } => SourceProjection::Document { version:v.version.reference.clone() },
            _ => SourceProjection::Observation,
        };
        let value = Self { kind, outcome_bytes:bytes.len(), outcome_sha256:digest(&bytes), source };
        value.validate(call)?;
        Ok(value)
    }
    pub fn validate(&self, call: &Invocation) -> Result<(), String> {
        if kind_for_tool(&call.tool) != Some(self.kind.clone()) || self.outcome_bytes == 0 || !valid_digest(&self.outcome_sha256) {
            return Err("invalid committed read classification or integrity evidence".into());
        }
        match &self.source {
            SourceProjection::Page{page} => page.validate(call)?,
            SourceProjection::ReadList{items,complete,next_cursor} => {
                if call.tool=="collab_read_continue" || *complete!=next_cursor.is_none() {
                    return Err("retained list traversal differs".into());
                }
                for item in items {match item {
                    ListItemProjection::Complete{bytes,sha256} if *bytes>0 && valid_digest(sha256)=>(),
                    ListItemProjection::Partial{content_complete,page} if *content_complete==page.complete => page.validate(call)?,
                    _=>return Err("retained list item differs".into()),
                }}
            },

            SourceProjection::Repository { source, file } => {
                if self.kind != ReadKind::Repository || source.commit.is_empty() || source.repository.is_empty()
                    || source.registration.repository_id.is_empty() || source.registration.registration_revision.is_empty()
                    || file.as_ref().is_some_and(|f| f.object.is_empty() || f.start_line == 0
                        || call.arguments.get("path").and_then(|v|v.as_str()) != Some(f.path.as_str())
                        || call.arguments.get("start_line").and_then(|v|v.as_u64()) != Some(f.start_line as u64))
                    || (call.tool == "collab_repository_read") != file.is_some() {
                    return Err("read source projection differs".into());
                }
            }
            SourceProjection::Document { version } => {
                if call.tool != "collab_document_get" || call.arguments.get("document_id").and_then(|v|v.as_str()) != Some(version.document_id.as_str())
                    || call.arguments.get("version").and_then(|v|v.as_u64()).is_some_and(|v|v != version.version) {
                    return Err("read document version differs".into());
                }
            }
            SourceProjection::Observation if matches!(self.kind,ReadKind::Repository|ReadKind::Page) || call.tool == "collab_document_get" => return Err("immutable read source missing".into()),
            _ => (),
        }
        Ok(())
    }
    pub fn verify(&self, call: &Invocation, outcome: &Outcome) -> Result<(), String> {
        if &Self::from_outcome(call, outcome)? != self { return Err("original read outcome digest, size or source differs".into()); }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadReference {
    pub invocation: Invocation,
    pub commit: MutationCommit,
    pub request_id: String,
    pub observed_controller_revision: u64,
    pub evidence: ReadEvidence,
}
impl ReadReference {
    pub fn new(call: &Invocation, result: &CallResult) -> Result<Self, String> {
        result.reply.validate_for(&call.operation())?;
        if result.requires_quiescence { return Err("quiescing result is not a compact read".into()); }
        let value = Self { invocation:call.clone(), commit:result.reply.commit.clone().ok_or("read is not committed")?,
            request_id:result.reply.request_id.clone(), observed_controller_revision:result.reply.observed_controller_revision.ok_or("read observation revision missing")?,
            evidence:ReadEvidence::from_outcome(call, &result.reply.outcome)? };
        value.validate(call)?;
        Ok(value)
    }
    pub fn validate(&self, call: &Invocation) -> Result<(), String> {
        if &self.invocation != call || self.commit.operation_id != call.operation_id || self.commit.controller_revision == 0
            || self.commit.controller_revision > self.observed_controller_revision || self.observed_controller_revision > i64::MAX as u64 {
            return Err("committed read reference binding differs".into());
        }
        self.evidence.validate(call)
    }
    pub fn restore(&self, call: &Invocation, recovered: CallResult) -> Result<CallResult, String> {
        self.validate(call)?;
        recovered.reply.validate_for(&call.operation())?;
        if recovered.requires_quiescence || recovered.reply.commit.as_ref() != Some(&self.commit) { return Err("original read commit differs".into()); }
        self.evidence.verify(call, &recovered.reply.outcome)?;
        Ok(CallResult { requires_quiescence:false, reply:Reply { request_id:self.request_id.clone(), commit:Some(self.commit.clone()),
            observed_controller_revision:Some(self.observed_controller_revision), outcome:recovered.reply.outcome } })
    }
}

// Existing full CallResult encodings are unchanged. A reference is always Some
// of this explicit variant, never the legacy unresolved null.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum CallBody { Inline(CallResult), Reference { committed_read_reference: ReadReference } }
impl CallBody {
    pub fn inline(&self) -> Option<&CallResult> { match self { Self::Inline(v)=>Some(v), _=>None } }
    pub fn reference(&self) -> Option<&ReadReference> { match self { Self::Reference { committed_read_reference:v }=>Some(v), _=>None } }
}
impl From<CallResult> for CallBody { fn from(v:CallResult)->Self { Self::Inline(v) } }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseItem { pub operation_id: String, pub commit: MutationCommit, pub evidence: ReadEvidence }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum TranscriptCustody {
    CodingCopy { manifest_sha256:String, bytes:u64, files:u64 },
    NativeJournal { manifest_sha256:String, bytes:u64, files:u64 },
}
impl TranscriptCustody {
    pub fn valid(&self) -> bool { let (hash,bytes,files)=match self { Self::CodingCopy {manifest_sha256,bytes,files} | Self::NativeJournal {manifest_sha256,bytes,files} => (manifest_sha256,bytes,files) }; valid_digest(hash) && *bytes>0 && *files>0 }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TerminalReadRelease {
    pub operation_id:String, pub caller:Caller, pub command_id:String, pub session_id:String,
    pub terminal_revision:u64, pub first_safe_operation_id:Option<String>, pub policy_revision:String,
    pub policy:crate::limits::WorkerRetentionLimits, pub custody:TranscriptCustody,
    pub custody_operation_id:String,
    pub reads:Vec<ReleaseItem>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseResult {
    pub attempt_id:String, pub released:Vec<String>, pub body_bytes_before:usize, pub body_bytes_after:usize,
    pub first_safe_operation_id:String, pub first_safe_ms:u64, pub eligible_at_ms:u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompactRead {
    pub evidence:ReadEvidence, pub release_operation_id:String, pub attempt_id:String, pub session_id:String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompactOutcome { pub committed_read_reference:CompactRead }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleasedRead { pub invocation:Invocation, pub original_commit:MutationCommit, pub record:CompactOutcome }
impl CompactOutcome {
    pub fn validate(&self, operation:&Operation) -> Result<(),String> {
        let Operation::CallCollaboration {call}=operation else {return Err("compact receipt is not an original read".into())};
        let v=&self.committed_read_reference;
        if v.attempt_id != call.caller.attempt_id || v.session_id.is_empty() || v.release_operation_id.is_empty() {return Err("compact read release binding differs".into())}
        v.evidence.validate(call)
    }
}
