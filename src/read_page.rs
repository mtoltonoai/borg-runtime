//! One explicit, source-bound public read. Cursors are data, never authority.
//! Fragments use byte positions in the credential-safe public encoding.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskPart { #[default] Record, Instruction, Result, RecentOutput }
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DocumentPart { #[default] Record, Body }
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Encoding { Utf8, CanonicalJsonUtf8 }

// A closed vocabulary, shared with retry classification. Neither a cursor nor
// changed=false can turn a mutation (including a no-op) into a read.
macro_rules! reads {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
        pub enum ReadTool { $(#[serde(rename=$name)] $variant),+ }
        impl ReadTool {
            pub fn parse(name: &str) -> Option<Self> { match name { $($name => Some(Self::$variant),)+ _ => None } }
            pub fn name(self) -> &'static str { match self { $(Self::$variant => $name,)+ } }
        }
    }
}
reads! {
    Tasks => "collab_tasks_list", Task => "collab_task_get", History => "collab_task_history_list",
    Agents => "collab_agents_list", Agent => "collab_agent_get", Definition => "collab_definition_get",
    MemoryKeys => "collab_memory_list", Memory => "collab_memory_get",
    Workers => "collab_workers_list", Worker => "collab_worker_get",
    Machines => "collab_machines_list", Machine => "collab_machine_get",
    Attempts => "collab_task_attempts_list", Checkpoints => "collab_task_checkpoints_list",
    Releases => "collab_task_releases_list", Release => "collab_release_get", Checks => "collab_release_checks_list",
    Call => "collab_call_get", RepositoryTree => "collab_repository_tree", RepositoryRead => "collab_repository_read",
    RepositorySearch => "collab_repository_search", RepositoryDiff => "collab_repository_diff", RepositoryHistory => "collab_repository_history",
    Session => "collab_session_get", Sessions => "collab_sessions_list", Limits => "collab_limits_get",
    Repositories => "collab_repositories_list", Repository => "collab_repository_get",
    Question => "collab_question_get", Questions => "collab_questions_list", Answers => "collab_question_answers",
    Actions => "collab_question_actions", Decisions => "collab_decisions_list",
    Document => "collab_document_get", Documents => "collab_documents_list", Versions => "collab_document_versions_list",
    Comments => "collab_comments_list", Review => "collab_review_get", Reviews => "collab_reviews_list",
    ReviewTarget => "collab_review_target_get", Coordination => "collab_task_coordination_get", Waits => "collab_task_waits",
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum Selection {
    Task { task_id: String, session_id: Option<String>, part: TaskPart },
    Document { document_id: String, version: Option<u64>, part: DocumentPart },
    /// The tool is a closed read enum and its arguments pass the original
    /// domain/profile validator on every occurrence. item selects the sole
    /// reachable row of a one-row keyset query, never an arbitrary JSON path.
    Public { tool: ReadTool, arguments: Value, item: bool, caller_task_id: Option<String> },
    Repository { task_id: String, source: crate::repository_view::SourceSelector,
        path: String, start_line: usize, max_lines: usize },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub selection: Selection,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum Source {
    Task { task_id: String, session_id: String, part: TaskPart, revision: String },
    Document { version: crate::collaboration::workflow::DocumentVersionRef, part: DocumentPart, record_revision: Option<u64> },
    Repository { registration: crate::repository_catalogue::Revision, commit: String, blob: String, checkpoint_operation: Option<String> },
    Public { tool: ReadTool, item_id: Option<String>, revision: Option<u64> },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Identity { pub source: Source, pub digest: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum Summary {
    Task { status: crate::protocol::TaskStatus, parts: Vec<TaskPart> },
    Document { parts: Vec<DocumentPart> },
    Repository { start_line: usize, max_lines: usize, total_lines: usize, next_line: Option<usize> },
    Public { tool: ReadTool },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadPage {
    pub subject: Selection,
    pub identity: Identity,
    pub summary: Summary,
    pub encoding: Encoding,
    pub offset: usize,
    pub returned_bytes: usize,
    pub total_bytes: usize,
    pub content: String,
    pub complete: bool,
    pub next_cursor: Option<String>,
    pub retained_truncation: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum PagedItem {
    Complete { value: Value },
    Partial { content_complete: bool, page: ReadPage },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadList {
    pub items: Vec<PagedItem>,
    pub complete: bool,
    /// List traversal and the content cursor on a partial item are distinct.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub selection: Selection,
    pub identity: Identity,
    pub encoding: Encoding,
    pub offset: usize,
    pub max_bytes: Option<usize>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error { pub code: &'static str, pub message: String }
impl Error {
    pub fn invalid(message: &str) -> Self { Self { code: "invalid_read_cursor", message: message.into() } }
    pub fn bound(field: &str) -> Self { Self { code: "response_bound", message: format!("read metadata and one useful scalar cannot fit: {field}") } }
    pub fn stale() -> Self { Self { code: "stale_read", message: "selected public content changed; explicitly restart the read without its cursor".into() } }
}
impl Cursor {
    pub fn decode(raw: &str, cap: usize) -> Result<Self, Error> {
        if raw.len() > cap { return Err(Error::bound("controller.collaboration_cursor_bytes")); }
        let value: Self = serde_json::from_str(raw).map_err(|_| Error::invalid("invalid or unknown read cursor fields"))?;
        if value.offset == 0 || value.max_bytes == Some(0) || value.encode(cap)? != raw {
            return Err(Error::invalid("noncanonical read cursor or zero progress"));
        }
        Ok(value)
    }
    pub fn encode(&self, cap: usize) -> Result<String, Error> {
        crate::protocol::bounded_json_size(self, cap).map_err(|_| Error::bound("controller.collaboration_cursor_bytes"))?;
        canonical(self).map_err(|_| Error::bound("controller.collaboration_cursor_bytes"))
    }
}

/// Serialize an already admitted public projection, allocating only its measured
/// size and reporting allocation failure. Source admission is owned by its reader.
pub fn canonical(value: &impl Serialize) -> Result<String, String> {
    let size = crate::protocol::bounded_json_size(value, usize::MAX)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|_| "read allocation unavailable")?;
    serde_json::to_writer(&mut bytes, value).map_err(|_| "read encoding unavailable")?;
    String::from_utf8(bytes).map_err(|_| "read encoding is not UTF-8".into())
}
pub fn digest(content: &[u8]) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, content);
    let mut value = String::from("sha256:");
    use std::fmt::Write;
    for b in digest.as_ref() { let _ = write!(value, "{b:02x}"); }
    value
}

/// Fit one page against the real consumer serializers supplied by the caller.
/// The chunk can itself be a bounded repository slice of the exact selected
/// range. No production consumer reconstructs or automatically requests pages.
// = Separate citations: each obligation has its own assertion in the tests.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# Permitted reads of lists or individual content that exceed the configured response bound MUST return bounded pages.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# Each read page MUST state whether the requested read is complete.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# A nonfinal read page MUST provide a continuation for the remaining permitted content.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# A paginated read MUST NOT silently truncate the requested content.
pub fn fit(
    mut page: ReadPage, chunk: &str, max_bytes: Option<usize>, cursor_bytes: usize,
    mut check: impl FnMut(&ReadPage) -> Result<(), Error>,
) -> Result<ReadPage, Error> {
    if page.offset > page.total_bytes || chunk.len() > page.total_bytes - page.offset
        || (chunk.is_empty() && page.offset < page.total_bytes) {
        return Err(Error::invalid("out-of-range read offset or empty nonfinal source"));
    }
    let make = |n: usize| -> Result<ReadPage, Error> {
        let end = page.offset.checked_add(n).ok_or_else(|| Error::invalid("read offset overflow"))?;
        let mut result = page.clone();
        result.content = String::new();
        result.content.try_reserve_exact(n).map_err(|_| Error::bound("read allocation"))?;
        result.content.push_str(&chunk[..n]);
        result.returned_bytes = n;
        result.complete = end == page.total_bytes;
        result.next_cursor = if result.complete { None } else { Some(Cursor {
            selection: page.subject.clone(), identity: page.identity.clone(), encoding: page.encoding,
            offset: end, max_bytes,
        }.encode(cursor_bytes)?) };
        Ok(result)
    };
    // A final page can be cheaper than an intermediate page because it has no
    // cursor. Check it first; do not incorrectly reject a fitting final scalar.
    if let Ok(full) = make(chunk.len()) { if check(&full).is_ok() { return Ok(full); } }
    let minimum = chunk.chars().next().map_or(0, char::len_utf8);
    let first = make(minimum)?;
    check(&first)?;
    if minimum == 0 { return Ok(first); }
    let mut low = minimum;
    let mut high = chunk.len();
    while low < high {
        let mut middle = low + (high - low).div_ceil(2);
        while !chunk.is_char_boundary(middle) { middle -= 1; }
        if middle <= low { break; }
        match make(middle).and_then(|p| check(&p)) {
            Ok(()) => low = middle,
            Err(_) => high = middle - 1,
        }
    }
    page = make(low)?;
    check(&page)?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    //= spec/impl/v2/implementation.md#bounded-read-pagination
    //# Permitted reads of lists or individual content that exceed the configured response bound MUST return bounded pages.
    //= spec/impl/v2/implementation.md#bounded-read-pagination
    //# Each read page MUST state whether the requested read is complete.
    //= spec/impl/v2/implementation.md#bounded-read-pagination
    //# A nonfinal read page MUST provide a continuation for the remaining permitted content.
    //= spec/impl/v2/implementation.md#bounded-read-pagination
    //# A paginated read MUST NOT silently truncate the requested content.
    #[test]
    fn shared_read_pagination_utf8_exact_framing_and_exhaustion() {
        let source = "\"\\\n\r\t\u{1}🙂e\u{301}".repeat(200);
        for source in [&source[..], "", "🙂"] {
            let mut original = ReadPage {
                subject: Selection::Task { task_id:"task".into(),session_id:Some("session".into()),part:TaskPart::Instruction },
                identity: Identity { source: Source::Task { task_id:"task".into(),session_id:"session".into(),part:TaskPart::Instruction,revision:"session".into() }, digest:digest(source.as_bytes()) },
                summary:Summary::Task { status:crate::protocol::TaskStatus::Active, parts:vec![TaskPart::Record,TaskPart::Instruction] },
                encoding:Encoding::Utf8, offset:0,returned_bytes:0,total_bytes:source.len(),content:String::new(),complete:false,next_cursor:None,retained_truncation:None,
            };
            let envelope = |p: &ReadPage| serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":"\\\"","result":{"content":[{"type":"text","text":serde_json::to_string(p).unwrap()}],"isError":false}})).unwrap().len();
            let cap = 1600;
            let mut reconstructed = String::new();
            loop {
                let p = fit(original.clone(),&source[original.offset..],Some(cap),1024,|p| if envelope(p)<=cap {Ok(())} else {Err(Error::bound("requested max_bytes"))}).unwrap();
                assert!(envelope(&p)<=cap);
                assert_eq!(p.returned_bytes,p.content.len());
                assert!(p.returned_bytes>0 || source.is_empty());
                assert_eq!(p.complete,p.offset+p.returned_bytes==source.len());
                assert_eq!(p.next_cursor.is_none(),p.complete);
                reconstructed.push_str(&p.content);
                if p.complete {break;}
                let c=Cursor::decode(p.next_cursor.as_ref().unwrap(),1024).unwrap();
                assert!(c.offset>original.offset && source.is_char_boundary(c.offset));
                original.offset=c.offset;
            }
            assert_eq!(reconstructed,source);
            original.offset=0;
            assert_eq!(fit(original,source,Some(1),1024,|_|Err(Error::bound("requested max_bytes"))).unwrap_err().message,"read metadata and one useful scalar cannot fit: requested max_bytes");
        }
    }
}
