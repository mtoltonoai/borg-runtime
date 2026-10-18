//! Read-only wire projection. This is data, never an executable read ticket.
use crate::repository_read::{self, Commit, Entry};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceSelector {
    Start,
    Checkpoint,
    Instructions,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    ReadPage {
        source: SourceSelector, path: String, start_line: usize, max_lines: usize,
        #[serde(default, skip_serializing_if="Option::is_none")]
        cursor: Option<String>,
        #[serde(default, skip_serializing_if="Option::is_none")]
        max_bytes: Option<usize>,
    },
    Tree {
        source: SourceSelector,
        path: String,
        after: Option<String>,
        limit: usize,
    },
    ReadFile {
        source: SourceSelector,
        path: String,
        start_line: usize,
        max_lines: usize,
    },
    Search {
        source: SourceSelector,
        path: String,
        needle: String,
    },
    Diff {
        source: SourceSelector,
        base_source: SourceSelector,
        path: String,
    },
    History {
        source: SourceSelector,
        limit: usize,
    },
}
impl Request {
    pub(crate) fn parts(
        &self,
    ) -> (
        SourceSelector,
        Option<SourceSelector>,
        repository_read::Query,
    ) {
        match self {
            Self::ReadPage { source,path,start_line,max_lines,.. } => (*source,None,
                repository_read::Query::ReadFileSlice { path:path.clone(),start_line:*start_line,max_lines:*max_lines,offset:0 }),
            Self::Tree {
                source,
                path,
                after,
                limit,
            } => (
                *source,
                None,
                repository_read::Query::Tree {
                    path: path.clone(),
                    after: after.clone(),
                    limit: *limit,
                },
            ),
            Self::ReadFile {
                source,
                path,
                start_line,
                max_lines,
            } => (
                *source,
                None,
                repository_read::Query::ReadFile {
                    path: path.clone(),
                    start_line: *start_line,
                    max_lines: *max_lines,
                },
            ),
            Self::Search {
                source,
                path,
                needle,
            } => (
                *source,
                None,
                repository_read::Query::Search {
                    path: path.clone(),
                    needle: needle.clone(),
                },
            ),
            Self::Diff {
                source,
                base_source,
                path,
            } => (
                *source,
                Some(*base_source),
                repository_read::Query::Diff { path: path.clone() },
            ),
            Self::History { source, limit } => (
                *source,
                None,
                repository_read::Query::History { limit: *limit },
            ),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub registration: crate::repository_catalogue::Revision,
    pub repository: String,
    pub commit: String,
    pub base_commit: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub source: Source,
    pub untrusted: bool,
    pub data: Data,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Data {
    ReadPage(Box<crate::read_page::ReadPage>),
    /// Internal bounded helper carrier; the controller must project it before
    /// any public response. It is never an executable source ticket.
    FileSlice { path: String, object: String, start_line: usize, max_lines: usize,
        total_lines: usize, next_line: Option<usize>, offset: usize,
        total_bytes: usize, digest: String, text: String },
    Tree {
        entries: Vec<Entry>,
        next_after: Option<String>,
    },
    File {
        path: String,
        object: String,
        start_line: usize,
        total_lines: usize,
        text: String,
        next_line: Option<usize>,
    },
    Search {
        text: String,
    },
    Diff {
        text: String,
    },
    History {
        commits: Vec<Commit>,
    },
}
impl From<repository_read::Output> for Output {
    fn from(value: repository_read::Output) -> Self {
        let source = Source {
            registration: value.source.registration,
            repository: value.source.repository,
            commit: value.source.commit,
            base_commit: value.source.base_commit,
        };
        let data = match value.data {
            repository_read::Data::FileSlice {path,object,start_line,max_lines,total_lines,next_line,offset,total_bytes,digest,text} =>
                Data::FileSlice {path,object,start_line,max_lines,total_lines,next_line,offset,total_bytes,digest,text},
            repository_read::Data::Tree {
                entries,
                next_after,
            } => Data::Tree {
                entries,
                next_after,
            },
            repository_read::Data::File {
                path,
                object,
                start_line,
                total_lines,
                text,
                next_line,
            } => Data::File {
                path,
                object,
                start_line,
                total_lines,
                text,
                next_line,
            },
            repository_read::Data::Search { text } => Data::Search { text },
            repository_read::Data::Diff { text } => Data::Diff { text },
            repository_read::Data::History { commits } => Data::History { commits },
        };
        Self {
            source,
            untrusted: true,
            data,
        }
    }
}
