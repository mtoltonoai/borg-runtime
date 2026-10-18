//! Optional charter policy: historical absence remains disabled and byte-preserved.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterLimits {
    pub record_bytes: usize,
    pub source_bytes: usize,
    pub files: usize,
    pub clauses: usize,
    pub metadata_bytes: usize,
    pub references: usize,
    pub reply_items: usize,
    pub tool_calls: usize,
    pub reference_depth: usize,
    /// Global retained charter binding/request/reply/result rows; no deletion.
    /// Sources remain in existing immutable definition/task storage.
    pub records: usize,
    pub page_items: usize,
    pub page_bytes: usize,
}
impl CharterLimits {
    pub fn validate(&self,current:&super::RuntimeLimits)->Result<(),String> {
        for (name,n) in [("record_bytes",self.record_bytes),("source_bytes",self.source_bytes),("files",self.files),("clauses",self.clauses),("metadata_bytes",self.metadata_bytes),("references",self.references),("reply_items",self.reply_items),("tool_calls",self.tool_calls),("reference_depth",self.reference_depth),("records",self.records),("page_items",self.page_items),("page_bytes",self.page_bytes)] {
            if n==0 || n>isize::MAX as usize || n>i64::MAX as usize {return Err(format!("charter.{name} must fit a positive addressable durable bound"));}
        }
        for (name,n,max) in [("record_bytes",self.record_bytes,current.transport.body_bytes),("source_bytes",self.source_bytes,self.record_bytes),("metadata_bytes",self.metadata_bytes,self.record_bytes),("page_items",self.page_items,current.controller.collaboration_page_items),("page_bytes",self.page_bytes,current.controller.collaboration_page_bytes)] {
            if n>max{return Err(format!("charter.{name} exceeds its enclosing bound"));}
        }
        Ok(())
    }
}
