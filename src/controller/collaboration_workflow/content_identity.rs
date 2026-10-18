//! Exact document-content identity using the already selected TLS primitive.
//! This is neither authentication nor an authority/security-service boundary.
use super::QueryContext;
use super::super::database::{encode, refuse, Fault, Result};
use crate::{
    collaboration::documents::DocumentContent,
    limits::{TransportLimits, WorkflowLimits},
    protocol::bounded_json_size,
};
use aws_lc_rs::digest;

const DOMAIN: &[u8] = b"borg.collaboration.document-content.v1\0";
const PREFIX: &str = "sha256:";
const IDENTITY_BYTES: usize = PREFIX.len() + 2 * digest::SHA256_OUTPUT_LEN;

pub(crate) fn content_identity(q: &QueryContext<'_, '_>, content: &DocumentContent) -> Result<String> {
    identify(content, q.limits, &q.tx.limits.transport)
}

fn identify(content: &DocumentContent, workflow: &WorkflowLimits, transport: &TransportLimits) -> Result<String> {
    // Match the document domain's existing text rules, including retained reads.
    if content.title.trim().is_empty() || content.title.contains('\0') || content.body.contains('\0') {
        return refuse("invalid_input", "document text is empty or invalid");
    }
    if content.title.len() > workflow.text_bytes || content.body.len() > workflow.text_bytes {
        return refuse("invalid_input", "Configured limit exceeded: workflow.text_bytes");
    }
    // Count the actual typed JSON encoding before its variable-size allocation.
    // Escapes count as encoded bytes, not as the original text's character count.
    let encoded_len = bounded_json_size(content, workflow.record_bytes)
        .map_err(|_| Fault::Refused("capacity", "Configured limit exceeded: workflow.record_bytes".into()))?;
    if encoded_len > transport.body_bytes {
        return refuse("capacity", "Configured limit exceeded: transport.body_bytes");
    }
    if IDENTITY_BYTES > transport.identity_bytes {
        return refuse("input_bound", "Configured limit exceeded: transport.identity_bytes");
    }
    // The fixed domain is streamed separately; no concatenated preimage buffer.
    if encoded_len.checked_add(DOMAIN.len()).and_then(|n| u64::try_from(n).ok()).is_none() {
        return refuse("capacity", "document content identity input exceeds digest representation");
    }
    let canonical = encode(content)?;
    if canonical.len() != encoded_len {
        return Err(Fault::Store("document content encoding differs from bounded projection".into()));
    }
    let mut state = digest::Context::new(&digest::SHA256);
    state.update(DOMAIN);
    state.update(canonical.as_bytes());
    let digest = state.finish();
    let mut identity = String::with_capacity(IDENTITY_BYTES);
    identity.push_str(PREFIX);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in digest.as_ref() {
        identity.push(HEX[(byte >> 4) as usize] as char);
        identity.push(HEX[(byte & 15) as usize] as char);
    }
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::RuntimeLimits;
    use serde::Deserialize;
    use std::collections::BTreeSet;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Vectors { domain_prefix_hex: String, cases: Vec<Vector> }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Vector {
        name: String,
        content: DocumentContent,
        canonical: String,
        canonical_utf8_bytes: usize,
        identity: String,
        without_domain: String,
    }
    fn vectors() -> Vectors {
        serde_json::from_str(include_str!("fixtures/content_identity_vectors.json")).unwrap()
    }
    fn limits() -> (WorkflowLimits, TransportLimits) {
        let limits = RuntimeLimits::seed().unwrap();
        (limits.workflow.unwrap(), limits.transport)
    }
    fn value(title: &str, body: &str) -> DocumentContent {
        DocumentContent { title: title.into(), body: body.into() }
    }
    fn refusal(result: Result<String>, field: &str) {
        match result {
            Err(Fault::Refused(_, message)) => assert_eq!(message, format!("Configured limit exceeded: {field}")),
            other => panic!("expected named bound refusal: {other:?}"),
        }
    }

    #[test]
    fn independent_python_vectors_match_typed_encoding_and_digest() {
        let (workflow, transport) = limits();
        let vectors = vectors();
        let actual_domain: String = DOMAIN.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(actual_domain, vectors.domain_prefix_hex);
        assert_eq!(vectors.cases.len(), 9);
        for v in vectors.cases {
            assert_eq!(encode(&v.content).unwrap(), v.canonical, "{}", v.name);
            assert_eq!(v.canonical.len(), v.canonical_utf8_bytes, "{}", v.name);
            assert_eq!(identify(&v.content, &workflow, &transport).unwrap(), v.identity, "{}", v.name);
            assert_ne!(v.identity, v.without_domain, "domain {}", v.name);
        }
    }

    #[test]
    fn title_body_boundaries_and_unicode_are_not_normalized_or_concatenated() {
        let (workflow, transport) = limits();
        let ids: BTreeSet<_> = vectors().cases.into_iter()
            .map(|v| identify(&v.content, &workflow, &transport).unwrap()).collect();
        // Includes independent title/body edits, a|bc versus ab|c, and composed
        // versus decomposed Unicode. Every exact content remains distinct.
        assert_eq!(ids.len(), 9);
    }

    #[test]
    fn typed_field_order_is_canonical_and_unknown_content_fields_refuse() {
        let (workflow, transport) = limits();
        let reordered: DocumentContent = serde_json::from_str(r#"{"body":"World","title":"Hello"}"#).unwrap();
        assert_eq!(identify(&reordered, &workflow, &transport).unwrap(),
            "sha256:3eeee3e05e54717a2437301646c91fb8460efcf2ac5c7846744dd97ec6173c63");
        assert!(serde_json::from_str::<DocumentContent>(r#"{"title":"Hello","body":"World","author":"forged"}"#).is_err());
    }

    #[test]
    fn utf8_text_bound_accepts_exact_and_refuses_one_under() {
        let (mut workflow, transport) = limits();
        let content = value("é", "");
        workflow.text_bytes = 2;
        assert!(identify(&content, &workflow, &transport).is_ok());
        workflow.text_bytes = 1;
        refusal(identify(&content, &workflow, &transport), "workflow.text_bytes");
        let content = value("T", "é");
        refusal(identify(&content, &workflow, &transport), "workflow.text_bytes");
    }

    #[test]
    fn escaped_json_record_bound_accepts_exact_and_refuses_one_under() {
        let (mut workflow, transport) = limits();
        let content = value("T", "\n\t\"\\");
        let size = encode(&content).unwrap().len();
        assert!(size > content.title.len() + content.body.len());
        workflow.record_bytes = size;
        assert!(identify(&content, &workflow, &transport).is_ok());
        workflow.record_bytes = size - 1;
        refusal(identify(&content, &workflow, &transport), "workflow.record_bytes");
    }

    #[test]
    fn transport_body_bound_is_not_bypassed_by_a_larger_workflow_bound() {
        let (workflow, mut transport) = limits();
        let content = value("T", "payload");
        let size = encode(&content).unwrap().len();
        transport.body_bytes = size;
        assert!(identify(&content, &workflow, &transport).is_ok());
        // Deliberately inconsistent component limits must still refuse; this
        // fixture does not claim the full RuntimeLimits configuration is valid.
        transport.body_bytes = size - 1;
        refusal(identify(&content, &workflow, &transport), "transport.body_bytes");
    }

    #[test]
    fn identity_bound_accepts_71_and_never_truncates_at_70() {
        let (workflow, mut transport) = limits();
        let content = value("T", "payload");
        transport.identity_bytes = 71;
        let id = identify(&content, &workflow, &transport).unwrap();
        assert_eq!(id.len(), 71);
        assert!(id.starts_with("sha256:"));
        assert!(id[7..].bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        transport.identity_bytes = 70;
        refusal(identify(&content, &workflow, &transport), "transport.identity_bytes");
    }

    #[test]
    fn existing_document_text_rules_remain_exact() {
        let (workflow, transport) = limits();
        assert!(identify(&value("Title", ""), &workflow, &transport).is_ok());
        for invalid in [value("", "body"), value(" \t", "body"), value("T\0", "body"), value("T", "\0")] {
            assert!(matches!(identify(&invalid, &workflow, &transport), Err(Fault::Refused("invalid_input", _))));
        }
    }
}
