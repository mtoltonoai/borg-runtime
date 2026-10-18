//! Compact presentation failures share one encoder with metadata feasibility.
//! The body limit covers this text too; it does not cover HTTP headers.
use std::fmt::{self, Write};

pub(crate) const OPERATION_PREFIX: &str = "/operations/";

#[derive(Clone, Copy, Debug)]
pub(super) enum LimitingField {
    RequestPath,
    Body,
    FormFields,
    FormKey,
    Identity,
    CollaborationCursor,
    WorkflowPage,
}
impl LimitingField {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::RequestPath => "product.request_path_bytes",
            Self::Body => "transport.body_bytes",
            Self::FormFields => "product.form_fields",
            Self::FormKey => "product.form_key_bytes",
            Self::Identity => "transport.identity_bytes",
            Self::CollaborationCursor => "controller.collaboration_cursor_bytes",
            Self::WorkflowPage => "workflow.page_bytes",
        }
    }
}
const FIELDS: [Option<LimitingField>; 8] = [
    None,
    Some(LimitingField::RequestPath),
    Some(LimitingField::Body),
    Some(LimitingField::FormFields),
    Some(LimitingField::FormKey),
    Some(LimitingField::Identity),
    Some(LimitingField::CollaborationCursor),
    Some(LimitingField::WorkflowPage),
];

fn encode(
    out: &mut impl Write,
    field: Option<LimitingField>,
    operation_id: Option<&str>,
) -> fmt::Result {
    out.write_str("Response unavailable: product.response_bytes.\n")?;
    if let Some(field) = field {
        writeln!(out, "Original limiting field: {}.", field.name())?;
    }
    if let Some(id) = operation_id {
        // A JSON string keeps the exact original identity recoverable, including
        // quotes and historical lookup IDs containing LF/CR/TAB. It is plain text.
        let quoted = serde_json::to_string(id).map_err(|_| fmt::Error)?;
        writeln!(out, "Original operation: {quoted}")?;
        out.write_str("This response does not establish its outcome. Look up this identity; do not submit a replacement.\n")
    } else {
        out.write_str("Request or view could not be displayed.\n")
    }
}

#[derive(Default)]
struct Count(usize);
impl Write for Count {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

pub(crate) fn minimum_bytes(path_bytes: usize, identity_bytes: usize) -> Result<usize, String> {
    let incompatible = || "product.response_bytes cannot represent correlation bounds from product.request_path_bytes and transport.identity_bytes".to_string();
    let empty = serde_json::to_string("").map_err(|_| incompatible())?;
    let escaped = serde_json::to_string("\"").map_err(|_| incompatible())?;
    let per_byte = escaped.len().checked_sub(empty.len()).ok_or_else(incompatible)?;
    // decode admits LF/CR/TAB but rejects other controls. Quotes, backslashes,
    // and those three controls attain this encoder's worst per-byte expansion;
    // non-ASCII UTF-8 is retained. Valid POST identities exclude all controls.
    let historical = path_bytes.saturating_sub(OPERATION_PREFIX.len());
    let payload = historical.max(identity_bytes).checked_mul(per_byte).ok_or_else(incompatible)?;
    let mut maximum = 0;
    for field in FIELDS {
        for operation_id in [None, Some("")] {
            let mut count = Count::default();
            encode(&mut count, field, operation_id).map_err(|_| incompatible())?;
            let size = if operation_id.is_some() {
                count.0.checked_add(payload).ok_or_else(incompatible)?
            } else { count.0 };
            maximum = maximum.max(size);
        }
    }
    Ok(maximum)
}

pub(super) fn response(
    original_status: u16,
    field: Option<LimitingField>,
    operation_id: Option<&str>,
) -> super::UiResponse {
    let mut body = String::new();
    // String formatting is infallible; actual IDs already passed route/form
    // admission, and RuntimeLimits validated the complete encoding capacity.
    encode(&mut body, field, operation_id).expect("compact product failure encoding");
    super::UiResponse {
        status: if original_status >= 400 || original_status == 202 { original_status } else { 502 },
        content_type: "text/plain; charset=utf-8".into(),
        body,
        location: None,
        limiting_field: field,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimum_counts_the_actual_worst_admitted_encoding_without_maximum_allocations() {
        let path_bytes = 64;
        let id = "\"".repeat(path_bytes - OPERATION_PREFIX.len());
        let minimum = minimum_bytes(path_bytes, 24).unwrap();
        let worst = response(503, Some(LimitingField::CollaborationCursor), Some(&id));
        assert_eq!(worst.body.len(), minimum);
        let varied = "quote\" slash\\ & <é>\n\r\t";
        let response = response(404, None, Some(varied));
        assert!(response.body.len() <= minimum);
        let encoded = response.body.lines().find_map(|line| line.strip_prefix("Original operation: ")).unwrap();
        assert_eq!(serde_json::from_str::<String>(encoded).unwrap(), varied);
        assert!(minimum_bytes(usize::MAX, 1).is_err());
        assert!(minimum_bytes(1, usize::MAX).is_err());
    }
}
