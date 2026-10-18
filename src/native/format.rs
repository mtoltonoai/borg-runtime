//! Closed, value-free observations of an invalid completed final answer.
//! These values do not establish provider completion or authorize another request.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FormatIssue {
    InvalidJson { line: u64, column: u64 },
    DuplicateKey {},
    Schema { rule: SchemaRule },
}

/// Only validator-owned keywords are represented. No instance, property name,
/// path, expected value, pattern, custom message or reference is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaRule {
    AdditionalItems,
    AdditionalProperties,
    AnyOf,
    Const,
    Contains,
    ContentEncoding,
    ContentMediaType,
    Enum,
    ExclusiveMaximum,
    ExclusiveMinimum,
    FalseSchema,
    Format,
    MaxItems,
    Maximum,
    MaxLength,
    MaxProperties,
    MinItems,
    Minimum,
    MinLength,
    MinProperties,
    MultipleOf,
    Not,
    OneOf,
    Pattern,
    PropertyNames,
    Required,
    Type,
    UnevaluatedItems,
    UnevaluatedProperties,
    UniqueItems,
}

impl SchemaRule {
    fn keyword(self) -> &'static str {
        match self {
            Self::AdditionalItems => "additionalItems",
            Self::AdditionalProperties => "additionalProperties",
            Self::AnyOf => "anyOf",
            Self::Const => "const",
            Self::Contains => "contains",
            Self::ContentEncoding => "contentEncoding",
            Self::ContentMediaType => "contentMediaType",
            Self::Enum => "enum",
            Self::ExclusiveMaximum => "exclusiveMaximum",
            Self::ExclusiveMinimum => "exclusiveMinimum",
            Self::FalseSchema => "false schema",
            Self::Format => "format",
            Self::MaxItems => "maxItems",
            Self::Maximum => "maximum",
            Self::MaxLength => "maxLength",
            Self::MaxProperties => "maxProperties",
            Self::MinItems => "minItems",
            Self::Minimum => "minimum",
            Self::MinLength => "minLength",
            Self::MinProperties => "minProperties",
            Self::MultipleOf => "multipleOf",
            Self::Not => "not",
            Self::OneOf => "oneOf",
            Self::Pattern => "pattern",
            Self::PropertyNames => "propertyNames",
            Self::Required => "required",
            Self::Type => "type",
            Self::UnevaluatedItems => "unevaluatedItems",
            Self::UnevaluatedProperties => "unevaluatedProperties",
            Self::UniqueItems => "uniqueItems",
        }
    }
}

impl FormatIssue {
    pub fn feedback(&self) -> String {
        match self {
            Self::InvalidJson { line, column } => format!(
                "The completed final answer was not valid JSON at line {line}, column {column}. Return one complete JSON value without surrounding text, conforming to the original task output schema."
            ),
            Self::DuplicateKey {} => "The completed final answer contained a duplicate JSON object key. Return one complete JSON value with unique object keys, conforming to the original task output schema.".into(),
            Self::Schema { rule } => format!(
                "The completed final answer did not satisfy the original task output schema's {} constraint. Correct the answer to satisfy the entire original schema; the schema and task are unchanged.",
                rule.keyword()
            ),
        }
    }

    pub fn stable_error(&self) -> &'static str {
        match self {
            Self::InvalidJson { .. } => "native_output_not_json",
            Self::DuplicateKey {} => "native_json_duplicate_key",
            Self::Schema { .. } => "native_output_schema_mismatch",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_issue_wire_is_closed_and_round_trips() {
        for issue in [
            FormatIssue::InvalidJson { line: 2, column: 7 },
            FormatIssue::DuplicateKey {},
            FormatIssue::Schema { rule: SchemaRule::Required },
        ] {
            let encoded = serde_json::to_value(issue).unwrap();
            assert_eq!(serde_json::from_value::<FormatIssue>(encoded.clone()).unwrap(), issue);
            let mut extra = encoded;
            extra["instance"] = json!("private-output");
            assert!(serde_json::from_value::<FormatIssue>(extra).is_err());
        }
        assert!(serde_json::from_value::<FormatIssue>(json!({"kind":"unknown"})).is_err());
        assert!(serde_json::from_value::<FormatIssue>(json!({"kind":"schema","rule":"custom"})).is_err());
        assert!(serde_json::from_value::<FormatIssue>(json!({"kind":"invalid_json","line":-1,"column":0})).is_err());
    }
}
