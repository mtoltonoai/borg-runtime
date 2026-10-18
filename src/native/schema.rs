//! Admission is deliberately separate from JSON Schema validation. It bounds the
//! work we ask the supplied validator to perform; it never returns a valid result.
use crate::limits::NativeLimits;
use super::format::{FormatIssue, SchemaRule};
use serde_json::Value;
use std::io::{self, Write};

pub(super) struct Schema {
    validator: jsonschema::Validator,
    work: usize,
    policy: NativeLimits,
}

struct Count(usize, usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("bound"))?;
        if self.0 > self.1 {
            return Err(io::Error::other("bound"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn measure(
    value: &Value,
    limit: usize,
    policy: &NativeLimits,
    field: &str,
) -> Result<(usize, usize), String> {
    let mut todo = vec![(value, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = todo.pop() {
        nodes += 1;
        if nodes > policy.json_nodes {
            return Err("native_json_complexity_bound_native_json_nodes".into());
        }
        if depth > policy.json_depth {
            return Err("native_json_complexity_bound_native_json_depth".into());
        }
        match value {
            Value::Array(items) => {
                if items.len().saturating_add(todo.len()).saturating_add(nodes) > policy.json_nodes
                {
                    return Err("native_json_complexity_bound_native_json_nodes".into());
                }
                todo.extend(items.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(items) => {
                if items.len().saturating_add(todo.len()).saturating_add(nodes) > policy.json_nodes
                {
                    return Err("native_json_complexity_bound_native_json_nodes".into());
                }
                todo.extend(items.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    let mut count = Count(0, limit);
    serde_json::to_writer(&mut count, value)
        .map_err(|_| format!("native_json_byte_bound_{field}"))?;
    Ok((count.0, nodes))
}

impl Schema {
    pub(super) fn compile(schema: &Value, policy: &NativeLimits) -> Result<Self, String> {
        let (bytes, _) = measure(schema, policy.schema_bytes, policy, "native_schema_bytes")?;
        let mut work = 0;
        visit(schema, schema, &mut Vec::new(), &mut work, policy)?;
        if bytes
            .checked_mul(work)
            .is_none_or(|work| work > policy.validation_work)
        {
            return Err("native_schema_work_bound_native_validation_work".into());
        }
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .with_pattern_options(
                jsonschema::PatternOptions::regex()
                    .size_limit(policy.regex_bytes)
                    .dfa_size_limit(policy.regex_dfa_bytes),
            )
            .build(schema)
            // jsonschema0.50.1 erases both regex syntax and compiled-size
            // errors to Format("regex"). Do not guess a limiting field or
            // disclose the schema/pattern from that ambiguous error.
            .map_err(|_| "native_schema_invalid_or_unsupported".to_string())?;
        Ok(Self {
            validator,
            work,
            policy: policy.clone(),
        })
    }

    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# The native executor MUST validate a result against that schema before accepting it.
    // Native component scope: actual bounded Draft202012 validator; a valid value is not controller acceptance or authority.
    pub(super) fn validate(&self, value: &Value, limit: usize, field: &str) -> Result<(), String> {
        self.admit(value, limit, field)?;
        if !self.validator.is_valid(value) {
            return Err("native_output_schema_mismatch".into());
        }
        Ok(())
    }

    /// Classify one actual validator failure only after the identical admission
    /// guards. Infrastructure failures are not repairable schema observations.
    pub(super) fn final_issue(
        &self, value: &Value, limit: usize, field: &str,
    ) -> Result<Option<FormatIssue>, String> {
        self.admit(value, limit, field)?;
        match self.validator.validate(value) {
            Ok(()) => Ok(None),
            Err(error) => Ok(Some(FormatIssue::Schema {
                rule: schema_rule(error.kind(), self.policy.validation_work)?,
            })),
        }
    }

    fn admit(&self, value: &Value, limit: usize, field: &str) -> Result<(), String> {
        let (bytes, nodes) = measure(value, limit, &self.policy, field)?;
        if bytes
            .checked_mul(nodes)
            .and_then(|work| work.checked_mul(self.work))
            .is_none_or(|work| work > self.policy.validation_work)
        {
            return Err("native_validation_work_bound_native_validation_work".into());
        }
        Ok(())
    }
}

/// Inspect compound failure causes without formatting any foreign error. An
/// infrastructure failure nested under an applicator remains infrastructure.
/// This traversal uses the same accepted validation-work budget, not a new cap.
fn schema_rule(
    kind: &jsonschema::error::ValidationErrorKind,
    work_limit: usize,
) -> Result<SchemaRule, String> {
    use jsonschema::error::ValidationErrorKind as K;
    let mut remaining = work_limit;
    let mut pending = vec![kind];
    while let Some(current) = pending.pop() {
        remaining = remaining.checked_sub(1)
            .ok_or("native_validation_work_bound_native_validation_work")?;
        match current {
            K::BacktrackLimitExceeded { .. } | K::RegexEngineFailure { .. }
            | K::Referencing(_) | K::Custom { .. } => {
                return Err("native_validation_infrastructure_failed".into());
            }
            K::AnyOf { context } | K::OneOfMultipleValid { context }
            | K::OneOfNotValid { context } => {
                for branch in context {
                    for error in branch {
                        if pending.len() >= remaining {
                            return Err("native_validation_work_bound_native_validation_work".into());
                        }
                        pending.push(error.kind());
                    }
                }
            }
            K::PropertyNames { error } => {
                if pending.len() >= remaining {
                    return Err("native_validation_work_bound_native_validation_work".into());
                }
                pending.push(error.kind());
            }
            _ => {}
        }
    }
    Ok(match kind {
        K::AdditionalItems { .. } => SchemaRule::AdditionalItems,
        K::AdditionalProperties { .. } => SchemaRule::AdditionalProperties,
        K::AnyOf { .. } => SchemaRule::AnyOf,
        K::Constant { .. } => SchemaRule::Const,
        K::Contains => SchemaRule::Contains,
        K::ContentEncoding { .. } | K::FromUtf8 { .. } => SchemaRule::ContentEncoding,
        K::ContentMediaType { .. } => SchemaRule::ContentMediaType,
        K::Enum { .. } => SchemaRule::Enum,
        K::ExclusiveMaximum { .. } => SchemaRule::ExclusiveMaximum,
        K::ExclusiveMinimum { .. } => SchemaRule::ExclusiveMinimum,
        K::FalseSchema => SchemaRule::FalseSchema,
        K::Format { .. } => SchemaRule::Format,
        K::MaxItems { .. } => SchemaRule::MaxItems,
        K::Maximum { .. } => SchemaRule::Maximum,
        K::MaxLength { .. } => SchemaRule::MaxLength,
        K::MaxProperties { .. } => SchemaRule::MaxProperties,
        K::MinItems { .. } => SchemaRule::MinItems,
        K::Minimum { .. } => SchemaRule::Minimum,
        K::MinLength { .. } => SchemaRule::MinLength,
        K::MinProperties { .. } => SchemaRule::MinProperties,
        K::MultipleOf { .. } => SchemaRule::MultipleOf,
        K::Not { .. } => SchemaRule::Not,
        K::OneOfMultipleValid { .. } | K::OneOfNotValid { .. } => SchemaRule::OneOf,
        K::Pattern { .. } => SchemaRule::Pattern,
        K::PropertyNames { .. } => SchemaRule::PropertyNames,
        K::Required { .. } => SchemaRule::Required,
        K::Type { .. } => SchemaRule::Type,
        K::UnevaluatedItems { .. } => SchemaRule::UnevaluatedItems,
        K::UnevaluatedProperties { .. } => SchemaRule::UnevaluatedProperties,
        K::UniqueItems => SchemaRule::UniqueItems,
        K::BacktrackLimitExceeded { .. } | K::RegexEngineFailure { .. }
        | K::Referencing(_) | K::Custom { .. } => {
            return Err("native_validation_infrastructure_failed".into());
        }
    })
}

fn visit<'a>(
    node: &'a Value,
    root: &'a Value,
    active: &mut Vec<&'a Value>,
    work: &mut usize,
    policy: &NativeLimits,
) -> Result<(), String> {
    *work += 1;
    if *work > policy.schema_expansion {
        return Err("native_schema_work_bound_native_schema_expansion".into());
    }
    if active.len() > policy.json_depth {
        return Err("native_schema_work_bound_native_json_depth".into());
    }
    if active.iter().any(|v| std::ptr::eq(*v, node)) {
        return Err("native_schema_recursive_reference".into());
    }
    if node.is_boolean() {
        return Ok(());
    }
    let obj = node.as_object().ok_or("native_schema_invalid")?;
    active.push(node);
    if let Some(draft) = obj.get("$schema") {
        if !matches!(
            draft.as_str(),
            Some(
                "https://json-schema.org/draft/2020-12/schema"
                    | "https://json-schema.org/draft/2020-12/schema#"
            )
        ) {
            return Err("native_schema_draft_unsupported".into());
        }
    }
    // Internal JSON pointers only. No base-URI, anchor or dynamic-ref machinery,
    // and no network/file retriever can be requested through a schema.
    for forbidden in [
        "$id",
        "$anchor",
        "$dynamicAnchor",
        "$dynamicRef",
        "$recursiveRef",
        "$recursiveAnchor",
        "$vocabulary",
    ] {
        if obj.contains_key(forbidden) {
            return Err("native_schema_reference_profile".into());
        }
    }
    if let Some(reference) = obj.get("$ref") {
        let reference = reference
            .as_str()
            .ok_or("native_schema_invalid_reference")?;
        if reference != "#" && !reference.starts_with("#/") {
            return Err("native_schema_external_reference".into());
        }
        let target = root
            .pointer(&reference[1..])
            .ok_or("native_schema_missing_reference")?;
        visit(target, root, active, work, policy)?;
    }
    for name in [
        "properties",
        "patternProperties",
        "$defs",
        "definitions",
        "dependentSchemas",
    ] {
        if let Some(children) = obj.get(name) {
            let children = children.as_object().ok_or("native_schema_invalid")?;
            for child in children.values() {
                visit(child, root, active, work, policy)?;
            }
        }
    }
    for name in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(children) = obj.get(name) {
            for child in children.as_array().ok_or("native_schema_invalid")? {
                visit(child, root, active, work, policy)?;
            }
        }
    }
    for name in [
        "items",
        "additionalProperties",
        "unevaluatedProperties",
        "additionalItems",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "unevaluatedItems",
        "contentSchema",
    ] {
        if let Some(child) = obj.get(name) {
            visit(child, root, active, work, policy)?;
        }
    }
    active.pop();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> NativeLimits {
        serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap()
    }

    #[test]
    fn erased_regex_size_and_syntax_failures_do_not_invent_a_limiting_field() {
        let configured = policy();
        let schema = serde_json::json!({
            "type": "string",
            "pattern": "^([a-z]{2,8}[0-9]{1,4})+$"
        });
        // The same valid nonliteral pattern works under the accepted seed bound.
        let compiled = Schema::compile(&schema, &configured).unwrap();
        compiled
            .validate(&serde_json::json!("abc12"), 32, "test_bytes")
            .unwrap();
        let mut small = configured.clone();
        small.regex_bytes = 1;
        let size_error = Schema::compile(&schema, &small).err().unwrap();
        let syntax_error = Schema::compile(
            &serde_json::json!({"type": "string", "pattern": "[private-fixture-pattern"}),
            &configured,
        )
        .err()
        .unwrap();
        for error in [size_error, syntax_error] {
            assert_eq!(error, "native_schema_invalid_or_unsupported");
        }
    }

    #[test]
    fn regex_dfa_cache_capacity_does_not_create_a_validation_refusal() {
        let mut configured = policy();
        configured.regex_dfa_bytes = 1;
        let compiled = Schema::compile(
            &serde_json::json!({"type":"string", "pattern":"^[a-z]+[0-9]+$"}),
            &configured,
        )
        .unwrap();
        compiled
            .validate(&serde_json::json!("abc123"), 32, "test_bytes")
            .unwrap();
        assert_eq!(
            compiled
                .validate(&serde_json::json!("abc"), 32, "test_bytes")
                .unwrap_err(),
            "native_output_schema_mismatch"
        );
    }

    #[test]
    fn schema_refusals_identify_the_limiting_policy_field() {
        let mut configured = policy();
        configured.schema_bytes = 3;
        assert_eq!(
            Schema::compile(&Value::Bool(true), &configured)
                .err()
                .as_deref(),
            Some("native_json_byte_bound_native_schema_bytes")
        );

        let mut configured = policy();
        configured.schema_expansion = 1;
        assert_eq!(
            Schema::compile(&serde_json::json!({"not": false}), &configured)
                .err()
                .as_deref(),
            Some("native_schema_work_bound_native_schema_expansion")
        );

        let mut configured = policy();
        configured.validation_work = 3;
        assert_eq!(
            Schema::compile(&Value::Bool(true), &configured)
                .err()
                .as_deref(),
            Some("native_schema_work_bound_native_validation_work")
        );
        configured.validation_work = 4;
        let compiled = Schema::compile(&Value::Bool(true), &configured).unwrap();
        assert_eq!(
            compiled
                .validate(&serde_json::json!([null]), 6, "test_bytes")
                .unwrap_err(),
            "native_validation_work_bound_native_validation_work"
        );
        assert_eq!(
            measure(&Value::Bool(true), 3, &policy(), "test_bytes").unwrap_err(),
            "native_json_byte_bound_test_bytes"
        );
    }

    #[test]
    // Native component scope: measurement and compiled validation retain metadata depth admission.
    fn json_depth_uses_metadata_above_and_below_the_seed_limit() {
        let seed = policy();
        let depth = seed.json_depth + 1;
        let value = (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let bytes = serde_json::to_vec(&value).unwrap().len();
        let mut configured = seed.clone();
        configured.json_depth = depth;

        assert_eq!(
            measure(&value, bytes, &configured, "test_bytes").unwrap(),
            (bytes, depth + 1)
        );
        assert_eq!(
            measure(&value, bytes, &seed, "test_bytes").unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );
        let compiled = Schema::compile(&Value::Bool(true), &configured).unwrap();
        configured.json_depth = 1;
        // A compiled validator keeps the policy selected when it was built.
        compiled.validate(&value, bytes, "test_bytes").unwrap();

        let small = serde_json::json!([[0]]);
        let small_bytes = serde_json::to_vec(&small).unwrap().len();
        measure(&small, small_bytes, &seed, "test_bytes").unwrap();
        assert_eq!(
            measure(&small, small_bytes, &configured, "test_bytes").unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );
        let restricted = Schema::compile(&Value::Bool(true), &configured).unwrap();
        assert_eq!(
            restricted
                .validate(&small, small_bytes, "test_bytes")
                .unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );
    }

    #[test]
    // Native component scope: measurement and compiled validation retain metadata node admission.
    fn json_nodes_uses_metadata_above_and_below_the_seed_limit() {
        let seed = policy();
        let value = Value::Array(vec![Value::Null; seed.json_nodes]);
        let bytes = serde_json::to_vec(&value).unwrap().len();
        let nodes = seed.json_nodes + 1;
        let mut configured = seed.clone();
        configured.json_nodes = nodes;
        // Isolate node admission from the separate validation-work budget.
        configured.validation_work = bytes.checked_mul(nodes).unwrap();

        assert_eq!(
            measure(&value, bytes, &configured, "test_bytes").unwrap(),
            (bytes, nodes)
        );
        assert_eq!(
            measure(&value, bytes, &seed, "test_bytes").unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );
        let compiled = Schema::compile(&Value::Bool(true), &configured).unwrap();
        configured.json_nodes = 2;
        // A compiled validator keeps the policy selected when it was built.
        compiled.validate(&value, bytes, "test_bytes").unwrap();

        let small = serde_json::json!([0, 0]);
        let small_bytes = serde_json::to_vec(&small).unwrap().len();
        measure(&small, small_bytes, &seed, "test_bytes").unwrap();
        assert_eq!(
            measure(&small, small_bytes, &configured, "test_bytes").unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );
        let restricted = Schema::compile(&Value::Bool(true), &configured).unwrap();
        assert_eq!(
            restricted
                .validate(&small, small_bytes, "test_bytes")
                .unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );
    }
}

#[cfg(test)]
mod diagnostic_work_budget_tests {
    use super::*;
    use serde_json::json;

    fn accepted_profile() -> (Value, NativeLimits) {
        let profile: Value = serde_json::from_str(include_str!(
            "../../metadata/operational-diagnostic.json"
        )).unwrap();
        let policy = serde_json::from_value(
            profile["definition"]["limit_overrides"]["native"].clone()
        ).unwrap();
        (profile["output_schema"].clone(), policy)
    }

    fn report() -> Value {
        // Synthetic structurally complete report, not the unretained actual06
        // provider response and not evidence of its claimed application.
        json!({
            "disposition": "applied_unmeasured",
            "origin_task_id": "controlled-origin",
            "target_agent_id": "controlled-target",
            "reason": "A configured limit refused the completed original work.",
            "change": {
                "operation_id": "original-apply",
                "controller_revision": 48,
                "previous_revision": "old",
                "selected_revision": "new",
                "global_revision": "global",
                "policy_revision": "policy",
                "changed_fields": ["collaboration.complete_result_bytes"],
                "classification": "widening",
                "effective_for": "future_submissions"
            },
            "measurement": {
                "status": "no_later_task",
                "baseline": null,
                "later": null,
                "comparison": "not_comparable",
                "causal_improvement_claim": false,
                "limitations": ["No independent later work was observed."]
            }
        })
    }

    fn pad_reason_to(value: &mut Value, size: usize) {
        let current = serde_json::to_vec(value).unwrap().len();
        assert!(current <= size);
        let reason = value["reason"].as_str().unwrap().to_owned();
        value["reason"] = Value::String(reason + &"x".repeat(size - current));
        assert_eq!(serde_json::to_vec(value).unwrap().len(), size);
    }

    #[test]
    fn diagnostic_work_budget_rejects_valid_report_at_exact_existing_boundary() {
        let (output_schema, mut policy) = accepted_profile();
        // Historical actual06 accepted the previous 8Mi work score. Keep that
        // frozen predecessor policy explicit while the current profile evolves.
        policy.validation_work = 8_388_608;
        let compiled = Schema::compile(&output_schema, &policy).unwrap();
        assert_eq!(compiled.work, 97);
        let mut value = report();
        let (_, nodes) = measure(&value, 65_536, &policy, "task_output_bytes").unwrap();
        assert_eq!(nodes, 24);
        let boundary = policy.validation_work / nodes / compiled.work;
        assert_eq!(boundary, 3_603);
        pad_reason_to(&mut value, boundary);
        compiled.validate(&value, 65_536, "task_output_bytes").unwrap();
        pad_reason_to(&mut value, boundary + 1);
        assert!(compiled.validator.is_valid(&value));
        assert_eq!(compiled.validate(&value, 65_536, "task_output_bytes").unwrap_err(),
                   "native_validation_work_bound_native_validation_work");
        let mut sufficient = policy.clone();
        sufficient.validation_work = (boundary + 1) * nodes * compiled.work;
        let corrected = Schema::compile(&output_schema, &sufficient).unwrap();
        corrected.validate(&value, 65_536, "task_output_bytes").unwrap();
        value["change"] = Value::Null;
        assert_eq!(corrected.validate(&value, 65_536, "task_output_bytes").unwrap_err(),
                   "native_output_schema_mismatch");
    }

    #[test]
    fn diagnostic_work_budget_exact_output_envelope_keeps_all_other_guards() {
        let (output_schema, mut policy) = accepted_profile();
        let initial = Schema::compile(&output_schema, &policy).unwrap();
        assert_eq!(initial.work, 97);
        assert_eq!(policy.json_nodes, 4_096);
        let output_bytes = 65_536usize;
        let exact_budget = output_bytes.checked_mul(policy.json_nodes)
            .and_then(|v| v.checked_mul(initial.work)).unwrap();
        assert_eq!(exact_budget, 26_038_239_232);
        assert_eq!(policy.validation_work, exact_budget);
        let mut value = report();
        value["measurement"]["limitations"] = json!([]);
        let (_, fixed_nodes) = measure(&value, output_bytes, &policy, "task_output_bytes").unwrap();
        value["measurement"]["limitations"] = json!(vec![""; policy.json_nodes - fixed_nodes]);
        pad_reason_to(&mut value, output_bytes);
        assert_eq!(measure(&value, output_bytes, &policy, "task_output_bytes").unwrap(),
                   (output_bytes, policy.json_nodes));
        assert!(initial.validator.is_valid(&value));
        policy.validation_work = exact_budget - 1;
        let below = Schema::compile(&output_schema, &policy).unwrap();
        assert_eq!(below.validate(&value, output_bytes, "task_output_bytes").unwrap_err(),
                   "native_validation_work_bound_native_validation_work");
        policy.validation_work = exact_budget;
        policy.validate().unwrap();
        let complete = Schema::compile(&output_schema, &policy).unwrap();
        complete.validate(&value, output_bytes, "task_output_bytes").unwrap();
        let mut excessive_bytes = value.clone();
        pad_reason_to(&mut excessive_bytes, output_bytes + 1);
        assert_eq!(complete.validate(&excessive_bytes, output_bytes, "task_output_bytes").unwrap_err(),
                   "native_json_byte_bound_task_output_bytes");
        let mut excessive_nodes = value.clone();
        excessive_nodes["measurement"]["limitations"].as_array_mut().unwrap().push(json!(""));
        assert_eq!(complete.validate(&excessive_nodes, output_bytes, "task_output_bytes").unwrap_err(),
                   "native_json_complexity_bound_native_json_nodes");
        let mut contradiction = value;
        contradiction["change"] = Value::Null;
        assert_eq!(complete.validate(&contradiction, output_bytes, "task_output_bytes").unwrap_err(),
                   "native_output_schema_mismatch");
    }
}

#[cfg(test)]
mod final_format_tests {
    use super::*;
    use serde_json::json;

    fn policy() -> NativeLimits {
        serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap()
    }

    #[test]
    fn actual_schema_issues_are_specific_without_instance_or_schema_values() {
        for (schema, value, rule) in [
            (json!({"type":"integer"}), json!("private_instance"), SchemaRule::Type),
            (json!({"required":["private_property"]}), json!({}), SchemaRule::Required),
            (json!({"enum":["private_schema_value"]}), json!("private_instance"), SchemaRule::Enum),
        ] {
            let compiled = Schema::compile(&schema, &policy()).unwrap();
            let issue = compiled.final_issue(&value, 1024, "task_output_bytes").unwrap().unwrap();
            assert_eq!(issue, FormatIssue::Schema { rule });
            assert_eq!(issue.stable_error(), "native_output_schema_mismatch");
            assert_eq!(compiled.validate(&value, 1024, "task_output_bytes").unwrap_err(), issue.stable_error());
            assert!(issue.feedback().contains(match rule {
                SchemaRule::Type => "type", SchemaRule::Required => "required", SchemaRule::Enum => "enum",
                _ => unreachable!(),
            }));
            let rendered = issue.feedback() + &serde_json::to_string(&issue).unwrap();
            for secret in ["private_instance", "private_property", "private_schema_value"] {
                assert!(!rendered.contains(secret));
            }
        }
        let compiled = Schema::compile(&json!({"type":"integer"}), &policy()).unwrap();
        assert_eq!(compiled.final_issue(&json!(7), 1024, "task_output_bytes").unwrap(), None);
        compiled.validate(&json!(7), 1024, "task_output_bytes").unwrap();
    }

    #[test]
    fn final_schema_admission_precedes_mismatch_classification() {
        let value = json!("private_instance");
        let compiled = Schema::compile(&json!(false), &policy()).unwrap();
        assert_eq!(compiled.final_issue(&value, 1, "task_output_bytes").unwrap_err(),
            "native_json_byte_bound_task_output_bytes");
        assert_eq!(compiled.validate(&value, 1, "task_output_bytes").unwrap_err(),
            "native_json_byte_bound_task_output_bytes");
        let mut p = policy(); p.validation_work = 8;
        let compiled = Schema::compile(&json!(false), &p).unwrap();
        assert_eq!(compiled.final_issue(&value, 1024, "task_output_bytes").unwrap_err(),
            "native_validation_work_bound_native_validation_work");
        assert_eq!(compiled.validate(&value, 1024, "task_output_bytes").unwrap_err(),
            "native_validation_work_bound_native_validation_work");

        let value = json!([[0]]);
        for (nodes, depth, expected) in [
            (2, 8, "native_json_complexity_bound_native_json_nodes"),
            (8, 1, "native_json_complexity_bound_native_json_depth"),
        ] {
            let mut p = policy(); p.json_nodes = nodes; p.json_depth = depth;
            let compiled = Schema::compile(&json!(false), &p).unwrap();
            assert_eq!(compiled.final_issue(&value, 1024, "task_output_bytes").unwrap_err(), expected);
            assert_eq!(compiled.validate(&value, 1024, "task_output_bytes").unwrap_err(), expected);
        }
    }

    #[test]
    fn nested_infrastructure_failures_are_never_schema_repair_observations() {
        use jsonschema::error::ValidationErrorKind as K;
        let limit = policy().validation_work;
        let direct = K::RegexEngineFailure { message: "private_engine_message".into() };
        assert_eq!(schema_rule(&direct, limit).unwrap_err(), "native_validation_infrastructure_failed");
        let nested = K::AnyOf { context: vec![vec![jsonschema::ValidationError::custom("private_custom_message")]] };
        assert_eq!(schema_rule(&nested, limit).unwrap_err(), "native_validation_infrastructure_failed");
        let property = K::PropertyNames { error: Box::new(jsonschema::ValidationError::custom("private_property_message")) };
        assert_eq!(schema_rule(&property, limit).unwrap_err(), "native_validation_infrastructure_failed");
        assert_eq!(schema_rule(&K::Required { property: json!("private_property") }, limit).unwrap(), SchemaRule::Required);
    }
}
