//! Admission is deliberately separate from JSON Schema validation. It bounds the
//! work we ask the supplied validator to perform; it never returns a valid result.
use crate::limits::NativeLimits;
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
        let (bytes, nodes) = measure(value, limit, &self.policy, field)?;
        if bytes
            .checked_mul(nodes)
            .and_then(|work| work.checked_mul(self.work))
            .is_none_or(|work| work > self.policy.validation_work)
        {
            return Err("native_validation_work_bound_native_validation_work".into());
        }
        if !self.validator.is_valid(value) {
            return Err("native_output_schema_mismatch".into());
        }
        Ok(())
    }
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
