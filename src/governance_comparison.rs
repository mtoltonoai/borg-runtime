//! Pure effective-value comparison for the existing twelve-field update path.
//! The caller still enforces allowed fields, authority, currentness and complete
//! configuration validity. A comparison verdict is not an invocation grant.
use crate::governance::{
    ComparisonRule, ConfigurationReason, EffectiveChange, FieldComparison,
};
use crate::limits::{DefinitionLimitField, ExecutionLimits};
use std::collections::{BTreeMap, BTreeSet};

/// Compare values resolved against the same active defaults. Identity, revision
/// and provenance are deliberately absent from this interface.
///
/// `changed` is the existing typed request: a BTreeMap cannot contain duplicate
/// keys. Its decoder must reject ambiguous input before constructing the map.
/// The trusted configured field list is checked for duplicates here. It selects
/// the permitted widening class; it does not replace the caller's allowed-field
/// check for neutral or narrowing requests.
///
/// This function neither resolves defaults nor calls clock-dependent validation.
/// The caller validates the complete resulting configuration before committing.
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A governed configuration field without a sound comparison rule MUST be treated as widening.
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# Version identifiers and provenance metadata MUST NOT affect the effective-configuration comparison.
pub(crate) fn compare(
    before: &ExecutionLimits,
    after: &ExecutionLimits,
    changed: &BTreeMap<DefinitionLimitField, usize>,
    configured_fields: &[DefinitionLimitField],
    allow_configured_widening: bool,
) -> Result<(Vec<FieldComparison>, bool, ConfigurationReason), String> {
    if changed.is_empty() {
        return Err("definition-limit comparison requires at least one requested field".into());
    }
    let configured: BTreeSet<_> = configured_fields.iter().copied().collect();
    if configured.len() != configured_fields.len() {
        return Err("definition-limit comparison has duplicate configured fields".into());
    }

    // Whole-value equality covers both the requested values and every untouched
    // effective value, including fields outside the supported change class.
    let mut expected = before.clone();
    for (&field, &value) in changed {
        if value == 0 {
            return Err(format!("definition-limit comparison requires a positive {field:?}"));
        }
        field.set(&mut expected, value);
    }
    if &expected != after {
        return Err("definition-limit comparison differs from the exact requested effective values".into());
    }

    let mut comparisons = Vec::with_capacity(changed.len());
    let mut narrowing = false;
    let mut widening = false;
    let mut widening_allowed = true;
    for (&field, &value) in changed {
        // An added protocol field must select its rule explicitly. There is no
        // wildcard that silently enrolls a new field in the resource class.
        let known_rule = match field {
            DefinitionLimitField::NativeContextBytes
            | DefinitionLimitField::NativeRequestBytes
            | DefinitionLimitField::NativeHelperValueBytes
            | DefinitionLimitField::NativeResponseWireBytes
            | DefinitionLimitField::CodingModelContextWindow
            | DefinitionLimitField::NativeOutputBytes
            | DefinitionLimitField::NativeResultBytes
            | DefinitionLimitField::NativeMaxOutputTokens
            | DefinitionLimitField::CodingCommandOutputBytes
            | DefinitionLimitField::CodingSummaryChars
            | DefinitionLimitField::CodingSummaryBytes
            | DefinitionLimitField::CollaborationCompleteResultBytes => {
                ComparisonRule::ResourceAllowance
            }
        };
        let previous = field.value(before);
        let (rule, change) = match previous {
            None => (ComparisonRule::ConservativeWidening, EffectiveChange::Widening),
            Some(old) if value < old => (known_rule, EffectiveChange::Narrowing),
            Some(old) if value > old => (known_rule, EffectiveChange::Widening),
            Some(_) => (known_rule, EffectiveChange::Neutral),
        };
        match change {
            EffectiveChange::Neutral => {}
            EffectiveChange::Narrowing => narrowing = true,
            EffectiveChange::Widening => {
                widening = true;
                widening_allowed &= allow_configured_widening && configured.contains(&field);
            }
        }
        comparisons.push(FieldComparison { field, before: previous, after: value, rule, change });
    }
    let reason = if widening {
        if widening_allowed {
            ConfigurationReason::ConfiguredWideningClass
        } else {
            ConfigurationReason::WideningNotConfigured
        }
    } else if narrowing {
        ConfigurationReason::NarrowsResourceAllowance
    } else {
        ConfigurationReason::SameEffectiveValues
    };
    Ok((comparisons, !widening || widening_allowed, reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::RuntimeLimits;

    const FIELDS: [DefinitionLimitField; 12] = [
        DefinitionLimitField::NativeContextBytes,
        DefinitionLimitField::NativeRequestBytes,
        DefinitionLimitField::NativeHelperValueBytes,
        DefinitionLimitField::NativeResponseWireBytes,
        DefinitionLimitField::CodingModelContextWindow,
        DefinitionLimitField::NativeOutputBytes,
        DefinitionLimitField::NativeResultBytes,
        DefinitionLimitField::NativeMaxOutputTokens,
        DefinitionLimitField::CodingCommandOutputBytes,
        DefinitionLimitField::CodingSummaryChars,
        DefinitionLimitField::CodingSummaryBytes,
        DefinitionLimitField::CollaborationCompleteResultBytes,
    ];

    fn limits() -> ExecutionLimits {
        let mut values = RuntimeLimits::seed().unwrap().execution;
        values.native.context_bytes = 65_536;
        values.native.output_bytes = 16_384;
        values.native.result_bytes = 16_384;
        values.native.request_bytes = 262_144;
        values.native.response_wire_bytes = 1_048_576;
        values.native.helper_value_bytes = 524_288;
        values.native.max_output_tokens = 4_096;
        values.coding.model_context_window = Some(65_536);
        values.coding.command_output_bytes = 16_384;
        values.coding.summary_chars = 8_192;
        values.coding.summary_bytes = 8_192;
        values.collaboration.complete_result_bytes = 262_144;
        values.validate().unwrap();
        values
    }

    fn apply(
        before: &ExecutionLimits,
        changes: &BTreeMap<DefinitionLimitField, usize>,
    ) -> ExecutionLimits {
        let mut after = before.clone();
        for (&field, &value) in changes {
            field.set(&mut after, value);
        }
        after
    }

    #[test]
    //= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
    //= type=test
    //# A governed configuration change MUST compare its effective before and after values composed with the same active defaults.
    fn all_twelve_fields_use_resource_allowance_direction() {
        let before = limits();
        for field in FIELDS {
            let original = field.value(&before).unwrap();
            for (value, change, reason) in [
                (original - 1, EffectiveChange::Narrowing, ConfigurationReason::NarrowsResourceAllowance),
                (original, EffectiveChange::Neutral, ConfigurationReason::SameEffectiveValues),
                (original + 1, EffectiveChange::Widening, ConfigurationReason::ConfiguredWideningClass),
            ] {
                let request = BTreeMap::from([(field, value)]);
                let after = apply(&before, &request);
                after.validate().unwrap();
                let actual = compare(&before, &after, &request, &FIELDS, true).unwrap();
                assert_eq!(actual, (vec![FieldComparison {
                    field, before: Some(original), after: value,
                    rule: ComparisonRule::ResourceAllowance, change,
                }], true, reason));
            }
        }
    }

    #[test]
    //= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
    //= type=test
    //# Version identifiers and provenance metadata MUST NOT affect the effective-configuration comparison.
    fn metadata_revision_does_not_change_an_equal_effective_comparison() {
        let mut old = RuntimeLimits::seed().unwrap();
        old.execution = limits();
        let mut selected = old.clone();
        selected.revision = "different-current-metadata-revision".into();
        selected.definition_updates.as_mut().unwrap().revision = "different-policy-revision".into();
        assert_ne!(old.revision, selected.revision);
        let field = DefinitionLimitField::CodingSummaryBytes;
        let request = BTreeMap::from([(field, field.value(&old.execution).unwrap())]);
        let (fields, allowed, reason) = compare(
            &old.execution, &selected.execution, &request, &[], false,
        ).unwrap();
        assert!(allowed);
        assert_eq!(reason, ConfigurationReason::SameEffectiveValues);
        assert_eq!(fields[0].change, EffectiveChange::Neutral);
        assert_eq!(fields[0].before, Some(fields[0].after));
    }

    #[test]
    //= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
    //= type=test
    //# A governed configuration field without a sound comparison rule MUST be treated as widening.
    fn unspecified_context_is_conservative_widening_without_inventing_a_before_value() {
        let mut before = limits();
        before.coding.model_context_window = None;
        before.validate().unwrap();
        let field = DefinitionLimitField::CodingModelContextWindow;
        let request = BTreeMap::from([(field, 1)]);
        let after = apply(&before, &request);
        after.validate().unwrap();
        let expected = FieldComparison {
            field, before: None, after: 1,
            rule: ComparisonRule::ConservativeWidening, change: EffectiveChange::Widening,
        };
        for (configured, permit, allowed, reason) in [
            (vec![field], true, true, ConfigurationReason::ConfiguredWideningClass),
            (vec![], true, false, ConfigurationReason::WideningNotConfigured),
            (vec![field], false, false, ConfigurationReason::WideningNotConfigured),
        ] {
            assert_eq!(compare(&before, &after, &request, &configured, permit).unwrap(),
                (vec![expected.clone()], allowed, reason));
        }
        assert_eq!(before.coding.model_context_window, None);
    }

    #[test]
    //= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
    //= type=test
    //# A widening configuration change MUST be refused unless the authenticated invocation is trusted for that configured class of change.
    fn mixed_changes_keep_each_direction_and_require_every_widening_field() {
        let before = limits();
        let neutral = DefinitionLimitField::NativeResultBytes;
        let narrow = DefinitionLimitField::CodingSummaryChars;
        let wide = DefinitionLimitField::CodingSummaryBytes;
        let request = BTreeMap::from([
            (wide, before.coding.summary_bytes + 1),
            (narrow, before.coding.summary_chars - 1),
            (neutral, before.native.result_bytes),
        ]);
        let after = apply(&before, &request);
        after.validate().unwrap();
        let (fields, allowed, reason) = compare(&before, &after, &request, &[wide], true).unwrap();
        assert!(allowed);
        assert_eq!(reason, ConfigurationReason::ConfiguredWideningClass);
        assert_eq!(fields.iter().map(|v| v.field).collect::<Vec<_>>(), request.keys().copied().collect::<Vec<_>>());
        assert_eq!(fields.iter().map(|v| v.change).collect::<Vec<_>>(), vec![
            EffectiveChange::Neutral, EffectiveChange::Narrowing, EffectiveChange::Widening,
        ]);
        let refused = compare(&before, &after, &request, &[neutral, narrow], true).unwrap();
        assert_eq!(refused.0, fields);
        assert!(!refused.1);
        assert_eq!(refused.2, ConfigurationReason::WideningNotConfigured);

        let second_wide = DefinitionLimitField::CodingCommandOutputBytes;
        let mut request = request;
        request.insert(second_wide, before.coding.command_output_bytes + 1);
        let after = apply(&before, &request);
        assert!(!compare(&before, &after, &request, &[wide], true).unwrap().1);
        assert!(compare(&before, &after, &request, &[wide, second_wide], true).unwrap().1);
        assert!(!compare(&before, &after, &request, &[wide, second_wide], false).unwrap().1);
    }

    #[test]
    fn narrowing_is_a_comparison_result_not_an_allowed_field_grant() {
        let before = limits();
        let field = DefinitionLimitField::CodingSummaryBytes;
        let request = BTreeMap::from([(field, before.coding.summary_bytes - 1)]);
        let after = apply(&before, &request);
        let (fields, allowed, reason) = compare(&before, &after, &request, &[], false).unwrap();
        assert!(allowed, "the caller separately enforces the allowed-field policy");
        assert_eq!(reason, ConfigurationReason::NarrowsResourceAllowance);
        assert_eq!(fields[0].change, EffectiveChange::Narrowing);
    }

    #[test]
    fn mismatched_requested_value_and_unrequested_changes_are_errors() {
        let before = limits();
        let field = DefinitionLimitField::CodingSummaryBytes;
        let request = BTreeMap::from([(field, before.coding.summary_bytes + 1)]);
        let intended = apply(&before, &request);
        let mut wrong_value = intended.clone();
        wrong_value.coding.summary_bytes += 1;
        let mut extra_supported = intended.clone();
        extra_supported.coding.summary_chars += 1;
        let mut extra_other_group = intended.clone();
        extra_other_group.memory_notice.empty_prefix_pages += 1;
        let mut extra_same_group = intended.clone();
        extra_same_group.coding.frame_bytes += 1;
        for after in [before.clone(), wrong_value, extra_supported, extra_other_group, extra_same_group] {
            assert!(compare(&before, &after, &request, &FIELDS, true).is_err());
        }
        let before_copy = before.clone();
        let intended_copy = intended.clone();
        assert!(compare(&before, &intended, &request, &FIELDS, true).unwrap().1);
        assert_eq!(before, before_copy);
        assert_eq!(intended, intended_copy);
    }

    #[test]
    fn unrequested_optional_value_cannot_be_materialized_or_removed() {
        let mut before = limits();
        before.coding.model_context_window = None;
        let field = DefinitionLimitField::CodingSummaryBytes;
        let request = BTreeMap::from([(field, before.coding.summary_bytes)]);
        let mut after = before.clone();
        after.coding.model_context_window = Some(65_536);
        assert!(compare(&before, &after, &request, &FIELDS, true).is_err());
        assert!(compare(&after, &before, &request, &FIELDS, true).is_err());
    }

    #[test]
    fn malformed_request_or_duplicate_configured_fields_fail_closed() {
        let before = limits();
        assert!(compare(&before, &before, &BTreeMap::new(), &FIELDS, true).is_err());
        let field = DefinitionLimitField::CodingSummaryBytes;
        let zero = BTreeMap::from([(field, 0)]);
        let zero_after = apply(&before, &zero);
        assert!(compare(&before, &zero_after, &zero, &FIELDS, true).is_err());
        let neutral = BTreeMap::from([(field, before.coding.summary_bytes)]);
        assert!(compare(&before, &before, &neutral, &[field, field], true).is_err());
    }
}
