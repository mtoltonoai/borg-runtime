//! Closed governance records for the selected V2 completion, configuration and
//! operator-control mechanisms. Records carry provenance, never grant authority.
use crate::collaboration::workflow::{EventId, RecordedActor, WaitRef};
use crate::limits::DefinitionLimitField;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DecisionPoint { CharterConfiguration, Completion, DefinitionLimits, EmergencyControl, RuleAdministration }

/// The existing original mutation supplies identity and replay. No new counter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DecisionRef { pub operation_id: String, pub point: DecisionPoint }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuleRef { pub id: String, pub revision: String }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationBinding {
    /// Current controller metadata used for the decision, not a caller value.
    pub global_revision: String,
    /// The immutable active governance selection revision.
    pub governance_revision: String,
    /// Exact accepted task metadata remains a separate historical binding.
    pub accepted_global_revision: Option<String>,
    /// Present only at the existing twelve-field definition-limit boundary.
    pub definition_update_policy_revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionSubject {
    pub task_id: String,
    pub session_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub task_revision: u64,
    pub accepted_global_revision: String,
}

/// Exact existing accepted checks, not a natural-language completeness judge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionReason {
    OpenWait { wait: WaitRef },
    DelegatedTask { task_id: String },
    AcceptanceCheck { check: CompletionCheck, index: usize },
    UnconsumedInput { event_id: EventId },
    EvaluationUnavailable { code: CompletionFailure },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionCheck {
    /// Index is zero for the task's one accepted native output schema.
    NativeOutput,
    /// Index identifies the original accepted acceptance_commands entry.
    CodingValidation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionFailure { Unavailable, InvalidCanonicalState, BoundExceeded }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionDisposition {
    Admit,
    Blocked { continuation_eligible: bool, exhausted: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionDecision {
    pub subject: CompletionSubject,
    pub disposition: CompletionDisposition,
    pub reasons: Vec<CompletionReason>,
    pub consecutive_count: usize,
    /// A validated independently originated canonical change, not a retry ID.
    pub independent_reset_event: Option<EventId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonRule { ResourceAllowance, ConservativeWidening }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveChange { Neutral, Narrowing, Widening }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldComparison {
    pub field: DefinitionLimitField,
    /// None remains unspecified; its authorization class is widening.
    pub before: Option<usize>,
    pub after: usize,
    pub rule: ComparisonRule,
    pub change: EffectiveChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionLimitDecision {
    pub agent_id: String,
    pub previous_revision: String,
    pub selected_revision: Option<String>,
    pub comparisons: Vec<FieldComparison>,
    pub allowed: bool,
    pub reason: ConfigurationReason,
}

/// A closed operator charter change; text is never semantically compared.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterConfigurationDecision {
    pub before: crate::charter::CharterSourceRef,
    pub proposed: crate::charter::CharterSourceRef,
    pub source_before: Option<crate::charter::CharterSourceRef>,
    pub source_after: crate::charter::CharterSourceRef,
    pub effect: crate::charter::CharterDefinitionEffect,
    pub comparison: ComparisonRule,
    pub change: EffectiveChange,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationReason {
    SameEffectiveValues, NarrowsResourceAllowance, ConfiguredWideningClass,
    WideningNotConfigured, SelfWeakening, EvaluationUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlScope { AllTasks, Task { task_id: String } }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlDecision {
    Paused { scope: ControlScope },
    StopRequested { task_id: String },
    HoldReleased { scope: ControlScope },
    OverrideSelected {
        scope: ControlScope, prior_revision: String, replacement_revision: String,
        expires_at_ms: u64, reason: String,
    },
    OverrideExpired { original_operation_id: String, restored_revision: String },
    OverrideSuperseded { original_operation_id: String, selected_revision: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionDetail {
    CharterConfiguration(CharterConfigurationDecision),
    Completion(CompletionDecision),
    DefinitionLimits(DefinitionLimitDecision),
    EmergencyControl(ControlDecision),
    RuleAdministration(RuleAdministrationDecision),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DecisionMode { Observation, Enforcing }

/// References the final original receipt rather than copying unrestricted data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActualDecisionOutcome {
    Applied,
    GovernanceBlocked,
    MandatoryRefusal { code: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DecisionRecord {
    pub mode: DecisionMode,
    pub id: DecisionRef,
    pub rule: RuleRef,
    pub configuration: ConfigurationBinding,
    /// Exact controller metadata at this original decision. Retained in the
    /// same receipt, independent of task lifetime and later selected defaults.
    /// Historical absence preserves old bytes and requires original evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration_metadata: Option<Box<crate::limits::RuntimeLimits>>,
    pub actor: RecordedActor,
    pub detail: DecisionDetail,
    pub actual_outcome: ActualDecisionOutcome,
    /// Exactly the original receipt's existing controller revision.
    pub controller_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_product_revision: Option<u64>,
}

/// A retained evaluated view, not an authorization to dispatch another turn.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionHold {
    pub decision: DecisionRef,
    pub state: CompletionDecision,
    pub observed_product_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<crate::governance_completion::CompletionContinuation>,
}

/// Closed data over two concrete mechanisms. No expressions or executable rules.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuleBody {
    Completion { allow_continuation: bool },
    DefinitionLimits { allow_configured_widening: bool },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuleVersion { pub reference: RuleRef, pub body: RuleBody }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationVersion {
    pub revision: String,
    pub global_revision: String,
    pub definition_update_policy_revision: Option<String>,
    pub completion: RuleRef,
    pub definition_limits: RuleRef,
    pub widening_fields: Vec<DefinitionLimitField>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShadowEvidenceRef { pub operation_id: String }
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowVerdict { Allow, Block }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShadowEvaluation {
    Evaluated { verdict: ShadowVerdict, actual_outcome_would_change: bool, passing: bool },
    Failed { code: String },
}
/// A bootstrap observation is an actual fresh view at the original receipt's
/// unchanged controller revision, never a fabricated enforcing decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShadowInput {
    RecordedDecision,
    CurrentNotEnforcedCompletion {
        assessment: CompletionDecision,
        observed_product_revision: u64,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShadowResult {
    pub original: DecisionRef,
    pub original_controller_revision: u64,
    pub input: ShadowInput,
    /// Exact candidate continuation disposition when this is a completion rule.
    pub candidate_completion: Option<CompletionDecision>,
    pub candidate: RuleRef,
    pub configuration_revision: String,
    pub evaluated_at_controller_revision: u64,
    pub evaluation: ShadowEvaluation,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuleAdministrationDecision {
    RegisteredRule { rule: RuleRef },
    RegisteredConfiguration { revision: String },
    Shadow { result: ShadowResult },
    Promoted { prior_revision: String, selected_revision: String, evidence: Vec<ShadowEvidenceRef> },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GovernanceMutation {
    RegisterRule { rule: RuleVersion },
    RegisterConfiguration { configuration: ConfigurationVersion },
    EvaluateShadow { original: DecisionRef, candidate: RuleRef, configuration_revision: String },
    Promote { expected_revision: String, configuration_revision: String, evidence: Vec<ShadowEvidenceRef>, supersedes_override: Option<String> },
    Pause { scope: ControlScope, reason: String },
    ReleaseHold { scope: ControlScope, expected_operation_id: String, reason: String },
    Stop { task_id: String, expected_revision: u64, reason: String },
    // Initial temporary policy replacement is controller-wide; Task-scoped
    // pause/stop remains available. There is no per-task permission engine.
    Override { expected_revision: String, configuration_revision: String, expires_at_ms: u64, reason: String },
    ReconcileExpiry { expected_operation_id: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GovernanceQuery {
    Decision { decision: DecisionRef },
    /// The retained completion assessment for this exact task, if one exists.
    TaskCompletion { task_id: String },
    Rule { rule: RuleRef },
    Configuration { revision: String },
    State,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EmergencyHold {
    pub scope: ControlScope, pub operation_id: String,
    pub actor: RecordedActor, pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TemporaryOverride {
    pub operation_id: String, pub prior_revision: String,
    pub replacement_revision: String, pub expires_at_ms: u64,
    pub actor: RecordedActor, pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GovernanceState {
    pub selected_revision: String,
    pub temporary_override: Option<TemporaryOverride>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum GovernanceResult {
    Rule(RuleVersion), Configuration(ConfigurationVersion),
    Decision(DecisionRecord), Shadow(ShadowResult), State(GovernanceState),
    Hold(EmergencyHold), Released(ControlScope), StopRequested { task_id: String },
    TaskCompletion { task_id: String, decision: Option<Box<DecisionRecord>> },
}
