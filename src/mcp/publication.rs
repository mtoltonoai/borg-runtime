//! Closed publication capability, shared catalogue and owned command carriers.
//! A read association never supplies this capability. Admission is not evidence
//! of an external effect; only the original settled typed proof is completion.
use crate::{limits::RuntimeLimits, protocol::*, pull_request as pr, repository_catalogue::Revision};
use super::{Caller, Invocation, Tool};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action { PublishBranch, OpenPullRequest }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "version", deny_unknown_fields)]
pub enum Selection {
    #[serde(rename = "1")]
    V1 {
        repository_id: String,
        registration_revision: String,
        target_revision: String,
        actions: Vec<Action>,
        /// Branch component prefix, without refs/heads/. The suffix is a digest
        /// of the original requester, source and accepted target binding.
        branch_namespace: String,
    },
}
pub fn present_selection<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Selection>, D::Error> {
    Selection::deserialize(d).map(Some)
}
impl Selection {
    pub fn registration(&self) -> Revision {
        let Self::V1 { repository_id, registration_revision, .. } = self;
        Revision { repository_id: repository_id.clone(), registration_revision: registration_revision.clone() }
    }
    pub fn target_revision(&self) -> &str { let Self::V1 { target_revision, .. } = self; target_revision }
    pub fn namespace(&self) -> &str { let Self::V1 { branch_namespace, .. } = self; branch_namespace }
    pub fn permits(&self, action: Action) -> bool { let Self::V1 { actions, .. } = self; actions.contains(&action) }
    pub fn validate(&self, limits: &RuntimeLimits) -> Result<(), String> {
        let Self::V1 { repository_id, registration_revision, target_revision, actions, branch_namespace } = self;
        for value in [repository_id, registration_revision, target_revision] {
            if value.is_empty() || value.len() > limits.transport.identity_bytes || value.chars().any(char::is_control) {
                return Err("publication selection identity exceeds transport.identity_bytes or is invalid".into());
            }
        }
        if actions.is_empty() || actions.len() > 2 || (actions.len() == 2 && actions[0] == actions[1]) {
            return Err("publication actions must be a nonempty closed unique selection".into());
        }
        pr::validate_branch_namespace(branch_namespace, &pr::Bounds::from_limits(limits))?;
        bounded_json_size(self, limits.controller.effect_value_bytes)
            .map_err(|_| "publication selection exceeds controller.effect_value_bytes".into()).map(|_| ())
    }
}
pub fn selection(profile: Option<&ExecutionProfile>) -> Option<&Selection> {
    match profile { Some(ExecutionProfile::CollaborationV2 { publication, .. }) => publication.as_ref(), _ => None }
}
pub fn handles(name: &str) -> bool {
    matches!(name, "collab_repository_publish" | "collab_pull_request_open" | "collab_publication_get")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "tool", content = "arguments", deny_unknown_fields)]
pub enum Arguments {
    #[serde(rename = "collab_repository_publish")]
    Publish {
        source_task_id: String,
        expected_source_revision: u64,
        checkpoint_operation_id: String,
        repository_id: String,
        registration_revision: String,
        target_revision: String,
    },
    #[serde(rename = "collab_pull_request_open")]
    Open { publication_id: String, title: String, body: String },
    #[serde(rename = "collab_publication_get")]
    Get { publication_id: String },
}
pub fn parse(name: &str, arguments: &Value) -> Result<Arguments, String> {
    serde_json::from_value(json!({"tool":name,"arguments":arguments}))
        .map_err(|_| "invalid closed publication arguments".into())
}
pub fn validate(name: &str, arguments: &Value, limits: &RuntimeLimits, selected: Option<&Selection>) -> Result<(), String> {
    let selected = selected.ok_or("publication capability is absent")?;
    selected.validate(limits)?;
    let args = parse(name, arguments)?;
    let identity = |value: &str| -> Result<(), String> {
        if value.is_empty() || value.len() > limits.transport.identity_bytes || value.chars().any(char::is_control) {
            Err("publication identity exceeds transport.identity_bytes or is invalid".into())
        } else { Ok(()) }
    };
    match args {
        Arguments::Publish { source_task_id, expected_source_revision, checkpoint_operation_id, repository_id, registration_revision, target_revision } => {
            if !selected.permits(Action::PublishBranch) { return Err("publish_branch is not selected".into()); }
            for value in [&source_task_id, &checkpoint_operation_id, &repository_id, &registration_revision, &target_revision] { identity(value)?; }
            if expected_source_revision == 0 || expected_source_revision > i64::MAX as u64 { return Err("invalid source revision".into()); }
        },
        Arguments::Open { publication_id, title, body } => {
            if !selected.permits(Action::OpenPullRequest) { return Err("open_pull_request is not selected".into()); }
            identity(&publication_id)?;
            pr::validate_request(&pr::Request { title, body }, &pr::Bounds::from_limits(limits))?;
        },
        Arguments::Get { publication_id } => identity(&publication_id)?,
    }
    bounded_json_size(arguments, limits.interface.mcp_call_bytes)
        .map_err(|_| "publication arguments exceed interface.mcp_call_bytes".into()).map(|_| ())
}

pub fn catalogue(limits: &RuntimeLimits, selected: Option<&Selection>) -> Vec<Tool> {
    let Some(selected) = selected.filter(|s| s.validate(limits).is_ok()) else { return vec![]; };
    // Same fixed catalogue file and typed admission for Native and Coding.
    let all: Vec<Tool> = serde_json::from_str(include_str!("catalogue.json")).expect("fixed MCP catalogue");
    all.into_iter().filter(|tool| match tool.name.as_str() {
        "collab_repository_publish" => selected.permits(Action::PublishBranch),
        "collab_pull_request_open" => selected.permits(Action::OpenPullRequest),
        "collab_publication_get" => true,
        _ => false,
    }).map(|mut tool| {
        for (name, field) in tool.input_schema["properties"].as_object_mut().unwrap() {
            if field["type"] == "string" {
                let maximum = match name.as_str() { "title" => limits.controller.max_progress_bytes,
                    "body" => limits.controller.effect_value_bytes, _ => limits.transport.identity_bytes };
                field["maxLength"] = json!(maximum); field["x-borg-maxUtf8Bytes"] = json!(maximum);
            }
        }
        tool
    }).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Requester {
    pub caller: Caller,
    pub session_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub task_id: String,
    pub session_id: String,
    pub task_revision: u64,
    #[serde(deserialize_with = "strict_result_ref")]
    pub accepted_result: crate::collaboration::workflow::AcceptedResultRef,
    pub result_sha256: String,
    /// The original checkpoint publisher may precede the accepting attempt.
    pub checkpoint_attempt_id: String,
    pub checkpoint_generation: u64,
    pub repository: Repository,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub publication_id: String,
    pub admission_operation_id: String,
    pub requester: Requester,
    pub source: Source,
    pub selection: Selection,
}
fn strict_result_ref<'de, D: serde::Deserializer<'de>>(d: D) -> Result<crate::collaboration::workflow::AcceptedResultRef, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fields {
        task_id: String, attempt_id: String, terminal_task_revision: u64, acceptance_history_sequence: u64,
        #[serde(deserialize_with = "pr::strict_checkpoint")]
        checkpoint: Checkpoint,
    }
    let f = Fields::deserialize(d)?;
    Ok(crate::collaboration::workflow::AcceptedResultRef { task_id: f.task_id, attempt_id: f.attempt_id,
        terminal_task_revision: f.terminal_task_revision, acceptance_history_sequence: f.acceptance_history_sequence, checkpoint: Some(f.checkpoint) })
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub invocation: Invocation,
    pub session: Session,
    pub attempt: Attempt,
    /// Actual requesting input, including a Native input without a workspace.
    pub input: TaskInput,
    pub head: pr::HeadIntent,
    /// Absent is head publication; present is the one admitted PR creation.
    pub create: Option<pr::Intent>,
}
impl Command {
    pub fn caller(&self) -> &Caller { &self.invocation.caller }
    pub fn effect(&self, limits: &RuntimeLimits) -> Result<OperationRecord, String> {
        let bounds = pr::Bounds::from_limits(limits);
        match &self.create { Some(intent) => Ok(pr::intent_record(intent, &bounds)?), None => Ok(pr::head_record(&self.head, &bounds)?) }
    }
    pub fn validate(&self, command_id: &str) -> Result<(), String> {
        self.session.validate_for(&self.attempt, &self.input)?;
        self.input.execution_limits()?;
        let limits = &self.input.resolved_limits.as_ref().ok_or("publication accepted limits missing")?.global_metadata;
        self.invocation.validate(limits)?;
        let binding = self.head.publication.as_deref().ok_or("publication source/requester binding missing")?;
        let caller = self.caller();
        if caller.task_id != self.attempt.task_id || caller.attempt_id != self.attempt.id
            || caller.generation != self.attempt.generation || caller.worker_id != self.attempt.worker_id
            || caller.incarnation != self.attempt.worker_incarnation
            || binding.requester.caller != *caller || binding.requester.session_id != self.session.id
            || selection(self.input.definition.execution_profile.as_ref()) != Some(&binding.selection) {
            return Err("publication command requester differs from accepted input".into());
        }
        validate(&self.invocation.tool, &self.invocation.arguments, limits, Some(&binding.selection))?;
        pr::validate_head_intent(&self.head, &pr::Bounds::from_limits(limits))?;
        let expected = if let Some(intent) = &self.create {
            if intent.head != self.head { return Err("publication create has another head".into()); }
            pr::validate_intent(intent, &pr::Bounds::from_limits(limits))?;
            let Arguments::Open { publication_id, title, body } = parse(&self.invocation.tool, &self.invocation.arguments)? else { return Err("publication create invocation differs".into()); };
            if publication_id != binding.publication_id { return Err("publication identity differs".into()); }
            pr::validate_requested_result(intent, &pr::Request { title, body }, &pr::Bounds::from_limits(limits))?;
            create_command_id(&binding.publication_id)?
        } else {
            let Arguments::Publish { source_task_id, expected_source_revision, checkpoint_operation_id, repository_id, registration_revision, target_revision } = parse(&self.invocation.tool, &self.invocation.arguments)? else { return Err("publication head invocation differs".into()); };
            if self.invocation.operation_id != binding.admission_operation_id
                || publication_id(&binding.admission_operation_id)? != binding.publication_id
                || source_task_id != binding.source.task_id || expected_source_revision != binding.source.task_revision
                || checkpoint_operation_id != self.head.binding.checkpoint.operation_id
                || (Revision { repository_id, registration_revision }) != binding.selection.registration()
                || target_revision != binding.selection.target_revision() { return Err("publication source selection differs".into()); }
            binding.publication_id.clone()
        };
        if command_id != expected { return Err("publication command identity differs".into()); }
        bounded_json_size(self, limits.worker.command_record_bytes).map(|_| ())
    }
}

pub fn digest(value: &(impl Serialize + ?Sized)) -> Result<String, String> {
    let bytes = serde_json::to_vec(value).map_err(|_| "publication encoding failed")?;
    Ok(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect())
}
pub fn publication_id(operation: &str) -> Result<String, String> { Ok(format!("publication-{}", digest(&("publication-v1", operation))?)) }
pub fn create_command_id(publication: &str) -> Result<String, String> { Ok(format!("publication-open-{}", digest(publication)?)) }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State { Pending, Completed, Refused, Unknown }
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Refusal { Configuration, StaleCaller, Cancelled, Deadline, Conflict, Capacity, NotDispatched }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActionView {
    pub state: State,
    pub refusal: Option<Refusal>,
    pub head: Option<pr::HeadProof>,
    pub pull_request: Option<pr::ResultReference>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct View {
    pub publication_id: String,
    pub source_task_id: String,
    pub source_revision: u64,
    pub source_attempt_id: String,
    pub checkpoint_operation_id: String,
    pub registration: Revision,
    pub target_revision: String,
    pub commit: String,
    pub branch: ActionView,
    pub pull_request: Option<ActionView>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn selection() -> Selection { Selection::V1 { repository_id:"runtime".into(), registration_revision:"one".into(), target_revision:"target-one".into(),
        actions:vec![Action::PublishBranch,Action::OpenPullRequest], branch_namespace:"borg/publications".into() } }
    fn head() -> pr::HeadIntent {
        let registration=selection().registration();
        let component=|s:&str|s.as_bytes().iter().map(|v|format!("{v:02x}")).collect::<String>();
        let checkpoint=Checkpoint{registration:registration.clone(),repository:"/tmp/publication-checkpoints.git".into(),commit:"a".repeat(40),
            remote_ref:format!("refs/heads/borg/tasks/{}/{}/{}",component("source"),component("source-attempt"),component("checkpoint")),operation_id:"checkpoint".into()};
        let target=pr::Target{revision:"target-one".into(),api_base:"http://127.0.0.1:9".into(),owner:"fixture".into(),repository:"fixture".into(),repository_id:71,
            head_remote:"/tmp/publication-target.git".into(),base_branch:"main".into(),credential_name:"GITHUB_TOKEN".into()};
        let source=Source{task_id:"source".into(),session_id:"source-session".into(),task_revision:9,
            accepted_result:crate::collaboration::workflow::AcceptedResultRef{task_id:"source".into(),attempt_id:"accepting-attempt".into(),terminal_task_revision:9,acceptance_history_sequence:11,checkpoint:Some(checkpoint)},
            result_sha256:"b".repeat(64),checkpoint_attempt_id:"source-attempt".into(),checkpoint_generation:1,
            repository:Repository{registration:Some(registration),url:"/tmp/publication-source.git".into(),start_commit:"c".repeat(40),checkpoint_remote:"/tmp/publication-checkpoints.git".into(),pull_request_target:Some(target.clone())}};
        let binding=Binding{publication_id:publication_id("admission").unwrap(),admission_operation_id:"admission".into(),requester:Requester{caller:Caller{task_id:"requester".into(),attempt_id:"request-attempt".into(),generation:7,worker_id:"worker".into(),incarnation:"incarnation".into()},session_id:"request-session".into()},source,selection:selection()};
        pr::prepare_publication_head(binding,target,&pr::Bounds::from_limits(&RuntimeLimits::seed().unwrap())).unwrap()
    }
    #[test]
    fn publication_selection_is_optional_closed_versioned_and_never_inferred_from_read_scope() {
        let limits=RuntimeLimits::seed().unwrap();let old=ExecutionProfile::CollaborationV2{web_search:None,publication:None};
        assert_eq!(serde_json::to_string(&old).unwrap(),r#"{"kind":"collaboration_v2"}"#);
        assert!(catalogue(&limits,None).is_empty());assert!(validate("collab_repository_publish",&json!({}),&limits,None).is_err());
        let profile=ExecutionProfile::CollaborationV2{web_search:None,publication:Some(selection())};
        assert_eq!(serde_json::from_value::<ExecutionProfile>(json!(profile)).unwrap(),profile);
        for field in ["operator","credential","repository_url"] {let mut bad=json!(profile);bad["publication"][field]=json!("forged");assert!(serde_json::from_value::<ExecutionProfile>(bad).is_err());}
        for value in [Value::Null,json!({"version":"2"}),json!({"version":"1","actions":["deploy"]})] {let mut bad=json!(profile);bad["publication"]=value;assert!(serde_json::from_value::<ExecutionProfile>(bad).is_err());}
        let tools=catalogue(&limits,Some(&selection()));assert_eq!(tools.len(),3);
        for tool in tools {assert!(!super::super::retryable_read_tool(&tool.name),"publication reads are not retention/retry eligible by annotation");}
    }
    #[test]
    fn publication_binding_keeps_requester_source_publisher_and_accepting_attempt_distinct() {
        let head=head();let b=pr::Bounds::from_limits(&RuntimeLimits::seed().unwrap());let record=pr::head_record(&head,&b).unwrap();
        assert_eq!(record.task_id,"requester");assert_eq!(record.attempt_id,"request-attempt");assert_eq!(record.generation,7);
        assert_eq!(head.binding.task_id,"source");assert_eq!(head.binding.attempt_id,"source-attempt");assert_eq!(head.binding.generation,1);
        assert_eq!(head.publication.as_ref().unwrap().source.accepted_result.attempt_id,"accepting-attempt");
        assert!(head.head_ref.starts_with("refs/heads/borg/publications/"));
        assert_eq!(pr::decode_head_intent(&serde_json::to_vec(&head).unwrap(),&b).unwrap(),head);
        let mut wrong=head.clone();wrong.binding.generation=7;assert!(pr::validate_head_intent(&wrong,&b).is_err());
        let mut wrong=head.clone();wrong.publication.as_mut().unwrap().requester.caller.generation+=1;assert!(pr::validate_head_intent(&wrong,&b).is_err());
    }
    #[test]
    fn publication_new_head_refuses_the_af5_strict_reader_and_new_reader_rejects_unknown_nested_fields() {
        // Exact af5 HeadIntent carrier fields. Its Binding remains unchanged.
        #[derive(Deserialize)] #[serde(deny_unknown_fields)]
        #[allow(dead_code)] struct Af5Head { binding:pr::Binding,target:pr::Target,head_operation_id:String,head_ref:String }
        let head=head();assert!(serde_json::from_value::<Af5Head>(json!(head)).is_err());
        let b=pr::Bounds::from_limits(&RuntimeLimits::seed().unwrap());let mut bad=json!(head);bad["publication"]["requester"]["caller"]["authority"]=json!("operator");
        assert!(pr::decode_head_intent(&serde_json::to_vec(&bad).unwrap(),&b).is_err());
        let binding=json!(&head.binding);assert!(binding.get("requester").is_none());assert!(binding.get("source").is_none());
        assert_eq!(serde_json::from_value::<pr::Binding>(binding.clone()).unwrap(),head.binding);
        assert_eq!(json!(pr::NotApplied{acknowledged:true,not_applied:true,refusal:None}),json!({"acknowledged":true,"not_applied":true}));
    }
    #[test]
    fn publication_branch_namespace_and_action_policy_refuse_unknown_or_oversized_values() {
        let limits=RuntimeLimits::seed().unwrap();
        for namespace in ["refs/heads/fake","../escape","borg//empty","borg/@{bad}","-option"] {
            let mut selected=selection();let Selection::V1{branch_namespace,..}=&mut selected;*branch_namespace=namespace.into();assert!(selected.validate(&limits).is_err(),"{namespace}");
        }
        let mut selected=selection();let Selection::V1{actions,..}=&mut selected;*actions=vec![Action::PublishBranch,Action::PublishBranch];assert!(selected.validate(&limits).is_err());
        let args=json!({"source_task_id":"source","expected_source_revision":1,"checkpoint_operation_id":"checkpoint","repository_id":"runtime","registration_revision":"one","target_revision":"target-one"});
        assert!(validate("collab_repository_publish",&args,&limits,Some(&selection())).is_ok());
        let mut bad=args;bad["head_remote"]=json!("/tmp/forged.git");assert!(validate("collab_repository_publish",&bad,&limits,Some(&selection())).is_err());
    }
}
