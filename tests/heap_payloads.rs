//! Representation regressions: heap ownership must preserve original stored and
//! wire bytes, while the owning envelopes stay small enough to pass by value.
//! The fixture was decoded and serialized by the actual retained pre-change
//! a9599d463827ceeefea2822d206a216c6760759b library, not the current implementation.
use borg_runtime::{
    collaboration::work::WorkView,
    limits::{LimitSnapshot, RuntimeLimits},
    protocol::{Data, Operation, Outcome, Task, TaskDraft, TaskInput, WorkerCommand},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::mem::size_of;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalFixture {
    source_commit: String,
    oracle: String,
    oracle_rlib_sha256: String,
    cases: Vec<HistoricalCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalCase {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    wire: String,
}

#[test]
fn heap_payload_aggregate_sizes_are_bounded() {
    // These cap aggregate storage, independently of the chosen Box fields.
    // A large future variant must not silently enlarge every transport result.
    let bounds = [
        ("Data", size_of::<Data>(), 576),
        ("Outcome", size_of::<Outcome>(), 576),
        ("Task", size_of::<Task>(), 768),
        ("Operation", size_of::<Operation>(), 768),
        ("WorkerCommand", size_of::<WorkerCommand>(), 512),
        ("LimitSnapshot", size_of::<LimitSnapshot>(), 128),
    ];
    for (name, actual, maximum) in bounds {
        assert!(
            actual <= maximum,
            "{name} occupies {actual} inline bytes (maximum {maximum}); move large owned payloads to the heap"
        );
    }
}

fn assert_original_wire<T: DeserializeOwned + Serialize>(case: &HistoricalCase) {
    let decoded: T = serde_json::from_str(&case.wire)
        .unwrap_or_else(|error| panic!("historical {} did not decode: {error}", case.name));
    let encoded = serde_json::to_string(&decoded).unwrap();
    assert_eq!(
        encoded.as_bytes(),
        case.wire.as_bytes(),
        "heap ownership changed original bytes for {}",
        case.name
    );
}

#[test]
fn historical_payloads_preserve_exact_wire_bytes() {
    let fixture: HistoricalFixture =
        serde_json::from_str(include_str!("fixtures/heap-payloads-a959.json")).unwrap();
    assert_eq!(
        fixture.source_commit,
        "a9599d463827ceeefea2822d206a216c6760759b"
    );
    assert_eq!(
        fixture.oracle_rlib_sha256,
        "ddebc32ff2a5b7b467464aaeb812ec1a5b8fb3a9bedf9f4808e6fecbdc955d88"
    );
    assert!(
        fixture
            .oracle
            .starts_with("Actual pre-change borg_runtime rlib;")
    );
    let mut names = std::collections::BTreeSet::new();
    let mut data_kinds = std::collections::BTreeSet::new();
    for case in &fixture.cases {
        assert!(
            names.insert(case.name.as_str()),
            "duplicate historical case {}",
            case.name
        );
        match case.kind.as_str() {
            "Task" => assert_original_wire::<Task>(case),
            "TaskInput" => assert_original_wire::<TaskInput>(case),
            "TaskDraft" => assert_original_wire::<TaskDraft>(case),
            "Operation" => assert_original_wire::<Operation>(case),
            "WorkerCommand" => assert_original_wire::<WorkerCommand>(case),
            "RuntimeLimits" => assert_original_wire::<RuntimeLimits>(case),
            "LimitSnapshot" => assert_original_wire::<LimitSnapshot>(case),
            "WorkView" => assert_original_wire::<WorkView>(case),
            "Outcome" => assert_original_wire::<Outcome>(case),
            "Data" => {
                assert_original_wire::<Data>(case);
                let value: serde_json::Value = serde_json::from_str(&case.wire).unwrap();
                data_kinds.insert(value["kind"].as_str().unwrap().to_owned());
            }
            unknown => panic!("unhandled historical fixture type {unknown}"),
        }
    }
    // Removing a difficult fixture must not accidentally narrow compatibility.
    for required in [
        "runtime_limits",
        "limit_snapshot",
        "task_input",
        "task_full_nested",
        "task_absent_optional_owners",
        "task_draft",
        "submit_task",
        "submit_task_draft",
        "delegate_task",
        "run_attempt_with_continuations",
        "work_view",
        "completion_outcome",
    ] {
        assert!(
            names.contains(required),
            "missing historical case {required}"
        );
    }
    for required in [
        "charter",
        "charter_query",
        "completion",
        "collaboration_delivery",
        "question_mutation",
        "question_query",
        "review_mutation",
        "review_query",
        "task",
        "work_query",
        "work_mutation",
    ] {
        assert!(
            data_kinds.contains(required),
            "missing historical Data::{required}"
        );
    }
}
