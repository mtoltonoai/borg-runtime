//! Candidate preparation only. Ordinary task, artifact and external-operation
//! metadata cannot establish a protected checker, submission or rollout grant.
use super::*;


fn hex(value: &str, size: usize) -> bool {
    value.len() == size
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn artifact(limits: &crate::limits::RuntimeLimits, a: &ReleaseArtifact) -> Result<()> {
    let source_size = match a.object_format.as_str() {
        "sha1" => 40,
        "sha256" => 64,
        _ => return refuse("invalid_input", "unsupported Git object format"),
    };
    if a.format != "borg-v2-release/1"
        || a.target != "x86_64-unknown-linux-gnu"
        || !hex(&a.package_sha256, 64)
        || !hex(&a.manifest_sha256, 64)
        || !hex(&a.binary_sha256, 64)
        || !hex(&a.source_commit, source_size)
        || !hex(&a.source_tree, source_size)
        || a.binary_bytes == 0
        || a.package_bytes <= a.binary_bytes
        || a.package_bytes % 10_240 != 0
    {
        return refuse("invalid_input", "invalid bounded release artifact claim");
    }
    if a.binary_bytes > limits.controller.release_binary_bytes {
        return refuse("invalid_input", "invalid bounded release artifact claim: controller.release_binary_bytes");
    }
    if a.package_bytes > limits.controller.release_package_bytes {
        return refuse("invalid_input", "invalid bounded release artifact claim: controller.release_package_bytes");
    }
    // The package producer verifies bytes. Shape/identity agreement here is
    // deliberately not an independent byte verification or protected verdict.
    Ok(())
}
fn unqualified() -> ReleaseQualification {
    ReleaseQualification {
        artifact: ArtifactQualification::Claimed,
        repository_submission: ProtectedStatus::Unconfigured,
        checks: ProtectedStatus::Unconfigured,
        human_rollout: ProtectedStatus::Unconfigured,
    }
}

fn source(tx: &Connection, task: &Task, cp: &Checkpoint) -> Result<String> {
    let limits = tx.limits;
    if task.status != TaskStatus::Succeeded
        || task.input.definition.executor != ExecutorKind::Coding
    {
        return refuse(
            "invalid_transition",
            "release preparation requires an accepted coding task",
        );
    }
    let result = task
        .result
        .as_ref()
        .ok_or_else(|| Fault::Store("successful coding task lacks result".into()))?;
    let attempt = task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("accepted result lacks attempt".into()))?;
    if result.attempt_id != attempt.id
        || result.checkpoint.as_ref() != Some(cp)
        || task.selected_checkpoint.as_ref() != Some(cp)
    {
        return refuse(
            "source_mismatch",
            "release differs from the accepted source checkpoint",
        );
    }
    let row: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT body,prepare,settlement FROM external_operations WHERE id=?1",
            [&cp.operation_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((body, prepare, Some(settlement))) = row else {
        return refuse(
            "checkpoint_unsettled",
            "original checkpoint acknowledgment is unavailable",
        );
    };
    let record: OperationRecord = decode(&body)?;
    if record.id != cp.operation_id
        || record.kind != "checkpoint"
        || record.state != "settled"
        || record.task_id != task.id
        || record
            .outcome
            .as_ref()
            .and_then(|v| v.get("acknowledged"))
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || effects::intended_checkpoint(&record.id, &record.intended)? != *cp
    {
        return refuse(
            "checkpoint_unsettled",
            "original publication does not acknowledge this checkpoint",
        );
    }
    // Recovery can accept unchanged source published by an earlier attempt.
    // Keep the accepting attempt separate from the original publication owner.
    let publication: Option<(String, String)> = tx
        .query_row(
            "SELECT task_id,body FROM attempts WHERE id=?1",
            [&record.attempt_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((publication_task, publication_body)) = publication else {
        return refuse(
            "checkpoint_unsettled",
            "original publication attempt is missing",
        );
    };
    let publication: Attempt = decode(&publication_body)?;
    if publication_task != task.id
        || publication.task_id != task.id
        || publication.id != record.attempt_id
        || publication.generation != record.generation
        || publication.generation > attempt.generation
    {
        return refuse(
            "checkpoint_unsettled",
            "checkpoint publication belongs to another owner",
        );
    }
    effects::check_checkpoint_for_attempt(limits, task, cp, &publication.id)?;
    let original: Operation = decode(&prepare)?;
    let settled: Operation = decode(&settlement)?;
    let settlement_id = match &settled {
        Operation::SettleOperation { settlement_id, .. } => settlement_id,
        _ => return refuse("checkpoint_unsettled", "original settlement kind differs"),
    };
    let (mutation, outcome) = super::super::credits::receipt(tx, settlement_id)?;
    if encode(&mutation)? != settlement
        || !matches!(outcome, Outcome::Ok { data: Data::Operation(ref original) } if original == &record)
    {
        return refuse(
            "checkpoint_unsettled",
            "original settlement mutation receipt differs",
        );
    }
    let correct_prepare = matches!(original, Operation::PrepareOperation {
        operation_id, task_id, attempt_id, generation, kind, intended
    } if operation_id == record.id && task_id == record.task_id && attempt_id == record.attempt_id
        && generation == record.generation && kind == record.kind && intended == record.intended);
    let correct_settlement = matches!(settled, Operation::SettleOperation {
        operation_id, task_id, attempt_id, generation, outcome, checkpoint: Some(checkpoint), ..
    } if operation_id == record.id && task_id == record.task_id && attempt_id == record.attempt_id
        && generation == record.generation && Some(&outcome) == record.outcome.as_ref() && checkpoint == *cp);
    if !correct_prepare || !correct_settlement {
        return refuse(
            "checkpoint_unsettled",
            "checkpoint original receipt association differs",
        );
    }
    Ok(result.attempt_id.clone())
}

pub(crate) fn read(tx: &Connection, id: &str) -> Result<ReleaseCandidate> {
    let limits = tx.limits;
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT task_id,package_sha256,body FROM releases WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((task_id, hash, body)) = row else {
        return refuse("not_found", "release candidate not found");
    };
    let r: ReleaseCandidate = decode(&body)?;
    if r.id != id
        || r.task_id != task_id
        || r.artifact.package_sha256 != hash
        || r.revision != 1
        || r.qualification != unqualified()
    {
        return Err(Fault::Store("corrupt release candidate association".into()));
    }
    artifact(limits, &r.artifact)?;
    let task = load(tx, &r.task_id)?.task;
    if r.task_revision == 0
        || r.task_revision > task.revision
        || r.artifact.source_commit != r.checkpoint.commit
        || source(tx, &task, &r.checkpoint)? != r.accepted_attempt_id
    {
        return Err(Fault::Store(
            "release candidate no longer matches its original source".into(),
        ));
    }
    Ok(r)
}

pub(crate) fn validate_existing(tx: &Connection, cfg: &ControllerConfig) -> Result<()> {
    let mut q = tx.prepare("SELECT id FROM releases ORDER BY seq")?;
    let mut rows = q.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > cfg.max_release_candidates {
            return Err(Fault::Store(
                "existing release count exceeds configured capacity".into(),
            ));
        }
        let id: String = row.get(0)?;
        read(tx, &id)?;
    }
    Ok(())
}

pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    match op {
        Operation::PrepareRelease(input) => {
            if !valid_id(limits, &input.task_id) {
                return refuse("invalid_input", "invalid release task identity");
            }
            artifact(limits, &input.artifact)?;
            let task = load(tx, &input.task_id)?.task;
            if input.expected_task_revision != task.revision {
                return refuse("stale_revision", "task changed before release preparation");
            }
            let accepted_attempt_id = source(tx, &task, &input.checkpoint)?;
            if input.artifact.source_commit != input.checkpoint.commit {
                return refuse(
                    "source_mismatch",
                    "artifact claim identifies another source commit",
                );
            }
            let original: Option<String> = tx
                .query_row(
                    "SELECT id FROM releases WHERE task_id=?1 AND package_sha256=?2",
                    params![task.id, input.artifact.package_sha256],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(id) = original {
                let r = read(tx, &id)?;
                if r.artifact != input.artifact
                    || r.checkpoint != input.checkpoint
                    || r.accepted_attempt_id != accepted_attempt_id
                {
                    return refuse(
                        "operation_conflict",
                        "package already has different preparation claims",
                    );
                }
                return Ok((Data::Release(Box::new(r)), false));
            }
            let count: i64 = tx.query_row("SELECT count(*) FROM releases", [], |r| r.get(0))?;
            if unsigned(count)? >= cfg.max_release_candidates as u64 {
                return refuse("capacity", "release candidate capacity exhausted: controller.max_release_candidates");
            }
            let seq = next(tx, "release")?;
            let database_id: String = tx.query_row(
                "SELECT value FROM settings WHERE name='database_id'",
                [],
                |r| r.get(0),
            )?;
            let r = ReleaseCandidate {
                id: format!("release-{database_id}-{seq:020}"),
                task_id: task.id.clone(),
                task_revision: task.revision,
                accepted_attempt_id,
                checkpoint: input.checkpoint.clone(),
                artifact: input.artifact.clone(),
                revision: 1,
                qualification: unqualified(),
            };
            tx.execute(
                "INSERT INTO releases(id,seq,task_id,package_sha256,body) VALUES(?1,?2,?3,?4,?5)",
                params![
                    r.id,
                    integer(seq)?,
                    r.task_id,
                    r.artifact.package_sha256,
                    encode(&r)?
                ],
            )?;
            history(tx, &task, "release_prepared", &r)?;
            Ok((Data::Release(Box::new(r)), true))
        }
        Operation::GetRelease { release_id } => {
            if !valid_id(limits, release_id) {
                return refuse("invalid_input", "invalid release identity");
            }
            Ok((Data::Release(Box::new(read(tx, release_id)?)), false))
        }
        Operation::ListReleases {
            task_id,
            after,
            limit,
        } => {
            if !valid_id(limits, task_id) || *limit == 0 {
                return refuse("invalid_input", "invalid release page bounds");
            }
            if *limit > cfg.max_list_tasks {
                return refuse("invalid_input", "invalid release page bounds: controller.max_list_tasks");
            }
            let page_limit = integer(u64::try_from(*limit).map_err(|_| {
                Fault::Refused(
                    "invalid_input",
                    "release page bound exceeds durable range".into(),
                )
            })?)?;
            load(tx, task_id)?;
            let seq: i64 = match after {
                None => 0,
                Some(id) => tx
                    .query_row(
                        "SELECT seq FROM releases WHERE id=?1 AND task_id=?2",
                        params![id, task_id],
                        |r| r.get(0),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        Fault::Refused(
                            "invalid_input",
                            "release cursor does not belong to task".into(),
                        )
                    })?,
            };
            let mut q = tx.prepare(
                "SELECT id FROM releases WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3",
            )?;
            let ids = q
                .query_map(params![task_id, seq, page_limit], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut values = Vec::new();
            for id in ids {
                values.push(read(tx, &id)?);
                if bounded_json_size(&values, cfg.list_page_bytes).is_err() {
                    values.pop();
                    break;
                }
            }
            Ok((Data::Releases(values), false))
        }
        _ => unreachable!("release operation routing"),
    }
}
