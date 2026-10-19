use super::*;
use serde_json::json;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
fn limits() -> Arc<WorkerLimits> {
    Arc::from(crate::worker::test_limits().worker.clone())
}
fn area() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts/v2/build-60156282/worker-envelope-implementation-e42rg9kw/fixtures");
    fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    path
}
fn value(path: &Path) -> serde_json::Value {
    json!({"cgroup_path":path,"cpu_quota_us":200000,"cpu_period_us":100000,"memory_max_bytes":1048576,"swap_max_bytes":0,"pids_max":64})
}
#[test]
fn resource_profile_is_closed_finite_and_has_no_implicit_limits() {
    let original = value(Path::new("/operator/worker"));
    assert_eq!(
        Profile::parse(&original.to_string(), &limits())
            .unwrap()
            .swap_max_bytes,
        0
    );
    for key in [
        "cpu_quota_us",
        "cpu_period_us",
        "memory_max_bytes",
        "swap_max_bytes",
        "pids_max",
        "cgroup_path",
    ] {
        let mut missing = original.clone();
        missing.as_object_mut().unwrap().remove(key);
        assert!(
            Profile::parse(&missing.to_string(), &limits()).is_err(),
            "missing {key}"
        );
        for invalid in [json!(null), json!(-1), json!("max"), json!(1.5), json!([])] {
            let mut v = original.clone();
            v[key] = invalid;
            assert!(Profile::parse(&v.to_string(), &limits()).is_err(), "invalid {key}");
        }
    }
    for key in [
        "cpu_quota_us",
        "cpu_period_us",
        "memory_max_bytes",
        "pids_max",
    ] {
        let mut v = original.clone();
        v[key] = json!(0);
        assert!(Profile::parse(&v.to_string(), &limits()).is_err());
    }
    let mut unknown = original.clone();
    unknown["per_task_reservation"] = json!(1);
    assert!(Profile::parse(&unknown.to_string(), &limits()).is_err());
    let duplicate = original.to_string().replacen('{', "{\"pids_max\":64,", 1);
    assert!(Profile::parse(&duplicate, &limits()).is_err());
    assert!(
        Profile::parse(
            &original
                .to_string()
                .replace("1048576", "18446744073709551616"),
            &limits(),
        )
        .is_err()
    );
    assert!(Profile::parse(&" ".repeat(limits().resource_profile_bytes + 1), &limits()).is_err());
    for path in [
        "relative",
        "/group/../worker",
        "/group/./worker",
        "/group//worker",
        "/group/worker/",
    ] {
        let mut v = original.clone();
        v["cgroup_path"] = json!(path);
        assert!(Profile::parse(&v.to_string(), &limits()).is_err());
    }
}
#[test]
fn resource_mount_and_membership_join_is_exact_and_namespace_aware() {
    let table = "44 1 0:2 / /ordinary rw - ext4 /dev/disk rw\n45 1 0:3 /tenant /visible\\040mount ro - cgroup2 cgroup2 rw\n";
    let m = mount_entry(table, 45, &limits()).unwrap();
    assert_eq!(m.location, Path::new("/visible mount"));
    binds(
        &m,
        Path::new("/visible mount/worker"),
        Path::new("/tenant/worker"),
    )
    .unwrap();
    assert!(binds(&m, Path::new("/visible mount/worker"), Path::new("/worker")).is_err());
    assert!(
        binds(
            &m,
            Path::new("/visible mount-other/worker"),
            Path::new("/tenant/worker")
        )
        .is_err()
    );
    assert!(mount_entry(table, 44, &limits()).is_err());
    assert!(mount_entry(table, 999, &limits()).is_err());
    assert!(mount_entry(&format!("{table}{table}"), 45, &limits()).is_err());
    for table in [
        "45 1 0:3 / /g rw cgroup2\n",
        "45 1 0:3 / /g rw - cgroup2 x\n",
        "45 1 0:3 / /g rw - cgroup2 x rw",
        "45 1 0:3 / /g\\000 rw - cgroup2 x rw\n",
    ] {
        assert!(mount_entry(table, 45, &limits()).is_err());
    }
    assert_eq!(fd_mount("pos:\t0\nmnt_id:\t45\n").unwrap(), 45);
    for text in [
        "pos:\t0\n",
        "mnt_id:\t45\nmnt_id:\t45\n",
        "mnt_id:\t0\n",
        "mnt_id:\t45",
        "mnt_id:\tx\n",
    ] {
        assert!(fd_mount(text).is_err());
    }
    assert_eq!(
        membership("2:legacy:/old\n0::/tenant/worker\n", &limits()).unwrap(),
        Path::new("/tenant/worker")
    );
    for text in [
        "0::/a\n0::/a\n",
        "0:cpu:/a\n",
        "0::/a",
        "0::/../a\n",
        "2:cpu:/a\n",
    ] {
        assert!(membership(text, &limits()).is_err());
    }
}
#[test]
fn resource_plain_files_and_all_directory_symlinks_cannot_supply_enforcement() {
    use std::os::unix::fs::symlink;
    let root = area();
    let group = root.join("group");
    fs::create_dir(&group).unwrap();
    for (name, data) in [
        ("cgroup.type", "domain\n"),
        ("cpu.max", "200000 100000\n"),
        ("memory.max", "1048576\n"),
        ("memory.swap.max", "0\n"),
        ("pids.max", "64\n"),
    ] {
        fs::write(group.join(name), data).unwrap();
    }
    let profile = root.join("profile.json");
    fs::write(&profile, value(&group).to_string()).unwrap();
    assert!(
        load(Some(&profile), limits())
            .err()
            .unwrap()
            .contains("genuine cgroup2")
    );
    let alias = root.join("alias");
    symlink(&group, &alias).unwrap();
    assert!(directory(&alias, &limits()).is_err());
    fs::create_dir(group.join("child")).unwrap();
    assert!(directory(&alias.join("child"), &limits()).is_err());
    let linked_profile = root.join("linked.json");
    symlink(&profile, &linked_profile).unwrap();
    assert!(load(Some(&linked_profile), limits()).is_err());
    assert!(load(Some(&group), limits()).is_err());
    fs::write(&profile, " ".repeat(limits().resource_profile_bytes + 1)).unwrap();
    assert!(load(Some(&profile), limits()).is_err());
    assert!(
        load(None, limits()).unwrap().is_none(),
        "development mode is absence, not a fabricated verified envelope"
    );
}
#[test]
// Traceability scope: Read-only genuine-kernel-source mismatch refusal; this does not qualify positive finite host or descendant enforcement.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# A worker MUST verify the configured host controls before claiming that those controls are enforced.
fn resource_actual_opened_cgroup_refuses_wrong_worker_or_unlimited_controls() {
    // Read-only host test: this establishes the genuine kernel-source join and
    // refusal only. It does not qualify finite enforcement or descendants.
    let member = membership(&proc_read("/proc/self/cgroup", limits().resource_line_bytes, "worker.resource_line_bytes").unwrap(), &limits()).unwrap();
    let table = proc_read("/proc/self/mountinfo", limits().resource_proc_bytes, "worker.resource_proc_bytes").unwrap();
    let mut actual = None;
    for line in table.lines() {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        if fields
            .iter()
            .position(|f| *f == "-")
            .is_some_and(|i| fields.get(i + 1) == Some(&"cgroup2"))
        {
            let m = mount_entry(&table, number(fields[0]).unwrap(), &limits()).unwrap();
            if let Ok(relative) = member.strip_prefix(&m.root) {
                actual = Some(m.location.join(relative));
                break;
            }
        }
    }
    let path = actual.expect("qualification host exposes unified cgroup metadata");
    let dir = directory(&path, &limits()).unwrap();
    let m = mount(&dir, &limits()).unwrap();
    binds(&m, &path, &member).unwrap();
    // A deliberately impossible current membership never becomes authority,
    // even though the directory and all controls are real kernel objects.
    assert!(binds(&m, &path, Path::new("/not-this-worker")).is_err());
    let mut expected = value(&path);
    expected["cpu_quota_us"] = json!(u64::MAX);
    let profile = Profile::parse(&expected.to_string(), &limits()).unwrap();
    let envelope = Envelope {
        profile,
        directory: dir,
        mount: m,
        limits: limits(),
    };
    assert!(envelope.verify().is_err());
}

#[test]
fn resource_metadata_refusals_name_the_exact_selected_field() {
    let mut policy = (*limits()).clone();
    let profile = value(Path::new("/operator/worker")).to_string();
    policy.resource_profile_bytes = profile.len() - 1;
    assert!(Profile::parse(&profile, &policy).unwrap_err().contains("worker.resource_profile_bytes"));
    policy = (*limits()).clone();
    policy.resource_path_bytes = 4;
    assert!(clean_path(Path::new("/long/path"), &policy).unwrap_err().contains("worker.resource_path_bytes"));
    let table = "44 1 0:2 / /ordinary rw - ext4 /dev/disk rw\n45 1 0:3 / /group rw - cgroup2 cgroup2 rw\n";
    policy = (*limits()).clone();
    policy.resource_proc_bytes = table.len() - 1;
    assert!(mount_entry(table, 45, &policy).unwrap_err().contains("worker.resource_proc_bytes"));
    policy = (*limits()).clone();
    policy.resource_lines = 1;
    assert!(mount_entry(table, 45, &policy).unwrap_err().contains("worker.resource_lines"));
    policy = (*limits()).clone();
    policy.resource_line_bytes = 8;
    assert!(mount_entry(table, 45, &policy).unwrap_err().contains("worker.resource_line_bytes"));
    assert!(membership("0::/operator/worker\n", &policy).unwrap_err().contains("worker.resource_line_bytes"));
    // Shape failures retain their original classification and are not mislabeled
    // as metadata limits merely because the same parser also has byte bounds.
    assert_eq!(mount_entry("incomplete", 45, &limits()).unwrap_err(), "incomplete or oversized resource mount table");
    assert_eq!(clean_path(Path::new("relative"), &limits()).unwrap_err(), "resource path must be a normalized absolute path");
}
