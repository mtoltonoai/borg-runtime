//! Test evidence only: retain one copied image, then link ordinary case paths.
//! Mutable/path-fault fixtures explicitly request private copies instead.
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

fn unique() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    )
}

pub fn artifact_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = std::env::var_os("BORG_TEST_ARTIFACT")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join(format!("borg-test-artifacts-{}", unique())));
        assert!(root.is_absolute(), "test artifact root must be absolute");
        assert!(root.components().all(|part| !matches!(part, std::path::Component::ParentDir)),
            "test artifact root must not contain parent traversal");
        // Resolve the existing ancestor before making anything. This catches a
        // configured symlink into the source tree even when its leaf is absent.
        let mut ancestor = root.clone();
        let mut missing = Vec::new();
        loop {
            match fs::symlink_metadata(&ancestor) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    missing.push(ancestor.file_name().unwrap().to_owned());
                    assert!(ancestor.pop(), "artifact root has no existing ancestor");
                }
                Err(error) => panic!("artifact root inspection failed: {error}"),
            }
        }
        let mut resolved = fs::canonicalize(ancestor).unwrap();
        for name in missing.into_iter().rev() { resolved.push(name); }
        let source = fs::canonicalize(env!("CARGO_MANIFEST_DIR")).unwrap();
        assert!(!resolved.starts_with(&source), "test artifact root must be outside the source tree");
        fs::create_dir_all(&resolved).unwrap();
        let actual = fs::canonicalize(&resolved).unwrap();
        assert_eq!(actual, resolved, "artifact root changed during creation");
        assert!(!actual.starts_with(&source), "test artifact root resolved inside source tree");
        actual
    })
}

fn sha256(path: &Path) -> String {
    let result = Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(path)
        .output()
        .unwrap();
    assert!(result.status.success(), "retained image hash failed");
    let output = String::from_utf8(result.stdout).unwrap();
    let hash = output.split_whitespace().next().unwrap();
    assert!(hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()));
    hash.to_owned()
}

// Excludes ctime/nlink: linking additional immutable case names changes those.
fn identity(path: &Path) -> (u64, u64, u64, u32, i64, i64) {
    let m = fs::symlink_metadata(path).unwrap();
    assert!(m.file_type().is_file(), "image must be a regular non-symlink file");
    (m.dev(), m.ino(), m.len(), m.mode(), m.mtime(), m.mtime_nsec())
}

struct Image {
    input: PathBuf,
    input_identity: (u64, u64, u64, u32, i64, i64),
    path: PathBuf,
    identity: (u64, u64, u64, u32, i64, i64),
    hash: String,
}

impl Image {
    fn capture(input: &Path) -> Self {
        let input_identity = identity(input);
        let hash = sha256(input);
        let mode = input_identity.3 & 0o555;
        assert_ne!(mode & 0o111, 0, "selected image must be executable");
        let parent = artifact_root().join("retained-images").join(format!("{hash}-{mode:o}"));
        fs::create_dir_all(&parent).unwrap();
        let path = parent.join("borg-runtime");
        if !path.try_exists().unwrap() {
            let pending = parent.join(format!(".pending-{}", unique()));
            // A new inode: never hardlink a writable Cargo output into evidence.
            let mut output = OpenOptions::new().write(true).create_new(true).open(&pending).unwrap();
            let copied = (|| -> io::Result<()> {
                io::copy(&mut fs::File::open(input)?, &mut output)?;
                output.set_permissions(fs::Permissions::from_mode(mode))?;
                output.sync_all()?;
                Ok(())
            })();
            drop(output);
            if let Err(error) = copied {
                let _ = fs::remove_file(&pending);
                panic!("retained image copy failed: {error}");
            }
            let correct = sha256(&pending) == hash && identity(input) == input_identity;
            if !correct {
                fs::remove_file(&pending).unwrap();
                panic!("selected runtime changed during image capture");
            }
            // Publish without replacing an existing image; a racing publisher
            // must have supplied exactly the same bytes and read-only mode.
            let published = fs::hard_link(&pending, &path);
            fs::remove_file(&pending).unwrap();
            if let Err(error) = published {
                assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "image publication failed: {error}");
            }
            fs::File::open(&parent).unwrap().sync_all().unwrap();
        }
        let retained_identity = identity(&path);
        assert_eq!(retained_identity.3 & 0o7777, mode);
        assert_eq!(sha256(&path), hash, "existing retained image differs");
        assert_ne!((retained_identity.0, retained_identity.1), (input_identity.0, input_identity.1));
        assert_eq!(identity(input), input_identity);
        Self { input: input.to_owned(), input_identity, path, identity: retained_identity, hash }
    }
}

pub fn materialize(input: &Path, destination: &Path, private: bool) {
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => panic!("case image already exists (including symlink)"),
        Err(error) => panic!("case image inspection failed: {error}"),
    }
    let (hash, retained) = if private {
        // Preserve original mode and independent inode for intentional image
        // disappearance/validation-hold cases; no shared image is modified.
        let mut source = fs::File::open(input).unwrap();
        let permissions = source.metadata().unwrap().permissions();
        // create_new atomically refuses every existing destination, including a
        // dangling symlink or a name created after the inspection above.
        let mut output = OpenOptions::new().write(true).create_new(true).open(destination).unwrap();
        io::copy(&mut source, &mut output).unwrap();
        output.set_permissions(permissions).unwrap();
        output.sync_all().unwrap();
        drop(output);
        (sha256(destination), None)
    } else {
        static IMAGE: OnceLock<Image> = OnceLock::new();
        let image = IMAGE.get_or_init(|| Image::capture(input));
        assert_eq!(image.input.as_path(), input, "one runtime subject per test executable");
        assert_eq!(identity(input), image.input_identity, "Cargo subject changed");
        assert_eq!(identity(&image.path), image.identity, "retained image changed");
        // Preserve each case's executable pathname (including current_exe()).
        // Refuse cross-device links rather than silently multiplying full copies.
        fs::hard_link(&image.path, destination).unwrap();
        (image.hash.clone(), Some(image.path.clone()))
    };
    let evidence = destination.with_file_name(format!(
        "{}.retention.json", destination.file_name().unwrap().to_str().unwrap()
    ));
    let mut output = OpenOptions::new().write(true).create_new(true).open(evidence).unwrap();
    writeln!(output, "{}", serde_json::json!({
        "input": input, "image": destination, "sha256": hash,
        "private_copy": private, "retained_image": retained,
        "case_inode": identity(destination).1,
        "scope": "test fixture image retention; no case/state/lock sharing"
    })).unwrap();
}
