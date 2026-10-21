use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use std::process::Command;

use super::wechat_messages::CollectionCancel;
use super::wechat_snapshot::{self, SnapshotError};

/// VDBE steps between two deadline checks inside one generic read statement.
const READ_PROGRESS_STEPS: i32 = 1000;

/// Stable read failures. Never carry database paths, SQL, keys or message bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WechatReadError {
    SessionUnavailable,
    NotLoggedIn,
    KeysUnavailable,
    DatabaseRead,
}

impl WechatReadError {
    pub fn code(self) -> &'static str {
        match self {
            Self::SessionUnavailable => "WECHAT_SESSION_UNAVAILABLE",
            Self::NotLoggedIn => "WECHAT_NOT_LOGGED_IN",
            Self::KeysUnavailable => "WECHAT_KEYS_UNAVAILABLE",
            Self::DatabaseRead => "WECHAT_DB_READ_FAILED",
        }
    }
}

impl std::fmt::Display for WechatReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for WechatReadError {}

/// Compatibility entry point for media readers that still use optional results.
/// HTTP chat/contact/message reads must use the fallible entry point below.
pub fn query_wechat_db(db_path: &str, hex_key: &str, sql: &str) -> Vec<Value> {
    try_query_wechat_db(db_path, hex_key, sql).unwrap_or_else(|error| {
        tracing::warn!(code = error.code(), "WeChat media database read failed");
        Vec::new()
    })
}

/// Query a WeChat database, rejecting the entire result on any read failure.
///
/// The source shard is not opened by SQLite at all. Its bytes and its `-wal`
/// are read as an ordinary `O_RDONLY` byte snapshot, the committed WAL prefix
/// is merged into a private encrypted copy, and the query runs on that copy
/// (`immutable=1`). The live file is therefore never locked, never needs its
/// `-shm` index, and is never checkpointed or rewritten; a writer that holds a
/// lock cannot block this read.
///
/// Because the copy is a byte image rather than a logical export, every rowid
/// is preserved exactly. Because the source's identity is verified around the
/// read, a source that changes while it is being captured is refused instead of
/// being read half-updated. The one behaviour this drops is transaction
/// isolation from the live writer's lock: isolation now comes from taking a
/// stable byte snapshot, not from a shared lock.
///
/// This entry point has no caller deadline, so it uses
/// [`wechat_snapshot::SNAPSHOT_DEFAULT_BUDGET`] for both the copy and the
/// statement that follows: a `Connection` alone would carry no interrupt, so a
/// read that ran past the budget would otherwise never stop.
pub fn try_query_wechat_db(
    db_path: &str,
    hex_key: &str,
    sql: &str,
) -> Result<Vec<Value>, WechatReadError> {
    let cancel = CollectionCancel::within(wechat_snapshot::SNAPSHOT_DEFAULT_BUDGET);
    let conn = wechat_snapshot::open_snapshot_conn(Path::new(db_path), hex_key, Some(&cancel))
        .map_err(snapshot_failure)?;

    let guard = cancel.clone();
    conn.progress_handler(READ_PROGRESS_STEPS, Some(move || guard.expired()));

    let mut stmt = conn.prepare(sql).map_err(read_failure)?;

    let col_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let rows = stmt.query_map([], |row| {
        let mut map = Map::new();
        for (i, name) in col_names.iter().enumerate() {
            map.insert(name.clone(), value_ref_to_json(row.get_ref(i)?));
        }
        Ok(Value::Object(map))
    });

    let result = rows
        .map_err(read_failure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(read_failure);
    result
}

/// Convert one SQLite value into the JSON the read helpers expose.
///
/// Blobs become uppercase hex (a safety net — callers typically use `hex()` in
/// SQL), text is decoded lossily, and a real that does not fit `serde_json`
/// becomes null. The message-snapshot reader uses this too, so the two paths
/// agree on the representation instead of carrying separate copies of the rule.
pub(crate) fn value_ref_to_json(value: rusqlite::types::ValueRef<'_>) -> Value {
    match value {
        rusqlite::types::ValueRef::Null => Value::Null,
        rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
        rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        rusqlite::types::ValueRef::Text(s) => {
            Value::String(String::from_utf8_lossy(s).into_owned())
        }
        rusqlite::types::ValueRef::Blob(b) => {
            use std::fmt::Write;
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                let _ = write!(hex, "{byte:02X}");
            }
            Value::String(hex)
        }
    }
}

/// Coarse, fixed classification of a failed SQLite read.
///
/// The source error is deliberately dropped: only the stable reason and the
/// public code reach the log. The error text, database path, SQL and key never
/// do, because any of them can carry account or message data.
fn read_failure(error: rusqlite::Error) -> WechatReadError {
    let reason = match &error {
        rusqlite::Error::SqliteFailure(sqlite, _)
            if matches!(
                sqlite.code,
                rusqlite::ffi::ErrorCode::DatabaseBusy | rusqlite::ffi::ErrorCode::DatabaseLocked
            ) =>
        {
            // Transient contention: the caller may retry, so keep it out of warn.
            "busy_locked"
        }
        rusqlite::Error::SqliteFailure(sqlite, _)
            if sqlite.code == rusqlite::ffi::ErrorCode::NotADatabase =>
        {
            "not_a_database"
        }
        rusqlite::Error::SqliteFailure(sqlite, _)
            if sqlite.code == rusqlite::ffi::ErrorCode::CannotOpen =>
        {
            "cannot_open"
        }
        _ => "other",
    };

    if reason == "busy_locked" {
        tracing::debug!(
            reason,
            code = WechatReadError::DatabaseRead.code(),
            "WeChat database read failed"
        );
    } else {
        tracing::warn!(
            reason,
            code = WechatReadError::DatabaseRead.code(),
            "WeChat database read failed"
        );
    }

    WechatReadError::DatabaseRead
}

/// Coarse, fixed classification of a failed source capture.
///
/// Same contract as [`read_failure`]: exactly one log event, a fixed reason and
/// the public code, and nothing that could carry a path, key or SQL.
fn snapshot_failure(error: SnapshotError) -> WechatReadError {
    let reason = match error {
        SnapshotError::CannotOpen => "cannot_open",
        SnapshotError::NotADatabase => "not_a_database",
        SnapshotError::SourceChanged => "source_changed",
        SnapshotError::BudgetExceeded => "budget_exceeded",
        SnapshotError::DatabaseRead => "other",
    };
    tracing::warn!(
        reason,
        code = WechatReadError::DatabaseRead.code(),
        "WeChat database read failed"
    );
    WechatReadError::DatabaseRead
}

/// Find the WeChat process PID.
pub fn find_wechat_pid() -> Option<i64> {
    let output = Command::new("pgrep")
        .args(["-f", "/usr/bin/wechat"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let pids: Vec<i64> = stdout
        .split_whitespace()
        .filter_map(|s| s.parse().ok())
        .collect();

    // Return the PID with the most open file descriptors
    let mut best_pid: Option<i64> = None;
    let mut best_fd_count = 0;

    for pid in pids {
        let fd_dir = format!("/proc/{pid}/fd");
        if let Ok(entries) = std::fs::read_dir(&fd_dir) {
            let count = entries.count();
            if count > best_fd_count {
                best_fd_count = count;
                best_pid = Some(pid);
            }
        }
    }

    best_pid
}

/// Detect the WeChat account directory by scanning /proc/<pid>/fd.
pub fn find_account_dir(wechat_pid: i64) -> Option<String> {
    let fd_dir = format!("/proc/{wechat_pid}/fd");
    let entries = std::fs::read_dir(&fd_dir).ok()?;

    for entry in entries.flatten() {
        if let Ok(target) = std::fs::read_link(entry.path()) {
            let target_str = target.to_string_lossy();
            if target_str.contains("db_storage") && target_str.ends_with(".db") {
                if let Some(idx) = target_str.find("xwechat_files/") {
                    let rest = &target_str[idx + "xwechat_files/".len()..];
                    if let Some(account_dir) = rest.split('/').next() {
                        if !account_dir.is_empty() {
                            return Some(account_dir.to_string());
                        }
                    }
                }
            }
        }
    }

    None
}

/// Candidate `xwechat_files` roots, in preference order.
const ACCOUNT_ROOTS: &[&str] = &[
    "/home/wechat/xwechat_files",
    "/home/wechat/Documents/xwechat_files",
];

/// Sharded database families that share the `message` sub-directory, e.g.
/// `message_10.db`, `biz_message_3.db` and `media_2.db`.
const SHARDED_MESSAGE_FAMILIES: &[&str] = &["message_", "biz_message_", "media_"];

/// Account roots that may hold `db_storage`, in preference order.
fn account_base_paths(account_dir: &str) -> Vec<PathBuf> {
    ACCOUNT_ROOTS
        .iter()
        .map(|root| Path::new(root).join(account_dir))
        .collect()
}

/// Sub-directory of `db_storage` that holds `db_name`.
///
/// Exact matches win, then the sharded `message_*` families, and finally the
/// legacy behaviour of using the file name without its `.db` suffix.
pub(crate) fn sub_dir_for(db_name: &str) -> &str {
    let fixed: &[(&str, &str)] = &[
        ("contact.db", "contact"),
        ("contact_fts.db", "contact"),
        ("session.db", "session"),
        ("message_0.db", "message"),
        ("message_fts.db", "message"),
        ("message_resource.db", "message"),
        ("biz_message_0.db", "message"),
        ("media_0.db", "message"),
        ("general.db", "general"),
        ("hardlink.db", "hardlink"),
        ("head_image.db", "head_image"),
        ("emoticon.db", "emoticon"),
        ("favorite.db", "favorite"),
        ("favorite_fts.db", "favorite"),
        ("sns.db", "sns"),
        ("bizchat.db", "bizchat"),
    ];

    if let Some((_, sub_dir)) = fixed.iter().find(|(name, _)| *name == db_name) {
        return sub_dir;
    }

    let stem = db_name.strip_suffix(".db").unwrap_or(db_name);
    for family in SHARDED_MESSAGE_FAMILIES {
        if let Some(shard) = stem.strip_prefix(family) {
            if !shard.is_empty() && shard.bytes().all(|byte| byte.is_ascii_digit()) {
                return "message";
            }
        }
    }

    stem
}

/// Resolve a database name under an already captured `db_storage` root. Unlike
/// [`get_db_path`], this never searches alternate account roots.
pub(crate) fn get_db_path_under_root(database_root: &Path, db_name: &str) -> PathBuf {
    database_root.join(sub_dir_for(db_name)).join(db_name)
}

/// Discover regular `.db` files beneath one already captured `db_storage`
/// directory. This intentionally does not consult alternate account roots.
pub(crate) fn list_db_names_under_root(database_root: &Path) -> Vec<String> {
    let Ok(root) = database_root.canonicalize() else { return Vec::new(); };
    if root.file_name().is_none_or(|name| name != "db_storage") { return Vec::new(); }
    let Ok(directories) = std::fs::read_dir(&root) else { return Vec::new(); };
    let mut names = Vec::new();
    for directory in directories.flatten() {
        if !directory.file_type().is_ok_and(|kind| kind.is_dir()) { continue; }
        let Ok(files) = std::fs::read_dir(directory.path()) else { continue; };
        for file in files.flatten() {
            if !file.file_type().is_ok_and(|kind| kind.is_file()) { continue; }
            let name = file.file_name().to_string_lossy().into_owned();
            if name.ends_with(".db") && valid_db_basename(&name) { names.push(name); }
        }
    }
    names.sort();
    names.dedup();
    names
}

fn valid_db_basename(name: &str) -> bool {
    name.ends_with(".db") && name.len() > 3
        && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// The digit suffix of a numbered database name `<prefix><digits>.db`, or `None`
/// for every other name.
///
/// The whole basename must be the prefix, digits and the `.db` suffix: a
/// directory component, an empty number, a non-digit or a different extension
/// all yield `None`, so a result can never be used to escape `<root>/message`.
/// This is the single definition of "numbered shard": callers that need the
/// number parse the returned digits, callers that only need the shape test for
/// presence.
pub(crate) fn numbered_db_digits<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(".db")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(digits)
}

/// The shard number of a numbered `<prefix><digits>.db`, or `None` when the name
/// is not one or its number does not fit `u32`.
pub(crate) fn numbered_db_index(name: &str, prefix: &str) -> Option<u32> {
    numbered_db_digits(name, prefix)?.parse().ok()
}

/// Resolve one database file inside the given account roots, preferring a
/// candidate that exists and otherwise falling back to the first root.
fn resolve_db_path(bases: &[PathBuf], sub_dir: &str, db_name: &str) -> String {
    for base in bases {
        let candidate = base.join("db_storage").join(sub_dir).join(db_name);
        if candidate.exists() {
            return candidate.to_string_lossy().to_string();
        }
    }

    bases
        .first()
        .map(|base| base.join("db_storage").join(sub_dir).join(db_name))
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

/// List all .db files that exist on disk for a given account.
pub fn list_account_dbs(account_dir: &str) -> Vec<String> {
    for base in account_base_paths(account_dir) {
        let db_storage = base.join("db_storage");
        if !db_storage.exists() {
            continue;
        }

        let mut db_names = Vec::new();
        if let Ok(sub_dirs) = std::fs::read_dir(&db_storage) {
            for sub_dir in sub_dirs.flatten() {
                if sub_dir.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Ok(files) = std::fs::read_dir(sub_dir.path()) {
                        for file in files.flatten() {
                            let name = file.file_name().to_string_lossy().to_string();
                            if name.ends_with(".db") {
                                db_names.push(name);
                            }
                        }
                    }
                }
            }
        }

        if !db_names.is_empty() {
            return db_names;
        }
    }

    Vec::new()
}

/// Get the full path to a single WeChat database file.
///
/// One shard is resolved per call: `message_1.db` maps to that shard's file, not
/// to a merge of every shard, and each call is an independent lookup, so no
/// cross-database snapshot is implied across shards.
pub fn get_db_path(account_dir: &str, db_name: &str) -> String {
    let sub_dir = sub_dir_for(db_name);
    resolve_db_path(&account_base_paths(account_dir), sub_dir, db_name)
}

#[cfg(test)]
mod tests {
    use super::{
        account_base_paths, get_db_path, resolve_db_path, sub_dir_for, try_query_wechat_db,
        WechatReadError,
    };
    use rusqlite::{Connection, OpenFlags};
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Barrier, Mutex, Once};
    use std::time::{Duration, Instant};

    const TEST_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn encrypted_fixture() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.db").to_str().unwrap().to_owned();
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT);
             INSERT INTO messages VALUES (1, 'fixture');"
        )).unwrap();
        drop(conn);
        (dir, path)
    }

    /// Compact description of how two file images differ, so a byte comparison
    /// does not dump whole encrypted databases into the test output.
    fn difference(before: &[u8], after: &[u8]) -> String {
        match before.iter().zip(after.iter()).position(|(a, b)| a != b) {
            Some(offset) => format!(
                "first difference at byte {offset} ({} bytes before, {} bytes after)",
                before.len(),
                after.len()
            ),
            None if before.len() == after.len() => "images are byte-identical".to_string(),
            None => format!(
                "identical up to byte {}, then the lengths differ ({} before, {} after)",
                before.len(),
                before.len(),
                after.len()
            ),
        }
    }

    #[test]
    fn strict_read_missing_database_is_error_and_does_not_create_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.db");
        assert_eq!(
            try_query_wechat_db(path.to_str().unwrap(), TEST_KEY, "SELECT 1"),
            Err(WechatReadError::DatabaseRead)
        );
        assert!(!path.exists());
    }

    #[test]
    fn strict_read_wrong_key_is_error_not_empty() {
        let (_dir, path) = encrypted_fixture();
        let wrong_key = "22".repeat(32);
        assert_eq!(
            try_query_wechat_db(&path, &wrong_key, "SELECT * FROM messages"),
            Err(WechatReadError::DatabaseRead)
        );
    }

    #[test]
    fn strict_read_missing_table_is_error_not_empty() {
        let (_dir, path) = encrypted_fixture();
        assert_eq!(
            try_query_wechat_db(&path, TEST_KEY, "SELECT * FROM absent_table"),
            Err(WechatReadError::DatabaseRead)
        );
    }

    #[test]
    fn strict_read_successful_empty_and_populated_results_are_distinct() {
        let (_dir, path) = encrypted_fixture();
        assert_eq!(
            try_query_wechat_db(&path, TEST_KEY, "SELECT * FROM messages WHERE id = 2"),
            Ok(vec![])
        );
        assert_eq!(
            try_query_wechat_db(&path, TEST_KEY, "SELECT * FROM messages"),
            Ok(vec![serde_json::json!({"id": 1, "content": "fixture"})])
        );
    }

    #[test]
    fn strict_read_late_row_error_rejects_the_entire_result() {
        let (_dir, path) = encrypted_fixture();
        // SQLite yields the first row before the second row overflows.
        assert_eq!(
            try_query_wechat_db(&path, TEST_KEY,
                "SELECT 1 AS value UNION ALL SELECT abs(-9223372036854775808)"),
            Err(WechatReadError::DatabaseRead)
        );
    }

    // ── Encrypted write-ahead-log fixture ──────────────────────────────────

    /// Real SQLCipher + WAL database with the writer still open.
    ///
    /// The fixture is built so the newest commit is provably still in the `-wal`
    /// file: `wal_autocheckpoint = 0` stops commit-time checkpoints and the only
    /// writer connection stays open, so SQLite never gets the "last connection
    /// closed" trigger. No sleep or timing guess is involved — the `-wal` size
    /// check below is the guard.
    struct WalFixture {
        _dir: tempfile::TempDir,
        path: String,
        writer: Connection,
    }

    impl WalFixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir
                .path()
                .join("wal_messages.db")
                .to_str()
                .unwrap()
                .to_owned();
            let writer = Connection::open(&path).unwrap();
            writer
                .execute_batch(&format!(
                    "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                     PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                     CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT);
                     INSERT INTO messages VALUES (1, 'checkpointed');"
                ))
                .unwrap();
            // Fold the first commit into the main database file, so the fixture
            // mixes one already-checkpointed row with one WAL-only row.
            let busy: i64 = writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                .unwrap();
            assert_eq!(busy, 0, "fixture checkpoint must not be blocked");
            writer
                .execute("INSERT INTO messages VALUES (2, 'wal_only')", [])
                .unwrap();

            let fixture = Self {
                _dir: dir,
                path,
                writer,
            };
            assert_eq!(fixture.journal_mode(), "wal");
            assert!(
                fixture.wal_bytes().len() > 32,
                "fixture must keep the newest commit in the -wal file"
            );
            fixture
        }

        fn wal_path(&self) -> PathBuf {
            PathBuf::from(&self.path).with_extension("db-wal")
        }

        /// Raw bytes of the database file and of its `-wal` file.
        ///
        /// The `-shm` index is deliberately left out: a read-only connection
        /// records its read mark there, which is expected and harmless.
        fn snapshot(&self) -> (Vec<u8>, Vec<u8>) {
            (
                std::fs::read(&self.path).unwrap(),
                std::fs::read(self.wal_path()).unwrap(),
            )
        }

        fn wal_bytes(&self) -> Vec<u8> {
            std::fs::read(self.wal_path()).unwrap_or_default()
        }

        fn journal_mode(&self) -> String {
            self.writer
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap()
        }

        fn count(&self) -> i64 {
            self.writer
                .query_row("SELECT count(*) FROM messages", [], |row| row.get(0))
                .unwrap()
        }
    }

    #[test]
    fn read_returns_rows_committed_to_the_write_ahead_log() {
        let fixture = WalFixture::new();
        // Fixture guard: the second commit is in the -wal file only.
        assert!(fixture.wal_bytes().len() > 32);

        let rows = try_query_wechat_db(
            &fixture.path,
            TEST_KEY,
            "SELECT id, content FROM messages ORDER BY id",
        );
        assert_eq!(
            rows,
            Ok(vec![
                serde_json::json!({"id": 1, "content": "checkpointed"}),
                serde_json::json!({"id": 2, "content": "wal_only"}),
            ])
        );
    }

    #[test]
    fn read_never_checkpoints_or_rewrites_the_source_database() {
        let fixture = WalFixture::new();
        // Compare raw bytes, not lengths: a PASSIVE checkpoint rewrites the
        // database file in place and can leave its size unchanged.
        let (main_before, wal_before) = fixture.snapshot();
        assert!(!main_before.is_empty(), "fixture needs a checkpointed database file");
        assert!(wal_before.len() > 32, "fixture needs a pending -wal commit");
        let rows_before = fixture.count();

        let rows = try_query_wechat_db(&fixture.path, TEST_KEY, "SELECT id FROM messages");
        assert_eq!(rows.map(|rows| rows.len()), Ok(2));

        let (main_after, wal_after) = fixture.snapshot();
        assert!(
            main_after == main_before,
            "read must not write the database file, not even in place: {}",
            difference(&main_before, &main_after)
        );
        assert!(
            wal_after == wal_before,
            "read must not rewrite the -wal file: {}",
            difference(&wal_before, &wal_after)
        );
        assert_eq!(fixture.journal_mode(), "wal", "read must not change journal_mode");
        assert_eq!(fixture.count(), rows_before, "read must not write source rows");
    }

    #[test]
    fn byte_snapshot_detects_an_in_place_checkpoint() {
        // Keeps the assertion above honest: a real checkpoint does change these
        // bytes, so the comparison cannot pass by accident.
        let fixture = WalFixture::new();
        let before = fixture.snapshot();
        let busy: i64 = fixture
            .writer
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy, 0, "no reader may hold the checkpoint back");
        let after = fixture.snapshot();
        assert!(
            before.0 != after.0,
            "a checkpoint must change the database file: {}",
            difference(&before.0, &after.0)
        );
        // A checkpoint rewrites pages in place, so the file can keep its exact
        // size. That is the case a length comparison cannot see, and the reason
        // the guard test above compares bytes.
        assert_eq!(
            before.0.len(),
            after.0.len(),
            "fixture premise: the in-place rewrite keeps the file size"
        );
    }

    #[test]
    fn a_read_only_source_directory_no_longer_blocks_the_read() {
        // The old path needed to create a `-shm` WAL index next to the source,
        // so a read-only source directory made the read fail. The snapshot path
        // only reads source bytes and opens its own private copy, so the same
        // source directory is readable and the committed `-wal` row still comes
        // back. A privileged runner that ignores directory permissions sees the
        // same successful result, so the assertion holds for both.
        let fixture = WalFixture::new();
        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join("copy.db");
        let source = PathBuf::from(&fixture.path);
        std::fs::copy(&source, &copy).unwrap();
        std::fs::copy(source.with_extension("db-wal"), copy.with_extension("db-wal")).unwrap();

        let mut permissions = std::fs::metadata(dir.path()).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(dir.path(), permissions).unwrap();

        let result = try_query_wechat_db(
            copy.to_str().unwrap(),
            TEST_KEY,
            "SELECT id FROM messages ORDER BY id",
        );

        // Restore write access so the temp directory can be cleaned up.
        let mut permissions = std::fs::metadata(dir.path()).unwrap().permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(dir.path(), permissions).unwrap();

        assert_eq!(
            result.map(|rows| rows.len()),
            Ok(2),
            "the snapshot read needs neither the source -shm nor a writable source directory"
        );
    }

    /// Encrypted database in rollback-journal mode whose writer holds an
    /// EXCLUSIVE lock, so a reader cannot take a shared lock.
    fn exclusive_lock_fixture(name: &str) -> (tempfile::TempDir, String, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name).to_str().unwrap().to_owned();
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                 PRAGMA journal_mode = DELETE;
                 CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT);
                 INSERT INTO messages VALUES (1, 'committed');"
            ))
            .unwrap();
        writer
            .execute_batch("BEGIN EXCLUSIVE; INSERT INTO messages VALUES (2, 'uncommitted');")
            .unwrap();
        (dir, path, writer)
    }

    #[test]
    fn a_writer_lock_does_not_block_the_snapshot_read() {
        // Historical contract: with a rollback-journal EXCLUSIVE lock the read
        // waited for a shared lock and then failed with `DatabaseRead`. The
        // snapshot read takes no lock on the source, so it is neither delayed by
        // the lock nor refused for lock reasons. It must still never turn a lock
        // into a silent empty success.
        let (_dir, path, writer) = exclusive_lock_fixture("locked.db");

        let started = Instant::now();
        let result = try_query_wechat_db(&path, TEST_KEY, "SELECT id FROM messages ORDER BY id");
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(100),
            "the snapshot read must not wait on the writer's lock, took {elapsed:?}"
        );
        match result {
            Ok(rows) => assert!(
                !rows.is_empty(),
                "a writer lock must never become a silent empty result"
            ),
            Err(WechatReadError::DatabaseRead) => {
                // Also acceptable: a hard read error. What is refused is an
                // empty-but-successful answer.
            }
            other => panic!("unexpected outcome under a writer lock: {other:?}"),
        }

        writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_generic_read_that_outlives_the_budget_is_refused() {
        let (_dir, path) = encrypted_fixture();
        let started = Instant::now();
        // A statement with no natural end: only the installed interrupt can stop
        // it, so this proves the generic entry point carries the budget past the
        // `Connection` it returns.
        let result = try_query_wechat_db(
            &path,
            TEST_KEY,
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2000000000)
             SELECT count(*) FROM seq",
        );
        assert_eq!(result, Err(WechatReadError::DatabaseRead));
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the generic read must stay bounded"
        );
    }

    // ── Failure classification and log redaction ───────────────────────────

    /// Marker that must never reach a log event. Underscores only, so it can
    /// also be embedded in a table name.
    const SECRET_MARKER: &str = "zq7_secret_marker";

    #[derive(Clone, Debug)]
    struct CapturedEvent {
        level: tracing::Level,
        message: String,
        fields: Vec<(String, String)>,
    }

    // Buffer of the thread that is currently inside `recorded`. The subscriber
    // is process-wide, so without this the tests would also collect the events
    // of every other test thread, and the "exactly one event" assertions below
    // would become load-dependent.
    thread_local! {
        static CAPTURE: RefCell<Option<Arc<Mutex<Vec<CapturedEvent>>>>> =
            const { RefCell::new(None) };
    }

    static CAPTURING_SUBSCRIBER: Once = Once::new();

    /// Subscriber that records the events of the capturing thread and drops
    /// everything else.
    struct RecordingSubscriber;

    impl RecordingSubscriber {
        /// Install the capturing subscriber as the process-wide default.
        ///
        /// This has to be the *global* default, not a thread-local one:
        /// `tracing` caches a per-callsite `Interest` in process-global state
        /// and computes it from whichever dispatcher is current on the thread
        /// that reaches the callsite first. A thread-local
        /// `tracing::subscriber::with_default` cannot repair that cache, so any
        /// other test thread that fails a WeChat read without a subscriber
        /// caches `Interest::never()` for `read_failure`'s callsites, and this
        /// test then observes no events at all — which is exactly the
        /// `events: []` failure this replaces. Registering globally also raises
        /// the cached maximum level, so no later `Dispatch::new` on another
        /// thread can drop the callsites again.
        fn install() {
            CAPTURING_SUBSCRIBER.call_once(|| {
                tracing::subscriber::set_global_default(RecordingSubscriber).expect(
                    "the log-capturing subscriber must own the process-wide default; \
                     another test installed a global default first",
                );
            });
        }
    }

    impl tracing::Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(
            &self,
            _span: &tracing::span::Id,
            _follows: &tracing::span::Id,
        ) {
        }

        fn event(&self, event: &tracing::Event<'_>) {
            let captured = CAPTURE.try_with(|slot| slot.borrow().clone()).unwrap_or(None);
            let Some(captured) = captured else {
                return;
            };
            let mut visitor = FieldVisitor {
                fields: Vec::new(),
            };
            event.record(&mut visitor);
            captured.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                message: visitor.fields.iter()
                    .find(|(name, _)| name == "message")
                    .map(|(_, value)| value.trim_matches('"').to_owned())
                    .unwrap_or_default(),
                fields: visitor.fields,
            });
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    struct FieldVisitor {
        fields: Vec<(String, String)>,
    }

    impl tracing::field::Visit for FieldVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields.push((field.name().to_string(), value.to_owned()));
        }

        fn record_debug(
            &mut self,
            field: &tracing::field::Field,
            value: &dyn std::fmt::Debug,
        ) {
            self.fields.push((field.name().to_string(), format!("{value:?}")));
        }
    }

    /// Run `action` and return the events it logged on this thread.
    fn recorded<R>(action: impl FnOnce() -> R) -> (R, Vec<CapturedEvent>) {
        RecordingSubscriber::install();
        let captured = Arc::new(Mutex::new(Vec::new()));
        CAPTURE.with(|slot| *slot.borrow_mut() = Some(captured.clone()));
        let result = action();
        CAPTURE.with(|slot| *slot.borrow_mut() = None);
        let events = std::mem::take(&mut *captured.lock().unwrap());
        (result, events)
    }

    /// A failure logs exactly one event, at `level`, with only the fixed reason
    /// and public code, and nothing derived from the SQLite error.
    #[track_caller]
    fn assert_failure_event(events: &[CapturedEvent], reason: &str, level: tracing::Level) {
        assert_eq!(events.len(), 1, "expected one log event, got {events:?}");
        let event = &events[0];
        assert_eq!(event.level, level);
        // `message` is the event's own text, checked separately below.
        let mut fields: Vec<(String, String)> = event
            .fields
            .iter()
            .filter(|(name, _)| name != "message")
            .cloned()
            .collect();
        fields.sort();
        assert_eq!(
            fields,
            vec![
                (
                    "code".to_string(),
                    WechatReadError::DatabaseRead.code().to_string()
                ),
                ("reason".to_string(), reason.to_string()),
            ]
        );
        assert_eq!(event.message, "WeChat database read failed");
        assert!(
            !event.message.contains(SECRET_MARKER),
            "log message leaked detail: {}",
            event.message
        );
        for (name, value) in &event.fields {
            assert!(!value.contains(SECRET_MARKER), "field {name} leaked detail");
        }
    }

    #[test]
    fn read_failures_are_classified_and_log_nothing_sensitive() {
        // cannot_open: the file does not exist. The path and the key carry the
        // marker, so a leak of either would be visible.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join(format!("missing_{SECRET_MARKER}.db"));
        let (result, events) = recorded(|| {
            try_query_wechat_db(
                missing.to_str().unwrap(),
                &format!("{SECRET_MARKER}{SECRET_MARKER}"),
                "SELECT 1",
            )
        });
        assert_eq!(result, Err(WechatReadError::DatabaseRead));
        assert_failure_event(&events, "cannot_open", tracing::Level::WARN);

        // not_a_database: a real database opened with the wrong key.
        let (_fixture_dir, path) = encrypted_fixture();
        let (result, events) = recorded(|| {
            try_query_wechat_db(&path, &"22".repeat(32), "SELECT count(*) FROM sqlite_master")
        });
        assert_eq!(result, Err(WechatReadError::DatabaseRead));
        assert_failure_event(&events, "not_a_database", tracing::Level::WARN);

        // A rollback-journal EXCLUSIVE writer lock no longer produces a failure
        // at all: the snapshot read takes no lock. The new contract and its
        // non-empty guarantee are asserted in
        // `a_writer_lock_does_not_block_the_snapshot_read`; here only that a
        // missing table still classifies as `other`.

        // other: a readable database with a missing table.
        let (result, events) = recorded(|| {
            try_query_wechat_db(&path, TEST_KEY, &format!("SELECT * FROM {SECRET_MARKER}"))
        });
        assert_eq!(result, Err(WechatReadError::DatabaseRead));
        assert_failure_event(&events, "other", tracing::Level::WARN);
    }

    // ── Database path resolution ────────────────────────────────────────────
    //
    // None of these tests may look at the host's real WeChat directories: a
    // mount on the machine running the suite would change what `exists()`
    // returns and turn a mapping check into a statement about the host. The
    // mapping is therefore pinned in two independent parts: the production
    // account roots, which are pure constants, and the composition of those
    // roots with the sub-directory rules, which runs against temp roots.

    /// The account name is unique to the test so a real container mount can
    /// never change the expected default paths.
    const FIXTURE_ACCOUNT: &str = "wxid_path_fixture";

    #[test]
    fn the_account_roots_are_the_production_paths_in_preference_order() {
        assert_eq!(
            account_base_paths(FIXTURE_ACCOUNT),
            vec![
                PathBuf::from(format!("/home/wechat/xwechat_files/{FIXTURE_ACCOUNT}")),
                PathBuf::from(format!("/home/wechat/Documents/xwechat_files/{FIXTURE_ACCOUNT}")),
            ]
        );
    }

    /// The sub-directory `get_db_path` picks for `db_name`, checked against temp
    /// account roots so no host directory can influence the result.
    fn resolved_sub_directory(roots: &[PathBuf], db_name: &str) -> String {
        let resolved = resolve_db_path(roots, sub_dir_for(db_name), db_name);
        Path::new(&resolved)
            .strip_prefix(&roots[0])
            .expect("the fallback must stay inside the first account root")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn shard_database_names_resolve_to_the_message_sub_directory() {
        let dir = tempfile::tempdir().unwrap();
        let roots = temp_account_roots(dir.path());
        for name in [
            "message_0.db",
            "message_1.db",
            "message_10.db",
            "biz_message_0.db",
            "biz_message_3.db",
            "media_0.db",
            "media_2.db",
            "media_11.db",
        ] {
            assert_eq!(
                resolved_sub_directory(&roots, name),
                Path::new("db_storage").join("message").join(name).to_string_lossy(),
                "{name} must live in the shared message sub-directory"
            );
            // The production entry point must pick the same sub-directory; the
            // tail check holds whichever account root it resolves against.
            assert!(
                Path::new(&get_db_path(FIXTURE_ACCOUNT, name))
                    .ends_with(Path::new("db_storage").join("message").join(name)),
                "{name} must live in the shared message sub-directory"
            );
        }
    }

    #[test]
    fn known_mappings_and_the_legacy_fallback_are_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let roots = temp_account_roots(dir.path());
        for (name, sub_dir) in [
            ("contact.db", "contact"),
            ("contact_fts.db", "contact"),
            ("session.db", "session"),
            ("message_fts.db", "message"),
            ("message_resource.db", "message"),
            ("general.db", "general"),
            ("hardlink.db", "hardlink"),
            ("head_image.db", "head_image"),
            ("emoticon.db", "emoticon"),
            ("favorite.db", "favorite"),
            ("favorite_fts.db", "favorite"),
            ("sns.db", "sns"),
            ("bizchat.db", "bizchat"),
        ] {
            let expected = Path::new("db_storage").join(sub_dir).join(name);
            assert_eq!(resolved_sub_directory(&roots, name), expected.to_string_lossy());
            assert!(
                Path::new(&get_db_path(FIXTURE_ACCOUNT, name)).ends_with(&expected),
                "{name} must resolve into {sub_dir}"
            );
        }
        // Unknown names keep the legacy "strip .db" directory behaviour.
        let legacy = Path::new("db_storage").join("future_shard").join("future_shard.db");
        assert_eq!(
            resolved_sub_directory(&roots, "future_shard.db"),
            legacy.to_string_lossy()
        );
        assert!(Path::new(&get_db_path(FIXTURE_ACCOUNT, "future_shard.db")).ends_with(&legacy));
    }

    /// Account roots under a temp directory, in the order the real lookup uses.
    fn temp_account_roots(dir: &Path) -> Vec<PathBuf> {
        vec![
            dir.join("xwechat_files").join(FIXTURE_ACCOUNT),
            dir.join("Documents").join("xwechat_files").join(FIXTURE_ACCOUNT),
        ]
    }

    fn touch_db(root: &Path, sub_dir: &str, db_name: &str) -> PathBuf {
        let path = root.join("db_storage").join(sub_dir).join(db_name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"").unwrap();
        path
    }

    #[test]
    fn sub_directory_rules_cover_shard_families_and_non_families() {
        for name in [
            "message_0.db",
            "message_1.db",
            "message_10.db",
            "biz_message_0.db",
            "biz_message_4.db",
            "media_0.db",
            "media_9.db",
            "message_fts.db",
            "message_resource.db",
        ] {
            assert_eq!(sub_dir_for(name), "message", "{name}");
        }
        for (name, sub_dir) in [
            ("contact.db", "contact"),
            ("contact_fts.db", "contact"),
            ("session.db", "session"),
            ("general.db", "general"),
            ("hardlink.db", "hardlink"),
            ("head_image.db", "head_image"),
            ("emoticon.db", "emoticon"),
            ("favorite.db", "favorite"),
            ("favorite_fts.db", "favorite"),
            ("sns.db", "sns"),
            ("bizchat.db", "bizchat"),
            // Not a numbered shard: the legacy name-derived directory is kept.
            ("message_abc.db", "message_abc"),
            ("message_.db", "message_"),
            ("future_shard", "future_shard"),
        ] {
            assert_eq!(sub_dir_for(name), sub_dir, "{name}");
        }
    }

    #[test]
    fn resolution_prefers_the_first_account_root() {
        let dir = tempfile::tempdir().unwrap();
        let roots = temp_account_roots(dir.path());
        let first = touch_db(&roots[0], "message", "message_1.db");
        touch_db(&roots[1], "message", "message_1.db");

        assert_eq!(
            resolve_db_path(&roots, sub_dir_for("message_1.db"), "message_1.db"),
            first.to_string_lossy()
        );
    }

    #[test]
    fn resolution_falls_back_to_the_alternate_account_root() {
        let dir = tempfile::tempdir().unwrap();
        let roots = temp_account_roots(dir.path());
        let alternate = touch_db(&roots[1], "message", "media_2.db");

        assert_eq!(
            resolve_db_path(&roots, sub_dir_for("media_2.db"), "media_2.db"),
            alternate.to_string_lossy()
        );
    }

    #[test]
    fn resolution_returns_the_first_root_when_no_candidate_exists() {
        let dir = tempfile::tempdir().unwrap();
        let roots = temp_account_roots(dir.path());
        let expected = roots[0]
            .join("db_storage")
            .join("message")
            .join("message_10.db");

        assert_eq!(
            resolve_db_path(&roots, sub_dir_for("message_10.db"), "message_10.db"),
            expected.to_string_lossy()
        );
    }

    // ── Legacy SQLite lock demonstrations ───────────────────────────────────
    //
    // The tests below document SQLite's locking behaviour with the plaintext
    // fixtures they open themselves. They are not the production read path:
    // `try_query_wechat_db` now reads an isolated byte snapshot of the source
    // and opens only its own private copy (`immutable=1`), so it takes no lock
    // on the source and needs no `-shm`. They remain as demonstrations of the
    // SQLite behaviour the old path depended on.

    /// Create a temp DB that simulates WeChat's encrypted DB pattern.
    /// Uses plaintext SQLite (no encryption) since we're testing lock behavior,
    /// not crypto. Lock semantics are identical.
    fn create_test_db(path: &str) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = DELETE;
             CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, content TEXT);
             INSERT INTO messages (content) VALUES ('hello');
             INSERT INTO messages (content) VALUES ('world');",
        )
        .unwrap();
        conn
    }

    /// Plain `SQLITE_OPEN_READ_ONLY` connection: takes shared locks, which is
    /// why the production read also bounds its wait.
    fn open_readonly(path: &str) -> Connection {
        Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    }

    /// `immutable=1` connection, used only by the demonstrations below. It takes
    /// no locks and ignores the `-wal` file. The snapshot reader uses the same
    /// mode, but on a private copy whose WAL was already merged, so it does not
    /// lose the uncheckpointed commit.
    fn open_immutable(path: &str) -> Connection {
        let uri = format!("file:{}?immutable=1", path);
        Connection::open_with_flags(
            &uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    }

    #[test]
    fn immutable_read_does_not_block_writer() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_path_str = db_path.to_str().unwrap();

        // Create DB with DELETE journal mode (not WAL) — worst case for lock contention
        let _setup = create_test_db(db_path_str);
        drop(_setup);

        let path = db_path_str.to_string();
        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: open immutable reader, hold it open, signal writer to proceed
        let b1 = barrier.clone();
        let p1 = path.clone();
        let reader = std::thread::spawn(move || {
            let conn = open_immutable(&p1);
            let count: i64 = conn
                .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
                .unwrap();
            assert!(count >= 2);

            // Signal: reader is holding connection open
            b1.wait();

            // Keep connection alive while writer tries to write
            std::thread::sleep(Duration::from_millis(200));
            drop(conn);
        });

        // Thread 2: wait for reader, then try to write — should NOT be blocked
        let b2 = barrier.clone();
        let p2 = path.clone();
        let writer = std::thread::spawn(move || {
            // Wait for reader to be holding its connection
            b2.wait();

            let start = Instant::now();
            let conn = Connection::open(&p2).unwrap();
            conn.execute_batch("PRAGMA journal_mode = DELETE;").unwrap();
            conn.execute(
                "INSERT INTO messages (content) VALUES (?1)",
                ["from writer"],
            )
            .unwrap();
            let elapsed = start.elapsed();

            // Writer should complete quickly (< 100ms), not blocked by reader
            assert!(
                elapsed < Duration::from_millis(100),
                "Writer was blocked for {:?} — immutable reader is holding locks!",
                elapsed
            );
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn readonly_reader_can_block_writer_in_delete_mode() {
        // Historical demonstration. With DELETE journal mode a read-only reader
        // holds a SHARED lock that can keep the writer from taking an EXCLUSIVE
        // lock. The old production read took part in that protocol and bounded
        // its wait; the snapshot read takes no lock at all, so this now only
        // demonstrates the SQLite behaviour it avoids.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_readonly.db");
        let db_path_str = db_path.to_str().unwrap();

        let _setup = create_test_db(db_path_str);
        drop(_setup);

        let path = db_path_str.to_string();
        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: plain read-only reader with active statement (holds SHARED lock)
        let b1 = barrier.clone();
        let p1 = path.clone();
        let reader = std::thread::spawn(move || {
            let conn = open_readonly(&p1);
            // Start a query to acquire SHARED lock
            let mut stmt = conn.prepare("SELECT * FROM messages").unwrap();
            let _rows: Vec<_> = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect();

            // Signal writer while we still hold the connection
            b1.wait();
            // Hold the connection open
            std::thread::sleep(Duration::from_millis(300));
            drop(stmt);
            drop(conn);
        });

        // Thread 2: try to write while reader holds SHARED lock
        let b2 = barrier.clone();
        let p2 = path.clone();
        let writer = std::thread::spawn(move || {
            b2.wait();

            let conn = Connection::open(&p2).unwrap();
            conn.execute_batch("PRAGMA journal_mode = DELETE; PRAGMA busy_timeout = 0;")
                .unwrap();
            let result = conn.execute(
                "INSERT INTO messages (content) VALUES (?1)",
                ["from writer"],
            );

            // With busy_timeout=0 and DELETE mode, the write may fail with
            // SQLITE_BUSY while the reader's shared lock is still held. This
            // depends on OS-level locking, so the outcome is only logged; the
            // production read bounds its own wait instead of asserting on it.
            match result {
                Ok(_) => eprintln!("[info] Writer succeeded (reader may have released lock)"),
                Err(e) => eprintln!("[expected] Writer blocked/failed as expected: {e}"),
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn immutable_reads_are_consistent_per_connection() {
        // Historical demonstration of an `immutable=1` connection: it is pinned
        // to the database file image as it was when the connection opened, so a
        // long-lived connection can keep serving pre-write data. The snapshot
        // reader uses the same mode but on a private copy with the source WAL
        // already merged, and separate read connections are separate
        // snapshots — no cross-database snapshot is implied.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_consistent.db");
        let db_path_str = db_path.to_str().unwrap();

        let _setup = create_test_db(db_path_str);
        drop(_setup);

        // Open immutable reader — should see 2 rows
        let reader = open_immutable(db_path_str);
        let count_before: i64 = reader
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count_before, 2);

        // Write more data via a separate connection
        {
            let writer = Connection::open(db_path_str).unwrap();
            writer
                .execute("INSERT INTO messages (content) VALUES ('new')", [])
                .unwrap();
        }

        // A pinned immutable connection may or may not observe the new row; it
        // never crashes, corrupts or locks.
        let count_after: i64 = reader
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert!(count_after >= 2); // At least the original data

        drop(reader);

        // A freshly opened immutable connection sees the committed write that is
        // already in the main database file.
        let reader2 = open_immutable(db_path_str);
        let count_fresh: i64 = reader2
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count_fresh, 3, "fresh immutable connection should see the committed write");
    }
}
