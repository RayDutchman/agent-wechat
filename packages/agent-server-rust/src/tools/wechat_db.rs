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
