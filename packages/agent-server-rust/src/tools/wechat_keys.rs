use super::exec::{exec_command_controlled_group, ExecOptions};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::future::Future;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static KEY_LIFECYCLE: OnceLock<KeyLifecycle> = OnceLock::new();

struct KeyLifecycle {
    gate: std::sync::Arc<Mutex<RefreshGate>>,
    cancel: CancellationToken,
    tasks: std::sync::Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl KeyLifecycle {
    fn new() -> Self {
        Self { gate: std::sync::Arc::new(Mutex::new(RefreshGate::default())), cancel: CancellationToken::new(), tasks: std::sync::Arc::new(Mutex::new(Vec::new())) }
    }

    fn session_probe_with<F>(&self, probe: F) -> impl Future<Output = KeyCheck> + Send + 'static
    where
        F: FnOnce(&super::wechat_messages::CollectionCancel) -> KeyCheck + Send + 'static,
    {
        let tasks = self.tasks.clone();
        let lifecycle_cancel = self.cancel.clone();
        async move {
            let collection_cancel = super::wechat_messages::CollectionCancel::within(super::wechat_snapshot::SNAPSHOT_DEFAULT_BUDGET);
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            {
                let Ok(mut tasks) = tasks.lock() else { return KeyCheck::Unavailable; };
                if lifecycle_cancel.is_cancelled() { return KeyCheck::Unavailable; }
                tasks.retain(|task| !task.is_finished());
                let worker_cancel = lifecycle_cancel.clone();
                let worker_collection_cancel = collection_cancel.clone();
                tasks.push(tokio::spawn(async move {
                    let mut blocking = tokio::task::spawn_blocking(move || probe(&worker_collection_cancel));
                    let status = tokio::select! {
                        result = &mut blocking => result.unwrap_or(KeyCheck::Unavailable),
                        _ = worker_cancel.cancelled() => {
                            collection_cancel.cancel();
                            blocking.await.unwrap_or(KeyCheck::Unavailable)
                        }
                    };
                    let _ = result_tx.send(status);
                }));
            }
            result_rx.await.unwrap_or(KeyCheck::Unavailable)
        }
    }

    fn schedule<F, V, P, PF>(&self, scope: String, required_keys: Vec<String>, helper: F, validate: V, persist: P)
    where
        F: Future + Send + 'static,
        F::Output: Into<RefreshWork>,
        V: Fn() -> bool + Send + 'static,
        P: FnOnce(HashMap<String, String>) -> PF + Send + 'static,
        PF: Future<Output = bool> + Send + 'static,
    {
        if self.cancel.is_cancelled() { return; }
        if !self.gate.lock().map(|mut gate| gate.begin(&scope, Instant::now())).unwrap_or(false) { return; }
        let lease = RefreshLease { scope, gate: self.gate.clone(), finished: false };
        let cancel = self.cancel.clone();
        let task = tokio::spawn(async move {
            match await_or_cancel(&cancel, helper).await.map(Into::into) {
                Some(RefreshWork::Noop) => lease.finish_noop(),
                Some(RefreshWork::Candidates(keys)) if !keys.is_empty() && extraction_covers_required_keys(&keys, &required_keys) => {
                    let success = if validate() { persist(keys).await } else { false };
                    lease.finish(success);
                }
                Some(RefreshWork::Candidates(_)) | None => lease.finish(false),
            }
        });
        if let Ok(mut tasks) = self.tasks.lock() {
            if self.cancel.is_cancelled() { task.abort(); return; }
            tasks.retain(|task| !task.is_finished());
            tasks.push(task);
        } else {
            task.abort();
        }
    }

    async fn shutdown(&self) {
        self.cancel.cancel();
        let pending = self.tasks.lock().map(|mut tasks| std::mem::take(&mut *tasks)).unwrap_or_default();
        for task in pending { let _ = task.await; }
    }
}

enum RefreshWork {
    Noop,
    Candidates(HashMap<String, String>),
}

impl From<HashMap<String, String>> for RefreshWork {
    fn from(keys: HashMap<String, String>) -> Self { Self::Candidates(keys) }
}

fn key_lifecycle() -> &'static KeyLifecycle {
    KEY_LIFECYCLE.get_or_init(KeyLifecycle::new)
}

#[derive(Default)]
struct RefreshGate(HashMap<String, RefreshStatus>);

struct RefreshStatus {
    in_flight: bool,
    failures: u8,
    retry_at: Instant,
}

struct RefreshLease {
    scope: String,
    gate: std::sync::Arc<Mutex<RefreshGate>>,
    finished: bool,
}

impl RefreshLease {
    fn finish(mut self, success: bool) {
        if let Ok(mut gate) = self.gate.lock() {
            gate.finish(&self.scope, success, Instant::now());
        }
        self.finished = true;
    }

    fn finish_noop(mut self) {
        if let Ok(mut gate) = self.gate.lock() {
            gate.finish_noop(&self.scope);
        }
        self.finished = true;
    }
}

impl Drop for RefreshLease {
    fn drop(&mut self) {
        if !self.finished {
            if let Ok(mut gate) = self.gate.lock() {
                gate.finish(&self.scope, false, Instant::now());
            }
        }
    }
}

impl RefreshGate {
    fn begin(&mut self, scope: &str, now: Instant) -> bool {
        let state = self.0.entry(scope.to_owned()).or_insert(RefreshStatus {
            in_flight: false, failures: 0, retry_at: now,
        });
        if state.in_flight || now < state.retry_at { return false; }
        state.in_flight = true;
        true
    }

    fn finish(&mut self, scope: &str, success: bool, now: Instant) {
        let Some(state) = self.0.get_mut(scope) else { return; };
        state.in_flight = false;
        let delay = if success {
            state.failures = 0;
            60
        } else {
            state.failures = state.failures.saturating_add(1).min(6);
            (30u64.saturating_mul(1u64 << (state.failures - 1))).min(900)
        };
        state.retry_at = now + Duration::from_secs(delay);
    }

    fn finish_noop(&mut self, scope: &str) {
        if let Some(state) = self.0.get_mut(scope) { state.in_flight = false; }
    }
}

/// Schedule one extraction per canonical session/account/PID scope. Expensive
/// work runs outside request, UI and agent-database lock lifetimes.
pub fn schedule_key_refresh(session_id: String, account: String, pid: i64) {
    schedule_key_refresh_mode(session_id, account, pid, false, Vec::new());
}

fn schedule_key_refresh_for_keys(session_id: String, account: String, pid: i64, required_keys: Vec<String>) {
    schedule_key_refresh_mode(session_id, account, pid, false, required_keys);
}

pub fn schedule_session_key_check_after_login(session_id: String, account: String, pid: i64) {
    schedule_key_refresh_mode(session_id, account, pid, true, vec!["session.db".to_string()]);
}

fn schedule_key_refresh_mode(session_id: String, account: String, pid: i64, check_session_key: bool, required_keys: Vec<String>) {
    let lifecycle = key_lifecycle();
    let cancellation = lifecycle.cancel.clone();
    if cancellation.is_cancelled() { return; }
    let Some(start_time) = process_start_time(pid) else { return; };
    let Some(database_root) = process_database_root(pid, &account) else { return; };
    let helper_account = account.clone();
    let helper_root = database_root.clone();
    let check_root = database_root.clone();
    let check_account = account.clone();
    let fence_start = start_time.clone();
    let persist_start = start_time.clone();
    let persist_account = account.clone();
    let persist_root = database_root.clone();
    let Ok(db) = crate::db::try_get_db(Duration::from_millis(25)) else { return; };
    let required_keys = if required_keys.is_empty() && !check_session_key {
        let canonical = crate::db::queries::resolve_session_id(&db, &session_id)
            .ok().map(|handle| handle.canonical_id().to_owned());
        canonical.map(|canonical| required_missing_keys(&get_stored_keys(&db, &canonical, &account), &database_root))
            .unwrap_or_default()
    } else {
        required_keys
    };
    schedule_refresh_adapter(
        lifecycle, &db, &session_id, &account, pid, &start_time, &database_root, check_session_key, required_keys,
        move || async move { extract_keys_async(pid, &helper_account, &helper_root).await },
        move || process_start_time(pid).as_deref() == Some(fence_start.as_str())
            && process_database_root(pid, &check_account).as_deref() == Some(check_root.as_path()),
        move |canonical, generation, keys| async move {
            let store_account = persist_account;
            let store_root = persist_root;
            tokio::task::spawn_blocking(move || {
                commit_extracted_keys(pid, &persist_start, &store_root, &canonical, &store_account, generation, &keys)
            }).await.unwrap_or(false)
        },
    );
}

pub fn request_refresh_after_auth_failure(session_id: &str, account: &str, failed_database: &str) {
    request_refresh_for_keys(session_id, account, vec![failed_database.to_string()]);
}

pub fn request_refresh_after_missing_keys(
    session_id: &str,
    account: &str,
    root: &Path,
    stored: &HashMap<String, String>,
    needs: Vec<KeyNeed>,
) {
    let mut required: Vec<String> = needs.into_iter()
        .flat_map(|need| missing_keys_for_need(stored, root, need))
        .collect();
    required.sort();
    required.dedup();
    if !required.is_empty() { request_refresh_for_keys(session_id, account, required); }
}

fn request_refresh_for_keys(session_id: &str, account: &str, required: Vec<String>) {
    let Ok(db) = crate::db::try_get_db(Duration::from_millis(25)) else { return; };
    let pid = db.query_row(
        "SELECT wechat_pid FROM sessions WHERE id=?1", [session_id], |row| row.get::<_, Option<i64>>(0),
    ).ok().flatten();
    let current_account = crate::db::queries::get_session_logged_in_user(&db, session_id);
    drop(db);
    if current_account.as_deref() == Some(account) {
        if let Some(pid) = pid { schedule_key_refresh_for_keys(session_id.to_owned(), account.to_owned(), pid, required); }
    }
}

async fn await_or_cancel<F: Future>(cancel: &CancellationToken, work: F) -> Option<F::Output> {
    tokio::select! {
        _ = cancel.cancelled() => None,
        result = work => Some(result),
    }
}

pub async fn shutdown_key_lifecycle() {
    key_lifecycle().shutdown().await;
}

fn process_start_time(pid: i64) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_process_start_time(&stat)
}

fn parse_process_start_time(stat: &str) -> Option<String> {
    stat.rsplit_once(')')?.1.split_whitespace().nth(19).map(str::to_owned)
}

fn refresh_scope_key(session_id: &str, account: &str, pid: i64, start: &str, generation: u64, root: &str) -> String {
    format!("{session_id}\0{account}\0{pid}\0{start}\0{generation}\0{root}")
}

fn resolve_refresh_scope(
    conn: &Connection,
    requested_session: &str,
    account: &str,
    pid: i64,
    start: &str,
    root: &Path,
) -> Option<(String, u64, String)> {
    let canonical = crate::db::queries::resolve_session_id(conn, requested_session).ok()?.canonical_id().to_owned();
    let generation = crate::db::queries::get_session_login_generation(conn, &canonical)?;
    let scope = refresh_scope_key(&canonical, account, pid, start, generation, &root.to_string_lossy());
    Some((canonical, generation, scope))
}

fn schedule_refresh_adapter<H, HF, V, P, PF>(
    lifecycle: &KeyLifecycle,
    conn: &Connection,
    requested_session: &str,
    account: &str,
    pid: i64,
    start: &str,
    root: &Path,
    check_session_key: bool,
    required_keys: Vec<String>,
    extract: H,
    validate: V,
    persist: P,
) -> bool
where
    H: FnOnce() -> HF + Send + 'static,
    HF: Future<Output = HashMap<String, String>> + Send + 'static,
    V: Fn() -> bool + Send + Sync + 'static,
    P: FnOnce(String, u64, HashMap<String, String>) -> PF + Send + 'static,
    PF: Future<Output = bool> + Send + 'static,
{
    let Some((canonical, generation, scope)) = resolve_refresh_scope(conn, requested_session, account, pid, start, root) else {
        return false;
    };
    let session_key = conn.query_row(
        "SELECT hex_key FROM wechat_keys WHERE session_id=?1 AND account_dir=?2 AND db_name='session.db'",
        params![canonical, account], |row| row.get::<_, String>(0),
    ).optional().ok().flatten();
    let probe_path = super::wechat_db::get_db_path_under_root(root, "session.db");
    let probe = if check_session_key {
        session_key.map(|key| lifecycle.session_probe_with(move |cancel| {
            verify_key_status_with_cancel(&probe_path.to_string_lossy(), &key, cancel)
        }))
    } else { None };
    let validation = std::sync::Arc::new(validate);
    let helper_validation = validation.clone();
    let helper = async move {
        let mut extract = Some(extract);
        if check_session_key {
            if let Some(probe) = probe {
                return session_key_check_then_extract(
                    probe,
                    move || helper_validation(),
                    move || async move { extract.take().expect("single extraction callback")().await },
                ).await;
            }
        }
        RefreshWork::Candidates(extract.take().expect("single extraction callback")().await)
    };
    lifecycle.schedule(
        scope,
        required_keys,
        helper,
        move || validation(),
        move |keys| persist(canonical, generation, keys),
    );
    true
}

fn database_root_from_target(target: &str, account: &str) -> Option<PathBuf> {
    if target.ends_with(" (deleted)") || !target.ends_with(".db") { return None; }
    let parts: Vec<_> = target.split('/').collect();
    let index = parts.windows(3).position(|window| window[0] == "xwechat_files" && window[1] == account && window[2] == "db_storage")?;
    Some(PathBuf::from(parts[..index + 3].join("/")))
}

fn process_database_root(pid: i64, account: &str) -> Option<PathBuf> {
    let mut roots = std::collections::HashSet::new();
    for fd in std::fs::read_dir(format!("/proc/{pid}/fd")).ok()? {
        let target = std::fs::read_link(fd.ok()?.path()).ok()?;
        if let Some(root) = database_root_from_target(target.to_str()?, account).and_then(|root| root.canonicalize().ok()) {
            roots.insert(root);
        }
    }
    (roots.len() == 1).then(|| roots.into_iter().next()).flatten()
}

/// Extract all WeChat DB credentials (async, non-blocking).
/// Calls the Python extract-keys script.
pub async fn extract_keys_async(wechat_pid: i64, expected_account: &str, database_root: &Path) -> HashMap<String, String> {
    let pid = wechat_pid.to_string();
    let database_root = database_root.to_string_lossy();
    let args = ["/opt/tools/extract-keys.py", "--pid", &pid, "--stdout-json", "--database-root", database_root.as_ref()];
    let result = exec_command_controlled_group(
        "python3",
        &args,
        &ExecOptions { timeout_ms: 120_000, ..Default::default() },
        std::time::Duration::from_secs(120),
    ).await;

    // Exit 1 is the documented partial-success result. Its JSON may still
    // contain independently page-HMAC-authenticated database candidates.
    let extracted = parse_extracted_keys(&result.stdout, expected_account, database_root.as_ref());
    if let Some(map) = extracted.as_ref() {

        let db_keys: Vec<_> = map.keys().filter(|k| !k.starts_with('_')).collect();
        let has_image_aes = map.contains_key("_image_aes");
        tracing::info!(
            "[wechat-keys] Extracted {} DB keys, image key: {}",
            db_keys.len(),
            if has_image_aes { "yes" } else { "no" }
        );
    }
    if result.timed_out {
        tracing::warn!("[wechat-keys] extraction timed out");
    } else if result.exit_code != 0 && extracted.is_none() {
        tracing::warn!("[wechat-keys] extraction produced no valid result (exit {})", result.exit_code);
    }
    extracted.unwrap_or_default()
}

fn parse_extracted_keys(output: &str, expected_account: &str, expected_root: &str) -> Option<HashMap<String, String>> {
    let parsed: serde_json::Value = serde_json::from_str(output).ok()?;
    if parsed.get("account").and_then(serde_json::Value::as_str) != Some(expected_account) {
        return None;
    }
    if parsed.get("database_root").and_then(serde_json::Value::as_str) != Some(expected_root) { return None; }
    let keys = parsed.get("keys")?.as_object()?;
    Some(keys.iter().filter_map(|(name, value)| {
        let value = value.as_str()?;
        (valid_db_name(name) || valid_image_key(name, value))
            .then(|| (name.clone(), value.to_string()))
    }).collect())
}

fn valid_db_name(name: &str) -> bool {
    name.ends_with(".db") && name.len() > 3
        && name != ".db" && name != "..db"
        && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_image_key(name: &str, value: &str) -> bool {
    match name {
        "_image_aes" => value.len() == 32 && value.bytes().all(|b| b.is_ascii_hexdigit()),
        "_image_xor" => value.len() == 2 && value.bytes().all(|b| b.is_ascii_hexdigit()),
        _ => false,
    }
}

struct PreparedKey {
    name: String,
    value: String,
    previous: Option<String>,
}

#[derive(Default)]
struct PreparedKeys {
    database: Vec<PreparedKey>,
    image: Vec<PreparedKey>,
}

/// Verify every candidate against the isolated source before acquiring the
/// shared agent database lock. An old key is replaceable only when its private
/// snapshot reaches the explicit SQLCipher authentication-failure result.
fn prepare_extracted_keys(
    database_root: &Path,
    candidates: &HashMap<String, String>,
    previous: &HashMap<String, String>,
) -> PreparedKeys {
    let mut prepared = PreparedKeys::default();
    for (name, value) in candidates {
        if valid_image_key(name, value) {
            if !previous.contains_key(name) {
                prepared.image.push(PreparedKey { name: name.clone(), value: value.clone(), previous: previous.get(name).cloned() });
            }
            continue;
        }
        if !valid_db_name(name) { continue; }
        let path = super::wechat_db::get_db_path_under_root(database_root, name);
        if !path.canonicalize().ok().is_some_and(|resolved| resolved.starts_with(database_root)) { continue; }
        let old = previous.get(name).cloned();
        let path = path.as_path();
        let acceptable = match old.as_deref() {
            Some(old) if old == value => false,
            Some(old) => super::wechat_snapshot::verify_key_replacement(path, value, old),
            None => verify_key_status(&path.to_string_lossy(), value) == KeyCheck::Valid,
        };
        if !acceptable {
            continue;
        }
        prepared.database.push(PreparedKey { name: name.clone(), value: value.clone(), previous: old });
    }
    prepared
}

fn write_prepared_keys(conn: &Connection, session: &str, account: &str, prepared: PreparedKeys) -> bool {
    let now = chrono::Utc::now().to_rfc3339();
    let mut written = 0usize;
    for key in prepared.image.into_iter().chain(prepared.database) {
        let current: Option<String> = match conn.query_row(
            "SELECT hex_key FROM wechat_keys WHERE session_id=?1 AND account_dir=?2 AND db_name=?3",
            params![session, account, key.name], |row| row.get(0),
        ).optional() {
            Ok(value) => value,
            Err(_) => return false,
        };
        // A concurrent update wins; the next reconciliation can assess it.
        if current != key.previous { continue; }
        let id = uuid::Uuid::new_v4().to_string();
        if conn.execute(
            "INSERT INTO wechat_keys (id, session_id, account_dir, db_name, hex_key, verified_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id, account_dir, db_name) DO UPDATE SET
               hex_key=excluded.hex_key, verified_at=excluded.verified_at",
            params![id, session, account, key.name, key.value, now],
        ).is_err() { return false; }
        written += 1;
    }
    written > 0
}

fn commit_prepared_keys_with<F>(
    conn: &mut Connection,
    session: &str,
    account: &str,
    pid: i64,
    generation: u64,
    prepared: PreparedKeys,
    mut os_fence: F,
) -> bool
where
    F: FnMut(&Connection) -> bool,
{
    let Ok(tx) = conn.transaction() else { return false; };
    if !session_binding_matches(&tx, session, account, pid, generation) || !os_fence(&tx) {
        return false;
    }
    if !write_prepared_keys(&tx, session, account, prepared) || !os_fence(&tx) {
        return false;
    }
    if !session_binding_matches(&tx, session, account, pid, generation) {
        return false;
    }
    tx.commit().is_ok()
}

fn session_binding_matches(conn: &Connection, session: &str, account: &str, pid: i64, generation: u64) -> bool {
    let current_pid = conn.query_row(
        "SELECT wechat_pid FROM sessions WHERE id=?1", [session], |row| row.get::<_, Option<i64>>(0),
    ).ok().flatten();
    current_pid == Some(pid)
        && crate::db::queries::get_session_logged_in_user(conn, session).as_deref() == Some(account)
        && crate::db::queries::get_session_login_generation(conn, session) == Some(generation)
}

fn commit_extracted_keys(
    pid: i64,
    start_time: &str,
    root: &Path,
    session: &str,
    account: &str,
    generation: u64,
    candidates: &HashMap<String, String>,
) -> bool {
    if process_start_time(pid).as_deref() != Some(start_time)
        || process_database_root(pid, account).as_deref() != Some(root)
    { return false; }
    let previous = {
        let Ok(db) = crate::db::try_get_db(Duration::from_millis(25)) else { return false; };
        if !session_binding_matches(&db, session, account, pid, generation) { return false; }
        get_stored_keys(&db, session, account)
    };
    let prepared = prepare_extracted_keys(root, candidates, &previous);
    let Ok(mut db) = crate::db::try_get_db(Duration::from_millis(50)) else { return false; };
    commit_prepared_keys_with(&mut db, session, account, pid, generation, prepared, |_| {
        process_start_time(pid).as_deref() == Some(start_time)
            && process_database_root(pid, account).as_deref() == Some(root)
    })
}

/// Get stored keys for a session + account from the agent DB.
pub fn get_stored_keys(
    conn: &Connection,
    session_id: &str,
    account_dir: &str,
) -> HashMap<String, String> {
    let mut stmt = conn
        .prepare(
            "SELECT db_name, hex_key FROM wechat_keys
             WHERE session_id = ?1 AND account_dir = ?2",
        )
        .unwrap();

    let rows = stmt
        .query_map(params![session_id, account_dir], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
            ))
        })
        .unwrap();

    let mut map = HashMap::new();
    for row in rows.flatten() {
        map.insert(row.0, row.1);
    }
    map
}

/// Verify a single key against a database file.
/// Reads an isolated byte snapshot of the file instead of opening the live
/// database with SQLite, so the check takes no lock on the source and needs no
/// `-shm`. A wrong key fails the copy's first schema read.
pub fn verify_key(db_path: &str, hex_key: &str) -> bool {
    super::wechat_snapshot::open_snapshot_conn(std::path::Path::new(db_path), hex_key, None).is_ok()
}

pub(crate) fn needs_extraction_from_keys(stored_keys: &HashMap<String, String>, database_root: &Path) -> bool {
    if stored_keys.is_empty() {
        tracing::info!("[wechat-keys] No stored keys, extraction needed");
        return true;
    }

    let required_exact: &[&str] = &[
        "session.db",
        "contact.db",
        "emoticon.db",
        "head_image.db",
        "hardlink.db",
    ];
    let required_prefixes: &[&str] = &["message_", "media_"];

    // Check required exact keys are present (regardless of disk scan)
    let missing_required: Vec<&str> = required_exact
        .iter()
        .filter(|name| !stored_keys.contains_key(**name))
        .copied()
        .collect();

    if !missing_required.is_empty() {
        tracing::info!(
            "[wechat-keys] Missing required keys: {}",
            missing_required.join(", ")
        );
        return true;
    }

    // Scan only the root captured from this process's open database descriptors.
    let existing_dbs = super::wechat_db::list_db_names_under_root(database_root);
    tracing::debug!(
        "[wechat-keys] Disk scan: {} files for account {}",
        existing_dbs.len(),
        "captured-root"
    );

    let missing_on_disk: Vec<_> = existing_dbs
        .iter()
        .filter(|name| {
            required_prefixes.iter().any(|p| name.starts_with(p))
                && !stored_keys.contains_key(name.as_str())
        })
        .collect();

    if !missing_on_disk.is_empty() {
        tracing::info!(
            "[wechat-keys] Missing keys for on-disk DBs: {}",
            missing_on_disk.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
        );
        return true;
    }

    tracing::info!(
        "[wechat-keys] Required stored keys are present, image_aes={}",
        stored_keys.contains_key("_image_aes")
    );
    false
}

/// Broad inventory is reserved for the existing login-time completeness check;
/// read-triggered recovery uses `missing_keys_for_need` below.
fn missing_database_keys_for_login(stored_keys: &HashMap<String, String>, database_root: &Path) -> Vec<String> {
    super::wechat_db::list_db_names_under_root(database_root)
        .into_iter()
        .filter(|name| !stored_keys.contains_key(name))
        .collect()
}

#[derive(Clone)]
pub(crate) enum KeyNeed {
    Database(String),
    MessageShards,
    MediaShards,
}

fn missing_keys_for_need(stored_keys: &HashMap<String, String>, database_root: &Path, need: KeyNeed) -> Vec<String> {
    match need {
        KeyNeed::Database(name) => {
            if stored_keys.contains_key(&name) || !valid_db_name(&name) { return Vec::new(); }
            let path = super::wechat_db::get_db_path_under_root(database_root, &name);
            std::fs::symlink_metadata(path).ok()
                .filter(|metadata| metadata.file_type().is_file())
                .map(|_| vec![name])
                .unwrap_or_default()
        }
        KeyNeed::MessageShards => missing_shard_keys(stored_keys, database_root, "message_"),
        KeyNeed::MediaShards => missing_shard_keys(stored_keys, database_root, "media_"),
    }
}

fn missing_shard_keys(stored_keys: &HashMap<String, String>, database_root: &Path, prefix: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(database_root.join("message")) else { return Vec::new(); };
    entries.flatten().filter_map(|entry| {
        if !entry.file_type().ok()?.is_file() { return None; }
        let name = entry.file_name().into_string().ok()?;
        let digits = name.strip_prefix(prefix)?.strip_suffix(".db")?;
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) { return None; }
        (!stored_keys.contains_key(&name)).then_some(name)
    }).collect()
}

fn required_missing_keys(stored_keys: &HashMap<String, String>, database_root: &Path) -> Vec<String> {
    let mut missing: Vec<String> = ["session.db", "contact.db", "emoticon.db", "head_image.db", "hardlink.db"]
        .iter().filter(|name| !stored_keys.contains_key(**name)).map(|name| (*name).to_string()).collect();
    missing.extend(missing_database_keys_for_login(stored_keys, database_root));
    missing.sort();
    missing.dedup();
    missing
}

pub(crate) fn needs_extraction_for_process(
    stored_keys: &HashMap<String, String>,
    account: &str,
    pid: i64,
) -> bool {
    if stored_keys.is_empty() { return true; }
    let Some(root) = process_database_root(pid, account) else { return true; };
    needs_extraction_from_keys(stored_keys, &root)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyCheck { Valid, Invalid, Unavailable }

fn verify_key_status(db_path: &str, hex_key: &str) -> KeyCheck {
    match super::wechat_snapshot::open_snapshot_conn(std::path::Path::new(db_path), hex_key, None) {
        Ok(_) => KeyCheck::Valid,
        Err(super::wechat_snapshot::SnapshotError::NotADatabase) => KeyCheck::Invalid,
        Err(_) => KeyCheck::Unavailable,
    }
}

fn verify_key_status_with_cancel(
    db_path: &str,
    hex_key: &str,
    cancel: &super::wechat_messages::CollectionCancel,
) -> KeyCheck {
    match super::wechat_snapshot::open_snapshot_conn(std::path::Path::new(db_path), hex_key, Some(cancel)) {
        Ok(_) => KeyCheck::Valid,
        Err(super::wechat_snapshot::SnapshotError::NotADatabase) => KeyCheck::Invalid,
        Err(_) => KeyCheck::Unavailable,
    }
}

async fn session_key_check_then_extract<P, C, F, Fut>(probe: P, scope_is_current: C, extract: F) -> RefreshWork
where
    P: Future<Output = KeyCheck>,
    C: FnOnce() -> bool,
    F: FnOnce() -> Fut,
    Fut: Future<Output = HashMap<String, String>>,
{
    match probe.await {
        KeyCheck::Invalid if scope_is_current() => RefreshWork::Candidates(extract().await),
        KeyCheck::Invalid | KeyCheck::Valid | KeyCheck::Unavailable => RefreshWork::Noop,
    }
}

/// Get stored image keys.
pub fn get_image_keys(
    conn: &Connection,
    session_id: &str,
    account_dir: &str,
) -> Option<(String, Option<u8>)> {
    let keys = get_stored_keys(conn, session_id, account_dir);
    let aes_hex = keys.get("_image_aes")?;
    let xor_byte = keys
        .get("_image_xor")
        .and_then(|h| u8::from_str_radix(h, 16).ok());
    Some((aes_hex.clone(), xor_byte))
}

fn extraction_covers_required_keys(candidates: &HashMap<String, String>, required: &[String]) -> bool {
    required.iter().all(|name| candidates.contains_key(name))
}

#[cfg(test)]
mod on_demand_recovery_tests {
    use super::*;

    #[test]
    fn nonempty_partial_extraction_does_not_cover_the_requested_keys() {
        let candidates = HashMap::from([
            ("contact.db".to_string(), "aa".to_string()),
            ("message_0.db".to_string(), "bb".to_string()),
        ]);
        let required = ["message_0.db".to_string(), "media_0.db".to_string()];
        assert!(!extraction_covers_required_keys(&candidates, &required));
    }

    #[test]
    fn extraction_is_complete_only_when_every_requested_key_is_present() {
        let candidates = HashMap::from([
            ("message_0.db".to_string(), "bb".to_string()),
            ("media_0.db".to_string(), "cc".to_string()),
        ]);
        let required = ["message_0.db".to_string(), "media_0.db".to_string()];
        assert!(extraction_covers_required_keys(&candidates, &required));
    }

    #[test]
    fn newly_created_message_and_media_shards_are_missing_key_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("xwechat_files").join("acct").join("db_storage");
        std::fs::create_dir_all(root.join("message")).unwrap();
        std::fs::write(root.join("message").join("message_12.db"), b"fixture").unwrap();
        std::fs::write(root.join("message").join("media_4.db"), b"fixture").unwrap();
        let stored = HashMap::from([("message_11.db".to_string(), "aa".to_string())]);
        let mut missing = missing_keys_for_need(&stored, &root, KeyNeed::MessageShards);
        missing.extend(missing_keys_for_need(&stored, &root, KeyNeed::MediaShards));
        missing.sort();
        assert_eq!(missing, vec!["media_4.db", "message_12.db"]);
    }

    #[test]
    fn contact_key_recovery_does_not_request_unrelated_message_or_media_keys() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("xwechat_files").join("acct").join("db_storage");
        for (directory, name) in [
            ("contact", "contact.db"),
            ("message", "message_12.db"),
            ("message", "media_4.db"),
        ] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
            std::fs::write(root.join(directory).join(name), b"fixture").unwrap();
        }
        let stored = HashMap::new();
        let missing = missing_keys_for_need(&stored, &root, KeyNeed::Database("contact.db".to_string()));
        assert_eq!(missing, vec!["contact.db"]);
        assert_eq!(missing_keys_for_need(&stored, &root, KeyNeed::MessageShards), vec!["message_12.db"]);
        assert_eq!(missing_keys_for_need(&stored, &root, KeyNeed::MediaShards), vec!["media_4.db"]);
    }

    #[test]
    fn refresh_gate_is_single_flight_and_uses_bounded_exponential_backoff() {
        let now = Instant::now();
        let mut gate = RefreshGate::default();
        assert!(gate.begin("scope", now));
        assert!(!gate.begin("scope", now));
        gate.finish("scope", false, now);
        assert!(!gate.begin("scope", now + Duration::from_secs(29)));
        assert!(gate.begin("scope", now + Duration::from_secs(30)));
        gate.finish("scope", false, now + Duration::from_secs(30));
        assert!(!gate.begin("scope", now + Duration::from_secs(89)));
        assert!(gate.begin("scope", now + Duration::from_secs(90)));
        gate.finish("scope", false, now + Duration::from_secs(90));
        assert!(!gate.begin("scope", now + Duration::from_secs(209)));
        assert!(gate.begin("scope", now + Duration::from_secs(210)));
        gate.finish("scope", false, now + Duration::from_secs(210));
        assert!(gate.begin("scope", now + Duration::from_secs(450)));
        gate.finish("scope", false, now + Duration::from_secs(450));
        assert!(gate.begin("scope", now + Duration::from_secs(930)));
        gate.finish("scope", false, now + Duration::from_secs(930));
        assert!(!gate.begin("scope", now + Duration::from_secs(1829)));
        assert!(gate.begin("scope", now + Duration::from_secs(1830)));
        gate.finish("scope", true, now + Duration::from_secs(1830));
        assert!(!gate.begin("scope", now + Duration::from_secs(1889)));
        assert!(gate.begin("scope", now + Duration::from_secs(1890)));
    }

    #[test]
    fn process_account_login_and_database_root_are_part_of_the_refresh_scope() {
        let base = refresh_scope_key("session", "account-a", 101, "start-a", 1, "/db/a");
        assert_ne!(base, refresh_scope_key("session", "account-b", 101, "start-a", 1, "/db/a"));
        assert_ne!(base, refresh_scope_key("session", "account-a", 102, "start-a", 1, "/db/a"));
        assert_ne!(base, refresh_scope_key("session", "account-a", 101, "start-a", 2, "/db/a"));
        assert_ne!(base, refresh_scope_key("session", "account-a", 101, "start-a", 1, "/db/b"));
    }

    #[tokio::test]
    async fn partial_nonempty_extraction_enters_failure_backoff_without_persisting() {
        let lifecycle = KeyLifecycle::new();
        lifecycle.schedule(
            "partial-scope".to_string(),
            vec!["message_12.db".to_string(), "media_4.db".to_string()],
            async {
                RefreshWork::Candidates(HashMap::from([("message_12.db".to_string(), "aa".to_string())]))
            },
            || true,
            |_| async { panic!("incomplete candidates must not be persisted") },
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let finished = lifecycle.gate.lock().unwrap()
                    .0.get("partial-scope").is_some_and(|status| !status.in_flight);
                if finished { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();

        let now = Instant::now();
        let mut gate = lifecycle.gate.lock().unwrap();
        assert_eq!(gate.0.get("partial-scope").unwrap().failures, 1);
        assert!(!gate.begin("partial-scope", now));
        assert!(gate.begin("partial-scope", now + Duration::from_secs(30)));
        drop(gate);
        lifecycle.shutdown().await;
    }

    #[tokio::test]
    async fn different_inflight_need_is_deduped_then_retried_after_cooldown() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let lifecycle = KeyLifecycle::new();
        let extraction_count = std::sync::Arc::new(AtomicUsize::new(0));
        let persisted = std::sync::Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let first_count = extraction_count.clone();
        let first_persisted = persisted.clone();
        lifecycle.schedule(
            "shared-scope".to_string(),
            vec!["message_12.db".to_string()],
            async move {
                first_count.fetch_add(1, Ordering::SeqCst);
                let _ = release_rx.await;
                RefreshWork::Candidates(HashMap::from([("message_12.db".to_string(), "aa".to_string())]))
            },
            || true,
            move |keys| async move {
                first_persisted.lock().unwrap().push(keys.keys().cloned().collect());
                true
            },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while extraction_count.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
        }).await.unwrap();

        let concurrent_count = extraction_count.clone();
        lifecycle.schedule(
            "shared-scope".to_string(),
            vec!["media_4.db".to_string()],
            async move {
                concurrent_count.fetch_add(1, Ordering::SeqCst);
                RefreshWork::Candidates(HashMap::from([("media_4.db".to_string(), "bb".to_string())]))
            },
            || true,
            |_| async { true },
        );
        assert_eq!(extraction_count.load(Ordering::SeqCst), 1);
        let _ = release_tx.send(());

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if lifecycle.gate.lock().unwrap().0.get("shared-scope").is_some_and(|state| !state.in_flight) { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();

        let cooldown_count = extraction_count.clone();
        lifecycle.schedule(
            "shared-scope".to_string(),
            vec!["media_4.db".to_string()],
            async move {
                cooldown_count.fetch_add(1, Ordering::SeqCst);
                RefreshWork::Candidates(HashMap::from([("media_4.db".to_string(), "bb".to_string())]))
            },
            || true,
            |_| async { true },
        );
        assert_eq!(extraction_count.load(Ordering::SeqCst), 1);

        {
            let mut gate = lifecycle.gate.lock().unwrap();
            gate.0.get_mut("shared-scope").unwrap().retry_at = Instant::now() - Duration::from_secs(1);
        }
        let retry_count = extraction_count.clone();
        let retry_persisted = persisted.clone();
        lifecycle.schedule(
            "shared-scope".to_string(),
            vec!["media_4.db".to_string()],
            async move {
                retry_count.fetch_add(1, Ordering::SeqCst);
                RefreshWork::Candidates(HashMap::from([("media_4.db".to_string(), "bb".to_string())]))
            },
            || true,
            move |keys| async move {
                retry_persisted.lock().unwrap().push(keys.keys().cloned().collect());
                true
            },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while extraction_count.load(Ordering::SeqCst) < 2 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(extraction_count.load(Ordering::SeqCst), 2);
        assert!(persisted.lock().unwrap()[1].contains(&"media_4.db".to_string()));
        lifecycle.shutdown().await;
    }
}
