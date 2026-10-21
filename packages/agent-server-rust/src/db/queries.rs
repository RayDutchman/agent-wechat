use rusqlite::{params, Connection};

// ============================================
// SYNC STATE QUERIES
// ============================================

pub fn get_sync_state(conn: &Connection, key: &str, session_id: Option<&str>) -> Option<String> {
    let result = match session_id {
        Some(sid) => conn
            .query_row(
                "SELECT value FROM sync_state WHERE key = ?1 AND session_id = ?2",
                params![key, sid],
                |row| row.get::<_, String>(0),
            )
            .ok(),
        None => conn
            .query_row(
                "SELECT value FROM sync_state WHERE key = ?1 AND session_id IS NULL",
                params![key],
                |row| row.get::<_, String>(0),
            )
            .ok(),
    };
    result
}

pub fn set_sync_state(conn: &Connection, key: &str, value: &str, session_id: Option<&str>) {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO sync_state (session_id, key, value, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(session_id, key) DO UPDATE SET
           value = excluded.value,
           updated_at = excluded.updated_at",
        params![session_id, key, value, now],
    )
    .ok();
}

// ============================================
// SESSION QUERIES
// ============================================

pub fn get_session_logged_in_user(conn: &Connection, session_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT logged_in_user FROM sessions WHERE id = ?1",
        params![session_id],
        |row| row.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
}

/// The login facts one read scope needs, read as a single statement.
///
/// A single statement is deliberate: a read scope that mixed two queries could
/// combine an account from one commit with a generation from another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLoginSnapshot {
    pub account: Option<String>,
    pub login_state: String,
    pub generation: u64,
    /// A login write of this session is not known to have advanced the
    /// generation, so the row still names the previous login.
    pub login_write_unverified: bool,
    /// The canonical `sessions.id` of the resolved row.
    ///
    /// Read in the same statement as the facts above, so it cannot describe a
    /// different row than the account it qualifies. Readers use it to bind
    /// id-keyed lookups (the quarantine marker, the stored keys) to the row this
    /// snapshot is about rather than to the handle they were given.
    pub session_id: String,
}

/// The one canonical session id a caller's session handle resolves to.
///
/// A caller names a session the way a person does — by its stable id (`id`) or
/// by its human-readable name (`name`). The rest of this module stores facts
/// keyed by the canonical id (`sessions.id`), which for the default session is a
/// UUID, not the literal `"default"`. Resolving the handle to one canonical row
/// *before* any id-keyed lookup is what keeps a name from quietly missing the
/// row it names (see [`resolve_session_id`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHandle {
    /// `sessions.id` of the single matching row.
    canonical_id: String,
    /// The row's `name` column. Two handles that name different rows but carry
    /// the same name are not the same session, so the name is part of the
    /// resolved handle and not only of the id.
    name: String,
}

impl SessionHandle {
    /// The canonical `sessions.id`.
    pub fn canonical_id(&self) -> &str {
        &self.canonical_id
    }

    /// The `sessions.name` of the resolved row.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A collision-free key for process-local state (quarantine markers): the
    /// canonical id is unique across rows, so this is not merely the name.
    pub fn state_key(&self) -> String {
        Self::state_key_for(&self.canonical_id)
    }

    /// The same key, from a canonical id alone — for readers that hold the id
    /// read out of a snapshot rather than a resolved handle.
    pub fn state_key_for(canonical_id: &str) -> String {
        format!("session:{canonical_id}")
    }
}

/// Why a session handle did not resolve to exactly one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLookupError {
    /// No row matches the handle by id or by name.
    NotFound,
    /// The handle matches more than one row — e.g. one row's `id` equals another
    /// row's `name`, or a duplicate name slipped past the unique constraint. A
    /// read cannot pick one of two sessions, so it refuses rather than guess.
    Ambiguous,
}

/// Resolve a session handle to the one canonical session row it names.
///
/// A handle is matched by `id` **or** `name`, because the default session is
/// created with a UUID `id` and the literal name `"default"` — a caller that
/// asks for `"default"` is naming that row, and a strict `WHERE id = 'default'`
/// would find nothing. Exactly one row must match; a handle that matches two
/// (for example an id equal to another row's name) is refused, because choosing
/// one of two sessions is exactly the mix-up a read scope must not make.
pub fn resolve_session_id(conn: &Connection, handle: &str) -> Result<SessionHandle, SessionLookupError> {
    let mut stmt = conn
        .prepare("SELECT id, name FROM sessions WHERE id = ?1 OR name = ?1 ORDER BY id")
        .map_err(|_| SessionLookupError::NotFound)?;
    let rows = stmt
        .query_map(params![handle], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|_| SessionLookupError::NotFound)?;

    let matched: Vec<(String, String)> = rows.flatten().collect();
    match matched.as_slice() {
        [] => Err(SessionLookupError::NotFound),
        [(canonical_id, name)] => Ok(SessionHandle {
            canonical_id: canonical_id.clone(),
            name: name.clone(),
        }),
        _ => Err(SessionLookupError::Ambiguous),
    }
}

/// Read a session's login generation on its own.
///
/// Independent of `Session` on purpose: the counter adds no field to the
/// exported session type, so no generated TypeScript changes.
///
/// The handle is resolved to a canonical id first, so a caller that names the
/// session by its `name` reads the same row as a caller that names its `id`.
pub fn get_session_login_generation(conn: &Connection, session_id: &str) -> Option<u64> {
    let handle = resolve_session_id(conn, session_id).ok()?;
    conn.query_row(
        "SELECT login_generation FROM sessions WHERE id = ?1",
        params![handle.canonical_id],
        |row| row.get::<_, Option<i64>>(0),
    )
    .ok()
    .flatten()
    .and_then(|value| u64::try_from(value).ok())
}

/// Read account, login state and generation as one consistent snapshot.
///
/// The quarantine flag is read here, in the same statement, for the same reason:
/// a flag read separately could belong to a different commit than the account it
/// is supposed to qualify.
///
/// The canonical id is projected too. `sessions.id` is what the child tables and
/// the login writers key on, so a resolution that only knew the `name` could not
/// find the quarantine row or the stored keys of the very session it named.
pub fn get_session_login_snapshot(
    conn: &Connection,
    session_id: &str,
) -> Option<SessionLoginSnapshot> {
    let handle = resolve_session_id(conn, session_id).ok()?;
    conn.query_row(
        "SELECT s.logged_in_user,
                s.login_state,
                s.login_generation,
                EXISTS(SELECT 1 FROM session_login_write w WHERE w.session_id = s.id),
                s.id
           FROM sessions s
          WHERE s.id = ?1",
        params![handle.canonical_id],
        |row| {
            Ok(SessionLoginSnapshot {
                account: row.get::<_, Option<String>>(0)?,
                login_state: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                generation: row
                    .get::<_, Option<i64>>(2)?
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or_default(),
                login_write_unverified: row.get::<_, bool>(3)?,
                session_id: row.get::<_, String>(4)?,
            })
        },
    )
    .ok()
}

/// Remove every row keyed to a session handle's canonical id.
///
/// The handle is resolved first, so clearing the default session by its name
/// clears the child rows of the row that name actually points at.
pub fn clear_session_data(conn: &Connection, session_id: &str) {
    let Ok(handle) = resolve_session_id(conn, session_id) else {
        return;
    };
    conn.execute(
        "DELETE FROM wechat_keys WHERE session_id = ?1",
        params![handle.canonical_id],
    )
    .ok();
    conn.execute(
        "DELETE FROM sync_state WHERE session_id = ?1",
        params![handle.canonical_id],
    )
    .ok();
}

#[cfg(test)]
mod login_generation_tests {
    use super::{
        get_session_login_generation, get_session_login_snapshot, update_session_logged_in_user,
        SessionWriteError,
    };
    use rusqlite::Connection;

    /// The migration this test suite runs against, so the fixture cannot drift
    /// away from the statement the server actually applies.
    const MIGRATION_V4: &str = include_str!("../../migrations/V4__session_login_generation.sql");

    /// A session table with the columns these queries use, then the real
    /// migration — the order a fresh database sees.
    fn migrated_fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 status TEXT NOT NULL DEFAULT 'stopped',
                 login_state TEXT NOT NULL DEFAULT 'logged_out',
                 logged_in_user TEXT,
                 wechat_pid INTEGER,
                 created_at TEXT DEFAULT (datetime('now')),
                 updated_at TEXT DEFAULT (datetime('now'))
             );
              INSERT INTO sessions (id, name) VALUES
                ('default', 'default'),
                ('other', 'other');",
        )
        .unwrap();
        conn.execute_batch(MIGRATION_V4).unwrap();
        conn
    }

    fn generation(conn: &Connection, session_id: &str) -> u64 {
        get_session_login_generation(conn, session_id).expect("session exists")
    }

    fn account(conn: &Connection, session_id: &str) -> Option<String> {
        conn.query_row(
            "SELECT logged_in_user FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap()
    }

    fn login_state(conn: &Connection, session_id: &str) -> String {
        conn.query_row("SELECT login_state FROM sessions WHERE id = ?1", [session_id], |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
    }

    #[test]
    fn migration_preserves_an_existing_login_with_a_recorded_generation() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE sessions (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, logged_in_user TEXT, login_state TEXT); INSERT INTO sessions VALUES ('default', 'default', 'wxid_existing_suffix', 'logged_in');").unwrap();
        conn.execute_batch(MIGRATION_V4).unwrap();
        let snapshot = get_session_login_snapshot(&conn, "default").unwrap();
        assert_eq!(snapshot.account.as_deref(), Some("wxid_existing_suffix"));
        assert_eq!(snapshot.login_state, "logged_in");
        assert_eq!(snapshot.generation, 1);
    }

    #[test]
    fn migration_adds_a_zero_generation_to_existing_sessions() {
        let conn = migrated_fixture();
        assert_eq!(generation(&conn, "default"), 0);
        assert_eq!(generation(&conn, "other"), 0);
        assert_eq!(account(&conn, "default"), None);
        assert_eq!(login_state(&conn, "default"), "logged_out");
    }

    #[test]
    fn first_login_assigns_generation_one() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_first")).expect("the login write is recorded");
        assert_eq!(generation(&conn, "default"), 1);
        assert_eq!(account(&conn, "default").as_deref(), Some("wxid_first"));
        assert_eq!(login_state(&conn, "default"), "logged_in");
    }

    #[test]
    fn relogin_of_the_same_account_still_advances_the_generation() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_same")).expect("the login write is recorded");
        let first = generation(&conn, "default");
        update_session_logged_in_user(&conn, "default", Some("wxid_same")).expect("the login write is recorded");

        // The account name is unchanged, so only the counter can tell the two
        // logins apart.
        assert_eq!(account(&conn, "default").as_deref(), Some("wxid_same"));
        assert_eq!(generation(&conn, "default"), first + 1);
    }

    #[test]
    fn an_a_b_a_login_sequence_advances_the_generation_at_every_step() {
        let conn = migrated_fixture();
        let mut seen = Vec::new();
        for account in ["wxid_a", "wxid_b", "wxid_a"] {
            update_session_logged_in_user(&conn, "default", Some(account)).expect("recorded");
            seen.push((account.to_string(), generation(&conn, "default")));
        }

        assert_eq!(
            seen,
            vec![
                ("wxid_a".to_string(), 1),
                ("wxid_b".to_string(), 2),
                ("wxid_a".to_string(), 3),
            ],
            "the account returning to `wxid_a` must not repeat generation 1"
        );
    }

    #[test]
    fn logout_clears_the_account_and_advances_the_generation() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_gone")).expect("the login write is recorded");
        let logged_in = generation(&conn, "default");
        update_session_logged_in_user(&conn, "default", None).expect("the login write is recorded");

        assert_eq!(account(&conn, "default"), None);
        assert_eq!(login_state(&conn, "default"), "logged_out");
        assert_eq!(
            generation(&conn, "default"),
            logged_in + 1,
            "a logged out session must not keep the epoch of the login before it"
        );
    }

    #[test]
    fn the_generation_is_monotonic_under_repeated_updates() {
        let conn = migrated_fixture();
        let mut last = 0;
        for step in 0..25 {
            update_session_logged_in_user(&conn, "default", Some("wxid_monotonic")).expect("recorded");
            let current = generation(&conn, "default");
            assert_eq!(current, last + 1, "step {step} did not advance by one");
            last = current;
        }
        assert_eq!(last, 25);
    }

    #[test]
    fn the_generation_is_per_session() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_main")).expect("the login write is recorded");
        update_session_logged_in_user(&conn, "default", Some("wxid_main")).expect("the login write is recorded");
        update_session_logged_in_user(&conn, "other", Some("wxid_other")).expect("the login write is recorded");

        assert_eq!(generation(&conn, "default"), 2);
        assert_eq!(generation(&conn, "other"), 1);
    }

    #[test]
    fn unrelated_session_updates_do_not_move_the_generation() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_stable")).expect("the login write is recorded");
        let before = generation(&conn, "default");

        // `sessions/manager.rs` updates these columns without touching login.
        conn.execute(
            "UPDATE sessions SET status = 'running', wechat_pid = 42, updated_at = ?1
              WHERE id = 'default'",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();

        assert_eq!(generation(&conn, "default"), before);
    }

    #[test]
    fn an_unknown_session_has_neither_generation_nor_snapshot() {
        let conn = migrated_fixture();
        assert_eq!(get_session_login_generation(&conn, "absent"), None);
        assert_eq!(get_session_login_snapshot(&conn, "absent"), None);
    }

    #[test]
    fn the_snapshot_reports_account_state_and_generation_together() {
        let conn = migrated_fixture();
        update_session_logged_in_user(&conn, "default", Some("wxid_snapshot")).expect("the login write is recorded");
        update_session_logged_in_user(&conn, "default", Some("wxid_snapshot")).expect("the login write is recorded");

        let snapshot = get_session_login_snapshot(&conn, "default").expect("session exists");
        assert_eq!(snapshot.account.as_deref(), Some("wxid_snapshot"));
        assert_eq!(snapshot.login_state, "logged_in");
        assert_eq!(snapshot.generation, 2);
        assert_eq!(Some(snapshot.generation), get_session_login_generation(&conn, "default"));

        update_session_logged_in_user(&conn, "default", None).expect("the login write is recorded");
        let logged_out = get_session_login_snapshot(&conn, "default").expect("session exists");
        assert_eq!(logged_out.account, None);
        assert_eq!(logged_out.login_state, "logged_out");
        assert_eq!(logged_out.generation, 3);
    }

    #[test]
    fn updating_an_unknown_session_creates_nothing() {
        let conn = migrated_fixture();
        assert_eq!(
            update_session_logged_in_user(&conn, "absent", Some("wxid_nowhere")),
            Err(SessionWriteError::NoSession)
        );
        assert_eq!(get_session_login_generation(&conn, "absent"), None);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM sessions", [], |row| row.get::<_, i64>(0)),
            Ok(2)
        );
    }
}

/// Resolution of a caller's session handle to one canonical row.
///
/// The default session's `id` is a UUID and its `name` is `"default"`; these
/// tests pin that a handle resolves by either column, that the result names the
/// canonical id, and that an ambiguous handle is refused rather than guessed.
#[cfg(test)]
mod session_handle_tests {
    use super::{resolve_session_id, SessionLookupError};
    use rusqlite::Connection;

    const UUID: &str = "9d4c7a52-1111-4c2e-9f00-cafebabefeed";

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (id, name) VALUES (?1, 'default'), ('aux', 'aux')",
            [UUID],
        )
        .unwrap();
        conn
    }

    #[test]
    fn a_handle_resolves_by_name_and_reports_the_canonical_id() {
        let conn = fixture();
        let handle = resolve_session_id(&conn, "default").expect("resolves by name");
        assert_eq!(handle.canonical_id(), UUID);
        assert_eq!(handle.name(), "default");
        assert_eq!(
            resolve_session_id(&conn, UUID).expect("resolves by id"),
            handle,
            "the name and the id must resolve to the same row"
        );
    }

    #[test]
    fn an_id_shaped_handle_resolves_by_id() {
        let conn = fixture();
        let handle = resolve_session_id(&conn, "aux").expect("resolves");
        assert_eq!(handle.canonical_id(), "aux");
        assert_eq!(handle.name(), "aux");
    }

    #[test]
    fn an_absent_handle_is_not_found() {
        let conn = fixture();
        assert_eq!(resolve_session_id(&conn, "absent"), Err(SessionLookupError::NotFound));
    }

    #[test]
    fn a_handle_matching_two_rows_is_refused() {
        let conn = fixture();
        // One row's `id` is another row's `name`.
        conn.execute("INSERT INTO sessions (id, name) VALUES ('default', 'named_default')", [])
            .unwrap();
        assert_eq!(
            resolve_session_id(&conn, "default"),
            Err(SessionLookupError::Ambiguous),
            "a handle that names two sessions must not pick one"
        );
    }
}

/// A login write that could not be recorded.
///
/// The login itself happened in the UI; these are the ways the ledger can fail
/// to say so. Every one of them leaves the row naming the *previous* login, so a
/// reader that trusted the row would answer from an epoch that is no longer the
/// one on screen. The codes are fixed and carry no account, session or path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionWriteError {
    /// There is no row for that session, so there is no generation to advance.
    NoSession,
    /// The generation counter is at the ceiling of the column and cannot be
    /// advanced without wrapping, which would reuse an epoch.
    GenerationExhausted,
    /// The write itself was refused or lost.
    Failed,
}

impl SessionWriteError {
    pub fn code(self) -> &'static str {
        match self {
            Self::NoSession => "WECHAT_LOGIN_WRITE_NO_SESSION",
            Self::GenerationExhausted => "WECHAT_LOGIN_GENERATION_EXHAUSTED",
            Self::Failed => "WECHAT_LOGIN_WRITE_FAILED",
        }
    }
}

impl std::fmt::Display for SessionWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for SessionWriteError {}

/// Sessions whose last login write is not known to have advanced the
/// generation, for the life of this process.
///
/// The durable marker in `session_login_write` is the primary record. This set
/// is the backstop for the one case the marker cannot cover: a database that
/// refuses the marker write too. It is process-local on purpose — it is a
/// statement about this process' own failed writes, not a claim about the
/// ledger.
static UNVERIFIED_LOGIN_WRITES: std::sync::Mutex<Option<std::collections::HashSet<String>>> =
    std::sync::Mutex::new(None);

fn mark_login_write_unverified(session_id: &str) {
    if let Ok(mut guard) = UNVERIFIED_LOGIN_WRITES.lock() {
        guard
            .get_or_insert_with(std::collections::HashSet::new)
            .insert(session_id.to_string());
    }
}

fn clear_login_write_unverified(session_id: &str) {
    if let Ok(mut guard) = UNVERIFIED_LOGIN_WRITES.lock() {
        if let Some(set) = guard.as_mut() {
            set.remove(session_id);
        }
    }
}

/// Whether this process has a login write of its own it could not record, for a
/// **canonical** session id.
///
/// Deliberately about the process, not the database: the durable marker is read
/// as part of [`get_session_login_snapshot`], and a reader combines the two.
/// The key is the canonical `sessions.id` ([`SessionHandle::state_key`]); a
/// caller that resolved a handle must pass the resolved key, not the raw handle,
/// or a marker written under the id would be missed under the name.
pub fn session_login_write_unverified_in_process(session_id: &str) -> bool {
    UNVERIFIED_LOGIN_WRITES
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|set| set.contains(session_id)))
        .unwrap_or(false)
}

/// Record a login or logout, and report the generation it assigned.
///
/// The write is write-ahead quarantined: a marker row is inserted in the same
/// transaction as the update and deleted by it, so the row is either updated
/// together with the marker being gone, or the marker is there on its own. That
/// is what makes a failed write visible to a reader afterwards instead of
/// leaving the previous login looking like the current one.
///
/// The caller must not report a login to anyone when this returns an error: the
/// UI has logged in, the ledger has not recorded it, and only a later
/// successful write can clear the marker.
///
/// The handle is resolved to a canonical id first, so a login recorded against
/// the default session by name and one recorded by id hit the same row and the
/// same quarantine marker.
pub fn update_session_logged_in_user(
    conn: &Connection,
    session_id: &str,
    logged_in_user: Option<&str>,
) -> Result<u64, SessionWriteError> {
    let handle = resolve_session_id(conn, session_id).map_err(|error| match error {
        SessionLookupError::NotFound => SessionWriteError::NoSession,
        SessionLookupError::Ambiguous => SessionWriteError::Failed,
    })?;
    let canonical_id = handle.canonical_id().to_string();
    let state_key = handle.state_key();

    let now = chrono::Utc::now().to_rfc3339();
    let login_state = if logged_in_user.is_some() {
        "logged_in"
    } else {
        "logged_out"
    };

    // The in-process flag goes up first and only comes down after the commit, so
    // there is no window in which a reader could believe the row.
    mark_login_write_unverified(&state_key);

    let tx = conn
        .unchecked_transaction()
        .map_err(|_| SessionWriteError::Failed)?;
    tx.execute(
        "INSERT INTO session_login_write (session_id, unverified_since)
         VALUES (?1, ?2)
         ON CONFLICT(session_id) DO UPDATE SET unverified_since = excluded.unverified_since",
        params![canonical_id, now],
    )
    .map_err(|_| SessionWriteError::Failed)?;

    // Validated inside the transaction, so the row cannot disappear between the
    // check and the update and leave a marker for a session that no longer
    // exists.
    let generation: Option<i64> = tx
        .query_row(
            "SELECT login_generation FROM sessions WHERE id = ?1",
            params![canonical_id],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            _ => Err(SessionWriteError::Failed),
        })
        .map_err(|_| SessionWriteError::Failed)?;
    let Some(generation) = generation else {
        // Nothing to protect: a session that does not exist has no epoch for a
        // reader to mistake, so the marker is dropped again rather than left
        // behind a row that no login can ever clear.
        tx.execute(
            "DELETE FROM session_login_write WHERE session_id = ?1",
            params![canonical_id],
        )
        .map_err(|_| SessionWriteError::Failed)?;
        tx.commit().map_err(|_| SessionWriteError::Failed)?;
        clear_login_write_unverified(&state_key);
        return Err(SessionWriteError::NoSession);
    };

    let Some(next) = generation.checked_add(1).filter(|next| *next >= 0) else {
        // Refuse rather than wrap: a wrapped counter would hand an old epoch
        // back to a new login.
        tx.commit().map_err(|_| SessionWriteError::Failed)?;
        return Err(SessionWriteError::GenerationExhausted);
    };

    // A refused statement rolls back the statement, not the transaction, so the
    // marker can still be committed on its own: that is the state a reader has
    // to see. The row itself is left exactly as it was.
    let updated = match tx.execute(
        "UPDATE sessions
            SET logged_in_user = ?1,
                login_state = ?2,
                login_generation = ?3,
                updated_at = ?4
          WHERE id = ?5",
        params![logged_in_user, login_state, next, now, canonical_id],
    ) {
        Ok(updated) => updated,
        Err(_) => {
            let _ = tx.commit();
            return Err(SessionWriteError::Failed);
        }
    };
    if updated == 0 {
        tx.commit().map_err(|_| SessionWriteError::Failed)?;
        return Err(SessionWriteError::NoSession);
    }

    tx.execute(
        "DELETE FROM session_login_write WHERE session_id = ?1",
        params![canonical_id],
    )
    .map_err(|_| SessionWriteError::Failed)?;
    tx.commit().map_err(|_| SessionWriteError::Failed)?;

    clear_login_write_unverified(&state_key);
    Ok(u64::try_from(next).map_err(|_| SessionWriteError::GenerationExhausted)?)
}

#[cfg(test)]
mod login_write_failure_tests {
    use super::{
        get_session_login_snapshot, session_login_write_unverified_in_process,
        update_session_logged_in_user, SessionHandle, SessionWriteError,
    };
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicU64, Ordering};

    const MIGRATION_V4: &str = include_str!("../../migrations/V4__session_login_generation.sql");

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 status TEXT NOT NULL DEFAULT 'stopped',
                 login_state TEXT NOT NULL DEFAULT 'logged_out',
                 logged_in_user TEXT,
                 wechat_pid INTEGER,
                 created_at TEXT DEFAULT (datetime('now')),
                 updated_at TEXT DEFAULT (datetime('now'))
             );",
        )
        .unwrap();
        conn.execute_batch(MIGRATION_V4).unwrap();
        conn
    }

    /// The quarantine is a process-wide fact, so no two tests may share a
    /// session id.
    fn own_session(conn: &Connection) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = format!("login_write_{}", COUNTER.fetch_add(1, Ordering::Relaxed));
        conn.execute(
            "INSERT INTO sessions (id, name) VALUES (?1, ?1)",
            [&id],
        )
        .unwrap();
        id
    }

    fn generation(conn: &Connection, id: &str) -> i64 {
        conn.query_row(
            "SELECT login_generation FROM sessions WHERE id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn account(conn: &Connection, id: &str) -> Option<String> {
        conn.query_row(
            "SELECT logged_in_user FROM sessions WHERE id = ?1",
            [id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap()
    }

    fn quarantined(conn: &Connection, id: &str) -> bool {
        get_session_login_snapshot(conn, id)
            .expect("the session exists")
            .login_write_unverified
    }

    fn quarantined_rows(conn: &Connection, id: &str) -> i64 {
        conn.query_row(
            "SELECT count(*) FROM session_login_write WHERE session_id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// Refuses every update of one session, the way a constraint, a trigger or a
    /// full disk would.
    fn block_updates(conn: &Connection, id: &str) {
        conn.execute_batch(&format!(
            "CREATE TRIGGER block_update_{id} BEFORE UPDATE ON sessions
               WHEN OLD.id = '{id}'
             BEGIN SELECT RAISE(ABORT, 'refused'); END;"
        ))
        .unwrap();
    }

    #[test]
    fn a_successful_write_reports_the_generation_it_assigned() {
        let conn = fixture();
        let id = own_session(&conn);
        assert_eq!(update_session_logged_in_user(&conn, &id, Some("wxid_ok")), Ok(1));
        assert_eq!(update_session_logged_in_user(&conn, &id, Some("wxid_ok")), Ok(2));
        assert_eq!(generation(&conn, &id), 2);
        assert!(!quarantined(&conn, &id));
        assert!(!session_login_write_unverified_in_process(&id));
    }

    #[test]
    fn a_refused_login_write_is_reported_instead_of_being_swallowed() {
        let conn = fixture();
        let id = own_session(&conn);
        block_updates(&conn, &id);

        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_new")),
            Err(SessionWriteError::Failed),
            "a write that did not happen must not be reported as one that did"
        );
    }

    #[test]
    fn a_refused_write_leaves_the_row_naming_the_previous_login() {
        let conn = fixture();
        let id = own_session(&conn);
        update_session_logged_in_user(&conn, &id, Some("wxid_first")).expect("recorded");
        block_updates(&conn, &id);

        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_second")),
            Err(SessionWriteError::Failed)
        );
        assert_eq!(account(&conn, &id).as_deref(), Some("wxid_first"));
        assert_eq!(generation(&conn, &id), 1, "a refused update does not advance");
    }

    #[test]
    fn a_refused_relogin_of_the_same_account_is_marked_unverified() {
        // The dangerous case: same account, same process, same database root.
        // Only the generation separates the two logins, so a silently dropped
        // write would hand the new login the epoch of the old one.
        let conn = fixture();
        let id = own_session(&conn);
        update_session_logged_in_user(&conn, &id, Some("wxid_same")).expect("recorded");
        block_updates(&conn, &id);

        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_same")),
            Err(SessionWriteError::Failed)
        );
        assert_eq!(account(&conn, &id).as_deref(), Some("wxid_same"));
        assert_eq!(generation(&conn, &id), 1);
        assert!(
            quarantined(&conn, &id),
            "nothing else in the row distinguishes the two logins"
        );
        assert!(session_login_write_unverified_in_process(
            &SessionHandle::state_key_for(&id)
        ));
    }

    #[test]
    fn the_quarantine_survives_as_durable_state() {
        let conn = fixture();
        let id = own_session(&conn);
        block_updates(&conn, &id);
        assert!(update_session_logged_in_user(&conn, &id, Some("wxid_new")).is_err());

        // A different connection, so this is the stored state and not something
        // the failed call left in memory.
        // A different connection, so this is the stored state and not something
        // the failed call left in memory.
        let stored = conn
            .query_row(
                "SELECT unverified_since FROM session_login_write WHERE session_id = ?1",
                [&id],
                |row| row.get::<_, String>(0),
            )
            .expect("the marker is committed on its own");
        assert!(!stored.is_empty());
        assert_eq!(quarantined_rows(&conn, &id), 1);
    }

    #[test]
    fn a_later_successful_write_recovers_and_advances_the_generation() {
        let conn = fixture();
        let id = own_session(&conn);
        block_updates(&conn, &id);
        assert!(update_session_logged_in_user(&conn, &id, Some("wxid_lost")).is_err());

        conn.execute_batch(&format!("DROP TRIGGER block_update_{id};")).unwrap();
        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_lost")),
            Ok(1),
            "recovery assigns the generation the failed write never got to"
        );
        assert!(!quarantined(&conn, &id));
        assert!(!session_login_write_unverified_in_process(&id));
        assert_eq!(quarantined_rows(&conn, &id), 0);
        assert_eq!(account(&conn, &id).as_deref(), Some("wxid_lost"));
    }

    #[test]
    fn a_logout_write_is_quarantined_the_same_way() {
        let conn = fixture();
        let id = own_session(&conn);
        update_session_logged_in_user(&conn, &id, Some("wxid_gone")).expect("recorded");
        block_updates(&conn, &id);

        assert_eq!(
            update_session_logged_in_user(&conn, &id, None),
            Err(SessionWriteError::Failed)
        );
        assert!(
            quarantined(&conn, &id),
            "a session that still names an account it no longer has must not be read"
        );
        assert_eq!(generation(&conn, &id), 1);
    }

    #[test]
    fn an_exhausted_generation_is_refused_instead_of_wrapping() {
        let conn = fixture();
        let id = own_session(&conn);
        conn.execute(
            "UPDATE sessions SET login_generation = ?1 WHERE id = ?2",
            rusqlite::params![i64::MAX, id],
        )
        .unwrap();

        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_never")),
            Err(SessionWriteError::GenerationExhausted)
        );
        assert_eq!(generation(&conn, &id), i64::MAX, "the counter is not wrapped");
        assert_eq!(account(&conn, &id), None);
        assert!(quarantined(&conn, &id));
    }

    #[test]
    fn an_unknown_session_is_reported_and_left_alone() {
        let conn = fixture();
        assert_eq!(
            update_session_logged_in_user(&conn, "absent", Some("wxid_nowhere")),
            Err(SessionWriteError::NoSession)
        );
        assert_eq!(quarantined_rows(&conn, "absent"), 0);
        assert!(!session_login_write_unverified_in_process("absent"));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM sessions", [], |row| row.get::<_, i64>(0)),
            Ok(0)
        );
    }

    #[test]
    fn a_quarantine_that_cannot_be_still_refuses_for_the_rest_of_the_process() {
        let conn = fixture();
        let id = own_session(&conn);
        let other = own_session(&conn);
        // The one case the durable marker cannot cover: a database that refuses
        // even the quarantine write.
        conn.execute_batch("DROP TABLE session_login_write").unwrap();

        assert_eq!(
            update_session_logged_in_user(&conn, &id, Some("wxid_nowhere")),
            Err(SessionWriteError::Failed)
        );
        assert!(session_login_write_unverified_in_process(
            &SessionHandle::state_key_for(&id)
        ));
        assert!(
            !session_login_write_unverified_in_process(&SessionHandle::state_key_for(&other)),
            "one session's failed write must not quarantine another"
        );
    }

    #[test]
    fn the_migration_creates_the_quarantine_table() {
        let conn = fixture();
        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(session_login_write)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(columns, vec!["session_id", "unverified_since"]);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM session_login_write", [], |row| {
                row.get::<_, i64>(0)
            }),
            Ok(0)
        );
    }

    #[test]
    fn the_write_error_codes_are_fixed_and_carry_no_content() {
        assert_eq!(
            [
                SessionWriteError::NoSession.to_string(),
                SessionWriteError::GenerationExhausted.to_string(),
                SessionWriteError::Failed.to_string(),
            ],
            [
                "WECHAT_LOGIN_WRITE_NO_SESSION",
                "WECHAT_LOGIN_GENERATION_EXHAUSTED",
                "WECHAT_LOGIN_WRITE_FAILED",
            ]
        );
        let debug = format!(
            "{:?}",
            [
                SessionWriteError::NoSession,
                SessionWriteError::GenerationExhausted,
                SessionWriteError::Failed
            ]
        );
        for forbidden in ["wxid", "login_write_", "sessions", "token"] {
            assert!(!debug.contains(forbidden), "{forbidden} leaked in {debug}");
        }
    }
}
