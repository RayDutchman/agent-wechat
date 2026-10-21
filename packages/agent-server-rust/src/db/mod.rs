pub mod queries;

use refinery::embed_migrations;
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock, TryLockError};
use std::time::{Duration, Instant};

embed_migrations!("migrations");

static DB: OnceLock<Mutex<Connection>> = OnceLock::new();

/// Open a SQLite connection and set the SQLCipher encryption key.
/// PRAGMA key MUST be the first statement after opening.
fn open_encrypted(path: &str, key: &str) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(|e| format!("Failed to open db: {e}"))?;

    conn.execute_batch(&format!("PRAGMA key = '{key}';"))
        .map_err(|e| format!("Failed to set encryption key: {e}"))?;

    conn.execute_batch("PRAGMA journal_mode = WAL;")
        .map_err(|e| format!("Failed to set WAL: {e}"))?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|e| format!("Failed to enable FK: {e}"))?;

    // Verify the key works by reading from the db
    conn.execute_batch("SELECT count(*) FROM sqlite_master;")
        .map_err(|e| format!("Decryption check failed: {e}"))?;

    Ok(conn)
}

/// Migrate an unencrypted database to an encrypted one using sqlcipher_export.
fn migrate_to_encrypted(path: &str, key: &str) -> Result<Connection, String> {
    let tmp_path = format!("{path}.encrypted");

    // Open the old db without a key
    let old_conn = Connection::open(path).map_err(|e| format!("Failed to open old db: {e}"))?;

    // Verify it's actually readable without a key
    old_conn
        .execute_batch("SELECT count(*) FROM sqlite_master;")
        .map_err(|e| format!("Old db is not readable without key: {e}"))?;

    // Attach a new encrypted database and export
    old_conn
        .execute_batch(&format!(
            "ATTACH DATABASE '{tmp_path}' AS encrypted KEY '{key}';"
        ))
        .map_err(|e| format!("Failed to attach encrypted db: {e}"))?;

    old_conn
        .execute_batch("SELECT sqlcipher_export('encrypted');")
        .map_err(|e| format!("sqlcipher_export failed: {e}"))?;

    old_conn
        .execute_batch("DETACH DATABASE encrypted;")
        .map_err(|e| format!("Failed to detach: {e}"))?;

    drop(old_conn);

    // Swap files: remove old, rename new
    std::fs::remove_file(path).map_err(|e| format!("Failed to remove old db: {e}"))?;
    std::fs::remove_file(format!("{path}-wal")).ok();
    std::fs::remove_file(format!("{path}-shm")).ok();
    std::fs::rename(&tmp_path, path)
        .map_err(|e| format!("Failed to rename encrypted db: {e}"))?;

    tracing::info!("[DB] Migrated unencrypted database to encrypted");

    open_encrypted(path, key)
}

/// Initialize the database: set encryption key, run migrations, set pragmas.
pub fn init_db() -> Result<(), String> {
    let db_path =
        std::env::var("AGENT_DB_PATH").unwrap_or_else(|_| "/data/agent.db".to_string());

    // Ensure directory exists
    if let Some(parent) = Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create db dir: {e}"))?;
    }

    let key = crate::router::auth::get_token();
    let db_exists = Path::new(&db_path).exists();

    let conn = if db_exists {
        match open_encrypted(&db_path, key) {
            Ok(c) => c,
            Err(_) => {
                // Could be unencrypted legacy DB — try migration
                match migrate_to_encrypted(&db_path, key) {
                    Ok(c) => c,
                    Err(e) => {
                        // Neither encrypted nor migratable — discard and recreate
                        tracing::warn!(
                            "[DB] Cannot open or migrate existing db ({e}), removing and starting fresh"
                        );
                        std::fs::remove_file(&db_path).ok();
                        std::fs::remove_file(format!("{db_path}-wal")).ok();
                        std::fs::remove_file(format!("{db_path}-shm")).ok();
                        open_encrypted(&db_path, key)
                            .map_err(|e| format!("Failed to create fresh encrypted db: {e}"))?
                    }
                }
            }
        }
    } else {
        // New DB — just open encrypted
        open_encrypted(&db_path, key)
            .map_err(|e| format!("Failed to create encrypted db: {e}"))?
    };

    // Run migrations (refinery)
    let mut conn = conn;
    migrations::runner()
        .run(&mut conn)
        .map_err(|e| format!("Failed to run migrations: {e}"))?;

    DB.set(Mutex::new(conn))
        .map_err(|_| "Database already initialized".to_string())?;

    tracing::info!("[DB] Initialized (encrypted) at {db_path}");
    Ok(())
}

/// Get a reference to the database connection.
/// Panics if init_db() hasn't been called.
pub fn get_db() -> std::sync::MutexGuard<'static, Connection> {
    DB.get()
        .expect("Database not initialized. Call init_db() first.")
        .lock()
        .expect("Database mutex poisoned")
}

/// Why a lock was not handed out.
///
/// The two are kept apart because they are not the same failure: a wait that ran
/// out says this caller was too slow, while a poisoned lock says a previous
/// holder panicked part way through, so whatever it was doing is not known to
/// have finished. A read that is answering from that state would be answering
/// from an unverified one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockError {
    /// A holder panicked while holding this lock.
    Poisoned,
    /// Nobody was holding it within the budget.
    WaitExpired,
}

/// Take a mutex within `budget`, or say why it could not be taken.
///
/// [`get_db`] and `Mutex::lock` wait for as long as the holder keeps the lock,
/// and a caller that is itself under a deadline has no way to give the turn back
/// while it waits. This is the same lock, waited for at most `budget`: the
/// holder is a thread doing database work, not a resource this crate can cancel,
/// so the honest answer at the deadline is to report that the lock was not
/// available rather than to keep the caller waiting.
///
/// A poisoned lock is refused, not handed out. The data behind it may well be
/// fine, but this function cannot show that it is, and reusing it silently would
/// turn one panicking writer into every later reader's problem.
pub(crate) fn lock_within<T>(mutex: &Mutex<T>, budget: Duration) -> Result<MutexGuard<'_, T>, LockError> {
    let deadline = Instant::now() + budget;
    loop {
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err(LockError::Poisoned),
            Err(TryLockError::WouldBlock) => {}
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(LockError::WaitExpired);
        }
        // Short enough to notice the deadline within a millisecond or so of it,
        // long enough not to spin a core while somebody finishes a statement.
        std::thread::sleep(Duration::from_millis(2).min(deadline.saturating_duration_since(now)));
    }
}

/// The shared connection, taken within `budget`.
///
/// The same connection [`get_db`] hands out, with a deadline attached, for the
/// callers that are answering a request under one. Not initialised is still a
/// panic, exactly as in `get_db`: that is a start-up ordering bug, not a
/// condition a read can report.
pub(crate) fn try_get_db(budget: Duration) -> Result<MutexGuard<'static, Connection>, LockError> {
    lock_within(
        DB.get().expect("Database not initialized. Call init_db() first."),
        budget,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_fresh_encrypted_db() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let path_str = path.to_str().unwrap();
        let key = "test_secret_key_123";

        // Create encrypted db and write data
        let conn = open_encrypted(path_str, key).unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1, 'hello');")
            .unwrap();
        drop(conn);

        // Verify: opening without key should fail to read
        let plain = Connection::open(path_str).unwrap();
        let result = plain.execute_batch("SELECT count(*) FROM sqlite_master;");
        assert!(result.is_err(), "Plain open should fail on encrypted db");
    }

    #[test]
    fn test_migrate_unencrypted_to_encrypted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let path_str = path.to_str().unwrap();
        let key = "migration_key_456";

        // Create a plain unencrypted db with data
        let plain = Connection::open(path_str).unwrap();
        plain
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        plain
            .execute_batch("INSERT INTO t VALUES (1, 'migrated');")
            .unwrap();
        drop(plain);

        // Migrate
        let conn = migrate_to_encrypted(path_str, key).unwrap();

        // Verify data survived
        let val: String = conn
            .query_row("SELECT val FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "migrated");
        drop(conn);

        // Verify: plain open now fails
        let plain = Connection::open(path_str).unwrap();
        let result = plain.execute_batch("SELECT count(*) FROM sqlite_master;");
        assert!(result.is_err(), "Plain open should fail after migration");
    }

    #[test]
    fn a_lock_that_outlives_the_budget_is_reported_instead_of_waited_for() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let mutex = std::sync::Arc::new(Mutex::new(7usize));
        let held_by_other = mutex.clone();
        let (is_holding, may_look) = mpsc::channel::<()>();
        // Somebody else holds the connection for longer than any round lasts.
        let other = std::thread::spawn(move || {
            let _held = held_by_other.lock().unwrap();
            is_holding.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(400));
        });
        may_look.recv().unwrap();

        let started = Instant::now();
        let outcome = lock_within(&mutex, Duration::from_millis(60));
        let waited = started.elapsed();

        assert_eq!(
            outcome.unwrap_err(),
            LockError::WaitExpired,
            "a lock nobody can see must not be waited out past the round"
        );
        assert!(waited < Duration::from_millis(300), "waited {waited:?}");
        other.join().unwrap();
    }

    #[test]
    fn a_free_lock_is_taken_at_once() {
        use std::time::{Duration, Instant};

        let mutex = Mutex::new(7usize);
        let started = Instant::now();
        assert_eq!(*lock_within(&mutex, Duration::from_millis(60)).expect("free"), 7);
        assert!(
            started.elapsed() < Duration::from_millis(20),
            "an uncontended lock must not wait at all"
        );
    }

    #[test]
    fn a_lock_left_poisoned_by_a_panicking_holder_is_refused_rather_than_handed_out() {
        use std::time::Duration;

        let mutex = std::sync::Arc::new(Mutex::new(7usize));
        let panicking = mutex.clone();
        // The only guard is taken inside the panicking thread and dropped by the
        // unwinding, which is what poisons the lock.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _held = panicking.lock().unwrap();
            panic!("a writer panicked while holding the connection");
        }));
        assert!(outcome.is_err(), "the panic is what poisons the lock");

        // The guard is gone, so nothing is holding the lock any more: this
        // refusal is about unverified state, not about a lock nobody can take.
        assert_eq!(
            lock_within(&mutex, Duration::from_millis(60)).unwrap_err(),
            LockError::Poisoned
        );
        // Which is recoverable on purpose, by an operator rather than by a read:
        // the data behind the lock is still there to be inspected.
        assert_eq!(*mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner()), 7);
    }

    #[test]
    fn test_wrong_key_fails() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let path_str = path.to_str().unwrap();

        // Create encrypted db with key A
        let conn = open_encrypted(path_str, "key_a").unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER);").unwrap();
        drop(conn);

        // Opening with key B should fail
        let result = open_encrypted(path_str, "key_b");
        assert!(result.is_err(), "Wrong key should fail");
    }
}
