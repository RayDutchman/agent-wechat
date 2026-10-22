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

/// Why a lock could not be handed out: it was poisoned or the wait budget expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockError {
    /// A holder panicked while holding this lock.
    Poisoned,
    /// Nobody was holding it within the budget.
    WaitExpired,
}

/// Take a mutex within `budget`; return timeout or poison distinctly. Poisoned
/// state is refused because this function cannot verify that it is safe to reuse.
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

/// Take the shared connection within `budget`. Panics if the DB is not initialized,
/// as that indicates a startup-ordering bug (same contract as [`get_db`]).
pub(crate) fn try_get_db(budget: Duration) -> Result<MutexGuard<'static, Connection>, LockError> {
    lock_within(
        DB.get().expect("Database not initialized. Call init_db() first."),
        budget,
    )
}
