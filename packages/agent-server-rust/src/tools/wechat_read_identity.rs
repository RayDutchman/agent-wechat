//! Identity capture for reliable local WeChat reads.
//!
//! A local read is only answerable if the answer names the login it came from.
//! `capture_read_identity` samples the agent database and `/proc` read-only and
//! returns an identity only when every sample agrees:
//!
//! * the session is logged in and has a recorded login generation, and its last
//!   login write is known to have completed (see
//!   [`ReadIdentityError::LoginUnverified`]),
//! * exactly one WeChat process is attributable to the session account through
//!   that process' own `db_storage` descriptors — never by taking the first
//!   match or the process with the most descriptors,
//! * exactly one database root backs those descriptors, and
//! * the session facts and the process instance are unchanged when the sample is
//!   repeated.
//!
//! Anything ambiguous or in flux returns an error instead of an identity, so a
//! read that cannot name its login epoch has no way to start one.
//!
//! What this is not:
//!
//! * It is **not** an atomic compare-and-set on the account. The client can log
//!   out and back in between two samples without the agent database recording
//!   it — this module never looks at the UI, so such a change is invisible here.
//!   Only a login that goes through `db::queries::update_session_logged_in_user`
//!   is guaranteed to advance the generation.
//! * It is **not** a replacement for `acquire_gui_guard()`. A route that logs in
//!   through the UI must hold that guard while it captures, so that a UI login
//!   cannot interleave with a capture.
//!
//! `account_id` is derived from the storage layout with the legacy rule (see
//! [`derive_account_id`]); it is not a UI-proven WeChat identifier.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::db::queries::SessionLoginSnapshot;
use crate::tools::wechat_messages::{CollectionCancel, COLLECTION_BUDGET};

/// Session id of the existing single-account reads.
pub const DEFAULT_SESSION_ID: &str = "default";

/// Why no identity was produced. The codes are fixed and carry no paths,
/// accounts, pids or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIdentityError {
    /// No session row, or no database at all.
    SessionUnavailable,
    /// The session is not logged in.
    NotLoggedIn,
    /// The session's account or generation is inconsistent, or changed while
    /// the sample was taken.
    SessionUnstable,
    /// No process could be identified for this session.
    ProcessUnidentified,
    /// More than one process holds the session account.
    ProcessAmbiguous,
    /// A process exposes more than one database root, or one process does and
    /// another does not agree on which account it holds.
    AccountAmbiguous,
    /// The only processes found hold a different account.
    AccountMismatch,
    /// The account directory has no legacy identifier mapping; the mapping is
    /// not guessed.
    AccountIdUnmapped,
    /// The session's last recorded login write did not complete, so which
    /// account is logged in cannot be established from the database. Refused
    /// until a login write that is known to have advanced the generation.
    LoginUnverified,
    /// The process instance changed while the sample was taken.
    ProcessUnstable,
    /// The caller's round ran out while the identity was being sampled.
    ///
    /// Its own refusal, not an outage: nothing is known about the account, and
    /// the caller can ask again. Reported separately so a busy machine is never
    /// told the account is unidentifiable.
    DeadlineExceeded,
}

impl ReadIdentityError {
    pub fn code(self) -> &'static str {
        match self {
            Self::SessionUnavailable => "WECHAT_READ_IDENTITY_SESSION_UNAVAILABLE",
            Self::NotLoggedIn => "WECHAT_READ_IDENTITY_NOT_LOGGED_IN",
            Self::SessionUnstable => "WECHAT_READ_IDENTITY_SESSION_UNSTABLE",
            Self::ProcessUnidentified => "WECHAT_READ_IDENTITY_PROCESS_UNFOUND",
            Self::ProcessAmbiguous => "WECHAT_READ_IDENTITY_PROCESS_AMBIGUOUS",
            Self::AccountAmbiguous => "WECHAT_READ_IDENTITY_ACCOUNT_AMBIGUOUS",
            Self::AccountMismatch => "WECHAT_READ_IDENTITY_ACCOUNT_MISMATCH",
            Self::AccountIdUnmapped => "WECHAT_READ_IDENTITY_ACCOUNT_ID_UNMAPPED",
            Self::LoginUnverified => "WECHAT_READ_IDENTITY_LOGIN_UNVERIFIED",
            Self::ProcessUnstable => "WECHAT_READ_IDENTITY_PROCESS_UNSTABLE",
            Self::DeadlineExceeded => "WECHAT_READ_IDENTITY_DEADLINE_EXCEEDED",
        }
    }
}

impl std::fmt::Display for ReadIdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for ReadIdentityError {}

/// One `db_storage` root taken from a process descriptor, with the account
/// directory that root belongs to.
///
/// The root is kept, not only the directory name: two roots can carry the same
/// account directory name, and reading the wrong one is exactly the silent
/// mix-up this module exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatabaseRoot {
    path: PathBuf,
    account_dir: String,
}

impl DatabaseRoot {
    pub fn new(path: PathBuf, account_dir: String) -> Self {
        Self { path, account_dir }
    }

    /// The real `…/db_storage` directory, for readers that bind to this root
    /// instead of searching default locations again.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn account_dir(&self) -> &str {
        &self.account_dir
    }
}

/// Everything one candidate process contributes to the sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSample {
    pub pid: i64,
    /// `/proc/<pid>/stat` start time, in ticks since boot.
    pub start_tick: u64,
    /// Boot id, so a pid reused after a reboot cannot be mistaken for the same
    /// instance.
    pub boot_id: String,
    /// Every distinct database root behind this process' descriptors.
    pub roots: BTreeSet<DatabaseRoot>,
}

impl ProcessSample {
    pub fn new(
        pid: i64,
        start_tick: u64,
        boot_id: impl Into<String>,
        roots: impl IntoIterator<Item = DatabaseRoot>,
    ) -> Self {
        Self {
            pid,
            start_tick,
            boot_id: boot_id.into(),
            roots: roots.into_iter().collect(),
        }
    }
}

/// The read-only sources one capture samples.
///
/// Every source in this trait only reads. Tests supply process samples from a
/// temporary fixture instead of a live client.
///
/// Both methods take the caller's round. Every source here runs on a blocking
/// thread, so a source that is handed no deadline is a source that keeps walking
/// `/proc` or waiting for the shared connection after its caller has given up.
pub trait IdentitySource {
    /// One consistent read of the session's login facts.
    fn session(
        &self,
        cancel: &CollectionCancel,
    ) -> Result<Option<SessionLoginSnapshot>, ReadIdentityError>;
    /// Every candidate WeChat process found right now, with its own facts.
    fn processes(&self, cancel: &CollectionCancel) -> Result<Vec<ProcessSample>, ReadIdentityError>;
}

/// Stop at the caller's deadline.
///
/// A capture that runs out of time refuses as such: it reports no identity, and
/// it does not report a login problem it never looked for.
fn check(cancel: &CollectionCancel) -> Result<(), ReadIdentityError> {
    if cancel.expired() {
        return Err(ReadIdentityError::DeadlineExceeded);
    }
    Ok(())
}

/// The identity one read scope is bound to.
///
/// All fields are private: the epoch is opaque, and the process facts are
/// deliberately not reachable, so nothing here can be turned into an exported
/// payload.
pub struct ReadIdentity {
    database_root: PathBuf,
    account_dir: String,
    account_id: String,
    epoch: String,
    session_generation: u64,
    session_id: String,
}

impl ReadIdentity {
    /// The account directory as the login plan recorded it.
    pub fn account_dir(&self) -> &str {
        &self.account_dir
    }

    /// Legacy identifier derived from the account directory. See
    /// [`derive_account_id`] for what it is and is not.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// Opaque read scope: changes whenever the login, the process instance or
    /// the database root changes.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The session's login generation this identity was captured at.
    pub fn session_generation(&self) -> u64 {
        self.session_generation
    }

    /// The canonical id of the session row this identity was read from.
    ///
    /// The session row's own id — not the handle the capture was asked for — so
    /// keyed lookups (the stored keys, the quarantine marker) bind to the row the
    /// facts came from.
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The verified database root. Internal: it exists so a reader binds to the
    /// root that was actually verified instead of retrying default locations,
    /// and it is not part of the HTTP surface.
    pub(crate) fn database_root(&self) -> &Path {
        &self.database_root
    }
}

impl std::fmt::Debug for ReadIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadIdentity")
            .field("account_dir", &"[redacted]")
            .field("account_id", &"[redacted]")
            .field("database_root", &"[redacted]")
            .field("epoch", &self.epoch)
            .field("session_generation", &self.session_generation)
            .finish()
    }
}

/// Capture the identity of the default session, under the collection budget.
pub fn capture_read_identity() -> Result<ReadIdentity, ReadIdentityError> {
    capture_read_identity_for(DEFAULT_SESSION_ID, &CollectionCancel::within(COLLECTION_BUDGET))
}

/// Capture the identity of one session, under a deadline the caller owns.
///
/// Same identity, same refusals — with a wall clock over the whole capture. The
/// caller is a request that has already taken its GUI guard and is already
/// spending a round's budget, so a sampler that walks a busy `/proc` or waits for
/// the shared connection has to end at the same moment the round does.
pub fn capture_read_identity_for(
    session_id: &str,
    cancel: &CollectionCancel,
) -> Result<ReadIdentity, ReadIdentityError> {
    capture_from_within(
        &LiveIdentitySource {
            session_id: session_id.to_string(),
        },
        cancel,
    )
}

/// Sample, verify and return one identity, under the collection budget.
pub fn capture_from<S>(source: &S) -> Result<ReadIdentity, ReadIdentityError>
where
    S: IdentitySource + ?Sized,
{
    capture_from_within(source, &CollectionCancel::within(COLLECTION_BUDGET))
}

/// Sample, verify and return one identity, under a deadline the caller owns.
///
/// Pure over its sources: same samples in, same identity out. The round is
/// checked between the stages, because a stage that ignores it is a stage that
/// keeps sampling after nobody is waiting for the answer.
pub fn capture_from_within<S>(source: &S, cancel: &CollectionCancel) -> Result<ReadIdentity, ReadIdentityError>
where
    S: IdentitySource + ?Sized,
{
    check(cancel)?;
    let before = source
        .session(cancel)?
        .ok_or(ReadIdentityError::SessionUnavailable)?;
    let account = logged_in_account(&before)?;

    let first = attribute_process(&source.processes(cancel)?, &account)?;

    // Re-read the session after sampling /proc: a login or logout that ran
    // while the sample was taken invalidates everything it produced.
    check(cancel)?;
    let after = source
        .session(cancel)?
        .ok_or(ReadIdentityError::SessionUnavailable)?;
    // The canonical id is part of the stability check, not only of the epoch.
    // Two samples that resolved to different session rows — e.g. a row was
    // replaced by one carrying the same name — are not the same login, even when
    // the account and generation happen to line up.
    if logged_in_account(&after)? != account
        || after.generation != before.generation
        || after.session_id != before.session_id
    {
        return Err(ReadIdentityError::SessionUnstable);
    }
    // Re-read the processes: the instance must be the same one, and it must
    // still be the only process holding this account.
    check(cancel)?;
    let second = attribute_process(&source.processes(cancel)?, &account)?;
    if second != first {
        return Err(ReadIdentityError::ProcessUnstable);
    }
    check(cancel)?;

    let root = first
        .roots
        .iter()
        .next()
        .expect("an attributable process carries exactly one root")
        .clone();
    let account_id = derive_account_id(root.account_dir())?;

    Ok(ReadIdentity {
        epoch: epoch_of(before.generation, &before.session_id, &first, &root),
        database_root: root.path().to_path_buf(),
        account_dir: root.account_dir().to_string(),
        account_id,
        session_generation: before.generation,
        session_id: before.session_id,
    })
}

/// The account of a logged-in session, or why there is none.
///
/// A session that claims an account without a recorded generation is
/// inconsistent: nothing has ever written a login for it.
///
/// A login write that did not complete is checked before the row is believed at
/// all. The row then names the *previous* login, and for a re-login of the same
/// account the account and the generation are both unchanged — the two logins
/// are the same account in the same process against the same root, so nothing
/// else here can tell them apart. Answering anyway would label this login with
/// the epoch of the previous one, which is the one thing the epoch is for.
fn logged_in_account(session: &SessionLoginSnapshot) -> Result<String, ReadIdentityError> {
    if session.login_write_unverified {
        return Err(ReadIdentityError::LoginUnverified);
    }
    let account = session
        .account
        .as_deref()
        .filter(|account| !account.is_empty())
        .ok_or(ReadIdentityError::NotLoggedIn)?;
    if session.login_state != "logged_in" {
        return Err(ReadIdentityError::NotLoggedIn);
    }
    if session.generation == 0 {
        return Err(ReadIdentityError::SessionUnstable);
    }
    Ok(account.to_string())
}

/// Pick the single process that holds `account`, or refuse.
///
/// Every candidate must expose at most one database root: a process with two is
/// a contradiction, and a contradiction is refused even when another process
/// would have looked clean.
fn attribute_process(
    candidates: &[ProcessSample],
    account: &str,
) -> Result<ProcessSample, ReadIdentityError> {
    if candidates.iter().any(|sample| sample.roots.len() > 1) {
        return Err(ReadIdentityError::AccountAmbiguous);
    }

    let holding: Vec<&ProcessSample> = candidates
        .iter()
        .filter(|sample| {
            sample
                .roots
                .iter()
                .any(|root| root.account_dir() == account)
        })
        .collect();

    match holding.as_slice() {
        [only] => Ok((*only).clone()),
        [] if candidates.iter().any(|sample| !sample.roots.is_empty()) => {
            Err(ReadIdentityError::AccountMismatch)
        }
        [] => Err(ReadIdentityError::ProcessUnidentified),
        _ => Err(ReadIdentityError::ProcessAmbiguous),
    }
}

/// Legacy identifier of an account directory.
///
/// The old read sources derived the ledger account from the directory name by
/// dropping everything after its last underscore (`name.rsplit('_', 1)[0]`).
/// That rule is implemented here, once, instead of being copied into business
/// code, and its output is only claimed to be a storage-layout identifier — not
/// a UI-proven WeChat id. A directory name is a storage fact: it says which
/// account directory a process has open, never which account the UI proved a
/// login for.
///
/// A name without an underscore has nothing to drop, and the legacy rule leaves
/// it unchanged, so it is used as it is. What is refused is a mapping that is
/// empty or carries a path separator: neither is a directory name.
fn derive_account_id(account_dir: &str) -> Result<String, ReadIdentityError> {
    // `rsplit_once('_')` is the Rust spelling of `name.rsplit('_', 1)[0]`: the
    // part in front of the *last* underscore. With no underscore the legacy rule
    // yields the whole name.
    let account_id = match account_dir.rsplit_once('_') {
        Some((before, _)) => before,
        None => account_dir,
    };
    if account_id.is_empty() || account_id.contains('/') || account_id.contains('\\') {
        return Err(ReadIdentityError::AccountIdUnmapped);
    }
    Ok(account_id.to_string())
}

/// The epoch of one login: generation, the canonical session row, process
/// instance and database root, and nothing else, so it is stable while those
/// hold and changes when any of them does.
///
/// The session id is in the digest on purpose. `generation` is only unique
/// within one row; if a session row were replaced by another carrying the same
/// name, its generation could restart at the same value, and a scope that had
/// only generation + root would mistake the new our row for the old one. Folding
/// the row id in keeps a replaced session out of the previous scope's epoch.
fn epoch_of(
    generation: u64,
    session_id: &str,
    process: &ProcessSample,
    root: &DatabaseRoot,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"agent-wechat/read-identity/v1\0");
    for field in [
        generation.to_string(),
        session_id.to_string(),
        process.pid.to_string(),
        process.start_tick.to_string(),
        process.boot_id.clone(),
        root.account_dir().to_string(),
        root.path().to_string_lossy().into_owned(),
    ] {
        hasher.update(field.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

// ── Live, read-only sampling ───────────────────────────────────────────────

/// Samples the agent database and `/proc`. Each database read takes the
/// connection lock for that one statement and releases it again, so no lock is
/// held while `/proc` is walked.
struct LiveIdentitySource {
    session_id: String,
}

impl IdentitySource for LiveIdentitySource {
    fn session(
        &self,
        cancel: &CollectionCancel,
    ) -> Result<Option<SessionLoginSnapshot>, ReadIdentityError> {
        // The connection is shared with the login and logout writers, and this
        // read is part of a request that is already under a deadline. Waiting for
        // the lock is bounded by what is left of the round: a read that queued
        // behind a long writer reports the deadline rather than holding its
        // caller for as long as the writer took.
        let db = crate::db::try_get_db(cancel.remaining()).map_err(|error| match error {
            crate::db::LockError::WaitExpired => ReadIdentityError::DeadlineExceeded,
            // A holder panicked on this connection, so the facts it was in the
            // middle of writing are not known to be complete. Refusing is the
            // only answer that does not read them as if they were.
            crate::db::LockError::Poisoned => ReadIdentityError::SessionUnavailable,
        })?;
        // Read by the handle the caller was given — which may be the session's
        // `name`, e.g. `"default"` — but the snapshot carries the canonical
        // `sessions.id` it resolved to. A strict id lookup would miss the
        // default session entirely; resolving the handle first is what makes the
        // name and the id read the same row.
        let mut snapshot =
            crate::db::queries::get_session_login_snapshot(&db, &self.session_id)
                .ok_or(ReadIdentityError::SessionUnavailable)?;
        // A login write of this process that could not be recorded — including
        // one whose quarantine row the database also refused. The stored flag
        // covers the normal case; this covers the one where there is nothing
        // stored to read. Looked up by the canonical id, the same key the writer
        // used, and because the login plan records under the row's own id
        // (`Context.session_id = session.id`), a name lookup and an id lookup
        // cannot land on different rows here.
        let state_key = crate::db::queries::SessionHandle::state_key_for(&snapshot.session_id);
        if crate::db::queries::session_login_write_unverified_in_process(&state_key) {
            snapshot.login_write_unverified = true;
        }
        Ok(Some(snapshot))
    }

    fn processes(&self, cancel: &CollectionCancel) -> Result<Vec<ProcessSample>, ReadIdentityError> {
        sample_wechat_processes_with(PGREP, Path::new("/proc"), cancel)
    }
}

/// The enumeration program. A parameter of [`sample_wechat_processes_with`] so a
/// test can run a program that is really stopped at a deadline.
const PGREP: &str = "pgrep";

/// How long between two checks that a bounded child is still running.
const CHILD_POLL: Duration = Duration::from_millis(2);

/// Sample every candidate WeChat process, under the caller's round.
///
/// `pgrep` exits 1 when nothing matches, which is an empty list rather than a
/// failure. Any other outcome is a refusal: a broken enumeration cannot be told
/// apart from an empty one, and a silently shortened candidate set can turn two
/// processes holding the account into one clean match.
///
/// The program is bounded by the same deadline as the rest of the capture. It is
/// a child process on a machine that can be under load, and `Command::output`
/// waits for it without a limit: an enumeration that never returns would hold
/// the round open past the moment the caller stopped waiting, on a thread nothing
/// can interrupt.
fn sample_wechat_processes_with(
    program: &str,
    proc: &Path,
    cancel: &CollectionCancel,
) -> Result<Vec<ProcessSample>, ReadIdentityError> {
    let (code, stdout) = run_pgrep_bounded(program, cancel)?;

    let pids = pgrep_candidates(code, &stdout)?;

    let mut samples = Vec::with_capacity(pids.len());
    for pid in pids {
        check(cancel)?;
        // A process that exits between discovery and sampling is dropped; the
        // repeat sample then finds no attributable process and refuses. A
        // process that is there but unreadable is not dropped — see
        // [`is_gone`].
        if let Some(sample) = sample_process_at(proc, pid, cancel)? {
            samples.push(sample);
        }
    }
    Ok(samples)
}

/// One bounded run of the enumeration program: its exit code and its stdout.
///
/// At the deadline the child is signalled and then waited for, so it is reaped
/// rather than left as a zombie this process would keep reaping for the rest of
/// its life. The pipe is drained on its own thread: a child that fills the pipe
/// buffer blocks in `write` and would otherwise never reach its exit at all,
/// which would look exactly like a hung enumeration.
///
/// A process that could not be started is a broken enumeration, not an empty one.
fn run_pgrep_bounded(
    program: &str,
    cancel: &CollectionCancel,
) -> Result<(Option<i32>, String), ReadIdentityError> {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let mut child = Command::new(program)
        .args(["-f", "/usr/bin/wechat"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ReadIdentityError::ProcessUnidentified)?;

    // The child owns the writing end; this thread only reads what arrives.
    let mut pipe = child.stdout.take().ok_or(ReadIdentityError::ProcessUnidentified)?;
    let drained = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = pipe.read_to_string(&mut text);
        text
    });

    let code = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.code(),
            Ok(None) => {}
            Err(_) => return Err(ReadIdentityError::ProcessUnidentified),
        }
        if cancel.expired() {
            // Signalled first, then waited for: a killed child still has to be
            // reaped, and `wait` is what reaps it.
            let _ = child.kill();
            let _ = child.wait();
            return Err(ReadIdentityError::DeadlineExceeded);
        }
        std::thread::sleep(CHILD_POLL);
    };
    let stdout = drained.join().unwrap_or_default();
    Ok((code, stdout))
}

/// The pids of one `pgrep` run, or a refusal.
///
/// `code` is `None` when the process was killed by a signal. Every token must
/// be a positive pid: a token that is not a pid means the output is not the
/// enumeration this module asked for, and dropping it would sample fewer
/// processes than exist.
fn pgrep_candidates(code: Option<i32>, stdout: &str) -> Result<Vec<i64>, ReadIdentityError> {
    if code != Some(0) && code != Some(1) {
        return Err(ReadIdentityError::ProcessUnidentified);
    }
    let mut pids = Vec::new();
    for token in stdout.split_whitespace() {
        let pid: i64 = token
            .parse()
            .ok()
            .filter(|pid| *pid > 0)
            .ok_or(ReadIdentityError::ProcessUnidentified)?;
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }
    Ok(pids)
}

/// Whether an I/O error means "this is gone" or "this cannot be seen".
///
/// Only a missing entry is a race: a process that exited between discovery and
/// sampling, or a descriptor that was closed while its directory was being read.
/// Every other error — a permission error, an I/O error, a path that is not what
/// it was assumed to be — means the entry is there and its contents are hidden
/// from this process. A hidden process is one whose database roots were not
/// looked at, and attributing an account while pretending not to have seen it is
/// the mix-up this module exists to prevent.
fn is_gone(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}

/// Sample one process below `proc`, or `None` when it exited before it could be
/// read.
///
/// The base directory is a parameter so a test can run this against a real temp
/// tree instead of the running system. Each stage checks the round: a process
/// with thousands of descriptors is thousands of reads, and a capture that is
/// only noticed at the end of them has already spent the budget it was supposed
/// to report.
fn sample_process_at(
    proc: &Path,
    pid: i64,
    cancel: &CollectionCancel,
) -> Result<Option<ProcessSample>, ReadIdentityError> {
    check(cancel)?;
    let base = proc.join(pid.to_string());
    let stat = match std::fs::read_to_string(base.join("stat")) {
        Ok(stat) => stat,
        Err(error) if is_gone(&error) => return Ok(None),
        Err(_) => return Err(ReadIdentityError::ProcessUnidentified),
    };
    let Some((stat_pid, start_tick)) = parse_proc_stat(&stat) else {
        return Err(ReadIdentityError::ProcessUnidentified);
    };
    if stat_pid != pid {
        return Err(ReadIdentityError::ProcessUnidentified);
    }

    check(cancel)?;
    let entries = match std::fs::read_dir(base.join("fd")) {
        Ok(entries) => entries,
        // The process exited between its `stat` and its descriptors.
        Err(error) if is_gone(&error) => return Ok(None),
        Err(_) => return Err(ReadIdentityError::ProcessUnidentified),
    };
    let roots = collect_roots(
        entries.map(|entry| entry.map(|entry| entry.path())),
        |path| std::fs::read_link(path),
        cancel,
    )?;

    check(cancel)?;
    Ok(Some(ProcessSample::new(
        pid,
        start_tick,
        read_boot_id_at(proc)?,
        roots,
    )))
}

/// `/proc/<pid>/stat` fields after `comm`: field 3 is the first, so start time
/// (field 22) is the twentieth. `comm` may contain spaces and parentheses, so
/// the fields are taken after the *last* `)`.
fn parse_proc_stat(stat: &str) -> Option<(i64, u64)> {
    let (pid_field, rest) = stat.split_once(' ')?;
    let pid: i64 = pid_field.parse().ok()?;
    let (_, fields) = rest.rsplit_once(')')?;
    let start_tick: u64 = fields.split_whitespace().nth(19)?.parse().ok()?;
    Some((pid, start_tick))
}

/// Boot id of the running kernel. Without it a pid reused after a reboot could
/// pass as the same process instance.
fn read_boot_id_at(proc: &Path) -> Result<String, ReadIdentityError> {
    let boot_id = std::fs::read_to_string(proc.join("sys/kernel/random/boot_id"))
        .map_err(|_| ReadIdentityError::ProcessUnidentified)?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return Err(ReadIdentityError::ProcessUnidentified);
    }
    Ok(boot_id.to_string())
}

/// Every database root behind one process' descriptors.
///
/// `read_target` is a parameter so a test can hand in descriptors whose target
/// cannot be read, which a temp tree cannot produce for a live process. Both it
/// and the listing are held to the same rule: an entry that is gone is a race,
/// an entry that cannot be read refuses the whole sample. The round is checked
/// per entry, because a descriptor walk is the one part of a capture whose
/// length is set by the process being sampled rather than by this code.
fn collect_roots<I, F>(
    entries: I,
    read_target: F,
    cancel: &CollectionCancel,
) -> Result<BTreeSet<DatabaseRoot>, ReadIdentityError>
where
    I: IntoIterator<Item = std::io::Result<PathBuf>>,
    F: Fn(&Path) -> std::io::Result<PathBuf>,
{
    let mut roots = BTreeSet::new();
    for entry in entries {
        check(cancel)?;
        let path = match entry {
            Ok(path) => path,
            Err(error) if is_gone(&error) => continue,
            Err(_) => return Err(ReadIdentityError::ProcessUnidentified),
        };
        let target = match read_target(&path) {
            Ok(target) => target,
            Err(error) if is_gone(&error) => continue,
            Err(_) => return Err(ReadIdentityError::ProcessUnidentified),
        };
        if let Some(root) = db_storage_root(&target.to_string_lossy()) {
            roots.insert(root);
        }
    }
    Ok(roots)
}

/// The `…/<account>/db_storage` root a descriptor points into.
///
/// Database files only, matching the existing read sources: other descriptors
/// of the same directory carry no account evidence.
fn db_storage_root(target: &str) -> Option<DatabaseRoot> {
    if !target.starts_with('/') || !target.ends_with(".db") {
        return None;
    }
    let segments: Vec<&str> = target.split('/').collect();
    // `db_storage` must have an account directory in front of it and a file
    // behind it, otherwise the target is not a database file of an account.
    let index = segments.iter().rposition(|segment| *segment == "db_storage")?;
    if index < 2 || index + 1 >= segments.len() {
        return None;
    }
    let account_dir = segments[index - 1];
    if account_dir.is_empty() {
        return None;
    }
    Some(DatabaseRoot::new(
        segments[..=index].join("/").into(),
        account_dir.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        attribute_process, capture_from, capture_from_within, collect_roots, db_storage_root,
        derive_account_id, is_gone, logged_in_account, parse_proc_stat, pgrep_candidates,
        read_boot_id_at, sample_process_at, sample_wechat_processes_with, DatabaseRoot,
        IdentitySource, ProcessSample, ReadIdentity, ReadIdentityError, SessionLoginSnapshot,
    };
    use crate::tools::wechat_messages::{CollectionCancel, COLLECTION_BUDGET};
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    const ACCOUNT: &str = "wxid_read_identity";

    fn root_for(account_dir: &str) -> DatabaseRoot {
        DatabaseRoot::new(
            PathBuf::from(format!("/home/wechat/xwechat_files/{account_dir}/db_storage")),
            account_dir.to_string(),
        )
    }

    /// The canonical session id the fixture rows carry. The captures below are
    /// asked for `"default"` by handle, but the row they resolve to has this id —
    /// which is what the epoch and the keyed lookups bind to.
    const SESSION_ID: &str = "6f2c1e0a-0000-4000-8000-00000000abcd";

    fn session(account: Option<&str>, generation: u64) -> Option<SessionLoginSnapshot> {
        session_with_id(account, generation, SESSION_ID)
    }

    fn session_with_id(
        account: Option<&str>,
        generation: u64,
        session_id: &str,
    ) -> Option<SessionLoginSnapshot> {
        Some(SessionLoginSnapshot {
            account: account.map(str::to_string),
            login_state: if account.is_some() {
                "logged_in".to_string()
            } else {
                "logged_out".to_string()
            },
            generation,
            login_write_unverified: false,
            session_id: session_id.to_string(),
        })
    }

    /// A session whose last login write is not known to have advanced the
    /// generation: the row still names the previous login.
    fn unverified_session(
        account: Option<&str>,
        generation: u64,
    ) -> Option<SessionLoginSnapshot> {
        let mut snapshot = session(account, generation).expect("a snapshot");
        snapshot.login_write_unverified = true;
        Some(snapshot)
    }

    fn process(pid: i64, start_tick: u64, boot_id: &str, roots: Vec<DatabaseRoot>) -> ProcessSample {
        ProcessSample::new(pid, start_tick, boot_id, roots)
    }

    /// Serves one steady sample, so two captures see identical facts.
    fn steady(generation: u64) -> FakeSource {
        FakeSource {
            session_reads: vec![session(Some(ACCOUNT), generation)],
            process_reads: vec![vec![process(
                4242,
                987_654,
                "boot-1",
                vec![root_for(ACCOUNT)],
            )]],
            session_calls: Cell::new(0),
            process_calls: Cell::new(0),
        }
    }

    /// Returns the listed reads in order and repeats the last one, so a steady
    /// source needs one entry and a source that changes mid-sample lists both
    /// states.
    struct FakeSource {
        session_reads: Vec<Option<SessionLoginSnapshot>>,
        process_reads: Vec<Vec<ProcessSample>>,
        session_calls: Cell<usize>,
        process_calls: Cell<usize>,
    }

    impl FakeSource {
        fn new(
            session_reads: Vec<Option<SessionLoginSnapshot>>,
            process_reads: Vec<Vec<ProcessSample>>,
        ) -> Self {
            Self {
                session_reads,
                process_reads,
                session_calls: Cell::new(0),
                process_calls: Cell::new(0),
            }
        }

        fn sessions(mut self, reads: Vec<Option<SessionLoginSnapshot>>) -> Self {
            self.session_reads = reads;
            self
        }

        fn processes(mut self, reads: Vec<Vec<ProcessSample>>) -> Self {
            self.process_reads = reads;
            self
        }

        /// The reads themselves, with no deadline of any kind: a test can hand
        /// these to a source that does not look at the round, so the checks in
        /// `capture_from_within` are the only ones that can end a capture.
        fn session_unchecked(&self) -> Option<SessionLoginSnapshot> {
            let index = self
                .session_calls
                .get()
                .min(self.session_reads.len().saturating_sub(1));
            self.session_calls.set(self.session_calls.get() + 1);
            self.session_reads.get(index).cloned().flatten()
        }

        fn processes_unchecked(&self) -> Result<Vec<ProcessSample>, ReadIdentityError> {
            let Some(_) = self.process_reads.first() else {
                return Ok(Vec::new());
            };
            let last = self.process_reads.len() - 1;
            let index = self.process_calls.get().min(last);
            self.process_calls.set(self.process_calls.get() + 1);
            Ok(self.process_reads[index].clone())
        }
    }

    impl IdentitySource for FakeSource {
        fn session(
            &self,
            cancel: &CollectionCancel,
        ) -> Result<Option<SessionLoginSnapshot>, ReadIdentityError> {
            if cancel.expired() {
                return Err(ReadIdentityError::DeadlineExceeded);
            }
            Ok(self.session_unchecked())
        }

        fn processes(
            &self,
            cancel: &CollectionCancel,
        ) -> Result<Vec<ProcessSample>, ReadIdentityError> {
            if cancel.expired() {
                return Err(ReadIdentityError::DeadlineExceeded);
            }
            self.processes_unchecked()
        }
    }

    /// A source that ignores the round entirely, so a capture can only end where
    /// the capture itself checks.
    struct DeafSource(FakeSource);

    impl IdentitySource for DeafSource {
        fn session(
            &self,
            _cancel: &CollectionCancel,
        ) -> Result<Option<SessionLoginSnapshot>, ReadIdentityError> {
            Ok(self.0.session_unchecked())
        }

        fn processes(
            &self,
            _cancel: &CollectionCancel,
        ) -> Result<Vec<ProcessSample>, ReadIdentityError> {
            self.0.processes_unchecked()
        }
    }

    #[test]
    fn a_steady_identity_is_reproducible() {
        let first = capture_from(&steady(3)).expect("steady login is identifiable");
        let second = capture_from(&steady(3)).expect("steady login is identifiable");

        assert_eq!(first.account_dir(), ACCOUNT);
        assert_eq!(first.session_generation(), 3);
        assert_eq!(first.epoch(), second.epoch());
        assert_eq!(first.epoch().len(), 64, "epoch is a hex sha256 digest");
        assert!(
            first.epoch().chars().all(|c| c.is_ascii_hexdigit()),
            "epoch must be an opaque digest, not a readable identity"
        );
        assert_eq!(
            first.database_root(),
            PathBuf::from(format!("/home/wechat/xwechat_files/{ACCOUNT}/db_storage")).as_path()
        );
    }

    #[test]
    fn a_steady_identity_samples_the_session_and_the_processes_twice() {
        let source = steady(3);
        capture_from(&source).expect("steady login is identifiable");
        assert_eq!(source.session_calls.get(), 2);
        assert_eq!(source.process_calls.get(), 2);
    }

    #[test]
    fn relogin_of_the_same_account_changes_the_epoch() {
        let before = capture_from(&steady(3)).expect("steady login is identifiable");
        let after = capture_from(&steady(4)).expect("steady login is identifiable");

        assert_eq!(before.account_dir(), after.account_dir());
        assert_ne!(
            before.epoch(),
            after.epoch(),
            "the account name is unchanged, only the login generation can differ"
        );
    }

    #[test]
    fn an_a_b_a_login_sequence_changes_the_epoch_at_every_step() {
        let epochs: Vec<String> = ["wxid_a", "wxid_b", "wxid_a"]
            .iter()
            .enumerate()
            .map(|(step, account)| {
                let source = FakeSource::new(
                    vec![session(Some(account), step as u64 + 1)],
                    vec![vec![process(
                        4242,
                        987_654,
                        "boot-1",
                        vec![root_for(account)],
                    )]],
                );
                capture_from(&source).expect("identifiable").epoch().to_string()
            })
            .collect();

        assert_eq!(epochs.len(), 3);
        assert_ne!(epochs[0], epochs[1]);
        assert_ne!(epochs[1], epochs[2]);
        assert_ne!(
            epochs[0], epochs[2],
            "returning to the first account must not repeat the first epoch"
        );
    }

    #[test]
    fn a_process_restart_changes_the_epoch_without_a_new_login() {
        let restarted = FakeSource::new(
            vec![session(Some(ACCOUNT), 7)],
            vec![vec![process(4242, 2_000_000, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        let before = capture_from(&steady(7)).expect("steady login is identifiable");
        let after = capture_from(&restarted).expect("restarted login is identifiable");

        assert_eq!(before.account_dir(), after.account_dir());
        assert_ne!(before.epoch(), after.epoch());
    }

    #[test]
    fn a_new_boot_changes_the_epoch_even_with_the_same_pid_and_tick() {
        let rebooted = FakeSource::new(
            vec![session(Some(ACCOUNT), 7)],
            vec![vec![process(4242, 987_654, "boot-2", vec![root_for(ACCOUNT)])]],
        );
        let before = capture_from(&steady(7)).expect("steady login is identifiable");
        let after = capture_from(&rebooted).expect("identifiable");

        assert_ne!(before.epoch(), after.epoch());
    }

    #[test]
    fn a_different_database_root_changes_the_epoch() {
        let elsewhere = DatabaseRoot::new(
            PathBuf::from(format!("/home/wechat/Documents/xwechat_files/{ACCOUNT}/db_storage")),
            ACCOUNT.to_string(),
        );
        let moved = FakeSource::new(
            vec![session(Some(ACCOUNT), 7)],
            vec![vec![process(4242, 987_654, "boot-1", vec![elsewhere])]],
        );
        let before = capture_from(&steady(7)).expect("steady login is identifiable");
        let after = capture_from(&moved).expect("identifiable");

        assert_ne!(before.epoch(), after.epoch());
    }

    #[test]
    fn a_capture_reports_the_canonical_session_row_it_read() {
        // The capture is asked for a handle (`DEFAULT_SESSION_ID`, i.e. the
        // session's *name*), but the row it lands on has a UUID id. The identity
        // must carry that row id, because that is the key the stored keys and the
        // quarantine marker are written under.
        let source = steady(3);
        let identity = capture_from(&source).expect("steady login is identifiable");
        assert_eq!(
            identity.session_id(),
            SESSION_ID,
            "the identity must name the canonical row, not the handle it was asked for"
        );
    }

    #[test]
    fn a_replaced_session_row_does_not_reuse_the_previous_epoch() {
        // Same account, same generation, same process, same root — only the
        // session row is a different one. `generation` is only unique within a
        // row, so a scope keyed on generation + root alone would hand the new
        // session the old session's epoch and mistake the two logins for one.
        let old_row = steady(3);
        let new_row = FakeSource::new(
            vec![session_with_id(Some(ACCOUNT), 3, "11111111-2222-4333-8444-555555555555")],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );

        let before = capture_from(&old_row).expect("identifiable");
        let after = capture_from(&new_row).expect("identifiable");
        assert_eq!(
            before.session_generation(),
            after.session_generation(),
            "only the row changed; the generation deliberately did not"
        );
        assert_ne!(
            before.epoch(),
            after.epoch(),
            "a replaced session row must not inherit the previous row's epoch"
        );
    }

    #[test]
    fn a_session_row_replaced_mid_sample_is_refused() {
        // The row the sample started on is gone and a different one answers the
        // second read: the facts look identical, so only the canonical id can
        // tell that this is not the same session any more.
        let source = FakeSource::new(
            vec![
                session_with_id(Some(ACCOUNT), 3, SESSION_ID),
                session_with_id(Some(ACCOUNT), 3, "11111111-2222-4333-8444-555555555555"),
            ],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );

        assert_eq!(
            capture_from(&source).unwrap_err(),
            ReadIdentityError::SessionUnstable
        );
    }

    #[test]
    fn a_logged_out_session_has_no_identity() {
        let source = FakeSource::new(
            vec![session(None, 5)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::NotLoggedIn);
    }

    #[test]
    fn a_session_whose_state_disagrees_with_its_account_is_not_logged_in() {
        let mut snapshot = session(Some(ACCOUNT), 5).unwrap();
        snapshot.login_state = "logged_out".to_string();
        let source = FakeSource::new(
            vec![Some(snapshot)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::NotLoggedIn);
    }

    #[test]
    fn a_session_whose_last_login_write_failed_has_no_identity() {
        // The row still names the previous login, with a generation from before
        // the login that is on screen now. Sampling the process would only
        // confirm that *some* WeChat is running.
        let source = FakeSource::new(
            vec![unverified_session(Some(ACCOUNT), 5)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(
            capture_from(&source).unwrap_err(),
            ReadIdentityError::LoginUnverified
        );
        assert_eq!(
            source.process_calls.get(),
            0,
            "the refusal happens before /proc is walked"
        );
    }

    #[test]
    fn an_unverified_write_outranks_everything_the_row_still_says() {
        // Also for a row that claims to be logged out: a refused logout leaves
        // exactly that row behind, and the account it names is one the client no
        // longer has.
        for snapshot in [
            unverified_session(Some(ACCOUNT), 5),
            unverified_session(None, 0),
        ] {
            let source = FakeSource::new(
                vec![snapshot],
                vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
            );
            assert_eq!(
                capture_from(&source).unwrap_err(),
                ReadIdentityError::LoginUnverified
            );
        }
    }

    #[test]
    fn a_write_that_mid_sample_clears_its_quarantine_is_still_refused() {
        // The marker is read with the session facts, so a capture that started
        // before a login write committed must not finish on the account the
        // write installed.
        let mut after = session(Some(ACCOUNT), 6).unwrap();
        after.login_write_unverified = false;
        let source = FakeSource::new(
            vec![unverified_session(Some(ACCOUNT), 5), Some(after)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(
            capture_from(&source).unwrap_err(),
            ReadIdentityError::LoginUnverified
        );
    }

    #[test]
    fn a_recovers_to_a_new_epoch_rather_than_the_one_the_failed_write_left() {
        let first = capture_from(&steady(5)).expect("identifiable").epoch().to_string();
        let recovered = capture_from(&steady(6)).expect("identifiable");
        assert_ne!(
            recovered.epoch(),
            first,
            "the epoch must not survive a login the ledger only just recorded"
        );
        assert_eq!(recovered.session_generation(), 6);
    }

    #[test]
    fn an_account_without_a_generation_is_refused() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 0)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::SessionUnstable);
    }

    #[test]
    fn a_missing_session_has_no_identity() {
        let source = FakeSource::new(
            vec![None],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)])]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::SessionUnavailable);
    }

    #[test]
    fn a_generation_change_during_the_sample_is_refused() {
        let source = steady(3)
            .sessions(vec![session(Some(ACCOUNT), 3), session(Some(ACCOUNT), 4)]);
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::SessionUnstable);
    }

    #[test]
    fn an_account_change_during_the_sample_is_refused() {
        let source = steady(3).sessions(vec![
            session(Some(ACCOUNT), 3),
            session(Some("wxid_somebody_else"), 3),
        ]);
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::SessionUnstable);
    }

    #[test]
    fn a_logout_during_the_sample_is_refused() {
        let source = steady(3).sessions(vec![session(Some(ACCOUNT), 3), session(None, 4)]);
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::NotLoggedIn);
    }

    #[test]
    fn a_session_that_disappears_mid_sample_is_refused() {
        let source = steady(3).sessions(vec![session(Some(ACCOUNT), 3), None]);
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::SessionUnavailable);
    }

    #[test]
    fn a_process_replaced_during_the_sample_is_refused() {
        let held = process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)]);
        let replaced = process(4242, 2_000_000, "boot-1", vec![root_for(ACCOUNT)]);
        let source = steady(3).processes(vec![vec![held], vec![replaced]]);

        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessUnstable);
    }

    #[test]
    fn a_process_that_exits_during_the_sample_is_refused() {
        let held = process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)]);
        let source = steady(3).processes(vec![vec![held], Vec::new()]);

        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessUnidentified);
    }

    #[test]
    fn a_competing_process_that_appears_during_the_sample_is_refused() {
        let held = process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)]);
        let other = process(5151, 111_111, "boot-1", vec![root_for(ACCOUNT)]);
        let source = steady(3).processes(vec![vec![held.clone()], vec![held, other]]);

        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessAmbiguous);
    }

    #[test]
    fn two_accounts_behind_one_process_are_refused() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![process(
                4242,
                987_654,
                "boot-1",
                vec![root_for(ACCOUNT), root_for("wxid_other")],
            )]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::AccountAmbiguous);
    }

    #[test]
    fn two_roots_with_the_same_account_name_are_refused() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![process(
                4242,
                987_654,
                "boot-1",
                vec![
                    root_for(ACCOUNT),
                    DatabaseRoot::new(
                        PathBuf::from(format!(
                            "/home/wechat/Documents/xwechat_files/{ACCOUNT}/db_storage"
                        )),
                        ACCOUNT.to_string(),
                    ),
                ],
            )]],
        );
        assert_eq!(
            capture_from(&source).unwrap_err(),
            ReadIdentityError::AccountAmbiguous,
            "the same directory name under two roots is still ambiguous"
        );
    }

    #[test]
    fn an_ambiguous_process_is_refused_even_when_another_one_looks_clean() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![
                process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)]),
                process(
                    5151,
                    111_111,
                    "boot-1",
                    vec![root_for(ACCOUNT), root_for("wxid_other")],
                ),
            ]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::AccountAmbiguous);
    }

    #[test]
    fn a_process_holding_only_another_account_is_a_mismatch() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![process(
                4242,
                987_654,
                "boot-1",
                vec![root_for("wxid_other")],
            )]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::AccountMismatch);
    }

    #[test]
    fn two_processes_holding_the_session_account_are_ambiguous() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![
                process(4242, 987_654, "boot-1", vec![root_for(ACCOUNT)]),
                process(5151, 111_111, "boot-1", vec![root_for(ACCOUNT)]),
            ]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessAmbiguous);
    }

    #[test]
    fn a_process_without_any_database_descriptor_is_unidentified() {
        let source = FakeSource::new(
            vec![session(Some(ACCOUNT), 2)],
            vec![vec![process(4242, 987_654, "boot-1", Vec::new())]],
        );
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessUnidentified);
    }

    #[test]
    fn a_failed_process_read_yields_no_identity() {
        /// A source whose process sampling fails, the way an unreadable
        /// `/proc` entry does.
        struct UnreadableSource {
            session: Option<SessionLoginSnapshot>,
        }

        impl IdentitySource for UnreadableSource {
            fn session(
                &self,
                _cancel: &CollectionCancel,
            ) -> Result<Option<SessionLoginSnapshot>, ReadIdentityError> {
                Ok(self.session.clone())
            }

            fn processes(
                &self,
                _cancel: &CollectionCancel,
            ) -> Result<Vec<ProcessSample>, ReadIdentityError> {
                Err(ReadIdentityError::ProcessUnidentified)
            }
        }

        let source = UnreadableSource {
            session: session(Some(ACCOUNT), 2),
        };
        assert_eq!(capture_from(&source).unwrap_err(), ReadIdentityError::ProcessUnidentified);
    }

    #[test]
    fn an_account_directory_without_an_underscore_keeps_its_own_name() {
        // `name.rsplit('_', 1)[0]` on a name without an underscore is the name
        // itself, so the derived identifier is unchanged rather than refused.
        let source = FakeSource::new(
            vec![session(Some("plain"), 2)],
            vec![vec![process(4242, 987_654, "boot-1", vec![root_for("plain")])]],
        );
        let identity = capture_from(&source).expect("a directory name without an underscore is usable");
        assert_eq!(identity.account_dir(), "plain");
        assert_eq!(identity.account_id(), "plain");
        assert_eq!(derive_account_id("plain"), Ok("plain".to_string()));
    }

    #[test]
    fn the_legacy_identifier_is_derived_from_the_directory_name() {
        // `name.rsplit('_', 1)[0]`, the rule the old read sources applied.
        assert_eq!(derive_account_id("wxid_abc123"), Ok("wxid".to_string()));
        assert_eq!(
            derive_account_id("wxid_abc_123"),
            Ok("wxid_abc".to_string()),
            "only the last underscore is the cut"
        );
        assert_eq!(derive_account_id("_leading"), Err(ReadIdentityError::AccountIdUnmapped));
        assert_eq!(derive_account_id("trailing_"), Ok("trailing".to_string()));
        assert_eq!(derive_account_id(""), Err(ReadIdentityError::AccountIdUnmapped));
        assert_eq!(
            derive_account_id("../etc_passwd_1"),
            Err(ReadIdentityError::AccountIdUnmapped),
            "a derived identifier never carries a path separator"
        );
        assert_eq!(
            derive_account_id("wxid\\abc_1"),
            Err(ReadIdentityError::AccountIdUnmapped)
        );
    }

    #[test]
    fn the_identifier_is_exposed_next_to_the_directory_it_came_from() {
        let source = FakeSource::new(
            vec![session(Some("wxid_abc123"), 2)],
            vec![vec![process(
                4242,
                987_654,
                "boot-1",
                vec![root_for("wxid_abc123")],
            )]],
        );
        let identity: ReadIdentity = capture_from(&source).expect("identifiable");

        assert_eq!(identity.account_dir(), "wxid_abc123");
        assert_eq!(identity.account_id(), "wxid");
    }

    #[test]
    fn the_error_codes_are_fixed_and_carry_no_content() {
        assert_eq!(
            [
                ReadIdentityError::SessionUnavailable.to_string(),
                ReadIdentityError::NotLoggedIn.to_string(),
                ReadIdentityError::SessionUnstable.to_string(),
                ReadIdentityError::ProcessUnidentified.to_string(),
                ReadIdentityError::ProcessAmbiguous.to_string(),
                ReadIdentityError::AccountAmbiguous.to_string(),
                ReadIdentityError::AccountMismatch.to_string(),
                ReadIdentityError::AccountIdUnmapped.to_string(),
                ReadIdentityError::LoginUnverified.to_string(),
                ReadIdentityError::ProcessUnstable.to_string(),
            ],
            [
                "WECHAT_READ_IDENTITY_SESSION_UNAVAILABLE",
                "WECHAT_READ_IDENTITY_NOT_LOGGED_IN",
                "WECHAT_READ_IDENTITY_SESSION_UNSTABLE",
                "WECHAT_READ_IDENTITY_PROCESS_UNFOUND",
                "WECHAT_READ_IDENTITY_PROCESS_AMBIGUOUS",
                "WECHAT_READ_IDENTITY_ACCOUNT_AMBIGUOUS",
                "WECHAT_READ_IDENTITY_ACCOUNT_MISMATCH",
                "WECHAT_READ_IDENTITY_ACCOUNT_ID_UNMAPPED",
                "WECHAT_READ_IDENTITY_LOGIN_UNVERIFIED",
                "WECHAT_READ_IDENTITY_PROCESS_UNSTABLE",
            ]
        );

        let debug = format!(
            "{:?}",
            [
                ReadIdentityError::SessionUnavailable,
                ReadIdentityError::NotLoggedIn,
                ReadIdentityError::SessionUnstable,
                ReadIdentityError::ProcessUnidentified,
                ReadIdentityError::ProcessAmbiguous,
                ReadIdentityError::AccountAmbiguous,
                ReadIdentityError::AccountMismatch,
                ReadIdentityError::AccountIdUnmapped,
                ReadIdentityError::LoginUnverified,
                ReadIdentityError::ProcessUnstable,
            ]
        );
        for forbidden in [ACCOUNT, "/home/wechat", "4242", "token"] {
            assert!(!debug.contains(forbidden), "{forbidden} leaked in {debug}");
        }
    }

    #[test]
    fn an_identity_does_not_print_its_account_root_or_process() {
        let identity = capture_from(&steady(3)).expect("identifiable");
        let debug = format!("{identity:?}");

        for forbidden in [ACCOUNT, "/home/wechat", "4242"] {
            assert!(!debug.contains(forbidden), "{forbidden} leaked in {debug}");
        }
        assert!(debug.contains(&identity.epoch()));
    }

    #[test]
    fn the_epoch_does_not_expose_its_inputs() {
        let identity = capture_from(&steady(3)).expect("identifiable");
        assert!(
            !identity.epoch().contains(ACCOUNT),
            "the account directory must not survive into the epoch"
        );
        assert!(!identity.epoch().contains("wxid"), "derived ids stay out too");
    }

    // ── Process table parsing ───────────────────────────────────────────────

    /// `/proc/<pid>/stat` with 52 fields; the 20th field after `comm` is the
    /// start time (field 22 of the whole line).
    fn stat_line(pid: i64, comm: &str, start_tick: u64) -> String {
        let mut fields: Vec<String> = (3..=52).map(|field| field.to_string()).collect();
        fields[19] = start_tick.to_string();
        format!("{pid} ({comm}) {}", fields.join(" "))
    }

    #[test]
    fn the_start_time_is_read_past_the_command_name() {
        let line = stat_line(4242, "wechat", 987_654);
        assert_eq!(parse_proc_stat(&line), Some((4242, 987_654)));
    }

    #[test]
    fn a_command_name_with_spaces_and_parentheses_does_not_shift_the_fields() {
        let line = stat_line(4242, "we chat (x11)", 111_222);
        assert_eq!(parse_proc_stat(&line), Some((4242, 111_222)));
    }

    #[test]
    fn a_truncated_or_foreign_process_line_is_refused() {
        assert_eq!(parse_proc_stat(""), None);
        assert_eq!(parse_proc_stat("not a process line"), None);
        assert_eq!(parse_proc_stat("4242 (wechat) S 1"), None, "no start time field");
        assert_eq!(parse_proc_stat("4242 (wechat) S 1 x"), None, "start time is not a number");
    }

    #[test]
    fn only_database_files_name_an_account_root() {
        assert_eq!(
            db_storage_root("/home/wechat/xwechat_files/wxid_abc/db_storage/message/message_0.db"),
            Some(DatabaseRoot::new(
                PathBuf::from("/home/wechat/xwechat_files/wxid_abc/db_storage"),
                "wxid_abc".to_string(),
            ))
        );
        assert_eq!(
            db_storage_root("/data/Accounts/wxid_abc/db_storage/session/session.db"),
            Some(DatabaseRoot::new(
                PathBuf::from("/data/Accounts/wxid_abc/db_storage"),
                "wxid_abc".to_string(),
            ))
        );

        for other in [
            "/home/wechat/xwechat_files/wxid_abc/backup/backup.db",
            "/home/wechat/xwechat_files/wxid_abc/db_storage/message/message_0.db-wal",
            "/home/wechat/xwechat_files/wxid_abc/db_storage",
            "/home/wechat/xwechat_files/wxid_abc/db_storage/message",
            "/db_storage/message/message_0.db",
            "xwechat_files/wxid_abc/db_storage/message/message_0.db",
        ] {
            assert_eq!(db_storage_root(other), None, "{other} must not name a root");
        }
    }

    #[test]
    fn the_last_db_storage_component_is_the_root() {
        assert_eq!(
            db_storage_root("/mnt/backup/db_storage/wechat/db_storage/message/message_0.db"),
            Some(DatabaseRoot::new(
                PathBuf::from("/mnt/backup/db_storage/wechat/db_storage"),
                "wechat".to_string(),
            )),
            "an account directory may itself be named db_storage"
        );
    }

    // ── Attribution helpers ─────────────────────────────────────────────────

    #[test]
    fn attribution_takes_the_account_match_not_the_first_or_the_largest() {
        let candidates = vec![
            process(1111, 10, "boot-1", vec![root_for("wxid_other")]),
            process(2222, 20, "boot-1", Vec::new()),
            process(3333, 30, "boot-1", vec![root_for(ACCOUNT)]),
        ];
        let chosen = attribute_process(&candidates, ACCOUNT).expect("one match");
        assert_eq!(chosen.pid, 3333);
    }

    #[test]
    fn attribution_refuses_an_empty_candidate_list() {
        assert_eq!(
            attribute_process(&[], ACCOUNT),
            Err(ReadIdentityError::ProcessUnidentified)
        );
    }

    #[test]
    fn a_logged_in_account_requires_state_and_generation() {
        let mut snapshot = session(Some(ACCOUNT), 1).unwrap();
        assert_eq!(logged_in_account(&snapshot), Ok(ACCOUNT.to_string()));

        snapshot.generation = 0;
        assert_eq!(
            logged_in_account(&snapshot),
            Err(ReadIdentityError::SessionUnstable)
        );

        snapshot.generation = 1;
        snapshot.account = None;
        assert_eq!(
            logged_in_account(&snapshot),
            Err(ReadIdentityError::NotLoggedIn)
        );

        snapshot.account = Some(String::new());
        assert_eq!(
            logged_in_account(&snapshot),
            Err(ReadIdentityError::NotLoggedIn)
        );
    }

    // ── /proc sampling ──────────────────────────────────────────────────────

    /// A real `/proc`-shaped tree in a temp directory: real files, real
    /// symlinks, real read errors. Nothing here is a stub of the sampler; the
    /// sampler runs on it exactly as it runs on `/proc`.
    struct ProcFixture {
        _dir: tempfile::TempDir,
        proc: PathBuf,
    }

    impl ProcFixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let proc = dir.path().to_path_buf();
            std::fs::create_dir_all(proc.join("sys/kernel/random")).unwrap();
            std::fs::write(
                proc.join("sys/kernel/random/boot_id"),
                "fixture-boot-id\n",
            )
            .unwrap();
            Self { _dir: dir, proc }
        }

        fn proc(&self) -> &Path {
            &self.proc
        }

        /// A process whose `stat` and `fd` are the given entries. A descriptor
        /// is a path relative to `fd/`: a name for a symlink to a database file,
        /// or a name for a directory, which `readlink` cannot resolve.
        fn process(&self, pid: i64, start_tick: u64, descriptors: &[(&str, &str)]) {
            let base = self.proc.join(pid.to_string());
            std::fs::create_dir_all(base.join("fd")).unwrap();
            std::fs::write(base.join("stat"), stat_line(pid, "wechat", start_tick)).unwrap();
            for (name, target) in descriptors {
                if target.starts_with('/') {
                    let link = base.join("fd").join(name);
                    std::os::unix::fs::symlink(target, link).unwrap();
                } else {
                    std::fs::create_dir_all(base.join("fd").join(name)).unwrap();
                }
            }
        }

        /// A `stat` that cannot be read as a file: a directory, so the read
        /// fails with a stable, non-`NotFound` error.
        fn unreadable_stat(&self, pid: i64) {
            std::fs::create_dir_all(self.proc.join(pid.to_string()).join("stat")).unwrap();
        }
    }

    fn db_file(account_dir: &str) -> String {
        format!(
            "/home/wechat/xwechat_files/{account_dir}/db_storage/message/message_0.db"
        )
    }

    #[test]
    fn a_real_proc_fixture_samples_the_process_that_owns_its_root() {
        let fixture = ProcFixture::new();
        fixture.process(
            4242,
            987_654,
            &[
                ("3", &db_file(ACCOUNT)),
                ("4", "/home/wechat/xwechat_files/wxid_other/db_storage/message/message_0.db"),
                ("5", "/tmp/socket"),
            ],
        );

        let sample = sample_process_at(fixture.proc(), 4242, &generous())
            .expect("a readable process is sampled")
            .expect("the process is there");

        assert_eq!(sample.pid, 4242);
        assert_eq!(sample.start_tick, 987_654);
        assert_eq!(sample.boot_id, "fixture-boot-id");
        assert_eq!(
            sample.roots,
            BTreeSet::from([
                root_for(ACCOUNT),
                root_for("wxid_other"),
            ]),
            "every database root behind the descriptors, not the first one"
        );
    }

    #[test]
    fn a_process_that_exited_is_skipped_instead_of_failing_the_sample() {
        let fixture = ProcFixture::new();
        assert_eq!(
            sample_process_at(fixture.proc(), 5151, &generous()).expect("a race is not a failure"),
            None,
            "a pid that is gone is dropped; the repeat sample refuses instead"
        );
    }

    #[test]
    fn a_stat_that_cannot_be_read_refuses_rather_than_hiding_the_process() {
        let fixture = ProcFixture::new();
        // The pid is there — `/proc/<pid>` exists — but its `stat` cannot be
        // read as a file. Silently dropping it would let a second, unreadable
        // account root disappear from the candidate set.
        fixture.unreadable_stat(4242);
        assert_eq!(
            sample_process_at(fixture.proc(), 4242, &generous()).unwrap_err(),
            ReadIdentityError::ProcessUnidentified
        );
    }

    #[test]
    fn a_descriptor_that_cannot_be_read_refuses_instead_of_hiding_a_root() {
        let fixture = ProcFixture::new();
        fixture.process(
            4242,
            1,
            &[("3", &db_file(ACCOUNT)), ("4", "unreadable-entry")],
        );
        // The second descriptor is a directory: `readlink` fails on it with a
        // stable error, so its target is unknown — it may be another account's
        // database.
        assert_eq!(
            sample_process_at(fixture.proc(), 4242, &generous()).unwrap_err(),
            ReadIdentityError::ProcessUnidentified
        );
    }

    #[test]
    fn an_unreadable_descriptor_entry_is_never_ignored() {
        // The listing itself failed for one entry: it may be a second account's
        // database, so it is not skipped.
        assert_eq!(
            collect_roots(
                vec![Ok(PathBuf::from("/proc/4242/fd/3")), Err(io::Error::from(io::ErrorKind::PermissionDenied))],
                |_| Ok(db_file(ACCOUNT).into()),
                &generous(),
            )
            .unwrap_err(),
            ReadIdentityError::ProcessUnidentified,
            "a descriptor we could not list is a root we did not look at"
        );
        // A descriptor we could list but not read, likewise.
        assert_eq!(
            collect_roots(
                vec![Ok(PathBuf::from("/proc/4242/fd/3"))],
                |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                &generous(),
            )
            .unwrap_err(),
            ReadIdentityError::ProcessUnidentified
        );
        assert_eq!(
            collect_roots(
                vec![Err(io::Error::from(io::ErrorKind::PermissionDenied))],
                |_| Ok(db_file(ACCOUNT).into()),
                &generous(),
            )
            .unwrap_err(),
            ReadIdentityError::ProcessUnidentified,
            "one unreadable entry is enough to refuse"
        );
    }

    #[test]
    fn a_descriptor_closed_while_the_directory_was_read_is_a_race_not_a_failure() {
        let roots = collect_roots(
            vec![
                Ok(PathBuf::from("/proc/4242/fd/3")),
                // The listing itself hit a closed entry: a race, not a fact
                // about the account.
                Err(io::Error::from(io::ErrorKind::NotFound)),
            ],
            |_| Ok(db_file(ACCOUNT).into()),
            &generous(),
        )
        .expect("a closed entry is skipped");
        assert_eq!(roots, BTreeSet::from([root_for(ACCOUNT)]));

        let roots = collect_roots(
            vec![Ok(PathBuf::from("/proc/4242/fd/3")), Ok(PathBuf::from("/proc/4242/fd/4"))],
            |path| {
                if path.ends_with("4") {
                    // Closed between listing and reading.
                    Err(io::Error::from(io::ErrorKind::NotFound))
                } else {
                    Ok(db_file(ACCOUNT).into())
                }
            },
            &generous(),
        )
        .expect("a closed descriptor is skipped");
        assert_eq!(roots, BTreeSet::from([root_for(ACCOUNT)]));
    }

    #[test]
    fn only_a_missing_entry_counts_as_a_vanished_process() {
        assert!(is_gone(&io::Error::from(io::ErrorKind::NotFound)));
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
        ] {
            assert!(!is_gone(&io::Error::from(kind)), "{kind:?} is not a race");
        }
        // A real errno rather than a hand-built kind: EACCES on Linux.
        assert!(!is_gone(&io::Error::from_raw_os_error(13)));
        assert!(!is_gone(&io::Error::other("is a directory")));
    }

    #[test]
    fn a_broken_pgrep_enumeration_is_never_treated_as_an_empty_one() {
        assert_eq!(pgrep_candidates(Some(1), ""), Ok(Vec::new()), "no match");
        assert_eq!(pgrep_candidates(Some(0), "11\n22\n"), Ok(vec![11, 22]));
        for (code, stdout) in [
            (Some(2), ""),       // pgrep itself failed
            (Some(3), ""),       // a fatal pgrep error
            (None, ""),           // killed by a signal
            (Some(0), "11\nxx\n"),// a token that is not a pid
            (Some(0), "11 0\n"),  // pid 0 is not a process
            (Some(0), "-1\n"),    // neither is a negative pid
        ] {
            assert_eq!(
                pgrep_candidates(code, stdout),
                Err(ReadIdentityError::ProcessUnidentified),
                "code {code:?} stdout {stdout:?} must refuse"
            );
        }
    }

    #[test]
    fn a_missing_boot_id_refuses_the_sample() {
        let fixture = ProcFixture::new();
        assert_eq!(
            read_boot_id_at(&fixture.proc().join("absent")).unwrap_err(),
            ReadIdentityError::ProcessUnidentified
        );
        let empty = ProcFixture::new();
        std::fs::write(empty.proc().join("sys/kernel/random/boot_id"), "  \n").unwrap();
        assert_eq!(
            read_boot_id_at(empty.proc()).unwrap_err(),
            ReadIdentityError::ProcessUnidentified,
            "an empty boot id cannot fence a reused pid"
        );
    }

    // ── Stopping at the caller's deadline ───────────────────────────────────

    fn generous() -> CollectionCancel {
        CollectionCancel::within(COLLECTION_BUDGET)
    }

    /// A real program that stands in for `pgrep`, so "the enumeration is stopped
    /// at the deadline" can be observed rather than asserted: the runner takes
    /// the program as a parameter instead of a fixed name, and this is what it
    /// runs.
    fn pgrep_program(dir: &Path, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let program = dir.join("pgrep-shim");
        std::fs::write(&program, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        program
    }

    #[test]
    fn a_hung_enumeration_is_stopped_at_the_deadline_and_taken_back() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("pid");
        // `exec` keeps the announced pid the one that is signalled, so there is
        // no second process left behind by the shim itself.
        let program = pgrep_program(
            dir.path(),
            &format!("echo $$ > '{}'\nexec sleep 30", pid_file.display()),
        );
        let cancel = CollectionCancel::within(Duration::from_millis(80));

        let started = Instant::now();
        let outcome = sample_wechat_processes_with(
            &program.to_string_lossy(),
            &dir.path().join("proc"),
            &cancel,
        );
        let elapsed = started.elapsed();

        assert_eq!(outcome, Err(ReadIdentityError::DeadlineExceeded));
        assert!(elapsed < Duration::from_secs(5), "waited {elapsed:?}");

        let pid: i64 = std::fs::read_to_string(&pid_file)
            .expect("the shim announced its pid")
            .trim()
            .parse()
            .expect("a pid");
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "the child must be killed and reaped, not merely abandoned"
        );
    }

    #[test]
    fn an_enumeration_that_answers_is_sampled_and_a_broken_one_still_refuses() {
        let fixture = ProcFixture::new();
        fixture.process(4242, 987_654, &[("3", &db_file(ACCOUNT))]);
        let dir = tempfile::tempdir().unwrap();

        let answering = pgrep_program(dir.path(), "printf '4242\\n'");
        let samples = sample_wechat_processes_with(
            &answering.to_string_lossy(),
            fixture.proc(),
            &generous(),
        )
        .expect("a working enumeration is sampled");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].pid, 4242);
        assert_eq!(samples[0].boot_id, "fixture-boot-id");
        assert_eq!(samples[0].roots, BTreeSet::from([root_for(ACCOUNT)]));

        // A program that answers with something that is not an enumeration is
        // refused by the same path a real broken `pgrep` would take.
        let lying = pgrep_program(dir.path(), "printf 'not-a-pid\\n'");
        assert_eq!(
            sample_wechat_processes_with(&lying.to_string_lossy(), fixture.proc(), &generous()),
            Err(ReadIdentityError::ProcessUnidentified)
        );
        // So is one that cannot be started at all.
        assert_eq!(
            sample_wechat_processes_with(
                &dir.path().join("absent").to_string_lossy(),
                fixture.proc(),
                &generous()
            ),
            Err(ReadIdentityError::ProcessUnidentified)
        );
    }

    #[test]
    fn a_capture_that_ran_out_of_time_refuses_before_it_reads_anything() {
        let source = DeafSource(steady(3));
        let cancel = CollectionCancel::within(Duration::from_millis(0));

        assert_eq!(
            capture_from_within(&source, &cancel).unwrap_err(),
            ReadIdentityError::DeadlineExceeded
        );
        assert_eq!(source.0.process_calls.get(), 0, "no process sample was taken");
        assert_eq!(source.0.session_calls.get(), 0, "the session was not read either");
    }

    #[test]
    fn the_descriptor_walk_stops_at_the_deadline_and_reads_no_further_descriptor() {
        let cancel = CollectionCancel::within(Duration::from_millis(50));
        let reads = Cell::new(0usize);
        let entries = vec![
            Ok(PathBuf::from("/proc/4242/fd/3")),
            Ok(PathBuf::from("/proc/4242/fd/4")),
            Ok(PathBuf::from("/proc/4242/fd/5")),
        ];

        let outcome = collect_roots(
            entries,
            |path| {
                reads.set(reads.get() + 1);
                if path.ends_with("3") {
                    // The round ends while the first descriptor is being read.
                    cancel.cancel();
                }
                Ok(db_file(ACCOUNT).into())
            },
            &cancel,
        );

        assert_eq!(outcome.unwrap_err(), ReadIdentityError::DeadlineExceeded);
        assert_eq!(
            reads.get(),
            1,
            "a walk that ignores the round would read every descriptor of every process"
        );
    }
}

/// The alias bug these tests pin:
///
/// The default session is created with a UUID `id` and the literal name
/// `"default"`. The read path used to look the session up with `WHERE id = ?`,
/// so a reader asking for `"default"` missed the very row it named and
/// `/api/read/identity` answered `503 identity_unavailable`. These run the real
/// SQL against a temporary SQLite database — no WeChat client, no real account —
/// and drive the same `LiveIdentitySource` production uses, so the fix is proven
/// against the production identity reader rather than against a standalone
/// helper.
#[cfg(test)]
mod session_alias_tests {
    use super::{
        capture_from_within, ReadIdentityError, DEFAULT_SESSION_ID,
    };
    use crate::db::queries::{
        get_session_login_generation, get_session_login_snapshot, resolve_session_id,
        session_login_write_unverified_in_process, update_session_logged_in_user, SessionHandle,
        SessionLookupError, SessionWriteError,
    };
    use crate::tools::wechat_messages::CollectionCancel;
    use rusqlite::Connection;
    use std::time::Duration;

    /// The canonical id of the default row in the shared fixture.
    const DEFAULT_UUID: &str = "9d4c7a52-1111-4c2e-9f00-cafebabefeed";

    /// A fixture database with a `sessions` table and the real V4 migration. The
    /// default row is inserted the way `get_or_create_default_session` does: a
    /// UUID id under the name `"default"`.
    ///
    /// The canonical id is a parameter because the in-process quarantine marker
    /// is process-global state keyed by that id: tests that write to a session
    /// must not share an id with a test that asserts on its marker.
    fn fixture_with_id(canonical_id: &str) -> Connection {
        const MIGRATION_V4: &str =
            include_str!("../../migrations/V4__session_login_generation.sql");
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
        conn.execute(
            "INSERT INTO sessions (id, name) VALUES (?1, 'default')",
            [canonical_id],
        )
        .unwrap();
        conn
    }

    fn fixture() -> Connection {
        fixture_with_id(DEFAULT_UUID)
    }

    /// A `LiveIdentitySource` bound to an in-memory connection.
    ///
    /// The production source reads the process-wide connection through
    /// `db::try_get_db`; a test cannot install that one without a whole server,
    /// so this reproduces its session read (the same snapshot query, the same
    /// marker combination, the same canonical-id fallback) from the caller's
    /// handle. The process sampler is never reached, because these tests assert
    /// on what the session read returns before `/proc` is walked.
    struct FixtureIdentitySource {
        conn: Connection,
        handle: String,
    }

    impl super::IdentitySource for FixtureIdentitySource {
        fn session(
            &self,
            _cancel: &CollectionCancel,
        ) -> Result<Option<super::SessionLoginSnapshot>, ReadIdentityError> {
            let mut snapshot = get_session_login_snapshot(&self.conn, &self.handle)
                .ok_or(ReadIdentityError::SessionUnavailable)?;
            let state_key = SessionHandle::state_key_for(&snapshot.session_id);
            if session_login_write_unverified_in_process(&state_key) {
                snapshot.login_write_unverified = true;
            }
            Ok(Some(snapshot))
        }

        fn processes(
            &self,
            _cancel: &CollectionCancel,
        ) -> Result<Vec<super::ProcessSample>, ReadIdentityError> {
            // Reaching here would mean the session read produced an account. These
            // tests stop before that, so an empty process list is never attributed.
            Ok(Vec::new())
        }
    }

    fn cancel() -> CollectionCancel {
        CollectionCancel::within(Duration::from_secs(5))
    }

    #[test]
    fn the_default_handle_resolves_to_the_uuid_row() {
        let conn = fixture();
        let handle = resolve_session_id(&conn, DEFAULT_SESSION_ID).expect("the default row resolves");
        assert_eq!(handle.canonical_id(), DEFAULT_UUID);
        assert_eq!(handle.name(), "default");

        // The same row is reachable by its id, and both handles agree.
        let by_id = resolve_session_id(&conn, DEFAULT_UUID).expect("the row resolves by id");
        assert_eq!(by_id, handle);
    }

    #[test]
    fn the_login_generation_is_visible_through_the_default_handle() {
        // This is the read `/api/read/identity` starts with. Under the old
        // `WHERE id = ?` it was `None`, which surfaced as `503`.
        let conn = fixture();
        update_session_logged_in_user(&conn, DEFAULT_SESSION_ID, Some("wxid_default_login"))
            .expect("the login write is recorded");
        assert_eq!(get_session_login_generation(&conn, DEFAULT_SESSION_ID), Some(1));

        // Recording by canonical id and reading by name must be the same row.
        assert_eq!(get_session_login_generation(&conn, DEFAULT_UUID), Some(1));
    }

    #[test]
    fn a_default_handle_ledger_write_and_an_id_write_agree() {
        let conn = fixture();
        // A writer that happens to hold the canonical id, as the login plan does.
        assert_eq!(update_session_logged_in_user(&conn, DEFAULT_UUID, Some("wxid_main")), Ok(1));
        // A reader that names the session the way a person does.
        let snapshot = get_session_login_snapshot(&conn, DEFAULT_SESSION_ID).expect("the row");
        assert_eq!(snapshot.account.as_deref(), Some("wxid_main"));
        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.session_id, DEFAULT_UUID);
    }

    #[test]
    fn the_identity_capture_reads_the_session_through_the_default_handle() {
        // The production identity reader, asked for the `"default"` handle over a
        // database whose default row has a UUID id. It must see the login and get
        // as far as process attribution; before the alias fix it stopped at
        // `SessionUnavailable` because `WHERE id = 'default'` matched nothing.
        // `router::read::session_alias_router_tests::a_quarantined_default_session_refuses_before_any_page`
        // deliberately leaves the process-global quarantine set for its
        // canonical id. Keep this fixture's state key private to this test.
        let conn = fixture_with_id("9d4c7a52-4444-4c2e-9f00-cafebabefeed");
        update_session_logged_in_user(&conn, DEFAULT_SESSION_ID, Some("wxid_default_login"))
            .expect("the login write is recorded");
        let source = FixtureIdentitySource {
            conn,
            handle: DEFAULT_SESSION_ID.to_string(),
        };

        // No process is attributed in this fixture, which is exactly what proves
        // the session read succeeded: the failure is about `/proc`, not the
        // session row.
        assert_eq!(
            capture_from_within(&source, &cancel()).unwrap_err(),
            ReadIdentityError::ProcessUnidentified,
            "the failure must come after the session was found, not from missing it"
        );
    }

    #[test]
    fn a_name_keyed_marker_binds_to_the_canonical_id_not_the_handle() {
        // The quarantine marker is written under `session:<canonical id>`. A
        // reader that looked it up by the `"default"` handle would miss it and
        // read a login that is known not to be recorded.
        //
        // The marker is process-global state keyed by the canonical id, so this
        // test uses an id of its own: no other test's successful write of a
        // shared id may clear the marker this one is asserting on.
        let uuid = "9d4c7a52-2222-4c2e-9f00-cafebabebeef";
        let conn = fixture_with_id(uuid);
        let canonical_key = SessionHandle::state_key_for(uuid);
        assert_ne!(
            canonical_key,
            DEFAULT_SESSION_ID,
            "the state key must not be the name, or the marker would key on the alias"
        );
        assert!(!session_login_write_unverified_in_process(&canonical_key));

        // A refused write quarantines the canonical key, and the snapshot read by
        // name reports it.
        let conn = block_updates(conn, uuid);
        assert_eq!(
            update_session_logged_in_user(&conn, DEFAULT_SESSION_ID, Some("wxid_new")),
            Err(SessionWriteError::Failed)
        );
        assert!(session_login_write_unverified_in_process(&canonical_key));
        assert!(
            get_session_login_snapshot(&conn, DEFAULT_SESSION_ID)
                .expect("the row")
                .login_write_unverified
        );
    }

    #[test]
    fn a_successful_default_name_write_clears_the_canonical_marker() {
        // The other half: a recovered write recorded through the default name
        // must clear the marker written under the canonical id, or reads would
        // stay refused for a login that is now recorded.
        let uuid = "9d4c7a52-3333-4c2e-9f00-cafebabebeef";
        let conn = fixture_with_id(uuid);
        let canonical_key = SessionHandle::state_key_for(uuid);

        assert_eq!(
            update_session_logged_in_user(&conn, DEFAULT_SESSION_ID, Some("wxid_recovered")),
            Ok(1)
        );
        assert!(!session_login_write_unverified_in_process(&canonical_key));
        assert!(
            !get_session_login_snapshot(&conn, DEFAULT_SESSION_ID)
                .expect("the row")
                .login_write_unverified
        );
    }

    #[test]
    fn an_ambiguous_handle_is_refused_rather_than_guessed() {
        // One row's `id` equals another row's `name`: the handle names two
        // sessions, so nothing may pick one of them.
        let conn = fixture();
        conn.execute(
            "INSERT INTO sessions (id, name) VALUES ('default', 'auxiliary')",
            [],
        )
        .unwrap();
        assert_eq!(
            resolve_session_id(&conn, DEFAULT_SESSION_ID),
            Err(SessionLookupError::Ambiguous)
        );
        assert_eq!(get_session_login_snapshot(&conn, DEFAULT_SESSION_ID), None);
        assert_eq!(
            update_session_logged_in_user(&conn, DEFAULT_SESSION_ID, Some("wxid_x")),
            Err(SessionWriteError::Failed)
        );
    }

    #[test]
    fn an_unknown_handle_is_reported_as_absent() {
        let conn = fixture();
        assert_eq!(
            resolve_session_id(&conn, "absent"),
            Err(SessionLookupError::NotFound)
        );
        assert_eq!(get_session_login_generation(&conn, "absent"), None);
        assert_eq!(get_session_login_snapshot(&conn, "absent"), None);
    }

    /// Refuses every update of one session, the way a constraint or a full disk
    /// would, so the quarantine marker is the only state left behind.
    fn block_updates(conn: Connection, id: &str) -> Connection {
        // A UUID id is not a legal trigger name; replace anything that is not a
        // word character.
        let trigger: String = id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        conn.execute_batch(&format!(
            "CREATE TRIGGER block_update_{trigger} BEFORE UPDATE ON sessions
               WHEN OLD.id = '{id}'
             BEGIN SELECT RAISE(ABORT, 'refused'); END;"
        ))
        .unwrap();
        conn
    }
}
