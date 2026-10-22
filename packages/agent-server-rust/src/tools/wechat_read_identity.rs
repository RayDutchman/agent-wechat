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
