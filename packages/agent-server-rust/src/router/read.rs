//! Frozen-read backend, identity fencing, and shared round helpers used by V2.
//! The retired V1 HTTP adapters and their expected-epoch request contract are
//! intentionally not kept in this module.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::execution::acquire_gui_guard;
use crate::tools::wechat_contacts::collect_contact_snapshot_until;
use crate::tools::wechat_messages::{
    collect_message_snapshot_until, is_message_db, CollectionCancel, MessageSnapshotError,
    COLLECTION_BUDGET,
};
use crate::tools::wechat_read_identity::{
    capture_read_identity_for, ReadIdentityError, DEFAULT_SESSION_ID,
};
use crate::tools::wechat_read_pages::{self, FrozenPage, FrozenPageStore, PageError, SnapshotScope};
use crate::tools::wechat_read_pages::PageStoreConfig;
use crate::tools::wechat_read_voice::{collect_voice as read_voice_sample, VoiceReadError, VoiceRequest};

// ============================================
// Errors
// ============================================

/// Stable, content-free failure reasons for the read endpoints.
///
/// [`ReadError::code`] is the whole public error contract: no message body,
/// key, account directory, cursor or chat id is ever echoed into a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadError {
    /// Missing, duplicated or malformed query parameter, chat id, or cursor.
    /// An out-of-range page size is refused rather than clamped.
    InvalidParameter,
    /// The scope the caller asked for is not the scope being read: a different
    /// account, chat or epoch, or a source set that changed while the sample
    /// was collected.
    ScopeChanged,
    /// The frozen sample or the cursor is gone: the TTL elapsed, the sample was
    /// purged, or the server restarted and lost the in-memory registry.
    FrozenExpired,
    /// A documented page-store budget would be exceeded. Nothing is stored and
    /// no live sample is evicted.
    BudgetExceeded,
    /// The source could not be read. Reported without the underlying message.
    SourceReadFailed,
    /// The read identity could not be captured: no session row, not logged in,
    /// no process, or no verified database root.
    IdentityUnavailable,
    /// No stored decryption keys for this account. These routes never start key
    /// extraction by themselves.
    KeysUnavailable,
    /// No media carries audio for this message: it is not here (yet). Its own
    /// code, because "not here yet" calls for a bounded retry rather than being
    /// treated as permanently absent or as a broken source.
    VoiceSourceMissing,
    /// More than one message row or media row matches. Its own code, because the
    /// association key is ambiguous rather than the scope having moved.
    VoiceSourceAmbiguous,
    /// No usable message source for this account: no readable message directory
    /// or no numbered shard in it. An account with zero message shards is this
    /// case, not an empty chat, so it is not reported as `200` with an
    /// empty `items` list.
    SourceUnavailable,
    /// The session's last login write did not complete, so the ledger still
    /// names the previous login and no read can be attributed to the login that
    /// is on screen. Refused until a login write that is known to have advanced
    /// the generation.
    LoginUnverified,
    /// The round ran out of its wall clock. Reported separately from a read
    /// failure because the correct response is to ask again, not to stop asking:
    /// nothing was frozen and nothing was half-registered.
    DeadlineExceeded,
}

impl ReadError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            ReadError::InvalidParameter => "invalid_parameter",
            ReadError::ScopeChanged => "scope_changed",
            ReadError::FrozenExpired => "frozen_expired",
            ReadError::BudgetExceeded => "budget_exceeded",
            ReadError::SourceReadFailed => "source_read_failed",
            ReadError::IdentityUnavailable => "identity_unavailable",
            ReadError::KeysUnavailable => "keys_unavailable",
            ReadError::SourceUnavailable => "source_unavailable",
            ReadError::LoginUnverified => "login_unverified",
            ReadError::VoiceSourceMissing => "voice_source_missing",
            ReadError::VoiceSourceAmbiguous => "voice_source_ambiguous",
            ReadError::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for ReadError {}

/// The HTTP status a read failure maps to. Kept separate from the code so the
/// mapping is asserted on its own.
pub(crate) fn status_for(error: ReadError) -> StatusCode {
    match error {
        ReadError::InvalidParameter => StatusCode::BAD_REQUEST,
        ReadError::ScopeChanged => StatusCode::CONFLICT,
        ReadError::VoiceSourceAmbiguous => StatusCode::CONFLICT,
        ReadError::FrozenExpired => StatusCode::GONE,
        ReadError::BudgetExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ReadError::SourceReadFailed => StatusCode::INTERNAL_SERVER_ERROR,
        ReadError::IdentityUnavailable
        | ReadError::KeysUnavailable
        | ReadError::SourceUnavailable
        | ReadError::VoiceSourceMissing
        | ReadError::LoginUnverified => StatusCode::SERVICE_UNAVAILABLE,
        // A round that ran out of time is not the gateway's problem to retry
        // forever: the client is told to come back, and nothing was frozen.
        ReadError::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
    }
}

impl From<PageError> for ReadError {
    fn from(error: PageError) -> Self {
        match error {
            PageError::InvalidRequest => ReadError::InvalidParameter,
            PageError::Expired => ReadError::FrozenExpired,
            PageError::ScopeChanged => ReadError::ScopeChanged,
            PageError::BudgetExceeded => ReadError::BudgetExceeded,
            // A sample that ran out of time while being measured is the round's
            // deadline, not a payload that was too large: nothing was stored, and
            // the caller may well have been asking for a smaller one.
            PageError::DeadlineExceeded => ReadError::DeadlineExceeded,
        }
    }
}

impl IntoResponse for ReadError {
    fn into_response(self) -> Response {
        let code = self.code();
        tracing::warn!(code, status = status_for(self).as_u16(), "reliable read request failed");
        (status_for(self), Json(json!({"error": code, "code": code}))).into_response()
    }
}

/// Map a sample failure onto the HTTP contract.
///
/// A shard set that moved under the collection is a scope change, an oversized
/// row is a budget failure, a missing key or a missing message directory is a
/// missing dependency, and everything else is a read failure. The mapping keeps
/// the sample's own distinctions instead of collapsing them, because "no
/// readable message source" and "the read failed" call for different retries.
fn from_snapshot_error(error: MessageSnapshotError) -> ReadError {
    match error {
        MessageSnapshotError::KeysUnavailable => ReadError::KeysUnavailable,
        MessageSnapshotError::SourceUnavailable => ReadError::SourceUnavailable,
        MessageSnapshotError::SourceChanged => ReadError::ScopeChanged,
        MessageSnapshotError::DatabaseRead => ReadError::SourceReadFailed,
        MessageSnapshotError::BudgetExceeded => ReadError::BudgetExceeded,
        MessageSnapshotError::RowTooLarge => ReadError::BudgetExceeded,
        MessageSnapshotError::RowInvalid | MessageSnapshotError::RowDuplicate => {
            ReadError::SourceReadFailed
        }
        // A sample that ran out of its wall clock is not a broken source, and the
        // caller can only tell those apart if this stays its own code.
        MessageSnapshotError::DeadlineExceeded => ReadError::DeadlineExceeded,
    }
}

/// Map one voice-collector refusal onto the HTTP contract.
///
/// The mapping keeps the collector's own distinctions where the public contract
/// has room for them. A missing message or a source that moved is the caller's
/// scope no longer matching; a missing media row keeps its own code because it
/// is the one failure a caller is expected to retry; ambiguity keeps its own
/// code because retrying does not help; a broken row, broken SILK, or failed
/// database step is a read failure; a schema the collector cannot use is a
/// missing source, not a corrupt one.
fn from_voice_error(error: VoiceReadError) -> ReadError {
    match error {
        VoiceReadError::InvalidRequest => ReadError::InvalidParameter,
        VoiceReadError::KeysUnavailable => ReadError::KeysUnavailable,
        VoiceReadError::SourceUnavailable | VoiceReadError::SchemaUnavailable => {
            ReadError::SourceUnavailable
        }
        VoiceReadError::SourceChanged
        | VoiceReadError::MessageMissing
        | VoiceReadError::MessageContradictory
        | VoiceReadError::MessageNotVoice
        | VoiceReadError::VoiceContradictory
        | VoiceReadError::VoiceChanged => ReadError::ScopeChanged,
        VoiceReadError::DatabaseRead | VoiceReadError::RowInvalid | VoiceReadError::VoiceInvalid => {
            ReadError::SourceReadFailed
        }
        VoiceReadError::MessageAmbiguous | VoiceReadError::VoiceAmbiguous => {
            ReadError::VoiceSourceAmbiguous
        }
        VoiceReadError::VoiceSourceMissing => ReadError::VoiceSourceMissing,
        VoiceReadError::VoiceTooLarge => ReadError::BudgetExceeded,
        VoiceReadError::DeadlineExceeded => ReadError::DeadlineExceeded,
    }
}

// ============================================
// Identity
// ============================================

/// The identity one collection ran under.
///
/// Every part is compared before and after a collection. `database_root` is
/// internal routing for the collector and is never serialized into a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadId {
    pub(crate) account_id: String,
    pub(crate) account_dir: String,
    pub(crate) epoch: String,
    pub(crate) database_root: PathBuf,
    /// The canonical `sessions.id` of the row the identity was read from. Keyed
    /// lookups (the stored keys) use this, not the handle the capture was asked
    /// for, which may have been a name such as `"default"`.
    pub(crate) session_id: String,
}

// ============================================
// Backend seam
// ============================================

/// One verified voice read handed from the backend to the route.
///
/// The audio is still raw: the route owns the base64 step, so encoding up to
/// 32 MiB happens on the blocking pool as part of the round rather than on the
/// runtime thread that is serving other requests. `sha256` is the collector's
/// digest of `audio` and is echoed, not recomputed.
pub(crate) struct VerifiedVoice {
    pub(crate) media_shard: String,
    pub(crate) media_row_id: i64,
    pub(crate) media_owner_id: i64,
    pub(crate) audio: Vec<u8>,
    pub(crate) sha256: String,
}

/// Source access the read routes need. The production implementation samples the
/// real process; tests inject fakes and never touch the real system.
///
/// Every stage takes the round. The identity capture and the key read are
/// blocking work on a thread nothing can interrupt, so a stage that is handed no
/// deadline keeps sampling or keeps waiting for the shared connection after its
/// caller has already answered.
#[async_trait::async_trait]
pub(crate) trait ReadBackend: Send + Sync + 'static {
    /// Capture the identity currently observable for the WeChat process.
    async fn capture_identity(&self, cancel: &CollectionCancel) -> Result<ReadId, ReadError>;

    /// Stored decryption keys for this account, read only. Implementations must
    /// not start key extraction, drive the GUI, or hold a database connection
    /// across an await point.
    async fn source_keys(
        &self,
        id: &ReadId,
        cancel: &CollectionCancel,
    ) -> Result<HashMap<String, String>, ReadError>;

    /// Stored decryption keys for the contact-roster read.
    ///
    /// The same identity-bound read as [`ReadBackend::source_keys`], but the
    /// requirement is the `contact.db` key: a roster read must not be refused
    /// because the account has no message shard. It never starts key extraction
    /// and applies no weaker identity fence than [`ReadBackend::source_keys`].
    async fn contact_keys(
        &self,
        id: &ReadId,
        cancel: &CollectionCancel,
    ) -> Result<HashMap<String, String>, ReadError>;

    /// Collect the rows of one chat's sample. The implementation owns the
    /// blocking work; the caller never holds a database or page-store lock while
    /// this runs.
    ///
    /// `cancel` carries the round's deadline into the blocking work: an
    /// implementation that cannot stop on its own is still expected to stop
    /// returning once the caller has given up, and a blocking implementation is
    /// expected to stop *doing* work when it is set.
    async fn collect_messages(
        &self,
        id: &ReadId,
        chat_id: &str,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<Vec<Value>, ReadError>;

    /// Collect the rows of one frozen contact-roster sample. The implementation
    /// owns the blocking work; the caller never holds a database or page-store
    /// lock while this runs.
    async fn collect_contacts(
        &self,
        id: &ReadId,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<Vec<Value>, ReadError>;

    /// Read one verified voice note. The implementation owns the blocking work;
    /// the caller never holds a database lock while this runs, and the same
    /// cancellation that ends the round is handed to it so a shard read stops
    /// with the caller rather than after it.
    async fn collect_voice(
        &self,
        id: &ReadId,
        request: &VoiceRequest,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<VerifiedVoice, ReadError>;
}

/// The wall clock of one read round.
///
/// A round is a request: it waits for the GUI guard, fences the identity, reads
/// the keys, collects the sample, fences it again and freezes it. Only the first
/// page touches the source, so a continuation is cheap by construction — but it
/// waits on the same guard, and a request that queues behind a long UI
/// operation must give the turn back rather than hold a connection open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoundBudget {
    /// Total wall clock of the round, counted from the moment the request enters
    /// it — before it waits for the GUI guard, not after it has one.
    ///
    /// This is the clock the caller was promised, and it is the one that reaches
    /// the backend. A round that started after the guard would hand the
    /// collection a fresh budget for work whose queueing the caller already
    /// waited for, so a request could take longer than its own deadline to
    /// answer.
    pub(crate) total: Duration,
    /// How long this request waits for the shared GUI guard before giving up.
    /// Its own bound rather than the round's, because a queued request has not
    /// spent anything yet and must not consume the budget of whoever is running.
    /// It is also bounded by what is left of the round, for the same reason.
    guard_wait: Duration,
}

impl RoundBudget {
    /// The production budget. The collection budget is the same clock, so the
    /// tighter of the two is the one that applies, and it is stated once.
    pub(crate) fn production() -> Self {
        Self {
            total: COLLECTION_BUDGET,
            guard_wait: Duration::from_secs(2),
        }
    }
}

impl Default for RoundBudget {
    fn default() -> Self {
        Self::production()
    }
}

/// Take the shared GUI guard without waiting longer than the budget allows.
///
/// A read that cannot get the guard is not answered with a page: it is answered
/// with the deadline, having changed nothing.
///
/// The wait is also bounded by what is left of the round. The two bounds are
/// different questions — the guard's own is about queueing, the round's is about
/// the answer — and the smaller one is the honest limit, because a caller that
/// gave up at its deadline has to be answered at that deadline.
pub(crate) async fn take_gui_guard(
    budget: RoundBudget,
    round: &CollectionCancel,
) -> Result<crate::execution::GuiGuard, ReadError> {
    let wait = budget.guard_wait.min(round.remaining());
    tokio::time::timeout(wait, acquire_gui_guard())
        .await
        .map_err(|_| ReadError::DeadlineExceeded)
}

/// Run one step of a round under the round's deadline.
///
/// On expiry the shared cancellation is set, so work already in flight on a
/// blocking thread stops at its next check instead of being left running behind
/// an answer nobody is waiting for.
pub(crate) async fn within_round<T, F>(cancel: &CollectionCancel, work: F) -> Result<T, ReadError>
where
    F: std::future::Future<Output = Result<T, ReadError>>,
{
    if cancel.expired() {
        cancel.cancel();
        return Err(ReadError::DeadlineExceeded);
    }
    match tokio::time::timeout(cancel.remaining(), work).await {
        Ok(result) => result,
        Err(_) => {
            cancel.cancel();
            Err(ReadError::DeadlineExceeded)
        }
    }
}

/// Reads the running client. Every method is read-only and blocking, so each one
/// runs on the blocking pool; nothing holds the agent database connection
/// across an await point.
pub(crate) struct ProcessReadBackend;

impl ProcessReadBackend {
    /// The stored keys bound to the pinned identity, read once.
    ///
    /// Shared by the message and roster reads so the identity/account check and
    /// the canonical-key lookup cannot drift between them. It validates nothing
    /// beyond that binding: each caller applies its own source requirement to the
    /// map this returns, and neither starts key extraction.
    async fn bound_keys(
        &self,
        id: &ReadId,
        cancel: &CollectionCancel,
    ) -> Result<HashMap<String, String>, ReadError> {
        let account_dir = id.account_dir.clone();
        // The canonical row id the identity was captured from. A session handle
        // resolved by name (`"default"`) points at a row whose `id` is a UUID;
        // binding the key read to the handle would look under the wrong key and
        // find no stored keys at all.
        let session_id = id.session_id.clone();
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            // The connection guard lives and dies inside this closure.
            let db = crate::db::try_get_db(cancel.remaining()).map_err(|error| match error {
                crate::db::LockError::WaitExpired => ReadError::DeadlineExceeded,
                crate::db::LockError::Poisoned => ReadError::KeysUnavailable,
            })?;
            let account = crate::db::queries::get_session_logged_in_user(&db, &session_id)
                .filter(|account| !account.is_empty())
                .ok_or(ReadError::IdentityUnavailable)?;
            // The session and the captured identity must agree about which
            // account is logged in; both name the same canonical row, so a
            // disagreement is a real one and not a handle mismatch.
            if account != account_dir {
                return Err(ReadError::IdentityUnavailable);
            }
            Ok(crate::tools::wechat_keys::get_stored_keys(
                &db,
                &session_id,
                &account,
            ))
        })
        .await
        .map_err(|_| ReadError::KeysUnavailable)?
    }
}

#[async_trait::async_trait]
impl ReadBackend for ProcessReadBackend {
    async fn capture_identity(&self, cancel: &CollectionCancel) -> Result<ReadId, ReadError> {
        // The round's own cancellation goes onto the blocking thread: a capture
        // that walks a busy `/proc` and waits for the shared connection has to
        // end with the round, not after the answer.
        let cancel = cancel.clone();
        let identity = tokio::task::spawn_blocking(move || {
            capture_read_identity_for(DEFAULT_SESSION_ID, &cancel)
        })
        .await
            .map_err(|_| ReadError::IdentityUnavailable)?
            .map_err(identity_error)?;
        Ok(ReadId {
            account_id: identity.account_id().to_string(),
            account_dir: identity.account_dir().to_string(),
            epoch: identity.epoch().to_string(),
            database_root: identity.database_root().to_path_buf(),
            session_id: identity.session_id().to_string(),
        })
    }

    async fn source_keys(
        &self,
        id: &ReadId,
        cancel: &CollectionCancel,
    ) -> Result<HashMap<String, String>, ReadError> {
        let keys = self.bound_keys(id, cancel).await?;
        // The message read keeps its own source requirement: at least one
        // numbered message shard must be stored for this account.
        if !has_message_source(&keys) {
            crate::tools::wechat_keys::request_refresh_after_missing_keys(
                &id.session_id, &id.account_dir, &id.database_root, &keys,
                vec![crate::tools::wechat_keys::KeyNeed::MessageShards],
            );
            return Err(ReadError::KeysUnavailable);
        }
        Ok(keys)
    }

    async fn contact_keys(
        &self,
        id: &ReadId,
        cancel: &CollectionCancel,
    ) -> Result<HashMap<String, String>, ReadError> {
        let keys = self.bound_keys(id, cancel).await?;
        // The roster requires its own key and nothing about message shards.
        if !has_contact_source(&keys) {
            crate::tools::wechat_keys::request_refresh_after_missing_keys(
                &id.session_id, &id.account_dir, &id.database_root, &keys,
                vec![crate::tools::wechat_keys::KeyNeed::Database("contact.db".to_string())],
            );
            return Err(ReadError::KeysUnavailable);
        }
        Ok(keys)
    }

    async fn collect_messages(
        &self,
        id: &ReadId,
        chat_id: &str,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<Vec<Value>, ReadError> {
        let database_root = id.database_root.clone();
        let account_dir = id.account_dir.clone();
        let chat_id = chat_id.to_string();
        let keys = keys.clone();
        let recovery_keys = keys.clone();
        // The same cancellation the caller holds, moved onto the blocking
        // thread: if the caller stops waiting, this collection stops too.
        let collection_cancel = cancel.clone();
        let collected = tokio::task::spawn_blocking(move || {
            collect_message_snapshot_until(&database_root, &account_dir, &chat_id, &keys, &collection_cancel)
        })
        .await
        .map_err(|_| ReadError::SourceReadFailed)?;
        if collected.is_err() {
            if let Some(name) = cancel.authentication_failure() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&id.session_id, &id.account_dir, &name);
            } else if collected == Err(crate::tools::wechat_messages::MessageSnapshotError::KeysUnavailable) {
                crate::tools::wechat_keys::request_refresh_after_missing_keys(
                    &id.session_id, &id.account_dir, &id.database_root, &recovery_keys,
                    vec![crate::tools::wechat_keys::KeyNeed::MessageShards],
                );
            }
        }
        collected.map_err(from_snapshot_error)
    }

    async fn collect_contacts(
        &self,
        id: &ReadId,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<Vec<Value>, ReadError> {
        let database_root = id.database_root.clone();
        let keys = keys.clone();
        let recovery_keys = keys.clone();
        // The same cancellation the caller holds, moved onto the blocking
        // thread: if the caller stops waiting, this collection stops too.
        let collection_cancel = cancel.clone();
        let collected = tokio::task::spawn_blocking(move || {
            collect_contact_snapshot_until(&database_root, &keys, &collection_cancel)
        })
        .await
        .map_err(|_| ReadError::SourceReadFailed)?;
        if collected.is_err() {
            if let Some(name) = cancel.authentication_failure() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&id.session_id, &id.account_dir, &name);
            } else if collected == Err(crate::tools::wechat_messages::MessageSnapshotError::KeysUnavailable) {
                crate::tools::wechat_keys::request_refresh_after_missing_keys(
                    &id.session_id, &id.account_dir, &id.database_root, &recovery_keys,
                    vec![crate::tools::wechat_keys::KeyNeed::Database("contact.db".to_string())],
                );
            }
        }
        collected.map_err(from_snapshot_error)
    }

    async fn collect_voice(
        &self,
        id: &ReadId,
        request: &VoiceRequest,
        keys: &HashMap<String, String>,
        cancel: &CollectionCancel,
    ) -> Result<VerifiedVoice, ReadError> {
        let database_root = id.database_root.clone();
        let source_shard = request.source_shard.clone();
        let request = request.clone();
        let keys = keys.clone();
        let recovery_keys = keys.clone();
        // The same cancellation the caller holds, moved onto the blocking
        // thread: a read that outlives the caller's patience has to stop there.
        let collection_cancel = cancel.clone();
        let collected = tokio::task::spawn_blocking(move || {
            read_voice_sample(&database_root, &keys, &request, &collection_cancel).map(|sample| VerifiedVoice {
                media_shard: sample.media_shard().to_string(),
                media_row_id: sample.media_rowid(),
                media_owner_id: sample.chat_name_id(),
                audio: sample.audio().to_vec(),
                sha256: sample.sha256().to_string(),
            })
        })
        .await
        .map_err(|_| ReadError::SourceReadFailed)?;
        if collected.is_err() {
            if let Some(name) = cancel.authentication_failure() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&id.session_id, &id.account_dir, &name);
            } else if matches!(&collected, Err(VoiceReadError::KeysUnavailable)) {
                crate::tools::wechat_keys::request_refresh_after_missing_keys(
                    &id.session_id, &id.account_dir, &id.database_root, &recovery_keys,
                    vec![
                        crate::tools::wechat_keys::KeyNeed::Database(source_shard),
                        crate::tools::wechat_keys::KeyNeed::MediaShards,
                    ],
                );
            }
        }
        collected.map_err(from_voice_error)
    }
}

/// How an identity refusal reaches the HTTP contract.
///
/// Almost all of them are "there is no identity to read under". The two that are
/// not are: an unrecorded login write — that session does have an account, the
/// account is simply not provably the one on screen, and a client that retries it
/// as an identity outage would keep reading the wrong epoch — and a round that
/// ran out, which says nothing at all about the account and must not be
/// reported as if it did.
fn identity_error(error: ReadIdentityError) -> ReadError {
    match error {
        ReadIdentityError::LoginUnverified => ReadError::LoginUnverified,
        ReadIdentityError::DeadlineExceeded => ReadError::DeadlineExceeded,
        _ => ReadError::IdentityUnavailable,
    }
}

// ============================================
// Frozen page cache
// ============================================

/// Process-wide frozen samples.
///
/// The lock is taken for the publication of one prepared sample, for one `next`
/// call and for nothing else: the measuring of a sample — one serialization per
/// row, up to 50 000 of them — happens before it, off this lock. Database
/// collection never runs while it is held.
pub(crate) struct PageCache {
    /// The budgets the store under this lock was built with. A copy, so a caller
    /// can measure a sample without holding the lock that guards the store it
    /// will be published into, and so the two can never disagree.
    config: PageStoreConfig,
    store: Mutex<FrozenPageStore>,
}

static GLOBAL_CACHE: OnceLock<Arc<PageCache>> = OnceLock::new();

impl PageCache {
    pub(crate) fn new() -> Self {
        PageCache {
            config: PageStoreConfig::default(),
            store: Mutex::new(FrozenPageStore::new()),
        }
    }

    /// The cache the routes use. One per process; a restart empties it, which
    /// surfaces to callers as `410`.
    pub(crate) fn global() -> &'static Arc<PageCache> {
        GLOBAL_CACHE.get_or_init(|| Arc::new(PageCache::new()))
    }

    /// The store's budgets, for measuring a sample before it is registered.
    pub(crate) fn config(&self) -> PageStoreConfig {
        self.config
    }

    /// Take the store's lock, waited for at most `budget`.
    ///
    /// This is a plain mutex on the runtime's threads, so whoever holds it can
    /// decide when this request is answered. A read that answers a request under
    /// a deadline cannot wait on it unboundedly: it reports the deadline and
    /// registers nothing, which is the same answer an unlocked round gives.
    ///
    /// A poisoned store is still reused, as before: the store's own mutations
    /// are whole-map operations, so a holder that panicked leaves it consistent,
    /// and refusing every later read over one panic would be a worse failure
    /// than the one being recovered from.
    fn lock_within(&self, budget: Duration) -> Result<MutexGuard<'_, FrozenPageStore>, ReadError> {
        match crate::db::lock_within(&self.store, budget) {
            Ok(store) => Ok(store),
            Err(crate::db::LockError::WaitExpired) => Err(ReadError::DeadlineExceeded),
            // Poisoning does not block acquisition: the guard is gone with the
            // holder that panicked, so this takes the lock at once.
            Err(crate::db::LockError::Poisoned) => {
                Ok(self.store.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
            }
        }
    }

    /// Drop a sample this round registered and is now refusing to serve.
    ///
    /// Best effort on purpose: this runs on a failure path whose answer is
    /// already decided, and it is a short operation against a lock that is not
    /// held for long. It waits a moment rather than indefinitely — an abandoned
    /// round must not hold its caller — and reports nothing if it cannot get in.
    /// What that costs is a sample left to expire on its own TTL, which is what a
    /// client that never comes back would have had anyway.
    fn release_best_effort(&self, snapshot_id: &str, scope: &SnapshotScope) {
        let grace = Duration::from_millis(250);
        let Ok(mut store) = self.lock_within(grace) else {
            return;
        };
        let _ = store.release(snapshot_id, scope, Instant::now());
    }

    fn next(
        &self,
        cursor: &str,
        scope: &SnapshotScope,
        now: Instant,
        budget: Duration,
    ) -> Result<FrozenPage, ReadError> {
        // A continuation is a pure cache read, but it is still a request under a
        // deadline waiting on a plain mutex, so it is bounded the same way.
        Ok(self.lock_within(budget)?.next(cursor, scope, now)?)
    }

    pub(crate) fn publish_v2(
        &self,
        prepared: wechat_read_pages::PreparedSnapshot,
        now: Instant,
        budget: Duration,
    ) -> Result<FrozenPage, ReadError> {
        Ok(self.lock_within(budget)?.publish(prepared, now)?)
    }

    pub(crate) fn release_v2(
        &self,
        token: &str,
        account_dir: &str,
        epoch: &str,
        now: Instant,
        budget: Duration,
    ) -> Result<bool, ReadError> {
        Ok(self.lock_within(budget)?.release_by_token_account(token, account_dir, epoch, now)?)
    }

    pub(crate) fn next_v2(
        &self,
        cursor: &str,
        scope: &SnapshotScope,
        now: Instant,
        budget: Duration,
    ) -> Result<FrozenPage, ReadError> {
        self.next(cursor, scope, now, budget)
    }

    pub(crate) fn cursor_scope_v2(
        &self,
        cursor: &str,
        now: Instant,
        budget: Duration,
    ) -> Result<SnapshotScope, ReadError> {
        Ok(self.lock_within(budget)?.scope_for_cursor(cursor, now)?)
    }
}

impl Default for PageCache {
    fn default() -> Self {
        PageCache::new()
    }
}

fn source_set(keys: &HashMap<String, String>) -> BTreeSet<String> {
    keys.keys().cloned().collect()
}

/// The stored-key facts the roster fence compares: only the `contact.db` key.
///
/// The roster reads that one database, so a message shard appearing or
/// disappearing mid-collection is not a change this read can act on and must not
/// refuse the page. Unlike [`source_set`], this compares the `contact.db` value
/// as well as its presence, because a re-keyed contact database is a different
/// source; the comparison is in process only and the value is never formatted,
/// hashed or logged.
fn contact_source_fence(keys: &HashMap<String, String>) -> Option<&str> {
    keys.get("contact.db").map(String::as_str)
}

/// Whether a stored-key map can back the message read: at least one numbered
/// message shard. Kept as the message route's own requirement and deliberately
/// not applied to the roster.
fn has_message_source(keys: &HashMap<String, String>) -> bool {
    keys.keys().any(|name| is_message_db(name))
}

/// Whether a stored-key map can back the roster read: the `contact.db` key and
/// nothing else. A contact-only account is a valid roster source.
fn has_contact_source(keys: &HashMap<String, String>) -> bool {
    keys.contains_key("contact.db")
}

/// The stored-key facts a voice read fences: the named message shard's key and
/// every stored media shard key.
///
/// The request reads exactly one message shard and whichever media shards carry
/// the audio, so those are the keys whose change would make the answer describe
/// a source the caller no longer has. The `contact.db` key and the other message
/// shards are not read by this route and must not refuse it. The comparison is
/// in process only; no key value is ever formatted, hashed or logged.
// ============================================
// Pipeline
// ============================================

/// Capture identity and refuse the request when it is not the scope the caller
/// asked for. Runs under the GUI guard, so a login or restart cannot start
/// between this check and the collection that follows it.
/// Re-capture identity and refuse the page when anything about the source moved.
/// Called with the GUI guard still held, so the "after" side is as trustworthy
/// as the "before" side.
pub(crate) async fn confirm_unchanged<B: ReadBackend>(
    backend: &B,
    before: &ReadId,
    cancel: &CollectionCancel,
) -> Result<(), ReadError> {
    let after = backend.capture_identity(cancel).await?;
    if after != *before {
        return Err(ReadError::ScopeChanged);
    }
    Ok(())
}
