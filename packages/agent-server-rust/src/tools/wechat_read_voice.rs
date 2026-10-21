//! Verified, in-process reads of one inbound voice note.
//!
//! A caller that already holds a verified read identity (an account directory and
//! its `db_storage` root) asks for one voice message and gets back the exact
//! bytes that belong to it, with the provenance that was checked on the way:
//! which message shard carried the message, which media shard carried the audio,
//! and the `server_id` / `create_time` / `media_local_id` / `rowid` that tie them
//! together.
//!
//! What this module is, stated plainly:
//!
//! * It reads the media shards that actually exist under the resolved root. It
//!   never searches default locations, never trusts a request-supplied path, and
//!   never falls back to another candidate root.
//! * The requested `source_shard` is only a basename. It is checked against
//!   `message_[0-9]+.db`, resolved under `<root>/message`, and must be a regular
//!   file there; a name that cannot be resolved that way is refused before any
//!   database is opened.
//! * The message row is located **only** by `local_id` inside the named chat's
//!   `Msg_<md5>` table, and every other message field (`server_id`,
//!   `create_time`, `local_type`) is then verified against the request. A missing
//!   row, a duplicate row, and a row whose fields disagree each fail on their own
//!   code.
//! * The audio is located in the media databases by their **own** `Name2Id`
//!   mapping: `VoiceInfo.chat_name_id` is the media shard's rowid for the chat,
//!   which is a different namespace from the message shards' `Name2Id`. The
//!   message `local_id` is never used as a join key into a media shard, and
//!   `data_index` is never used to guess which row is the right one.
//! * The association key is `chat_name_id` + `svr_id` + `create_time`; the
//!   remaining candidates must be exactly one. Zero candidates is
//!   [`VoiceReadError::VoiceSourceMissing`]; more than one is
//!   [`VoiceReadError::VoiceAmbiguous`]; any row that shares the
//!   `chat_name_id`/`svr_id` key but carries a different `create_time` is a
//!   contradiction and fails the whole read.
//! * Before any `voice_data` blob is materialized, its length is checked against
//!   [`MAX_VOICE_BYTES`]; an oversized blob is refused from its length alone.
//!   The metadata is then re-checked inside a transaction on the same connection
//!   before the blob is read, and the blob is accepted only if it is still a
//!   `BLOB` of the same length.
//! * The SILK header is checked to the minimum the Python reader checks (an
//!   optional `0x02` prefix followed by `#!SILK_V3`). Full packet-boundary and
//!   duration validation stays on the Python `validate_silk` path; this module
//!   does **not** claim to be an equivalent port of it.
//! * Nothing here transcodes, downloads, drives the GUI, or persists audio. A
//!   missing or unreadable shard, a missing key, a bad schema and a changing
//!   media shard set all refuse the whole read rather than returning a partial
//!   or empty answer.
//! * A media entry named `media_<digits>.db` whose number does not fit `u32` is
//!   refused as [`VoiceReadError::SourceUnavailable`] rather than skipped, so a
//!   present voice cannot be reported as missing.
//! * After the audio is read, the message anchor is re-observed on a fresh
//!   read-only connection: a message that was deleted, replaced or turned into a
//!   non-voice row by then is [`VoiceReadError::VoiceChanged`], and a source
//!   shard that can no longer be resolved is [`VoiceReadError::SourceChanged`].
//!   Audio already in hand is therefore never attached to a newer identity. This
//!   is a second observation, not an atomic snapshot across databases and not an
//!   OS-level guarantee.
//!
//! What this module is **not**:
//!
//! * It is not an authorization decision. It knows no owner, no binding epoch,
//!   no inbound claim and no recall, and it takes none of them as input. Where
//!   this file says "verified" it means source and content verification, never
//!   permission. The caller establishes the right to read the message before
//!   calling and owns the business claim around it; this module only refuses to
//!   return bytes it cannot tie to the requested message.
//!
//! The caller owns the round: the same [`CollectionCancel`] the read routes
//! already use is checked at every stage and installed on every connection as an
//! interrupt, so an 8-second HTTP deadline reaches the SQLite work instead of
//! being noticed only after it returns.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::tools::wechat_db::{numbered_db_digits, numbered_db_index};
use crate::tools::wechat_messages::{
    get_msg_table_name, open_shard_readonly, CollectionCancel, MessageSnapshotError,
};

/// Largest stored `VoiceInfo.voice_data` this module will materialize (32 MiB).
///
/// Above this the metadata alone is enough to refuse: the length is read before
/// the blob, so an oversized row is never copied out of SQLCipher.
pub const MAX_VOICE_BYTES: usize = 32 * 1024 * 1024;

/// Upper bound on the `VoiceInfo` rows scanned for one `(chat_name_id, svr_id)`
/// key. A key that returns more than this is treated as ambiguous rather than
/// scanned without limit.
const MAX_VOICE_ROWS: usize = 64;

/// Longest accepted `chat_id`/`source_shard` string, so a request cannot turn an
/// identifier into an unbounded allocation.
const MAX_REQUEST_FIELD: usize = 4096;

/// One request for a single voice note.
///
/// `source_shard` is the basename of the message shard the caller believes holds
/// the message; the module resolves and verifies it. None of these fields is a
/// filesystem path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceRequest {
    pub chat_id: String,
    pub source_shard: String,
    pub local_id: i64,
    pub server_id: i64,
    pub created: i64,
}

impl VoiceRequest {
    /// Reject a request that cannot name a message before any source is touched.
    ///
    /// `local_id` must be positive, `server_id` must be non-zero, and
    /// `source_shard` must be exactly a numbered `message_N.db` basename — no
    /// directory component, extension or whitespace trick.
    pub fn validate(&self) -> Result<(), VoiceReadError> {
        if self.chat_id.is_empty() || self.chat_id.len() > MAX_REQUEST_FIELD {
            return Err(VoiceReadError::InvalidRequest);
        }
        if self.local_id <= 0 {
            return Err(VoiceReadError::InvalidRequest);
        }
        if self.server_id == 0 {
            return Err(VoiceReadError::InvalidRequest);
        }
        if numbered_db_index(&self.source_shard, "message_").is_none() {
            return Err(VoiceReadError::InvalidRequest);
        }
        Ok(())
    }
}

/// One verified voice note: the bytes and the provenance that was checked.
///
/// Every field is private and the `Debug` implementation redacts the chat id and
/// the audio; the byte length and the digest are safe to log, the content is not.
pub struct VoiceSample {
    chat_id: String,
    source_shard: String,
    media_shard: String,
    server_id: i64,
    create_time: i64,
    media_local_id: i64,
    media_rowid: i64,
    chat_name_id: i64,
    audio: Vec<u8>,
    sha256: String,
}

impl VoiceSample {
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    /// The message shard the message row was verified in.
    pub fn source_shard(&self) -> &str {
        &self.source_shard
    }

    /// The media shard the audio row was read from.
    pub fn media_shard(&self) -> &str {
        &self.media_shard
    }

    pub fn server_id(&self) -> i64 {
        self.server_id
    }

    pub fn create_time(&self) -> i64 {
        self.create_time
    }

    /// The media shard's own `VoiceInfo.local_id`. Recorded to show it was read
    /// but never used as a cross-database join key.
    pub fn media_local_id(&self) -> i64 {
        self.media_local_id
    }

    /// The media shard's `VoiceInfo` rowid.
    pub fn media_rowid(&self) -> i64 {
        self.media_rowid
    }

    /// The media shard's `Name2Id` rowid for this chat, which is independent of
    /// the message shards' namespace.
    pub fn chat_name_id(&self) -> i64 {
        self.chat_name_id
    }

    pub fn audio(&self) -> &[u8] {
        &self.audio
    }

    /// Lowercase hex SHA-256 of [`VoiceSample::audio`].
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

impl fmt::Debug for VoiceSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VoiceSample")
            .field("chat_id", &"[redacted]")
            .field("source_shard", &self.source_shard)
            .field("media_shard", &self.media_shard)
            .field("server_id", &self.server_id)
            .field("create_time", &self.create_time)
            .field("media_local_id", &self.media_local_id)
            .field("media_rowid", &self.media_rowid)
            .field("chat_name_id", &self.chat_name_id)
            .field("bytes", &self.audio.len())
            .field("sha256", &self.sha256)
            .finish()
    }
}

/// Why a voice read produced no sample.
///
/// Fieldless on purpose: no path, key, chat id or message content can leak into a
/// log line or an HTTP body through this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceReadError {
    /// A field of the request cannot name a message: empty chat id, non-positive
    /// `local_id`, zero `server_id`, or a `source_shard` that is not a numbered
    /// `message_N.db` basename.
    InvalidRequest,
    /// A discovered shard has no decryption key.
    KeysUnavailable,
    /// The message directory or a named shard is missing, not a regular file, or
    /// otherwise cannot be resolved under the verified root.
    SourceUnavailable,
    /// A shard is readable but does not carry the schema this read requires.
    SchemaUnavailable,
    /// The media shard set differed between the first and the last discovery.
    SourceChanged,
    /// A shard could not be opened, decrypted, queried or stepped.
    DatabaseRead,
    /// A stored column has the wrong SQLite type where an integer was required.
    RowInvalid,
    /// The named message shard has no row for this `local_id`.
    MessageMissing,
    /// More than one message row carries this `local_id`.
    MessageAmbiguous,
    /// The message row disagrees with the request's `server_id` or
    /// `create_time`.
    MessageContradictory,
    /// The message row's base type is not 34 (voice).
    MessageNotVoice,
    /// No media shard carries audio for this message: it is not here (yet).
    VoiceSourceMissing,
    /// More than one media row (or media chat mapping) matches.
    VoiceAmbiguous,
    /// A media row shares the association key but contradicts `create_time`.
    VoiceContradictory,
    /// The stored audio blob is above [`MAX_VOICE_BYTES`].
    VoiceTooLarge,
    /// The blob, or its metadata, changed between the metadata scan and the read.
    VoiceChanged,
    /// The stored bytes are not a minimally valid SILK v3 payload.
    VoiceInvalid,
    /// The read ran out of its wall clock. Nothing is returned.
    DeadlineExceeded,
}

impl VoiceReadError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "WECHAT_VOICE_INVALID_REQUEST",
            Self::KeysUnavailable => "WECHAT_VOICE_KEYS_UNAVAILABLE",
            Self::SourceUnavailable => "WECHAT_VOICE_SOURCE_UNAVAILABLE",
            Self::SchemaUnavailable => "WECHAT_VOICE_SCHEMA_UNAVAILABLE",
            Self::SourceChanged => "WECHAT_VOICE_SOURCE_CHANGED",
            Self::DatabaseRead => "WECHAT_VOICE_DB_READ_FAILED",
            Self::RowInvalid => "WECHAT_VOICE_ROW_INVALID",
            Self::MessageMissing => "WECHAT_VOICE_MESSAGE_MISSING",
            Self::MessageAmbiguous => "WECHAT_VOICE_MESSAGE_AMBIGUOUS",
            Self::MessageContradictory => "WECHAT_VOICE_MESSAGE_CONTRADICTORY",
            Self::MessageNotVoice => "WECHAT_VOICE_MESSAGE_NOT_VOICE",
            Self::VoiceSourceMissing => "WECHAT_VOICE_SOURCE_MISSING",
            Self::VoiceAmbiguous => "WECHAT_VOICE_SOURCE_AMBIGUOUS",
            Self::VoiceContradictory => "WECHAT_VOICE_METADATA_CONTRADICTORY",
            Self::VoiceTooLarge => "WECHAT_VOICE_TOO_LARGE",
            Self::VoiceChanged => "WECHAT_VOICE_CHANGED",
            Self::VoiceInvalid => "WECHAT_VOICE_INVALID_SILK",
            Self::DeadlineExceeded => "WECHAT_VOICE_DEADLINE_EXCEEDED",
        }
    }
}

impl fmt::Display for VoiceReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for VoiceReadError {}

/// Stop at the caller's deadline.
fn check(cancel: &CollectionCancel) -> Result<(), VoiceReadError> {
    if cancel.expired() {
        return Err(VoiceReadError::DeadlineExceeded);
    }
    Ok(())
}

/// Map a failed database step, keeping "ran out of time" distinct from "this
/// source is broken".
fn read_failure(cancel: &CollectionCancel) -> VoiceReadError {
    if cancel.expired() {
        VoiceReadError::DeadlineExceeded
    } else {
        VoiceReadError::DatabaseRead
    }
}

/// Map one column read: a wrong SQLite type is its own failure, everything else
/// is a database read.
fn column_failure(cancel: &CollectionCancel, error: rusqlite::Error) -> VoiceReadError {
    match error {
        rusqlite::Error::InvalidColumnType(..) => VoiceReadError::RowInvalid,
        _ => read_failure(cancel),
    }
}

/// Map the shard opener's refusals onto this module's codes.
fn from_snapshot(error: MessageSnapshotError) -> VoiceReadError {
    match error {
        MessageSnapshotError::KeysUnavailable => VoiceReadError::KeysUnavailable,
        MessageSnapshotError::SourceUnavailable => VoiceReadError::SourceUnavailable,
        MessageSnapshotError::SourceChanged => VoiceReadError::SourceChanged,
        MessageSnapshotError::DatabaseRead => VoiceReadError::DatabaseRead,
        MessageSnapshotError::BudgetExceeded => VoiceReadError::DatabaseRead,
        MessageSnapshotError::RowInvalid => VoiceReadError::RowInvalid,
        MessageSnapshotError::RowDuplicate => VoiceReadError::RowInvalid,
        MessageSnapshotError::RowTooLarge => VoiceReadError::DatabaseRead,
        MessageSnapshotError::DeadlineExceeded => VoiceReadError::DeadlineExceeded,
    }
}

/// Whether `name` is a table this shard carries.
fn table_exists(
    conn: &Connection,
    name: &str,
    cancel: &CollectionCancel,
) -> Result<bool, VoiceReadError> {
    check(cancel)?;
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
        [name],
        |_| Ok(()),
    )
    .optional()
    .map(|found| found.is_some())
    .map_err(|_| read_failure(cancel))
}

/// The `db_storage/message` path of one request-named message shard.
///
/// The name is only ever a basename: it must match `message_[0-9]+.db`, and the
/// resolved entry must be a regular file (not a symlink, directory or device)
/// directly under `<root>/message`. A request can therefore never point the read
/// at another file, and a name is never guessed to be a shard.
fn message_shard_path(
    database_root: &Path,
    source_shard: &str,
) -> Result<PathBuf, VoiceReadError> {
    if numbered_db_index(source_shard, "message_").is_none() {
        return Err(VoiceReadError::InvalidRequest);
    }
    let path = database_root.join("message").join(source_shard);
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|_| VoiceReadError::SourceUnavailable)?;
    if !metadata.file_type().is_file() {
        return Err(VoiceReadError::SourceUnavailable);
    }
    Ok(path)
}

/// How an entry name inside `<root>/message` relates to the media shards.
enum MediaShardName {
    /// `media_<digits>.db` with a number that fits `u32`.
    Numbered(u32),
    /// Any other name: not a media shard this read looks at.
    NotAShard,
    /// `media_<digits>.db` whose number does not fit `u32`. It looks like a shard
    /// this code cannot order or address, so it is refused rather than dropped.
    Unrepresentable,
}

/// Classify one directory entry name as a media shard.
///
/// A name that is `media_<digits>.db` but whose number overflows `u32` cannot be
/// silently skipped: skipping it could hide the only copy of the audio and turn a
/// present voice into [`VoiceReadError::VoiceSourceMissing`].
fn media_shard_name(name: &str) -> MediaShardName {
    let Some(digits) = numbered_db_digits(name, "media_") else {
        return MediaShardName::NotAShard;
    };
    match digits.parse::<u32>() {
        Ok(index) => MediaShardName::Numbered(index),
        Err(_) => MediaShardName::Unrepresentable,
    }
}

/// Discover the numbered media shards under `<root>/message`, in number order.
///
/// Reading no media shard is not an error here: it is a source with no audio, and
/// the candidate count decides how that is reported. An unreadable directory, an
/// entry whose name is not valid UTF-8, an entry named like a shard that is not a
/// regular file, and a shard name whose number does not fit `u32` are all
/// refusals, because none of them can be told apart from "the audio is not
/// there".
fn discover_media_shards(database_root: &Path) -> Result<Vec<String>, VoiceReadError> {
    let directory = database_root.join("message");
    let entries = std::fs::read_dir(&directory).map_err(|_| VoiceReadError::SourceUnavailable)?;

    let mut shards: Vec<(u32, String)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| VoiceReadError::SourceUnavailable)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(VoiceReadError::SourceUnavailable)?;
        let index = match media_shard_name(name) {
            MediaShardName::Numbered(index) => index,
            MediaShardName::NotAShard => continue,
            MediaShardName::Unrepresentable => return Err(VoiceReadError::SourceUnavailable),
        };
        let file_type = entry
            .file_type()
            .map_err(|_| VoiceReadError::SourceUnavailable)?;
        if !file_type.is_file() {
            return Err(VoiceReadError::SourceUnavailable);
        }
        shards.push((index, name.to_string()));
    }
    shards.sort();
    Ok(shards.into_iter().map(|(_, name)| name).collect())
}

/// Verify the message row and return its `create_time`.
///
/// The row is located by `local_id` alone (the table's identity inside the chat),
/// and the request's `server_id`, `create_time` and voice base type are then all
/// checked. A missing row, a repeated `local_id`, and a row that disagrees with
/// the request each get their own refusal.
fn read_message_evidence(
    conn: &Connection,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
) -> Result<i64, VoiceReadError> {
    check(cancel)?;
    let table = get_msg_table_name(&request.chat_id);
    if !table_exists(conn, &table, cancel)? {
        return Err(VoiceReadError::SchemaUnavailable);
    }
    check(cancel)?;

    let sql =
        format!("SELECT server_id, local_type, create_time FROM \"{table}\" WHERE local_id = ?1 LIMIT 2");
    let mut statement = conn.prepare(&sql).map_err(|_| read_failure(cancel))?;
    let mut rows = statement
        .query([request.local_id])
        .map_err(|_| read_failure(cancel))?;

    let mut found: Vec<(i64, i64, i64)> = Vec::new();
    while let Some(row) = rows.next().map_err(|_| read_failure(cancel))? {
        check(cancel)?;
        let server_id: i64 = row.get(0).map_err(|error| column_failure(cancel, error))?;
        let local_type: i64 = row.get(1).map_err(|error| column_failure(cancel, error))?;
        let create_time: i64 = row.get(2).map_err(|error| column_failure(cancel, error))?;
        found.push((server_id, local_type, create_time));
    }

    match found.as_slice() {
        [] => Err(VoiceReadError::MessageMissing),
        [(server_id, local_type, create_time)] => {
            if *server_id != request.server_id || *create_time != request.created {
                return Err(VoiceReadError::MessageContradictory);
            }
            if (*local_type & 0xffff_ffff) != 34 {
                return Err(VoiceReadError::MessageNotVoice);
            }
            Ok(*create_time)
        }
        _ => Err(VoiceReadError::MessageAmbiguous),
    }
}

/// One narrow `VoiceInfo` row, plus the media-shard `Name2Id` rowid that owns it.
struct VoiceRow {
    rowid: i64,
    local_id: i64,
    create_time: i64,
    size: i64,
}

struct MediaVoiceRows {
    owner_id: i64,
    rows: Vec<VoiceRow>,
}

/// Read a media shard's own chat mapping and the narrow `VoiceInfo` metadata for
/// one server id.
///
/// Returns `Ok(None)` when this shard simply does not carry the chat: a
/// shard that has no `Name2Id` row for the chat is an unrelated shard, not a
/// cross-database inconsistency. A shard whose mapping is ambiguous, whose
/// schema is missing, or whose columns have the wrong SQLite type is refused.
/// No `voice_data` blob is selected here.
fn media_voice_metadata(
    conn: &Connection,
    chat_id: &str,
    server_id: i64,
    cancel: &CollectionCancel,
) -> Result<Option<MediaVoiceRows>, VoiceReadError> {
    check(cancel)?;
    if !table_exists(conn, "Name2Id", cancel)? || !table_exists(conn, "VoiceInfo", cancel)? {
        return Err(VoiceReadError::SchemaUnavailable);
    }
    check(cancel)?;

    let mut statement = conn
        .prepare("SELECT rowid, user_name FROM Name2Id WHERE user_name = ?1 LIMIT 2")
        .map_err(|_| read_failure(cancel))?;
    let mut rows = statement.query([chat_id]).map_err(|_| read_failure(cancel))?;
    let mut owners: Vec<(i64, Option<String>)> = Vec::new();
    while let Some(row) = rows.next().map_err(|_| read_failure(cancel))? {
        check(cancel)?;
        let rowid: i64 = row.get(0).map_err(|error| column_failure(cancel, error))?;
        let user_name: Option<String> =
            row.get(1).map_err(|error| column_failure(cancel, error))?;
        owners.push((rowid, user_name));
    }

    if owners.is_empty() {
        return Ok(None);
    }
    if owners.len() != 1 {
        return Err(VoiceReadError::VoiceAmbiguous);
    }
    let (owner_id, user_name) = owners.pop().expect("exactly one owner");
    if user_name.as_deref() != Some(chat_id) {
        return Err(VoiceReadError::VoiceAmbiguous);
    }
    check(cancel)?;

    let limit = MAX_VOICE_ROWS + 1;
    let sql = format!(
        "SELECT rowid, chat_name_id, local_id, create_time, length(voice_data)
         FROM VoiceInfo WHERE chat_name_id = ?1 AND svr_id = ?2 LIMIT {limit}"
    );
    let mut statement = conn.prepare(&sql).map_err(|_| read_failure(cancel))?;
    let mut rows = statement
        .query(params![owner_id, server_id])
        .map_err(|_| read_failure(cancel))?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|_| read_failure(cancel))? {
        check(cancel)?;
        let rowid: i64 = row.get(0).map_err(|error| column_failure(cancel, error))?;
        let chat_name_id: i64 = row.get(1).map_err(|error| column_failure(cancel, error))?;
        let local_id: i64 = row.get(2).map_err(|error| column_failure(cancel, error))?;
        let create_time: i64 = row.get(3).map_err(|error| column_failure(cancel, error))?;
        let size: Option<i64> = row.get(4).map_err(|error| column_failure(cancel, error))?;
        // The statement already filters on it; a disagreement means the row is
        // not the one this read asked about.
        if chat_name_id != owner_id {
            return Err(VoiceReadError::VoiceContradictory);
        }
        // A row that matches the association key but carries no blob is a
        // contradiction with what the message advertised, not "no audio".
        let size = size.ok_or(VoiceReadError::VoiceChanged)?;
        out.push(VoiceRow {
            rowid,
            local_id,
            create_time,
            size,
        });
        if out.len() > MAX_VOICE_ROWS {
            return Err(VoiceReadError::VoiceAmbiguous);
        }
    }
    check(cancel)?;
    Ok(Some(MediaVoiceRows { owner_id, rows: out }))
}

/// One audio row that matched the message, before its blob is read.
struct VoiceCandidate {
    shard: String,
    owner_id: i64,
    rowid: i64,
    media_local_id: i64,
    size: i64,
}

/// Read one candidate's blob, refusing before the blob is copied when its length
/// is over budget, and re-checking the metadata in the same transaction first.
fn read_verified_blob(
    database_root: &Path,
    key: &str,
    candidate: &VoiceCandidate,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
) -> Result<Vec<u8>, VoiceReadError> {
    if candidate.size > MAX_VOICE_BYTES as i64 {
        return Err(VoiceReadError::VoiceTooLarge);
    }
    let path = database_root.join("message").join(&candidate.shard);
    let mut conn = open_shard_readonly(&path, key, cancel).map_err(from_snapshot)?;
    check(cancel)?;

    let tx = conn.transaction().map_err(|_| read_failure(cancel))?;
    // The metadata the candidate was chosen on is re-checked on the connection
    // that will read the blob, before the blob is selected.
    let recheck = tx
        .query_row(
            "SELECT chat_name_id, svr_id, create_time, length(voice_data)
             FROM VoiceInfo WHERE rowid = ?1",
            [candidate.rowid],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|error| column_failure(cancel, error))?;

    let Some((chat_name_id, svr_id, create_time, size)) = recheck else {
        return Err(VoiceReadError::VoiceChanged);
    };
    if chat_name_id != Some(candidate.owner_id)
        || svr_id != Some(request.server_id)
        || create_time != Some(request.created)
        || size != Some(candidate.size)
    {
        return Err(VoiceReadError::VoiceChanged);
    }
    let size = size.expect("the length was just compared with Some");
    if size > MAX_VOICE_BYTES as i64 {
        return Err(VoiceReadError::VoiceTooLarge);
    }
    check(cancel)?;

    let blob = tx
        .query_row(
            "SELECT voice_data FROM VoiceInfo WHERE rowid = ?1",
            [candidate.rowid],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()
        .map_err(|error| column_failure(cancel, error))?;
    let Some(Some(audio)) = blob else {
        return Err(VoiceReadError::VoiceChanged);
    };
    if audio.len() as i64 != size {
        return Err(VoiceReadError::VoiceChanged);
    }
    Ok(audio)
}

/// The minimum SILK check the Python reader makes: an optional `0x02` prefix,
/// then the `#!SILK_V3` header. Packet boundaries and duration are validated by
/// the Python `validate_silk`, which this module does not duplicate.
fn validate_silk_header(data: &[u8]) -> bool {
    const HEADER: &[u8] = b"#!SILK_V3";
    let offset = if data.starts_with(b"\x02#!SILK_V3") {
        1
    } else {
        0
    };
    data.get(offset..offset + HEADER.len()) == Some(HEADER)
}

/// Re-observe the message anchor after the audio has been read, on a connection
/// that is opened fresh for this check.
///
/// A row that is gone, repeated, or no longer carries the requested
/// `server_id` / `create_time` / voice base type is a changed source identity:
/// the bytes already read must not be attached to it. The source shard itself
/// becoming unresolvable or unreadable is a changed source. Both are fixed
/// refusals — [`VoiceReadError::VoiceChanged`] for the row, and
/// [`VoiceReadError::SourceChanged`] for the shard — so a caller never has to
/// guess which one it got. This deliberately holds no transaction across the two
/// checks, and claims no OS-level atomicity.
fn reverify_message_anchor(
    database_root: &Path,
    key: &str,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
) -> Result<(), VoiceReadError> {
    check(cancel)?;
    let path = message_shard_path(database_root, &request.source_shard)
        .map_err(|_| VoiceReadError::SourceChanged)?;
    let conn = open_shard_readonly(&path, key, cancel).map_err(|error| match error {
        MessageSnapshotError::DeadlineExceeded => VoiceReadError::DeadlineExceeded,
        _ => VoiceReadError::SourceChanged,
    })?;

    match read_message_evidence(&conn, request, cancel) {
        Ok(_) => Ok(()),
        Err(VoiceReadError::DeadlineExceeded) => Err(VoiceReadError::DeadlineExceeded),
        // The row is gone, repeated, or no longer the message that was read.
        Err(VoiceReadError::MessageMissing)
        | Err(VoiceReadError::MessageAmbiguous)
        | Err(VoiceReadError::MessageContradictory)
        | Err(VoiceReadError::MessageNotVoice) => Err(VoiceReadError::VoiceChanged),
        // The shard became unreadable or its schema moved under the read.
        Err(_) => Err(VoiceReadError::SourceChanged),
    }
}

/// Read one voice note under the caller's round.
///
/// `database_root` is the trusted `db_storage` root a read identity resolved; it
/// is never taken from the request. See the module docs for what is and is not
/// verified.
pub fn collect_voice(
    database_root: &Path,
    keys: &HashMap<String, String>,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
) -> Result<VoiceSample, VoiceReadError> {
    collect_voice_with(database_root, keys, request, cancel, &discover_media_shards)
}

/// `discover` is a parameter so a test can make the media shard set change
/// between the first and the last discovery; production passes the directory
/// scan.
fn collect_voice_with(
    database_root: &Path,
    keys: &HashMap<String, String>,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
    discover: &dyn Fn(&Path) -> Result<Vec<String>, VoiceReadError>,
) -> Result<VoiceSample, VoiceReadError> {
    collect_voice_with_hooks(database_root, keys, request, cancel, discover, &|| {})
}

/// The full pipeline with the test-only seam.
///
/// `after_blob` runs once the candidate's bytes have been read and before the
/// source is re-observed. Production passes a no-op; a test uses it to change the
/// fixture at a controlled point, instead of sleeping or racing the clock.
fn collect_voice_with_hooks(
    database_root: &Path,
    keys: &HashMap<String, String>,
    request: &VoiceRequest,
    cancel: &CollectionCancel,
    discover: &dyn Fn(&Path) -> Result<Vec<String>, VoiceReadError>,
    after_blob: &dyn Fn(),
) -> Result<VoiceSample, VoiceReadError> {
    check(cancel)?;
    request.validate()?;

    // The message shard is resolved and checked before any connection is opened,
    // so an unnameable or missing shard is refused without touching a database.
    let message_path = message_shard_path(database_root, &request.source_shard)?;
    let message_key = keys
        .get(&request.source_shard)
        .ok_or(VoiceReadError::KeysUnavailable)?;

    let media_before = discover(database_root)?;

    let message_conn =
        open_shard_readonly(&message_path, message_key, cancel).map_err(from_snapshot)?;
    let create_time = read_message_evidence(&message_conn, request, cancel)?;
    drop(message_conn);

    let mut candidates: Vec<VoiceCandidate> = Vec::new();
    let mut contradictory = false;
    for shard in &media_before {
        check(cancel)?;
        // A shard without a key fails the whole read: skipping it would return an
        // answer that silently ignored a media source.
        let key = keys.get(shard).ok_or(VoiceReadError::KeysUnavailable)?;
        let path = database_root.join("message").join(shard);
        let conn = open_shard_readonly(&path, key, cancel).map_err(from_snapshot)?;
        let Some(metadata) =
            media_voice_metadata(&conn, &request.chat_id, request.server_id, cancel)?
        else {
            continue;
        };
        for row in metadata.rows {
            if row.create_time != create_time {
                // A row on the association key with a different timestamp is a
                // contradiction, not a candidate and not a miss.
                contradictory = true;
            } else {
                candidates.push(VoiceCandidate {
                    shard: shard.clone(),
                    owner_id: metadata.owner_id,
                    rowid: row.rowid,
                    media_local_id: row.local_id,
                    size: row.size,
                });
            }
        }
    }
    check(cancel)?;
    if contradictory {
        return Err(VoiceReadError::VoiceContradictory);
    }
    let candidate = match candidates.len() {
        0 => return Err(VoiceReadError::VoiceSourceMissing),
        1 => candidates.pop().expect("exactly one candidate"),
        _ => return Err(VoiceReadError::VoiceAmbiguous),
    };

    let media_key = keys
        .get(&candidate.shard)
        .ok_or(VoiceReadError::KeysUnavailable)?;
    let audio = read_verified_blob(database_root, media_key, &candidate, request, cancel)?;

    // Test-only: change the fixture now that the bytes are in hand, so the
    // re-observation below has something real to detect. Production is a no-op.
    after_blob();

    // Re-discover before returning: audio read from a shard set that moved is
    // reported as a changed source, not as a complete answer.
    check(cancel)?;
    if discover(database_root)? != media_before {
        return Err(VoiceReadError::SourceChanged);
    }

    // The message anchor is re-observed on a fresh read-only connection. The
    // connection that first verified it is gone on purpose: an open read
    // transaction keeps its own snapshot and would keep reporting the row it saw
    // even after the source changed. This is a second observation, not an atomic
    // snapshot across the message and media databases, and not an OS-level
    // guarantee — a change that happens after this read is simply not seen. Its
    // only job is to refuse the audio already in hand when the message row is no
    // longer the one that was read.
    check(cancel)?;
    reverify_message_anchor(database_root, message_key, request, cancel)?;

    check(cancel)?;
    if !validate_silk_header(&audio) {
        return Err(VoiceReadError::VoiceInvalid);
    }

    let sha256 = {
        let mut hasher = Sha256::new();
        hasher.update(&audio);
        format!("{:x}", hasher.finalize())
    };

    Ok(VoiceSample {
        chat_id: request.chat_id.clone(),
        source_shard: request.source_shard.clone(),
        media_shard: candidate.shard,
        server_id: request.server_id,
        create_time,
        media_local_id: candidate.media_local_id,
        media_rowid: candidate.rowid,
        chat_name_id: candidate.owner_id,
        audio,
        sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::time::Duration;

    use tempfile::TempDir;

    const TEST_KEY: &str =
        "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    const CHAT: &str = "wxid_voice_chat";
    const OTHER_CHAT: &str = "wxid_voice_other";

    fn generous() -> CollectionCancel {
        CollectionCancel::within(Duration::from_secs(300))
    }

    fn keys(names: &[&str]) -> HashMap<String, String> {
        names
            .iter()
            .map(|name| (name.to_string(), TEST_KEY.to_string()))
            .collect()
    }

    fn request(
        chat_id: &str,
        source_shard: &str,
        local_id: i64,
        server_id: i64,
        created: i64,
    ) -> VoiceRequest {
        VoiceRequest {
            chat_id: chat_id.to_string(),
            source_shard: source_shard.to_string(),
            local_id,
            server_id,
            created,
        }
    }

    /// A minimally valid SILK v3 payload: the optional `0x02` prefix followed by
    /// the header the Python reader requires. The trailing bytes stand for
    /// opaque frame data this module does not parse.
    fn silk(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(payload.len() + 10);
        bytes.extend_from_slice(b"\x02#!SILK_V3");
        bytes.extend_from_slice(payload);
        bytes
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    const NAME2ID_DDL: &str = "CREATE TABLE Name2Id (user_name TEXT);";
    const VOICEINFO_DDL: &str = "CREATE TABLE VoiceInfo (
        chat_name_id INTEGER, create_time INTEGER, local_id INTEGER,
        svr_id INTEGER, data_index INTEGER, voice_data BLOB);";

    fn message_table_ddl(chat: &str) -> String {
        format!(
            "CREATE TABLE \"{}\" (local_id INTEGER, server_id INTEGER,
             local_type INTEGER, create_time INTEGER);",
            get_msg_table_name(chat)
        )
    }

    fn insert_message_row(
        conn: &Connection,
        chat: &str,
        local_id: i64,
        server_id: i64,
        local_type: i64,
        create_time: i64,
    ) {
        conn.execute(
            &format!(
                "INSERT INTO \"{}\" (local_id, server_id, local_type, create_time)
                 VALUES (?1, ?2, ?3, ?4)",
                get_msg_table_name(chat)
            ),
            params![local_id, server_id, local_type, create_time],
        )
        .unwrap();
    }

    fn insert_owner(conn: &Connection, chat: &str) -> i64 {
        conn.execute("INSERT INTO Name2Id (user_name) VALUES (?1)", [chat])
            .unwrap();
        conn.last_insert_rowid()
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_voice(
        conn: &Connection,
        owner: i64,
        create_time: i64,
        local_id: i64,
        svr_id: i64,
        data_index: i64,
        data: &[u8],
    ) {
        conn.execute(
            "INSERT INTO VoiceInfo
             (chat_name_id, create_time, local_id, svr_id, data_index, voice_data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![owner, create_time, local_id, svr_id, data_index, data],
        )
        .unwrap();
    }

    /// Reopen the message shard read-write and run `change` on it, so a test can
    /// make the source change after the audio has already been read.
    fn mutate_message_shard(root: &Root, shard: &str, change: impl FnOnce(&Connection)) {
        let path = root.message_dir().join(shard);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
        ))
        .unwrap();
        change(&conn);
        conn.close().unwrap();
    }

    /// A `db_storage` root holding encrypted message and media shards.
    struct Root {
        _dir: TempDir,
        path: PathBuf,
    }

    impl Root {
        fn empty() -> Self {
            let dir = tempfile::tempdir().unwrap();
            Self {
                path: dir.path().to_path_buf(),
                _dir: dir,
            }
        }

        fn message_dir(&self) -> PathBuf {
            self.path.join("message")
        }

        fn write_db(&self, name: &str, build: impl FnOnce(&Connection)) -> PathBuf {
            let path = self.message_dir().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
            ))
            .unwrap();
            build(&conn);
            conn.close().unwrap();
            path
        }

        /// A message shard carrying `rows` in `chat`'s table and a `Name2Id`
        /// mapping for `chat` (rowid 1, a different namespace from the media
        /// shards).
        fn add_message_shard(&self, shard: &str, chat: &str, rows: &[(i64, i64, i64, i64)]) {
            self.write_db(shard, |conn| {
                conn.execute_batch(&message_table_ddl(chat)).unwrap();
                conn.execute_batch(NAME2ID_DDL).unwrap();
                insert_owner(conn, chat);
                for (local_id, server_id, local_type, create_time) in rows {
                    insert_message_row(conn, chat, *local_id, *server_id, *local_type, *create_time);
                }
            });
        }

        /// A media shard with the `Name2Id`/`VoiceInfo` schema. `build` inserts
        /// the rows.
        fn add_media_shard(&self, shard: &str, build: impl FnOnce(&Connection)) {
            self.write_db(shard, |conn| {
                conn.execute_batch(NAME2ID_DDL).unwrap();
                conn.execute_batch(VOICEINFO_DDL).unwrap();
                build(conn);
            });
        }
    }

    /// The happy path: bytes, digest and provenance, with a media `Name2Id`
    /// rowid and a media `local_id` that both differ from the message shard's.
    #[test]
    fn a_voice_read_returns_verified_bytes_and_provenance() {
        let root = Root::empty();
        let audio = silk(b"payload-bytes");
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            insert_owner(conn, "filler-a");
            insert_owner(conn, "filler-b");
            let owner = insert_owner(conn, CHAT);
            assert_eq!(owner, 3, "media Name2Id rowid must differ from the message one");
            // media local_id 999 differs from the message local_id 5; only
            // server_id + create_time may associate the two.
            insert_voice(conn, owner, 1_000, 999, 77, 5, &audio);
        });

        let sample = collect_voice(
            &root.path,
            &keys(&["message_0.db", "media_0.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            &generous(),
        )
        .expect("the voice is readable");

        assert_eq!(sample.chat_id(), CHAT);
        assert_eq!(sample.source_shard(), "message_0.db");
        assert_eq!(sample.media_shard(), "media_0.db");
        assert_eq!(sample.server_id(), 77);
        assert_eq!(sample.create_time(), 1_000);
        assert_eq!(sample.chat_name_id(), 3, "the media namespace rowid is used, not the message one");
        assert_eq!(sample.media_local_id(), 999);
        assert_eq!(sample.audio(), audio.as_slice());
        assert_eq!(sample.sha256(), sha256_hex(&audio));
    }

    /// Two different chats may reuse a `local_id`; only the requested chat's
    /// media row may be returned.
    #[test]
    fn a_media_row_from_another_chat_is_not_returned() {
        let root = Root::empty();
        let wanted = silk(b"wanted");
        let other = silk(b"other");
        root.write_db("message_0.db", |conn| {
            conn.execute_batch(&message_table_ddl(CHAT)).unwrap();
            conn.execute_batch(&message_table_ddl(OTHER_CHAT)).unwrap();
            conn.execute_batch(NAME2ID_DDL).unwrap();
            insert_owner(conn, CHAT);
            insert_owner(conn, OTHER_CHAT);
            insert_message_row(conn, CHAT, 5, 77, 34, 1_000);
            insert_message_row(conn, OTHER_CHAT, 5, 77, 34, 1_000);
        });
        root.add_media_shard("media_0.db", |conn| {
            let owner_a = insert_owner(conn, CHAT);
            let owner_b = insert_owner(conn, OTHER_CHAT);
            insert_voice(conn, owner_a, 1_000, 999, 77, 0, &wanted);
            insert_voice(conn, owner_b, 1_000, 999, 77, 0, &other);
        });

        let sample = collect_voice(
            &root.path,
            &keys(&["message_0.db", "media_0.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            &generous(),
        )
        .expect("the requested chat's voice is readable");
        assert_eq!(sample.audio(), wanted.as_slice());
        assert_eq!(sample.chat_name_id(), 1);
    }

    #[test]
    fn a_message_that_is_not_voice_is_rejected() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 1, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::MessageNotVoice
        );
    }

    #[test]
    fn a_message_row_that_is_absent_is_reported() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 6, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::MessageMissing
        );
    }

    #[test]
    fn a_duplicate_message_row_is_ambiguous() {
        let root = Root::empty();
        root.add_message_shard(
            "message_0.db",
            CHAT,
            &[(5, 77, 34, 1_000), (5, 77, 34, 1_000)],
        );

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::MessageAmbiguous
        );
    }

    #[test]
    fn a_message_row_with_a_wrong_server_id_is_contradictory() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 78, 34, 1_000)]);

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::MessageContradictory
        );
    }

    #[test]
    fn a_message_row_with_a_wrong_create_time_is_contradictory() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 2_000)]);

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::MessageContradictory
        );
    }

    #[test]
    fn a_message_field_with_the_wrong_sql_type_is_row_invalid() {
        let root = Root::empty();
        root.write_db("message_0.db", |conn| {
            conn.execute_batch(&message_table_ddl(CHAT)).unwrap();
            conn.execute(
                &format!(
                    "INSERT INTO \"{}\" (local_id, server_id, local_type, create_time)
                     VALUES (5, 77, 34, 'not-a-time')",
                    get_msg_table_name(CHAT)
                ),
                [],
            )
            .unwrap();
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::RowInvalid
        );
    }

    #[test]
    fn a_message_shard_without_the_chat_schema_is_refused() {
        let root = Root::empty();
        root.write_db("message_0.db", |conn| {
            conn.execute_batch(NAME2ID_DDL).unwrap();
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::SchemaUnavailable
        );
    }

    #[test]
    fn no_media_candidate_is_source_missing() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            // A different server id: this shard has no audio for the message.
            insert_voice(conn, owner, 1_000, 999, 998, 0, &silk(b"x"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceSourceMissing
        );
    }

    #[test]
    fn no_media_shard_at_all_is_source_missing() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceSourceMissing
        );
    }

    #[test]
    fn a_media_shard_without_this_chat_is_skipped() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            insert_owner(conn, OTHER_CHAT);
        });
        root.add_media_shard("media_1.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"second-shard"));
        });

        let sample = collect_voice(
            &root.path,
            &keys(&["message_0.db", "media_0.db", "media_1.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            &generous(),
        )
        .expect("the audio is in the second media shard");
        assert_eq!(sample.media_shard(), "media_1.db");
    }

    #[test]
    fn two_media_candidates_are_ambiguous() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        for shard in ["media_0.db", "media_1.db"] {
            root.add_media_shard(shard, |conn| {
                let owner = insert_owner(conn, CHAT);
                insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
            });
        }

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db", "media_1.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceAmbiguous
        );
    }

    #[test]
    fn a_media_row_with_a_wrong_create_time_is_contradictory() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 2_000, 999, 77, 0, &silk(b"x"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceContradictory
        );
    }

    #[test]
    fn an_ambiguous_media_chat_mapping_is_refused() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            conn.execute(
                "INSERT INTO Name2Id (user_name) VALUES (?1), (?1)",
                [CHAT],
            )
            .unwrap();
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceAmbiguous
        );
    }

    #[test]
    fn a_media_shard_without_the_voice_schema_is_refused() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.write_db("media_0.db", |conn| {
            conn.execute_batch(NAME2ID_DDL).unwrap();
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::SchemaUnavailable
        );
    }

    #[test]
    fn a_missing_message_key_fails_the_whole_read() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);

        assert_eq!(
            collect_voice(
                &root.path,
                &HashMap::new(),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::KeysUnavailable
        );
    }

    #[test]
    fn a_missing_media_key_fails_the_whole_read() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::KeysUnavailable
        );
    }

    #[test]
    fn a_media_shard_that_cannot_be_decrypted_is_a_read_failure() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        // A plaintext database: the stored key cannot decrypt it.
        let path = root.message_dir().join("media_0.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!("{NAME2ID_DDL}{VOICEINFO_DDL}")).unwrap();
        conn.close().unwrap();

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::DatabaseRead
        );
    }

    #[test]
    fn an_oversized_voice_blob_is_refused_from_its_length() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        let oversized = vec![0u8; MAX_VOICE_BYTES + 1];
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &oversized);
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceTooLarge
        );
    }

    #[test]
    fn bytes_that_are_not_silk_are_rejected() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, b"not silk at all");
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceInvalid
        );
    }

    #[test]
    fn malformed_requests_are_refused_before_any_read() {
        let root = Root::empty();
        let empty = HashMap::new();
        let cancel = generous();

        for bad in [
            request("", "message_0.db", 5, 77, 1_000),
            request(CHAT, "media_0.db", 5, 77, 1_000),
            request(CHAT, "message_0.db/../x", 5, 77, 1_000),
            request(CHAT, "../message_0.db", 5, 77, 1_000),
            request(CHAT, "message_abc.db", 5, 77, 1_000),
            request(CHAT, "message_.db", 5, 77, 1_000),
            request(CHAT, "message_4294967296.db", 5, 77, 1_000),
            request(CHAT, "message_0.db", 0, 77, 1_000),
            request(CHAT, "message_0.db", 5, 0, 1_000),
        ] {
            assert_eq!(
                collect_voice(&root.path, &empty, &bad, &cancel).unwrap_err(),
                VoiceReadError::InvalidRequest,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_source_shard_that_is_not_present_is_source_unavailable() {
        let root = Root::empty();
        std::fs::create_dir_all(root.message_dir()).unwrap();

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::SourceUnavailable
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_source_shard_symlink_is_not_followed() {
        use std::os::unix::fs::symlink;

        let root = Root::empty();
        root.add_message_shard("message_9.db", CHAT, &[(5, 77, 34, 1_000)]);
        symlink(
            root.message_dir().join("message_9.db"),
            root.message_dir().join("message_0.db"),
        )
        .unwrap();

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::SourceUnavailable
        );
    }

    #[test]
    fn a_stopped_round_reads_nothing() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
        });
        let cancel = CollectionCancel::within(Duration::from_millis(0));
        cancel.cancel();

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &cancel,
            )
            .unwrap_err(),
            VoiceReadError::DeadlineExceeded
        );
    }

    #[test]
    fn a_media_source_set_that_changes_is_refused() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"x"));
        });

        let seen = Cell::new(0usize);
        let discover = |_: &Path| -> Result<Vec<String>, VoiceReadError> {
            let call = seen.get() + 1;
            seen.set(call);
            Ok(if call == 1 {
                vec!["media_0.db".to_string()]
            } else {
                vec!["media_0.db".to_string(), "media_1.db".to_string()]
            })
        };

        assert_eq!(
            collect_voice_with(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
                &discover,
            )
            .unwrap_err(),
            VoiceReadError::SourceChanged
        );
        assert_eq!(seen.get(), 2, "the media set is discovered before and after");
    }

    /// A media shard whose newest commit is provably still only in its `-wal`
    /// file. The read has to see it, and must not checkpoint it.
    #[test]
    fn a_voice_row_committed_only_to_the_write_ahead_log_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let message_path = root.join("message").join("message_0.db");
        std::fs::create_dir_all(message_path.parent().unwrap()).unwrap();
        let message = Connection::open(&message_path).unwrap();
        message
            .execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                 {chat}{names}",
                chat = message_table_ddl(CHAT),
                names = NAME2ID_DDL
            ))
            .unwrap();
        insert_owner(&message, CHAT);
        insert_message_row(&message, CHAT, 5, 77, 34, 1_000);
        message.close().unwrap();

        let media_path = root.join("message").join("media_0.db");
        let writer = Connection::open(&media_path).unwrap();
        writer
            .execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                 PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                 {names}{voices}",
                names = NAME2ID_DDL,
                voices = VOICEINFO_DDL
            ))
            .unwrap();
        let owner = insert_owner(&writer, CHAT);
        let busy: i64 = writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy, 0, "fixture checkpoint must not be blocked");
        let audio = silk(b"wal-only");
        insert_voice(&writer, owner, 1_000, 999, 77, 0, &audio);

        let wal = std::fs::metadata(media_path.with_extension("db-wal"))
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(wal > 32, "fixture guard: the newest commit must stay in the -wal");
        let main_before = std::fs::metadata(&media_path).unwrap().len();

        let sample = collect_voice(
            &root,
            &keys(&["message_0.db", "media_0.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            &generous(),
        )
        .expect("a row in the -wal is readable");
        assert_eq!(sample.audio(), audio.as_slice());

        assert_eq!(
            std::fs::metadata(&media_path).unwrap().len(),
            main_before,
            "the read must not checkpoint the -wal"
        );
        let mode: String = writer
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal", "the read must not change journal_mode");
    }

    #[test]
    fn debug_does_not_expose_bytes_paths_or_the_chat_id() {
        let root = Root::empty();
        let audio = silk(b"a-recognisable-secret-payload");
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &audio);
        });

        let sample = collect_voice(
            &root.path,
            &keys(&["message_0.db", "media_0.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            &generous(),
        )
        .expect("the voice is readable");

        let debug = format!("{sample:?}");
        assert!(!debug.contains("a-recognisable-secret-payload"), "{debug}");
        assert!(!debug.contains(CHAT), "{debug}");
        assert!(
            !debug.contains(&root.path.to_string_lossy().to_string()),
            "no filesystem path: {debug}"
        );
        assert!(debug.contains(sample.sha256()), "{debug}");
    }

    // ── Re-observing the source after the audio was read ───────────────────

    /// A message/audio pair that reads cleanly, for the tests that change the
    /// source after the audio has been read.
    fn voiced_root() -> Root {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"original-audio"));
        });
        root
    }

    fn read_with(
        root: &Root,
        cancel: &CollectionCancel,
        hook: &dyn Fn(),
    ) -> Result<VoiceSample, VoiceReadError> {
        collect_voice_with_hooks(
            &root.path,
            &keys(&["message_0.db", "media_0.db"]),
            &request(CHAT, "message_0.db", 5, 77, 1_000),
            cancel,
            &discover_media_shards,
            hook,
        )
    }

    #[test]
    fn a_source_message_deleted_after_the_audio_is_read_is_changed() {
        let root = voiced_root();
        let hook = || {
            mutate_message_shard(&root, "message_0.db", |conn| {
                conn.execute(
                    &format!("DELETE FROM \"{}\" WHERE local_id = 5", get_msg_table_name(CHAT)),
                    [],
                )
                .unwrap();
            });
        };

        assert_eq!(
            read_with(&root, &generous(), &hook).unwrap_err(),
            VoiceReadError::VoiceChanged,
            "a message deleted after its audio was read must not be answered"
        );
    }

    #[test]
    fn a_source_message_replaced_with_a_non_voice_type_is_changed() {
        let root = voiced_root();
        let hook = || {
            mutate_message_shard(&root, "message_0.db", |conn| {
                conn.execute(
                    &format!(
                        "UPDATE \"{}\" SET local_type = 1 WHERE local_id = 5",
                        get_msg_table_name(CHAT)
                    ),
                    [],
                )
                .unwrap();
            });
        };

        assert_eq!(
            read_with(&root, &generous(), &hook).unwrap_err(),
            VoiceReadError::VoiceChanged
        );
    }

    #[test]
    fn a_source_message_replaced_with_a_new_server_id_is_changed() {
        let root = voiced_root();
        let hook = || {
            mutate_message_shard(&root, "message_0.db", |conn| {
                conn.execute(
                    &format!(
                        "UPDATE \"{}\" SET server_id = 999 WHERE local_id = 5",
                        get_msg_table_name(CHAT)
                    ),
                    [],
                )
                .unwrap();
            });
        };

        assert_eq!(
            read_with(&root, &generous(), &hook).unwrap_err(),
            VoiceReadError::VoiceChanged,
            "the audio read for the old server id must not attach to the new one"
        );
    }

    #[test]
    fn a_source_shard_deleted_after_the_audio_is_read_is_source_changed() {
        let root = voiced_root();
        let hook = || {
            std::fs::remove_file(root.message_dir().join("message_0.db")).unwrap();
        };

        assert_eq!(
            read_with(&root, &generous(), &hook).unwrap_err(),
            VoiceReadError::SourceChanged
        );
    }

    #[test]
    fn a_round_cancelled_during_the_read_is_reported() {
        let root = voiced_root();
        let cancel = generous();
        let armed = cancel.clone();
        let hook = move || armed.cancel();

        assert_eq!(
            read_with(&root, &cancel, &hook).unwrap_err(),
            VoiceReadError::DeadlineExceeded
        );
    }

    // ── Focused association coverage ────────────────────────────────────────

    #[test]
    fn two_media_rows_in_one_shard_are_ambiguous() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            insert_voice(conn, owner, 1_000, 999, 77, 0, &silk(b"first"));
            insert_voice(conn, owner, 1_000, 998, 77, 1, &silk(b"second"));
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::VoiceAmbiguous
        );
    }

    #[test]
    fn a_media_column_with_the_wrong_sql_type_is_row_invalid() {
        let root = Root::empty();
        root.add_message_shard("message_0.db", CHAT, &[(5, 77, 34, 1_000)]);
        root.add_media_shard("media_0.db", |conn| {
            let owner = insert_owner(conn, CHAT);
            conn.execute(
                "INSERT INTO VoiceInfo
                 (chat_name_id, create_time, local_id, svr_id, data_index, voice_data)
                 VALUES (?1, 'not-a-time', 999, 77, 0, ?2)",
                params![owner, silk(b"x")],
            )
            .unwrap();
        });

        assert_eq!(
            collect_voice(
                &root.path,
                &keys(&["message_0.db", "media_0.db"]),
                &request(CHAT, "message_0.db", 5, 77, 1_000),
                &generous(),
            )
            .unwrap_err(),
            VoiceReadError::RowInvalid
        );
    }

    #[test]
    fn an_unrepresentable_media_shard_name_is_source_unavailable() {
        let root = Root::empty();
        std::fs::create_dir_all(root.message_dir()).unwrap();
        std::fs::write(root.message_dir().join("media_0.db"), b"x").unwrap();
        std::fs::write(root.message_dir().join("media_4294967295.db"), b"x").unwrap();

        assert_eq!(
            discover_media_shards(&root.path).unwrap(),
            vec!["media_0.db".to_string(), "media_4294967295.db".to_string()],
            "the largest representable shard number is still discovered"
        );

        std::fs::write(root.message_dir().join("media_4294967296.db"), b"x").unwrap();
        assert_eq!(
            discover_media_shards(&root.path).unwrap_err(),
            VoiceReadError::SourceUnavailable,
            "a name that looks like a shard but cannot be represented is refused, not skipped"
        );
    }

    #[test]
    fn error_codes_are_stable_and_carry_no_content() {
        let sample_error = VoiceReadError::VoiceSourceMissing;
        assert_eq!(sample_error.to_string(), "WECHAT_VOICE_SOURCE_MISSING");
        assert_eq!(
            VoiceReadError::DeadlineExceeded.code(),
            "WECHAT_VOICE_DEADLINE_EXCEEDED"
        );
        assert_eq!(
            format!("{:?}", VoiceReadError::KeysUnavailable),
            "KeysUnavailable"
        );
    }
}
