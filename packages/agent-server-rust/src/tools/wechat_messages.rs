use super::wechat_db::{get_db_path, numbered_db_index, try_query_wechat_db, value_ref_to_json, WechatReadError};
use super::wechat_snapshot;
use crate::ia::types::{Message, MessageV2, ReferenceV2, ReplyInfo, XmlParseStatus};
use super::message_record::{
    extract_reference_v2 as extract_reference_fields, project_message_record,
    unparsed_message_record,
};
use md5::{Digest, Md5};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// ZSTD magic number (little-endian): 0xFD2FB528
const ZSTD_MAGIC: &str = "28b52ffd";

/// Get the Msg table name for a given chat username.
/// WeChat uses MD5(username) as the table suffix.
pub fn get_msg_table_name(chat_id: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(chat_id.as_bytes());
    let hash = hasher.finalize();
    format!("Msg_{:x}", hash)
}

/// Split WeChat's packed signed SQLite integer without first narrowing it.
pub fn split_message_type(raw: i64) -> (u32, u32) {
    let bits = raw as u64;
    (bits as u32, (bits >> 32) as u32)
}

/// Project an existing frozen row into the lossless V2 wire contract.
pub fn message_v2_from_snapshot(row: &Value) -> Option<MessageV2> {
    let raw_type = row.get("rawType")?.as_i64()?;
    let (msg_type, sub_type) = split_message_type(raw_type);
    let server_id = row.get("serverId").and_then(Value::as_i64).filter(|id| *id != 0).map(|id| id.to_string());
    let local_id = row.get("localId").and_then(Value::as_i64).filter(|id| *id != 0).map(|id| id.to_string());
    let chat_username = row.get("chatId")?.as_str()?.to_string();
    let raw_content = row.get("rawContent").and_then(Value::as_str).map(str::to_string);
    let raw_content_compression = row.get("rawContentCompression")?.as_bool()?;
    let compressed = raw_content_compression;
    let records = match raw_content.as_deref() {
        Some(hex) => match decode_stored_content_for_record(hex, compressed) {
            Ok(body) => {
                let body = if chat_username.contains("@chatroom") {
                    extract_group_sender(&body).1
                } else {
                    body
                };
                project_message_record(&body, &chat_username, msg_type, sub_type)
            }
            Err(RecordBodyError::Truncated) => {
                unparsed_message_record(msg_type, sub_type, XmlParseStatus::Truncated)
            }
            Err(RecordBodyError::Invalid) => {
                unparsed_message_record(msg_type, sub_type, XmlParseStatus::Invalid)
            }
        },
        None => unparsed_message_record(msg_type, sub_type, XmlParseStatus::Invalid),
    };
    Some(MessageV2 {
        server_id,
        local_id,
        raw_type: raw_type.to_string(),
        r#type: msg_type,
        sub_type,
        chat_username,
        sender_username: row.get("sender").and_then(Value::as_str).map(str::to_string),
        sender_display_name: row.get("senderName").and_then(Value::as_str).map(str::to_string),
        content: row.get("content")?.as_str()?.to_string(),
        timestamp: row.get("timestamp").and_then(Value::as_str).filter(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()).map(str::to_string),
        is_self: row.get("isSelf").and_then(Value::as_bool),
        is_mentioned: row.get("isMentioned").and_then(Value::as_bool),
        raw_content,
        raw_content_compression,
        xml_sub_type: records.xml_sub_type,
        reference: records.reference,
        forwarded_record: records.forwarded_record,
    })
}

/// Extract a reference's identity using the context the record was read from.
/// The old display-only `ReplyInfo` extractor remains intentionally unchanged.
pub fn extract_reference_v2(body: &str, chat_username: &str) -> Option<ReferenceV2> {
    extract_reference_fields(body, chat_username)
}

#[derive(Debug, Clone)]
pub struct MessageSourceAnchor {
    pub message: MessageV2,
    pub source_shard: String,
    pub table_name: String,
    pub local_id: i64,
    pub server_id: i64,
    pub created: i64,
    pub sort_seq: Option<String>,
    pub local_type: i64,
    pub raw_content: String,
    pub raw_source_hex: String,
    pub raw_source_compressed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageLocateError {
    InvalidRequest,
    NotFound,
    Ambiguous,
    ChatNameUnavailable,
    Snapshot(MessageSnapshotError),
}

/// Find one exact native server ID in the current account's isolated shard set.
/// This is an indexed lookup per validated Msg table, not a full history scan.
pub fn locate_message_by_server_id(
    database_root: &Path,
    account_dir: &str,
    keys: &HashMap<String, String>,
    server_id: i64,
    chat_username: Option<&str>,
    known_chat_usernames: &[String],
    cancel: &CollectionCancel,
) -> Result<MessageSourceAnchor, MessageLocateError> {
    if server_id == 0 || chat_username.is_some_and(|chat| chat.trim().is_empty()) {
        return Err(MessageLocateError::InvalidRequest);
    }
    cancel.check().map_err(MessageLocateError::Snapshot)?;
    let shard_names = discover_message_shards(database_root).map_err(MessageLocateError::Snapshot)?;
    let shards = resolve_message_shards(database_root, &shard_names, keys).map_err(MessageLocateError::Snapshot)?;
    let target_table = chat_username.map(get_msg_table_name);
    let mut found: Option<(String, String, Value)> = None;

    for shard in &shards {
        cancel.check().map_err(MessageLocateError::Snapshot)?;
        let conn = open_shard_readonly(&shard.path, &shard.key, cancel).map_err(MessageLocateError::Snapshot)?;
        let tables = if let Some(table) = target_table.as_ref() {
            if shard_has_chat_table(&conn, table, cancel).map_err(MessageLocateError::Snapshot)? { vec![table.clone()] } else { Vec::new() }
        } else {
            let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'Msg_[0-9a-f]*'")
                .map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let mut rows = statement.query([]).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let mut names = Vec::new();
            while let Some(row) = rows.next().map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))? {
                let name: String = row.get(0).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
                if valid_msg_table_name(&name) { names.push(name); }
            }
            names
        };

        for table in tables {
            cancel.check().map_err(MessageLocateError::Snapshot)?;
            // The table name comes from the MD5 helper or validated sqlite_master data.
            let sql = format!(
                "SELECT m.local_id, m.server_id, m.local_type, m.create_time, CAST(m.sort_seq AS TEXT) AS sort_seq,
                        hex(m.message_content) AS hex_content, m.WCDB_CT_message_content AS is_compressed,
                        hex(m.source) AS hex_source, m.WCDB_CT_source AS source_compressed,
                        n.user_name AS sender_name
                 FROM \"{table}\" m LEFT JOIN Name2Id n ON m.real_sender_id=n.rowid
                 WHERE m.server_id=?1 LIMIT 2"
            );
            let mut statement = conn.prepare(&sql).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let columns: Vec<String> = statement.column_names().iter().map(|name| name.to_string()).collect();
            let mut rows = statement.query([server_id]).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            while let Some(row) = rows.next().map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))? {
                cancel.check().map_err(MessageLocateError::Snapshot)?;
                let value = row_to_value(row, &columns).map_err(MessageLocateError::Snapshot)?;
                if found.is_some() { return Err(MessageLocateError::Ambiguous); }
                found = Some((shard.name.clone(), table.clone(), value));
            }
        }
    }
    let (source_shard, table_name, row) = found.ok_or(MessageLocateError::NotFound)?;
    let chat_username = if let Some(chat) = chat_username {
        chat.to_string()
    } else {
        let candidates: Vec<_> = known_chat_usernames.iter().filter(|chat| get_msg_table_name(chat) == table_name).collect();
        if candidates.len() != 1 { return Err(MessageLocateError::ChatNameUnavailable); }
        candidates[0].clone()
    };
    let source_index = message_shard_index(&source_shard).ok_or(MessageLocateError::Snapshot(MessageSnapshotError::SourceUnavailable))?;
    let contact_names = HashMap::new();
    let context = RowContext {
        chat_id: &chat_username,
        account_dir,
        contact_names: &contact_names,
        require_create_time: true,
        bounded_decode: true,
    };
    let decoded = decode_message_row(&row, context).map_err(|error| MessageLocateError::Snapshot(error.into()))?;
    let local_id = decoded.message.local_id;
    if local_id == 0 { return Err(MessageLocateError::InvalidRequest); }
    let server_id_value = row.get("server_id").and_then(Value::as_i64).ok_or(MessageLocateError::InvalidRequest)?;
    let created = decoded.create_time.ok_or(MessageLocateError::InvalidRequest)?;
    let shard = MessageShard { name: source_shard.clone(), path: database_root.join("message").join(&source_shard), key: String::new(), index: source_index };
    let mut sample = sample_value(&decoded, &shard).map_err(MessageLocateError::Snapshot)?;
    if let Some(sender) = decoded.message.sender.as_ref() {
        let names = resolve_contact_names(database_root, keys, &HashSet::from([sender.clone()]), cancel)
            .map_err(MessageLocateError::Snapshot)?;
        apply_contact_names(std::slice::from_mut(&mut sample), &names, &SampleBudget::default(), cancel)
            .map_err(MessageLocateError::Snapshot)?;
    }
    let message = message_v2_from_snapshot(&sample).ok_or(MessageLocateError::InvalidRequest)?;
    let raw_source_hex = row.get("hex_content").and_then(Value::as_str).unwrap_or_default().to_string();
    let raw_source_compressed = row.get("is_compressed").and_then(Value::as_i64).unwrap_or_default() != 0;
    Ok(MessageSourceAnchor {
        message,
        source_shard,
        table_name,
        local_id,
        server_id: server_id_value,
        created,
        sort_seq: row.get("sort_seq").and_then(Value::as_str).map(str::to_string),
        local_type: decoded.raw_type,
        raw_content: decoded.raw_content,
        raw_source_hex,
        raw_source_compressed,
    })
}

fn valid_msg_table_name(name: &str) -> bool {
    name.strip_prefix("Msg_").is_some_and(|suffix| suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
}

/// Decompressed bytes one stored blob may expand into on the sample path (4 MiB).
///
/// A stored blob is capped at [`SNAPSHOT_ROW_CONTENT_LIMIT`] before it is read,
/// which bounds what the *database* holds, not what the decoder produces: a
/// megabyte of zstd can expand into orders of magnitude more. The sample answers
/// from a byte budget, so its decode is bounded too. The legacy list keeps
/// [`decode_message_content`]'s unbounded behaviour — it is a display path with
/// no budget behind it, and changing it would change what the list has always
/// returned.
const SNAPSHOT_DECODE_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Window log the sample's decoder accepts (8 MiB windows). The default accepts
/// 128 MiB, which is itself more than the whole output budget.
const SNAPSHOT_DECODE_WINDOW_LOG: u32 = 23;

/// The placeholder a bounded decode reports instead of a body it will not build.
const DECODE_TOO_LARGE: &str = "[compressed content - exceeds the read budget]";

/// Decode stored content, bounding the decompression when the caller needs it.
fn decode_stored_content(hex: &str, is_compressed: bool, bounded: bool) -> String {
    decode_stored_content_with_limit(hex, is_compressed, bounded, SNAPSHOT_DECODE_MAX_BYTES)
}

fn decode_stored_content_with_limit(
    hex: &str,
    is_compressed: bool,
    bounded: bool,
    max_bytes: usize,
) -> String {
    if !bounded {
        return decode_message_content(hex, is_compressed);
    }
    if hex.is_empty() {
        return String::new();
    }
    let Some(bytes) = hex_decode(hex) else {
        return String::new();
    };
    if !(is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC)) {
        // A plain blob is already bounded by the stored length the sample
        // statement checked.
        return String::from_utf8_lossy(&bytes).to_string();
    }
    match decode_zstd_bounded(&bytes, max_bytes) {
        Ok(decoded) => String::from_utf8_lossy(&decoded).to_string(),
        Err(_) => DECODE_TOO_LARGE.to_string(),
    }
}

/// Decompress one zstd body under an output cap, or fail.
///
/// A frame whose declared content size is already above the cap is refused
/// before any window is allocated. For a frame that does not declare one, the
/// decoder reads at most `cap + 1` bytes, so a frame that lies about its size
/// still cannot make this allocate without limit — the cap is what stops it, not
/// the declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedDecodeError {
    Invalid,
    TooLarge,
}

fn decode_zstd_bounded(bytes: &[u8], max_bytes: usize) -> Result<Vec<u8>, BoundedDecodeError> {
    use std::io::Read;
    let mut decoder = zstd::stream::read::Decoder::new(bytes).map_err(|_| BoundedDecodeError::Invalid)?;
    // A frame that asks for a window above the cap is refused by the decoder
    // itself, before any window is allocated for it.
    decoder
        .window_log_max(SNAPSHOT_DECODE_WINDOW_LOG)
        .map_err(|_| BoundedDecodeError::TooLarge)?;
    let mut out = Vec::new();
    // One byte past the cap: reaching it proves the output is over the cap. The
    // cap is what bounds this, not the frame's own declared size.
    let mut limited = (&mut decoder).take(max_bytes as u64 + 1);
    limited.read_to_end(&mut out).map_err(|_| BoundedDecodeError::Invalid)?;
    if out.len() > max_bytes {
        return Err(BoundedDecodeError::TooLarge);
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordBodyError {
    Invalid,
    Truncated,
}

fn decode_stored_content_for_record(
    hex: &str,
    is_compressed: bool,
) -> Result<String, RecordBodyError> {
    if hex.is_empty() {
        return Ok(String::new());
    }
    let bytes = hex_decode(hex).ok_or(RecordBodyError::Invalid)?;
    if is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC) {
        let decoded = decode_zstd_bounded(&bytes, SNAPSHOT_DECODE_MAX_BYTES).map_err(|error| match error {
            BoundedDecodeError::Invalid => RecordBodyError::Invalid,
            BoundedDecodeError::TooLarge => RecordBodyError::Truncated,
        })?;
        String::from_utf8(decoded).map_err(|_| RecordBodyError::Invalid)
    } else {
        String::from_utf8(bytes).map_err(|_| RecordBodyError::Invalid)
    }
}

/// Decode hex-encoded message content, decompressing zstd if needed.
///
/// This is the display decode the message list has always used, and it is lossy
/// on purpose: invalid UTF-8 becomes replacement characters, an odd-length hex
/// string becomes an empty string, and a zstd body that cannot be decompressed
/// becomes a placeholder. The fixed sample therefore also carries the stored
/// bytes as `rawContent`, so a reader that needs the original text does not have
/// to trust this string.
pub fn decode_message_content(hex: &str, is_compressed: bool) -> String {
    if hex.is_empty() {
        return String::new();
    }
    let bytes = match hex_decode(hex) {
        Some(b) => b,
        None => return String::new(),
    };
    if is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC) {
        match zstd::decode_all(bytes.as_slice()) {
            Ok(decompressed) => String::from_utf8_lossy(&decompressed).to_string(),
            Err(_) => "[compressed content - decompression failed]".to_string(),
        }
    } else {
        String::from_utf8_lossy(&bytes).to_string()
    }
}

/// Decode a hex string to bytes.
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}

/// Extract sender from group message content.
/// Group messages have format "sender_wxid:\nmessage_content".
fn extract_group_sender(content: &str) -> (Option<String>, String) {
    if let Some(idx) = content.find(":\n") {
        if idx < 80 {
            let sender = content[..idx].to_string();
            let msg = content[idx + 2..].to_string();
            return (Some(sender), msg);
        }
    }
    (None, content.to_string())
}

/// Clean message content for display based on message type.
/// Replaces verbose XML with concise summaries.
fn clean_content(content: &str, msg_type: i32) -> String {
    let base = msg_type & 0x7FFFFFFF;
    match base {
        // Image (type 3): replace XML with empty string
        3 if content.contains("<img") => String::new(),
        // Emoji (type 47): show cdnurl or [emoji]
        47 if content.contains("<emoji") => {
            extract_xml_attr(content, "cdnurl")
                .filter(|u| u.starts_with("http"))
                .unwrap_or_else(|| "[emoji]".to_string())
        }
        // Appmsg (type 49): handle subtypes
        49 if content.contains("<msg>") => {
            let title = extract_xml_tag(content, "title").unwrap_or_default();
            let appmsg_type = extract_xml_tag(content, "type")
                .and_then(|t| t.parse::<i32>().ok())
                .unwrap_or(0);
            match appmsg_type {
                // Link share (5), video link (4), music share (3)
                3 | 4 | 5 => {
                    let mut parts = Vec::new();
                    parts.push(format!("[Link] {title}"));
                    if let Some(des) = extract_xml_tag(content, "des") {
                        parts.push(des);
                    }
                    if let Some(url) = extract_xml_tag(content, "url") {
                        let url = url.replace("&amp;", "&");
                        parts.push(url);
                    }
                    parts.join("\n")
                }
                _ => {
                    if title.is_empty() {
                        content.to_string()
                    } else {
                        title
                    }
                }
            }
        }
        _ => content.to_string(),
    }
}

/// Extract reply info from type 49 (appmsg) messages with <refermsg>.
fn extract_reply_info(content: &str, msg_type: i32) -> Option<ReplyInfo> {
    let base = msg_type & 0x7FFFFFFF;
    if base != 49 || !content.contains("<refermsg>") {
        return None;
    }
    // Extract the refermsg block
    let rm_start = content.find("<refermsg>")?;
    let rm_end = content.find("</refermsg>")? + "</refermsg>".len();
    let refermsg = &content[rm_start..rm_end];

    let sender = extract_xml_tag(refermsg, "displayname");
    let ref_content = extract_xml_tag(refermsg, "content").unwrap_or_default();

    // The referred content may be XML-escaped — unescape first
    let unescaped = ref_content
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"");

    // The referred content may itself be XML — clean it to a short text
    let clean = if unescaped.contains("<msg>") {
        extract_xml_tag(&unescaped, "title").unwrap_or(unescaped)
    } else {
        unescaped
    };
    Some(ReplyInfo {
        sender,
        content: clean,
    })
}

/// Extract an XML attribute value: attr="value"
fn extract_xml_attr(xml: &str, attr: &str) -> Option<String> {
    let pattern = format!("{attr}=\"");
    let start = xml.find(&pattern)? + pattern.len();
    let end = xml[start..].find('"')? + start;
    let val = xml[start..end].trim().to_string();
    if val.is_empty() { None } else { Some(val) }
}

/// Extract text between XML tags: <tag>text</tag>
/// Also handles CDATA: <tag><![CDATA[text]]></tag>
pub(crate) fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    let mut val = xml[start..end].trim().to_string();
    // Strip CDATA wrapper if present
    if val.starts_with("<![CDATA[") && val.ends_with("]]>") {
        val = val[9..val.len() - 3].to_string();
    }
    if val.is_empty() { None } else { Some(val) }
}

/// Check if the source XML indicates the current user is @-mentioned.
fn check_is_mentioned(source: &str, account_dir: &str) -> bool {
    if let Some(at_list) = extract_xml_tag(source, "atuserlist") {
        // atuserlist may contain comma-separated wxids
        for wxid in at_list.split(',') {
            let wxid = wxid.trim();
            if !wxid.is_empty() && account_dir.starts_with(wxid) {
                return true;
            }
        }
    }
    false
}

/// Find which message DB contains a chat and return (db_name, key).
pub fn find_message_db<'a>(
    account_dir: &str,
    keys: &'a HashMap<String, String>,
    chat_id: &str,
) -> Option<(String, &'a str)> {
    // Media callers retain their optional-result contract until migrated.
    try_find_message_db(account_dir, keys, chat_id).unwrap_or_else(|error| {
        tracing::warn!(code = error.code(), "WeChat media database lookup failed");
        None
    })
}

pub fn is_message_db(name: &str) -> bool {
    name.starts_with("message_") && name.ends_with(".db")
        && !name.contains("fts") && !name.contains("resource")
}

pub fn try_find_message_db<'a>(
    account_dir: &str,
    keys: &'a HashMap<String, String>,
    chat_id: &str,
) -> Result<Option<(String, &'a str)>, WechatReadError> {
    let table_name = get_msg_table_name(chat_id);
    let mut message_dbs: Vec<(&str, &str)> = keys
        .iter()
        .filter(|(k, _)| is_message_db(k))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    message_dbs.sort_by_key(|(k, _)| k.to_string());

    if message_dbs.is_empty() {
        return Err(WechatReadError::KeysUnavailable);
    }
    let paths: Vec<_> = message_dbs.into_iter()
        .map(|(name, key)| (name, key, get_db_path(account_dir, name)))
        .collect();
    find_message_db_from_paths(&paths, &table_name)
}

fn find_message_db_from_paths<'a>(
    databases: &[(&str, &'a str, String)],
    table_name: &str,
) -> Result<Option<(String, &'a str)>, WechatReadError> {
    for (db_name, key, db_path) in databases {
        let check = try_query_wechat_db(
            db_path,
            key,
            &format!(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            ),
        )?;
        if !check.is_empty() {
            return Ok(Some((db_name.to_string(), *key)));
        }
    }
    Ok(None)
}

/// List messages for a specific chat.
///
/// Messages may be spread across message_0.db, message_1.db, etc.
/// Each chat's messages are in a `Msg_{MD5(username)}` table.
pub fn list_messages(
    account_dir: &str,
    keys: &HashMap<String, String>,
    chat_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Message>, WechatReadError> {
    let table_name = get_msg_table_name(chat_id);

    let (db_name, key) = match try_find_message_db(account_dir, keys, chat_id)? {
        Some(dk) => dk,
        None => return Ok(Vec::new()),
    };
    let db_path = get_db_path(account_dir, &db_name);

    // Query messages using hex() for safe binary/compressed content extraction
    let rows = try_query_wechat_db(
        &db_path,
        key,
        &format!(
            "SELECT m.local_id, m.server_id, m.local_type, m.create_time,
                    hex(m.message_content) as hex_content,
                    m.WCDB_CT_message_content as is_compressed,
                    hex(m.source) as hex_source,
                    m.WCDB_CT_source as source_compressed,
                    n.user_name as sender_name
             FROM \"{table_name}\" m
             LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
             ORDER BY m.create_time DESC
             LIMIT {limit} OFFSET {offset};"
        ),
    )?;

    // Resolve sender display names from contact.db
    let contact_names: HashMap<String, String> = {
        let mut map = HashMap::new();
        if let Some(contact_key) = keys.get("contact.db") {
            // Collect unique sender wxids
            let senders: Vec<String> = rows.iter()
                .filter_map(|row| {
                    row.get("sender_name")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            if !senders.is_empty() {
                let contact_db = get_db_path(account_dir, "contact.db");
                let placeholders = senders.iter().map(|s| format!("'{}'", s.replace('\'', "''"))).collect::<Vec<_>>().join(",");
                let contacts = try_query_wechat_db(
                    &contact_db,
                    contact_key,
                    &format!("SELECT username, nick_name, remark FROM contact WHERE username IN ({placeholders});"),
                )?;
                for c in contacts {
                    if let Some(username) = c.get("username").and_then(|v| v.as_str()) {
                        let name = c.get("remark").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                            .or_else(|| c.get("nick_name").and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
                            .unwrap_or(username);
                        map.insert(username.to_string(), name.to_string());
                    }
                }
            }
        }
        map
    };

    let context = RowContext {
        chat_id,
        account_dir,
        contact_names: &contact_names,
        // Unchanged behaviour: a row without a usable `create_time` still
        // produces a message with an empty timestamp, and a row that cannot be
        // decoded at all is skipped, exactly as before.
        require_create_time: false,
        // The list is a display path with no budget behind it, so its decode
        // stays what it has always been.
        bounded_decode: false,
    };

    Ok(rows
        .iter()
        .filter_map(|row| decode_message_row(row, context).ok().map(|decoded| decoded.message))
        .collect())
}

// ── Row decoding ───────────────────────────────────────────────────────────

/// The inputs both message readers decode a row with, so the legacy list and
/// the fixed sample cannot drift apart.
#[derive(Clone, Copy)]
struct RowContext<'a> {
    chat_id: &'a str,
    /// The WeChat account directory name. It is only used by the `isSelf`
    /// heuristic below, which compares name prefixes; it is not a verified
    /// account identity and says nothing about who the session belongs to.
    account_dir: &'a str,
    contact_names: &'a HashMap<String, String>,
    /// The sample requires a decodable `create_time`, because the sample's order
    /// is defined by it. The legacy list tolerates its absence.
    require_create_time: bool,
    /// Decode compressed content under a bound. The sample sets it: a stored
    /// blob is capped before it is read, but its decompressed size is not, so
    /// the sample — which answers from a fixed budget — must not let one row
    /// allocate whatever the stored bytes expand into.
    bounded_decode: bool,
}

impl RowContext<'_> {
    fn is_group(&self) -> bool {
        self.chat_id.contains("@chatroom")
    }
}

/// Why a row cannot become a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowError {
    /// A field the sample needs for identity or ordering is missing or is not an
    /// integer, or a stored blob is above the per-row limit.
    Invalid,
    /// A stored blob is above [`SNAPSHOT_ROW_CONTENT_LIMIT`].
    TooLarge,
}

impl From<RowError> for MessageSnapshotError {
    fn from(error: RowError) -> Self {
        match error {
            RowError::Invalid => MessageSnapshotError::RowInvalid,
            RowError::TooLarge => MessageSnapshotError::RowTooLarge,
        }
    }
}

/// A decoded row: the message the legacy list returns, plus the stored values
/// the display fields cannot carry.
#[derive(Debug)]
struct DecodedRow {
    message: Message,
    create_time: Option<i64>,
    /// `local_type` before the 64 → 32 bit truncation of `Message.msg_type`.
    raw_type: i64,
    /// The stored `message_content` bytes, hex encoded.
    raw_content: String,
    raw_content_compressed: bool,
}

/// Reject a row whose stored blob is above the per-row limit.
///
/// The sample statement only transfers blobs up to the limit, so an oversized
/// row arrives with its length but without content; the length column is what
/// separates "too large" from "empty". Nothing is truncated, the row is refused.
/// The legacy list has no length column and is unaffected.
fn check_row_blob_length(row: &Value, column: &str) -> Result<(), RowError> {
    match row.get(column).and_then(Value::as_i64) {
        Some(length) if length > SNAPSHOT_ROW_CONTENT_LIMIT as i64 => Err(RowError::TooLarge),
        _ => Ok(()),
    }
}

/// Decode one message row.
///
/// This is the single decode both readers use. The legacy list keeps its
/// tolerant behaviour by ignoring failures, while the sample turns them into a
/// fixed error: a row that cannot be decoded is never dropped silently.
fn decode_message_row(row: &Value, context: RowContext<'_>) -> Result<DecodedRow, RowError> {
    check_row_blob_length(row, "content_len")?;
    check_row_blob_length(row, "source_len")?;

    // The sample's order and its duplicate check both rest on these two fields,
    // so a row without a usable value is refused rather than skipped.
    let local_id = row.get("local_id").and_then(Value::as_i64).ok_or(RowError::Invalid)?;
    let create_time = row.get("create_time").and_then(Value::as_i64);
    if context.require_create_time && create_time.is_none() {
        return Err(RowError::Invalid);
    }

    let server_id = row.get("server_id").and_then(Value::as_i64).unwrap_or(0);
    let raw_type = row.get("local_type").and_then(Value::as_i64).unwrap_or(0);
    let msg_type = raw_type as i32;

    let hex_content = row.get("hex_content").and_then(Value::as_str).unwrap_or("");
    let is_compressed = row.get("is_compressed").and_then(Value::as_i64).unwrap_or(0) != 0;
    let raw_content = decode_stored_content(hex_content, is_compressed, context.bounded_decode);

    // Get sender from Name2Id join (works for both group and 1:1 chats)
    let sender = row
        .get("sender_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // Strip group sender prefix from content ("wxid:\ncontent" format)
    let body = if context.is_group() {
        extract_group_sender(&raw_content).1
    } else {
        raw_content
    };

    // Extract reply info before cleaning (needs raw XML)
    let reply = extract_reply_info(&body, msg_type);

    // Clean content for display (replace XML with summaries)
    let content = clean_content(&body, msg_type);

    let timestamp = create_time
        .map(|time| {
            chrono::DateTime::from_timestamp(time, 0)
                .map(|date| date.to_rfc3339())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    // Check @-mention from source XML (only for group chats)
    let is_mentioned = if context.is_group() {
        let hex_source = row.get("hex_source").and_then(Value::as_str).unwrap_or("");
        let source_compressed = row.get("source_compressed").and_then(Value::as_i64).unwrap_or(0) != 0;
        let from_source = if !hex_source.is_empty() {
            let source_xml = decode_stored_content(hex_source, source_compressed, context.bounded_decode);
            check_is_mentioned(&source_xml, context.account_dir)
        } else {
            false
        };

        // For type-49 (appmsg) messages — especially reference/quote messages —
        // WeChat may place <atuserlist> inside the content XML instead of source.
        let from_content = if !from_source && (msg_type & 0x7FFFFFFF) == 49 {
            check_is_mentioned(&body, context.account_dir)
        } else {
            false
        };

        if from_source || from_content {
            Some(true)
        } else {
            None
        }
    } else {
        None
    };

    // Check if message was sent by the logged-in user. This compares the
    // account directory name with the sender name and is only a prefix
    // heuristic, not proof of which account is logged in.
    let is_self = sender.as_ref().map(|s| context.account_dir.starts_with(s.as_str()));

    let sender_name = sender
        .as_ref()
        .and_then(|wxid| context.contact_names.get(wxid))
        .cloned();

    Ok(DecodedRow {
        message: Message {
            local_id,
            server_id,
            chat_id: context.chat_id.to_string(),
            sender,
            sender_name,
            msg_type,
            content,
            timestamp,
            is_mentioned,
            is_self,
            reply,
        },
        create_time,
        raw_type,
        raw_content: hex_content.to_string(),
        raw_content_compressed: is_compressed,
    })
}


// ── Fixed cross-shard message sample ───────────────────────────────────────

/// Rows one sample may contain before it is refused.
const SNAPSHOT_MAX_ROWS: usize = 50_000;
/// Serialized JSON bytes one sample may contain before it is refused.
const SNAPSHOT_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Largest stored `message_content` or `source` blob one row may carry (1 MiB).
const SNAPSHOT_ROW_CONTENT_LIMIT: usize = 1024 * 1024;
/// Wall clock one whole collection may take, discovery and contact names
/// included.
///
/// A round that cannot answer in this time answers "later" instead: the caller
/// keeps its own answerable work — the guard, the identity capture, the rows it
/// has already frozen — instead of being pinned behind a single shard that is
/// slow or locked. The message list's own budget is longer, so the paginated
/// read is the tighter of the two, not the looser.
pub const COLLECTION_BUDGET: Duration = Duration::from_secs(8);
/// VDBE steps between two deadline checks inside one statement.
const COLLECTION_PROGRESS_STEPS: u32 = 1_000;

/// The deadline of one collection, shared with the work it is collecting.
///
/// Shared because the collection runs on a blocking thread that outlives the
/// caller's patience: when the caller stops waiting it sets the flag, and the
/// thread sees it at its next check instead of running to whatever its own idea
/// of the budget was. Cloning shares one deadline, so every clone stops at the
/// same moment.
#[derive(Clone)]
pub struct CollectionCancel {
    inner: Arc<CollectionState>,
}

struct CollectionState {
    at: Instant,
    cancelled: AtomicBool,
    authentication_failure: std::sync::Mutex<Option<String>>,
}

impl CollectionCancel {
    /// A deadline `budget` from now.
    pub fn within(budget: Duration) -> Self {
        Self {
            inner: Arc::new(CollectionState {
                at: Instant::now() + budget,
                cancelled: AtomicBool::new(false),
                authentication_failure: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Whether the collection has to stop: its own deadline passed, or nobody is
    /// waiting for it any more.
    pub fn expired(&self) -> bool {
        self.inner.cancelled.load(Ordering::Relaxed) || self.now() >= self.inner.at
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    /// Time left, or zero once expired. Used to shorten the wait for a lock, so
    /// a collision with WeChat's writer cannot outlive the deadline: a caller that
    /// stopped waiting is zero time left, not the time the deadline still had.
    pub fn remaining(&self) -> Duration {
        if self.expired() {
            return Duration::ZERO;
        }
        self.inner.at.saturating_duration_since(self.now())
    }

    /// Stop waiting. The collection still in flight will see this and stop; it
    /// is what keeps a caller that gave up from leaving a thread behind.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn record_authentication_failure(&self, database_name: &str) {
        if !valid_database_basename(database_name) { return; }
        if let Ok(mut failure) = self.inner.authentication_failure.lock() {
            if failure.is_none() { *failure = Some(database_name.to_owned()); }
        }
    }

    pub(crate) fn authentication_failure(&self) -> Option<String> {
        self.inner.authentication_failure.lock().ok()?.clone()
    }

    /// The one check every stage, shard and row makes.
    pub fn check(&self) -> Result<(), MessageSnapshotError> {
        if self.expired() {
            return Err(MessageSnapshotError::DeadlineExceeded);
        }
        Ok(())
    }
}

/// A read error that happened at or after the deadline is reported as the
/// deadline being missed, not as a database failure: the same collision, one
/// millisecond earlier, would have been `DatabaseRead`, and the distinction the
/// caller acts on is "ask again later" against "this source is broken".
pub(crate) fn read_outcome(cancel: &CollectionCancel) -> MessageSnapshotError {
    if cancel.expired() {
        MessageSnapshotError::DeadlineExceeded
    } else {
        MessageSnapshotError::DatabaseRead
    }
}

/// Fixed outcomes of a message sample.
///
/// Every variant is fieldless, so the value carries no path, key, SQL or
/// message content and is safe to log and to map onto an HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageSnapshotError {
    /// A discovered shard has no decryption key.
    KeysUnavailable,
    /// The shard set cannot be used: no readable `message` directory, no
    /// numbered shard in it, or an entry that is not a shard file.
    SourceUnavailable,
    /// The shard set differed between the first and the last discovery.
    SourceChanged,
    /// A shard could not be opened, decrypted, queried or stepped.
    DatabaseRead,
    /// The sample would exceed the row or byte budget.
    BudgetExceeded,
    /// A row lacks a field the sample needs for identity or ordering.
    RowInvalid,
    /// One shard reports the same `local_id` twice.
    RowDuplicate,
    /// A stored blob is above the per-row limit.
    RowTooLarge,
    /// The collection ran out of its wall clock. The sample is discarded whole:
    /// a partial sample is not a sample.
    DeadlineExceeded,
}

impl MessageSnapshotError {
    pub fn code(self) -> &'static str {
        match self {
            Self::KeysUnavailable => "WECHAT_KEYS_UNAVAILABLE",
            Self::SourceUnavailable => "WECHAT_MESSAGE_SOURCE_UNAVAILABLE",
            Self::SourceChanged => "WECHAT_MESSAGE_SOURCE_CHANGED",
            Self::DatabaseRead => "WECHAT_DB_READ_FAILED",
            Self::BudgetExceeded => "WECHAT_MESSAGE_BUDGET_EXCEEDED",
            Self::RowInvalid => "WECHAT_MESSAGE_ROW_INVALID",
            Self::RowDuplicate => "WECHAT_MESSAGE_ROW_DUPLICATE",
            Self::RowTooLarge => "WECHAT_MESSAGE_ROW_TOO_LARGE",
            Self::DeadlineExceeded => "WECHAT_MESSAGE_DEADLINE_EXCEEDED",
        }
    }
}

impl std::fmt::Display for MessageSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for MessageSnapshotError {}

/// Collect one fixed sample of a chat's messages from every message shard.
///
/// `database_root` is the `db_storage` directory the identity module resolved
/// from the running client. The sample never picks a root of its own and never
/// falls back to another candidate: it reads the shards that are actually there.
///
/// The result is the frozen sample a paginated read serves from, not a single
/// instant across databases. Each shard is one read-only connection with one
/// complete bounded statement, so every shard is internally consistent, and
/// nothing here implies that two shards were read at the same moment. The shard
/// set is discovered again afterwards, so a shard that appeared or disappeared
/// during the reads is reported instead of being missed.
///
/// Every row keeps the fields the message list returns, plus `sourceShard` and
/// the stored `rawType` / `rawContent` / `rawContentCompression` values, so a
/// later reader can parse recalls and media from the original bytes. Rows are
/// never merged across shards by `local_id` or `server_id`: the same value in
/// two shards is two rows. The order is `create_time`, `local_id`, shard.
///
/// A shard that cannot be found, keyed, opened, decrypted, queried or decoded
/// fails the whole sample rather than being left out of it.
pub fn collect_message_snapshot(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
) -> Result<Vec<Value>, MessageSnapshotError> {
    collect_message_snapshot_until(
        database_root,
        account_dir,
        chat_id,
        keys,
        &CollectionCancel::within(COLLECTION_BUDGET),
    )
}

/// Collect one sample under a deadline the caller owns.
///
/// Same sample, same refusals, same order — with a wall clock over the *whole*
/// collection rather than per statement: discovery, every shard, the second
/// discovery and the contact names all draw on the same budget. A caller that
/// stops waiting calls [`CollectionCancel::cancel`]; the sample is then reported
/// as [`MessageSnapshotError::DeadlineExceeded`] and nothing of it is returned.
pub fn collect_message_snapshot_until(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    collect_fixed_sample(database_root, account_dir, chat_id, keys, &discover_message_shards, cancel)
}

/// `discover` is a parameter so a test can make the shard set change between
/// the first and the last discovery; production passes the directory scan.
fn collect_fixed_sample(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
    discover: &dyn Fn(&Path) -> Result<Vec<String>, MessageSnapshotError>,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    cancel.check()?;
    let before = discover(database_root)?;
    let shards = resolve_message_shards(database_root, &before, keys)?;
    cancel.check()?;

    let sample = collect_shard_rows(&shards, account_dir, chat_id, SampleBudget::default(), cancel)?;

    // Re-discover before returning: a sample that cannot prove its shard set was
    // stable is reported as a changed source, not as a complete sample.
    cancel.check()?;
    if discover(database_root)? != before {
        return Err(MessageSnapshotError::SourceChanged);
    }

    let names = resolve_contact_names(database_root, keys, &sample.senders, cancel)?;
    let mut values: Vec<Value> = sample.rows.into_iter().map(|row| row.value).collect();
    apply_contact_names(&mut values, &names, &SampleBudget::default(), cancel)?;
    cancel.check()?;
    Ok(values)
}

/// The row and byte budget of one sample.
struct SampleBudget {
    max_rows: usize,
    max_bytes: usize,
}

impl Default for SampleBudget {
    fn default() -> Self {
        Self {
            max_rows: SNAPSHOT_MAX_ROWS,
            max_bytes: SNAPSHOT_MAX_BYTES,
        }
    }
}

/// One `message_N.db` shard: its file name, its path inside the resolved root
/// and its key. Shards are separate databases with separate read views.
struct MessageShard {
    name: String,
    path: PathBuf,
    key: String,
    /// Shard number, the sample's last ordering key.
    index: u32,
}

/// Shard number of a numbered `message_N.db`, or `None` for every other file
/// (`message_fts.db`, `message_resource.db`, `message_abc.db`, ...).
fn message_shard_index(name: &str) -> Option<u32> {
    numbered_db_index(name, "message_")
}

/// Discover the shards inside `<database_root>/message`, in shard order.
///
/// The resolved root is the only directory consulted. A missing or unreadable
/// directory, an entry that cannot be read, a name that is not valid UTF-8 and
/// a numbered shard name that is not a regular file are all
/// `SourceUnavailable`: none of them may be reported as "no messages". Finding
/// no numbered shard at all is also `SourceUnavailable`, because a source that
/// cannot be read and a chat that was never stored look the same from here, and
/// an empty sample would claim the history is complete.
fn discover_message_shards(database_root: &Path) -> Result<Vec<String>, MessageSnapshotError> {
    let directory = database_root.join("message");
    let entries = std::fs::read_dir(&directory).map_err(|_| MessageSnapshotError::SourceUnavailable)?;

    let mut shards: Vec<(u32, String)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| MessageSnapshotError::SourceUnavailable)?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(MessageSnapshotError::SourceUnavailable)?;
        let Some(index) = message_shard_index(name) else {
            continue;
        };
        let file_type = entry
            .file_type()
            .map_err(|_| MessageSnapshotError::SourceUnavailable)?;
        if !file_type.is_file() {
            return Err(MessageSnapshotError::SourceUnavailable);
        }
        shards.push((index, name.to_string()));
    }

    if shards.is_empty() {
        return Err(MessageSnapshotError::SourceUnavailable);
    }
    shards.sort();
    Ok(shards.into_iter().map(|(_, name)| name).collect())
}

/// Attach the path and key of every discovered shard. A shard without a key
/// fails the whole sample: returning the other shards' rows would be a sample
/// that silently misses a chat.
fn resolve_message_shards(
    database_root: &Path,
    names: &[String],
    keys: &HashMap<String, String>,
) -> Result<Vec<MessageShard>, MessageSnapshotError> {
    names
        .iter()
        .map(|name| {
            let key = keys.get(name).ok_or(MessageSnapshotError::KeysUnavailable)?;
            Ok(MessageShard {
                index: message_shard_index(name).ok_or(MessageSnapshotError::SourceUnavailable)?,
                path: database_root.join("message").join(name),
                key: key.clone(),
                name: name.clone(),
            })
        })
        .collect()
}

/// A collected row together with the keys that fix the sample's order.
#[derive(Debug)]
struct SampledRow {
    create_time: i64,
    local_id: i64,
    shard_index: u32,
    shard_name: String,
    value: Value,
}

/// Everything one collection produced.
#[derive(Debug)]
struct CollectedSample {
    /// Already in sample order.
    rows: Vec<SampledRow>,
    /// Distinct senders, for the optional contact display names.
    senders: HashSet<String>,
}

/// Running state of one collection, so every shard spends the same row and byte
/// budget and the accounting cannot drift from one shard to the next.
struct SampleCollector {
    budget: SampleBudget,
    rows: Vec<SampledRow>,
    senders: HashSet<String>,
    bytes: usize,
}

impl SampleCollector {
    fn new(budget: SampleBudget) -> Self {
        Self {
            budget,
            rows: Vec::new(),
            senders: HashSet::new(),
            bytes: 0,
        }
    }

    /// Rows still allowed, which also bounds each shard's statement.
    fn remaining_rows(&self) -> Result<usize, MessageSnapshotError> {
        self.budget
            .max_rows
            .checked_sub(self.rows.len())
            .ok_or(MessageSnapshotError::BudgetExceeded)
    }

    /// Add one row, or refuse the whole sample. The budget is charged while the
    /// rows are read, so an oversized shard is never read in full first.
    fn push(&mut self, row: SampledRow) -> Result<(), MessageSnapshotError> {
        if self.rows.len() >= self.budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        let size = serialized_len(&row.value);
        if self.bytes + size > self.budget.max_bytes {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        self.bytes += size;
        self.rows.push(row);
        Ok(())
    }

    /// Order the sample and hand it over, or report that the round is over.
    ///
    /// The ordering is the last thing that touches every row, and `sort_by` has
    /// no place to stop inside a comparison, so the round is checked on both
    /// sides of it. That is not the same as interrupting the sort: what is
    /// refused is *returning* the result of one that ran out of time.
    fn into_sample(mut self, cancel: &CollectionCancel) -> Result<CollectedSample, MessageSnapshotError> {
        cancel.check()?;
        // `create_time`, `local_id`, shard number and shard name are unique
        // together, so the order is total and repeatable.
        self.rows.sort_by(|left, right| {
            left.create_time
                .cmp(&right.create_time)
                .then(left.local_id.cmp(&right.local_id))
                .then(left.shard_index.cmp(&right.shard_index))
                .then(left.shard_name.cmp(&right.shard_name))
        });
        cancel.check()?;
        Ok(CollectedSample {
            rows: self.rows,
            senders: self.senders,
        })
    }
}

/// Serialized size of a row, which is what the byte budget measures.
fn serialized_len(value: &Value) -> usize {
    // A `Value` cannot fail to serialize: it never holds a NaN, the only input
    // `to_vec` rejects.
    serde_json::to_vec(value).map(|bytes| bytes.len()).unwrap_or(0)
}

/// Read every shard into one sample.
fn collect_shard_rows(
    shards: &[MessageShard],
    account_dir: &str,
    chat_id: &str,
    budget: SampleBudget,
    cancel: &CollectionCancel,
) -> Result<CollectedSample, MessageSnapshotError> {
    let table = get_msg_table_name(chat_id);
    let mut collector = SampleCollector::new(budget);

    for shard in shards {
        // Between shards as well as inside one: with a hundred shards, the check
        // that matters is the one after the ninety-ninth.
        cancel.check()?;
        // One more row than the budget allows, so a shard that is too large is
        // detected instead of being silently cut short.
        let limit = i64::try_from(collector.remaining_rows()? + 1)
            .map_err(|_| MessageSnapshotError::BudgetExceeded)?;
        // The existence check and the sample read share one connection, so both
        // see the same view of this shard.
        let conn = open_shard_readonly(&shard.path, &shard.key, cancel)?;
        if !shard_has_chat_table(&conn, &table, cancel)? {
            // This shard simply does not hold the chat: zero rows, no failure.
            continue;
        }
        read_shard_rows(&conn, shard, chat_id, account_dir, limit, &mut collector, cancel)?;
    }

    collector.into_sample(cancel)
}

/// Whether a shard holds the chat at all.
fn shard_has_chat_table(
    conn: &Connection,
    table: &str,
    cancel: &CollectionCancel,
) -> Result<bool, MessageSnapshotError> {
    cancel.check()?;
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement
        .query([table])
        .map_err(|_| read_outcome(cancel))?;
    match rows.next() {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(_) => Err(read_outcome(cancel)),
    }
}

/// The single statement one shard is sampled with.
///
/// `length()` measures the stored bytes rather than the hex text the client
/// would receive, and the `CASE` keeps an oversized blob from being transferred
/// at all: such a row arrives with its length but without content, which
/// [`decode_message_row`] refuses instead of truncating.
fn sample_sql(chat_id: &str, limit: i64) -> String {
    let table = get_msg_table_name(chat_id);
    let cap = SNAPSHOT_ROW_CONTENT_LIMIT;
    format!(
        "SELECT m.local_id, m.server_id, m.local_type, m.create_time,
                CASE WHEN length(CAST(m.message_content AS BLOB)) <= {cap}
                     THEN hex(m.message_content) ELSE NULL END AS hex_content,
                length(CAST(m.message_content AS BLOB)) AS content_len,
                m.WCDB_CT_message_content AS is_compressed,
                CASE WHEN length(CAST(m.source AS BLOB)) <= {cap}
                     THEN hex(m.source) ELSE NULL END AS hex_source,
                length(CAST(m.source AS BLOB)) AS source_len,
                m.WCDB_CT_source AS source_compressed,
                n.user_name AS sender_name
         FROM \"{table}\" m
         LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
         ORDER BY m.create_time ASC, m.local_id ASC
         LIMIT {limit};"
    )
}

/// Stream one shard's rows into the sample.
///
/// The statement is complete and bounded: `ORDER BY create_time, local_id`
/// gives the shard its own deterministic order, and the `LIMIT` is the row
/// budget left plus one. Rows are stepped one at a time, so the byte budget can
/// stop the read before the rest of the shard is materialised, and a repeated
/// `local_id` inside one shard is reported instead of being deduplicated.
fn read_shard_rows(
    conn: &Connection,
    shard: &MessageShard,
    chat_id: &str,
    account_dir: &str,
    limit: i64,
    collector: &mut SampleCollector,
    cancel: &CollectionCancel,
) -> Result<(), MessageSnapshotError> {
    // Contact display names are resolved after the sample is collected, so the
    // rows are decoded without them first and the byte budget is charged again
    // once they are filled in.
    let contact_names = HashMap::new();
    let context = RowContext {
        chat_id,
        account_dir,
        contact_names: &contact_names,
        require_create_time: true,
        bounded_decode: true,
    };
    let mut statement = conn
        .prepare(&sample_sql(chat_id, limit))
        .map_err(|_| read_outcome(cancel))?;
    let columns: Vec<String> = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut rows = statement.query([]).map_err(|_| read_outcome(cancel))?;
    // `local_id` identifies a row only inside its own shard and chat.
    let mut local_ids: HashSet<i64> = HashSet::new();

    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        // Per row, not per statement: the statement may be a hundred thousand
        // rows long, and the deadline has to be able to land in the middle of
        // it. The interrupt installed on the connection covers the case where
        // one row's own work is what is slow.
        cancel.check()?;
        let value = row_to_value(row, &columns)?;
        let decoded = decode_message_row(&value, context)?;
        if !local_ids.insert(decoded.message.local_id) {
            return Err(MessageSnapshotError::RowDuplicate);
        }
        let create_time = decoded
            .create_time
            .ok_or(MessageSnapshotError::RowInvalid)?;
        if let Some(sender) = decoded.message.sender.as_ref() {
            collector.senders.insert(sender.clone());
        }
        collector.push(SampledRow {
            create_time,
            local_id: decoded.message.local_id,
            shard_index: shard.index,
            shard_name: shard.name.clone(),
            value: sample_value(&decoded, shard)?,
        })?;
    }
    Ok(())
}

/// One sample row: the legacy message fields, the shard it came from and the
/// stored values the display fields cannot carry.
fn sample_value(decoded: &DecodedRow, shard: &MessageShard) -> Result<Value, MessageSnapshotError> {
    let mut value = serde_json::to_value(&decoded.message)
        .map_err(|_| MessageSnapshotError::RowInvalid)?;
    let object = value
        .as_object_mut()
        .ok_or(MessageSnapshotError::RowInvalid)?;
    object.insert("sourceShard".to_string(), Value::String(shard.name.clone()));
    object.insert("rawType".to_string(), Value::Number(decoded.raw_type.into()));
    object.insert("rawContent".to_string(), Value::String(decoded.raw_content.clone()));
    object.insert(
        "rawContentCompression".to_string(),
        Value::Bool(decoded.raw_content_compressed),
    );
    Ok(value)
}

/// Convert one row into JSON. A column that cannot be read fails the sample
/// instead of leaving a hole in it. The value representation is the shared
/// database helper's, so the snapshot path and the plain read agree.
fn row_to_value(row: &rusqlite::Row<'_>, columns: &[String]) -> Result<Value, MessageSnapshotError> {
    let mut object = serde_json::Map::new();
    for (index, name) in columns.iter().enumerate() {
        let value = row
            .get_ref(index)
            .map_err(|_| MessageSnapshotError::DatabaseRead)?;
        object.insert(name.clone(), value_ref_to_json(value));
    }
    Ok(Value::Object(object))
}

/// Open one shard from an isolated byte snapshot of it.
///
/// The shard file and its `-wal` are copied as source bytes, the committed WAL
/// prefix is merged, and the returned connection reads a private encrypted copy
/// (`immutable=1`). The live source is never opened by SQLite, never locked and
/// never needs its `-shm`; a source that changes mid-capture is refused as
/// [`MessageSnapshotError::SourceChanged`]. Every rowid of the copy is the
/// source's, so `WHERE rowid = ?1` lookups (voice notes, sender joins) keep
/// working.
///
/// The snapshot path returns its own connection because it streams rows and
/// stops early, which a helper that materialises every row cannot do; the key
/// and read-only policy are the same. The connection carries an interrupt so a
/// statement that is already running when the deadline passes is stopped from
/// inside SQLite rather than at the next row.
pub(crate) fn open_shard_readonly(
    path: &Path,
    hex_key: &str,
    cancel: &CollectionCancel,
) -> Result<Connection, MessageSnapshotError> {
    cancel.check()?;
    let conn = wechat_snapshot::open_snapshot_conn(path, hex_key, Some(cancel)).map_err(|error| {
        match error {
            wechat_snapshot::SnapshotError::SourceChanged => MessageSnapshotError::SourceChanged,
            wechat_snapshot::SnapshotError::BudgetExceeded => MessageSnapshotError::BudgetExceeded,
            wechat_snapshot::SnapshotError::NotADatabase => {
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    cancel.record_authentication_failure(name);
                }
                MessageSnapshotError::DatabaseRead
            }
            wechat_snapshot::SnapshotError::CannotOpen
            | wechat_snapshot::SnapshotError::DatabaseRead => read_outcome(cancel),
        }
    })?;
    let guard = cancel.clone();
    // `true` from the progress callback is what interrupts: it is called every
    // `COLLECTION_PROGRESS_STEPS` VDBE instructions, so a statement that is
    // already running when the deadline passes stops from inside SQLite.
    conn.progress_handler(
        COLLECTION_PROGRESS_STEPS as i32,
        Some(move || guard.expired()),
    );
    Ok(conn)
}

fn valid_database_basename(name: &str) -> bool {
    name.ends_with(".db") && name.len() > 3
        && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// Optional display names for the sample's senders, resolved like the message
/// list resolves them.
///
/// Without a `contact.db` key the sample carries no display names. A contact
/// database that cannot be read is a read failure, not an empty name.
///
/// Opened and queried here rather than through the shared helper, because the
/// helper has no deadline: this query is part of the collection the caller is
/// waiting on, so it draws on the same budget as the shards. The sender set is
/// bounded by the sample's row budget, so the statement is bounded with it.
fn resolve_contact_names(
    database_root: &Path,
    keys: &HashMap<String, String>,
    senders: &HashSet<String>,
    cancel: &CollectionCancel,
) -> Result<HashMap<String, String>, MessageSnapshotError> {
    let contact_key = match keys.get("contact.db") {
        Some(key) if !senders.is_empty() => key,
        _ => return Ok(HashMap::new()),
    };
    cancel.check()?;

    let mut senders: Vec<&str> = senders.iter().map(String::as_str).collect();
    senders.sort_unstable();
    let placeholders = senders
        .iter()
        .map(|sender| format!("'{}'", sender.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");

    let conn = open_shard_readonly(
        &database_root.join("contact").join("contact.db"),
        contact_key,
        cancel,
    )?;
    let mut statement = conn
        .prepare(&format!(
            "SELECT username, nick_name, remark FROM contact WHERE username IN ({placeholders});"
        ))
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement
        .query([])
        .map_err(|_| read_outcome(cancel))?;

    let mut names = HashMap::new();
    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        cancel.check()?;
        let username: Option<String> = row.get(0).map_err(|_| read_outcome(cancel))?;
        let Some(username) = username.filter(|name| !name.is_empty()) else {
            continue;
        };
        let name: Option<String> = row.get(2).ok().flatten();
        let nick_name: Option<String> = row.get(1).ok().flatten();
        let display = name
            .filter(|name| !name.is_empty())
            .or_else(|| nick_name.filter(|name| !name.is_empty()))
            .unwrap_or(username.clone());
        names.insert(username, display);
    }
    Ok(names)
}

/// Fill in the optional `senderName` field and charge it against the byte
/// budget, because the names are part of the sample the caller receives.
///
/// The round is checked per row. This is the last pass over a sample that can
/// hold 50 000 rows, and every row here costs a serialization; without a check
/// inside the loop a round that ended during the shards would still walk the
/// whole tail before anything reported the deadline.
fn apply_contact_names(
    values: &mut [Value],
    names: &HashMap<String, String>,
    budget: &SampleBudget,
    cancel: &CollectionCancel,
) -> Result<(), MessageSnapshotError> {
    let mut bytes = 0usize;
    for value in values.iter_mut() {
        cancel.check()?;
        let display_name = value
            .get("sender")
            .and_then(Value::as_str)
            .and_then(|sender| names.get(sender))
            .map(String::as_str);
        if let (Some(display_name), Some(object)) = (display_name, value.as_object_mut()) {
            object.insert("senderName".to_string(), Value::String(display_name.to_string()));
        }
        bytes += serialized_len(value);
    }
    cancel.check()?;
    if bytes > budget.max_bytes {
        return Err(MessageSnapshotError::BudgetExceeded);
    }
    Ok(())
}
