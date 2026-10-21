use super::wechat_db::{get_db_path, try_query_wechat_db, WechatReadError};
use crate::ia::types::{Contact, ContactKind, ContactV2};
use std::collections::HashMap;

/// Known system/internal usernames to exclude from contact listings.
const SYSTEM_USERNAMES: &[&str] = &[
    "qmessage",
    "floatbottle",
    "medianote",
    "notifymessage",
    "weixin",
    "fmessage",
    "filehelper",
    "newsapp",
    "tmessage",
    "mphelper",
    "qqmail",
    "weixingongzhong",
    "qqsafe",
    "exmail_tool",
    "lbsapp",
    "pc_qq",
];

fn classify_contact(username: &str, local_type: i64) -> &'static str {
    if username.ends_with("@chatroom") {
        "chatroom"
    } else if username.starts_with("gh_") {
        "official"
    } else if username.contains("@openim") || local_type == 5 {
        "openim"
    } else {
        "individual"
    }
}

const V2_SYSTEM_USERNAMES: &[&str] = &[
    "filehelper",
    "fmessage",
    "medianote",
    "floatbottle",
    "newsapp",
    "fenvmsg",
    "officialaccounts",
    "weixin",
    "weibo",
    "qqmail",
    "brandsessionholder",
    "brandservicesessionholder",
];

/// Classify without inferring an account's kind from a `wxid_` prefix.
pub fn classify_contact_v2(
    username: &str,
    local_type: i64,
    biz_type: Option<i64>,
) -> ContactKind {
    if V2_SYSTEM_USERNAMES.contains(&username) {
        ContactKind::System
    } else if username.ends_with("@chatroom") {
        ContactKind::Group
    } else if username.ends_with("@openim") {
        ContactKind::Enterprise
    } else if biz_type.is_some() {
        ContactKind::Official
    } else {
        match local_type {
            5 => ContactKind::Enterprise,
            1 | 3 => ContactKind::Individual,
            _ => ContactKind::Unknown,
        }
    }
}

fn is_system_account(username: &str) -> bool {
    SYSTEM_USERNAMES.contains(&username)
}

/// Quote a value as a SQL string literal.
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The row filter both queries share, built from [`SYSTEM_USERNAMES`] so the
/// SQL and the row mapping can never disagree about what is excluded.
///
/// Everything the reader cannot return as a contact is excluded here rather than
/// after the query, so `LIMIT`/`OFFSET` only ever count real contacts.
/// local_type: 0=system notifications, 1=contacts+official, 2=chatrooms, 3=contacts, 5=openim
/// Include 1, 3, 5 (skip 0=system notifications, 2=chatrooms)
fn listable_contact_clause() -> String {
    let system: Vec<String> = SYSTEM_USERNAMES.iter().copied().map(sql_literal).collect();
    format!(
        "local_type IN (1, 3, 5) \
         AND typeof(username) = 'text' \
         AND username NOT LIKE '%@chatroom' \
         AND username NOT IN ({})",
        system.join(", ")
    )
}

/// Remarked contacts first, then nickname case-insensitively. The username
/// tie-break keeps equal nicknames in one fixed order, so consecutive pages of
/// the same result set stay consistent with each other.
const CONTACT_ORDER: &str =
    "ORDER BY remark != '' DESC, nick_name COLLATE NOCASE ASC, username ASC";

/// Convert contact rows into contacts, dropping system accounts and unusable rows.
fn contacts_from_rows(rows: &[serde_json::Value]) -> Vec<Contact> {
    rows.iter()
        .filter_map(|row| {
            let username = row.get("username")?.as_str()?;
            if is_system_account(username) {
                return None;
            }

            let local_type = row
                .get("local_type")
                .and_then(|v| v.as_i64())
                .unwrap_or(3);

            Some(Contact {
                username: username.to_string(),
                nick_name: row
                    .get("nick_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                remark: row
                    .get("remark")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from),
                alias: row
                    .get("alias")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from),
                small_head_url: row
                    .get("small_head_url")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from),
                contact_type: classify_contact(username, local_type).to_string(),
            })
        })
        .collect()
}

/// List contacts from contact.db.
/// Queries the contact table directly (not session.db), returning all stored contacts.
pub fn list_contacts(
    account_dir: &str,
    keys: &HashMap<String, String>,
    limit: i64,
    offset: i64,
) -> Result<Vec<Contact>, WechatReadError> {
    let contact_key = keys.get("contact.db").ok_or(WechatReadError::KeysUnavailable)?;
    let contact_db = get_db_path(account_dir, "contact.db");

    list_contacts_from_db(&contact_db, contact_key, limit, offset)
}

fn list_contacts_from_db(
    contact_db: &str,
    contact_key: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Contact>, WechatReadError> {
    let rows = try_query_wechat_db(
        contact_db,
        contact_key,
        &format!(
            "SELECT username, nick_name, remark, alias, small_head_url, local_type
             FROM contact
             WHERE {}
             {CONTACT_ORDER}
             LIMIT {limit} OFFSET {offset};",
            listable_contact_clause()
        ),
    )?;

    Ok(contacts_from_rows(&rows))
}

/// Search contacts by name (partial match on nick_name, remark, alias, or username).
pub fn find_contacts(
    account_dir: &str,
    keys: &HashMap<String, String>,
    query: &str,
) -> Result<Vec<Contact>, WechatReadError> {
    let contact_key = keys.get("contact.db").ok_or(WechatReadError::KeysUnavailable)?;
    let contact_db = get_db_path(account_dir, "contact.db");

    find_contacts_from_db(&contact_db, contact_key, query)
}

fn find_contacts_from_db(
    contact_db: &str,
    contact_key: &str,
    query: &str,
) -> Result<Vec<Contact>, WechatReadError> {
    let escaped = query.replace('\'', "''");

    let rows = try_query_wechat_db(
        contact_db,
        contact_key,
        &format!(
            "SELECT username, nick_name, remark, alias, small_head_url, local_type
             FROM contact
             WHERE {listable}
               AND (nick_name LIKE '%{escaped}%'
                    OR remark LIKE '%{escaped}%'
                    OR alias LIKE '%{escaped}%'
                    OR username LIKE '%{escaped}%')
             {CONTACT_ORDER}
             LIMIT 50;",
            listable = listable_contact_clause(),
        ),
    )?;

    Ok(contacts_from_rows(&rows))
}

// ============================================
// Frozen contact roster
// ============================================
//
// A read-only, single-table sample of `contact.db` that the paginated
// `GET /api/read/contacts` route freezes and serves. It deliberately does not
// reuse `listable_contact_clause`: that clause excludes rows by `local_type`,
// `@chatroom` suffix and a system-username allow-list, which are exactly the
// rows the legacy desktop roster (`wechat_agent/linux_wechat/receive_contacts.py`)
// still discovers. This path reads every non-deleted row instead, so a contact
// the old listings would drop is still visible here. It grants no contact
// permission; it is a roster, not an authorization.

use crate::tools::wechat_messages::{
    open_shard_readonly, read_outcome, CollectionCancel, MessageSnapshotError, COLLECTION_BUDGET,
};
use rusqlite::types::ValueRef;
use rusqlite::Transaction;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::Path;

/// Row budget of one roster sample: the same 50 000 rows a message sample may
/// hold. A roster above it is refused whole rather than truncated.
const CONTACT_MAX_ROWS: usize = 50_000;
/// Serialized byte budget of one roster sample: the same 16 MiB as a message
/// sample.
const CONTACT_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Largest serialized row one contact may cost. A wider row is refused instead of
/// being materialized into the sample.
const CONTACT_MAX_ROW_BYTES: usize = 1024 * 1024;

/// Columns the legacy desktop roster requires. `alias` is optional and is read
/// only when the live schema actually has it.
const REQUIRED_CONTACT_COLUMNS: [&str; 4] = ["username", "remark", "nick_name", "delete_flag"];

/// The row and byte budget of one roster sample.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ContactBudget {
    pub(crate) max_rows: usize,
    pub(crate) max_bytes: usize,
    pub(crate) max_row_bytes: usize,
}

impl Default for ContactBudget {
    fn default() -> Self {
        Self {
            max_rows: CONTACT_MAX_ROWS,
            max_bytes: CONTACT_MAX_BYTES,
            max_row_bytes: CONTACT_MAX_ROW_BYTES,
        }
    }
}

/// Collect one fixed roster sample from `contact.db`, under the collection budget.
pub fn collect_contact_snapshot(
    database_root: &Path,
    keys: &HashMap<String, String>,
) -> Result<Vec<Value>, MessageSnapshotError> {
    collect_contact_snapshot_until(
        database_root,
        keys,
        &CollectionCancel::within(COLLECTION_BUDGET),
    )
}

/// Collect V2 contact fields from the account's WeChat `db_storage` root.
/// Both passes isolate `contact.db` through `open_shard_readonly`; the existing
/// bounded roster scan supplies names, while this query adds source fields and
/// joins business metadata by full username.
pub fn collect_contact_snapshot_v2(
    database_root: &Path,
    keys: &HashMap<String, String>,
) -> Result<Vec<ContactV2>, MessageSnapshotError> {
    let cancel = CollectionCancel::within(COLLECTION_BUDGET);
    collect_contact_snapshot_v2_until(database_root, keys, &cancel)
}

pub(crate) fn collect_contact_snapshot_v2_until(
    database_root: &Path,
    keys: &HashMap<String, String>,
    cancel: &CollectionCancel,
) -> Result<Vec<ContactV2>, MessageSnapshotError> {
    collect_contact_snapshot_v2_inner(database_root, keys, ContactBudget::default(), cancel)
}

fn collect_contact_snapshot_v2_with_budget(
    database_root: &Path,
    keys: &HashMap<String, String>,
    budget: ContactBudget,
) -> Result<Vec<ContactV2>, MessageSnapshotError> {
    let cancel = CollectionCancel::within(COLLECTION_BUDGET);
    collect_contact_snapshot_v2_inner(database_root, keys, budget, &cancel)
}

fn collect_contact_snapshot_v2_inner(
    database_root: &Path,
    keys: &HashMap<String, String>,
    budget: ContactBudget,
    cancel: &CollectionCancel,
) -> Result<Vec<ContactV2>, MessageSnapshotError> {
    let roster = collect_contact_snapshot_until(database_root, keys, &cancel)?;
    let key = keys
        .get("contact.db")
        .ok_or(MessageSnapshotError::KeysUnavailable)?;
    let path = database_root.join("contact").join("contact.db");
    let conn = open_shard_readonly(&path, key, &cancel)?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|_| read_outcome(&cancel))?;
    let contact_columns = contact_columns(&tx, &cancel)?;
    if !["username", "local_type", "delete_flag"]
        .iter()
        .all(|column| contact_columns.contains(*column))
    {
        return Err(MessageSnapshotError::SourceUnavailable);
    }
    let biz_columns = table_columns(&tx, "biz_info", &cancel)?;
    if !["username", "type"]
        .iter()
        .all(|column| biz_columns.contains(*column))
    {
        return Err(MessageSnapshotError::SourceUnavailable);
    }

    let verify_projection = if contact_columns.contains("verify_flag") {
        "c.verify_flag"
    } else {
        "NULL"
    };
    let username_projection = contact_field_projection("c.username", budget.max_row_bytes);
    let mut statement = tx
        .prepare(&format!(
            "SELECT {username_projection}, c.local_type, {verify_projection}, b.type \
             FROM contact AS c LEFT JOIN biz_info AS b ON b.username = c.username \
             WHERE c.delete_flag = 0 \
               AND typeof(c.username) = 'text' \
               AND length(CAST(c.username AS BLOB)) > 0"
        ))
        .map_err(|_| read_outcome(&cancel))?;
    let mut rows = statement.query([]).map_err(|_| read_outcome(&cancel))?;
    let mut source_types = HashMap::with_capacity(roster.len().min(budget.max_rows));
    let mut metadata_bytes = 0usize;
    while let Some(row) = rows.next().map_err(|_| read_outcome(&cancel))? {
        cancel.check()?;
        let username_bytes: i64 = row.get(1).map_err(|_| read_outcome(&cancel))?;
        let username_bytes =
            usize::try_from(username_bytes).map_err(|_| MessageSnapshotError::RowTooLarge)?;
        if username_bytes > budget.max_row_bytes {
            return Err(MessageSnapshotError::RowTooLarge);
        }
        metadata_bytes = metadata_bytes.saturating_add(username_bytes);
        if metadata_bytes > budget.max_bytes {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        if source_types.len() >= budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        let username: String = row.get(0).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let local_type: i64 = row.get(2).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let verify_flag: Option<i64> = row.get(3).map_err(|_| read_outcome(&cancel))?;
        let official_type: Option<i64> = row.get(4).map_err(|_| read_outcome(&cancel))?;
        if source_types
            .insert(username, (local_type, verify_flag, official_type))
            .is_some()
        {
            return Err(MessageSnapshotError::RowDuplicate);
        }
    }

    let mut contacts = Vec::with_capacity(roster.len());
    let mut sample_bytes = 0usize;
    for source in roster {
        cancel.check()?;
        let username = source["username"]
            .as_str()
            .ok_or(MessageSnapshotError::RowInvalid)?;
        let (local_type, verify_flag, official_type) = source_types
            .remove(username)
            .ok_or(MessageSnapshotError::RowInvalid)?;
        let alias = source["alias"]
            .as_str()
            .ok_or(MessageSnapshotError::RowInvalid)?;
        let nick_name = source["nick_name"]
            .as_str()
            .ok_or(MessageSnapshotError::RowInvalid)?;
        let remark = source["remark"]
            .as_str()
            .ok_or(MessageSnapshotError::RowInvalid)?;
        let contact = ContactV2 {
            username: username.to_string(),
            alias: alias.to_string(),
            nick_name: nick_name.to_string(),
            remark: remark.to_string(),
            display_name: contact_display_name_v2(remark, nick_name, username),
            kind: classify_contact_v2(username, local_type, official_type),
            local_type,
            official_type,
            verify_flag,
        };
        let row_bytes = serde_json::to_vec(&contact)
            .map_err(|_| MessageSnapshotError::RowInvalid)?
            .len();
        if row_bytes > budget.max_row_bytes {
            return Err(MessageSnapshotError::RowTooLarge);
        }
        sample_bytes = sample_bytes.saturating_add(row_bytes);
        if sample_bytes > budget.max_bytes || contacts.len() >= budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        contacts.push(contact);
    }
    if !source_types.is_empty() {
        return Err(MessageSnapshotError::RowInvalid);
    }
    Ok(contacts)
}

fn contact_display_name_v2(remark: &str, nick_name: &str, username: &str) -> String {
    let remark = remark.trim();
    if !remark.is_empty() {
        remark.to_string()
    } else {
        let nick_name = nick_name.trim();
        if nick_name.is_empty() {
            username.to_string()
        } else {
            nick_name.to_string()
        }
    }
}

/// Collect one fixed roster sample under a deadline the caller owns.
///
/// `database_root` is the `db_storage` directory the identity module resolved;
/// `contact.db` is read at `<database_root>/contact/contact.db` and its stored
/// key is required — nothing here falls back to another root or starts key
/// extraction. One read-only transaction covers both the schema probe and the
/// scan, so the columns the scan uses are the columns that were verified.
///
/// Every row keeps the five legacy fields the roster has always had
/// (`username`, `remark`, `nick_name`, `alias`, `display_name`), all strings. A
/// row whose `username` is not text or is empty is skipped; a repeated non-empty
/// `username`, a row wider than the per-row budget, more rows or bytes than the
/// sample budget, or a missing required column each fail the whole sample rather
/// than returning a partial or empty roster.
pub fn collect_contact_snapshot_until(
    database_root: &Path,
    keys: &HashMap<String, String>,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    cancel.check()?;
    let key = keys
        .get("contact.db")
        .ok_or(MessageSnapshotError::KeysUnavailable)?;
    let path = database_root.join("contact").join("contact.db");
    let conn = open_shard_readonly(&path, key, cancel)?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|_| read_outcome(cancel))?;
    let columns = contact_columns(&tx, cancel)?;
    if let Some(missing) = REQUIRED_CONTACT_COLUMNS
        .iter()
        .find(|column| !columns.contains(**column))
    {
        tracing::warn!(column = missing, "contact roster schema is incomplete");
        return Err(MessageSnapshotError::SourceUnavailable);
    }
    collect_contact_rows(&tx, &columns, ContactBudget::default(), cancel)
}

/// The column names of the live `contact` table.
///
/// A missing table reports an empty set, which the caller refuses: an account
/// with no roster and a source that is not there must not look the same.
fn contact_columns(
    tx: &Transaction<'_>,
    cancel: &CollectionCancel,
) -> Result<HashSet<String>, MessageSnapshotError> {
    table_columns(tx, "contact", cancel)
}

fn table_columns(
    tx: &Transaction<'_>,
    table: &str,
    cancel: &CollectionCancel,
) -> Result<HashSet<String>, MessageSnapshotError> {
    cancel.check()?;
    let pragma = match table {
        "contact" => "PRAGMA table_info(contact)",
        "biz_info" => "PRAGMA table_info(biz_info)",
        _ => return Err(MessageSnapshotError::SourceUnavailable),
    };
    let mut statement = tx
        .prepare(pragma)
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement.query([]).map_err(|_| read_outcome(cancel))?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        cancel.check()?;
        // Column 1 of `table_info` is the column name.
        let name = text_field(row.get_ref(1).map_err(|_| read_outcome(cancel))?)?;
        if !name.is_empty() {
            columns.insert(name);
        }
    }
    Ok(columns)
}

/// The contact fields the roster projects, in order. `alias` is read only when
/// the live schema has it, so a shorter slice of this list is used then.
const CONTACT_FIELDS: [&str; 4] = ["username", "remark", "nick_name", "alias"];

/// Fields read for a schema with or without the optional `alias`.
fn contact_field_count(has_alias: bool) -> usize {
    if has_alias { 4 } else { 3 }
}

/// SQL for one contact field: the value only when its stored byte length fits
/// `cap`, followed by that byte length.
///
/// `length(CAST(col AS BLOB))` measures the stored bytes without decoding them
/// and is NULL for a NULL value, so the condition is never true for one. A value
/// wider than `cap` is projected as NULL by the `CASE` and is therefore never
/// handed to Rust to allocate; the companion length column is what the caller
/// reads to report the refusal.
pub(crate) fn contact_field_projection(field: &str, cap: usize) -> String {
    format!(
        "CASE WHEN length(CAST({field} AS BLOB)) <= {cap} THEN {field} ELSE NULL END, \
         length(CAST({field} AS BLOB))"
    )
}

/// The single bounded, streamed statement the roster sample is read with.
///
/// Every input is a constant derived from the budget or a fixed field name, so
/// nothing request-shaped reaches the SQL text. The `LIMIT` is the row budget
/// plus one, the same "one more row than the sample" probe the scan uses.
fn contact_row_sql(has_alias: bool, cap: usize, limit: i64) -> String {
    let fields = &CONTACT_FIELDS[..contact_field_count(has_alias)];
    let projections: Vec<String> = fields
        .iter()
        .map(|field| contact_field_projection(field, cap))
        .collect();
    format!(
        "SELECT {} FROM contact WHERE delete_flag = 0 LIMIT {limit}",
        projections.join(", ")
    )
}

/// Stream the roster into a frozen sample, charging the row, per-field, raw-row
/// and serialized-row budgets as it goes.
///
/// The statement is bounded by `max_rows + 1`, so a roster that is larger than
/// the sample is detected instead of being silently cut short, and it is stepped
/// one row at a time, so the budgets can stop the read before the rest of the
/// table is materialized.
///
/// Each field is also bounded in SQL to the row budget, and the stored byte
/// lengths of the whole row are summed before any value is decoded, so an
/// arbitrarily wide `TEXT` is refused without being copied into a Rust string.
/// The final serialized-row check stays because JSON escaping can widen a value
/// past its stored length.
fn collect_contact_rows(
    tx: &Transaction<'_>,
    columns: &HashSet<String>,
    budget: ContactBudget,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    let has_alias = columns.contains("alias");
    let field_count = contact_field_count(has_alias);
    let limit = i64::try_from(budget.max_rows.saturating_add(1))
        .map_err(|_| MessageSnapshotError::BudgetExceeded)?;
    // One field may not cost more than the whole row budget, so the SQL
    // projection itself refuses to return a wider value.
    let cap = budget.max_row_bytes;
    let mut statement = tx
        .prepare(&contact_row_sql(has_alias, cap, limit))
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement.query([]).map_err(|_| read_outcome(cancel))?;

    let mut items: Vec<Value> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut bytes = 0usize;
    let mut scanned = 0usize;

    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        cancel.check()?;
        scanned += 1;
        // One more row was requested than the sample can hold, so reaching this
        // far means the table has at least one row beyond the budget.
        if scanned > budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }

        // The stored byte length of every field, read before any value is copied
        // into a Rust string. A field above the cap was projected as NULL and is
        // refused here, so the huge value never reaches `text_field`. The sum is
        // saturating for the same reason the cap exists: a hostile row must not
        // be able to overflow the accounting.
        let mut raw_bytes = 0usize;
        for index in 0..field_count {
            let length: Option<i64> = row
                .get(2 * index + 1)
                .map_err(|_| read_outcome(cancel))?;
            if let Some(length) = length {
                let length =
                    usize::try_from(length).map_err(|_| MessageSnapshotError::RowTooLarge)?;
                if length > cap {
                    return Err(MessageSnapshotError::RowTooLarge);
                }
                raw_bytes = raw_bytes.saturating_add(length);
            }
        }
        if raw_bytes > budget.max_row_bytes {
            return Err(MessageSnapshotError::RowTooLarge);
        }

        let username = text_field(row.get_ref(0).map_err(|_| read_outcome(cancel))?)?;
        // A row without a usable username is not a contact: the legacy roster
        // skips it rather than fabricating an identity for it.
        if username.is_empty() {
            continue;
        }
        let remark = text_field(row.get_ref(2).map_err(|_| read_outcome(cancel))?)?;
        let nick_name = text_field(row.get_ref(4).map_err(|_| read_outcome(cancel))?)?;
        let alias = if has_alias {
            text_field(row.get_ref(6).map_err(|_| read_outcome(cancel))?)?
        } else {
            String::new()
        };
        if !seen.insert(username.clone()) {
            return Err(MessageSnapshotError::RowDuplicate);
        }
        let display_name = display_name(&remark, &nick_name);
        let value = json!({
            "username": username,
            "remark": remark,
            "nick_name": nick_name,
            "alias": alias,
            "display_name": display_name,
        });
        // JSON escaping can widen a value past its stored length, so the
        // serialized row is charged as well as the raw lengths.
        let size = serde_json::to_vec(&value).map(|encoded| encoded.len()).unwrap_or(0);
        if size > budget.max_row_bytes {
            return Err(MessageSnapshotError::RowTooLarge);
        }
        if bytes + size > budget.max_bytes {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        bytes += size;
        items.push(value);
    }
    cancel.check()?;
    Ok(items)
}

/// The roster's text rule: a stored value is used only when it is text, and the
/// text must decode as UTF-8.
///
/// A NULL, integer, real or blob column is empty, and nothing is stringified
/// into a contact field. A `TEXT` value with invalid bytes is refused with
/// [`MessageSnapshotError::RowInvalid`] instead of being replaced: replacing the
/// bytes would synthesize a username the source never held and could merge
/// distinct contacts into one stable identity. The message sample's own
/// `value_ref_to_json` keeps its lossy rule; only this roster path is strict.
fn text_field(value: ValueRef<'_>) -> Result<String, MessageSnapshotError> {
    match value {
        ValueRef::Text(bytes) => std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| MessageSnapshotError::RowInvalid),
        _ => Ok(String::new()),
    }
}

/// `remark.trim()` when it has content, otherwise `nick_name.trim()`.
///
/// The stored fields themselves are kept raw; only the derived display name is
/// trimmed, matching the legacy roster.
fn display_name(remark: &str, nick_name: &str) -> String {
    let remark = remark.trim();
    if remark.is_empty() {
        nick_name.trim().to_string()
    } else {
        remark.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// 32-byte key in the hex form WeChat stores it as.
    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    const CONTACT_SCHEMA: &str = "
        CREATE TABLE contact (
            username TEXT PRIMARY KEY,
            nick_name TEXT,
            remark TEXT,
            alias TEXT,
            small_head_url TEXT,
            local_type INTEGER
        );
    ";

    fn keys(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Contact and Chat do not implement PartialEq, so compare the error, not the whole Result.
    fn assert_read_err<T: std::fmt::Debug>(
        result: Result<T, WechatReadError>,
        expected: WechatReadError,
    ) {
        assert_eq!(result.unwrap_err(), expected);
    }

    /// Create a contact.db that the read path can decrypt with `key`.
    fn create_contact_db<S: AsRef<str>>(path: &Path, key: &str, rows: &[S]) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{key}'\"; PRAGMA cipher_compatibility = 4;"
        ))
        .unwrap();
        conn.execute_batch(CONTACT_SCHEMA).unwrap();
        for row in rows {
            conn.execute(row.as_ref(), []).unwrap();
        }
    }

    /// An account directory that cannot exist, so the resolved database path is unreadable.
    fn absent_account_dir() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!(
            "wechat-agent-absent-test-account-{}-{nanos}",
            std::process::id()
        )
    }

    fn fixture_contact_db<S: AsRef<str>>(rows: &[S]) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contact.db");
        create_contact_db(&path, TEST_KEY, rows);
        let path_str = path.to_str().unwrap().to_string();
        (dir, path_str)
    }

    #[test]
    fn list_contacts_without_contact_key_reports_keys_unavailable() {
        let account = absent_account_dir();

        assert_read_err(
            list_contacts(&account, &keys(&[]), 200, 0), WechatReadError::KeysUnavailable);
        // Only unrelated keys are present: still not readable.
        assert_read_err(
            list_contacts(
                &account,
                &keys(&[("session.db", TEST_KEY), ("message_0.db", TEST_KEY)]),
                200,
                0
            ), WechatReadError::KeysUnavailable);
    }

    #[test]
    fn find_contacts_without_contact_key_reports_keys_unavailable() {
        let account = absent_account_dir();

        assert_read_err(
            find_contacts(&account, &keys(&[]), "alice"), WechatReadError::KeysUnavailable);
        assert_read_err(
            find_contacts(&account, &keys(&[("session.db", TEST_KEY)]), "alice"),
            WechatReadError::KeysUnavailable,
        );
    }

    #[test]
    fn list_contacts_with_unreadable_db_reports_database_read() {
        let account = absent_account_dir();

        let result = list_contacts(&account, &keys(&[("contact.db", TEST_KEY)]), 200, 0);

        assert_read_err(result, WechatReadError::DatabaseRead);
    }

    #[test]
    fn find_contacts_with_unreadable_db_reports_database_read() {
        let account = absent_account_dir();

        let result = find_contacts(&account, &keys(&[("contact.db", TEST_KEY)]), "alice");

        assert_read_err(result, WechatReadError::DatabaseRead);
    }

    #[test]
    fn list_contacts_returns_empty_for_empty_table() {
        let (_dir, path) = fixture_contact_db::<&str>(&[]);

        let contacts = list_contacts_from_db(&path, TEST_KEY, 200, 0).unwrap();

        assert!(contacts.is_empty());
    }

    #[test]
    fn list_contacts_maps_rows_and_skips_system_accounts() {
        let (_dir, path) = fixture_contact_db(&[
            "INSERT INTO contact VALUES
             ('alice', 'Alice', 'Alice Remark', 'alice_alias', 'https://img/a', 3)",
            "INSERT INTO contact VALUES ('filehelper', 'File Transfer', '', '', '', 3)",
            "INSERT INTO contact VALUES ('gh_news', 'News', '', '', '', 1)",
            "INSERT INTO contact VALUES ('team@chatroom', 'Team', '', '', '', 2)",
        ]);

        let contacts = list_contacts_from_db(&path, TEST_KEY, 200, 0).unwrap();

        let usernames: Vec<&str> = contacts.iter().map(|c| c.username.as_str()).collect();
        assert_eq!(usernames, vec!["alice", "gh_news"]);
        assert_eq!(contacts[0].nick_name, "Alice");
        assert_eq!(contacts[0].remark.as_deref(), Some("Alice Remark"));
        assert_eq!(contacts[0].alias.as_deref(), Some("alice_alias"));
        assert_eq!(contacts[0].contact_type, "individual");
        assert_eq!(contacts[1].remark, None);
        assert_eq!(contacts[1].contact_type, "official");
    }

    #[test]
    fn list_contacts_honours_limit_and_offset() {
        let (_dir, path) = fixture_contact_db(&[
            "INSERT INTO contact VALUES ('a1', 'Ann', '', '', '', 3)",
            "INSERT INTO contact VALUES ('b2', 'Bob', '', '', '', 3)",
            "INSERT INTO contact VALUES ('c3', 'Cid', '', '', '', 3)",
        ]);

        let first = list_contacts_from_db(&path, TEST_KEY, 2, 0).unwrap();
        let second = list_contacts_from_db(&path, TEST_KEY, 2, 2).unwrap();

        let first_names: Vec<&str> = first.iter().map(|c| c.nick_name.as_str()).collect();
        let second_names: Vec<&str> = second.iter().map(|c| c.nick_name.as_str()).collect();
        assert_eq!(first_names, vec!["Ann", "Bob"]);
        assert_eq!(second_names, vec!["Cid"]);
    }

    /// Interleaves system accounts, a blob and a NULL username among the
    /// listable rows, and gives two listable contacts the same nickname so the
    /// username tie-break is observable.
    fn interleaved_contact_rows() -> Vec<String> {
        [
            // Sorting places these on both sides of the listable rows.
            "INSERT INTO contact VALUES ('filehelper', 'Aaa System', '', '', '', 3)",
            "INSERT INTO contact VALUES ('a_ann', 'Ann', '', '', '', 3)",
            "INSERT INTO contact VALUES (x'2A2A', 'Bbb Blob', '', '', '', 3)",
            "INSERT INTO contact VALUES (NULL, 'Bbb Null', '', '', '', 3)",
            "INSERT INTO contact VALUES ('qmessage', 'Ccc System', '', '', '', 3)",
            "INSERT INTO contact VALUES ('b_bob', 'Bob', '', '', '', 3)",
            "INSERT INTO contact VALUES ('zz_same', 'Same', '', '', '', 3)",
            "INSERT INTO contact VALUES ('aa_same', 'Same', '', '', '', 3)",
            "INSERT INTO contact VALUES ('team@chatroom', 'Same', '', '', '', 2)",
            "INSERT INTO contact VALUES ('qqmail', 'Zzz System', '', '', '', 3)",
            // A remark sorts the contact ahead of everything above.
            "INSERT INTO contact VALUES ('c_carol', 'Carol', 'Carol Remark', '', '', 3)",
        ]
        .iter()
        .map(|row| row.to_string())
        .collect()
    }

    /// Every listable contact of `interleaved_contact_rows`, in list order.
    const INTERLEAVED_EXPECTED: &[&str] = &["c_carol", "a_ann", "b_bob", "aa_same", "zz_same"];
    fn expected_interleaved() -> Vec<String> {
        INTERLEAVED_EXPECTED.iter().map(|u| u.to_string()).collect()
    }

    fn usernames(contacts: &[Contact]) -> Vec<String> {
        contacts.iter().map(|c| c.username.clone()).collect()
    }

    #[test]
    fn listable_clause_excludes_every_system_username() {
        let clause = listable_contact_clause();

        // The SQL exclusion must be derived from the one list, never a copy.
        for username in SYSTEM_USERNAMES {
            assert!(
                clause.contains(&sql_literal(username)),
                "{username} is missing from the SQL exclusion"
            );
        }
        assert!(clause.contains("typeof(username) = 'text'"));
        // Literals must be escaped, so a quote cannot terminate the predicate.
        assert_eq!(sql_literal("o'brien"), "'o''brien'");
    }

    #[test]
    fn list_contacts_pages_skip_system_and_non_text_rows() {
        let (_dir, path) = fixture_contact_db(&interleaved_contact_rows());

        // One page holding everything must agree with the concatenated pages:
        // neither the excluded rows nor the equal nicknames may eat a slot.
        let whole = usernames(&list_contacts_from_db(&path, TEST_KEY, 200, 0).unwrap());
        let mut paged: Vec<String> = Vec::new();
        for offset in [0, 3, 6] {
            let page = list_contacts_from_db(&path, TEST_KEY, 3, offset).unwrap();
            assert!(page.len() <= 3);
            paged.extend(usernames(&page));
        }

        assert_eq!(whole, expected_interleaved());
        assert_eq!(paged, expected_interleaved());
    }

    #[test]
    fn list_contacts_returns_short_last_page_and_empty_page_past_the_end() {
        let (_dir, path) = fixture_contact_db(&interleaved_contact_rows());

        let last = usernames(&list_contacts_from_db(&path, TEST_KEY, 3, 3).unwrap());
        let past_end = list_contacts_from_db(&path, TEST_KEY, 3, 6).unwrap();

        assert_eq!(last, vec!["aa_same".to_string(), "zz_same".to_string()]);
        assert!(past_end.is_empty());
    }

    #[test]
    fn find_contacts_fixed_limit_ignores_system_accounts() {
        // The system accounts match the query and sort ahead of every filler, so
        // they consume slots of the fixed LIMIT 50 unless SQL excludes them first.
        let mut rows: Vec<String> = SYSTEM_USERNAMES
            .iter()
            .enumerate()
            .map(|(i, username)| {
                format!(
                    "INSERT INTO contact VALUES ('{username}', 'Aaa Target {i:02}', '', '', '', 3)"
                )
            })
            .collect();
        assert!(
            rows.len() + 45 > 50,
            "matches must outnumber the fixed limit, otherwise the limit is never reached"
        );
        for i in 0..45 {
            rows.push(format!(
                "INSERT INTO contact VALUES ('wxid_filler_{i:02}', 'Mmm Target {i:02}', '', '', '', 3)"
            ));
        }
        rows.push("INSERT INTO contact VALUES ('wxid_target', 'Zzz Target', '', '', '', 3)".into());

        let (_dir, path) = fixture_contact_db(&rows);

        let hits = usernames(&find_contacts_from_db(&path, TEST_KEY, "Target").unwrap());

        assert_eq!(hits.last().map(String::as_str), Some("wxid_target"));
        assert!(!hits.iter().any(|u| SYSTEM_USERNAMES.contains(&u.as_str())));
    }

    #[test]
    fn find_contacts_matches_remark_alias_and_username() {
        let (_dir, path) = fixture_contact_db(&[
            "INSERT INTO contact VALUES ('wxid_alice', 'Alice', 'Zed Remark', '', '', 3)",
            "INSERT INTO contact VALUES ('wxid_bob', 'Bob', '', 'zed_alias', '', 3)",
            "INSERT INTO contact VALUES ('zed_cid', 'Carol', '', '', '', 3)",
            "INSERT INTO contact VALUES ('wxid_dave', 'Dave', '', '', '', 3)",
        ]);

        let by_remark = find_contacts_from_db(&path, TEST_KEY, "Zed Remark").unwrap();
        let by_alias = find_contacts_from_db(&path, TEST_KEY, "zed_alias").unwrap();
        let by_username = find_contacts_from_db(&path, TEST_KEY, "zed_cid").unwrap();
        let by_nick = find_contacts_from_db(&path, TEST_KEY, "Dave").unwrap();

        assert_eq!(by_remark.len(), 1);
        assert_eq!(by_remark[0].username, "wxid_alice");
        assert_eq!(by_alias.len(), 1);
        assert_eq!(by_alias[0].username, "wxid_bob");
        assert_eq!(by_username.len(), 1);
        assert_eq!(by_username[0].username, "zed_cid");
        assert_eq!(by_nick.len(), 1);
        assert_eq!(by_nick[0].username, "wxid_dave");
    }

    #[test]
    fn find_contacts_returns_empty_when_nothing_matches() {
        let (_dir, path) = fixture_contact_db(&[
            "INSERT INTO contact VALUES ('wxid_alice', 'Alice', '', '', '', 3)",
        ]);

        assert!(find_contacts_from_db(&path, TEST_KEY, "nobody")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn missing_contact_file_reports_database_read_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contact.db");
        let path_str = path.to_str().unwrap().to_string();

        // The file does not exist, so the read must fail rather than look like "no match".
        assert_read_err(
            find_contacts_from_db(&path_str, TEST_KEY, "alice"), WechatReadError::DatabaseRead);
    }
}

/// The frozen roster at the collection boundary.
///
/// These drive the real collector against an encrypted temporary `contact.db`,
/// so the schema compatibility, the row mapping and the budgets are exercised
/// without a WeChat client, a real account or the agent database. The router
/// boundary (epoch fence, pagination, release, resource isolation) lives in
/// `router::read`'s tests.
#[cfg(test)]
mod roster_tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use rusqlite::Connection;
    use serde_json::Value;

    use super::*;
    use crate::tools::wechat_messages::{CollectionCancel, MessageSnapshotError};

    /// 32-byte key in the hex form WeChat stores it as.
    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    /// The live `contact` table: the four required columns plus the optional
    /// `alias`, a `local_type` the roster ignores and a `small_head_url` the
    /// roster does not read.
    const ROSTER_SCHEMA: &str = "
        CREATE TABLE contact (
            username TEXT,
            nick_name TEXT,
            remark TEXT,
            alias TEXT,
            delete_flag INTEGER,
            local_type INTEGER,
            small_head_url TEXT
        );
    ";

    const ROSTER_SCHEMA_NO_ALIAS: &str = "
        CREATE TABLE contact (
            username TEXT,
            nick_name TEXT,
            remark TEXT,
            delete_flag INTEGER
        );
    ";

    /// No declared types, so a stored integer or blob keeps its storage class
    /// instead of being coerced into text by column affinity. This is the only
    /// way to observe the "a non-text field is empty" rule.
    const ROSTER_SCHEMA_NO_AFFINITY: &str = "
        CREATE TABLE contact (username, nick_name, remark, alias, delete_flag, local_type);
    ";

    fn roster_root() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        std::fs::create_dir_all(root.join("contact")).unwrap();
        (dir, root)
    }

    /// Create the encrypted `contact.db` under `root` and run every statement.
    fn seed_roster(root: &Path, schema: &str, rows: &[&str]) -> String {
        let path = root.join("contact").join("contact.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
        ))
        .unwrap();
        conn.execute_batch(schema).unwrap();
        for row in rows {
            conn.execute(row, []).unwrap();
        }
        path.to_str().unwrap().to_string()
    }

    fn roster_keys() -> HashMap<String, String> {
        HashMap::from([("contact.db".to_string(), TEST_KEY.to_string())])
    }

    /// Create an encrypted `contact.db` and insert one live row with parameters,
    /// so a value too large or too weird to embed in a SQL literal can still be
    /// stored without quoting it.
    fn seed_contact(root: &Path, username: &str, nick_name: &str, remark: &str) -> String {
        let path = root.join("contact").join("contact.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4; {ROSTER_SCHEMA}"
        ))
        .unwrap();
        conn.execute(
            "INSERT INTO contact (username, nick_name, remark, alias, delete_flag, local_type) \
             VALUES (?1, ?2, ?3, '', 0, 3)",
            rusqlite::params![username, nick_name, remark],
        )
        .unwrap();
        drop(conn);
        path.to_str().unwrap().to_string()
    }

    fn usernames(items: &[Value]) -> Vec<String> {
        items
            .iter()
            .map(|item| item["username"].as_str().unwrap().to_string())
            .collect()
    }

    /// Run the scanner directly with a caller-chosen budget, so a small budget
    /// can be exercised without building 50 000 rows.
    fn scan_with_budget(
        path: &str,
        budget: ContactBudget,
    ) -> Result<Vec<Value>, MessageSnapshotError> {
        let conn = open_shard_readonly(
            Path::new(path),
            TEST_KEY,
            &CollectionCancel::within(Duration::from_secs(5)),
        )
        .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        let cancel = CollectionCancel::within(Duration::from_secs(5));
        let columns = contact_columns(&tx, &cancel).unwrap();
        collect_contact_rows(&tx, &columns, budget, &cancel)
    }

    #[test]
    fn items_carry_exactly_the_five_legacy_string_fields() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA,
            &[
                "INSERT INTO contact VALUES ('alice', 'Alice', '  Alice Remark  ', 'alice_alias', 0, 3, 'https://img/a')",
                "INSERT INTO contact VALUES ('bob', 'Bob', NULL, NULL, 0, 3, NULL)",
                "INSERT INTO contact VALUES ('carol', '  Carol  ', '', NULL, 0, 1, NULL)",
            ],
        );

        let items = collect_contact_snapshot(&root, &roster_keys()).unwrap();

        assert_eq!(usernames(&items), vec!["alice", "bob", "carol"]);
        for item in &items {
            let object = item.as_object().unwrap();
            let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(keys, vec!["alias", "display_name", "nick_name", "remark", "username"]);
            assert!(
                object.values().all(Value::is_string),
                "every roster field must be a string: {item}"
            );
        }
        assert_eq!(items[0]["remark"], "  Alice Remark  ", "stored text stays raw");
        assert_eq!(items[0]["alias"], "alice_alias");
        assert_eq!(items[0]["display_name"], "Alice Remark", "display name is trimmed");
        assert_eq!(items[1]["remark"], "", "NULL becomes empty");
        assert_eq!(items[1]["alias"], "", "NULL alias becomes empty");
        assert_eq!(items[1]["display_name"], "Bob");
        assert_eq!(items[2]["remark"], "");
        assert_eq!(items[2]["nick_name"], "  Carol  ", "stored text stays raw");
        assert_eq!(items[2]["display_name"], "Carol", "nick_name is trimmed when used");
    }

    #[test]
    fn deleted_rows_are_excluded_and_every_other_local_type_is_kept() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA,
            &[
                // The old listable clause would drop every one of these:
                // local_type 0/2/7, a @chatroom, a gh_ and a system username.
                "INSERT INTO contact VALUES ('sys_type0', 'Type 0', '', '', 0, 0, NULL)",
                "INSERT INTO contact VALUES ('room@chatroom', 'Room', '', '', 0, 2, NULL)",
                "INSERT INTO contact VALUES ('weird_type7', 'Type 7', '', '', 0, 7, NULL)",
                "INSERT INTO contact VALUES ('gh_news', 'News', '', '', 0, 1, NULL)",
                "INSERT INTO contact VALUES ('filehelper', 'File Transfer', '', '', 0, 3, NULL)",
                // Deleted or unset delete_flag rows are not in the roster.
                "INSERT INTO contact VALUES ('deleted_1', 'Deleted', '', '', 1, 3, NULL)",
                "INSERT INTO contact VALUES ('deleted_2', 'Deleted', '', '', 2, 3, NULL)",
                "INSERT INTO contact VALUES ('null_flag', 'Null flag', '', '', NULL, 3, NULL)",
            ],
        );

        let items = collect_contact_snapshot(&root, &roster_keys()).unwrap();

        assert_eq!(
            usernames(&items),
            vec!["sys_type0", "room@chatroom", "weird_type7", "gh_news", "filehelper"],
            "no local_type, group or system-username filter may narrow the roster"
        );
    }

    #[test]
    fn non_text_and_empty_usernames_are_skipped_and_non_text_fields_are_empty() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA_NO_AFFINITY,
            &[
                "INSERT INTO contact VALUES (x'2A2A', 'Blob', '', '', 0, 3)",
                "INSERT INTO contact VALUES (NULL, 'Null', '', '', 0, 3)",
                "INSERT INTO contact VALUES ('', 'Empty', '', '', 0, 3)",
                "INSERT INTO contact VALUES (7, 'Integer', '', '', 0, 3)",
                // A valid username with non-text remark/alias: the fields are
                // empty rather than stringified.
                "INSERT INTO contact VALUES ('valid', 'Valid', 42, x'4142', 0, 3)",
            ],
        );

        let items = collect_contact_snapshot(&root, &roster_keys()).unwrap();

        assert_eq!(usernames(&items), vec!["valid"]);
        assert_eq!(items[0]["remark"], "", "an integer remark is empty, not \"42\"");
        assert_eq!(items[0]["alias"], "", "a blob alias is empty hex, not text");
        assert_eq!(items[0]["display_name"], "Valid", "display falls back to nick_name");
    }

    #[test]
    fn a_repeated_username_fails_the_whole_round() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA,
            &[
                "INSERT INTO contact VALUES ('dup', 'First', '', '', 0, 3, NULL)",
                "INSERT INTO contact VALUES ('ok', 'Fine', '', '', 0, 3, NULL)",
                "INSERT INTO contact VALUES ('dup', 'Second', '', '', 0, 3, NULL)",
            ],
        );

        assert_eq!(
            collect_contact_snapshot(&root, &roster_keys()).unwrap_err(),
            MessageSnapshotError::RowDuplicate,
            "a repeated identity must not be silently de-duplicated"
        );
    }

    #[test]
    fn a_roster_without_the_optional_alias_column_still_reads() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA_NO_ALIAS,
            &["INSERT INTO contact VALUES ('alice', 'Alice', 'Remark', 0)"],
        );

        let items = collect_contact_snapshot(&root, &roster_keys()).unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["alias"], "", "alias is empty when the column is absent");
        assert_eq!(items[0]["remark"], "Remark");
        assert_eq!(items[0]["display_name"], "Remark");
    }

    #[test]
    fn a_missing_required_column_or_table_is_source_unavailable() {
        for (name, schema) in [
            ("remark", "CREATE TABLE contact (username TEXT, nick_name TEXT, delete_flag INTEGER)"),
            ("nick_name", "CREATE TABLE contact (username TEXT, remark TEXT, delete_flag INTEGER)"),
            ("delete_flag", "CREATE TABLE contact (username TEXT, nick_name TEXT, remark TEXT)"),
            ("username", "CREATE TABLE contact (nick_name TEXT, remark TEXT, delete_flag INTEGER)"),
            ("contact", "CREATE TABLE other (username TEXT)"),
        ] {
            let (_dir, root) = roster_root();
            let path = root.join("contact").join("contact.db");
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
            ))
            .unwrap();
            conn.execute_batch(schema).unwrap();
            drop(conn);

            assert_eq!(
                collect_contact_snapshot(&root, &roster_keys()).unwrap_err(),
                MessageSnapshotError::SourceUnavailable,
                "missing {name} must refuse the sample, not return an empty roster"
            );
        }
    }

    #[test]
    fn a_missing_contact_key_is_keys_unavailable_and_an_unreadable_db_is_a_read_failure() {
        let (_dir, root) = roster_root();

        assert_eq!(
            collect_contact_snapshot(&root, &HashMap::new()).unwrap_err(),
            MessageSnapshotError::KeysUnavailable
        );
        // The key is present but the file is not.
        assert_eq!(
            collect_contact_snapshot(&root, &roster_keys()).unwrap_err(),
            MessageSnapshotError::DatabaseRead
        );
    }

    #[test]
    fn the_row_byte_and_per_row_budgets_refuse_without_partial_output() {
        let (_dir, root) = roster_root();
        let path = seed_roster(
            &root,
            ROSTER_SCHEMA,
            &[
                "INSERT INTO contact VALUES ('a1', 'A', '', '', 0, 3, NULL)",
                "INSERT INTO contact VALUES ('b2', 'B', '', '', 0, 3, NULL)",
                "INSERT INTO contact VALUES ('c3', 'C', '', '', 0, 3, NULL)",
            ],
        );

        // Two rows allowed, three present: the +1 probe row trips the budget.
        let over_rows = scan_with_budget(
            &path,
            ContactBudget { max_rows: 2, ..ContactBudget::default() },
        );
        assert_eq!(over_rows.unwrap_err(), MessageSnapshotError::BudgetExceeded);

        // A byte budget too small for even one serialized row.
        let over_bytes = scan_with_budget(
            &path,
            ContactBudget { max_bytes: 4, ..ContactBudget::default() },
        );
        assert_eq!(over_bytes.unwrap_err(), MessageSnapshotError::BudgetExceeded);

        // A single row wider than the per-row budget.
        let over_row = scan_with_budget(
            &path,
            ContactBudget { max_row_bytes: 4, ..ContactBudget::default() },
        );
        assert_eq!(over_row.unwrap_err(), MessageSnapshotError::RowTooLarge);

        // At the limit is accepted, so the budgets are not off by one.
        let at_limit = scan_with_budget(
            &path,
            ContactBudget { max_rows: 3, ..ContactBudget::default() },
        )
        .unwrap();
        assert_eq!(usernames(&at_limit), vec!["a1", "b2", "c3"]);
    }

    #[test]
    fn every_field_is_bounded_in_sql_before_rust_can_read_it() {
        // The SQL seam the whole fix rests on: a field is returned only when its
        // stored byte length fits the cap, and its length is returned alongside.
        for (has_alias, fields) in [
            (false, vec!["username", "remark", "nick_name"]),
            (true, vec!["username", "remark", "nick_name", "alias"]),
        ] {
            let sql = contact_row_sql(has_alias, 4096, 7);
            for field in fields {
                assert!(
                    sql.contains(&format!(
                        "CASE WHEN length(CAST({field} AS BLOB)) <= 4096 THEN {field} ELSE NULL END"
                    )),
                    "field {field} is not bounded before it is returned: {sql}"
                );
                assert!(
                    sql.contains(&format!("length(CAST({field} AS BLOB))")),
                    "field {field} has no length column to refuse it by: {sql}"
                );
            }
            assert!(sql.contains("WHERE delete_flag = 0"));
            assert!(sql.contains("LIMIT 7"));
            if !has_alias {
                assert!(
                    !sql.contains("alias"),
                    "an unaliased schema must not project the optional column: {sql}"
                );
            }
        }
    }

    #[test]
    fn an_oversized_field_is_refused_not_returned_as_empty_or_skipped() {
        let (_dir, root) = roster_root();
        // Wider than the production cap (the row budget). The SQL projection
        // keeps this value out of Rust entirely and the length column refuses it.
        seed_contact(
            &root,
            "wxid_big",
            "Big",
            &"x".repeat(CONTACT_MAX_ROW_BYTES + 64),
        );

        assert_eq!(
            collect_contact_snapshot(&root, &roster_keys()).unwrap_err(),
            MessageSnapshotError::RowTooLarge,
            "an oversized field must fail the row, not be emptied or dropped"
        );
    }

    #[test]
    fn a_modest_field_cap_refuses_in_sql_without_returning_an_empty_field() {
        let (_dir, root) = roster_root();
        // The per-field cap is the row budget; 8 is far below the stored 64.
        let path = seed_contact(&root, "wxid_a", "A", &"y".repeat(64));

        assert_eq!(
            scan_with_budget(
                &path,
                ContactBudget { max_row_bytes: 8, ..ContactBudget::default() },
            )
            .unwrap_err(),
            MessageSnapshotError::RowTooLarge
        );
    }

    #[test]
    fn json_escaping_widening_is_still_charged_after_the_raw_lengths() {
        let (_dir, root) = roster_root();
        // 40 quote characters: a raw length of 40, but the JSON form escapes each
        // into two characters, so the serialized row is far above 50 while the
        // raw sum stays below it. The final budget is what must refuse this.
        let path = seed_contact(&root, "u", "n", &"\"".repeat(40));

        assert_eq!(
            scan_with_budget(
                &path,
                ContactBudget { max_row_bytes: 50, ..ContactBudget::default() },
            )
            .unwrap_err(),
            MessageSnapshotError::RowTooLarge
        );
    }

    #[test]
    fn chinese_text_is_preserved_verbatim() {
        let (_dir, root) = roster_root();
        seed_contact(&root, "wxid_张三", "昵称", "备注 张三");

        let items = collect_contact_snapshot(&root, &roster_keys()).unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["username"], "wxid_张三");
        assert_eq!(items[0]["nick_name"], "昵称");
        assert_eq!(items[0]["remark"], "备注 张三");
        assert_eq!(items[0]["alias"], "");
        assert_eq!(items[0]["display_name"], "备注 张三");
    }

    #[test]
    fn a_store_encrypted_with_a_different_key_is_a_read_failure() {
        let (_dir, root) = roster_root();
        seed_contact(&root, "alice", "Alice", "");

        // A well-formed but wrong 32-byte key: the file exists and decrypts to
        // nothing usable, which must not look like an empty roster.
        let wrong = HashMap::from([("contact.db".to_string(), "ab".repeat(32))]);
        assert_eq!(
            collect_contact_snapshot(&root, &wrong).unwrap_err(),
            MessageSnapshotError::DatabaseRead
        );
    }

    /// Append one live row whose `field` column stores bytes that are not valid
    /// UTF-8 (`x'FFFE'`), with an otherwise usable row.
    fn append_invalid_utf8_row(path: &str, field: &str) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
        ))
        .unwrap();
        let bad = "CAST(x'FFFE' AS TEXT)";
        let (username, nick_name, remark) = match field {
            "username" => (bad, "'Nick'", "''"),
            "nick_name" => ("'wxid_bad'", bad, "''"),
            "remark" => ("'wxid_bad'", "'Nick'", bad),
            other => panic!("unsupported field {other}"),
        };
        conn.execute(
            &format!(
                "INSERT INTO contact (username, nick_name, remark, alias, delete_flag, local_type) \
                 VALUES ({username}, {nick_name}, {remark}, '', 0, 3)"
            ),
            [],
        )
        .unwrap();
    }

    #[test]
    fn invalid_utf8_in_a_text_field_fails_the_whole_round() {
        for field in ["username", "nick_name", "remark"] {
            let (_dir, root) = roster_root();
            // A valid row is collected first, so a partial result would be
            // observable if the refusal were not whole-round.
            let path = seed_contact(&root, "wxid_ok", "OK", "");
            append_invalid_utf8_row(&path, field);

            assert_eq!(
                collect_contact_snapshot(&root, &roster_keys()).unwrap_err(),
                MessageSnapshotError::RowInvalid,
                "invalid UTF-8 in {field} must fail the round, not be replaced or skipped"
            );
        }
    }

    #[test]
    fn a_deadline_that_already_passed_reads_no_row() {
        let (_dir, root) = roster_root();
        seed_roster(
            &root,
            ROSTER_SCHEMA,
            &["INSERT INTO contact VALUES ('alice', 'Alice', '', '', 0, 3, NULL)"],
        );

        let cancel = CollectionCancel::within(Duration::from_millis(0));
        assert_eq!(
            collect_contact_snapshot_until(&root, &roster_keys(), &cancel).unwrap_err(),
            MessageSnapshotError::DeadlineExceeded,
            "an expired round must not return a partial roster"
        );
    }

    /// An encrypted `contact.db` whose newest commit is provably still in the
    /// `-wal` file: `wal_autocheckpoint = 0` stops commit-time checkpoints and
    /// the writer stays open, so SQLite never sees the "last connection closed"
    /// trigger. The `-wal` size is the guard.
    struct WalRoster {
        _dir: tempfile::TempDir,
        root: PathBuf,
        writer: Connection,
    }

    impl WalRoster {
        fn new() -> Self {
            let (dir, root) = roster_root();
            let path = root.join("contact").join("contact.db");
            let writer = Connection::open(&path).unwrap();
            writer
                .execute_batch(&format!(
                    "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                     PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                     {ROSTER_SCHEMA}"
                ))
                .unwrap();
            writer
                .execute(
                    "INSERT INTO contact VALUES ('checkpointed', 'A', '', '', 0, 3, NULL)",
                    [],
                )
                .unwrap();
            let busy: i64 = writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                .unwrap();
            assert_eq!(busy, 0, "fixture checkpoint must not be blocked");
            writer
                .execute(
                    "INSERT INTO contact VALUES ('wal_only', 'B', '', '', 0, 3, NULL)",
                    [],
                )
                .unwrap();
            Self { _dir: dir, root, writer }
        }

        fn path(&self) -> PathBuf {
            self.root.join("contact").join("contact.db")
        }

        fn wal_len(&self) -> u64 {
            std::fs::metadata(self.path().with_extension("db-wal"))
                .map(|meta| meta.len())
                .unwrap_or(0)
        }

        fn main_len(&self) -> u64 {
            std::fs::metadata(self.path()).map(|meta| meta.len()).unwrap_or(0)
        }

        fn journal_mode(&self) -> String {
            self.writer
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap()
        }
    }

    #[test]
    fn rows_committed_only_to_the_write_ahead_log_are_collected() {
        let fixture = WalRoster::new();
        assert!(fixture.wal_len() > 32, "fixture guard: the newest commit must stay in the -wal");
        let main_before = fixture.main_len();

        let items = collect_contact_snapshot(&fixture.root, &roster_keys()).unwrap();

        assert_eq!(usernames(&items), vec!["checkpointed", "wal_only"]);
        assert_eq!(fixture.journal_mode(), "wal", "the read must not change journal_mode");
        assert_eq!(fixture.main_len(), main_before, "the read must not checkpoint the -wal");
    }
}

#[cfg(test)]
mod contact_contract_tests {
    use super::*;
    use crate::ia::types::ContactKind;
    use rusqlite::Connection;
    use serde_json::Value;
    use std::path::Path;

    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    fn seed_contract_db(root: &Path) {
        let dir = root.join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE contact (
                username TEXT, nick_name TEXT, remark TEXT, alias TEXT,
                delete_flag INTEGER, local_type INTEGER, verify_flag INTEGER
             );
             CREATE TABLE biz_info (username TEXT, type INTEGER);
             INSERT INTO contact VALUES
                ('custom-service', 'Service', '  Service Remark  ', 'svc', 0, 1, 7);
             INSERT INTO biz_info VALUES ('custom-service', 1);
             INSERT INTO contact VALUES ('wxid_subscription', '', '', NULL, 0, 1, 8);
             INSERT INTO biz_info VALUES ('wxid_subscription', 0);
             INSERT INTO contact VALUES ('room@chatroom', 'Room', '', '', 0, 2, 0);
             INSERT INTO contact VALUES ('filehelper', 'File Helper', '', '', 0, 0, 0);
             INSERT INTO contact VALUES ('wxid_other_official', '', '', '', 0, 1, 9);
             INSERT INTO biz_info VALUES ('wxid_other_official', 2);
             INSERT INTO contact VALUES ('opaque-source', '', '', '', 0, 91, NULL);
             INSERT INTO contact VALUES ('deleted-source', 'Deleted', '', '', 1, 1, 0);
             INSERT INTO biz_info VALUES ('deleted-source', 1);"
        ))
        .unwrap();
    }

    fn test_keys() -> HashMap<String, String> {
        HashMap::from([("contact.db".to_string(), TEST_KEY.to_string())])
    }

    #[test]
    fn contact_contract_classifies_sources_before_fallback_types() {
        assert_eq!(
            classify_contact_v2("custom-service", 1, Some(1)),
            ContactKind::Official
        );
        assert_eq!(
            classify_contact_v2("wxid_subscription", 1, Some(0)),
            ContactKind::Official
        );
        assert_eq!(
            classify_contact_v2("room@chatroom", 2, None),
            ContactKind::Group
        );
        assert_eq!(
            classify_contact_v2("filehelper", 0, None),
            ContactKind::System
        );
        assert_eq!(
            classify_contact_v2("member@openim", 3, None),
            ContactKind::Enterprise
        );
        assert_eq!(
            classify_contact_v2("wxid_subscription_2", 1, Some(2)),
            ContactKind::Official
        );
        assert_eq!(
            classify_contact_v2("wxid_mapped_subscription", 5, Some(0)),
            ContactKind::Official
        );
        assert_eq!(
            classify_contact_v2("opaque-source", 91, None),
            ContactKind::Unknown
        );
    }

    #[test]
    fn contact_contract_zero_type_needs_positive_kind_evidence() {
        for username in ["ordinary-contact", "wxid_ordinary_fixture"] {
            assert_eq!(classify_contact_v2(username, 0, None), ContactKind::Unknown);
        }
        for username in V2_SYSTEM_USERNAMES {
            assert_eq!(classify_contact_v2(username, 0, None), ContactKind::System);
        }
        assert_eq!(classify_contact_v2("room@chatroom", 0, Some(1)), ContactKind::Group);
        assert_eq!(classify_contact_v2("member@openim", 0, Some(1)), ContactKind::Enterprise);
        for biz_type in [0, 1, 2, 3] {
            assert_eq!(classify_contact_v2("ordinary-contact", 0, Some(biz_type)), ContactKind::Official);
        }
    }

    #[test]
    fn contact_contract_collects_source_types_and_keeps_all_live_contacts() {
        let root = tempfile::tempdir().unwrap();
        seed_contract_db(root.path());
        let conn = Connection::open(root.path().join("contact/contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             INSERT INTO contact VALUES ('ordinary-zero', 'Person', '', '', 0, 0, 0);"
        )).unwrap();
        drop(conn);

        let contacts = collect_contact_snapshot_v2(root.path(), &test_keys()).unwrap();

        assert_eq!(contacts.len(), 7);
        let ordinary = contacts.iter().find(|c| c.username == "ordinary-zero").unwrap();
        assert_eq!(ordinary.kind, ContactKind::Unknown);
        assert_eq!(ordinary.local_type, 0);
        assert_eq!(ordinary.official_type, None);
        assert_eq!(ordinary.verify_flag, Some(0));
        let service = contacts.iter().find(|c| c.username == "custom-service").unwrap();
        assert_eq!(service.kind, ContactKind::Official);
        assert_eq!(service.official_type, Some(1));
        assert_eq!(service.verify_flag, Some(7));
        assert_eq!(service.alias, "svc");
        assert_eq!(service.remark, "  Service Remark  ");
        assert_eq!(service.display_name, "Service Remark");
        let subscription = contacts.iter().find(|c| c.username == "wxid_subscription").unwrap();
        assert_eq!(subscription.kind, ContactKind::Official);
        assert_eq!(subscription.official_type, Some(0));
        assert_eq!(subscription.alias, "");
        assert_eq!(subscription.display_name, "wxid_subscription");
        assert_eq!(
            contacts.iter().find(|c| c.username == "room@chatroom").unwrap().kind,
            ContactKind::Group
        );
        assert_eq!(
            contacts.iter().find(|c| c.username == "filehelper").unwrap().kind,
            ContactKind::System
        );
        let other = contacts.iter().find(|c| c.username == "wxid_other_official").unwrap();
        assert_eq!(other.kind, ContactKind::Official);
        assert_eq!(other.official_type, Some(2));
        let unknown = contacts.iter().find(|c| c.username == "opaque-source").unwrap();
        assert_eq!(unknown.kind, ContactKind::Unknown);
        assert_eq!(unknown.local_type, 91);
        assert!(!contacts.iter().any(|c| c.username == "deleted-source"));

        let json = serde_json::to_value(service).unwrap();
        assert_eq!(json["username"], Value::String("custom-service".into()));
        assert_eq!(json["kind"], Value::String("official".into()));
        assert!(json.get("official_type").is_some());
        assert!(json.get("local_type").is_some());
        assert!(json.get("localType").is_none());
        assert_eq!(json["verify_flag"], 7);
    }

    #[test]
    fn contact_contract_fails_closed_when_business_schema_is_unavailable() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE contact (
                username TEXT, nick_name TEXT, remark TEXT, alias TEXT,
                delete_flag INTEGER, local_type INTEGER
             );
             INSERT INTO contact VALUES ('wxid_service', 'Service', '', '', 0, 1);"
        ))
        .unwrap();
        drop(conn);

        assert_eq!(
            collect_contact_snapshot_v2(root.path(), &test_keys()).unwrap_err(),
            MessageSnapshotError::SourceUnavailable,
            "a failed biz_info read must not silently classify every account as an individual"
        );
    }

    #[test]
    fn contact_contract_treats_absent_alias_and_verify_flag_as_optional_source_columns() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE contact (
                username TEXT, nick_name TEXT, remark TEXT,
                delete_flag INTEGER, local_type INTEGER
             );
             CREATE TABLE biz_info (username TEXT, type INTEGER);
             INSERT INTO contact VALUES ('wxid_plain', 'Plain', '', 0, 1);"
        ))
        .unwrap();
        drop(conn);

        let contacts = collect_contact_snapshot_v2(root.path(), &test_keys()).unwrap();
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].alias, "");
        assert_eq!(contacts[0].verify_flag, None);
    }

    #[test]
    fn contact_contract_skips_empty_and_non_text_usernames_like_the_roster() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE contact (
                username, nick_name TEXT, remark TEXT, alias TEXT,
                delete_flag INTEGER, local_type INTEGER
             );
             CREATE TABLE biz_info (username TEXT, type INTEGER);
             INSERT INTO contact VALUES (NULL, 'Null', '', '', 0, 1);
             INSERT INTO contact VALUES ('', 'Empty', '', '', 0, 1);
             INSERT INTO contact VALUES (17, 'Integer', '', '', 0, 1);
             INSERT INTO contact VALUES (x'4142', 'Blob', '', '', 0, 1);
             INSERT INTO contact VALUES ('valid-user', 'Valid', '', '', 0, 1);"
        ))
        .unwrap();
        drop(conn);

        let contacts = collect_contact_snapshot_v2(root.path(), &test_keys()).unwrap();
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].username, "valid-user");
    }

    #[test]
    fn contact_contract_bounds_metadata_and_serialized_v2_samples() {
        let root = tempfile::tempdir().unwrap();
        seed_contract_db(root.path());

        let overlong = collect_contact_snapshot_v2_with_budget(
            root.path(),
            &test_keys(),
            ContactBudget {
                max_row_bytes: 8,
                ..ContactBudget::default()
            },
        );
        assert_eq!(overlong.unwrap_err(), MessageSnapshotError::RowTooLarge);

        let over_metadata_bytes = collect_contact_snapshot_v2_with_budget(
            root.path(),
            &test_keys(),
            ContactBudget {
                max_bytes: 1,
                max_row_bytes: 256,
                ..ContactBudget::default()
            },
        );
        assert_eq!(
            over_metadata_bytes.unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );

        let over_metadata_rows = collect_contact_snapshot_v2_with_budget(
            root.path(),
            &test_keys(),
            ContactBudget {
                max_rows: 1,
                max_row_bytes: 256,
                ..ContactBudget::default()
            },
        );
        assert_eq!(
            over_metadata_rows.unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );

        let over_serialized_row = collect_contact_snapshot_v2_with_budget(
            root.path(),
            &test_keys(),
            ContactBudget {
                max_row_bytes: 64,
                ..ContactBudget::default()
            },
        );
        assert_eq!(
            over_serialized_row.unwrap_err(),
            MessageSnapshotError::RowTooLarge
        );

        let over_serialized_sample = collect_contact_snapshot_v2_with_budget(
            root.path(),
            &test_keys(),
            ContactBudget {
                max_bytes: 120,
                max_row_bytes: 1024,
                ..ContactBudget::default()
            },
        );
        assert_eq!(
            over_serialized_sample.unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );
    }
}
