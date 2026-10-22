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
