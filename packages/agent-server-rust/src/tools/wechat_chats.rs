use super::wechat_db::{get_db_path, try_query_wechat_db, WechatReadError};
use crate::ia::types::Chat;
use crate::ia::types::ChatV2;
use crate::tools::wechat_contacts::{contact_field_projection, ContactBudget};
use crate::tools::wechat_messages::{open_shard_readonly, CollectionCancel, MessageSnapshotError};
use rusqlite::types::ValueRef;
use std::collections::HashMap;
use std::path::Path;

/// Read the bounded session roster and its contact display fields from isolated
/// database copies. `username` remains the source identity; display and sender
/// names are separate fields.
pub(crate) fn list_chat_snapshot_v2(
    database_root: &Path,
    keys: &HashMap<String, String>,
    cancel: &CollectionCancel,
) -> Result<Vec<ChatV2>, MessageSnapshotError> {
    cancel.check()?;
    let session_key = keys.get("session.db").ok_or(MessageSnapshotError::KeysUnavailable)?;
    keys.get("contact.db").ok_or(MessageSnapshotError::KeysUnavailable)?;
    let contacts = crate::tools::wechat_contacts::collect_contact_snapshot_v2_until(database_root, keys, cancel)?;
    let mut contacts_by_username = HashMap::with_capacity(contacts.len());
    for contact in contacts {
        contacts_by_username.insert(contact.username.clone(), contact);
    }

    let conn = open_shard_readonly(&database_root.join("session").join("session.db"), session_key, cancel)?;
    let budget = ContactBudget::default();
    let username_projection = contact_field_projection("username", budget.max_row_bytes);
    let summary_projection = contact_field_projection("summary", budget.max_row_bytes);
    let sender_projection = contact_field_projection("last_msg_sender", budget.max_row_bytes);
    let sender_name_projection = contact_field_projection("last_sender_display_name", budget.max_row_bytes);
    let sql = format!(
        "SELECT {username_projection}, unread_count, {summary_projection}, last_timestamp, \
         {sender_projection}, {sender_name_projection}, last_msg_locald_id \
         FROM SessionTable WHERE is_hidden = 0 AND typeof(username) = 'text' \
         AND length(CAST(username AS BLOB)) > 0 ORDER BY sort_timestamp DESC LIMIT {}",
        budget.max_rows.saturating_add(1)
    );
    let mut statement = conn.prepare(&sql).map_err(|_| crate::tools::wechat_messages::read_outcome(cancel))?;
    let mut rows = statement.query([]).map_err(|_| crate::tools::wechat_messages::read_outcome(cancel))?;
    let mut chats = Vec::new();
    let mut sample_bytes = 0usize;
    while let Some(row) = rows.next().map_err(|_| crate::tools::wechat_messages::read_outcome(cancel))? {
        cancel.check()?;
        if chats.len() >= budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        let username_bytes = row.get::<_, i64>(1).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let summary_bytes = row.get::<_, Option<i64>>(4).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let sender_bytes = row.get::<_, Option<i64>>(7).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let sender_name_bytes = row.get::<_, Option<i64>>(9).map_err(|_| MessageSnapshotError::RowInvalid)?;
        for length in [Some(username_bytes), summary_bytes, sender_bytes, sender_name_bytes].into_iter().flatten() {
            if usize::try_from(length).unwrap_or(usize::MAX) > budget.max_row_bytes {
                return Err(MessageSnapshotError::RowTooLarge);
            }
        }
        let username = required_text(row.get_ref(0).map_err(|_| MessageSnapshotError::RowInvalid)?)?;
        let unread_count = row.get::<_, Option<i64>>(2).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let summary = optional_text(row.get_ref(3).map_err(|_| MessageSnapshotError::RowInvalid)?)?;
        let last_timestamp = row.get::<_, Option<i64>>(5).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let sender_username = optional_text(row.get_ref(6).map_err(|_| MessageSnapshotError::RowInvalid)?)?;
        let sender_display = optional_text(row.get_ref(8).map_err(|_| MessageSnapshotError::RowInvalid)?)?;
        let last_msg_local_id = row.get::<_, Option<i64>>(10).map_err(|_| MessageSnapshotError::RowInvalid)?;
        let contact = contacts_by_username.get(&username);
        let (remark, nick_name, display_name) = contact.map_or(("", "", username.as_str()), |c| {
            (c.remark.as_str(), c.nick_name.as_str(), c.display_name.as_str())
        });
        let chat = ChatV2 {
            username: username.clone(),
            display_name: display_name.to_string(),
            remark: remark.to_string(),
            nick_name: nick_name.to_string(),
            unread_count,
            is_group: username.ends_with("@chatroom"),
            last_message_preview: summary.filter(|value| !value.is_empty()),
            last_msg_sender: sender_username.filter(|value| !value.is_empty()),
            last_sender_display_name: sender_display.filter(|value| !value.is_empty()),
            last_activity_at: last_timestamp.filter(|&timestamp| timestamp > 0).and_then(|timestamp| {
                chrono::DateTime::from_timestamp(timestamp, 0).map(|dt| dt.to_rfc3339())
            }),
            last_msg_local_id,
        };
        let encoded = serde_json::to_vec(&chat).map_err(|_| MessageSnapshotError::RowInvalid)?;
        if encoded.len() > budget.max_row_bytes {
            return Err(MessageSnapshotError::RowTooLarge);
        }
        sample_bytes = sample_bytes.saturating_add(encoded.len());
        if sample_bytes > budget.max_bytes {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        chats.push(chat);
    }
    Ok(chats)
}

fn required_text(value: ValueRef<'_>) -> Result<String, MessageSnapshotError> {
    match value {
        ValueRef::Text(bytes) => String::from_utf8(bytes.to_vec()).map_err(|_| MessageSnapshotError::RowInvalid),
        _ => Err(MessageSnapshotError::RowInvalid),
    }
}

fn optional_text(value: ValueRef<'_>) -> Result<Option<String>, MessageSnapshotError> {
    match value {
        ValueRef::Null => Ok(None),
        ValueRef::Text(bytes) => String::from_utf8(bytes.to_vec()).map(Some).map_err(|_| MessageSnapshotError::RowInvalid),
        _ => Err(MessageSnapshotError::RowInvalid),
    }
}

const SESSION_COLUMNS: &str = "username, type, unread_count, summary, draft, last_timestamp,
                    sort_timestamp, last_msg_sender, last_sender_display_name, is_hidden,
                    last_msg_locald_id";

const CONTACT_COLUMNS: &str = "username, nick_name, remark, alias, small_head_url, local_type";

/// Build a chat from a session row, using the matching contact row for display fields.
fn chat_from_rows(
    username: &str,
    session: &serde_json::Value,
    contact: Option<&serde_json::Value>,
) -> Chat {
    let is_group = username.contains("@chatroom");

    let name = contact
        .and_then(|c| c.get("remark").and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
        .or_else(|| {
            contact.and_then(|c| {
                c.get("nick_name")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
            })
        })
        .unwrap_or(username)
        .to_string();

    let remark = contact
        .and_then(|c| c.get("remark").and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
        .map(String::from);

    let unread_count = session
        .get("unread_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;

    let last_message_preview = session
        .get("summary")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);

    let last_message_sender = session
        .get("last_sender_display_name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);

    let last_activity_at = session
        .get("last_timestamp")
        .and_then(|v| v.as_i64())
        .filter(|&t| t > 0)
        .map(|t| {
            chrono::DateTime::from_timestamp(t, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default()
        });

    let last_msg_local_id = session
        .get("last_msg_locald_id")
        .and_then(|v| v.as_i64());

    Chat {
        id: username.to_string(),
        username: username.to_string(),
        name,
        remark,
        unread_count,
        is_group,
        last_message_preview,
        last_message_sender,
        last_activity_at,
        last_msg_local_id,
    }
}

/// List chats by querying WeChat's session.db and contact.db.
pub fn list_chats(
    account_dir: &str,
    keys: &HashMap<String, String>,
    limit: i64,
    offset: i64,
) -> Result<Vec<Chat>, WechatReadError> {
    let session_key = keys
        .get("session.db")
        .ok_or(WechatReadError::KeysUnavailable)?;
    let contact_key = keys
        .get("contact.db")
        .ok_or(WechatReadError::KeysUnavailable)?;

    list_chats_from_dbs(
        &get_db_path(account_dir, "session.db"),
        session_key,
        &get_db_path(account_dir, "contact.db"),
        contact_key,
        limit,
        offset,
    )
}

fn list_chats_from_dbs(
    session_db: &str,
    session_key: &str,
    contact_db: &str,
    contact_key: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Chat>, WechatReadError> {
    let sessions = try_query_wechat_db(
        session_db,
        session_key,
        &format!(
            "SELECT {SESSION_COLUMNS}
             FROM SessionTable
             WHERE is_hidden = 0
             ORDER BY sort_timestamp DESC
             LIMIT {limit} OFFSET {offset};"
        ),
    )?;

    if sessions.is_empty() {
        return Ok(Vec::new());
    }

    // Batch lookup contacts
    let usernames: Vec<String> = sessions
        .iter()
        .filter_map(|s| s.get("username").and_then(|v| v.as_str()).map(String::from))
        .collect();

    let mut contact_map: HashMap<String, serde_json::Value> = HashMap::new();
    for chunk in usernames.chunks(50) {
        let placeholders: String = chunk
            .iter()
            .map(|u| format!("'{}'", u.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");

        let contacts = try_query_wechat_db(
            contact_db,
            contact_key,
            &format!(
                "SELECT {CONTACT_COLUMNS}
                 FROM contact
                 WHERE username IN ({placeholders});"
            ),
        )?;

        for c in contacts {
            if let Some(u) = c.get("username").and_then(|v| v.as_str()) {
                contact_map.insert(u.to_string(), c);
            }
        }
    }

    Ok(sessions
        .iter()
        .filter_map(|session| {
            let username = session.get("username")?.as_str()?.to_string();
            let mut chat = chat_from_rows(
                &username,
                session,
                contact_map.get(&username),
            );
            // Only the list endpoint historically falls back to the raw sender.
            if chat.last_message_sender.is_none() {
                chat.last_message_sender = session
                    .get("last_msg_sender")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from);
            }
            Some(chat)
        })
        .collect())
}

/// Find a chat by WeChat username (exact match).
pub fn get_chat_by_username(
    account_dir: &str,
    keys: &HashMap<String, String>,
    username: &str,
) -> Result<Option<Chat>, WechatReadError> {
    let session_key = keys
        .get("session.db")
        .ok_or(WechatReadError::KeysUnavailable)?;
    let contact_key = keys
        .get("contact.db")
        .ok_or(WechatReadError::KeysUnavailable)?;

    get_chat_by_username_from_dbs(
        &get_db_path(account_dir, "session.db"),
        session_key,
        &get_db_path(account_dir, "contact.db"),
        contact_key,
        username,
    )
}

fn get_chat_by_username_from_dbs(
    session_db: &str,
    session_key: &str,
    contact_db: &str,
    contact_key: &str,
    username: &str,
) -> Result<Option<Chat>, WechatReadError> {
    let escaped = username.replace('\'', "''");

    let sessions = try_query_wechat_db(
        session_db,
        session_key,
        &format!(
            "SELECT {SESSION_COLUMNS}
             FROM SessionTable
             WHERE username = '{escaped}';"
        ),
    )?;

    let session = match sessions.first() {
        Some(s) => s,
        None => return Ok(None),
    };

    let contacts = try_query_wechat_db(
        contact_db,
        contact_key,
        &format!(
            "SELECT {CONTACT_COLUMNS}
             FROM contact
             WHERE username = '{escaped}';"
        ),
    )?;

    Ok(Some(chat_from_rows(username, session, contacts.first())))
}

/// Find chats by name (partial match).
pub fn find_chats_by_name(
    account_dir: &str,
    keys: &HashMap<String, String>,
    query: &str,
) -> Result<Vec<Chat>, WechatReadError> {
    let contact_key = keys
        .get("contact.db")
        .ok_or(WechatReadError::KeysUnavailable)?;
    let session_key = keys
        .get("session.db")
        .ok_or(WechatReadError::KeysUnavailable)?;

    find_chats_by_name_from_dbs(
        &get_db_path(account_dir, "contact.db"),
        contact_key,
        &get_db_path(account_dir, "session.db"),
        session_key,
        query,
    )
}

fn find_chats_by_name_from_dbs(
    contact_db: &str,
    contact_key: &str,
    session_db: &str,
    session_key: &str,
    query: &str,
) -> Result<Vec<Chat>, WechatReadError> {
    let escaped = query.replace('\'', "''");

    let contacts = try_query_wechat_db(
        contact_db,
        contact_key,
        &format!(
            "SELECT {CONTACT_COLUMNS}
             FROM contact
             WHERE nick_name LIKE '%{escaped}%'
                OR remark LIKE '%{escaped}%'
                OR username LIKE '%{escaped}%'
             LIMIT 20;"
        ),
    )?;

    if contacts.is_empty() {
        return Ok(Vec::new());
    }

    let usernames: String = contacts
        .iter()
        .filter_map(|c| c.get("username").and_then(|v| v.as_str()))
        .map(|u| format!("'{}'", u.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");

    let sessions = try_query_wechat_db(
        session_db,
        session_key,
        &format!(
            "SELECT {SESSION_COLUMNS}
             FROM SessionTable
             WHERE username IN ({usernames})
             ORDER BY sort_timestamp DESC;"
        ),
    )?;

    let session_map: HashMap<String, &serde_json::Value> = sessions
        .iter()
        .filter_map(|s| {
            s.get("username")
                .and_then(|v| v.as_str())
                .map(|u| (u.to_string(), s))
        })
        .collect();

    Ok(contacts
        .iter()
        .filter_map(|contact| {
            let username = contact.get("username")?.as_str()?;
            let session = session_map.get(username)?;
            Some(chat_from_rows(username, session, Some(contact)))
        })
        .collect())
}
