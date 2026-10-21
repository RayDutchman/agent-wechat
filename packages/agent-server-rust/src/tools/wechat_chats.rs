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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::wechat_messages::CollectionCancel;
    use rusqlite::Connection;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::time::Duration;

    /// 32-byte key in the hex form WeChat stores it as.
    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    const SESSION_SCHEMA: &str = "
        CREATE TABLE SessionTable (
            username TEXT PRIMARY KEY,
            type INTEGER,
            unread_count INTEGER,
            summary TEXT,
            draft TEXT,
            last_timestamp INTEGER,
            sort_timestamp INTEGER,
            last_msg_sender TEXT,
            last_sender_display_name TEXT,
            is_hidden INTEGER,
            last_msg_locald_id INTEGER
        );
    ";

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

    #[test]
    fn v2_chat_snapshot_keeps_username_separate_from_display_and_sender_names() {
        let root = tempfile::tempdir().unwrap();
        let contact_dir = root.path().join("contact");
        let session_dir = root.path().join("session");
        std::fs::create_dir_all(&contact_dir).unwrap();
        std::fs::create_dir_all(&session_dir).unwrap();
        let contact_path = contact_dir.join("contact.db");
        create_db(
            &contact_path,
            TEST_KEY,
            "CREATE TABLE contact (username TEXT, nick_name TEXT, remark TEXT, alias TEXT, local_type INTEGER, delete_flag INTEGER, verify_flag INTEGER); CREATE TABLE biz_info (username TEXT, type INTEGER);",
            &["INSERT INTO contact VALUES ('chat-user', 'Global', 'Work', 'alias', 1, 0, NULL)"],
        );
        let session_path = session_dir.join("session.db");
        create_db(
            &session_path,
            TEST_KEY,
            SESSION_SCHEMA,
            &["INSERT INTO SessionTable VALUES ('chat-user', 77, 2, 'preview', '', 1700000000, 1700000000, 'sender-id', 'Sender display', 0, 9)"],
        );
        let keys = keys(&[("contact.db", TEST_KEY), ("session.db", TEST_KEY)]);

        let chats = list_chat_snapshot_v2(root.path(), &keys, &CollectionCancel::within(Duration::from_secs(5))).unwrap();
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].username, "chat-user");
        assert_eq!(chats[0].display_name, "Work");
        assert_eq!(chats[0].last_msg_sender.as_deref(), Some("sender-id"));
        assert_eq!(chats[0].last_sender_display_name.as_deref(), Some("Sender display"));
        assert_eq!(chats[0].unread_count, Some(2));
        assert_eq!(chats[0].last_msg_local_id, Some(9));
    }

    #[test]
    fn v2_chat_snapshot_preserves_null_unread_and_composite_sender_verbatim() {
        let root = tempfile::tempdir().unwrap();
        let contact_dir = root.path().join("contact");
        let session_dir = root.path().join("session");
        std::fs::create_dir_all(&contact_dir).unwrap();
        std::fs::create_dir_all(&session_dir).unwrap();
        create_db(&contact_dir.join("contact.db"), TEST_KEY,
            "CREATE TABLE contact (username TEXT, nick_name TEXT, remark TEXT, alias TEXT, local_type INTEGER, delete_flag INTEGER, verify_flag INTEGER); CREATE TABLE biz_info (username TEXT, type INTEGER);", &[]);
        create_db(&session_dir.join("session.db"), TEST_KEY, SESSION_SCHEMA,
            &["INSERT INTO SessionTable VALUES ('chat-user', 77, NULL, 'preview', '', 1700000000, 1700000000, '_$_CUSTOM_USERNAME_PREFIX_$_1234567890123456789:gh_fixture', 'A; B', 0, 9)"]);
        let keys = keys(&[("contact.db", TEST_KEY), ("session.db", TEST_KEY)]);
        let chats = list_chat_snapshot_v2(root.path(), &keys, &CollectionCancel::within(Duration::from_secs(5))).unwrap();
        assert_eq!(chats[0].unread_count, None);
        assert_eq!(chats[0].last_msg_sender.as_deref(), Some("_$_CUSTOM_USERNAME_PREFIX_$_1234567890123456789:gh_fixture"));
        assert_eq!(chats[0].last_sender_display_name.as_deref(), Some("A; B"));
    }

    fn keys(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Chat does not implement PartialEq, so compare the error, not the whole Result.
    fn assert_read_err<T: std::fmt::Debug>(
        result: Result<T, WechatReadError>,
        expected: WechatReadError,
    ) {
        assert_eq!(result.unwrap_err(), expected);
    }

    fn create_db(path: &Path, key: &str, schema: &str, rows: &[&str]) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{key}'\"; PRAGMA cipher_compatibility = 4;"
        ))
        .unwrap();
        conn.execute_batch(schema).unwrap();
        for row in rows {
            conn.execute(row, []).unwrap();
        }
    }

    /// A path inside `dir` that does not exist, standing in for an unreadable database.
    fn absent_db_path(dir: &Path, name: &str) -> String {
        let path: PathBuf = dir.join(name);
        path.to_str().unwrap().to_string()
    }

    /// An account directory that cannot exist, so resolved database paths are unreadable.
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

    struct Fixture {
        _dir: tempfile::TempDir,
        session_db: String,
        contact_db: String,
    }

    fn fixture(session_rows: &[&str], contact_rows: &[&str]) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let session_path = dir.path().join("session.db");
        let contact_path = dir.path().join("contact.db");
        create_db(&session_path, TEST_KEY, SESSION_SCHEMA, session_rows);
        create_db(&contact_path, TEST_KEY, CONTACT_SCHEMA, contact_rows);
        Fixture {
            _dir: dir,
            session_db: session_path.to_str().unwrap().to_string(),
            contact_db: contact_path.to_str().unwrap().to_string(),
        }
    }

    const ALICE_SESSION: &str = "INSERT INTO SessionTable VALUES
        ('wxid_alice', 1, 3, 'hi there', '', 1700000000, 1700000000, 'wxid_alice', 'Alice', 0, 42)";
    const ALICE_CONTACT: &str =
        "INSERT INTO contact VALUES ('wxid_alice', 'Alice', 'Alice Remark', '', '', 3)";

    #[test]
    fn only_chat_list_falls_back_to_raw_sender_when_display_name_is_empty() {
        let session = ALICE_SESSION.replace("'Alice', 0, 42", "'', 0, 42");
        let f = fixture(&[&session], &[ALICE_CONTACT]);
        let listed = list_chats_from_dbs(
            &f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, 50, 0,
        ).unwrap();
        assert_eq!(listed[0].last_message_sender.as_deref(), Some("wxid_alice"));
        let single = get_chat_by_username_from_dbs(
            &f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, "wxid_alice",
        ).unwrap().unwrap();
        assert_eq!(single.last_message_sender, None);
        let found = find_chats_by_name_from_dbs(
            &f.contact_db, TEST_KEY, &f.session_db, TEST_KEY, "Alice",
        ).unwrap();
        assert_eq!(found[0].last_message_sender, None);
    }

    #[test]
    fn list_chats_without_both_keys_reports_keys_unavailable() {
        let account = absent_account_dir();

        assert_read_err(
            list_chats(&account, &keys(&[]), 50, 0),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            list_chats(&account, &keys(&[("session.db", TEST_KEY)]), 50, 0),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            list_chats(&account, &keys(&[("contact.db", TEST_KEY)]), 50, 0),
            WechatReadError::KeysUnavailable,
        );
    }

    #[test]
    fn get_chat_without_both_keys_reports_keys_unavailable() {
        let account = absent_account_dir();

        assert_read_err(
            get_chat_by_username(&account, &keys(&[]), "wxid_alice"),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            get_chat_by_username(&account, &keys(&[("session.db", TEST_KEY)]), "wxid_alice"),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            get_chat_by_username(&account, &keys(&[("contact.db", TEST_KEY)]), "wxid_alice"),
            WechatReadError::KeysUnavailable,
        );
    }

    #[test]
    fn find_chats_without_both_keys_reports_keys_unavailable() {
        let account = absent_account_dir();

        assert_read_err(
            find_chats_by_name(&account, &keys(&[]), "alice"),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            find_chats_by_name(&account, &keys(&[("contact.db", TEST_KEY)]), "alice"),
            WechatReadError::KeysUnavailable,
        );
        assert_read_err(
            find_chats_by_name(&account, &keys(&[("session.db", TEST_KEY)]), "alice"),
            WechatReadError::KeysUnavailable,
        );
    }

    #[test]
    fn unreadable_databases_report_database_read() {
        let account = absent_account_dir();
        let both = keys(&[("session.db", TEST_KEY), ("contact.db", TEST_KEY)]);

        assert_read_err(list_chats(&account, &both, 50, 0), WechatReadError::DatabaseRead);
        assert_read_err(
            find_chats_by_name(&account, &both, "alice"),
            WechatReadError::DatabaseRead,
        );
        // A read failure must not be reported as "chat not found".
        assert_read_err(
            get_chat_by_username(&account, &both, "wxid_alice"),
            WechatReadError::DatabaseRead,
        );
    }

    #[test]
    fn list_chats_returns_empty_when_session_table_is_empty() {
        let f = fixture(&[], &[]);

        assert!(list_chats_from_dbs(&f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, 50, 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn list_chats_maps_sessions_and_contact_names() {
        let group = "INSERT INTO SessionTable VALUES
            ('team@chatroom', 2, 0, 'group', '', 1700000100, 1700000100, 'wxid_bob', 'Bob', 0, 7)";
        let f = fixture(&[ALICE_SESSION, group], &[ALICE_CONTACT]);

        let chats =
            list_chats_from_dbs(&f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, 50, 0).unwrap();

        assert_eq!(chats.len(), 2);
        let group = &chats[0];
        assert_eq!(group.id, "team@chatroom");
        assert!(group.is_group);
        assert_eq!(group.name, "team@chatroom");
        let alice = &chats[1];
        assert_eq!(alice.username, "wxid_alice");
        assert_eq!(alice.name, "Alice Remark");
        assert_eq!(alice.remark.as_deref(), Some("Alice Remark"));
        assert_eq!(alice.unread_count, 3);
        assert_eq!(alice.last_message_preview.as_deref(), Some("hi there"));
        assert_eq!(alice.last_message_sender.as_deref(), Some("Alice"));
        assert_eq!(alice.last_msg_local_id, Some(42));
        assert!(alice.last_activity_at.is_some());
        assert!(!alice.is_group);
    }

    #[test]
    fn list_chats_honours_limit_and_offset() {
        let f = fixture(
            &[
                "INSERT INTO SessionTable VALUES
                 ('wxid_a', 1, 0, 'a', '', 1700000300, 1700000300, '', '', 0, 1)",
                "INSERT INTO SessionTable VALUES
                 ('wxid_b', 1, 0, 'b', '', 1700000200, 1700000200, '', '', 0, 2)",
                "INSERT INTO SessionTable VALUES
                 ('wxid_c', 1, 0, 'c', '', 1700000100, 1700000100, '', '', 0, 3)",
            ],
            &[],
        );

        let first =
            list_chats_from_dbs(&f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, 2, 0).unwrap();
        let second =
            list_chats_from_dbs(&f.session_db, TEST_KEY, &f.contact_db, TEST_KEY, 2, 2).unwrap();

        let first_ids: Vec<&str> = first.iter().map(|c| c.id.as_str()).collect();
        let second_ids: Vec<&str> = second.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(first_ids, vec!["wxid_a", "wxid_b"]);
        assert_eq!(second_ids, vec!["wxid_c"]);
    }

    #[test]
    fn list_chats_fails_when_contact_lookup_fails() {
        let f = fixture(&[ALICE_SESSION], &[]);
        let missing_contact_db = absent_db_path(f._dir.path(), "missing-contact.db");

        let result = list_chats_from_dbs(
            &f.session_db,
            TEST_KEY,
            &missing_contact_db,
            TEST_KEY,
            50,
            0,
        );

        // No partial page: the session row must not be returned without its contact lookup.
        assert_read_err(result, WechatReadError::DatabaseRead);
    }

    #[test]
    fn get_chat_by_username_returns_none_for_unknown_username() {
        let f = fixture(&[ALICE_SESSION], &[ALICE_CONTACT]);

        let chat = get_chat_by_username_from_dbs(
            &f.session_db,
            TEST_KEY,
            &f.contact_db,
            TEST_KEY,
            "wxid_nobody",
        )
        .unwrap();

        assert!(chat.is_none());
    }

    #[test]
    fn get_chat_by_username_returns_chat_and_falls_back_to_username() {
        let f = fixture(
            &[
                ALICE_SESSION,
                "INSERT INTO SessionTable VALUES ('wxid_ghost', 1, 0, '', '', 0, 0, '', '', 0, 0)",
            ],
            &[ALICE_CONTACT],
        );

        let alice = get_chat_by_username_from_dbs(
            &f.session_db,
            TEST_KEY,
            &f.contact_db,
            TEST_KEY,
            "wxid_alice",
        )
        .unwrap()
        .expect("chat should be found");
        assert_eq!(alice.id, "wxid_alice");
        assert_eq!(alice.name, "Alice Remark");
        assert_eq!(alice.unread_count, 3);

        let ghost = get_chat_by_username_from_dbs(
            &f.session_db,
            TEST_KEY,
            &f.contact_db,
            TEST_KEY,
            "wxid_ghost",
        )
        .unwrap()
        .expect("chat should be found");
        assert_eq!(ghost.name, "wxid_ghost");
        assert!(ghost.last_activity_at.is_none());
    }

    #[test]
    fn get_chat_by_username_fails_when_contact_lookup_fails() {
        let f = fixture(&[ALICE_SESSION], &[]);
        let missing_contact_db = absent_db_path(f._dir.path(), "missing-contact.db");

        let result = get_chat_by_username_from_dbs(
            &f.session_db,
            TEST_KEY,
            &missing_contact_db,
            TEST_KEY,
            "wxid_alice",
        );

        // Session row exists, so this must be an error rather than None.
        assert_read_err(result, WechatReadError::DatabaseRead);
    }

    #[test]
    fn find_chats_by_name_returns_empty_when_no_contact_matches() {
        let f = fixture(&[ALICE_SESSION], &[ALICE_CONTACT]);

        let chats = find_chats_by_name_from_dbs(
            &f.contact_db,
            TEST_KEY,
            &f.session_db,
            TEST_KEY,
            "nobody",
        )
        .unwrap();

        assert!(chats.is_empty());
    }

    #[test]
    fn find_chats_by_name_matches_contact_with_session() {
        let f = fixture(&[ALICE_SESSION], &[ALICE_CONTACT]);

        let chats = find_chats_by_name_from_dbs(
            &f.contact_db,
            TEST_KEY,
            &f.session_db,
            TEST_KEY,
            "Alice Remark",
        )
        .unwrap();

        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].username, "wxid_alice");
        assert_eq!(chats[0].name, "Alice Remark");
        assert_eq!(chats[0].unread_count, 3);
    }

    #[test]
    fn find_chats_by_name_skips_contacts_without_session() {
        let f = fixture(
            &[],
            &[
                ALICE_CONTACT,
                "INSERT INTO contact VALUES ('wxid_nosession', 'No Session', '', '', '', 3)",
            ],
        );

        let chats = find_chats_by_name_from_dbs(
            &f.contact_db,
            TEST_KEY,
            &f.session_db,
            TEST_KEY,
            "wxid_",
        )
        .unwrap();

        assert!(chats.is_empty());
    }

    #[test]
    fn find_chats_by_name_fails_when_session_lookup_fails() {
        let f = fixture(&[], &[ALICE_CONTACT]);
        let missing_session_db = absent_db_path(f._dir.path(), "missing-session.db");

        let result = find_chats_by_name_from_dbs(
            &f.contact_db,
            TEST_KEY,
            &missing_session_db,
            TEST_KEY,
            "Alice Remark",
        );

        assert_read_err(result, WechatReadError::DatabaseRead);
    }
}
