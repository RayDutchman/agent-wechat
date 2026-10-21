use crate::ia::types::GroupMemberV2;
use crate::tools::wechat_contacts::{
    collect_contact_snapshot_until, contact_field_projection, ContactBudget,
};
use crate::tools::wechat_messages::{
    open_shard_readonly, read_outcome, CollectionCancel, MessageSnapshotError, COLLECTION_BUDGET,
};
use rusqlite::params;
use rusqlite::types::ValueRef;
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupMemberReadError {
    Snapshot(MessageSnapshotError),
    RoomNotFound,
    SourceUnavailable,
    InvalidBuffer,
    DuplicateRoom,
    DuplicateMember,
    BudgetExceeded,
}

impl From<MessageSnapshotError> for GroupMemberReadError {
    fn from(error: MessageSnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawGroupMember {
    username: String,
    group_nickname: String,
}

struct WireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_varint(&mut self) -> Result<u64, GroupMemberReadError> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or(GroupMemberReadError::InvalidBuffer)?;
            self.offset += 1;
            if index == 9 && byte > 1 {
                return Err(GroupMemberReadError::InvalidBuffer);
            }
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(GroupMemberReadError::InvalidBuffer)
    }

    fn read_key(&mut self) -> Result<(u32, u8), GroupMemberReadError> {
        let key = self.read_varint()?;
        let field = key >> 3;
        let wire = (key & 7) as u8;
        if field == 0 || field > 0x1fff_ffff {
            return Err(GroupMemberReadError::InvalidBuffer);
        }
        Ok((field as u32, wire))
    }

    fn read_slice(&mut self) -> Result<&'a [u8], GroupMemberReadError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| GroupMemberReadError::InvalidBuffer)?;
        let end = self
            .offset
            .checked_add(len)
            .ok_or(GroupMemberReadError::InvalidBuffer)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(GroupMemberReadError::InvalidBuffer)?;
        self.offset = end;
        Ok(value)
    }

    fn skip(&mut self, wire: u8) -> Result<(), GroupMemberReadError> {
        match wire {
            0 => {
                self.read_varint()?;
            }
            1 => self.skip_bytes(8)?,
            2 => {
                self.read_slice()?;
            }
            5 => self.skip_bytes(4)?,
            // Protobuf group wire types are intentionally not accepted.
            3 | 4 => return Err(GroupMemberReadError::InvalidBuffer),
            _ => return Err(GroupMemberReadError::InvalidBuffer),
        }
        Ok(())
    }

    fn skip_bytes(&mut self, count: usize) -> Result<(), GroupMemberReadError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(GroupMemberReadError::InvalidBuffer)?;
        if end > self.bytes.len() {
            return Err(GroupMemberReadError::InvalidBuffer);
        }
        self.offset = end;
        Ok(())
    }
}

fn decode_ext_buffer(
    bytes: &[u8],
    budget: ContactBudget,
) -> Result<Vec<RawGroupMember>, GroupMemberReadError> {
    if bytes.len() > budget.max_row_bytes {
        return Err(GroupMemberReadError::BudgetExceeded);
    }
    let mut reader = WireReader::new(bytes);
    let mut members = Vec::new();
    let mut seen_usernames = HashSet::new();
    while !reader.is_empty() {
        let (field, wire) = reader.read_key()?;
        if field == 1 {
            if wire != 2 || members.len() >= budget.max_rows {
                return Err(if members.len() >= budget.max_rows {
                    GroupMemberReadError::BudgetExceeded
                } else {
                    GroupMemberReadError::InvalidBuffer
                });
            }
            let member_bytes = reader.read_slice()?;
            let member = decode_member(member_bytes, budget)?;
            if !seen_usernames.insert(member.username.clone()) {
                return Err(GroupMemberReadError::DuplicateMember);
            }
            members.push(member);
        } else {
            reader.skip(wire)?;
        }
    }
    Ok(members)
}

fn decode_member(
    bytes: &[u8],
    budget: ContactBudget,
) -> Result<RawGroupMember, GroupMemberReadError> {
    let mut reader = WireReader::new(bytes);
    let mut username = None;
    let mut group_nickname = String::new();
    while !reader.is_empty() {
        let (field, wire) = reader.read_key()?;
        if field == 1 || field == 2 {
            if wire != 2 {
                return Err(GroupMemberReadError::InvalidBuffer);
            }
            let value = reader.read_slice()?;
            if value.len() > budget.max_row_bytes {
                return Err(GroupMemberReadError::BudgetExceeded);
            }
            let value = std::str::from_utf8(value)
                .map_err(|_| GroupMemberReadError::InvalidBuffer)?
                .to_string();
            if field == 1 {
                username = Some(value);
            } else {
                group_nickname = value;
            }
        } else {
            reader.skip(wire)?;
        }
    }
    let username = username
        .filter(|username| !username.is_empty())
        .ok_or(GroupMemberReadError::InvalidBuffer)?;
    Ok(RawGroupMember {
        username,
        group_nickname,
    })
}

pub fn resolve_member_display_name(
    remark: &str,
    group_nickname: &str,
    nickname: &str,
    username: &str,
) -> String {
    for name in [remark, group_nickname, nickname] {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    username.to_string()
}

pub fn list_group_members_v2(
    database_root: &Path,
    keys: &HashMap<String, String>,
    room_username: &str,
) -> Result<Vec<GroupMemberV2>, GroupMemberReadError> {
    list_group_members_v2_with_budget(
        database_root,
        keys,
        room_username,
        ContactBudget::default(),
    )
}

fn list_group_members_v2_with_budget(
    database_root: &Path,
    keys: &HashMap<String, String>,
    room_username: &str,
    budget: ContactBudget,
) -> Result<Vec<GroupMemberV2>, GroupMemberReadError> {
    let cancel = CollectionCancel::within(COLLECTION_BUDGET);
    list_group_members_v2_until(database_root, keys, room_username, budget, &cancel)
}

pub(crate) fn list_group_members_v2_until(
    database_root: &Path,
    keys: &HashMap<String, String>,
    room_username: &str,
    budget: ContactBudget,
    cancel: &CollectionCancel,
) -> Result<Vec<GroupMemberV2>, GroupMemberReadError> {
    cancel.check()?;
    if room_username.len() > budget.max_row_bytes {
        return Err(GroupMemberReadError::BudgetExceeded);
    }
    let key = keys
        .get("contact.db")
        .ok_or(GroupMemberReadError::Snapshot(
            MessageSnapshotError::KeysUnavailable,
        ))?;
    let path = database_root.join("contact").join("contact.db");
    let conn = open_shard_readonly(&path, key, cancel)?;
    let columns = chat_room_columns(&conn, cancel)?;
    if !columns.contains("username") || !columns.contains("ext_buffer") {
        return Err(GroupMemberReadError::SourceUnavailable);
    }
    let projection = contact_field_projection("ext_buffer", budget.max_row_bytes);
    let mut statement = conn
        .prepare(&format!(
            "SELECT {projection} FROM chat_room WHERE username = ?1 LIMIT 2"
        ))
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement
        .query(params![room_username])
        .map_err(|_| read_outcome(cancel))?;
    let raw_members = {
        let row = rows
            .next()
            .map_err(|_| read_outcome(cancel))?
            .ok_or(GroupMemberReadError::RoomNotFound)?;
        let stored_len: Option<i64> = row.get(1).map_err(|_| read_outcome(cancel))?;
        if stored_len
            .and_then(|len| usize::try_from(len).ok())
            .is_some_and(|len| len > budget.max_row_bytes)
        {
            return Err(GroupMemberReadError::BudgetExceeded);
        }
        let buffer = match row.get_ref(0).map_err(|_| read_outcome(cancel))? {
            ValueRef::Blob(bytes) => bytes,
            ValueRef::Null if stored_len.is_none() || stored_len == Some(0) => &[][..],
            _ => return Err(GroupMemberReadError::InvalidBuffer),
        };
        decode_ext_buffer(buffer, budget)?
    };
    if rows.next().map_err(|_| read_outcome(cancel))?.is_some() {
        return Err(GroupMemberReadError::DuplicateRoom);
    }
    drop(rows);
    drop(statement);
    drop(conn);

    if raw_members.is_empty() {
        return Ok(Vec::new());
    }
    let contacts = collect_contact_snapshot_until(database_root, keys, cancel)?;
    let mut global_names = HashMap::with_capacity(contacts.len());
    for contact in &contacts {
        cancel.check()?;
        let username = contact["username"]
            .as_str()
            .ok_or(GroupMemberReadError::Snapshot(MessageSnapshotError::RowInvalid))?;
        let remark = contact["remark"]
            .as_str()
            .ok_or(GroupMemberReadError::Snapshot(MessageSnapshotError::RowInvalid))?;
        let nickname = contact["nick_name"]
            .as_str()
            .ok_or(GroupMemberReadError::Snapshot(MessageSnapshotError::RowInvalid))?;
        global_names.insert(username, (remark, nickname));
    }

    let mut members = Vec::with_capacity(raw_members.len());
    let mut sample_bytes = 0usize;
    for member in raw_members {
        cancel.check()?;
        let (remark, nickname) = global_names
            .get(member.username.as_str())
            .copied()
            .unwrap_or(("", ""));
        let resolved = GroupMemberV2 {
            room_username: room_username.to_string(),
            display_name: resolve_member_display_name(
                remark,
                &member.group_nickname,
                nickname,
                &member.username,
            ),
            username: member.username,
            group_nickname: member.group_nickname,
        };
        let row_bytes = serde_json::to_vec(&resolved)
            .map_err(|_| GroupMemberReadError::InvalidBuffer)?
            .len();
        if row_bytes > budget.max_row_bytes {
            return Err(GroupMemberReadError::BudgetExceeded);
        }
        sample_bytes = sample_bytes.saturating_add(row_bytes);
        if sample_bytes > budget.max_bytes || members.len() >= budget.max_rows {
            return Err(GroupMemberReadError::BudgetExceeded);
        }
        members.push(resolved);
    }
    Ok(members)
}

fn chat_room_columns(
    conn: &rusqlite::Connection,
    cancel: &CollectionCancel,
) -> Result<HashSet<String>, GroupMemberReadError> {
    cancel.check()?;
    let mut statement = conn
        .prepare("PRAGMA table_info(chat_room)")
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement.query([]).map_err(|_| read_outcome(cancel))?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        cancel.check()?;
        let name: String = row.get(1).map_err(|_| read_outcome(cancel))?;
        columns.insert(name);
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn varint(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        while value >= 0x80 {
            bytes.push((value as u8) | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
        bytes
    }

    fn bytes_field(field: u64, value: &[u8]) -> Vec<u8> {
        let mut encoded = varint((field << 3) | 2);
        encoded.extend(varint(value.len() as u64));
        encoded.extend(value);
        encoded
    }

    fn member(username: &str, group_nickname: Option<&str>) -> Vec<u8> {
        let mut encoded = bytes_field(1, username.as_bytes());
        if let Some(group_nickname) = group_nickname {
            encoded.extend(bytes_field(2, group_nickname.as_bytes()));
        }
        encoded
    }

    fn ext_buffer(members: &[Vec<u8>]) -> Vec<u8> {
        members
            .iter()
            .flat_map(|member| bytes_field(1, member))
            .collect()
    }

    #[test]
    fn resolver_uses_remark_then_group_nickname_then_global_nickname_then_username() {
        assert_eq!(resolve_member_display_name(" Work ", "Room", "Global", "member"), "Work");
        assert_eq!(resolve_member_display_name("", " Room A ", "Global", "member"), "Room A");
        assert_eq!(resolve_member_display_name("", "Room B", "Global", "member"), "Room B");
        assert_eq!(resolve_member_display_name("", "", " Global ", "member"), "Global");
        assert_eq!(resolve_member_display_name("", "", "", "member"), "member");
        assert_eq!(resolve_member_display_name("", "", "", " member "), " member ");
    }

    #[test]
    fn decoder_reads_identity_and_group_name_and_skips_legal_unknown_fields() {
        let mut nested = member("wxid_member", Some("Room name"));
        nested.extend([0x18]);
        nested.extend(varint(300)); // unknown varint field 3
        nested.extend([0x21, 1, 2, 3, 4, 5, 6, 7, 8]); // unknown fixed64 field 4
        nested.extend(bytes_field(5, b"opaque")); // unknown bytes field 5
        nested.extend([0x35, 1, 2, 3, 4]); // unknown fixed32 field 6

        let mut encoded = bytes_field(1, &nested);
        encoded.extend([0x40]);
        encoded.extend(varint(77)); // unknown outer varint field 8

        let members = decode_ext_buffer(&encoded, ContactBudget::default()).unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].username, "wxid_member");
        assert_eq!(members[0].group_nickname, "Room name");
    }

    #[test]
    fn decoder_distinguishes_missing_nickname_from_missing_username() {
        let decoded = decode_ext_buffer(
            &ext_buffer(&[member("wxid_member", None)]),
            ContactBudget::default(),
        )
        .unwrap();
        assert_eq!(decoded[0].group_nickname, "");
        assert_eq!(
            decode_ext_buffer(&ext_buffer(&[bytes_field(2, b"nickname")]), ContactBudget::default())
                .unwrap_err(),
            GroupMemberReadError::InvalidBuffer
        );
    }

    #[test]
    fn decoder_rejects_truncated_or_overflowing_varints_and_lengths() {
        for malformed in [
            vec![0x0a, 0x80],
            vec![0x0a, 0x05, 0x01],
            [vec![0x0a], vec![0x80; 9], vec![0x02]].concat(),
            vec![0x1b], // protobuf group wire type is intentionally unsupported
        ] {
            assert_eq!(
                decode_ext_buffer(&malformed, ContactBudget::default()).unwrap_err(),
                GroupMemberReadError::InvalidBuffer
            );
        }
    }

    #[test]
    fn decoder_rejects_duplicate_member_identity_and_budget_overflow() {
        assert_eq!(
            decode_ext_buffer(
                &ext_buffer(&[
                    member("wxid_member", Some("A")),
                    member("wxid_member", Some("B")),
                ]),
                ContactBudget::default(),
            )
            .unwrap_err(),
            GroupMemberReadError::DuplicateMember
        );
        let budget = ContactBudget {
            max_rows: 1,
            ..ContactBudget::default()
        };
        assert_eq!(
            decode_ext_buffer(
                &ext_buffer(&[
                    member("one", None),
                    member("two", None),
                ]),
                budget,
            )
            .unwrap_err(),
            GroupMemberReadError::BudgetExceeded
        );
    }

    fn seed_group_database(root: &Path) -> HashMap<String, String> {
        let dir = root.join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE contact (
                username TEXT, nick_name TEXT, remark TEXT, alias TEXT, delete_flag INTEGER
             );
             CREATE TABLE chat_room (
                id INTEGER, username TEXT, owner TEXT, ext_buffer BLOB
             );
             INSERT INTO contact VALUES ('member-shared', 'Global Name', '', '', 0);
             INSERT INTO contact VALUES ('member-remark', 'Global Nick', 'Work name', '', 0);"
        ))
        .unwrap();
        let room_a = ext_buffer(&[
            member("member-shared", Some("Room A")),
            member("member-remark", Some("Room C")),
        ]);
        let room_b = ext_buffer(&[member("member-shared", Some("Room B"))]);
        conn.execute(
            "INSERT INTO chat_room VALUES (1, 'room-a', 'owner', ?1), \
             (2, 'room-b', 'owner', ?2), (3, 'room-empty', 'owner', x'')",
            rusqlite::params![room_a, room_b],
        )
        .unwrap();
        HashMap::from([("contact.db".to_string(), TEST_KEY.to_string())])
    }

    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    #[test]
    fn collector_keeps_room_names_separate_and_does_not_change_global_contact_names() {
        let root = tempfile::tempdir().unwrap();
        let keys = seed_group_database(root.path());
        let global_before = crate::tools::wechat_contacts::collect_contact_snapshot(root.path(), &keys)
            .unwrap();

        let room_a = list_group_members_v2(root.path(), &keys, "room-a").unwrap();
        let room_b = list_group_members_v2(root.path(), &keys, "room-b").unwrap();

        let shared_a = room_a.iter().find(|m| m.username == "member-shared").unwrap();
        let shared_b = room_b.iter().find(|m| m.username == "member-shared").unwrap();
        assert_eq!(shared_a.room_username, "room-a");
        assert_eq!(shared_a.group_nickname, "Room A");
        assert_eq!(shared_a.display_name, "Room A");
        assert_eq!(shared_b.room_username, "room-b");
        assert_eq!(shared_b.group_nickname, "Room B");
        assert_eq!(shared_b.display_name, "Room B");
        let remarked = room_a.iter().find(|m| m.username == "member-remark").unwrap();
        assert_eq!(remarked.group_nickname, "Room C");
        assert_eq!(remarked.display_name, "Work name");

        let global_after = crate::tools::wechat_contacts::collect_contact_snapshot(root.path(), &keys)
            .unwrap();
        assert_eq!(global_before, global_after);
        let global_member = global_after
            .iter()
            .find(|contact| contact["username"] == "member-shared")
            .unwrap();
        assert_eq!(global_member["nick_name"], "Global Name");
    }

    #[test]
    fn collector_distinguishes_empty_room_from_missing_room() {
        let root = tempfile::tempdir().unwrap();
        let keys = seed_group_database(root.path());
        assert!(list_group_members_v2(root.path(), &keys, "room-empty")
            .unwrap()
            .is_empty());
        assert_eq!(
            list_group_members_v2(root.path(), &keys, "room-missing").unwrap_err(),
            GroupMemberReadError::RoomNotFound
        );
    }

    #[test]
    fn collector_rejects_corrupt_and_over_budget_buffers() {
        let root = tempfile::tempdir().unwrap();
        let keys = seed_group_database(root.path());
        let dir = root.path().join("contact");
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             INSERT INTO chat_room VALUES (4, 'room-corrupt', 'owner', x'0A05FF');"
        ))
        .unwrap();
        drop(conn);
        assert_eq!(
            list_group_members_v2(root.path(), &keys, "room-corrupt").unwrap_err(),
            GroupMemberReadError::InvalidBuffer
        );
        assert_eq!(
            list_group_members_v2_with_budget(
                root.path(),
                &keys,
                "room-a",
                ContactBudget {
                    max_row_bytes: 8,
                    ..ContactBudget::default()
                }
            )
            .unwrap_err(),
            GroupMemberReadError::BudgetExceeded
        );
    }

    #[test]
    fn collector_fails_closed_when_chat_room_schema_lacks_required_columns() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("contact");
        std::fs::create_dir_all(&dir).unwrap();
        let conn = Connection::open(dir.join("contact.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE chat_room (username TEXT);"
        ))
        .unwrap();
        drop(conn);
        let keys = HashMap::from([("contact.db".to_string(), TEST_KEY.to_string())]);

        assert_eq!(
            list_group_members_v2(root.path(), &keys, "room-a").unwrap_err(),
            GroupMemberReadError::SourceUnavailable
        );
    }

    #[test]
    fn collector_enforces_serialized_row_and_sample_budgets() {
        let root = tempfile::tempdir().unwrap();
        let keys = seed_group_database(root.path());

        assert_eq!(
            list_group_members_v2_with_budget(
                root.path(),
                &keys,
                "room-b",
                ContactBudget {
                    max_row_bytes: 64,
                    ..ContactBudget::default()
                }
            )
            .unwrap_err(),
            GroupMemberReadError::BudgetExceeded
        );
        assert_eq!(
            list_group_members_v2_with_budget(
                root.path(),
                &keys,
                "room-b",
                ContactBudget {
                    max_bytes: 1,
                    max_row_bytes: 256,
                    ..ContactBudget::default()
                }
            )
            .unwrap_err(),
            GroupMemberReadError::BudgetExceeded
        );
    }
}
