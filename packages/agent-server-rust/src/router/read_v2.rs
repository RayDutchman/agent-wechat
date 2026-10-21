use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path as AxumPath, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use serde_json::{json, Value};

use crate::ia::types::ContactKind;
use crate::router::read::{
    self, PageCache, ProcessReadBackend, ReadBackend, ReadError, ReadId, RoundBudget,
};
use crate::tools::wechat_group_members::GroupMemberReadError;
use crate::tools::wechat_messages::{CollectionCancel, MessageSnapshotError};
use crate::tools::wechat_messages::{MessageLocateError, MessageSourceAnchor};
use crate::tools::wechat_read_voice::VoiceRequest;
use crate::tools::wechat_read_pages::{self, FrozenPage, PageError, SnapshotScope};
use crate::tools::wechat_chats::list_chat_snapshot_v2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2Error {
    InvalidParameter,
    ScopeChanged,
    Expired,
    NotFound,
    Ambiguous,
    VoiceSourceMissing,
    BudgetExceeded,
    SourceReadFailed,
    IdentityUnavailable,
    KeysUnavailable,
    SourceUnavailable,
    DeadlineExceeded,
}

impl V2Error {
    fn code(self) -> &'static str {
        match self {
            Self::InvalidParameter => "invalid_parameter",
            Self::ScopeChanged => "scope_changed",
            Self::Expired => "expired",
            Self::NotFound => "not_found",
            Self::Ambiguous => "ambiguous",
            Self::VoiceSourceMissing => "voice_source_missing",
            Self::BudgetExceeded => "budget_exceeded",
            Self::SourceReadFailed => "source_read_failed",
            Self::IdentityUnavailable => "identity_unavailable",
            Self::KeysUnavailable => "keys_unavailable",
            Self::SourceUnavailable => "source_unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }

    fn status(self) -> StatusCode {
        match self {
            Self::InvalidParameter => StatusCode::BAD_REQUEST,
            Self::ScopeChanged => StatusCode::CONFLICT,
            Self::Expired => StatusCode::GONE,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Ambiguous => StatusCode::CONFLICT,
            Self::VoiceSourceMissing => StatusCode::SERVICE_UNAVAILABLE,
            Self::BudgetExceeded => StatusCode::PAYLOAD_TOO_LARGE,
            Self::SourceReadFailed => StatusCode::INTERNAL_SERVER_ERROR,
            Self::IdentityUnavailable | Self::KeysUnavailable | Self::SourceUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        }
    }
}

impl IntoResponse for V2Error {
    fn into_response(self) -> Response {
        let code = self.code();
        (self.status(), Json(json!({"error": code, "code": code}))).into_response()
    }
}

impl From<ReadError> for V2Error {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::InvalidParameter => Self::InvalidParameter,
            ReadError::ScopeChanged => Self::ScopeChanged,
            ReadError::FrozenExpired => Self::Expired,
            ReadError::BudgetExceeded => Self::BudgetExceeded,
            ReadError::SourceReadFailed => Self::SourceReadFailed,
            ReadError::IdentityUnavailable | ReadError::LoginUnverified => Self::IdentityUnavailable,
            ReadError::KeysUnavailable => Self::KeysUnavailable,
            ReadError::SourceUnavailable => Self::SourceUnavailable,
            ReadError::DeadlineExceeded => Self::DeadlineExceeded,
            ReadError::VoiceSourceMissing => Self::VoiceSourceMissing,
            ReadError::VoiceSourceAmbiguous => Self::Ambiguous,
        }
    }
}

impl From<PageError> for V2Error {
    fn from(error: PageError) -> Self {
        match error {
            PageError::InvalidRequest => Self::InvalidParameter,
            PageError::Expired => Self::Expired,
            PageError::ScopeChanged => Self::ScopeChanged,
            PageError::BudgetExceeded => Self::BudgetExceeded,
            PageError::DeadlineExceeded => Self::DeadlineExceeded,
        }
    }
}

impl From<MessageSnapshotError> for V2Error {
    fn from(error: MessageSnapshotError) -> Self {
        match error {
            MessageSnapshotError::KeysUnavailable => Self::KeysUnavailable,
            MessageSnapshotError::SourceUnavailable => Self::SourceUnavailable,
            MessageSnapshotError::SourceChanged => Self::ScopeChanged,
            MessageSnapshotError::BudgetExceeded | MessageSnapshotError::RowTooLarge => Self::BudgetExceeded,
            MessageSnapshotError::DeadlineExceeded => Self::DeadlineExceeded,
            MessageSnapshotError::DatabaseRead | MessageSnapshotError::RowInvalid | MessageSnapshotError::RowDuplicate => Self::SourceReadFailed,
        }
    }
}

impl From<MessageLocateError> for V2Error {
    fn from(error: MessageLocateError) -> Self {
        match error {
            MessageLocateError::InvalidRequest => Self::InvalidParameter,
            MessageLocateError::NotFound => Self::NotFound,
            MessageLocateError::Ambiguous => Self::Ambiguous,
            MessageLocateError::ChatNameUnavailable => Self::SourceReadFailed,
            MessageLocateError::Snapshot(error) => error.into(),
        }
    }
}

impl From<GroupMemberReadError> for V2Error {
    fn from(error: GroupMemberReadError) -> Self {
        match error {
            GroupMemberReadError::Snapshot(error) => error.into(),
            GroupMemberReadError::RoomNotFound => Self::NotFound,
            GroupMemberReadError::SourceUnavailable => Self::SourceUnavailable,
            GroupMemberReadError::InvalidBuffer | GroupMemberReadError::DuplicateMember | GroupMemberReadError::DuplicateRoom => Self::SourceReadFailed,
            GroupMemberReadError::BudgetExceeded => Self::BudgetExceeded,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V2Resource {
    Contacts,
    Chats,
    Members,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V2Query {
    resource: V2Resource,
    q: Option<String>,
    kind: Option<ContactKind>,
    limit: Option<usize>,
    cursor: Option<String>,
    expected_account_username: Option<String>,
}

impl V2Query {
    fn resource_key(&self, room: Option<&str>) -> String {
        let resource = match self.resource {
            V2Resource::Contacts => "contacts",
            V2Resource::Chats => "chats",
            V2Resource::Members => "members",
        };
        let kind = self.kind.map(kind_name).unwrap_or("-");
        let q = self.q.as_deref().map(|value| format!("1{}", hex(value.as_bytes()))).unwrap_or_else(|| "0".to_string());
        format!("v2:{resource}:q={q}:k={kind}:r={}", hex(room.unwrap_or_default().as_bytes()))
    }
}

fn kind_name(kind: ContactKind) -> &'static str {
    match kind {
        ContactKind::Individual => "individual", ContactKind::Group => "group",
        ContactKind::Enterprise => "enterprise", ContactKind::Official => "official",
        ContactKind::System => "system", ContactKind::Unknown => "unknown",
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes { output.push(DIGITS[(byte >> 4) as usize] as char); output.push(DIGITS[(byte & 0x0f) as usize] as char); }
    output
}

fn parse_v2_query(raw: Option<&str>, resource: V2Resource) -> Result<V2Query, V2Error> {
    let mut q = None;
    let mut kind = None;
    let mut limit = None;
    let mut cursor = None;
    let mut expected_account_username = None;
    let mut seen = std::collections::HashSet::new();
    for pair in raw.unwrap_or_default().split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if !seen.insert(key.clone()) {
            return Err(V2Error::InvalidParameter);
        }
        match key.as_str() {
            "offset" => return Err(V2Error::InvalidParameter),
            "q" if matches!(resource, V2Resource::Contacts | V2Resource::Chats) => {
                q = Some(value);
            }
            "kind" if resource == V2Resource::Contacts => {
                kind = Some(parse_kind(&value)?);
            }
            "limit" => limit = Some(parse_v2_limit(&value)?),
            "cursor" => {
                if value.trim().is_empty() { return Err(V2Error::InvalidParameter); }
                cursor = Some(value);
            }
            "expected_account_username" => {
                if value.trim().is_empty() { return Err(V2Error::InvalidParameter); }
                expected_account_username = Some(value);
            }
            _ => return Err(V2Error::InvalidParameter),
        }
    }
    if cursor.is_some() && (limit.is_some() || expected_account_username.is_some()) {
        return Err(V2Error::InvalidParameter);
    }
    Ok(V2Query { resource, q, kind, limit, cursor, expected_account_username })
}

fn percent_decode(raw: &str) -> Result<String, V2Error> {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = raw.get(index + 1..index + 3).ok_or(V2Error::InvalidParameter)?;
            decoded.push(u8::from_str_radix(hex, 16).map_err(|_| V2Error::InvalidParameter)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| V2Error::InvalidParameter)
}

fn parse_v2_limit(raw: &str) -> Result<usize, V2Error> {
    if raw.is_empty() || raw.len() > 4 || !raw.bytes().all(|b| b.is_ascii_digit()) || raw.starts_with('0') {
        return Err(V2Error::InvalidParameter);
    }
    let value = raw.parse::<usize>().map_err(|_| V2Error::InvalidParameter)?;
    (1..=1000).contains(&value).then_some(value).ok_or(V2Error::InvalidParameter)
}

fn parse_kind(raw: &str) -> Result<ContactKind, V2Error> {
    match raw {
        "individual" => Ok(ContactKind::Individual), "group" => Ok(ContactKind::Group),
        "enterprise" => Ok(ContactKind::Enterprise), "official" => Ok(ContactKind::Official),
        "system" => Ok(ContactKind::System), "unknown" => Ok(ContactKind::Unknown),
        _ => Err(V2Error::InvalidParameter),
    }
}

#[async_trait::async_trait]
trait V2Backend: Send + Sync + 'static {
    async fn capture(&self, cancel: &CollectionCancel) -> Result<ReadId, ReadError>;
    async fn contact_keys(&self, id: &ReadId, cancel: &CollectionCancel) -> Result<HashMap<String, String>, ReadError>;
    async fn contacts(&self, id: &ReadId, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error>;
    async fn chats(&self, id: &ReadId, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error>;
    async fn members(&self, id: &ReadId, keys: &HashMap<String, String>, room: &str, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error>;
    async fn messages(&self, id: &ReadId, keys: &HashMap<String, String>, chat: &str, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error>;
    async fn locate_message(&self, id: &ReadId, keys: &HashMap<String, String>, server_id: i64, chat: Option<&str>, cancel: &CollectionCancel) -> Result<MessageSourceAnchor, V2Error>;
    async fn voice(&self, id: &ReadId, request: &VoiceRequest, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<read::VerifiedVoice, V2Error>;
}

#[async_trait::async_trait]
impl V2Backend for ProcessReadBackend {
    async fn capture(&self, cancel: &CollectionCancel) -> Result<ReadId, ReadError> {
        ReadBackend::capture_identity(self, cancel).await
    }

    async fn contact_keys(&self, id: &ReadId, cancel: &CollectionCancel) -> Result<HashMap<String, String>, ReadError> {
        ReadBackend::contact_keys(self, id, cancel).await
    }

    async fn contacts(&self, id: &ReadId, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
        let root = id.database_root.clone();
        let session_id = id.session_id.clone();
        let account = id.account_dir.clone();
        let keys = keys.clone();
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = crate::tools::wechat_contacts::collect_contact_snapshot_v2_until(&root, &keys, &cancel);
            if snapshot.is_err() && cancel.authentication_failure().is_some() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&session_id, &account);
            }
            snapshot.map_err(V2Error::from)?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| V2Error::SourceReadFailed)
        }).await.map_err(|_| V2Error::SourceReadFailed)?
    }

    async fn chats(&self, id: &ReadId, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
        let root = id.database_root.clone();
        let session_id = id.session_id.clone();
        let account = id.account_dir.clone();
        let keys = keys.clone();
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = list_chat_snapshot_v2(&root, &keys, &cancel);
            if snapshot.is_err() && cancel.authentication_failure().is_some() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&session_id, &account);
            }
            snapshot.map_err(V2Error::from)?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| V2Error::SourceReadFailed)
        }).await.map_err(|_| V2Error::SourceReadFailed)?
    }

    async fn members(&self, id: &ReadId, keys: &HashMap<String, String>, room: &str, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
        let root = id.database_root.clone();
        let session_id = id.session_id.clone();
        let account = id.account_dir.clone();
        let keys = keys.clone();
        let room = room.to_string();
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = crate::tools::wechat_group_members::list_group_members_v2_until(&root, &keys, &room, crate::tools::wechat_contacts::ContactBudget::default(), &cancel);
            if snapshot.is_err() && cancel.authentication_failure().is_some() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&session_id, &account);
            }
            snapshot.map_err(V2Error::from)?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| V2Error::SourceReadFailed)
        }).await.map_err(|_| V2Error::SourceReadFailed)?
    }

    async fn messages(&self, id: &ReadId, keys: &HashMap<String, String>, chat: &str, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
        let root = id.database_root.clone();
        let account_dir = id.account_dir.clone();
        let session_id = id.session_id.clone();
        let account = id.account_dir.clone();
        let keys = keys.clone();
        let chat = chat.to_string();
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = crate::tools::wechat_messages::collect_message_snapshot_until(&root, &account_dir, &chat, &keys, &cancel);
            if snapshot.is_err() && cancel.authentication_failure().is_some() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&session_id, &account);
            }
            snapshot.map_err(V2Error::from)
        }).await.map_err(|_| V2Error::SourceReadFailed)?
    }

    async fn locate_message(&self, id: &ReadId, keys: &HashMap<String, String>, server_id: i64, chat: Option<&str>, cancel: &CollectionCancel) -> Result<MessageSourceAnchor, V2Error> {
        let known_chats = if chat.is_none() {
            self.chats(id, keys, cancel).await?.into_iter()
                .filter_map(|row| row.get("username").and_then(Value::as_str).map(str::to_string)).collect()
        } else { Vec::new() };
        let root = id.database_root.clone();
        let account_dir = id.account_dir.clone();
        let session_id = id.session_id.clone();
        let account = id.account_dir.clone();
        let keys = keys.clone();
        let chat = chat.map(str::to_string);
        let cancel = cancel.clone();
        tokio::task::spawn_blocking(move || {
            let located = crate::tools::wechat_messages::locate_message_by_server_id(&root, &account_dir, &keys, server_id, chat.as_deref(), &known_chats, &cancel);
            if located.is_err() && cancel.authentication_failure().is_some() {
                crate::tools::wechat_keys::request_refresh_after_auth_failure(&session_id, &account);
            }
            located.map_err(V2Error::from)
        }).await.map_err(|_| V2Error::SourceReadFailed)?
    }

    async fn voice(&self, id: &ReadId, request: &VoiceRequest, keys: &HashMap<String, String>, cancel: &CollectionCancel) -> Result<read::VerifiedVoice, V2Error> {
        ReadBackend::collect_voice(self, id, request, keys, cancel).await.map_err(V2Error::from)
    }
}

fn make_scope(id: &ReadId, resource: String) -> SnapshotScope {
    SnapshotScope { account_dir: id.account_dir.clone(), resource, epoch: id.epoch.clone() }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct MessageTypeFilter {
    #[serde(rename = "type")]
    message_type: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_type: Option<u32>,
}

fn parse_message_type_filters(value: &str) -> Result<Vec<MessageTypeFilter>, V2Error> {
    let parsed: Value = serde_json::from_str(value).map_err(|_| V2Error::InvalidParameter)?;
    let values = parsed.as_array().ok_or(V2Error::InvalidParameter)?;
    values.iter().map(|value| {
        let object = value.as_object().ok_or(V2Error::InvalidParameter)?;
        if object.keys().any(|key| key != "type" && key != "sub_type") { return Err(V2Error::InvalidParameter); }
        let message_type = object.get("type").and_then(Value::as_u64).and_then(|code| u32::try_from(code).ok()).ok_or(V2Error::InvalidParameter)?;
        let sub_type = match object.get("sub_type") {
            None => None,
            Some(value) => Some(value.as_u64().and_then(|code| u32::try_from(code).ok()).ok_or(V2Error::InvalidParameter)?),
        };
        Ok(MessageTypeFilter { message_type, sub_type })
    }).collect()
}

fn type_filter_matches(msg_type: u32, sub_type: u32, filter: &MessageTypeFilter) -> bool {
    msg_type == filter.message_type && filter.sub_type.is_none_or(|wanted| sub_type == wanted)
}

async fn chat_messages_handler<B: V2Backend>(State(backend): State<Arc<B>>, AxumPath(chat): AxumPath<String>, RawQuery(raw): RawQuery, cache: Option<Extension<Arc<PageCache>>>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    if chat.trim().is_empty() { return Err(V2Error::InvalidParameter); }
    let mut limit = None;
    let mut cursor = None;
    let mut order = None;
    let mut expected = None;
    let mut include: Option<Vec<MessageTypeFilter>> = None;
    let mut exclude: Option<Vec<MessageTypeFilter>> = None;
    let mut seen = std::collections::HashSet::new();
    for pair in raw.as_deref().unwrap_or_default().split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if !seen.insert(key.clone()) { return Err(V2Error::InvalidParameter); }
        match key.as_str() {
            "offset" => return Err(V2Error::InvalidParameter),
            "limit" => limit = Some(parse_v2_limit(&value)?),
            "cursor" if !value.trim().is_empty() => cursor = Some(value),
            "cursor" => return Err(V2Error::InvalidParameter),
            "order" if value == "asc" || value == "desc" => order = Some(value),
            "order" => return Err(V2Error::InvalidParameter),
            "include" => include = Some(parse_message_type_filters(&value)?),
            "exclude" => exclude = Some(parse_message_type_filters(&value)?),
            "expected_account_username" => expected = Some(value),
            _ => return Err(V2Error::InvalidParameter),
        }
    }
    if cursor.is_some() && limit.is_some() { return Err(V2Error::InvalidParameter); }
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let id = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if expected.as_deref().is_some_and(|value| value != id.account_id) { return Err(V2Error::ScopeChanged); }
    let cache = cache.map(|Extension(v)| v).unwrap_or_else(|| PageCache::global().clone());
    if let Some(cursor) = cursor.as_deref() {
        let scope = cache.cursor_scope_v2(cursor, Instant::now(), round.remaining()).map_err(V2Error::from)?;
        let base = format!("v2:messages:chat={}:order=", hex(chat.as_bytes()));
        if scope.account_dir != id.account_dir || scope.epoch != id.epoch || !scope.resource.starts_with(&base) { return Err(V2Error::ScopeChanged); }
        if let Some(order) = order.as_deref() {
            if !scope.resource.contains(&format!(":order={order}:")) { return Err(V2Error::ScopeChanged); }
        }
        if include.as_ref().is_some_and(|filters| !scope.resource.contains(&format!(":i={}:x=", hex(serde_json::to_string(filters).unwrap_or_default().as_bytes()))))
            || exclude.as_ref().is_some_and(|filters| !scope.resource.ends_with(&format!(":x={}", hex(serde_json::to_string(filters).unwrap_or_default().as_bytes()))))
        { return Err(V2Error::ScopeChanged); }
        let page = cache.next_v2(cursor, &scope, Instant::now(), round.remaining()).map_err(V2Error::from)?;
        let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
        if after != id { return Err(V2Error::ScopeChanged); }
        return Ok(Json(page_json(page)));
    }
    let keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let rows = within_v2_round(&round, backend.messages(&id, &keys, &chat, &round)).await?;
    let mut items = rows.iter().map(crate::tools::wechat_messages::message_v2_from_snapshot)
        .collect::<Option<Vec<_>>>().ok_or(V2Error::SourceReadFailed)?;
    items.retain(|message| {
        let included = include.as_ref().is_none_or(|filters| filters.iter().any(|filter| type_filter_matches(message.r#type, message.sub_type, filter)));
        let excluded = exclude.as_ref().is_some_and(|filters| filters.iter().any(|filter| type_filter_matches(message.r#type, message.sub_type, filter)));
        included && !excluded
    });
    let order = order.unwrap_or_else(|| "desc".into());
    if order == "desc" { items.reverse(); }
    let after_keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if keys != after_keys || after != id { return Err(V2Error::ScopeChanged); }
    let include_key = include.as_ref().map(|v| hex(serde_json::to_string(v).unwrap_or_default().as_bytes())).unwrap_or_else(|| "*".into());
    let exclude_key = exclude.as_ref().map(|v| hex(serde_json::to_string(v).unwrap_or_default().as_bytes())).unwrap_or_else(|| "*".into());
    let scope = make_scope(&id, format!("v2:messages:chat={}:order={order}:i={include_key}:x={exclude_key}", hex(chat.as_bytes())));
    let config = cache.config();
    let page_size = limit.unwrap_or(100);
    let prepared = tokio::task::spawn_blocking({
        let cancel = round.clone();
        move || wechat_read_pages::prepare(&config, scope, items.drain(..).map(|item| serde_json::to_value(item).unwrap()).collect(), page_size, &cancel)
    }).await.map_err(|_| V2Error::DeadlineExceeded)?.map_err(V2Error::from)?;
    let page = cache.publish_v2(prepared, Instant::now(), round.remaining()).map_err(V2Error::from)?;
    Ok(Json(page_json(page)))
}

fn filter_contacts(rows: Vec<Value>, query: &V2Query, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
    let mut filtered = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        if index % 64 == 0 && cancel.expired() { return Err(V2Error::DeadlineExceeded); }
        let kind_matches = query.kind.is_none_or(|kind| row.get("kind").and_then(Value::as_str) == Some(match kind {
            ContactKind::Individual => "individual", ContactKind::Group => "group", ContactKind::Enterprise => "enterprise",
            ContactKind::Official => "official", ContactKind::System => "system", ContactKind::Unknown => "unknown",
        }));
        let q_matches = query.q.as_deref().is_none_or(|q| {
            ["username", "alias", "remark", "nick_name"].iter().any(|key| row.get(*key).and_then(Value::as_str).is_some_and(|value| value.to_lowercase().contains(&q.to_lowercase())))
        });
        if kind_matches && q_matches { filtered.push(row); }
    }
    Ok(filtered)
}

fn filter_chats(rows: Vec<Value>, query: &V2Query, cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
    let mut filtered = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        if index % 64 == 0 && cancel.expired() { return Err(V2Error::DeadlineExceeded); }
        let matches = query.q.as_deref().is_none_or(|q| {
            ["username", "display_name", "remark", "nick_name"].iter().any(|key| row.get(*key).and_then(Value::as_str).is_some_and(|value| value.to_lowercase().contains(&q.to_lowercase())))
        });
        if matches { filtered.push(row); }
    }
    Ok(filtered)
}

fn page_json(page: FrozenPage) -> Value {
    json!({"items": page.items, "next_cursor": page.next_cursor, "release_token": page.release_token})
}

async fn serve_list<B: V2Backend>(
    backend: &B,
    cache: &PageCache,
    query: V2Query,
    room: Option<String>,
    budget: RoundBudget,
) -> Result<Json<Value>, V2Error> {
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let id = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if query.expected_account_username.as_deref().is_some_and(|expected| expected != id.account_id) {
        return Err(V2Error::ScopeChanged);
    }
    if let Some(cursor) = query.cursor.as_deref() {
        let scope = cache.cursor_scope_v2(cursor, Instant::now(), round.remaining()).map_err(V2Error::from)?;
        let base_resource = match query.resource { V2Resource::Contacts => "contacts", V2Resource::Chats => "chats", V2Resource::Members => "members" };
        let room_scope = format!(":r={}", hex(room.as_deref().unwrap_or_default().as_bytes()));
        if scope.account_dir != id.account_dir
            || scope.epoch != id.epoch
            || !scope.resource.starts_with(&format!("v2:{base_resource}:"))
            || !scope.resource.ends_with(&room_scope)
        {
            return Err(V2Error::ScopeChanged);
        }
        if let Some(q) = &query.q {
            if !scope.resource.contains(&format!("q=1{}:", hex(q.as_bytes()))) { return Err(V2Error::ScopeChanged); }
        }
        if let Some(kind) = query.kind {
            if !scope.resource.contains(&format!("k={}:", kind_name(kind))) { return Err(V2Error::ScopeChanged); }
        }
        let page = cache.next_v2(cursor, &scope, Instant::now(), round.remaining()).map_err(V2Error::from)?;
        let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
        if after != id { return Err(V2Error::ScopeChanged); }
        return Ok(Json(page_json(page)));
    }
    let keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let rows = match query.resource {
        V2Resource::Contacts => within_v2_round(&round, backend.contacts(&id, &keys, &round)).await?,
        V2Resource::Chats => within_v2_round(&round, backend.chats(&id, &keys, &round)).await?,
        V2Resource::Members => within_v2_round(&round, backend.members(&id, &keys, room.as_deref().unwrap_or_default(), &round)).await?,
    };
    let query_for_filter = query.clone();
    let cancel_for_filter = round.clone();
    let mut items = tokio::task::spawn_blocking(move || match query_for_filter.resource {
        V2Resource::Contacts => filter_contacts(rows, &query_for_filter, &cancel_for_filter),
        V2Resource::Chats => filter_chats(rows, &query_for_filter, &cancel_for_filter),
        V2Resource::Members => Ok(rows),
    }).await.map_err(|_| V2Error::SourceReadFailed)??;
    let after_keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    if keys != after_keys { return Err(V2Error::ScopeChanged); }
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if after != id { return Err(V2Error::ScopeChanged); }
    let resource = query.resource_key(room.as_deref());
    let scope = make_scope(&id, resource);
    let page_size = query.limit.unwrap_or(100);
    let config = cache.config();
    let prepared = tokio::task::spawn_blocking({
        let scope = scope.clone();
        let cancel = round.clone();
        move || wechat_read_pages::prepare(&config, scope, std::mem::take(&mut items), page_size, &cancel)
    }).await.map_err(|_| V2Error::DeadlineExceeded)?.map_err(V2Error::from)?;
    let page = cache.publish_v2(prepared, Instant::now(), round.remaining()).map_err(V2Error::from)?;
    Ok(Json(page_json(page)))
}

async fn within_v2_round<T, F>(round: &CollectionCancel, work: F) -> Result<T, V2Error>
where F: std::future::Future<Output = Result<T, V2Error>> {
    if round.expired() { round.cancel(); return Err(V2Error::DeadlineExceeded); }
    match tokio::time::timeout(round.remaining(), work).await {
        Ok(result) => result,
        Err(_) => { round.cancel(); Err(V2Error::DeadlineExceeded) }
    }
}

async fn contacts_handler<B: V2Backend>(State(backend): State<Arc<B>>, RawQuery(raw): RawQuery, cache: Option<Extension<Arc<PageCache>>>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    let query = parse_v2_query(raw.as_deref(), V2Resource::Contacts)?;
    let cache = cache.map(|Extension(v)| v).unwrap_or_else(|| PageCache::global().clone());
    serve_list(&*backend, &cache, query, None, budget.map(|Extension(v)| v).unwrap_or_default()).await
}

async fn chats_handler<B: V2Backend>(State(backend): State<Arc<B>>, RawQuery(raw): RawQuery, cache: Option<Extension<Arc<PageCache>>>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    let query = parse_v2_query(raw.as_deref(), V2Resource::Chats)?;
    let cache = cache.map(|Extension(v)| v).unwrap_or_else(|| PageCache::global().clone());
    serve_list(&*backend, &cache, query, None, budget.map(|Extension(v)| v).unwrap_or_default()).await
}

async fn members_handler<B: V2Backend>(State(backend): State<Arc<B>>, AxumPath(room): AxumPath<String>, RawQuery(raw): RawQuery, cache: Option<Extension<Arc<PageCache>>>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    if room.trim().is_empty() { return Err(V2Error::InvalidParameter); }
    let query = parse_v2_query(raw.as_deref(), V2Resource::Members)?;
    let cache = cache.map(|Extension(v)| v).unwrap_or_else(|| PageCache::global().clone());
    serve_list(&*backend, &cache, query, Some(room), budget.map(|Extension(v)| v).unwrap_or_default()).await
}

async fn chat_handler<B: V2Backend>(State(backend): State<Arc<B>>, AxumPath(username): AxumPath<String>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    if username.trim().is_empty() { return Err(V2Error::InvalidParameter); }
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let before = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    let keys = read::within_round(&round, backend.contact_keys(&before, &round)).await.map_err(V2Error::from)?;
    let chats = within_v2_round(&round, backend.chats(&before, &keys, &round)).await?;
    let after_keys = read::within_round(&round, backend.contact_keys(&before, &round)).await.map_err(V2Error::from)?;
    if keys != after_keys { return Err(V2Error::ScopeChanged); }
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if before != after { return Err(V2Error::ScopeChanged); }
    let chat = chats.into_iter().find(|chat| chat.get("username").and_then(Value::as_str) == Some(username.as_str())).ok_or(V2Error::NotFound)?;
    Ok(Json(chat))
}

async fn release_handler<B: V2Backend>(State(backend): State<Arc<B>>, AxumPath(token): AxumPath<String>, cache: Option<Extension<Arc<PageCache>>>, budget: Option<Extension<RoundBudget>>) -> Result<Json<Value>, V2Error> {
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let before = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    let cache = cache.map(|Extension(v)| v).unwrap_or_else(|| PageCache::global().clone());
    let released = cache.release_v2(&token, &before.account_dir, &before.epoch, Instant::now(), round.remaining()).map_err(V2Error::from)?;
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if after != before { return Err(V2Error::ScopeChanged); }
    Ok(Json(json!({"released": released})))
}

async fn message_types_handler() -> Result<Json<Value>, V2Error> {
    serde_json::from_str(crate::tools::message_types::MESSAGE_TYPE_DICTIONARY_JSON)
        .map(Json).map_err(|_| V2Error::SourceReadFailed)
}

fn parse_native_server_id(raw: &str) -> Result<i64, V2Error> {
    let value = raw.parse::<i64>().map_err(|_| V2Error::InvalidParameter)?;
    if value == 0 || value.to_string() != raw { return Err(V2Error::InvalidParameter); }
    Ok(value)
}

fn parse_locator_query(raw: Option<&str>) -> Result<(Option<String>, Option<String>), V2Error> {
    let mut chat = None;
    let mut expected = None;
    let mut seen = std::collections::HashSet::new();
    for pair in raw.unwrap_or_default().split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if !seen.insert(key.clone()) || value.trim().is_empty() { return Err(V2Error::InvalidParameter); }
        match key.as_str() {
            "chat_username" => chat = Some(value),
            "expected_account_username" => expected = Some(value),
            _ => return Err(V2Error::InvalidParameter),
        }
    }
    Ok((chat, expected))
}

async fn message_by_server_id_handler<B: V2Backend>(
    State(backend): State<Arc<B>>, AxumPath(raw_id): AxumPath<String>, RawQuery(raw): RawQuery,
    budget: Option<Extension<RoundBudget>>,
) -> Result<Json<Value>, V2Error> {
    let server_id = parse_native_server_id(&raw_id)?;
    let (chat, expected) = parse_locator_query(raw.as_deref())?;
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let id = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if expected.as_deref().is_some_and(|username| username != id.account_id) { return Err(V2Error::ScopeChanged); }
    let keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let anchor = within_v2_round(&round, backend.locate_message(&id, &keys, server_id, chat.as_deref(), &round)).await?;
    let after_keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if keys != after_keys || after != id { return Err(V2Error::ScopeChanged); }
    serde_json::to_value(anchor.message).map(Json).map_err(|_| V2Error::SourceReadFailed)
}

async fn raw_voice_by_server_id_handler<B: V2Backend>(
    State(backend): State<Arc<B>>, AxumPath(raw_id): AxumPath<String>, RawQuery(raw): RawQuery,
    budget: Option<Extension<RoundBudget>>,
) -> Result<Response, V2Error> {
    let server_id = parse_native_server_id(&raw_id)?;
    let (chat, expected) = parse_locator_query(raw.as_deref())?;
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let _guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let id = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if expected.as_deref().is_some_and(|username| username != id.account_id) { return Err(V2Error::ScopeChanged); }
    let keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let anchor = within_v2_round(&round, backend.locate_message(&id, &keys, server_id, chat.as_deref(), &round)).await?;
    let request = VoiceRequest {
        chat_id: anchor.message.chat_username.clone(), source_shard: anchor.source_shard.clone(),
        local_id: anchor.local_id, server_id: anchor.server_id, created: anchor.created,
    };
    let voice = within_v2_round(&round, backend.voice(&id, &request, &keys, &round)).await?;
    let after_keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if keys != after_keys || after != id { return Err(V2Error::ScopeChanged); }
    if voice.audio.is_empty() || voice.audio.len() > crate::tools::wechat_read_voice::MAX_VOICE_BYTES { return Err(V2Error::SourceReadFailed); }
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "audio/silk")
        .header(axum::http::header::CACHE_CONTROL, "no-store")
        .body(axum::body::Body::from(voice.audio))
        .map_err(|_| V2Error::SourceReadFailed)
}

fn parse_media_locator_query(raw: Option<&str>) -> Result<(Option<String>, Option<String>, crate::tools::wechat_media::ImageQuality), V2Error> {
    let mut chat = None;
    let mut expected = None;
    let mut quality = crate::tools::wechat_media::ImageQuality::Best;
    let mut seen = std::collections::HashSet::new();
    for pair in raw.unwrap_or_default().split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if !seen.insert(key.clone()) || value.trim().is_empty() { return Err(V2Error::InvalidParameter); }
        match key.as_str() {
            "chat_username" => chat = Some(value),
            "expected_account_username" => expected = Some(value),
            "quality" => quality = serde_json::from_value(Value::String(value)).map_err(|_| V2Error::InvalidParameter)?,
            _ => return Err(V2Error::InvalidParameter),
        }
    }
    Ok((chat, expected, quality))
}

async fn message_media_by_server_id_handler<B: V2Backend>(
    State(backend): State<Arc<B>>, AxumPath(raw_id): AxumPath<String>, RawQuery(raw): RawQuery,
    budget: Option<Extension<RoundBudget>>,
) -> Result<Json<Value>, V2Error> {
    use crate::tools::wechat_media::{best_image_target, download_metadata_from_anchor, get_message_media_from_anchor, pending, ImageQuality};
    use crate::tools::media_download::{current_process, ensure_queued};

    let server_id = parse_native_server_id(&raw_id)?;
    let (chat, expected, quality) = parse_media_locator_query(raw.as_deref())?;
    let budget = budget.map(|Extension(v)| v).unwrap_or_default();
    let round = CollectionCancel::within(budget.total);
    let guard = read::take_gui_guard(budget, &round).await.map_err(V2Error::from)?;
    let id = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if expected.as_deref().is_some_and(|username| username != id.account_id) { return Err(V2Error::ScopeChanged); }
    let keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let anchor = within_v2_round(&round, backend.locate_message(&id, &keys, server_id, chat.as_deref(), &round)).await?;
    let after_keys = read::within_round(&round, backend.contact_keys(&id, &round)).await.map_err(V2Error::from)?;
    let after = read::within_round(&round, backend.capture(&round)).await.map_err(V2Error::from)?;
    if keys != after_keys || after != id { return Err(V2Error::ScopeChanged); }
    drop(guard);

    let image_keys = {
        let db = crate::db::get_db();
        crate::tools::wechat_keys::get_image_keys(&db, &id.session_id, &id.account_dir)
    };

    macro_rules! finish_media {
        ($media:expr) => {{
            let verify_round = CollectionCancel::within(RoundBudget::default().total);
            let _verify_guard = read::take_gui_guard(RoundBudget::default(), &verify_round).await.map_err(V2Error::from)?;
            let verify_id = read::within_round(&verify_round, backend.capture(&verify_round)).await.map_err(V2Error::from)?;
            let verify_keys = read::within_round(&verify_round, backend.contact_keys(&verify_id, &verify_round)).await.map_err(V2Error::from)?;
            let verify_image_keys = {
                let db = crate::db::get_db();
                crate::tools::wechat_keys::get_image_keys(&db, &verify_id.session_id, &verify_id.account_dir)
            };
            if verify_id != id || verify_keys != keys || verify_image_keys != image_keys { return Err(V2Error::ScopeChanged); }
            return serde_json::to_value($media).map(Json).map_err(|_| V2Error::SourceReadFailed);
        }};
    }

    let account = id.account_dir.clone();
    let chat = anchor.message.chat_username.clone();
    let image_keys_for_read = image_keys.clone();
    let read_anchor = anchor.clone();
    let read_keys = keys.clone();
    let (result, media_content) = tokio::task::spawn_blocking(move || {
        let content = crate::tools::wechat_media::decode_media_message_content(
            &read_anchor.raw_source_hex, read_anchor.raw_source_compressed,
        );
        let result = get_message_media_from_anchor(
            &account, &read_keys, &chat, read_anchor.local_id, read_anchor.local_type,
            read_anchor.created, content.clone(), image_keys_for_read, quality,
        );
        (result, content)
    }).await.unwrap_or_else(|_| (pending(), String::new()));
    if result.media_type == "unsupported"
        || (result.data.is_some() && (quality != ImageQuality::Best || result.media_type != "image" || result.quality.as_deref() == Some("full")))
    {
        finish_media!(result);
    }
    let metadata = download_metadata_from_anchor(
        &id.account_id, &anchor.message.chat_username, anchor.message.sender_username.as_deref(),
        anchor.local_id, anchor.local_type, anchor.server_id, anchor.sort_seq.as_deref(),
        anchor.created, &media_content,
    );
    let Some(metadata) = metadata else { finish_media!(result); };
    let advertised = best_image_target(metadata["content"].as_str().unwrap_or(""));
    let target_rank = if quality == ImageQuality::Best && metadata["local_type"] == 3 { crate::router::messages::image_quality_rank(Some(advertised)) } else { 0 };
    if target_rank > 0 && result.data.is_some() && crate::router::messages::image_quality_rank(result.quality.as_deref()) >= target_rank {
        finish_media!(result);
    }
    if !crate::router::messages::needs_native_transfer(quality, metadata["local_type"].as_i64().unwrap_or_default(), advertised) {
        finish_media!(result);
    }
    let Some((_, process_identity)) = current_process(&id.account_dir) else {
        finish_media!(result);
    };
    if !ensure_queued(id.account_dir.clone(), metadata.clone()).await {
        let same_process = current_process(&id.account_dir).map(|(_, current)| current).as_ref() == Some(&process_identity);
        finish_media!(if same_process { result } else { pending() });
    }
    let mut best_so_far = result;
    let target = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while tokio::time::Instant::now() < target {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if current_process(&id.account_dir).map(|(_, current)| current).as_ref() != Some(&process_identity) {
            finish_media!(pending());
        }
        let account = id.account_dir.clone();
        let chat = anchor.message.chat_username.clone();
        let keys = keys.clone();
        let image_keys = image_keys.clone();
        let read_anchor = anchor.clone();
        let media_content = media_content.clone();
        let found = tokio::task::spawn_blocking(move || get_message_media_from_anchor(
            &account, &keys, &chat, read_anchor.local_id, read_anchor.local_type,
            read_anchor.created, media_content, image_keys, quality,
        )).await.unwrap_or_else(|_| pending());
        if found.data.is_some() {
            if current_process(&id.account_dir).map(|(_, current)| current).as_ref() != Some(&process_identity) {
                finish_media!(pending());
            }
            if target_rank == 0 || crate::router::messages::image_quality_rank(found.quality.as_deref()) >= target_rank {
                finish_media!(found);
            }
            if best_so_far.data.is_none() || crate::router::messages::image_quality_rank(found.quality.as_deref()) > crate::router::messages::image_quality_rank(best_so_far.quality.as_deref()) { best_so_far = found; }
        }
    }
    finish_media!(best_so_far);
}

pub(crate) fn read_v2_routes() -> Router {
    read_v2_routes_with(Arc::new(ProcessReadBackend), None, RoundBudget::default())
}

fn read_v2_routes_with<B: V2Backend>(backend: Arc<B>, cache: Option<Arc<PageCache>>, budget: RoundBudget) -> Router {
    let router = Router::new()
        .route("/api/v2/contacts", get(contacts_handler::<B>))
        .route("/api/v2/chats", get(chats_handler::<B>))
        .route("/api/v2/chats/{username}", get(chat_handler::<B>))
        .route("/api/v2/chats/{username}/members", get(members_handler::<B>))
        .route("/api/v2/chats/{username}/messages", get(chat_messages_handler::<B>))
        .route("/api/v2/message-types", get(message_types_handler))
        .route("/api/v2/messages/{server_id}", get(message_by_server_id_handler::<B>))
        .route("/api/v2/messages/{server_id}/voice", get(raw_voice_by_server_id_handler::<B>))
        .route("/api/v2/messages/{server_id}/media", get(message_media_by_server_id_handler::<B>))
        .route("/api/v2/read-snapshots/{release_token}", delete(release_handler::<B>))
        .with_state(backend)
        .layer(Extension(budget));
    match cache { Some(cache) => router.layer(Extension(cache)), None => router }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    struct FakeBackend {
        identities: Mutex<VecDeque<ReadId>>,
        rows: Vec<Value>,
        contact_collections: AtomicUsize,
        voice_requests: Mutex<Vec<VoiceRequest>>,
    }

    impl FakeBackend {
        fn new(rows: Vec<Value>) -> Arc<Self> {
            Arc::new(Self { identities: Mutex::new(VecDeque::from([test_id("acct-a", "epoch-a")])), rows, contact_collections: AtomicUsize::new(0), voice_requests: Mutex::new(Vec::new()) })
        }

        fn identities(rows: Vec<Value>, identities: Vec<ReadId>) -> Arc<Self> {
            Arc::new(Self { identities: Mutex::new(VecDeque::from(identities)), rows, contact_collections: AtomicUsize::new(0), voice_requests: Mutex::new(Vec::new()) })
        }

        fn next_identity(&self) -> ReadId {
            let mut queue = self.identities.lock().unwrap();
            if queue.len() > 1 { queue.pop_front().unwrap() } else { queue.front().unwrap().clone() }
        }
    }

    #[async_trait::async_trait]
    impl V2Backend for FakeBackend {
        async fn capture(&self, _cancel: &CollectionCancel) -> Result<ReadId, ReadError> { Ok(self.next_identity()) }
        async fn contact_keys(&self, _id: &ReadId, _cancel: &CollectionCancel) -> Result<HashMap<String, String>, ReadError> {
            Ok(HashMap::from([("contact.db".to_string(), "synthetic".to_string()), ("session.db".to_string(), "synthetic".to_string())]))
        }
        async fn contacts(&self, _id: &ReadId, _keys: &HashMap<String, String>, _cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> {
            self.contact_collections.fetch_add(1, Ordering::SeqCst);
            Ok(self.rows.clone())
        }
        async fn chats(&self, _id: &ReadId, _keys: &HashMap<String, String>, _cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> { Ok(self.rows.clone()) }
        async fn members(&self, _id: &ReadId, _keys: &HashMap<String, String>, _room: &str, _cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> { Ok(self.rows.clone()) }
        async fn messages(&self, _id: &ReadId, _keys: &HashMap<String, String>, _chat: &str, _cancel: &CollectionCancel) -> Result<Vec<Value>, V2Error> { Ok(self.rows.clone()) }
        async fn locate_message(&self, _id: &ReadId, _keys: &HashMap<String, String>, server_id: i64, chat: Option<&str>, _cancel: &CollectionCancel) -> Result<MessageSourceAnchor, V2Error> {
            let matches: Vec<_> = self.rows.iter().filter(|row| {
                row.get("serverId").and_then(Value::as_i64) == Some(server_id)
                    && chat.is_none_or(|chat| row.get("chatId").and_then(Value::as_str) == Some(chat))
            }).collect();
            if matches.len() > 1 { return Err(V2Error::Ambiguous); }
            let row = matches.first().ok_or(V2Error::NotFound)?;
            let message = crate::tools::wechat_messages::message_v2_from_snapshot(row).ok_or(V2Error::SourceReadFailed)?;
            Ok(MessageSourceAnchor {
                local_id: row["localId"].as_i64().unwrap(), server_id, created: 123,
                local_type: row["rawType"].as_i64().unwrap(), source_shard: "message_7.db".into(),
                table_name: crate::tools::wechat_messages::get_msg_table_name(&message.chat_username),
                raw_content: row["rawContent"].as_str().unwrap_or_default().into(),
                raw_source_hex: row["rawContent"].as_str().unwrap_or_default().into(),
                raw_source_compressed: false, sort_seq: Some("55".into()), message,
            })
        }
        async fn voice(&self, _id: &ReadId, request: &VoiceRequest, _keys: &HashMap<String, String>, _cancel: &CollectionCancel) -> Result<read::VerifiedVoice, V2Error> {
            self.voice_requests.lock().unwrap().push(request.clone());
            Ok(read::VerifiedVoice { media_shard: "media_0.db".into(), media_row_id: 1, media_owner_id: 2, audio: vec![0x02, 0x23], sha256: "synthetic".into() })
        }
    }

    fn test_id(account: &str, epoch: &str) -> ReadId {
        ReadId {
            account_id: account.to_string(), account_dir: account.to_string(),
            epoch: epoch.to_string(), database_root: PathBuf::from("/synthetic"),
            session_id: "test-session".to_string(),
        }
    }

    fn contact_rows(count: usize) -> Vec<Value> {
        (0..count).map(|index| json!({
            "username": format!("member-{index}"), "alias": "", "nick_name": format!("Name {index}"),
            "remark": "", "display_name": format!("Name {index}"), "kind": "individual", "local_type": 1,
        })).collect()
    }

    fn contact(username: &str, nickname: &str, kind: &str) -> Value {
        json!({
            "username": username, "alias": "", "nick_name": nickname,
            "remark": "", "display_name": nickname, "kind": kind, "local_type": 1,
        })
    }

    fn router(backend: Arc<FakeBackend>) -> Router {
        read_v2_routes_with(backend, Some(Arc::new(PageCache::new())), RoundBudget::default())
    }

    #[test]
    fn locator_queries_use_chat_username_and_reject_legacy_key() {
        assert_eq!(parse_locator_query(Some("chat_username=chat-a")).unwrap().0.as_deref(), Some("chat-a"));
        assert_eq!(parse_locator_query(Some("conversation_username=chat-a")), Err(V2Error::InvalidParameter));
        assert_eq!(parse_media_locator_query(Some("chat_username=chat-a")).unwrap().0.as_deref(), Some("chat-a"));
        assert!(matches!(parse_media_locator_query(Some("conversation_username=chat-a")), Err(V2Error::InvalidParameter)));
    }

    async fn call(router: &Router, method: &str, uri: &str) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri).body(Body::empty()).unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn call_bytes(router: &Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let request = Request::builder().method("GET").uri(uri).body(Body::empty()).unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap().to_vec();
        (status, headers, bytes)
    }

    #[test]
    fn v2_list_queries_reject_offset_and_bind_filters_to_the_resource() {
        assert_eq!(
            parse_v2_query(Some("limit=2&offset=0"), V2Resource::Contacts).unwrap_err(),
            V2Error::InvalidParameter
        );
        let first = parse_v2_query(
            Some("q=Alice&kind=group&limit=1"),
            V2Resource::Contacts,
        )
        .unwrap();
        let changed = parse_v2_query(
            Some("q=Bob&kind=group&cursor=opaque"),
            V2Resource::Contacts,
        )
        .unwrap();
        assert_ne!(first.resource_key(None), changed.resource_key(None));
    }

    #[tokio::test]
    async fn short_pages_repeat_stably_end_and_release_with_opaque_token() {
        let router = router(FakeBackend::new(contact_rows(3)));
        let (status, first) = call(&router, "GET", "/api/v2/contacts?limit=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["items"].as_array().unwrap().len(), 1);
        let cursor = first["next_cursor"].as_str().unwrap();
        let token = first["release_token"].as_str().unwrap();
        let (_, second) = call(&router, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await;
        let (_, second_retry) = call(&router, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await;
        assert_eq!(second, second_retry);
        assert_eq!(second["items"][0]["username"], "member-1");
        assert_eq!(second["release_token"], token);
        let last_cursor = second["next_cursor"].as_str().unwrap();
        let (_, last) = call(&router, "GET", &format!("/api/v2/contacts?cursor={last_cursor}")).await;
        assert_eq!(last["next_cursor"], Value::Null);
        assert_eq!(call(&router, "DELETE", &format!("/api/v2/read-snapshots/{token}")).await.0, StatusCode::OK);
        assert_eq!(call(&router, "DELETE", &format!("/api/v2/read-snapshots/{token}")).await.0, StatusCode::GONE);
        assert_eq!(call(&router, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await.0, StatusCode::GONE);
    }

    #[tokio::test]
    async fn v2_rejects_offset_expired_cursor_account_switch_and_filter_tampering() {
        let routes = router(FakeBackend::new(contact_rows(3)));
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?offset=0").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?cursor={}", uuid::Uuid::new_v4())).await.0, StatusCode::GONE);
        let (_, first) = call(&routes, "GET", "/api/v2/contacts?q=Name&limit=1").await;
        let cursor = first["next_cursor"].as_str().unwrap();
        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?q=Different&cursor={cursor}")).await.0, StatusCode::CONFLICT);

        let switching = router(FakeBackend::identities(contact_rows(3), vec![test_id("acct-a", "epoch-a"), test_id("acct-a", "epoch-a"), test_id("acct-b", "epoch-b")]));
        let (_, first) = call(&switching, "GET", "/api/v2/contacts?limit=1").await;
        let cursor = first["next_cursor"].as_str().unwrap();
        assert_eq!(call(&switching, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn expected_account_username_and_member_room_are_bound_to_the_snapshot() {
        let routes = router(FakeBackend::new(contact_rows(3)));
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?expected_account_username=acct-a").await.0, StatusCode::OK);
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?expected_account_username=wrong").await.0, StatusCode::CONFLICT);

        let (_, first) = call(&routes, "GET", "/api/v2/chats/room-a%40chatroom/members?limit=1").await;
        let cursor = first["next_cursor"].as_str().unwrap();
        assert_eq!(call(&routes, "GET", &format!("/api/v2/chats/room-b%40chatroom/members?cursor={cursor}")).await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/chats/room-a%40chatroom/members?cursor={cursor}")).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn expected_account_username_matches_account_id_not_directory_suffix() {
        let mut identity = test_id("other", "epoch-foo");
        identity.account_id = "foo".to_string();
        identity.account_dir = "foo_install".to_string();
        let backend = FakeBackend::identities(contact_rows(1), vec![identity]);
        let routes = router(backend);

        assert_eq!(call(&routes, "GET", "/api/v2/contacts?expected_account_username=foo").await.0, StatusCode::OK);
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?expected_account_username=wrong").await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?expected_account_username=foo_install").await.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cursor_only_inherits_frozen_filters_without_recollecting_and_rejects_changes() {
        let backend = FakeBackend::new(vec![
            contact("alice", "Alice", "individual"),
            contact("alicia", "Alicia", "individual"),
            contact("ali-group", "Ali Group", "group"),
            contact("bob", "Bob", "individual"),
        ]);
        let routes = router(backend.clone());
        let (status, first) = call(&routes, "GET", "/api/v2/contacts?q=Ali&kind=individual&limit=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["items"][0]["username"], "alice");
        let cursor = first["next_cursor"].as_str().unwrap();

        let (status, inherited) = call(&routes, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(inherited["items"][0]["username"], "alicia");
        let (status, repeated) = call(&routes, "GET", &format!("/api/v2/contacts?cursor={cursor}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeated, inherited);
        assert_eq!(backend.contact_collections.load(Ordering::SeqCst), 1);

        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?q=Ali&kind=individual&cursor={cursor}")).await.0, StatusCode::OK);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?q=Bob&cursor={cursor}")).await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?q=&cursor={cursor}")).await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/contacts?kind=group&cursor={cursor}")).await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", &format!("/api/v2/chats?cursor={cursor}")).await.0, StatusCode::CONFLICT);
        assert_eq!(backend.contact_collections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn single_page_response_has_explicit_nulls_and_no_release_slot() {
        let router = router(FakeBackend::new(contact_rows(1)));
        let (status, page) = call(&router, "GET", "/api/v2/contacts").await;
        assert_eq!(status, StatusCode::OK);
        assert!(page["next_cursor"].is_null());
        assert!(page["release_token"].is_null());
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn message_type_dictionary_is_served_from_the_canonical_shared_json() {
        let routes = router(FakeBackend::new(Vec::new()));
        let (status, dictionary) = call(&routes, "GET", "/api/v2/message-types").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(dictionary["main"][0]["key"], "text");
        assert_eq!(dictionary["appmsg"][3]["key"], "file");
        assert_eq!(dictionary["forwarded_inner"][0]["key"], "text");
        assert_eq!(dictionary["appmsg"][3]["code"], 6);
    }

    #[tokio::test]
    async fn messages_v2_page_keeps_large_ids_as_decimal_strings() {
        let row = json!({
            "serverId": 9168265261376983777_i64, "localId": 7_i64,
            "rawType": 25769803825_i64, "chatId": "chat-a", "sender": "wxid-a",
            "senderName": "Name", "type": 49, "content": "body",
            "timestamp": "2026-01-01T00:00:00Z", "isSelf": null,
            "isMentioned": null, "rawContent": "3c6d73672f3e", "rawContentCompression": false
        });
        let routes = router(FakeBackend::new(vec![row]));
        let (status, page) = call(&routes, "GET", "/api/v2/chats/chat-a/messages").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["items"][0]["server_id"], "9168265261376983777");
        assert_eq!(page["items"][0]["raw_type"], "25769803825");
        assert_eq!(page["items"][0]["type"], 49);
        assert_eq!(page["items"][0]["sub_type"], 6);
        assert_eq!(page["items"][0]["raw_content_compression"], false);
    }

    #[tokio::test]
    async fn v2_routes_serialize_renamed_chat_fields_and_nullable_source_values() {
        let chat_row = json!({
            "username": "room@chatroom", "display_name": "Room", "remark": "", "nick_name": "Room",
            "unread_count": null, "is_group": true, "last_message_preview": null,
            "last_msg_sender": "_$_CUSTOM_USERNAME_PREFIX_$_1234567890123456789:gh_fixture", "last_sender_display_name": "A; B",
            "last_activity_at": null, "last_msg_local_id": null
        });
        let mut chat_with_count = chat_row.clone();
        chat_with_count["username"] = json!("chat-with-count");
        chat_with_count["unread_count"] = json!(0);
        let routes = router(FakeBackend::new(vec![chat_row.clone(), chat_with_count]));
        let (status, page) = call(&routes, "GET", "/api/v2/chats").await;
        assert_eq!(status, StatusCode::OK);
        let chat = page["items"].as_array().unwrap().iter().find(|c| c["username"] == "room@chatroom").unwrap();
        assert_eq!(chat["unread_count"], Value::Null);
        assert_eq!(chat["last_msg_sender"], "_$_CUSTOM_USERNAME_PREFIX_$_1234567890123456789:gh_fixture");
        assert_eq!(chat["last_sender_display_name"], "A; B");
        assert!(chat.get("last_message_sender_username").is_none());
        assert!(chat.get("last_message_sender_display_name").is_none());
        assert_eq!(page["items"].as_array().unwrap().iter().find(|c| c["username"] == "chat-with-count").unwrap()["unread_count"], 0);
        let chat_fixture = page["items"].clone();

        let reference_xml = "<msg><refermsg><svrid>12345</svrid><fromusr>chat-a</fromusr><chatusr>quoted-user</chatusr><displayname>Quoted</displayname><content>sample</content></refermsg></msg>";
        let mut valid_row = message_row("78", 1, 0, 8);
        valid_row["rawContent"] = json!(reference_xml.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>());
        let mut invalid_row = message_row("77", 1, 0, 7);
        invalid_row["timestamp"] = json!("");
        let routes = router(FakeBackend::new(vec![invalid_row, valid_row]));
        let (status, page) = call(&routes, "GET", "/api/v2/chats/chat-a/messages").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["items"].as_array().unwrap().iter().find(|m| m["server_id"] == "77").unwrap()["timestamp"], Value::Null);
        assert_eq!(page["items"].as_array().unwrap().iter().find(|m| m["server_id"] == "78").unwrap()["timestamp"], "2026-01-01T00:00:00Z");
        let (lookup_status, lookup) = call(&routes, "GET", "/api/v2/messages/78?chat_username=chat-a").await;
        assert_eq!(lookup_status, StatusCode::OK);
        assert_eq!(lookup["reference"]["chat_username"], "chat-a");
        assert!(lookup["reference"].get("conversation_username").is_none());
        let fixture = json!({
            "source": "Synthetic DTOs serialized through Rust read_v2 Router; timestamp:null here is synthetic; real V2 collection rejects missing/non-integer create_time, while an out-of-range integer becomes the existing empty time then null",
            "chats": chat_fixture,
            "messages": page["items"],
            "message_lookup_request": {"method": "GET", "path": "/api/v2/messages/78", "query": {"chat_username": "chat-a"}},
            "message_lookup": lookup
        });
        std::fs::write("/tmp/opencode/source-field-semantics-fixtures.json", serde_json::to_vec_pretty(&fixture).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn message_list_and_native_get_share_reference_and_forwarded_projections() {
        let body = r#"<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datatype>1</datatype><dataid>9007199254740993</dataid><datadesc>inner text</datadesc><sourcename>Inner name</sourcename><dataitemsource><hashusername>hash-only</hashusername></dataitemsource></dataitem></datalist></recordinfo>]]></recorditem><refermsg><svrid>1234567890123456789</svrid><fromusr>chat-a</fromusr><chatusr>quoted-author</chatusr><displayname>42</displayname><content>quoted</content></refermsg></appmsg></msg>"#;
        let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = json!({
            "serverId": 77_i64, "localId": 7_i64, "rawType": ((6_i64 << 32) | 49),
            "chatId": "chat-a", "sender": "outer-author", "senderName": "Outer name",
            "content": "old clean title", "timestamp": "2026-01-01T00:00:00Z",
            "isSelf": null, "isMentioned": null, "rawContent": raw_hex,
            "rawContentCompression": false,
        });
        let routes = router(FakeBackend::new(vec![row]));
        let (list_status, page) = call(&routes, "GET", "/api/v2/chats/chat-a/messages").await;
        let (get_status, message) = call(&routes, "GET", "/api/v2/messages/77?chat_username=chat-a").await;
        assert_eq!(list_status, StatusCode::OK);
        assert_eq!(get_status, StatusCode::OK);
        let listed = &page["items"][0];
        assert_eq!(listed["reference"], message["reference"]);
        assert_eq!(listed["forwarded_record"], message["forwarded_record"]);
        assert_eq!(listed["xml_sub_type"], 19);
        assert_eq!(listed["sub_type"], 6);
        assert_eq!(listed["content"], "old clean title");
        assert_eq!(listed["raw_content_compression"], false);
        assert_eq!(listed["raw_content_compression"], message["raw_content_compression"]);
        assert_eq!(listed["reference"]["chat_username"], "chat-a");
        assert!(listed["reference"].get("conversation_username").is_none());
        assert_eq!(listed["forwarded_record"]["items"][0]["dataitem_source_hash_username"], "hash-only");
        assert!(listed["forwarded_record"]["items"][0].get("username").is_none());
    }

    #[tokio::test]
    async fn message_routes_fail_closed_when_raw_content_compression_metadata_is_missing() {
        let mut row = message_row("77", 1, 0, 7);
        row.as_object_mut().unwrap().remove("rawContentCompression");
        let routes = router(FakeBackend::new(vec![row]));

        let (list_status, list_error) = call(&routes, "GET", "/api/v2/chats/chat-a/messages").await;
        assert_eq!(list_status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(list_error["code"], "source_read_failed");

        let (get_status, get_error) = call(&routes, "GET", "/api/v2/messages/77?chat_username=chat-a").await;
        assert_eq!(get_status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(get_error["code"], "source_read_failed");
    }

    #[tokio::test]
    async fn native_message_and_raw_voice_routes_use_exact_source_anchor() {
        let large_id = 9_168_265_261_376_983_777_i64;
        let backend = FakeBackend::new(vec![message_row(&large_id.to_string(), 34, 0, 7)]);
        let routes = router(backend.clone());
        let (status, message) = call(&routes, "GET", &format!("/api/v2/messages/{large_id}?chat_username=chat-a&expected_account_username=acct-a")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(message["server_id"], "9168265261376983777");
        assert_eq!(message["chat_username"], "chat-a");

        let (status, headers, bytes) = call_bytes(&routes, &format!("/api/v2/messages/{large_id}/voice?chat_username=chat-a")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(axum::http::header::CONTENT_TYPE).unwrap(), "audio/silk");
        assert_eq!(bytes, [0x02, 0x23]);
        let request = backend.voice_requests.lock().unwrap()[0].clone();
        assert_eq!(request.chat_id, "chat-a");
        assert_eq!(request.source_shard, "message_7.db");
        assert_eq!(request.local_id, 7);
        assert_eq!(request.server_id, large_id);
        assert_eq!(request.created, 123);
    }

    #[tokio::test]
    async fn native_id_routes_refuse_zero_ambiguity_and_account_drift() {
        let one = message_row("77", 1, 0, 1);
        let two = message_row("77", 1, 0, 2);
        let routes = router(FakeBackend::new(vec![one, two]));
        assert_eq!(call(&routes, "GET", "/api/v2/messages/0").await.0, StatusCode::BAD_REQUEST);
        let (status, error) = call(&routes, "GET", "/api/v2/messages/77?chat_username=chat-a").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["code"], "ambiguous");

        let unique = router(FakeBackend::new(vec![message_row("77", 1, 0, 1)]));
        assert_eq!(call(&unique, "GET", "/api/v2/messages/77?conversation_username=chat-a").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(call(&unique, "GET", "/api/v2/messages/77?expected_account_username=not-this-account").await.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn message_type_filters_freeze_before_paging_and_keep_unknown_codes() {
        let rows = vec![
            message_row("101", 1, 0, 1),
            message_row("102", 49, 6, 2),
            message_row("103", 49, 999, 3),
            message_row("104", 777, 0, 4),
        ];
        let routes = router(FakeBackend::new(rows));
        let uri = "/api/v2/chats/chat-a/messages?include=%5B%7B%22type%22%3A49%7D%5D&exclude=%5B%7B%22type%22%3A49%2C%22sub_type%22%3A100%7D%5D&order=asc&limit=1";
        let (status, first) = call(&routes, "GET", uri).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["items"].as_array().unwrap().len(), 1);
        assert_eq!(first["items"][0]["server_id"], "102");
        let cursor = first["next_cursor"].as_str().unwrap();
        let (status, second) = call(&routes, "GET", &format!("/api/v2/chats/chat-a/messages?cursor={cursor}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(second["items"][0]["server_id"], "103");
        assert_eq!(call(&routes, "GET", &format!("/api/v2/chats/chat-a/messages?cursor={cursor}&include=%5B%5D")).await.0, StatusCode::CONFLICT);
        assert_eq!(call(&routes, "GET", "/api/v2/chats/chat-a/messages?include=%5B%5D").await.1["items"].as_array().unwrap().len(), 0);
        assert_eq!(call(&routes, "GET", "/api/v2/chats/chat-a/messages?include=bad-json").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(call(&routes, "GET", "/api/v2/chats/chat-a/messages?include=%5B%7B%22type%22%3A49%2C%22oops%22%3A1%7D%5D").await.0, StatusCode::BAD_REQUEST);
        let (_, unknown) = call(&routes, "GET", "/api/v2/chats/chat-a/messages?include=%5B%7B%22type%22%3A777%7D%5D").await;
        assert_eq!(unknown["items"][0]["type"], 777);
        assert_eq!(unknown["items"][0]["server_id"], "104");
    }

    fn message_row(server_id: &str, msg_type: u32, sub_type: u32, local_id: i64) -> Value {
        let raw_type = ((sub_type as i64) << 32) | msg_type as i64;
        json!({
            "serverId": server_id.parse::<i64>().unwrap(), "localId": local_id,
            "rawType": raw_type, "chatId": "chat-a", "sender": "wxid-a",
            "senderName": "Name", "content": format!("body-{server_id}"),
            "timestamp": "2026-01-01T00:00:00Z", "isSelf": null,
            "isMentioned": null, "rawContent": "opaque", "rawContentCompression": false
        })
    }

    #[tokio::test]
    async fn contact_filters_are_applied_before_freezing_and_unknown_kind_is_rejected() {
        let routes = router(FakeBackend::new(contact_rows(3)));
        let (status, filtered) = call(&routes, "GET", "/api/v2/contacts?q=Name%202&kind=individual").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
        assert_eq!(filtered["items"][0]["username"], "member-2");
        assert_eq!(filtered["release_token"], Value::Null);
        assert_eq!(call(&routes, "GET", "/api/v2/contacts?kind=not-a-kind").await.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn chat_get_and_group_member_routes_keep_snake_case_resource_identity() {
        let routes = router(FakeBackend::new(contact_rows(1)));
        let (status, chat) = call(&routes, "GET", "/api/v2/chats/member-0").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(chat["username"], "member-0");
        let (status, members) = call(&routes, "GET", "/api/v2/chats/room%40chatroom/members").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(members["items"][0]["username"], "member-0");
        assert_eq!(members["items"][0]["display_name"], "Name 0");
        assert_eq!(call(&routes, "GET", "/api/v2/chats/missing").await.0, StatusCode::NOT_FOUND);
    }
}
