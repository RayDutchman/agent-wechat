use std::{sync::Arc, time::Duration};

use axum::{
    extract::rejection::JsonRejection,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    context::create_context,
    db::get_db,
    execution::{acquire_gui_guard, run_execution_loop_locked},
    ia::types::SubscriptionEvent,
    plans::send_message::{
        SendFailureStage, SendFence, SendMessageParams, SendMessagePlan,
        FENCE_SCOPE_CHANGED, FENCE_TARGET_CHANGED,
    },
    sessions::manager::get_session,
};

use super::{send_file, send_text, MAX_UPLOAD_BASE64_BYTES, MAX_UPLOAD_BYTES};

const SEND_DEADLINE: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendV2Request {
    chat_username: Value,
    #[serde(default)]
    expected_account_username: Option<Value>,
    #[serde(default)]
    text: Option<Value>,
    #[serde(default)]
    image: Option<Value>,
    #[serde(default)]
    file: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SelectedPayload {
    Text(String),
    Image { bytes: Vec<u8>, mime_type: String },
    File { bytes: Vec<u8>, filename: String, sha256: String },
}

impl SelectedPayload {
    fn kind(&self) -> &'static str {
        match self { Self::Text(_) => "text", Self::Image { .. } => "image", Self::File { .. } => "file" }
    }
}

#[derive(Debug)]
struct ValidatedRequest {
    chat: String,
    expected_account: Option<String>,
    selected: SelectedPayload,
    ignored_fields: Vec<String>,
}

impl ValidatedRequest {
    fn facts(&self) -> PreflightFacts {
        let (payload_sha256, payload_size) = match &self.selected {
            SelectedPayload::File { bytes, sha256, .. } => (Some(sha256.clone()), Some(bytes.len() as u64)),
            _ => (None, None),
        };
        PreflightFacts {
            chat_username: Some(self.chat.clone()),
            selected_payload: Some(self.selected.kind()),
            ignored_fields: self.ignored_fields.clone(),
            payload_sha256,
            payload_size,
            ..PreflightFacts::default()
        }
    }
}

fn object_fields<'a>(value: &'a Value, allowed: &[&str]) -> Result<&'a serde_json::Map<String, Value>, V2SendError> {
    let object = value.as_object().ok_or(V2SendError::invalid())?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) || allowed.iter().any(|key| !object.contains_key(*key)) {
        return Err(V2SendError::invalid());
    }
    Ok(object)
}

fn string_field(object: &serde_json::Map<String, Value>, name: &str) -> Result<String, V2SendError> {
    object.get(name).and_then(Value::as_str).map(str::to_owned).ok_or(V2SendError::invalid())
}

#[derive(Clone, Debug, Default)]
struct PreflightFacts {
    account_username: Option<String>,
    chat_username: Option<String>,
    selected_payload: Option<&'static str>,
    ignored_fields: Vec<String>,
    payload_sha256: Option<String>,
    payload_size: Option<u64>,
}

impl PreflightFacts {
    fn from_value(value: &Value) -> Self {
        let Some(object) = value.as_object() else { return Self::default(); };
        let chat_username = object.get("chat_username")
            .and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned);
        let selected_payload = if object.contains_key("file") { Some("file") }
            else if object.contains_key("image") { Some("image") }
            else if object.contains_key("text") { Some("text") } else { None };
        let ignored_fields = ["text", "image", "file"].into_iter()
            .filter(|field| object.contains_key(*field) && Some(*field) != selected_payload)
            .map(str::to_owned).collect();
        Self { chat_username, selected_payload, ignored_fields, ..Self::default() }
    }

    fn response(&self, error_code: &'static str) -> SendV2Response {
        SendV2Response {
            success: false,
            account_username: self.account_username.clone(),
            chat_username: self.chat_username.clone(),
            selected_payload: self.selected_payload,
            ignored_fields: self.ignored_fields.clone(),
            commit_attempted: false,
            ui_confirmed: false,
            error_code: Some(error_code),
            failure_stage: Some("preflight"),
            payload_sha256: (self.selected_payload == Some("file")).then(|| self.payload_sha256.clone()),
            payload_size: (self.selected_payload == Some("file")).then_some(self.payload_size),
        }
    }
}

fn validate_request(request: SendV2Request) -> Result<ValidatedRequest, V2SendError> {
    let facts = PreflightFacts {
        chat_username: request.chat_username.as_str().map(str::trim)
            .filter(|s| !s.is_empty()).map(str::to_owned),
        selected_payload: if request.file.is_some() { Some("file") }
            else if request.image.is_some() { Some("image") }
            else if request.text.is_some() { Some("text") } else { None },
        ignored_fields: [("text", request.text.is_some()), ("image", request.image.is_some()), ("file", request.file.is_some())]
            .into_iter().filter(|(field, present)| *present && Some(*field) != if request.file.is_some() { Some("file") } else if request.image.is_some() { Some("image") } else if request.text.is_some() { Some("text") } else { None })
            .map(|(field, _)| field.to_owned()).collect(),
        ..PreflightFacts::default()
    };
    validate_request_with_limits(request, MAX_UPLOAD_BASE64_BYTES, MAX_UPLOAD_BYTES)
        .map_err(|error| error.with_facts(facts))
}

fn validate_request_with_limits(request: SendV2Request, max_base64_bytes: usize, max_upload_bytes: usize) -> Result<ValidatedRequest, V2SendError> {
    let chat = request.chat_username.as_str().ok_or(V2SendError::invalid())?.trim();
    if chat.is_empty() { return Err(V2SendError::invalid()); }
    let expected_account = match request.expected_account_username {
        None => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Some(_) => return Err(V2SendError::invalid()),
    };
    let text = match request.text {
        None => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value),
        Some(_) => return Err(V2SendError::invalid()),
    };
    let image = match request.image {
        None => None,
        Some(value) => {
            let object = object_fields(&value, &["data", "mime_type"])?;
            let data = string_field(object, "data")?;
            let mime = string_field(object, "mime_type")?;
            if !matches!(mime.as_str(), "image/png" | "image/jpeg" | "image/gif" | "image/webp") {
                return Err(V2SendError::invalid());
            }
            Some((data, mime))
        }
    };
    let file = match request.file {
        None => None,
        Some(value) => {
            let object = object_fields(&value, &["filename", "data", "sha256", "size"])?;
            let filename = string_field(object, "filename")?;
            send_file::validate_filename(&filename).map_err(V2SendError::from_file)?;
            let data = string_field(object, "data")?;
            let sha256 = string_field(object, "sha256")?;
            if !send_file::is_lower_hex_64(&sha256) { return Err(V2SendError::invalid()); }
            let size = object.get("size").and_then(Value::as_u64).ok_or(V2SendError::invalid())?;
            Some((filename, data, sha256, size))
        }
    };
    let fields: Vec<&'static str> = [
        (text.is_some(), "text"), (image.is_some(), "image"), (file.is_some(), "file"),
    ].into_iter().filter_map(|(exists, name)| exists.then_some(name)).collect();
    if fields.is_empty() { return Err(V2SendError::invalid()); }

    // Match the legacy plan's branch order exactly. Only the selected payload's
    // bytes are decoded/verified; malformed lower-priority content is ignored
    // only after its JSON shape has been validated.
    let selected = if let Some((filename, data, digest, declared_size)) = file {
        if data.len() > max_base64_bytes { return Err(V2SendError::too_large()); }
        let bytes = base64::engine::general_purpose::STANDARD.decode(data).map_err(|_| V2SendError::new("invalid_encoding", StatusCode::BAD_REQUEST))?;
        if bytes.len() > max_upload_bytes { return Err(V2SendError::too_large()); }
        if bytes.len() as u64 != declared_size || send_file::sha256_hex(&bytes) != digest {
            return Err(V2SendError::new("payload_mismatch", StatusCode::BAD_REQUEST));
        }
        SelectedPayload::File { bytes, filename, sha256: digest }
    } else if let Some((data, mime_type)) = image {
        if data.len() > max_base64_bytes { return Err(V2SendError::too_large()); }
        let bytes = base64::engine::general_purpose::STANDARD.decode(data).map_err(|_| V2SendError::new("invalid_encoding", StatusCode::BAD_REQUEST))?;
        if bytes.is_empty() { return Err(V2SendError::invalid()); }
        if bytes.len() > max_upload_bytes { return Err(V2SendError::too_large()); }
        SelectedPayload::Image { bytes, mime_type }
    } else {
        SelectedPayload::Text(text.expect("text is the remaining legal payload"))
    };
    let ignored_fields = ["text", "image", "file"].into_iter()
        .filter(|field| fields.contains(field) && *field != selected.kind())
        .map(str::to_owned).collect();
    Ok(ValidatedRequest { chat: chat.to_owned(), expected_account, selected, ignored_fields })
}

#[derive(Debug)]
struct V2SendError { code: &'static str, status: StatusCode, facts: PreflightFacts }

impl V2SendError {
    fn new(code: &'static str, status: StatusCode) -> Self { Self { code, status, facts: PreflightFacts::default() } }
    fn with_facts(mut self, facts: PreflightFacts) -> Self { self.facts = facts; self }
    fn invalid() -> Self { Self::new("invalid_parameter", StatusCode::BAD_REQUEST) }
    fn too_large() -> Self { Self::new("payload_too_large", StatusCode::PAYLOAD_TOO_LARGE) }
    fn from_file(error: send_file::SendFileError) -> Self {
        match error {
            send_file::SendFileError::InvalidFilename => Self::new("invalid_filename", StatusCode::BAD_REQUEST),
            _ => Self::invalid(),
        }
    }
}

impl IntoResponse for V2SendError {
    fn into_response(self) -> Response {
        (self.status, Json(self.facts.response(self.code))).into_response()
    }
}

enum SendV2HandlerError {
    Contract(V2SendError),
    Passthrough(Response),
}

impl From<V2SendError> for SendV2HandlerError {
    fn from(error: V2SendError) -> Self { Self::Contract(error) }
}

impl IntoResponse for SendV2HandlerError {
    fn into_response(self) -> Response {
        match self {
            Self::Contract(error) => error.into_response(),
            Self::Passthrough(response) => response,
        }
    }
}

#[derive(Debug, Serialize)]
struct SendV2Response {
    success: bool,
    account_username: Option<String>,
    chat_username: Option<String>,
    selected_payload: Option<&'static str>,
    ignored_fields: Vec<String>,
    commit_attempted: bool,
    ui_confirmed: bool,
    error_code: Option<&'static str>,
    failure_stage: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_sha256: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_size: Option<Option<u64>>,
}

enum PreparedPayload {
    Text(String),
    Image { upload: send_file::TempUpload, mime_type: String },
    File { upload: send_file::TempUpload, sha256: String, size: u64 },
}

impl PreparedPayload {
    fn for_plan(&self) -> (Option<String>, Option<String>, Option<String>) {
        match self {
            Self::Text(text) => (Some(text.clone()), None, None),
            Self::Image { upload, mime_type } => (None, Some(upload.path().to_owned()), Some(mime_type.clone())),
            Self::File { upload, .. } => (None, None, Some(upload.path().to_owned())),
        }
    }
    fn file_facts(&self) -> (Option<String>, Option<u64>) {
        match self { Self::File { sha256, size, .. } => (Some(sha256.clone()), Some(*size)), _ => (None, None) }
    }
}

struct V2Execution {
    reached_goal: bool,
    commit_attempted: bool,
    fence_error: Option<&'static str>,
    error: Option<String>,
    failure_stage: Option<SendFailureStage>,
}

#[async_trait::async_trait]
trait SendV2Backend: Send + Sync + 'static {
    async fn resolve_target(&self) -> Result<send_file::SendFileTarget, V2SendError>;
    async fn capture_identity(&self) -> Result<send_text::SendIdentity, send_text::SendTextError>;
    async fn execute(&self, target: &send_file::SendFileTarget, chat: &str, payload: &PreparedPayload, epoch: &str, cancel: CancellationToken) -> V2Execution;
    async fn selected_chat(&self) -> Result<Option<String>, send_text::SendTextError>;
}

struct ProcessSendV2Backend;

#[async_trait::async_trait]
impl SendV2Backend for ProcessSendV2Backend {
    async fn resolve_target(&self) -> Result<send_file::SendFileTarget, V2SendError> {
        let session = get_session("default").ok_or(V2SendError::new("identity_unavailable", StatusCode::SERVICE_UNAVAILABLE))?;
        let passwd = std::fs::read_to_string("/etc/passwd").map_err(|_| V2SendError::new("identity_unavailable", StatusCode::SERVICE_UNAVAILABLE))?;
        send_file::target_from_session(session, &passwd).map_err(|_| V2SendError::new("identity_unavailable", StatusCode::SERVICE_UNAVAILABLE))
    }
    async fn capture_identity(&self) -> Result<send_text::SendIdentity, send_text::SendTextError> { send_text::capture_identity_now().await }
    async fn execute(&self, target: &send_file::SendFileTarget, chat: &str, payload: &PreparedPayload, epoch: &str, cancel: CancellationToken) -> V2Execution {
        let mut context = { let db = get_db(); create_context(target.session.clone(), &db) };
        let fence: Arc<dyn SendFence> = Arc::new(send_text::LiveSendFence::new(epoch.to_owned(), chat.to_owned(), send_text::exec_options_for(&context)));
        let (message, image_path, image_mime, file_path) = match payload {
            PreparedPayload::Text(text) => (Some(text.clone()), None, None, None),
            PreparedPayload::Image { upload, mime_type } => (None, Some(upload.path().to_owned()), Some(mime_type.clone()), None),
            PreparedPayload::File { upload, .. } => (None, None, None, Some(upload.path().to_owned())),
        };
        let params = SendMessageParams { chat_id: chat.to_owned(), message, image_path, image_mime, file_path, fence: Some(fence) };
        let emit = |_: SubscriptionEvent| {};
        let (result, state) = run_execution_loop_locked(&SendMessagePlan, &params, &mut context, &emit, cancel).await;
        V2Execution { reached_goal: result.success, commit_attempted: state.commit_attempted, fence_error: state.fence_error, error: result.error, failure_stage: state.failure_stage }
    }
    async fn selected_chat(&self) -> Result<Option<String>, send_text::SendTextError> { send_text::selected_chat_now().await }
}

async fn send_v2_handler<B: SendV2Backend>(
    State(backend): State<Arc<B>>,
    payload: Result<Json<Value>, JsonRejection>,
) -> Result<Json<SendV2Response>, SendV2HandlerError> {
    let Json(raw) = match payload {
        Ok(Json(raw)) => Json(raw),
        Err(JsonRejection::BytesRejection(rejection)) => {
            return Err(SendV2HandlerError::Passthrough(rejection.into_response()));
        }
        Err(_) => return Err(V2SendError::invalid().into()),
    };
    let raw_facts = PreflightFacts::from_value(&raw);
    let request = serde_json::from_value(raw).map_err(|_| V2SendError::invalid().with_facts(raw_facts))?;
    let request = validate_request(request)?;
    let facts = request.facts();
    let target = backend.resolve_target().await.map_err(|error| error.with_facts(facts.clone()))?;

    // Stage uploads before taking the GUI guard. TempUpload's Drop cleanup
    // preserves the established permission/lifetime behavior on every exit.
    let prepared = match request.selected {
        SelectedPayload::Text(text) => PreparedPayload::Text(text),
        SelectedPayload::Image { bytes, mime_type } => {
            let ext = match mime_type.as_str() { "image/jpeg" => "image.jpg", "image/gif" => "image.gif", "image/webp" => "image.webp", _ => "image.png" };
            let upload = send_file::TempUpload::stage(ext, &bytes, target.owner).map_err(|_| V2SendError::new("temp_storage_failed", StatusCode::INTERNAL_SERVER_ERROR).with_facts(facts.clone()))?;
            PreparedPayload::Image { upload, mime_type }
        }
        SelectedPayload::File { bytes, filename, sha256 } => {
            let size = bytes.len() as u64;
            let upload = send_file::TempUpload::stage(&filename, &bytes, target.owner).map_err(|_| V2SendError::new("temp_storage_failed", StatusCode::INTERNAL_SERVER_ERROR).with_facts(facts.clone()))?;
            PreparedPayload::File { upload, sha256, size }
        }
    };

    let _guard = acquire_gui_guard().await;
    let before = backend.capture_identity().await.map_err(|error| map_identity_error(error).with_facts(facts.clone()))?;
    if request.expected_account.as_deref().is_some_and(|expected| expected != before.account_id) {
        return Err(V2SendError::new("account_mismatch", StatusCode::CONFLICT).with_facts(facts).into());
    }
    let cancel = CancellationToken::new();
    let cancel_on_drop = cancel.clone();
    let _cancel_guard = CancelOnDrop(cancel_on_drop);
    let deadline_cancel = cancel.clone();
    let deadline = tokio::spawn(async move { tokio::time::sleep(SEND_DEADLINE).await; deadline_cancel.cancel(); });
    let execution = backend.execute(&target, &request.chat, &prepared, &before.epoch, cancel).await;
    deadline.abort();

    let mut error_code = execution.fence_error;
    if error_code.is_none() && !execution.reached_goal {
        error_code = Some(send_text::execution_error_code(execution.error.as_deref()));
    }
    let mut ui_confirmed = false;
    if execution.reached_goal && execution.commit_attempted {
        match backend.capture_identity().await {
            Ok(after) if after == before => match backend.selected_chat().await {
                Ok(Some(chat)) if chat == request.chat => { ui_confirmed = true; error_code = None; }
                _ => error_code = Some(FENCE_TARGET_CHANGED),
            },
            _ => error_code = Some(FENCE_SCOPE_CHANGED),
        }
    } else if execution.reached_goal {
        error_code = Some("commit_unconfirmed");
    }
    let stage = if ui_confirmed { None } else { execution.failure_stage };
    let (payload_sha256, payload_size) = prepared.file_facts();
    Ok(Json(SendV2Response {
        success: ui_confirmed,
        account_username: Some(before.account_id),
        chat_username: Some(request.chat),
        selected_payload: Some(match &prepared { PreparedPayload::Text(_) => "text", PreparedPayload::Image { .. } => "image", PreparedPayload::File { .. } => "file" }),
        ignored_fields: request.ignored_fields,
        commit_attempted: execution.commit_attempted,
        ui_confirmed,
        error_code,
        failure_stage: stage.map(|s| match s { SendFailureStage::Prepare => "prepare", SendFailureStage::PreCommit => "pre_commit", SendFailureStage::PostCommit => "post_commit" }),
        payload_sha256: payload_sha256.map(Some),
        payload_size: payload_size.map(Some),
    }))
}

fn map_identity_error(error: send_text::SendTextError) -> V2SendError {
    let (code, status) = match error {
        send_text::SendTextError::NotLoggedIn => ("not_logged_in", StatusCode::SERVICE_UNAVAILABLE),
        send_text::SendTextError::LoginUnverified => ("login_unverified", StatusCode::SERVICE_UNAVAILABLE),
        send_text::SendTextError::DeadlineExceeded => ("deadline_exceeded", StatusCode::SERVICE_UNAVAILABLE),
        send_text::SendTextError::ScopeChanged => ("scope_changed", StatusCode::CONFLICT),
        _ => ("identity_unavailable", StatusCode::SERVICE_UNAVAILABLE),
    };
    V2SendError::new(code, status)
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop { fn drop(&mut self) { self.0.cancel(); } }

pub(crate) fn send_v2_routes() -> Router {
    send_v2_routes_with(Arc::new(ProcessSendV2Backend))
}

fn send_v2_routes_with<B: SendV2Backend>(backend: Arc<B>) -> Router {
    Router::new().route("/api/v2/messages/send", post(send_v2_handler::<B>)).with_state(backend)
}
