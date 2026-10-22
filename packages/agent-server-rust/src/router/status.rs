use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query,
    },
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use base64::Engine;
use crate::context::create_context;
use crate::db::get_db;
use crate::execution::run_execution_loop;
use crate::ia::types::*;
use crate::ia::{find_state_by_id, identify_states};
use crate::plans::login::{LoginParams, LoginPlan};
use crate::plans::logout::{LogoutParams, LogoutPlan};
use crate::sessions::manager::get_session;
use crate::tools::a11y::get_a11y_desktop;
use crate::tools::exec::ExecOptions;
use crate::tools::qr::{decode_qr_from_base64, to_data_url};
use crate::tools::screenshot::capture_screenshot;

/// Observe the current account for the V2 contract. This is deliberately a
/// fresh, bounded observation: persisted login state is never used as evidence.
pub async fn account_v2() -> Json<serde_json::Value> {
    let Some(session) = get_session("default") else { return account_unknown("session_unavailable"); };
    account_observation(&ProcessAccountBackend { session }, std::time::Duration::from_secs(10)).await
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountScope { account_username: String, epoch: String }

#[async_trait::async_trait]
trait AccountObservationBackend: Send + Sync {
    fn process_running(&self) -> bool;
    async fn capture_scope(&self) -> Result<AccountScope, &'static str>;
    async fn observe_ui_logged_in(&self) -> Result<bool, &'static str>;
}

struct ProcessAccountBackend { session: crate::ia::types::Session }

#[async_trait::async_trait]
impl AccountObservationBackend for ProcessAccountBackend {
    fn process_running(&self) -> bool { crate::tools::wechat_db::find_wechat_pid().is_some() }
    async fn capture_scope(&self) -> Result<AccountScope, &'static str> {
        use crate::tools::wechat_messages::CollectionCancel;
        let cancel = CollectionCancel::within(std::time::Duration::from_secs(8));
        let worker_cancel = cancel.clone();
        let _cancel = CancelCollectionOnDrop(cancel);
        let identity = tokio::task::spawn_blocking(move || {
            crate::tools::wechat_read_identity::capture_read_identity_for(
                crate::tools::wechat_read_identity::DEFAULT_SESSION_ID, &worker_cancel,
            )
        }).await.map_err(|_| "identity_unavailable")?.map_err(|_| "identity_unavailable")?;
        Ok(AccountScope { account_username: identity.account_id().to_owned(), epoch: identity.epoch().to_owned() })
    }
    async fn observe_ui_logged_in(&self) -> Result<bool, &'static str> {
        let options = ExecOptions { session: Some(self.session.clone()), timeout_ms: 8_000 };
        let a11y = get_a11y_desktop(&options).await.map_err(|_| "observation_failed")?;
        let screenshot = capture_screenshot(&options).await.map_err(|_| "observation_failed")?;
        let identified = identify_states(&a11y, &screenshot);
        let context = { let db = get_db(); create_context(self.session.clone(), &db) };
        let screenshot_bytes = base64::engine::general_purpose::STANDARD.decode(&screenshot).unwrap_or_default();
        let observed = observed_auth_state(&context.state, &identified, &a11y, &screenshot_bytes)
            .ok_or("observation_failed")?;
        Ok(observed.main_window.is_logged_in)
    }
}

struct CancelCollectionOnDrop(crate::tools::wechat_messages::CollectionCancel);
impl Drop for CancelCollectionOnDrop { fn drop(&mut self) { self.0.cancel(); } }

fn account_unknown(code: &str) -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"unknown","account_username":null,"error_code":code}))
}

async fn account_observation<B: AccountObservationBackend>(backend: &B, budget: std::time::Duration) -> Json<serde_json::Value> {
    if !backend.process_running() {
        return Json(serde_json::json!({"status":"app_not_running","account_username":null,"error_code":null}));
    }
    match tokio::time::timeout(budget, account_observation_inner(backend)).await {
        Ok(Ok(response)) => Json(response),
        Ok(Err(code)) => account_unknown(code),
        Err(_) => account_unknown("observation_timeout"),
    }
}

async fn account_observation_inner<B: AccountObservationBackend>(backend: &B) -> Result<serde_json::Value, &'static str> {
    let scope_before = backend.capture_scope().await;
    let logged_in = backend.observe_ui_logged_in().await?;
    if !logged_in {
        return Ok(serde_json::json!({"status":"logged_out","account_username":null,"error_code":null}));
    }
    let before = scope_before?;
    let after = backend.capture_scope().await?;
    if before.epoch != after.epoch { return Err("scope_changed"); }
    Ok(serde_json::json!({"status":"logged_in","account_username":before.account_username,"error_code":null}))
}

fn observed_auth_state(
    prev: &AppState,
    identified: &IdentifiedStates,
    a11y: &A11yNode,
    screenshot: &[u8],
) -> Option<AppState> {
    let mw = identified.main_window.as_ref()?;
    let state_impl = find_state_by_id(&mw.state_id)?;
    Some(state_impl.reduce(&ReduceArgs { prev, a11y, screenshot }))
}

/// Log out of WeChat via FSM execution loop.
pub async fn logout() -> Json<serde_json::Value> {
    let session = match get_session("default") {
        Some(s) => s,
        None => {
            return Json(serde_json::json!({
                "success": false,
                "error": "No session available"
            }))
        }
    };

    // Quick auth check first
    let exec_options = ExecOptions {
        session: Some(session.clone()),
        timeout_ms: 30_000,
    };

    let a11y = match get_a11y_desktop(&exec_options).await {
        Ok(tree) => tree,
        Err(e) => {
            return Json(serde_json::json!({
                "success": false,
                "error": format!("Failed to get a11y tree: {e}")
            }))
        }
    };

    let screenshot = capture_screenshot(&exec_options).await.unwrap_or_default();
    let identified = identify_states(&a11y, &screenshot);

    // Load persisted state and check if logged in
    let mut context = {
        let db = get_db();
        create_context(session.clone(), &db)
    };

    if let Some(ref mw) = identified.main_window {
        if let Some(state_impl) = find_state_by_id(&mw.state_id) {
            let screenshot_bytes = base64::engine::general_purpose::STANDARD
                .decode(&screenshot)
                .unwrap_or_default();
            context.state = state_impl.reduce(&ReduceArgs {
                prev: &context.state,
                a11y: &a11y,
                screenshot: &screenshot_bytes,
            });
        }
    }

    if !context.state.main_window.is_logged_in {
        return Json(serde_json::json!({
            "success": false,
            "error": "Not logged in"
        }));
    }

    // Run logout FSM
    let cancel = CancellationToken::new();
    let plan = LogoutPlan;
    let params = LogoutParams;
    let emit = |_event: SubscriptionEvent| {};
    let (result, _) = run_execution_loop(&plan, &params, &mut context, &emit, cancel).await;

    if result.success {
        // Clear logged_in_user from session
        let db = get_db();
        if let Err(error) =
            crate::db::queries::update_session_logged_in_user(&db, &session.id, None)
        {
            // The client is logged out; the row still names the account it no
            // longer has. Reporting success would let a read be answered from
            // that row, so the failure is reported as the failure it is and the
            // session stays quarantined until a write does succeed.
            return Json(serde_json::json!({
                "success": false,
                "error": error.code()
            }));
        }
    }

    Json(serde_json::json!({
        "success": result.success,
        "error": result.error
    }))
}

pub async fn login() -> Json<serde_json::Value> {
    let screenshot = capture_screenshot(&ExecOptions::default()).await;

    match screenshot {
        Ok(b64) => {
            if let Some(qr_result) = decode_qr_from_base64(&b64) {
                let data_url = to_data_url(&qr_result.data).ok();
                return Json(serde_json::json!({
                    "success": false,
                    "state": { "status": "qr_pending" },
                    "qrDataUrl": data_url
                }));
            }

            Json(serde_json::json!({
                "success": false,
                "state": { "status": "qr_pending" }
            }))
        }
        Err(_) => Json(serde_json::json!({
            "success": false,
            "state": { "status": "logged_out" }
        })),
    }
}

#[derive(Deserialize)]
pub struct LoginWsParams {
    #[serde(rename = "timeoutMs", default = "default_timeout")]
    timeout_ms: u64,
    #[serde(rename = "newAccount", default)]
    new_account: bool,
}

fn default_timeout() -> u64 {
    300_000
}

pub async fn login_ws(
    ws: WebSocketUpgrade,
    Query(params): Query<LoginWsParams>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_login_ws(socket, params))
}

async fn handle_login_ws(mut socket: WebSocket, params: LoginWsParams) {
    let session = match get_session("default") {
        Some(s) => s,
        None => {
            let msg = serde_json::to_string(&LoginSubscriptionEvent::Error {
                message: "No session available".to_string(),
            })
            .unwrap();
            let _ = socket.send(Message::Text(msg.into())).await;
            return;
        }
    };

    // Send initial status
    let msg = serde_json::to_string(&LoginSubscriptionEvent::Status {
        message: "Navigating login flow...".to_string(),
    })
    .unwrap();
    if socket.send(Message::Text(msg.into())).await.is_err() {
        return;
    }

    // Channel to bridge sync emit callback → async WebSocket sends
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<SubscriptionEvent>();
    let cancel = CancellationToken::new();
    let cancel_for_exec = cancel.clone();
    let login_params = LoginParams {
        new_account: params.new_account,
    };

    // Spawn the execution loop in a separate task
    let exec_handle = tokio::spawn(async move {
        let mut context = {
            let db = get_db();
            create_context(session, &db)
        };
        let plan = LoginPlan;
        let emit = move |event: SubscriptionEvent| {
            let _ = tx.send(event);
        };
        run_execution_loop(&plan, &login_params, &mut context, &emit, cancel_for_exec).await.0
    });

    // Main loop: bridge events to WebSocket, handle timeout + disconnect
    let timeout = tokio::time::sleep(std::time::Duration::from_millis(params.timeout_ms));
    tokio::pin!(timeout);
    let mut sent_terminal = false;
    let mut client_disconnected = false;
    let mut server_timeout = false;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(evt) => {
                        let ws_event = subscription_event_to_login_event(evt);
                        if is_terminal_login_event(&ws_event) {
                            sent_terminal = true;
                        }
                        let msg = serde_json::to_string(&ws_event).unwrap();
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            cancel.cancel();
                            client_disconnected = true;
                            break;
                        }
                    }
                    None => break, // channel closed = execution done
                }
            }
            _ = &mut timeout => {
                cancel.cancel();
                server_timeout = true;
                sent_terminal = true;
                let msg = serde_json::to_string(&LoginSubscriptionEvent::LoginTimeout).unwrap();
                if socket.send(Message::Text(msg.into())).await.is_err() {
                    client_disconnected = true;
                }
                break;
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(_)) => continue,
                    _ => {
                        cancel.cancel();
                        client_disconnected = true;
                        break;
                    }
                }
            }
        }
    }

    // Wait for execution to finish and emit a fallback terminal event if needed.
    let exec_result = exec_handle.await.ok();
    if !client_disconnected && !sent_terminal {
        let fallback = match exec_result {
            Some(result) if result.success => LoginSubscriptionEvent::LoginSuccess { user_id: None },
            Some(result) => {
                let message = result.error.unwrap_or_else(|| "Login failed".to_string());
                if message.starts_with("Unknown state for")
                    || message.starts_with("Execution timeout after")
                    || (server_timeout && message == "Aborted")
                {
                    LoginSubscriptionEvent::LoginTimeout
                } else {
                    LoginSubscriptionEvent::Error { message }
                }
            }
            None => LoginSubscriptionEvent::Error {
                message: "Login execution task failed".to_string(),
            },
        };
        let msg = serde_json::to_string(&fallback).unwrap();
        let _ = socket.send(Message::Text(msg.into())).await;
    }
}

fn is_terminal_login_event(event: &LoginSubscriptionEvent) -> bool {
    matches!(
        event,
        LoginSubscriptionEvent::LoginSuccess { .. }
            | LoginSubscriptionEvent::LoginTimeout
            | LoginSubscriptionEvent::Error { .. }
    )
}

/// Convert generic SubscriptionEvent (from plans) to typed LoginSubscriptionEvent (for WS).
fn subscription_event_to_login_event(event: SubscriptionEvent) -> LoginSubscriptionEvent {
    match event.event_type.as_str() {
        "status" => LoginSubscriptionEvent::Status {
            message: event
                .data
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "qr" => {
            let qr_data = event
                .data
                .get("qrData")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let qr_data_url = to_data_url(&qr_data).ok();
            LoginSubscriptionEvent::Qr {
                qr_data,
                qr_binary_data: None,
                qr_data_url,
            }
        }
        "phone_confirm" => LoginSubscriptionEvent::PhoneConfirm {
            message: event
                .data
                .get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        },
        "login_success" => LoginSubscriptionEvent::LoginSuccess {
            user_id: event
                .data
                .get("userId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        },
        "login_timeout" => LoginSubscriptionEvent::LoginTimeout,
        "error" => LoginSubscriptionEvent::Error {
            message: event
                .data
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error")
                .to_string(),
        },
        _ => LoginSubscriptionEvent::Status {
            message: format!("Unknown event: {}", event.event_type),
        },
    }
}
