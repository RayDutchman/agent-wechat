pub mod auth;
mod chats;
mod debug;
mod events;
mod messages;
mod read;
mod read_v2;
mod send_file;
mod send_text;
mod sessions;
mod send_v2;
mod status;
mod vnc;
mod voice;

pub use voice::recover_interrupted_jobs;

use axum::{
    extract::DefaultBodyLimit,
    http::Method,
    routing::{get, post},
    Json, Router,
};
use tower_http::cors::{Any, CorsLayer};
use crate::tools::wechat_db::WechatReadError;

impl axum::response::IntoResponse for WechatReadError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        let status = match self {
            Self::SessionUnavailable | Self::KeysUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotLoggedIn => StatusCode::CONFLICT,
            Self::DatabaseRead => StatusCode::INTERNAL_SERVER_ERROR,
        };
        tracing::warn!(code = self.code(), "WeChat API read failed");
        (status, Json(serde_json::json!({"error": self.code(), "code": self.code()}))).into_response()
    }
}

const MIB: usize = 1024 * 1024;
pub(super) const MAX_UPLOAD_BYTES: usize = 128 * MIB;
pub(super) const MAX_UPLOAD_BASE64_BYTES: usize = MAX_UPLOAD_BYTES.div_ceil(3) * 4;
const HTTP_BODY_LIMIT_BYTES: usize = MAX_UPLOAD_BASE64_BYTES + MIB;

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Build the full axum Router.
pub fn build_router() -> Router {
    let cors = CorsLayer::new()
        // Keep DELETE available for the V2 snapshot release route.
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_origin(Any)
        .allow_headers(Any);

    routes()
        // Middleware: auth → body limit → CORS (applied bottom-up)
        .layer(axum::middleware::from_fn(auth::auth_middleware))
        // Send requests carry media as base64 JSON. Reserve expansion plus
        // bounded JSON overhead for the decoded upload limit above.
        .layer(DefaultBodyLimit::max(HTTP_BODY_LIMIT_BYTES))
        .layer(cors)
}

fn routes() -> Router {
    Router::new()
        // Health (exempt from auth via middleware check)
        .route("/health", get(health))
        // Status
        .route("/api/v2/account", get(status::account_v2))
        .route("/api/status/login", post(status::login))
        .route("/api/status/logout", post(status::logout))
        // GUI chat selection remains independent of V2 data reads.
        .route("/api/chats/{id}/open", post(chats::open_chat))
        // Retired V1 contact/chat/message/read/send routes are intentionally
        // absent. Voice jobs remain an independent asynchronous capability.
        .route("/api/messages/voice", post(voice::create_job))
        .route("/api/messages/voice/{job_id}", get(voice::get_job))
        .route("/api/messages/voice/{job_id}/cancel", post(voice::cancel_job))
        .merge(read_v2::read_v2_routes())
        .merge(send_v2::send_v2_routes())
        // Debug
        .route("/api/debug/screenshot", get(debug::screenshot))
        .route("/api/debug/a11y", get(debug::a11y))
        // Sessions
        .route("/api/sessions", get(sessions::list_sessions).post(sessions::create_session))
        .route("/api/sessions/{id}", get(sessions::get_session).delete(sessions::delete_session))
        .route("/api/sessions/{id}/start", post(sessions::start_session))
        .route("/api/sessions/{id}/stop", post(sessions::stop_session))
        // WebSocket for login subscription
        .route("/api/ws/login", get(status::login_ws))
        // Events WebSocket
        .route("/api/ws/events", get(events::events_ws))
        // VNC: WebSocket proxy + static files (behind auth)
        .route("/vnc/websockify", get(vnc::vnc_ws))
        .route("/vnc/{*path}", get(vnc::vnc_static))
        .route("/vnc/", get(vnc::vnc_static))
}
