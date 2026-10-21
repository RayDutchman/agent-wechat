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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::{to_bytes, Body}, http::{Request, StatusCode}, response::IntoResponse};
    use crate::tools::wechat_db::{try_query_wechat_db, WechatReadError};
    use tower::ServiceExt;

    #[tokio::test]
    async fn retired_v1_data_and_send_routes_are_not_registered() {
        let router = routes();
        for (method, uri) in [
            ("GET", "/api/status"), ("GET", "/api/status/auth"),
            ("GET", "/api/read/identity"), ("GET", "/api/read/messages/chat"),
            ("GET", "/api/read/contacts"), ("DELETE", "/api/read/messages/chat/snapshots/x"),
            ("GET", "/api/read/voice"), ("GET", "/api/messages/chat"),
            ("GET", "/api/messages/chat/media/1"), ("POST", "/api/messages/send"),
            ("POST", "/api/send/text"), ("POST", "/api/send/file"),
            ("GET", "/api/contacts"), ("GET", "/api/chats"),
        ] {
            let response = router.clone().oneshot(Request::builder().method(method).uri(uri).body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
        }
    }

    #[tokio::test]
    async fn independent_voice_and_login_actions_remain_registered() {
        let router = routes();
        for uri in ["/api/status/login", "/api/status/logout", "/api/messages/voice"] {
            let response = router.clone().oneshot(Request::builder().method("GET").uri(uri).body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "GET {uri} confirms the independent POST route exists without invoking it");
        }
    }

    #[tokio::test]
    async fn read_errors_have_non_success_status_and_stable_public_codes() {
        for (error, status, code) in [
            (WechatReadError::SessionUnavailable, StatusCode::SERVICE_UNAVAILABLE, "WECHAT_SESSION_UNAVAILABLE"),
            (WechatReadError::NotLoggedIn, StatusCode::CONFLICT, "WECHAT_NOT_LOGGED_IN"),
            (WechatReadError::KeysUnavailable, StatusCode::SERVICE_UNAVAILABLE, "WECHAT_KEYS_UNAVAILABLE"),
            (WechatReadError::DatabaseRead, StatusCode::INTERNAL_SERVER_ERROR, "WECHAT_DB_READ_FAILED"),
        ] {
            let response = error.into_response();
            assert_eq!(response.status(), status);
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                serde_json::json!({"error": code, "code": code}));
        }
    }

    #[tokio::test]
    async fn database_failure_response_does_not_expose_query_key_or_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("private-account.db");
        let error = try_query_wechat_db(path.to_str().unwrap(), "secret-key", "SELECT private_body")
            .unwrap_err();
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"error": "WECHAT_DB_READ_FAILED", "code": "WECHAT_DB_READ_FAILED"}));
    }

    #[tokio::test]
    async fn successful_empty_result_keeps_existing_json_shape() {
        let result: Result<Json<Vec<serde_json::Value>>, WechatReadError> = Ok(Json(vec![]));
        let response = result.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(&to_bytes(response.into_body(), 1024).await.unwrap()[..], b"[]");
        let result: Result<Json<Option<serde_json::Value>>, WechatReadError> = Ok(Json(None));
        let response = result.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(&to_bytes(response.into_body(), 1024).await.unwrap()[..], b"null");
    }

    #[test]
    fn body_limit_accounts_for_base64_expansion() {
        let encoded_100_mib = (100 * MIB).div_ceil(3) * 4;
        assert!(HTTP_BODY_LIMIT_BYTES > encoded_100_mib + MIB / 2);
        assert_eq!(MAX_UPLOAD_BYTES, 128 * MIB);
    }

    #[tokio::test]
    async fn a_browser_preflight_for_the_v2_release_route_is_allowed() {
        use axum::body::Body;
        use tower::ServiceExt;

        // The V2 release route is a DELETE, and CORS is outermost, so preflight
        // does not need to invoke the authenticated handler.
        let response = build_router()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/v2/read-snapshots/token-1")
                    .header("origin", "https://example.invalid")
                    .header("access-control-request-method", "DELETE")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router responds");

        let allowed = response
            .headers()
            .get("access-control-allow-methods")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert_eq!(
            allowed, "GET,POST,DELETE,OPTIONS",
            "a browser could not release a V2 frozen sample"
        );
    }
}
