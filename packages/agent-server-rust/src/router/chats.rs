use axum::{extract::Path, extract::Query, Json};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::context::create_context;
use crate::db::get_db;
use crate::execution::run_execution_loop;
use crate::ia::types::SubscriptionEvent;
use crate::plans::chat_open::{ChatOpenParams, ChatOpenPlan};
use crate::sessions::manager::get_session;

#[derive(Deserialize)]
pub struct OpenChatParams {
    #[serde(default, rename = "clearUnreads")]
    clear_unreads: bool,
}

pub async fn open_chat(
    Path(chat_id): Path<String>,
    Query(params): Query<OpenChatParams>,
) -> Json<serde_json::Value> {
    let clear_unreads = params.clear_unreads;
    let session = match get_session("default") {
        Some(s) => s,
        None => {
            return Json(serde_json::json!({
                "ok": false,
                "error": "No session available"
            }))
        }
    };

    if session.logged_in_user.is_none() {
        return Json(serde_json::json!({
            "ok": false,
            "error": "NOT_LOGGED_IN"
        }));
    }

    if chat_id.starts_with("gh_") {
        return Json(serde_json::json!({
            "ok": false,
            "error": "Opening official accounts is not supported"
        }));
    }

    let mut context = {
        let db = get_db();
        create_context(session, &db)
    };

    let plan = ChatOpenPlan;
    let params = ChatOpenParams { chat_id, clear_unreads };
    let cancel = CancellationToken::new();
    let noop_emit = |_: SubscriptionEvent| {};

    let (result, plan_state) =
        run_execution_loop(&plan, &params, &mut context, &noop_emit, cancel).await;

    if result.success {
        if let Some(open_result) = plan_state.result {
            Json(serde_json::to_value(open_result).unwrap_or_else(|_| serde_json::json!({"ok": true})))
        } else {
            Json(serde_json::json!({ "ok": true }))
        }
    } else {
        Json(serde_json::json!({
            "ok": false,
            "error": result.error.unwrap_or_else(|| "Chat open failed".to_string())
        }))
    }
}
