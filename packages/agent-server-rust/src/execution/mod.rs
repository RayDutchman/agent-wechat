pub mod actions;

use base64::Engine;
use crate::context::Context;
use crate::ia::{find_state_by_id, identify_states};
use crate::ia::types::*;
use crate::tools::a11y::get_a11y_desktop;
use crate::tools::exec::ExecOptions;
use crate::tools::screenshot::capture_screenshot;
use crate::effects::collect_effects;
use crate::db::get_db;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Only one plan can run at a time — they all drive the GUI.
static PLAN_LOCK: Mutex<()> = Mutex::const_new(());

pub struct GuiGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for GuiGuard {
    fn drop(&mut self) {
        crate::sessions::health_monitor::resume_monitoring();
    }
}

pub async fn acquire_gui_guard() -> GuiGuard {
    let lock = PLAN_LOCK.lock().await;
    crate::sessions::health_monitor::pause_monitoring();
    GuiGuard { _lock: lock }
}

pub struct ExecutionResult {
    pub success: bool,
    pub error: Option<String>,
}

const EXECUTION_TIMEOUT_MS: u64 = 300_000; // 5 minutes
const UNKNOWN_STATE_TIMEOUT_MS: u64 = 60_000; // 1 minute
const MAX_STEPS: u32 = 500;

/// Run the FSM execution loop with a generic plan.
///
/// 1. OBSERVE    → a11y tree + screenshot
/// 2. IDENTIFY   → match IAState from tree
/// 3. REDUCE     → update AppState via identified state's reduce()
/// 4. EFFECTS    → emit events on state change
/// 5. PERSIST    → save AppState to SQLite
/// 6. SELECT     → plan picks next action
/// 7. EXECUTE    → run action via tool scripts (emits fire inline)
/// 8. GOAL?      → plan checks if done
/// 9. LOOP
pub async fn run_execution_loop<P, PS, PA>(
    plan: &P,
    params: &PA,
    context: &mut Context,
    emit: &(dyn Fn(SubscriptionEvent) + Send + Sync),
    cancel: CancellationToken,
) -> (ExecutionResult, PS)
where
    P: crate::plans::Plan<PlanState = PS, Params = PA>,
    PS: Send,
    PA: Send,
{
    let _gui_guard = acquire_gui_guard().await;
    run_execution_loop_locked(plan, params, context, emit, cancel).await
}

/// The same loop as [`run_execution_loop`] without taking the shared GUI guard.
///
/// The caller must already hold a [`GuiGuard`] — normally the one
/// [`acquire_gui_guard`] returned. `PLAN_LOCK` is not reentrant, so an operation
/// that keeps one guard across several stages (an identity fence, the plan, a
/// re-check) runs the loop through here; calling [`run_execution_loop`] while
/// holding the guard would wait on the lock the caller already holds.
pub async fn run_execution_loop_locked<P, PS, PA>(
    plan: &P,
    params: &PA,
    context: &mut Context,
    emit: &(dyn Fn(SubscriptionEvent) + Send + Sync),
    cancel: CancellationToken,
) -> (ExecutionResult, PS)
where
    P: crate::plans::Plan<PlanState = PS, Params = PA>,
    PS: Send,
    PA: Send,
{
    let mut plan_state = plan.initial_plan_state();
    let session_id = context.session_id.clone();

    let exec_options = ExecOptions {
        session: Some(context.session.clone()),
        timeout_ms: 60_000,
    };

    let execution_start = std::time::Instant::now();
    let mut unknown_state_since: Option<std::time::Instant> = None;

    for step in 0..MAX_STEPS {
        // Check execution timeout
        if execution_start.elapsed().as_millis() as u64 > EXECUTION_TIMEOUT_MS {
            return (ExecutionResult {
                success: false,
                error: Some(format!(
                    "Execution timeout after {}s",
                    execution_start.elapsed().as_secs()
                )),
            }, plan_state);
        }

        if cancel.is_cancelled() {
            return (ExecutionResult {
                success: false,
                error: Some("Aborted".to_string()),
            }, plan_state);
        }

        // 1. OBSERVE: get a11y tree + screenshot
        let a11y_result = get_a11y_desktop(&exec_options).await;
        let a11y = match a11y_result {
            Ok(tree) => tree,
            Err(e) => {
                tracing::warn!("[exec] a11y failed on step {step}: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        let screenshot = capture_screenshot(&exec_options).await.unwrap_or_default();

        // 2. IDENTIFY: find current states
        let identified = identify_states(&a11y, &screenshot);

        if identified.main_window.is_none() {
            if unknown_state_since.is_none() {
                unknown_state_since = Some(std::time::Instant::now());
            }
            let elapsed = unknown_state_since.unwrap().elapsed();
            if elapsed.as_millis() as u64 > UNKNOWN_STATE_TIMEOUT_MS {
                tracing::error!("[exec] Unknown state timeout after {}s", elapsed.as_secs());
                return (ExecutionResult {
                    success: false,
                    error: Some(format!(
                        "Unknown state for {}s - no matching IAState found",
                        elapsed.as_secs()
                    )),
                }, plan_state);
            }
            tracing::warn!("[exec] Unknown state ({}s), waiting...", elapsed.as_secs());
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        unknown_state_since = None;

        // Log identified states
        tracing::info!(
            "[exec] step={step} mainWindow={}, popup={}, contactCard={}, settings={}",
            identified
                .main_window
                .as_ref()
                .map(|s| s.state_id.as_str())
                .unwrap_or("none"),
            identified
                .popup
                .as_ref()
                .map(|s| s.state_id.as_str())
                .unwrap_or("none"),
            identified
                .contact_card
                .as_ref()
                .map(|s| s.state_id.as_str())
                .unwrap_or("none"),
            identified
                .settings
                .as_ref()
                .map(|s| s.state_id.as_str())
                .unwrap_or("none"),
        );

        // 3. REDUCE: update AppState via identified state reduce()
        let prev_state = context.state.clone();
        let screenshot_bytes = base64::engine::general_purpose::STANDARD
            .decode(&screenshot)
            .unwrap_or_default();

        if let Some(ref mw) = identified.main_window {
            if let Some(state_impl) = find_state_by_id(&mw.state_id) {
                context.state = state_impl.reduce(&ReduceArgs {
                    prev: &prev_state,
                    a11y: &a11y,
                    screenshot: &screenshot_bytes,
                });
            }
        }

        if let Some(ref popup) = identified.popup {
            if let Some(state_impl) = find_state_by_id(&popup.state_id) {
                let current = context.state.clone();
                context.state = state_impl.reduce(&ReduceArgs {
                    prev: &current,
                    a11y: &a11y,
                    screenshot: &screenshot_bytes,
                });
            }
        } else {
            context.state.popup = None;
        }

        if let Some(ref cc) = identified.contact_card {
            if let Some(state_impl) = find_state_by_id(&cc.state_id) {
                let current = context.state.clone();
                context.state = state_impl.reduce(&ReduceArgs {
                    prev: &current,
                    a11y: &a11y,
                    screenshot: &screenshot_bytes,
                });
            }
        } else {
            context.state.contact_card = None;
        }

        if let Some(ref s) = identified.settings {
            if let Some(state_impl) = find_state_by_id(&s.state_id) {
                let current = context.state.clone();
                context.state = state_impl.reduce(&ReduceArgs {
                    prev: &current,
                    a11y: &a11y,
                    screenshot: &screenshot_bytes,
                });
            }
        } else {
            context.state.settings = None;
        }

        // 4. EFFECTS
        let effects = collect_effects(&prev_state, &context.state);
        for effect in effects {
            match effect {
                Effect::Emit { event } => emit(event),
            }
        }

        // 5. PERSIST
        {
            let db = get_db();
            context.save(&db);
        }

        // 6. SELECT: plan picks next action
        let selected = plan
            .select_action(
                &context.state,
                params,
                &identified,
                &mut plan_state,
                &a11y,
                &session_id,
            )
            .await;

        // Logged without the payload: `Action::Type` carries the message text
        // and `Action::Sequence` carries whole sub-actions, so formatting the
        // action itself would put an outgoing message in the log.
        log_selected_action(selected.as_ref());

        // 7. EXECUTE: run the action (emits fire inline via callback)
        if let Some(sel) = &selected {
            actions::execute_action(&sel.action, sel.frame.as_ref(), &exec_options, &a11y, emit).await;
        }

        // 8. GOAL CHECK (after action)
        if plan.is_goal_reached(&context.state, &plan_state) {
            return (ExecutionResult {
                success: true,
                error: None,
            }, plan_state);
        }

        // No action = stuck (only if plan returns None)
        if selected.is_none() {
            return (ExecutionResult {
                success: false,
                error: Some(stuck_reason(plan, &plan_state)),
            }, plan_state);
        }
    }

    (ExecutionResult {
        success: false,
        error: Some("Max steps reached".to_string()),
    }, plan_state)
}

/// The error a run that selected no action reports.
///
/// The plan may name a stable short reason for stopping, and that reason is what
/// the run reports; a plan that names none keeps the historical text. The reason
/// is a fixed key the plan chose, never composed from the request, so nothing
/// about a chat, a message or driver output reaches it.
fn stuck_reason<P, PS>(plan: &P, plan_state: &PS) -> String
where
    P: crate::plans::Plan<PlanState = PS>,
{
    plan.get_failure_reason(plan_state)
        .unwrap_or_else(|| "No action selected".to_string())
}

/// Log the selected action without its payload.
///
/// `Action::Type` carries the message text and `Action::Sequence` carries whole
/// sub-actions, so a message body would reach the log through
/// `format!("{action:?}")`. Only [`describe_action`]'s redacted description is
/// written.
fn log_selected_action(selected: Option<&SelectedAction>) {
    match selected {
        Some(selected) => {
            tracing::debug!("[exec] selected action: {}", describe_action(&selected.action))
        }
        None => tracing::debug!("[exec] selected action: none"),
    }
}

/// A log-safe description of an action: its kind and, for a sequence, how many
/// steps it holds. It never includes text, selectors, coordinates or event
/// payloads.
fn describe_action(action: &Action) -> String {
    match action {
        Action::ClickSelector { .. } => "click_selector".to_string(),
        Action::ClickCoords { .. } => "click_coords".to_string(),
        Action::Type { .. } => "type_text".to_string(),
        Action::Key { .. } => "key".to_string(),
        Action::Scroll { .. } => "scroll".to_string(),
        Action::Wait { .. } => "wait".to_string(),
        Action::Emit { .. } => "emit".to_string(),
        Action::Sequence { actions } => format!("sequence({})", actions.len()),
    }
}
