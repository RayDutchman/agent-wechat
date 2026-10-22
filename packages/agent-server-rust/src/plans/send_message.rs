use super::Plan;
use super::composer::{find_edit_send_pair, is_send_button};
use crate::ia::actions;
use crate::ia::selectors::query_selector;
use crate::ia::types::*;
use crate::tools::chat_select::{open_chat, OpenChatResult};
use crate::tools::exec::{exec_command, exec_command_controlled, ExecOptions};
use std::sync::Arc;
use std::time::Duration;

pub struct SendMessagePlan;

/// Stable refusal codes a [`SendFence`] may return. They are the public keys of
/// the fenced send contract, so they carry no path, account or chat id.
pub const FENCE_SCOPE_CHANGED: &str = "scope_changed";
pub const FENCE_TARGET_CHANGED: &str = "target_changed";
pub const FENCE_IDENTITY_UNAVAILABLE: &str = "identity_unavailable";
/// The fenced file composer already held text or an attachment, so pasting into
/// it could stage the caller's file on top of someone else's draft. Nothing is
/// pasted and nothing is cleared.
pub const FENCE_COMPOSER_NOT_EMPTY: &str = "composer_not_empty";
/// The fenced file paste did not exit successfully within its deadline. Nothing
/// is committed.
pub const FENCE_PASTE_FAILED: &str = "paste_failed";
pub const FENCE_COMPOSER_NOT_READY: &str = "composer_not_ready";

/// How long the fenced file paste may take before it is terminated and the send
/// is refused.
const FILE_PASTE_DEADLINE: Duration = Duration::from_secs(10);

/// Stable short reasons for a plan that selected no action. They say which
/// observation the send was waiting for and are deliberately not a diagnosis:
/// `chat_not_open` only records that the focus round did not observe an open
/// chat, which is not attributable to any single cause from inside the plan.
///
/// They carry no chat id, account, path, text, coordinate or driver output.
pub const SEND_FAIL_CHAT_NOT_OPEN: &str = "chat_not_open";
/// The chat selection helper answered without success, so the send never reached
/// a chat it could confirm.
pub const SEND_FAIL_CHAT_SELECTION_FAILED: &str = "chat_selection_failed";

/// Re-verification of a fenced send, called immediately before the `Return`.
///
/// The fenced HTTP route holds the shared GUI guard for the whole operation and
/// injects an implementation here through [`SendMessageParams::fence`]. The plan
/// calls [`SendFence::recheck`] in the same step that dispatches `Return` — not
/// while it is still typing the input — so a login, restart or chat switch that
/// landed after the input was prepared refuses the send instead of committing
/// it.
///
/// The trait exists so the plan is testable without a WeChat client: a test
/// injects a fence that reports a changed scope, a drifted target, or agreement.
#[async_trait::async_trait]
pub trait SendFence: Send + Sync {
    /// Re-check that the fenced identity epoch and the selected chat still hold.
    /// `Err(code)` is one of [`FENCE_SCOPE_CHANGED`], [`FENCE_TARGET_CHANGED`] or
    /// [`FENCE_IDENTITY_UNAVAILABLE`].
    async fn recheck(&self) -> Result<(), &'static str>;

    /// Execution options for UI side effects the plan performs — the fenced file
    /// paste — so it uses the session the route fenced rather than the
    /// compiled-in environment default. The default keeps the legacy callers and
    /// the test fakes unchanged.
    fn exec_options(&self) -> ExecOptions {
        ExecOptions::default()
    }
}


pub struct SendMessageParams {
    pub chat_id: String,
    pub message: Option<String>,
    pub image_path: Option<String>,
    pub image_mime: Option<String>,
    pub file_path: Option<String>,
    /// Internal fence for the fenced routes. `None` keeps the legacy, unfenced
    /// behaviour, and a route must not fabricate one: the caller that owns the
    /// identity epoch is the one that can supply a real check. The fence also
    /// carries the execution options a fenced file paste runs with.
    pub fence: Option<Arc<dyn SendFence>>,
}

pub enum SendMessagePhase {
    Opening,
    Focusing,
    Inputting,
    /// Text is typed and the composer is ready, but `Return` has not been sent.
    /// The fence is re-checked here, in the step that dispatches `Return`.
    ReadyToCommit,
    Confirming,
    Done,
    /// The fence or the composer refused the send before `Return`. Nothing was
    /// committed and no further action is taken.
    Failed,
}

pub struct SendMessagePlanState {
    pub phase: SendMessagePhase,
    pub open_result: Option<OpenChatResult>,
    pub confirm_attempts: u32,
    /// Set just before the `Return` action is handed to the driver. It records
    /// that a commit was attempted, not that a message was observed: a later
    /// failure, timeout or cancellation keeps it set.
    pub commit_attempted: bool,
    /// Why [`SendMessagePhase::Failed`] was entered, when a fence refused.
    pub fence_error: Option<&'static str>,
    /// The fenced file run recorded an empty composer just before it pasted.
    /// The commit step requires this, so it can never paste onto, or send, a
    /// draft or a previous attachment it did not stage itself.
    pub file_empty_baseline: bool,
    /// The fenced file paste was observed to exit successfully within its
    /// deadline. The commit step requires this as well as the live readiness.
    pub file_paste_succeeded: bool,
    /// Stable short failure reason when returning None (observability only).
    pub failure_reason: Option<String>,
    /// Stage recorded at a concrete fenced stop/commit point; never inferred
    /// from an error string or the commit boolean.
    pub failure_stage: Option<SendFailureStage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendFailureStage { Prepare, PreCommit, PostCommit }

fn find_edit_and_send_button(a11y: &A11yNode) -> Option<(&A11yNode, &A11yNode)> {
    find_edit_send_pair(a11y)
}

/// The composer's Send control, found without pairing it with a text editor.
///
/// A staged file attachment is sent with an empty message, so the text-mode
/// requirement that the editable field and the Send button appear together does
/// not describe it. The Send control is the observable part of the composer that
/// matters for that case.
fn find_send_button(node: &A11yNode) -> Option<&A11yNode> {
    if let Some(children) = &node.children {
        if let Some(send) = children.iter().find(|c| is_send_button(c)) {
            return Some(send);
        }
        for child in children {
            if let Some(found) = find_send_button(child) {
                return Some(found);
            }
        }
    }
    None
}

/// Whether the composer's Send control carries the `DISABLED` state, which is
/// the client's own signal that the composer is empty.
fn send_is_disabled(send: &A11yNode) -> bool {
    send.states
        .as_ref()
        .is_some_and(|states| states.iter().any(|state| state == "DISABLED"))
}

/// Whether a staged file attachment looks ready to send.
///
/// This reads the composer from the observed tree: the Send control must be
/// present and not `DISABLED`, which is the client's own signal that the
/// composer holds something to send. It deliberately does not reuse the
/// text-mode editor == message pairing, because a file attachment commits with
/// no typed text. It also does not assume the paste succeeded: a composer whose
/// Send control is still disabled is refused rather than committed.
fn file_composer_ready(a11y: &A11yNode) -> bool {
    find_send_button(a11y).is_some_and(|send| !send_is_disabled(send))
}

/// Whether the editable field's derived text state marks it empty.
///
/// The accessible `name` of an editable field is its label, not its contents, so
/// emptiness is read from the a11y helper's derived `TEXT_EMPTY` state. A helper
/// that reports no derived state (interface missing, error, negative count) is
/// not treated as empty: the run fails closed.
fn text_state_is_empty(edit: &A11yNode) -> bool {
    edit.states
        .as_ref()
        .is_some_and(|states| states.iter().any(|state| state == "TEXT_EMPTY"))
}

/// Whether the composer a fenced file run is about to paste into is empty.
///
/// An empty composer is an editable field whose contents are empty — the
/// derived `TEXT_EMPTY` state, never the label in `name` — with a disabled Send
/// control, read from the same accessibility sample. A run that cannot observe
/// that state refuses instead of pasting on top of a draft or a previous
/// attachment, and it never clears what it found.
fn composer_empty_baseline(a11y: &A11yNode) -> bool {
    match find_edit_and_send_button(a11y) {
        Some((edit, send)) => text_state_is_empty(edit) && send_is_disabled(send),
        None => false,
    }
}

/// The execution options for the fenced file paste: the route's session when the
/// fence carries one, the environment default otherwise.
fn paste_options(params: &SendMessageParams) -> ExecOptions {
    params
        .fence
        .as_ref()
        .map(|fence| fence.exec_options())
        .unwrap_or_default()
}

#[async_trait::async_trait]
impl Plan for SendMessagePlan {
    type PlanState = SendMessagePlanState;
    type Params = SendMessageParams;

    fn id(&self) -> &str { "send_message" }

    fn initial_plan_state(&self) -> SendMessagePlanState {
        SendMessagePlanState {
            phase: SendMessagePhase::Opening,
            open_result: None,
            confirm_attempts: 0,
            commit_attempted: false,
            fence_error: None,
            file_empty_baseline: false,
            file_paste_succeeded: false,
            failure_reason: None,
            failure_stage: None,
        }
    }

    fn is_goal_reached(&self, _state: &AppState, plan_state: &SendMessagePlanState) -> bool {
        matches!(plan_state.phase, SendMessagePhase::Done)
    }

    fn get_failure_reason(&self, plan_state: &Self::PlanState) -> Option<String> {
        plan_state.failure_reason.clone()
    }

    async fn select_action(
        &self,
        state: &AppState,
        params: &SendMessageParams,
        identified: &IdentifiedStates,
        plan_state: &mut SendMessagePlanState,
        a11y: &A11yNode,
        _session_id: &str,
    ) -> Option<SelectedAction> {
        let main_state_id = identified.main_window.as_ref().map(|m| m.state_id.as_str());

        // Dismiss popups
        if state.popup.is_some() && identified.popup.is_some() {
            return Some(SelectedAction {
                action: actions::dismiss_popup(),
                frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
            });
        }

        loop {
            match &plan_state.phase {
                SendMessagePhase::Opening => {
                    if main_state_id != Some("chat") && main_state_id != Some("chat_open") {
                        return None;
                    }

                    let chat_list_item = query_selector(a11y, r#"list[name="Chats"] > list-item"#);
                    let click_xy = chat_list_item.and_then(|item| {
                        item.bounds.as_ref().map(|b| (
                            (b.x + b.width / 2.0).round(),
                            (b.y + b.height / 2.0).round(),
                        ))
                    });

                    let force = main_state_id == Some("chat");
                    let result = open_chat(&params.chat_id, force, click_xy).await;

                    if !result.ok {
                        // Observability only: the selection helper answered
                        // without success. `open_result`, the phase and the
                        // retry path are unchanged.
                        plan_state.failure_reason =
                            Some(SEND_FAIL_CHAT_SELECTION_FAILED.to_string());
                        return None;
                    }

                    let skipped = result.skipped.unwrap_or(false);
                    plan_state.open_result = Some(result);
                    plan_state.phase = SendMessagePhase::Focusing;

                    if !skipped {
                        return Some(SelectedAction {
                            action: actions::wait_short(),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }
                    continue;
                }

                SendMessagePhase::Focusing => {
                    if main_state_id != Some("chat_open") {
                        // Observability only: this round could not observe an
                        // open chat. The phase, the conditions and the next
                        // action are unchanged.
                        plan_state.failure_reason = Some(SEND_FAIL_CHAT_NOT_OPEN.to_string());
                        return None;
                    }

                    let found = find_edit_and_send_button(a11y);
                    let (edit_node, _) = match found {
                        Some(f) => f,
                        None => return None,
                    };

                    plan_state.phase = SendMessagePhase::Inputting;

                    let is_focused = edit_node
                        .states
                        .as_ref()
                        .map(|s| s.iter().any(|st| st == "FOCUSED"))
                        .unwrap_or(false);

                    if is_focused {
                        continue;
                    }

                    if let Some(bounds) = &edit_node.bounds {
                        return Some(SelectedAction {
                            action: actions::click_bounds(bounds),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }
                    return None;
                }

                SendMessagePhase::Inputting => {
                    let found = find_edit_and_send_button(a11y);
                    if found.is_none() {
                        return None;
                    }

                    // File
                    if let Some(fp) = &params.file_path {
                        // Fenced file: paste and commit in separate steps, and
                        // only paste onto a composer this run has just observed
                        // empty. A draft or a previous attachment is refused,
                        // never overwritten, and the paste must exit zero before
                        // the commit step can be reached.
                        if params.fence.is_some() {
                            if main_state_id != Some("chat_open")
                                || !composer_empty_baseline(a11y)
                            {
                                plan_state.fence_error = Some(FENCE_COMPOSER_NOT_EMPTY);
                                plan_state.failure_stage = Some(SendFailureStage::Prepare);
                                plan_state.phase = SendMessagePhase::Failed;
                                return None;
                            }
                            plan_state.file_empty_baseline = true;

                            let result = exec_command_controlled(
                                "paste-file",
                                &[fp],
                                &paste_options(params),
                                FILE_PASTE_DEADLINE,
                            )
                            .await;
                            if !result.succeeded() {
                                plan_state.fence_error = Some(FENCE_PASTE_FAILED);
                                plan_state.failure_stage = Some(SendFailureStage::Prepare);
                                plan_state.phase = SendMessagePhase::Failed;
                                return None;
                            }
                            plan_state.file_paste_succeeded = true;
                            plan_state.phase = SendMessagePhase::ReadyToCommit;
                            return Some(SelectedAction {
                                action: Action::Wait { ms: 100 },
                                frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                            });
                        }

                        plan_state.phase = SendMessagePhase::Confirming;
                        exec_command("paste-file", &[fp], &ExecOptions::default()).await;
                        return Some(SelectedAction {
                            action: actions::sequence(vec![
                                Action::Wait { ms: 100 },
                                Action::Key { combo: "Return".to_string() },
                            ]),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }

                    // Image
                    if let Some(ip) = &params.image_path {
                        if params.fence.is_some() {
                            if main_state_id != Some("chat_open") || !composer_empty_baseline(a11y) {
                                plan_state.fence_error = Some(FENCE_COMPOSER_NOT_EMPTY);
                                plan_state.failure_stage = Some(SendFailureStage::Prepare);
                                plan_state.phase = SendMessagePhase::Failed;
                                return None;
                            }
                            plan_state.file_empty_baseline = true;
                            let mut args: Vec<&str> = vec![ip];
                            if let Some(mime) = &params.image_mime { args.push(mime); }
                            let result = exec_command_controlled(
                                "paste-image", &args, &paste_options(params), FILE_PASTE_DEADLINE,
                            ).await;
                            if !result.succeeded() {
                                plan_state.fence_error = Some(FENCE_PASTE_FAILED);
                                plan_state.failure_stage = Some(SendFailureStage::Prepare);
                                plan_state.phase = SendMessagePhase::Failed;
                                return None;
                            }
                            plan_state.file_paste_succeeded = true;
                            plan_state.phase = SendMessagePhase::ReadyToCommit;
                            return Some(SelectedAction {
                                action: Action::Wait { ms: 100 },
                                frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                            });
                        }
                        plan_state.phase = SendMessagePhase::Confirming;
                        let mut args: Vec<&str> = vec![ip];
                        if let Some(mime) = &params.image_mime {
                            args.push(mime);
                        }
                        exec_command("paste-image", &args, &ExecOptions::default()).await;
                        return Some(SelectedAction {
                            action: actions::sequence(vec![
                                Action::Wait { ms: 100 },
                                Action::Key { combo: "Return".to_string() },
                            ]),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }

                    // Text
                    if let Some(msg) = &params.message {
                        // Fenced text: prepare the input only. `Return` is a
                        // separate step, so the fence can be re-checked in the
                        // exact step that commits the send.
                        if params.fence.is_some() {
                            plan_state.phase = SendMessagePhase::ReadyToCommit;
                            return Some(SelectedAction {
                                action: actions::sequence(vec![
                                    Action::Key { combo: "ctrl+a".to_string() },
                                    Action::Type { text: msg.clone(), selector: None },
                                    Action::Wait { ms: 100 },
                                ]),
                                frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                            });
                        }

                        // Legacy unfenced text: type and send in one sequence.
                        plan_state.phase = SendMessagePhase::Confirming;
                        return Some(SelectedAction {
                            action: actions::sequence(vec![
                                Action::Key { combo: "ctrl+a".to_string() },
                                Action::Type { text: msg.clone(), selector: None },
                                Action::Wait { ms: 100 },
                                Action::Key { combo: "Return".to_string() },
                            ]),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }

                    return None;
                }

                SendMessagePhase::ReadyToCommit => {
                    // The composer and the open chat are re-observed in this
                    // same step, under the caller's already-held guard. A file
                    // commit additionally requires the empty baseline and a paste
                    // that this run observed to succeed; a staged file attachment
                    // keeps the message empty, so its readiness is read from the
                    // Send control rather than the text-mode editor/message
                    // pairing.
                    let composer_ready = if params.file_path.is_some() || params.image_path.is_some() {
                        plan_state.file_empty_baseline
                            && plan_state.file_paste_succeeded
                            && file_composer_ready(a11y)
                    } else {
                        find_edit_and_send_button(a11y).is_some()
                    };
                    if main_state_id != Some("chat_open") || !composer_ready {
                        plan_state.fence_error = Some(if main_state_id == Some("chat_open") {
                            FENCE_COMPOSER_NOT_READY
                        } else {
                            FENCE_TARGET_CHANGED
                        });
                        plan_state.failure_stage = Some(SendFailureStage::PreCommit);
                        plan_state.phase = SendMessagePhase::Failed;
                        return None;
                    }

                    let Some(fence) = &params.fence else {
                        // This phase is only entered with a fence; reaching it
                        // without one is a caller error, not a licence to send.
                        plan_state.fence_error = Some(FENCE_IDENTITY_UNAVAILABLE);
                        plan_state.failure_stage = Some(SendFailureStage::PreCommit);
                        plan_state.phase = SendMessagePhase::Failed;
                        return None;
                    };

                    // The fence is checked here, and only here, immediately
                    // before `Return`.
                    if let Err(code) = fence.recheck().await {
                        plan_state.fence_error = Some(code);
                        plan_state.failure_stage = Some(SendFailureStage::PreCommit);
                        plan_state.phase = SendMessagePhase::Failed;
                        return None;
                    }

                    // Recorded before dispatch: this says a commit was handed to
                    // the UI driver, never that the message reached anyone.
                    plan_state.commit_attempted = true;
                    plan_state.failure_stage = Some(SendFailureStage::PostCommit);
                    plan_state.phase = SendMessagePhase::Confirming;

                    return Some(SelectedAction {
                        action: Action::Key { combo: "Return".to_string() },
                        frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                    });
                }

                SendMessagePhase::Confirming => {
                    let found = find_edit_and_send_button(a11y);
                    let (_, send_btn) = match found {
                        Some(f) => f,
                        None => return None,
                    };

                    if send_is_disabled(send_btn) {
                        plan_state.phase = SendMessagePhase::Done;
                        return Some(SelectedAction {
                            action: actions::wait_short(),
                            frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                        });
                    }

                    plan_state.confirm_attempts += 1;
                    if plan_state.confirm_attempts >= 5 {
                        return None;
                    }

                    return Some(SelectedAction {
                        action: actions::wait_short(),
                        frame: identified.main_window.as_ref().and_then(|m| m.frame.clone()),
                    });
                }

                SendMessagePhase::Done => return None,

                SendMessagePhase::Failed => return None,
            }
        }
    }
}
