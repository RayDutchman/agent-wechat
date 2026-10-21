use super::Plan;
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

#[cfg(test)]
mod composer_tests {
    use super::*;
    use serde_json::{json, Value};

    fn editor(y: f64) -> Value {
        json!({"role":"text","name":"","states":["EDITABLE"],
            "bounds":{"x":300.0,"y":y,"width":650.0,"height":80.0}})
    }
    fn send(name: &str) -> Value {
        json!({"role":"push-button","name":name,"states":["DISABLED"],
            "bounds":{"x":890.0,"y":700.0,"width":55.0,"height":24.0}})
    }
    fn group(children: Vec<Value>) -> Value {
        json!({"role":"filler","name":"","children":children})
    }
    fn parse(value: Value) -> A11yNode { serde_json::from_value(value).unwrap() }

    #[test]
    fn legacy_sibling_composer() {
        let tree = parse(group(vec![editor(610.0), send("Send(S)")]));
        assert!(find_edit_and_send_button(&tree).is_some());
    }

    #[test]
    fn nested_new_composer_with_search_field() {
        let composer = group(vec![group(vec![editor(610.0)]),
            group(vec![group(vec![send("Send Voice"), send("Send")])])]);
        let tree = parse(group(vec![editor(45.0), composer]));
        let (edit, button) = find_edit_and_send_button(&tree).unwrap();
        assert_eq!(edit.bounds.as_ref().unwrap().y, 610.0);
        assert_eq!(button.name, "Send");
    }

    #[test]
    fn does_not_pair_search_field_with_send() {
        let tree = parse(group(vec![group(vec![editor(45.0)]), group(vec![send("Send")])]));
        assert!(find_edit_and_send_button(&tree).is_none());
    }

    #[test]
    fn rejects_ambiguous_editors_and_voice_button() {
        let tree = parse(group(vec![group(vec![editor(610.0), editor(610.0)]), group(vec![send("Send")])]));
        assert!(find_edit_and_send_button(&tree).is_none());
        let voice = parse(group(vec![editor(610.0), send("Send Voice")]));
        assert!(find_edit_and_send_button(&voice).is_none());
    }
}

#[cfg(test)]
mod fenced_send_tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A fence whose verdict the test chooses, counting how often it was asked.
    struct FakeFence {
        outcome: Result<(), &'static str>,
        calls: AtomicU32,
    }

    impl FakeFence {
        fn new(outcome: Result<(), &'static str>) -> Arc<Self> {
            Arc::new(FakeFence {
                outcome,
                calls: AtomicU32::new(0),
            })
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SendFence for FakeFence {
        async fn recheck(&self) -> Result<(), &'static str> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome
        }
    }

    fn editor(y: f64) -> Value {
        json!({"role":"text","name":"","states":["EDITABLE"],
            "bounds":{"x":300.0,"y":y,"width":650.0,"height":80.0}})
    }
    fn send(name: &str) -> Value {
        json!({"role":"push-button","name":name,"states":["DISABLED"],
            "bounds":{"x":890.0,"y":700.0,"width":55.0,"height":24.0}})
    }
    fn group(children: Vec<Value>) -> Value {
        json!({"role":"filler","name":"","children":children})
    }

    /// The nested 4.1.13 composer the production focus path accepts.
    fn composer_tree() -> Value {
        let composer = group(vec![
            group(vec![editor(610.0)]),
            group(vec![group(vec![send("Send Voice"), send("Send")])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    fn params(text: &str, fence: Option<Arc<dyn SendFence>>) -> SendMessageParams {
        SendMessageParams {
            chat_id: "filehelper".to_string(),
            message: Some(text.to_string()),
            image_path: None,
            image_mime: None,
            file_path: None,
            fence,
        }
    }

    fn state_at(phase: SendMessagePhase) -> SendMessagePlanState {
        SendMessagePlanState {
            phase,
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

    async fn select(
        plan_state: &mut SendMessagePlanState,
        params: &SendMessageParams,
        tree: &Value,
    ) -> Option<SelectedAction> {
        let plan = SendMessagePlan;
        let a11y: A11yNode = serde_json::from_value(tree.clone()).unwrap();
        let state = AppState::default();
        let identified = IdentifiedStates {
            main_window: Some(IdentifiedState {
                state_id: "chat_open".to_string(),
                fsm: "chat".to_string(),
                frame: None,
            }),
            popup: None,
            contact_card: None,
            settings: None,
        };
        plan.select_action(&state, params, &identified, plan_state, &a11y, "session")
            .await
    }

    fn contains_return(action: &Action) -> bool {
        match action {
            Action::Key { combo } => combo == "Return",
            Action::Sequence { actions } => actions.iter().any(contains_return),
            _ => false,
        }
    }

    #[tokio::test]
    async fn a_changed_identity_refuses_before_return() {
        let fence = FakeFence::new(Err(FENCE_SCOPE_CHANGED));
        let p = params("hola", Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        assert!(selected.is_none(), "no Return may be dispatched after a scope change");
        assert!(matches!(plan_state.phase, SendMessagePhase::Failed));
        assert_eq!(plan_state.fence_error, Some(FENCE_SCOPE_CHANGED));
        assert!(!plan_state.commit_attempted, "a refusal is not a commit attempt");
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn a_drifted_target_refuses_before_return() {
        let fence = FakeFence::new(Err(FENCE_TARGET_CHANGED));
        let p = params("hola", Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        assert!(selected.is_none(), "no Return may be dispatched after a target change");
        assert!(matches!(plan_state.phase, SendMessagePhase::Failed));
        assert_eq!(plan_state.fence_error, Some(FENCE_TARGET_CHANGED));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn an_agreed_fence_dispatches_return_and_records_the_attempt() {
        let fence = FakeFence::new(Ok(()));
        let p = params("hola", Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        let selected = selected.expect("an agreed fence dispatches the commit");
        assert!(matches!(&selected.action, Action::Key { combo } if combo == "Return"));
        assert!(plan_state.commit_attempted, "the attempt is recorded before dispatch");
        assert!(matches!(plan_state.phase, SendMessagePhase::Confirming));
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn a_missing_composer_refuses_without_consulting_the_fence() {
        let fence = FakeFence::new(Ok(()));
        let p = params("hola", Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);
        // A tree with an editor but no Send button: the composer is gone.
        let tree = group(vec![editor(610.0)]);

        let selected = select(&mut plan_state, &p, &tree).await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_READY));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0, "a missing composer is refused before the fence is asked");
    }

    #[tokio::test]
    async fn ready_to_commit_without_a_fence_fails_closed() {
        let p = params("hola", None);
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_IDENTITY_UNAVAILABLE));
        assert!(!plan_state.commit_attempted);
    }

    #[tokio::test]
    async fn the_fenced_input_round_prepares_without_returning() {
        let fence = FakeFence::new(Ok(()));
        let p = params("hola", Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        let selected = selected.expect("the prepare round returns an input action");
        assert!(
            !contains_return(&selected.action),
            "the prepare round must not dispatch Return"
        );
        assert!(
            matches!(plan_state.phase, SendMessagePhase::ReadyToCommit),
            "the next round is the fenced commit"
        );
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0, "the fence is not asked during preparation");
    }

    #[tokio::test]
    async fn the_unfenced_input_round_keeps_the_legacy_single_sequence() {
        let p = params("hola", None);
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &composer_tree()).await;

        let selected = selected.expect("the legacy path still sends");
        assert!(contains_return(&selected.action));
        assert!(matches!(plan_state.phase, SendMessagePhase::Confirming));
        assert!(!plan_state.commit_attempted, "the legacy path does not use the fence flag");
    }
}

#[cfg(test)]
mod fenced_file_tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A fence whose verdict the test chooses, counting how often it was asked.
    struct FakeFence {
        outcome: Result<(), &'static str>,
        calls: AtomicU32,
    }

    impl FakeFence {
        fn new(outcome: Result<(), &'static str>) -> Arc<Self> {
            Arc::new(FakeFence {
                outcome,
                calls: AtomicU32::new(0),
            })
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SendFence for FakeFence {
        async fn recheck(&self) -> Result<(), &'static str> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome
        }
    }

    fn editor(y: f64) -> Value {
        json!({"role":"text","name":"","states":["EDITABLE"],
            "bounds":{"x":300.0,"y":y,"width":650.0,"height":80.0}})
    }
    /// An editable field with an explicit label and the derived text state the
    /// a11y helper reports for its contents.
    fn editor_with_states(y: f64, name: &str, states: Value) -> Value {
        json!({"role":"text","name":name,"states":states,
            "bounds":{"x":300.0,"y":y,"width":650.0,"height":80.0}})
    }
    fn send(states: Value) -> Value {
        json!({"role":"push-button","name":"Send","states":states,
            "bounds":{"x":890.0,"y":700.0,"width":55.0,"height":24.0}})
    }
    fn group(children: Vec<Value>) -> Value {
        json!({"role":"filler","name":"","children":children})
    }

    /// A composer whose editable field is truly empty but carries a non-empty
    /// label, as the client's accessibility name does; the derived `TEXT_EMPTY`
    /// state, not the label, is the evidence.
    fn attachment_tree(send_disabled: bool) -> Value {
        let states = if send_disabled {
            json!(["DISABLED"])
        } else {
            json!([])
        };
        let composer = group(vec![
            group(vec![editor_with_states(
                610.0,
                "Chats",
                json!(["EDITABLE", "TEXT_EMPTY"]),
            )]),
            group(vec![group(vec![send(states)])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    /// An editable field whose label is empty but whose true contents are not.
    fn nonempty_text_tree(send_disabled: bool) -> Value {
        let states = if send_disabled {
            json!(["DISABLED"])
        } else {
            json!([])
        };
        let composer = group(vec![
            group(vec![editor_with_states(
                610.0,
                "",
                json!(["EDITABLE", "TEXT_NONEMPTY"]),
            )]),
            group(vec![group(vec![send(states)])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    /// An editable field from a helper that does not report a derived text
    /// state at all: the run must fail closed and never paste.
    fn unannotated_text_tree() -> Value {
        let composer = group(vec![
            group(vec![editor_with_states(610.0, "", json!(["EDITABLE"]))]),
            group(vec![group(vec![send(json!(["DISABLED"]))])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    fn file_params(fence: Option<Arc<dyn SendFence>>) -> SendMessageParams {
        SendMessageParams {
            chat_id: "filehelper".to_string(),
            message: None,
            image_path: None,
            image_mime: None,
            file_path: Some("/tmp/staged/attachment.bin".to_string()),
            fence,
        }
    }

    fn image_params(fence: Option<Arc<dyn SendFence>>) -> SendMessageParams {
        SendMessageParams {
            chat_id: "filehelper".to_string(), message: None,
            image_path: Some("/tmp/staged/image.png".to_string()), image_mime: Some("image/png".to_string()),
            file_path: None, fence,
        }
    }

    fn state_at(phase: SendMessagePhase) -> SendMessagePlanState {
        SendMessagePlanState {
            phase,
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

    /// A plan state at the commit step that already recorded the empty baseline
    /// and a successful paste, as the prepare step would.
    fn staged_state_at_commit() -> SendMessagePlanState {
        let mut state = state_at(SendMessagePhase::ReadyToCommit);
        state.file_empty_baseline = true;
        state.file_paste_succeeded = true;
        state
    }

    /// A fence that only supplies execution options, for the display test.
    struct DisplayFence(ExecOptions);

    #[async_trait::async_trait]
    impl SendFence for DisplayFence {
        async fn recheck(&self) -> Result<(), &'static str> {
            Ok(())
        }

        fn exec_options(&self) -> ExecOptions {
            self.0.clone()
        }
    }

    fn session(display: &str) -> Session {
        Session {
            id: "s".into(),
            name: "default".into(),
            linux_user: "wechat".into(),
            display: display.into(),
            dbus_address: None,
            vnc_port: 5900,
            status: "running".into(),
            login_state: "logged_in".into(),
            logged_in_user: None,
            wechat_pid: None,
            xvfb_pid: None,
            dbus_pid: None,
            error_message: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    /// A composer holding a typed draft: the label is empty but the true
    /// contents are not, so the run must not paste on top of it.
    fn draft_tree() -> Value {
        let composer = group(vec![
            group(vec![editor_with_states(
                610.0,
                "",
                json!(["EDITABLE", "TEXT_NONEMPTY"]),
            )]),
            group(vec![group(vec![send(json!([]))])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    async fn select(
        plan_state: &mut SendMessagePlanState,
        params: &SendMessageParams,
        tree: &Value,
        state_id: &str,
    ) -> Option<SelectedAction> {
        let plan = SendMessagePlan;
        let a11y: A11yNode = serde_json::from_value(tree.clone()).unwrap();
        let state = AppState::default();
        let identified = IdentifiedStates {
            main_window: Some(IdentifiedState {
                state_id: state_id.to_string(),
                fsm: "chat".to_string(),
                frame: None,
            }),
            popup: None,
            contact_card: None,
            settings: None,
        };
        plan.select_action(&state, params, &identified, plan_state, &a11y, "session")
            .await
    }

    fn contains_return(action: &Action) -> bool {
        match action {
            Action::Key { combo } => combo == "Return",
            Action::Sequence { actions } => actions.iter().any(contains_return),
            _ => false,
        }
    }

    /// An occupied composer is never pasted into: neither a user's draft nor a
    /// previous run's leftover attachment may be combined with this request.
    #[tokio::test]
    async fn an_occupied_composer_is_refused_before_any_paste() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        // Send enabled: the composer already holds a previous attachment.
        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none(), "an occupied composer must never be pasted into");
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert!(matches!(plan_state.phase, SendMessagePhase::Failed));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    /// A typed draft is refused too, even though it is not an attachment.
    #[tokio::test]
    async fn a_typed_draft_is_refused_before_any_paste() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &draft_tree(), "chat_open").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    /// A field with a non-empty label but truly empty contents is accepted: the
    /// label is not the contents.
    #[tokio::test]
    async fn a_labelled_but_truly_empty_composer_is_accepted_for_pasting() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &attachment_tree(true), "chat_open").await;

        assert!(
            plan_state.file_empty_baseline,
            "a derived TEXT_EMPTY field must be accepted as the empty baseline"
        );
        assert_ne!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert!(!plan_state.commit_attempted);
        // No paste helper is available here, so the run stops at the paste,
        // which is after the baseline was accepted.
        assert!(selected.is_none());
    }

    /// A field whose true contents are non-empty is refused even when the label
    /// is empty and Send is disabled.
    #[tokio::test]
    async fn a_truly_nonempty_composer_is_refused_before_any_paste() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &nonempty_text_tree(true), "chat_open").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert!(!plan_state.file_empty_baseline);
        assert!(!plan_state.commit_attempted);
    }

    /// A helper that reports no derived text state must not be read as empty.
    #[tokio::test]
    async fn an_editable_without_a_derived_text_state_is_refused() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &unannotated_text_tree(), "chat_open").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert!(!plan_state.commit_attempted);
    }

    /// The paste step never dispatches a Return, whether it succeeds or not.
    #[tokio::test]
    async fn the_fenced_file_prepare_never_dispatches_return() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &attachment_tree(true), "chat_open").await;

        if let Some(selected) = selected {
            assert!(
                !contains_return(&selected.action),
                "the prepare round must not dispatch Return"
            );
        }
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0, "the fence is not asked during preparation");
    }

    /// A paste that does not exit zero within its deadline is refused and never
    /// reaches Return. The paste command is absent in the test environment, so
    /// this exercises the failure path without a real helper.
    #[tokio::test]
    async fn a_paste_that_did_not_succeed_refuses_before_return() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);

        let selected = select(&mut plan_state, &p, &attachment_tree(true), "chat_open").await;

        assert!(selected.is_none(), "a paste that did not succeed must not reach Return");
        assert_eq!(plan_state.fence_error, Some(FENCE_PASTE_FAILED));
        assert!(matches!(plan_state.phase, SendMessagePhase::Failed));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    /// The commit step requires the recorded empty baseline; without it the run
    /// never commits even if the composer currently looks ready.
    #[tokio::test]
    async fn a_commit_without_a_recorded_baseline_refuses() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);
        plan_state.file_paste_succeeded = true;

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none(), "a commit without the empty baseline must not Return");
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    /// The commit step requires the paste to have been observed succeeding.
    #[tokio::test]
    async fn a_commit_without_a_successful_paste_refuses() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::ReadyToCommit);
        plan_state.file_empty_baseline = true;

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none(), "a commit without a successful paste must not Return");
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    #[tokio::test]
    async fn a_ready_attachment_dispatches_exactly_one_return() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        let selected = selected.expect("a ready attachment dispatches the commit");
        assert!(matches!(&selected.action, Action::Key { combo } if combo == "Return"));
        assert!(plan_state.commit_attempted, "the attempt is recorded before dispatch");
        assert!(matches!(plan_state.phase, SendMessagePhase::Confirming));
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn a_ready_fenced_image_dispatches_one_return_after_the_live_fence() {
        let fence = FakeFence::new(Ok(()));
        let p = image_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();
        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await.unwrap();
        assert!(matches!(&selected.action, Action::Key { combo } if combo == "Return"));
        assert!(plan_state.commit_attempted);
        assert_eq!(plan_state.failure_stage, Some(SendFailureStage::PostCommit));
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn a_fenced_image_with_an_occupied_composer_stops_in_prepare() {
        let fence = FakeFence::new(Ok(()));
        let p = image_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = state_at(SendMessagePhase::Inputting);
        let selected = select(&mut plan_state, &p, &draft_tree(), "chat_open").await;
        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_EMPTY));
        assert_eq!(plan_state.failure_stage, Some(SendFailureStage::Prepare));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    #[tokio::test]
    async fn a_changed_identity_refuses_the_file_commit_before_return() {
        let fence = FakeFence::new(Err(FENCE_SCOPE_CHANGED));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none(), "no Return may be dispatched after a scope change");
        assert_eq!(plan_state.fence_error, Some(FENCE_SCOPE_CHANGED));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 1);
    }

    #[tokio::test]
    async fn a_drifted_target_refuses_the_file_commit_before_return() {
        let fence = FakeFence::new(Err(FENCE_TARGET_CHANGED));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none(), "no Return may be dispatched after a target change");
        assert_eq!(plan_state.fence_error, Some(FENCE_TARGET_CHANGED));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 1);
    }

    /// A Send control that is still disabled means the attachment is not
    /// observable in the composer. The plan refuses instead of assuming the
    /// paste worked, and it does not consult the fence for a composer it cannot
    /// confirm.
    #[tokio::test]
    async fn an_attachment_that_is_not_visible_refuses_without_returning() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(true), "chat_open").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_COMPOSER_NOT_READY));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0, "a composer with no staged content is refused first");
    }

    #[tokio::test]
    async fn a_file_commit_requires_the_open_chat() {
        let fence = FakeFence::new(Ok(()));
        let p = file_params(Some(fence.clone() as Arc<dyn SendFence>));
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_TARGET_CHANGED));
        assert!(!plan_state.commit_attempted);
        assert_eq!(fence.calls(), 0);
    }

    #[tokio::test]
    async fn a_file_commit_without_a_fence_fails_closed() {
        let p = file_params(None);
        let mut plan_state = staged_state_at_commit();

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        assert!(selected.is_none());
        assert_eq!(plan_state.fence_error, Some(FENCE_IDENTITY_UNAVAILABLE));
        assert!(!plan_state.commit_attempted);
    }

    /// The paste uses the fence's execution options, so it runs on the session
    /// the route fenced rather than the compiled-in fallback.
    #[test]
    fn the_fenced_file_paste_uses_the_fences_display() {
        let fence = Arc::new(DisplayFence(ExecOptions {
            session: Some(session(":77")),
            timeout_ms: 1_000,
        }));
        let p = file_params(Some(fence as Arc<dyn SendFence>));

        let options = paste_options(&p);

        assert_eq!(options.session.expect("a session").display, ":77");
    }

    /// Once a Return has been handed over (or an error/cancel ended the round),
    /// the confirming phase never produces a second Return.
    #[tokio::test]
    async fn a_confirming_file_send_never_returns_again() {
        let p = file_params(None);
        let mut plan_state = state_at(SendMessagePhase::Confirming);

        let selected = select(&mut plan_state, &p, &attachment_tree(false), "chat_open").await;

        if let Some(selected) = selected {
            assert!(!contains_return(&selected.action), "no second Return after the commit");
        }
    }
}

#[cfg(test)]
mod failure_reason_tests {
    use super::*;
    use serde_json::{json, Value};

    fn editor(y: f64) -> Value {
        json!({"role":"text","name":"","states":["EDITABLE"],
            "bounds":{"x":300.0,"y":y,"width":650.0,"height":80.0}})
    }
    fn send(name: &str) -> Value {
        json!({"role":"push-button","name":name,"states":["DISABLED"],
            "bounds":{"x":890.0,"y":700.0,"width":55.0,"height":24.0}})
    }
    fn group(children: Vec<Value>) -> Value {
        json!({"role":"filler","name":"","children":children})
    }

    fn composer_tree() -> Value {
        let composer = group(vec![
            group(vec![editor(610.0)]),
            group(vec![group(vec![send("Send Voice"), send("Send")])]),
        ]);
        group(vec![editor(45.0), composer])
    }

    fn params(text: &str) -> SendMessageParams {
        SendMessageParams {
            chat_id: "filehelper".to_string(),
            message: Some(text.to_string()),
            image_path: None,
            image_mime: None,
            file_path: None,
            fence: None,
        }
    }

    fn state_at(phase: SendMessagePhase) -> SendMessagePlanState {
        SendMessagePlanState {
            phase,
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

    /// One selection round with the main window identified as `state_id`, which
    /// is how the focus branch decides whether the chat is open.
    async fn select_as(
        plan_state: &mut SendMessagePlanState,
        params: &SendMessageParams,
        tree: &Value,
        state_id: &str,
    ) -> Option<SelectedAction> {
        let plan = SendMessagePlan;
        let a11y: A11yNode = serde_json::from_value(tree.clone()).unwrap();
        let state = AppState::default();
        let identified = IdentifiedStates {
            main_window: Some(IdentifiedState {
                state_id: state_id.to_string(),
                fsm: "chat".to_string(),
                frame: None,
            }),
            popup: None,
            contact_card: None,
            settings: None,
        };
        plan.select_action(&state, params, &identified, plan_state, &a11y, "session")
            .await
    }

    /// The focus round only refuses with `chat_not_open` when the open chat was
    /// not identified. Nothing else about the round moves: the phase stays, no
    /// action is produced and no commit is recorded.
    #[tokio::test]
    async fn focusing_without_an_open_chat_records_chat_not_open_and_nothing_else() {
        let p = params("hola");
        let mut plan_state = state_at(SendMessagePhase::Focusing);

        let selected = select_as(&mut plan_state, &p, &composer_tree(), "chat").await;

        assert!(selected.is_none(), "the round still selects no action");
        assert_eq!(
            plan_state.failure_reason.as_deref(),
            Some(SEND_FAIL_CHAT_NOT_OPEN)
        );
        assert_eq!(
            SendMessagePlan.get_failure_reason(&plan_state),
            Some(SEND_FAIL_CHAT_NOT_OPEN.to_string())
        );
        // Unchanged behaviour: no phase move, no fence verdict, no commit.
        assert!(matches!(plan_state.phase, SendMessagePhase::Focusing));
        assert_eq!(plan_state.fence_error, None);
        assert!(plan_state.open_result.is_none());
        assert!(!plan_state.commit_attempted);
    }

    #[tokio::test]
    async fn open_pane_without_visible_selected_row_reaches_composer_focus() {
        // Exercise the real identifier, not a hand-supplied "chat_open" state.
        // The target is already selected by the opening step; its row is outside
        // the visible list. No title match is needed to focus the composer.
        let tree = group(vec![
            json!({"role":"push-button","name":"Weixin"}),
            json!({"role":"push-button","name":"Contacts"}),
            json!({"role":"list","name":"Chats","children":[
                {"role":"list-item","name":"Other visible contact"}
            ]}),
            json!({"role":"list","name":"Messages",
                "bounds":{"x":300.0,"y":80.0,"width":650.0,"height":500.0}}),
            composer_tree(),
        ]);
        let a11y: A11yNode = serde_json::from_value(tree).unwrap();
        let identified = crate::ia::identify_states(&a11y, "");
        let mut plan_state = state_at(SendMessagePhase::Focusing);
        let selected = SendMessagePlan.select_action(
            &AppState::default(), &params("hello"), &identified,
            &mut plan_state, &a11y, "session",
        ).await;
        assert!(selected.is_some(), "the composer must be focusable even without a visible selected row");
        assert!(matches!(plan_state.phase, SendMessagePhase::Inputting));
        assert!(plan_state.failure_reason.is_none());
        assert!(!plan_state.commit_attempted, "focusing must not commit a send");
    }

    /// A selection helper that did not answer with success is a different
    /// observation from an unidentified open chat, and is reported as such.
    /// The chat-select command is absent in the test environment, so this
    /// exercises the real failure branch.
    #[tokio::test]
    async fn a_chat_selection_that_failed_records_its_own_reason() {
        let p = params("hola");
        let mut plan_state = state_at(SendMessagePhase::Opening);

        let selected = select_as(&mut plan_state, &p, &composer_tree(), "chat").await;

        assert!(selected.is_none(), "the round still selects no action");
        assert_eq!(
            plan_state.failure_reason.as_deref(),
            Some(SEND_FAIL_CHAT_SELECTION_FAILED)
        );
        assert_ne!(
            plan_state.failure_reason.as_deref(),
            Some(SEND_FAIL_CHAT_NOT_OPEN),
            "a failed selection is not an unidentified chat"
        );
        // Unchanged behaviour: the phase stays, the result is not stored and no
        // commit is attempted.
        assert!(matches!(plan_state.phase, SendMessagePhase::Opening));
        assert!(plan_state.open_result.is_none());
        assert_eq!(plan_state.fence_error, None);
        assert!(!plan_state.commit_attempted);
    }

    /// A round that selects an action records nothing: the reason describes a
    /// stuck run only.
    #[tokio::test]
    async fn a_round_that_selects_an_action_records_no_reason() {
        let p = params("hola");
        let mut plan_state = state_at(SendMessagePhase::Focusing);

        let selected = select_as(&mut plan_state, &p, &composer_tree(), "chat_open").await;

        assert!(selected.is_some());
        assert_eq!(plan_state.failure_reason, None);
        assert_eq!(SendMessagePlan.get_failure_reason(&plan_state), None);
    }

    /// The recorded reason is a fixed key: it carries no chat id, no message
    /// body and nothing else about the request.
    #[tokio::test]
    async fn the_recorded_reason_carries_nothing_about_the_request() {
        let p = params("a-private-message-body");
        let mut plan_state = state_at(SendMessagePhase::Focusing);

        let _ = select_as(&mut plan_state, &p, &composer_tree(), "chat").await;

        let reason = plan_state.failure_reason.expect("a reason is recorded");
        assert!(reason.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        assert!(!reason.contains("filehelper"));
        assert!(!reason.contains("a-private-message-body"));
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

/// Whether a node is the composer's Send control.
///
/// The client labels it `Send` in some builds and `Send(S)` in others; both are
/// the same control, and this one predicate is shared by every lookup below so
/// the two cannot drift apart.
fn is_send_button(node: &A11yNode) -> bool {
    node.role == "push-button" && matches!(node.name.as_str(), "Send(S)" | "Send")
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

fn find_edit_send_pair(node: &A11yNode) -> Option<(&A11yNode, &A11yNode)> {
    if let Some(children) = &node.children {
        let send_btn = children.iter().find(|c| is_send_button(c));
        let edit_node = children.iter().find(|c| {
            c.role == "text"
                && c.states
                    .as_ref()
                    .map(|s| s.iter().any(|st| st == "EDITABLE"))
                    .unwrap_or(false)
        });

        if let (Some(edit), Some(send)) = (edit_node, send_btn) {
            return Some((edit, send));
        }

        // Recurse
        for child in children {
            if let Some(result) = find_edit_send_pair(child) {
                return Some(result);
            }
        }

        // 4.1.13 nests the editor and Send button in separate composer
        // branches. Search the smallest common subtree, requiring one editor
        // and a nearby button below it so the chat-search field cannot match.
        fn collect<'a>(node: &'a A11yNode, edits: &mut Vec<&'a A11yNode>, sends: &mut Vec<&'a A11yNode>) {
            if node.role == "text" && node.states.as_ref().is_some_and(|s| s.iter().any(|s| s == "EDITABLE")) {
                edits.push(node);
            }
            if is_send_button(node) {
                sends.push(node);
            }
            for child in node.children.as_deref().unwrap_or_default() {
                collect(child, edits, sends);
            }
        }
        let (mut edits, mut sends) = (Vec::new(), Vec::new());
        collect(node, &mut edits, &mut sends);
        if let ([edit], [send]) = (edits.as_slice(), sends.as_slice()) {
            if let (Some(e), Some(s)) = (&edit.bounds, &send.bounds) {
                let gap = s.y - (e.y + e.height);
                if e.width > 0.0 && e.height > 0.0 && s.width > 0.0 && s.height > 0.0
                    && (-8.0..=120.0).contains(&gap)
                    && s.x < e.x + e.width && s.x + s.width > e.x
                {
                    return Some((edit, send));
                }
            }
        }
    }
    None
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
