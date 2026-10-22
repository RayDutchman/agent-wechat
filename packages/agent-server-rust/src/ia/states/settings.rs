use crate::ia::helpers::frame_hint_from_node;
use crate::ia::selectors::query_selector;
use crate::ia::types::*;

/// Find the Settings frame in the a11y tree.
fn find_settings_frame<'a>(a11y: &'a A11yNode) -> Option<&'a A11yNode> {
    query_selector(a11y, r#"frame[name="Settings"]"#)
}

/// Settings modal state — Settings frame with confirmation dialog (OK + Cancel).
struct SettingsModalState;

impl IAState for SettingsModalState {
    fn fsm(&self) -> &str { "settings" }
    fn id(&self) -> &str { "settings_modal" }

    fn identify(&self, args: &IdentifyArgs) -> Result<IdentifyResult, String> {
        let frame = match find_settings_frame(args.a11y) {
            Some(f) => f,
            None => return Ok(IdentifyResult { identified: false, frame: None }),
        };

        let has_ok = query_selector(frame, r#"push-button[name="OK"]"#).is_some();
        let has_cancel = query_selector(frame, r#"push-button[name="Cancel"]"#).is_some();

        Ok(IdentifyResult {
            identified: has_ok && has_cancel,
            frame: frame_hint_from_node(frame),
        })
    }

    fn reduce(&self, args: &ReduceArgs) -> AppState {
        let mut state = args.prev.clone();
        if let Some(frame) = find_settings_frame(args.a11y) {
            state.settings = Some(SettingsState {
                logout_button_bounds: query_selector(frame, r#"push-button[name="Log Out"]"#)
                    .and_then(|n| n.bounds.clone()),
                my_account_bounds: query_selector(frame, r#"push-button[name="My Account"]"#)
                    .and_then(|n| n.bounds.clone()),
                modal_ok_bounds: query_selector(frame, r#"push-button[name="OK"]"#)
                    .and_then(|n| n.bounds.clone()),
                modal_cancel_bounds: query_selector(frame, r#"push-button[name="Cancel"]"#)
                    .and_then(|n| n.bounds.clone()),
            });
        }
        state
    }
}

/// Settings state — Settings frame open, no confirmation modal.
struct SettingsStateImpl;

impl IAState for SettingsStateImpl {
    fn fsm(&self) -> &str { "settings" }
    fn id(&self) -> &str { "settings" }

    fn identify(&self, args: &IdentifyArgs) -> Result<IdentifyResult, String> {
        let frame = match find_settings_frame(args.a11y) {
            Some(f) => f,
            None => return Ok(IdentifyResult { identified: false, frame: None }),
        };

        let has_ok = query_selector(frame, r#"push-button[name="OK"]"#).is_some();
        let has_cancel = query_selector(frame, r#"push-button[name="Cancel"]"#).is_some();

        Ok(IdentifyResult {
            identified: !has_ok && !has_cancel,
            frame: frame_hint_from_node(frame),
        })
    }

    fn reduce(&self, args: &ReduceArgs) -> AppState {
        let mut state = args.prev.clone();
        if let Some(frame) = find_settings_frame(args.a11y) {
            state.settings = Some(SettingsState {
                logout_button_bounds: query_selector(frame, r#"push-button[name="Log Out"]"#)
                    .and_then(|n| n.bounds.clone()),
                my_account_bounds: query_selector(frame, r#"push-button[name="My Account"]"#)
                    .and_then(|n| n.bounds.clone()),
                modal_ok_bounds: None,
                modal_cancel_bounds: None,
            });
        }
        state
    }
}

/// Settings states — settings_modal first so it takes priority when modal is present.
pub static SETTINGS_STATES: std::sync::LazyLock<Vec<Box<dyn IAState>>> =
    std::sync::LazyLock::new(|| {
        vec![Box::new(SettingsModalState), Box::new(SettingsStateImpl)]
    });
