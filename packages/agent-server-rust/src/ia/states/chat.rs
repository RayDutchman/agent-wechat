use crate::ia::helpers::{extract_active_chat_id, find_frame_for, has_valid_bounds};
use crate::ia::selectors::query_selector;
use crate::ia::types::*;
use super::base::extract_window_control_bounds;

/// Find the More menu dropdown filler (filler containing Settings + Lock buttons).
/// Searches recursively since the filler may be nested under application > ...
fn find_more_menu_filler<'a>(node: &'a A11yNode) -> Option<&'a A11yNode> {
    if let Some(children) = &node.children {
        for child in children {
            if child.role == "filler"
                && query_selector(child, r#"push-button[name="Settings"]"#).is_some()
                && query_selector(child, r#"push-button[name="Lock"]"#).is_some()
            {
                return Some(child);
            }
            // Recurse, but skip frame nodes (the filler is outside the WeChat frame)
            if child.role != "frame" {
                if let Some(found) = find_more_menu_filler(child) {
                    return Some(found);
                }
            }
        }
    }
    None
}

/// Extract More button bounds and Settings menu item bounds (when More dropdown is open).
fn extract_more_menu_bounds(a11y: &A11yNode, state: &mut AppState) {
    // More button in Navigation toolbar
    state.main_window.more_button_bounds =
        query_selector(a11y, r#"tool-bar[name="Navigation"] push-button[name="More"]"#)
            .and_then(|n| n.bounds.clone());

    // Settings menu item from More dropdown (only when dropdown is open)
    state.main_window.settings_menu_item_bounds = find_more_menu_filler(a11y)
        .and_then(|filler| query_selector(filler, r#"push-button[name="Settings"]"#))
        .and_then(|n| n.bounds.clone());
}

fn is_chat_view(a11y: &A11yNode) -> bool {
    let main_btn = query_selector(a11y, r#"push-button[name="Weixin"]"#)
        .or_else(|| query_selector(a11y, r#"push-button[name="WeChat"]"#));
    if main_btn.is_none() {
        return false;
    }
    let has_contacts = query_selector(a11y, r#"push-button[name="Contacts"]"#).is_some();
    let has_chats = query_selector(a11y, r#"list[name="Chats"]"#).is_some();
    has_contacts && has_chats
}

fn find_selected_chat_item(a11y: &A11yNode) -> Option<&A11yNode> {
    let chat_list = query_selector(a11y, r#"list[name="Chats"]"#)?;
    chat_list
        .children
        .as_ref()?
        .iter()
        .find(|item| item.states.as_ref().map(|s| s.iter().any(|st| st == "SELECTED")).unwrap_or(false))
}

/// Recognize the chat pane independently of the virtualized chat list.
/// A selected row can be outside the accessibility tree, and a title is not a
/// unique identity. Identity verification remains in chat selection/send fences;
/// composer readiness is checked by the plan before input or commit.
fn has_open_chat_pane(a11y: &A11yNode) -> bool {
    let messages = query_selector(a11y, r#"list[name="Messages"]"#);
    let send = query_selector(a11y, r#"push-button[name="Send"]"#)
        .or_else(|| query_selector(a11y, r#"push-button[name="Send(S)"]"#));
    messages.is_some_and(|node| has_valid_bounds(&node.bounds))
        && send.is_some_and(|node| has_valid_bounds(&node.bounds))
}

/// Chat list view without an observable chat pane.
struct ChatState;

impl IAState for ChatState {
    fn fsm(&self) -> &str { "mainWindow" }
    fn id(&self) -> &str { "chat" }

    fn identify(&self, args: &IdentifyArgs) -> Result<IdentifyResult, String> {
        if !is_chat_view(args.a11y) {
            return Ok(IdentifyResult { identified: false, frame: None });
        }
        if has_open_chat_pane(args.a11y) {
            return Ok(IdentifyResult { identified: false, frame: None });
        }
        Ok(IdentifyResult { identified: true, frame: find_frame_for(args.a11y, r#"list[name="Chats"]"#) })
    }

    fn reduce(&self, args: &ReduceArgs) -> AppState {
        let wb = extract_window_control_bounds(None);
        let mut state = args.prev.clone();
        state.main_window.view = MainWindowView::Chat;
        state.main_window.is_logged_in = true;
        state.main_window.opened_chat_name = None;
        state.main_window.opened_chat_is_group = None;
        state.main_window.selected_chat_bounds = None;
        state.main_window.close_button_bounds = wb.close_button_bounds;
        state.main_window.minimize_button_bounds = wb.minimize_button_bounds;
        state.main_window.maximize_button_bounds = wb.maximize_button_bounds;
        extract_more_menu_bounds(args.a11y, &mut state);
        state
    }
}

/// Chat open state — the chat pane is visible, regardless of list scroll.
struct ChatOpenState;

impl IAState for ChatOpenState {
    fn fsm(&self) -> &str { "mainWindow" }
    fn id(&self) -> &str { "chat_open" }

    fn identify(&self, args: &IdentifyArgs) -> Result<IdentifyResult, String> {
        if !is_chat_view(args.a11y) {
            return Ok(IdentifyResult { identified: false, frame: None });
        }
        if !has_open_chat_pane(args.a11y) {
            return Ok(IdentifyResult { identified: false, frame: None });
        }
        Ok(IdentifyResult { identified: true, frame: find_frame_for(args.a11y, r#"list[name="Chats"]"#) })
    }

    fn reduce(&self, args: &ReduceArgs) -> AppState {
        let wb = extract_window_control_bounds(None);
        let a11y = args.a11y;

        // Extract opened chat name from header area
        let chat_list = query_selector(a11y, r#"list[name="Chats"]"#);
        let chat_list_right = chat_list
            .and_then(|c| c.bounds.as_ref())
            .map(|b| b.x + b.width)
            .unwrap_or(272.0);

        // Collect all labels and find one to the right of chat list, near top
        let mut all_labels = Vec::new();
        collect_labels(a11y, &mut all_labels);

        let header_label = all_labels.iter().find(|label| {
            if let Some(b) = &label.bounds {
                b.x > chat_list_right
                    && b.y < 70.0
                    && !label.name.is_empty()
                    && !label.name.contains("Send")
            } else {
                false
            }
        });

        let raw_name = header_label.map(|l| l.name.clone());

        // Detect group via "(n)" suffix
        let member_count_re = regex::Regex::new(r"\((\d+)\)$").unwrap();
        let is_group = raw_name.as_ref().map(|n| member_count_re.is_match(n)).unwrap_or(false);
        let opened_chat_name = if is_group {
            raw_name.as_ref().map(|n| member_count_re.replace(n, "").trim().to_string())
        } else {
            raw_name.clone()
        };

        // Selected chat bounds
        let selected_item = find_selected_chat_item(a11y);
        let selected_chat_bounds = selected_item.and_then(|item| item.bounds.clone());

        // Active chat ID
        let selected_chat_id = extract_active_chat_id(a11y)
            .or_else(|| args.prev.main_window.selected_chat_id.clone());

        let mut state = args.prev.clone();
        state.main_window.view = MainWindowView::ChatOpen;
        state.main_window.is_logged_in = true;
        state.main_window.selected_chat_id = selected_chat_id;
        state.main_window.opened_chat_name = opened_chat_name;
        state.main_window.opened_chat_is_group = Some(is_group);
        state.main_window.selected_chat_bounds = selected_chat_bounds;
        state.main_window.close_button_bounds = wb.close_button_bounds;
        state.main_window.minimize_button_bounds = wb.minimize_button_bounds;
        state.main_window.maximize_button_bounds = wb.maximize_button_bounds;
        extract_more_menu_bounds(args.a11y, &mut state);
        state
    }
}

fn collect_labels<'a>(node: &'a A11yNode, out: &mut Vec<&'a A11yNode>) {
    if node.role == "label" && !node.name.is_empty() && node.bounds.is_some() {
        out.push(node);
    }
    if let Some(children) = &node.children {
        for child in children {
            collect_labels(child, out);
        }
    }
}

pub static CHAT_STATES: std::sync::LazyLock<Vec<Box<dyn IAState>>> = std::sync::LazyLock::new(|| {
    vec![Box::new(ChatState), Box::new(ChatOpenState)]
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ia::identify_states;
    use serde_json::{json, Value};

    fn chat_tree(selected: bool, pane: Vec<Value>) -> A11yNode {
        serde_json::from_value(json!({"role":"frame","name":"WeChat","children":[
            {"role":"push-button","name":"Weixin"},
            {"role":"push-button","name":"Contacts"},
            {"role":"list","name":"Chats","children":[
                {"role":"list-item","name":"Visible contact",
                 "states":if selected { vec!["SELECTED"] } else { vec![] }}
            ]},
            {"role":"filler","name":"","children":pane}
        ]})).unwrap()
    }

    fn messages() -> Value {
        json!({"role":"list","name":"Messages",
            "bounds":{"x":310.0,"y":80.0,"width":650.0,"height":500.0}})
    }

    fn send() -> Value {
        // An empty composer normally has a disabled Send button. It still
        // proves the pane is present; readiness to commit is checked later.
        json!({"role":"push-button","name":"Send","states":["DISABLED"],
            "bounds":{"x":890.0,"y":700.0,"width":55.0,"height":24.0}})
    }

    fn assert_state(tree: &A11yNode, expected: &str) {
        let identified = identify_states(tree, "");
        assert_eq!(identified.main_window.unwrap().state_id, expected);
        let args = IdentifyArgs { a11y: tree, screenshot: "" };
        assert_ne!(ChatState.identify(&args).unwrap().identified,
                   ChatOpenState.identify(&args).unwrap().identified,
                   "chat states must remain mutually exclusive");
    }

    #[test]
    fn open_pane_without_selected_row_or_title_is_chat_open() {
        assert_state(&chat_tree(false, vec![messages(), send()]), "chat_open");
    }

    #[test]
    fn selected_row_alone_does_not_prove_an_open_pane() {
        assert_state(&chat_tree(true, vec![]), "chat");
    }

    #[test]
    fn incomplete_or_hidden_pane_is_not_open() {
        for pane in [vec![], vec![messages()], vec![send()]] {
            assert_state(&chat_tree(false, pane), "chat");
        }
        let mut hidden_send = send();
        hidden_send["bounds"]["width"] = json!(0.0);
        assert_state(&chat_tree(false, vec![messages(), hidden_send]), "chat");
        let mut hidden_messages = messages();
        hidden_messages["bounds"]["height"] = json!(0.0);
        assert_state(&chat_tree(false, vec![hidden_messages, send()]), "chat");
        let mut voice = send();
        voice["name"] = json!("Send Voice");
        assert_state(&chat_tree(false, vec![messages(), voice]), "chat");
    }

    #[test]
    fn visible_selected_row_and_legacy_send_button_still_work() {
        let mut button = send();
        button["name"] = json!("Send(S)");
        assert_state(&chat_tree(true, vec![messages(), button]), "chat_open");
    }
}
