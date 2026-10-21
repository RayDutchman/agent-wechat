use super::Plan;
use crate::db::{get_db, queries};
use crate::ia::actions;
use crate::ia::types::*;
use crate::tools::wechat_db::{find_account_dir, find_wechat_pid};
use crate::tools::wechat_keys::{get_stored_keys, needs_extraction_for_process, schedule_key_refresh, schedule_session_key_check_after_login};
use rusqlite::params;

pub struct LoginPlan;

pub struct LoginParams {
    pub new_account: bool,
}

#[derive(Clone)]
pub enum LoginPhase {
    Initializing,
    Authenticating,
    Maximizing,
    DetectingUser,
    ExtractingKeys,
    Done,
}

pub struct LoginPlanState {
    pub phase: LoginPhase,
    pub account_dir: Option<String>,
    pub last_emitted_qr: Option<String>,
    pub emitted_phone_confirm: bool,
    pub detect_retries: u32,
}

#[async_trait::async_trait]
impl Plan for LoginPlan {
    type PlanState = LoginPlanState;
    type Params = LoginParams;

    fn id(&self) -> &str { "login" }

    fn initial_plan_state(&self) -> LoginPlanState {
        LoginPlanState {
            phase: LoginPhase::Initializing,
            account_dir: None,
            last_emitted_qr: None,
            emitted_phone_confirm: false,
            detect_retries: 0,
        }
    }

    fn is_goal_reached(&self, state: &AppState, plan_state: &LoginPlanState) -> bool {
        matches!(
            state.main_window.view,
            MainWindowView::Chat | MainWindowView::ChatOpen
        ) && state.popup.is_none()
            && matches!(plan_state.phase, LoginPhase::Done)
    }

    async fn select_action(
        &self,
        state: &AppState,
        params: &LoginParams,
        identified: &IdentifiedStates,
        plan_state: &mut LoginPlanState,
        _a11y: &A11yNode,
        session_id: &str,
    ) -> Option<SelectedAction> {
        let frame = || identified.main_window.as_ref().and_then(|m| m.frame.clone());

        // Dismiss popups first
        if state.popup.is_some() && identified.popup.is_some() {
            return Some(SelectedAction {
                action: actions::dismiss_popup(),
                frame: frame(),
            });
        }

        match plan_state.phase.clone() {
            LoginPhase::Initializing => {
                handle_initializing(state, params, plan_state, &frame)
            }
            LoginPhase::Authenticating => {
                handle_authenticating(state, params, plan_state, &frame)
            }
            LoginPhase::Maximizing => {
                plan_state.phase = LoginPhase::DetectingUser;
                Some(SelectedAction { action: actions::wait(500), frame: None })
            }
            LoginPhase::DetectingUser => {
                handle_detecting_user(state, plan_state, session_id, frame()).await
            }
            LoginPhase::ExtractingKeys => {
                handle_extracting_keys(plan_state, session_id, frame()).await
            }
            LoginPhase::Done => {
                Some(SelectedAction { action: actions::wait_short(), frame: None })
            }
        }
    }
}

// ============================================
// Phase handlers
// ============================================

fn handle_initializing(
    state: &AppState,
    params: &LoginParams,
    plan_state: &mut LoginPlanState,
    frame: &dyn Fn() -> Option<FrameHint>,
) -> Option<SelectedAction> {
    match state.main_window.view {
        MainWindowView::NetworkProxySettings => {
            // Exit proxy settings page before proceeding
            let action = if state.main_window.proxy_save_failed == Some(true) {
                actions::click_selector(r#"push-button[name="Discard"]"#)
            } else {
                actions::click_back()
            };
            Some(SelectedAction { action, frame: frame() })
        }
        _ => {
            plan_state.phase = LoginPhase::Authenticating;
            handle_authenticating(state, params, plan_state, frame)
        }
    }
}

fn handle_authenticating(
    state: &AppState,
    params: &LoginParams,
    plan_state: &mut LoginPlanState,
    frame: &dyn Fn() -> Option<FrameHint>,
) -> Option<SelectedAction> {
    match state.main_window.view {
        MainWindowView::LoginQr => {
            let qr_data = state.main_window.qr_data.as_ref();
            if let Some(qr) = qr_data {
                if plan_state.last_emitted_qr.as_ref() != Some(qr) {
                    plan_state.last_emitted_qr = Some(qr.clone());
                    return Some(SelectedAction {
                        action: actions::sequence(vec![
                            Action::Emit {
                                event: SubscriptionEvent {
                                    event_type: "qr".to_string(),
                                    data: [
                                        ("qrData".to_string(), serde_json::Value::String(qr.clone())),
                                    ].into_iter().collect(),
                                },
                            },
                            actions::wait(500),
                        ]),
                        frame: frame(),
                    });
                }
            }
            Some(SelectedAction { action: actions::wait(500), frame: None })
        }

        MainWindowView::LoginAccount => {
            let action = if params.new_account {
                actions::click_switch_account()
            } else {
                actions::click_login()
            };
            Some(SelectedAction { action, frame: frame() })
        }

        MainWindowView::LoginPhoneConfirm => {
            if !plan_state.emitted_phone_confirm {
                plan_state.emitted_phone_confirm = true;
                return Some(SelectedAction {
                    action: actions::sequence(vec![
                        Action::Emit {
                            event: SubscriptionEvent {
                                event_type: "phone_confirm".to_string(),
                                data: [(
                                    "message".to_string(),
                                    serde_json::Value::String("Please confirm login on your phone".to_string()),
                                )].into_iter().collect(),
                            },
                        },
                        actions::wait(500),
                    ]),
                    frame: frame(),
                });
            }
            Some(SelectedAction { action: actions::wait(500), frame: None })
        }

        MainWindowView::LoginLoading => {
            Some(SelectedAction { action: actions::wait(500), frame: None })
        }

        MainWindowView::Chat | MainWindowView::ChatOpen => {
            plan_state.phase = LoginPhase::Maximizing;
            Some(SelectedAction {
                action: actions::sequence(vec![
                    actions::wait_long(),
                    actions::maximize(),
                    actions::wait(500),
                ]),
                frame: frame(),
            })
        }

        MainWindowView::NetworkProxySettings => {
            // Landed on proxy page unexpectedly — navigate back
            let action = if state.main_window.proxy_save_failed == Some(true) {
                actions::click_selector(r#"push-button[name="Discard"]"#)
            } else {
                actions::click_back()
            };
            Some(SelectedAction { action, frame: frame() })
        }
    }
}

async fn handle_detecting_user(
    state: &AppState,
    plan_state: &mut LoginPlanState,
    session_id: &str,
    frame: Option<FrameHint>,
) -> Option<SelectedAction> {
    if !matches!(state.main_window.view, MainWindowView::Chat | MainWindowView::ChatOpen) {
        return Some(SelectedAction { action: actions::wait(500), frame: None });
    }

    // All DB access is scoped in blocks so MutexGuard is dropped before any await
    let (mut wechat_pid, mut account_dir) = {
        let db = get_db();
        let pid: Option<i64> = db
            .query_row(
                "SELECT wechat_pid FROM sessions WHERE id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .ok()
            .flatten();

        let acct = pid.and_then(|p| find_account_dir(p));
        (pid, acct)
    };

    if account_dir.is_none() {
        wechat_pid = find_wechat_pid();
        if let Some(pid) = wechat_pid {
            account_dir = find_account_dir(pid);
            let db = get_db();
            let now = chrono::Utc::now().to_rfc3339();
            db.execute(
                "UPDATE sessions SET wechat_pid = ?1, updated_at = ?2 WHERE id = ?3",
                params![pid, now, session_id],
            ).ok();
        }
    }

    if let (Some(pid), Some(acct)) = (wechat_pid, account_dir.clone()) {
        plan_state.account_dir = Some(acct.clone());

        {
            let db = get_db();
            let previous = queries::get_session_logged_in_user(&db, session_id);
            if previous.as_ref().filter(|p| *p != &acct).is_some() {
                queries::clear_session_data(&db, session_id);
            }
            let written = queries::update_session_logged_in_user(&db, session_id, Some(&acct));

            if written.is_err() {
                // The UI is logged in; the ledger is not. Reporting a login
                // here would tell the client it may read a scope whose epoch
                // belongs to the *previous* login, which is what the generation
                // exists to prevent. The plan ends with the code instead, and
                // reads stay refused until a login write does succeed.
                plan_state.phase = LoginPhase::Done;
                return Some(SelectedAction {
                    action: actions::sequence(vec![
                        Action::Emit {
                            event: login_write_outcome(written, &acct),
                        },
                        actions::wait_short(),
                    ]),
                    frame: frame.clone(),
                });
            }

            let stored_keys = get_stored_keys(&db, session_id, &acct);
            drop(db);
            if !needs_extraction_for_process(&stored_keys, &acct, pid) {
                schedule_session_key_check_after_login(session_id.to_string(), acct.clone(), pid);
                plan_state.phase = LoginPhase::Done;
                return Some(SelectedAction {
                    action: actions::sequence(vec![
                        Action::Emit {
                            event: login_write_outcome(written, &acct),
                        },
                        actions::wait_short(),
                    ]),
                    frame: frame.clone(),
                });
            }
        }

        plan_state.phase = LoginPhase::ExtractingKeys;
        return Some(SelectedAction {
            action: actions::sequence(vec![
                Action::Emit {
                    event: SubscriptionEvent {
                        event_type: "status".to_string(),
                        data: [(
                            "message".to_string(),
                            serde_json::Value::String("Getting your WeChat messages...".to_string()),
                        )].into_iter().collect(),
                    },
                },
                actions::wait_short(),
            ]),
            frame: frame.clone(),
        });
    }

    plan_state.detect_retries += 1;
    if plan_state.detect_retries >= 10 {
        plan_state.phase = LoginPhase::Done;
        return Some(SelectedAction {
            action: actions::sequence(vec![
                Action::Emit {
                    event: SubscriptionEvent {
                        event_type: "login_success".to_string(),
                        data: std::collections::HashMap::new(),
                    },
                },
                actions::wait_short(),
            ]),
            frame: frame.clone(),
        });
    }

    Some(SelectedAction { action: actions::wait(2000), frame: None })
}

/// What a finished login write is allowed to tell the client.
///
/// A recorded write is a login. A write that was refused is not: the account is
/// on screen but the generation that names its epoch was never advanced, so the
/// only honest terminal event is the code of the failure. The code is content
/// free, and it is what an operator needs to see — a login that cannot be
/// recorded is not something a retry silently fixes.
fn login_write_outcome(
    written: Result<u64, crate::db::queries::SessionWriteError>,
    account_dir: &str,
) -> SubscriptionEvent {
    match written {
        Ok(_) => SubscriptionEvent {
            event_type: "login_success".to_string(),
            data: [(
                "userId".to_string(),
                serde_json::Value::String(account_dir.to_string()),
            )]
            .into_iter()
            .collect(),
        },
        Err(error) => {
            tracing::error!("[login] the login was not recorded: {}", error.code());
            SubscriptionEvent {
                event_type: "error".to_string(),
                data: [(
                    "message".to_string(),
                    serde_json::Value::String(error.code().to_string()),
                )]
                .into_iter()
                .collect(),
            }
        }
    }
}

async fn handle_extracting_keys(
    plan_state: &mut LoginPlanState,
    session_id: &str,
    frame: Option<FrameHint>,
) -> Option<SelectedAction> {
    let wechat_pid: Option<i64> = {
        let db = get_db();
        db.query_row(
            "SELECT wechat_pid FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .ok()
        .flatten()
        .or_else(|| find_wechat_pid())
    };

    if let (Some(pid), Some(acct)) = (wechat_pid, plan_state.account_dir.clone()) {
        schedule_key_refresh(session_id.to_string(), acct, pid);
    }

    plan_state.phase = LoginPhase::Done;
    Some(SelectedAction {
        action: actions::sequence(vec![
            Action::Emit {
                event: SubscriptionEvent {
                    event_type: "login_success".to_string(),
                    data: plan_state.account_dir.as_ref()
                        .map(|a| [(
                            "userId".to_string(),
                            serde_json::Value::String(a.clone()),
                        )].into_iter().collect())
                        .unwrap_or_default(),
                },
            },
            actions::wait_short(),
        ]),
        frame: frame.clone(),
    })
}

#[cfg(test)]
mod login_write_tests {
    use super::login_write_outcome;
    use crate::db::queries::SessionWriteError;

    fn message(event: &crate::ia::types::SubscriptionEvent) -> Option<&str> {
        event.data.get("message").and_then(|v| v.as_str())
    }

    #[test]
    fn a_recorded_write_is_reported_as_the_login_it_is() {
        let event = login_write_outcome(Ok(7), "wxid_account_dir");
        assert_eq!(event.event_type, "login_success");
        assert_eq!(
            event.data.get("userId").and_then(|v| v.as_str()),
            Some("wxid_account_dir")
        );
    }

    #[test]
    fn a_write_that_was_not_recorded_is_never_reported_as_a_login() {
        for error in [
            SessionWriteError::NoSession,
            SessionWriteError::GenerationExhausted,
            SessionWriteError::Failed,
        ] {
            let event = login_write_outcome(Err(error), "wxid_account_dir");
            assert_eq!(
                event.event_type, "error",
                "{} must end the plan, not claim a login",
                error.code()
            );
            assert_eq!(
                message(&event),
                Some(error.code()),
                "the code is what the client is told, and nothing else"
            );
            assert!(
                !event.data.contains_key("userId"),
                "a refused write must not hand out the account as a readable scope"
            );
        }
    }

    #[test]
    fn a_refused_login_never_carries_the_account_directory() {
        let event = login_write_outcome(Err(SessionWriteError::Failed), "wxid_secret_dir");
        let rendered = format!("{:?}", event.data);
        assert!(!rendered.contains("wxid_secret_dir"), "{rendered}");
    }
}
