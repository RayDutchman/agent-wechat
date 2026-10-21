//! Shared identity and UI-fence primitives used by the single V2 send route.

use crate::tools::exec::ExecOptions;
use crate::tools::wechat_messages::{CollectionCancel, COLLECTION_BUDGET};
use crate::tools::wechat_read_identity::{
    capture_read_identity_for, ReadIdentityError, DEFAULT_SESSION_ID,
};
use crate::plans::send_message::{
    SendFence, FENCE_IDENTITY_UNAVAILABLE, FENCE_SCOPE_CHANGED, FENCE_TARGET_CHANGED,
    SEND_FAIL_CHAT_NOT_OPEN, SEND_FAIL_CHAT_SELECTION_FAILED,
};

// ============================================
// Errors
// ============================================

/// Identity and destination observation failures consumed by the V2 coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendTextError {
    /// The captured account scope changed during the fenced operation.
    ScopeChanged,
    /// No identity could be captured: no session, no process, or an ambiguous
    /// account.
    IdentityUnavailable,
    /// The session is not logged in.
    NotLoggedIn,
    /// The session's last login write did not complete.
    LoginUnverified,
    /// The selected chat could not be read back.
    TargetUnavailable,
    /// The identity round ran out of its wall clock.
    DeadlineExceeded,
}

fn map_identity_error(error: ReadIdentityError) -> SendTextError {
    match error {
        ReadIdentityError::NotLoggedIn => SendTextError::NotLoggedIn,
        ReadIdentityError::LoginUnverified => SendTextError::LoginUnverified,
        ReadIdentityError::DeadlineExceeded => SendTextError::DeadlineExceeded,
        _ => SendTextError::IdentityUnavailable,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendIdentity {
    pub account_id: String,
    /// Opaque read scope; see `ReadIdentity::epoch`.
    pub epoch: String,
}

pub(crate) async fn capture_identity_now() -> Result<SendIdentity, SendTextError> {
    let cancel = CollectionCancel::within(COLLECTION_BUDGET);
    let identity = tokio::task::spawn_blocking(move || {
        capture_read_identity_for(DEFAULT_SESSION_ID, &cancel)
    })
    .await
    .map_err(|_| SendTextError::IdentityUnavailable)?
    .map_err(map_identity_error)?;
    Ok(SendIdentity {
        account_id: identity.account_id().to_string(),
        epoch: identity.epoch().to_string(),
    })
}

pub(crate) async fn selected_chat_now() -> Result<Option<String>, SendTextError> {
    let result = crate::tools::chat_select::current_chat().await;
    if !result.ok {
        return Err(SendTextError::TargetUnavailable);
    }
    Ok(result.selected().map(str::to_string))
}

/// Exec options bound to the session used by the fenced plan.
pub(crate) fn exec_options_for(context: &crate::context::Context) -> ExecOptions {
    ExecOptions {
        session: Some(context.session.clone()),
        timeout_ms: 60_000,
    }
}

/// The production fence: re-captures the identity and reads back the selected
/// chat, comparing them to the values bound by V2.
pub(crate) struct LiveSendFence {
    expected_epoch: String,
    chat_id: String,
    options: ExecOptions,
}

impl LiveSendFence {
    /// Build the production fence for the one unified V2 send operation.
    pub(crate) fn new(expected_epoch: String, chat_id: String, options: ExecOptions) -> Self {
        Self {
            expected_epoch,
            chat_id,
            options,
        }
    }
}

#[async_trait::async_trait]
impl SendFence for LiveSendFence {
    fn exec_options(&self) -> ExecOptions {
        self.options.clone()
    }

    async fn recheck(&self) -> Result<(), &'static str> {
        let identity = match capture_identity_now().await {
            Ok(identity) => identity,
            Err(_) => return Err(FENCE_IDENTITY_UNAVAILABLE),
        };
        if identity.epoch != self.expected_epoch {
            return Err(FENCE_SCOPE_CHANGED);
        }
        match selected_chat_now().await {
            Ok(Some(chat)) if chat == self.chat_id => Ok(()),
            // No selection, a different chat, or a helper that could not answer:
            // fail closed rather than commit to an unverified target.
            _ => Err(FENCE_TARGET_CHANGED),
        }
    }
}

/// Choose a stable code for a plan failure. Raw plan details are never returned.
///
/// The plan's own stuck reasons are matched exactly, against the constants the
/// plan sets them from — not by substring — so no new code is ever inferred from
/// prose. The historical branches below are unchanged: a run that selects no
/// action without naming a reason reports `execution_failed`.
pub(crate) fn execution_error_code(error: Option<&str>) -> &'static str {
    match error {
        Some(reason) if reason == SEND_FAIL_CHAT_NOT_OPEN => SEND_FAIL_CHAT_NOT_OPEN,
        Some(reason) if reason == SEND_FAIL_CHAT_SELECTION_FAILED => SEND_FAIL_CHAT_SELECTION_FAILED,
        Some(message) if message == "Aborted" => "aborted",
        Some(message) if message.contains("timeout") || message.contains("Unknown state") => {
            "timeout"
        }
        _ => "execution_failed",
    }
}
