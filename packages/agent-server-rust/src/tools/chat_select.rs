use super::exec::{exec_command, ExecOptions};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenChatResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Open a chat in the WeChat UI using the chat-select tool.
///
/// Args format: chat-select [--force] [--click-xy X Y] <username>
pub async fn open_chat(
    chat_id: &str,
    force: bool,
    click_xy: Option<(f64, f64)>,
) -> OpenChatResult {
    let mut args: Vec<String> = Vec::new();

    if force {
        args.push("--force".into());
    }

    if let Some((x, y)) = click_xy {
        args.push("--click-xy".into());
        args.push((x as i32).to_string());
        args.push((y as i32).to_string());
    }

    // chat_id is a positional arg — must be last
    args.push(chat_id.into());

    let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let started = Instant::now();
    let result = exec_command(
        "chat-select",
        &args_ref,
        &ExecOptions::default(),
    )
    .await;

    finish_open_chat(result, started.elapsed().as_millis())
}

fn finish_open_chat(result: super::exec::CommandResult, elapsed_ms: u128) -> OpenChatResult {
    // Result JSON is on stdout regardless of exit code
    if let Ok(parsed) = serde_json::from_str::<OpenChatResult>(&result.stdout) {
        if !parsed.ok {
            record_selection_failure(&result, elapsed_ms, true);
        }
        return parsed;
    }

    record_selection_failure(&result, elapsed_ms, false);
    // Fallback: couldn't parse stdout
    OpenChatResult {
        ok: false,
        username: None,
        index: None,
        skipped: None,
        error: Some(if result.stderr.is_empty() {
            format!("chat-select exited with code {}", result.exit_code)
        } else {
            result.stderr
        }),
    }
}

/// Emit only fixed vocabulary and process metadata. Never log helper output,
/// error strings or target arguments, even when the helper's JSON is valid.
fn record_selection_failure(
    result: &super::exec::CommandResult,
    elapsed_ms: u128,
    stdout_parsed: bool,
) {
    use super::exec::CommandOutcome;
    let outcome = match result.outcome {
        CommandOutcome::Exited => "exited",
        CommandOutcome::Signaled => "signaled",
        CommandOutcome::IoFailed => "io_failed",
        CommandOutcome::TimedOut => "timed_out",
    };
    let (stage, reason) = match result.outcome {
        CommandOutcome::TimedOut => ("process", "command_timeout"),
        CommandOutcome::IoFailed => ("process", "process_io_error"),
        CommandOutcome::Signaled => ("process", "process_signaled"),
        CommandOutcome::Exited if !stdout_parsed => ("process", "invalid_stdout"),
        CommandOutcome::Exited => {
            let value = serde_json::from_str::<serde_json::Value>(&result.stdout).ok();
            let diagnostic = value.as_ref().and_then(|v| v.get("diagnostic"));
            let field = |key| diagnostic.and_then(|v| v.get(key)).and_then(|v| v.as_str());
            let stage = match field("stage") {
                Some("startup") => "startup",
                Some("profile") => "profile",
                Some("enumeration") => "enumeration",
                Some("target") => "target",
                Some("ui") => "ui",
                Some("selection") => "selection",
                Some("verification") => "verification",
                Some("lock") => "lock",
                _ => "unknown",
            };
            let reason = match field("reason") {
                Some("not_running") => "not_running",
                Some("build_id_unavailable") => "build_id_unavailable",
                Some("unsupported_build") => "unsupported_build",
                Some("no_sessions") => "no_sessions",
                Some("unsupported_target") => "unsupported_target",
                Some("target_missing") => "target_missing",
                Some("ui_unavailable") => "ui_unavailable",
                Some("click_failed") => "click_failed",
                Some("hook_not_ready") => "hook_not_ready",
                Some("hook_error") => "hook_error",
                Some("selection_unverified") => "selection_unverified",
                Some("pane_unverified") => "pane_unverified",
                Some("vector_unavailable") => "vector_unavailable",
                Some("selection_busy") => "selection_busy",
                Some("helper_exception") => "helper_exception",
                Some("helper_timeout") => "helper_timeout",
                _ => "unknown",
            };
            (stage, reason)
        }
    };
    tracing::warn!(
        stage, reason, outcome, exit_code = result.exit_code,
        elapsed_ms = elapsed_ms as u64, stdout_parsed,
        "chat selection failed"
    );
}

/// One pure read of the currently selected chat.
///
/// `current` is the username the native session manager reports as selected, or
/// absent when nothing is selected. It is read back from the client, so it is not
/// the echo of a requested chat that [`OpenChatResult::username`] can be.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentChatResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CurrentChatResult {
    /// The selected username, treating an empty string as no selection.
    pub fn selected(&self) -> Option<&str> {
        self.current.as_deref().filter(|username| !username.is_empty())
    }
}

/// Read the selected chat without selecting one and without changing the UI.
///
/// This is the read-back a fenced send uses to confirm the open chat before it
/// commits and again before it reports success. It never clicks and never
/// rewrites an argument, so it is safe to call under the shared GUI guard.
pub async fn current_chat() -> CurrentChatResult {
    let result = exec_command("chat-select", &["--current"], &ExecOptions::default()).await;

    if let Some(parsed) = parse_current(&result.stdout) {
        return parsed;
    }

    CurrentChatResult {
        ok: false,
        current: None,
        error: Some(if result.stderr.is_empty() {
            format!("chat-select exited with code {}", result.exit_code)
        } else {
            result.stderr
        }),
    }
}

fn parse_current(stdout: &str) -> Option<CurrentChatResult> {
    serde_json::from_str::<CurrentChatResult>(stdout).ok()
}
