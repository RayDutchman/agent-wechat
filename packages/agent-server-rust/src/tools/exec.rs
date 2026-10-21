use crate::ia::types::Session;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

#[derive(Debug)]
pub struct CommandResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub outcome: CommandOutcome,
}

/// Process evidence, independent of any untrusted stdout/stderr text.
#[derive(Debug, Clone, Copy)]
pub enum CommandOutcome {
    Exited,
    Signaled,
    IoFailed,
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct ExecOptions {
    pub session: Option<Session>,
    pub timeout_ms: u64,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            session: None,
            timeout_ms: 60_000,
        }
    }
}

/// The environment a child command runs with.
///
/// A session, when present, supplies the display, D-Bus address and home the
/// client actually runs under; without one the inherited environment is used and
/// only a missing `DISPLAY` falls back to `:99`.
fn build_env(options: &ExecOptions) -> HashMap<String, String> {
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.insert("QT_ACCESSIBILITY".into(), "1".into());
    env.insert("QT_LINUX_ACCESSIBILITY_ALWAYS_ON".into(), "1".into());

    if let Some(session) = &options.session {
        env.insert("DISPLAY".into(), session.display.clone());
        env.insert(
            "DBUS_SESSION_BUS_ADDRESS".into(),
            session.dbus_address.clone().unwrap_or_default(),
        );
        env.insert("HOME".into(), format!("/home/{}", session.linux_user));
    } else {
        env.entry("DISPLAY".into())
            .or_insert_with(|| ":99".into());
    }
    env
}

/// Execute a command with fixed arguments (no shell interpolation).
///
/// If a session is provided, the command runs with that session's
/// DISPLAY and DBUS_SESSION_BUS_ADDRESS environment.
pub async fn exec_command(
    command: &str,
    args: &[&str],
    options: &ExecOptions,
) -> CommandResult {
    let timeout = std::time::Duration::from_millis(options.timeout_ms);

    let result = tokio::time::timeout(timeout, async {
        let output = Command::new(command)
            .args(args)
            .envs(build_env(options))
            .output()
            .await;

        match output {
            Ok(out) => CommandResult {
                stdout: String::from_utf8_lossy(&out.stdout).trim().to_string(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
                exit_code: out.status.code().unwrap_or(1),
                outcome: if out.status.code().is_some() {
                    CommandOutcome::Exited
                } else {
                    CommandOutcome::Signaled
                },
            },
            Err(err) => CommandResult {
                stdout: String::new(),
                stderr: err.to_string(),
                exit_code: 1,
                outcome: CommandOutcome::IoFailed,
            },
        }
    })
    .await;

    match result {
        Ok(r) => r,
        Err(_) => CommandResult {
            stdout: String::new(),
            stderr: "Command timed out".to_string(),
            exit_code: 1,
            outcome: CommandOutcome::TimedOut,
        },
    }
}

/// The outcome of a command that ran under an explicit deadline.
#[derive(Debug, Clone)]
pub struct ControlledResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    /// The deadline expired while the command was still running.
    pub timed_out: bool,
}

impl ControlledResult {
    /// Whether the command exited zero within its deadline.
    pub fn succeeded(&self) -> bool {
        self.exit_code == 0 && !self.timed_out
    }
}

/// Run a command with controlled termination.
///
/// Unlike [`exec_command`], the direct child is killed when the deadline passes
/// or when this future is dropped (`kill_on_drop`), so a caller that is itself
/// cancelled does not leave the command running.
///
/// What this does **not** do: it does not signal a process tree. A wrapper that
/// has already started a descendant — for example a clipboard owner that must
/// keep serving a selection — keeps that descendant alive. Terminating the
/// direct child is therefore not a claim that every process the wrapper started
/// is gone; callers that need that guarantee must handle the descendant
/// themselves.
pub async fn exec_command_controlled(
    command: &str,
    args: &[&str],
    options: &ExecOptions,
    deadline: Duration,
) -> ControlledResult {
    let mut cmd = Command::new(command);
    cmd.args(args)
        .envs(build_env(options))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    match tokio::time::timeout(deadline, cmd.output()).await {
        Ok(Ok(out)) => ControlledResult {
            stdout: String::from_utf8_lossy(&out.stdout).trim().to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            exit_code: out.status.code().unwrap_or(1),
            timed_out: false,
        },
        Ok(Err(err)) => ControlledResult {
            stdout: String::new(),
            stderr: err.to_string(),
            exit_code: 1,
            timed_out: false,
        },
        Err(_) => ControlledResult {
            stdout: String::new(),
            stderr: "Command timed out".to_string(),
            exit_code: 1,
            timed_out: true,
        },
    }
}

/// Credentials-only variant: isolate the helper in a process group so dropping
/// or timing out the request also terminates descendants without changing the
/// clipboard/voice semantics of `exec_command_controlled`.
#[cfg(unix)]
pub async fn exec_command_controlled_group(
    command: &str,
    args: &[&str],
    options: &ExecOptions,
    deadline: Duration,
) -> ControlledResult {
    use tokio::io::AsyncReadExt;

    struct GroupGuard(i32);
    impl Drop for GroupGuard {
        fn drop(&mut self) {
            unsafe { let _ = kill(-self.0, 9); }
        }
    }
    extern "C" { fn kill(pid: i32, signal: i32) -> i32; }

    let mut cmd = Command::new(command);
    cmd.args(args)
        .envs(build_env(options))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let Ok(mut child) = cmd.spawn() else {
        return ControlledResult { stdout: String::new(), stderr: "Failed to spawn helper".into(), exit_code: 1, timed_out: false };
    };
    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return ControlledResult { stdout: String::new(), stderr: "Helper has no process id".into(), exit_code: 1, timed_out: false };
    };
    let _group = GroupGuard(pid);
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let read_stdout = async { let mut bytes = Vec::new(); stdout.read_to_end(&mut bytes).await.map(|_| bytes) };
    let read_stderr = async { let mut bytes = Vec::new(); stderr.read_to_end(&mut bytes).await.map(|_| bytes) };
    let work = async {
        let (status, out, err) = tokio::join!(child.wait(), read_stdout, read_stderr);
        (status, out, err)
    };
    match tokio::time::timeout(deadline, work).await {
        Ok((Ok(status), Ok(out), Ok(err))) => ControlledResult {
            stdout: String::from_utf8_lossy(&out).trim().to_owned(),
            stderr: String::from_utf8_lossy(&err).trim().to_owned(),
            exit_code: status.code().unwrap_or(1),
            timed_out: false,
        },
        Ok((status, out, err)) => ControlledResult {
            stdout: String::new(),
            stderr: format!("helper wait/read failed: {:?} {:?} {:?}", status.err(), out.err(), err.err()),
            exit_code: 1,
            timed_out: false,
        },
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            ControlledResult { stdout: String::new(), stderr: "Command timed out".into(), exit_code: 1, timed_out: true }
        }
    }
}

#[cfg(test)]
mod controlled_tests {
    use super::*;

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

    fn options() -> ExecOptions {
        ExecOptions {
            session: Some(session(":77")),
            timeout_ms: 1_000,
        }
    }

    /// A command that exits zero within its deadline is a success.
    #[tokio::test]
    async fn a_command_that_exits_zero_is_a_success() {
        let result =
            exec_command_controlled("sh", &["-c", "exit 0"], &options(), Duration::from_secs(5))
                .await;
        assert!(result.succeeded(), "{result:?}");
        assert!(!result.timed_out);
    }

    /// A non-zero exit is a failure, even though the child ran to completion.
    #[tokio::test]
    async fn a_nonzero_exit_is_a_failure() {
        let result =
            exec_command_controlled("sh", &["-c", "exit 7"], &options(), Duration::from_secs(5))
                .await;
        assert!(!result.succeeded());
        assert_eq!(result.exit_code, 7);
        assert!(!result.timed_out);
    }

    /// The session display reaches the child; the compiled-in `:99` is not used
    /// when a session is present.
    #[tokio::test]
    async fn the_child_inherits_the_sessions_display() {
        let result = exec_command_controlled(
            "sh",
            &["-c", "printf %s \"$DISPLAY\""],
            &options(),
            Duration::from_secs(5),
        )
        .await;
        assert!(result.succeeded(), "{result:?}");
        assert_eq!(result.stdout, ":77");
    }

    /// Passing the deadline terminates the direct child, so a follow-up step it
    /// had queued does not run.
    #[tokio::test]
    async fn a_command_past_its_deadline_is_terminated() {
        let marker = std::env::temp_dir().join(format!(
            "agent-wechat-exec-contract-{}",
            uuid::Uuid::new_v4()
        ));
        let script = format!("sleep 5; touch {}", marker.to_string_lossy());

        let result = exec_command_controlled(
            "sh",
            &["-c", &script],
            &options(),
            Duration::from_millis(100),
        )
        .await;

        assert!(result.timed_out, "the deadline must be reported: {result:?}");
        assert!(!result.succeeded());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !marker.exists(),
            "the terminated child must not run its follow-up"
        );
        let _ = std::fs::remove_file(&marker);
    }
}
