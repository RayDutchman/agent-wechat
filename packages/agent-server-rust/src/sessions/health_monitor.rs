use std::{path::PathBuf, sync::atomic::{AtomicBool, Ordering}};
use crate::sessions::manager::get_session;
use crate::tools::wechat_db::{find_account_dir, find_wechat_pid};

/// How often to run the health scan (in seconds).
const SCAN_INTERVAL_SECS: u64 = 1;

/// Delay before restarting WeChat after a crash (in seconds).
const RESTART_DELAY_SECS: u64 = 3;

/// If WeChat crashes this many times within RAPID_WINDOW_SECS, back off.
const MAX_RAPID_RESTARTS: u32 = 5;
const RAPID_WINDOW_SECS: u64 = 60;
const BACKOFF_DELAY_SECS: u64 = 30;

/// Global flag to pause health monitoring during active execution loops.
static MONITORING_PAUSED: AtomicBool = AtomicBool::new(false);

/// Pause health monitoring (call when an execution loop starts).
pub fn pause_monitoring() {
    MONITORING_PAUSED.store(true, Ordering::Relaxed);
}

/// Resume health monitoring (call when an execution loop ends).
pub fn resume_monitoring() {
    MONITORING_PAUSED.store(false, Ordering::Relaxed);
}

fn process_should_restart(process_present: bool, previously_running: bool) -> bool {
    !process_present && previously_running
}

/// Spawn WeChat process for the given session using the shared launch script.
fn spawn_wechat(session: &crate::ia::types::Session) {
    // Use DBUS_SESSION_BUS_ADDRESS from our own environment (inherited from
    // entrypoint.sh) rather than the DB value. The entrypoint's D-Bus session
    // is the one AT-SPI is connected to, so WeChat must use it for a11y to work.
    let result = std::process::Command::new("/opt/tools/launch-wechat")
        .env("DISPLAY", &session.display)
        .env("WECHAT_HOME", format!("/home/{}", session.linux_user))
        .env("WECHAT_USER", &session.linux_user)
        .spawn();

    match result {
        Ok(_) => tracing::info!("[health] Spawned WeChat for session '{}'", session.name),
        Err(e) => tracing::error!("[health] Failed to spawn WeChat: {}", e),
    }
}

/// Spawn the background health monitor task.
///
/// Checks the default session's WeChat process. A live but unresponsive process
/// is left alone; a process that disappears is restarted with bounded backoff.
pub fn spawn_health_monitor() {
    tokio::spawn(async move {
        tracing::info!("[health] WeChat health monitor started");

        let mut was_running = false;
        let mut restart_count: u32 = 0;
        let mut window_start = tokio::time::Instant::now();
        let mut waiting_restart_since: Option<tokio::time::Instant> = None;

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(SCAN_INTERVAL_SECS)).await;

            // Skip if monitoring is paused (an execution loop is active)
            if MONITORING_PAUSED.load(Ordering::Relaxed) {
                continue;
            }

            // Only monitor the default session
            let session = match get_session("default") {
                Some(s) if s.status == "running" => s,
                _ => {
                    crate::tools::wechat_events::change_monitor().set_source(None).await;
                    continue;
                }
            };

            // Check if WeChat process is even running
            match find_wechat_pid() {
                Some(pid) => {
                    let source = find_account_dir(pid).and_then(message_data_root);
                    crate::tools::wechat_events::change_monitor().set_source(source).await;
                    if !was_running {
                        tracing::info!("[health] WeChat process found (pid={})", pid);
                        was_running = true;
                        waiting_restart_since = None;
                    }
                }
                None => {
                    crate::tools::wechat_events::change_monitor().set_source(None).await;
                    if process_should_restart(false, was_running) {
                        tracing::warn!(
                            "[health] WeChat process disappeared (likely crashed), restarting"
                        );
                        was_running = false;
                        waiting_restart_since = Some(tokio::time::Instant::now());
                    }

                    // Handle restart with crash loop protection
                    if let Some(since) = waiting_restart_since {
                        // Check crash loop
                        if window_start.elapsed().as_secs() > RAPID_WINDOW_SECS {
                            restart_count = 0;
                            window_start = tokio::time::Instant::now();
                        }

                        let delay = if restart_count >= MAX_RAPID_RESTARTS {
                            if since.elapsed().as_secs() == RESTART_DELAY_SECS {
                                tracing::warn!(
                                    "[health] Crash loop detected ({} restarts in {}s), backing off to {}s",
                                    restart_count, RAPID_WINDOW_SECS, BACKOFF_DELAY_SECS
                                );
                            }
                            BACKOFF_DELAY_SECS
                        } else {
                            RESTART_DELAY_SECS
                        };

                        if since.elapsed().as_secs() >= delay {
                            spawn_wechat(&session);
                            restart_count += 1;
                            waiting_restart_since = None;
                        }
                    }

                    continue;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::process_should_restart;

    #[test]
    fn fake_process_loss_is_the_only_restart_trigger() {
        let previously_running = true;
        assert!(!process_should_restart(true, previously_running));
        assert!(process_should_restart(false, previously_running));
        assert!(!process_should_restart(false, false));
    }
}

/// Resolve only the already-discovered account to its message data root.
/// This observes the same process/account metadata used by the read handlers;
/// it does not probe SQLite or expose the path through the event API.
fn message_data_root(account: String) -> Option<PathBuf> {
    ["/home/wechat/xwechat_files", "/home/wechat/Documents/xwechat_files"]
        .iter()
        .map(|root| PathBuf::from(root).join(&account).join("db_storage"))
        .find(|path| path.is_dir())
}
