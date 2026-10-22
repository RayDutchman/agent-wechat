use serde::Serialize;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt},
    process::Command,
    sync::{broadcast, Mutex},
    task::JoinHandle,
    time::{sleep, Instant},
};
use tokio_util::sync::CancellationToken;

const COALESCE_WINDOW: Duration = Duration::from_millis(300);
const BROADCAST_CAPACITY: usize = 16;
const HELPER_PATH: &str = "/opt/tools/watch-wechat-data.py";
const RESTART_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ChangeEvent {
    #[serde(rename = "type")]
    kind: ChangeEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChangeEventKind {
    DataChanged,
}

impl ChangeEvent {
    pub fn data_changed() -> Self {
        Self { kind: ChangeEventKind::DataChanged }
    }
}

#[derive(Clone)]
pub struct EventHub {
    sender: broadcast::Sender<ChangeEvent>,
}

impl Default for EventHub {
    fn default() -> Self { Self::with_capacity(BROADCAST_CAPACITY) }
}

impl EventHub {
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> { self.sender.subscribe() }

    pub fn invalidate(&self) {
        let _ = self.sender.send(ChangeEvent::data_changed());
    }
}

static GLOBAL_HUB: OnceLock<EventHub> = OnceLock::new();
static SHUTDOWN: OnceLock<CancellationToken> = OnceLock::new();
static CHANGE_MONITOR: OnceLock<ChangeMonitor> = OnceLock::new();

/// Shared production hub. Creating it does not start background work.
pub fn event_hub() -> &'static EventHub {
    GLOBAL_HUB.get_or_init(EventHub::default)
}

/// The application lifecycle owner can cancel this token during shutdown.
pub fn event_shutdown_token() -> CancellationToken {
    SHUTDOWN.get_or_init(CancellationToken::new).clone()
}

pub fn change_monitor() -> &'static ChangeMonitor {
    CHANGE_MONITOR.get_or_init(ChangeMonitor::new)
}

struct SourceTask {
    path: PathBuf,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// Owns the single data-root helper and its stdout/stderr consumers.
pub struct ChangeMonitor {
    hub: EventHub,
    helper: PathBuf,
    source: Mutex<Option<SourceTask>>,
    child_running: Arc<AtomicBool>,
}

impl ChangeMonitor {
    pub fn new() -> Self {
        Self::with_helper(event_hub().clone(), PathBuf::from(HELPER_PATH))
    }

    fn with_helper(hub: EventHub, helper: PathBuf) -> Self {
        Self {
            hub,
            helper,
            source: Mutex::new(None),
            child_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Replace the trusted source. Replacing it only cancels this source child,
    /// never the process-wide token used by WebSocket subscribers.
    pub async fn set_source(&self, path: Option<PathBuf>) {
        let mut current = self.source.lock().await;
        if current.as_ref().map(|source| &source.path) == path.as_ref() {
            return;
        }
        if let Some(old) = current.take() {
            old.cancel.cancel();
            let _ = old.task.await;
        }
        if let Some(path) = path {
            let cancel = event_shutdown_token().child_token();
            let task = tokio::spawn(run_source(
                self.helper.clone(),
                path.clone(),
                self.hub.clone(),
                cancel.clone(),
                self.child_running.clone(),
            ));
            *current = Some(SourceTask { path, cancel, task });
            self.hub.invalidate();
        }
    }

    pub async fn shutdown(&self) {
        let mut current = self.source.lock().await;
        if let Some(source) = current.take() {
            source.cancel.cancel();
            let _ = source.task.await;
        }
    }

}

impl Default for ChangeMonitor {
    fn default() -> Self {
        Self::new()
    }
}

async fn run_source(
    helper: PathBuf,
    source: PathBuf,
    hub: EventHub,
    cancel: CancellationToken,
    child_running: Arc<AtomicBool>,
) {
    let mut spawned_once = false;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let spawned = Command::new("python3")
            .arg(&helper)
            .arg(&source)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn();
        let mut child = match spawned {
            Ok(child) => child,
            Err(_) => {
                if wait_retry(&cancel).await { break; }
                continue;
            }
        };
        if spawned_once {
            hub.invalidate();
        }
        spawned_once = true;
        child_running.store(true, Ordering::Release);
        let stdout = child.stdout.take().expect("stdout was piped");
        let mut stderr = child.stderr.take().expect("stderr was piped");
        let stderr_task = tokio::spawn(async move {
            tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await
        });
        let producer = tokio::spawn(run_changed_events_with(
            tokio::io::BufReader::new(stdout),
            hub.clone(),
            cancel.clone(),
            COALESCE_WINDOW,
        ));
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = child.kill().await;
            }
            _ = child.wait() => {}
        }
        let _ = child.wait().await;
        let _ = producer.await;
        let _ = stderr_task.await;
        child_running.store(false, Ordering::Release);
        if cancel.is_cancelled() || wait_retry(&cancel).await { break; }
    }
}

async fn wait_retry(cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = sleep(RESTART_DELAY) => false,
    }
}

/// Consume the helper's line stream. Only the exact `changed` line is an input event.
pub async fn run_changed_events<R>(mut lines: R, shutdown: CancellationToken)
where
    R: AsyncBufRead + Unpin,
{
    run_changed_events_with(&mut lines, event_hub().clone(), shutdown, COALESCE_WINDOW).await;
}

async fn run_changed_events_with<R>(
    mut lines: R,
    hub: EventHub,
    shutdown: CancellationToken,
    coalesce_window: Duration,
) where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    let clock_start = Instant::now();
    let mut window = CoalesceWindow::default();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            read = lines.read_line(&mut line) => match read {
                Ok(0) => {
                    if window.flush_pending() {
                        hub.invalidate();
                    }
                    break;
                }
                Err(_) => break,
                Ok(_) => {
                    if line.trim_end() == "changed" {
                        window.invalidate(clock_start.elapsed(), coalesce_window);
                    }
                    line.clear();
                }
            },
            _ = async {
                if let Some(at) = window.deadline() {
                    sleep(at.saturating_sub(clock_start.elapsed())).await
                }
            }, if window.deadline().is_some() => {
                if window.emit_if_due(clock_start.elapsed()) {
                    hub.invalidate();
                }
            }
        }
    }
}

#[derive(Default)]
struct CoalesceWindow { deadline: Option<Duration> }

impl CoalesceWindow {
    fn invalidate(&mut self, now: Duration, coalesce_window: Duration) -> bool {
        if self.deadline.is_some() { false } else {
            self.deadline = Some(now + coalesce_window);
            true
        }
    }
    fn deadline(&self) -> Option<Duration> { self.deadline }
    fn emit_if_due(&mut self, now: Duration) -> bool {
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            self.deadline = None;
            true
        } else { false }
    }

    fn flush_pending(&mut self) -> bool { self.deadline.take().is_some() }
}
