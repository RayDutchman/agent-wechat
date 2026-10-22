use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
};
use std::future::Future;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use crate::tools::wechat_events::{event_hub, event_shutdown_token, ChangeEvent};

pub async fn events_ws(ws: WebSocketUpgrade) -> impl IntoResponse {
    events_ws_with(ws, event_hub().clone(), event_shutdown_token())
}

fn events_ws_with(
    ws: WebSocketUpgrade,
    hub: crate::tools::wechat_events::EventHub,
    shutdown: CancellationToken,
) -> impl IntoResponse {
    let events = hub.subscribe();
    ws.on_upgrade(move |socket| handle_events_ws(socket, events, shutdown))
}

async fn handle_events_ws(
    mut socket: WebSocket,
    mut events: broadcast::Receiver<ChangeEvent>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = socket.recv() => match incoming {
                Some(Ok(_)) => continue,
                _ => break,
            },
            received = events.recv() => {
                let event = match received {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(_)) => ChangeEvent::data_changed(),
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let Ok(payload) = serde_json::to_string(&event) else { continue };
                let send = socket.send(axum::extract::ws::Message::Text(payload.into()));
                if !send_until_shutdown(send, shutdown.clone()).await {
                    break;
                }
            }
        }
    }
}

async fn send_until_shutdown<F, E>(send: F, shutdown: CancellationToken) -> bool
where
    F: Future<Output = Result<(), E>>,
{
    tokio::select! {
        _ = shutdown.cancelled() => false,
        result = send => result.is_ok(),
    }
}
