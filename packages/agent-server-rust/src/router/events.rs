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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use futures::StreamExt;
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    #[tokio::test]
    async fn pending_socket_send_is_cancelled_by_shutdown() {
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(send_until_shutdown(
            std::future::pending::<Result<(), ()>>(),
            task_shutdown,
        ));
        shutdown.cancel();
        assert!(!tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap());
    }

    #[tokio::test]
    async fn websocket_broadcasts_json_and_disconnect_only_ends_that_subscription() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/events", get(events_ws)))
                .await
                .unwrap();
        });
        let url = format!("ws://{address}/events");
        let (mut first, _) = connect_async(&url).await.unwrap();
        let (mut second, _) = connect_async(&url).await.unwrap();
        first.close(None).await.unwrap();
        event_hub().invalidate();
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), second.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message, Message::Text(r#"{"type":"data_changed"}"#.into()));
        second.close(None).await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn lagged_websocket_gets_one_local_hint_without_rebroadcast() {
        let hub = crate::tools::wechat_events::EventHub::with_capacity(1);
        let mut observer = hub.subscribe();
        let route_hub = hub.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let route = get(move |ws: WebSocketUpgrade| async move {
                events_ws_with(ws, route_hub.clone(), event_shutdown_token())
            });
            axum::serve(listener, Router::new().route("/events", route))
                .await
                .unwrap();
        });
        let (mut socket, _) = connect_async(format!("ws://{address}/events")).await.unwrap();
        for _ in 0..3 {
            hub.invalidate();
            assert_eq!(observer.recv().await.unwrap(), ChangeEvent::data_changed());
        }

        let message = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message, Message::Text(r#"{"type":"data_changed"}"#.into()));
        assert!(observer.try_recv().is_err(), "the handler must not rebroadcast its lag hint");
        socket.close(None).await.unwrap();
        server.abort();
    }
}
