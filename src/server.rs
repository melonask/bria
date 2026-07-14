use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};

#[cfg(feature = "server")]
use crate::config;
use crate::context::Job;
use crate::error::Error;

#[cfg(feature = "server")]
use {
    crate::util::{cancel_signal_ttl, prune_expired_cancel_signals},
    axum::extract::ws::{self, WebSocketUpgrade},
    axum::{
        Json, Router,
        body::Bytes,
        extract::{DefaultBodyLimit, OriginalUri, Path, Request, State},
        http::{HeaderMap, StatusCode},
        middleware::{self, Next},
        response::{IntoResponse, Response},
        routing::{delete, get, post},
    },
    hmac::{Hmac, KeyInit, Mac},
    sha2::Sha256,
};

/// Shared server state.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<crate::config::Config>,
    /// Channels for each source, keyed by source id.
    pub source_txs: Arc<HashMap<String, mpsc::UnboundedSender<Job>>>,
    /// Broadcast channel for stream sink.
    pub broadcast_tx: Option<broadcast::Sender<serde_json::Value>>,
    /// In-memory cancellation signals, keyed by job id.
    pub cancel_signals: Arc<DashMap<String, Instant>>,
    /// Pipeline pause state, keyed by pipeline id.
    pub pipeline_pauses: Arc<DashMap<String, Arc<crate::pipeline::PipelinePause>>>,
}

/// Result of starting the server: the join handle and the shared app state.
pub struct ServerHandle {
    pub join_handle: Option<tokio::task::JoinHandle<()>>,
    pub cancel_signals: Arc<DashMap<String, Instant>>,
    pub pipeline_pauses: Arc<DashMap<String, Arc<crate::pipeline::PipelinePause>>>,
}

/// Start the HTTP server if enabled.
pub async fn start_server(
    config: Arc<crate::config::Config>,
    _source_txs: HashMap<String, mpsc::UnboundedSender<Job>>,
    _broadcast_tx: Option<broadcast::Sender<serde_json::Value>>,
    _shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> crate::error::Result<ServerHandle> {
    let server_enabled = config.server.enabled;

    if !server_enabled {
        tracing::info!("Server is disabled");
        return Ok(ServerHandle {
            join_handle: None,
            cancel_signals: Arc::new(DashMap::new()),
            pipeline_pauses: Arc::new(DashMap::new()),
        });
    }

    #[cfg(not(feature = "server"))]
    {
        Err(Error::Unsupported(
            "Server requires the 'server' feature".to_string(),
        ))
    }

    #[cfg(feature = "server")]
    {
        start_server_inner(config, _source_txs, _broadcast_tx, _shutdown_rx).await
    }
}

#[cfg(feature = "server")]
async fn start_server_inner(
    config: Arc<crate::config::Config>,
    source_txs: HashMap<String, mpsc::UnboundedSender<Job>>,
    broadcast_tx: Option<broadcast::Sender<serde_json::Value>>,
    shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> crate::error::Result<ServerHandle> {
    let server_cfg = &config.server;

    let state = AppState {
        config: config.clone(),
        source_txs: Arc::new(source_txs),
        broadcast_tx,
        cancel_signals: Arc::new(DashMap::new()),
        pipeline_pauses: Arc::new(DashMap::new()),
    };

    let app = build_router(state.clone(), server_cfg);

    let bind_addr = format!("{}:{}", server_cfg.bind, server_cfg.port);
    tracing::info!("Starting server on {bind_addr}");

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .map_err(|e| Error::Server(format!("Cannot bind to {bind_addr}: {e}")))?;

    let cancel_signals_ret = state.cancel_signals.clone();
    let pipeline_pauses_ret = state.pipeline_pauses.clone();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                if let Some(mut rx) = shutdown_rx {
                    let _ = rx.changed().await;
                } else {
                    // Wait for Ctrl+C
                    let _ = tokio::signal::ctrl_c().await;
                }
                tracing::info!("Server shutting down gracefully...");
            })
            .await
            .unwrap_or_else(|e| {
                tracing::error!("Server error: {e}");
            });
    });

    Ok(ServerHandle {
        join_handle: Some(handle),
        cancel_signals: cancel_signals_ret,
        pipeline_pauses: pipeline_pauses_ret,
    })
}

#[cfg(feature = "server")]
/// Build the Axum router.
fn build_router(state: AppState, server_cfg: &config::ServerConfig) -> Router {
    let prefix = server_cfg.prefix.clone();
    let api_key = server_cfg.api_key.clone();

    let mut router = Router::new()
        // Health check
        .route(&format!("/{prefix}/ping"), get(ping_handler))
        // Pipeline resume (for failure action "stop")
        .route(
            &format!("/{prefix}/pipelines/{{pipeline_id}}/resume"),
            post(resume_pipeline_handler),
        );

    // Add explicit HTTP/webhook source routes. Axum rejects overlapping catch-all routes,
    // so each configured source path gets concrete submit and cancel endpoints.
    for source in &state.config.sources {
        if source.enabled
            && (source.r#type == config::SourceType::Http
                || source.r#type == config::SourceType::Webhook)
        {
            let source_path = source.path.to_string_lossy();
            let source_path = source_path.trim_matches('/');
            if !source_path.is_empty() {
                router = router
                    .route(
                        &format!("/{prefix}/{source_path}"),
                        post(submit_job_handler),
                    )
                    .route(
                        &format!("/{prefix}/{source_path}/{{job_id}}"),
                        delete(cancel_job_handler),
                    );
            }
        }
    }

    // Add stream routes if any stream sinks are configured
    for sink in &state.config.sinks {
        if sink.enabled && sink.r#type == config::SinkType::Stream && !sink.sse.is_empty() {
            router = router.route(&format!("/{prefix}/{}", sink.sse), get(sse_handler));
        }
    }

    // Add WebSocket route at /{prefix}/{ws_path} — matches any stream sink's websocket path
    // The handler looks up the sink by path to get ws_heartbeat_secs.
    {
        let has_ws =
            state.config.sinks.iter().any(|s| {
                s.enabled && s.r#type == config::SinkType::Stream && !s.websocket.is_empty()
            });
        if has_ws {
            router = router.route(&format!("/{prefix}/{{ws_path}}"), get(ws_handler));
        }
    }

    // Dashboard static file serving when server.dashboard is non-empty
    if !server_cfg.dashboard.is_empty() {
        let dashboard_dir = server_cfg.dashboard.clone();
        router = router.nest_service(
            &format!("/{prefix}/dashboard"),
            tower_http::services::ServeDir::new(dashboard_dir),
        );
    }

    // Auth middleware: enforce on ALL routes (ping, source, SSE, WS) when api_key is non-empty
    if !api_key.is_empty() {
        router = router.layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    }

    // Global body size limit (generous server-wide cap; per-source limits checked in handler)
    router = router.layer(DefaultBodyLimit::max(server_cfg.max_body_bytes));

    // CORS (outermost, so preflight OPTIONS bypass auth)
    let cors = tower_http::cors::CorsLayer::permissive();
    router = router.layer(cors);

    router.with_state(state)
}

// ─────────────────────────────────────────────────────────────────────────────
// Auth middleware
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "server")]
/// Enforce API key authentication on all routes when configured.
async fn auth_middleware(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let api_key = &state.config.server.api_key;
    if api_key.is_empty() {
        return next.run(request).await;
    }

    let headers = request.headers();

    // Check X-Bria-Api-Key header
    if let Some(key) = headers.get("x-bria-api-key").and_then(|v| v.to_str().ok())
        && constant_time_eq(key.trim().as_bytes(), api_key.as_bytes())
    {
        return next.run(request).await;
    }

    // Check Authorization: Bearer <key>
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok())
        && let Some(token) = auth.strip_prefix("Bearer ")
        && constant_time_eq(token.trim().as_bytes(), api_key.as_bytes())
    {
        return next.run(request).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        "Unauthorized: invalid or missing API key",
    )
        .into_response()
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

/// GET /{prefix}/ping — health check.
#[cfg(feature = "server")]
async fn ping_handler() -> &'static str {
    "pong"
}

/// POST /{prefix}/{source_path} — submit a job to an HTTP/webhook source.
#[cfg(feature = "server")]
async fn submit_job_handler(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    let source_path = source_path_from_uri_path(uri.path(), &state.config.server.prefix)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("No HTTP/webhook source found for path: {}", uri.path()),
            )
        })?;

    // Find the source matching this path
    let source = find_http_source_by_path(&state.config, &source_path).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No HTTP/webhook source found for path: {source_path}"),
        )
    })?;

    // ── HMAC verification for webhook sources ────────────────────────────
    if source.r#type == config::SourceType::Webhook && !source.hmac_secret.is_empty() {
        let sig_header = if source.hmac_header.is_empty() {
            "X-Bria-Signature"
        } else {
            &source.hmac_header
        };

        let raw_sig = headers
            .get(sig_header)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Missing HMAC signature header: {sig_header}"),
                )
            })?;

        // Accept either raw hex or "sha256=" prefix (e.g. GitHub-style)
        let raw_sig = raw_sig.trim();
        let expected_hex = raw_sig.strip_prefix("sha256=").unwrap_or(raw_sig);

        let mut mac =
            Hmac::<Sha256>::new_from_slice(source.hmac_secret.as_bytes()).map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "HMAC key error".to_string(),
                )
            })?;

        mac.update(&body);

        // Constant-time comparison of hex-encoded HMAC
        let computed_hex = hex::encode(mac.finalize().into_bytes());
        if !constant_time_eq(computed_hex.as_bytes(), expected_hex.as_bytes()) {
            return Err((
                StatusCode::UNAUTHORIZED,
                "HMAC signature mismatch".to_string(),
            ));
        }
    }

    // Check max body size against source-specific limit BEFORE JSON parsing
    let max_bytes = if source.max_body_bytes > 0 {
        source.max_body_bytes
    } else {
        state.config.global.max_payload_bytes
    };

    if body.len() > max_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "Payload exceeds max_body_bytes limit of {max_bytes} bytes (received {} bytes)",
                body.len()
            ),
        ));
    }

    // Parse JSON
    let payload: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")))?;

    let correlation_key = submission_correlation_key(&headers)?;

    // Create the durable job identity before enqueueing it. The worker persists
    // that identity with its lifecycle state as soon as it is accepted.
    let job_id = if source.id_field.is_empty() {
        ulid::Ulid::r#gen().to_string()
    } else {
        payload
            .get(&source.id_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| ulid::Ulid::r#gen().to_string())
    };

    let job = Job {
        id: job_id.clone(),
        source: source.id.clone(),
        payload,
        correlation_key: correlation_key.clone(),
        labels: source.labels.clone(),
    };

    // Send to source channel
    let tx = state.source_txs.get(&source.id).ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Source channel not found".to_string(),
        )
    })?;

    tx.send(job).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to submit job: {e}"),
        )
    })?;

    // Determine response status: webhook returns ack_status, HTTP returns 201 Created
    let status = if source.r#type == config::SourceType::Webhook {
        StatusCode::from_u16(source.ack_status).unwrap_or(StatusCode::ACCEPTED)
    } else {
        StatusCode::CREATED
    };

    Ok((
        status,
        Json(serde_json::json!({
            "status": "accepted",
            "job_id": job_id,
            "correlation_key": correlation_key,
        })),
    ))
}

/// DELETE /{prefix}/{source_path}/{id} — cancel a job.
/// Bria is stateless, so we record a cancellation signal and acknowledge the request.
#[cfg(feature = "server")]
async fn cancel_job_handler(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Path(job_id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    let cancel_path = source_path_from_uri_path(uri.path(), &state.config.server.prefix)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!(
                    "Invalid cancel path: expected {{source_path}}/{{id}}, got '{}'",
                    uri.path()
                ),
            )
        })?;
    let source_path = cancel_path
        .strip_suffix(&format!("/{job_id}"))
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!(
                    "Invalid cancel path: expected {{source_path}}/{{id}}, got '{cancel_path}'"
                ),
            )
        })?;
    if source_path.is_empty() || job_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid cancel path: expected {{source_path}}/{{id}}, got '{cancel_path}'"),
        ));
    }

    // Verify the source exists
    let source = find_http_source_by_path(&state.config, source_path).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No HTTP/webhook source found for path: {source_path}"),
        )
    })?;

    // Record cancellation signal
    prune_expired_cancel_signals(&state.cancel_signals, cancel_signal_ttl(&state.config));
    state.cancel_signals.insert(job_id.clone(), Instant::now());

    tracing::info!(
        "Cancellation requested for job '{}' on source '{}' (path: {source_path})",
        job_id,
        source.id
    );

    // Publish cancellation event to broadcast channel if available
    if let Some(ref tx) = state.broadcast_tx {
        let msg = serde_json::json!({
            "type": "cancellation_requested",
            "job_id": job_id,
            "source_id": source.id,
            "source_path": source_path,
        });
        let _ = tx.send(msg);
    }

    // Also try to send a cancellation message to the source channel
    // (the pipeline worker can check cancel_signals to skip processing)
    if let Some(tx) = state.source_txs.get(&source.id) {
        // Construct a synthetic cancellation job with a magic marker
        let cancel_job = Job {
            id: format!("__cancel__{}", job_id),
            source: source.id.clone(),
            payload: serde_json::json!({
                "__bria_cancel": true,
                "target_job_id": job_id,
            }),
            correlation_key: None,
            labels: source.labels.clone(),
        };
        let _ = tx.send(cancel_job);
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "status": "cancellation_requested",
            "job_id": job_id,
        })),
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// SSE / WebSocket handlers
// ─────────────────────────────────────────────────────────────────────────────

/// GET /{prefix}/sse — Server-Sent Events stream of results.
#[cfg(feature = "server")]
async fn sse_handler(
    State(state): State<AppState>,
) -> axum::response::Sse<
    impl tokio_stream::Stream<
        Item = std::result::Result<axum::response::sse::Event, std::convert::Infallible>,
    >,
> {
    use std::pin::Pin;

    let stream: Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = std::result::Result<
                        axum::response::sse::Event,
                        std::convert::Infallible,
                    >,
                > + Send,
        >,
    > = if let Some(ref tx) = state.broadcast_tx {
        let mut rx = tx.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(value) => {
                        let data = value.to_string();
                        yield Ok(axum::response::sse::Event::default().data(data));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    } else {
        Box::pin(async_stream::stream! {
            yield Ok(axum::response::sse::Event::default().data("{\"error\":\"stream not configured\"}"));
        })
    };

    let keepalive_secs = state
        .config
        .sinks
        .iter()
        .find(|s| s.r#type == config::SinkType::Stream && !s.sse.is_empty())
        .map(|s| s.sse_keepalive_secs.max(1))
        .unwrap_or(5);

    axum::response::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(keepalive_secs))
            .text("keepalive"),
    )
}

/// GET /{prefix}/ws — WebSocket stream of results.
#[cfg(feature = "server")]
async fn ws_handler(
    State(state): State<AppState>,
    Path(ws_path): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let heartbeat = state
        .config
        .sinks
        .iter()
        .find(|s| s.r#type == config::SinkType::Stream && s.websocket == ws_path)
        .map(|s| s.ws_heartbeat_secs)
        .unwrap_or(30);

    let broadcast_tx = state.broadcast_tx.clone();
    ws.on_upgrade(move |socket| handle_ws_socket(socket, broadcast_tx, heartbeat))
}

/// Handle an upgraded WebSocket connection: send broadcast events as text and
/// periodic ping frames. Close gracefully when the broadcast channel is gone.
#[cfg(feature = "server")]
async fn handle_ws_socket(
    mut socket: ws::WebSocket,
    broadcast_tx: Option<broadcast::Sender<serde_json::Value>>,
    heartbeat_secs: u64,
) {
    let Some(tx) = broadcast_tx else {
        let frame = ws::CloseFrame {
            code: ws::close_code::NORMAL,
            reason: ws::Utf8Bytes::from_static("stream not configured: no broadcast channel"),
        };
        let _ = socket.send(ws::Message::Close(Some(frame))).await;
        return;
    };

    let mut rx = tx.subscribe();
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(value) => {
                        let text = value.to_string();
                        if socket.send(ws::Message::Text(ws::Utf8Bytes::from(text))).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let frame = ws::CloseFrame {
                            code: ws::close_code::NORMAL,
                            reason: ws::Utf8Bytes::from_static("stream ended"),
                        };
                        let _ = socket.send(ws::Message::Close(Some(frame))).await;
                        break;
                    }
                }
            }
            _ = heartbeat.tick() => {
                // Use an empty ping frame
                let empty: Vec<u8> = Vec::new();
                if socket.send(ws::Message::Ping(empty.into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(ws::Message::Close(_))) | None => break,
                    Some(Ok(_)) => {} // ignore other client messages
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Find an HTTP or Webhook source by its URL path component.
#[cfg(feature = "server")]
fn find_http_source_by_path<'a>(
    config: &'a crate::config::Config,
    path: &str,
) -> Option<&'a config::SourceConfig> {
    config.sources.iter().find(|s| {
        (s.r#type == config::SourceType::Http || s.r#type == config::SourceType::Webhook)
            && s.path.to_string_lossy() == path
    })
}

#[cfg(feature = "server")]
fn source_path_from_uri_path(path: &str, prefix: &str) -> Option<String> {
    let prefix = format!("/{}", prefix.trim_matches('/'));
    path.strip_prefix(&prefix)
        .and_then(|path| path.strip_prefix('/'))
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
}

/// Extract the caller's opaque correlation key without assigning any gateway
/// policy to Bria. Artur may provide either standard `Idempotency-Key` or
/// `X-Correlation-ID`; if both are present they must identify the same request.
#[cfg(feature = "server")]
fn submission_correlation_key(headers: &HeaderMap) -> Result<Option<String>, (StatusCode, String)> {
    const MAX_CORRELATION_KEY_BYTES: usize = 512;

    let idempotency_key = header_correlation_key(headers, "idempotency-key")?;
    let correlation_id = header_correlation_key(headers, "x-correlation-id")?;
    match (idempotency_key, correlation_id) {
        (Some(idempotency_key), Some(correlation_id)) if idempotency_key != correlation_id => {
            Err((
                StatusCode::BAD_REQUEST,
                "Idempotency-Key and X-Correlation-ID must match when both are supplied"
                    .to_string(),
            ))
        }
        (Some(key), _) | (_, Some(key)) => {
            if key.len() > MAX_CORRELATION_KEY_BYTES {
                Err((
                    StatusCode::BAD_REQUEST,
                    format!("correlation key exceeds {MAX_CORRELATION_KEY_BYTES} bytes"),
                ))
            } else {
                Ok(Some(key))
            }
        }
        (None, None) => Ok(None),
    }
}

#[cfg(feature = "server")]
fn header_correlation_key(
    headers: &HeaderMap,
    name: &str,
) -> Result<Option<String>, (StatusCode, String)> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("{name} must contain valid visible ASCII text"),
        )
    })?;
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{name} must be a non-empty visible ASCII value"),
        ));
    }
    Ok(Some(value.to_string()))
}

/// POST /{prefix}/pipelines/{pipeline_id}/resume — resume a stopped pipeline.
#[cfg(feature = "server")]
async fn resume_pipeline_handler(
    State(state): State<AppState>,
    Path(pipeline_id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    // Verify the pipeline exists
    let pipeline_exists = state.config.pipelines.iter().any(|p| p.id == pipeline_id);
    if !pipeline_exists {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Pipeline '{pipeline_id}' not found"),
        ));
    }

    // Record the request before waking waiters so a resume sent just before a
    // worker starts waiting is retained.
    let pause = state
        .pipeline_pauses
        .entry(pipeline_id.clone())
        .or_insert_with(|| Arc::new(crate::pipeline::PipelinePause::new()))
        .clone();
    pause.resume();

    tracing::info!("Pipeline '{}' resumed by operator", pipeline_id);

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "resumed",
            "pipeline_id": pipeline_id,
        })),
    ))
}

/// Constant-time byte comparison to avoid timing side-channels on HMAC.
#[cfg(feature = "server")]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
