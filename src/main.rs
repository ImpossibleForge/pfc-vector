use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

// ─── Config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Config {
    pfc_jsonl_binary: String,
    api_key: Option<String>,
    vector_dir: Option<PathBuf>,
    rotate_mb: u64,
    rotate_sec: u64,
    prefix: String,
    host: String,
    port: u16,
}

impl Config {
    fn from_env() -> Self {
        Config {
            pfc_jsonl_binary: std::env::var("PFC_JSONL_BINARY")
                .unwrap_or_else(|_| "/usr/local/bin/pfc_jsonl".to_string()),
            api_key: std::env::var("PFC_API_KEY").ok(),
            vector_dir: std::env::var("PFC_VECTOR_DIR").ok().map(PathBuf::from),
            rotate_mb: std::env::var("PFC_VECTOR_ROTATE_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),
            rotate_sec: std::env::var("PFC_VECTOR_ROTATE_SEC")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3600),
            prefix: std::env::var("PFC_VECTOR_PREFIX")
                .unwrap_or_else(|_| "vector".to_string()),
            host: std::env::var("PFC_VECTOR_HOST")
                .unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("PFC_VECTOR_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8766),
        }
    }
}

// ─── State ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct BufferState {
    row_count: u64,
    byte_count: u64,
    last_flush: Instant,
    last_file: String,
}

impl BufferState {
    fn new() -> Self {
        BufferState {
            row_count: 0,
            byte_count: 0,
            last_flush: Instant::now(),
            last_file: String::new(),
        }
    }
}

struct AppState {
    config: Config,
    buffer: Mutex<BufferState>,
}

type Shared = Arc<AppState>;

// ─── Auth ──────────────────────────────────────────────────────────────────

fn authorized(headers: &HeaderMap, cfg: &Config) -> bool {
    match &cfg.api_key {
        None => true,
        Some(key) => headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == key)
            .unwrap_or(false),
    }
}

// ─── Body parsing ───────────────────────────────────────────────────────────

fn parse_rows(body: &[u8]) -> Result<Vec<Value>, String> {
    let text = std::str::from_utf8(body).map_err(|e| format!("UTF-8 error: {e}"))?;
    let stripped = text.trim();

    if stripped.is_empty() {
        return Ok(vec![]);
    }

    // Try JSON (array or object with rows/events)
    if stripped.starts_with('[') || stripped.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<Value>(stripped) {
            let rows = match &v {
                Value::Array(arr) => arr.clone(),
                Value::Object(obj) => {
                    if let Some(Value::Array(arr)) = obj.get("rows") {
                        arr.clone()
                    } else if let Some(Value::Array(arr)) = obj.get("events") {
                        arr.clone()
                    } else if let Some(Value::Array(arr)) = obj.get("logs") {
                        arr.clone()
                    } else {
                        // single object
                        vec![v.clone()]
                    }
                }
                _ => return Err("Unexpected JSON type".to_string()),
            };
            return Ok(rows.into_iter().filter(|r| r.is_object()).collect());
        }
        // If it starts with { but fails JSON parse → try NDJSON below
    }

    // NDJSON fallback
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(v) if v.is_object() => rows.push(v),
            Ok(_) => {} // skip non-object lines
            Err(e) => return Err(format!("NDJSON parse error: {e}")),
        }
    }
    Ok(rows)
}

// ─── Flush logic (two-phase: lock → rename → release → compress) ────────────

async fn flush_buffer(state: &Shared, reason: &str) -> Value {
    let dir = match &state.config.vector_dir {
        Some(d) => d.clone(),
        None => return json!({"flushed": false, "reason": "ingest disabled"}),
    };

    let buffer_path = dir.join(".pfc_buffer.jsonl");
    let tmp_path = dir.join(".pfc_buffer.compressing");

    // ── Phase 1: hold lock only for the rename ──────────────────────────────
    let (rows, bytes) = {
        let mut buf = state.buffer.lock().await;

        if buf.row_count == 0 {
            return json!({"flushed": false, "reason": "empty"});
        }

        // Atomic rename — fast, under lock
        if let Err(e) = tokio::fs::rename(&buffer_path, &tmp_path).await {
            error!("rename buffer→tmp failed: {e}");
            return json!({"flushed": false, "reason": format!("rename error: {e}")});
        }

        let rows = buf.row_count;
        let bytes = buf.byte_count;
        buf.row_count = 0;
        buf.byte_count = 0;
        buf.last_flush = Instant::now();

        (rows, bytes)
    };
    // Lock released here — new rows can be accepted immediately

    // ── Phase 2: compress outside lock ─────────────────────────────────────
    let ts = Utc::now().format("%Y%m%dT%H%M%S");
    let out_name = format!("{}_{}.pfc", state.config.prefix, ts);
    let out_path = dir.join(&out_name);

    info!("Flushing {rows} rows ({bytes} bytes) → {out_name} [reason: {reason}]");

    let result = tokio::process::Command::new(&state.config.pfc_jsonl_binary)
        .arg("compress")
        .arg(&tmp_path)
        .arg(&out_path)
        .output()
        .await;

    match result {
        Ok(output) if output.status.success() => {
            // Remove tmp file on success
            let _ = tokio::fs::remove_file(&tmp_path).await;
            let mut buf = state.buffer.lock().await;
            buf.last_file = out_path.to_string_lossy().to_string();
            info!("Flush complete: {out_name}");
            json!({
                "flushed": true,
                "rows": rows,
                "file": out_path.to_string_lossy()
            })
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("pfc_jsonl compress failed: {stderr}");
            restore_tmp(&tmp_path, &buffer_path).await;
            // Restore row count
            let mut buf = state.buffer.lock().await;
            buf.row_count += rows;
            buf.byte_count += bytes;
            json!({"flushed": false, "reason": format!("compress failed: {stderr}")})
        }
        Err(e) => {
            error!("Failed to spawn pfc_jsonl: {e}");
            restore_tmp(&tmp_path, &buffer_path).await;
            let mut buf = state.buffer.lock().await;
            buf.row_count += rows;
            buf.byte_count += bytes;
            json!({"flushed": false, "reason": format!("spawn error: {e}")})
        }
    }
}

/// Restore tmp file back to buffer after compress failure.
async fn restore_tmp(tmp_path: &PathBuf, buffer_path: &PathBuf) {
    if buffer_path.exists() {
        // New rows arrived while we were compressing — prepend old data
        match tokio::fs::read(tmp_path).await {
            Ok(old_data) => {
                match tokio::fs::read(buffer_path).await {
                    Ok(new_data) => {
                        let mut combined = old_data;
                        combined.extend_from_slice(&new_data);
                        if let Err(e) = tokio::fs::write(buffer_path, &combined).await {
                            error!("Failed to write combined buffer: {e}");
                        } else {
                            let _ = tokio::fs::remove_file(tmp_path).await;
                        }
                    }
                    Err(e) => {
                        error!("Failed to read new buffer for merge: {e}");
                        // Best effort: rename tmp back
                        let _ = tokio::fs::rename(tmp_path, buffer_path).await;
                    }
                }
            }
            Err(e) => error!("Failed to read tmp for restore: {e}"),
        }
    } else {
        // No new rows — simple rename back
        if let Err(e) = tokio::fs::rename(tmp_path, buffer_path).await {
            error!("Failed to restore tmp→buffer: {e}");
        }
    }
}

/// Check size threshold — call while holding the lock!
async fn maybe_flush_by_size(state: &Shared) {
    let row_count = {
        let buf = state.buffer.lock().await;
        buf.row_count
    };
    if row_count == 0 {
        return;
    }

    let dir = match &state.config.vector_dir {
        Some(d) => d.clone(),
        None => return,
    };
    let buffer_path = dir.join(".pfc_buffer.jsonl");
    if let Ok(meta) = tokio::fs::metadata(&buffer_path).await {
        let size_mb = meta.len() / (1024 * 1024);
        if size_mb >= state.config.rotate_mb {
            info!("Size threshold reached ({size_mb} MB >= {}), flushing", state.config.rotate_mb);
            flush_buffer(state, "size").await;
        }
    }
}

// ─── Background watchdog ────────────────────────────────────────────────────

async fn watchdog(state: Shared) {
    info!("Watchdog started (check interval: 60s, rotate_sec: {})", state.config.rotate_sec);
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;

        let age_secs = {
            let buf = state.buffer.lock().await;
            if buf.row_count == 0 {
                continue;
            }
            buf.last_flush.elapsed().as_secs()
        };

        if age_secs >= state.config.rotate_sec {
            info!("Time threshold reached ({age_secs}s >= {}s), flushing", state.config.rotate_sec);
            flush_buffer(&state, "time").await;
        }
    }
}

// ─── Handlers ──────────────────────────────────────────────────────────────

async fn health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "service": "pfc-vector"
    }))
}

async fn ingest(
    State(state): State<Shared>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&headers, &state.config) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
    }

    if state.config.vector_dir.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ingest not enabled — set PFC_VECTOR_DIR"})),
        )
            .into_response();
    }

    // Parse rows
    let rows = match parse_rows(&body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("parse error: {e}")})),
            )
                .into_response();
        }
    };

    if rows.is_empty() {
        return Json(json!({"accepted": 0})).into_response();
    }

    // Write to buffer
    let dir = state.config.vector_dir.as_ref().unwrap();
    let buffer_path = dir.join(".pfc_buffer.jsonl");

    let mut ndjson = String::new();
    for row in &rows {
        ndjson.push_str(&serde_json::to_string(row).unwrap());
        ndjson.push('\n');
    }
    let ndjson_bytes = ndjson.as_bytes().len() as u64;

    // Append to buffer (using tokio file append)
    use tokio::io::AsyncWriteExt;
    match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&buffer_path)
        .await
    {
        Ok(mut f) => {
            if let Err(e) = f.write_all(ndjson.as_bytes()).await {
                error!("Buffer write error: {e}");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("write error: {e}")})),
                )
                    .into_response();
            }
        }
        Err(e) => {
            error!("Cannot open buffer file: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("file error: {e}")})),
            )
                .into_response();
        }
    }

    {
        let mut buf = state.buffer.lock().await;
        buf.row_count += rows.len() as u64;
        buf.byte_count += ndjson_bytes;
    }

    // Check size threshold
    maybe_flush_by_size(&state).await;

    Json(json!({"accepted": rows.len()})).into_response()
}

async fn ingest_flush(
    State(state): State<Shared>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &state.config) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
    }
    if state.config.vector_dir.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "ingest not enabled"})),
        )
            .into_response();
    }
    let result = flush_buffer(&state, "manual").await;
    Json(result).into_response()
}

async fn ingest_status(
    State(state): State<Shared>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &state.config) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let enabled = state.config.vector_dir.is_some();

    let (row_count, byte_count, age_sec, last_file) = {
        let buf = state.buffer.lock().await;
        (
            buf.row_count,
            buf.byte_count,
            buf.last_flush.elapsed().as_secs(),
            buf.last_file.clone(),
        )
    };

    Json(json!({
        "enabled": enabled,
        "buffer_rows": row_count,
        "buffer_mb": (byte_count as f64) / (1024.0 * 1024.0),
        "last_flush_age_sec": age_sec,
        "last_file": last_file,
        "rotate_mb": state.config.rotate_mb,
        "rotate_sec": state.config.rotate_sec,
        "version": env!("CARGO_PKG_VERSION"),
    }))
    .into_response()
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pfc_vector=info".into()),
        )
        .init();

    let config = Config::from_env();

    // Validate / create vector_dir
    if let Some(ref dir) = config.vector_dir {
        if let Err(e) = std::fs::create_dir_all(dir) {
            error!("Cannot create PFC_VECTOR_DIR {:?}: {e}", dir);
            std::process::exit(1);
        }
        info!("Ingest enabled → {:?}", dir);
    } else {
        warn!("PFC_VECTOR_DIR not set — ingest endpoints return 503");
    }

    let bind = format!("{}:{}", config.host, config.port);
    info!("pfc-vector v{} listening on {bind}", env!("CARGO_PKG_VERSION"));

    let shared = Arc::new(AppState {
        config,
        buffer: Mutex::new(BufferState::new()),
    });

    // Start watchdog
    tokio::spawn(watchdog(shared.clone()));

    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/ingest", post(ingest))
        .route("/ingest/flush", post(ingest_flush))
        .route("/ingest/status", get(ingest_status))
        .with_state(shared);

    let listener = tokio::net::TcpListener::bind(&bind).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    // ── helpers ─────────────────────────────────────────────────────────────

    fn make_state(dir: Option<PathBuf>, api_key: Option<&str>) -> Shared {
        Arc::new(AppState {
            config: Config {
                pfc_jsonl_binary: "false".to_string(), // always fails — safe for tests
                api_key: api_key.map(|s| s.to_string()),
                vector_dir: dir,
                rotate_mb: 64,
                rotate_sec: 3600,
                prefix: "test".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8766,
            },
            buffer: Mutex::new(BufferState::new()),
        })
    }

    fn app(state: Shared) -> Router {
        Router::new()
            .route("/", get(health))
            .route("/health", get(health))
            .route("/ingest", post(ingest))
            .route("/ingest/flush", post(ingest_flush))
            .route("/ingest/status", get(ingest_status))
            .with_state(state)
    }

    // ── parse_rows tests ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_json_array() {
        let body = br#"[{"a":1},{"b":2}]"#;
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_rows_key() {
        let body = br#"{"rows":[{"x":1}]}"#;
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_parse_events_key() {
        let body = br#"{"events":[{"x":1},{"y":2}]}"#;
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_ndjson() {
        let body = b"{\"a\":1}\n{\"b\":2}\n";
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_ndjson_blank_lines_skipped() {
        let body = b"{\"a\":1}\n\n{\"b\":2}\n";
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_parse_empty_array() {
        let rows = parse_rows(b"[]").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_parse_empty_body() {
        let rows = parse_rows(b"").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_parse_invalid_returns_error() {
        assert!(parse_rows(b"not json at all!!!").is_err());
    }

    // ── health endpoint ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_health_ok() {
        let state = make_state(None, None);
        let response = app(state)
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── auth tests ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_no_api_key_allows_all() {
        let dir = tempfile::tempdir().unwrap();
        // Status endpoint is GET — verify no-auth-key allows all
        let state2 = make_state(Some(dir.path().to_path_buf()), None);
        let response2 = app(state2)
            .oneshot(
                Request::get("/ingest/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response2.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_wrong_api_key_returns_401() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), Some("correct"));
        let response = app(state)
            .oneshot(
                Request::get("/ingest/status")
                    .header("x-api-key", "wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_correct_api_key_allows() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), Some("secret"));
        let response = app(state)
            .oneshot(
                Request::get("/ingest/status")
                    .header("x-api-key", "secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── ingest disabled ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_ingest_disabled_returns_503() {
        let state = make_state(None, None);
        let response = app(state)
            .oneshot(
                Request::post("/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"[{"msg":"hi"}]"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_flush_disabled_returns_503() {
        let state = make_state(None, None);
        let response = app(state)
            .oneshot(Request::post("/ingest/flush").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── ingest accepted ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_ingest_json_array_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);
        let response = app(state.clone())
            .oneshot(
                Request::post("/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"[{"ts":"2026-01-01","msg":"hello"}]"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["accepted"], 1);
        assert_eq!(state.buffer.lock().await.row_count, 1);
    }

    #[tokio::test]
    async fn test_ingest_ndjson_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);
        let response = app(state.clone())
            .oneshot(
                Request::post("/ingest")
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from("{\"a\":1}\n{\"b\":2}\n"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["accepted"], 2);
    }

    #[tokio::test]
    async fn test_ingest_empty_array_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);
        let response = app(state.clone())
            .oneshot(
                Request::post("/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from("[]"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["accepted"], 0);
    }

    #[tokio::test]
    async fn test_ingest_invalid_body_returns_400() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);
        let response = app(state)
            .oneshot(
                Request::post("/ingest")
                    .body(Body::from("this is not json or ndjson!!!"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ── status ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_status_reflects_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);
        let svc = app(state.clone());

        // Ingest 3 rows
        svc.oneshot(
            Request::post("/ingest")
                .header("content-type", "application/json")
                .body(Body::from(r#"[{"a":1},{"b":2},{"c":3}]"#))
                .unwrap(),
        )
        .await
        .unwrap();

        let state2 = make_state(Some(dir.path().to_path_buf()), None);
        {
            let mut buf = state2.buffer.lock().await;
            buf.row_count = state.buffer.lock().await.row_count;
        }

        let response = app(state)
            .oneshot(Request::get("/ingest/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["enabled"], true);
        assert_eq!(v["buffer_rows"], 3);
    }

    // ── concurrent stress test ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_concurrent_ingest() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_state(Some(dir.path().to_path_buf()), None);

        let tasks = 20u64;
        let rows_per_task = 50u64;

        let mut handles = vec![];
        for i in 0..tasks {
            let state_clone = state.clone();
            let handle = tokio::spawn(async move {
                let rows: Vec<Value> = (0..rows_per_task)
                    .map(|j| json!({"task": i, "row": j, "msg": "stress"}))
                    .collect();
                let body = serde_json::to_string(&rows).unwrap();
                let svc = app(state_clone);
                let response = svc
                    .oneshot(
                        Request::post("/ingest")
                            .header("content-type", "application/json")
                            .body(Body::from(body))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        let buf = state.buffer.lock().await;
        assert_eq!(buf.row_count, tasks * rows_per_task);
    }
}
