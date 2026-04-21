# Changelog — pfc-vector

## v0.1.0 (2026-04-21)

### Initial release — High-performance PFC ingest daemon in Rust

pfc-vector is a standalone Rust binary (axum + tokio) that receives NDJSON from
Vector.dev, Telegraf, Fluent Bit, or any HTTP client and compresses it to `.pfc`
archives automatically.

**Key design decisions:**

- **Mutex held only for atomic rename** — not during compression.  
  New rows are accepted at full speed while the previous block is being compressed.
  This eliminates the bottleneck present in Python/asyncio implementations where
  the lock is held during the entire `pfc_jsonl compress` subprocess call.

- **Zero data loss on failure** — if `pfc_jsonl compress` fails, the buffer file
  is automatically restored (with any new rows that arrived during compression prepended).

- **No GIL** — true async parallelism via Tokio. No Python runtime overhead.

**Endpoints:**
- `GET /health` — service status + version
- `POST /ingest` — accept rows (JSON array, `{"rows":[...]}`, `{"events":[...]}`, raw NDJSON)
- `POST /ingest/flush` — force-compress current buffer to a `.pfc` file immediately
- `GET /ingest/status` — buffer stats (rows, bytes, age, last output file, thresholds)

**Auto-rotation:**
- Size threshold: flush when buffer reaches `PFC_VECTOR_ROTATE_MB` (default 64 MB)
- Time threshold: flush when buffer age exceeds `PFC_VECTOR_ROTATE_SEC` (default 3600 s)
- Background Tokio watchdog checks every 60 s

**Body format auto-detection:**
- JSON array: `[{...}, {...}]`
- Object with rows key: `{"rows": [{...}]}`, `{"events": [{...}]}`, `{"logs": [{...}]}`
- Single JSON object: `{...}` (wrapped automatically)
- Raw NDJSON: `{...}\n{...}\n`

**Test suite:**
- 20 tests covering: parse_rows (8), auth (3), disabled mode (2), ingest formats (5),
  status, and concurrent stress test (20 tasks × 50 rows = 1000 total, verified atomic)
- **20/20 PASS**, zero warnings

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_VECTOR_DIR` | *(none — ingest off)* | Directory for buffer and output `.pfc` files |
| `PFC_API_KEY` | *(none — auth off)* | API key for `X-API-Key` header |
| `PFC_JSONL_BINARY` | `/usr/local/bin/pfc_jsonl` | Path to pfc_jsonl binary |
| `PFC_VECTOR_ROTATE_MB` | `64` | Size rotation threshold (MB) |
| `PFC_VECTOR_ROTATE_SEC` | `3600` | Time rotation threshold (seconds) |
| `PFC_VECTOR_PREFIX` | `vector` | Output filename prefix (`vector_<ts>.pfc`) |
| `PFC_VECTOR_HOST` | `0.0.0.0` | Bind address |
| `PFC_VECTOR_PORT` | `8766` | Port (8766 — one above pfc-gateway's 8765) |
