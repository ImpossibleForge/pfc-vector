# pfc-vector

**High-performance PFC ingest daemon for Vector.dev, Telegraf, Fluent Bit, and any HTTP source.**

pfc-vector is a lightweight Rust HTTP server that receives log/metric events from any tool with HTTP output, buffers them in memory-safe Tokio async code, and compresses them to `.pfc` archives automatically.

Built on **axum + tokio** — no GIL, no Python overhead, no lock held during compression.

Part of the [PFC Ecosystem](https://github.com/ImpossibleForge).

---

## What it does

```
[Vector.dev / Telegraf / Fluent Bit / curl]
          │
          ▼  POST /ingest — push NDJSON rows
     pfc-vector  (this Rust binary)
          │
          ├── .pfc_buffer.jsonl    (live ring buffer, append-only)
          └── vector_<ts>.pfc      (auto-rotated on size or time)
                    │
                    ▼
             S3 / local storage
```

**Key design:**
- Mutex held only for the **atomic rename** (microseconds), never during compression
- Compression runs **outside the lock** → new rows accepted at full speed while old block compresses
- On compress failure → buffer is **automatically restored**, zero data loss

---

## Install

```bash
# Prerequisites: pfc_jsonl binary
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# Build pfc-vector
git clone https://github.com/ImpossibleForge/pfc-vector
cd pfc-vector
cargo build --release

# Start
PFC_VECTOR_DIR=/data/pfc PFC_API_KEY=secret \
  ./target/release/pfc-vector
```

> **License note:** This tool requires the `pfc_jsonl` binary. `pfc_jsonl` is free for personal and open-source use — commercial use requires a separate license. See [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) for details.

---

## Vector.dev sink configuration

```toml
# vector.toml
[sinks.pfc]
type   = "http"
inputs = ["your_source"]
uri    = "http://your-server:8766/ingest"
encoding.codec = "ndjson"

[sinks.pfc.request.headers]
X-API-Key = "secret"

# Optional: tune batch size to match PFC block size
[sinks.pfc.batch]
max_bytes  = 10485760   # 10 MB
timeout_secs = 30
```

---

## Telegraf HTTP output

```toml
[[outputs.http]]
url             = "http://your-server:8766/ingest"
method          = "POST"
data_format     = "json"
[outputs.http.headers]
  X-API-Key   = "secret"
  Content-Type = "application/json"
```

---

## Fluent Bit HTTP output

```ini
[OUTPUT]
    Name       http
    Match      *
    Host       your-server
    Port       8766
    URI        /ingest
    Format     json
    Header     X-API-Key secret
```

---

## curl

```bash
# JSON array
curl -X POST http://localhost:8766/ingest \
  -H "X-API-Key: secret" \
  -H "Content-Type: application/json" \
  -d '[{"ts":"2026-04-21T10:00:00Z","level":"INFO","msg":"server started"}]'

# NDJSON (native Vector format)
printf '{"ts":"2026-04-21T10:00:01Z","level":"WARN","msg":"high cpu"}\n' | \
  curl -X POST http://localhost:8766/ingest \
  -H "X-API-Key: secret" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @-
```

---

## API Reference

### `GET /` or `GET /health`

Health check.

```json
{"status": "ok", "version": "0.1.0", "service": "pfc-vector"}
```

### `POST /ingest`

Accepts rows in three formats (auto-detected):
- JSON array: `[{...}, {...}]`
- Object with rows/events/logs key: `{"rows": [{...}]}`, `{"events": [{...}]}`
- Raw NDJSON: `{...}\n{...}\n`

Returns: `{"accepted": N}`

Requires `PFC_VECTOR_DIR` to be set (returns 503 otherwise).

### `POST /ingest/flush`

Force-compress current buffer to a `.pfc` file immediately.

```json
{"flushed": true, "rows": 4821, "file": "/data/pfc/vector_20260421T103045.pfc"}
```

### `GET /ingest/status`

```json
{
  "enabled": true,
  "buffer_rows": 142,
  "buffer_mb": 0.021,
  "last_flush_age_sec": 312,
  "last_file": "/data/pfc/vector_20260421T100000.pfc",
  "rotate_mb": 64,
  "rotate_sec": 3600,
  "version": "0.1.0"
}
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_VECTOR_DIR` | *(none — ingest off)* | Directory for buffer and output `.pfc` files |
| `PFC_API_KEY` | *(none — auth off)* | API key for `X-API-Key` header |
| `PFC_JSONL_BINARY` | `/usr/local/bin/pfc_jsonl` | Path to pfc_jsonl binary |
| `PFC_VECTOR_ROTATE_MB` | `64` | Rotate when buffer reaches this size (MB) |
| `PFC_VECTOR_ROTATE_SEC` | `3600` | Rotate when buffer is older than this (seconds) |
| `PFC_VECTOR_PREFIX` | `vector` | Output filename prefix: `vector_<ts>.pfc` |
| `PFC_VECTOR_HOST` | `0.0.0.0` | Bind address |
| `PFC_VECTOR_PORT` | `8766` | Port |

Standard AWS variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`) are respected by `pfc_jsonl` automatically.

---

## Run as systemd service

```ini
# /etc/systemd/system/pfc-vector.service
[Unit]
Description=pfc-vector — PFC ingest daemon for Vector/Telegraf/Fluent Bit
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-vector
ExecStart=/opt/pfc-vector/pfc-vector
Restart=on-failure
Environment=PFC_API_KEY=your-secret-key
Environment=PFC_VECTOR_DIR=/data/pfc
Environment=PFC_VECTOR_ROTATE_MB=64
Environment=PFC_VECTOR_ROTATE_SEC=3600

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now pfc-vector
```

---

## Run as Docker container

```bash
docker run -d \
  -p 8766:8766 \
  -e PFC_API_KEY=your-secret-key \
  -e PFC_VECTOR_DIR=/data/pfc \
  -v /data/pfc:/data/pfc \
  --name pfc-vector \
  impossibleforge/pfc-vector:latest
```

---

## Architecture — Why Rust?

pfc-gateway (Python/FastAPI) is our bidirectional gateway — it's great for most workloads. pfc-vector exists for one reason: **extreme throughput with zero GIL pressure**.

| | pfc-gateway (Python) | pfc-vector (Rust) |
|---|---|---|
| Language | Python + asyncio | Rust + Tokio |
| GIL | Yes (no true parallelism) | None |
| Lock scope | Was held during compression | Held only for rename (~µs) |
| Compression | asyncio subprocess | Tokio subprocess (same pfc_jsonl) |
| Best for | General use, query + ingest | High-frequency Vector/Telegraf pipelines |

Both use the same `pfc_jsonl` binary for compression. The PFC archive format is identical.

---

## Architecture in the full PFC ecosystem

```
Your data sources
    │
    ├── pfc-migrate     (one-shot export)
    ├── pfc-archiver-*  (autonomous daemon)
    ├── pfc-fluentbit   (live Fluent Bit → PFC pipeline)
    ├── pfc-gateway     (POST /ingest — Python, bidirectional)
    └── pfc-vector      (POST /ingest — Rust, high-frequency)  ← this repo
              │
              ▼
    .pfc archives (local / S3)
              │
              ▼
    pfc-gateway  ← query via HTTP REST (Grafana, Python, curl)
```

---

## Related repos

- [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) — core binary (compress/decompress/query)
- [pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway) — HTTP REST gateway (query + ingest, Python)
- [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) — Fluent Bit → PFC pipeline
- [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) — one-shot export and archive conversion
- [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) — DuckDB extension for SQL queries on PFC files

---

*ImpossibleForge — [github.com/ImpossibleForge](https://github.com/ImpossibleForge)*  
*Contact: info@impossibleforge.com*
