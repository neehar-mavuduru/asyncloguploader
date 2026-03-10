# Load Test for AsyncLogUploader

Sustained multi-goroutine write test with optional GCS upload. Uses a pre-allocated log pool (zero allocation in hot path) and an optional rate limiter. Use this to validate throughput, resource usage, and end-to-end behavior on a Linux VM with real GCS credentials.

## What It Does

1. **Sustained writes**: N goroutines write continuously for a configurable duration (e.g., 5–30 minutes).
2. **Multi-event**: Writes are distributed across event names (e.g., `event1`, `event2`).
3. **Pre-allocated pool**: 50KB payloads from a pool of 100 entries; no allocation in the write hot path.
4. **Rate limiting**: Optional cap at ~1000 RPS (configurable).
5. **GCS by default**: GCS upload enabled with `gcs-dsci-srch-search-prd` and `Image_search/gcs-flush/`; set `GCS_ENABLED=false` for disk-only.
6. **Periodic stats**: Every 10 seconds prints total logs, dropped, bytes written, upload stats.
7. **Graceful shutdown**: Handles SIGINT/SIGTERM and duration timeout; flushes before exit.
8. **Validation**: Exits with code 1 if any logs were dropped; reports leftover `.tmp` files.

## Defaults (No Config)

| Setting        | Default                          |
|----------------|----------------------------------|
| GCS bucket     | `gcs-dsci-srch-search-prd`       |
| GCS path       | `Image_search/gcs-flush/`        |
| Max file size  | 256 MB                           |
| Buffer size    | 64 MB                            |
| Num shards     | 8                                |
| Events         | `event1`, `event2`               |
| Payload size   | 50 KB                            |
| Pool size      | 100 entries                      |
| Target RPS     | 1000 logs/sec                    |
| Duration       | 5m                               |
| Goroutines     | 4                                |

## Quick Start (Disk Only)

```bash
# 5 seconds, 8 goroutines (no GCS)
GCS_ENABLED=false LOADTEST_DURATION=5s LOADTEST_GOROUTINES=8 go run ./cmd/loadtest
```

## Full GCS Load Test (Linux VM)

Ensure the VM has a service account with write access to the bucket and path. GCS is enabled by default.

```bash
export LOADTEST_DURATION=10m
export LOADTEST_GOROUTINES=32
export LOADTEST_EVENTS=payments,logins,clicks
export LOG_FILE_PATH=/var/log/asyncloguploader-loadtest

go run ./cmd/loadtest
```

Or build and run:

```bash
go build -o loadtest ./cmd/loadtest
./loadtest
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOADTEST_DURATION` | 5m | Run duration (e.g., 5m, 30m) |
| `LOADTEST_GOROUTINES` | 4 | Number of concurrent writers |
| `LOADTEST_EVENTS` | event1,event2 | Comma-separated event names |
| `LOADTEST_PAYLOAD_SIZE` | 51200 (50KB) | Bytes per payload |
| `LOADTEST_POOL_SIZE` | 100 | Number of pre-allocated log entries |
| `LOADTEST_RPS` | 1000 | Target rate (logs/sec). 0 = unbounded |
| `LOADTEST_BUFFER_MB` | 64 | Total buffer size (MB) |
| `LOADTEST_MAX_FILE_MB` | 256 | Max file size before rotation (MB) |
| `LOADTEST_NUM_SHARDS` | 8 | Number of shards |
| `LOG_FILE_PATH` | /tmp/asyncloguploader-loadtest | Base directory for logs |
| `GCS_ENABLED` | true | Enable GCS upload |
| `GCS_BUCKET` | gcs-dsci-srch-search-prd | GCS bucket name |
| `GCS_PREFIX` | Image_search/gcs-flush/ | Object prefix |
| `GCS_POLL_INTERVAL` | 3s | Uploader poll interval |

## Tuning for High Throughput

If you see dropped logs, increase buffer size:

```bash
export LOADTEST_BUFFER_MB=256
export LOADTEST_MAX_FILE_MB=1024
```

To run without rate limiting:

```bash
export LOADTEST_RPS=0
```

## Exit Codes

- **0**: Success, no dropped logs
- **1**: Config error, or one or more logs were dropped (buffer full)
