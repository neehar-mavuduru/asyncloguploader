# AsyncLogUploader — Technical Solution Document

**Version:** 2.0  
**Module:** `github.com/Meesho/BharatMLStack/asyncloguploader`  
**Last Updated:** March 2026

---

## 1. Executive Summary

**AsyncLogUploader** is a high-performance, asynchronous logging library for Go that writes structured log data to disk and optionally uploads completed log files to Google Cloud Storage (GCS). It is designed for low-latency, high-throughput event logging with minimal allocation in hot paths and crash-safe durability.

### Key Capabilities

| Capability | Description |
|------------|-------------|
| **Non-blocking writes** | Lock-free CAS-based buffers; writers rarely block |
| **Sharded buffering** | Random shard selection for write distribution |
| **Double buffering** | Per-shard A/B buffers with atomic swap |
| **Size-based rotation** | Configurable max file size with proactive next-file pre-creation |
| **Extension-based discovery** | `.tmp` = in-progress, `.log` = sealed and ready for upload |
| **Optional GCS upload** | Parallel chunk upload, compose, retry with exponential backoff |

---

## 2. Problem Statement

### Business Context

Applications need to log high-volume event data (e.g., ML training events, analytics, audit logs) with:

- **Low latency** — Log calls must not block request handling
- **High throughput** — Support millions of events per second
- **Durability** — Data must survive process crashes
- **Cloud integration** — Completed logs should be uploaded to GCS for downstream processing

### Technical Challenges

1. **Lock contention** — Traditional mutex-based logging serializes writers
2. **Disk I/O blocking** — Synchronous writes stall the calling goroutine
3. **Buffer exhaustion** — Under backpressure, behavior must be predictable (drop vs block)
4. **Crash recovery** — Sealed files must be discoverable and uploadable after restart

---

## 3. Solution Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LoggerManager (entry point)                        │
│  • Event-name multiplexing  • Lazy logger creation  • Optional GCS uploader │
└─────────────────────────────────────────────────────────────────────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
            ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
            │ Logger (evt1) │     │ Logger (evt2) │     │ Logger (evtN) │
            └───────┬───────┘     └───────┬───────┘     └───────┬───────┘
                    │                     │                     │
                    └─────────────────────┼─────────────────────┘
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ShardCollection (per Logger)                         │
│  • Random shard selection  • Flush threshold (25% shards ready)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                          │
        ┌─────────────────────────────────┼─────────────────────────────────┐
        ▼                                 ▼                                 ▼
┌───────────────┐                 ┌───────────────┐                 ┌───────────────┐
│ Shard 0       │                 │ Shard 1       │                 │ Shard N       │
│ Buffer A ↔ B  │                 │ Buffer A ↔ B  │                 │ Buffer A ↔ B  │
└───────┬───────┘                 └───────┬───────┘                 └───────┬───────┘
        │                                 │                                 │
        └─────────────────────────────────┼─────────────────────────────────┘
                                          ▼
                              ┌───────────────────────┐
                              │ flushChan (buffered)   │
                              └───────────┬───────────┘
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         flushWorker (background goroutine)                    │
│  • Drain buffers  • WaitForInflight  • WriteVectored to disk                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SizeFileWriter (file I/O)                             │
│  • Size-based rotation  • Proactive next-file  • .tmp → .log rename          │
└─────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │ logs/                  │
                              │ *.log.tmp (active)     │
                              │ *.log     (sealed)     │
                              └───────────┬───────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │ GCS Uploader           │
                              │ (poll logs/, skip .tmp │
                              │  upload .log, delete)  │
                              └───────────────────────┘
```

### Directory Layout

```
{LogFilePath}/
└── logs/                        # All log files live here
    ├── event1_2026-03-09_14-00-00_1.log.tmp    # Active (being written)
    ├── event1_2026-03-09_14-00-00_2.log        # Sealed (ready for upload)
    ├── event1_2026-03-09_14-05-00_3.log        # Sealed (ready for upload)
    └── ...
```

### File Lifecycle

```
 Created as .tmp ──▶ Written to ──▶ Synced ──▶ Renamed to .log ──▶ Uploaded to GCS ──▶ Deleted
    (active)          (active)     (sealed)      (discoverable)       (cleanup)
```

The `.tmp` → `.log` rename is the **atomic readiness signal**. The uploader discovers work by scanning `logs/` for `.log` files and skipping `.tmp` files. This eliminates the need for a separate `upload_ready/` directory and symlinks.

**Empty files:** Files with zero bytes (e.g., from shutdown immediately after rotation) are not sealed; they are removed instead of renamed to `.log`. The uploader also skips any empty `.log` files it encounters.

---

## 4. Component Design

### 4.1 LoggerManager

**Purpose:** Multiplexes log writes across event-specific Loggers and optionally runs the GCS Uploader.

| Aspect | Detail |
|--------|--------|
| **Concurrency** | `sync.Map` for thread-safe logger creation |
| **Event isolation** | One Logger per event name; separate log files |
| **Event sanitization** | Replaces `/ \ : * ? " < > \|` and spaces with `_`; truncates to 255 chars |
| **Lazy creation** | Loggers created on first `LogBytesWithEvent` / `LogWithEvent` |

**API:**

```go
lm, err := NewLoggerManager(config)
lm.LogBytesWithEvent("click_events", payload)
lm.LogWithEvent("click_events", "user clicked")
lm.CloseEventLogger("click_events")
lm.Close()
```

---

### 4.2 Logger

**Purpose:** Orchestrates sharded writes, flush worker, and file writer.

| Aspect | Detail |
|--------|--------|
| **Write path** | `LogBytes` → `ShardCollection.Write` → random shard |
| **Backpressure** | On buffer full: acquire semaphore (50ms timeout) → `FlushAll` → retry → drop |
| **Graceful shutdown** | Wait for inflight writes → `FlushAll` → drain flushChan → close file |

**Statistics:** `TotalLogs`, `DroppedLogs`, `BytesWritten` (atomic counters)

---

### 4.3 Buffer

**Purpose:** Lock-free, CAS-based write buffer for length-prefixed records.

| Aspect | Detail |
|--------|--------|
| **Record format** | 4-byte little-endian length + payload |
| **Memory** | Linux: `mmap(MAP_PRIVATE\|MAP_ANONYMOUS)`; others: `make([]byte)` |
| **Concurrency** | CAS on `offset`; `inflight` for in-flight write tracking |
| **Swap signal** | At 90% capacity, returns `shouldSwap=true` for proactive swap |

---

### 4.4 Shard

**Purpose:** Double-buffered write target; two Buffers alternate as active.

| Aspect | Detail |
|--------|--------|
| **Swap trigger** | Full buffer → synchronous `trySwap`; 90% full → async `go trySwap()` |
| **Atomicity** | `swapping` CAS prevents concurrent swaps |
| **Safety** | `WaitForInflight` on outgoing buffer before sending to flushChan |

---

### 4.5 ShardCollection

**Purpose:** Distributes writes across shards and triggers interval flush.

| Aspect | Detail |
|--------|--------|
| **Selection** | `rand.IntN(numShards)` |
| **Flush threshold** | 25% of shards with `readyForFlush` → signal for interval flush |
| **FlushAll** | Calls `trySwap` on every shard |

---

### 4.6 SizeFileWriter

**Purpose:** Vectored file I/O with size-based rotation.

| Aspect | Detail |
|--------|--------|
| **Rotation** | When `fileOffset >= maxFileSize` |
| **Proactive next file** | At 90% of max size, pre-create next `.tmp` file |
| **Naming** | `{base}_{YYYY-MM-DD_HH-MM-SS}_{seq}.log.tmp` → renamed to `.log` on seal |
| **Discovery** | Uploader scans `logs/` for `.log` files; `.tmp` files are skipped |
| **Platform** | Linux: `O_DIRECT`, `Pwritev`, `Fallocate`; others: `os.WriteAt` |

---

### 4.7 Uploader

**Purpose:** Polls `logs/` directory for sealed `.log` files, uploads them to GCS in chunks, composes, verifies, and deletes the local file.

| Aspect | Detail |
|--------|--------|
| **Discovery** | `os.ReadDir(logsDir)`; upload files ending in `.log`, skip `.tmp` |
| **Chunking** | Configurable chunk size (default 32 MB); parallel upload |
| **Compose** | GCS Compose API; multi-level for >32 chunks |
| **Retry** | Exponential backoff; configurable max retries |
| **Cleanup** | Single `os.Remove(filePath)` after verified upload |
| **Credentials** | Application Default Credentials (ADC) |

---

### 4.8 ChunkManager

**Purpose:** Handles GCS Compose API limits (max 32 source objects).

| Aspect | Detail |
|--------|--------|
| **<=32 chunks** | Single `ComposerFrom` call |
| **>32 chunks** | Multi-level: compose in groups of 32 → intermediate objects → final compose |

---

## 5. Data Flow

### Write Path (Happy Path)

1. `LogBytesWithEvent("evt", data)` → `LoggerManager.getOrCreateLogger("evt")` → `Logger.LogBytes(data)`
2. `Logger.LogBytes` → `ShardCollection.Write(data)` → random shard → `Shard.Write(data)`
3. `Shard.Write` → active `Buffer.writeData(data)` → CAS on `offset`; on 90% full, `go trySwap()`
4. `trySwap` → swap active buffer → `WaitForInflight` → send full buffer to `flushChan`
5. `flushWorker` receives buffer → `collectBuffers` → `WaitForInflight` on each → `WriteVectored` → `Reset` buffers

### Backpressure Path

1. All shards return `n==0` (buffer full)
2. `Logger` acquires semaphore (50ms timeout)
3. On timeout → `DroppedLogs++`, return `ErrBufferFull`
4. On success → `FlushAll` → retry write → if still full, drop

### Upload Path

1. `Uploader` polls `logs/` every `PollInterval`
2. For each `.log` file (skip `.tmp`): read file data
3. Split into chunks → parallel upload to GCS
4. `ChunkManager.Compose` → verify size → delete chunks → `os.Remove(filePath)`

### File Seal & Discovery

```
Writer side:                    Uploader side:
                                
  write to .tmp file              scan logs/ directory
       │                              │
  sync + truncate                 skip *.tmp files
       │                              │
  os.Rename(.tmp → .log)  ◄──── pick up *.log files
       │                              │
  (atomic, POSIX)                 upload to GCS
                                      │
                                  os.Remove(.log)
```

---

## 6. Configuration

### Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `NumShards` | int | — | Number of shards (>=1). Auto-reduced if per-shard capacity < 64 KB |
| `BufferSize` | int | — | Total buffer size (bytes). Split across shards |
| `MaxFileSize` | int64 | — | Max size per log file before rotation |
| `LogFilePath` | string | — | Base directory; `logs/` created underneath |
| `FlushInterval` | time.Duration | 1m | Interval for periodic buffer flush; 0 = 1 minute |
| `GCSUploadConfig` | *GCSUploadConfig | nil | Optional; nil disables uploader |

### GCSUploadConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BucketName` | string | — | GCS bucket name |
| `ObjectPrefix` | string | "" | Prefix for object names |
| `ChunkSize` | int | 32 MB | Chunk size for parallel upload |
| `MaxRetries` | int | 3 | Retries per file with exponential backoff |
| `GRPCPoolSize` | int | 64 | gRPC connection pool size |
| `PollInterval` | time.Duration | 3s | How often to scan `logs/` |

### Example

```go
cfg := asyncloguploader.Config{
    NumShards:   8,
    BufferSize:  64 * 1024 * 1024, // 64 MB
    MaxFileSize: 256 * 1024 * 1024, // 256 MB
    LogFilePath: "/var/log/events",
    GCSUploadConfig: &asyncloguploader.GCSUploadConfig{
        BucketName:   "my-bucket",
        ObjectPrefix: "events/",
        ChunkSize:    32 * 1024 * 1024,
        MaxRetries:   3,
        PollInterval: 3 * time.Second,
    },
}
lm, err := asyncloguploader.NewLoggerManager(cfg)
```

---

## 7. Error Handling

### Sentinel Errors

| Error | When |
|-------|------|
| `ErrBufferFull` | All shard buffers full; log dropped after 50ms retry |
| `ErrLoggerClosed` | Write to closed logger |
| `ErrInvalidConfig` | `Config.Validate()` failed |

### Behavior

- **No panics** in library code (except empty sanitized event name)
- **Logging** via `zerolog` (Warn, Error) for internal failures (rotation, upload)

---

## 8. Performance Characteristics

### Benchmarks (Apple M1 Pro)

| Benchmark | ns/op | allocs/op |
|-----------|-------|-----------|
| LogBytesWithEvent (hot path) | ~54 | 0 |
| LogBytesWithEvent (64 goroutines) | ~331 | 0 |
| Shard.Write (no swap) | ~16 | 0 |
| Shard.Write (with swap) | ~45 | 0 |
| WriteVectored (8 buffers) | ~2.67 ms | 0 |
| Throughput (WriteVectored) | — | ~3.1 GB/s |

### Tuning Tips

- **NumShards**: Higher = less contention; ensure per-shard capacity >= 64 KB
- **BufferSize**: Larger = fewer swaps, more memory
- **ChunkSize**: 32 MB balances parallelism and GCS compose limits

---

## 9. Security Considerations

| Area | Mitigation |
|------|------------|
| **Event names** | Sanitized (path separators, special chars replaced); max 255 chars |
| **Path traversal** | No user input in file paths |
| **GCS credentials** | ADC only; no hardcoded keys |
| **Logging** | No secrets in zerolog output |

---

## 10. Observability & Monitoring

### Logger Statistics

```go
stats := logger.GetStats()
// stats.TotalLogs, stats.DroppedLogs, stats.BytesWritten
```

### Uploader Statistics

```go
stats := uploader.GetStats()
// stats.FilesUploaded, stats.BytesUploaded, stats.UploadErrors, stats.RetryCount
```

### LoggerManager Aggregate

```go
allStats := lm.GetStats()
// map[eventName]*Statistics
```

### Internal Logging

- **zerolog** at Warn/Error for rotation errors, upload retries
- Configure zerolog level/format in application

---

## 11. Deployment & Operations

### Graceful Shutdown

```go
lm.Close()  // Flushes all loggers, stops uploader, waits for in-flight uploads
```

### Crash Recovery

- **Logger**: In-memory buffers are lost; only flushed data is durable
- **Uploader**: On restart, the uploader scans `logs/` and finds any `.log` files that were sealed but not yet uploaded. These are processed automatically.
- **Orphaned .tmp files**: If the process crashes mid-write, `.tmp` files are left behind. These are safely ignored by the uploader. Periodic cleanup of old `.tmp` files (e.g., older than N minutes) is recommended.

### Dependencies

- Go 1.25+
- `cloud.google.com/go/storage` (GCS)
- `github.com/rs/zerolog` (internal logging)
- `golang.org/x/sys` (Linux syscalls)

---

## 12. Troubleshooting

| Symptom | Possible Cause | Action |
|---------|----------------|--------|
| `ErrBufferFull` | Disk slow or buffer too small | Increase `BufferSize` or `NumShards`; check disk I/O |
| High `DroppedLogs` | Sustained backpressure | Scale buffers; reduce write rate or add backpressure upstream |
| Upload errors | GCS permissions, network | Check ADC; verify bucket/prefix; inspect `UploadErrors`, `RetryCount` |
| `.tmp` files accumulating | Crash during write | Manual cleanup of old `.tmp` files; ensure `Close()` on shutdown |

---

## 13. API Quick Reference

```go
// LoggerManager
lm, err := NewLoggerManager(config)
lm.LogBytesWithEvent(eventName string, data []byte) error
lm.LogWithEvent(eventName string, msg string) error
lm.CloseEventLogger(eventName string) error
lm.GetStats() map[string]*Statistics
lm.Close() error

// Errors
ErrBufferFull
ErrLoggerClosed
ErrInvalidConfig
```

---

## 14. Load Test

A standalone load test lives in `cmd/loadtest/`. It runs sustained multi-goroutine writes with optional GCS upload. Configure via environment variables; see `cmd/loadtest/README.md` for details.

```bash
# Disk-only (5s, 8 goroutines)
LOADTEST_DURATION=5s LOADTEST_GOROUTINES=8 go run ./cmd/loadtest

# With GCS (Linux VM)
export GCS_ENABLED=true GCS_BUCKET=gcs-dsci-srch-search-prd GCS_PREFIX=Image_search/gcs-flush/
export LOADTEST_DURATION=10m LOADTEST_GOROUTINES=32
go run ./cmd/loadtest
```

Exits with code 1 if any logs were dropped. Use `LOADTEST_BUFFER_MB` and `LOADTEST_MAX_FILE_MB` to tune for higher throughput.

---

## 15. File Structure

```
asyncloguploader/
├── cmd/
│   └── loadtest/          # Load test binary
├── config.go              # Config, GCSUploadConfig, Validate
├── errors.go              # Sentinel errors
├── stats.go               # Statistics, UploaderStats
├── buffer.go              # Buffer (CAS, length-prefixed)
├── mmap_linux.go          # mmap on Linux
├── mmap_default.go        # make([]byte) elsewhere
├── shard.go               # Double-buffered Shard
├── shard_collection.go    # ShardCollection
├── file_writer.go         # SizeFileWriter (rotation, .tmp → .log rename)
├── file_writer_linux.go   # O_DIRECT, Pwritev, Fallocate
├── file_writer_default.go # os.WriteAt
├── logger.go              # Logger (flush worker, backpressure)
├── logger_manager.go      # LoggerManager
├── chunk_manager.go       # GCS Compose (multi-level)
├── uploader.go            # GCS Uploader (scans logs/ for .log files)
└── docs/
    └── TECH_SOLUTION.md
```

---

## 16. Design Decision: Extension-Based Discovery (v2.0)

In v1.0, sealed log files were discovered via symlinks in a separate `upload_ready/` directory. In v2.0, this was simplified:

| Aspect | v1.0 (symlinks) | v2.0 (extension scan) |
|--------|-----------------|----------------------|
| **Readiness signal** | Symlink creation | `.tmp` → `.log` rename (atomic on POSIX) |
| **Crash recovery** | Symlinks survive crash | `.log` files survive crash |
| **Cleanup** | Remove symlink + remove file | Remove file |
| **Failure modes** | Symlink create/read errors | None added |
| **Extra directory** | `upload_ready/` required | Not needed |
| **Code complexity** | Symlink in `rotate()`, `Close()`, uploader | File extension check in uploader |

The `.tmp` → `.log` rename already served as an atomic readiness signal, making the symlink layer redundant. Removing it eliminated an entire category of bugs (symlink creation failures, duplicate symlinks, missing directory) without losing any crash-safety guarantees.

---

*Document maintained by the BharatMLStack team.*
