// Load test for asyncloguploader: sustained multi-goroutine writes with optional GCS upload.
//
// Uses a pre-allocated pool of log entries (zero allocation in hot path) and an optional
// rate limiter. Defaults are tuned for ~1000 RPS with 50KB payloads and GCS upload.
//
// Configuration via environment variables:
//
//	LOADTEST_DURATION       - Run duration (e.g., 5m, 30m). Default: 5m
//	LOADTEST_GOROUTINES     - Number of concurrent writers. Default: 4
//	LOADTEST_EVENTS         - Comma-separated event names. Default: event1,event2
//	LOADTEST_PAYLOAD_SIZE   - Bytes per payload. Default: 51200 (50KB)
//	LOADTEST_POOL_SIZE      - Number of pre-allocated log entries to rotate. Default: 100
//	LOADTEST_RPS            - Target rate (logs/sec). 0 = unbounded. Default: 1000
//	LOADTEST_BUFFER_MB      - Total buffer size in MB. Default: 64
//	LOADTEST_MAX_FILE_MB    - Max file size before rotation, in MB. Default: 256
//	LOADTEST_NUM_SHARDS     - Number of shards. Default: 8
//	LOG_FILE_PATH           - Base directory for logs. Default: /tmp/asyncloguploader-loadtest
//	GCS_ENABLED             - Enable GCS upload (true/false). Default: true
//	GCS_BUCKET              - GCS bucket name. Default: gcs-dsci-srch-search-prd
//	GCS_PREFIX              - Object prefix. Default: Image_search/gcs-flush/
//	GCS_POLL_INTERVAL       - Uploader poll interval (e.g., 3s). Default: 3s
//
// Example (override defaults):
//
//	export LOADTEST_RPS=2000
//	export LOADTEST_DURATION=10m
//	go run ./cmd/loadtest
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Meesho/BharatMLStack/asyncloguploader"
	"golang.org/x/time/rate"
)

func main() {
	cfg := loadConfig()
	if err := cfg.Config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	lm, err := asyncloguploader.NewLoggerManager(cfg.Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create LoggerManager: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[loadtest] received signal, shutting down...")
		cancel()
	}()

	durationTimer := time.NewTimer(cfg.LoadTestDuration)
	go func() {
		<-durationTimer.C
		fmt.Println("\n[loadtest] duration reached, shutting down...")
		cancel()
	}()

	fmt.Printf("[loadtest] starting: duration=%s goroutines=%d events=%v payload=%d bytes pool=%d rps=%d\n",
		cfg.LoadTestDuration, cfg.LoadTestGoroutines, cfg.LoadTestEvents, cfg.LoadTestPayloadSize,
		cfg.LoadTestPoolSize, cfg.LoadTestRPS)
	if cfg.GCSUploadConfig != nil {
		fmt.Printf("[loadtest] GCS enabled: bucket=%s prefix=%s\n",
			cfg.GCSUploadConfig.BucketName, cfg.GCSUploadConfig.ObjectPrefix)
	} else {
		fmt.Println("[loadtest] GCS disabled (disk-only)")
	}
	fmt.Println()

	start := time.Now()
	var totalWritten atomic.Int64
	var totalDropped atomic.Int64

	logPool := buildLogPool(cfg.LoadTestPayloadSize, cfg.LoadTestPoolSize)

	var limiter *rate.Limiter
	if cfg.LoadTestRPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.LoadTestRPS), cfg.LoadTestRPS)
	}

	var wg sync.WaitGroup
	for g := 0; g < cfg.LoadTestGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			runWriter(ctx, lm, cfg, gid, logPool, limiter, &totalWritten, &totalDropped)
		}(g)
	}

	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-statsTicker.C:
			printStats(lm, start, totalWritten.Load(), totalDropped.Load())
		}
	}

	wg.Wait()

	if err := lm.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Close error: %v\n", err)
	}
	printStats(lm, start, totalWritten.Load(), totalDropped.Load())

	elapsed := time.Since(start)
	fmt.Println()
	fmt.Println("--- Final Summary ---")
	fmt.Printf("Duration:        %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total written:   %d logs\n", totalWritten.Load())
	fmt.Printf("Total dropped:   %d logs\n", totalDropped.Load())
	if elapsed.Seconds() > 0 {
		rate := float64(totalWritten.Load()) / elapsed.Seconds()
		fmt.Printf("Throughput:      %.0f logs/sec\n", rate)
	}

	allStats := lm.GetStats()
	for event, s := range allStats {
		fmt.Printf("  [%s] total=%d dropped=%d bytes=%d\n",
			event, s.TotalLogs.Load(), s.DroppedLogs.Load(), s.BytesWritten.Load())
	}

	if us := lm.GetUploaderStats(); us != nil {
		fmt.Printf("GCS: files=%d bytes=%d errors=%d retries=%d\n",
			us.FilesUploaded.Load(), us.BytesUploaded.Load(),
			us.UploadErrors.Load(), us.RetryCount.Load())
	}

	logsDir := filepath.Join(cfg.LogFilePath, "logs")
	tmpCount := countTmpFiles(logsDir)
	if tmpCount > 0 {
		fmt.Printf("WARNING: %d .tmp files remain in %s\n", tmpCount, logsDir)
	} else {
		fmt.Printf("Cleanup: no .tmp files in %s\n", logsDir)
	}

	if totalDropped.Load() > 0 {
		fmt.Printf("\nWARNING: %d logs were dropped (buffer full)\n", totalDropped.Load())
		os.Exit(1)
	}
}

// buildLogPool creates a pool of pre-allocated log entries. Each entry is payloadSize
// bytes. Zero allocation in the write hot path.
func buildLogPool(payloadSize, poolSize int) [][]byte {
	pool := make([][]byte, poolSize)
	for i := range pool {
		pool[i] = make([]byte, payloadSize)
		prefix := fmt.Sprintf("entry_%d_", i)
		copy(pool[i], prefix)
		for j := len(prefix); j < payloadSize; j++ {
			pool[i][j] = byte('a' + (j % 26))
		}
	}
	return pool
}

func runWriter(ctx context.Context, lm *asyncloguploader.LoggerManager, cfg *loadTestConfig, gid int, logPool [][]byte, limiter *rate.Limiter, written, dropped *atomic.Int64) {
	events := cfg.LoadTestEvents
	poolSize := len(logPool)

	seq := int64(0)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}

		event := events[gid%len(events)]
		payload := logPool[seq%int64(poolSize)]

		err := lm.LogBytesWithEvent(event, payload)
		if err != nil {
			if err == asyncloguploader.ErrBufferFull {
				dropped.Add(1)
			}
			continue
		}
		written.Add(1)
		seq++
	}
}

func printStats(lm *asyncloguploader.LoggerManager, start time.Time, written, dropped int64) {
	elapsed := time.Since(start).Seconds()
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(written) / elapsed
	}

	var totalBytes int64
	allStats := lm.GetStats()
	for _, s := range allStats {
		totalBytes += s.BytesWritten.Load()
	}

	line := fmt.Sprintf("[%s] written=%d dropped=%d rate=%.0f/s bytes=%d",
		time.Now().Format("15:04:05"), written, dropped, rate, totalBytes)

	if us := lm.GetUploaderStats(); us != nil {
		line += fmt.Sprintf(" uploads=%d upload_bytes=%d upload_errors=%d",
			us.FilesUploaded.Load(), us.BytesUploaded.Load(), us.UploadErrors.Load())
	}

	fmt.Println(line)
}

func countTmpFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	count := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			count++
		}
	}
	return count
}

type loadTestConfig struct {
	asyncloguploader.Config
	LoadTestDuration    time.Duration
	LoadTestGoroutines  int
	LoadTestEvents      []string
	LoadTestPayloadSize int
	LoadTestPoolSize    int
	LoadTestRPS         int
}

func loadConfig() *loadTestConfig {
	duration := getEnv("LOADTEST_DURATION", "5m")
	d, err := time.ParseDuration(duration)
	if err != nil {
		d = 5 * time.Minute
	}

	goroutines := getEnvInt("LOADTEST_GOROUTINES", 4)
	if goroutines < 1 {
		goroutines = 1
	}

	eventsStr := getEnv("LOADTEST_EVENTS", "event1,event2")
	events := strings.Split(eventsStr, ",")
	for i := range events {
		events[i] = strings.TrimSpace(events[i])
	}
	if len(events) == 0 || (len(events) == 1 && events[0] == "") {
		events = []string{"event1", "event2"}
	}

	payloadSize := getEnvInt("LOADTEST_PAYLOAD_SIZE", 50*1024) // 50KB
	if payloadSize < 1 {
		payloadSize = 50 * 1024
	}

	poolSize := getEnvInt("LOADTEST_POOL_SIZE", 100)
	if poolSize < 1 {
		poolSize = 100
	}

	rps := getEnvInt("LOADTEST_RPS", 100)
	if rps < 0 {
		rps = 0
	}

	logPath := getEnv("LOG_FILE_PATH", "/mnt/localdisk/asyncloguploader-loadtest")

	bufferMB := getEnvInt("LOADTEST_BUFFER_MB", 64)
	maxFileMB := getEnvInt("LOADTEST_MAX_FILE_MB", 256)
	numShards := getEnvInt("LOADTEST_NUM_SHARDS", 8)
	if numShards < 1 {
		numShards = 1
	}

	cfg := &loadTestConfig{
		Config: asyncloguploader.Config{
			NumShards:   numShards,
			BufferSize:  bufferMB * 1024 * 1024,
			MaxFileSize: int64(maxFileMB) * 1024 * 1024,
			LogFilePath: logPath,
		},
		LoadTestDuration:    d,
		LoadTestGoroutines:  goroutines,
		LoadTestEvents:      events,
		LoadTestPayloadSize: payloadSize,
		LoadTestPoolSize:    poolSize,
		LoadTestRPS:         rps,
	}

	gcsEnabled := getEnv("GCS_ENABLED", "true") == "true"
	bucket := getEnv("GCS_BUCKET", "gcs-dsci-srch-search-prd")
	prefix := getEnv("GCS_PREFIX", "Image_search/gcs-flush/")

	if gcsEnabled {
		if bucket == "" {
			fmt.Fprintf(os.Stderr, "GCS_BUCKET must be set when GCS_ENABLED=true\n")
			os.Exit(1)
		}
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		pollStr := getEnv("GCS_POLL_INTERVAL", "3s")
		poll, _ := time.ParseDuration(pollStr)
		if poll <= 0 {
			poll = 3 * time.Second
		}
		cfg.GCSUploadConfig = &asyncloguploader.GCSUploadConfig{
			BucketName:   bucket,
			ObjectPrefix: prefix,
			ChunkSize:    32 * 1024 * 1024,
			MaxRetries:   3,
			PollInterval: poll,
		}
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return defaultVal
}
