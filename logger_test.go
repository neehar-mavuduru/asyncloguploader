package asyncloguploader

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogger_LogBytes_BasicWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	cfg.MaxFileSize = 1024 * 1024
	logger := newTestLogger(t, cfg)

	payload := []byte("hello world")
	require.NoError(t, logger.LogBytes(payload))
	require.NoError(t, logger.Close())

	logsDir := filepath.Join(dir, "logs")
	assertNoTmpFiles(t, logsDir)

	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1)

	var allRecords [][]byte
	for _, f := range logFiles {
		allRecords = append(allRecords, readAllRecords(t, f)...)
	}
	found := false
	for _, r := range allRecords {
		if string(r) == "hello world" {
			found = true
		}
	}
	assert.True(t, found, "payload should appear in log file")
}

func TestLogger_LogBytes_StatsTracking(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	cfg.MaxFileSize = 10 * 1024 * 1024
	logger := newTestLogger(t, cfg)

	for i := 0; i < 1000; i++ {
		require.NoError(t, logger.LogBytes([]byte("test message")))
	}

	stats := logger.GetStats()
	assert.Equal(t, int64(1000), stats.TotalLogs.Load())
	assert.Greater(t, stats.BytesWritten.Load(), int64(0))
	assert.Equal(t, int64(0), stats.DroppedLogs.Load())
}

func TestLogger_LogBytes_DropsWhenClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	logger := newTestLogger(t, cfg)

	require.NoError(t, logger.Close())

	err := logger.LogBytes([]byte("after close"))
	assert.ErrorIs(t, err, ErrLoggerClosed)
	assert.Equal(t, int64(1), logger.stats.DroppedLogs.Load())
}

func TestLogger_LogBytes_DropsOnFullBuffer_WithTimeout(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   1,
		BufferSize:  65536,
		MaxFileSize: 10 * 1024 * 1024,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	sw := &slowWriter{writeCh: make(chan struct{})}

	flushChan := make(chan *Buffer, cfg.NumShards*2)
	sc, err := NewShardCollection(cfg.NumShards, cfg.BufferSize, flushChan)
	require.NoError(t, err)

	l := &Logger{
		shardCollection: sc,
		fileWriter:      sw,
		flushChan:       flushChan,
		done:            make(chan struct{}),
		semaphore:       make(chan struct{}, 1),
		config:          cfg,
	}
	l.semaphore <- struct{}{}
	l.wg.Add(1)
	go l.flushWorker()

	payload := make([]byte, 1000)
	var fillErr error
	for i := 0; i < 100000; i++ {
		fillErr = l.LogBytes(payload)
		if fillErr != nil {
			break
		}
	}
	require.ErrorIs(t, fillErr, ErrBufferFull)

	start := time.Now()
	err = l.LogBytes(payload)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, ErrBufferFull)
	assert.Less(t, elapsed, 200*time.Millisecond)
	assert.GreaterOrEqual(t, l.stats.DroppedLogs.Load(), int64(1))

	l.closed.Store(true)
	close(l.done)
	close(sw.writeCh)
	l.wg.Wait()
	sc.Close()
}

func TestLogger_Concurrent_NoDataLoss(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	cfg.MaxFileSize = 10 * 1024 * 1024
	cfg.BufferSize = 4 * 1024 * 1024
	logger := newTestLogger(t, cfg)

	const goroutines = 50
	const msgsPerGoroutine = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for m := 0; m < msgsPerGoroutine; m++ {
				msg := fmt.Sprintf("g%d-m%d", gid, m)
				logger.LogBytes([]byte(msg))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, logger.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1)

	seen := make(map[string]bool)
	for _, f := range logFiles {
		for _, rec := range readAllRecords(t, f) {
			seen[string(rec)] = true
		}
	}

	for g := 0; g < goroutines; g++ {
		for m := 0; m < msgsPerGoroutine; m++ {
			msg := fmt.Sprintf("g%d-m%d", g, m)
			assert.True(t, seen[msg], "missing message: %s", msg)
		}
	}
}

func TestLogger_Concurrent_NoDeadlock(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   2,
		BufferSize:  256 * 1024,
		MaxFileSize: 10 * 1024 * 1024,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())
	logger := newTestLogger(t, cfg)

	const goroutines = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := 0; m < 100; m++ {
				logger.LogBytes(make([]byte, 64))
			}
		}()
	}
	wg.Wait()

	done := make(chan struct{})
	go func() {
		logger.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() deadlocked")
	}

	stats := logger.GetStats()
	total := stats.TotalLogs.Load()
	dropped := stats.DroppedLogs.Load()
	assert.Equal(t, int64(goroutines*100), total)
	assert.GreaterOrEqual(t, total, dropped)
}

func TestLogger_FlushWorker_IntervalFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	cfg.MaxFileSize = 10 * 1024 * 1024
	cfg.BufferSize = 4 * 1024 * 1024
	logger := newTestLogger(t, cfg)

	require.NoError(t, logger.LogBytes([]byte("small payload")))

	time.Sleep(600 * time.Millisecond)

	require.NoError(t, logger.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1)

	var allRecords [][]byte
	for _, f := range logFiles {
		allRecords = append(allRecords, readAllRecords(t, f)...)
	}
	found := false
	for _, r := range allRecords {
		if string(r) == "small payload" {
			found = true
		}
	}
	assert.True(t, found, "interval flush should flush small payload")
}

func TestLogger_Shutdown_NoDataLoss(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	cfg.MaxFileSize = 10 * 1024 * 1024
	cfg.BufferSize = 4 * 1024 * 1024
	logger := newTestLogger(t, cfg)

	for i := 0; i < 500; i++ {
		logger.LogBytes([]byte(fmt.Sprintf("msg-%d", i)))
	}
	require.NoError(t, logger.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1)

	seen := make(map[string]bool)
	for _, f := range logFiles {
		for _, rec := range readAllRecords(t, f) {
			seen[string(rec)] = true
		}
	}

	for i := 0; i < 500; i++ {
		msg := fmt.Sprintf("msg-%d", i)
		assert.True(t, seen[msg], "missing: %s", msg)
	}
}

func TestLogger_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	logger := newTestLogger(t, cfg)

	assert.NotPanics(t, func() {
		logger.Close()
		logger.Close()
		logger.Close()
	})
}

// slowWriter blocks WriteVectored until writeCh is closed.
type slowWriter struct {
	writeCh chan struct{}
}

func (sw *slowWriter) WriteVectored(buffers [][]byte) (int, error) {
	<-sw.writeCh
	total := 0
	for _, b := range buffers {
		total += len(b)
	}
	return total, nil
}

func (sw *slowWriter) GetLastPwritevDuration() time.Duration { return 0 }
func (sw *slowWriter) Close() error                          { return nil }
