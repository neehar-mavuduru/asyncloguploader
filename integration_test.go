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

func TestIntegration_EndToEnd_WriteFlushRotateClose(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   4,
		BufferSize:  1024 * 1024,
		MaxFileSize: 4096,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logger, err := NewLogger(cfg, "events", logsDir)
	require.NoError(t, err)

	const numMessages = 500
	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = fmt.Sprintf("event-%04d-payload-data", i)
		err := logger.LogBytes([]byte(messages[i]))
		require.NoError(t, err)
	}

	require.NoError(t, logger.Close())

	assertNoTmpFiles(t, logsDir)

	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1, "should have at least one rotated file")

	seen := make(map[string]bool)
	for _, f := range logFiles {
		for _, rec := range readAllRecords(t, f) {
			seen[string(rec)] = true
		}
	}

	for _, msg := range messages {
		assert.True(t, seen[msg], "missing message: %s", msg)
	}

	stats := logger.GetStats()
	assert.Equal(t, int64(numMessages), stats.TotalLogs.Load())
	assert.Equal(t, int64(0), stats.DroppedLogs.Load())
}

func TestIntegration_MultiEvent_ConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   4,
		BufferSize:  2 * 1024 * 1024,
		MaxFileSize: 8192,
		LogFilePath: dir,
	}
	lm, err := NewLoggerManager(cfg)
	require.NoError(t, err)

	events := []string{"payments", "logins", "clicks", "errors"}
	const msgsPerEvent = 200
	const goroutines = 10

	var wg sync.WaitGroup
	for _, event := range events {
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(ev string, gid int) {
				defer wg.Done()
				for m := 0; m < msgsPerEvent; m++ {
					msg := fmt.Sprintf("%s-g%d-m%d", ev, gid, m)
					lm.LogBytesWithEvent(ev, []byte(msg))
				}
			}(event, g)
		}
	}
	wg.Wait()
	require.NoError(t, lm.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), len(events))

	allRecords := make(map[string]bool)
	for _, f := range logFiles {
		for _, rec := range readAllRecords(t, f) {
			allRecords[string(rec)] = true
		}
	}

	for _, event := range events {
		for g := 0; g < goroutines; g++ {
			for m := 0; m < msgsPerEvent; m++ {
				msg := fmt.Sprintf("%s-g%d-m%d", event, g, m)
				assert.True(t, allRecords[msg], "missing: %s", msg)
			}
		}
	}
}

func TestIntegration_Rotation_ProducesMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   2,
		BufferSize:  1024 * 1024,
		MaxFileSize: 2048,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logger, err := NewLogger(cfg, "rotate_test", logsDir)
	require.NoError(t, err)

	totalData := 0
	payload := make([]byte, 200)
	for totalData < 20000 {
		logger.LogBytes(payload)
		totalData += len(payload) + 4
	}

	require.NoError(t, logger.Close())

	logFiles := findLogFiles(t, logsDir)
	assert.Greater(t, len(logFiles), 1, "rotation should produce multiple files")
	assertNoTmpFiles(t, logsDir)
}

func TestIntegration_IntervalFlush_SmallWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   1,
		BufferSize:  1024 * 1024,
		MaxFileSize: 10 * 1024 * 1024,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logger, err := NewLogger(cfg, "interval", logsDir)
	require.NoError(t, err)

	require.NoError(t, logger.LogBytes([]byte("tiny payload")))

	time.Sleep(700 * time.Millisecond)

	require.NoError(t, logger.Close())

	logFiles := findLogFiles(t, logsDir)
	require.Len(t, logFiles, 1)

	records := readAllRecords(t, logFiles[0])
	found := false
	for _, r := range records {
		if string(r) == "tiny payload" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestIntegration_SealedFilesAreDiscoverable(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NumShards:   2,
		BufferSize:  512 * 1024,
		MaxFileSize: 1024,
		LogFilePath: dir,
	}
	require.NoError(t, cfg.Validate())

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logger, err := NewLogger(cfg, "discover", logsDir)
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		logger.LogBytes([]byte(fmt.Sprintf("msg-%d", i)))
	}
	require.NoError(t, logger.Close())

	logFiles := findLogFiles(t, logsDir)
	assert.GreaterOrEqual(t, len(logFiles), 1)

	nonEmptyCount := 0
	for _, f := range logFiles {
		info, err := os.Stat(f)
		require.NoError(t, err)
		if info.Size() > 0 {
			nonEmptyCount++
		}
	}
	assert.Greater(t, nonEmptyCount, 0, "at least one sealed .log file should have content")

	assertNoTmpFiles(t, logsDir)
}
