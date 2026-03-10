package asyncloguploader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLoggerManager(t *testing.T) (*LoggerManager, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		NumShards:   4,
		BufferSize:  1024 * 1024,
		MaxFileSize: 512 * 1024,
		LogFilePath: dir,
	}
	lm, err := NewLoggerManager(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { lm.Close() })
	return lm, dir
}

func TestLoggerManager_CreatesDirectories(t *testing.T) {
	_, dir := newTestLoggerManager(t)

	_, err := os.Stat(filepath.Join(dir, "logs"))
	assert.NoError(t, err)
}

func TestLoggerManager_LazyLoggerCreation(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	logsDir := filepath.Join(dir, "logs")
	entries, _ := os.ReadDir(logsDir)
	assert.Empty(t, entries, "no files should exist before first write")

	require.NoError(t, lm.LogBytesWithEvent("payments", []byte("test")))

	entries, _ = os.ReadDir(logsDir)
	hasTmp := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			hasTmp = true
		}
	}
	assert.True(t, hasTmp, "a .tmp file should exist after first write")
}

func TestLoggerManager_EventIsolation(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	for i := 0; i < 50; i++ {
		lm.LogBytesWithEvent("payments", []byte(fmt.Sprintf("pay-%d", i)))
		lm.LogBytesWithEvent("logins", []byte(fmt.Sprintf("login-%d", i)))
	}
	require.NoError(t, lm.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 2)

	paymentRecords := make(map[string]bool)
	loginRecords := make(map[string]bool)
	for _, f := range logFiles {
		base := filepath.Base(f)
		records := readAllRecords(t, f)
		for _, r := range records {
			s := string(r)
			if strings.HasPrefix(base, "payments") {
				paymentRecords[s] = true
			} else if strings.HasPrefix(base, "logins") {
				loginRecords[s] = true
			}
		}
	}

	for i := 0; i < 50; i++ {
		assert.True(t, paymentRecords[fmt.Sprintf("pay-%d", i)])
		assert.True(t, loginRecords[fmt.Sprintf("login-%d", i)])
	}
}

func TestLoggerManager_SanitizesEventName(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	require.NoError(t, lm.LogBytesWithEvent("../../etc/passwd", []byte("data")))
	require.NoError(t, lm.Close())

	logsDir := filepath.Join(dir, "logs")

	entries, err := os.ReadDir(filepath.Dir(filepath.Dir(dir)))
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotEqual(t, "passwd", e.Name(), "path traversal should not escape base dir")
	}

	logFiles := findLogFiles(t, logsDir)
	require.GreaterOrEqual(t, len(logFiles), 1)
	for _, f := range logFiles {
		base := filepath.Base(f)
		assert.NotContains(t, base, "/")
	}
}

func TestLoggerManager_SanitizesEventName_AllSpecialChars(t *testing.T) {
	input := "a/b\\c:d*e?f\"g<h>i|j k"
	sanitized := sanitizeEventName(input)
	for _, ch := range []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|", " "} {
		assert.NotContains(t, sanitized, ch)
	}
	assert.Equal(t, "a_b_c_d_e_f_g_h_i_j_k", sanitized)
}

func TestLoggerManager_SanitizesEventName_Truncation(t *testing.T) {
	long := strings.Repeat("a", 300)
	sanitized := sanitizeEventName(long)
	assert.Len(t, sanitized, 255)
}

func TestLoggerManager_ConcurrentLoggerCreation_NoLeak(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	const goroutines = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := lm.LogBytesWithEvent("same_event", []byte("data"))
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	logsDir := filepath.Join(dir, "logs")
	entries, err := os.ReadDir(logsDir)
	require.NoError(t, err)
	tmpCount := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") && strings.HasPrefix(e.Name(), "same_event") {
			tmpCount++
		}
	}
	assert.Equal(t, 1, tmpCount, "only one logger (one .tmp file) should be created")
}

func TestLoggerManager_Close_FlushesAllEvents(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	events := []string{"e1", "e2", "e3", "e4", "e5"}
	for _, ev := range events {
		require.NoError(t, lm.LogBytesWithEvent(ev, []byte("data-"+ev)))
	}
	require.NoError(t, lm.Close())

	logsDir := filepath.Join(dir, "logs")

	for _, ev := range events {
		found := false
		for _, f := range findLogFiles(t, logsDir) {
			if strings.Contains(filepath.Base(f), ev) {
				found = true
				info, _ := os.Stat(f)
				assert.Greater(t, info.Size(), int64(0))
			}
		}
		assert.True(t, found, "should have .log file for event %s", ev)
	}
}

func TestLoggerManager_CloseEventLogger_RemovesLogger(t *testing.T) {
	lm, dir := newTestLoggerManager(t)

	require.NoError(t, lm.LogBytesWithEvent("payments", []byte("first")))
	require.NoError(t, lm.CloseEventLogger("payments"))

	require.NoError(t, lm.LogBytesWithEvent("payments", []byte("second")))
	require.NoError(t, lm.Close())

	logsDir := filepath.Join(dir, "logs")
	logFiles := findLogFiles(t, logsDir)

	paymentFiles := 0
	for _, f := range logFiles {
		if strings.Contains(filepath.Base(f), "payments") {
			paymentFiles++
		}
	}
	assert.GreaterOrEqual(t, paymentFiles, 2, "closing and reopening should create separate files")
}
