package asyncloguploader

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func makeConfig(t *testing.T, baseDir string) Config {
	t.Helper()
	return Config{
		NumShards:   4,
		BufferSize:  1024 * 1024,
		MaxFileSize: 512 * 1024,
		LogFilePath: baseDir,
	}
}

func newTestLogger(t *testing.T, cfg Config) *Logger {
	t.Helper()
	logsDir := filepath.Join(cfg.LogFilePath, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatal(err)
	}

	logger, err := NewLogger(cfg, "test", logsDir)
	if err != nil {
		t.Fatalf("failed to create test logger: %v", err)
	}
	t.Cleanup(func() { logger.Close() })
	return logger
}

func readAllRecords(t *testing.T, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read log file %s: %v", path, err)
	}

	var records [][]byte
	offset := 0
	for offset+4 <= len(data) {
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if length == 0 {
			continue
		}
		if offset+int(length) > len(data) {
			t.Fatalf("malformed record at offset %d: length %d exceeds data size %d",
				offset-4, length, len(data))
		}
		record := make([]byte, length)
		copy(record, data[offset:offset+int(length)])
		records = append(records, record)
		offset += int(length)
	}
	return records
}

func findLogFiles(t *testing.T, logsDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatalf("failed to read logs dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") && !strings.HasSuffix(e.Name(), ".tmp") {
			files = append(files, filepath.Join(logsDir, e.Name()))
		}
	}
	return files
}

func assertNoTmpFiles(t *testing.T, logsDir string) {
	t.Helper()
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatalf("failed to read logs dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("unexpected .tmp file remains: %s", e.Name())
		}
	}
}
