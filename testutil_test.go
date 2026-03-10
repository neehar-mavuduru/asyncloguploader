package asyncloguploader

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func makeTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func makeConfig(t *testing.T, baseDir string) Config {
	t.Helper()
	return Config{
		NumShards:     4,
		BufferSize:    1024 * 1024,
		MaxFileSize:  512 * 1024,
		LogFilePath:   baseDir,
		FlushInterval: 100 * time.Millisecond, // short interval for fast tests
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

// readAllRecords parses the block-based file format. Each block has an 8-byte
// header: [4 bytes blockSize][4 bytes validOffset], followed by length-prefixed
// records in bytes [8..validOffset], and zero-padding to blockSize.
func readAllRecords(t *testing.T, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read log file %s: %v", path, err)
	}

	var records [][]byte
	pos := 0

	for pos+8 <= len(data) {
		blockSize := int(binary.LittleEndian.Uint32(data[pos:]))
		validOff := int(binary.LittleEndian.Uint32(data[pos+4:]))

		if blockSize == 0 {
			break
		}
		if pos+blockSize > len(data) {
			t.Fatalf("block at pos %d: blockSize %d exceeds file size %d", pos, blockSize, len(data))
		}
		if validOff < 8 || validOff > blockSize {
			t.Fatalf("block at pos %d: invalid validOffset %d (blockSize %d)", pos, validOff, blockSize)
		}

		offset := pos + 8
		end := pos + validOff
		for offset+4 <= end {
			length := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if length == 0 {
				continue
			}
			if offset+length > end {
				t.Fatalf("record at offset %d: length %d exceeds valid data boundary %d", offset-4, length, end)
			}
			record := make([]byte, length)
			copy(record, data[offset:offset+length])
			records = append(records, record)
			offset += length
		}

		pos += blockSize
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
