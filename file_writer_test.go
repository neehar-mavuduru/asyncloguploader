package asyncloguploader

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestFileWriter(t *testing.T, maxFileSize int64) (*SizeFileWriter, string) {
	t.Helper()
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	require.NoError(t, os.MkdirAll(logsDir, 0755))

	fw, err := NewSizeFileWriter(filepath.Join(logsDir, "test"), logsDir, maxFileSize)
	require.NoError(t, err)
	return fw, logsDir
}

func TestSizeFileWriter_TmpSuffix_OnCreate(t *testing.T) {
	fw, logsDir := newTestFileWriter(t, 1024*1024)
	defer fw.Close()

	entries, err := os.ReadDir(logsDir)
	require.NoError(t, err)

	hasTmp := false
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			hasTmp = true
		}
		assert.NotEqual(t, ".log", filepath.Ext(e.Name()), "no .log file should exist yet")
	}
	assert.True(t, hasTmp, "a .tmp file should exist")
}

func TestSizeFileWriter_Rotation_RenamesTmpToFinal(t *testing.T) {
	maxSize := int64(1024)
	fw, logsDir := newTestFileWriter(t, maxSize)
	defer fw.Close()

	data := make([]byte, maxSize+1)
	for i := range data {
		data[i] = 'A'
	}
	_, err := fw.WriteVectored([][]byte{data})
	require.NoError(t, err)

	logFiles := findLogFiles(t, logsDir)
	assert.GreaterOrEqual(t, len(logFiles), 1, "at least one .log file should exist after rotation")
}

func TestSizeFileWriter_ProactiveNextFile_CreatedAt90Percent(t *testing.T) {
	maxSize := int64(4096)
	fw, logsDir := newTestFileWriter(t, maxSize)
	defer fw.Close()

	under90 := make([]byte, int(float64(maxSize)*0.85))
	_, err := fw.WriteVectored([][]byte{under90})
	require.NoError(t, err)

	entries, _ := os.ReadDir(logsDir)
	tmpCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			tmpCount++
		}
	}
	assert.Equal(t, 1, tmpCount, "only the current .tmp file should exist below 90%%")

	over90 := make([]byte, int(float64(maxSize)*0.06)+1)
	_, err = fw.WriteVectored([][]byte{over90})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(logsDir)
		count := 0
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".tmp" {
				count++
			}
		}
		return count >= 2
	}, 200*time.Millisecond, 5*time.Millisecond, "proactive next file should be created after crossing 90%%")
}

func TestSizeFileWriter_Rotation_PromotesPreallocatedNextFile(t *testing.T) {
	maxSize := int64(2048)
	fw, logsDir := newTestFileWriter(t, maxSize)
	defer fw.Close()

	past90 := make([]byte, int(float64(maxSize)*0.92))
	_, err := fw.WriteVectored([][]byte{past90})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return fw.nextFileReady.Load()
	}, 200*time.Millisecond, 5*time.Millisecond)

	remaining := make([]byte, int(maxSize)-len(past90)+1)
	_, err = fw.WriteVectored([][]byte{remaining})
	require.NoError(t, err)

	logFiles := findLogFiles(t, logsDir)
	assert.GreaterOrEqual(t, len(logFiles), 1)
}

func TestSizeFileWriter_WriteVectored_MultipleBuffers(t *testing.T) {
	fw, logsDir := newTestFileWriter(t, 1024*1024)

	bufs := [][]byte{
		[]byte("AAAA"),
		[]byte("BBBB"),
		[]byte("CCCC"),
		[]byte("DDDD"),
	}
	_, err := fw.WriteVectored(bufs)
	require.NoError(t, err)
	require.NoError(t, fw.Close())

	logFiles := findLogFiles(t, logsDir)
	require.Len(t, logFiles, 1)

	data, err := os.ReadFile(logFiles[0])
	require.NoError(t, err)
	assert.Equal(t, "AAAABBBBCCCCDDDD", string(data))
}

func TestSizeFileWriter_Close_FinalFileIsNotTmp(t *testing.T) {
	fw, logsDir := newTestFileWriter(t, 1024*1024)

	_, err := fw.WriteVectored([][]byte{[]byte("some data")})
	require.NoError(t, err)
	require.NoError(t, fw.Close())

	assertNoTmpFiles(t, logsDir)

	logFiles := findLogFiles(t, logsDir)
	assert.Len(t, logFiles, 1)
}
