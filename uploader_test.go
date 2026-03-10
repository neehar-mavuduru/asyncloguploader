package asyncloguploader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestUploader(t *testing.T, logsDir string) (*Uploader, *fakestorage.Server) {
	t.Helper()
	server := fakestorage.NewServer([]fakestorage.Object{})
	t.Cleanup(server.Stop)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "test-bucket"})

	cfg := GCSUploadConfig{
		BucketName:   "test-bucket",
		ObjectPrefix: "logs/",
		ChunkSize:    1024,
		MaxRetries:   2,
		GRPCPoolSize: 1,
		PollInterval: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	u := newUploader(logsDir, cfg, server.Client(), ctx, cancel)
	t.Cleanup(u.Stop)
	return u, server
}

func TestUploader_PicksUpLogFiles(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logFile := filepath.Join(logsDir, "event_2026-03-07_14-00-00_1.log")
	os.WriteFile(logFile, []byte("hello world"), 0644)

	u, _ := newTestUploader(t, logsDir)
	u.Start()

	require.Eventually(t, func() bool {
		return u.stats.FilesUploaded.Load() >= 1
	}, 3*time.Second, 50*time.Millisecond)

	assert.Equal(t, int64(11), u.stats.BytesUploaded.Load())

	_, err := os.Stat(logFile)
	assert.True(t, os.IsNotExist(err), "original file should be removed after upload")
}

func TestUploader_IgnoresTmpFiles(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	os.WriteFile(filepath.Join(logsDir, "test.log.tmp"), []byte("tmp"), 0644)

	u, _ := newTestUploader(t, logsDir)
	u.scanAndUpload()

	assert.Equal(t, int64(0), u.stats.FilesUploaded.Load())
}

func TestUploader_CrashRecovery_ProcessesExistingLogFiles(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	for i := 0; i < 3; i++ {
		name := filepath.Join(logsDir, "crash_2026-03-07_14-00-00_"+string(rune('1'+i))+".log")
		os.WriteFile(name, []byte("crash data"), 0644)
	}

	u, _ := newTestUploader(t, logsDir)
	u.Start()

	require.Eventually(t, func() bool {
		return u.stats.FilesUploaded.Load() >= 3
	}, 5*time.Second, 50*time.Millisecond)
}

func TestUploader_ObjectNameDerivation(t *testing.T) {
	u := &Uploader{
		config: GCSUploadConfig{ObjectPrefix: "prod/"},
	}

	name := u.deriveObjectName("event_2026-03-07_14-30-00_1.log")
	assert.Contains(t, name, "prod/")
	assert.Contains(t, name, "event/")           // event name in path
	assert.Contains(t, name, "2026-03-07")
	assert.Contains(t, name, "14")
	assert.Contains(t, name, "event_2026-03-07_14-30-00_1.log")
	assert.Equal(t, "prod/event/2026-03-07/14/event_2026-03-07_14-30-00_1.log", name)
}

func TestUploader_Stop_DrainsInFlight(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logFile := filepath.Join(logsDir, "drain_2026-03-07_14-00-00_1.log")
	os.WriteFile(logFile, []byte("drain test data"), 0644)

	u, _ := newTestUploader(t, logsDir)
	u.Start()

	done := make(chan struct{})
	go func() {
		u.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked")
	}
}

func TestUploader_GetStats_ReturnsSnapshot(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	u, _ := newTestUploader(t, logsDir)

	u.stats.FilesUploaded.Store(5)
	u.stats.BytesUploaded.Store(1024)

	stats := u.GetStats()
	assert.Equal(t, int64(5), stats.FilesUploaded.Load())
	assert.Equal(t, int64(1024), stats.BytesUploaded.Load())
}
