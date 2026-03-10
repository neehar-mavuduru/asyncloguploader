package asyncloguploader

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdversarial_CAS_HighContention_NoCorruption(t *testing.T) {
	var mu sync.Mutex
	buf, cleanup, err := NewBuffer(int32(alignTo4096(8*1024*1024)), 0, &mu)
	require.NoError(t, err)
	defer cleanup()

	const goroutines = 500
	const payloadSize = 16

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			payload := make([]byte, payloadSize)
			binary.BigEndian.PutUint64(payload, uint64(gid))
			binary.BigEndian.PutUint64(payload[8:], uint64(gid*1000))
			n, _ := buf.Write(payload)
			require.Greater(t, n, 0)
		}(g)
	}
	wg.Wait()

	data := buf.data[headerOffset:buf.offset.Load()]
	seen := make(map[uint64]bool)
	offset := 0
	for offset+4 <= len(data) {
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if length == 0 {
			continue
		}
		require.Equal(t, uint32(payloadSize), length, "unexpected record length at offset %d", offset-4)
		gid := binary.BigEndian.Uint64(data[offset:])
		verify := binary.BigEndian.Uint64(data[offset+8:])
		require.Equal(t, gid*1000, verify, "corrupted record for goroutine %d", gid)
		require.False(t, seen[gid], "duplicate record for goroutine %d", gid)
		seen[gid] = true
		offset += int(length)
	}
	assert.Len(t, seen, goroutines)
}

func TestAdversarial_DoubleSwap_NoBufferLeak(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		flushChan := make(chan *Buffer, 100)
		s, err := NewShard(0, int32(alignTo4096(1024*1024)), flushChan)
		require.NoError(t, err)

		s.Write(make([]byte, 64))

		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			s.trySwap()
		}()
		go func() {
			defer wg.Done()
			<-start
			s.trySwap()
		}()
		close(start)
		wg.Wait()

		assert.LessOrEqual(t, len(flushChan), 1)
		assert.False(t, s.swapping.Load())
		s.Close()
	}
}

func TestAdversarial_WriteAfterSwapBeforeFlush(t *testing.T) {
	flushChan := make(chan *Buffer, 10)
	s, err := NewShard(0, int32(alignTo4096(1024*1024)), flushChan)
	require.NoError(t, err)
	defer s.Close()

	s.Write([]byte("before-swap"))
	beforeBuf := s.activeBuffer.Load()

	s.trySwap()
	afterBuf := s.activeBuffer.Load()
	require.NotEqual(t, beforeBuf, afterBuf, "active buffer should have changed")

	n, _ := s.Write([]byte("after-swap"))
	assert.Greater(t, n, 0, "write after swap should succeed in new buffer")

	currentBuf := s.activeBuffer.Load()
	data := currentBuf.data[headerOffset:currentBuf.offset.Load()]
	length := binary.LittleEndian.Uint32(data)
	assert.Equal(t, uint32(len("after-swap")), length)
	assert.Equal(t, "after-swap", string(data[4:4+length]))
}

func TestAdversarial_FlushWorker_StalledDisk(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{NumShards: 1, BufferSize: 65536, MaxFileSize: 10 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()

	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	sw := &slowWriter{writeCh: make(chan struct{})}
	flushChan := make(chan *Buffer, cfg.NumShards*2)
	sc, err := NewShardCollection(cfg.NumShards, cfg.BufferSize, flushChan)
	require.NoError(t, err)

	l := &Logger{
		shardCollection: sc, fileWriter: sw, flushChan: flushChan,
		done: make(chan struct{}), semaphore: make(chan struct{}, 1), config: cfg,
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

	err = l.LogBytes(payload)
	assert.ErrorIs(t, err, ErrBufferFull)

	l.closed.Store(true)
	close(l.done)
	close(sw.writeCh)
	l.wg.Wait()
	sc.Close()
}

func TestAdversarial_Rotation_PowerLoss_Simulation(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	maxSize := int64(1024)
	fw, err := NewSizeFileWriter(filepath.Join(logsDir, "test"), logsDir, maxSize)
	require.NoError(t, err)

	past90 := make([]byte, int(float64(maxSize)*0.92))
	_, err = fw.WriteVectored([][]byte{past90})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return fw.nextFileReady.Load() }, 200*time.Millisecond, 5*time.Millisecond)

	os.Remove(fw.nextFileTmpPath)

	remaining := make([]byte, int(maxSize)-len(past90)+1)
	assert.NotPanics(t, func() {
		fw.WriteVectored([][]byte{remaining})
	})

	require.NoError(t, fw.Close())
}

func TestAdversarial_ConcurrentLoggerCreation_RaceCondition(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{NumShards: 2, BufferSize: 256 * 1024, MaxFileSize: 10 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	lm, err := NewLoggerManager(cfg)
	require.NoError(t, err)
	defer lm.Close()

	const goroutines = 200
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := lm.LogBytesWithEvent("new_event", []byte(fmt.Sprintf("data-%d", id)))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	logsDir := filepath.Join(dir, "logs")
	entries, _ := os.ReadDir(logsDir)
	tmpCount := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") && strings.HasPrefix(e.Name(), "new_event") {
			tmpCount++
		}
	}
	assert.Equal(t, 1, tmpCount, "exactly one logger should be created")
}

func TestAdversarial_Close_WhileWriting(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{NumShards: 4, BufferSize: 1024 * 1024, MaxFileSize: 10 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	logger, err := NewLogger(cfg, "test", logsDir)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				logger.LogBytes(make([]byte, 64))
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		logger.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked while writing")
	}

	wg.Wait()
}

func TestAdversarial_MultipleClose_Concurrent(t *testing.T) {
	dir := t.TempDir()
	cfg := makeConfig(t, dir)
	logger := newTestLogger(t, cfg)

	closeCalls := 0
	var mu sync.Mutex

	mockFw := &countingWriter{wrapped: logger.fileWriter, mu: &mu, closeCount: &closeCalls}
	logger.fileWriter = mockFw

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NotPanics(t, func() { logger.Close() })
		}()
	}
	wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, closeCalls, "fileWriter.Close should be called exactly once")
	mu.Unlock()
}

func TestAdversarial_Uploader_TmpFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	maxSize := int64(512)
	fw, err := NewSizeFileWriter(filepath.Join(logsDir, "test"), logsDir, maxSize)
	require.NoError(t, err)

	entries, _ := os.ReadDir(logsDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			t.Errorf("no .log files should exist before rotation: %s", e.Name())
		}
	}

	data := make([]byte, maxSize+1)
	_, err = fw.WriteVectored([][]byte{data})
	require.NoError(t, err)

	logFiles := findLogFiles(t, logsDir)
	assert.GreaterOrEqual(t, len(logFiles), 1, ".log file should exist after rotation")

	tmpFiles := 0
	entries, _ = os.ReadDir(logsDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			tmpFiles++
		}
	}
	assert.GreaterOrEqual(t, tmpFiles, 1, "active .tmp file should still exist")

	require.NoError(t, fw.Close())
}

type countingWriter struct {
	wrapped    FileWriter
	mu         *sync.Mutex
	closeCount *int
}

func (cw *countingWriter) WriteVectored(buffers [][]byte) (int, error) {
	return cw.wrapped.WriteVectored(buffers)
}
func (cw *countingWriter) GetLastPwritevDuration() time.Duration {
	return cw.wrapped.GetLastPwritevDuration()
}
func (cw *countingWriter) Close() error {
	cw.mu.Lock()
	*cw.closeCount++
	cw.mu.Unlock()
	return cw.wrapped.Close()
}
