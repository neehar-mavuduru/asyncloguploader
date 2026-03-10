package asyncloguploader

import (
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Logger writes structured log data to sharded memory buffers and flushes
// them asynchronously to a size-rotated file via a background worker.
type Logger struct {
	shardCollection *ShardCollection
	fileWriter      FileWriter
	flushChan       chan *Buffer
	done            chan struct{}
	semaphore       chan struct{}
	config          Config
	stats           Statistics
	closed          atomic.Bool
	inflightWrites  atomic.Int64
	wg              sync.WaitGroup
}

// NewLogger creates a Logger backed by a SizeFileWriter in logsDir.
func NewLogger(config Config, eventName string, logsDir string) (*Logger, error) {
	flushChan := make(chan *Buffer, config.NumShards*2)

	sc, err := NewShardCollection(config.NumShards, config.BufferSize, flushChan)
	if err != nil {
		return nil, err
	}

	baseFileName := filepath.Join(logsDir, eventName)
	fw, err := NewSizeFileWriter(baseFileName, logsDir, config.MaxFileSize)
	if err != nil {
		sc.Close()
		return nil, err
	}

	l := &Logger{
		shardCollection: sc,
		fileWriter:      fw,
		flushChan:       flushChan,
		done:            make(chan struct{}),
		semaphore:       make(chan struct{}, 1),
		config:          config,
	}
	l.semaphore <- struct{}{}

	l.wg.Add(1)
	go l.flushWorker()

	return l, nil
}

// LogBytes writes a raw byte payload. This is the primary write method.
func (l *Logger) LogBytes(data []byte) error {
	l.stats.TotalLogs.Add(1)
	l.inflightWrites.Add(1)
	defer l.inflightWrites.Add(-1)

	if l.closed.Load() {
		l.stats.DroppedLogs.Add(1)
		return ErrLoggerClosed
	}

	n, _ := l.shardCollection.Write(data)
	if n > 0 {
		l.stats.BytesWritten.Add(int64(n))
		return nil
	}

	// Buffer full — acquire semaphore with 50 ms timeout.
	select {
	case <-l.semaphore:
	case <-time.After(50 * time.Millisecond):
		l.stats.DroppedLogs.Add(1)
		return ErrBufferFull
	}

	n, _ = l.shardCollection.Write(data)
	if n > 0 {
		l.stats.BytesWritten.Add(int64(n))
		l.semaphore <- struct{}{}
		return nil
	}

	l.shardCollection.FlushAll()

	n, _ = l.shardCollection.Write(data)
	if n > 0 {
		l.stats.BytesWritten.Add(int64(n))
		l.semaphore <- struct{}{}
		return nil
	}

	l.stats.DroppedLogs.Add(1)
	l.semaphore <- struct{}{}
	return ErrBufferFull
}

// Log is a convenience wrapper that converts a string to bytes without
// allocation and calls LogBytes.
func (l *Logger) Log(msg string) error {
	return l.LogBytes(stringToBytes(msg))
}

// Close gracefully shuts down the logger: flushes all remaining data,
// waits for the flush worker, and closes the file writer.
func (l *Logger) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}

	for l.inflightWrites.Load() != 0 {
		runtime.Gosched()
	}

	l.shardCollection.FlushAll()
	close(l.done)
	l.wg.Wait()
	err := l.fileWriter.Close()
	l.shardCollection.Close()
	return err
}

// GetStats returns a snapshot of the current statistics.
func (l *Logger) GetStats() *Statistics {
	s := &Statistics{}
	s.TotalLogs.Store(l.stats.TotalLogs.Load())
	s.DroppedLogs.Store(l.stats.DroppedLogs.Load())
	s.BytesWritten.Store(l.stats.BytesWritten.Load())
	return s
}

// flushWorker is the background goroutine that receives full buffers from
// shards, writes them to disk, and resets them for reuse.
func (l *Logger) flushWorker() {
	defer l.wg.Done()

	ticker := time.NewTicker(l.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case buf := <-l.flushChan:
			l.handleFlush(buf)
		case <-ticker.C:
			l.shardCollection.FlushAll()
		case <-l.done:
			l.drainFlushChan()
			return
		}
	}
}

func (l *Logger) handleFlush(first *Buffer) {
	buffers := l.collectBuffers(first)

	for _, buf := range buffers {
		buf.WaitForInflight()
	}

	dataSlices := make([][]byte, 0, len(buffers))
	for _, buf := range buffers {
		offset := buf.offset.Load()
		if offset > int32(headerOffset) {
			dataSlices = append(dataSlices, buf.data[headerOffset:offset])
		}
	}

	if len(dataSlices) > 0 {
		l.fileWriter.WriteVectored(dataSlices)
	}

	for _, buf := range buffers {
		buf.Reset()
		if int(buf.shardID) < len(l.shardCollection.shards) {
			l.shardCollection.shards[buf.shardID].readyForFlush.Store(false)
		}
	}
}

func (l *Logger) collectBuffers(first *Buffer) []*Buffer {
	buffers := []*Buffer{first}
	for {
		select {
		case buf := <-l.flushChan:
			buffers = append(buffers, buf)
		default:
			return buffers
		}
	}
}

func (l *Logger) drainFlushChan() {
	for {
		select {
		case buf := <-l.flushChan:
			l.handleFlush(buf)
		default:
			return
		}
	}
}

func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
