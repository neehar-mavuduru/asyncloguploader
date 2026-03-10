package asyncloguploader

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkLogBytesWithEvent_HotPath(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{NumShards: 8, BufferSize: 64 * 1024 * 1024, MaxFileSize: 256 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)
	lm := &LoggerManager{baseDir: dir, logsDir: logsDir, config: cfg}
	defer lm.Close()

	lm.LogBytesWithEvent("bench", make([]byte, 64))
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lm.LogBytesWithEvent("bench", payload)
	}
}

func BenchmarkLogBytesWithEvent_Parallel_8Goroutines(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{NumShards: 8, BufferSize: 64 * 1024 * 1024, MaxFileSize: 256 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)
	lm := &LoggerManager{baseDir: dir, logsDir: logsDir, config: cfg}
	defer lm.Close()

	lm.LogBytesWithEvent("bench", make([]byte, 64))
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lm.LogBytesWithEvent("bench", payload)
		}
	})
}

func BenchmarkLogBytesWithEvent_Parallel_64Goroutines(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{NumShards: 16, BufferSize: 128 * 1024 * 1024, MaxFileSize: 256 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)
	lm := &LoggerManager{baseDir: dir, logsDir: logsDir, config: cfg}
	defer lm.Close()

	lm.LogBytesWithEvent("bench", make([]byte, 64))
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.SetParallelism(64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lm.LogBytesWithEvent("bench", payload)
		}
	})
}

func BenchmarkLogBytesWithEvent_LargePayload(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{NumShards: 8, BufferSize: 128 * 1024 * 1024, MaxFileSize: 256 * 1024 * 1024, LogFilePath: dir}
	cfg.Validate()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)
	lm := &LoggerManager{baseDir: dir, logsDir: logsDir, config: cfg}
	defer lm.Close()

	payload := make([]byte, 4096)
	lm.LogBytesWithEvent("bench", payload)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lm.LogBytesWithEvent("bench", payload)
	}
}

func BenchmarkShardWrite_NoSwap(b *testing.B) {
	flushChan := make(chan *Buffer, 100)
	s, err := NewShard(0, int32(alignTo4096(128*1024*1024)), flushChan)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	payload := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Write(payload)
	}
}

func BenchmarkShardWrite_WithSwap(b *testing.B) {
	flushChan := make(chan *Buffer, 1000)
	s, err := NewShard(0, int32(alignTo4096(8192)), flushChan)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	payload := make([]byte, 64)

	go func() {
		for buf := range flushChan {
			buf.Reset()
			s.readyForFlush.Store(false)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Write(payload)
	}
}

func BenchmarkFileWriter_WriteVectored_8Buffers(b *testing.B) {
	dir := b.TempDir()
	logsDir := filepath.Join(dir, "logs")
	os.MkdirAll(logsDir, 0755)

	fw, err := NewSizeFileWriter(filepath.Join(logsDir, "bench"), logsDir, 1024*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer fw.Close()

	bufs := make([][]byte, 8)
	for i := range bufs {
		bufs[i] = make([]byte, 1024*1024)
	}

	b.ReportAllocs()
	b.SetBytes(8 * 1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fw.WriteVectored(bufs)
	}
}

func BenchmarkSanitizeEventName(b *testing.B) {
	name := "some/event:name*with?special<chars>and|spaces here"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sanitizeEventName(name)
	}
}
