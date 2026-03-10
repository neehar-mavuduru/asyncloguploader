package asyncloguploader

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestShard(t *testing.T, capacity int32) (*Shard, chan *Buffer) {
	t.Helper()
	flushChan := make(chan *Buffer, 100)
	s, err := NewShard(0, capacity, flushChan)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s, flushChan
}

func TestShard_Write_BasicDataIntegrity(t *testing.T) {
	s, _ := newTestShard(t, int32(alignTo4096(1024*1024)))

	payload := []byte("hello shard")
	n, full := s.Write(payload)
	require.Greater(t, n, 0)
	assert.False(t, full)

	buf := s.activeBuffer.Load()
	stored := binary.LittleEndian.Uint32(buf.data[headerOffset:])
	assert.Equal(t, uint32(len(payload)), stored)
	assert.Equal(t, payload, buf.data[headerOffset+4:headerOffset+4+len(payload)])
}

func TestShard_Write_LockFreeUnderContention(t *testing.T) {
	s, _ := newTestShard(t, int32(alignTo4096(16*1024*1024)))

	const goroutines = 200
	const msgsPerGoroutine = 100
	const payloadSize = 64

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for m := 0; m < msgsPerGoroutine; m++ {
				payload := make([]byte, payloadSize)
				binary.BigEndian.PutUint32(payload, uint32(gid))
				binary.BigEndian.PutUint32(payload[4:], uint32(m))
				n, _ := s.Write(payload)
				assert.Greater(t, n, 0)
			}
		}(g)
	}
	wg.Wait()

	buf := s.activeBuffer.Load()
	data := buf.data[headerOffset:buf.offset.Load()]
	count := 0
	offset := 0
	for offset+4 <= len(data) {
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if length == 0 {
			continue
		}
		assert.Equal(t, uint32(payloadSize), length)
		offset += int(length)
		count++
	}
	assert.Equal(t, goroutines*msgsPerGoroutine, count)
}

func TestShard_TrySwap_OnlyOneGoroutineSwaps(t *testing.T) {
	flushChan := make(chan *Buffer, 200)
	s, err := NewShard(0, int32(alignTo4096(1024*1024)), flushChan)
	require.NoError(t, err)
	defer s.Close()

	s.Write(make([]byte, 64))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.trySwap()
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, len(flushChan), "exactly one buffer should be in flushChan")
	assert.False(t, s.swapping.Load())
}

func TestShard_TrySwap_BufferAlternation(t *testing.T) {
	flushChan := make(chan *Buffer, 10)
	s, err := NewShard(0, int32(alignTo4096(1024*1024)), flushChan)
	require.NoError(t, err)
	defer s.Close()

	assert.True(t, s.activeBuffer.Load() == s.bufferA, "initial active should be bufferA")

	s.Write(make([]byte, 64))
	s.trySwap()
	assert.True(t, s.activeBuffer.Load() == s.bufferB, "after first swap, active should be bufferB")

	buf := <-flushChan
	buf.Reset()
	s.readyForFlush.Store(false)

	s.Write(make([]byte, 64))
	s.trySwap()
	assert.True(t, s.activeBuffer.Load() == s.bufferA, "after second swap, active should be bufferA")
}

func TestShard_TrySwap_WaitsForInflight(t *testing.T) {
	flushChan := make(chan *Buffer, 10)
	s, err := NewShard(0, int32(alignTo4096(1024*1024)), flushChan)
	require.NoError(t, err)
	defer s.Close()

	s.Write(make([]byte, 64))
	activeBuf := s.activeBuffer.Load()
	activeBuf.inflight.Add(5)

	done := make(chan struct{})
	go func() {
		s.trySwap()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("trySwap completed with inflight > 0")
	case <-time.After(50 * time.Millisecond):
	}

	activeBuf.inflight.Add(-5)

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("trySwap did not complete after inflight reached 0")
	}
}

func TestShard_Write_TriggersProactiveSwap(t *testing.T) {
	capacity := int32(alignTo4096(4096))
	s, _ := newTestShard(t, capacity)

	payload := make([]byte, 100)
	for {
		n, _ := s.Write(payload)
		if n == 0 {
			break
		}
	}

	require.Eventually(t, func() bool {
		return s.readyForFlush.Load()
	}, 100*time.Millisecond, time.Millisecond)
}

func TestShard_Close_DoesNotPanic(t *testing.T) {
	flushChan := make(chan *Buffer, 10)
	s, err := NewShard(0, int32(alignTo4096(4096)), flushChan)
	require.NoError(t, err)

	assert.NotPanics(t, func() { s.Close() })
	assert.NotPanics(t, func() { s.Close() })
}

func TestShard_Close_ReleasesMemory(t *testing.T) {
	flushChan := make(chan *Buffer, 10)
	cleanupACalled := 0
	cleanupBCalled := 0

	s, err := NewShard(0, int32(alignTo4096(4096)), flushChan)
	require.NoError(t, err)

	origCleanupA := s.cleanupA
	origCleanupB := s.cleanupB
	s.cleanupA = func() { cleanupACalled++; origCleanupA() }
	s.cleanupB = func() { cleanupBCalled++; origCleanupB() }

	s.Close()

	assert.Equal(t, 1, cleanupACalled)
	assert.Equal(t, 1, cleanupBCalled)
}
