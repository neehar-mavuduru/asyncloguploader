package asyncloguploader

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBuffer(t *testing.T, capacity int32) (*Buffer, func()) {
	t.Helper()
	var mu sync.Mutex
	buf, cleanup, err := NewBuffer(capacity, 0, &mu)
	require.NoError(t, err)
	return buf, cleanup
}

func TestBuffer_InitialOffset(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	assert.Equal(t, int32(headerOffset), buf.offset.Load())
}

func TestBuffer_Write_LengthPrefix(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	payload := []byte("hello")
	n, _ := buf.Write(payload)
	require.Greater(t, n, 0)

	stored := binary.LittleEndian.Uint32(buf.data[headerOffset:])
	assert.Equal(t, uint32(len(payload)), stored)
	assert.Equal(t, payload, buf.data[headerOffset+4:headerOffset+4+len(payload)])
}

func TestBuffer_Write_OffsetAdvances(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 8192)
	defer cleanup()

	p1 := []byte("hello")
	n1, _ := buf.Write(p1)
	require.Greater(t, n1, 0)
	expected1 := int32(headerOffset + 4 + int(len(p1)))
	assert.Equal(t, expected1, buf.offset.Load())

	p2 := []byte("world!")
	n2, _ := buf.Write(p2)
	require.Greater(t, n2, 0)
	expected2 := expected1 + int32(4+len(p2))
	assert.Equal(t, expected2, buf.offset.Load())
}

func TestBuffer_Write_FullBuffer(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	payload := make([]byte, 100)
	var writtenPayloads [][]byte

	for {
		p := make([]byte, 100)
		copy(p, payload)
		binary.BigEndian.PutUint64(p, uint64(len(writtenPayloads)))

		n, full := buf.Write(p)
		if n == 0 && full {
			break
		}
		writtenPayloads = append(writtenPayloads, p)
	}

	require.Greater(t, len(writtenPayloads), 0, "should have written at least one payload")

	// Verify all written payloads are intact.
	data := buf.data[headerOffset:buf.offset.Load()]
	offset := 0
	for i := 0; i < len(writtenPayloads); i++ {
		length := binary.LittleEndian.Uint32(data[offset:])
		assert.Equal(t, uint32(100), length)
		offset += 4
		assert.Equal(t, writtenPayloads[i], data[offset:offset+100])
		offset += 100
	}
}

func TestBuffer_Write_ProactiveSwapSignal(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	payload := make([]byte, 100)
	var sawSignal bool

	for {
		n, swap := buf.Write(payload)
		if n == 0 {
			break
		}
		if swap {
			sawSignal = true
			break
		}
	}

	assert.True(t, sawSignal, "should have received proactive swap signal at ~90%% capacity")
	usedBytes := buf.offset.Load()
	threshold := int32(float64(buf.capacity) * 0.9)
	assert.GreaterOrEqual(t, usedBytes, threshold)
}

func TestBuffer_Write_CASRetry(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 1024*1024)
	defer cleanup()

	const numGoroutines = 50
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := make([]byte, 8)
			binary.BigEndian.PutUint64(payload, uint64(id))
			n, _ := buf.Write(payload)
			require.Greater(t, n, 0, "write should succeed for goroutine %d", id)
		}(i)
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
		require.Equal(t, uint32(8), length)
		id := binary.BigEndian.Uint64(data[offset : offset+8])
		require.False(t, seen[id], "duplicate payload: %d", id)
		seen[id] = true
		offset += int(length)
	}
	assert.Len(t, seen, numGoroutines)
}

func TestBuffer_Reset(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	buf.Write([]byte("test data"))
	buf.Reset()

	assert.Equal(t, int32(headerOffset), buf.offset.Load())
	assert.Equal(t, int64(0), buf.inflight.Load())
	for _, b := range buf.data[headerOffset:100] {
		assert.Equal(t, byte(0), b)
	}
}

func TestBuffer_WaitForInflight_Blocks(t *testing.T) {
	buf, cleanup := newTestBuffer(t, 4096)
	defer cleanup()

	buf.inflight.Add(1)

	done := make(chan struct{})
	go func() {
		buf.WaitForInflight()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("WaitForInflight returned while inflight > 0")
	case <-time.After(50 * time.Millisecond):
	}

	buf.inflight.Add(-1)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("WaitForInflight did not return after inflight reached 0")
	}
}

func TestBuffer_Close_PreventsFurtherWrites(t *testing.T) {
	buf, _ := newTestBuffer(t, 4096)
	buf.Close()

	n, full := buf.Write([]byte("data"))
	assert.Equal(t, 0, n)
	assert.True(t, full)
}
