package asyncloguploader

import (
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
)

// headerOffset reserves 8 bytes at the start of each buffer:
//   - bytes 0-3: block size (uint32, = capacity)
//   - bytes 4-7: valid data offset (uint32, = write offset at flush time)
// This header makes each on-disk block self-describing for the reader.
const headerOffset = 8

// Buffer is a lock-free, CAS-based write buffer backed by mmap on Linux
// or a heap-allocated byte slice on other platforms. The entire buffer
// (including the header) is flushed to disk as a single block, enabling
// zero-copy writes: no allocation or memcpy in the flush path.
type Buffer struct {
	data     []byte
	offset   atomic.Int32
	inflight atomic.Int64
	capacity int32
	shardID  uint32
	mu       *sync.Mutex
}

// NewBuffer allocates a buffer of the given capacity (aligned to 4096) and
// associates it with the supplied shard mutex. Returns the buffer, a cleanup
// closure, and any allocation error.
func NewBuffer(capacity int32, shardID uint32, mu *sync.Mutex) (*Buffer, func(), error) {
	alignedCap := alignTo4096(int(capacity))

	data, err := mmapAlloc(alignedCap)
	if err != nil {
		return nil, nil, err
	}

	b := &Buffer{
		data:     data,
		capacity: int32(alignedCap),
		shardID:  shardID,
		mu:       mu,
	}
	b.offset.Store(int32(headerOffset))

	cleanup := func() {
		b.Close()
	}

	return b, cleanup, nil
}

// writeData performs the lock-free CAS write without inflight tracking.
// Callers that need inflight guarantees (e.g. Shard) manage inflight
// externally with a double-check on the active buffer pointer.
func (b *Buffer) writeData(data []byte) (int, bool) {
	if b.data == nil {
		return 0, true
	}

	totalSize := int32(4 + len(data))

	for {
		currentOffset := b.offset.Load()
		if currentOffset+totalSize >= b.capacity {
			return 0, true
		}

		if !b.offset.CompareAndSwap(currentOffset, currentOffset+totalSize) {
			continue
		}

		binary.LittleEndian.PutUint32(b.data[currentOffset:], uint32(len(data)))
		copy(b.data[currentOffset+4:], data)

		newOffset := currentOffset + totalSize
		threshold := int32(float64(b.capacity) * 0.9)
		return int(totalSize), newOffset >= threshold
	}
}

// Write appends a length-prefixed record to the buffer using CAS for lock-free
// concurrency. Returns (bytesWritten, shouldSwap). If the buffer is full,
// returns (0, true). If the buffer has crossed 90% capacity, the second return
// value is true to signal a proactive swap.
func (b *Buffer) Write(data []byte) (int, bool) {
	b.inflight.Add(1)
	n, swap := b.writeData(data)
	b.inflight.Add(-1)
	return n, swap
}

// Reset zeroes the data slice and resets the write offset to headerOffset.
func (b *Buffer) Reset() {
	b.offset.Store(int32(headerOffset))
	clear(b.data)
}

// WaitForInflight spins until all in-flight writes have completed.
func (b *Buffer) WaitForInflight() {
	for b.inflight.Load() != 0 {
		runtime.Gosched()
	}
}

// Close releases the buffer's backing memory.
func (b *Buffer) Close() {
	if b.data != nil {
		mmapFree(b.data)
		b.data = nil
	}
}

// PrepareForFlush writes the block header (block size + valid offset) into
// the first 8 bytes. Must be called after WaitForInflight and before passing
// the buffer to WriteVectored.
func (b *Buffer) PrepareForFlush() {
	binary.LittleEndian.PutUint32(b.data[0:4], uint32(b.capacity))
	binary.LittleEndian.PutUint32(b.data[4:8], uint32(b.offset.Load()))
}

// FullBlock returns the entire buffer as a slice [0:capacity]. The address is
// page-aligned (mmap on Linux) and the size is 4096-aligned, satisfying
// O_DIRECT requirements without any copy.
func (b *Buffer) FullBlock() []byte {
	return b.data[:b.capacity]
}

// HasData returns true if any records have been written beyond the header.
func (b *Buffer) HasData() bool {
	return b.offset.Load() > int32(headerOffset)
}

func alignTo4096(n int) int {
	if n <= 0 {
		return 4096
	}
	return ((n + 4095) / 4096) * 4096
}
