package asyncloguploader

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Shard implements double-buffered, lock-free writing. Two Buffers alternate
// as the active write target; when one fills up the shard atomically swaps
// to the other and sends the full buffer to a flush channel.
type Shard struct {
	bufferA       *Buffer
	bufferB       *Buffer
	activeBuffer  atomic.Pointer[Buffer]
	capacity      int32
	mu            sync.Mutex
	id            uint32
	swapping      atomic.Bool
	readyForFlush atomic.Bool
	swapSemaphore chan struct{}
	flushChan     chan<- *Buffer
	cleanupA      func()
	cleanupB      func()
}

// NewShard creates a shard with two buffers of the given capacity, wired to
// flushChan for asynchronous disk writes.
func NewShard(id uint32, capacity int32, flushChan chan<- *Buffer) (*Shard, error) {
	s := &Shard{
		id:            id,
		capacity:      capacity,
		swapSemaphore: make(chan struct{}, 1),
		flushChan:     flushChan,
	}

	var err error
	s.bufferA, s.cleanupA, err = NewBuffer(capacity, id, &s.mu)
	if err != nil {
		return nil, err
	}

	s.bufferB, s.cleanupB, err = NewBuffer(capacity, id, &s.mu)
	if err != nil {
		s.cleanupA()
		return nil, err
	}

	s.activeBuffer.Store(s.bufferA)
	s.swapSemaphore <- struct{}{}

	runtime.SetFinalizer(s, (*Shard).Close)

	return s, nil
}

// Write appends data to the active buffer. Returns (bytesWritten, bufferFull).
// On a full buffer it performs a synchronous swap; at 90% capacity it triggers
// an asynchronous swap in a background goroutine.
//
// Uses a double-check on the active buffer pointer to prevent a TOCTOU race
// between loading the buffer and the flush worker draining it.
func (s *Shard) Write(data []byte) (int, bool) {
	for {
		buf := s.activeBuffer.Load()
		buf.inflight.Add(1)

		if s.activeBuffer.Load() != buf {
			buf.inflight.Add(-1)
			continue
		}

		n, shouldSwap := buf.writeData(data)
		buf.inflight.Add(-1)

		if n == 0 && shouldSwap {
			s.trySwap()
			return 0, true
		}

		if shouldSwap {
			go s.trySwap()
		}

		return n, false
	}
}

// trySwap atomically swaps the active buffer and sends the old one to flushChan.
func (s *Shard) trySwap() {
	if !s.swapping.CompareAndSwap(false, true) {
		return
	}

	current := s.activeBuffer.Load()

	var next *Buffer
	if current == s.bufferA {
		next = s.bufferB
	} else {
		next = s.bufferA
	}

	// Don't swap to a buffer that hasn't been reset by the flush worker yet.
	if next.offset.Load() != int32(headerOffset) {
		s.swapping.Store(false)
		return
	}

	s.activeBuffer.CompareAndSwap(current, next)

	current.WaitForInflight()

	select {
	case s.flushChan <- current:
	default:
	}

	s.readyForFlush.Store(true)
	s.swapping.Store(false)
}

// Close releases both buffers and clears the runtime finalizer.
func (s *Shard) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	runtime.SetFinalizer(s, nil)
	if s.cleanupA != nil {
		s.cleanupA()
		s.cleanupA = nil
	}
	if s.cleanupB != nil {
		s.cleanupB()
		s.cleanupB = nil
	}
}
