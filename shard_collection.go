package asyncloguploader

import "math/rand/v2"

// ShardCollection distributes writes across multiple shards using random
// selection and tracks a flush threshold based on the fraction of shards
// that have been swapped.
type ShardCollection struct {
	shards    []*Shard
	numShards int
	threshold int32
	flushChan chan<- *Buffer
}

// NewShardCollection creates numShards shards, each with capacity
// bufferSize/numShards (aligned to 4096).
func NewShardCollection(numShards int, bufferSize int, flushChan chan<- *Buffer) (*ShardCollection, error) {
	perShardCap := int32(alignTo4096(bufferSize / numShards))

	shards := make([]*Shard, numShards)
	for i := range shards {
		var err error
		shards[i], err = NewShard(uint32(i), perShardCap, flushChan)
		if err != nil {
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, err
		}
	}

	threshold := int32(max(1, numShards*25/100))

	return &ShardCollection{
		shards:    shards,
		numShards: numShards,
		threshold: threshold,
		flushChan: flushChan,
	}, nil
}

// Write picks a random shard and writes data to it. Returns (bytesWritten,
// flushThresholdReached).
func (sc *ShardCollection) Write(data []byte) (int, bool) {
	idx := rand.IntN(sc.numShards)
	n, _ := sc.shards[idx].Write(data)

	count := int32(sc.ReadyShardCount())
	return n, count >= sc.threshold
}

// ReadyShardCount returns the number of shards with readyForFlush set.
func (sc *ShardCollection) ReadyShardCount() int {
	count := 0
	for _, s := range sc.shards {
		if s.readyForFlush.Load() {
			count++
		}
	}
	return count
}

// FlushAll attempts to swap every shard that has pending data, sending
// their buffers to the flush channel.
func (sc *ShardCollection) FlushAll() {
	for _, s := range sc.shards {
		s.trySwap()
	}
}

// Close releases all shard resources.
func (sc *ShardCollection) Close() {
	for _, s := range sc.shards {
		s.Close()
	}
}
