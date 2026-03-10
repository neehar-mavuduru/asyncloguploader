package asyncloguploader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardCollection_Write_DistributesAcrossShards(t *testing.T) {
	flushChan := make(chan *Buffer, 100)
	sc, err := NewShardCollection(8, 8*1024*1024, flushChan)
	require.NoError(t, err)
	defer sc.Close()

	for i := 0; i < 10000; i++ {
		n, _ := sc.Write(make([]byte, 64))
		require.Greater(t, n, 0)
	}

	shardsWithData := 0
	for _, s := range sc.shards {
		buf := s.activeBuffer.Load()
		if buf.offset.Load() > int32(headerOffset) {
			shardsWithData++
		}
	}
	assert.Greater(t, shardsWithData, 1, "writes should be distributed across multiple shards")
}

func TestShardCollection_Threshold_TriggersFlushSignal(t *testing.T) {
	flushChan := make(chan *Buffer, 100)
	sc, err := NewShardCollection(8, 8*1024*1024, flushChan)
	require.NoError(t, err)
	defer sc.Close()

	// threshold = max(1, 8*25/100) = 2
	sc.shards[0].readyForFlush.Store(true)
	sc.shards[1].readyForFlush.Store(true)

	_, flush := sc.Write(make([]byte, 8))
	assert.True(t, flush, "should trigger flush signal when threshold reached")
}

func TestShardCollection_Threshold_BelowThreshold(t *testing.T) {
	flushChan := make(chan *Buffer, 100)
	sc, err := NewShardCollection(8, 8*1024*1024, flushChan)
	require.NoError(t, err)
	defer sc.Close()

	sc.shards[0].readyForFlush.Store(true)

	_, flush := sc.Write(make([]byte, 8))
	assert.False(t, flush, "should not trigger flush signal below threshold")
}

func TestShardCollection_FlushAll_SwapsAllReadyShards(t *testing.T) {
	flushChan := make(chan *Buffer, 100)
	sc, err := NewShardCollection(8, 8*1024*1024, flushChan)
	require.NoError(t, err)
	defer sc.Close()

	for i := 0; i < 4; i++ {
		sc.shards[i].Write(make([]byte, 64))
	}

	sc.FlushAll()

	require.Eventually(t, func() bool {
		return len(flushChan) >= 4
	}, 200*time.Millisecond, time.Millisecond)
}

func TestShardCollection_MinimumShardSize_Enforcement(t *testing.T) {
	cfg := Config{
		NumShards:   64,
		BufferSize:  65536,
		MaxFileSize: 1024,
		LogFilePath: "/tmp",
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 1, cfg.NumShards)
}
