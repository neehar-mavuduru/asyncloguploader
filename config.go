package asyncloguploader

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// Config holds the configuration for a Logger instance.
type Config struct {
	NumShards       int
	BufferSize      int
	MaxFileSize     int64
	LogFilePath     string
	GCSUploadConfig *GCSUploadConfig
}

// GCSUploadConfig holds configuration for uploading log files to Google Cloud Storage.
type GCSUploadConfig struct {
	BucketName   string
	ObjectPrefix string
	ChunkSize    int
	MaxRetries   int
	GRPCPoolSize int
	PollInterval time.Duration
}

const (
	defaultChunkSize    = 32 * 1024 * 1024 // 32 MB
	defaultMaxRetries   = 3
	defaultGRPCPoolSize = 64
	defaultPollInterval = 3 * time.Second
	minShardCapacity    = 65536 // 64 KB
)

// Validate checks the configuration for correctness, applies defaults for zero-valued
// GCS fields, and auto-reduces NumShards if per-shard capacity falls below 64 KB.
func (c *Config) Validate() error {
	if c.NumShards < 1 {
		return fmt.Errorf("%w: NumShards must be >= 1", ErrInvalidConfig)
	}
	if c.BufferSize <= 0 {
		return fmt.Errorf("%w: BufferSize must be > 0", ErrInvalidConfig)
	}
	if c.MaxFileSize <= 0 {
		return fmt.Errorf("%w: MaxFileSize must be > 0", ErrInvalidConfig)
	}
	if c.LogFilePath == "" {
		return fmt.Errorf("%w: LogFilePath must not be empty", ErrInvalidConfig)
	}

	originalShards := c.NumShards
	for c.BufferSize/c.NumShards < minShardCapacity && c.NumShards > 1 {
		c.NumShards--
	}
	if c.NumShards != originalShards {
		log.Warn().
			Int("original_shards", originalShards).
			Int("reduced_shards", c.NumShards).
			Int("buffer_size", c.BufferSize).
			Msg("reduced NumShards to satisfy minimum per-shard capacity of 64KB")
	}

	if c.GCSUploadConfig != nil {
		g := c.GCSUploadConfig
		if g.ChunkSize == 0 {
			g.ChunkSize = defaultChunkSize
		}
		if g.MaxRetries == 0 {
			g.MaxRetries = defaultMaxRetries
		}
		if g.GRPCPoolSize == 0 {
			g.GRPCPoolSize = defaultGRPCPoolSize
		}
		if g.PollInterval == 0 {
			g.PollInterval = defaultPollInterval
		}
	}

	return nil
}
