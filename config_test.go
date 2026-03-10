package asyncloguploader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate_ValidConfig(t *testing.T) {
	cfg := Config{
		NumShards:   4,
		BufferSize:  1024 * 1024,
		MaxFileSize: 10 * 1024 * 1024,
		LogFilePath: "/tmp/test",
	}
	require.NoError(t, cfg.Validate())
}

func TestConfig_Validate_ZeroNumShards(t *testing.T) {
	cfg := Config{NumShards: 0, BufferSize: 1024 * 1024, MaxFileSize: 1024, LogFilePath: "/tmp"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NumShards")
}

func TestConfig_Validate_ZeroBufferSize(t *testing.T) {
	cfg := Config{NumShards: 1, BufferSize: 0, MaxFileSize: 1024, LogFilePath: "/tmp"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BufferSize")
}

func TestConfig_Validate_ZeroMaxFileSize(t *testing.T) {
	cfg := Config{NumShards: 1, BufferSize: 1024, MaxFileSize: 0, LogFilePath: "/tmp"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxFileSize")
}

func TestConfig_Validate_EmptyLogFilePath(t *testing.T) {
	cfg := Config{NumShards: 1, BufferSize: 1024, MaxFileSize: 1024, LogFilePath: ""}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestConfig_Validate_ShardSizeAutoReduction(t *testing.T) {
	cfg := Config{
		NumShards:   64,
		BufferSize:  65536,
		MaxFileSize: 1024,
		LogFilePath: "/tmp",
	}
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, 1, cfg.NumShards, "NumShards should be reduced to 1")
}

func TestConfig_Validate_GCSDefaults(t *testing.T) {
	cfg := Config{
		NumShards:   1,
		BufferSize:  1024 * 1024,
		MaxFileSize: 1024,
		LogFilePath: "/tmp",
		GCSUploadConfig: &GCSUploadConfig{
			BucketName: "test-bucket",
		},
	}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 32*1024*1024, cfg.GCSUploadConfig.ChunkSize)
	assert.Equal(t, 3, cfg.GCSUploadConfig.MaxRetries)
	assert.Equal(t, 64, cfg.GCSUploadConfig.GRPCPoolSize)
	assert.Equal(t, 3*time.Second, cfg.GCSUploadConfig.PollInterval)
}
