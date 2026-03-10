package asyncloguploader

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
)

// ChunkManager handles GCS compose operations, including multi-level
// composition for files that exceed the 32-source-object limit.
type ChunkManager struct {
	bucket *storage.BucketHandle
}

// NewChunkManager creates a ChunkManager bound to the given bucket.
func NewChunkManager(bucket *storage.BucketHandle) *ChunkManager {
	return &ChunkManager{bucket: bucket}
}

// Compose merges chunkObjectNames into finalObjectName, recursively
// composing in groups of 32 when there are more than 32 sources.
func (cm *ChunkManager) Compose(ctx context.Context, bucket *storage.BucketHandle, chunkObjectNames []string, finalObjectName string) error {
	if len(chunkObjectNames) <= 32 {
		return cm.composeSingle(ctx, bucket, chunkObjectNames, finalObjectName)
	}
	return cm.composeMultiLevel(ctx, bucket, chunkObjectNames, finalObjectName)
}

func (cm *ChunkManager) composeSingle(ctx context.Context, bucket *storage.BucketHandle, chunkNames []string, destName string) error {
	sources := make([]*storage.ObjectHandle, len(chunkNames))
	for i, name := range chunkNames {
		sources[i] = bucket.Object(name)
	}

	composer := bucket.Object(destName).ComposerFrom(sources...)
	_, err := composer.Run(ctx)
	return err
}

func (cm *ChunkManager) composeMultiLevel(ctx context.Context, bucket *storage.BucketHandle, chunkNames []string, finalName string) error {
	var intermediateNames []string

	for i := 0; i < len(chunkNames); i += 32 {
		end := min(i+32, len(chunkNames))
		group := chunkNames[i:end]

		intermediateName := fmt.Sprintf("%s_intermediate_%d", finalName, i/32)
		intermediateNames = append(intermediateNames, intermediateName)

		if err := cm.composeSingle(ctx, bucket, group, intermediateName); err != nil {
			cm.deleteObjects(ctx, bucket, intermediateNames)
			return err
		}
	}

	err := cm.Compose(ctx, bucket, intermediateNames, finalName)

	cm.deleteObjects(ctx, bucket, intermediateNames)

	return err
}

func (cm *ChunkManager) deleteObjects(ctx context.Context, bucket *storage.BucketHandle, names []string) {
	for _, name := range names {
		if err := bucket.Object(name).Delete(ctx); err != nil {
			log.Warn().Err(err).Str("object", name).Msg("failed to delete intermediate object")
		}
	}
}
