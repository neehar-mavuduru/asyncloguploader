package asyncloguploader

import (
	"context"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFakeGCS(t *testing.T) *fakestorage.Server {
	t.Helper()
	server := fakestorage.NewServer([]fakestorage.Object{})
	t.Cleanup(server.Stop)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "test-bucket"})
	return server
}

func TestChunkManager_ComposeSingle_Under32(t *testing.T) {
	server := newFakeGCS(t)
	client := server.Client()
	bucket := client.Bucket("test-bucket")
	cm := NewChunkManager(bucket)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		w := bucket.Object("chunk_" + string(rune('A'+i))).NewWriter(ctx)
		w.Write([]byte("data"))
		require.NoError(t, w.Close())
	}

	names := []string{"chunk_A", "chunk_B", "chunk_C", "chunk_D", "chunk_E"}
	err := cm.Compose(ctx, bucket, names, "final_object")
	require.NoError(t, err)

	attrs, err := bucket.Object("final_object").Attrs(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(20), attrs.Size)
}

func TestChunkManager_ComposeMultiLevel_Over32(t *testing.T) {
	server := newFakeGCS(t)
	client := server.Client()
	bucket := client.Bucket("test-bucket")
	cm := NewChunkManager(bucket)
	ctx := context.Background()

	names := make([]string, 40)
	for i := 0; i < 40; i++ {
		name := "chunk_" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		names[i] = name
		w := bucket.Object(name).NewWriter(ctx)
		w.Write([]byte("xx"))
		require.NoError(t, w.Close())
	}

	err := cm.Compose(ctx, bucket, names, "final_large")
	require.NoError(t, err)

	attrs, err := bucket.Object("final_large").Attrs(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(80), attrs.Size)
}

func TestChunkManager_DeleteIntermediates_OnError(t *testing.T) {
	server := newFakeGCS(t)
	client := server.Client()
	bucket := client.Bucket("test-bucket")
	cm := NewChunkManager(bucket)
	ctx := context.Background()

	names := make([]string, 40)
	for i := 0; i < 32; i++ {
		name := "chunk_" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		names[i] = name
		w := bucket.Object(name).NewWriter(ctx)
		w.Write([]byte("x"))
		require.NoError(t, w.Close())
	}
	for i := 32; i < 40; i++ {
		names[i] = "nonexistent_" + string(rune('A'+i%26))
	}

	err := cm.Compose(ctx, bucket, names, "final_err")
	assert.Error(t, err)
}
