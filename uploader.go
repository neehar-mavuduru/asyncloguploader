package asyncloguploader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
)

// Uploader polls the logs directory for sealed .log files and uploads them
// to GCS in parallel chunks with retry and compose. Files still being
// written have a .tmp extension and are skipped. After a successful upload
// the local .log file is deleted.
type Uploader struct {
	config   GCSUploadConfig
	logsDir  string
	client   *storage.Client
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stats    UploaderStats
	statsMu  sync.RWMutex
	chunkMgr *ChunkManager
	stopOnce sync.Once
}

// NewUploader creates an Uploader with a real GCS gRPC client using ADC.
// Call Start() to begin polling.
func NewUploader(logsDir string, config GCSUploadConfig) (*Uploader, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client, err := storage.NewClient(ctx,
		option.WithGRPCConnectionPool(config.GRPCPoolSize),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return newUploader(logsDir, config, client, ctx, cancel), nil
}

func newUploader(logsDir string, config GCSUploadConfig, client *storage.Client, ctx context.Context, cancel context.CancelFunc) *Uploader {
	bucket := client.Bucket(config.BucketName)
	return &Uploader{
		config:   config,
		logsDir:  logsDir,
		client:   client,
		ctx:      ctx,
		cancel:   cancel,
		chunkMgr: NewChunkManager(bucket),
	}
}

// Start launches the background poll worker.
func (u *Uploader) Start() {
	u.wg.Add(1)
	go u.pollWorker()
}

// Stop cancels polling, waits for in-flight uploads to finish (including a
// final drain scan), and closes the GCS client. Idempotent.
func (u *Uploader) Stop() {
	u.stopOnce.Do(func() {
		u.cancel()
		u.wg.Wait()
		u.client.Close()
	})
}

// GetStats returns a snapshot of uploader statistics.
func (u *Uploader) GetStats() *UploaderStats {
	s := &UploaderStats{}
	s.FilesUploaded.Store(u.stats.FilesUploaded.Load())
	s.BytesUploaded.Store(u.stats.BytesUploaded.Load())
	s.UploadErrors.Store(u.stats.UploadErrors.Load())
	s.RetryCount.Store(u.stats.RetryCount.Load())
	return s
}

func (u *Uploader) pollWorker() {
	defer u.wg.Done()

	u.scanAndUpload()

	ticker := time.NewTicker(u.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			u.scanAndUpload()
		case <-u.ctx.Done():
			u.scanAndUpload()
			return
		}
	}
}

func (u *Uploader) scanAndUpload() {
	entries, err := os.ReadDir(u.logsDir)
	if err != nil {
		log.Warn().Err(err).Str("dir", u.logsDir).Msg("failed to read logs directory")
		return
	}

	for _, entry := range entries {
		name := entry.Name()

		if strings.HasSuffix(name, ".tmp") {
			continue
		}
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(u.logsDir, name)
		u.uploadFileWithRetry(filePath)
	}
}

var timestampRe = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})`)

func (u *Uploader) deriveObjectName(baseName string) string {
	matches := timestampRe.FindStringSubmatch(baseName)

	var t time.Time
	if len(matches) >= 2 {
		parsed, err := time.Parse("2006-01-02_15-04-05", matches[1])
		if err != nil {
			log.Warn().Str("filename", baseName).Msg("failed to parse timestamp from filename, using current time")
			t = time.Now()
		} else {
			t = parsed
		}
	} else {
		log.Warn().Str("filename", baseName).Msg("no timestamp found in filename, using current time")
		t = time.Now()
	}

	datePart := t.Format("2006-01-02")
	hourPart := t.Format("15")
	return fmt.Sprintf("%s%s/%s/%s", u.config.ObjectPrefix, datePart, hourPart, baseName)
}

func (u *Uploader) uploadFileWithRetry(filePath string) {
	var lastErr error

	for attempt := 0; attempt < u.config.MaxRetries; attempt++ {
		if attempt > 0 {
			u.stats.RetryCount.Add(1)
			backoff := time.Second * time.Duration(1<<uint(attempt))
			time.Sleep(backoff)
			log.Warn().Err(lastErr).Int("attempt", attempt).Str("file", filePath).Msg("retrying upload")
		}

		lastErr = u.uploadFile(filePath)
		if lastErr == nil {
			return
		}
	}

	u.stats.UploadErrors.Add(1)
	log.Error().Err(lastErr).Str("file", filePath).Msg("permanent upload failure after retries")
}

func (u *Uploader) uploadFile(filePath string) error {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	baseName := filepath.Base(filePath)
	objectName := u.deriveObjectName(baseName)
	bucket := u.client.Bucket(u.config.BucketName)

	uploadCtx := context.Background()

	chunkSize := u.config.ChunkSize
	var chunkNames []string
	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploadErr error

	for i := 0; i*chunkSize < len(fileData); i++ {
		start := i * chunkSize
		end := min(start+chunkSize, len(fileData))
		chunk := fileData[start:end]
		chunkName := fmt.Sprintf("%s_chunk_%d", objectName, i)

		mu.Lock()
		chunkNames = append(chunkNames, chunkName)
		mu.Unlock()

		wg.Add(1)
		go func(name string, data []byte) {
			defer wg.Done()
			w := bucket.Object(name).NewWriter(uploadCtx)
			if _, werr := w.Write(data); werr != nil {
				mu.Lock()
				uploadErr = werr
				mu.Unlock()
				w.Close()
				return
			}
			if cerr := w.Close(); cerr != nil {
				mu.Lock()
				uploadErr = cerr
				mu.Unlock()
			}
		}(chunkName, chunk)
	}

	wg.Wait()

	if uploadErr != nil {
		u.deleteChunks(uploadCtx, bucket, chunkNames)
		return fmt.Errorf("chunk upload: %w", uploadErr)
	}

	if err := u.chunkMgr.Compose(uploadCtx, bucket, chunkNames, objectName); err != nil {
		u.deleteChunks(uploadCtx, bucket, chunkNames)
		return fmt.Errorf("compose: %w", err)
	}

	attrs, err := bucket.Object(objectName).Attrs(uploadCtx)
	if err != nil {
		return fmt.Errorf("verify attrs: %w", err)
	}
	if attrs.Size != int64(len(fileData)) {
		return fmt.Errorf("size mismatch: expected %d, got %d", len(fileData), attrs.Size)
	}

	u.deleteChunks(uploadCtx, bucket, chunkNames)

	os.Remove(filePath)

	u.stats.FilesUploaded.Add(1)
	u.stats.BytesUploaded.Add(int64(len(fileData)))

	return nil
}

func (u *Uploader) deleteChunks(ctx context.Context, bucket *storage.BucketHandle, names []string) {
	for _, name := range names {
		_ = bucket.Object(name).Delete(ctx)
	}
}
