package asyncloguploader

import "sync/atomic"

// Statistics holds counters for a single Logger instance.
type Statistics struct {
	TotalLogs    atomic.Int64
	DroppedLogs  atomic.Int64
	BytesWritten atomic.Int64
}

// UploaderStats holds counters for the GCS uploader.
type UploaderStats struct {
	FilesUploaded atomic.Int64
	BytesUploaded atomic.Int64
	UploadErrors  atomic.Int64
	RetryCount    atomic.Int64
}
