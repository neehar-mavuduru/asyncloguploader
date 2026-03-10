// Package asyncloguploader provides a high-performance, asynchronous logging
// library that writes structured log data to disk and optionally uploads
// completed log files to Google Cloud Storage.
package asyncloguploader

import "errors"

var (
	// ErrBufferFull is returned when all shard buffers are full and the log cannot be written.
	ErrBufferFull = errors.New("asyncloguploader: buffer full, log dropped")

	// ErrLoggerClosed is returned when attempting to write to a closed logger.
	ErrLoggerClosed = errors.New("asyncloguploader: logger is closed")

	// ErrInvalidConfig is returned when the provided configuration is invalid.
	ErrInvalidConfig = errors.New("asyncloguploader: invalid configuration")
)
