//go:build !linux

package asyncloguploader

import (
	"os"
	"time"
)

// NewSizeFileWriter creates a SizeFileWriter using standard POSIX I/O
// (no O_DIRECT).
func NewSizeFileWriter(baseFileName, logsDir string, maxFileSize int64) (*SizeFileWriter, error) {
	tmpPath := generateTmpPath(baseFileName)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &SizeFileWriter{
		baseFileName:       baseFileName,
		logsDir:            logsDir,
		currentFile:        f,
		currentFileTmpPath: tmpPath,
		maxFileSize:        maxFileSize,
	}, nil
}

func (w *SizeFileWriter) createNewFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func (w *SizeFileWriter) preallocateFile(_ *os.File, _ int64) error {
	return nil
}

func (w *SizeFileWriter) syncFile(f *os.File) error {
	return f.Sync()
}

func (w *SizeFileWriter) truncateFile(f *os.File, size int64) error {
	return f.Truncate(size)
}

// WriteVectored writes complete buffer blocks sequentially to the current file.
// Each buffer is a full block with an 8-byte header (block size + valid offset).
func (w *SizeFileWriter) WriteVectored(buffers [][]byte) (int, error) {
	if w.currentFile == nil {
		return 0, nil
	}

	start := time.Now()

	totalWritten := 0
	for _, buf := range buffers {
		n, err := w.currentFile.WriteAt(buf, w.fileOffset)
		if err != nil {
			return totalWritten, err
		}
		w.fileOffset += int64(n)
		totalWritten += n
	}

	w.lastPwritevDuration.Store(int64(time.Since(start)))

	if w.fileOffset >= int64(float64(w.maxFileSize)*0.9) && !w.nextFileReady.Load() {
		go w.prepareNextFile()
	}

	if w.fileOffset >= w.maxFileSize {
		if err := w.rotate(); err != nil {
			return totalWritten, err
		}
	}

	return totalWritten, nil
}
