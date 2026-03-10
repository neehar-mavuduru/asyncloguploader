//go:build linux

package asyncloguploader

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// NewSizeFileWriter creates a SizeFileWriter using O_DIRECT and O_DSYNC for
// minimal kernel buffering on Linux.
func NewSizeFileWriter(baseFileName, logsDir string, maxFileSize int64) (*SizeFileWriter, error) {
	tmpPath := generateTmpPath(baseFileName)

	fd, err := syscall.Open(tmpPath, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC|syscall.O_DIRECT|syscall.O_DSYNC, 0644)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), tmpPath)

	w := &SizeFileWriter{
		baseFileName:       baseFileName,
		logsDir:            logsDir,
		currentFile:        f,
		currentFileTmpPath: tmpPath,
		maxFileSize:        maxFileSize,
	}

	alignedSize := ((maxFileSize + 4095) / 4096) * 4096
	_ = unix.Fallocate(int(f.Fd()), 0, 0, alignedSize)

	return w, nil
}

func (w *SizeFileWriter) createNewFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC|syscall.O_DIRECT|syscall.O_DSYNC, 0644)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func (w *SizeFileWriter) preallocateFile(f *os.File, size int64) error {
	alignedSize := ((size + 4095) / 4096) * 4096
	return unix.Fallocate(int(f.Fd()), 0, 0, alignedSize)
}

func (w *SizeFileWriter) syncFile(f *os.File) error {
	return unix.Fsync(int(f.Fd()))
}

func (w *SizeFileWriter) truncateFile(f *os.File, size int64) error {
	return unix.Ftruncate(int(f.Fd()), size)
}

// WriteVectored writes complete buffer blocks to disk with a single pwritev
// syscall. Each buffer is a full mmap region: page-aligned address, 4096-
// aligned size, with an 8-byte header (block size + valid offset). Zero-copy:
// no allocation or memcpy — the mmap memory is passed directly to the kernel.
func (w *SizeFileWriter) WriteVectored(buffers [][]byte) (int, error) {
	if w.currentFile == nil {
		return 0, nil
	}

	start := time.Now()

	totalBytes := 0
	for _, buf := range buffers {
		totalBytes += len(buf)
	}

	_, err := unix.Pwritev(int(w.currentFile.Fd()), buffers, w.fileOffset)
	if err != nil {
		return 0, err
	}

	w.fileOffset += int64(totalBytes)
	w.lastPwritevDuration.Store(int64(time.Since(start)))

	if w.fileOffset >= int64(float64(w.maxFileSize)*0.9) && !w.nextFileReady.Load() {
		go w.prepareNextFile()
	}

	if w.fileOffset >= w.maxFileSize {
		if err := w.rotate(); err != nil {
			return totalBytes, err
		}
	}

	return totalBytes, nil
}
