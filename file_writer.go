package asyncloguploader

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

var fileSeq atomic.Uint64

// FileWriter is the interface for vectored file writing with rotation support.
type FileWriter interface {
	WriteVectored(buffers [][]byte) (int, error)
	GetLastPwritevDuration() time.Duration
	Close() error
}

// SizeFileWriter implements FileWriter with size-based rotation and proactive
// next-file pre-creation. Sealed files are renamed from .tmp to .log so
// the uploader can discover them by scanning the logs directory.
type SizeFileWriter struct {
	baseFileName        string
	logsDir             string
	currentFile         *os.File
	currentFileTmpPath  string
	nextFile            *os.File
	nextFileTmpPath     string
	fileOffset          int64
	maxFileSize         int64
	rotationMu          sync.Mutex
	lastPwritevDuration atomic.Int64
	nextFileReady       atomic.Bool
}

// GetLastPwritevDuration returns the duration of the most recent WriteVectored call.
func (w *SizeFileWriter) GetLastPwritevDuration() time.Duration {
	return time.Duration(w.lastPwritevDuration.Load())
}

func generateTmpPath(baseFileName string) string {
	ts := time.Now().Format("2006-01-02_15-04-05")
	seq := fileSeq.Add(1)
	return fmt.Sprintf("%s_%s_%d.log.tmp", baseFileName, ts, seq)
}

func tmpPathToFinalPath(tmpPath string) string {
	return tmpPath[:len(tmpPath)-4] // strip ".tmp"
}

// rotate seals the current file and creates or promotes the next one.
// The sealed file is renamed from .tmp to .log; the uploader discovers
// sealed files by scanning the logs directory for .log extensions.
func (w *SizeFileWriter) rotate() error {
	w.rotationMu.Lock()
	defer w.rotationMu.Unlock()

	if w.currentFile == nil {
		return nil
	}

	_ = w.syncFile(w.currentFile)
	_ = w.truncateFile(w.currentFile, w.fileOffset)
	w.currentFile.Close()

	if w.fileOffset > 0 {
		finalPath := tmpPathToFinalPath(w.currentFileTmpPath)
		if err := os.Rename(w.currentFileTmpPath, finalPath); err != nil {
			log.Error().Err(err).Str("src", w.currentFileTmpPath).Str("dst", finalPath).Msg("failed to rename tmp to final")
		}
	} else {
		os.Remove(w.currentFileTmpPath)
	}

	if w.nextFile != nil && w.nextFileReady.Load() {
		w.currentFile = w.nextFile
		w.currentFileTmpPath = w.nextFileTmpPath
		w.nextFile = nil
		w.nextFileTmpPath = ""
		w.nextFileReady.Store(false)
		w.fileOffset = 0
	} else {
		tmpPath := generateTmpPath(w.baseFileName)
		f, err := w.createNewFile(tmpPath)
		if err != nil {
			log.Error().Err(err).Msg("failed to create new log file after rotation")
			w.currentFile = nil
			return err
		}
		_ = w.preallocateFile(f, w.maxFileSize)
		w.currentFile = f
		w.currentFileTmpPath = tmpPath
		w.fileOffset = 0
	}

	return nil
}

// prepareNextFile pre-creates and pre-allocates the next .tmp file so that
// rotation can promote it without blocking.
func (w *SizeFileWriter) prepareNextFile() {
	if w.nextFileReady.Load() {
		return
	}

	w.rotationMu.Lock()
	if w.currentFile == nil || w.nextFile != nil {
		w.rotationMu.Unlock()
		return
	}

	tmpPath := generateTmpPath(w.baseFileName)
	f, err := w.createNewFile(tmpPath)
	if err != nil {
		w.rotationMu.Unlock()
		log.Error().Err(err).Msg("failed to pre-create next file")
		return
	}
	_ = w.preallocateFile(f, w.maxFileSize)

	w.nextFile = f
	w.nextFileTmpPath = tmpPath
	w.rotationMu.Unlock()

	w.nextFileReady.Store(true)
}

// Close seals the current file: sync, truncate, rename .tmp → .log.
func (w *SizeFileWriter) Close() error {
	w.rotationMu.Lock()
	defer w.rotationMu.Unlock()

	if w.currentFile == nil {
		return nil
	}

	_ = w.syncFile(w.currentFile)
	_ = w.truncateFile(w.currentFile, w.fileOffset)
	w.currentFile.Close()

	if w.fileOffset > 0 {
		finalPath := tmpPathToFinalPath(w.currentFileTmpPath)
		_ = os.Rename(w.currentFileTmpPath, finalPath)
	} else {
		os.Remove(w.currentFileTmpPath)
	}

	w.currentFile = nil

	if w.nextFile != nil {
		w.nextFile.Close()
		os.Remove(w.nextFileTmpPath)
		w.nextFile = nil
	}

	return nil
}
