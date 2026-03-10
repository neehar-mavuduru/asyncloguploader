package asyncloguploader

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// LoggerManager multiplexes log writes across event-specific Loggers and
// optionally runs a GCS Uploader that polls the logs directory for sealed files.
type LoggerManager struct {
	loggers     sync.Map
	baseDir     string
	logsDir     string
	config      Config
	uploader    *Uploader
	ownUploader bool
}

// NewLoggerManager validates the config, creates the logs directory, and
// optionally starts the GCS uploader.
func NewLoggerManager(config Config) (*LoggerManager, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	baseDir := config.LogFilePath
	logsDir := filepath.Join(baseDir, "logs")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, err
	}

	lm := &LoggerManager{
		baseDir: baseDir,
		logsDir: logsDir,
		config:  config,
	}

	if config.GCSUploadConfig != nil {
		u, err := NewUploader(logsDir, *config.GCSUploadConfig)
		if err != nil {
			return nil, err
		}
		u.Start()
		lm.uploader = u
		lm.ownUploader = true
	}

	return lm, nil
}

var eventNameReplacer = strings.NewReplacer(
	"/", "_", "\\", "_", ":", "_", "*", "_",
	"?", "_", "\"", "_", "<", "_", ">", "_",
	"|", "_", " ", "_",
)

func sanitizeEventName(name string) string {
	result := eventNameReplacer.Replace(name)
	if len(result) > 255 {
		result = result[:255]
	}
	if result == "" {
		panic("asyncloguploader: sanitized event name is empty")
	}
	return result
}

func (lm *LoggerManager) getOrCreateLogger(eventName string) (*Logger, error) {
	sanitized := sanitizeEventName(eventName)

	if val, ok := lm.loggers.Load(sanitized); ok {
		return val.(*Logger), nil
	}

	logger, err := NewLogger(lm.config, sanitized, lm.logsDir)
	if err != nil {
		return nil, err
	}

	actual, loaded := lm.loggers.LoadOrStore(sanitized, logger)
	if loaded {
		logger.Close()
		return actual.(*Logger), nil
	}

	return logger, nil
}

// LogBytesWithEvent writes raw bytes to the logger for the given event.
func (lm *LoggerManager) LogBytesWithEvent(eventName string, data []byte) error {
	logger, err := lm.getOrCreateLogger(eventName)
	if err != nil {
		return err
	}
	return logger.LogBytes(data)
}

// LogWithEvent writes a string message to the logger for the given event.
func (lm *LoggerManager) LogWithEvent(eventName string, msg string) error {
	logger, err := lm.getOrCreateLogger(eventName)
	if err != nil {
		return err
	}
	return logger.Log(msg)
}

// CloseEventLogger closes and removes the logger for a specific event.
func (lm *LoggerManager) CloseEventLogger(eventName string) error {
	sanitized := sanitizeEventName(eventName)
	val, ok := lm.loggers.LoadAndDelete(sanitized)
	if !ok {
		return nil
	}
	return val.(*Logger).Close()
}

// GetUploaderStats returns uploader statistics if GCS upload is enabled,
// otherwise nil.
func (lm *LoggerManager) GetUploaderStats() *UploaderStats {
	if lm.uploader == nil {
		return nil
	}
	return lm.uploader.GetStats()
}

// GetStats returns a snapshot of statistics for every active logger.
func (lm *LoggerManager) GetStats() map[string]*Statistics {
	result := make(map[string]*Statistics)
	lm.loggers.Range(func(key, value any) bool {
		result[key.(string)] = value.(*Logger).GetStats()
		return true
	})
	return result
}

// Close flushes and closes all loggers, then stops the uploader (if owned).
func (lm *LoggerManager) Close() error {
	var lastErr error
	lm.loggers.Range(func(key, value any) bool {
		if err := value.(*Logger).Close(); err != nil {
			lastErr = err
		}
		return true
	})

	if lm.ownUploader && lm.uploader != nil {
		lm.uploader.Stop()
	}

	return lastErr
}
