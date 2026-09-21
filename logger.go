package sdk

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// Level is a log severity understood by host.log.
type Level string

// Log levels, ordered from least to most severe.
const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

func levelRank(l Level) int {
	switch l {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	default:
		return 1
	}
}

// LevelLogger writes human readable lines to stderr and mirrors them to the
// host once the connection is up. stdout is reserved for the protocol, so no
// log line may ever be written there.
type LevelLogger struct {
	mu sync.Mutex
	w  io.Writer

	// Min is the lowest level written at all.
	Min Level
	// HostMin is the lowest level forwarded to host.log. Lines below it, and
	// lines emitted before the host is ready, only reach stderr.
	HostMin Level
}

// NewLogger builds a logger writing to w.
func NewLogger(w io.Writer) *LevelLogger {
	return &LevelLogger{w: w, Min: LevelDebug, HostMin: LevelInfo}
}

// Logger is the package level logger. It writes to stderr.
var Logger = NewLogger(os.Stderr)

// SetOutput replaces the fallback writer, mainly for tests.
func (l *LevelLogger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w = w
}

// Log emits one line at the given level.
func (l *LevelLogger) Log(level Level, msg string, fields map[string]any) {
	if levelRank(level) < levelRank(l.Min) {
		return
	}

	if h := CurrentHost(); h != nil && h.Ready() && levelRank(level) >= levelRank(l.HostMin) {
		if err := h.Log(string(level), msg, fields); err == nil {
			return
		}
		// Fall through to stderr when the host call could not be delivered.
	}

	l.writeStderr(level, msg, fields)
}

func (l *LevelLogger) writeStderr(level Level, msg string, fields map[string]any) {
	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(strings.ToUpper(string(level)))
	b.WriteString("] ")
	b.WriteString(msg)

	for k, v := range fields {
		fmt.Fprintf(&b, " %s=%v", k, v)
	}
	b.WriteByte('\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return
	}
	_, _ = io.WriteString(l.w, b.String())
}

// Debugf logs a formatted message at debug level.
func (l *LevelLogger) Debugf(format string, args ...any) {
	l.Log(LevelDebug, fmt.Sprintf(format, args...), nil)
}

// Infof logs a formatted message at info level.
func (l *LevelLogger) Infof(format string, args ...any) {
	l.Log(LevelInfo, fmt.Sprintf(format, args...), nil)
}

// Warnf logs a formatted message at warn level.
func (l *LevelLogger) Warnf(format string, args ...any) {
	l.Log(LevelWarn, fmt.Sprintf(format, args...), nil)
}

// Errorf logs a formatted message at error level.
func (l *LevelLogger) Errorf(format string, args ...any) {
	l.Log(LevelError, fmt.Sprintf(format, args...), nil)
}

// Debugf logs to the package level logger.
func Debugf(format string, args ...any) { Logger.Debugf(format, args...) }

// Infof logs to the package level logger.
func Infof(format string, args ...any) { Logger.Infof(format, args...) }

// Warnf logs to the package level logger.
func Warnf(format string, args ...any) { Logger.Warnf(format, args...) }

// Errorf logs to the package level logger.
func Errorf(format string, args ...any) { Logger.Errorf(format, args...) }

// LogFields logs a message with structured fields on the package level logger.
func LogFields(level Level, msg string, fields map[string]any) {
	Logger.Log(level, msg, fields)
}
