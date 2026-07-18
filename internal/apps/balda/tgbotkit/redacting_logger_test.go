package tgbotkit

import (
	"fmt"
	"strings"
	"testing"
)

type recordingRuntimeLogger struct {
	entries []string
}

func (l *recordingRuntimeLogger) recordf(format string, args ...interface{}) {
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *recordingRuntimeLogger) Errorf(format string, args ...interface{}) {
	l.recordf(format, args...)
}
func (l *recordingRuntimeLogger) Fatalf(format string, args ...interface{}) {
	l.recordf(format, args...)
}
func (l *recordingRuntimeLogger) Fatal(args ...interface{}) { l.recordf("%s", fmt.Sprint(args...)) }
func (l *recordingRuntimeLogger) Infof(format string, args ...interface{}) {
	l.recordf(format, args...)
}
func (l *recordingRuntimeLogger) Info(args ...interface{}) { l.recordf("%s", fmt.Sprint(args...)) }
func (l *recordingRuntimeLogger) Warnf(format string, args ...interface{}) {
	l.recordf(format, args...)
}
func (l *recordingRuntimeLogger) Debugf(format string, args ...interface{}) {
	l.recordf(format, args...)
}
func (l *recordingRuntimeLogger) Debug(args ...interface{}) { l.recordf("%s", fmt.Sprint(args...)) }

func TestRedactingRuntimeLoggerRemovesTelegramTokens(t *testing.T) {
	t.Parallel()

	const token = "123456:ABCdefGhIjkLMNopQRST_uvwx"
	delegate := &recordingRuntimeLogger{}
	logger := newRedactingRuntimeLogger(delegate)
	logger.Errorf("fetch updates: %v", fmt.Errorf("Post https://api.telegram.org/bot%s/getUpdates: timeout", token))

	if len(delegate.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(delegate.entries))
	}
	if strings.Contains(delegate.entries[0], token) || !strings.Contains(delegate.entries[0], "bot[REDACTED_TOKEN]") {
		t.Fatalf("entry = %q, want Telegram token redacted", delegate.entries[0])
	}
}
