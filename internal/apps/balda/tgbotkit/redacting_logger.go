package tgbotkit

import (
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/redaction"
	"github.com/tgbotkit/runtime/logger"
)

// redactingRuntimeLogger prevents provider credentials embedded in generated
// request URLs from crossing the runtime logging boundary.
type redactingRuntimeLogger struct {
	delegate logger.Logger
}

func newRedactingRuntimeLogger(delegate logger.Logger) logger.Logger {
	if delegate == nil {
		delegate = logger.NewNop()
	}
	return &redactingRuntimeLogger{delegate: delegate}
}

func (l *redactingRuntimeLogger) Errorf(format string, args ...interface{}) {
	l.delegate.Errorf("%s", redactedLogf(format, args...))
}

func (l *redactingRuntimeLogger) Fatalf(format string, args ...interface{}) {
	l.delegate.Fatalf("%s", redactedLogf(format, args...))
}

func (l *redactingRuntimeLogger) Fatal(args ...interface{}) {
	l.delegate.Fatal(redaction.Secrets(fmt.Sprint(args...)))
}

func (l *redactingRuntimeLogger) Infof(format string, args ...interface{}) {
	l.delegate.Infof("%s", redactedLogf(format, args...))
}

func (l *redactingRuntimeLogger) Info(args ...interface{}) {
	l.delegate.Info(redaction.Secrets(fmt.Sprint(args...)))
}

func (l *redactingRuntimeLogger) Warnf(format string, args ...interface{}) {
	l.delegate.Warnf("%s", redactedLogf(format, args...))
}

func (l *redactingRuntimeLogger) Debugf(format string, args ...interface{}) {
	l.delegate.Debugf("%s", redactedLogf(format, args...))
}

func (l *redactingRuntimeLogger) Debug(args ...interface{}) {
	l.delegate.Debug(redaction.Secrets(fmt.Sprint(args...)))
}

func redactedLogf(format string, args ...interface{}) string {
	return redaction.Secrets(fmt.Sprintf(format, args...))
}
