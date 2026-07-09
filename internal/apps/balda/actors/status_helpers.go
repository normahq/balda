package actors

import (
	"strings"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func isTerminalTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case baldastate.JobStatusCompleted,
		baldastate.JobStatusFailed,
		baldastate.JobStatusCanceled,
		baldastate.JobStatusDeadLettered:
		return true
	default:
		return false
	}
}
