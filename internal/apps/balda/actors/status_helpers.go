package actors

import (
	"strings"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func isTerminalTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case baldastate.SwarmTaskStatusCompleted,
		baldastate.SwarmTaskStatusFailed,
		baldastate.SwarmTaskStatusCanceled,
		baldastate.SwarmTaskStatusDeadLettered:
		return true
	default:
		return false
	}
}
