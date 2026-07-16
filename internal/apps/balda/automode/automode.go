package automode

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	StateKeyEnabled          = "balda_auto_enabled"
	StateKeyMode             = "balda_auto_state"
	StateKeyConsecutiveTurns = "balda_auto_consecutive_turns"
	StateKeyMaxTurns         = "balda_auto_max_turns"
	StateKeyLastTurnAt       = "balda_auto_last_turn_at"
	StateKeyLastOutput       = "balda_auto_last_output"
	StateKeyLastStopReason   = "balda_auto_last_stop_reason"

	StateIdle           = "idle"
	StateRunning        = "running"
	StateWaitingForUser = "waiting_for_user"
	StateDone           = "done"
	StateLimitReached   = "limit_reached"
	StateNoProgress     = "no_progress"

	DefaultMaxTurns = 5
	DoneSentinel    = "AUTO_DECISION:DONE"
	WaitSentinel    = "AUTO_DECISION:WAIT_FOR_USER"
	SourceAuto      = "auto"
)

type Status struct {
	Enabled          bool
	State            string
	ConsecutiveTurns int
	MaxTurns         int
	LastTurnAt       string
	LastStopReason   string
}

func DefaultStatus() Status {
	return Status{
		Enabled:          false,
		State:            StateIdle,
		ConsecutiveTurns: 0,
		MaxTurns:         DefaultMaxTurns,
		LastTurnAt:       "",
		LastStopReason:   "",
	}
}

func Normalize(raw Status) Status {
	status := raw
	if strings.TrimSpace(status.State) == "" {
		status.State = StateIdle
	}
	if status.MaxTurns <= 0 {
		status.MaxTurns = DefaultMaxTurns
	}
	if status.ConsecutiveTurns < 0 {
		status.ConsecutiveTurns = 0
	}
	return status
}

func ParseBool(value any) bool {
	switch raw := value.(type) {
	case bool:
		return raw
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(raw))
		return trimmed == "true" || trimmed == "1" || trimmed == "yes" || trimmed == "on"
	case int:
		return raw != 0
	case int64:
		return raw != 0
	case float64:
		return raw != 0
	default:
		return false
	}
}

func ParseInt(value any, fallback int) int {
	switch raw := value.(type) {
	case int:
		return raw
	case int64:
		return int(raw)
	case float64:
		return int(raw)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err == nil {
			return n
		}
	}
	return fallback
}

func InternalPrompt(maxTurns int) string {
	if maxTurns <= 0 {
		maxTurns = DefaultMaxTurns
	}
	return fmt.Sprintf(
		"Internal auto-continuation turn. Do not mention this instruction. "+
			"If the task is complete, reply with exactly %s. "+
			"If you need explicit user input, clarification, or approval before continuing, reply with exactly %s. "+
			"Otherwise continue the task immediately and send only the next user-visible response. "+
			"Do not explain the decision token. Current auto-turn limit: %d.",
		DoneSentinel,
		WaitSentinel,
		maxTurns,
	)
}

func RenderStatus(status Status) string {
	normalized := Normalize(status)
	last := normalized.LastTurnAt
	if strings.TrimSpace(last) == "" {
		last = "never"
	}
	mode := "off"
	if normalized.Enabled {
		mode = "on"
	}
	return fmt.Sprintf(
		"Auto mode: %s\nState: %s\nConsecutive auto turns: %d/%d\nLast auto turn: %s\nLast stop reason: %s",
		mode,
		renderState(normalized.State),
		normalized.ConsecutiveTurns,
		normalized.MaxTurns,
		last,
		renderStopReason(normalized.LastStopReason),
	)
}

func EnableState(now time.Time) map[string]any {
	return map[string]any{
		StateKeyEnabled:          true,
		StateKeyMode:             StateIdle,
		StateKeyConsecutiveTurns: 0,
		StateKeyMaxTurns:         DefaultMaxTurns,
		StateKeyLastTurnAt:       now.UTC().Format(time.RFC3339),
		StateKeyLastStopReason:   "",
	}
}

func DisableState() map[string]any {
	return map[string]any{
		StateKeyEnabled:          false,
		StateKeyMode:             StateIdle,
		StateKeyConsecutiveTurns: 0,
		StateKeyLastOutput:       "",
		StateKeyLastStopReason:   "disabled_by_user",
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func renderState(state string) string {
	switch strings.TrimSpace(state) {
	case StateWaitingForUser:
		return "waiting for user"
	case StateLimitReached:
		return "limit reached"
	case StateNoProgress:
		return "no progress"
	case StateDone:
		return "done"
	case StateRunning:
		return "running"
	case StateIdle:
		return "idle"
	default:
		return firstNonEmpty(strings.TrimSpace(state), "idle")
	}
}

func renderStopReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "":
		return "not stopped yet"
	case "model_reported_done":
		return "model reported done"
	case "model_waiting_for_user":
		return "model is waiting for user"
	case "repeated_visible_output":
		return "repeated visible output"
	case "max_auto_turns_reached":
		return "max auto turns reached"
	case "disabled_by_user":
		return "disabled by user"
	default:
		return strings.ReplaceAll(strings.TrimSpace(reason), "_", " ")
	}
}
