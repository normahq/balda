package handlers

import (
	"context"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/automode"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

type autoStateManager interface {
	RuntimeStateValue(ctx context.Context, locator baldasession.SessionLocator, key string) (any, bool, error)
	UpdateRuntimeState(ctx context.Context, locator baldasession.SessionLocator, state map[string]any) error
}

func loadAutoStatus(ctx context.Context, sessions autoStateManager, locator baldasession.SessionLocator) (automode.Status, error) {
	status := automode.DefaultStatus()
	if sessions == nil {
		return status, nil
	}
	if value, ok, err := sessions.RuntimeStateValue(ctx, locator, automode.StateKeyEnabled); err != nil {
		return status, err
	} else if ok {
		status.Enabled = automode.ParseBool(value)
	}
	if value, ok, err := sessions.RuntimeStateValue(ctx, locator, automode.StateKeyMode); err != nil {
		return status, err
	} else if ok {
		if text, ok := value.(string); ok {
			status.State = strings.TrimSpace(text)
		}
	}
	if value, ok, err := sessions.RuntimeStateValue(ctx, locator, automode.StateKeyConsecutiveTurns); err != nil {
		return status, err
	} else if ok {
		status.ConsecutiveTurns = automode.ParseInt(value, 0)
	}
	if value, ok, err := sessions.RuntimeStateValue(ctx, locator, automode.StateKeyMaxTurns); err != nil {
		return status, err
	} else if ok {
		status.MaxTurns = automode.ParseInt(value, automode.DefaultMaxTurns)
	}
	if value, ok, err := sessions.RuntimeStateValue(ctx, locator, automode.StateKeyLastTurnAt); err != nil {
		return status, err
	} else if ok {
		if text, ok := value.(string); ok {
			status.LastTurnAt = strings.TrimSpace(text)
		}
	}
	return automode.Normalize(status), nil
}

func setAutoEnabled(ctx context.Context, sessions autoStateManager, locator baldasession.SessionLocator, enabled bool, now time.Time) error {
	if sessions == nil {
		return nil
	}
	if enabled {
		return sessions.UpdateRuntimeState(ctx, locator, automode.EnableState(now))
	}
	return sessions.UpdateRuntimeState(ctx, locator, automode.DisableState())
}
