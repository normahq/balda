package handlers

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/actors"
)

// DispatchingSessionTurnRunner routes session turn payloads to the correct
// SessionTurnRunner based on the locator's ChannelType.
type DispatchingSessionTurnRunner struct {
	runners       map[string]actors.SessionTurnRunner
	defaultRunner actors.SessionTurnRunner
}

// NewDispatchingSessionTurnRunner creates a runner that dispatches to channel-specific runners.
func NewDispatchingSessionTurnRunner(
	runners map[string]actors.SessionTurnRunner,
	defaultRunner actors.SessionTurnRunner,
) *DispatchingSessionTurnRunner {
	return &DispatchingSessionTurnRunner{
		runners:       runners,
		defaultRunner: defaultRunner,
	}
}

// RunSessionTurnPayload routes the payload to the correct runner by channel type.
func (d *DispatchingSessionTurnRunner) RunSessionTurnPayload(
	ctx context.Context,
	payload actors.SessionTurnPayload,
) error {
	channelType := payload.Locator.ChannelType
	runner, ok := d.runners[channelType]
	if !ok {
		if d.defaultRunner != nil {
			return d.defaultRunner.RunSessionTurnPayload(ctx, payload)
		}
		return fmt.Errorf("no session turn runner for channel type %q", channelType)
	}
	return runner.RunSessionTurnPayload(ctx, payload)
}
