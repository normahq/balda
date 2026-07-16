package sessionturnapp

import (
	"context"
	"testing"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/automode"
	"github.com/normahq/balda/internal/apps/balda/automodecmd"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"github.com/rs/zerolog"
)

type fakeAutoRuntimeState struct {
	state map[string]any
}

func (f *fakeAutoRuntimeState) RuntimeStateValue(_ context.Context, _ baldasession.SessionLocator, key string) (any, bool, error) {
	if f == nil || f.state == nil {
		return nil, false, nil
	}
	value, ok := f.state[key]
	return value, ok, nil
}

func (f *fakeAutoRuntimeState) UpdateRuntimeState(_ context.Context, _ baldasession.SessionLocator, state map[string]any) error {
	if f.state == nil {
		f.state = map[string]any{}
	}
	for key, value := range state {
		f.state[key] = value
	}
	return nil
}

type fakeAutoDispatcher struct {
	envelopes []actorlayer.Envelope
	state     *fakeAutoRuntimeState
}

func (f *fakeAutoDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	if env.Namespace == baldaexecution.NamespaceAutoModeCommand && f.state != nil {
		var payload automodecmd.Payload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			return nil, err
		}
		if err := f.state.UpdateRuntimeState(context.Background(), payload.Locator, payload.State); err != nil {
			return nil, err
		}
	}
	f.envelopes = append(f.envelopes, env)
	return &actortransport.DispatchReceipt{}, nil
}

func TestMaybeScheduleAutoTurnDispatchesSyntheticTurn(t *testing.T) {
	t.Parallel()

	state := &fakeAutoRuntimeState{
		state: map[string]any{
			automode.StateKeyEnabled:  true,
			automode.StateKeyMode:     automode.StateIdle,
			automode.StateKeyMaxTurns: automode.DefaultMaxTurns,
		},
	}
	dispatcher := &fakeAutoDispatcher{state: state}
	service := NewTurnExecutionServiceWithJobEvents(dispatcher, nil, state, zerolog.Nop())
	locator := baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0"}

	err := service.maybeScheduleAutoTurn(context.Background(), ExecutionRequest{
		UserID:          "tg-101",
		RequesterUserID: "tg-101",
		AgentSessionID:  "tg-1-0",
		Locator:         locator,
		DeliveryOptions: turncmd.NormalizeSessionDeliveryOptions(turncmd.SessionTurnPayload{}),
	}, "streamed_text", "visible output")
	if err != nil {
		t.Fatalf("maybeScheduleAutoTurn() error = %v", err)
	}
	if len(dispatcher.envelopes) != 2 {
		t.Fatalf("dispatches = %d, want 2", len(dispatcher.envelopes))
	}
	var payload turncmd.SessionTurnPayload
	if err := actorlayer.UnmarshalPayload(dispatcher.envelopes[1].Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Source != turncmd.SourceAuto {
		t.Fatalf("payload.Source = %q, want auto", payload.Source)
	}
	if payload.Text != automode.InternalPrompt(automode.DefaultMaxTurns) {
		t.Fatalf("payload.Text = %q, want internal prompt", payload.Text)
	}
	if got := automode.ParseInt(state.state[automode.StateKeyConsecutiveTurns], 0); got != 1 {
		t.Fatalf("consecutive turns state = %d, want 1", got)
	}
}

func TestMaybeScheduleAutoTurnStopsOnNoProgressForAutoTurns(t *testing.T) {
	t.Parallel()

	state := &fakeAutoRuntimeState{
		state: map[string]any{
			automode.StateKeyEnabled:          true,
			automode.StateKeyMode:             automode.StateRunning,
			automode.StateKeyMaxTurns:         automode.DefaultMaxTurns,
			automode.StateKeyConsecutiveTurns: 1,
			automode.StateKeyLastOutput:       "same-output",
		},
	}
	dispatcher := &fakeAutoDispatcher{state: state}
	service := NewTurnExecutionServiceWithJobEvents(dispatcher, nil, state, zerolog.Nop())
	locator := baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0"}

	err := service.maybeScheduleAutoTurn(context.Background(), ExecutionRequest{
		UserID:          "tg-101",
		RequesterUserID: "tg-101",
		AgentSessionID:  "tg-1-0",
		Locator:         locator,
		TurnSource:      turncmd.SourceAuto,
	}, "same-output", "same-output")
	if err != nil {
		t.Fatalf("maybeScheduleAutoTurn() error = %v", err)
	}
	if len(dispatcher.envelopes) != 1 {
		t.Fatalf("dispatches = %d, want 1", len(dispatcher.envelopes))
	}
	if got := state.state[automode.StateKeyMode]; got != automode.StateNoProgress {
		t.Fatalf("state mode = %#v, want %q", got, automode.StateNoProgress)
	}
	if got := state.state[automode.StateKeyLastStopReason]; got != "repeated_visible_output" {
		t.Fatalf("last stop reason = %#v, want repeated_visible_output", got)
	}
}
