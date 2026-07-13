package natsbus

import (
	"context"
	"testing"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

// TestRuntimeHarness provides a reusable built-in runtime command bus for tests.
type TestRuntimeHarness struct {
	Bus *Bus
}

// StartTestRuntime creates a built-in runtime bus backed by a temp store dir.
// It ensures required streams/consumers are available through NewBus startup.
func StartTestRuntime(t *testing.T, executionCfg baldaexecution.Config) *TestRuntimeHarness {
	t.Helper()
	bus, err := newStartedBus(t, Params{
		LC:        fxtest.NewLifecycle(t),
		Config:    baldaeventbus.Config{Embedded: true},
		Execution: executionCfg,
		StateDir:  t.TempDir(),
		Logger:    zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("StartTestRuntime() NewBus error = %v", err)
	}
	t.Cleanup(func() { _ = bus.Drain(context.Background()) })
	return &TestRuntimeHarness{Bus: bus}
}

func newStartedBus(t *testing.T, params Params) (*Bus, error) {
	t.Helper()
	bus, err := NewBus(params)
	if err != nil {
		return nil, err
	}
	if err := bus.Start(context.Background()); err != nil {
		return nil, err
	}
	return bus, nil
}

// Dispatch is a test command publisher helper for fixtures/scenarios.
func (h *TestRuntimeHarness) Dispatch(t *testing.T, env actorlayer.Envelope) *actortransport.DispatchReceipt {
	t.Helper()
	ack, err := h.Bus.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	return ack
}
