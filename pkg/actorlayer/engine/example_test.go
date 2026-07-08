package engine_test

import (
	"context"
	"fmt"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
	"github.com/normahq/balda/pkg/actorlayer/engine"
)

type exampleActor struct{}

func (exampleActor) Address() string { return actorlayer.WildcardAddress("session") }

func (exampleActor) Handle(_ context.Context, env actorlayer.Envelope) error {
	fmt.Println(env.To.Key)
	return nil
}

type exampleDelivery struct {
	env actorlayer.Envelope
}

func (d exampleDelivery) Envelope() engine.Envelope { return d.env }
func (exampleDelivery) Attempt() int                { return 1 }
func (exampleDelivery) MaxAttempts() int            { return 1 }
func (exampleDelivery) InProgress(context.Context) error {
	return nil
}
func (exampleDelivery) Ack(context.Context) error { return nil }
func (exampleDelivery) Retry(context.Context, time.Duration, string) error {
	return nil
}
func (exampleDelivery) DeadLetter(context.Context, string) error { return nil }

func ExampleDispatchRuntime_Handle() {
	registry := dispatch.NewMemoryRegistry()
	_ = registry.Register(exampleActor{})
	runtime, _ := engine.NewDispatchRuntime(engine.RuntimeConfig{
		Registry:  registry,
		AddressOf: func(env engine.Envelope) (string, error) { return env.To.String() },
		Retry: engine.RetryPolicy{
			IsRetryable: actorlayer.IsRetryableError,
		},
	})
	_ = runtime.Handle(context.Background(), exampleDelivery{env: actorlayer.Envelope{
		ID:          "env-1",
		Namespace:   "example.command",
		Kind:        "message",
		From:        actorlayer.SystemAddress("example"),
		To:          actorlayer.ActorAddress{Target: "session", Key: "one"},
		PayloadJSON: `{"ok":true}`,
	}})
	// Output: one
}
