package memory_test

import (
	"context"
	"fmt"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/engine"
	"github.com/normahq/balda/pkg/actorlayer/transport/memory"
)

func ExampleTransport() {
	bus := memory.New(1)
	_, _ = bus.Dispatch(context.Background(), actorlayer.Envelope{
		ID:          "env-1",
		Namespace:   "example.command",
		Kind:        "message",
		From:        actorlayer.SystemAddress("example"),
		To:          actorlayer.ActorAddress{Target: "session", Key: "one"},
		PayloadJSON: `{"ok":true}`,
	})
	_ = bus.Run(context.Background(), func(ctx context.Context, delivery engine.Delivery) error {
		fmt.Println(delivery.Envelope().ID)
		_ = delivery.Ack(ctx)
		return bus.Drain(ctx)
	})
	// Output: env-1
}
