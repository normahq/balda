package actorlayer_test

import (
	"fmt"

	"github.com/normahq/balda/pkg/actorlayer"
)

func ExampleMarshalPayload() {
	payload, _ := actorlayer.MarshalPayload(struct {
		Text string `json:"text"`
	}{Text: "hello"})
	env := actorlayer.Envelope{
		ID:          "env-1",
		Namespace:   "example.command",
		Kind:        "message",
		From:        actorlayer.SystemAddress("example"),
		To:          actorlayer.ActorAddress{Target: "session", Key: "one"},
		PayloadJSON: payload,
	}
	raw, _ := actorlayer.EncodeEnvelope(env)
	got, _ := actorlayer.DecodeEnvelope(raw)
	fmt.Println(got.Kind, got.PayloadJSON)
	// Output: message {"text":"hello"}
}
