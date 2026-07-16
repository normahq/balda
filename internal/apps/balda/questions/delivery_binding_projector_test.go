package questions

import (
	"context"
	"testing"

	"github.com/baldaworks/go-actorlayer"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/rs/zerolog"
)

func TestDeliveryBindingProjectorBindsQuestionRefFromDeliveryEvent(t *testing.T) {
	store := &fakeStore{}
	projector := NewDeliveryBindingProjector(deliveryBindingProjectorParams{
		Service: New(store, nil, zerolog.Nop()),
		Logger:  zerolog.Nop(),
	})
	if err := projector.Project(context.Background(), baldaexecution.SubjectEventDeliverySent, actorlayer.Envelope{
		Payload: actorlayer.Payload{
			Encoding: actorlayer.EncodingJSON,
			Data: []byte(`{
				"provider":"telegram",
				"conversation_key":"1:0",
				"provider_message_id":"42",
				"control_handle":"telegram:message:delete",
				"refs":{"question_id":"question-1"}
			}`),
		},
	}); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if store.record.QuestionID != "question-1" {
		t.Fatalf("question id = %q, want question-1", store.record.QuestionID)
	}
	if store.record.ProviderMessageID != "42" {
		t.Fatalf("provider message id = %q, want 42", store.record.ProviderMessageID)
	}
	if store.record.ControlHandle != "telegram:message:delete" {
		t.Fatalf("control handle = %q, want telegram:message:delete", store.record.ControlHandle)
	}
}
