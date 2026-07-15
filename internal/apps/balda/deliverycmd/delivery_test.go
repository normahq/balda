package deliverycmd

import (
	"testing"

	"github.com/baldaworks/go-actorlayer"
)

func TestQuestionEnvelopeCarriesTransportNeutralOptions(t *testing.T) {
	env, err := QuestionEnvelope(
		"",
		actorlayer.SystemAddress("test"),
		testLocator(),
		Profile{Format: FormatMarkdown},
		SettlementOutbox,
		"Choose",
		"question-1",
		"test",
		[]QuestionOption{{ID: "allow", Label: "Allow"}, {ID: "cancel", Label: "Cancel"}},
	)
	if err != nil {
		t.Fatalf("QuestionEnvelope() error = %v", err)
	}
	var payload Payload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Question == nil || payload.Question.ID != "question-1" || len(payload.Question.Options) != 2 {
		t.Fatalf("question = %+v", payload.Question)
	}
	if payload.Refs["question_id"] != "question-1" {
		t.Fatalf("refs = %+v", payload.Refs)
	}
}

func TestClearQuestionControlsEnvelopeIsIdempotentlyKeyed(t *testing.T) {
	env, err := ClearQuestionControlsEnvelope(actorlayer.SystemAddress("question"), testLocator(), "question-1", "42")
	if err != nil {
		t.Fatalf("ClearQuestionControlsEnvelope() error = %v", err)
	}
	if env.DedupeKey != "question:question-1:controls:clear" {
		t.Fatalf("dedupe key = %q", env.DedupeKey)
	}
	var payload Payload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Mode != ModeClearQuestionControls || payload.MessageID != "42" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestValidateQuestionRejectsDuplicateOptions(t *testing.T) {
	err := Validate(Payload{
		Mode: ModeAgentReply,
		Text: "Choose",
		Question: &Question{ID: "question-1", Options: []QuestionOption{
			{ID: "same", Label: "First"},
			{ID: "same", Label: "Second"},
		}},
	})
	if err == nil {
		t.Fatal("Validate() error = nil, want duplicate option error")
	}
}

func testLocator() Locator {
	return Locator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`}
}
