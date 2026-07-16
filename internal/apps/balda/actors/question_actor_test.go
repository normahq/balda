package actors

import (
	"context"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
)

const testQuestionID = "question-1"

func TestQuestionActorAnsweredDispatchesSessionTurn(t *testing.T) {
	dispatcher := &fakeTurnDispatcher{}
	actor := &questionActor{dispatcher: dispatcher}
	env, err := questioncmd.AnsweredEnvelope(
		questioncmd.ResumeTarget{To: "session:tg-1-0"},
		questioncmd.InteractionContext{
			SessionID:   "tg-1-0",
			ChannelKind: "telegram",
			Locator:     baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
			RequestedBy: questioncmd.UserRef{UserID: "101"},
		},
		questioncmd.Answer{
			Text:       "да",
			AnsweredAt: time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC),
		},
		testQuestionID,
	)
	if err != nil {
		t.Fatalf("AnsweredEnvelope() error = %v", err)
	}

	if err := actor.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatched commands = %d, want 1", len(dispatcher.commands))
	}
	if got := dispatcher.commands[0].Namespace; got != baldaexecution.NamespaceGoalkeeperCommand {
		t.Fatalf("namespace = %q, want %q", got, baldaexecution.NamespaceGoalkeeperCommand)
	}
	var payload turncmd.SessionTurnPayload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode session continuation payload: %v", err)
	}
	if payload.QuestionID != testQuestionID {
		t.Fatalf("question_id = %q, want question-1", payload.QuestionID)
	}
	if payload.Text != "да" {
		t.Fatalf("text = %q, want да", payload.Text)
	}
	if got := dispatcher.commands[0].DedupeKey; got != env.DedupeKey+":session-resume" {
		t.Fatalf("dedupe key = %q, want downstream-specific key", got)
	}
}

func TestQuestionActorTimedOutDispatchesUsableSessionTurn(t *testing.T) {
	dispatcher := &fakeTurnDispatcher{}
	actor := &questionActor{dispatcher: dispatcher}
	env, err := questioncmd.TimedOutEnvelope(
		questioncmd.ResumeTarget{To: "session:tg-1-0"},
		questioncmd.InteractionContext{
			SessionID:   "tg-1-0",
			ChannelKind: "telegram",
			Locator:     baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
		},
		testQuestionID,
		time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("TimedOutEnvelope() error = %v", err)
	}

	if err := actor.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatched commands = %d, want 1", len(dispatcher.commands))
	}
	var payload turncmd.SessionTurnPayload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode session continuation payload: %v", err)
	}
	if payload.Text == "" || payload.QuestionID != testQuestionID {
		t.Fatalf("timeout payload = %+v, want usable question continuation", payload)
	}
}

func TestQuestionActorAnsweredDispatchesGoalContinuation(t *testing.T) {
	dispatcher := &fakeTurnDispatcher{}
	actor := &questionActor{dispatcher: dispatcher}
	env, err := questioncmd.AnsweredEnvelope(
		questioncmd.ResumeTarget{
			To: "goalkeeper:goal-tg-1-0-123",
			Metadata: map[string]string{
				"goal_payload": `{"job_id":"goal-tg-1-0-123","locator":{"session_id":"tg-1-0","channel_type":"telegram","address_key":"1:0","address_json":"{\"chat_id\":1,\"topic_id\":0}"},"objective":"ship release","transport_user_id":"101","max_iterations":1}`,
			},
		},
		questioncmd.InteractionContext{
			SessionID:   "tg-1-0",
			ChannelKind: "telegram",
			Locator:     baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
			RequestedBy: questioncmd.UserRef{UserID: "101"},
		},
		questioncmd.Answer{
			Text:       "да",
			AnsweredAt: time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC),
		},
		testQuestionID,
	)
	if err != nil {
		t.Fatalf("AnsweredEnvelope() error = %v", err)
	}

	if err := actor.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatched commands = %d, want 1", len(dispatcher.commands))
	}
	var payload goalkeepercmd.EnvelopePayload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode goal continuation payload: %v", err)
	}
	if payload.Kind != goalkeepercmd.PayloadKindQuestion || payload.Question == nil {
		t.Fatalf("goal continuation payload = %+v, want question payload", payload)
	}
	if payload.Question.QuestionID != testQuestionID {
		t.Fatalf("question_id = %q, want question-1", payload.Question.QuestionID)
	}
	if payload.Question.AnswerText != "да" {
		t.Fatalf("answer_text = %q, want да", payload.Question.AnswerText)
	}
	if dispatcher.commands[0].Meta["goal_payload"] == "" {
		t.Fatal("goal_payload metadata = empty, want propagated resume metadata")
	}
}
