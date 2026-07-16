package questioncmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
)

const (
	StatusPending      = "pending"
	StatusAnswered     = "answered"
	StatusTimedOut     = "timed_out"
	StatusCanceled     = "canceled"
	StatusFailed       = "failed"
	ActionAnswered     = "answered"
	ActionTimedOut     = "timed_out"
	ActionFailed       = "failed"
	DefaultKindAsk     = "question"
	ResponderAny       = "any"
	ResponderRequester = "requester"
	timeoutContentKind = "question_timeout"
)

type InteractionOrigin struct {
	RootTurnID   string `json:"root_turn_id,omitempty"`
	RootJobID    string `json:"root_job_id,omitempty"`
	RootRunID    string `json:"root_run_id,omitempty"`
	TriggerMsgID string `json:"trigger_message_id,omitempty"`
}

type UserRef struct {
	UserID      string `json:"user_id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type InteractionContext struct {
	SessionID      string              `json:"session_id"`
	ChannelKind    string              `json:"channel_kind,omitempty"`
	Locator        deliverycmd.Locator `json:"locator"`
	RequestedBy    UserRef             `json:"requested_by,omitempty"`
	Origin         InteractionOrigin   `json:"origin,omitempty"`
	ConversationID string              `json:"conversation_id,omitempty"`
}

type ResumeTarget struct {
	To            string            `json:"to"`
	Namespace     string            `json:"namespace,omitempty"`
	Subject       string            `json:"subject,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type Option struct {
	ID    string `json:"id,omitempty"`
	Label string `json:"label"`
}

type Request struct {
	Prompt        string            `json:"prompt"`
	Options       []Option          `json:"options,omitempty"`
	AllowFreeText bool              `json:"allow_free_text,omitempty"`
	Responder     string            `json:"responder,omitempty"`
	Timeout       time.Duration     `json:"timeout,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type DeliveryRef struct {
	Provider          string `json:"provider,omitempty"`
	ConversationKey   string `json:"conversation_key,omitempty"`
	ProviderMessageID string `json:"provider_message_id,omitempty"`
	ReplyHandle       string `json:"reply_handle,omitempty"`
	ControlHandle     string `json:"control_handle,omitempty"`
}

type InboundReply struct {
	Provider         string    `json:"provider,omitempty"`
	SessionID        string    `json:"session_id,omitempty"`
	ConversationKey  string    `json:"conversation_key,omitempty"`
	ReplyToMessageID string    `json:"reply_to_message_id,omitempty"`
	MessageID        string    `json:"message_id,omitempty"`
	User             UserRef   `json:"user,omitempty"`
	Text             string    `json:"text,omitempty"`
	ReceivedAt       time.Time `json:"received_at,omitempty"`
}

// InboundSelection describes a structured choice made through channel-native
// controls. Channels may identify the choice by stable option ID or by its
// one-based position in the original question.
type InboundSelection struct {
	Provider          string    `json:"provider,omitempty"`
	SessionID         string    `json:"session_id,omitempty"`
	ConversationKey   string    `json:"conversation_key,omitempty"`
	QuestionID        string    `json:"question_id"`
	ProviderMessageID string    `json:"provider_message_id,omitempty"`
	User              UserRef   `json:"user,omitempty"`
	OptionID          string    `json:"option_id,omitempty"`
	OptionIndex       int       `json:"option_index,omitempty"`
	ReceivedAt        time.Time `json:"received_at,omitempty"`
}

type Answer struct {
	Text           string    `json:"text,omitempty"`
	SelectedOption string    `json:"selected_option,omitempty"`
	AnsweredBy     UserRef   `json:"answered_by,omitempty"`
	AnsweredAt     time.Time `json:"answered_at,omitempty"`
	ProviderMsgID  string    `json:"provider_message_id,omitempty"`
}

type AnsweredContinuation struct {
	QuestionID  string             `json:"question_id"`
	Action      string             `json:"action"`
	Resume      ResumeTarget       `json:"resume"`
	Interaction InteractionContext `json:"interaction"`
	Answer      Answer             `json:"answer"`
}

type TimedOutContinuation struct {
	QuestionID  string             `json:"question_id"`
	Action      string             `json:"action"`
	Resume      ResumeTarget       `json:"resume"`
	Interaction InteractionContext `json:"interaction"`
	TimedOutAt  time.Time          `json:"timed_out_at"`
}

// Failure describes why a pending question could not be presented or
// completed. Code is stable policy input; Message is diagnostic only.
type Failure struct {
	Code     string    `json:"code"`
	Message  string    `json:"message,omitempty"`
	FailedAt time.Time `json:"failed_at"`
}

type FailedContinuation struct {
	QuestionID  string             `json:"question_id"`
	Action      string             `json:"action"`
	Resume      ResumeTarget       `json:"resume"`
	Interaction InteractionContext `json:"interaction"`
	Failure     Failure            `json:"failure"`
}

func AnsweredEnvelope(resume ResumeTarget, interaction InteractionContext, answer Answer, questionID string) (actorlayer.Envelope, error) {
	to, err := routerAddress()
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	data, err := actorlayer.MarshalPayload(AnsweredContinuation{
		QuestionID:  strings.TrimSpace(questionID),
		Action:      ActionAnswered,
		Resume:      resume,
		Interaction: interaction,
		Answer:      answer,
	})
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode answered continuation: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: actorcmd.NamespaceQuestionCommand,
		Kind:      actorcmd.KindQuestionAnswered,
		From:      actorlayer.SystemAddress("question"),
		To:        to,
		Meta: map[string]string{
			"resume_to":             strings.TrimSpace(resume.To),
			"resume_namespace":      strings.TrimSpace(resume.Namespace),
			"resume_subject":        strings.TrimSpace(resume.Subject),
			"resume_correlation_id": strings.TrimSpace(resume.CorrelationID),
			"session_id":            strings.TrimSpace(interaction.SessionID),
			"question_id":           strings.TrimSpace(questionID),
			"answered_at":           answer.AnsweredAt.UTC().Format(time.RFC3339),
		},
		Priority:  80,
		DedupeKey: "question:" + strings.TrimSpace(questionID) + ":answered",
		Payload:   data,
	}, nil
}

func TimedOutEnvelope(resume ResumeTarget, interaction InteractionContext, questionID string, timedOutAt time.Time) (actorlayer.Envelope, error) {
	to, err := routerAddress()
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	data, err := actorlayer.MarshalPayload(TimedOutContinuation{
		QuestionID:  strings.TrimSpace(questionID),
		Action:      ActionTimedOut,
		Resume:      resume,
		Interaction: interaction,
		TimedOutAt:  timedOutAt.UTC(),
	})
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode timed out continuation: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: actorcmd.NamespaceQuestionCommand,
		Kind:      actorcmd.KindQuestionTimedOut,
		From:      actorlayer.SystemAddress("question"),
		To:        to,
		Meta: map[string]string{
			"resume_to":             strings.TrimSpace(resume.To),
			"resume_namespace":      strings.TrimSpace(resume.Namespace),
			"resume_subject":        strings.TrimSpace(resume.Subject),
			"resume_correlation_id": strings.TrimSpace(resume.CorrelationID),
			"session_id":            strings.TrimSpace(interaction.SessionID),
			"question_id":           strings.TrimSpace(questionID),
			"timed_out_at":          timedOutAt.UTC().Format(time.RFC3339),
		},
		Priority:  80,
		DedupeKey: "question:" + strings.TrimSpace(questionID) + ":timed_out",
		Payload:   data,
	}, nil
}

func FailedEnvelope(resume ResumeTarget, interaction InteractionContext, questionID string, failure Failure) (actorlayer.Envelope, error) {
	to, err := routerAddress()
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	data, err := actorlayer.MarshalPayload(FailedContinuation{
		QuestionID:  strings.TrimSpace(questionID),
		Action:      ActionFailed,
		Resume:      resume,
		Interaction: interaction,
		Failure:     failure,
	})
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode failed continuation: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: actorcmd.NamespaceQuestionCommand,
		Kind:      actorcmd.KindQuestionFailed,
		From:      actorlayer.SystemAddress("question"),
		To:        to,
		Meta: map[string]string{
			"resume_to":             strings.TrimSpace(resume.To),
			"resume_namespace":      strings.TrimSpace(resume.Namespace),
			"resume_subject":        strings.TrimSpace(resume.Subject),
			"resume_correlation_id": strings.TrimSpace(resume.CorrelationID),
			"session_id":            strings.TrimSpace(interaction.SessionID),
			"question_id":           strings.TrimSpace(questionID),
			"failure_code":          strings.TrimSpace(failure.Code),
			"failed_at":             failure.FailedAt.UTC().Format(time.RFC3339),
		},
		Priority:  80,
		DedupeKey: "question:" + strings.TrimSpace(questionID) + ":failed",
		Payload:   data,
	}, nil
}

func ParseResumeAddress(raw string) (actorlayer.ActorAddress, error) {
	parts := strings.SplitN(strings.TrimSpace(raw), ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return actorlayer.ActorAddress{}, fmt.Errorf("resume target %q must be <target>:<key>", raw)
	}
	return actorlayer.ActorAddress{Target: strings.TrimSpace(parts[0]), Key: strings.TrimSpace(parts[1])}, nil
}

func routerAddress() (actorlayer.ActorAddress, error) {
	return ParseResumeAddress(actorcmd.ActorTypeQuestion + ":router")
}

type timeoutContent struct {
	Kind       string `json:"kind"`
	QuestionID string `json:"question_id"`
}

func TimeoutScheduledContent(questionID string) (string, error) {
	data, err := json.Marshal(timeoutContent{
		Kind:       timeoutContentKind,
		QuestionID: strings.TrimSpace(questionID),
	})
	if err != nil {
		return "", fmt.Errorf("encode question timeout content: %w", err)
	}
	return string(data), nil
}

func ParseTimeoutScheduledContent(text string) (string, bool) {
	var payload timeoutContent
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &payload); err != nil {
		return "", false
	}
	if strings.TrimSpace(payload.Kind) != timeoutContentKind || strings.TrimSpace(payload.QuestionID) == "" {
		return "", false
	}
	return strings.TrimSpace(payload.QuestionID), true
}
