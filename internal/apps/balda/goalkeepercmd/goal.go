package goalkeepercmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

const (
	PayloadKindGoal      = "goal"
	PayloadKindQuestion  = "question"
	DefaultMaxIterations = 25
)

type EnvelopePayload struct {
	Kind     string           `json:"kind"`
	Goal     *JobPayload      `json:"goal,omitempty"`
	Question *QuestionPayload `json:"question,omitempty"`
}

type JobPayload struct {
	JobID           string                      `json:"job_id,omitempty"`
	Locator         baldasession.SessionLocator `json:"locator"`
	DeliveryOptions deliveryfmt.Options         `json:"delivery_options,omitempty,omitzero"`
	Objective       string                      `json:"objective"`
	TransportUserID string                      `json:"transport_user_id"`
	MaxIterations   int                         `json:"max_iterations,omitempty"`
}

type QuestionPayload struct {
	JobID      string `json:"job_id,omitempty"`
	QuestionID string `json:"question_id"`
	Action     string `json:"action"`
	AnswerText string `json:"answer_text,omitempty"`
	AnsweredAt string `json:"answered_at,omitempty"`
	TimedOutAt string `json:"timed_out_at,omitempty"`
}

func JobEnvelope(
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
	maxIterations int,
) (actorlayer.Envelope, error) {
	return JobEnvelopeWithOptions(locator, deliveryfmt.Options{}, objective, transportUserID, maxIterations)
}

func JobEnvelopeWithOptions(
	locator baldasession.SessionLocator,
	deliveryOptions deliveryfmt.Options,
	objective string,
	transportUserID string,
	maxIterations int,
) (actorlayer.Envelope, error) {
	jobID := "goal-" + locator.SessionID + "-" + uuid.NewString()
	payload := EnvelopePayload{
		Kind: PayloadKindGoal,
		Goal: &JobPayload{
			JobID:           jobID,
			Locator:         locator,
			DeliveryOptions: deliveryfmt.NormalizeOptions(deliveryOptions),
			Objective:       strings.TrimSpace(objective),
			TransportUserID: strings.TrimSpace(transportUserID),
			MaxIterations:   NormalizeMaxIterations(maxIterations),
		},
	}
	data, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode goalkeeper job payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: baldaexecution.NamespaceGoalkeeperCommand,
		Kind:      baldaexecution.KindGoal,
		From:      actorlayer.ActorAddress{Target: "telegram", Key: firstNonEmpty(transportUserID, locator.AddressKey, "unknown")},
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeGoalkeeper, Key: jobID},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, jobID), locator.SessionID),
		Priority:  90,
		Payload:   data,
	}, nil
}

func ResumeEnvelope(payload JobPayload) (actorlayer.Envelope, error) {
	trimmedJobID := strings.TrimSpace(payload.JobID)
	if trimmedJobID == "" {
		return actorlayer.Envelope{}, fmt.Errorf("job id is required")
	}
	normalized := payload
	normalized.JobID = trimmedJobID
	normalized.Objective = strings.TrimSpace(normalized.Objective)
	normalized.TransportUserID = strings.TrimSpace(normalized.TransportUserID)
	normalized.DeliveryOptions = deliveryfmt.NormalizeOptions(normalized.DeliveryOptions)
	normalized.MaxIterations = NormalizeMaxIterations(normalized.MaxIterations)
	data, err := actorlayer.MarshalPayload(EnvelopePayload{
		Kind: PayloadKindGoal,
		Goal: &normalized,
	})
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode resumed goalkeeper job payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: baldaexecution.NamespaceGoalkeeperCommand,
		Kind:      baldaexecution.KindGoal,
		From:      actorlayer.SystemAddress("question"),
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeGoalkeeper, Key: trimmedJobID},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, trimmedJobID), normalized.Locator.SessionID),
		Priority:  90,
		DedupeKey: "goal-resume:" + trimmedJobID,
		Payload:   data,
	}, nil
}

func EncodeJobPayload(payload JobPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode goalkeeper job payload json: %w", err)
	}
	return string(data), nil
}

func DecodeJobPayload(raw string) (JobPayload, error) {
	var payload JobPayload
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload); err != nil {
		return JobPayload{}, fmt.Errorf("decode goalkeeper job payload json: %w", err)
	}
	return payload, nil
}

func QuestionAnsweredEnvelope(jobID string, questionID string, answerText string, answeredAt string) (actorlayer.Envelope, error) {
	return questionEnvelope(jobID, QuestionPayload{
		JobID:      strings.TrimSpace(jobID),
		QuestionID: strings.TrimSpace(questionID),
		Action:     "answered",
		AnswerText: strings.TrimSpace(answerText),
		AnsweredAt: strings.TrimSpace(answeredAt),
	})
}

func QuestionTimedOutEnvelope(jobID string, questionID string, timedOutAt string) (actorlayer.Envelope, error) {
	return questionEnvelope(jobID, QuestionPayload{
		JobID:      strings.TrimSpace(jobID),
		QuestionID: strings.TrimSpace(questionID),
		Action:     "timed_out",
		TimedOutAt: strings.TrimSpace(timedOutAt),
	})
}

func questionEnvelope(jobID string, payload QuestionPayload) (actorlayer.Envelope, error) {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return actorlayer.Envelope{}, fmt.Errorf("job id is required")
	}
	if strings.TrimSpace(payload.QuestionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("question id is required")
	}
	data, err := actorlayer.MarshalPayload(EnvelopePayload{
		Kind:     PayloadKindQuestion,
		Question: &payload,
	})
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode goalkeeper question payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: baldaexecution.NamespaceGoalkeeperCommand,
		Kind:      baldaexecution.KindGoal,
		From:      actorlayer.SystemAddress("question"),
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeGoalkeeper, Key: trimmedJobID},
		Meta:      baldaexecution.WithJobIDMeta(nil, trimmedJobID),
		Priority:  85,
		DedupeKey: "goal-question:" + trimmedJobID + ":" + strings.TrimSpace(payload.QuestionID) + ":" + strings.TrimSpace(payload.Action),
		Payload:   data,
	}, nil
}

func NormalizeMaxIterations(v int) int {
	if v <= 0 {
		return DefaultMaxIterations
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
