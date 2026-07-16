package goalcmd

// Package goalcmd is a compatibility shim over goalkeepercmd.

import (
	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

const (
	PayloadKindGoal      = goalkeepercmd.PayloadKindGoal
	PayloadKindQuestion  = goalkeepercmd.PayloadKindQuestion
	DefaultMaxIterations = goalkeepercmd.DefaultMaxIterations
)

type EnvelopePayload = goalkeepercmd.EnvelopePayload
type JobPayload = goalkeepercmd.JobPayload
type QuestionPayload = goalkeepercmd.QuestionPayload

func JobEnvelope(locator baldasession.SessionLocator, objective string, transportUserID string, maxIterations int) (actorlayer.Envelope, error) {
	return goalkeepercmd.JobEnvelope(locator, objective, transportUserID, maxIterations)
}

func JobEnvelopeWithOptions(locator baldasession.SessionLocator, deliveryOptions deliveryfmt.Options, objective string, transportUserID string, maxIterations int) (actorlayer.Envelope, error) {
	return goalkeepercmd.JobEnvelopeWithOptions(locator, deliveryOptions, objective, transportUserID, maxIterations)
}

func ResumeEnvelope(payload JobPayload) (actorlayer.Envelope, error) {
	return goalkeepercmd.ResumeEnvelope(payload)
}

func EncodeJobPayload(payload JobPayload) (string, error) {
	return goalkeepercmd.EncodeJobPayload(payload)
}

func DecodeJobPayload(raw string) (JobPayload, error) {
	return goalkeepercmd.DecodeJobPayload(raw)
}

func QuestionAnsweredEnvelope(jobID string, questionID string, answerText string, answeredAt string) (actorlayer.Envelope, error) {
	return goalkeepercmd.QuestionAnsweredEnvelope(jobID, questionID, answerText, answeredAt)
}

func QuestionTimedOutEnvelope(jobID string, questionID string, timedOutAt string) (actorlayer.Envelope, error) {
	return goalkeepercmd.QuestionTimedOutEnvelope(jobID, questionID, timedOutAt)
}

func NormalizeMaxIterations(v int) int {
	return goalkeepercmd.NormalizeMaxIterations(v)
}
