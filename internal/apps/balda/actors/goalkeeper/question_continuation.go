package goalkeeper

import (
	"context"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func (c *coordinator) handleQuestionContinuation(ctx context.Context, env actorlayer.Envelope, payload goalQuestionPayload) error {
	if c == nil || c.jobs == nil {
		return actorlayer.TransientError(fmt.Errorf("goal job service is required"))
	}
	jobID := strings.TrimSpace(payload.JobID)
	if jobID == "" {
		jobID = strings.TrimSpace(env.To.Key)
	}
	if jobID == "" {
		return actorlayer.PolicyError(fmt.Errorf("goalkeeper question continuation job id is required"))
	}
	statusReason := "question continuation received"
	if strings.TrimSpace(payload.Action) == "timed_out" {
		statusReason = "question timed out"
	}
	if err := c.jobs.MarkStatus(ctx, jobID, baldastate.JobStatusWaitingForUser, actorName, env.ID, statusReason, map[string]any{
		"question_id":  strings.TrimSpace(payload.QuestionID),
		"action":       strings.TrimSpace(payload.Action),
		"answer_text":  strings.TrimSpace(payload.AnswerText),
		"answered_at":  strings.TrimSpace(payload.AnsweredAt),
		"timed_out_at": strings.TrimSpace(payload.TimedOutAt),
	}); err != nil {
		return actorlayer.TransientError(err)
	}
	if c.events != nil {
		if err := c.events.AppendEvent(ctx, jobID, "goal.question."+strings.TrimSpace(payload.Action), actorName, env.ID, map[string]any{
			"question_id": strings.TrimSpace(payload.QuestionID),
			"answer_text": strings.TrimSpace(payload.AnswerText),
		}); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	if strings.TrimSpace(payload.Action) == "timed_out" {
		return nil
	}
	rawResumePayload := strings.TrimSpace(env.Meta["resume_goal_payload"])
	if rawResumePayload == "" {
		rawResumePayload = strings.TrimSpace(env.Meta["goal_payload"])
	}
	if rawResumePayload == "" {
		return actorlayer.PolicyError(fmt.Errorf("goalkeeper resume payload is required"))
	}
	resumePayload, err := goalkeepercmd.DecodeJobPayload(rawResumePayload)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	resumePayload.JobID = jobID
	resumePayload.Objective = appendGoalClarification(resumePayload.Objective, payload.AnswerText)
	next, err := goalkeepercmd.ResumeEnvelope(resumePayload)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	if c.dispatcher == nil {
		return actorlayer.TransientError(fmt.Errorf("actor dispatcher is required"))
	}
	if _, err := c.dispatcher.Dispatch(ctx, next); err != nil {
		return actorlayer.TransientError(fmt.Errorf("dispatch resumed goal after question: %w", err))
	}
	return nil
}

func appendGoalClarification(objective string, answer string) string {
	trimmedObjective := strings.TrimSpace(objective)
	trimmedAnswer := strings.TrimSpace(answer)
	if trimmedAnswer == "" {
		return trimmedObjective
	}
	if trimmedObjective == "" {
		return "User clarification:\n" + trimmedAnswer
	}
	return trimmedObjective + "\n\nUser clarification:\n" + trimmedAnswer
}
