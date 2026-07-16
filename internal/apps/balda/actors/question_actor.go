package actors

import (
	"context"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/goalcmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"go.uber.org/fx"
)

type questionActor struct {
	dispatcher actortransport.Dispatcher
}

type questionActorParams struct {
	fx.In

	Dispatcher actortransport.Dispatcher
}

func (a *questionActor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeQuestion)
}

func (a *questionActor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Namespace) != baldaexecution.NamespaceQuestionCommand {
		return actorlayer.PolicyError(fmt.Errorf("unsupported question namespace %q", env.Namespace))
	}
	switch strings.TrimSpace(env.Kind) {
	case baldaexecution.KindQuestionAnswered:
		return a.handleAnswered(ctx, env)
	case baldaexecution.KindQuestionTimedOut:
		return a.handleTimedOut(ctx, env)
	case baldaexecution.KindQuestionFailed:
		return a.handleFailed(ctx, env)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported question kind %q", env.Kind))
	}
}

func (a *questionActor) handleFailed(ctx context.Context, env actorlayer.Envelope) error {
	var payload questioncmd.FailedContinuation
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode failed continuation: %w", err))
	}
	return a.resume(ctx, env, payload.QuestionID, payload.Resume, payload.Interaction, sessionQuestionFailedText(payload.QuestionID))
}

func (a *questionActor) handleAnswered(ctx context.Context, env actorlayer.Envelope) error {
	var payload questioncmd.AnsweredContinuation
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode answered continuation: %w", err))
	}
	return a.resume(ctx, env, payload.QuestionID, payload.Resume, payload.Interaction, payload.Answer.Text)
}

func (a *questionActor) handleTimedOut(ctx context.Context, env actorlayer.Envelope) error {
	var payload questioncmd.TimedOutContinuation
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode timed out continuation: %w", err))
	}
	return a.resume(ctx, env, payload.QuestionID, payload.Resume, payload.Interaction, sessionQuestionTimedOutText(payload.QuestionID))
}

func (a *questionActor) resume(ctx context.Context, env actorlayer.Envelope, questionID string, resume questioncmd.ResumeTarget, interaction questioncmd.InteractionContext, text string) error {
	if a.dispatcher == nil {
		return actorlayer.TransientError(fmt.Errorf("dispatcher is required"))
	}
	resumeTo, err := questioncmd.ParseResumeAddress(resume.To)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	if resumeTo.Target == baldaexecution.ActorTypeSession {
		dedupeKey := actorlayer.DedupeKeyOrID(env) + ":session-resume"
		next, err := turncmd.SessionTurnEnvelope(turncmd.SessionTurnPayload{
			Text:            strings.TrimSpace(text),
			Locator:         interaction.Locator,
			UserID:          strings.TrimSpace(interaction.RequestedBy.UserID),
			RequesterUserID: strings.TrimSpace(interaction.RequestedBy.UserID),
			Deliver:         true,
			Source:          "agent",
			DedupeKey:       dedupeKey,
			QuestionID:      strings.TrimSpace(questionID),
		})
		if err != nil {
			return actorlayer.PermanentError(fmt.Errorf("build question session continuation: %w", err))
		}
		_, err = a.dispatcher.Dispatch(ctx, next)
		if err != nil {
			return actorlayer.TransientError(fmt.Errorf("dispatch question session continuation: %w", err))
		}
		return nil
	}
	if resumeTo.Target == baldaexecution.ActorTypeGoalkeeper {
		var (
			next actorlayer.Envelope
			err  error
		)
		if strings.TrimSpace(env.Kind) == baldaexecution.KindQuestionTimedOut || strings.TrimSpace(env.Kind) == baldaexecution.KindQuestionFailed {
			next, err = goalcmd.QuestionTimedOutEnvelope(resumeTo.Key, questionID, env.Meta["timed_out_at"])
		} else {
			next, err = goalcmd.QuestionAnsweredEnvelope(resumeTo.Key, questionID, text, env.Meta["answered_at"])
		}
		if err != nil {
			return actorlayer.PermanentError(fmt.Errorf("build goal question continuation: %w", err))
		}
		if next.Meta == nil {
			next.Meta = make(map[string]string)
		}
		for key, value := range resume.Metadata {
			if trimmedKey := strings.TrimSpace(key); trimmedKey != "" && strings.TrimSpace(value) != "" {
				next.Meta[trimmedKey] = strings.TrimSpace(value)
			}
		}
		_, err = a.dispatcher.Dispatch(ctx, next)
		if err != nil {
			return actorlayer.TransientError(fmt.Errorf("dispatch goal question continuation: %w", err))
		}
		return nil
	}
	out := env
	out.ID = env.ID + ":resume"
	out.To = resumeTo
	out.Namespace = firstNonEmpty(strings.TrimSpace(resume.Namespace), env.Namespace)
	out.CorrelationID = strings.TrimSpace(resume.CorrelationID)
	out.DedupeKey = env.DedupeKey + ":resume"
	_, err = a.dispatcher.Dispatch(ctx, out)
	if err != nil {
		return actorlayer.TransientError(fmt.Errorf("dispatch question continuation: %w", err))
	}
	return nil
}

func sessionQuestionTimedOutText(questionID string) string {
	return fmt.Sprintf("Interactive question %s timed out without an answer. Continue based on that outcome.", strings.TrimSpace(questionID))
}

func sessionQuestionFailedText(questionID string) string {
	return fmt.Sprintf("Interactive question %s failed before an answer was received. Continue based on that outcome.", strings.TrimSpace(questionID))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
