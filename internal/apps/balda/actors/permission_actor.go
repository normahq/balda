package actors

import (
	"context"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
)

type permissionDecisionSink interface {
	Resolve(questionID string, decision permissioncmd.Decision)
}

type permissionActor struct {
	sink permissionDecisionSink
}

func (a *permissionActor) Address() string {
	return actorlayer.WildcardAddress(actorcmd.ActorTypePermission)
}

func (a *permissionActor) Handle(_ context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Namespace) != actorcmd.NamespacePermissionCommand {
		return actorlayer.PolicyError(fmt.Errorf("unsupported permission namespace %q", env.Namespace))
	}
	if a.sink == nil {
		return actorlayer.TransientError(fmt.Errorf("permission decision sink is required"))
	}
	resumeTarget := ""
	questionID := ""
	var decision permissioncmd.Decision
	switch strings.TrimSpace(env.Kind) {
	case actorcmd.KindQuestionAnswered:
		var payload questioncmd.AnsweredContinuation
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			return actorlayer.PermanentError(fmt.Errorf("decode permission answer: %w", err))
		}
		questionID = payload.QuestionID
		resumeTarget = payload.Resume.To
		decision = permissioncmd.Decision{OptionID: payload.Answer.SelectedOption, Source: "user"}
	case actorcmd.KindQuestionTimedOut:
		var payload questioncmd.TimedOutContinuation
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			return actorlayer.PermanentError(fmt.Errorf("decode permission timeout: %w", err))
		}
		questionID = payload.QuestionID
		resumeTarget = payload.Resume.To
		decision = permissioncmd.Decision{Source: "timeout", Canceled: true}
	case actorcmd.KindQuestionFailed:
		var payload questioncmd.FailedContinuation
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			return actorlayer.PermanentError(fmt.Errorf("decode permission failure: %w", err))
		}
		questionID = payload.QuestionID
		resumeTarget = payload.Resume.To
		decision = permissioncmd.Decision{Source: firstNonEmpty(payload.Failure.Code, "fail_closed"), Canceled: true}
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported permission kind %q", env.Kind))
	}
	resume, err := questioncmd.ParseResumeAddress(resumeTarget)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	if resume.Target != actorcmd.ActorTypePermission || strings.TrimSpace(resume.Key) != strings.TrimSpace(env.To.Key) {
		return actorlayer.PolicyError(fmt.Errorf("permission resume target mismatch"))
	}
	a.sink.Resolve(firstNonEmpty(questionID, resume.Key), decision)
	return nil
}
