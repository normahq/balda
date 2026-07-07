package goalkeeperworkflow

import (
	"fmt"
	"iter"
	"strings"
	"time"

	adkagent "google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/workflowagents/loopagent"
	adksession "google.golang.org/adk/v2/session"
)

const (
	RootAgentName                 = "GoalKeeper"
	MetadataEventKey              = "norma.goal.event"
	MetadataStepKey               = "norma.goal.step"
	MetadataAgentKey              = "norma.goal.agent"
	MetadataStepIDKey             = "norma.goal.step_id"
	MetadataEventCountKey         = "event_count"
	MetadataFinalResponseCountKey = "final_response_count"
	MetadataVisibleTextLenKey     = "visible_text_len"
	MetadataDurationMSKey         = "duration_ms"
	MetadataEscalatedKey          = "escalated"
	MetadataErrorKey              = "error"

	StepStarted   = "step_started"
	StepCompleted = "step_completed"
	StepFailed    = "step_failed"

	WorkerStep    = "worker"
	ValidatorStep = "validator"
)

type stepSpec struct {
	name string
	id   int
}

func New(worker, validator adkagent.Agent, maxIterations uint) (adkagent.Agent, error) {
	if worker == nil {
		return nil, fmt.Errorf("goal worker agent is required")
	}
	if validator == nil {
		return nil, fmt.Errorf("goal validator agent is required")
	}
	if maxIterations == 0 {
		return nil, fmt.Errorf("max iterations must be greater than zero")
	}

	validatorWithVerdict, err := newVerdictEscalatingValidator(validator)
	if err != nil {
		return nil, err
	}
	workerWithEvents, err := newStepEventAgent(worker, stepSpec{name: WorkerStep, id: 1})
	if err != nil {
		return nil, err
	}
	validatorWithEvents, err := newStepEventAgent(validatorWithVerdict, stepSpec{name: ValidatorStep, id: 2})
	if err != nil {
		return nil, err
	}
	return loopagent.New(loopagent.Config{
		MaxIterations: maxIterations,
		AgentConfig: adkagent.Config{
			Name:        RootAgentName,
			Description: "Retries a goal worker and goal validator until validation passes one goal.",
			SubAgents:   []adkagent.Agent{workerWithEvents, validatorWithEvents},
		},
	})
}

func newStepEventAgent(inner adkagent.Agent, spec stepSpec) (adkagent.Agent, error) {
	return adkagent.New(adkagent.Config{
		Name:        inner.Name(),
		Description: inner.Description(),
		SubAgents:   inner.SubAgents(),
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return runStepWithEvents(ctx, inner, spec)
		},
	})
}

func runStepWithEvents(
	ctx adkagent.InvocationContext,
	inner adkagent.Agent,
	spec stepSpec,
) iter.Seq2[*adksession.Event, error] {
	return func(yield func(*adksession.Event, error) bool) {
		startedAt := time.Now()
		if !yield(newStepEvent(ctx, inner, spec, StepStarted, stepStats{}), nil) {
			return
		}

		stats := stepStats{}
		for ev, err := range inner.Run(ctx) {
			if err != nil {
				stats.err = err
				stats.duration = time.Since(startedAt)
				if !yield(newStepEvent(ctx, inner, spec, StepFailed, stats), nil) {
					return
				}
				yield(ev, err)
				return
			}
			stats.record(ev)
			if ev != nil {
				ev.Author = RootAgentName
			}
			if !yield(ev, nil) {
				return
			}
		}
		stats.duration = time.Since(startedAt)
		yield(newStepEvent(ctx, inner, spec, StepCompleted, stats), nil)
	}
}

type stepStats struct {
	eventCount         int
	finalResponseCount int
	visibleTextLen     int
	duration           time.Duration
	escalated          bool
	err                error
}

func (s *stepStats) record(ev *adksession.Event) {
	if ev == nil {
		return
	}
	s.eventCount++
	text := visibleText(ev)
	s.visibleTextLen += len(text)
	if ev.IsFinalResponse() {
		s.finalResponseCount++
	}
	if ev.Actions.Escalate {
		s.escalated = true
	}
}

func newStepEvent(
	ctx adkagent.InvocationContext,
	inner adkagent.Agent,
	spec stepSpec,
	eventType string,
	stats stepStats,
) *adksession.Event {
	ev := adksession.NewEvent(ctx, ctx.InvocationID())
	ev.Author = RootAgentName
	ev.CustomMetadata = map[string]any{
		MetadataEventKey:  eventType,
		MetadataStepKey:   spec.name,
		MetadataStepIDKey: spec.id,
		MetadataAgentKey:  inner.Name(),
	}
	if eventType == StepCompleted || eventType == StepFailed {
		ev.CustomMetadata[MetadataEventCountKey] = stats.eventCount
		ev.CustomMetadata[MetadataFinalResponseCountKey] = stats.finalResponseCount
		ev.CustomMetadata[MetadataVisibleTextLenKey] = stats.visibleTextLen
		ev.CustomMetadata[MetadataDurationMSKey] = stats.duration.Milliseconds()
		ev.CustomMetadata[MetadataEscalatedKey] = stats.escalated
	}
	if stats.err != nil {
		ev.CustomMetadata[MetadataErrorKey] = stats.err.Error()
	}
	return ev
}

func newVerdictEscalatingValidator(validator adkagent.Agent) (adkagent.Agent, error) {
	return adkagent.New(adkagent.Config{
		Name:        validator.Name(),
		Description: validator.Description(),
		SubAgents:   validator.SubAgents(),
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				for ev, err := range validator.Run(ctx) {
					if err != nil {
						yield(nil, err)
						return
					}
					if ev != nil && ev.IsFinalResponse() && strings.HasPrefix(visibleText(ev), "verdict: pass") {
						ev.Actions.Escalate = true
					}
					if !yield(ev, nil) {
						return
					}
				}
			}
		},
	})
}

func visibleText(ev *adksession.Event) string {
	if ev == nil || ev.Content == nil {
		return ""
	}
	var parts []string
	for _, part := range ev.Content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, strings.TrimSpace(part.Text))
		}
	}
	return strings.Join(parts, "\n\n")
}
