package goalkeeperworkflow

import (
	"fmt"
	"iter"
	"strings"
	"time"

	adkagent "google.golang.org/adk/agent"
	"google.golang.org/adk/agent/workflowagents/loopagent"
	adksession "google.golang.org/adk/session"
)

const (
	RootAgentName    = "GoalKeeper"
	MetadataEventKey = "norma.goal.event"
	MetadataStepKey  = "norma.goal.step"

	StepStarted   = "step_started"
	StepCompleted = "step_completed"
	StepFailed    = "step_failed"

	WorkerStep    = "worker"
	ValidatorStep = "validator"
)

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
	workerWithEvents, err := newStepEventAgent(worker, WorkerStep)
	if err != nil {
		return nil, err
	}
	validatorWithEvents, err := newStepEventAgent(validatorWithVerdict, ValidatorStep)
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

func newStepEventAgent(inner adkagent.Agent, step string) (adkagent.Agent, error) {
	return adkagent.New(adkagent.Config{
		Name:        inner.Name(),
		Description: inner.Description(),
		SubAgents:   inner.SubAgents(),
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				startedAt := time.Now()
				if !yield(newStepEvent(ctx.InvocationID(), step, StepStarted), nil) {
					return
				}
				for ev, err := range inner.Run(ctx) {
					if err != nil {
						failed := newStepEvent(ctx.InvocationID(), step, StepFailed)
						failed.CustomMetadata["duration_ms"] = time.Since(startedAt).Milliseconds()
						if !yield(failed, nil) {
							return
						}
						yield(ev, err)
						return
					}
					if !yield(ev, nil) {
						return
					}
				}
				completed := newStepEvent(ctx.InvocationID(), step, StepCompleted)
				completed.CustomMetadata["duration_ms"] = time.Since(startedAt).Milliseconds()
				yield(completed, nil)
			}
		},
	})
}

func newStepEvent(invocationID, step, eventType string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.CustomMetadata = map[string]any{
		MetadataEventKey: eventType,
		MetadataStepKey:  step,
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
