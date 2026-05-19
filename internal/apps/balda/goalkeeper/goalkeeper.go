package goalkeeper

import (
	"fmt"
	"iter"
	"strings"
	"time"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/workflowagents/loopagent"
	"google.golang.org/adk/session"
)

const rootAgentName = "Goalkeeper"

const (
	metadataEventKey              = "norma.goalkeeper.event"
	metadataStepKey               = "norma.goalkeeper.step"
	metadataStepIndexKey          = "norma.goalkeeper.step_index"
	metadataAgentKey              = "norma.goalkeeper.agent"
	metadataEventCountKey         = "event_count"
	metadataFinalResponseCountKey = "final_response_count"
	metadataVisibleTextLenKey     = "visible_text_len"
	metadataDurationMSKey         = "duration_ms"
	metadataEscalatedKey          = "escalated"
	metadataErrorKey              = "error"
)

const (
	stepEventStarted   = "step_started"
	stepEventCompleted = "step_completed"
	stepEventFailed    = "step_failed"

	workerStep    = "worker"
	validatorStep = "validator"
)

type stepSpec struct {
	name  string
	index int
}

// New creates a Goalkeeper workflow agent from options.
func New(opts Options) (agent.Agent, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate goalkeeper options: %w", err)
	}
	validator, err := newVerdictEscalatingValidator(opts.validator)
	if err != nil {
		return nil, err
	}
	worker, err := newStepEventAgent(opts.worker, stepSpec{name: workerStep, index: 1})
	if err != nil {
		return nil, err
	}
	validator, err = newStepEventAgent(validator, stepSpec{name: validatorStep, index: 2})
	if err != nil {
		return nil, err
	}
	workflow, err := loopagent.New(loopagent.Config{
		MaxIterations: opts.maxIterations,
		AgentConfig: agent.Config{
			Name:        rootAgentName,
			Description: "Retries a worker agent and validator agent until the validator passes one goal.",
			SubAgents:   []agent.Agent{worker, validator},
		},
	})
	if err != nil {
		return nil, err
	}
	return &subAgentViewAgent{
		Agent:     workflow,
		subAgents: []agent.Agent{opts.worker, opts.validator},
	}, nil
}

type subAgentViewAgent struct {
	agent.Agent
	subAgents []agent.Agent
}

func (a *subAgentViewAgent) SubAgents() []agent.Agent {
	return a.subAgents
}

func (a *subAgentViewAgent) FindAgent(name string) agent.Agent {
	if a.Name() == name {
		return a
	}
	return a.FindSubAgent(name)
}

func (a *subAgentViewAgent) FindSubAgent(name string) agent.Agent {
	for _, subAgent := range a.SubAgents() {
		if result := subAgent.FindAgent(name); result != nil {
			return result
		}
	}
	return nil
}

func newStepEventAgent(inner agent.Agent, spec stepSpec) (agent.Agent, error) {
	return agent.New(agent.Config{
		Name:        inner.Name(),
		Description: inner.Description(),
		SubAgents:   inner.SubAgents(),
		Run: func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
			return runStepWithEvents(ctx, inner, spec)
		},
	})
}

func runStepWithEvents(
	ctx agent.InvocationContext,
	inner agent.Agent,
	spec stepSpec,
) iter.Seq2[*session.Event, error] {
	return func(yield func(*session.Event, error) bool) {
		startedAt := time.Now()
		if !yield(newStepEvent(ctx.InvocationID(), inner, spec, stepEventStarted, stepStats{}), nil) {
			return
		}

		stats := stepStats{}
		for ev, err := range inner.Run(ctx) {
			if err != nil {
				stats.err = err
				stats.duration = time.Since(startedAt)
				if !yield(newStepEvent(ctx.InvocationID(), inner, spec, stepEventFailed, stats), nil) {
					return
				}
				yield(ev, err)
				return
			}
			stats.record(ev)
			if !yield(ev, nil) {
				return
			}
		}
		stats.duration = time.Since(startedAt)
		yield(newStepEvent(ctx.InvocationID(), inner, spec, stepEventCompleted, stats), nil)
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

func (s *stepStats) record(ev *session.Event) {
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
	invocationID string,
	inner agent.Agent,
	spec stepSpec,
	eventType string,
	stats stepStats,
) *session.Event {
	ev := session.NewEvent(invocationID)
	ev.CustomMetadata = map[string]any{
		metadataEventKey:     eventType,
		metadataStepKey:      spec.name,
		metadataStepIndexKey: spec.index,
		metadataAgentKey:     inner.Name(),
	}
	if eventType == stepEventCompleted || eventType == stepEventFailed {
		ev.CustomMetadata[metadataEventCountKey] = stats.eventCount
		ev.CustomMetadata[metadataFinalResponseCountKey] = stats.finalResponseCount
		ev.CustomMetadata[metadataVisibleTextLenKey] = stats.visibleTextLen
		ev.CustomMetadata[metadataDurationMSKey] = stats.duration.Milliseconds()
		ev.CustomMetadata[metadataEscalatedKey] = stats.escalated
	}
	if stats.err != nil {
		ev.CustomMetadata[metadataErrorKey] = stats.err.Error()
	}
	return ev
}

func newVerdictEscalatingValidator(validator agent.Agent) (agent.Agent, error) {
	return agent.New(agent.Config{
		Name:        validator.Name(),
		Description: validator.Description(),
		SubAgents:   validator.SubAgents(),
		Run: func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
			return func(yield func(*session.Event, error) bool) {
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

func visibleText(ev *session.Event) string {
	if ev == nil || ev.Content == nil {
		return ""
	}
	var parts []string
	for _, part := range ev.Content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}
