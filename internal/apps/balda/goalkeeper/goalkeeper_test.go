package goalkeeper

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"

	"google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	"google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestNewRequiresWorker(t *testing.T) {
	t.Parallel()

	validator := mustNewTestAgent(t, "validator", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})
	workflow, err := New(NewOptions(nil, validator))
	if err == nil {
		t.Fatalf("New(NewOptions(nil, validator)) error = nil, want validation error")
	}
	if workflow != nil {
		t.Fatalf("New(nil, validator) workflow = %v, want nil", workflow)
	}
}

func TestNewRequiresValidator(t *testing.T) {
	t.Parallel()

	worker := mustNewTestAgent(t, "worker", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})
	workflow, err := New(NewOptions(worker, nil))
	if err == nil {
		t.Fatalf("New(NewOptions(worker, nil)) error = nil, want validation error")
	}
	if workflow != nil {
		t.Fatalf("New(worker, nil) workflow = %v, want nil", workflow)
	}
}

func TestWorkflowSubAgentsAreProvidedWorkerAndValidator(t *testing.T) {
	t.Parallel()

	worker := mustNewTestAgent(t, "worker", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})
	validator := mustNewTestAgent(t, "validator", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	subAgents := workflow.SubAgents()
	if len(subAgents) != 2 {
		t.Fatalf("len(SubAgents()) = %d, want 2", len(subAgents))
	}
	if subAgents[0] != worker || subAgents[1] != validator {
		t.Fatalf("SubAgents() = [%s, %s], want provided worker and validator", subAgents[0].Name(), subAgents[1].Name())
	}
	if got := workflow.FindAgent(worker.Name()); got != worker {
		t.Fatalf("FindAgent(%q) = %v, want provided worker", worker.Name(), got)
	}
	if got := workflow.FindAgent(validator.Name()); got != validator {
		t.Fatalf("FindAgent(%q) = %v, want provided validator", validator.Name(), got)
	}
}

func TestWorkflowRunsWorkerThenValidatorWithSharedSession(t *testing.T) {
	t.Parallel()

	var order []string
	worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			order = append(order, "worker")
			if err := ctx.Session().State().Set("worker_result", "created artifact"); err != nil {
				yield(nil, err)
				return
			}
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			order = append(order, "validator")
			value, err := ctx.Session().State().Get("worker_result")
			if err != nil {
				yield(nil, err)
				return
			}
			yield(textEvent(ctx.InvocationID(), "verdict: pass\nworker_result="+value.(string)), nil)
		}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := runTestAgentOnce(t, workflow, "Goal:\nship")
	if got != "verdict: pass\nworker_result=created artifact" {
		t.Fatalf("workflow output = %q, want validator output from shared state", got)
	}
	if strings.Join(order, ",") != "worker,validator" {
		t.Fatalf("order = %v, want worker then validator", order)
	}
}

func TestWorkflowPassVerdictEscalatesAndStopsAfterOneIteration(t *testing.T) {
	t.Parallel()

	var workerRuns int
	var validatorRuns int
	worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			workerRuns++
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			validatorRuns++
			yield(textEvent(ctx.InvocationID(), "verdict: pass\nall good"), nil)
		}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := runTestAgentOnce(t, workflow, "Goal:\nship")
	if got != "verdict: pass\nall good" {
		t.Fatalf("workflow output = %q, want validator pass output", got)
	}
	if workerRuns != 1 || validatorRuns != 1 {
		t.Fatalf("workerRuns, validatorRuns = %d, %d; want 1, 1", workerRuns, validatorRuns)
	}
}

func TestWorkflowEmitsSyntheticStepEvents(t *testing.T) {
	t.Parallel()

	worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			yield(textEvent(ctx.InvocationID(), "verdict: pass\nall good"), nil)
		}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	events, storedSession := collectTestRun(t, workflow, "Goal:\nship")

	want := []string{
		"worker:step_started",
		"worker:worker result",
		"worker:step_completed",
		"validator:step_started",
		"validator:verdict: pass\nall good",
		"validator:step_completed",
	}
	if got := eventSequence(events); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}

	completed := metadataEvents(events, stepEventCompleted)
	if len(completed) != 2 {
		t.Fatalf("completed event count = %d, want 2", len(completed))
	}
	for _, ev := range completed {
		if ev.Content != nil {
			t.Fatalf("synthetic event content = %#v, want nil", ev.Content)
		}
		if ev.Partial {
			t.Fatalf("synthetic event Partial = true, want false")
		}
		if _, ok := ev.CustomMetadata[metadataDurationMSKey]; !ok {
			t.Fatalf("completed metadata = %#v, missing duration", ev.CustomMetadata)
		}
	}
	if got := completed[1].CustomMetadata[metadataEscalatedKey]; got != true {
		t.Fatalf("validator completed escalated = %v, want true", got)
	}

	if got := countStoredStepEvents(storedSession); got != 4 {
		t.Fatalf("stored synthetic step events = %d, want 4", got)
	}
}

func TestWorkflowFailThenPassRetriesUntilPass(t *testing.T) {
	t.Parallel()

	var order []string
	var validatorRuns int
	worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			order = append(order, "worker")
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			validatorRuns++
			order = append(order, "validator")
			if validatorRuns == 1 {
				yield(textEvent(ctx.InvocationID(), "verdict: fail\nretry with feedback"), nil)
				return
			}
			yield(textEvent(ctx.InvocationID(), "verdict: pass\nfixed"), nil)
		}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := runTestAgentOnce(t, workflow, "Goal:\nship")
	if got != "verdict: pass\nfixed" {
		t.Fatalf("workflow output = %q, want second validator pass output", got)
	}
	if strings.Join(order, ",") != "worker,validator,worker,validator" {
		t.Fatalf("order = %v, want two worker-validator iterations", order)
	}

	validatorRuns = 0
	events, _ := collectTestRun(t, workflow, "Goal:\nship")
	completed := metadataEvents(events, stepEventCompleted)
	if len(completed) != 4 {
		t.Fatalf("completed event count = %d, want 4", len(completed))
	}
}

func TestWorkflowRepeatedFailStopsAtMaxIterations(t *testing.T) {
	t.Parallel()

	var workerRuns int
	var validatorRuns int
	worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			workerRuns++
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			validatorRuns++
			yield(textEvent(ctx.InvocationID(), "verdict: fail\nnot yet"), nil)
		}
	})

	workflow, err := New(NewOptions(worker, validator, WithMaxIterations(3)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := runTestAgentOnce(t, workflow, "Goal:\nship")
	if got != "verdict: fail\nnot yet" {
		t.Fatalf("workflow output = %q, want final validator fail output", got)
	}
	if workerRuns != 3 || validatorRuns != 3 {
		t.Fatalf("workerRuns, validatorRuns = %d, %d; want 3, 3", workerRuns, validatorRuns)
	}
}

func TestWorkflowEmitsStepFailedBeforeChildError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("worker failed")
	worker := mustNewTestAgent(t, "worker", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(yield func(*session.Event, error) bool) {
			yield(nil, wantErr)
		}
	})
	validator := mustNewTestAgent(t, "validator", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})

	workflow, err := New(NewOptions(worker, validator))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	events, runErr := collectTestRunUntilError(t, workflow, "Goal:\nship")
	if !errors.Is(runErr, wantErr) {
		t.Fatalf("runner error = %v, want %v", runErr, wantErr)
	}
	want := []string{
		"worker:step_started",
		"worker:step_failed",
	}
	if got := eventSequence(events); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}
	failed := metadataEvents(events, stepEventFailed)
	if len(failed) != 1 {
		t.Fatalf("failed event count = %d, want 1", len(failed))
	}
	if got := failed[0].CustomMetadata[metadataErrorKey]; got != wantErr.Error() {
		t.Fatalf("failed error metadata = %v, want %q", got, wantErr.Error())
	}
}

func TestWorkflowPassVerdictRequiresExactVisiblePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event func(string) *session.Event
	}{
		{
			name: "leading space",
			event: func(invocationID string) *session.Event {
				return textEvent(invocationID, " verdict: pass\nnot exact")
			},
		},
		{
			name: "thought only",
			event: func(invocationID string) *session.Event {
				ev := session.NewEvent(invocationID)
				ev.Content = &genai.Content{
					Role: genai.RoleModel,
					Parts: []*genai.Part{
						{Text: "verdict: pass\nhidden", Thought: true},
						{Text: "verdict: fail\nvisible"},
					},
				}
				return ev
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var validatorRuns int
			worker := mustNewTestAgent(t, "worker", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
				return func(yield func(*session.Event, error) bool) {
					yield(textEvent(ctx.InvocationID(), "worker result"), nil)
				}
			})
			validator := mustNewTestAgent(t, "validator", func(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
				return func(yield func(*session.Event, error) bool) {
					validatorRuns++
					yield(tc.event(ctx.InvocationID()), nil)
				}
			})

			workflow, err := New(NewOptions(worker, validator, WithMaxIterations(2)))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_ = runTestAgentOnce(t, workflow, "Goal:\nship")
			if validatorRuns != 2 {
				t.Fatalf("validatorRuns = %d, want 2", validatorRuns)
			}
		})
	}
}

func TestNewRejectsZeroMaxIterations(t *testing.T) {
	t.Parallel()

	worker := mustNewTestAgent(t, "worker", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})
	validator := mustNewTestAgent(t, "validator", func(agent.InvocationContext) iter.Seq2[*session.Event, error] {
		return func(func(*session.Event, error) bool) {}
	})
	workflow, err := New(NewOptions(worker, validator, WithMaxIterations(0)))
	if err == nil {
		t.Fatalf("New(WithMaxIterations(0)) error = nil, want validation error")
	}
	if workflow != nil {
		t.Fatalf("New(WithMaxIterations(0)) workflow = %v, want nil", workflow)
	}
}

func mustNewTestAgent(
	t *testing.T,
	name string,
	run func(agent.InvocationContext) iter.Seq2[*session.Event, error],
) agent.Agent {
	t.Helper()
	ag, err := agent.New(agent.Config{
		Name:        name,
		Description: name + " test agent",
		Run:         run,
	})
	if err != nil {
		t.Fatalf("agent.New() error = %v", err)
	}
	return ag
}

func runTestAgentOnce(t *testing.T, ag agent.Agent, prompt string) string {
	t.Helper()

	sessionService := session.InMemoryService()
	runner, err := adkrunner.New(adkrunner.Config{
		AppName:        "goalkeeper-test",
		Agent:          ag,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	created, err := sessionService.Create(context.Background(), &session.CreateRequest{
		AppName: "goalkeeper-test",
		UserID:  "goalkeeper-test-user",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	var last string
	events := runner.Run(
		context.Background(),
		"goalkeeper-test-user",
		created.Session.ID(),
		genai.NewContentFromText(prompt, genai.RoleUser),
		agent.RunConfig{},
	)
	for ev, runErr := range events {
		if runErr != nil {
			t.Fatalf("runner.Run() error = %v", runErr)
		}
		if ev == nil || ev.Content == nil {
			continue
		}
		if text := contentText(ev.Content); text != "" {
			last = text
		}
	}
	return last
}

func collectTestRun(t *testing.T, ag agent.Agent, prompt string) ([]*session.Event, session.Session) {
	t.Helper()
	events, storedSession, err := collectTestRunWithSession(t, ag, prompt)
	if err != nil {
		t.Fatalf("runner.Run() error = %v", err)
	}
	return events, storedSession
}

func collectTestRunUntilError(t *testing.T, ag agent.Agent, prompt string) ([]*session.Event, error) {
	t.Helper()
	events, _, err := collectTestRunWithSession(t, ag, prompt)
	return events, err
}

func collectTestRunWithSession(t *testing.T, ag agent.Agent, prompt string) ([]*session.Event, session.Session, error) {
	t.Helper()

	sessionService := session.InMemoryService()
	runner, err := adkrunner.New(adkrunner.Config{
		AppName:        "goalkeeper-test",
		Agent:          ag,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	created, err := sessionService.Create(context.Background(), &session.CreateRequest{
		AppName: "goalkeeper-test",
		UserID:  "goalkeeper-test-user",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	var events []*session.Event
	for ev, runErr := range runner.Run(
		context.Background(),
		"goalkeeper-test-user",
		created.Session.ID(),
		genai.NewContentFromText(prompt, genai.RoleUser),
		agent.RunConfig{},
	) {
		if runErr != nil {
			return events, created.Session, runErr
		}
		events = append(events, ev)
	}
	got, err := sessionService.Get(context.Background(), &session.GetRequest{
		AppName:   "goalkeeper-test",
		UserID:    "goalkeeper-test-user",
		SessionID: created.Session.ID(),
	})
	if err != nil {
		t.Fatalf("session.Get() error = %v", err)
	}
	return events, got.Session, nil
}

func textEvent(invocationID string, text string) *session.Event {
	ev := session.NewEvent(invocationID)
	ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	return ev
}

func contentText(content *genai.Content) string {
	if content == nil {
		return ""
	}
	var parts []string
	for _, part := range content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, strings.TrimSpace(part.Text))
		}
	}
	return strings.Join(parts, "\n\n")
}

func eventSequence(events []*session.Event) []string {
	var seq []string
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if kind, ok := ev.CustomMetadata[metadataEventKey].(string); ok {
			step, _ := ev.CustomMetadata[metadataStepKey].(string)
			seq = append(seq, step+":"+kind)
			continue
		}
		if text := contentText(ev.Content); text != "" {
			seq = append(seq, ev.Author+":"+text)
		}
	}
	return seq
}

func metadataEvents(events []*session.Event, kind string) []*session.Event {
	var matches []*session.Event
	for _, ev := range events {
		if ev != nil && ev.CustomMetadata[metadataEventKey] == kind {
			matches = append(matches, ev)
		}
	}
	return matches
}

func countStoredStepEvents(storedSession session.Session) int {
	count := 0
	events := storedSession.Events()
	for i := 0; i < events.Len(); i++ {
		ev := events.At(i)
		if ev != nil && ev.CustomMetadata[metadataEventKey] != nil {
			count++
		}
	}
	return count
}
