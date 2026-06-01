package agent

import (
	"context"
	"iter"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/goalkeeperworkflow"
	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func visibleContentText(content *genai.Content) string {
	if content == nil {
		return ""
	}
	var parts []string
	for _, part := range content.Parts {
		if part == nil || part.Thought {
			continue
		}
		text := strings.TrimSpace(part.Text)
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func TestGoalChildBuildRequest_SetsOutputKeyAndInstructions(t *testing.T) {
	t.Parallel()

	builder := &Builder{workingDir: "/repo"}
	cfg := goalChildAgentConfig{
		ProviderID:        "provider",
		Name:              goalWorkerName,
		Description:       "Goal worker agent",
		SessionID:         "tg-1-2",
		SessionBranch:     "norma/balda/tg-1-2",
		WorkspaceDir:      "/tmp/workspace",
		RepoBranchAtStart: "main",
		RoleInstruction:   "worker role instruction",
		OutputKey:         "  goal_worker_output  ",
		MCPServerIDs:      []string{"balda"},
	}

	req := builder.goalChildBuildRequest(cfg)
	if req.OutputKey != goalWorkerOutputStateKey {
		t.Fatalf("req.OutputKey = %q, want %q", req.OutputKey, goalWorkerOutputStateKey)
	}
	if !strings.Contains(req.Instruction, "worker role instruction") {
		t.Fatalf("req.Instruction = %q, want role instruction", req.Instruction)
	}
	if !strings.Contains(req.Instruction, "Workspace settings:") {
		t.Fatalf("req.Instruction = %q, want Balda base instruction", req.Instruction)
	}
}

func TestGoalValidatorInstruction_DoesNotUseWorkerOutputPlaceholder(t *testing.T) {
	t.Parallel()

	got := goalValidatorInstruction()
	if strings.Contains(got, "{goal_worker_output?}") {
		t.Fatalf("goalValidatorInstruction() = %q, should not include worker output placeholder", got)
	}
	if !strings.Contains(got, "shared runtime session context") {
		t.Fatalf("goalValidatorInstruction() = %q, want shared session validation guidance", got)
	}
}

func TestGoalValidatorWrapperUsesLatestWorkerOutputEachInvocation(t *testing.T) {
	t.Parallel()

	var workerRuns int
	worker := mustNewGoalTestAgent(t, "worker", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			workerRuns++
			workerOutput := "first output"
			if workerRuns == 2 {
				workerOutput = "second output"
			}
			if err := ctx.Session().State().Set(goalWorkerOutputStateKey, workerOutput); err != nil {
				yield(nil, err)
				return
			}
			yield(goalTestTextEvent(ctx.InvocationID(), workerOutput), nil)
		}
	})
	var validatorRuns int
	inner := mustNewGoalTestAgent(t, "validator", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			validatorRuns++
			result := "verdict: fail\n" + visibleContentText(ctx.UserContent())
			if validatorRuns == 2 {
				result = "verdict: pass\n" + visibleContentText(ctx.UserContent())
			}
			yield(goalTestTextEvent(ctx.InvocationID(), result), nil)
		}
	})
	wrapped, err := wrapGoalValidatorWithWorkerOutput(inner, goalWorkerOutputStateKey)
	if err != nil {
		t.Fatalf("wrapGoalValidatorWithWorkerOutput() error = %v", err)
	}
	workflow, err := goalkeeperworkflow.New(worker, wrapped, 2)
	if err != nil {
		t.Fatalf("goalkeeperworkflow.New() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	r, err := adkrunner.New(adkrunner.Config{
		AppName:        "goal-wrapper-test",
		Agent:          workflow,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	created, err := sessionService.Create(context.Background(), &adksession.CreateRequest{
		AppName: "goal-wrapper-test",
		UserID:  "tg-101",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}
	got := runGoalAgentOnce(t, r, "tg-101", created.Session.ID(), "Goal:\ntest")
	if !strings.Contains(got, "Worker result:\nsecond output") {
		t.Fatalf("final validator text = %q, want latest worker output", got)
	}
	if strings.Contains(got, "Worker result:\nfirst output") {
		t.Fatalf("final validator text = %q, contains earlier worker output", got)
	}
	if workerRuns != 2 || validatorRuns != 2 {
		t.Fatalf("workerRuns, validatorRuns = %d, %d; want 2, 2", workerRuns, validatorRuns)
	}
}

func TestBuildGoalWorkflow_UsesGoalKeeperRootName(t *testing.T) {
	t.Parallel()

	worker := mustNewGoalTestAgent(t, "worker", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			yield(goalTestTextEvent(ctx.InvocationID(), "worker"), nil)
		}
	})
	validator := mustNewGoalTestAgent(t, "validator", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			yield(goalTestTextEvent(ctx.InvocationID(), "verdict: pass\nok"), nil)
		}
	})

	workflow, err := goalkeeperworkflow.New(worker, validator, 1)
	if err != nil {
		t.Fatalf("goalkeeperworkflow.New() error = %v", err)
	}
	if got := workflow.Name(); got != goalkeeperworkflow.RootAgentName {
		t.Fatalf("workflow.Name() = %q, want %q", got, goalkeeperworkflow.RootAgentName)
	}
}

func mustNewGoalTestAgent(
	t *testing.T,
	name string,
	run func(adkagent.InvocationContext) iter.Seq2[*adksession.Event, error],
) adkagent.Agent {
	t.Helper()

	ag, err := adkagent.New(adkagent.Config{
		Name:        name,
		Description: name + " test agent",
		Run:         run,
	})
	if err != nil {
		t.Fatalf("agent.New() error = %v", err)
	}
	return ag
}

func runGoalAgentOnce(
	t *testing.T,
	r *adkrunner.Runner,
	userID string,
	sessionID string,
	prompt string,
) string {
	t.Helper()

	var out string
	for ev, err := range r.Run(
		context.Background(),
		userID,
		sessionID,
		genai.NewContentFromText(prompt, genai.RoleUser),
		adkagent.RunConfig{},
	) {
		if err != nil {
			t.Fatalf("runner.Run() error = %v", err)
		}
		text := visibleContentText(ev.Content)
		if text != "" {
			out = text
		}
	}
	return out
}

func goalTestTextEvent(invocationID string, text string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	return ev
}

func TestGoalValidatorWrapperIncludesMissingWorkerResultMarker(t *testing.T) {
	t.Parallel()

	inner := mustNewGoalTestAgent(t, "validator", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			yield(goalTestTextEvent(ctx.InvocationID(), visibleContentText(ctx.UserContent())), nil)
		}
	})
	wrapped, err := wrapGoalValidatorWithWorkerOutput(inner, goalWorkerOutputStateKey)
	if err != nil {
		t.Fatalf("wrapGoalValidatorWithWorkerOutput() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	r, err := adkrunner.New(adkrunner.Config{
		AppName:        "goal-wrapper-missing-output-test",
		Agent:          wrapped,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	created, err := sessionService.Create(context.Background(), &adksession.CreateRequest{
		AppName: "goal-wrapper-missing-output-test",
		UserID:  "tg-101",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	got := runGoalAgentOnce(t, r, "tg-101", created.Session.ID(), "Goal:\ntest")
	if !strings.Contains(got, "Worker result:\n(none)") {
		t.Fatalf("validator wrapper output = %q, want explicit missing worker result marker", got)
	}
}
