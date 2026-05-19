package handlers

import (
	"context"
	"iter"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/goalkeeper"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/rs/zerolog"
	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestGoalRunnerRunGoalLoopUsesCopiedWorkflowRuntime(t *testing.T) {
	var order []string
	var prompts []string

	goalRuntime, agentSessionID := newGoalRunnerWorkflowRuntime(t, 3,
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				order = append(order, "worker")
				prompts = append(prompts, goalRunnerContentText(ctx.UserContent()))
				if err := ctx.Session().State().Set("worker_result", "created artifact"); err != nil {
					yield(nil, err)
					return
				}
				yield(goalRunnerTextEvent(ctx.InvocationID(), "worker result"), nil)
			}
		},
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				order = append(order, "validator")
				value, err := ctx.Session().State().Get("worker_result")
				if err != nil {
					yield(nil, err)
					return
				}
				yield(goalRunnerTextEvent(ctx.InvocationID(), "verdict: pass\nworker_result="+value.(string)), nil)
			}
		},
	)

	locator := baldatelegram.NewLocator(-1002667079342, 8939)
	ts := newSchedulerTopicSession(t, locator, "tg-101", agentSessionID, nil)
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	setUnexportedField(t, ts, "branchName", "norma/balda/"+locator.SessionID)

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	runner := &GoalRunner{
		runtimeManager: &fakeGoalkeeperRuntimeBuilder{runtime: goalRuntime},
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		logger:        zerolog.Nop(),
		maxIterations: 3,
	}

	runner.runGoalLoop(context.Background(), locator, ts, "deploy release")

	if got := strings.Join(order, ","); got != "worker,validator" {
		t.Fatalf("workflow order = %s, want worker,validator", got)
	}
	if got := strings.Join(prompts, "\n"); got != "Goal:\ndeploy release" {
		t.Fatalf("worker prompt = %q, want copied Goalkeeper prompt", got)
	}
	if strings.Contains(strings.Join(prompts, "\n"), "You are Goalkeeper running an iterative goal loop") {
		t.Fatalf("worker prompt used removed single-root loop prompt: %q", prompts)
	}

	got := goalRunnerSentText(tgClient)
	if strings.Contains(got, "Goal run failed") {
		t.Fatalf("goal runner sent failure message:\n%s", got)
	}
	if !strings.Contains(got, "Goal iteration 1/3: worker started.") {
		t.Fatalf("goal runner messages = %q, want worker started progress", got)
	}
	if !strings.Contains(got, "Goal iteration 1/3: worker finished.\nworker result") {
		t.Fatalf("goal runner messages = %q, want worker finished result", got)
	}
	if !strings.Contains(got, "Goal iteration 1/3: validator started.") {
		t.Fatalf("goal runner messages = %q, want validator started progress", got)
	}
	if !strings.Contains(got, "Goal iteration 1/3: validator finished (pass).\nverdict: pass\nworker_result=created artifact") {
		t.Fatalf("goal runner messages = %q, want passing validator result", got)
	}
	if !strings.Contains(got, "Goal run completed.") {
		t.Fatalf("goal runner messages = %q, want completion message", got)
	}
}

func TestGoalRunnerRunGoalLoopRetriesFailVerdictUntilMax(t *testing.T) {
	var workerRuns int
	var validatorRuns int

	goalRuntime, agentSessionID := newGoalRunnerWorkflowRuntime(t, 2,
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				workerRuns++
				yield(goalRunnerTextEvent(ctx.InvocationID(), "worker result"), nil)
			}
		},
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				validatorRuns++
				yield(goalRunnerTextEvent(ctx.InvocationID(), "verdict: fail\nnot done"), nil)
			}
		},
	)

	locator := baldatelegram.NewLocator(-1002667079342, 8940)
	ts := newSchedulerTopicSession(t, locator, "tg-101", agentSessionID, nil)
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	runner := &GoalRunner{
		runtimeManager: &fakeGoalkeeperRuntimeBuilder{runtime: goalRuntime},
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		logger:        zerolog.Nop(),
		maxIterations: 2,
	}

	runner.runGoalLoop(context.Background(), locator, ts, "finish docs")

	if workerRuns != 2 || validatorRuns != 2 {
		t.Fatalf("workerRuns, validatorRuns = %d, %d; want 2, 2", workerRuns, validatorRuns)
	}
	got := goalRunnerSentText(tgClient)
	if !strings.Contains(got, "Goal iteration 2/2: worker started.") {
		t.Fatalf("goal runner messages = %q, want second iteration worker started progress", got)
	}
	if !strings.Contains(got, "Goal iteration 2/2: validator finished (fail).\nverdict: fail\nnot done") {
		t.Fatalf("goal runner messages = %q, want second failed validator result", got)
	}
	if !strings.Contains(got, "Goal run reached max iterations without passing validation.") {
		t.Fatalf("goal runner messages = %q, want max-iteration failure", got)
	}
}

type fakeGoalkeeperRuntimeBuilder struct {
	runtime *baldaagent.GoalkeeperRuntime
	calls   []baldaagent.GoalkeeperRuntimeConfig
	err     error
}

func (f *fakeGoalkeeperRuntimeBuilder) BuildGoalkeeperRuntime(
	_ context.Context,
	cfg baldaagent.GoalkeeperRuntimeConfig,
) (*baldaagent.GoalkeeperRuntime, error) {
	f.calls = append(f.calls, cfg)
	if f.err != nil {
		return nil, f.err
	}
	return f.runtime, nil
}

func newGoalRunnerWorkflowRuntime(
	t *testing.T,
	maxIterations uint,
	workerRun func(adkagent.InvocationContext) iter.Seq2[*adksession.Event, error],
	validatorRun func(adkagent.InvocationContext) iter.Seq2[*adksession.Event, error],
) (*baldaagent.GoalkeeperRuntime, string) {
	t.Helper()

	worker := goalRunnerTestAgent(t, "GoalkeeperWorker", workerRun)
	validator := goalRunnerTestAgent(t, "GoalkeeperValidator", validatorRun)
	workflow, err := goalkeeper.New(goalkeeper.NewOptions(worker, validator, goalkeeper.WithMaxIterations(maxIterations)))
	if err != nil {
		t.Fatalf("goalkeeper.New() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	adkRunner, err := adkrunner.New(adkrunner.Config{
		AppName:        "goal-runner-test",
		Agent:          workflow,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	sess, err := sessionService.Create(context.Background(), &adksession.CreateRequest{
		AppName:   "goal-runner-test",
		UserID:    "tg-101",
		SessionID: "goal-runner-session",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	return &baldaagent.GoalkeeperRuntime{
		Agent:  workflow,
		Runner: adkRunner,
	}, sess.Session.ID()
}

func goalRunnerTestAgent(
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

func goalRunnerTextEvent(invocationID string, text string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	return ev
}

func goalRunnerContentText(content *genai.Content) string {
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

func goalRunnerSentText(tgClient *fakeTelegramClient) string {
	parts := make([]string, 0, len(tgClient.messages))
	for _, msg := range tgClient.messages {
		parts = append(parts, msg.Text)
	}
	return strings.Join(parts, "\n")
}

func TestGoalRunnerRunGoalLoopUsesAgentFormattingMode(t *testing.T) {
	goalRuntime, agentSessionID := newGoalRunnerWorkflowRuntime(t, 1,
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				yield(goalRunnerTextEvent(ctx.InvocationID(), "**worker** done"), nil)
			}
		},
		func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				yield(goalRunnerTextEvent(ctx.InvocationID(), "verdict: pass\n`ok`"), nil)
			}
		},
	)

	locator := baldatelegram.NewLocator(-1002667079342, 9940)
	ts := newSchedulerTopicSession(t, locator, "tg-101", agentSessionID, nil)
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	setUnexportedField(t, ts, "branchName", "norma/balda/"+locator.SessionID)

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("markdownv2")
	runner := &GoalRunner{
		runtimeManager: &fakeGoalkeeperRuntimeBuilder{runtime: goalRuntime},
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		logger:        zerolog.Nop(),
		maxIterations: 1,
	}

	runner.runGoalLoop(context.Background(), locator, ts, "deploy release")

	if len(tgClient.messages) == 0 {
		t.Fatal("sent messages = 0, want at least one goal update")
	}
	for i, sent := range tgClient.messages {
		if sent.ParseMode == nil || *sent.ParseMode != testParseModeMarkdown {
			t.Fatalf("messages[%d].parse_mode = %v, want MarkdownV2", i, sent.ParseMode)
		}
	}
}
