package goalkeeperworkflow

import (
	"context"
	"iter"
	"testing"

	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestWorkflowEmitsGoalKeeperAuthoredEvents(t *testing.T) {
	t.Parallel()

	worker := mustNewTestAgent(t, "GoalWorker", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			yield(textEvent(ctx.InvocationID(), "worker result"), nil)
		}
	})
	validator := mustNewTestAgent(t, "GoalValidator", func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
		return func(yield func(*adksession.Event, error) bool) {
			yield(textEvent(ctx.InvocationID(), "verdict: pass\nok"), nil)
		}
	})

	workflow, err := New(worker, validator, 1)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	r, err := adkrunner.New(adkrunner.Config{
		AppName:        "goalkeeperworkflow-test",
		Agent:          workflow,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}
	created, err := sessionService.Create(context.Background(), &adksession.CreateRequest{
		AppName: "goalkeeperworkflow-test",
		UserID:  "tg-101",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	for ev, err := range r.Run(
		context.Background(),
		"tg-101",
		created.Session.ID(),
		genai.NewContentFromText("Goal:\nship", genai.RoleUser),
		adkagent.RunConfig{},
	) {
		if err != nil {
			t.Fatalf("runner.Run() error = %v", err)
		}
		if ev == nil {
			continue
		}
		if ev.Author != RootAgentName {
			t.Fatalf("event author = %q, want %q", ev.Author, RootAgentName)
		}
	}

	stored, err := sessionService.Get(context.Background(), &adksession.GetRequest{
		AppName:   "goalkeeperworkflow-test",
		UserID:    "tg-101",
		SessionID: created.Session.ID(),
	})
	if err != nil {
		t.Fatalf("session.Get() error = %v", err)
	}
	for i := range stored.Session.Events().Len() {
		ev := stored.Session.Events().At(i)
		if ev == nil || ev.Author == "user" {
			continue
		}
		if ev.Author != RootAgentName {
			t.Fatalf("stored event[%d] author = %q, want %q", i, ev.Author, RootAgentName)
		}
	}
}

func mustNewTestAgent(
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

func textEvent(invocationID string, text string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	return ev
}
