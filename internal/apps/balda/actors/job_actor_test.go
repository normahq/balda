package actors

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskActorDispatchesWebhookSessionTurn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bus, dispatcher, tasks := newTaskActorDispatchServices(t, ctx)
	exec := &jobActorExecutor{tasks: tasks, dispatcher: dispatcher}
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	env, taskID, err := WebhookJobEnvelope(SessionTurnPayload{
		Text:    "handle webhook",
		Locator: locator,
		UserID:  "101",
		Source:  sessionTurnSourceWebhook,
	}, "github", "delivery-1")
	if err != nil {
		t.Fatalf("WebhookJobEnvelope() error = %v", err)
	}
	if !strings.HasPrefix(taskID, "webhook-github-") {
		t.Fatalf("task id = %q, want webhook-github-*", taskID)
	}
	if err := exec.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	published := lastPublishedCommandTo(t, bus, baldaexecution.ActorTypeSession, locator.SessionID)
	if baldaexecution.EnvelopeJobID(published) != taskID {
		t.Fatalf("published task id = %q, want %q", baldaexecution.EnvelopeJobID(published), taskID)
	}
	if got, want := published.Namespace, baldaexecution.NamespaceWebhookInbound; got != want {
		t.Fatalf("published namespace = %q, want %q", got, want)
	}
	if got, want := published.Kind, baldaexecution.KindWebhookEvent; got != want {
		t.Fatalf("published kind = %q, want %q", got, want)
	}
	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", taskID)
	}
	if task.Status != baldastate.JobStatusRunning {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusRunning)
	}
}

func TestTaskActorRejectsWebhookSessionTurnWithoutEnvelopeJobID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, dispatcher, tasks := newTaskActorDispatchServices(t, ctx)
	exec := &jobActorExecutor{tasks: tasks, dispatcher: dispatcher}
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	env, _, err := WebhookJobEnvelope(SessionTurnPayload{
		Text:    "handle webhook",
		Locator: locator,
		UserID:  "101",
		Source:  sessionTurnSourceWebhook,
	}, "github", "delivery-1")
	if err != nil {
		t.Fatalf("WebhookJobEnvelope() error = %v", err)
	}
	env.Meta = nil

	err = exec.Handle(ctx, env)
	if err == nil {
		t.Fatal("Handle() error = nil, want policy error")
	}
	if got, want := actorlayer.ClassifyError(err), actorlayer.ErrorKindPolicy; got != want {
		t.Fatalf("Handle() error kind = %q, want %q (err=%v)", got, want, err)
	}
}

func TestTaskActorRejectsNonWebhookSessionTurnTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, dispatcher, tasks := newTaskActorDispatchServices(t, ctx)
	exec := &jobActorExecutor{tasks: tasks, dispatcher: dispatcher}
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	data, err := json.Marshal(jobEnvelopePayload{
		Kind: jobPayloadKindWebhookSessionTurn,
		SessionTurn: &SessionTurnPayload{
			Text:    "repeat the review",
			Locator: locator,
			UserID:  "101",
			Source:  sessionTurnSourceTelegram,
		},
	})
	if err != nil {
		t.Fatalf("Marshal(jobEnvelopePayload) error = %v", err)
	}

	err = exec.Handle(ctx, actorlayer.Envelope{
		ID:          "task-1",
		Namespace:   baldaexecution.NamespaceHumanInbound,
		Kind:        baldaexecution.KindMessage,
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "turn-1"},
		SessionID:   locator.SessionID,
		PayloadJSON: string(data),
	})
	if err == nil {
		t.Fatal("Handle() error = nil, want policy error")
	}
	if got, want := actorlayer.ClassifyError(err), actorlayer.ErrorKindPolicy; got != want {
		t.Fatalf("Handle() error kind = %q, want %q (err=%v)", got, want, err)
	}
}

func TestScheduledJobEnvelopeDispatchesSessionTurn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bus, dispatcher, tasks := newTaskActorDispatchServices(t, ctx)
	exec := &jobActorExecutor{tasks: tasks, dispatcher: dispatcher}
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	env, err := ScheduledJobEnvelope("daily", "summarize", locator, nil, "101", 0, "tick-1")
	if err != nil {
		t.Fatalf("ScheduledJobEnvelope() error = %v", err)
	}
	if err := exec.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	published := lastPublishedCommandTo(t, bus, baldaexecution.ActorTypeSession, locator.SessionID)
	if published.Namespace != baldaexecution.NamespaceScheduleInbound {
		t.Fatalf("published namespace = %q, want %q", published.Namespace, baldaexecution.NamespaceScheduleInbound)
	}
}

func lastPublishedCommandTo(t *testing.T, bus *recordingHandlerCommandBus, target string, key string) actorlayer.Envelope {
	t.Helper()
	for i := len(bus.commands) - 1; i >= 0; i-- {
		env := bus.commands[i]
		if env.To.Target == target && env.To.Key == key {
			return env
		}
	}
	data, _ := json.MarshalIndent(bus.commands, "", "  ")
	t.Fatalf("no published command to %s:%s; published=%s", target, key, string(data))
	return actorlayer.Envelope{}
}

func newTaskActorRuntimeServices(t *testing.T, ctx context.Context) (baldastate.Provider, *recordingHandlerCommandBus, actortransport.Dispatcher, *baldajobs.JobService, any) {
	t.Helper()
	provider, err := baldastate.NewSQLiteProvider(ctx, ":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Close()
	})
	bus := &recordingHandlerCommandBus{}
	var dispatcher actortransport.Dispatcher
	var tasks *baldajobs.JobService
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			zerolog.Nop(),
			baldaexecution.Config{},
		),
		fx.Provide(func() actortransport.Dispatcher { return bus }),
		fx.Provide(func() actortransport.EventPublisher { return bus }),
		fx.Provide(baldajobs.NewJobService),
		fx.Populate(&dispatcher, &tasks),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, bus, dispatcher, tasks, nil
}

func newTaskActorDispatchServices(t *testing.T, ctx context.Context) (*recordingHandlerCommandBus, actortransport.Dispatcher, *baldajobs.JobService) {
	t.Helper()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	return bus, dispatcher, tasks
}
