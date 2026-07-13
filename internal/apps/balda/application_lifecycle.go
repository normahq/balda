package balda

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/normahq/balda/internal/apps/balda/actors"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	natsbus "github.com/normahq/balda/internal/apps/balda/eventbus/nats"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/handlers"
	"github.com/normahq/balda/internal/apps/balda/internalmcp"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/scheduledjobs"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/shutdown"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/runtime"
	"go.uber.org/fx"
)

type lifecycleStage struct {
	name  string
	start func(context.Context) error
	stop  func(context.Context) error
}

type applicationLifecycle struct {
	logger zerolog.Logger
	stages []lifecycleStage

	mu      sync.Mutex
	started int
}

func newApplicationLifecycle(logger zerolog.Logger, stages []lifecycleStage) *applicationLifecycle {
	return &applicationLifecycle{
		logger: logger.With().Str("component", "balda.lifecycle").Logger(),
		stages: append([]lifecycleStage(nil), stages...),
	}
}

func (l *applicationLifecycle) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started != 0 {
		return nil
	}

	for i, stage := range l.stages {
		if stage.start != nil {
			l.logger.Debug().Str("stage", stage.name).Msg("starting application lifecycle stage")
			if err := stage.start(ctx); err != nil {
				rollbackErr := l.stopStarted(ctx, i)
				return errors.Join(fmt.Errorf("start %s: %w", stage.name, err), rollbackErr)
			}
		}
		l.started = i + 1
	}
	return nil
}

func (l *applicationLifecycle) Stop(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopStarted(ctx, l.started)
}

func (l *applicationLifecycle) stopStarted(ctx context.Context, count int) error {
	var errs []error
	for i := count - 1; i >= 0; i-- {
		stage := l.stages[i]
		if stage.stop == nil {
			continue
		}
		l.logger.Debug().Str("stage", stage.name).Msg("stopping application lifecycle stage")
		if err := stage.stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", stage.name, err))
		}
	}
	l.started = 0
	return errors.Join(errs...)
}

type telegramLifecycle struct {
	enabled bool
	bot     *runtime.Bot
	logger  zerolog.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

func (t *telegramLifecycle) Start(context.Context) error {
	if !t.enabled {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	t.done = make(chan struct{})
	go func() {
		defer close(t.done)
		if err := t.bot.Run(runCtx); err != nil {
			if shutdown.IsExpected(err) {
				t.logger.Debug().Err(err).Msg("telegram runtime stopped during shutdown")
				return
			}
			t.logger.Error().Err(err).Msg("telegram runtime stopped")
		}
	}()
	return nil
}

func (t *telegramLifecycle) Stop(ctx context.Context) error {
	if t.cancel == nil {
		return nil
	}
	t.cancel()
	select {
	case <-t.done:
		t.cancel = nil
		t.done = nil
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type applicationLifecycleParams struct {
	fx.In

	LC              fx.Lifecycle
	Logger          zerolog.Logger
	MCP             *internalmcp.InternalMCPManager
	Runtime         *baldaagent.RuntimeManager
	Sessions        *session.Manager
	Bus             *natsbus.Bus
	Projector       *baldajobs.EventProjector
	OutboxPublisher *baldajobs.OutboxPublisher
	ActorHost       *baldaexecution.ActorHost
	TurnDispatcher  *actors.TurnDispatcher
	BaldaHandler    *handlers.BaldaHandler
	Scheduler       *scheduledjobs.ScheduledJobScheduler
	InboundWebhook  *handlers.InboundWebhookReceiver
	Zulip           *handlers.ZulipBaldaHandler
	Slack           *handlers.SlackHandler
	TelegramBot     *runtime.Bot
	TelegramEnabled bool `name:"balda_telegram_enabled"`
}

func registerApplicationLifecycle(p applicationLifecycleParams) {
	telegram := &telegramLifecycle{
		enabled: p.TelegramEnabled,
		bot:     p.TelegramBot,
		logger:  p.Logger.With().Str("component", "balda.telegram_runtime").Logger(),
	}
	coordinator := newApplicationLifecycle(p.Logger, []lifecycleStage{
		{name: "bundled MCP", start: p.MCP.EnsureStarted, stop: p.MCP.Stop},
		{name: "provider runtime", start: p.Runtime.EnsureRuntime, stop: p.Runtime.Stop},
		{name: "session manager", start: p.Sessions.Start, stop: p.Sessions.Stop},
		{name: "turn dispatcher", stop: p.TurnDispatcher.Shutdown},
		{name: "durable transport", start: p.Bus.Start, stop: p.Bus.Drain},
		{name: "job event projector", start: p.Projector.Start, stop: p.Projector.Stop},
		{name: "job event outbox", start: p.OutboxPublisher.Start, stop: p.OutboxPublisher.Stop},
		{name: "actor host", start: p.ActorHost.Start, stop: p.ActorHost.Stop},
		{name: "telegram bootstrap", start: p.BaldaHandler.Start},
		{name: "scheduled jobs", start: p.Scheduler.Start, stop: p.Scheduler.Stop},
		{name: "inbound webhooks", start: p.InboundWebhook.Start, stop: p.InboundWebhook.Stop},
		{name: "zulip ingress", start: p.Zulip.Start, stop: p.Zulip.Stop},
		{name: "slack ingress", start: p.Slack.Start, stop: p.Slack.Stop},
		{name: "telegram ingress", start: telegram.Start, stop: telegram.Stop},
	})
	p.LC.Append(fx.Hook{OnStart: coordinator.Start, OnStop: coordinator.Stop})
}
