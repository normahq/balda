package swarm

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const (
	defaultRecoveryInterval = 2 * time.Second
	retryBaseDelay          = time.Second
	retryMaxDelay           = time.Minute
)

type Actor interface {
	Address() string
	Handle(ctx context.Context, env Envelope) error
}

type ActorRegistry interface {
	Register(actor Actor) error
	Resolve(address string) (Actor, bool)
}

type Registry struct {
	actors map[string]Actor
}

func NewRegistry() *Registry {
	return &Registry{actors: make(map[string]Actor)}
}

func (r *Registry) Register(actor Actor) error {
	if actor == nil {
		return nil
	}
	address := strings.ToLower(strings.TrimSpace(actor.Address()))
	if address == "" {
		return fmt.Errorf("actor address is required")
	}
	r.actors[address] = actor
	return nil
}

func (r *Registry) Resolve(address string) (Actor, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(address))
	if trimmed == "" {
		return nil, false
	}
	actor, ok := r.actors[trimmed]
	if ok {
		return actor, true
	}
	idx := strings.Index(trimmed, ":")
	if idx <= 0 {
		return nil, false
	}
	actor, ok = r.actors[trimmed[:idx]+":*"]
	return actor, ok
}

type Runtime struct {
	mailboxes *MailboxService
	tasks     *TaskService
	registry  ActorRegistry
	scheduler *KeyedActorScheduler
	logger    zerolog.Logger
	workerID  string

	mu       sync.Mutex
	draining map[string]struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type runtimeParams struct {
	fx.In

	LC        fx.Lifecycle
	Bus       EventBus
	Mailboxes *MailboxService
	Tasks     *TaskService
	Logger    zerolog.Logger
	Actors    []Actor `group:"balda_swarm_actors"`
}

func NewRuntime(params runtimeParams) (*Runtime, error) {
	if params.Bus == nil {
		return nil, fmt.Errorf("swarm wake bus is required")
	}
	if params.Mailboxes == nil {
		return nil, fmt.Errorf("swarm mailbox service is required")
	}
	registry := NewRegistry()
	for _, actor := range params.Actors {
		if err := registry.Register(actor); err != nil {
			return nil, err
		}
	}
	r := &Runtime{
		mailboxes: params.Mailboxes,
		tasks:     params.Tasks,
		registry:  registry,
		scheduler: NewKeyedActorScheduler(),
		logger:    params.Logger.With().Str("component", "balda.swarm.runtime").Logger(),
		workerID:  "balda-single-worker",
		draining:  make(map[string]struct{}),
	}
	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return r.Start(ctx, params.Bus) },
		OnStop:  func(ctx context.Context) error { return r.Stop(ctx) },
	})
	return r, nil
}

func (r *Runtime) Start(ctx context.Context, bus EventBus) error {
	if r.cancel != nil {
		return nil
	}
	if !r.mailboxes.Enabled() {
		r.logger.Info().Msg("swarm mailbox runtime disabled")
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if _, err := r.mailboxes.Recover(ctx, time.Now().UTC()); err != nil {
		cancel()
		return err
	}
	if _, err := bus.Subscribe(runCtx, SubjectWakeupMailbox, func(ctx context.Context, _ string, env Envelope) error {
		mailboxID, err := env.To.MailboxID()
		if err != nil {
			return err
		}
		r.wake(ctx, mailboxID)
		return nil
	}); err != nil {
		cancel()
		return err
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.scanLoop(runCtx)
	}()
	return r.wakeReady(ctx)
}

func (r *Runtime) Stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) wake(ctx context.Context, mailboxID string) {
	trimmed := strings.TrimSpace(mailboxID)
	if trimmed == "" {
		return
	}
	r.mu.Lock()
	if _, exists := r.draining[trimmed]; exists {
		r.mu.Unlock()
		return
	}
	r.draining[trimmed] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() {
			r.mu.Lock()
			delete(r.draining, trimmed)
			r.mu.Unlock()
		}()
		r.runMailbox(ctx, trimmed)
	}()
}

func (r *Runtime) runMailbox(ctx context.Context, mailbox string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch, err := r.mailboxes.Claim(ctx, mailbox, r.workerID, DefaultClaimLimit, DefaultLeaseDuration)
		if err != nil {
			r.logger.Warn().Err(err).Str("mailbox", mailbox).Msg("failed to claim swarm messages")
			return
		}
		if len(batch) == 0 {
			return
		}
		for _, env := range batch {
			if err := r.handleEnvelope(ctx, mailbox, env); err != nil {
				r.logger.Warn().Err(err).Str("mailbox", mailbox).Str("message_id", env.ID).Msg("failed to settle swarm message")
			}
		}
	}
}

func (r *Runtime) handleEnvelope(ctx context.Context, mailbox string, env Envelope) error {
	if r.scheduler != nil {
		return r.scheduler.Dispatch(ctx, env, func(ctx context.Context, env Envelope) error {
			return r.handleEnvelopeDirect(ctx, mailbox, env)
		})
	}
	return r.handleEnvelopeDirect(ctx, mailbox, env)
}

func (r *Runtime) handleEnvelopeDirect(ctx context.Context, mailbox string, env Envelope) error {
	to, err := env.To.String()
	if err != nil {
		return r.mailboxes.DeadLetter(context.Background(), mailbox, env.ID, err.Error())
	}
	actor, ok := r.registry.Resolve(to)
	if !ok {
		r.deadletterTask(ctx, env, "actor not found: "+to)
		return r.mailboxes.DeadLetter(context.Background(), mailbox, env.ID, "actor not found: "+to)
	}
	if err := actor.Handle(ctx, env); err != nil {
		return r.settleError(context.Background(), mailbox, env, err)
	}
	return r.mailboxes.Ack(context.Background(), mailbox, env.ID)
}

func (r *Runtime) settleError(ctx context.Context, mailbox string, env Envelope, err error) error {
	switch classifyError(err) {
	case ErrorKindDuplicate:
		return r.mailboxes.Ack(ctx, mailbox, env.ID)
	case ErrorKindAuth, ErrorKindPolicy, ErrorKindPermanent:
		r.deadletterTask(ctx, env, err.Error())
		return r.mailboxes.DeadLetter(ctx, mailbox, env.ID, err.Error())
	default:
		if env.MaxAttempts > 0 && env.Attempt >= env.MaxAttempts {
			r.deadletterTask(ctx, env, err.Error())
			return r.mailboxes.DeadLetter(ctx, mailbox, env.ID, err.Error())
		}
		return r.mailboxes.Retry(ctx, mailbox, env.ID, nextRetryAt(env.Attempt), err.Error())
	}
}

func (r *Runtime) deadletterTask(ctx context.Context, env Envelope, reason string) {
	if r == nil || r.tasks == nil {
		return
	}
	taskID := strings.TrimSpace(env.TaskID)
	if taskID == "" {
		return
	}
	if err := r.tasks.DeadLetter(ctx, taskID, "swarm.runtime", env.ID, reason); err != nil {
		r.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to mark swarm task deadlettered")
	}
}

func (r *Runtime) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.mailboxes.Recover(ctx, time.Now().UTC()); err != nil {
				r.logger.Warn().Err(err).Msg("failed to recover swarm mailboxes")
			}
			if err := r.wakeReady(ctx); err != nil {
				r.logger.Warn().Err(err).Msg("failed to wake ready swarm mailboxes")
			}
		}
	}
}

func (r *Runtime) wakeReady(ctx context.Context) error {
	mailboxes, err := r.mailboxes.ListReadyMailboxes(ctx, 100)
	if err != nil {
		return err
	}
	for _, mailboxID := range mailboxes {
		r.wake(ctx, mailboxID)
	}
	return nil
}

func nextRetryAt(attempt int) time.Time {
	if attempt < 0 {
		attempt = 0
	}
	delay := retryBaseDelay
	for range attempt {
		delay *= 2
		if delay >= retryMaxDelay {
			delay = retryMaxDelay
			break
		}
	}
	jitterCap := max(delay/4, time.Millisecond)
	jitter := time.Duration(rand.Int64N(int64(jitterCap)))
	return time.Now().UTC().Add(delay + jitter)
}
