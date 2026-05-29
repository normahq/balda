package swarm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// ErrCommandQueueFull means the durable command stream rejected new work due to pressure.
var ErrCommandQueueFull = errors.New("command queue is full")

// ErrDLQEntryNotFound means a DLQ sequence does not exist in the stream.
var ErrDLQEntryNotFound = errors.New("dlq entry not found")

const (
	// CommandLifecycleEventPublishingMode documents that command lifecycle events are visibility telemetry.
	CommandLifecycleEventPublishingMode = "best_effort_visibility"
	// SwarmDisabledModeContract documents supported behavior when balda.swarm.enabled=false.
	SwarmDisabledModeContract = "runtime_unavailable_no_fallback"
)

// IsCommandQueueFull reports whether an error came from command stream pressure.
func IsCommandQueueFull(err error) bool {
	return errors.Is(err, ErrCommandQueueFull)
}

// CommandPublishResult is the JetStream acknowledgement for an accepted command.
type CommandPublishResult struct {
	Stream    string
	Sequence  uint64
	Subject   string
	MsgID     string
	Duplicate bool
}

// CommandMessage is a command delivered by the durable command bus.
type CommandMessage interface {
	Envelope() Envelope
	Subject() string
	InProgress(ctx context.Context) error
	DeliveryAttempt() int
	MaxDeliveries() int
	Ack(ctx context.Context) error
	Retry(ctx context.Context, delay time.Duration, reason string) error
	DeadLetter(ctx context.Context, reason string) error
}

// CommandHandler handles one durable command message.
type CommandHandler func(ctx context.Context, msg CommandMessage) error

// CommandPublisher publishes durable actor commands.
type CommandPublisher interface {
	PublishCommand(ctx context.Context, env Envelope) (*CommandPublishResult, error)
}

// EventPublisher publishes durable visibility events.
type EventPublisher interface {
	PublishEvent(ctx context.Context, subject string, env Envelope) error
}

// DLQPublisher publishes terminal command failures.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, env Envelope, reason string) error
}

// CommandConsumer consumes durable actor commands.
type CommandConsumer interface {
	RunCommandConsumer(ctx context.Context, handler CommandHandler) error
}

// BusDrainer drains transport resources.
type BusDrainer interface {
	Drain(ctx context.Context) error
}

// CoordinatorBus is the command/event subset used by ingress coordinators.
type CoordinatorBus interface {
	CommandPublisher
	EventPublisher
}

// RuntimeBus is the command/event subset used by the actor runtime.
type RuntimeBus interface {
	CommandConsumer
	EventPublisher
}

// CommandBus is Balda's full transport contract. JetStream is the only runtime implementation.
type CommandBus interface {
	CommandPublisher
	EventPublisher
	DLQPublisher
	CommandConsumer
	BusDrainer
}

// CommandBusStatus describes JetStream stream and consumer state for /swarm status.
type CommandBusStatus struct {
	CommandBus                string
	Embedded                  bool
	Running                   bool
	JetStream                 bool
	ClientURL                 string
	DisabledMode              string
	Commands                  StreamStatus
	Events                    StreamStatus
	DLQ                       StreamStatus
	Worker                    ConsumerStatus
	ProjectionLag             map[string]uint64
	CommandsPublishedTotal    uint64
	CommandsRunningTotal      uint64
	CommandsAckedTotal        uint64
	CommandsRetryingTotal     uint64
	CommandsDeadletteredTotal uint64
	CommandDurationSeconds    float64
	ActorDurationSeconds      float64
	// DeliveryDuplicateSuppressedTotal counts duplicate command publishes that were
	// suppressed by JetStream idempotency semantics.
	DeliveryDuplicateSuppressedTotal uint64
}

// StreamStatus contains compact JetStream stream metadata.
type StreamStatus struct {
	Name     string
	Messages uint64
	Bytes    uint64
	FirstSeq uint64
	LastSeq  uint64
}

// ConsumerStatus contains compact JetStream consumer metadata.
type ConsumerStatus struct {
	Name           string
	NumPending     uint64
	NumAckPending  int
	NumRedelivered uint64
	NumWaiting     int
	DeliveredSeq   uint64
	AckFloorSeq    uint64
}

// CommandBusStatusProvider is implemented by buses that can report runtime status.
type CommandBusStatusProvider interface {
	Status(ctx context.Context) (CommandBusStatus, error)
}

// DLQEntry describes a terminal command message stored in BALDA_DLQ.
type DLQEntry struct {
	Stream      string
	Sequence    uint64
	Subject     string
	PublishedAt time.Time
	Reason      string
	Envelope    Envelope
}

// DLQInspector provides targeted inspection for /dlq <id>.
type DLQInspector interface {
	GetDLQEntry(ctx context.Context, sequence uint64) (DLQEntry, error)
}

// UnsupportedCommandBus is installed when the swarm runtime is disabled.
type UnsupportedCommandBus struct{}

func (UnsupportedCommandBus) PublishCommand(context.Context, Envelope) (*CommandPublishResult, error) {
	return nil, fmt.Errorf("command bus is unavailable")
}

func (UnsupportedCommandBus) PublishEvent(context.Context, string, Envelope) error { return nil }

func (UnsupportedCommandBus) PublishDLQ(context.Context, Envelope, string) error { return nil }

func (UnsupportedCommandBus) RunCommandConsumer(ctx context.Context, _ CommandHandler) error {
	<-ctx.Done()
	return ctx.Err()
}

func (UnsupportedCommandBus) Drain(context.Context) error { return nil }

func (UnsupportedCommandBus) Status(context.Context) (CommandBusStatus, error) {
	return CommandBusStatus{
		CommandBus:   "unavailable",
		DisabledMode: SwarmDisabledModeContract,
	}, nil
}

// EventHandler is kept for event projector code that consumes decoded events.
type EventHandler func(ctx context.Context, subject string, env Envelope) error

// EventConsumer consumes durable JetStream events for read-model projection.
type EventConsumer interface {
	RunEventConsumer(ctx context.Context, handler EventHandler) error
}

// Subscription is a cancellable event subscription.
type Subscription interface {
	Unsubscribe() error
}

// RetryDelay computes the first retry delay for simple bus adapters.
func RetryDelay(attempt int) time.Duration {
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
	return delay + jitter
}

// RetryExhausted reports whether an attempt has reached terminal retry limit.
func RetryExhausted(attempt int, maxAttempts int) bool {
	return maxAttempts > 0 && attempt >= maxAttempts
}

const (
	retryBaseDelay = time.Second
	retryMaxDelay  = time.Minute
)
