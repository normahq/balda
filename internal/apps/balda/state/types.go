package state

import (
	"context"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/tgbotkit/runtime/updatepoller"
	adksession "google.golang.org/adk/v2/session"
)

const (
	// NamespaceApp stores balda app internal state (for example owner auth).
	NamespaceApp = "balda.app"
	// NamespaceSessionMCP stores balda.state MCP key-value data.
	NamespaceSessionMCP = "balda.session_mcp"

	// SessionStatusActive marks a session that can be lazily restored.
	SessionStatusActive = "active"

	// ChannelTypeTelegram is the current balda channel type backed by Telegram.
	ChannelTypeTelegram = "telegram"

	// ChannelTypeZulip is the balda channel type backed by Zulip.
	ChannelTypeZulip = "zulip"

	// ChannelTypeSlack is the balda channel type backed by Slack.
	ChannelTypeSlack = "slack"

	// ScheduledTaskStatusActive means the task is eligible for scheduler dispatch.
	ScheduledTaskStatusActive = "active"
	// ScheduledTaskStatusPaused means the task is persisted but not dispatched.
	ScheduledTaskStatusPaused = "paused"

	// JobStatusCreated means a task record exists but has not been queued.
	JobStatusCreated = "created"
	// JobStatusQueued means task work is queued for actor execution.
	JobStatusQueued = "queued"
	// JobStatusRunning means a task actor is actively coordinating work.
	JobStatusRunning = "running"
	// JobStatusWaitingForAgent means task execution waits on an agent role.
	JobStatusWaitingForAgent = "waiting_for_agent"
	// JobStatusWaitingForUser means task execution is blocked on user input.
	JobStatusWaitingForUser = "waiting_for_user"
	// JobStatusValidating means a reviewer/validator is checking the work.
	JobStatusValidating = "validating"
	// JobStatusCompleted means the task finished successfully.
	JobStatusCompleted = "completed"
	// JobStatusFailed means the task exhausted its retry/iteration budget.
	JobStatusFailed = "failed"
	// JobStatusCanceled means the task was canceled before completion.
	JobStatusCanceled = "canceled"
	// JobStatusDeadLettered means the actor runtime deadlettered the task.
	JobStatusDeadLettered = "deadlettered"

	// DeliveryStatusPending means a delivery side effect is reserved but not confirmed.
	DeliveryStatusPending = "pending"
	// DeliveryStatusSending means a delivery side effect attempt is in progress
	// or its outcome is ambiguous after process failure.
	DeliveryStatusSending = "sending"
	// DeliveryStatusSent means a delivery side effect was successfully sent.
	DeliveryStatusSent = "sent"
	// DeliveryStatusFailed means the latest delivery attempt failed.
	DeliveryStatusFailed = "failed"

	// AgentStepStatusRunning means an agent step has been reserved but no result is stored.
	AgentStepStatusRunning = "running"
	// AgentStepStatusSucceeded means an agent step result is stored and can be replayed.
	AgentStepStatusSucceeded = "succeeded"
	// AgentStepStatusFailed means an agent step error result is stored and can be replayed.
	AgentStepStatusFailed = "failed"
)

// Provider exposes balda state capabilities behind a backend-agnostic interface.
// This allows swapping SQLite with another provider later.
type Provider interface {
	AppKV() KVStore
	RuntimeSessions() adksession.Service
	SessionMCPKV() KVStore
	Sessions() SessionStore
	ScheduledTasks() ScheduledTaskStore
	Jobs() JobStore
	PollingOffsetStore() updatepoller.OffsetStore
	Collaborators() CollaboratorStore
	Close() error
}

// KVStore stores string and JSON key/value records.
type KVStore interface {
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Clear(ctx context.Context) error
	GetJSON(ctx context.Context, key string) (value any, ok bool, err error)
	// ConsumeJSON atomically reads a JSON value and deletes it when shouldConsume returns true.
	ConsumeJSON(ctx context.Context, key string, shouldConsume func(value any) (bool, error)) (value any, consumed bool, err error)
	SetJSON(ctx context.Context, key string, value any) error
	SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error
	MergeJSON(ctx context.Context, key string, fields map[string]any) (merged map[string]any, err error)
}

// CollaboratorStore persists authorized collaborators.
type CollaboratorStore interface {
	AddCollaborator(ctx context.Context, c auth.Collaborator) error
	RemoveCollaborator(ctx context.Context, userID string) error
	GetCollaborator(ctx context.Context, userID string) (*auth.Collaborator, bool, error)
	ListCollaborators(ctx context.Context) ([]auth.Collaborator, error)
}

// SessionRecord persists balda session metadata for lazy restore.
type SessionRecord struct {
	SessionID    string
	UserID       string
	ChannelType  string
	AddressKey   string
	AddressJSON  string
	AgentName    string
	WorkspaceDir string
	BranchName   string
	Status       string
}

// SessionStore persists balda session metadata.
type SessionStore interface {
	Upsert(ctx context.Context, record SessionRecord) error
	GetByAddress(ctx context.Context, channelType, addressKey string) (SessionRecord, bool, error)
	GetBySessionID(ctx context.Context, sessionID string) (SessionRecord, bool, error)
	DeleteBySessionID(ctx context.Context, sessionID string) error
	List(ctx context.Context) ([]SessionRecord, error)
}

// ScheduledTaskRecord persists locator-targeted recurring task metadata.
type ScheduledTaskRecord struct {
	JobID               string
	SessionID           string
	ChannelType         string
	AddressKey          string
	AddressJSON         string
	ReportToEnabled     bool
	ReportToSessionID   string
	ReportToChannelType string
	ReportToAddressKey  string
	ReportToAddressJSON string
	Content             string
	ScheduleSpec        string
	Timezone            string
	Status              string
	MaxRetries          int
	RetryCount          int
	LastDispatchKey     string
	NextRunAt           time.Time
	LastRunAt           time.Time
	LastError           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// ScheduledTaskStore persists scheduler tasks bound to canonical locators.
type ScheduledTaskStore interface {
	Upsert(ctx context.Context, record ScheduledTaskRecord) error
	GetByID(ctx context.Context, taskID string) (ScheduledTaskRecord, bool, error)
	List(ctx context.Context) ([]ScheduledTaskRecord, error)
	ListByAddress(ctx context.Context, channelType, addressKey string) ([]ScheduledTaskRecord, error)
	ListDue(ctx context.Context, now time.Time, limit int) ([]ScheduledTaskRecord, error)
	Delete(ctx context.Context, taskID string) error
}

// JobRecord persists one assignable work item.
type JobRecord struct {
	ID            string
	SessionID     string
	ParentJobID   string
	Title         string
	Objective     string
	Status        string
	OwnerActor    string
	AssignedActor string
	Priority      int
	CreatedBy     string
	ResultJSON    string
	Error         string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	StartedAt     time.Time
	CompletedAt   time.Time
	CanceledAt    time.Time
}

// JobEventRecord persists an append-only task event.
type JobEventRecord struct {
	ID          string
	JobID       string
	EventType   string
	Actor       string
	MessageID   string
	PayloadJSON string
	CreatedAt   time.Time
}

// DeliveryRecord persists idempotency state for external delivery side effects.
type DeliveryRecord struct {
	ID                string
	DeliveryKey       string
	JobID             string
	SessionID         string
	Channel           string
	AddressKey        string
	Kind              string
	PayloadJSON       string
	PayloadHash       string
	Status            string
	ProviderMessageID string
	SentAt            time.Time
	Error             string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// AgentStepRecord persists idempotency state for one logical agent step.
type AgentStepRecord struct {
	ID          string
	StepKey     string
	JobID       string
	AgentName   string
	Role        string
	Iteration   int
	PayloadHash string
	Status      string
	ResultJSON  string
	Error       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time
}

// JobStore persists Balda job orchestration state and read models.
type JobStore interface {
	CreateJob(ctx context.Context, record JobRecord) (bool, error)
	GetJob(ctx context.Context, taskID string) (JobRecord, bool, error)
	ListActiveJobsBySession(ctx context.Context, sessionID string) ([]JobRecord, error)
	UpdateJobStatus(ctx context.Context, taskID string, status string, reason string) error
	SetJobResult(ctx context.Context, taskID string, resultJSON string, status string, reason string) error
	AppendJobEvent(ctx context.Context, record JobEventRecord) error
	ListJobEvents(ctx context.Context, taskID string) ([]JobEventRecord, error)
	ReserveDelivery(ctx context.Context, record DeliveryRecord) (DeliveryRecord, bool, error)
	MarkDeliverySending(ctx context.Context, deliveryKey string) error
	MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error
	MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error
	ReserveAgentStep(ctx context.Context, record AgentStepRecord) (AgentStepRecord, bool, error)
	CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error
	FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error
}
