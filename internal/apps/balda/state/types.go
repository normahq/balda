package state

import (
	"context"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/tgbotkit/runtime/updatepoller"
	adksession "google.golang.org/adk/session"
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

	// ScheduledTaskStatusActive means the task is eligible for scheduler dispatch.
	ScheduledTaskStatusActive = "active"
	// ScheduledTaskStatusPaused means the task is persisted but not dispatched.
	ScheduledTaskStatusPaused = "paused"

	// SwarmTaskStatusCreated means a task record exists but has not been queued.
	SwarmTaskStatusCreated = "created"
	// SwarmTaskStatusQueued means task work is queued for actor execution.
	SwarmTaskStatusQueued = "queued"
	// SwarmTaskStatusRunning means a task actor is actively coordinating work.
	SwarmTaskStatusRunning = "running"
	// SwarmTaskStatusWaitingForAgent means task execution waits on an agent role.
	SwarmTaskStatusWaitingForAgent = "waiting_for_agent"
	// SwarmTaskStatusWaitingForUser means task execution is blocked on user input.
	SwarmTaskStatusWaitingForUser = "waiting_for_user"
	// SwarmTaskStatusValidating means a reviewer/validator is checking the work.
	SwarmTaskStatusValidating = "validating"
	// SwarmTaskStatusCompleted means the task finished successfully.
	SwarmTaskStatusCompleted = "completed"
	// SwarmTaskStatusFailed means the task exhausted its retry/iteration budget.
	SwarmTaskStatusFailed = "failed"
	// SwarmTaskStatusCanceled means the task was canceled before completion.
	SwarmTaskStatusCanceled = "canceled"
	// SwarmTaskStatusDeadLettered means the actor runtime deadlettered the task.
	SwarmTaskStatusDeadLettered = "deadlettered"

	// SwarmDeliveryStatusPending means a delivery side effect is reserved but not confirmed.
	SwarmDeliveryStatusPending = "pending"
	// SwarmDeliveryStatusSending means a delivery side effect attempt is in progress
	// or its outcome is ambiguous after process failure.
	SwarmDeliveryStatusSending = "sending"
	// SwarmDeliveryStatusSent means a delivery side effect was successfully sent.
	SwarmDeliveryStatusSent = "sent"
	// SwarmDeliveryStatusFailed means the latest delivery attempt failed.
	SwarmDeliveryStatusFailed = "failed"

	// SwarmAgentStepStatusRunning means an agent step has been reserved but no result is stored.
	SwarmAgentStepStatusRunning = "running"
	// SwarmAgentStepStatusSucceeded means an agent step result is stored and can be replayed.
	SwarmAgentStepStatusSucceeded = "succeeded"
	// SwarmAgentStepStatusFailed means an agent step error result is stored and can be replayed.
	SwarmAgentStepStatusFailed = "failed"
)

// Provider exposes balda state capabilities behind a backend-agnostic interface.
// This allows swapping SQLite with another provider later.
type Provider interface {
	AppKV() KVStore
	ADKSessions() adksession.Service
	SessionMCPKV() KVStore
	Sessions() SessionStore
	ScheduledTasks() ScheduledTaskStore
	Swarm() SwarmStore
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
	TaskID              string
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

// SwarmStatusCount describes an aggregate count by status.
type SwarmStatusCount struct {
	Status string
	Count  int
}

// SwarmTaskRecord persists one assignable work item.
type SwarmTaskRecord struct {
	ID            string
	SessionID     string
	ParentTaskID  string
	Title         string
	Objective     string
	Status        string
	OwnerActor    string
	AssignedActor string
	Priority      int
	CreatedBy     string
	CreatedFrom   string
	PlanJSON      string
	ResultJSON    string
	Error         string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	StartedAt     time.Time
	CompletedAt   time.Time
	CanceledAt    time.Time
}

// SwarmTaskEventRecord persists an append-only task event.
type SwarmTaskEventRecord struct {
	ID          string
	TaskID      string
	EventType   string
	Actor       string
	MessageID   string
	PayloadJSON string
	CreatedAt   time.Time
}

// SwarmDeliveryRecord persists idempotency state for external delivery side effects.
type SwarmDeliveryRecord struct {
	ID                string
	DeliveryKey       string
	TaskID            string
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

// SwarmAgentStepRecord persists idempotency state for one logical agent step.
type SwarmAgentStepRecord struct {
	ID          string
	StepKey     string
	TaskID      string
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

// SwarmStore persists swarm product/read-model state.
type SwarmStore interface {
	CreateTask(ctx context.Context, record SwarmTaskRecord) (bool, error)
	GetTask(ctx context.Context, taskID string) (SwarmTaskRecord, bool, error)
	ListActiveTasksBySession(ctx context.Context, sessionID string) ([]SwarmTaskRecord, error)
	ListTaskStatusCounts(ctx context.Context) ([]SwarmStatusCount, error)
	ListDeliveryStatusCounts(ctx context.Context) ([]SwarmStatusCount, error)
	UpdateTaskStatus(ctx context.Context, taskID string, status string, reason string) error
	SetTaskPlan(ctx context.Context, taskID string, planJSON string) error
	SetTaskResult(ctx context.Context, taskID string, resultJSON string, status string, reason string) error
	AppendTaskEvent(ctx context.Context, record SwarmTaskEventRecord) error
	ListTaskEvents(ctx context.Context, taskID string) ([]SwarmTaskEventRecord, error)
	ReserveDelivery(ctx context.Context, record SwarmDeliveryRecord) (SwarmDeliveryRecord, bool, error)
	MarkDeliverySending(ctx context.Context, deliveryKey string) error
	MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error
	MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error
	ReserveAgentStep(ctx context.Context, record SwarmAgentStepRecord) (SwarmAgentStepRecord, bool, error)
	CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error
	FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error
}
