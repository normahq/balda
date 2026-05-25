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

	// ScheduledJobStatusActive means the job is eligible for scheduler dispatch.
	ScheduledJobStatusActive = "active"
	// ScheduledJobStatusPaused means the job is persisted but not dispatched.
	ScheduledJobStatusPaused = "paused"

	// SwarmMessageStatusQueued means the message can be claimed immediately.
	SwarmMessageStatusQueued = "queued"
	// SwarmMessageStatusLeased means a worker owns the message until lease_until.
	SwarmMessageStatusLeased = "leased"
	// SwarmMessageStatusAcked means the actor handled the message successfully.
	SwarmMessageStatusAcked = "acked"
	// SwarmMessageStatusRetry means the message can be claimed after not_before.
	SwarmMessageStatusRetry = "retry"
	// SwarmMessageStatusDead means the message failed permanently.
	SwarmMessageStatusDead = "dead"
	// SwarmMessageStatusCanceled means the message was canceled before completion.
	SwarmMessageStatusCanceled = "canceled"
	// SwarmMessageStatusExpired means expires_at elapsed before successful handling.
	SwarmMessageStatusExpired = "expired"
	// SwarmMessageStatusShadow means the message is stored for rollout comparison only.
	SwarmMessageStatusShadow = "shadow"

	// SwarmMessageDefaultMaxAttempts is the default retry budget for messages.
	SwarmMessageDefaultMaxAttempts = 3

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
)

// Provider exposes balda state capabilities behind a backend-agnostic interface.
// This allows swapping SQLite with another provider later.
type Provider interface {
	AppKV() KVStore
	ADKSessions() adksession.Service
	SessionMCPKV() KVStore
	Sessions() SessionStore
	ScheduledJobs() ScheduledJobStore
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

// ScheduledJobRecord persists locator-targeted recurring job metadata.
type ScheduledJobRecord struct {
	JobID           string
	SessionID       string
	ChannelType     string
	AddressKey      string
	AddressJSON     string
	Prompt          string
	ScheduleSpec    string
	Timezone        string
	Status          string
	MaxRetries      int
	RetryCount      int
	LastDispatchKey string
	NextRunAt       time.Time
	LastRunAt       time.Time
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ScheduledJobStore persists scheduler jobs bound to canonical locators.
type ScheduledJobStore interface {
	Upsert(ctx context.Context, record ScheduledJobRecord) error
	GetByID(ctx context.Context, jobID string) (ScheduledJobRecord, bool, error)
	List(ctx context.Context) ([]ScheduledJobRecord, error)
	ListByAddress(ctx context.Context, channelType, addressKey string) ([]ScheduledJobRecord, error)
	ListDue(ctx context.Context, now time.Time, limit int) ([]ScheduledJobRecord, error)
	Delete(ctx context.Context, jobID string) error
}

// SwarmMessageRecord persists one durable actor mailbox message.
type SwarmMessageRecord struct {
	ID            string
	Mailbox       string
	Namespace     string
	Kind          string
	FromAddr      string
	ToAddr        string
	SessionID     string
	TaskID        string
	CorrelationID string
	CausationID   string
	Priority      int
	DedupeKey     string
	Status        string
	Attempt       int
	MaxAttempts   int
	NotBefore     time.Time
	ExpiresAt     time.Time
	LeaseOwner    string
	LeaseUntil    time.Time
	PayloadJSON   string
	MetaJSON      string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CompletedAt   time.Time
	Error         string
}

// SwarmPublishResult describes whether a publish inserted a new row or deduped.
type SwarmPublishResult struct {
	ID        string
	Mailbox   string
	Published bool
}

// SwarmRecoveryResult describes rows repaired by a recovery sweep.
type SwarmRecoveryResult struct {
	RetriedLeases int
	Expired       int
}

// SwarmStatusCount describes an aggregate count by status.
type SwarmStatusCount struct {
	Status string
	Count  int
}

// SwarmMailboxStatusCount describes one mailbox/status aggregate.
type SwarmMailboxStatusCount struct {
	Mailbox string
	Status  string
	Count   int
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

// SwarmStore persists actor mailbox messages and task state.
type SwarmStore interface {
	Publish(ctx context.Context, record SwarmMessageRecord) (SwarmPublishResult, error)
	PublishBatch(ctx context.Context, records []SwarmMessageRecord) ([]SwarmPublishResult, error)
	Claim(ctx context.Context, mailbox string, owner string, limit int, lease time.Duration) ([]SwarmMessageRecord, error)
	Ack(ctx context.Context, mailbox string, messageID string) error
	Retry(ctx context.Context, mailbox string, messageID string, next time.Time, reason string) error
	DeadLetter(ctx context.Context, mailbox string, messageID string, reason string) error
	CancelByTask(ctx context.Context, taskID string, reason string) (int, error)
	CancelBySession(ctx context.Context, sessionID string, reason string) (int, error)
	PendingCount(ctx context.Context, mailbox string) (int, error)
	CancelDroppable(ctx context.Context, mailbox string, limit int, reason string) ([]SwarmMessageRecord, error)
	Recover(ctx context.Context, now time.Time) (SwarmRecoveryResult, error)
	ListReadyMailboxes(ctx context.Context, limit int) ([]string, error)
	ListMailboxStatusCounts(ctx context.Context, limit int) ([]SwarmMailboxStatusCount, error)
	GetMessage(ctx context.Context, messageID string) (SwarmMessageRecord, bool, error)
	CreateTask(ctx context.Context, record SwarmTaskRecord) (bool, error)
	GetTask(ctx context.Context, taskID string) (SwarmTaskRecord, bool, error)
	ListActiveTasksBySession(ctx context.Context, sessionID string) ([]SwarmTaskRecord, error)
	ListTaskStatusCounts(ctx context.Context) ([]SwarmStatusCount, error)
	UpdateTaskStatus(ctx context.Context, taskID string, status string, reason string) error
	SetTaskPlan(ctx context.Context, taskID string, planJSON string) error
	SetTaskResult(ctx context.Context, taskID string, resultJSON string, status string, reason string) error
	AppendTaskEvent(ctx context.Context, record SwarmTaskEventRecord) error
	ListTaskEvents(ctx context.Context, taskID string) ([]SwarmTaskEventRecord, error)
}
