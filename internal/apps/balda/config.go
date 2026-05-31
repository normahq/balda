package balda

import baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"

// Config holds the configuration for the Balda bot.
type Config struct {
	Balda BaldaConfig `mapstructure:"balda"`
}

// BaldaConfig holds the balda-specific configuration.
type BaldaConfig struct {
	Provider          string               `mapstructure:"provider"`
	Telegram          TelegramConfig       `mapstructure:"telegram"`
	Webhooks          WebhooksConfig       `mapstructure:"webhooks"`
	Logger            LoggerConfig         `mapstructure:"logger"`
	WorkingDir        string               `mapstructure:"working_dir"`
	StateDir          string               `mapstructure:"state_dir"`
	Sessions          SessionsConfig       `mapstructure:"sessions"`
	Memory            MemoryConfig         `mapstructure:"memory"`
	Goal              GoalConfig           `mapstructure:"goal"`
	NATS              baldaeventbus.Config `mapstructure:"nats"`
	Swarm             SwarmConfig          `mapstructure:"swarm"`
	Scheduler         SchedulerConfig      `mapstructure:"scheduler"`
	Workspace         WorkspaceConfig      `mapstructure:"workspace"`
	MCPServers        []string             `mapstructure:"mcp_servers"`
	GlobalInstruction string               `mapstructure:"global_instruction"`
	// RemovedEventBus detects removed balda.event_bus config and fails startup loudly.
	RemovedEventBus any `mapstructure:"event_bus"`
}

// TelegramConfig holds the Telegram bot configuration.
type TelegramConfig struct {
	Token          string        `mapstructure:"token"`
	FormattingMode string        `mapstructure:"formatting_mode"`
	PlanUpdates    bool          `mapstructure:"plan_updates"`
	Webhook        WebhookConfig `mapstructure:"webhook"`
}

// WebhookConfig holds Telegram webhook receiver settings.
type WebhookConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
	Path       string `mapstructure:"path"`
	URL        string `mapstructure:"url"`
	AuthToken  string `mapstructure:"auth_token"`
}

// WebhooksConfig controls Balda-owned external webhook ingestion.
type WebhooksConfig struct {
	Enabled    bool                          `mapstructure:"enabled"`
	ListenAddr string                        `mapstructure:"listen_addr"`
	Routes     map[string]WebhookRouteConfig `mapstructure:"routes"`
	// RemovedMode detects removed balda.webhooks.mode config and fails startup loudly.
	RemovedMode any `mapstructure:"mode"`
}

// WebhookRouteConfig binds a webhook path to route envelope/auth/dedupe policy.
type WebhookRouteConfig struct {
	Path           string                     `mapstructure:"path"`
	PromptTemplate string                     `mapstructure:"prompt_template"`
	Envelope       WebhookRouteEnvelopeConfig `mapstructure:"envelope"`
	Auth           WebhookRouteAuthConfig     `mapstructure:"auth"`
	Dedupe         WebhookRouteDedupeConfig   `mapstructure:"dedupe"`
}

// WebhookRouteEnvelopeConfig configures the session envelope for one route.
type WebhookRouteEnvelopeConfig struct {
	Target   string                            `mapstructure:"target"`
	Key      string                            `mapstructure:"key"`
	Mode     string                            `mapstructure:"mode"`
	ReportTo *WebhookRouteEnvelopeTargetConfig `mapstructure:"report_to"`
}

// WebhookRouteEnvelopeTargetConfig defines a route report_to address.
type WebhookRouteEnvelopeTargetConfig struct {
	Target string `mapstructure:"target"`
	Key    string `mapstructure:"key"`
}

// WebhookRouteAuthConfig configures route-level request authentication.
type WebhookRouteAuthConfig struct {
	Type      string `mapstructure:"type"`
	Header    string `mapstructure:"header"`
	Value     string `mapstructure:"value"`
	SecretEnv string `mapstructure:"secret_env"`
}

// WebhookRouteDedupeConfig configures dedupe key extraction for one route.
type WebhookRouteDedupeConfig struct {
	Source string `mapstructure:"source"`
	Header string `mapstructure:"header"`
}

// LoggerConfig holds the logger configuration.
type LoggerConfig struct {
	Level  string `mapstructure:"level"`
	Pretty bool   `mapstructure:"pretty"`
}

// WorkspaceConfig controls balda Git workspace behavior.
type WorkspaceConfig struct {
	Mode        string `mapstructure:"mode"`
	BaseBranch  string `mapstructure:"base_branch"`
	SessionsDir string `mapstructure:"sessions_dir"`
}

type SessionsConfig struct {
	Persistence string `mapstructure:"persistence"`
}

type MemoryConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// GoalConfig controls /goal command execution behavior.
type GoalConfig struct {
	MaxIterations int `mapstructure:"max_iterations"`
}

// SwarmConfig controls the JetStream-backed actor runtime.
type SwarmConfig struct {
	Commands SwarmCommandConfig `mapstructure:"commands"`
	Events   SwarmEventConfig   `mapstructure:"events"`
	DLQ      SwarmDLQConfig     `mapstructure:"dlq"`
	// RemovedMode detects removed balda.swarm.mode config and fails startup loudly.
	RemovedMode any `mapstructure:"mode"`
}

type SwarmCommandConfig struct {
	Stream        string `mapstructure:"stream"`
	Consumer      string `mapstructure:"consumer"`
	AckWait       string `mapstructure:"ack_wait"`
	MaxDeliver    int    `mapstructure:"max_deliver"`
	MaxAckPending int    `mapstructure:"max_ack_pending"`
	FetchBatch    int    `mapstructure:"fetch_batch"`
	FetchWait     string `mapstructure:"fetch_wait"`
}

type SwarmEventConfig struct {
	Stream string `mapstructure:"stream"`
}

type SwarmDLQConfig struct {
	Stream string `mapstructure:"stream"`
}

// SchedulerConfig controls startup-managed recurring tasks.
type SchedulerConfig struct {
	Tasks []ScheduledTaskConfig `mapstructure:"tasks"`
	// RemovedMode detects removed balda.scheduler.mode config so it fails loudly instead of being ignored.
	RemovedMode any `mapstructure:"mode"`
	// RemovedJobs detects removed balda.scheduler.jobs config so it fails loudly instead of being ignored.
	RemovedJobs any `mapstructure:"jobs"`
}

// ScheduledTaskConfig defines a config-managed recurring task.
type ScheduledTaskConfig struct {
	ID       string                      `mapstructure:"id"`
	Cron     string                      `mapstructure:"cron"`
	Envelope ScheduledTaskEnvelopeConfig `mapstructure:"envelope"`
}

// ScheduledTaskEnvelopeConfig defines the task envelope produced by a schedule.
type ScheduledTaskEnvelopeConfig struct {
	Target   string                             `mapstructure:"target"`
	Key      string                             `mapstructure:"key"`
	Content  string                             `mapstructure:"content"`
	ReportTo *ScheduledTaskEnvelopeTargetConfig `mapstructure:"report_to"`
}

// ScheduledTaskEnvelopeTargetConfig defines a scheduler envelope address.
type ScheduledTaskEnvelopeTargetConfig struct {
	Target string `mapstructure:"target"`
	Key    string `mapstructure:"key"`
}
