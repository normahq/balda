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
}

// WebhookRouteConfig binds a webhook path to a prompt template.
type WebhookRouteConfig struct {
	Path           string `mapstructure:"path"`
	PromptTemplate string `mapstructure:"prompt_template"`
}

// LoggerConfig holds the logger configuration.
type LoggerConfig struct {
	Level  string `mapstructure:"level"`
	Pretty bool   `mapstructure:"pretty"`
}

// WorkspaceConfig controls balda Git workspace behavior.
type WorkspaceConfig struct {
	Mode       string `mapstructure:"mode"`
	BaseBranch string `mapstructure:"base_branch"`
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
	Enabled  bool                        `mapstructure:"enabled"`
	Commands SwarmCommandConfig          `mapstructure:"commands"`
	Events   SwarmEventConfig            `mapstructure:"events"`
	DLQ      SwarmDLQConfig              `mapstructure:"dlq"`
	Agents   map[string]SwarmAgentConfig `mapstructure:"agents"`
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

// SwarmAgentConfig configures a logical single-process swarm agent.
type SwarmAgentConfig struct {
	Role        string   `mapstructure:"role"`
	Tools       []string `mapstructure:"tools"`
	CostPenalty int      `mapstructure:"cost_penalty"`
}

// SchedulerConfig controls startup-managed recurring tasks.
type SchedulerConfig struct {
	Tasks []ScheduledTaskConfig `mapstructure:"tasks"`
	// RemovedJobs detects stale balda.scheduler.jobs config so it fails loudly instead of being ignored.
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
