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
	EventBus          baldaeventbus.Config `mapstructure:"event_bus"`
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

// SwarmConfig controls the actor mailbox runtime.
type SwarmConfig struct {
	Enabled       bool                        `mapstructure:"enabled"`
	Mode          string                      `mapstructure:"mode"`
	WebhookMode   string                      `mapstructure:"webhook_mode"`
	SchedulerMode string                      `mapstructure:"scheduler_mode"`
	Shadow        SwarmShadowConfig           `mapstructure:"shadow"`
	Queue         SwarmQueueConfig            `mapstructure:"queue"`
	Agents        map[string]SwarmAgentConfig `mapstructure:"agents"`
}

// SwarmShadowConfig controls safe dual-write rollout behavior.
type SwarmShadowConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// SwarmQueueConfig controls mailbox queue policy.
type SwarmQueueConfig struct {
	DefaultMode string         `mapstructure:"default_mode"`
	DebounceMS  int            `mapstructure:"debounce_ms"`
	Cap         int            `mapstructure:"cap"`
	Drop        string         `mapstructure:"drop"`
	ByNamespace map[string]any `mapstructure:"by_namespace"`
}

// SwarmAgentConfig configures a logical single-process swarm agent.
type SwarmAgentConfig struct {
	Role        string   `mapstructure:"role"`
	Tools       []string `mapstructure:"tools"`
	CostPenalty int      `mapstructure:"cost_penalty"`
}

// SchedulerConfig controls startup-managed recurring jobs.
type SchedulerConfig struct {
	Jobs []ScheduledJobConfig `mapstructure:"jobs"`
}

// ScheduledJobConfig defines a config-managed recurring job.
type ScheduledJobConfig struct {
	ID     string `mapstructure:"id"`
	Cron   string `mapstructure:"cron"`
	Prompt string `mapstructure:"prompt"`
}
