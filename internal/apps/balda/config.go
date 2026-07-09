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
	Zulip             ZulipConfig          `mapstructure:"zulip"`
	Slack             SlackConfig          `mapstructure:"slack"`
	Webhooks          WebhooksConfig       `mapstructure:"webhooks"`
	Logger            LoggerConfig         `mapstructure:"logger"`
	WorkingDir        string               `mapstructure:"working_dir"`
	StateDir          string               `mapstructure:"state_dir"`
	Sessions          SessionsConfig       `mapstructure:"sessions"`
	Memory            MemoryConfig         `mapstructure:"memory"`
	Goal              GoalConfig           `mapstructure:"goal"`
	NATS              baldaeventbus.Config `mapstructure:"nats"`
	Execution         ExecutionConfig      `mapstructure:"execution"`
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

// ZulipConfig holds the Zulip bot configuration.
type ZulipConfig struct {
	BotEmail     string             `mapstructure:"bot_email"`
	APIKey       string             `mapstructure:"api_key"`
	ServerURL    string             `mapstructure:"server_url"`
	WebhookToken string             `mapstructure:"webhook_token"`
	Webhook      ZulipWebhookConfig `mapstructure:"webhook"`
}

// ZulipWebhookConfig holds Zulip webhook receiver settings.
type ZulipWebhookConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
	Path       string `mapstructure:"path"`
}

// SlackConfig holds Slack app configuration.
type SlackConfig struct {
	Enabled                bool   `mapstructure:"enabled"`
	BotToken               string `mapstructure:"bot_token"`
	SigningSecret          string `mapstructure:"signing_secret"`
	ListenAddr             string `mapstructure:"listen_addr"`
	EventsPath             string `mapstructure:"events_path"`
	CommandsPath           string `mapstructure:"commands_path"`
	IncludePrivateChannels bool   `mapstructure:"include_private_channels"`
}

// WebhooksConfig controls Balda-owned external webhook ingestion.
type WebhooksConfig struct {
	Enabled    bool                          `mapstructure:"enabled"`
	ListenAddr string                        `mapstructure:"listen_addr"`
	Routes     map[string]WebhookRouteConfig `mapstructure:"routes"`
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

// ExecutionConfig controls Balda's durable actor execution layer.
type ExecutionConfig struct {
	Commands ExecutionCommandConfig `mapstructure:"commands"`
	Events   ExecutionEventConfig   `mapstructure:"events"`
	DLQ      ExecutionDLQConfig     `mapstructure:"dlq"`
}

type ExecutionCommandConfig struct {
	Stream        string `mapstructure:"stream"`
	Consumer      string `mapstructure:"consumer"`
	AckWait       string `mapstructure:"ack_wait"`
	MaxDeliver    int    `mapstructure:"max_deliver"`
	MaxAckPending int    `mapstructure:"max_ack_pending"`
	FetchBatch    int    `mapstructure:"fetch_batch"`
	FetchWait     string `mapstructure:"fetch_wait"`
}

type ExecutionEventConfig struct {
	Stream string `mapstructure:"stream"`
}

type ExecutionDLQConfig struct {
	Stream string `mapstructure:"stream"`
}

// SchedulerConfig controls startup-managed recurring jobs.
type SchedulerConfig struct {
	Jobs []ScheduledJobConfig `mapstructure:"jobs"`
}

// ScheduledJobConfig defines a config-managed recurring job.
type ScheduledJobConfig struct {
	ID       string                     `mapstructure:"id"`
	Cron     string                     `mapstructure:"cron"`
	Envelope ScheduledJobEnvelopeConfig `mapstructure:"envelope"`
}

// ScheduledJobEnvelopeConfig defines the job envelope produced by a schedule.
type ScheduledJobEnvelopeConfig struct {
	Target   string                            `mapstructure:"target"`
	Key      string                            `mapstructure:"key"`
	Content  string                            `mapstructure:"content"`
	ReportTo *ScheduledJobEnvelopeTargetConfig `mapstructure:"report_to"`
}

// ScheduledJobEnvelopeTargetConfig defines a scheduler envelope address.
type ScheduledJobEnvelopeTargetConfig struct {
	Target string `mapstructure:"target"`
	Key    string `mapstructure:"key"`
}
