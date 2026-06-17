package balda

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/ipfans/fxlogger"
	"github.com/normahq/balda/internal/apps/balda/actors"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	natsbus "github.com/normahq/balda/internal/apps/balda/eventbus/nats"
	"github.com/normahq/balda/internal/apps/balda/handlers"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/normahq/balda/internal/apps/balda/shutdown"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/internal/apps/sessionmcp"
	"github.com/normahq/balda/internal/git"
	"github.com/normahq/norma/pkg/runtime/agentconfig"
	"github.com/normahq/norma/pkg/runtime/agentfactory"
	runtimeconfig "github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/normahq/norma/pkg/runtime/mcpregistry"
	"github.com/rs/zerolog/log"
	"github.com/tgbotkit/runtime"
	"github.com/tgbotkit/runtime/updatepoller"
	"go.uber.org/fx"
	adksession "google.golang.org/adk/session"
)

type workspaceBaseBranchParams struct {
	fx.In

	WorkspaceEnabled bool `name:"balda_workspace_enabled"`
}

const bundledBaldaMCPServerID = "balda"

const (
	sessionPersistenceMemory = "memory"
	sessionPersistenceSQLite = "sqlite"
)

var configIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const defaultWorkspaceSessionsDirName = "sessions"

// App creates a new fx.App for the Balda bot with the provided configuration.
func App(
	cfg Config,
	normaCfg runtimeconfig.RuntimeConfig,
	ownerToken string,
	runtimeLoadOpts runtimeconfig.RuntimeLoadOptions,
	defaultsYAML []byte,
) *fx.App {
	return fx.New(
		fx.WithLogger(
			fxlogger.WithZerolog(
				log.Logger.With().Str("component", "balda").Logger(),
			),
		),
		Module(cfg, normaCfg, ownerToken, runtimeLoadOpts, defaultsYAML),
	)
}

// Module returns the fx.Module for the Balda bot, initialized with the provided configurations.
func Module(
	cfg Config,
	normaCfg runtimeconfig.RuntimeConfig,
	ownerToken string,
	runtimeLoadOpts runtimeconfig.RuntimeLoadOptions,
	defaultsYAML []byte,
) fx.Option {
	// Convert balda config to tgbotkit config.
	tgbotkitCfg := tgbotkit.Config{
		Token: cfg.Balda.Telegram.Token,
		Webhook: tgbotkit.WebhookConfig{
			Enabled:    cfg.Balda.Telegram.Webhook.Enabled,
			ListenAddr: cfg.Balda.Telegram.Webhook.ListenAddr,
			Path:       cfg.Balda.Telegram.Webhook.Path,
			URL:        cfg.Balda.Telegram.Webhook.URL,
			AuthToken:  cfg.Balda.Telegram.Webhook.AuthToken,
		},
	}

	logger := log.Logger.With().Str("component", "balda").Logger()
	workingDir, err := paths.ResolveWorkingDir(cfg.Balda.WorkingDir)
	if err != nil {
		return fx.Module("balda", fx.Error(fmt.Errorf("resolve balda working_dir: %w", err)))
	}
	if err := validateBaldaMCPConfiguration(normaCfg); err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	formattingMode, err := telegramfmt.ValidateMode(cfg.Balda.Telegram.FormattingMode)
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	stateDir, err := paths.ResolveStateDir(workingDir, cfg.Balda.StateDir)
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	sessionPersistence, err := validateSessionPersistence(cfg.Balda.Sessions.Persistence)
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	workspaceSessionsDir, err := resolveWorkspaceSessionsDir(cfg.Balda.Workspace.SessionsDir)
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	scheduledTaskSchedulerConfig := handlers.ScheduledTaskSchedulerConfig{
		Tasks: make([]handlers.ConfiguredScheduledTask, 0, len(cfg.Balda.Scheduler.Tasks)),
	}
	for _, task := range cfg.Balda.Scheduler.Tasks {
		var reportTo *handlers.ConfiguredScheduledTaskTarget
		if task.Envelope.ReportTo != nil {
			reportTo = &handlers.ConfiguredScheduledTaskTarget{
				Target: strings.TrimSpace(task.Envelope.ReportTo.Target),
				Key:    strings.TrimSpace(task.Envelope.ReportTo.Key),
			}
		}
		scheduledTaskSchedulerConfig.Tasks = append(scheduledTaskSchedulerConfig.Tasks, handlers.ConfiguredScheduledTask{
			ID:       strings.TrimSpace(task.ID),
			Cron:     strings.TrimSpace(task.Cron),
			Target:   strings.TrimSpace(task.Envelope.Target),
			Key:      strings.TrimSpace(task.Envelope.Key),
			Content:  strings.TrimSpace(task.Envelope.Content),
			ReportTo: reportTo,
		})
	}
	inboundWebhookConfig := buildInboundWebhookConfig(cfg.Balda)
	swarmConfig := swarm.Config{
		Commands: swarm.CommandConfig{
			Stream:        strings.TrimSpace(cfg.Balda.Swarm.Commands.Stream),
			Consumer:      strings.TrimSpace(cfg.Balda.Swarm.Commands.Consumer),
			AckWait:       strings.TrimSpace(cfg.Balda.Swarm.Commands.AckWait),
			MaxDeliver:    cfg.Balda.Swarm.Commands.MaxDeliver,
			MaxAckPending: cfg.Balda.Swarm.Commands.MaxAckPending,
			FetchBatch:    cfg.Balda.Swarm.Commands.FetchBatch,
			FetchWait:     strings.TrimSpace(cfg.Balda.Swarm.Commands.FetchWait),
		},
		Events: swarm.EventStreamConfig{Stream: strings.TrimSpace(cfg.Balda.Swarm.Events.Stream)},
		DLQ:    swarm.DLQConfig{Stream: strings.TrimSpace(cfg.Balda.Swarm.DLQ.Stream)},
	}
	swarmConfig, err = swarmConfig.Normalized()
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	eventBusConfig, err := cfg.Balda.NATS.Normalized()
	if err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	if err := validateRuntimeConfigLint(swarmConfig, inboundWebhookConfig); err != nil {
		return fx.Module("balda", fx.Error(err))
	}
	if err := validateZulipConfig(cfg.Balda.Zulip); err != nil {
		return fx.Module("balda", fx.Error(err))
	}

	// Start with global MCP servers.
	mcpServers := make(map[string]agentconfig.MCPServerConfig, len(normaCfg.MCPServers))
	for k, v := range normaCfg.MCPServers {
		mcpServers[k] = v
	}
	mcpReg := mcpregistry.New(mcpServers)

	return fx.Module("balda",
		fx.Supply(
			tgbotkitCfg,
			logger,
			normaCfg,
			workingDir,
			mcpReg,
			scheduledTaskSchedulerConfig,
			inboundWebhookConfig,
			eventBusConfig,
			swarmConfig,
		),
		fx.Provide(
			fx.Annotate(
				func() string { return stateDir },
				fx.ResultTags(`name:"balda_state_dir"`),
			),
			func() *memory.Store {
				return memory.NewStore(stateDir, cfg.Balda.Memory.Enabled)
			},
		),
		fx.Provide(
			func(lc fx.Lifecycle) (baldastate.Provider, error) {
				provider, err := openBaldaStateProvider(context.Background(), stateDir)
				if err != nil {
					return nil, err
				}
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						return provider.Close()
					},
				})
				return provider, nil
			},
			func(provider baldastate.Provider) updatepoller.OffsetStore {
				return provider.PollingOffsetStore()
			},
			func(provider baldastate.Provider) sessionmcp.Store {
				return provider.SessionMCPKV()
			},
		),
		fx.Provide(
			fx.Annotate(
				func(provider baldastate.Provider) adksession.Service {
					if sessionPersistence == sessionPersistenceSQLite {
						return provider.RuntimeSessions()
					}
					return adksession.InMemoryService()
				},
				fx.ResultTags(`name:"balda_runtime_session_service"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() bool {
					return sessionPersistence == sessionPersistenceSQLite
				},
				fx.ResultTags(`name:"balda_sessions_persistent"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() (bool, error) {
					mode, enabled, err := resolveWorkspaceEnabledForApp(
						context.Background(),
						cfg.Balda.Workspace.Mode,
						workingDir,
						cfg.Balda.Workspace.BaseBranch,
						git.Available,
					)
					if err != nil {
						return false, err
					}
					logger.Info().
						Str("workspace_mode", string(mode)).
						Bool("workspace_enabled", enabled).
						Str("working_dir", workingDir).
						Str("state_dir", stateDir).
						Msg("balda workspace mode resolved")
					return enabled, nil
				},
				fx.ResultTags(`name:"balda_workspace_enabled"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string { return workspaceSessionsDir },
				fx.ResultTags(`name:"balda_workspace_sessions_dir"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func(p workspaceBaseBranchParams) (string, error) {
					baseBranch, source, err := resolveWorkspaceBaseBranch(
						context.Background(),
						workingDir,
						cfg.Balda.Workspace.BaseBranch,
						p.WorkspaceEnabled,
					)
					if err != nil {
						return "", err
					}
					logger.Info().
						Str("workspace_base_branch", baseBranch).
						Str("workspace_base_branch_source", source).
						Bool("workspace_enabled", p.WorkspaceEnabled).
						Msg("balda workspace base branch resolved")
					return baseBranch, nil
				},
				fx.ResultTags(`name:"balda_workspace_base_branch"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() []string { return append([]string(nil), cfg.Balda.MCPServers...) },
				fx.ResultTags(`name:"balda_mcp_servers"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() []string {
					if len(normaCfg.MCPServers) == 0 {
						return nil
					}
					ids := make([]string, 0, len(normaCfg.MCPServers))
					for id := range normaCfg.MCPServers {
						trimmed := strings.TrimSpace(id)
						if trimmed == "" {
							continue
						}
						ids = append(ids, trimmed)
					}
					sort.Strings(ids)
					return ids
				},
				fx.ResultTags(`name:"balda_runtime_mcp_server_ids"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string {
					return strings.TrimSpace(cfg.Balda.GlobalInstruction)
				},
				fx.ResultTags(`name:"balda_global_instruction"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string {
					return formattingMode
				},
				fx.ResultTags(`name:"balda_telegram_formatting_mode"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() bool {
					return cfg.Balda.Telegram.PlanUpdates
				},
				fx.ResultTags(`name:"balda_telegram_plan_updates"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() bool {
					return strings.TrimSpace(cfg.Balda.Telegram.Token) != ""
				},
				fx.ResultTags(`name:"balda_telegram_enabled"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() int {
					return cfg.Balda.Goal.MaxIterations
				},
				fx.ResultTags(`name:"balda_goal_max_iterations"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string { return strings.TrimSpace(ownerToken) },
				fx.ResultTags(`name:"balda_auth_token"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string {
					return cfg.Balda.Provider
				},
				fx.ResultTags(`name:"balda_provider"`),
			),
		),
		// Zulip transport
		fx.Provide(func() *baldazulip.Client {
			return baldazulip.NewClient(
				cfg.Balda.Zulip.ServerURL,
				cfg.Balda.Zulip.BotEmail,
				cfg.Balda.Zulip.APIKey,
			)
		}),
		fx.Provide(
			fx.Annotate(
				func() bool { return cfg.Balda.Zulip.Webhook.Enabled },
				fx.ResultTags(`name:"balda_zulip_webhook_enabled"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string { return strings.TrimSpace(cfg.Balda.Zulip.Webhook.ListenAddr) },
				fx.ResultTags(`name:"balda_zulip_listen_addr"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string { return strings.TrimSpace(cfg.Balda.Zulip.Webhook.Path) },
				fx.ResultTags(`name:"balda_zulip_webhook_path"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() string { return strings.TrimSpace(cfg.Balda.Zulip.WebhookToken) },
				fx.ResultTags(`name:"balda_zulip_webhook_token"`),
			),
		),
		fx.Provide(
			fx.Annotate(
				func() []string { return normalizedZulipAllowedOwners(cfg.Balda.Zulip.AllowedOwners) },
				fx.ResultTags(`name:"balda_zulip_allowed_owners"`),
			),
		),
		fx.Provide(func(provider baldastate.Provider) (*auth.OwnerStore, error) {
			return auth.NewOwnerStore(provider.AppKV())
		}),
		fx.Provide(func(provider baldastate.Provider) (*auth.InviteStore, error) {
			return auth.NewInviteStore(provider.AppKV())
		}),
		fx.Provide(func(provider baldastate.Provider) *auth.CollaboratorStore {
			// Wrap the state.CollaboratorStore interface in *auth.CollaboratorStore
			// The wrapper delegates to the underlying store implementation
			return auth.NewCollaboratorStore(provider.Collaborators())
		}),
		fx.Provide(func(reg *mcpregistry.MapRegistry) *agentfactory.Factory {
			return agentfactory.New(
				normaCfg.Providers,
				reg,
				agentfactory.WithPermissionHandler(baldaagent.DefaultPermissionHandler),
			)
		}),
		tgbotkit.Module,
		natsbus.Module,
		swarm.Module,
		actors.Module,
		handlers.Module,
		fx.Provide(
			handlers.NewInternalMCPManager,
		),
		// Start Balda provider runtime and bot runtime only after bundled internal MCP is started.
		fx.Invoke(func(lc fx.Lifecycle, bot *runtime.Bot, runtimeManager *baldaagent.RuntimeManager, mcpManager *handlers.InternalMCPManager) {
			telegramEnabled := strings.TrimSpace(cfg.Balda.Telegram.Token) != ""
			runCtx, cancel := context.WithCancel(context.Background())
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					if err := mcpManager.EnsureStarted(ctx); err != nil {
						return fmt.Errorf("start bundled internal MCP servers: %w", err)
					}
					if err := runtimeManager.EnsureRuntime(ctx); err != nil {
						return fmt.Errorf("start Balda provider runtime: %w", err)
					}
					if telegramEnabled {
						go func() {
							if err := bot.Run(runCtx); err != nil {
								if shutdown.IsExpected(err) {
									bot.Logger().Debugf("bot run stopped during shutdown: %v", err)
									return
								}
								bot.Logger().Errorf("bot run failed: %v", err)
							}
						}()
					}
					return nil
				},
				OnStop: func(ctx context.Context) error {
					cancel()
					return nil
				},
			})
		}),
	)
}

func normalizedZulipAllowedOwners(owners []string) []string {
	out := make([]string, 0, len(owners))
	for _, owner := range owners {
		trimmed := strings.TrimSpace(owner)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func validateBaldaMCPConfiguration(normaCfg runtimeconfig.RuntimeConfig) error {
	errs := make([]string, 0)

	for id := range normaCfg.MCPServers {
		if id == bundledBaldaMCPServerID {
			errs = append(errs, `runtime.mcp_servers.balda is reserved for the built-in balda MCP server`)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return fmt.Errorf("invalid balda MCP configuration: %s", strings.Join(errs, "; "))
}

func openBaldaStateProvider(ctx context.Context, stateDir string) (baldastate.Provider, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create balda state dir: %w", err)
	}
	provider, err := baldastate.NewSQLiteProvider(ctx, paths.StateDBPath(stateDir))
	if err != nil {
		return nil, fmt.Errorf("open balda state provider: %w", err)
	}
	return provider, nil
}

func resolveWorkspaceSessionsDir(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultWorkspaceSessionsDirName, nil
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") || filepath.IsAbs(trimmed) {
		return "", fmt.Errorf("invalid balda.workspace.sessions_dir %q: expected a single directory name", raw)
	}
	if !configIdentifierPattern.MatchString(trimmed) {
		return "", fmt.Errorf("invalid balda.workspace.sessions_dir %q: must match %q", raw, configIdentifierPattern.String())
	}
	return trimmed, nil
}

func resolveWorkspaceBaseBranch(
	ctx context.Context,
	workingDir string,
	configuredBranch string,
	workspaceEnabled bool,
) (branch string, source string, err error) {
	configured := strings.TrimSpace(configuredBranch)
	if !workspaceEnabled {
		if configured == "" {
			return "", "disabled", nil
		}
		return configured, "config", nil
	}

	if configured != "" {
		ref := "refs/heads/" + configured
		if err := git.GitRunCmdErr(ctx, workingDir, "git", "show-ref", "--verify", "--quiet", ref); err == nil {
			return configured, "config", nil
		}
	}

	headBranch, err := git.CurrentBranch(ctx, workingDir)
	if err != nil {
		return "", "", fmt.Errorf("resolve balda.workspace.base_branch: %w", err)
	}
	return headBranch, "head", nil
}

func resolveWorkspaceEnabledForApp(
	ctx context.Context,
	modeRaw string,
	workingDir string,
	configuredBaseBranch string,
	isGitRepo func(context.Context, string) bool,
) (WorkspaceMode, bool, error) {
	mode, enabled, err := ResolveWorkspaceEnabled(ctx, modeRaw, workingDir, isGitRepo)
	if err != nil {
		return "", false, err
	}

	// In auto mode, keep startup safe: if base branch can't be resolved yet
	// (for example unborn HEAD), run without git workspaces.
	if mode == WorkspaceModeAuto && enabled {
		if _, _, err := resolveWorkspaceBaseBranch(ctx, workingDir, configuredBaseBranch, true); err != nil {
			return mode, false, nil
		}
	}

	return mode, enabled, nil
}

func validateSessionPersistence(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		mode = sessionPersistenceSQLite
	}
	switch mode {
	case sessionPersistenceMemory, sessionPersistenceSQLite:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid balda.sessions.persistence %q: supported values are %q and %q", raw, sessionPersistenceMemory, sessionPersistenceSQLite)
	}
}

func validateRuntimeConfigLint(swarmCfg swarm.Config, webhookCfg handlers.InboundWebhookConfig) error {
	errs := make([]string, 0)

	streamNames := map[string]string{
		"balda.swarm.commands.stream": swarmCfg.Commands.Stream,
		"balda.swarm.events.stream":   swarmCfg.Events.Stream,
		"balda.swarm.dlq.stream":      swarmCfg.DLQ.Stream,
	}
	for field, value := range streamNames {
		if err := validateIdentifierValue(field, value); err != nil {
			errs = append(errs, err.Error())
		}
	}
	seenStreamNames := make(map[string]struct{}, len(streamNames))
	for _, value := range streamNames {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if _, ok := seenStreamNames[normalized]; ok {
			errs = append(errs, "balda.swarm.commands.stream, balda.swarm.events.stream, and balda.swarm.dlq.stream must be distinct")
			break
		}
		seenStreamNames[normalized] = struct{}{}
	}

	consumerNames := map[string]string{
		"balda.swarm.commands.consumer": swarmCfg.Commands.Consumer,
		"balda.swarm.events.consumer":   swarm.DefaultEventProjectorConsumer,
	}
	for field, value := range consumerNames {
		if err := validateIdentifierValue(field, value); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if strings.EqualFold(strings.TrimSpace(swarmCfg.Commands.Consumer), strings.TrimSpace(swarm.DefaultEventProjectorConsumer)) {
		errs = append(errs, "balda.swarm.commands.consumer must differ from balda.swarm.events.consumer")
	}

	if webhookCfg.Enabled {
		host := strings.TrimSpace(webhookCfg.ListenAddr)
		publicListenAddress := false
		if host != "" {
			if parsedHost, _, err := net.SplitHostPort(host); err == nil {
				host = strings.TrimSpace(parsedHost)
			}
			host = strings.Trim(host, "[]")
			switch {
			case host == "", host == "0.0.0.0", host == "::":
				publicListenAddress = true
			case strings.EqualFold(host, "localhost"):
				publicListenAddress = false
			default:
				ip := net.ParseIP(host)
				publicListenAddress = ip == nil || !ip.IsLoopback()
			}
		}
		if publicListenAddress {
			unsecuredRoutes := make([]string, 0)
			for routeName, route := range webhookCfg.Routes {
				authType := strings.ToLower(strings.TrimSpace(route.Auth.Type))
				authValue := strings.TrimSpace(route.Auth.Value)
				if authType == "header" && authValue != "" {
					continue
				}
				unsecuredRoutes = append(unsecuredRoutes, routeName)
			}
			if len(unsecuredRoutes) > 0 {
				sort.Strings(unsecuredRoutes)
				errs = append(errs, fmt.Sprintf(
					"balda.webhooks.listen_addr %q is publicly reachable; routes %s must configure auth.type=header with a non-empty auth value",
					webhookCfg.ListenAddr,
					strings.Join(unsecuredRoutes, ", "),
				))
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid runtime configuration: %s", strings.Join(errs, "; "))
}

func validateZulipConfig(cfg ZulipConfig) error {
	if !cfg.Webhook.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.WebhookToken) == "" {
		return fmt.Errorf("balda.zulip.webhook_token is required when Zulip webhook is enabled")
	}
	if path := strings.TrimSpace(cfg.Webhook.Path); path != "" && !strings.HasPrefix(path, "/") {
		return fmt.Errorf("balda.zulip.webhook.path must start with /")
	}
	if err := baldazulip.ValidateConfig(cfg.ServerURL, cfg.BotEmail, cfg.APIKey); err != nil {
		return err
	}
	return nil
}

func validateIdentifierValue(field, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !configIdentifierPattern.MatchString(trimmed) {
		return fmt.Errorf("%s must match %q", field, configIdentifierPattern.String())
	}
	return nil
}

func buildInboundWebhookConfig(cfg BaldaConfig) handlers.InboundWebhookConfig {
	routes := make(map[string]handlers.InboundWebhookRouteConfig, len(cfg.Webhooks.Routes))
	for routeName, route := range cfg.Webhooks.Routes {
		var reportTo *handlers.InboundWebhookRouteTargetConfig
		if route.Envelope.ReportTo != nil {
			reportTo = &handlers.InboundWebhookRouteTargetConfig{
				Target: strings.TrimSpace(route.Envelope.ReportTo.Target),
				Key:    strings.TrimSpace(route.Envelope.ReportTo.Key),
			}
		}
		authValue := strings.TrimSpace(route.Auth.Value)
		if authValue == "" {
			if envKey := strings.TrimSpace(route.Auth.SecretEnv); envKey != "" {
				authValue = strings.TrimSpace(os.Getenv(envKey))
			}
		}
		routes[strings.TrimSpace(routeName)] = handlers.InboundWebhookRouteConfig{
			Path:           strings.TrimSpace(route.Path),
			PromptTemplate: strings.TrimSpace(route.PromptTemplate),
			Envelope: handlers.InboundWebhookRouteEnvelopeConfig{
				Target:   strings.TrimSpace(route.Envelope.Target),
				Key:      strings.TrimSpace(route.Envelope.Key),
				Mode:     strings.TrimSpace(route.Envelope.Mode),
				ReportTo: reportTo,
			},
			Auth: handlers.InboundWebhookRouteAuthConfig{
				Type:   strings.TrimSpace(route.Auth.Type),
				Header: strings.TrimSpace(route.Auth.Header),
				Value:  authValue,
			},
			Dedupe: handlers.InboundWebhookRouteDedupeConfig{
				Source: strings.TrimSpace(route.Dedupe.Source),
				Header: strings.TrimSpace(route.Dedupe.Header),
			},
		}
	}

	return handlers.InboundWebhookConfig{
		Enabled:    cfg.Webhooks.Enabled,
		ListenAddr: strings.TrimSpace(cfg.Webhooks.ListenAddr),
		Routes:     routes,
	}
}
