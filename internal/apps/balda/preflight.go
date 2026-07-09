package balda

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfans/fxlogger"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/handlers"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/normahq/balda/internal/apps/sessionmcp"
	"github.com/normahq/balda/internal/git"
	"github.com/normahq/runtime/v2/agentconfig"
	"github.com/normahq/runtime/v2/agentfactory"
	runtimeconfig "github.com/normahq/runtime/v2/appconfig"
	"github.com/normahq/runtime/v2/mcpregistry"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
	adksession "google.golang.org/adk/v2/session"
)

// PreflightRuntime validates Balda app wiring and verifies that the configured
// provider runtime can be started and stopped without launching channel loops.
func PreflightRuntime(
	ctx context.Context,
	cfg Config,
	normaCfg runtimeconfig.RuntimeConfig,
	runtimeLoadOpts runtimeconfig.RuntimeLoadOptions,
) error {
	logger := log.Logger.With().Str("component", "balda.preflight").Logger()
	workingDir, err := paths.ResolveWorkingDir(cfg.Balda.WorkingDir)
	if err != nil {
		return fmt.Errorf("resolve balda working_dir: %w", err)
	}
	if err := validateBaldaMCPConfiguration(normaCfg); err != nil {
		return err
	}
	formattingMode, err := telegramfmt.ValidateMode(cfg.Balda.Telegram.FormattingMode)
	if err != nil {
		return err
	}
	stateDir, err := paths.ResolveStateDir(workingDir, cfg.Balda.StateDir)
	if err != nil {
		return fmt.Errorf("resolve balda state_dir: %w", err)
	}
	sessionPersistence, err := validateSessionPersistence(cfg.Balda.Sessions.Persistence)
	if err != nil {
		return err
	}
	workspaceSessionsDir, err := resolveWorkspaceSessionsDir(cfg.Balda.Workspace.SessionsDir)
	if err != nil {
		return err
	}
	inboundWebhookConfig := buildInboundWebhookConfig(cfg.Balda)
	executionConfig := executionConfigFromBalda(cfg.Balda)
	if err := validateExecutionConfigLint(executionConfig, inboundWebhookConfig); err != nil {
		return err
	}
	if err := validateZulipConfig(cfg.Balda.Zulip); err != nil {
		return err
	}
	if err := validateSlackConfig(cfg.Balda.Slack); err != nil {
		return err
	}

	workspaceMode, workspaceEnabled, err := resolveWorkspaceEnabledForApp(
		ctx,
		cfg.Balda.Workspace.Mode,
		workingDir,
		cfg.Balda.Workspace.BaseBranch,
		git.Available,
	)
	if err != nil {
		return err
	}
	baseBranch, _, err := resolveWorkspaceBaseBranch(ctx, workingDir, cfg.Balda.Workspace.BaseBranch, workspaceEnabled)
	if err != nil {
		return err
	}
	logger.Info().
		Str("workspace_mode", string(workspaceMode)).
		Bool("workspace_enabled", workspaceEnabled).
		Str("workspace_base_branch", baseBranch).
		Msg("balda preflight workspace resolved")

	mcpServers := make(map[string]agentconfig.MCPServerConfig, len(normaCfg.MCPServers))
	for k, v := range normaCfg.MCPServers {
		mcpServers[k] = v
	}
	mcpReg := mcpregistry.New(mcpServers)

	var runtimeManager *baldaagent.RuntimeManager
	var mcpManager *handlers.InternalMCPManager

	app := fx.New(
		fx.WithLogger(
			fxlogger.WithZerolog(
				logger,
			),
		),
		fx.Supply(logger, normaCfg, workingDir, mcpReg),
		fx.Provide(
			fx.Annotate(
				func() string { return stateDir },
				fx.ResultTags(`name:"balda_state_dir"`),
			),
			func(lc fx.Lifecycle) (baldastate.Provider, error) {
				provider, openErr := openBaldaStateProvider(ctx, stateDir)
				if openErr != nil {
					return nil, openErr
				}
				lc.Append(fx.Hook{OnStop: func(context.Context) error { return provider.Close() }})
				return provider, nil
			},
			func(provider baldastate.Provider) *memory.Store {
				return memory.NewStore(provider.AppKV(), stateDir, cfg.Balda.Memory.Enabled)
			},
			func(provider baldastate.Provider) sessionmcp.Store {
				return provider.SessionMCPKV()
			},
			fx.Annotate(
				func(provider baldastate.Provider) adksession.Service {
					if sessionPersistence == sessionPersistenceSQLite {
						return provider.RuntimeSessions()
					}
					return adksession.InMemoryService()
				},
				fx.ResultTags(`name:"balda_runtime_session_service"`),
			),
			fx.Annotate(
				func() bool { return sessionPersistence == sessionPersistenceSQLite },
				fx.ResultTags(`name:"balda_sessions_persistent"`),
			),
			fx.Annotate(
				func() bool { return workspaceEnabled },
				fx.ResultTags(`name:"balda_workspace_enabled"`),
			),
			fx.Annotate(
				func() string { return workspaceSessionsDir },
				fx.ResultTags(`name:"balda_workspace_sessions_dir"`),
			),
			fx.Annotate(
				func() string { return baseBranch },
				fx.ResultTags(`name:"balda_workspace_base_branch"`),
			),
			fx.Annotate(
				func() string { return strings.TrimSpace(cfg.Balda.Provider) },
				fx.ResultTags(`name:"balda_provider"`),
			),
			fx.Annotate(
				func() []string { return append([]string(nil), cfg.Balda.MCPServers...) },
				fx.ResultTags(`name:"balda_mcp_servers"`),
			),
			fx.Annotate(
				func() string { return strings.TrimSpace(cfg.Balda.GlobalInstruction) },
				fx.ResultTags(`name:"balda_global_instruction"`),
			),
			fx.Annotate(
				func() string { return formattingMode },
				fx.ResultTags(`name:"balda_telegram_formatting_mode"`),
			),
			func(reg *mcpregistry.MapRegistry) *agentfactory.Factory {
				return agentfactory.New(
					normaCfg.Providers,
					reg,
					agentfactory.WithPermissionHandler(baldaagent.DefaultPermissionHandler),
				)
			},
			baldaagent.NewBuilder,
			baldaagent.NewRuntimeManager,
			session.NewManager,
			handlers.NewInternalMCPManager,
		),
		fx.Populate(&runtimeManager, &mcpManager),
		fx.Invoke(func(lc fx.Lifecycle, manager *handlers.InternalMCPManager, runtimeManager *baldaagent.RuntimeManager) {
			lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
				if err := manager.EnsureStarted(ctx); err != nil {
					return fmt.Errorf("start bundled internal MCP servers: %w", err)
				}
				if err := runtimeManager.EnsureRuntime(ctx); err != nil {
					return fmt.Errorf("start Balda provider runtime: %w", err)
				}
				return nil
			}})
		}),
	)
	if err := app.Err(); err != nil {
		return err
	}
	if runtimeManager == nil || mcpManager == nil {
		return fmt.Errorf("balda preflight wiring incomplete")
	}
	if err := app.Start(ctx); err != nil {
		return err
	}
	if err := app.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func executionConfigFromBalda(cfg BaldaConfig) baldaexecution.Config {
	executionConfig := baldaexecution.Config{
		Commands: baldaexecution.CommandConfig{
			Stream:        strings.TrimSpace(cfg.Execution.Commands.Stream),
			Consumer:      strings.TrimSpace(cfg.Execution.Commands.Consumer),
			AckWait:       strings.TrimSpace(cfg.Execution.Commands.AckWait),
			MaxDeliver:    cfg.Execution.Commands.MaxDeliver,
			MaxAckPending: cfg.Execution.Commands.MaxAckPending,
			FetchBatch:    cfg.Execution.Commands.FetchBatch,
			FetchWait:     strings.TrimSpace(cfg.Execution.Commands.FetchWait),
		},
		Events: baldaexecution.EventStreamConfig{Stream: strings.TrimSpace(cfg.Execution.Events.Stream)},
		DLQ:    baldaexecution.DLQConfig{Stream: strings.TrimSpace(cfg.Execution.DLQ.Stream)},
	}
	normalized, err := executionConfig.Normalized()
	if err != nil {
		return executionConfig
	}
	return normalized
}
