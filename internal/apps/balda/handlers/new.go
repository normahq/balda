package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	baldawelcome "github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/rs/zerolog/log"
	"github.com/tgbotkit/runtime/events"
	"go.uber.org/fx"
)

type commandSessionManager interface {
	CreateSession(ctx context.Context, sessionCtx session.SessionContext, agentName string) error
	GetAgentMetadata(agentName string) session.AgentMetadata
	BaldaProviderID() string
	ResetSession(ctx context.Context, locator session.SessionLocator) error
	StopSession(locator session.SessionLocator)
}

// CommandHandler handles balda commands like /topic and /close.
type CommandHandler struct {
	ownerStore        *auth.OwnerStore
	collaboratorStore *auth.CollaboratorStore
	channel           *baldatelegram.Adapter
	sessionManager    commandSessionManager
	turnDispatcher    turnQueue
	swarmCoordinator  *swarm.Coordinator
	swarmConfig       swarm.Config
	commandBus        swarm.CommandBusStatusProvider
	agentRegistry     *swarm.AgentRegistry
	tasks             *swarm.TaskService
	taskRuns          *taskRunRegistry
	goalMaxIterations int
	messenger         *messenger.Messenger
	userHandler       *userHandler
	memoryStore       *memory.Store
}

func BuildAgentWelcomeMessage(name, sessionID, agentType, model string, mcpServers []string) string {
	return baldawelcome.BuildAgentWelcomeMessage(name, sessionID, agentType, model, mcpServers)
}

type commandHandlerParams struct {
	fx.In

	OwnerStore        *auth.OwnerStore
	CollaboratorStore *auth.CollaboratorStore
	Channel           *baldatelegram.Adapter
	SessionManager    *session.Manager
	TurnDispatcher    *TurnDispatcher
	SwarmCoordinator  *swarm.Coordinator
	SwarmConfig       swarm.Config
	CommandBus        swarm.CommandBusStatusProvider
	AgentRegistry     *swarm.AgentRegistry
	TaskService       *swarm.TaskService
	TaskRuns          *taskRunRegistry
	MaxIterations     int `name:"balda_goal_max_iterations"`
	Messenger         *messenger.Messenger
	UserHandler       *userHandler
	MemoryStore       *memory.Store
}

// NewCommandHandler creates a new balda command handler.
func NewCommandHandler(params commandHandlerParams) *CommandHandler {
	return &CommandHandler{
		ownerStore:        params.OwnerStore,
		collaboratorStore: params.CollaboratorStore,
		channel:           params.Channel,
		sessionManager:    params.SessionManager,
		turnDispatcher:    params.TurnDispatcher,
		swarmCoordinator:  params.SwarmCoordinator,
		swarmConfig:       params.SwarmConfig,
		commandBus:        params.CommandBus,
		agentRegistry:     params.AgentRegistry,
		tasks:             params.TaskService,
		taskRuns:          params.TaskRuns,
		goalMaxIterations: normalizeGoalMaxIterations(params.MaxIterations),
		messenger:         params.Messenger,
		userHandler:       params.UserHandler,
		memoryStore:       params.MemoryStore,
	}
}

// Register registers the handler with the registry.
func (h *CommandHandler) Register(registry tgbotkit.Registry) {
	registry.OnCommand(h.onCommand)
}

func (h *CommandHandler) onCommand(ctx context.Context, event *events.CommandEvent) error {
	commandCtx, ok := h.channel.CommandContextFromEvent(event)
	if !ok {
		return nil
	}

	switch commandCtx.Command {
	case "topic":
		return h.onTopicCommand(ctx, commandCtx)
	case "close":
		return h.onCloseCommand(ctx, commandCtx)
	case "reset":
		return h.onResetCommand(ctx, commandCtx)
	case "cancel":
		return h.onCancelCommand(ctx, commandCtx)
	case "goal":
		return h.onGoalCommand(ctx, commandCtx)
	case "tasks":
		return h.onTasksCommand(ctx, commandCtx)
	case "task":
		return h.onTaskCommand(ctx, commandCtx)
	case "swarm":
		return h.onSwarmCommand(ctx, commandCtx)
	case "queue":
		return h.onQueueCommand(ctx, commandCtx)
	case "mailbox":
		return h.onMailboxCommand(ctx, commandCtx)
	case "memory":
		return h.onMemoryCommand(ctx, commandCtx)
	case "user":
		// Route to UserHandler
		return h.userHandler.HandleUserCommand(ctx, commandCtx)
	default:
		return nil
	}
}

func (h *CommandHandler) onGoalCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendAgentReply(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	objective := strings.TrimSpace(commandCtx.Args)
	if objective == "" {
		if err := h.channel.SendAgentReply(ctx, commandCtx.Locator, "Usage: /goal <objective>"); err != nil {
			return err
		}
		return nil
	}

	started, err := h.submitGoalTask(ctx, commandCtx.Locator, objective, baldatelegram.UserID(commandCtx.UserID))
	if err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to start /goal run")
		if sendErr := h.channel.SendAgentReply(ctx, commandCtx.Locator, fmt.Sprintf("Failed to start goal run: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if !started {
		if err := h.channel.SendAgentReply(ctx, commandCtx.Locator, "A goal run is already active for this session."); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (h *CommandHandler) onTopicCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if !commandCtx.IsDM {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "This command is only available in direct messages."); err != nil {
			return err
		}
		return nil
	}

	topicName := strings.TrimSpace(commandCtx.Args)
	if topicName == "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /topic <name>"); err != nil {
			return err
		}
		return nil
	}
	baldaProviderID := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	if baldaProviderID == "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "balda.provider is not configured."); err != nil {
			return err
		}
		return nil
	}

	log.Info().
		Int64("user_id", commandCtx.UserID).
		Int64("chat_id", commandCtx.ChatID).
		Str("topic_name", topicName).
		Msg("creating topic session")

	topicLocator, err := h.channel.CreateTopicLocator(ctx, commandCtx.ChatID, fmt.Sprintf("Balda: %s", topicName))
	if err != nil {
		log.Error().Err(err).Str("topic_name", topicName).Msg("failed to create topic")
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to create topic session: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := h.sessionManager.CreateSession(ctx, session.SessionContext{
		Locator: topicLocator,
		UserID:  baldatelegram.UserID(commandCtx.UserID),
	}, topicName); err != nil {
		log.Error().Err(err).Str("topic_name", topicName).Msg("failed to create topic session after topic creation")
		_ = h.channel.Close(ctx, topicLocator)
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to create topic session: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}

	metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)

	welcomeMsg := BuildAgentWelcomeMessage(topicName, topicLocator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers)
	if err := h.channel.SendMarkdown(ctx, topicLocator, welcomeMsg); err != nil {
		log.Error().Err(err).Msg("failed to send welcome message")
		return err
	}

	return nil
}

func (h *CommandHandler) onMemoryCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}
	if !commandCtx.IsDM {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "This command is only available in direct messages."); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /memory"); err != nil {
			return err
		}
		return nil
	}
	if h.memoryStore == nil || !h.memoryStore.MemoryEnabled() {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Memory is disabled."); err != nil {
			return err
		}
		return nil
	}
	content, err := h.memoryStore.ReadMemory(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to read balda memory")
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read memory: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	content = strings.TrimSpace(content)
	if content == "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Memory is empty."); err != nil {
			return err
		}
		return nil
	}
	return h.sendPlainChunks(ctx, commandCtx.Locator, content)
}

func (h *CommandHandler) sendPlainChunks(ctx context.Context, locator session.SessionLocator, text string) error {
	const maxPlainChunkRunes = 3500
	runes := []rune(text)
	for len(runes) > 0 {
		n := len(runes)
		if n > maxPlainChunkRunes {
			n = maxPlainChunkRunes
		}
		chunk := strings.TrimSpace(string(runes[:n]))
		if chunk != "" {
			if err := h.channel.SendPlain(ctx, locator, chunk); err != nil {
				return err
			}
		}
		runes = runes[n:]
	}
	return nil
}

func (h *CommandHandler) onCloseCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if !commandCtx.IsDM {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "This command is only available in direct messages."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /close"); err != nil {
			return err
		}
		return nil
	}

	if commandCtx.TopicID > 0 {
		if h.turnDispatcher != nil {
			_, _, _ = h.turnDispatcher.CancelSession(commandCtx.Locator, true)
		}
		if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
			log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to reset session during /close")
			if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to reset this session before close: %v", err)); sendErr != nil {
				return sendErr
			}
			return nil
		}
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Closing this topic and resetting session history."); err != nil {
			log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Int("topic_id", commandCtx.TopicID).Msg("failed to send /close confirmation")
		}
		if err := h.channel.Close(ctx, commandCtx.Locator); err != nil {
			log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Int("topic_id", commandCtx.TopicID).Msg("failed to close topic")
		}
		return nil
	}

	if h.turnDispatcher != nil {
		_, _, _ = h.turnDispatcher.CancelSession(commandCtx.Locator, true)
	}
	if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to reset owner session during /close")
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to reset this session: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Session history reset. The balda provider session will be recreated on your next message."); err != nil {
		log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Msg("failed to send /close owner session confirmation")
	}
	return nil
}

func (h *CommandHandler) onResetCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /reset"); err != nil {
			return err
		}
		return nil
	}

	if h.turnDispatcher != nil {
		_, _, _ = h.turnDispatcher.CancelSession(commandCtx.Locator, true)
	}
	if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to reset session")
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to reset this session: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Session history reset. Send a new message to start fresh in this chat."); err != nil {
		return err
	}
	return nil
}

func (h *CommandHandler) onCancelCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /cancel"); err != nil {
			return err
		}
		return nil
	}

	if h.swarmCoordinator == nil || !h.swarmCoordinator.RuntimeEnabled() {
		if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Cancel is unavailable right now. Please try again."); err != nil {
			return err
		}
		return nil
	}
	env, err := controlCancelEnvelope(commandCtx.Locator, "", baldatelegram.UserID(commandCtx.UserID), "session canceled by user")
	if err != nil {
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to build cancel request: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if _, err := h.swarmCoordinator.Submit(ctx, env); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to publish cancel command")
		if sendErr := h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to request cancel: %v", err)); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := h.channel.SendPlain(ctx, commandCtx.Locator, "Cancel requested."); err != nil {
		return err
	}
	return nil
}

func (h *CommandHandler) canUseSessionCommand(ctx context.Context, userID int64) bool {
	if h.ownerStore != nil && h.ownerStore.IsOwner(userID) {
		return true
	}
	if h.collaboratorStore == nil {
		return false
	}
	_, found, err := h.collaboratorStore.GetCollaborator(ctx, fmt.Sprintf("%d", userID))
	if err != nil {
		log.Warn().Err(err).Int64("user_id", userID).Msg("failed to check collaborator access")
		return false
	}
	return found
}
