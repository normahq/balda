package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/appports"
	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/normahq/balda/internal/apps/balda/automode"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/locatorref"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/rs/zerolog/log"
	"github.com/tgbotkit/runtime/events"
	"go.uber.org/fx"
)

type commandSessionManager interface {
	CreateSession(ctx context.Context, sessionCtx session.SessionContext, agentName string) error
	GetAgentMetadata(agentName string) session.AgentMetadata
	GetSessionInfo(ctx context.Context, sessionID string) (session.TopicSessionInfo, error)
	RuntimeStateValue(ctx context.Context, locator session.SessionLocator, key string) (any, bool, error)
	UpdateRuntimeState(ctx context.Context, locator session.SessionLocator, state map[string]any) error
	BaldaProviderID() string
	ResetSession(ctx context.Context, locator session.SessionLocator) error
	TakeStartupNotice(sessionID string) string
}

type goalJobService interface {
	ListActiveGoalJobsBySession(ctx context.Context, sessionID string) ([]baldastate.JobRecord, error)
}

type sessionWorkCanceller interface {
	CancelWork(ctx context.Context, locator session.SessionLocator, actor string, reason string) error
}

const (
	commandStart    = "start"
	commandTopic    = "topic"
	commandLocator  = "locator"
	commandCancel   = "cancel"
	commandGoal     = "goal"
	commandUser     = "user"
	commandUsage    = "usage"
	commandAuto     = "auto"
	commandReset    = "reset"
	commandRestart  = "restart"
	commandClose    = "close"
	chatTypePrivate = "private"

	userActionAdd    = "add"
	userActionInvite = "invite"
	userActionList   = "list"
	userActionRemove = "remove"
)

// CommandHandler handles Balda chat commands such as /topic, /goal, /reset,
// /restart, /locator, /close, /cancel, and /user.
type CommandHandler struct {
	ownerStore        *auth.OwnerStore
	collaboratorStore *auth.CollaboratorStore
	channel           *baldatelegram.Adapter
	sessionManager    commandSessionManager
	workCanceller     sessionWorkCanceller
	actorDispatcher   actortransport.Dispatcher
	jobService        goalJobService
	goalMaxIterations int
	userHandler       *userHandler
}

type commandHandlerParams struct {
	fx.In

	OwnerStore        *auth.OwnerStore
	CollaboratorStore *auth.CollaboratorStore
	Channel           *baldatelegram.Adapter
	SessionManager    *session.Manager
	WorkCanceller     appports.SessionWorkCanceller `optional:"true"`
	Dispatcher        actortransport.Dispatcher
	GoalJobs          *baldajobs.JobLifecycleService `optional:"true"`
	MaxIterations     int                            `name:"balda_goal_max_iterations"`
	UserHandler       *userHandler
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
	case commandTopic:
		return h.onTopicCommand(ctx, commandCtx)
	case commandReset, commandRestart:
		return h.onResetCommand(ctx, commandCtx)
	case commandLocator:
		return h.onLocatorCommand(ctx, commandCtx)
	case commandClose:
		return h.onCloseCommand(ctx, commandCtx)
	case commandCancel:
		return h.onCancelCommand(ctx, commandCtx)
	case commandGoal:
		return h.onGoalCommand(ctx, commandCtx)
	case commandUsage:
		return h.onUsageCommand(ctx, commandCtx)
	case commandAuto:
		return h.onAutoCommand(ctx, commandCtx)
	case commandUser:
		// Route to UserHandler
		return h.userHandler.HandleUserCommand(ctx, commandCtx)
	default:
		return nil
	}
}

func (h *CommandHandler) onAutoCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	arg := strings.ToLower(strings.TrimSpace(commandCtx.Args))
	switch arg {
	case "":
		status, err := loadAutoStatus(ctx, h.sessionManager, commandCtx.Locator)
		if err != nil {
			return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not read auto mode status.")
		}
		return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, automode.RenderStatus(status))
	case "on":
		if err := dispatchAutoStateUpdate(ctx, h.actorDispatcher, commandCtx.Locator, automode.EnableState(time.Now())); err != nil {
			return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not enable auto mode.")
		}
		return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, automode.RenderStatus(automode.Normalize(automode.Status{
			Enabled:  true,
			State:    automode.StateIdle,
			MaxTurns: automode.DefaultMaxTurns,
		})))
	case "off":
		if err := dispatchAutoStateUpdate(ctx, h.actorDispatcher, commandCtx.Locator, automode.DisableState()); err != nil {
			return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not disable auto mode.")
		}
		return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, automode.RenderStatus(automode.DefaultStatus()))
	default:
		return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /auto\n/auto on\n/auto off")
	}
}

func (h *CommandHandler) onUsageCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /usage"); err != nil {
			return err
		}
		return nil
	}
	snapshot, ok, err := loadUsageSnapshot(ctx, h.sessionManager, commandCtx.Locator)
	if err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to load usage snapshot")
	}
	if err != nil || !ok {
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "No provider usage has been recorded for this session yet."); sendErr != nil {
			return sendErr
		}
		return nil
	}
	return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, renderUsageSnapshot(snapshot))
}

func (h *CommandHandler) onGoalCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	objective := strings.TrimSpace(commandCtx.Args)
	if objective == "" {
		if err := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage:\n/goal <objective>\n/goal clear"); err != nil {
			return err
		}
		return nil
	}
	if strings.EqualFold(objective, "clear") {
		if h.actorDispatcher == nil {
			if err := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Goal control is unavailable right now. Please try again."); err != nil {
				return err
			}
			return nil
		}
		if err := submitGoalClearControl(ctx, h.actorDispatcher, commandCtx.Locator, baldatelegram.UserID(commandCtx.UserID), "goal cleared by user", true); err != nil {
			log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to publish goal clear command")
			if sendErr := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not clear goal run."); sendErr != nil {
				return sendErr
			}
		}
		return nil
	}

	started, err := h.submitGoalJobWithOptions(ctx, commandCtx.Locator, commandCtx.DeliveryOptions, objective, baldatelegram.UserID(commandCtx.UserID))
	if err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to start /goal run")
		if sendErr := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not start goal run."); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if !started {
		if err := sendAgentReply(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "A goal run is already active for this session."); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (h *CommandHandler) onResetCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	commandName := commandCtx.Command
	if commandName == "" {
		commandName = commandReset
	}

	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, fmt.Sprintf("Usage: /%s", commandName)); err != nil {
			return err
		}
		return nil
	}

	info, infoErr := h.sessionManager.GetSessionInfo(ctx, commandCtx.Locator.SessionID)
	if infoErr != nil {
		log.Debug().Err(infoErr).Str("session_id", commandCtx.Locator.SessionID).Str("command", commandName).Msg("session info unavailable before restart")
	}
	h.cancelSessionWork(ctx, commandCtx.Locator, fmt.Sprintf("session canceled by %s command", commandName), commandName)
	if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Str("command", commandName).Msg("failed to reset session during session restart command")
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not reset this session."); sendErr != nil {
			return sendErr
		}
		return nil
	}
	label := restartSessionLabel(commandCtx, info)
	userID := restartSessionUserID(commandCtx, info)
	if err := h.sessionManager.CreateSession(ctx, session.SessionContext{
		Locator: commandCtx.Locator,
		UserID:  userID,
	}, label); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Str("command", commandName).Msg("failed to recreate session during session restart command")
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not restart this session."); sendErr != nil {
			return sendErr
		}
		return nil
	}

	baldaProviderID := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)
	welcomeName := restartWelcomeDisplayName(commandCtx, label)
	welcomeMsg := welcome.BuildAgentWelcomeMessage(welcomeName, commandCtx.Locator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers)
	if err := sendMarkdown(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, welcomeMsg); err != nil {
		log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Int("topic_id", commandCtx.TopicID).Str("command", commandName).Msg("failed to send restart welcome")
	}
	h.sendSessionStartupNotice(ctx, commandCtx.Locator, commandCtx.Locator.SessionID)
	return nil
}

func (h *CommandHandler) cancelSessionWork(ctx context.Context, locator session.SessionLocator, reason string, commandName string) {
	if h.workCanceller == nil {
		return
	}
	if err := h.workCanceller.CancelWork(ctx, locator, "command."+commandName, reason); err != nil {
		log.Warn().Err(err).Str("session_id", locator.SessionID).Str("command", commandName).Msg("failed to synchronously cancel session work")
	}
}

func (h *CommandHandler) sendSessionStartupNotice(ctx context.Context, locator session.SessionLocator, sessionID string) {
	if h.sessionManager == nil {
		return
	}
	notice := strings.TrimSpace(h.sessionManager.TakeStartupNotice(sessionID))
	if notice == "" {
		return
	}
	if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, locator, notice); err != nil {
		log.Warn().Err(err).Str("session_id", sessionID).Msg("failed to send restart startup notice")
	}
}

func restartSessionLabel(commandCtx baldatelegram.CommandContext, info session.TopicSessionInfo) string {
	if label := strings.TrimSpace(info.AgentName); label != "" {
		return label
	}
	if commandCtx.IsDM && commandCtx.TopicID == 0 {
		return ownerSessionLabel
	}
	return autoSessionLabel
}

func restartSessionUserID(commandCtx baldatelegram.CommandContext, info session.TopicSessionInfo) string {
	if userID := strings.TrimSpace(info.UserID); userID != "" {
		return userID
	}
	return baldatelegram.UserID(commandCtx.UserID)
}

func restartWelcomeDisplayName(commandCtx baldatelegram.CommandContext, label string) string {
	if !commandCtx.IsDM {
		return ownerSessionLabel
	}
	return label
}

func (h *CommandHandler) onTopicCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if !commandCtx.IsDM {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "This command is only available in direct messages."); err != nil {
			return err
		}
		return nil
	}

	topicName := strings.TrimSpace(commandCtx.Args)
	if topicName == "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /topic <name>"); err != nil {
			return err
		}
		return nil
	}
	baldaProviderID := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	if baldaProviderID == "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Balda is not ready right now."); err != nil {
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
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not create topic session."); sendErr != nil {
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
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not create topic session."); sendErr != nil {
			return sendErr
		}
		return nil
	}

	metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)

	welcomeMsg := welcome.BuildAgentWelcomeMessage(topicName, topicLocator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers)
	if err := sendMarkdown(ctx, h.actorDispatcher, commandHandlerActorAddress, topicLocator, welcomeMsg); err != nil {
		log.Error().Err(err).Msg("failed to send welcome message")
		return err
	}

	return nil
}

func (h *CommandHandler) onLocatorCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /locator"); err != nil {
			return err
		}
		return nil
	}

	ref := locatorref.Format(commandCtx.Locator)
	message := fmt.Sprintf("Transport: %s\nLocator: %s\n\nUse in scheduler/webhook config:\ntarget: locator\nkey: %s", commandCtx.Locator.ChannelType, ref, ref)
	return sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, message)
}

func (h *CommandHandler) onCloseCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if !commandCtx.IsDM {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "This command is only available in direct messages."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /close"); err != nil {
			return err
		}
		return nil
	}

	if commandCtx.TopicID > 0 {
		if err := submitSessionCancelControl(ctx, h.actorDispatcher, commandCtx.Locator, baldatelegram.UserID(commandCtx.UserID), "session canceled by close command", false); err != nil {
			log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to publish /close cancel control command")
		}
		if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
			log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to reset session during /close")
			if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not close this topic."); sendErr != nil {
				return sendErr
			}
			return nil
		}
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Closing this topic and resetting session history."); err != nil {
			log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Int("topic_id", commandCtx.TopicID).Msg("failed to send /close confirmation")
		}
		if err := h.channel.Close(ctx, commandCtx.Locator); err != nil {
			log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Int("topic_id", commandCtx.TopicID).Msg("failed to close topic")
		}
		return nil
	}

	if err := submitSessionCancelControl(ctx, h.actorDispatcher, commandCtx.Locator, baldatelegram.UserID(commandCtx.UserID), "session canceled by close command", false); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to publish /close cancel control command")
	}
	if err := h.sessionManager.ResetSession(ctx, commandCtx.Locator); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to reset main dm session during /close")
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not reset this session."); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Session history reset."); err != nil {
		log.Warn().Err(err).Int64("chat_id", commandCtx.ChatID).Msg("failed to send /close main dm session confirmation")
	}
	return nil
}

func (h *CommandHandler) onCancelCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Only the bot owner or collaborators can use this command."); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(commandCtx.Args) != "" {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Usage: /cancel"); err != nil {
			return err
		}
		return nil
	}

	if h.actorDispatcher == nil {
		if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Cancel is unavailable right now. Please try again."); err != nil {
			return err
		}
		return nil
	}
	if err := submitSessionTurnCancelControl(ctx, h.actorDispatcher, commandCtx.Locator, baldatelegram.UserID(commandCtx.UserID), "session turn canceled by user", true); err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to publish cancel command")
		if sendErr := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Could not request cancel."); sendErr != nil {
			return sendErr
		}
		return nil
	}
	if err := sendPlain(ctx, h.actorDispatcher, commandHandlerActorAddress, commandCtx.Locator, "Cancel requested."); err != nil {
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
