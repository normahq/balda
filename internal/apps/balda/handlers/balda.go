package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/appports"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/sessionturnapp"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
	"go.uber.org/fx"
	"google.golang.org/adk/v2/runner"
)

const (
	ownerSessionLabel = "balda"
	autoSessionLabel  = "auto"
)

type jobEventAppender interface {
	AppendEvent(ctx context.Context, jobID string, eventType string, actor string, messageID string, payload any) error
}

// BaldaHandler handles bidirectional session messages for the owner and
// collaborators.
type BaldaHandler struct {
	ownerStore         *auth.OwnerStore
	collaboratorStore  *auth.CollaboratorStore
	channel            *baldatelegram.Adapter
	sessionManager     *baldasession.Manager
	turnDispatcher     appports.TurnQueue
	actorDispatcher    actortransport.Dispatcher
	jobEvents          jobEventAppender
	messenger          *messenger.Messenger
	tgClient           client.ClientWithResponsesInterface
	authToken          string
	baldaProviderName  string
	telegramEnabled    bool
	telegramConfigured bool
	logger             zerolog.Logger
	outboundFrom       actorlayer.ActorAddress
	progressEmitter    sessionturnapp.SessionProgressEmitter
	turnExecution      *sessionturnapp.TurnExecutionService
	questionService    *questions.Service

	mu          sync.RWMutex
	ownerID     int64
	chatID      int64
	botUsername string
	botUserID   int64
	now         func() time.Time
}

type baldaHandlerDeps struct {
	fx.In

	OwnerStore        *auth.OwnerStore
	CollaboratorStore *auth.CollaboratorStore
	Channel           *baldatelegram.Adapter
	SessionManager    *baldasession.Manager
	TurnDispatcher    appports.TurnQueue
	Dispatcher        actortransport.Dispatcher
	JobEvents         *baldajobs.JobEventsService `optional:"true"`
	Messenger         *messenger.Messenger
	TGClient          client.ClientWithResponsesInterface
	AuthToken         string `name:"balda_auth_token"`
	BaldaProviderID   string `name:"balda_provider"`
	TelegramEnabled   bool   `name:"balda_telegram_enabled"`
	Logger            zerolog.Logger
	TurnExecution     *sessionturnapp.TurnExecutionService
	QuestionService   *questions.Service `optional:"true"`
}

// Start validates the Telegram identity and bootstraps owner state.
func (h *BaldaHandler) Start(ctx context.Context) error {
	return h.onStart(ctx)
}

// Register registers the handler with the registry.
func (h *BaldaHandler) Register(registry tgbotkit.Registry) {
	registry.OnMessage(h.onMessage)
	registry.OnCallbackDataPrefix(baldatelegram.QuestionCallbackPrefix, h.onQuestionCallback)
	registry.OnMessageType(messagetype.ForumTopicCreated, h.onForumTopicLifecycle)
	registry.OnMessageType(messagetype.ForumTopicEdited, h.onForumTopicLifecycle)
	registry.OnMessageType(messagetype.ForumTopicClosed, h.onForumTopicLifecycle)
	registry.OnMessageType(messagetype.ForumTopicReopened, h.onForumTopicLifecycle)
}

func (h *BaldaHandler) onMessage(ctx context.Context, event *events.MessageEvent) error {
	messageCtx, ok := h.channel.MessageContextFromEvent(event)
	if !ok {
		return nil
	}

	h.logger.Debug().
		Str("message_type", string(event.Type)).
		Interface("raw_transport_message", event.Message).
		Msg("received inbound telegram transport message")

	ownerID := h.getOwnerID()
	chatID := h.getChatID()

	if ownerID == 0 {
		return nil
	}

	// RBAC check: owner or collaborator
	if !h.canAccessCollaboratorScope(ctx, messageCtx.UserID) {
		return nil // Silent drop for unknown users
	}

	if chatID == 0 {
		h.setChatID(messageCtx.ChatID)
		log.Info().Int64("chat_id", messageCtx.ChatID).Msg("Chat ID set from message")
	}

	if messageCtx.HasCommand {
		return nil
	}

	if handled, err := h.handleQuestionReply(ctx, messageCtx); err != nil {
		h.logger.Warn().Err(err).Str("session_id", messageCtx.Locator.SessionID).Msg("failed to handle question reply")
		_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, messageCtx.Locator, "Could not process this reply right now. Please try again.")
		return nil
	} else if handled {
		return nil
	}

	topicID := messageCtx.TopicID
	var text string
	if messageCtx.IsDM {
		text = h.normalizeDMText(messageCtx)
	} else {
		normalized, ok := h.normalizePublicText(messageCtx)
		if !ok {
			return nil
		}
		text = normalized
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}

	locator := messageCtx.Locator
	transportUserID := baldatelegram.UserID(messageCtx.UserID)

	log.Info().Int64("user_id", ownerID).Int("topic_id", topicID).Msg("Forwarding message to balda agent")

	var ts *baldasession.TopicSession
	var err error

	if messageCtx.IsDM && topicID == 0 {
		existingSession, _ := h.sessionManager.GetSession(locator)
		sendOwnerWelcome := existingSession == nil
		baldaProviderName := h.getProviderName()
		if baldaProviderName == "" {
			_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Balda is not ready right now. Please close this chat and try again.")
			return nil
		}
		ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
			Locator: locator,
			UserID:  transportUserID,
		}, ownerSessionLabel)
		if err != nil {
			log.Error().Err(err).Str("agent", baldaProviderName).Msg("failed to ensure main dm session")
			_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Could not start this session. Please close this chat and try again.")
			return nil
		}
		if sendOwnerWelcome {
			metadata := h.sessionManager.GetAgentMetadata(baldaProviderName)
			welcomeMsg := welcome.BuildAgentWelcomeMessage(ownerSessionLabel, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
			_ = sendMarkdown(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, welcomeMsg)
			h.sendSessionStartupNotice(ctx, locator, ts.GetSessionID())
		}
	} else {
		ts, err = h.sessionManager.GetSession(locator)
		if err != nil {
			_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Restoring agent session...")
			ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
				Locator:                    locator,
				UserID:                     transportUserID,
				AllowBaldaProviderFallback: false,
			})
			if err != nil {
				if errors.Is(err, baldasession.ErrNoPersistedSession) {
					baldaProviderName := h.getProviderName()
					if baldaProviderName == "" {
						_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Balda is not ready right now. Please close this chat topic and try again.")
						return nil
					}
					ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
						Locator: locator,
						UserID:  transportUserID,
					}, autoSessionLabel)
					if err != nil {
						log.Error().Err(err).Str("agent", baldaProviderName).Int("topic_id", topicID).Msg("failed to create session")
						_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Could not start this session. Please close this chat topic and create a new one.")
						return nil
					}
				} else {
					log.Warn().Err(err).Int("topic_id", topicID).Msg("failed to restore session")
					_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Could not restore this session. Please close this chat topic and create a new one.")
					return nil
				}
			}
			if ts != nil {
				baldaProviderID := h.getProviderName()
				metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)
				welcomeName := h.welcomeDisplayName(messageCtx, ts)
				welcomeMsg := welcome.BuildAgentWelcomeMessage(welcomeName, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
				_ = sendMarkdown(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, welcomeMsg)
				h.sendSessionStartupNotice(ctx, locator, ts.GetSessionID())
			}
		}
	}

	if err := h.enqueueTurn(
		ctx,
		text,
		ts,
		locator,
		messageCtx.MessageID,
		topicID,
		messageCtx.DeliveryOptions,
		messageCtx.ProgressPolicy,
		baldatelegram.UserID(messageCtx.UserID),
	); err != nil {
		if baldaexecution.IsCommandQueueFull(err) {
			_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Session command queue is full. Please wait or use /cancel.")
			return nil
		}

		log.Error().Err(err).Int("topic_id", topicID).Msg("failed to publish balda session command")
		_ = sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, "Failed to publish your message for processing. Please try again.")
	}

	return nil
}

func (h *BaldaHandler) enqueueTurn(
	ctx context.Context,
	text string,
	ts *baldasession.TopicSession,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	deliveryOptions deliveryfmt.Options,
	progressPolicy baldachannel.ProgressPolicy,
	requesterUserID string,
) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}

	_, err := h.submitSessionTurn(ctx, turncmd.SessionTurnPayload{
		Text:            text,
		Locator:         locator,
		UserID:          ts.GetUserID(),
		RequesterUserID: strings.TrimSpace(requesterUserID),
		AgentSessionID:  ts.GetAgentSessionID(),
		MessageID:       messageID,
		TopicID:         topicID,
		DeliveryOptions: deliveryfmt.Options{
			Profile:        deliveryOptions.Profile,
			ProgressPolicy: progressPolicy,
		},
		ProgressPolicy: progressPolicy,
		Deliver:        true,
		Source:         "telegram",
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *BaldaHandler) outboundActorAddress(sessionID string) actorlayer.ActorAddress {
	if h != nil && strings.TrimSpace(h.outboundFrom.Target) != "" {
		return h.outboundFrom
	}
	return baldaHandlerActorAddress
}

func (h *BaldaHandler) runTurnJobWithDelivery(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	jobID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	return h.runTurnJobWithDeliveryOptions(ctx, text, r, userID, sessionID, jobID, agentSessionID, locator, messageID, topicID, deliveryfmt.Options{ProgressPolicy: progressPolicy}, deliver)
}

func (h *BaldaHandler) runTurnJobWithDeliveryOptions(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	jobID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	deliveryOptions deliveryfmt.Options,
	deliver bool,
	runOpts ...runner.RunOption,
) error {
	if !deliver {
		deliveryOptions.ProgressPolicy = deliveryfmt.ProgressPolicy{}
	}
	err := h.runTurnWithDeliveryOptions(ctx, text, r, userID, sessionID, jobID, agentSessionID, locator, messageID, deliveryOptions, deliver, runOpts...)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		h.logger.Info().
			Str("session_id", sessionID).
			Int("topic_id", topicID).
			Msg("balda turn canceled")
		return err
	}
	if _, getErr := h.sessionManager.GetSession(locator); getErr != nil {
		h.logger.Debug().
			Str("session_id", sessionID).
			Int("topic_id", topicID).
			Msg("suppressing balda turn error for inactive session")
		return nil
	}
	if !deliver {
		h.logger.Warn().
			Err(err).
			Str("session_id", sessionID).
			Int("topic_id", topicID).
			Msg("fire-and-forget balda turn failed")
		return err
	}

	log.Error().Err(err).Int("topic_id", topicID).Msg("agent execution failed")
	errText := "Agent execution failed. Use /reset or /restart to restart this session."
	if sendErr := sendPlain(context.Background(), h.actorDispatcher, baldaHandlerActorAddress, locator, errText); sendErr != nil {
		log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda error message")
	}
	return err
}

func (h *BaldaHandler) onForumTopicLifecycle(ctx context.Context, event *events.MessageEvent) error {
	lifecycle, ok := h.channel.TopicLifecycleFromEvent(event)
	if !ok {
		return nil
	}

	chatID := lifecycle.ChatID
	boundChatID := h.getChatID()
	if boundChatID != 0 && chatID != boundChatID {
		return nil
	}

	topicID := lifecycle.TopicID
	if topicID <= 0 {
		h.logger.Debug().
			Int64("chat_id", chatID).
			Str("event_type", string(lifecycle.Type)).
			Msg("ignoring forum topic lifecycle event without topic id")
		return nil
	}

	evt := h.logger.Info().
		Int64("chat_id", chatID).
		Int("topic_id", topicID).
		Int("message_id", lifecycle.MessageID).
		Str("event_type", string(lifecycle.Type))
	if lifecycle.UserID != 0 {
		evt = evt.Int64("user_id", lifecycle.UserID)
	}

	switch lifecycle.Type {
	case messagetype.ForumTopicCreated:
		evt.Msg("forum topic created")
	case messagetype.ForumTopicEdited:
		evt.Msg("forum topic edited")
	case messagetype.ForumTopicClosed:
		evt.Msg("forum topic closed")
		if err := submitSessionCancelControl(ctx, h.actorDispatcher, lifecycle.Locator, "system", "session canceled because forum topic was closed", false); err != nil {
			h.logger.Warn().Err(err).Int64("chat_id", chatID).Int("topic_id", topicID).Msg("failed to publish forum-topic-close cancel control command")
		}
		if h.sessionManager != nil {
			h.sessionManager.StopSession(lifecycle.Locator)
		}
	case messagetype.ForumTopicReopened:
		evt.Msg("forum topic reopened")
	default:
		evt.Msg("forum topic lifecycle event")
	}

	return nil
}

func (h *BaldaHandler) canAccessCollaboratorScope(ctx context.Context, userID int64) bool {
	if h.ownerStore.IsOwner(userID) {
		return true
	}

	collab, found, err := h.collaboratorStore.GetCollaborator(ctx, fmt.Sprintf("%d", userID))
	if err != nil || !found {
		return false
	}
	return collab != nil
}

func (h *BaldaHandler) runTurnWithDelivery(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	jobID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	return h.runTurnWithDeliveryOptions(ctx, text, r, userID, sessionID, jobID, agentSessionID, locator, messageID, deliveryfmt.Options{ProgressPolicy: progressPolicy}, deliver)
}

func (h *BaldaHandler) runTurnWithDeliveryOptions(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	jobID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	deliveryOptions deliveryfmt.Options,
	deliver bool,
	runOpts ...runner.RunOption,
) error {
	execution := h.turnExecution
	if execution == nil {
		return fmt.Errorf("turn execution service is not configured")
	}
	return execution.Execute(ctx, sessionturnapp.ExecutionRequest{
		Text:            text,
		Runner:          r,
		UserID:          userID,
		SessionID:       sessionID,
		JobID:           jobID,
		AgentSessionID:  agentSessionID,
		Locator:         locator,
		MessageID:       messageID,
		DeliveryOptions: deliveryOptions,
		Deliver:         deliver,
		ProgressEmitter: h.progressEmitter,
		OutboundFrom:    h.outboundActorAddress(sessionID),
		RunOptions:      runOpts,
	})
}
