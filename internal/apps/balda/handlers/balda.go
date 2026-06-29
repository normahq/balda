package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/normahq/balda/internal/throttle"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
	"go.uber.org/fx"
	"google.golang.org/adk/agent"
	"google.golang.org/adk/runner"
	"google.golang.org/genai"
)

const (
	ownerSessionLabel                = "balda"
	autoSessionLabel                 = "auto"
	telegramProgressThrottleInterval = 4 * time.Second
)

// BaldaHandler handles bidirectional session messages for the owner and
// collaborators.
type BaldaHandler struct {
	ownerStore         *auth.OwnerStore
	collaboratorStore  *auth.CollaboratorStore
	channel            *baldatelegram.Adapter
	sessionManager     *baldasession.Manager
	turnDispatcher     actors.TurnQueue
	actorDispatcher    actortransport.Dispatcher
	taskService        *swarm.TaskService
	messenger          *messenger.Messenger
	tgClient           client.ClientWithResponsesInterface
	authToken          string
	baldaProviderName  string
	planUpdatesEnabled bool
	telegramEnabled    bool
	telegramConfigured bool
	logger             zerolog.Logger

	mu          sync.RWMutex
	ownerID     int64
	chatID      int64
	botUsername string
	botUserID   int64
	now         func() time.Time
}

type baldaHandlerDeps struct {
	fx.In

	LC                 fx.Lifecycle
	OwnerStore         *auth.OwnerStore
	CollaboratorStore  *auth.CollaboratorStore
	Channel            *baldatelegram.Adapter
	SessionManager     *baldasession.Manager
	TurnDispatcher     *actors.TurnDispatcher
	ActorDispatcher    actortransport.Dispatcher
	TaskService        *swarm.TaskService `optional:"true"`
	Messenger          *messenger.Messenger
	TGClient           client.ClientWithResponsesInterface
	AuthToken          string `name:"balda_auth_token"`
	BaldaProviderID    string `name:"balda_provider"`
	PlanUpdatesEnabled bool   `name:"balda_telegram_plan_updates"`
	TelegramEnabled    bool   `name:"balda_telegram_enabled"`
	Logger             zerolog.Logger
	InternalMCPManager *InternalMCPManager `optional:"true"`
}

// Register registers the handler with the registry.
func (h *BaldaHandler) Register(registry tgbotkit.Registry) {
	registry.OnMessage(h.onMessage)
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
	); err != nil {
		if swarm.IsCommandQueueFull(err) {
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
) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}

	_, err := h.submitPromptTurnTask(ctx, actors.SessionTurnPayload{
		Text:           text,
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		MessageID:      messageID,
		TopicID:        topicID,
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

func (h *BaldaHandler) runTurnTaskWithDelivery(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	taskID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	return h.runTurnTaskWithDeliveryOptions(ctx, text, r, userID, sessionID, taskID, agentSessionID, locator, messageID, topicID, deliveryfmt.Options{ProgressPolicy: progressPolicy}, deliver)
}

func (h *BaldaHandler) runTurnTaskWithDeliveryOptions(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	taskID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	deliveryOptions deliveryfmt.Options,
	deliver bool,
) error {
	if !deliver {
		deliveryOptions.ProgressPolicy = deliveryfmt.ProgressPolicy{}
	}
	err := h.runTurnWithDeliveryOptions(ctx, text, r, userID, sessionID, taskID, agentSessionID, locator, messageID, deliveryOptions, deliver)
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
	taskID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	return h.runTurnWithDeliveryOptions(ctx, text, r, userID, sessionID, taskID, agentSessionID, locator, messageID, deliveryfmt.Options{ProgressPolicy: progressPolicy}, deliver)
}

func (h *BaldaHandler) runTurnWithDeliveryOptions(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	taskID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	deliveryOptions deliveryfmt.Options,
	deliver bool,
) error {
	if r == nil {
		return fmt.Errorf("session turn: no runner in session %s", sessionID)
	}
	if strings.TrimSpace(agentSessionID) == "" {
		agentSessionID = sessionID
	}

	topicID := 0
	if address, ok, err := baldatelegram.DecodeLocator(locator); err == nil && ok {
		topicID = address.TopicID
	}

	userContent := genai.NewContentFromText(text, genai.RoleUser)
	draftID := messageID + 1
	taskBackedDelivery := deliver && strings.TrimSpace(taskID) != "" && h.actorDispatcher != nil
	deliveryOptions = deliveryfmt.NormalizeOptions(deliveryOptions)
	progressPolicy := deliveryOptions.ProgressPolicy
	deliveryProfile := deliveryOptions.Profile

	runCtx := zerolog.Ctx(ctx).With().
		Str("channel_type", locator.ChannelType).
		Str("address_key", locator.AddressKey).
		Int("topic_id", topicID).
		Str("session_id", sessionID).
		Str("task_id", strings.TrimSpace(taskID)).
		Str("agent_session_id", agentSessionID).
		Str("transport_user_id", userID).
		Logger().
		WithContext(ctx)

	var streamedText strings.Builder
	sawTurnComplete := false
	var terminalFinishReason genai.FinishReason
	terminalErrorCode := ""
	terminalErrorMessage := ""
	thinkingStages := []string{"Thinking.", "Thinking..", "Thinking..."}
	thinkingIdx := 0
	typingThrottle := throttle.New(telegramProgressThrottleInterval, throttle.WithClock(h.currentTime))
	thinkingThrottle := throttle.New(telegramProgressThrottleInterval, throttle.WithClock(h.currentTime))
	lastPlanProgressText := ""
	planDraftActive := false
	deliverySeq := 0

	for ev, err := range r.Run(runCtx, userID, agentSessionID, userContent, agent.RunConfig{}) {
		if err != nil {
			return fmt.Errorf("agent run: %w", err)
		}
		if ev == nil {
			continue
		}
		if finishReason := strings.TrimSpace(string(ev.FinishReason)); finishReason != "" {
			terminalFinishReason = ev.FinishReason
		}
		if errorCode := strings.TrimSpace(ev.ErrorCode); errorCode != "" {
			terminalErrorCode = errorCode
		}
		if errorMessage := strings.TrimSpace(ev.ErrorMessage); errorMessage != "" {
			terminalErrorMessage = errorMessage
		}
		planProgressText := ""
		hasPlanUpdate := false
		if h.planUpdatesEnabled {
			planProgressText, hasPlanUpdate = baldaPlanProgressText(ev)
		}
		if !ev.TurnComplete {
			if progressPolicy.Typing {
				typingThrottle.Do(func() {
					if sendErr := sendTyping(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send typing chat action")
					}
				})
			}
			if taskBackedDelivery {
				if hasPlanUpdate && planProgressText != "" && planProgressText != lastPlanProgressText {
					deliverySeq++
					if err := h.dispatchTaskDelivery(ctx, taskID, locator, sessionID, deliveryProfile, planProgressText, fmt.Sprintf("progress:plan:%03d", deliverySeq)); err != nil {
						return err
					}
					if err := h.appendTaskEvent(ctx, taskID, swarm.TaskEventAgentProgress, "session.actor", "", map[string]any{
						"kind": "plan",
						"text": strings.TrimSpace(planProgressText),
					}); err != nil {
						return err
					}
					lastPlanProgressText = planProgressText
				}
			}
			if !taskBackedDelivery && hasPlanUpdate && planProgressText != "" && planProgressText != lastPlanProgressText {
				switch {
				case progressPolicy.Thinking:
					if sendErr := sendDraftPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, draftID, planProgressText); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send plan update placeholder")
					} else {
						lastPlanProgressText = planProgressText
						planDraftActive = true
					}
				default:
					if sendErr := sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, planProgressText); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send plan update message")
					} else {
						lastPlanProgressText = planProgressText
					}
				}
			}
			if !taskBackedDelivery && progressPolicy.Thinking && !planDraftActive {
				thinkingThrottle.Do(func() {
					if sendErr := sendDraftPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, draftID, thinkingStages[thinkingIdx%len(thinkingStages)]); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send thinking placeholder")
					}
					thinkingIdx++
				})
			}
		}
		contentRole := ""
		partCount := 0
		thoughtPartCount := 0
		textPartCount := 0
		textCharCount := 0
		functionCallPartCount := 0
		functionResponsePartCount := 0
		executableCodePartCount := 0
		codeExecutionResultPartCount := 0
		fileDataPartCount := 0
		inlineDataPartCount := 0
		var eventTextBuilder strings.Builder
		if ev.Content != nil {
			contentRole = ev.Content.Role
			partCount = len(ev.Content.Parts)
			for _, part := range ev.Content.Parts {
				if part == nil {
					continue
				}
				if part.Thought {
					thoughtPartCount++
					continue
				}
				if part.Text != "" {
					textPartCount++
					textCharCount += len(part.Text)
					eventTextBuilder.WriteString(part.Text)
				}
				if part.FunctionCall != nil {
					functionCallPartCount++
				}
				if part.FunctionResponse != nil {
					functionResponsePartCount++
				}
				if part.ExecutableCode != nil {
					executableCodePartCount++
				}
				if part.CodeExecutionResult != nil {
					codeExecutionResultPartCount++
				}
				if part.FileData != nil {
					fileDataPartCount++
				}
				if part.InlineData != nil {
					inlineDataPartCount++
				}
			}
		}
		eventText := eventTextBuilder.String()
		if eventText != "" && ev.IsFinalResponse() {
			currentText := streamedText.String()
			if eventText != currentText {
				streamedText.WriteString(eventText)
			}
		}
		zerolog.Ctx(runCtx).Debug().
			Str("event_id", ev.ID).
			Str("event_invocation_id", ev.InvocationID).
			Str("event_author", ev.Author).
			Str("event_branch", ev.Branch).
			Bool("partial", ev.Partial).
			Bool("interrupted", ev.Interrupted).
			Bool("turn_complete", ev.TurnComplete).
			Bool("has_content", ev.Content != nil).
			Str("content_role", contentRole).
			Int("part_count", partCount).
			Int("thought_part_count", thoughtPartCount).
			Int("text_part_count", textPartCount).
			Int("text_char_count", textCharCount).
			Int("function_call_part_count", functionCallPartCount).
			Int("function_response_part_count", functionResponsePartCount).
			Int("executable_code_part_count", executableCodePartCount).
			Int("code_execution_result_part_count", codeExecutionResultPartCount).
			Int("file_data_part_count", fileDataPartCount).
			Int("inline_data_part_count", inlineDataPartCount).
			Str("error_code", strings.TrimSpace(ev.ErrorCode)).
			Bool("has_error_message", strings.TrimSpace(ev.ErrorMessage) != "").
			Interface("finish_reason", ev.FinishReason).
			Int("custom_metadata_count", len(ev.CustomMetadata)).
			Int("long_running_tool_ids_count", len(ev.LongRunningToolIDs)).
			Int("state_delta_count", len(ev.Actions.StateDelta)).
			Bool("has_plan_update", hasPlanUpdate).
			Int("plan_progress_char_count", len(planProgressText)).
			Int("artifact_delta_count", len(ev.Actions.ArtifactDelta)).
			Int("requested_tool_confirmations_count", len(ev.Actions.RequestedToolConfirmations)).
			Bool("skip_summarization", ev.Actions.SkipSummarization).
			Str("transfer_to_agent", strings.TrimSpace(ev.Actions.TransferToAgent)).
			Bool("escalate", ev.Actions.Escalate).
			Bool("final_response", ev.IsFinalResponse()).
			Int("streamed_text_char_count", streamedText.Len()).
			Msg("received provider event")
		if ev.TurnComplete {
			sawTurnComplete = true
			responseText := streamedText.String()
			responseEmitted := false
			responseSource := "none"
			handledEmptyTerminalReason := false
			switch {
			case !deliver:
				responseSource = "fire_and_forget"
			case strings.TrimSpace(responseText) != "":
				if taskBackedDelivery {
					if err := h.dispatchTaskDelivery(ctx, taskID, locator, sessionID, deliveryProfile, responseText, "final"); err != nil {
						return err
					}
					if err := h.appendTaskEvent(ctx, taskID, swarm.TaskEventAgentResult, "session.actor", "", map[string]any{
						"text": strings.TrimSpace(responseText),
					}); err != nil {
						return err
					}
					responseEmitted = true
					responseSource = "streamed_text"
				} else if sendErr := sendAgentReplyWithProfile(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, deliveryProfile, responseText); sendErr != nil {
					log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda response")
				} else {
					responseEmitted = true
					responseSource = "streamed_text"
				}
			default:
				terminalMessage := terminalErrorTurnMessage(terminalErrorMessage)
				if terminalMessage == "" {
					terminalMessage = terminalTurnMessage(terminalFinishReason)
				}
				if terminalMessage != "" {
					if taskBackedDelivery {
						if err := h.dispatchTaskDelivery(ctx, taskID, locator, sessionID, deliveryProfile, terminalMessage, "terminal"); err != nil {
							return err
						}
						if err := h.appendTaskEvent(ctx, taskID, swarm.TaskEventAgentResult, "session.actor", "", map[string]any{
							"text":          strings.TrimSpace(terminalMessage),
							"finish_reason": strings.TrimSpace(string(terminalFinishReason)),
						}); err != nil {
							return err
						}
						responseEmitted = true
						responseSource = "finish_reason"
						handledEmptyTerminalReason = true
					} else if sendErr := sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, terminalMessage); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda terminal finish reason message")
					} else {
						responseEmitted = true
						responseSource = "finish_reason"
						handledEmptyTerminalReason = true
					}
				}
			}
			zerolog.Ctx(runCtx).Debug().
				Str("response_source", responseSource).
				Bool("response_emitted_on_turn_complete", responseEmitted).
				Interface("terminal_finish_reason", terminalFinishReason).
				Str("terminal_error_code", terminalErrorCode).
				Bool("terminal_has_error_message", terminalErrorMessage != "").
				Bool("handled_empty_terminal_reason", handledEmptyTerminalReason).
				Msg("processed turn complete event")
			break
		}
	}
	if !sawTurnComplete {
		zerolog.Ctx(runCtx).Warn().
			Int("streamed_text_char_count", streamedText.Len()).
			Msg("provider event stream ended without turn complete; suppressing balda response")
	}

	return nil
}

func terminalErrorTurnMessage(errorMessage string) string {
	errorMessage = strings.TrimSpace(errorMessage)
	if errorMessage == "" {
		return ""
	}
	return "Provider error: " + errorMessage
}

func terminalTurnMessage(reason genai.FinishReason) string {
	switch reason {
	case genai.FinishReasonMaxTokens:
		return "The provider hit the output limit before producing a visible reply. Ask for a shorter answer or split the request."
	case genai.FinishReasonSafety:
		return "The provider blocked this turn for safety reasons. Please rephrase and try again."
	case genai.FinishReasonRecitation:
		return "The provider blocked this turn because it may reproduce protected source material. Please rephrase and try again."
	case genai.FinishReasonLanguage:
		return "The provider could not answer because the request used an unsupported language. Please rephrase in a supported language and try again."
	case genai.FinishReasonBlocklist:
		return "The provider blocked this turn because it matched restricted terms. Please rephrase and try again."
	case genai.FinishReasonProhibitedContent:
		return "The provider rejected this turn as prohibited content. Please rephrase and try again."
	case genai.FinishReasonSPII:
		return "The provider blocked this turn because it may contain sensitive personal information. Please remove that information and try again."
	case genai.FinishReasonMalformedFunctionCall:
		return "The provider ended the turn with an invalid function call. Please try again."
	case genai.FinishReasonUnexpectedToolCall:
		return "The provider ended the turn with an unexpected tool call. Please try again."
	case genai.FinishReasonImageSafety:
		return "The provider blocked image generation for safety reasons. Please try a different request."
	case genai.FinishReasonImageProhibitedContent:
		return "The provider rejected image generation as prohibited content. Please try a different request."
	case genai.FinishReasonNoImage:
		return "The provider completed the turn without returning an image. Please try a different request."
	case genai.FinishReasonImageRecitation:
		return "The provider blocked image generation because it may reproduce protected source material. Please try a different request."
	case genai.FinishReasonImageOther:
		return "The provider ended image generation without a usable result. Please try again."
	case genai.FinishReasonStop, genai.FinishReasonOther, genai.FinishReasonUnspecified:
		return "The provider ended the turn without a usable reply. Please try again."
	default:
		return "The provider ended the turn without a usable reply. Please try again."
	}
}

func (h *BaldaHandler) dispatchTaskDelivery(
	ctx context.Context,
	taskID string,
	locator baldasession.SessionLocator,
	sessionID string,
	profile deliveryfmt.Profile,
	text string,
	dedupeSuffix string,
) error {
	if h == nil || h.actorDispatcher == nil {
		return fmt.Errorf("swarm runtime is unavailable")
	}
	env, err := actors.AgentReplyDeliveryEnvelopeWithProfile(taskID, actorlayer.ActorAddress{Target: swarm.ActorTypeSession, Key: sessionID}, locator, profile, text, dedupeSuffix)
	if err != nil {
		return err
	}
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	return err
}

func (h *BaldaHandler) appendTaskEvent(
	ctx context.Context,
	taskID string,
	eventType string,
	actor string,
	messageID string,
	payload any,
) error {
	if h == nil || h.taskService == nil || strings.TrimSpace(taskID) == "" {
		return nil
	}
	return h.taskService.AppendEvent(ctx, taskID, eventType, actor, messageID, payload)
}
