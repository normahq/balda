package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/internal/throttle"
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

// baldaAuthorizer wraps OwnerStore and CollaboratorStore for auth.CanAccess.
type baldaAuthorizer struct {
	ownerStore        *auth.OwnerStore
	collaboratorStore *auth.CollaboratorStore
}

const (
	ownerSessionLabel                = "balda"
	autoSessionLabel                 = "auto"
	telegramProgressThrottleInterval = 4 * time.Second
)

func (a *baldaAuthorizer) IsOwner(userID int64) bool {
	return a.ownerStore.IsOwner(userID)
}

func (a *baldaAuthorizer) IsCollaborator(userID int64) bool {
	collab, found, err := a.collaboratorStore.GetCollaborator(context.Background(), fmt.Sprintf("%d", userID))
	if err != nil || !found {
		return false
	}
	return collab != nil
}

// BaldaHandler handles bidirectional messages between the owner and agent.
type BaldaHandler struct {
	ownerStore         *auth.OwnerStore
	collaboratorStore  *auth.CollaboratorStore
	channel            *baldatelegram.Adapter
	sessionManager     *baldasession.Manager
	turnDispatcher     turnQueue
	swarmCoordinator   *swarm.Coordinator
	messenger          *messenger.Messenger
	tgClient           client.ClientWithResponsesInterface
	authToken          string
	baldaProviderName  string
	planUpdatesEnabled bool
	logger             zerolog.Logger
	authorizer         auth.Authorizer

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
	TurnDispatcher     *TurnDispatcher
	SwarmCoordinator   *swarm.Coordinator
	Messenger          *messenger.Messenger
	TGClient           client.ClientWithResponsesInterface
	AuthToken          string `name:"balda_auth_token"`
	BaldaProviderID    string `name:"balda_provider"`
	PlanUpdatesEnabled bool   `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
	InternalMCPManager *InternalMCPManager `optional:"true"`
}

func NewBaldaHandler(deps baldaHandlerDeps) (*BaldaHandler, error) {
	h := &BaldaHandler{
		ownerStore:         deps.OwnerStore,
		collaboratorStore:  deps.CollaboratorStore,
		channel:            deps.Channel,
		sessionManager:     deps.SessionManager,
		turnDispatcher:     deps.TurnDispatcher,
		swarmCoordinator:   deps.SwarmCoordinator,
		messenger:          deps.Messenger,
		tgClient:           deps.TGClient,
		authToken:          strings.TrimSpace(deps.AuthToken),
		baldaProviderName:  strings.TrimSpace(deps.BaldaProviderID),
		planUpdatesEnabled: deps.PlanUpdatesEnabled,
		logger:             deps.Logger.With().Str("component", "balda.handler").Logger(),
	}
	h.authorizer = &baldaAuthorizer{ownerStore: deps.OwnerStore, collaboratorStore: deps.CollaboratorStore}

	deps.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return h.onStart(ctx)
		},
	})

	return h, nil
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
	if auth.CanAccess(h.authorizer, messageCtx.UserID, auth.ScopeCollaborator) != auth.Allow {
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
			_ = h.channel.SendPlain(ctx, locator, "Balda provider is not configured (`balda.provider`). Please close this chat and restart balda.")
			return nil
		}
		ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
			Locator: locator,
			UserID:  transportUserID,
		}, ownerSessionLabel)
		if err != nil {
			log.Error().Err(err).Str("agent", baldaProviderName).Msg("failed to ensure owner session")
			_ = h.channel.SendPlain(ctx, locator, fmt.Sprintf("Failed to start owner session: %v.\n\nPlease close this chat and start again.", err))
			return nil
		}
		if sendOwnerWelcome {
			metadata := h.sessionManager.GetAgentMetadata(baldaProviderName)
			welcomeMsg := BuildAgentWelcomeMessage(ownerSessionLabel, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
			_ = h.channel.SendMarkdown(ctx, locator, welcomeMsg)
			h.sendSessionStartupNotice(ctx, locator, ts.GetSessionID())
		}
	} else {
		ts, err = h.sessionManager.GetSession(locator)
		if err != nil {
			_ = h.channel.SendPlain(ctx, locator, "Restoring agent session...")
			ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
				Locator:                    locator,
				UserID:                     transportUserID,
				AllowBaldaProviderFallback: false,
			})
			if err != nil {
				if errors.Is(err, baldasession.ErrNoPersistedSession) {
					baldaProviderName := h.getProviderName()
					if baldaProviderName == "" {
						_ = h.channel.SendPlain(ctx, locator, "Balda provider is not configured (`balda.provider`). Please close this chat and restart balda.")
						return nil
					}
					ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
						Locator: locator,
						UserID:  transportUserID,
					}, autoSessionLabel)
					if err != nil {
						log.Error().Err(err).Str("agent", baldaProviderName).Int("topic_id", topicID).Msg("failed to create session")
						_ = h.channel.SendPlain(ctx, locator, fmt.Sprintf("Failed to start session: %v.\n\nPlease close this chat topic and create a new session with /topic <name>.", err))
						return nil
					}
				} else {
					log.Warn().Err(err).Int("topic_id", topicID).Msg("failed to restore session")
					_ = h.channel.SendPlain(ctx, locator, fmt.Sprintf("Failed to restore this session: %v.\n\nPlease close this chat topic and create a new session with /topic <name>.", err))
					return nil
				}
			}
			if ts != nil {
				baldaProviderID := h.getProviderName()
				metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)
				welcomeName := h.welcomeDisplayName(messageCtx, ts)
				welcomeMsg := BuildAgentWelcomeMessage(welcomeName, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
				_ = h.channel.SendMarkdown(ctx, locator, welcomeMsg)
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
		messageCtx.ProgressPolicy,
	); err != nil {
		if swarm.IsCommandQueueFull(err) {
			_ = h.channel.SendPlain(ctx, locator, "Session command queue is full. Please wait or use /cancel.")
			return nil
		}

		log.Error().Err(err).Int("topic_id", topicID).Msg("failed to publish balda session command")
		_ = h.channel.SendPlain(ctx, locator, "Failed to publish your message for processing. Please try again.")
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
	progressPolicy baldachannel.ProgressPolicy,
) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}

	_, err := h.submitSessionTurn(ctx, sessionTurnPayload{
		Text:           text,
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		MessageID:      messageID,
		TopicID:        topicID,
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
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	topicID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	if !deliver {
		progressPolicy = baldachannel.ProgressPolicy{}
	}
	err := h.runTurnWithDelivery(ctx, text, r, userID, sessionID, agentSessionID, locator, messageID, progressPolicy, deliver)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		h.logger.Info().
			Str("session_id", sessionID).
			Int("topic_id", topicID).
			Msg("balda turn canceled")
		return nil
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
	errText := fmt.Sprintf("Agent execution failed: %v.\n\nPlease close this chat and start a new session.", err)
	if topicID > 0 {
		errText = fmt.Sprintf("Agent execution failed: %v.\n\nPlease close this chat topic and create a new session with /topic <name>.", err)
	}
	if sendErr := h.channel.SendPlain(context.Background(), locator, errText); sendErr != nil {
		log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda error message")
	}
	return err
}

func (h *BaldaHandler) onForumTopicLifecycle(_ context.Context, event *events.MessageEvent) error {
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
		if h.turnDispatcher != nil {
			if _, _, err := h.turnDispatcher.CancelSession(lifecycle.Locator, true); err != nil {
				h.logger.Warn().Err(err).Int64("chat_id", chatID).Int("topic_id", topicID).Msg("failed to cancel closed topic session turns")
			}
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

func (h *BaldaHandler) runTurn(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	progressPolicy baldachannel.ProgressPolicy,
) error {
	return h.runTurnWithDelivery(ctx, text, r, userID, sessionID, agentSessionID, locator, messageID, progressPolicy, true)
}

func (h *BaldaHandler) runTurnWithDelivery(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	progressPolicy baldachannel.ProgressPolicy,
	deliver bool,
) error {
	if strings.TrimSpace(agentSessionID) == "" {
		agentSessionID = sessionID
	}

	address, ok, err := baldatelegram.DecodeLocator(locator)
	if err != nil {
		return fmt.Errorf("decode telegram locator: %w", err)
	}
	if !ok {
		return fmt.Errorf("unsupported channel type %q", locator.ChannelType)
	}

	chatID := address.ChatID
	topicID := address.TopicID
	userContent := genai.NewContentFromText(text, genai.RoleUser)
	draftID := messageID + 1

	runCtx := zerolog.Ctx(ctx).With().
		Int64("chat_id", chatID).
		Int("topic_id", topicID).
		Str("session_id", sessionID).
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
					if sendErr := h.channel.SendTyping(ctx, locator); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send typing chat action")
					}
				})
			}
			if hasPlanUpdate && planProgressText != "" && planProgressText != lastPlanProgressText {
				switch {
				case progressPolicy.Thinking:
					if sendErr := h.channel.SendDraftPlain(ctx, locator, draftID, planProgressText); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send plan update draft")
					} else {
						lastPlanProgressText = planProgressText
						planDraftActive = true
					}
				default:
					if sendErr := h.channel.SendPlain(ctx, locator, planProgressText); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send plan update message")
					} else {
						lastPlanProgressText = planProgressText
					}
				}
			}
			if progressPolicy.Thinking && !planDraftActive {
				thinkingThrottle.Do(func() {
					if sendErr := h.channel.SendDraftPlain(ctx, locator, draftID, thinkingStages[thinkingIdx%len(thinkingStages)]); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send thinking draft")
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
			Msg("received ACP event")
		if ev.TurnComplete {
			sawTurnComplete = true
			responseText := streamedText.String()
			responseEmitted := false
			responseSource := "none"
			handledEmptyTerminalReason := false
			if !deliver {
				responseSource = "fire_and_forget"
			} else if strings.TrimSpace(responseText) != "" {
				if sendErr := h.channel.SendAgentReply(ctx, locator, responseText); sendErr != nil {
					log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda response")
				} else {
					responseEmitted = true
					responseSource = "streamed_text"
				}
			} else if terminalMessage := emptyTerminalTurnMessage(terminalFinishReason, terminalErrorMessage); terminalMessage != "" {
				if sendErr := h.channel.SendPlain(ctx, locator, terminalMessage); sendErr != nil {
					log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda terminal finish reason message")
				} else {
					responseEmitted = true
					responseSource = "finish_reason"
					handledEmptyTerminalReason = true
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
			Msg("ACP stream ended without turn complete; suppressing balda response")
	}

	return nil
}

func emptyTerminalTurnMessage(finishReason genai.FinishReason, errorMessage string) string {
	baseMessage := emptyTerminalTurnBaseMessage(finishReason)
	if baseMessage == "" {
		return ""
	}
	if excerpt := providerMessageExcerpt(errorMessage); excerpt != "" {
		return fmt.Sprintf("%s\n\nProvider message: %s", baseMessage, excerpt)
	}
	return baseMessage
}

func emptyTerminalTurnBaseMessage(finishReason genai.FinishReason) string {
	switch finishReason {
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
	case genai.FinishReasonStop,
		genai.FinishReasonOther,
		genai.FinishReasonUnspecified:
		return "The provider ended the turn without a usable reply. Please try again."
	default:
		return "The provider ended the turn without a usable reply. Please try again."
	}
}

func providerMessageExcerpt(message string) string {
	normalized := strings.Join(strings.Fields(message), " ")
	if normalized == "" {
		return ""
	}
	runes := []rune(normalized)
	if len(runes) > 300 {
		return string(runes[:300])
	}
	return normalized
}
