package sessionturnapp

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/automode"
	"github.com/normahq/balda/internal/apps/balda/automodecmd"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
	"github.com/normahq/balda/internal/apps/balda/progress"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/telegramref"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"github.com/normahq/balda/internal/apps/balda/usageview"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/runner"
	adksession "google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

type jobEventAppender interface {
	AppendEvent(ctx context.Context, jobID string, eventType string, actor string, messageID string, payload any) error
}

type runtimeStateReader interface {
	RuntimeStateValue(ctx context.Context, locator baldasession.SessionLocator, key string) (any, bool, error)
}

const (
	responseSourceNone            = "none"
	responseSourceAutoDone        = "auto_done"
	responseSourceAutoWaitForUser = "auto_wait_for_user"
)

type TurnExecutionService struct {
	dispatcher actortransport.Dispatcher
	jobEvents  jobEventAppender
	sessions   runtimeStateReader
	logger     zerolog.Logger
	now        func() time.Time
}

type ExecutionRequest struct {
	Text            string
	Runner          *runner.Runner
	UserID          string
	RequesterUserID string
	SessionID       string
	JobID           string
	AgentSessionID  string
	Locator         baldasession.SessionLocator
	MessageID       int
	DeliveryOptions deliveryfmt.Options
	Deliver         bool
	ProgressEmitter SessionProgressEmitter
	OutboundFrom    actorlayer.ActorAddress
	RunOptions      []runner.RunOption
	TurnSource      string
	DedupeKey       string
}

func NewTurnExecutionService(dispatcher actortransport.Dispatcher, jobEvents *baldajobs.JobEventsService, sessions *baldasession.Manager, logger zerolog.Logger) *TurnExecutionService {
	return NewTurnExecutionServiceWithJobEvents(dispatcher, jobEvents, sessions, logger)
}

func NewTurnExecutionServiceWithJobEvents(dispatcher actortransport.Dispatcher, jobEvents jobEventAppender, sessions runtimeStateReader, logger zerolog.Logger) *TurnExecutionService {
	return &TurnExecutionService{
		dispatcher: dispatcher,
		jobEvents:  jobEvents,
		sessions:   sessions,
		logger:     logger.With().Str("component", "balda.turn_execution").Logger(),
		now:        time.Now,
	}
}

func (s *TurnExecutionService) dispatchJobDelivery(
	ctx context.Context,
	jobID string,
	locator baldasession.SessionLocator,
	sessionID string,
	profile deliveryfmt.Profile,
	text string,
	dedupeSuffix string,
) error {
	if s == nil || s.dispatcher == nil {
		return fmt.Errorf("runtime is unavailable")
	}
	env, err := deliverycmd.AgentReplyEnvelopeWithProfileAndSettlement(jobID, actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: sessionID}, locator, deliverycmd.Profile{
		Format:         deliverycmd.Format(profile.Format),
		TelegramMode:   profile.TelegramMode,
		FormattingMode: profile.FormattingMode,
	}, deliverycmd.SettlementOutbox, text, dedupeSuffix)
	if err != nil {
		return err
	}
	_, err = s.dispatcher.Dispatch(ctx, env)
	return err
}

func (s *TurnExecutionService) appendJobEvent(
	ctx context.Context,
	jobID string,
	eventType string,
	actor string,
	messageID string,
	payload any,
) error {
	if s == nil || s.jobEvents == nil || strings.TrimSpace(jobID) == "" {
		return nil
	}
	return s.jobEvents.AppendEvent(ctx, jobID, eventType, actor, messageID, payload)
}

func (s *TurnExecutionService) Execute(ctx context.Context, req ExecutionRequest) error {
	if req.Runner == nil {
		return fmt.Errorf("session turn: no runner in session %s", req.SessionID)
	}
	if strings.TrimSpace(req.AgentSessionID) == "" {
		req.AgentSessionID = req.SessionID
	}

	topicID := 0
	if address, ok, err := telegramref.DecodeLocator(req.Locator); err == nil && ok {
		topicID = address.TopicID
	}

	userContent := genai.NewContentFromText(req.Text, genai.RoleUser)
	jobBackedDelivery := req.Deliver && strings.TrimSpace(req.JobID) != "" && s.dispatcher != nil
	req.DeliveryOptions = deliveryfmt.NormalizeOptions(req.DeliveryOptions)
	progressPolicy := req.DeliveryOptions.ProgressPolicy
	deliveryProfile := req.DeliveryOptions.Profile

	runCtx := zerolog.Ctx(ctx).With().
		Str("channel_type", req.Locator.ChannelType).
		Str("address_key", req.Locator.AddressKey).
		Int("topic_id", topicID).
		Str("session_id", req.SessionID).
		Str("job_id", strings.TrimSpace(req.JobID)).
		Bool("job_backed_delivery", jobBackedDelivery).
		Str("agent_session_id", req.AgentSessionID).
		Str("transport_user_id", req.UserID).
		Logger().
		WithContext(ctx)
	requesterUserID := strings.TrimSpace(req.RequesterUserID)
	if requesterUserID == "" {
		requesterUserID = strings.TrimSpace(req.UserID)
	}
	permissionOutcomes := &permissionOutcomeRecorder{}
	runCtx = permissioncmd.WithOutcomeSink(runCtx, permissionOutcomes)
	runCtx = permissioncmd.WithInteraction(runCtx, questioncmd.InteractionContext{
		SessionID:   req.SessionID,
		ChannelKind: req.Locator.ChannelType,
		Locator:     req.Locator,
		RequestedBy: questioncmd.UserRef{UserID: requesterUserID},
		Origin:      questioncmd.InteractionOrigin{RootJobID: strings.TrimSpace(req.JobID)},
	})

	progressEmitter := req.ProgressEmitter
	if progressEmitter == nil && s.dispatcher != nil {
		progressEmitter = NewSessionProgressDispatcher(
			s.dispatcher,
			req.OutboundFrom,
			req.Locator,
			req.JobID,
			topicID,
			progressPolicy,
			jobBackedDelivery,
			zerolog.Ctx(runCtx).With().Logger(),
		)
	}

	var streamedText strings.Builder
	sawTurnComplete := false
	var terminalFinishReason genai.FinishReason
	terminalErrorCode := ""
	terminalErrorMessage := ""
	lastNonRetryErrorMessage := ""

	for ev, err := range req.Runner.Run(runCtx, req.UserID, req.AgentSessionID, userContent, agent.RunConfig{}, req.RunOptions...) {
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
			if !looksLikeRetryOnlyProviderError(errorMessage) {
				lastNonRetryErrorMessage = errorMessage
			}
		}
		if snapshot, ok := usageview.SnapshotFromMetadata(ev.UsageMetadata); ok {
			if ev.Actions.StateDelta == nil {
				ev.Actions.StateDelta = make(map[string]any)
			}
			ev.Actions.StateDelta[usageview.UsageStateKey] = map[string]any{
				"prompt_token_count":          snapshot.PromptTokenCount,
				"cached_content_token_count":  snapshot.CachedContentTokenCount,
				"response_token_count":        snapshot.ResponseTokenCount,
				"tool_use_prompt_token_count": snapshot.ToolUsePromptTokenCount,
				"thoughts_token_count":        snapshot.ThoughtsTokenCount,
				"total_token_count":           snapshot.TotalTokenCount,
				"traffic_type":                snapshot.TrafficType,
			}
		}
		planProgress, planProgressText, hasPlanUpdate := baldaPlanProgress(ev)
		reasoningText, hasThoughtUpdate := progress.ReasoningText(ev)
		hasVisibleResponseText := false
		if ev.Content != nil {
			for _, part := range ev.Content.Parts {
				if part == nil || part.Thought {
					continue
				}
				if strings.TrimSpace(part.Text) != "" {
					hasVisibleResponseText = true
					break
				}
			}
		}
		if hasThoughtUpdate || (reasoningText != "" && !hasThoughtUpdate) {
			zerolog.Ctx(runCtx).Debug().
				Bool("has_thought_update", hasThoughtUpdate).
				Int("reasoning_text_char_count", len(reasoningText)).
				Bool("has_visible_response_text", hasVisibleResponseText).
				Msg("provider reasoning candidate")
		}
		if !ev.TurnComplete && progressEmitter != nil {
			result, err := progressEmitter.HandleNonTerminal(ctx, SessionProgressUpdate{
				Plan:                   planProgress,
				PlanProgressText:       planProgressText,
				HasPlanUpdate:          hasPlanUpdate,
				ReasoningText:          reasoningText,
				HasThoughtUpdate:       hasThoughtUpdate,
				HasVisibleResponseText: hasVisibleResponseText,
				VisibleResponseText:    visibleResponseDelta(ev),
			})
			if err != nil {
				return err
			}
			if jobBackedDelivery && result.DispatchedPlanText != "" {
				if err := s.appendJobEvent(ctx, req.JobID, baldajobs.JobEventAgentProgress, "session.actor", "", map[string]any{
					"kind": "plan",
					"text": result.DispatchedPlanText,
				}); err != nil {
					return err
				}
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
					if failure, ok := toolFailureFromFunctionResponse(part.FunctionResponse); ok {
						zerolog.Ctx(runCtx).Warn().
							Str("tool_name", failure.ToolName).
							Str("tool_server", failure.Server).
							Str("tool_status", failure.Status).
							Str("tool_error_code", failure.Code).
							Str("tool_error_message", failure.Message).
							Str("function_name", strings.TrimSpace(part.FunctionResponse.Name)).
							Str("tool_call_id", strings.TrimSpace(part.FunctionResponse.ID)).
							Msg("ADK tool call failed")
					}
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
			Bool("has_thought_update", hasThoughtUpdate).
			Int("reasoning_text_char_count", len(reasoningText)).
			Bool("has_visible_response_text", hasVisibleResponseText).
			Int("streamed_text_char_count", streamedText.Len()).
			Msg("received provider event")
		if ev.TurnComplete {
			sawTurnComplete = true
			responseText := streamedText.String()
			responseEmitted := false
			responseSource := responseSourceNone
			handledEmptyTerminalReason := false
			switch {
			case !req.Deliver:
				responseSource = "fire_and_forget"
			case strings.TrimSpace(responseText) != "":
				if notification, source, ok := autoDecisionNotification(req.TurnSource, responseText); ok {
					responseText = notification
					responseSource = source
					switch source {
					case responseSourceAutoDone:
						if err := s.updateAutoState(ctx, req.Locator, map[string]any{
							automode.StateKeyMode:             automode.StateIdle,
							automode.StateKeyConsecutiveTurns: 0,
							automode.StateKeyLastTurnAt:       s.now().UTC().Format(time.RFC3339),
							automode.StateKeyLastStopReason:   "model_reported_done",
						}); err != nil {
							return err
						}
					case responseSourceAutoWaitForUser:
						if err := s.updateAutoState(ctx, req.Locator, map[string]any{
							automode.StateKeyMode:             automode.StateWaitingForUser,
							automode.StateKeyConsecutiveTurns: 0,
							automode.StateKeyLastTurnAt:       s.now().UTC().Format(time.RFC3339),
							automode.StateKeyLastStopReason:   "model_waiting_for_user",
						}); err != nil {
							return err
						}
					}
				}
				if jobBackedDelivery {
					if err := s.dispatchJobDelivery(ctx, req.JobID, req.Locator, req.SessionID, deliveryProfile, responseText, "final"); err != nil {
						return err
					}
					if err := s.appendJobEvent(ctx, req.JobID, baldajobs.JobEventAgentResult, "session.actor", "", map[string]any{
						"text": strings.TrimSpace(responseText),
					}); err != nil {
						return err
					}
					responseEmitted = true
					if responseSource == responseSourceNone {
						responseSource = "streamed_text"
					}
				} else if sendErr := sendAgentReplyWithProfile(ctx, s.dispatcher, req.OutboundFrom, req.Locator, deliveryProfile, responseText); sendErr != nil {
					log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda response")
				} else {
					responseEmitted = true
					if responseSource == responseSourceNone {
						responseSource = "streamed_text"
					}
				}
			default:
				if terminalErrorMessage == "" {
					terminalErrorMessage = lastNonRetryErrorMessage
				}
				terminalMessage := terminalErrorTurnMessage(terminalErrorMessage)
				terminalSource := "provider_error"
				if terminalMessage == "" {
					terminalMessage = permissionOutcomeTurnMessage(permissionOutcomes.Latest())
					terminalSource = "permission_outcome"
				}
				if terminalMessage == "" {
					terminalMessage = terminalTurnMessage(terminalFinishReason)
					terminalSource = "finish_reason"
				}
				if terminalMessage != "" {
					if jobBackedDelivery {
						if err := s.dispatchJobDelivery(ctx, req.JobID, req.Locator, req.SessionID, deliveryProfile, terminalMessage, "terminal"); err != nil {
							return err
						}
						if err := s.appendJobEvent(ctx, req.JobID, baldajobs.JobEventAgentResult, "session.actor", "", map[string]any{
							"text":          strings.TrimSpace(terminalMessage),
							"finish_reason": strings.TrimSpace(string(terminalFinishReason)),
						}); err != nil {
							return err
						}
						responseEmitted = true
						responseSource = terminalSource
						handledEmptyTerminalReason = true
					} else if sendErr := sendPlain(ctx, s.dispatcher, req.OutboundFrom, req.Locator, terminalMessage); sendErr != nil {
						log.Warn().Err(sendErr).Int("topic_id", topicID).Msg("failed to send balda terminal finish reason message")
					} else {
						responseEmitted = true
						responseSource = terminalSource
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
			if err := s.maybeScheduleAutoTurn(ctx, req, responseSource, strings.TrimSpace(responseText)); err != nil {
				return err
			}
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

func (s *TurnExecutionService) autoStatus(ctx context.Context, locator baldasession.SessionLocator) (automode.Status, error) {
	status := automode.DefaultStatus()
	if s == nil || s.sessions == nil {
		return status, nil
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyEnabled); err != nil {
		return status, err
	} else if ok {
		status.Enabled = automode.ParseBool(value)
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyMode); err != nil {
		return status, err
	} else if ok {
		if text, ok := value.(string); ok {
			status.State = strings.TrimSpace(text)
		}
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyConsecutiveTurns); err != nil {
		return status, err
	} else if ok {
		status.ConsecutiveTurns = automode.ParseInt(value, 0)
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyMaxTurns); err != nil {
		return status, err
	} else if ok {
		status.MaxTurns = automode.ParseInt(value, automode.DefaultMaxTurns)
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyLastTurnAt); err != nil {
		return status, err
	} else if ok {
		if text, ok := value.(string); ok {
			status.LastTurnAt = strings.TrimSpace(text)
		}
	}
	if value, ok, err := s.sessions.RuntimeStateValue(ctx, locator, automode.StateKeyLastStopReason); err != nil {
		return status, err
	} else if ok {
		if text, ok := value.(string); ok {
			status.LastStopReason = strings.TrimSpace(text)
		}
	}
	return automode.Normalize(status), nil
}

func autoDecisionNotification(turnSource, responseText string) (string, string, bool) {
	if !strings.EqualFold(strings.TrimSpace(turnSource), turncmd.SourceAuto) {
		return "", "", false
	}
	switch strings.TrimSpace(responseText) {
	case automode.DoneSentinel:
		return "Auto mode is idle.", responseSourceAutoDone, true
	case automode.WaitSentinel:
		return "Auto mode is waiting for user.", responseSourceAutoWaitForUser, true
	default:
		return "", "", false
	}
}

func (s *TurnExecutionService) updateAutoState(ctx context.Context, locator baldasession.SessionLocator, state map[string]any) error {
	if s == nil || s.dispatcher == nil || len(state) == 0 {
		return nil
	}
	env, err := automodecmd.Envelope(automodecmd.Payload{
		Locator: locator,
		State:   state,
	})
	if err != nil {
		return err
	}
	_, err = s.dispatcher.Dispatch(ctx, env)
	return err
}

func (s *TurnExecutionService) maybeScheduleAutoTurn(ctx context.Context, req ExecutionRequest, responseSource string, visibleOutput string) error {
	if s == nil || s.dispatcher == nil || s.sessions == nil {
		return nil
	}
	if responseSource == responseSourceAutoDone || responseSource == responseSourceAutoWaitForUser {
		return nil
	}
	status, err := s.autoStatus(ctx, req.Locator)
	if err != nil || !status.Enabled {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(req.TurnSource), turncmd.SourceAuto) {
		if lastOutput, ok, err := s.sessions.RuntimeStateValue(ctx, req.Locator, automode.StateKeyLastOutput); err == nil && ok {
			currentOutput := strings.TrimSpace(visibleOutput)
			if currentOutput == "" {
				currentOutput = strings.TrimSpace(responseSource)
			}
			if lastText, ok := lastOutput.(string); ok && strings.TrimSpace(lastText) != "" && strings.TrimSpace(lastText) == currentOutput {
				return s.updateAutoState(ctx, req.Locator, map[string]any{
					automode.StateKeyMode:             automode.StateNoProgress,
					automode.StateKeyConsecutiveTurns: status.ConsecutiveTurns,
					automode.StateKeyLastTurnAt:       s.now().UTC().Format(time.RFC3339),
					automode.StateKeyLastStopReason:   "repeated_visible_output",
				})
			}
		}
	}
	nextCount := 1
	if strings.EqualFold(strings.TrimSpace(req.TurnSource), turncmd.SourceAuto) {
		nextCount = status.ConsecutiveTurns + 1
	}
	if nextCount > status.MaxTurns {
		return s.updateAutoState(ctx, req.Locator, map[string]any{
			automode.StateKeyMode:             automode.StateLimitReached,
			automode.StateKeyConsecutiveTurns: status.MaxTurns,
			automode.StateKeyLastTurnAt:       s.now().UTC().Format(time.RFC3339),
			automode.StateKeyLastStopReason:   "max_auto_turns_reached",
		})
	}
	lastOutput := strings.TrimSpace(visibleOutput)
	if lastOutput == "" {
		lastOutput = strings.TrimSpace(responseSource)
	}
	if err := s.updateAutoState(ctx, req.Locator, map[string]any{
		automode.StateKeyMode:             automode.StateRunning,
		automode.StateKeyConsecutiveTurns: nextCount,
		automode.StateKeyLastTurnAt:       s.now().UTC().Format(time.RFC3339),
		automode.StateKeyMaxTurns:         status.MaxTurns,
		automode.StateKeyLastOutput:       lastOutput,
		automode.StateKeyLastStopReason:   "",
	}); err != nil {
		return err
	}
	env, err := turncmd.SessionTurnEnvelope(turncmd.SessionTurnPayload{
		Text:            automode.InternalPrompt(status.MaxTurns),
		Locator:         req.Locator,
		UserID:          req.UserID,
		RequesterUserID: req.RequesterUserID,
		AgentSessionID:  req.AgentSessionID,
		Deliver:         true,
		Source:          turncmd.SourceAuto,
		DedupeKey:       autoTurnDedupeKey(req.Locator.SessionID, req.DedupeKey, nextCount),
		DeliveryOptions: req.DeliveryOptions,
	})
	if err != nil {
		return err
	}
	_, err = s.dispatcher.Dispatch(ctx, env)
	return err
}

func autoTurnDedupeKey(sessionID string, parentDedupeKey string, turn int) string {
	parentSum := sha256.Sum256([]byte(strings.TrimSpace(parentDedupeKey)))
	return fmt.Sprintf("auto:%s:%d:%x", strings.TrimSpace(sessionID), turn, parentSum[:16])
}

func visibleResponseDelta(ev *adksession.Event) string {
	if ev == nil || !ev.Partial || ev.Content == nil {
		return ""
	}
	var b strings.Builder
	for _, part := range ev.Content.Parts {
		if part == nil || part.Thought {
			continue
		}
		if strings.TrimSpace(part.Text) != "" {
			b.WriteString(part.Text)
		}
	}
	return strings.TrimSpace(b.String())
}

func looksLikeRetryOnlyProviderError(message string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(message))
	if trimmed == "" {
		return false
	}
	return strings.Contains(trimmed, "reconnecting") || strings.Contains(trimmed, "will retry")
}
