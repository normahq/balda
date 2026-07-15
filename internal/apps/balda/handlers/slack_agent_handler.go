package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldaslackagent "github.com/normahq/balda/internal/apps/balda/channel/slackagent"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type SlackAgentConfig struct {
	Enabled       bool
	ListenAddr    string
	EventsPath    string
	SigningSecret string
}

type slackAgentEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge"`
	EventID   string          `json:"event_id"`
	TeamID    string          `json:"team_id"`
	Event     slackAgentEvent `json:"event"`
}

type slackAgentEvent struct {
	Type             string `json:"type"`
	UserID           string `json:"user_id"`
	Text             string `json:"text"`
	ConversationID   string `json:"conversation_id"`
	ThreadID         string `json:"thread_id"`
	MessageID        string `json:"message_id"`
	ReplyToMessageID string `json:"reply_to_message_id"`
	TeamID           string `json:"team_id"`
}

type SlackAgentHandler struct {
	sessionManager  *baldasession.Manager
	actorDispatcher transport.Dispatcher
	questionService *questions.Service
	config          SlackAgentConfig
	logger          zerolog.Logger

	server     *http.Server
	ln         net.Listener
	processSem chan struct{}
	processWG  sync.WaitGroup
}

type slackAgentHandlerParams struct {
	fx.In

	SessionManager   *baldasession.Manager
	Dispatcher       transport.Dispatcher
	QuestionService  *questions.Service `optional:"true"`
	SlackAgentConfig SlackAgentConfig
	Logger           zerolog.Logger
}

func NewSlackAgentHandler(params slackAgentHandlerParams) *SlackAgentHandler {
	return &SlackAgentHandler{
		sessionManager:  params.SessionManager,
		actorDispatcher: params.Dispatcher,
		questionService: params.QuestionService,
		config:          params.SlackAgentConfig,
		logger:          params.Logger.With().Str("component", "balda.handler.slack_agent").Logger(),
		processSem:      make(chan struct{}, slackWebhookMaxConcurrentTasks),
	}
}

func (h *SlackAgentHandler) Start(ctx context.Context) error { return h.onStart(ctx) }
func (h *SlackAgentHandler) Stop(ctx context.Context) error  { return h.onStop(ctx) }

func (h *SlackAgentHandler) onStart(context.Context) error {
	if !h.config.Enabled {
		h.logger.Debug().Msg("slack agent disabled; skipping server start")
		return nil
	}
	eventsPath, err := normalizeSlackPath(h.config.EventsPath, "/slack/agent/events")
	if err != nil {
		return err
	}
	listenAddr := strings.TrimSpace(h.config.ListenAddr)
	if listenAddr == "" {
		listenAddr = ":8092"
	}
	mux := http.NewServeMux()
	mux.HandleFunc(eventsPath, h.handleEvents)
	h.server = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: slackWebhookReadHeaderTimeout,
		ReadTimeout:       slackWebhookReadTimeout,
		WriteTimeout:      slackWebhookWriteTimeout,
		IdleTimeout:       slackWebhookIdleTimeout,
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen slack agent endpoint on %q: %w", listenAddr, err)
	}
	h.ln = ln
	go func() {
		h.logger.Info().Str("addr", listenAddr).Str("events_path", eventsPath).Msg("slack agent http server starting")
		if err := h.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Error().Err(err).Msg("slack agent http server error")
		}
	}()
	return nil
}

func (h *SlackAgentHandler) onStop(ctx context.Context) error {
	if h.server == nil {
		return nil
	}
	if err := h.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown slack agent server: %w", err)
	}
	done := make(chan struct{})
	go func() {
		h.processWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for slack agent processing: %w", ctx.Err())
	}
}

func (h *SlackAgentHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	body, ok := h.readAndVerifySlackRequest(w, r)
	if !ok {
		return
	}
	var env slackAgentEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		h.logger.Warn().Err(err).Msg("failed to decode slack agent event payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if env.Type == "url_verification" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(env.Challenge))
		return
	}
	release, ok := h.acquireProcessSlot()
	if !ok {
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	h.processWG.Add(1)
	go func() {
		defer h.processWG.Done()
		defer release()
		h.processEvent(context.WithoutCancel(r.Context()), env)
	}()
}

func (h *SlackAgentHandler) readAndVerifySlackRequest(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, slackWebhookMaxBodyBytes+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return nil, false
	}
	if len(body) > slackWebhookMaxBodyBytes {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	if err := verifySlackSignature(strings.TrimSpace(h.config.SigningSecret), r.Header.Get("X-Slack-Request-Timestamp"), r.Header.Get("X-Slack-Signature"), body, time.Now()); err != nil {
		h.logger.Warn().Err(err).Msg("slack agent signature verification failed")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func (h *SlackAgentHandler) acquireProcessSlot() (func(), bool) {
	if h.processSem == nil {
		return func() {}, true
	}
	select {
	case h.processSem <- struct{}{}:
		return func() { <-h.processSem }, true
	default:
		return nil, false
	}
}

func (h *SlackAgentHandler) processEvent(requestCtx context.Context, env slackAgentEnvelope) {
	ctx, cancel := context.WithTimeout(requestCtx, slackWebhookProcessingTimeout)
	defer cancel()
	event := env.Event
	if strings.TrimSpace(event.Text) == "" || strings.TrimSpace(event.UserID) == "" {
		return
	}
	teamID := firstNonEmpty(event.TeamID, env.TeamID)
	var locator baldasession.SessionLocator
	if strings.TrimSpace(event.ThreadID) != "" {
		locator = baldaslackagent.NewThreadLocator(teamID, event.ConversationID, event.ThreadID)
	} else {
		locator = baldaslackagent.NewConversationLocator(teamID, event.ConversationID)
	}
	subject := baldaslack.UserID(teamID, event.UserID)
	if handled, err := h.handleQuestionReply(ctx, locator, subject, event); err != nil {
		h.logger.Warn().Err(err).Str("address_key", locator.AddressKey).Msg("failed to handle slack agent question reply")
		return
	} else if handled {
		return
	}
	ts, err := h.getOrCreateSession(ctx, locator, subject)
	if err != nil {
		h.logger.Warn().Err(err).Str("address_key", locator.AddressKey).Msg("failed to get or create slack agent session")
		return
	}
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	payload := turncmd.SessionTurnPayload{
		Text:           strings.TrimSpace(event.Text),
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		MessageID:      slackMessageID(firstNonEmpty(event.MessageID, env.EventID)),
		DeliveryOptions: deliveryfmt.Options{
			Profile:        deliveryfmt.Profile{Format: deliveryfmt.FormatMarkdown},
			ProgressPolicy: progressPolicy,
		},
		ProgressPolicy: progressPolicy,
		Deliver:        true,
		Source:         "slack_agent",
		DedupeKey:      "slack_agent:" + firstNonEmpty(env.EventID, event.MessageID),
	}
	envelope, err := turncmd.SessionTurnEnvelope(payload)
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to build slack agent session turn envelope")
		return
	}
	if _, err := h.actorDispatcher.Dispatch(ctx, envelope); err != nil {
		if actorcmd.IsCommandQueueFull(err) {
			h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("slack agent session command queue full")
			return
		}
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to dispatch slack agent session turn")
	}
}

func (h *SlackAgentHandler) getOrCreateSession(ctx context.Context, locator baldasession.SessionLocator, subject string) (*baldasession.TopicSession, error) {
	if existing, _ := h.sessionManager.GetSession(locator); existing != nil {
		return existing, nil
	}
	ts, err := h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject})
	if err == nil && ts != nil {
		return ts, nil
	}
	if err != nil && !errors.Is(err, baldasession.ErrNoPersistedSession) {
		return nil, err
	}
	return h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject}, autoSessionLabel)
}

func (h *SlackAgentHandler) handleQuestionReply(ctx context.Context, locator baldasession.SessionLocator, subject string, event slackAgentEvent) (bool, error) {
	if h == nil || h.questionService == nil || strings.TrimSpace(event.ReplyToMessageID) == "" || strings.TrimSpace(event.Text) == "" {
		return false, nil
	}
	record, matched, err := h.questionService.ResolveReply(ctx, questioncmd.InboundReply{
		Provider:         baldaslackagent.ChannelType,
		SessionID:        locator.SessionID,
		ConversationKey:  locator.AddressKey,
		ReplyToMessageID: strings.TrimSpace(event.ReplyToMessageID),
		MessageID:        strings.TrimSpace(event.MessageID),
		User:             questioncmd.UserRef{UserID: subject},
		Text:             strings.TrimSpace(event.Text),
		ReceivedAt:       time.Now().UTC(),
	})
	if err != nil || !matched {
		return matched, err
	}
	if err := dispatchQuestionContinuation(ctx, h.actorDispatcher, record); err != nil {
		return true, err
	}
	return true, nil
}
