package handlers

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/locatorref"
	"github.com/normahq/balda/internal/apps/balda/memory"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/genai"
)

var zulipHandlerActorAddress = actorlayer.ActorAddress{Target: "handler", Key: "zulip"}

const (
	zulipWebhookMaxBodyBytes       = 1 << 20
	zulipWebhookReadHeaderTimeout  = 5 * time.Second
	zulipWebhookReadTimeout        = 10 * time.Second
	zulipWebhookWriteTimeout       = 10 * time.Second
	zulipWebhookIdleTimeout        = 30 * time.Second
	zulipWebhookProcessingTimeout  = 5 * time.Minute
	zulipWebhookMaxConcurrentTasks = 16
	zulipMessageTypeStream         = "stream"
	zulipTriggerMention            = "mention"
)

// zulipWebhookPayload is the payload Zulip sends to the webhook endpoint.
type zulipWebhookPayload struct {
	BotEmail string       `json:"bot_email"`
	Data     string       `json:"data"`
	Trigger  string       `json:"trigger"`
	Token    string       `json:"token"`
	Message  zulipMessage `json:"message"`
}

// zulipMessage is the message object within the Zulip webhook payload.
type zulipMessage struct {
	ID          int    `json:"id"`
	SenderID    int    `json:"sender_id"`
	SenderEmail string `json:"sender_email"`
	Type        string `json:"type"`
	StreamID    int    `json:"stream_id"`
	Subject     string `json:"subject"`
	Content     string `json:"content"`
}

// ZulipBaldaHandler handles inbound Zulip webhook messages.
type ZulipBaldaHandler struct {
	ownerStore        *auth.OwnerStore
	inviteStore       *auth.InviteStore
	collaboratorStore *auth.CollaboratorStore
	channelAuth       *auth.ChannelAuthService
	sessionManager    zulipSessionManager
	turnDispatcher    actors.TurnQueue
	actorDispatcher   actortransport.Dispatcher
	jobService        *baldajobs.JobService
	memoryStore       *memory.Store
	authToken         string
	baldaProviderName string
	webhookToken      string
	listenAddr        string
	webhookPath       string
	enabled           bool
	goalMaxIterations int
	logger            zerolog.Logger

	mu         sync.RWMutex
	ownerID    int64
	server     *http.Server
	ln         net.Listener
	processSem chan struct{}
	processWG  sync.WaitGroup
}

type zulipSessionManager interface {
	CreateSession(ctx context.Context, sessionCtx baldasession.SessionContext, agentName string) error
	EnsureSession(ctx context.Context, sessionCtx baldasession.SessionContext, agentName string) (*baldasession.TopicSession, error)
	GetAgentMetadata(agentName string) baldasession.AgentMetadata
	GetSession(locator baldasession.SessionLocator) (*baldasession.TopicSession, error)
	GetSessionInfo(ctx context.Context, sessionID string) (baldasession.TopicSessionInfo, error)
	RuntimeStateValue(ctx context.Context, locator baldasession.SessionLocator, key string) (any, bool, error)
	RestoreSession(ctx context.Context, sessionCtx baldasession.SessionContext) (*baldasession.TopicSession, error)
	BaldaProviderID() string
	ResetSession(ctx context.Context, locator baldasession.SessionLocator) error
	TakeStartupNotice(sessionID string) string
}

type zulipBaldaHandlerParams struct {
	fx.In

	LC                fx.Lifecycle
	OwnerStore        *auth.OwnerStore
	InviteStore       *auth.InviteStore
	CollaboratorStore *auth.CollaboratorStore
	ChannelAuth       *auth.ChannelAuthService
	SessionManager    *baldasession.Manager
	TurnDispatcher    *actors.TurnDispatcher
	Dispatcher        actortransport.Dispatcher
	JobService        *baldajobs.JobService `optional:"true"`
	MemoryStore       *memory.Store
	AuthToken         string `name:"balda_auth_token"`
	BaldaProviderID   string `name:"balda_provider"`
	ZulipWebhookToken string `name:"balda_zulip_webhook_token"`
	ZulipListenAddr   string `name:"balda_zulip_listen_addr"`
	ZulipWebhookPath  string `name:"balda_zulip_webhook_path"`
	ZulipEnabled      bool   `name:"balda_zulip_webhook_enabled"`
	MaxIterations     int    `name:"balda_goal_max_iterations"`
	Logger            zerolog.Logger
}

// NewZulipBaldaHandler creates a ZulipBaldaHandler and registers lifecycle hooks.
func NewZulipBaldaHandler(params zulipBaldaHandlerParams) *ZulipBaldaHandler {
	h := &ZulipBaldaHandler{
		ownerStore:        params.OwnerStore,
		inviteStore:       params.InviteStore,
		collaboratorStore: params.CollaboratorStore,
		channelAuth:       params.ChannelAuth,
		sessionManager:    params.SessionManager,
		turnDispatcher:    params.TurnDispatcher,
		actorDispatcher:   params.Dispatcher,
		jobService:        params.JobService,
		memoryStore:       params.MemoryStore,
		authToken:         strings.TrimSpace(params.AuthToken),
		baldaProviderName: strings.TrimSpace(params.BaldaProviderID),
		webhookToken:      strings.TrimSpace(params.ZulipWebhookToken),
		listenAddr:        strings.TrimSpace(params.ZulipListenAddr),
		webhookPath:       strings.TrimSpace(params.ZulipWebhookPath),
		enabled:           params.ZulipEnabled,
		goalMaxIterations: normalizeGoalMaxIterations(params.MaxIterations),
		logger:            params.Logger.With().Str("component", "balda.handler.zulip").Logger(),
		processSem:        make(chan struct{}, zulipWebhookMaxConcurrentTasks),
	}

	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return h.onStart(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return h.onStop(ctx)
		},
	})

	return h
}

func (h *ZulipBaldaHandler) onStart(_ context.Context) error {
	if !h.enabled {
		h.logger.Info().Msg("zulip webhook disabled; skipping server start")
		return nil
	}
	if h.processSem == nil {
		h.processSem = make(chan struct{}, zulipWebhookMaxConcurrentTasks)
	}
	h.initOwnerFromStore()

	path, err := normalizeZulipWebhookPath(h.webhookPath)
	if err != nil {
		return err
	}
	listenAddr := h.listenAddr
	if listenAddr == "" {
		listenAddr = ":8090"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, h.handleWebhook)
	h.server = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: zulipWebhookReadHeaderTimeout,
		ReadTimeout:       zulipWebhookReadTimeout,
		WriteTimeout:      zulipWebhookWriteTimeout,
		IdleTimeout:       zulipWebhookIdleTimeout,
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen zulip webhook endpoint on %q: %w", listenAddr, err)
	}
	h.ln = ln

	go func() {
		h.logger.Info().Str("addr", listenAddr).Str("path", path).Msg("zulip webhook server starting")
		if err := h.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Error().Err(err).Msg("zulip webhook server error")
		}
	}()

	return nil
}

func normalizeZulipWebhookPath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "/zulip/webhook", nil
	}
	if !strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("balda.zulip.webhook.path must start with /")
	}
	return trimmed, nil
}

func (h *ZulipBaldaHandler) onStop(ctx context.Context) error {
	if h.server == nil {
		return nil
	}
	if err := h.server.Shutdown(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("zulip webhook server shutdown error")
		return fmt.Errorf("shutdown zulip webhook server: %w", err)
	}
	if err := h.waitForWebhookProcessing(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("zulip webhook processing shutdown wait error")
		return err
	}
	h.ln = nil
	return nil
}

func (h *ZulipBaldaHandler) waitForWebhookProcessing(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.processWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for zulip webhook processing: %w", ctx.Err())
	}
}

func (h *ZulipBaldaHandler) initOwnerFromStore() {
	if h.ownerStore == nil {
		h.logger.Warn().Msg("zulip handler owner store is unavailable")
		return
	}
	if !h.ownerStore.HasOwner() {
		return
	}
	owner := h.ownerStore.GetOwner()
	if owner == nil {
		return
	}
	for _, subject := range h.ownerStore.OwnerSubjects() {
		value := strings.TrimPrefix(strings.TrimSpace(subject), auth.ChannelZulip+":")
		if value == subject || value == "" {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(value, "%d", &id); err == nil && id > 0 {
			h.mu.Lock()
			h.ownerID = int64(id)
			h.mu.Unlock()
			h.logger.Info().Int("owner_id", id).Msg("zulip handler owner initialized")
			return
		}
	}
	h.mu.Lock()
	h.ownerID = owner.UserID
	h.mu.Unlock()
	h.logger.Info().Int64("owner_id", owner.UserID).Msg("zulip handler owner initialized")
}

func (h *ZulipBaldaHandler) getOwnerID() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ownerID
}

func (h *ZulipBaldaHandler) setOwnerID(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ownerID = id
}

func (h *ZulipBaldaHandler) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, zulipWebhookMaxBodyBytes+1))
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to read zulip webhook body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(body) > zulipWebhookMaxBodyBytes {
		h.logger.Warn().Int("bytes", len(body)).Msg("zulip webhook body too large")
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	var payload zulipWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.logger.Warn().Err(err).Msg("failed to decode zulip webhook payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if h.webhookToken == "" {
		h.logger.Error().Msg("zulip webhook token is not configured")
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if subtle.ConstantTimeCompare([]byte(payload.Token), []byte(h.webhookToken)) != 1 {
		h.logger.Warn().Str("sender", payload.Message.SenderEmail).Msg("zulip webhook token mismatch")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := validateZulipWebhookPayload(payload); err != nil {
		h.logger.Warn().Err(err).Msg("invalid zulip webhook payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if isZulipBotEcho(payload) {
		h.logger.Debug().Str("sender", payload.Message.SenderEmail).Msg("ignoring zulip bot echo")
		writeZulipWebhookNoResponse(w)
		return
	}
	release, ok := h.acquireWebhookProcessSlot()
	if !ok {
		h.logger.Warn().Msg("zulip webhook processing queue full")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}

	// Respond immediately; process asynchronously.
	writeZulipWebhookNoResponse(w)

	h.processWG.Add(1)
	go func() {
		defer h.processWG.Done()
		defer release()
		h.processWebhookPayload(r.Context(), payload)
	}()
}

func (h *ZulipBaldaHandler) processWebhookPayload(requestCtx context.Context, payload zulipWebhookPayload) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Error().
				Interface("panic", recovered).
				Int("sender_id", payload.Message.SenderID).
				Str("session_id", h.locatorFromPayload(payload).SessionID).
				Msg("zulip webhook processing panic recovered")
		}
	}()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), zulipWebhookProcessingTimeout)
	defer cancel()
	h.processMessage(ctx, payload)
}

func writeZulipWebhookNoResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"response_not_required": true}`))
}

func isZulipBotEcho(payload zulipWebhookPayload) bool {
	botEmail := strings.TrimSpace(payload.BotEmail)
	if botEmail == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(payload.Message.SenderEmail), botEmail)
}

func (h *ZulipBaldaHandler) acquireWebhookProcessSlot() (func(), bool) {
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

func validateZulipWebhookPayload(payload zulipWebhookPayload) error {
	if payload.Message.SenderID <= 0 {
		return fmt.Errorf("message.sender_id is required")
	}
	if strings.TrimSpace(payload.Message.SenderEmail) == "" {
		return fmt.Errorf("message.sender_email is required")
	}
	switch strings.TrimSpace(payload.Message.Type) {
	case zulipMessageTypeStream:
		if payload.Message.StreamID <= 0 {
			return fmt.Errorf("message.stream_id is required for stream messages")
		}
	case chatTypePrivate:
	default:
		return fmt.Errorf("unsupported message.type %q", payload.Message.Type)
	}
	return nil
}

func (h *ZulipBaldaHandler) processMessage(ctx context.Context, payload zulipWebhookPayload) {
	locator := h.locatorFromPayload(payload)
	senderID := payload.Message.SenderID
	text := normalizeZulipMessageText(payload)
	isDM := payload.Message.Type == chatTypePrivate

	h.logger.Debug().
		Str("trigger", payload.Trigger).
		Str("type", payload.Message.Type).
		Int("sender_id", senderID).
		Msg("processing zulip message")

	if strings.HasPrefix(text, "/") {
		h.handleCommand(ctx, locator, senderID, text, isDM)
		return
	}
	if isDM {
		if token, ok := firstFieldToken(text); ok {
			h.handleOwnerBindToken(ctx, locator, senderID, token)
			return
		}
	}

	h.handleMessage(ctx, locator, senderID, payload.Message.ID, text, isDM)
}

func normalizeZulipMessageText(payload zulipWebhookPayload) string {
	text := firstNonEmptyText(payload.Data, payload.Message.Content)
	if strings.TrimSpace(payload.Trigger) != zulipTriggerMention {
		return text
	}
	return stripLeadingZulipMentions(text)
}

func firstNonEmptyText(values ...string) string {
	for _, value := range values {
		text := strings.TrimSpace(value)
		if text != "" {
			return text
		}
	}
	return ""
}

func stripLeadingZulipMentions(text string) string {
	trimmed := strings.TrimSpace(text)
	for {
		next, ok := trimLeadingZulipMention(trimmed)
		if !ok {
			return trimmed
		}
		trimmed = strings.TrimSpace(next)
	}
}

func trimLeadingZulipMention(text string) (string, bool) {
	for _, prefix := range []string{"@**", "@_**"} {
		if !strings.HasPrefix(text, prefix) {
			continue
		}
		rest := text[len(prefix):]
		end := strings.Index(rest, "**")
		if end < 0 {
			return text, false
		}
		return rest[end+len("**"):], true
	}
	return text, false
}

func (h *ZulipBaldaHandler) handleCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	text string,
	isDM bool,
) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return
	}
	cmd := strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	args := ""
	if len(fields) > 1 {
		args = strings.Join(fields[1:], " ")
	}

	transportUserID := int64(senderID)
	if cmd != commandStart && !h.canAccessCollaboratorScope(ctx, transportUserID) {
		_ = h.sendPlain(ctx, locator, "Only the bot owner or collaborators can use this bot.")
		return
	}

	switch cmd {
	case commandStart:
		h.handleStartCommand(ctx, locator, senderID, args, isDM)
	case commandReset, commandRestart:
		h.handleResetCommand(ctx, locator, senderID, cmd, args, isDM)
	case commandCancel:
		h.handleCancelCommand(ctx, locator, senderID, args)
	case commandLocator:
		h.handleLocatorCommand(ctx, locator, args)
	case commandTopic:
		h.handleTopicCommand(ctx, locator, senderID, args, isDM)
	case commandGoal:
		h.handleGoalCommand(ctx, locator, senderID, args)
	case commandUsage:
		h.handleUsageCommand(ctx, locator, args)
	case commandClose:
		h.handleCloseCommand(ctx, locator, senderID, args, isDM)
	case commandUser:
		h.handleUserCommand(ctx, locator, senderID, args)
	default:
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Unknown command: /%s", cmd))
	}
}

func (h *ZulipBaldaHandler) handleStartCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
	isDM bool,
) {
	if !isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in direct messages.")
		return
	}
	if args == "" {
		ownerID := h.getOwnerID()
		if ownerID != 0 {
			if h.ownerStore != nil && h.ownerStore.IsOwnerSubject(auth.ZulipSubject(senderID)) {
				msg := ownerAlreadyRegisteredMessage
				if bundle, ok := ownerBindTokenBundleMessage(ctx, h.channelAuth, auth.ZulipSubject(senderID)); ok {
					msg += "\n\n" + bundle
				}
				_ = h.sendPlain(ctx, locator, msg)
			} else {
				_ = h.sendPlain(ctx, locator, "Bot owner is already registered.")
			}
			return
		}
		_ = h.sendPlain(
			ctx, locator,
			"Welcome to Balda Bot!\n\nTo authenticate:\n"+
				"• /start owner=<your_owner_token>\n"+
				"• /start invite=<your_invite_token>",
		)
		return
	}
	if token, ok := firstFieldToken(args); ok {
		h.handleOwnerBindToken(ctx, locator, senderID, token)
		return
	}

	key, value, ok := strings.Cut(args, "=")
	if !ok || strings.TrimSpace(value) == "" {
		_ = h.sendPlain(
			ctx, locator,
			"Invalid /start format. Use one of:\n"+
				"• /start owner=<your_owner_token>\n"+
				"• /start invite=<your_invite_token>",
		)
		return
	}
	mode := strings.TrimSpace(key)
	token := strings.TrimSpace(value)

	if mode == userActionInvite {
		h.handleInviteStart(ctx, locator, senderID, token)
		return
	}

	if mode != startModeOwner {
		_ = h.sendPlain(
			ctx, locator,
			"Invalid /start format. Use one of:\n"+
				"• /start owner=<your_owner_token>\n"+
				"• /start invite=<your_invite_token>",
		)
		return
	}

	ownerID := h.getOwnerID()
	if ownerID != 0 {
		if ownerID == int64(senderID) {
			_ = h.sendPlain(ctx, locator, ownerAlreadyRegisteredMessage)
		} else {
			_ = h.sendPlain(ctx, locator, "Bot owner is already registered.")
		}
		return
	}

	if token != h.authToken {
		_ = h.sendPlain(ctx, locator, "Invalid authentication token. Please try again.")
		return
	}
	if h.ownerStore == nil {
		h.logger.Error().Int("sender_id", senderID).Msg("zulip: owner store is unavailable during owner registration")
		_ = h.sendPlain(ctx, locator, "Could not register owner. Ask the operator to check Balda storage configuration.")
		return
	}
	newOwnerID := int64(senderID)
	registered, err := h.ownerStore.RegisterOwnerSubject(auth.ZulipSubject(senderID))
	if err != nil {
		log.Error().Err(err).Int("sender_id", senderID).Msg("zulip: failed to register owner")
		_ = h.sendPlain(ctx, locator, "Failed to register owner. Please try again.")
		return
	}
	if !registered {
		_ = h.sendPlain(ctx, locator, "Owner is already registered.")
		return
	}
	h.setOwnerID(newOwnerID)
	log.Info().Int64("owner_id", newOwnerID).Msg("zulip: owner registered")
	_ = h.sendPlain(ctx, locator, "You are now registered as the bot owner.")
}

func (h *ZulipBaldaHandler) handleOwnerBindToken(ctx context.Context, locator baldasession.SessionLocator, senderID int, token string) {
	if h.channelAuth == nil {
		_ = h.sendPlain(ctx, locator, "Token authentication is unavailable right now.")
		return
	}
	subject := auth.ZulipSubject(senderID)
	consumed, err := h.channelAuth.ConsumeOwnerBind(ctx, auth.ChannelZulip, subject, token)
	if err != nil {
		h.logger.Warn().Err(err).Int("sender_id", senderID).Msg("zulip: failed to consume owner bind token")
		_ = h.sendPlain(ctx, locator, "Failed to process token. Please try again.")
		return
	}
	if !consumed {
		_ = h.sendPlain(ctx, locator, "This token is invalid or has expired.")
		return
	}
	h.setOwnerID(int64(senderID))
	_ = h.sendPlain(ctx, locator, "This Zulip account is now connected to the Balda owner.")
}

func (h *ZulipBaldaHandler) handleInviteStart(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	token string,
) {
	userIDStr := fmt.Sprintf("%d", senderID)
	if h.ownerStore == nil {
		h.logger.Error().Str("user_id", userIDStr).Msg("zulip: owner store is unavailable during invite registration")
		_ = h.sendPlain(ctx, locator, "Failed to process invite. Ask the operator to check Balda storage configuration.")
		return
	}
	if h.ownerStore.IsOwnerSubject(auth.ZulipSubject(senderID)) {
		_ = h.sendPlain(ctx, locator, "You are already the bot owner.")
		return
	}
	if h.collaboratorStore != nil {
		if _, ok, err := h.collaboratorStore.GetCollaborator(ctx, userIDStr); err != nil {
			h.logger.Warn().Err(err).Str("user_id", userIDStr).Msg("failed to check collaborator")
		} else if ok {
			_ = h.sendPlain(ctx, locator, "You are already a collaborator.")
			return
		}
	}
	if h.inviteStore == nil || h.collaboratorStore == nil {
		_ = h.sendPlain(ctx, locator, "Failed to process invite. Please try again.")
		return
	}
	invite, err := h.inviteStore.GetInvite(ctx, token)
	if err != nil {
		h.logger.Warn().Err(err).Str("user_id", userIDStr).Msg("zulip: failed to get invite")
		_ = h.sendPlain(ctx, locator, "Failed to process invite. Please try again.")
		return
	}
	if invite == nil {
		_ = h.sendPlain(ctx, locator, "This invite token is invalid or has expired.")
		return
	}
	collaborator := auth.Collaborator{
		UserID:  userIDStr,
		AddedBy: invite.CreatedBy,
		AddedAt: time.Now(),
	}
	if err := h.collaboratorStore.AddCollaborator(ctx, collaborator); err != nil {
		h.logger.Error().Err(err).Msg("zulip: failed to add collaborator from invite")
		_ = h.sendPlain(ctx, locator, "Failed to complete registration. Please try again.")
		return
	}
	log.Info().Str("user_id", userIDStr).Str("invited_by", invite.CreatedBy).Msg("zulip: user registered as collaborator via invite")
	_ = h.sendPlain(ctx, locator, "Welcome! You are now a bot collaborator.")
}

func (h *ZulipBaldaHandler) handleResetCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	cmd string,
	args string,
	isDM bool,
) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Usage: /%s", cmd))
		return
	}
	if h.sessionManager == nil {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now. Please try again.")
		return
	}
	info, infoErr := h.sessionManager.GetSessionInfo(ctx, locator.SessionID)
	if infoErr != nil {
		h.logger.Debug().Err(infoErr).Str("session_id", locator.SessionID).Str("cmd", cmd).Msg("zulip: session info unavailable before restart")
	}
	transportUserID := baldazulip.UserID(senderID)
	reason := fmt.Sprintf("session canceled by %s command", cmd)
	if submitErr := submitSessionCancelControl(
		ctx, h.actorDispatcher, locator, transportUserID, reason, false,
	); submitErr != nil {
		h.logger.Warn().Err(submitErr).Str("session_id", locator.SessionID).Str("cmd", cmd).Msg("failed to submit cancel control")
	}
	if err := h.sessionManager.ResetSession(ctx, locator); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to reset session")
		_ = h.sendPlain(ctx, locator, "Could not reset this session.")
		return
	}
	label := zulipRestartSessionLabel(isDM, info)
	userID := zulipRestartSessionUserID(senderID, info)
	if err := h.sessionManager.CreateSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  userID,
	}, label); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Str("cmd", cmd).Msg("zulip: failed to recreate session during restart command")
		_ = h.sendPlain(ctx, locator, "Could not restart this session.")
		return
	}

	providerName := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	metadata := h.sessionManager.GetAgentMetadata(providerName)
	welcomeName := zulipRestartWelcomeDisplayName(isDM, label)
	welcomeMsg := welcome.BuildAgentWelcomeMessage(welcomeName, locator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers)
	if err := h.sendMarkdown(ctx, locator, welcomeMsg); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Str("cmd", cmd).Msg("zulip: failed to send restart welcome")
	}
	h.sendSessionStartupNotice(ctx, locator, locator.SessionID)
}

func zulipRestartSessionLabel(isDM bool, info baldasession.TopicSessionInfo) string {
	if label := strings.TrimSpace(info.AgentName); label != "" {
		return label
	}
	if isDM {
		return ownerSessionLabel
	}
	return autoSessionLabel
}

func zulipRestartSessionUserID(senderID int, info baldasession.TopicSessionInfo) string {
	if userID := strings.TrimSpace(info.UserID); userID != "" {
		return userID
	}
	return baldazulip.UserID(senderID)
}

func zulipRestartWelcomeDisplayName(isDM bool, label string) string {
	if !isDM {
		return ownerSessionLabel
	}
	return label
}

func (h *ZulipBaldaHandler) handleCancelCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /cancel")
		return
	}
	if h.actorDispatcher == nil {
		_ = h.sendPlain(ctx, locator, "Cancel is unavailable right now. Please try again.")
		return
	}
	transportUserID := baldazulip.UserID(senderID)
	if err := submitSessionTurnCancelControl(
		ctx, h.actorDispatcher, locator, transportUserID, "session turn canceled by user", true,
	); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to submit turn cancel control")
		_ = h.sendPlain(ctx, locator, "Could not request cancel.")
		return
	}
	_ = h.sendPlain(ctx, locator, "Cancel requested.")
}

func (h *ZulipBaldaHandler) handleLocatorCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	args string,
) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /locator")
		return
	}
	ref := locatorref.Format(locator)
	msg := fmt.Sprintf(
		"Transport: %s\nLocator: %s\n\nUse in scheduler/webhook config:\ntarget: locator\nkey: %s",
		locator.ChannelType, ref, ref,
	)
	_ = h.sendPlain(ctx, locator, msg)
}

func (h *ZulipBaldaHandler) handleUsageCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	args string,
) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /usage")
		return
	}
	snapshot, ok, err := loadUsageSnapshot(ctx, h.sessionManager, locator)
	if err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to load usage snapshot")
	}
	if err != nil || !ok {
		_ = h.sendPlain(ctx, locator, "No provider usage has been recorded for this session yet.")
		return
	}
	_ = h.sendPlain(ctx, locator, renderUsageSnapshot(snapshot))
}

func (h *ZulipBaldaHandler) handleCloseCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
	isDM bool,
) {
	if !isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in direct messages.")
		return
	}
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /close")
		return
	}
	if h.sessionManager == nil {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now. Please try again.")
		return
	}
	transportUserID := baldazulip.UserID(senderID)
	if submitErr := submitSessionCancelControl(
		ctx, h.actorDispatcher, locator, transportUserID, "session canceled by close command", false,
	); submitErr != nil {
		h.logger.Warn().Err(submitErr).Str("session_id", locator.SessionID).Msg("failed to submit cancel control for /close")
	}
	if err := h.sessionManager.ResetSession(ctx, locator); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to reset session for /close")
		_ = h.sendPlain(ctx, locator, "Could not close this session.")
		return
	}
	_ = h.sendPlain(ctx, locator, "Session history reset.")
}

func (h *ZulipBaldaHandler) handleUserCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
) {
	ownerID := h.getOwnerID()
	if ownerID == 0 || int64(senderID) != ownerID {
		_ = h.sendPlain(ctx, locator, "This command is only for the owner.")
		return
	}
	fields := strings.Fields(args)
	if len(fields) == 0 {
		h.sendUserUsage(ctx, locator)
		return
	}
	switch fields[0] {
	case userActionAdd, userActionInvite:
		h.handleUserInvite(ctx, locator, senderID)
	case userActionList:
		h.handleUserList(ctx, locator)
	case userActionRemove:
		if len(fields) < 2 {
			_ = h.sendPlain(ctx, locator, "Usage: /user remove <user_id>")
			return
		}
		h.handleUserRemove(ctx, locator, fields[1])
	default:
		h.sendUserUsage(ctx, locator)
	}
}

func (h *ZulipBaldaHandler) sendUserUsage(ctx context.Context, locator baldasession.SessionLocator) {
	usage := "Usage:\n" +
		"• /user add - Generate invite token\n" +
		"• /user list - Show collaborators and active invites\n" +
		"• /user remove <user_id> - Remove collaborator by ID\n"
	_ = h.sendPlain(ctx, locator, usage)
}

func (h *ZulipBaldaHandler) handleUserInvite(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
) {
	if h.inviteStore == nil {
		_ = h.sendPlain(ctx, locator, "Invite store is unavailable.")
		return
	}
	ownerIDStr := fmt.Sprintf("%d", senderID)
	token, _, err := h.inviteStore.CreateInvite(ctx, ownerIDStr)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Failed to create invite. Please try again.")
		return
	}
	msg := fmt.Sprintf("Invite token created:\n%s\n\nHave the collaborator send:\n/start invite=%s", token, token)
	_ = h.sendPlain(ctx, locator, msg)
}

func (h *ZulipBaldaHandler) handleUserList(
	ctx context.Context,
	locator baldasession.SessionLocator,
) {
	if h.collaboratorStore == nil {
		_ = h.sendPlain(ctx, locator, "Collaborator store is unavailable.")
		return
	}
	var lines []string

	collaborators, err := h.collaboratorStore.ListCollaborators(ctx)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Failed to list collaborators. Please try again.")
		return
	}
	if len(collaborators) > 0 {
		lines = append(lines, "Collaborators:")
		for _, c := range collaborators {
			name := "unknown"
			if strings.TrimSpace(c.Username) != "" {
				name = c.Username
			} else if strings.TrimSpace(c.FirstName) != "" {
				name = c.FirstName
			}
			lines = append(lines, fmt.Sprintf("• %s (%s) - added %s",
				c.UserID, name, c.AddedAt.Format("2006-01-02 15:04")))
		}
	} else {
		lines = append(lines, "No collaborators")
	}

	if h.inviteStore != nil {
		invites, err := h.inviteStore.ListInvites(ctx)
		if err != nil {
			_ = h.sendPlain(ctx, locator, "Failed to list invites. Please try again.")
			return
		}
		if len(invites) > 0 {
			lines = append(lines, "", "Active Invites:")
			for _, inv := range invites {
				lines = append(lines, fmt.Sprintf("expires %s", inv.ExpiresAt.Format("2006-01-02 15:04")))
			}
		}
	}

	_ = h.sendPlain(ctx, locator, strings.Join(lines, "\n"))
}

func (h *ZulipBaldaHandler) handleUserRemove(
	ctx context.Context,
	locator baldasession.SessionLocator,
	userID string,
) {
	if h.collaboratorStore == nil {
		_ = h.sendPlain(ctx, locator, "Collaborator store is unavailable.")
		return
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		_ = h.sendPlain(ctx, locator, "User ID required.")
		return
	}
	if err := h.collaboratorStore.RemoveCollaborator(ctx, userID); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not remove collaborator. Please try again.")
		return
	}
	_ = h.sendPlain(ctx, locator, fmt.Sprintf("Collaborator removed: %s", userID))
}

func (h *ZulipBaldaHandler) handleGoalCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
) {
	objective := strings.TrimSpace(args)
	if objective == "" {
		_ = h.sendPlain(ctx, locator, "Usage:\n/goal <objective>\n/goal clear")
		return
	}
	if strings.EqualFold(objective, "clear") {
		if h.actorDispatcher == nil {
			_ = h.sendPlain(ctx, locator, "Goal control is unavailable right now. Please try again.")
			return
		}
		if err := submitGoalClearControl(
			ctx, h.actorDispatcher, locator, baldazulip.UserID(senderID), "goal cleared by user", true,
		); err != nil {
			h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to submit goal clear control")
			_ = h.sendPlain(ctx, locator, "Could not clear goal run.")
		}
		return
	}
	started, err := h.submitGoalJob(ctx, locator, objective, baldazulip.UserID(senderID))
	if err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to start /goal run")
		_ = h.sendPlain(ctx, locator, "Could not start goal run.")
		return
	}
	if !started {
		_ = h.sendPlain(ctx, locator, "A goal run is already active for this session.")
	}
}

func (h *ZulipBaldaHandler) submitGoalJob(
	ctx context.Context,
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
) (bool, error) {
	if h.jobService != nil {
		activeGoals, err := h.jobService.ListActiveGoalJobsBySession(ctx, locator.SessionID)
		if err != nil {
			return false, fmt.Errorf("list active goal jobs: %w", err)
		}
		if len(activeGoals) > 0 {
			return false, nil
		}
	}
	maxIterations := normalizeGoalMaxIterations(h.goalMaxIterations)
	env, err := goalkeeper.GoalJobEnvelopeWithOptions(locator, deliveryfmt.Options{Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatMarkdown}, ProgressPolicy: deliveryfmt.ProgressPolicy{Typing: true, Thinking: false, PlanUpdates: true}}, objective, transportUserID, maxIterations)
	if err != nil {
		return false, err
	}
	if h.actorDispatcher == nil {
		return false, fmt.Errorf("runtime is unavailable")
	}
	if _, err = h.actorDispatcher.Dispatch(ctx, env); err != nil {
		return false, err
	}
	return true, nil
}

func (h *ZulipBaldaHandler) handleTopicCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	args string,
	isDM bool,
) {
	if isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in stream messages.")
		return
	}
	topicName := strings.TrimSpace(args)
	if topicName == "" {
		_ = h.sendPlain(ctx, locator, "Usage: /topic <name>")
		return
	}
	if h.sessionManager == nil {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now.")
		return
	}
	baldaProviderID := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	if baldaProviderID == "" {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now.")
		return
	}

	streamID, ok := baldazulip.StreamIDFromLocator(locator)
	if !ok {
		_ = h.sendPlain(ctx, locator, "Could not determine stream ID from current context.")
		return
	}

	h.logger.Info().
		Int("sender_id", senderID).
		Int("stream_id", streamID).
		Str("topic_name", topicName).
		Msg("creating zulip topic session")

	topicLocator := baldazulip.NewStreamLocator(streamID, topicName)
	transportUserID := baldazulip.UserID(senderID)
	if err := h.sessionManager.CreateSession(ctx, baldasession.SessionContext{
		Locator: topicLocator,
		UserID:  transportUserID,
	}, topicName); err != nil {
		h.logger.Error().Err(err).Str("topic_name", topicName).Msg("failed to create zulip topic session")
		_ = h.sendPlain(ctx, locator, "Could not create topic session.")
		return
	}
	metadata := h.sessionManager.GetAgentMetadata(baldaProviderID)
	welcomeMsg := welcome.BuildAgentWelcomeMessage(
		topicName, topicLocator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers,
	)
	if err := h.sendZulipAgentReply(ctx, topicLocator, welcomeMsg); err != nil {
		h.logger.Warn().Err(err).Str("topic_name", topicName).Msg("failed to send welcome to new topic")
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Session created for topic '%s'.", topicName))
		return
	}
	_ = h.sendPlain(ctx, locator, fmt.Sprintf("Session created. Post in topic '%s' to continue.", topicName))
}

func (h *ZulipBaldaHandler) handleMessage(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	messageID int,
	text string,
	isDM bool,
) {
	if h.getOwnerID() == 0 {
		return
	}
	if !h.canAccessCollaboratorScope(ctx, int64(senderID)) {
		return
	}
	if strings.TrimSpace(text) == "" {
		return
	}

	transportUserID := baldazulip.UserID(senderID)
	providerName := h.getProviderName()

	ts, err := h.getOrCreateSession(ctx, locator, transportUserID, providerName, isDM)
	if err != nil {
		return
	}

	if err := h.enqueueTurn(ctx, text, ts, locator, messageID, isDM); err != nil {
		if baldaexecution.IsCommandQueueFull(err) {
			_ = h.sendPlain(ctx, locator, "Session command queue is full. Please wait or use /cancel.")
			return
		}
		h.logger.Error().Err(err).Str("session_id", locator.SessionID).Msg("zulip: failed to enqueue turn")
		_ = h.sendPlain(ctx, locator, "Failed to publish your message for processing. Please try again.")
	}
}

func (h *ZulipBaldaHandler) getOrCreateSession(
	ctx context.Context,
	locator baldasession.SessionLocator,
	transportUserID string,
	providerName string,
	isDM bool,
) (*baldasession.TopicSession, error) {
	if h.sessionManager == nil {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now. Please try again.")
		return nil, fmt.Errorf("session manager is unavailable")
	}
	existing, _ := h.sessionManager.GetSession(locator)
	if existing != nil {
		return existing, nil
	}

	if providerName == "" {
		_ = h.sendPlain(ctx, locator, "Balda is not ready right now. Please try again.")
		return nil, fmt.Errorf("no provider configured")
	}

	ts, err := h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  transportUserID,
	})
	if err != nil && !errors.Is(err, baldasession.ErrNoPersistedSession) {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("zulip: failed to restore session")
		_ = h.sendPlain(ctx, locator, "Could not restore this session. Please try again.")
		return nil, err
	}
	if err == nil && ts != nil {
		h.sendSessionWelcome(ctx, locator, ts, providerName, isDM)
		return ts, nil
	}

	label := autoSessionLabel
	if isDM {
		label = ownerSessionLabel
	}
	ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  transportUserID,
	}, label)
	if err != nil {
		h.logger.Error().Err(err).Str("agent", providerName).Msg("zulip: failed to create session")
		_ = h.sendPlain(ctx, locator, "Could not start this session. Please try again.")
		return nil, err
	}
	h.sendSessionWelcome(ctx, locator, ts, providerName, isDM)
	return ts, nil
}

func (h *ZulipBaldaHandler) sendSessionWelcome(
	ctx context.Context,
	locator baldasession.SessionLocator,
	ts *baldasession.TopicSession,
	providerName string,
	isDM bool,
) {
	label := autoSessionLabel
	if isDM {
		label = ownerSessionLabel
	}
	metadata := h.sessionManager.GetAgentMetadata(providerName)
	welcomeMsg := welcome.BuildAgentWelcomeMessage(label, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
	_ = h.sendPlain(ctx, locator, welcomeMsg)
}

func (h *ZulipBaldaHandler) enqueueTurn(
	ctx context.Context,
	text string,
	ts *baldasession.TopicSession,
	locator baldasession.SessionLocator,
	messageID int,
	isDM bool,
) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}
	if h.actorDispatcher == nil {
		return fmt.Errorf("runtime is unavailable")
	}
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: false, PlanUpdates: true}
	payload := actors.SessionTurnPayload{
		Text:           text,
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		MessageID:      messageID,
		DeliveryOptions: deliveryfmt.Options{
			Profile:        deliveryfmt.Profile{Format: deliveryfmt.FormatMarkdown},
			ProgressPolicy: progressPolicy,
		},
		ProgressPolicy: progressPolicy,
		Deliver:        true,
		Source:         "zulip",
	}
	if messageID > 0 {
		payload.DedupeKey = fmt.Sprintf("zulip:%d", messageID)
	}
	env, err := actors.SessionTurnEnvelope(payload)
	if err != nil {
		return err
	}
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	return err
}

// RunSessionTurnPayload implements actors.SessionTurnRunner for Zulip sessions.
func (h *ZulipBaldaHandler) RunSessionTurnPayload(
	ctx context.Context,
	payload actors.SessionTurnPayload,
) error {
	if h.sessionManager == nil {
		return fmt.Errorf("zulip turn: session manager is unavailable")
	}
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil || ts == nil {
		userID := strings.TrimSpace(payload.UserID)
		ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
			Locator: payload.Locator,
			UserID:  userID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return fmt.Errorf("restore session for queued zulip turn: %w", err)
			}
			if userID == "" {
				h.logger.Debug().
					Str("session_id", payload.Locator.SessionID).
					Msg("dropping queued zulip turn for unknown session without transport user")
				return nil
			}
			ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
				Locator: payload.Locator,
				UserID:  userID,
			}, ownerSessionLabel)
			if err != nil {
				return fmt.Errorf("create session for queued zulip turn: %w", err)
			}
		}
	}
	if ts == nil {
		return fmt.Errorf("zulip turn: session %s unavailable after restore", payload.Locator.SessionID)
	}

	userID := strings.TrimSpace(payload.UserID)
	if userID == "" {
		userID = ts.GetUserID()
	}
	agentSessionID := strings.TrimSpace(payload.AgentSessionID)
	if agentSessionID == "" {
		agentSessionID = ts.GetAgentSessionID()
	}
	deliveryLocator := payload.Locator
	if payload.ReportTo != nil {
		deliveryLocator = *payload.ReportTo
	}

	r := ts.GetRunner()
	if r == nil {
		return fmt.Errorf("zulip turn: no runner in session %s", payload.Locator.SessionID)
	}

	userContent := genai.NewContentFromText(strings.TrimSpace(payload.Text), genai.RoleUser)
	runOpts, err := prepareMemoryRunOptions(ctx, h.memoryStore, ts)
	if err != nil {
		return err
	}

	var responseText strings.Builder
	sawTurnComplete := false
	for ev, err := range r.Run(ctx, userID, agentSessionID, userContent, agent.RunConfig{}, runOpts...) {
		if err != nil {
			return fmt.Errorf("zulip agent run: %w", err)
		}
		if ev == nil {
			continue
		}
		if ev.Content != nil {
			var evText strings.Builder
			for _, part := range ev.Content.Parts {
				if part == nil || part.Thought || part.Text == "" {
					continue
				}
				evText.WriteString(part.Text)
			}
			if evText.Len() > 0 && ev.IsFinalResponse() {
				text := evText.String()
				if text != responseText.String() {
					responseText.Reset()
					responseText.WriteString(text)
				}
			}
		}
		if ev.TurnComplete {
			sawTurnComplete = true
			break
		}
	}

	if !sawTurnComplete {
		h.logger.Warn().
			Str("session_id", payload.Locator.SessionID).
			Msg("zulip turn did not complete normally")
	}

	text := strings.TrimSpace(responseText.String())
	if text == "" || !payload.Deliver {
		return nil
	}

	return h.deliverZulipAgentReply(ctx, deliveryLocator, payload.Locator.SessionID, text)
}

func (h *ZulipBaldaHandler) deliverZulipAgentReply(
	ctx context.Context,
	locator baldasession.SessionLocator,
	sessionID string,
	text string,
) error {
	if err := h.sendZulipAgentReply(ctx, locator, text); err != nil {
		h.logger.Warn().
			Err(err).
			Str("session_id", sessionID).
			Msg("zulip: failed to deliver response")
		return fmt.Errorf("deliver zulip response for session %s: %w", sessionID, err)
	}
	return nil
}

func (h *ZulipBaldaHandler) sendZulipAgentReply(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	if h == nil || h.actorDispatcher == nil {
		return fmt.Errorf("runtime is unavailable")
	}
	env, err := actors.AgentReplyDeliveryEnvelopeWithSettlement("", zulipHandlerActorAddress, locator, deliverycmd.SettlementBypass, text, "")
	if err != nil {
		return err
	}
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	return err
}

func (h *ZulipBaldaHandler) canAccessCollaboratorScope(ctx context.Context, userID int64) bool {
	if h.ownerStore != nil && h.ownerStore.IsOwnerSubject(auth.ZulipSubject(int(userID))) {
		return true
	}
	if h.collaboratorStore == nil {
		return false
	}
	_, found, err := h.collaboratorStore.GetCollaborator(ctx, fmt.Sprintf("%d", userID))
	if err != nil || !found {
		return false
	}
	return true
}

func (h *ZulipBaldaHandler) getProviderName() string {
	if h.sessionManager == nil {
		return strings.TrimSpace(h.baldaProviderName)
	}
	providerName := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	if providerName == "" {
		providerName = strings.TrimSpace(h.baldaProviderName)
	}
	return providerName
}

func (h *ZulipBaldaHandler) sendPlain(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	return sendPlain(ctx, h.actorDispatcher, zulipHandlerActorAddress, locator, text)
}

func (h *ZulipBaldaHandler) sendMarkdown(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	return sendMarkdown(ctx, h.actorDispatcher, zulipHandlerActorAddress, locator, text)
}

func (h *ZulipBaldaHandler) sendSessionStartupNotice(ctx context.Context, locator baldasession.SessionLocator, sessionID string) {
	if h.sessionManager == nil {
		return
	}
	notice := strings.TrimSpace(h.sessionManager.TakeStartupNotice(sessionID))
	if notice == "" {
		return
	}
	if err := h.sendPlain(ctx, locator, notice); err != nil {
		h.logger.Warn().Err(err).Str("session_id", sessionID).Msg("zulip: failed to send restart startup notice")
	}
}

func (h *ZulipBaldaHandler) locatorFromPayload(payload zulipWebhookPayload) baldasession.SessionLocator {
	if payload.Message.Type == chatTypePrivate {
		return baldazulip.NewDMLocator(payload.Message.SenderID)
	}
	return baldazulip.NewStreamLocator(payload.Message.StreamID, payload.Message.Subject)
}
