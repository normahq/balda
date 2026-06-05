package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	"github.com/normahq/balda/internal/apps/balda/locatorref"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
	"google.golang.org/adk/agent"
	"google.golang.org/genai"
)

var zulipHandlerActorAddress = actorlayer.ActorAddress{Target: "handler", Key: "zulip"}

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
	zulipAdapter      *baldazulip.Adapter
	sessionManager    *baldasession.Manager
	turnDispatcher    actors.TurnQueue
	actorDispatcher   actortransport.Dispatcher
	taskService       *swarm.TaskService
	authToken         string
	baldaProviderName string
	webhookToken      string
	listenAddr        string
	webhookPath       string
	enabled           bool
	logger            zerolog.Logger

	mu      sync.RWMutex
	ownerID int64
	server  *http.Server
}

type zulipBaldaHandlerParams struct {
	fx.In

	LC                fx.Lifecycle
	OwnerStore        *auth.OwnerStore
	InviteStore       *auth.InviteStore
	CollaboratorStore *auth.CollaboratorStore
	ZulipAdapter      *baldazulip.Adapter
	SessionManager    *baldasession.Manager
	TurnDispatcher    *actors.TurnDispatcher
	ActorDispatcher   actortransport.Dispatcher
	TaskService       *swarm.TaskService `optional:"true"`
	AuthToken          string `name:"balda_auth_token"`
	BaldaProviderID    string `name:"balda_provider"`
	ZulipWebhookToken  string `name:"balda_zulip_webhook_token"`
	ZulipListenAddr    string `name:"balda_zulip_listen_addr"`
	ZulipWebhookPath   string `name:"balda_zulip_webhook_path"`
	ZulipEnabled       bool   `name:"balda_zulip_webhook_enabled"`
	Logger             zerolog.Logger
}

// NewZulipBaldaHandler creates a ZulipBaldaHandler and registers lifecycle hooks.
func NewZulipBaldaHandler(params zulipBaldaHandlerParams) *ZulipBaldaHandler {
	h := &ZulipBaldaHandler{
		ownerStore:        params.OwnerStore,
		inviteStore:       params.InviteStore,
		collaboratorStore: params.CollaboratorStore,
		zulipAdapter:      params.ZulipAdapter,
		sessionManager:    params.SessionManager,
		turnDispatcher:    params.TurnDispatcher,
		actorDispatcher:   params.ActorDispatcher,
		taskService:       params.TaskService,
		authToken:         strings.TrimSpace(params.AuthToken),
		baldaProviderName: strings.TrimSpace(params.BaldaProviderID),
		webhookToken:      strings.TrimSpace(params.ZulipWebhookToken),
		listenAddr:        strings.TrimSpace(params.ZulipListenAddr),
		webhookPath:       strings.TrimSpace(params.ZulipWebhookPath),
		enabled:           params.ZulipEnabled,
		logger:            params.Logger.With().Str("component", "balda.handler.zulip").Logger(),
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
	h.initOwnerFromStore()

	path := h.webhookPath
	if path == "" {
		path = "/zulip/webhook"
	}
	listenAddr := h.listenAddr
	if listenAddr == "" {
		listenAddr = ":8090"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, h.handleWebhook)
	h.server = &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	go func() {
		h.logger.Info().Str("addr", listenAddr).Str("path", path).Msg("zulip webhook server starting")
		if err := h.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Error().Err(err).Msg("zulip webhook server error")
		}
	}()

	return nil
}

func (h *ZulipBaldaHandler) onStop(ctx context.Context) error {
	if h.server == nil {
		return nil
	}
	if err := h.server.Shutdown(ctx); err != nil {
		h.logger.Warn().Err(err).Msg("zulip webhook server shutdown error")
	}
	return nil
}

func (h *ZulipBaldaHandler) initOwnerFromStore() {
	if !h.ownerStore.HasOwner() {
		return
	}
	owner := h.ownerStore.GetOwner()
	if owner == nil {
		return
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to read zulip webhook body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var payload zulipWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.logger.Warn().Err(err).Msg("failed to decode zulip webhook payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if h.webhookToken != "" && payload.Token != h.webhookToken {
		h.logger.Warn().Str("sender", payload.Message.SenderEmail).Msg("zulip webhook token mismatch")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Respond immediately; process asynchronously.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"response_not_required": true}`))

	go h.processMessage(r.Context(), payload)
}

func (h *ZulipBaldaHandler) processMessage(ctx context.Context, payload zulipWebhookPayload) {
	locator := h.locatorFromPayload(payload)
	senderID := payload.Message.SenderID
	text := strings.TrimSpace(payload.Data)
	isDM := payload.Message.Type == "private"

	h.logger.Debug().
		Str("trigger", payload.Trigger).
		Str("type", payload.Message.Type).
		Int("sender_id", senderID).
		Msg("processing zulip message")

	if strings.HasPrefix(text, "/") {
		h.handleCommand(ctx, locator, senderID, text, isDM)
		return
	}

	h.handleMessage(ctx, locator, senderID, text, isDM)
}

func (h *ZulipBaldaHandler) handleCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	text string,
	_ bool,
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
	if !h.canAccessCollaboratorScope(ctx, transportUserID) {
		_ = h.sendPlain(ctx, locator, "Only the bot owner or collaborators can use this bot.")
		return
	}

	switch cmd {
	case "start":
		h.handleStartCommand(ctx, locator, senderID, args)
	case "reset", "restart":
		h.handleResetCommand(ctx, locator, senderID, cmd)
	case "cancel":
		h.handleCancelCommand(ctx, locator, senderID)
	case "locator":
		h.handleLocatorCommand(ctx, locator)
	case "topic":
		_ = h.sendPlain(ctx, locator, "Zulip already has topics — organize conversations using stream topics.")
	case "close":
		h.handleCloseCommand(ctx, locator, senderID)
	case "user":
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
) {
	ownerID := h.getOwnerID()
	if ownerID != 0 {
		_ = h.sendPlain(ctx, locator, "Bot owner is already registered.")
		return
	}
	if args == "" {
		_ = h.sendPlain(ctx, locator, "Welcome to Balda Bot!\n\nTo authenticate as owner:\n/start owner=<your_owner_token>")
		return
	}
	key, value, ok := strings.Cut(args, "=")
	if !ok || strings.TrimSpace(key) != "owner" || strings.TrimSpace(value) == "" {
		_ = h.sendPlain(ctx, locator, "Usage: /start owner=<your_owner_token>")
		return
	}
	token := strings.TrimSpace(value)
	if token != h.authToken {
		_ = h.sendPlain(ctx, locator, "Invalid authentication token. Please try again.")
		return
	}
	newOwnerID := int64(senderID)
	registered, err := h.ownerStore.RegisterOwner(newOwnerID, 0)
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

func (h *ZulipBaldaHandler) handleResetCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
	cmd string,
) {
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
	_ = h.sendPlain(ctx, locator, "Session restarted.")
}

func (h *ZulipBaldaHandler) handleCancelCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
) {
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
) {
	ref := locatorref.Format(locator)
	msg := fmt.Sprintf(
		"Transport: %s\nLocator: %s\n\nUse in scheduler/webhook config:\ntarget: locator\nkey: %s",
		locator.ChannelType, ref, ref,
	)
	_ = h.sendPlain(ctx, locator, msg)
}

func (h *ZulipBaldaHandler) handleCloseCommand(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
) {
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
	case "invite", "add":
		h.handleUserInvite(ctx, locator, senderID)
	case "list":
		h.handleUserList(ctx, locator)
	case "remove":
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
		"• /user invite - Generate invite token\n" +
		"• /user list - Show collaborators\n" +
		"• /user remove <user_id> - Remove collaborator"
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
	msg := fmt.Sprintf("Invite token created:\n%s\n\nHave the collaborator send:\n/start owner=%s", token, token)
	_ = h.sendPlain(ctx, locator, msg)
}

func (h *ZulipBaldaHandler) handleUserList(
	ctx context.Context,
	locator baldasession.SessionLocator,
) {
	collaborators, err := h.collaboratorStore.ListCollaborators(ctx)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Failed to list collaborators. Please try again.")
		return
	}
	if len(collaborators) == 0 {
		_ = h.sendPlain(ctx, locator, "No collaborators.")
		return
	}
	lines := []string{"Collaborators:"}
	for _, c := range collaborators {
		name := c.UserID
		if strings.TrimSpace(c.FirstName) != "" {
			name += " (" + c.FirstName + ")"
		}
		lines = append(lines, fmt.Sprintf("• %s - added %s", name, c.AddedAt.Format("2006-01-02 15:04")))
	}
	_ = h.sendPlain(ctx, locator, strings.Join(lines, "\n"))
}

func (h *ZulipBaldaHandler) handleUserRemove(
	ctx context.Context,
	locator baldasession.SessionLocator,
	userID string,
) {
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

func (h *ZulipBaldaHandler) handleMessage(
	ctx context.Context,
	locator baldasession.SessionLocator,
	senderID int,
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

	if err := h.enqueueTurn(ctx, text, ts, locator); err != nil {
		if swarm.IsCommandQueueFull(err) {
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

	label := "auto"
	if isDM {
		label = "balda"
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
	label := "auto"
	if isDM {
		label = "balda"
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
) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}
	if h.actorDispatcher == nil {
		return fmt.Errorf("swarm runtime is unavailable")
	}
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: false}
	payload := actors.SessionTurnPayload{
		Text:           text,
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		ProgressPolicy: progressPolicy,
		Deliver:        true,
		Source:         "zulip",
	}
	env, taskID, err := actors.PromptTurnTaskEnvelope(payload)
	if err != nil {
		return err
	}
	_ = taskID
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	return err
}

// RunSessionTurnPayload implements actors.SessionTurnRunner for Zulip sessions.
func (h *ZulipBaldaHandler) RunSessionTurnPayload(
	ctx context.Context,
	payload actors.SessionTurnPayload,
) error {
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil {
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
			}, "balda")
			if err != nil {
				return fmt.Errorf("create session for queued zulip turn: %w", err)
			}
		}
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

	var responseText strings.Builder
	sawTurnComplete := false
	for ev, err := range r.Run(ctx, userID, agentSessionID, userContent, agent.RunConfig{}) {
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

	if err := h.zulipAdapter.SendAgentReply(ctx, deliveryLocator, text); err != nil {
		h.logger.Warn().
			Err(err).
			Str("session_id", payload.Locator.SessionID).
			Msg("zulip: failed to deliver response")
	}
	return nil
}

func (h *ZulipBaldaHandler) canAccessCollaboratorScope(ctx context.Context, userID int64) bool {
	if h.ownerStore.IsOwner(userID) {
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

func (h *ZulipBaldaHandler) locatorFromPayload(payload zulipWebhookPayload) baldasession.SessionLocator {
	if payload.Message.Type == "private" {
		return baldazulip.NewDMLocator(payload.Message.SenderID)
	}
	return baldazulip.NewStreamLocator(payload.Message.StreamID, payload.Message.Subject)
}
