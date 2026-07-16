package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/normahq/balda/internal/apps/balda/automode"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/goalcmd"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/locatorref"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

var slackChatHandlerActorAddress = actorlayer.ActorAddress{Target: "handler", Key: "slack"}

const (
	slackWebhookMaxBodyBytes       = 1 << 20
	slackWebhookReadHeaderTimeout  = 5 * time.Second
	slackWebhookReadTimeout        = 10 * time.Second
	slackWebhookWriteTimeout       = 10 * time.Second
	slackWebhookIdleTimeout        = 30 * time.Second
	slackWebhookProcessingTimeout  = 5 * time.Minute
	slackWebhookMaxConcurrentTasks = 16
	slackSignatureVersion          = "v0"
)

// SlackChatConfig is the normalized handler config for the current Slack chat integration.
type SlackChatConfig struct {
	Enabled                bool
	BotToken               string
	SigningSecret          string
	ListenAddr             string
	EventsPath             string
	CommandsPath           string
	IncludePrivateChannels bool
}

type slackEventEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge"`
	TeamID    string          `json:"team_id"`
	EventID   string          `json:"event_id"`
	Event     slackEvent      `json:"event"`
	RawEvent  json.RawMessage `json:"-"`
}

type slackEvent struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	ChannelType string `json:"channel_type"`
	Channel     string `json:"channel"`
	User        string `json:"user"`
	Text        string `json:"text"`
	TS          string `json:"ts"`
	ThreadTS    string `json:"thread_ts"`
	BotID       string `json:"bot_id"`
}

type slackSlashCommand struct {
	TeamID      string
	ChannelID   string
	ChannelName string
	UserID      string
	Text        string
	TriggerID   string
}

// SlackChatHandler handles inbound Slack Events API and slash command requests
// for the current Slack chat integration.
type SlackChatHandler struct {
	ownerStore        *auth.OwnerStore
	inviteStore       *auth.InviteStore
	collaboratorStore *auth.CollaboratorStore
	channelAuth       *auth.ChannelAuthService
	sessionManager    *baldasession.Manager
	actorDispatcher   actortransport.Dispatcher
	goalJobs          goalJobService
	client            *baldaslack.Client
	config            SlackChatConfig
	authToken         string
	baldaProviderName string
	goalMaxIterations int
	logger            zerolog.Logger

	mu         sync.RWMutex
	botUserID  string
	botTeamID  string
	server     *http.Server
	ln         net.Listener
	processSem chan struct{}
	processWG  sync.WaitGroup
}

type slackHandlerParams struct {
	fx.In

	OwnerStore        *auth.OwnerStore
	InviteStore       *auth.InviteStore
	CollaboratorStore *auth.CollaboratorStore
	ChannelAuth       *auth.ChannelAuthService
	SessionManager    *baldasession.Manager
	Dispatcher        actortransport.Dispatcher
	GoalJobs          *baldajobs.JobLifecycleService `optional:"true"`
	SlackClient       *baldaslack.Client
	SlackChatConfig   SlackChatConfig
	AuthToken         string `name:"balda_auth_token"`
	BaldaProviderID   string `name:"balda_provider"`
	MaxIterations     int    `name:"balda_goal_max_iterations"`
	Logger            zerolog.Logger
}

// NewSlackChatHandler creates a Slack chat HTTP receiver.
func NewSlackChatHandler(params slackHandlerParams) *SlackChatHandler {
	h := &SlackChatHandler{
		ownerStore:        params.OwnerStore,
		inviteStore:       params.InviteStore,
		collaboratorStore: params.CollaboratorStore,
		channelAuth:       params.ChannelAuth,
		sessionManager:    params.SessionManager,
		actorDispatcher:   params.Dispatcher,
		goalJobs:          params.GoalJobs,
		client:            params.SlackClient,
		config:            params.SlackChatConfig,
		authToken:         strings.TrimSpace(params.AuthToken),
		baldaProviderName: strings.TrimSpace(params.BaldaProviderID),
		goalMaxIterations: normalizeGoalMaxIterations(params.MaxIterations),
		logger:            params.Logger.With().Str("component", "balda.handler.slack_chat").Logger(),
		processSem:        make(chan struct{}, slackWebhookMaxConcurrentTasks),
	}
	return h
}

// Start authenticates the Slack bot and begins accepting requests.
func (h *SlackChatHandler) Start(ctx context.Context) error { return h.onStart(ctx) }

// Stop gracefully shuts down the Slack receiver.
func (h *SlackChatHandler) Stop(ctx context.Context) error { return h.onStop(ctx) }

func (h *SlackChatHandler) onStart(ctx context.Context) error {
	if !h.config.Enabled {
		h.logger.Info().Msg("slack disabled; skipping server start")
		return nil
	}
	if h.client == nil {
		return fmt.Errorf("slack client is required when slack is enabled")
	}
	teamID, botUserID, err := h.client.AuthTest(ctx)
	if err != nil {
		return fmt.Errorf("slack auth.test: %w", err)
	}
	h.mu.Lock()
	h.botTeamID = teamID
	h.botUserID = botUserID
	h.mu.Unlock()

	eventsPath, err := normalizeSlackPath(h.config.EventsPath, "/slack/events")
	if err != nil {
		return err
	}
	commandsPath, err := normalizeSlackPath(h.config.CommandsPath, "/slack/commands")
	if err != nil {
		return err
	}
	listenAddr := strings.TrimSpace(h.config.ListenAddr)
	if listenAddr == "" {
		listenAddr = ":8091"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(eventsPath, h.handleEvents)
	mux.HandleFunc(commandsPath, h.handleCommand)
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
		return fmt.Errorf("listen slack endpoint on %q: %w", listenAddr, err)
	}
	h.ln = ln
	go func() {
		h.logger.Info().Str("addr", listenAddr).Str("events_path", eventsPath).Str("commands_path", commandsPath).Msg("slack http server starting")
		if err := h.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Error().Err(err).Msg("slack http server error")
		}
	}()
	return nil
}

func normalizeSlackPath(path string, fallback string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fallback, nil
	}
	if !strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("slack path %q must start with /", path)
	}
	return trimmed, nil
}

func (h *SlackChatHandler) onStop(ctx context.Context) error {
	if h.server == nil {
		return nil
	}
	if err := h.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown slack server: %w", err)
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
		return fmt.Errorf("wait for slack processing: %w", ctx.Err())
	}
}

func (h *SlackChatHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	body, ok := h.readAndVerifySlackRequest(w, r)
	if !ok {
		return
	}
	var env slackEventEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		h.logger.Warn().Err(err).Msg("failed to decode slack event payload")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if env.Type == "url_verification" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(env.Challenge))
		return
	}
	if env.Type != "event_callback" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if h.isBotEcho(env.Event) {
		w.WriteHeader(http.StatusOK)
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

func (h *SlackChatHandler) handleCommand(w http.ResponseWriter, r *http.Request) {
	body, ok := h.readAndVerifySlackRequest(w, r)
	if !ok {
		return
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	cmd := slackSlashCommand{
		TeamID:      values.Get("team_id"),
		ChannelID:   values.Get("channel_id"),
		ChannelName: values.Get("channel_name"),
		UserID:      values.Get("user_id"),
		Text:        values.Get("text"),
		TriggerID:   values.Get("trigger_id"),
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
		h.processSlashCommand(context.WithoutCancel(r.Context()), cmd)
	}()
}

func (h *SlackChatHandler) readAndVerifySlackRequest(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
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
	if err := verifySlackSignature(h.config.SigningSecret, r.Header.Get("X-Slack-Request-Timestamp"), r.Header.Get("X-Slack-Signature"), body, time.Now()); err != nil {
		h.logger.Warn().Err(err).Msg("slack signature verification failed")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func verifySlackSignature(secret, timestamp, signature string, body []byte, now time.Time) error {
	trimmedSecret := strings.TrimSpace(secret)
	if trimmedSecret == "" {
		return fmt.Errorf("slack signing secret is required")
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid slack request timestamp")
	}
	requestTime := time.Unix(ts, 0)
	if now.Sub(requestTime) > 5*time.Minute || requestTime.Sub(now) > 5*time.Minute {
		return fmt.Errorf("stale slack request timestamp")
	}
	base := slackSignatureVersion + ":" + strings.TrimSpace(timestamp) + ":" + string(body)
	mac := hmac.New(sha256.New, []byte(trimmedSecret))
	_, _ = mac.Write([]byte(base))
	expected := slackSignatureVersion + "=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(strings.TrimSpace(signature))) {
		return fmt.Errorf("slack request signature mismatch")
	}
	return nil
}

func (h *SlackChatHandler) acquireProcessSlot() (func(), bool) {
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

func (h *SlackChatHandler) processEvent(requestCtx context.Context, env slackEventEnvelope) {
	ctx, cancel := context.WithTimeout(requestCtx, slackWebhookProcessingTimeout)
	defer cancel()
	event := env.Event
	teamID := firstNonEmpty(env.TeamID, h.getBotTeamID())
	switch event.Type {
	case "app_mention":
		locator := baldaslack.NewThreadLocator(teamID, event.Channel, firstNonEmpty(event.ThreadTS, event.TS))
		text := stripSlackBotMentions(event.Text, h.getBotUserID())
		h.handleMessage(ctx, locator, teamID, event.User, event.TS, text, false, true)
	case "message":
		if event.Subtype != "" || event.BotID != "" {
			return
		}
		switch event.ChannelType {
		case "im":
			locator := baldaslack.NewDMLocator(teamID, event.Channel)
			h.handleMessage(ctx, locator, teamID, event.User, event.TS, event.Text, true, true)
		case "channel", "group":
			if event.ChannelType == "group" && !h.config.IncludePrivateChannels {
				return
			}
			if strings.TrimSpace(event.ThreadTS) == "" {
				return
			}
			locator := baldaslack.NewThreadLocator(teamID, event.Channel, event.ThreadTS)
			if _, err := h.sessionManager.GetSession(locator); err != nil {
				return
			}
			h.handleMessage(ctx, locator, teamID, event.User, event.TS, event.Text, false, false)
		}
	}
}

func (h *SlackChatHandler) processSlashCommand(requestCtx context.Context, cmd slackSlashCommand) {
	ctx, cancel := context.WithTimeout(requestCtx, slackWebhookProcessingTimeout)
	defer cancel()
	fields := strings.Fields(strings.TrimSpace(cmd.Text))
	if !slackCommandIsDM(cmd) {
		if len(fields) > 0 && strings.EqualFold(fields[0], commandTopic) {
			args := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(cmd.Text), fields[0]))
			h.handleTopicSlashCommand(ctx, cmd, args)
			return
		}
		if _, err := h.client.PostMessage(ctx, cmd.ChannelID, "", "Use `/balda topic <name>` to create a Balda thread session, or mention Balda in a thread.", true); err != nil {
			h.logger.Warn().Err(err).Str("channel", cmd.ChannelID).Msg("failed to send slack slash channel guidance")
		}
		return
	}
	locator := baldaslack.NewDMLocator(cmd.TeamID, cmd.ChannelID)
	h.handleCommandText(ctx, locator, cmd.TeamID, cmd.UserID, strings.TrimSpace(cmd.Text), true)
}

func slackCommandIsDM(cmd slackSlashCommand) bool {
	return strings.HasPrefix(strings.TrimSpace(cmd.ChannelID), "D") || strings.EqualFold(strings.TrimSpace(cmd.ChannelName), "directmessage")
}

func (h *SlackChatHandler) handleCommandText(ctx context.Context, locator baldasession.SessionLocator, teamID, userID, text string, isDM bool) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		_ = h.sendPlain(ctx, locator, "Usage: /balda <start|topic|goal|cancel|locator|usage|auto|close|user>")
		return
	}
	cmd := strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	args := ""
	if len(fields) > 1 {
		args = strings.Join(fields[1:], " ")
	}
	subject := baldaslack.UserID(teamID, userID)
	if cmd != commandStart && !h.canAccessSubject(ctx, subject) {
		_ = h.sendPlain(ctx, locator, "Only the bot owner or collaborators can use this bot.")
		return
	}
	switch cmd {
	case commandStart:
		h.handleStartCommand(ctx, locator, subject, args, isDM)
	case commandReset, commandRestart:
		h.handleResetCommand(ctx, locator, subject, cmd, args, isDM)
	case commandCancel:
		h.handleCancelCommand(ctx, locator, subject, args)
	case commandLocator:
		h.handleLocatorCommand(ctx, locator, args)
	case commandTopic:
		h.handleTopicCommand(ctx, locator, subject, args, isDM)
	case commandGoal:
		h.handleGoalCommand(ctx, locator, subject, args)
	case commandUsage:
		h.handleUsageCommand(ctx, locator, args)
	case commandAuto:
		h.handleAutoCommand(ctx, locator, args)
	case commandClose:
		h.handleCloseCommand(ctx, locator, subject, args, isDM)
	case commandUser:
		h.handleUserCommand(ctx, locator, subject, args)
	default:
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Unknown command: %s", cmd))
	}
}

func (h *SlackChatHandler) handleAutoCommand(ctx context.Context, locator baldasession.SessionLocator, args string) {
	arg := strings.ToLower(strings.TrimSpace(args))
	switch arg {
	case "":
		status, err := loadAutoStatus(ctx, h.sessionManager, locator)
		if err != nil {
			_ = h.sendPlain(ctx, locator, "Could not read auto mode status.")
			return
		}
		_ = h.sendPlain(ctx, locator, automode.RenderStatus(status))
	case "on":
		if err := dispatchAutoStateUpdate(ctx, h.actorDispatcher, locator, automode.EnableState(time.Now())); err != nil {
			_ = h.sendPlain(ctx, locator, "Could not enable auto mode.")
			return
		}
		_ = h.sendPlain(ctx, locator, automode.RenderStatus(automode.Normalize(automode.Status{
			Enabled:  true,
			State:    automode.StateIdle,
			MaxTurns: automode.DefaultMaxTurns,
		})))
	case "off":
		if err := dispatchAutoStateUpdate(ctx, h.actorDispatcher, locator, automode.DisableState()); err != nil {
			_ = h.sendPlain(ctx, locator, "Could not disable auto mode.")
			return
		}
		_ = h.sendPlain(ctx, locator, automode.RenderStatus(automode.DefaultStatus()))
	default:
		_ = h.sendPlain(ctx, locator, "Usage: /balda auto [on|off]")
	}
}

func (h *SlackChatHandler) handleStartCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string, isDM bool) {
	if !isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in direct messages.")
		return
	}
	if args == "" {
		if h.ownerStore != nil && h.ownerStore.HasOwner() {
			if h.ownerStore.IsOwnerSubject(subject) {
				msg := ownerAlreadyRegisteredMessage
				if bundle, ok := ownerBindTokenBundleMessage(ctx, h.channelAuth, subject); ok {
					msg += "\n\n" + bundle
				}
				_ = h.sendPlain(ctx, locator, msg)
			} else {
				_ = h.sendPlain(ctx, locator, "Bot owner is already registered.")
			}
			return
		}
		_ = h.sendPlain(ctx, locator, "Welcome to Balda Bot!\n\nTo authenticate:\n/balda start owner=<your_owner_token>\n/balda start invite=<your_invite_token>\n\nYou can also DM Balda a channel token.")
		return
	}
	if token, ok := firstFieldToken(args); ok {
		h.handleOwnerBindToken(ctx, locator, subject, token)
		return
	}
	key, value, ok := strings.Cut(args, "=")
	if !ok || strings.TrimSpace(value) == "" {
		_ = h.sendPlain(ctx, locator, "Invalid start format. Use /balda start owner=<token> or /balda start invite=<token>.")
		return
	}
	mode := strings.TrimSpace(key)
	token := strings.TrimSpace(value)
	if mode == userActionInvite {
		h.handleInviteStart(ctx, locator, subject, token)
		return
	}
	if mode != startModeOwner {
		_ = h.sendPlain(ctx, locator, "Invalid start format. Use /balda start owner=<token> or /balda start invite=<token>.")
		return
	}
	if h.ownerStore == nil {
		_ = h.sendPlain(ctx, locator, "Could not register owner. Ask the operator to check Balda storage configuration.")
		return
	}
	if h.ownerStore.HasOwner() {
		if h.ownerStore.IsOwnerSubject(subject) {
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
	registered, err := h.ownerStore.RegisterOwnerSubject(subject)
	if err != nil {
		h.logger.Error().Err(err).Str("user", subject).Msg("slack: failed to register owner")
		_ = h.sendPlain(ctx, locator, "Failed to register owner. Please try again.")
		return
	}
	if !registered {
		_ = h.sendPlain(ctx, locator, "Owner is already registered.")
		return
	}
	_ = h.sendPlain(ctx, locator, "You are now registered as the bot owner.")
}

func (h *SlackChatHandler) handleInviteStart(ctx context.Context, locator baldasession.SessionLocator, subject, token string) {
	if h.ownerStore != nil && h.ownerStore.IsOwnerSubject(subject) {
		_ = h.sendPlain(ctx, locator, "You are already the bot owner.")
		return
	}
	if h.collaboratorStore != nil {
		if _, ok, err := h.collaboratorStore.GetCollaborator(ctx, subject); err == nil && ok {
			_ = h.sendPlain(ctx, locator, "You are already a collaborator.")
			return
		}
	}
	if h.inviteStore == nil || h.collaboratorStore == nil {
		_ = h.sendPlain(ctx, locator, "Failed to process invite. Please try again.")
		return
	}
	invite, err := h.inviteStore.GetInvite(ctx, token)
	if err != nil || invite == nil {
		_ = h.sendPlain(ctx, locator, "This invite token is invalid or has expired.")
		return
	}
	if err := h.collaboratorStore.AddCollaborator(ctx, auth.Collaborator{UserID: subject, AddedBy: invite.CreatedBy, AddedAt: time.Now()}); err != nil {
		_ = h.sendPlain(ctx, locator, "Failed to complete registration. Please try again.")
		return
	}
	_ = h.sendPlain(ctx, locator, "Welcome! You are now a bot collaborator.")
}

func (h *SlackChatHandler) handleOwnerBindToken(ctx context.Context, locator baldasession.SessionLocator, subject, token string) {
	if h.channelAuth == nil {
		_ = h.sendPlain(ctx, locator, "Token authentication is unavailable right now.")
		return
	}
	consumed, err := h.channelAuth.ConsumeOwnerBind(ctx, auth.ChannelSlack, subject, token)
	if err != nil {
		h.logger.Warn().Err(err).Str("user", subject).Msg("slack: failed to consume owner bind token")
		_ = h.sendPlain(ctx, locator, "Failed to process token. Please try again.")
		return
	}
	if !consumed {
		_ = h.sendPlain(ctx, locator, "This token is invalid or has expired.")
		return
	}
	_ = h.sendPlain(ctx, locator, "This Slack account is now connected to the Balda owner.")
}

func (h *SlackChatHandler) handleResetCommand(ctx context.Context, locator baldasession.SessionLocator, subject, cmd, args string, isDM bool) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Usage: /balda %s", cmd))
		return
	}
	info, _ := h.sessionManager.GetSessionInfo(ctx, locator.SessionID)
	if err := h.sessionManager.ResetSession(ctx, locator); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not reset this session.")
		return
	}
	label := autoSessionLabel
	if isDM {
		label = ownerSessionLabel
	}
	if strings.TrimSpace(info.AgentName) != "" {
		label = info.AgentName
	}
	userID := firstNonEmpty(info.UserID, subject)
	if err := h.sessionManager.CreateSession(ctx, baldasession.SessionContext{Locator: locator, UserID: userID}, label); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not restart this session.")
		return
	}
	metadata := h.sessionManager.GetAgentMetadata(h.getProviderName())
	_ = h.sendMarkdown(ctx, locator, welcome.BuildAgentWelcomeMessage(label, locator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers))
	h.sendSessionStartupNotice(ctx, locator, locator.SessionID)
}

func (h *SlackChatHandler) handleCancelCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /balda cancel")
		return
	}
	if err := submitSessionTurnCancelControl(ctx, h.actorDispatcher, locator, subject, "session turn canceled by user", true); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not request cancel.")
		return
	}
	_ = h.sendPlain(ctx, locator, "Cancel requested.")
}

func (h *SlackChatHandler) handleLocatorCommand(ctx context.Context, locator baldasession.SessionLocator, args string) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /balda locator")
		return
	}
	ref := locatorref.Format(locator)
	_ = h.sendPlain(ctx, locator, fmt.Sprintf("Transport: %s\nLocator: %s\n\nUse in scheduler/webhook config:\ntarget: locator\nkey: %s", locator.ChannelType, ref, ref))
}

func (h *SlackChatHandler) handleTopicCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string, isDM bool) {
	if isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in channel messages.")
		return
	}
	topicName := strings.TrimSpace(args)
	if topicName == "" {
		_ = h.sendPlain(ctx, locator, "Usage: /balda topic <name>")
		return
	}
	if err := h.sessionManager.CreateSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject}, topicName); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not create topic session.")
		return
	}
	metadata := h.sessionManager.GetAgentMetadata(h.getProviderName())
	_ = h.sendMarkdown(ctx, locator, welcome.BuildAgentWelcomeMessage(topicName, locator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers))
}

func (h *SlackChatHandler) handleTopicSlashCommand(ctx context.Context, cmd slackSlashCommand, args string) {
	subject := baldaslack.UserID(cmd.TeamID, cmd.UserID)
	if !h.canAccessSubject(ctx, subject) {
		if _, err := h.client.PostMessage(ctx, cmd.ChannelID, "", "Only the bot owner or collaborators can use this bot.", true); err != nil {
			h.logger.Warn().Err(err).Str("channel", cmd.ChannelID).Msg("failed to send slack access denial")
		}
		return
	}
	topicName := strings.TrimSpace(args)
	if topicName == "" {
		if _, err := h.client.PostMessage(ctx, cmd.ChannelID, "", "Usage: `/balda topic <name>`", true); err != nil {
			h.logger.Warn().Err(err).Str("channel", cmd.ChannelID).Msg("failed to send slack topic usage")
		}
		return
	}
	seedTS, err := h.client.PostMessage(ctx, cmd.ChannelID, "", "Balda topic: "+topicName, true)
	if err != nil {
		h.logger.Warn().Err(err).Str("channel", cmd.ChannelID).Msg("failed to create slack topic seed message")
		return
	}
	locator := baldaslack.NewThreadLocator(cmd.TeamID, cmd.ChannelID, seedTS)
	if err := h.sessionManager.CreateSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject}, topicName); err != nil {
		h.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to create slack topic session")
		if _, sendErr := h.client.PostMessage(ctx, cmd.ChannelID, seedTS, "Could not create topic session.", true); sendErr != nil {
			h.logger.Warn().Err(sendErr).Str("channel", cmd.ChannelID).Msg("failed to send slack topic creation failure")
		}
		return
	}
	metadata := h.sessionManager.GetAgentMetadata(h.getProviderName())
	_ = h.sendMarkdown(ctx, locator, welcome.BuildAgentWelcomeMessage(topicName, locator.SessionID, metadata.Type, metadata.Model, metadata.MCPServers))
}

func (h *SlackChatHandler) handleGoalCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string) {
	objective := strings.TrimSpace(args)
	if objective == "" {
		_ = h.sendPlain(ctx, locator, "Usage:\n/balda goal <objective>\n/balda goal clear")
		return
	}
	if strings.EqualFold(objective, "clear") {
		if err := submitGoalClearControl(ctx, h.actorDispatcher, locator, subject, "goal cleared by user", true); err != nil {
			_ = h.sendPlain(ctx, locator, "Could not clear goal run.")
		}
		return
	}
	if h.goalJobs != nil {
		activeGoals, err := h.goalJobs.ListActiveGoalJobsBySession(ctx, locator.SessionID)
		if err != nil {
			_ = h.sendPlain(ctx, locator, "Could not start goal run.")
			return
		}
		if len(activeGoals) > 0 {
			_ = h.sendPlain(ctx, locator, "A goal run is already active for this session.")
			return
		}
	}
	env, err := goalcmd.JobEnvelopeWithOptions(locator, deliveryfmt.Options{Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatMarkdown}, ProgressPolicy: deliveryfmt.ProgressPolicy{Typing: false, Thinking: false, PlanUpdates: true}}, objective, subject, h.goalMaxIterations)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Could not start goal run.")
		return
	}
	if _, err := h.actorDispatcher.Dispatch(ctx, env); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not start goal run.")
	}
}

func (h *SlackChatHandler) handleUsageCommand(ctx context.Context, locator baldasession.SessionLocator, args string) {
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /balda usage")
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

func (h *SlackChatHandler) handleCloseCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string, isDM bool) {
	if !isDM {
		_ = h.sendPlain(ctx, locator, "This command is only available in direct messages.")
		return
	}
	if strings.TrimSpace(args) != "" {
		_ = h.sendPlain(ctx, locator, "Usage: /balda close")
		return
	}
	_ = submitSessionCancelControl(ctx, h.actorDispatcher, locator, subject, "session canceled by close command", false)
	if err := h.sessionManager.ResetSession(ctx, locator); err != nil {
		_ = h.sendPlain(ctx, locator, "Could not close this session.")
		return
	}
	_ = h.sendPlain(ctx, locator, "Session history reset.")
}

func (h *SlackChatHandler) handleUserCommand(ctx context.Context, locator baldasession.SessionLocator, subject, args string) {
	if h.ownerStore == nil || !h.ownerStore.IsOwnerSubject(subject) {
		_ = h.sendPlain(ctx, locator, "This command is only for the owner.")
		return
	}
	fields := strings.Fields(args)
	if len(fields) == 0 {
		_ = h.sendPlain(ctx, locator, "Usage:\n/balda user add\n/balda user list\n/balda user remove <user_id>")
		return
	}
	switch fields[0] {
	case userActionAdd, userActionInvite:
		if h.inviteStore == nil {
			_ = h.sendPlain(ctx, locator, "Invite store is unavailable.")
			return
		}
		token, _, err := h.inviteStore.CreateInvite(ctx, subject)
		if err != nil {
			_ = h.sendPlain(ctx, locator, "Failed to create invite. Please try again.")
			return
		}
		_ = h.sendPlain(ctx, locator, fmt.Sprintf("Invite token created:\n%s\n\nHave the collaborator run:\n/balda start invite=%s", token, token))
	case userActionList:
		if h.collaboratorStore == nil {
			_ = h.sendPlain(ctx, locator, "Collaborator store is unavailable.")
			return
		}
		collaborators, err := h.collaboratorStore.ListCollaborators(ctx)
		if err != nil {
			_ = h.sendPlain(ctx, locator, "Failed to list collaborators. Please try again.")
			return
		}
		lines := []string{"Collaborators:"}
		if len(collaborators) == 0 {
			lines = []string{"No collaborators"}
		}
		for _, c := range collaborators {
			lines = append(lines, fmt.Sprintf("%s - added %s", c.UserID, c.AddedAt.Format("2006-01-02 15:04")))
		}
		_ = h.sendPlain(ctx, locator, strings.Join(lines, "\n"))
	case userActionRemove:
		if len(fields) < 2 {
			_ = h.sendPlain(ctx, locator, "Usage: /balda user remove <user_id>")
			return
		}
		if h.collaboratorStore == nil {
			_ = h.sendPlain(ctx, locator, "Collaborator store is unavailable.")
			return
		}
		if err := h.collaboratorStore.RemoveCollaborator(ctx, fields[1]); err != nil {
			_ = h.sendPlain(ctx, locator, "Could not remove collaborator. Please try again.")
			return
		}
		_ = h.sendPlain(ctx, locator, "Collaborator removed: "+fields[1])
	default:
		_ = h.sendPlain(ctx, locator, "Usage:\n/balda user add\n/balda user list\n/balda user remove <user_id>")
	}
}

func (h *SlackChatHandler) handleMessage(ctx context.Context, locator baldasession.SessionLocator, teamID, userID, messageID, text string, isDM bool, createIfMissing bool) {
	subject := baldaslack.UserID(teamID, userID)
	if h.ownerStore == nil || !h.ownerStore.HasOwner() {
		return
	}
	if isDM {
		if token, ok := firstFieldToken(text); ok {
			h.handleOwnerBindToken(ctx, locator, subject, token)
			return
		}
	}
	if !h.canAccessSubject(ctx, subject) {
		return
	}
	if strings.TrimSpace(text) == "" {
		return
	}
	ts, err := h.getOrCreateSession(ctx, locator, subject, isDM, createIfMissing)
	if err != nil {
		return
	}
	progressPolicy := baldachannel.ProgressPolicy{Typing: false, Thinking: false, PlanUpdates: true}
	payload := turncmd.SessionTurnPayload{
		Text:           text,
		Locator:        locator,
		UserID:         ts.GetUserID(),
		AgentSessionID: ts.GetAgentSessionID(),
		MessageID:      slackMessageID(messageID),
		DeliveryOptions: deliveryfmt.Options{
			Profile:        deliveryfmt.Profile{Format: deliveryfmt.FormatMarkdown},
			ProgressPolicy: progressPolicy,
		},
		ProgressPolicy: progressPolicy,
		Deliver:        true,
		Source:         "slack",
		DedupeKey:      "slack:" + strings.TrimSpace(messageID),
	}
	env, err := turncmd.SessionTurnEnvelope(payload)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Failed to publish your message for processing. Please try again.")
		return
	}
	if _, err := h.actorDispatcher.Dispatch(ctx, env); err != nil {
		if baldaexecution.IsCommandQueueFull(err) {
			_ = h.sendPlain(ctx, locator, "Session command queue is full. Please wait or use /balda cancel.")
			return
		}
		_ = h.sendPlain(ctx, locator, "Failed to publish your message for processing. Please try again.")
	}
}

func (h *SlackChatHandler) getOrCreateSession(ctx context.Context, locator baldasession.SessionLocator, subject string, isDM bool, createIfMissing bool) (*baldasession.TopicSession, error) {
	if existing, _ := h.sessionManager.GetSession(locator); existing != nil {
		return existing, nil
	}
	ts, err := h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject})
	if err == nil && ts != nil {
		h.sendSessionWelcome(ctx, locator, ts, isDM)
		return ts, nil
	}
	if err != nil && !errors.Is(err, baldasession.ErrNoPersistedSession) {
		_ = h.sendPlain(ctx, locator, "Could not restore this session. Please try again.")
		return nil, err
	}
	if !createIfMissing {
		return nil, baldasession.ErrNoPersistedSession
	}
	label := autoSessionLabel
	if isDM {
		label = ownerSessionLabel
	}
	ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{Locator: locator, UserID: subject}, label)
	if err != nil {
		_ = h.sendPlain(ctx, locator, "Could not start this session. Please try again.")
		return nil, err
	}
	h.sendSessionWelcome(ctx, locator, ts, isDM)
	return ts, nil
}

func (h *SlackChatHandler) sendSessionWelcome(ctx context.Context, locator baldasession.SessionLocator, ts *baldasession.TopicSession, isDM bool) {
	label := autoSessionLabel
	if isDM {
		label = ownerSessionLabel
	}
	metadata := h.sessionManager.GetAgentMetadata(h.getProviderName())
	_ = h.sendMarkdown(ctx, locator, welcome.BuildAgentWelcomeMessage(label, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers))
	h.sendSessionStartupNotice(ctx, locator, ts.GetSessionID())
}

func (h *SlackChatHandler) canAccessSubject(ctx context.Context, subject string) bool {
	if h.ownerStore != nil && h.ownerStore.IsOwnerSubject(subject) {
		return true
	}
	if h.collaboratorStore == nil {
		return false
	}
	_, found, err := h.collaboratorStore.GetCollaborator(ctx, subject)
	return err == nil && found
}

func (h *SlackChatHandler) getProviderName() string {
	if h.sessionManager == nil {
		return h.baldaProviderName
	}
	return firstNonEmpty(h.sessionManager.BaldaProviderID(), h.baldaProviderName)
}

func (h *SlackChatHandler) sendPlain(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	return sendPlain(ctx, h.actorDispatcher, slackChatHandlerActorAddress, locator, text)
}

func (h *SlackChatHandler) sendMarkdown(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	return sendMarkdown(ctx, h.actorDispatcher, slackChatHandlerActorAddress, locator, text)
}

func (h *SlackChatHandler) sendSessionStartupNotice(ctx context.Context, locator baldasession.SessionLocator, sessionID string) {
	notice := strings.TrimSpace(h.sessionManager.TakeStartupNotice(sessionID))
	if notice == "" {
		return
	}
	_ = h.sendPlain(ctx, locator, notice)
}

func (h *SlackChatHandler) isBotEcho(event slackEvent) bool {
	if event.BotID != "" {
		return true
	}
	botUserID := h.getBotUserID()
	return botUserID != "" && event.User == botUserID
}

func (h *SlackChatHandler) getBotUserID() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.botUserID
}

func (h *SlackChatHandler) getBotTeamID() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.botTeamID
}

func stripSlackBotMentions(text string, botUserID string) string {
	trimmed := strings.TrimSpace(text)
	if strings.TrimSpace(botUserID) == "" {
		return trimmed
	}
	mention := "<@" + strings.TrimSpace(botUserID) + ">"
	for strings.HasPrefix(trimmed, mention) {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, mention))
	}
	return trimmed
}

func slackMessageID(ts string) int {
	var out int
	for _, r := range strings.TrimSpace(ts) {
		if r >= '0' && r <= '9' {
			out = out*10 + int(r-'0')
			if out > 1_000_000_000 {
				return out
			}
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
