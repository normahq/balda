package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"google.golang.org/adk/runner"
)

const (
	defaultInboundWebhookListenAddr = "127.0.0.1:8090"

	inboundWebhookReadHeaderTimeout = 5 * time.Second
	inboundWebhookReadTimeout       = 10 * time.Second
	inboundWebhookWriteTimeout      = 30 * time.Second
	inboundWebhookIdleTimeout       = 60 * time.Second
	inboundWebhookMaxBodyBytes      = 1 << 20
)

const (
	inboundWebhookStatusAccepted = "accepted"
	inboundWebhookStatusError    = "error"

	inboundWebhookCodeInvalidMethod   = "invalid_method"
	inboundWebhookCodeRouteNotFound   = "route_not_found"
	inboundWebhookCodeInvalidPayload  = "invalid_payload"
	inboundWebhookCodeSessionNotFound = "session_not_found"
	inboundWebhookCodeQueueFull       = "queue_full"
	inboundWebhookCodeDispatchFailed  = "dispatch_failed"
)

// InboundWebhookRouteConfig configures one inbound webhook route.
type InboundWebhookRouteConfig struct {
	Path           string
	PromptTemplate string
}

// InboundWebhookConfig controls inbound webhook routing and dispatch behavior.
type InboundWebhookConfig struct {
	Enabled    bool
	ListenAddr string
	Routes     map[string]InboundWebhookRouteConfig
}

type inboundWebhookRoute struct {
	Name           string
	Path           string
	PromptTemplate *template.Template
}

type inboundWebhookSessionManager interface {
	GetSession(locator baldasession.SessionLocator) (*baldasession.TopicSession, error)
	EnsureSession(ctx context.Context, sessionCtx baldasession.SessionContext, label string) (*baldasession.TopicSession, error)
	RestoreSession(ctx context.Context, sessionCtx baldasession.SessionContext) (*baldasession.TopicSession, error)
}

type inboundTurnExecutor interface {
	runTurnTaskWithDelivery(
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
	) error
}

type inboundWebhookParams struct {
	fx.In

	LC         fx.Lifecycle
	Config     InboundWebhookConfig
	Sessions   *baldasession.Manager
	Dispatcher *TurnDispatcher
	Balda      *BaldaHandler
	OwnerStore *auth.OwnerStore
	Logger     zerolog.Logger
}

// InboundWebhookReceiver receives inbound webhook events and dispatches them into bound session turns.
type InboundWebhookReceiver struct {
	enabled    bool
	listenAddr string
	routes     map[string]inboundWebhookRoute
	sessions   inboundWebhookSessionManager
	dispatch   turnQueue
	balda      inboundTurnExecutor
	owner      *auth.OwnerStore
	logger     zerolog.Logger

	metrics inboundWebhookMetrics

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	started  bool
}

type inboundWebhookMetrics struct {
	accepted    atomic.Uint64
	invalid     atomic.Uint64
	notFound    atomic.Uint64
	queueFull   atomic.Uint64
	dispatchErr atomic.Uint64
}

type inboundWebhookTemplateData struct {
	RequestID string
	Path      string
	Method    string
	RawBody   string
	Headers   map[string]string
}

type inboundWebhookAcceptedResponse struct {
	Status        string `json:"status"`
	RequestID     string `json:"request_id"`
	SessionID     string `json:"session_id"`
	QueuePosition int    `json:"queue_position"`
}

type inboundWebhookErrorResponse struct {
	Status    string                    `json:"status"`
	RequestID string                    `json:"request_id"`
	Error     inboundWebhookErrorDetail `json:"error"`
}

type inboundWebhookErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type inboundWebhookHTTPError struct {
	status  int
	code    string
	message string
	cause   error
}

func (e *inboundWebhookHTTPError) Error() string {
	if e == nil {
		return ""
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func newInboundWebhookHTTPError(status int, code, message string, cause error) *inboundWebhookHTTPError {
	return &inboundWebhookHTTPError{
		status:  status,
		code:    code,
		message: message,
		cause:   cause,
	}
}

func NewInboundWebhookReceiver(params inboundWebhookParams) (*InboundWebhookReceiver, error) {
	normalized, err := normalizeInboundWebhookConfig(params.Config)
	if err != nil {
		return nil, err
	}

	receiver := &InboundWebhookReceiver{
		enabled:    normalized.Enabled,
		listenAddr: normalized.ListenAddr,
		routes:     normalized.Routes,
		sessions:   params.Sessions,
		dispatch:   params.Dispatcher,
		balda:      params.Balda,
		owner:      params.OwnerStore,
		logger:     params.Logger.With().Str("component", "balda.inbound_webhook").Logger(),
	}

	if !receiver.enabled {
		return receiver, nil
	}
	if receiver.sessions == nil {
		return nil, fmt.Errorf("balda session manager is required for inbound webhooks")
	}
	if receiver.dispatch == nil {
		return nil, fmt.Errorf("balda turn dispatcher is required for inbound webhooks")
	}
	if receiver.balda == nil {
		return nil, fmt.Errorf("balda handler is required for inbound webhooks")
	}
	if receiver.owner == nil {
		return nil, fmt.Errorf("balda owner store is required for inbound webhooks")
	}

	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return receiver.start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return receiver.stop(ctx)
		},
	})

	return receiver, nil
}

type normalizedInboundWebhookConfig struct {
	Enabled    bool
	ListenAddr string
	Routes     map[string]inboundWebhookRoute
}

func normalizeInboundWebhookConfig(cfg InboundWebhookConfig) (normalizedInboundWebhookConfig, error) {
	normalized := normalizedInboundWebhookConfig{
		Enabled:    cfg.Enabled,
		ListenAddr: normalizeInboundWebhookListenAddr(cfg.ListenAddr),
		Routes:     make(map[string]inboundWebhookRoute),
	}
	if !cfg.Enabled {
		return normalized, nil
	}

	if len(cfg.Routes) == 0 {
		return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes is required when webhooks are enabled")
	}

	seenPaths := make(map[string]string, len(cfg.Routes))
	for rawName, rawRoute := range cfg.Routes {
		routeName := strings.TrimSpace(rawName)
		if routeName == "" {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes key is required")
		}

		path, err := normalizeInboundWebhookPath(rawRoute.Path)
		if err != nil {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.path: %w", routeName, err)
		}
		if existingName, exists := seenPaths[path]; exists {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.path duplicates route %q", routeName, existingName)
		}
		seenPaths[path] = routeName

		templateText := strings.TrimSpace(rawRoute.PromptTemplate)
		if templateText == "" {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.prompt_template is required", routeName)
		}
		tmpl, err := template.New("inbound_webhook." + routeName).Option("missingkey=error").Parse(templateText)
		if err != nil {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("invalid balda.webhooks.routes.%s.prompt_template: %w", routeName, err)
		}

		normalized.Routes[path] = inboundWebhookRoute{
			Name:           routeName,
			Path:           path,
			PromptTemplate: tmpl,
		}
	}

	return normalized, nil
}

func normalizeInboundWebhookListenAddr(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultInboundWebhookListenAddr
	}
	return trimmed
}

func normalizeInboundWebhookPath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return trimmed, nil
}

func (r *InboundWebhookReceiver) start(_ context.Context) error {
	if !r.enabled {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}

	listener, err := net.Listen("tcp", r.listenAddr)
	if err != nil {
		return fmt.Errorf("listen inbound webhook on %q: %w", r.listenAddr, err)
	}

	server := &http.Server{
		Handler:           http.HandlerFunc(r.handleInboundWebhook),
		ReadHeaderTimeout: inboundWebhookReadHeaderTimeout,
		ReadTimeout:       inboundWebhookReadTimeout,
		WriteTimeout:      inboundWebhookWriteTimeout,
		IdleTimeout:       inboundWebhookIdleTimeout,
	}

	r.listener = listener
	r.server = server
	r.started = true

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			r.logger.Error().Err(serveErr).Msg("inbound webhook server failed")
		}
	}()

	r.logger.Info().
		Str("listen_addr", listener.Addr().String()).
		Str("paths", strings.Join(r.routePaths(), ", ")).
		Int("routes", len(r.routes)).
		Msg("inbound webhook server started")
	return nil
}

func (r *InboundWebhookReceiver) stop(ctx context.Context) error {
	r.mu.Lock()
	server := r.server
	r.server = nil
	r.listener = nil
	r.started = false
	r.mu.Unlock()

	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown inbound webhook server: %w", err)
	}
	return nil
}

func (r *InboundWebhookReceiver) routePaths() []string {
	paths := make([]string, 0, len(r.routes))
	for path := range r.routes {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func (r *InboundWebhookReceiver) handleInboundWebhook(w http.ResponseWriter, req *http.Request) {
	requestID := requestIDFromInboundWebhookRequest(req)
	if req.Method != http.MethodPost {
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusMethodNotAllowed,
			inboundWebhookCodeInvalidMethod,
			"method must be POST",
			nil,
		))
		return
	}

	route, ok := r.routes[req.URL.Path]
	if !ok {
		r.metrics.notFound.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusNotFound,
			inboundWebhookCodeRouteNotFound,
			fmt.Sprintf("no inbound webhook route for path %q", req.URL.Path),
			nil,
		))
		return
	}

	rawBody, readErr := readInboundWebhookBody(req.Body)
	if readErr != nil {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"invalid request body",
			readErr,
		))
		return
	}

	prompt, renderErr := renderInboundWebhookPrompt(route, inboundWebhookTemplateData{
		RequestID: requestID,
		Path:      req.URL.Path,
		Method:    req.Method,
		RawBody:   rawBody,
		Headers:   inboundWebhookHeaders(req.Header),
	})
	if renderErr != nil {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"failed to render prompt template",
			renderErr,
		))
		return
	}

	env := ownerEnvelope(prompt)
	target, targetErr := resolveEnvelopeTarget(req.Context(), r.owner, env.Target)
	if targetErr != nil {
		r.metrics.notFound.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusNotFound,
			inboundWebhookCodeSessionNotFound,
			"failed to resolve webhook target",
			targetErr,
		))
		return
	}

	ts, resolveErr := r.resolveInboundWebhookSession(req.Context(), target)
	if resolveErr != nil {
		r.metrics.notFound.Add(1)
		r.writeInboundWebhookError(w, requestID, resolveErr)
		return
	}

	position, enqueueErr := r.dispatch.Enqueue(TurnTask{
		SessionID: ts.GetSessionID(),
		Run: func(runCtx context.Context) error {
			if _, getErr := r.sessions.GetSession(target.Locator); getErr != nil {
				r.logger.Debug().
					Str("request_id", requestID).
					Str("session_id", target.Locator.SessionID).
					Msg("dropping inbound webhook turn for inactive session")
				return nil
			}

			return r.balda.runTurnTaskWithDelivery(
				runCtx,
				env.Content,
				ts.GetRunner(),
				ts.GetUserID(),
				ts.GetSessionID(),
				ts.GetAgentSessionID(),
				target.Locator,
				0,
				target.TopicID,
				inboundWebhookProgressPolicy(),
				env.ReportTo != nil,
			)
		},
	})
	if enqueueErr != nil {
		if errors.Is(enqueueErr, ErrTurnQueueFull) {
			r.metrics.queueFull.Add(1)
			r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
				http.StatusTooManyRequests,
				inboundWebhookCodeQueueFull,
				"turn queue is full",
				enqueueErr,
			))
			return
		}

		r.metrics.dispatchErr.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusInternalServerError,
			inboundWebhookCodeDispatchFailed,
			"failed to dispatch inbound turn",
			enqueueErr,
		))
		return
	}

	r.metrics.accepted.Add(1)
	r.logger.Info().
		Str("request_id", requestID).
		Str("route", route.Name).
		Str("path", route.Path).
		Str("session_id", target.Locator.SessionID).
		Str("channel_type", target.Locator.ChannelType).
		Str("address_key", target.Locator.AddressKey).
		Int("queue_position", position).
		Msg("inbound webhook accepted")

	writeInboundWebhookJSON(w, http.StatusAccepted, inboundWebhookAcceptedResponse{
		Status:        inboundWebhookStatusAccepted,
		RequestID:     requestID,
		SessionID:     target.Locator.SessionID,
		QueuePosition: position,
	})
}

func requestIDFromInboundWebhookRequest(req *http.Request) string {
	requestID := strings.TrimSpace(req.Header.Get("X-Request-Id"))
	if requestID != "" {
		return requestID
	}
	return fmt.Sprintf("inbound-%d", time.Now().UnixNano())
}

func readInboundWebhookBody(body io.ReadCloser) (string, error) {
	defer func() { _ = body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(body, inboundWebhookMaxBodyBytes+1))
	if err != nil {
		return "", err
	}
	if len(payload) > inboundWebhookMaxBodyBytes {
		return "", fmt.Errorf("request body exceeds %d bytes", inboundWebhookMaxBodyBytes)
	}
	return string(payload), nil
}

func inboundWebhookHeaders(header http.Header) map[string]string {
	out := make(map[string]string, len(header))
	for name, values := range header {
		if len(values) == 0 {
			out[name] = ""
			continue
		}
		out[name] = values[0]
	}
	return out
}

func renderInboundWebhookPrompt(route inboundWebhookRoute, data inboundWebhookTemplateData) (string, error) {
	var buf bytes.Buffer
	if err := route.PromptTemplate.Execute(&buf, data); err != nil {
		return "", err
	}
	prompt := strings.TrimSpace(buf.String())
	if prompt == "" {
		return "", fmt.Errorf("rendered prompt is empty")
	}
	return prompt, nil
}

func (r *InboundWebhookReceiver) resolveInboundWebhookSession(
	ctx context.Context,
	target resolvedEnvelopeTarget,
) (*baldasession.TopicSession, *inboundWebhookHTTPError) {
	locator := target.Locator
	ts, err := r.sessions.GetSession(locator)
	if err != nil {
		userID := strings.TrimSpace(target.UserID)
		if userID == "" {
			return nil, newInboundWebhookHTTPError(
				http.StatusNotFound,
				inboundWebhookCodeSessionNotFound,
				fmt.Sprintf("session %q has no user id for restore", locator.SessionID),
				nil,
			)
		}
		ts, err = r.sessions.RestoreSession(ctx, baldasession.SessionContext{
			Locator: locator,
			UserID:  userID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return nil, newInboundWebhookHTTPError(
					http.StatusNotFound,
					inboundWebhookCodeSessionNotFound,
					fmt.Sprintf("session %q restore failed", locator.SessionID),
					err,
				)
			}
			ts, err = r.sessions.EnsureSession(ctx, baldasession.SessionContext{
				Locator: locator,
				UserID:  userID,
			}, ownerSessionLabel)
			if err != nil {
				return nil, newInboundWebhookHTTPError(
					http.StatusNotFound,
					inboundWebhookCodeSessionNotFound,
					fmt.Sprintf("session %q create failed", locator.SessionID),
					err,
				)
			}
		}
	}

	return ts, nil
}

func (r *InboundWebhookReceiver) writeInboundWebhookError(
	w http.ResponseWriter,
	requestID string,
	handlerErr *inboundWebhookHTTPError,
) {
	if handlerErr == nil {
		handlerErr = newInboundWebhookHTTPError(
			http.StatusInternalServerError,
			inboundWebhookCodeDispatchFailed,
			"internal error",
			nil,
		)
	}

	evt := r.logger.Warn().
		Str("request_id", requestID).
		Str("error_code", handlerErr.code).
		Int("status_code", handlerErr.status)
	if handlerErr.cause != nil {
		evt = evt.Err(handlerErr.cause)
	}
	evt.Msg("inbound webhook rejected")

	writeInboundWebhookJSON(w, handlerErr.status, inboundWebhookErrorResponse{
		Status:    inboundWebhookStatusError,
		RequestID: requestID,
		Error: inboundWebhookErrorDetail{
			Code:    handlerErr.code,
			Message: handlerErr.message,
		},
	})
}

func writeInboundWebhookJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return
	}
}

func inboundWebhookProgressPolicy() baldachannel.ProgressPolicy {
	return baldachannel.ProgressPolicy{
		Typing:   false,
		Thinking: false,
	}
}
