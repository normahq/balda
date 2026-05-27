package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
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
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
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
	inboundWebhookCodeUnauthorized    = "unauthorized"
	inboundWebhookCodeInvalidPayload  = "invalid_payload"
	inboundWebhookCodeSessionNotFound = "session_not_found"
	inboundWebhookCodeQueueFull       = "queue_full"
	inboundWebhookCodeDispatchFailed  = "dispatch_failed"
)

const (
	inboundWebhookRouteModeTask    = "task"
	inboundWebhookRouteModeSession = "session"
)

const (
	inboundWebhookAuthTypeNone   = "none"
	inboundWebhookAuthTypeHeader = "header"
)

const (
	inboundWebhookDedupeSourceRequestID = "request_id"
	inboundWebhookDedupeSourceHeader    = "header"
	inboundWebhookDedupeSourceBodySHA   = "body_sha256"
)

// InboundWebhookRouteConfig configures one inbound webhook route.
type InboundWebhookRouteConfig struct {
	Path           string
	PromptTemplate string
	Envelope       InboundWebhookRouteEnvelopeConfig
	Auth           InboundWebhookRouteAuthConfig
	Dedupe         InboundWebhookRouteDedupeConfig
}

type InboundWebhookRouteEnvelopeConfig struct {
	Target   string
	Key      string
	Mode     string
	ReportTo *InboundWebhookRouteTargetConfig
}

type InboundWebhookRouteTargetConfig struct {
	Target string
	Key    string
}

type InboundWebhookRouteAuthConfig struct {
	Type   string
	Header string
	Value  string
}

type InboundWebhookRouteDedupeConfig struct {
	Source string
	Header string
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
	Target         envelopeTarget
	Mode           string
	ReportTo       *envelopeTarget
	Auth           inboundWebhookAuthPolicy
	Dedupe         inboundWebhookDedupePolicy
}

type inboundWebhookAuthPolicy struct {
	Type   string
	Header string
	Value  string
}

type inboundWebhookDedupePolicy struct {
	Source string
	Header string
}

type inboundTurnExecutor interface {
	submitWebhookTask(ctx context.Context, payload sessionTurnPayload, routeName string, requestID string) (*swarm.CommandPublishResult, string, error)
	submitSessionTurn(ctx context.Context, payload sessionTurnPayload) (*swarm.CommandPublishResult, error)
}

type inboundWebhookParams struct {
	fx.In

	LC         fx.Lifecycle
	Config     InboundWebhookConfig
	Balda      *BaldaHandler
	OwnerStore *auth.OwnerStore
	Logger     zerolog.Logger
}

// InboundWebhookReceiver receives inbound webhook events and dispatches them into bound session turns.
type InboundWebhookReceiver struct {
	enabled    bool
	listenAddr string
	routes     map[string]inboundWebhookRoute
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
	accepted     atomic.Uint64
	invalid      atomic.Uint64
	notFound     atomic.Uint64
	unauthorized atomic.Uint64
	queueFull    atomic.Uint64
	dispatchErr  atomic.Uint64
}

type inboundWebhookTemplateData struct {
	RequestID string
	Path      string
	Method    string
	RawBody   string
	Headers   map[string]string
}

type inboundWebhookAcceptedResponse struct {
	Status    string `json:"status"`
	Accepted  bool   `json:"accepted"`
	RequestID string `json:"request_id"`
	MessageID string `json:"message_id"`
	TaskID    string `json:"task_id"`
	SessionID string `json:"session_id"`
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"sequence"`
	Duplicate bool   `json:"duplicate,omitempty"`
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
		balda:      params.Balda,
		owner:      params.OwnerStore,
		logger:     params.Logger.With().Str("component", "balda.inbound_webhook").Logger(),
	}

	if !receiver.enabled {
		return receiver, nil
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
		target := envelopeTarget{
			Target: strings.TrimSpace(rawRoute.Envelope.Target),
			Key:    strings.TrimSpace(rawRoute.Envelope.Key),
		}
		if target.Target == "" && target.Key == "" {
			target = ownerEnvelopeTarget()
		}
		if target.Target == "" {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.target is required", routeName)
		}
		if target.Key == "" {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.key is required", routeName)
		}
		var reportTo *envelopeTarget
		if rawRoute.Envelope.ReportTo != nil {
			reportTo = &envelopeTarget{
				Target: strings.TrimSpace(rawRoute.Envelope.ReportTo.Target),
				Key:    strings.TrimSpace(rawRoute.Envelope.ReportTo.Key),
			}
			if reportTo.Target == "" {
				return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.report_to.target is required", routeName)
			}
			if reportTo.Key == "" {
				return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.report_to.key is required", routeName)
			}
		}
		mode, err := normalizeInboundWebhookRouteMode(rawRoute.Envelope.Mode)
		if err != nil {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.mode: %w", routeName, err)
		}
		authPolicy, err := normalizeInboundWebhookAuthPolicy(rawRoute.Auth)
		if err != nil {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.auth: %w", routeName, err)
		}
		dedupePolicy, err := normalizeInboundWebhookDedupePolicy(rawRoute.Dedupe)
		if err != nil {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.dedupe: %w", routeName, err)
		}

		normalized.Routes[path] = inboundWebhookRoute{
			Name:           routeName,
			Path:           path,
			PromptTemplate: tmpl,
			Target:         target,
			Mode:           mode,
			ReportTo:       reportTo,
			Auth:           authPolicy,
			Dedupe:         dedupePolicy,
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

func normalizeInboundWebhookRouteMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return inboundWebhookRouteModeTask, nil
	}
	switch mode {
	case inboundWebhookRouteModeTask, inboundWebhookRouteModeSession:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported mode %q", raw)
	}
}

func normalizeInboundWebhookAuthPolicy(raw InboundWebhookRouteAuthConfig) (inboundWebhookAuthPolicy, error) {
	policy := inboundWebhookAuthPolicy{
		Type:   strings.ToLower(strings.TrimSpace(raw.Type)),
		Header: strings.TrimSpace(raw.Header),
		Value:  strings.TrimSpace(raw.Value),
	}
	if policy.Type == "" {
		policy.Type = inboundWebhookAuthTypeNone
	}
	switch policy.Type {
	case inboundWebhookAuthTypeNone:
		return inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone}, nil
	case inboundWebhookAuthTypeHeader:
		if policy.Header == "" {
			return inboundWebhookAuthPolicy{}, fmt.Errorf("header is required for type=%q", inboundWebhookAuthTypeHeader)
		}
		if policy.Value == "" {
			return inboundWebhookAuthPolicy{}, fmt.Errorf("value is required for type=%q", inboundWebhookAuthTypeHeader)
		}
		return policy, nil
	default:
		return inboundWebhookAuthPolicy{}, fmt.Errorf("unsupported type %q", raw.Type)
	}
}

func normalizeInboundWebhookDedupePolicy(raw InboundWebhookRouteDedupeConfig) (inboundWebhookDedupePolicy, error) {
	policy := inboundWebhookDedupePolicy{
		Source: strings.ToLower(strings.TrimSpace(raw.Source)),
		Header: strings.TrimSpace(raw.Header),
	}
	if policy.Source == "" && policy.Header != "" {
		policy.Source = inboundWebhookDedupeSourceHeader
	}
	if policy.Source == "" {
		policy.Source = inboundWebhookDedupeSourceRequestID
	}
	switch policy.Source {
	case inboundWebhookDedupeSourceRequestID, inboundWebhookDedupeSourceBodySHA:
		return policy, nil
	case inboundWebhookDedupeSourceHeader:
		if policy.Header == "" {
			return inboundWebhookDedupePolicy{}, fmt.Errorf("header is required for source=%q", inboundWebhookDedupeSourceHeader)
		}
		return policy, nil
	default:
		return inboundWebhookDedupePolicy{}, fmt.Errorf("unsupported source %q", raw.Source)
	}
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
	if authErr := authorizeInboundWebhookRequest(req, route.Auth); authErr != nil {
		r.metrics.unauthorized.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusUnauthorized,
			inboundWebhookCodeUnauthorized,
			"webhook request is unauthorized",
			authErr,
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
	target, targetErr := resolveEnvelopeTarget(req.Context(), r.owner, route.Target)
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
	var reportTo *baldasession.SessionLocator
	if route.ReportTo != nil {
		resolved, err := resolveEnvelopeTarget(req.Context(), r.owner, *route.ReportTo)
		if err != nil {
			r.metrics.notFound.Add(1)
			r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
				http.StatusNotFound,
				inboundWebhookCodeSessionNotFound,
				"failed to resolve webhook report_to",
				err,
			))
			return
		}
		reportTo = &resolved.Locator
	}
	dedupeKey := dedupeKeyForInboundWebhook(route, req, requestID, rawBody)
	payload := sessionTurnPayload{
		Text:           prompt,
		Locator:        target.Locator,
		ReportTo:       reportTo,
		UserID:         target.UserID,
		TopicID:        target.TopicID,
		ProgressPolicy: inboundWebhookProgressPolicy(),
		Deliver:        reportTo != nil,
		Source:         sessionTurnSourceWebhook,
		DedupeKey:      dedupeKey,
	}
	var (
		result     *swarm.CommandPublishResult
		taskID     string
		enqueueErr error
	)
	if route.Mode == inboundWebhookRouteModeSession {
		result, enqueueErr = r.balda.submitSessionTurn(req.Context(), payload)
	} else {
		result, taskID, enqueueErr = r.balda.submitWebhookTask(req.Context(), payload, route.Name, requestID)
	}
	if enqueueErr != nil {
		if swarm.IsCommandQueueFull(enqueueErr) {
			r.metrics.queueFull.Add(1)
			r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
				http.StatusTooManyRequests,
				inboundWebhookCodeQueueFull,
				"command queue is full",
				enqueueErr,
			))
			return
		}

		r.metrics.dispatchErr.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusServiceUnavailable,
			inboundWebhookCodeDispatchFailed,
			"failed to publish inbound command",
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
		Str("mode", route.Mode).
		Str("dedupe_key", dedupeKey).
		Str("stream", result.Stream).
		Uint64("sequence", result.Sequence).
		Str("task_id", taskID).
		Msg("inbound webhook accepted")

	writeInboundWebhookJSON(w, http.StatusAccepted, inboundWebhookAcceptedResponse{
		Status:    inboundWebhookStatusAccepted,
		Accepted:  true,
		RequestID: requestID,
		MessageID: result.MsgID,
		TaskID:    taskID,
		SessionID: target.Locator.SessionID,
		Stream:    result.Stream,
		Sequence:  result.Sequence,
		Duplicate: result.Duplicate,
	})
}

func requestIDFromInboundWebhookRequest(req *http.Request) string {
	requestID := strings.TrimSpace(req.Header.Get("X-Request-Id"))
	if requestID != "" {
		return requestID
	}
	return fmt.Sprintf("inbound-%d", time.Now().UnixNano())
}

func authorizeInboundWebhookRequest(req *http.Request, policy inboundWebhookAuthPolicy) error {
	switch policy.Type {
	case inboundWebhookAuthTypeNone:
		return nil
	case inboundWebhookAuthTypeHeader:
		got := strings.TrimSpace(req.Header.Get(policy.Header))
		want := strings.TrimSpace(policy.Value)
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			return fmt.Errorf("header %q does not match", policy.Header)
		}
		return nil
	default:
		return fmt.Errorf("unsupported auth type %q", policy.Type)
	}
}

func dedupeKeyForInboundWebhook(route inboundWebhookRoute, req *http.Request, requestID string, body string) string {
	base := strings.TrimSpace(requestID)
	switch route.Dedupe.Source {
	case inboundWebhookDedupeSourceHeader:
		if header := strings.TrimSpace(req.Header.Get(route.Dedupe.Header)); header != "" {
			base = header
		}
	case inboundWebhookDedupeSourceBodySHA:
		base = webhookBodySHA256(body)
	}
	return strings.Join([]string{"webhook", strings.TrimSpace(route.Name), base}, ":")
}

func webhookBodySHA256(body string) string {
	sum := sha256.Sum256([]byte(body))
	return fmt.Sprintf("%x", sum[:])
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
