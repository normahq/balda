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

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
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
	inboundWebhookRouteModeJob     = "job"
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

const (
	inboundWebhookMessageCouldNotAccept  = "could not accept request"
	inboundWebhookMessageTemporarilyBusy = "temporarily busy"
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
	submitWebhookTask(ctx context.Context, payload actors.SessionTurnPayload, routeName string, requestID string) (*actortransport.DispatchReceipt, string, error)
	submitSessionTurn(ctx context.Context, payload actors.SessionTurnPayload) (*actortransport.DispatchReceipt, error)
}

type inboundWebhookParams struct {
	fx.In

	LC         fx.Lifecycle
	Config     InboundWebhookConfig
	Balda      *BaldaHandler
	OwnerStore *auth.OwnerStore
	Logger     zerolog.Logger
}

// InboundWebhookReceiver receives inbound webhook events and dispatches them
// into either direct session turns or durable webhook job commands.
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

type normalizedInboundWebhookConfig struct {
	Enabled    bool
	ListenAddr string
	Routes     map[string]inboundWebhookRoute
}

func normalizeInboundWebhookConfig(cfg InboundWebhookConfig) (normalizedInboundWebhookConfig, error) {
	listenAddr := strings.TrimSpace(cfg.ListenAddr)
	if listenAddr == "" {
		listenAddr = defaultInboundWebhookListenAddr
	}
	normalized := normalizedInboundWebhookConfig{
		Enabled:    cfg.Enabled,
		ListenAddr: listenAddr,
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

		path := strings.TrimSpace(rawRoute.Path)
		if path == "" {
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.path: path is required", routeName)
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
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
			target = envelopeTarget{Target: envelopeTargetAlias, Key: envelopeAliasOwner}
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
		mode := strings.ToLower(strings.TrimSpace(rawRoute.Envelope.Mode))
		if mode == "" {
			mode = inboundWebhookRouteModeJob
		}
		switch mode {
		case inboundWebhookRouteModeJob, inboundWebhookRouteModeSession:
		default:
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.envelope.mode: unsupported mode %q", routeName, rawRoute.Envelope.Mode)
		}
		authPolicy := inboundWebhookAuthPolicy{
			Type:   strings.ToLower(strings.TrimSpace(rawRoute.Auth.Type)),
			Header: strings.TrimSpace(rawRoute.Auth.Header),
			Value:  strings.TrimSpace(rawRoute.Auth.Value),
		}
		if authPolicy.Type == "" {
			authPolicy.Type = inboundWebhookAuthTypeNone
		}
		switch authPolicy.Type {
		case inboundWebhookAuthTypeNone:
			authPolicy = inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone}
		case inboundWebhookAuthTypeHeader:
			if authPolicy.Header == "" {
				return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.auth: header is required for type=%q", routeName, inboundWebhookAuthTypeHeader)
			}
			if authPolicy.Value == "" {
				return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.auth: value is required for type=%q", routeName, inboundWebhookAuthTypeHeader)
			}
		default:
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.auth: unsupported type %q", routeName, rawRoute.Auth.Type)
		}
		dedupePolicy := inboundWebhookDedupePolicy{
			Source: strings.ToLower(strings.TrimSpace(rawRoute.Dedupe.Source)),
			Header: strings.TrimSpace(rawRoute.Dedupe.Header),
		}
		if dedupePolicy.Source == "" && dedupePolicy.Header != "" {
			dedupePolicy.Source = inboundWebhookDedupeSourceHeader
		}
		if dedupePolicy.Source == "" {
			dedupePolicy.Source = inboundWebhookDedupeSourceRequestID
		}
		switch dedupePolicy.Source {
		case inboundWebhookDedupeSourceRequestID, inboundWebhookDedupeSourceBodySHA:
		case inboundWebhookDedupeSourceHeader:
			if dedupePolicy.Header == "" {
				return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.dedupe: header is required for source=%q", routeName, inboundWebhookDedupeSourceHeader)
			}
		default:
			return normalizedInboundWebhookConfig{}, fmt.Errorf("balda.webhooks.routes.%s.dedupe: unsupported source %q", routeName, rawRoute.Dedupe.Source)
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
	requestID := strings.TrimSpace(req.Header.Get("X-Request-Id"))
	if requestID == "" {
		requestID = fmt.Sprintf("inbound-%d", time.Now().UnixNano())
	}
	if req.Method != http.MethodPost {
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusMethodNotAllowed,
			code:    inboundWebhookCodeInvalidMethod,
			message: inboundWebhookMessageCouldNotAccept,
		})
		return
	}

	route, ok := r.routes[req.URL.Path]
	if !ok {
		r.metrics.notFound.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusNotFound,
			code:    inboundWebhookCodeRouteNotFound,
			message: inboundWebhookMessageCouldNotAccept,
		})
		return
	}
	if authErr := authorizeInboundWebhookRequest(req, route.Auth); authErr != nil {
		r.metrics.unauthorized.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusUnauthorized,
			code:    inboundWebhookCodeUnauthorized,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   authErr,
		})
		return
	}

	defer func() { _ = req.Body.Close() }()
	bodyBytes, readErr := io.ReadAll(io.LimitReader(req.Body, inboundWebhookMaxBodyBytes+1))
	if readErr != nil {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusBadRequest,
			code:    inboundWebhookCodeInvalidPayload,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   readErr,
		})
		return
	}
	if len(bodyBytes) > inboundWebhookMaxBodyBytes {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusBadRequest,
			code:    inboundWebhookCodeInvalidPayload,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   fmt.Errorf("request body exceeds %d bytes", inboundWebhookMaxBodyBytes),
		})
		return
	}
	rawBody := string(bodyBytes)

	headers := make(map[string]string, len(req.Header))
	for name, values := range req.Header {
		if len(values) == 0 {
			headers[name] = ""
			continue
		}
		headers[name] = values[0]
	}

	var promptBuf bytes.Buffer
	renderErr := route.PromptTemplate.Execute(&promptBuf, inboundWebhookTemplateData{
		RequestID: requestID,
		Path:      req.URL.Path,
		Method:    req.Method,
		RawBody:   rawBody,
		Headers:   headers,
	})
	if renderErr != nil {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusBadRequest,
			code:    inboundWebhookCodeInvalidPayload,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   renderErr,
		})
		return
	}
	prompt := strings.TrimSpace(promptBuf.String())
	if prompt == "" {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusBadRequest,
			code:    inboundWebhookCodeInvalidPayload,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   fmt.Errorf("rendered prompt is empty"),
		})
		return
	}
	target, targetErr := resolveEnvelopeTarget(req.Context(), r.owner, route.Target)
	if targetErr != nil {
		r.metrics.notFound.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusNotFound,
			code:    inboundWebhookCodeSessionNotFound,
			message: inboundWebhookMessageCouldNotAccept,
			cause:   targetErr,
		})
		return
	}
	var reportTo *baldasession.SessionLocator
	if route.ReportTo != nil {
		resolved, err := resolveEnvelopeTarget(req.Context(), r.owner, *route.ReportTo)
		if err != nil {
			r.metrics.notFound.Add(1)
			r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
				status:  http.StatusNotFound,
				code:    inboundWebhookCodeSessionNotFound,
				message: inboundWebhookMessageCouldNotAccept,
				cause:   err,
			})
			return
		}
		reportTo = &resolved.Locator
	}
	dedupeBase := strings.TrimSpace(requestID)
	switch route.Dedupe.Source {
	case inboundWebhookDedupeSourceHeader:
		if header := strings.TrimSpace(req.Header.Get(route.Dedupe.Header)); header != "" {
			dedupeBase = header
		}
	case inboundWebhookDedupeSourceBodySHA:
		sum := sha256.Sum256([]byte(rawBody))
		dedupeBase = fmt.Sprintf("%x", sum[:])
	}
	dedupeKey := strings.Join([]string{"webhook", strings.TrimSpace(route.Name), dedupeBase}, ":")
	payload := actors.SessionTurnPayload{
		Text:     prompt,
		Locator:  target.Locator,
		ReportTo: reportTo,
		UserID:   target.UserID,
		TopicID:  target.TopicID,
		DeliveryOptions: deliveryfmt.Options{
			Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatAuto},
			ProgressPolicy: deliveryfmt.ProgressPolicy{
				Typing:   false,
				Thinking: false,
			},
		},
		ProgressPolicy: baldachannel.ProgressPolicy{
			Typing:   false,
			Thinking: false,
		},
		Deliver:   reportTo != nil,
		Source:    "webhook",
		DedupeKey: dedupeKey,
	}
	var (
		result     *actortransport.DispatchReceipt
		taskID     string
		enqueueErr error
	)
	if route.Mode == inboundWebhookRouteModeSession {
		result, enqueueErr = r.balda.submitSessionTurn(req.Context(), payload)
	} else {
		result, taskID, enqueueErr = r.balda.submitWebhookTask(req.Context(), payload, route.Name, requestID)
	}
	if enqueueErr != nil {
		if baldaexecution.IsCommandQueueFull(enqueueErr) {
			r.metrics.queueFull.Add(1)
			r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
				status:  http.StatusTooManyRequests,
				code:    inboundWebhookCodeQueueFull,
				message: inboundWebhookMessageTemporarilyBusy,
				cause:   enqueueErr,
			})
			return
		}

		r.metrics.dispatchErr.Add(1)
		r.writeInboundWebhookError(w, requestID, &inboundWebhookHTTPError{
			status:  http.StatusServiceUnavailable,
			code:    inboundWebhookCodeDispatchFailed,
			message: inboundWebhookMessageTemporarilyBusy,
			cause:   enqueueErr,
		})
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
		Str("job_id", taskID).
		Msg("inbound webhook accepted")

	writeInboundWebhookJSON(w, http.StatusAccepted, inboundWebhookAcceptedResponse{
		Status:    inboundWebhookStatusAccepted,
		Accepted:  true,
		RequestID: requestID,
		MessageID: result.MsgID,
		Duplicate: result.Duplicate,
	})
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

func (r *InboundWebhookReceiver) writeInboundWebhookError(
	w http.ResponseWriter,
	requestID string,
	handlerErr *inboundWebhookHTTPError,
) {
	if handlerErr == nil {
		handlerErr = &inboundWebhookHTTPError{
			status:  http.StatusInternalServerError,
			code:    inboundWebhookCodeDispatchFailed,
			message: "internal error",
		}
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
