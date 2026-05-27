package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"text/template"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
)

func TestNormalizeInboundWebhookConfig_RequiresRoutesWhenEnabled(t *testing.T) {
	t.Parallel()

	_, err := normalizeInboundWebhookConfig(InboundWebhookConfig{Enabled: true})
	if err == nil {
		t.Fatal("normalizeInboundWebhookConfig() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "balda.webhooks.routes is required") {
		t.Fatalf("normalizeInboundWebhookConfig() error = %v", err)
	}
}

func TestNormalizeInboundWebhookConfig_AllowsRouteWithoutReportTo(t *testing.T) {
	t.Parallel()

	got, err := normalizeInboundWebhookConfig(InboundWebhookConfig{
		Enabled: true,
		Routes: map[string]InboundWebhookRouteConfig{
			"webhook1": {
				Path:           "/webhook1",
				PromptTemplate: "{{.RawBody}}",
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeInboundWebhookConfig() error = %v", err)
	}
	route, ok := got.Routes["/webhook1"]
	if !ok {
		t.Fatal("route /webhook1 missing")
	}
	if route.Target != ownerEnvelopeTarget() {
		t.Fatalf("target = %+v, want owner alias target", route.Target)
	}
	if route.Mode != inboundWebhookRouteModeTask {
		t.Fatalf("mode = %q, want %q", route.Mode, inboundWebhookRouteModeTask)
	}
	if route.Auth.Type != inboundWebhookAuthTypeNone {
		t.Fatalf("auth type = %q, want %q", route.Auth.Type, inboundWebhookAuthTypeNone)
	}
	if route.Dedupe.Source != inboundWebhookDedupeSourceRequestID {
		t.Fatalf("dedupe source = %q, want %q", route.Dedupe.Source, inboundWebhookDedupeSourceRequestID)
	}
}

func TestNormalizeInboundWebhookConfig_RejectsDuplicatePaths(t *testing.T) {
	t.Parallel()

	_, err := normalizeInboundWebhookConfig(InboundWebhookConfig{
		Enabled: true,
		Routes: map[string]InboundWebhookRouteConfig{
			"webhook1": {
				Path:           "/shared",
				PromptTemplate: "first",
			},
			"webhook2": {
				Path:           "shared",
				PromptTemplate: "second",
			},
		},
	})
	if err == nil {
		t.Fatal("normalizeInboundWebhookConfig() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "duplicates route") {
		t.Fatalf("normalizeInboundWebhookConfig() error = %v", err)
	}
}

func TestNormalizeInboundWebhookConfig_RejectsInvalidAuthHeaderPolicy(t *testing.T) {
	t.Parallel()

	_, err := normalizeInboundWebhookConfig(InboundWebhookConfig{
		Enabled: true,
		Routes: map[string]InboundWebhookRouteConfig{
			"webhook1": {
				Path:           "/webhook1",
				PromptTemplate: "{{.RawBody}}",
				Auth: InboundWebhookRouteAuthConfig{
					Type: inboundWebhookAuthTypeHeader,
				},
			},
		},
	})
	if err == nil {
		t.Fatal("normalizeInboundWebhookConfig() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "auth") {
		t.Fatalf("normalizeInboundWebhookConfig() error = %v", err)
	}
}

func TestNormalizeInboundWebhookConfig_RejectsInvalidDedupeHeaderSource(t *testing.T) {
	t.Parallel()

	_, err := normalizeInboundWebhookConfig(InboundWebhookConfig{
		Enabled: true,
		Routes: map[string]InboundWebhookRouteConfig{
			"webhook1": {
				Path:           "/webhook1",
				PromptTemplate: "{{.RawBody}}",
				Dedupe: InboundWebhookRouteDedupeConfig{
					Source: inboundWebhookDedupeSourceHeader,
				},
			},
		},
	})
	if err == nil {
		t.Fatal("normalizeInboundWebhookConfig() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "dedupe") {
		t.Fatalf("normalizeInboundWebhookConfig() error = %v", err)
	}
}

func TestInboundWebhookReceiver_InvalidMethod(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/webhook1", nil)
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusMethodNotAllowed, inboundWebhookCodeInvalidMethod)
}

func TestInboundWebhookReceiver_RouteNotFound(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/missing", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusNotFound, inboundWebhookCodeRouteNotFound)
}

func TestInboundWebhookReceiver_Unauthorized(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeTask,
			Auth: inboundWebhookAuthPolicy{
				Type:   inboundWebhookAuthTypeHeader,
				Header: "X-Webhook-Token",
				Value:  "secret",
			},
			Dedupe: inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceRequestID},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusUnauthorized, inboundWebhookCodeUnauthorized)
}

func TestInboundWebhookReceiver_TemplateRenderError(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.Missing}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeTask,
			Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
			Dedupe:         inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceRequestID},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusBadRequest, inboundWebhookCodeInvalidPayload)
}

func TestInboundWebhookReceiver_AcceptsWithoutSessionRestore(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	if got, want := rec.Code, http.StatusAccepted; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}

func TestInboundWebhookReceiver_QueueFull(t *testing.T) {
	t.Parallel()

	executor := &fakeInboundTurnExecutor{submitErr: swarm.ErrCommandQueueFull}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.balda = executor
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeTask,
			Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
			Dedupe:         inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceRequestID},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("hello"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusTooManyRequests, inboundWebhookCodeQueueFull)
}

func TestInboundWebhookReceiver_TurnQueueFullIsNotIngressQueueFull(t *testing.T) {
	t.Parallel()

	executor := &fakeInboundTurnExecutor{submitErr: ErrTurnQueueFull}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.balda = executor

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("hello"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusServiceUnavailable, inboundWebhookCodeDispatchFailed)
}

func TestInboundWebhookReceiver_AcceptsAndPublishesCommand(t *testing.T) {
	t.Parallel()

	locator := baldatelegram.NewLocator(9001, 0)
	executor := &fakeInboundTurnExecutor{}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.balda = executor
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("route={{.Path}} body={{.RawBody}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeTask,
			Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
			Dedupe:         inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceRequestID},
		},
	}

	body := `{"event":"release"}`
	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString(body))
	req.Header.Set("X-Request-Id", "req-1")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	if got, want := rec.Code, http.StatusAccepted; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	var response inboundWebhookAcceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := response.Status, inboundWebhookStatusAccepted; got != want {
		t.Fatalf("status body = %q, want %q", got, want)
	}
	if got, want := response.RequestID, "req-1"; got != want {
		t.Fatalf("request_id = %q, want %q", got, want)
	}
	if got, want := response.SessionID, locator.SessionID; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got, want := response.Stream, swarm.DefaultCommandStream; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
	if response.Sequence == 0 {
		t.Fatal("sequence = 0, want JetStream sequence")
	}
	if response.MessageID == "" {
		t.Fatal("message_id is empty")
	}
	if response.TaskID == "" {
		t.Fatal("task_id is empty")
	}
	if got := executor.calls; got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
	if got, want := executor.prompt, "route=/webhook1 body={\"event\":\"release\"}"; got != want {
		t.Fatalf("executor prompt = %q, want %q", got, want)
	}
	if executor.deliver {
		t.Fatal("executor deliver = true, want false for omitted report_to")
	}
	if got, want := executor.payload.DedupeKey, "webhook:webhook1:req-1"; got != want {
		t.Fatalf("dedupe_key = %q, want %q", got, want)
	}
}

func TestInboundWebhookReceiver_UsesRouteDedupeHeaderAndReportTo(t *testing.T) {
	t.Parallel()

	executor := &fakeInboundTurnExecutor{}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.balda = executor
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeTask,
			ReportTo:       &envelopeTarget{Target: envelopeTargetAlias, Key: envelopeAliasOwner},
			Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
			Dedupe: inboundWebhookDedupePolicy{
				Source: inboundWebhookDedupeSourceHeader,
				Header: "X-Delivery-ID",
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString(`{"event":"release"}`))
	req.Header.Set("X-Request-Id", "req-1")
	req.Header.Set("X-Delivery-ID", "delivery-123")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	if got, want := rec.Code, http.StatusAccepted; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	if !executor.deliver {
		t.Fatal("executor deliver = false, want true for report_to route")
	}
	if executor.payload.ReportTo == nil {
		t.Fatal("report_to payload = nil, want resolved report_to locator")
	}
	if got, want := executor.payload.DedupeKey, "webhook:webhook1:delivery-123"; got != want {
		t.Fatalf("dedupe_key = %q, want %q", got, want)
	}
}

func TestInboundWebhookReceiver_SessionModePublishesSessionCommand(t *testing.T) {
	t.Parallel()

	executor := &fakeInboundTurnExecutor{}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.balda = executor
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
			Target:         ownerEnvelopeTarget(),
			Mode:           inboundWebhookRouteModeSession,
			Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
			Dedupe:         inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceBodySHA},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString(`{"event":"release"}`))
	req.Header.Set("X-Request-Id", "req-1")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	if got, want := rec.Code, http.StatusAccepted; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	if executor.submitSessionCalls != 1 {
		t.Fatalf("submitSessionTurn calls = %d, want 1", executor.submitSessionCalls)
	}
	if executor.submitWebhookCalls != 0 {
		t.Fatalf("submitWebhookTask calls = %d, want 0", executor.submitWebhookCalls)
	}
	var response inboundWebhookAcceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.TaskID != "" {
		t.Fatalf("task_id = %q, want empty in session mode", response.TaskID)
	}
}

type fakeInboundTurnExecutor struct {
	calls              int
	submitWebhookCalls int
	submitSessionCalls int
	prompt             string
	deliver            bool
	payload            sessionTurnPayload
	lastRouteName      string
	lastRequestID      string
	submitErr          error
}

func (f *fakeInboundTurnExecutor) submitWebhookTask(
	_ context.Context,
	payload sessionTurnPayload,
	routeName string,
	requestID string,
) (*swarm.CommandPublishResult, string, error) {
	if f.submitErr != nil {
		return nil, "", f.submitErr
	}
	f.calls++
	f.submitWebhookCalls++
	f.prompt = payload.Text
	f.deliver = payload.Deliver
	f.payload = payload
	f.lastRouteName = routeName
	f.lastRequestID = requestID
	taskID := "webhook-" + routeName + "-test"
	return &swarm.CommandPublishResult{
		Stream:   swarm.DefaultCommandStream,
		Sequence: 1,
		Subject:  swarm.SubjectCommandTask,
		MsgID:    "webhook:" + routeName + ":" + requestID,
	}, taskID, nil
}

func (f *fakeInboundTurnExecutor) submitSessionTurn(_ context.Context, payload sessionTurnPayload) (*swarm.CommandPublishResult, error) {
	if f.submitErr != nil {
		return nil, f.submitErr
	}
	f.calls++
	f.submitSessionCalls++
	f.prompt = payload.Text
	f.deliver = payload.Deliver
	f.payload = payload
	return &swarm.CommandPublishResult{
		Stream:   swarm.DefaultCommandStream,
		Sequence: 1,
		Subject:  swarm.SubjectCommandSession,
		MsgID:    payload.DedupeKey,
	}, nil
}

func newInboundWebhookReceiverForTest(t *testing.T) *InboundWebhookReceiver {
	t.Helper()

	return &InboundWebhookReceiver{
		enabled: true,
		routes: map[string]inboundWebhookRoute{
			"/webhook1": {
				Name:           "webhook1",
				Path:           "/webhook1",
				PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
				Target:         ownerEnvelopeTarget(),
				Mode:           inboundWebhookRouteModeTask,
				Auth:           inboundWebhookAuthPolicy{Type: inboundWebhookAuthTypeNone},
				Dedupe:         inboundWebhookDedupePolicy{Source: inboundWebhookDedupeSourceRequestID},
			},
		},
		balda:  &fakeInboundTurnExecutor{},
		owner:  newOwnerStoreForTest(t, 101, 9001),
		logger: zerolog.Nop(),
	}
}

func assertInboundWebhookError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()

	if got := rec.Code; got != wantStatus {
		t.Fatalf("status = %d, want %d", got, wantStatus)
	}
	var payload inboundWebhookErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload.Status; got != inboundWebhookStatusError {
		t.Fatalf("status body = %q, want %q", got, inboundWebhookStatusError)
	}
	if got := payload.Error.Code; got != wantCode {
		t.Fatalf("error.code = %q, want %q", got, wantCode)
	}
}
