package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"text/template"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
	"google.golang.org/adk/runner"
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
	if _, ok := got.Routes["/webhook1"]; !ok {
		t.Fatal("route /webhook1 missing")
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

func TestInboundWebhookReceiver_TemplateRenderError(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest(t)
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.Missing}}")),
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusBadRequest, inboundWebhookCodeInvalidPayload)
}

func TestInboundWebhookReceiver_SessionNotFound(t *testing.T) {
	t.Parallel()

	sessionMgr := &fakeInboundSessionManager{
		infoErr: errors.New("missing session"),
	}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.sessions = sessionMgr

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusNotFound, inboundWebhookCodeSessionNotFound)
}

func TestInboundWebhookReceiver_QueueFull(t *testing.T) {
	t.Parallel()

	locator := baldatelegram.NewLocator(9001, 0)
	ts := newSchedulerTopicSession(t, locator, "tg-101", locator.SessionID, nil)

	sessionMgr := &fakeInboundSessionManager{
		session: ts,
	}
	queue := &fakeInboundTurnQueue{
		enqueueErr: ErrTurnQueueFull,
	}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.sessions = sessionMgr
	receiver.dispatch = queue
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("{{.RawBody}}")),
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook1", bytes.NewBufferString("hello"))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusTooManyRequests, inboundWebhookCodeQueueFull)
}

func TestInboundWebhookReceiver_AcceptsAndDispatches(t *testing.T) {
	t.Parallel()

	locator := baldatelegram.NewLocator(9001, 0)
	ts := newSchedulerTopicSession(t, locator, "tg-101", locator.SessionID, nil)

	sessionMgr := &fakeInboundSessionManager{
		session:       ts,
		getErrOnce:    errors.New("not in memory"),
		info:          baldasession.TopicSessionInfo{SessionID: locator.SessionID, Locator: locator, UserID: "tg-101"},
		restoreResult: ts,
	}
	queue := &fakeInboundTurnQueue{runImmediately: true}
	executor := &fakeInboundTurnExecutor{}
	receiver := newInboundWebhookReceiverForTest(t)
	receiver.sessions = sessionMgr
	receiver.dispatch = queue
	receiver.balda = executor
	receiver.routes = map[string]inboundWebhookRoute{
		"/webhook1": {
			Name:           "webhook1",
			Path:           "/webhook1",
			PromptTemplate: template.Must(template.New("webhook1").Option("missingkey=error").Parse("route={{.Path}} body={{.RawBody}}")),
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
	if got := len(queue.tasks); got != 1 {
		t.Fatalf("queued tasks = %d, want 1", got)
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
	if got := sessionMgr.restoreCalls; got != 1 {
		t.Fatalf("restore calls = %d, want 1", got)
	}
}

type fakeInboundSessionManager struct {
	session       *baldasession.TopicSession
	getErrOnce    error
	getCalls      int
	info          baldasession.TopicSessionInfo
	infoErr       error
	restoreResult *baldasession.TopicSession
	restoreErr    error
	restoreCalls  int
	ensureResult  *baldasession.TopicSession
	ensureErr     error
	ensureCalls   int
}

func (f *fakeInboundSessionManager) GetSession(_ baldasession.SessionLocator) (*baldasession.TopicSession, error) {
	f.getCalls++
	if f.getErrOnce != nil {
		err := f.getErrOnce
		f.getErrOnce = nil
		return nil, err
	}
	if f.session == nil {
		return nil, errors.New("session missing")
	}
	return f.session, nil
}

func (f *fakeInboundSessionManager) GetSessionInfo(_ context.Context, _ string) (baldasession.TopicSessionInfo, error) {
	if f.infoErr != nil {
		return baldasession.TopicSessionInfo{}, f.infoErr
	}
	return f.info, nil
}

func (f *fakeInboundSessionManager) RestoreSession(
	_ context.Context,
	_ baldasession.SessionContext,
) (*baldasession.TopicSession, error) {
	f.restoreCalls++
	if f.restoreErr != nil {
		return nil, f.restoreErr
	}
	if f.restoreResult != nil {
		f.session = f.restoreResult
		return f.restoreResult, nil
	}
	return nil, errors.New("no restore result")
}

func (f *fakeInboundSessionManager) EnsureSession(
	_ context.Context,
	_ baldasession.SessionContext,
	_ string,
) (*baldasession.TopicSession, error) {
	f.ensureCalls++
	if f.ensureErr != nil {
		return nil, f.ensureErr
	}
	if f.ensureResult != nil {
		f.session = f.ensureResult
		return f.ensureResult, nil
	}
	return nil, errors.New("no ensure result")
}

type fakeInboundTurnQueue struct {
	tasks          []TurnTask
	enqueueErr     error
	runImmediately bool
}

func (f *fakeInboundTurnQueue) Enqueue(task TurnTask) (int, error) {
	if f.enqueueErr != nil {
		return 0, f.enqueueErr
	}
	f.tasks = append(f.tasks, task)
	position := len(f.tasks) - 1
	if f.runImmediately {
		_ = task.Run(context.Background())
	}
	return position, nil
}

func (*fakeInboundTurnQueue) CancelSession(baldasession.SessionLocator, bool) (bool, int, error) {
	return false, 0, nil
}

type fakeInboundTurnExecutor struct {
	calls   int
	prompt  string
	deliver bool
}

func (f *fakeInboundTurnExecutor) runTurnTaskWithDelivery(
	_ context.Context,
	text string,
	_ *runner.Runner,
	_ string,
	_ string,
	_ string,
	_ baldasession.SessionLocator,
	_ int,
	_ int,
	_ baldachannel.ProgressPolicy,
	deliver bool,
) error {
	f.calls++
	f.prompt = text
	f.deliver = deliver
	return nil
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
			},
		},
		sessions: &fakeInboundSessionManager{},
		dispatch: &fakeInboundTurnQueue{},
		balda:    &fakeInboundTurnExecutor{},
		owner:    newOwnerStoreForTest(t, 101, 9001),
		logger:   zerolog.Nop(),
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
