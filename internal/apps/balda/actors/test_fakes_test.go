package actors

import (
	"context"
	"net/http"
	"reflect"
	"unsafe"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/tgbotkit/client"
	"testing"
)

const testTelegramUserID101 = "tg-101"

type cancelSessionCall struct {
	SessionID   string
	ClearQueued bool
}

type fakeTurnDispatcher struct {
	commands     []actorlayer.Envelope
	cancelCalls  []cancelSessionCall
	enqueueCalls []TurnTask
}

func (f *fakeTurnDispatcher) Enqueue(task TurnTask) (int, error) {
	f.enqueueCalls = append(f.enqueueCalls, task)
	return 0, nil
}

func (f *fakeTurnDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	f.commands = append(f.commands, env)
	return &actortransport.DispatchReceipt{
		Stream:   baldaexecution.DefaultCommandStream,
		Sequence: uint64(len(f.commands)),
		Subject:  baldaexecution.SubjectForEnvelope(env),
		MsgID:    actorlayer.DedupeKeyOrID(env),
	}, nil
}

func (*fakeTurnDispatcher) PublishEvent(context.Context, string, actorlayer.Envelope) error {
	return nil
}

func (f *fakeTurnDispatcher) CancelSession(locator session.SessionLocator, clearQueued bool) (bool, int, error) {
	f.cancelCalls = append(f.cancelCalls, cancelSessionCall{
		SessionID:   locator.SessionID,
		ClearQueued: clearQueued,
	})
	return false, 0, nil
}

type recordingHandlerCommandBus struct {
	commands      []actorlayer.Envelope
	commandErrs   []error
	eventSubjects []string
	eventEnvs     []actorlayer.Envelope
	eventErrs     []error
}

func (b *recordingHandlerCommandBus) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	if len(b.commandErrs) > 0 {
		err := b.commandErrs[0]
		b.commandErrs = b.commandErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	b.commands = append(b.commands, env)
	return &actortransport.DispatchReceipt{Stream: baldaexecution.DefaultCommandStream, Sequence: uint64(len(b.commands)), Subject: baldaexecution.SubjectForEnvelope(env), MsgID: actorlayer.DedupeKeyOrID(env)}, nil
}

func (b *recordingHandlerCommandBus) PublishEvent(_ context.Context, subject string, env actorlayer.Envelope) error {
	b.eventSubjects = append(b.eventSubjects, subject)
	b.eventEnvs = append(b.eventEnvs, env)
	if len(b.eventErrs) > 0 {
		err := b.eventErrs[0]
		b.eventErrs = b.eventErrs[1:]
		return err
	}
	return nil
}

type fakeTelegramClient struct {
	client.ClientWithResponsesInterface
	sendErr      error
	messages     []client.SendMessageJSONRequestBody
	richMessages []client.SendRichMessageJSONRequestBody
	drafts       []client.SendMessageDraftJSONRequestBody
	richDrafts   []client.SendRichMessageDraftJSONRequestBody
	chatActions  []client.SendChatActionJSONRequestBody
}

func (c *fakeTelegramClient) SendMessageWithResponse(_ context.Context, body client.SendMessageJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendMessageResponse, error) {
	c.messages = append(c.messages, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendMessage200Ok `json:"ok"`
			Result client.Message          `json:"result"`
		}{
			Ok:     true,
			Result: client.Message{MessageId: len(c.messages)},
		},
	}, nil
}

func (c *fakeTelegramClient) SendRichMessageWithResponse(_ context.Context, body client.SendRichMessageJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendRichMessageResponse, error) {
	c.richMessages = append(c.richMessages, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendRichMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendRichMessage200Ok `json:"ok"`
			Result client.Message              `json:"result"`
		}{
			Ok:     true,
			Result: client.Message{MessageId: len(c.richMessages)},
		},
	}, nil
}

func (c *fakeTelegramClient) SendMessageDraftWithResponse(_ context.Context, body client.SendMessageDraftJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendMessageDraftResponse, error) {
	c.drafts = append(c.drafts, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendMessageDraftResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendMessageDraft200Ok `json:"ok"`
			Result bool                         `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func (c *fakeTelegramClient) SendRichMessageDraftWithResponse(_ context.Context, body client.SendRichMessageDraftJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendRichMessageDraftResponse, error) {
	c.richDrafts = append(c.richDrafts, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendRichMessageDraftResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendRichMessageDraft200Ok `json:"ok"`
			Result bool                             `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func (c *fakeTelegramClient) SendChatActionWithResponse(_ context.Context, body client.SendChatActionJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendChatActionResponse, error) {
	c.chatActions = append(c.chatActions, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendChatActionResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendChatAction200Ok `json:"ok"`
			Result bool                       `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func newBaldaSessionManagerWithSession(t *testing.T, locator session.SessionLocator, ts *session.TopicSession) *session.Manager {
	t.Helper()

	m := &session.Manager{}
	setUnexportedField(t, m, "sessions", map[string]*session.TopicSession{locator.SessionID: ts})
	setUnexportedField(t, m, "sessionStore", &fakeBaldaRestoreSessionStore{})
	return m
}

func newBaldaTopicSession(t *testing.T, sessionID string) *session.TopicSession {
	t.Helper()

	ts := &session.TopicSession{}
	setUnexportedField(t, ts, "sessionID", sessionID)
	return ts
}

type fakeBaldaRestoreSessionStore struct {
	record         baldastate.SessionRecord
	foundByAddress bool
	lastUpsert     baldastate.SessionRecord
}

func (f *fakeBaldaRestoreSessionStore) Upsert(_ context.Context, record baldastate.SessionRecord) error {
	f.lastUpsert = record
	f.record = record
	f.foundByAddress = true
	return nil
}

func (f *fakeBaldaRestoreSessionStore) GetByAddress(_ context.Context, channelType, addressKey string) (baldastate.SessionRecord, bool, error) {
	if !f.foundByAddress {
		return baldastate.SessionRecord{}, false, nil
	}
	if f.record.ChannelType != channelType || f.record.AddressKey != addressKey {
		return baldastate.SessionRecord{}, false, nil
	}
	return f.record, true, nil
}

func (f *fakeBaldaRestoreSessionStore) GetBySessionID(_ context.Context, sessionID string) (baldastate.SessionRecord, bool, error) {
	if !f.foundByAddress || f.record.SessionID != sessionID {
		return baldastate.SessionRecord{}, false, nil
	}
	return f.record, true, nil
}

func (*fakeBaldaRestoreSessionStore) DeleteBySessionID(context.Context, string) error {
	return nil
}

func (f *fakeBaldaRestoreSessionStore) List(context.Context) ([]baldastate.SessionRecord, error) {
	if !f.foundByAddress {
		return nil, nil
	}
	return []baldastate.SessionRecord{f.record}, nil
}

func setUnexportedField[T any](t *testing.T, target any, fieldName string, value T) {
	t.Helper()

	rv := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	reflect.NewAt(rv.Type(), unsafe.Pointer(rv.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
