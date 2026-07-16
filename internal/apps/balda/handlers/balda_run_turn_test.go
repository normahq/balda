package handlers

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/actors"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/sessionturn"
	"github.com/normahq/balda/internal/apps/balda/sessionturnapp"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	adkagent "google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/runner"
	adksession "google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

const (
	baldaRunTurnGenericEmptyTerminalMessage = "The provider ended the turn without a usable reply. Please try again."
	baldaRunTurnFinalAnswerText             = "final answer"
	baldaRunTurnReasoningOne                = "inspect current delivery path"
	baldaRunTurnReasoningTwo                = "compare retry semantics"
)

type baldaRunTurnTelegramClient struct {
	client.ClientWithResponsesInterface
	drafts       []client.SendMessageDraftJSONRequestBody
	richDrafts   []client.SendRichMessageDraftJSONRequestBody
	messages     []client.SendMessageJSONRequestBody
	richMessages []client.SendRichMessageJSONRequestBody
	chatActions  []client.SendChatActionJSONRequestBody
}

func (h *BaldaHandler) runTurn(
	ctx context.Context,
	text string,
	r *runner.Runner,
	userID string,
	sessionID string,
	agentSessionID string,
	locator baldasession.SessionLocator,
	messageID int,
	progressPolicy baldachannel.ProgressPolicy,
) error {
	return h.runTurnWithDelivery(ctx, text, r, userID, sessionID, "", agentSessionID, locator, messageID, progressPolicy, true)
}

func (c *baldaRunTurnTelegramClient) SendMessageWithResponse(
	_ context.Context,
	body client.SendMessageJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendMessageResponse, error) {
	c.messages = append(c.messages, body)
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

func (c *baldaRunTurnTelegramClient) SendRichMessageWithResponse(
	_ context.Context,
	body client.SendRichMessageJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendRichMessageResponse, error) {
	c.richMessages = append(c.richMessages, body)
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

func (c *baldaRunTurnTelegramClient) SendMessageDraftWithResponse(
	_ context.Context,
	body client.SendMessageDraftJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendMessageDraftResponse, error) {
	c.drafts = append(c.drafts, body)
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

func (c *baldaRunTurnTelegramClient) SendRichMessageDraftWithResponse(
	_ context.Context,
	body client.SendRichMessageDraftJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendRichMessageDraftResponse, error) {
	c.richDrafts = append(c.richDrafts, body)
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

func (c *baldaRunTurnTelegramClient) SendChatActionWithResponse(
	_ context.Context,
	body client.SendChatActionJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendChatActionResponse, error) {
	c.chatActions = append(c.chatActions, body)
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

func baldaRunTurnDraftText(t *testing.T, draft client.SendMessageDraftJSONRequestBody) string {
	t.Helper()
	if draft.Text == nil {
		t.Fatal("draft text is nil")
	}
	return *draft.Text
}

func baldaRunTurnRichDraftMarkdown(t *testing.T, draft client.SendRichMessageDraftJSONRequestBody) string {
	t.Helper()
	if draft.RichMessage.Markdown == nil {
		t.Fatal("rich draft markdown is nil")
	}
	return *draft.RichMessage.Markdown
}

func baldaRunTurnRichMessageMarkdown(t *testing.T, message client.SendRichMessageJSONRequestBody) string {
	t.Helper()
	if message.RichMessage.Markdown == nil {
		t.Fatal("rich message markdown is nil")
	}
	return *message.RichMessage.Markdown
}

func newBaldaPlanSnapshot(entries ...map[string]any) map[string]any {
	return map[string]any{
		"entries": entries,
	}
}

func TestRunTurn_SendsPlanUpdateDraftFromCustomMetadataInDM(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan": newBaldaPlanSnapshot(
				map[string]any{"content": "Run tests", "status": "in_progress", "priority": "medium"},
				map[string]any{"content": "Ship fix", "status": "pending", "priority": "high"},
			),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 1 {
		t.Fatalf("draft calls = %d, want 1", len(tgClient.drafts))
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[0]); got != "Plan update\n- [in progress] Run tests\n- [pending] Ship fix" {
		t.Fatalf("draft[0].text = %q, want plan update text", got)
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnFinalAnswerText {
		t.Fatalf("message text = %q, want final answer", got)
	}
}

func TestRunTurn_SendsProgressForNonTerminalEventsInDM(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("markdownv2")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 2 {
		t.Fatalf("draft calls = %d, want 2", len(tgClient.drafts))
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[0]); got != baldaRunTurnReasoningOne {
		t.Fatalf("draft[0].text = %q, want %s", got, baldaRunTurnReasoningOne)
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[1]); got != baldaRunTurnReasoningTwo {
		t.Fatalf("draft[1].text = %q, want %s", got, baldaRunTurnReasoningTwo)
	}
	for i, draft := range tgClient.drafts {
		if draft.MessageThreadId == nil || *draft.MessageThreadId != 77 {
			t.Fatalf("draft[%d].message_thread_id = %v, want 77", i, draft.MessageThreadId)
		}
	}

	if len(tgClient.chatActions) != 3 {
		t.Fatalf("chat action calls = %d, want 3", len(tgClient.chatActions))
	}
	for i, action := range tgClient.chatActions {
		if action.Action != "typing" {
			t.Fatalf("chatActions[%d].action = %q, want typing", i, action.Action)
		}
		if action.ChatId != 9001 {
			t.Fatalf("chatActions[%d].chat_id = %d, want 9001", i, action.ChatId)
		}
		if action.MessageThreadId == nil || *action.MessageThreadId != 77 {
			t.Fatalf("chatActions[%d].message_thread_id = %v, want 77", i, action.MessageThreadId)
		}
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if !strings.Contains(tgClient.messages[0].Text, baldaRunTurnFinalAnswerText) {
		t.Fatalf("message text = %q, want to contain final answer", tgClient.messages[0].Text)
	}
	if tgClient.messages[0].ParseMode == nil || *tgClient.messages[0].ParseMode != testParseModeMarkdown {
		t.Fatalf("parse_mode = %v, want MarkdownV2", tgClient.messages[0].ParseMode)
	}
}

func TestRunTurn_SkipsTypingAndDraftWhenAllProgressDisabled(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("markdownv2")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.chatActions) != 0 {
		t.Fatalf("chat action calls = %d, want 0", len(tgClient.chatActions))
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if !strings.Contains(tgClient.messages[0].Text, baldaRunTurnFinalAnswerText) {
		t.Fatalf("message text = %q, want to contain final answer", tgClient.messages[0].Text)
	}
}

func TestRunTurn_SendsTypingWithoutThinkingDraftInPublicChat(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("markdownv2")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: false, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.chatActions) != 3 {
		t.Fatalf("chat action calls = %d, want 3", len(tgClient.chatActions))
	}
}

func TestRunTurn_DirectTelegramPathUsesDeliveryEnvelopesWithoutJobEvents(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	bus, ok := h.actorDispatcher.(*recordingHandlerCommandBus)
	if !ok || bus == nil {
		t.Fatal("actorDispatcher is not a recording handler bus")
	}

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "in_progress"}),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(bus.eventEnvs) != 0 {
		t.Fatalf("job events = %d, want 0", len(bus.eventEnvs))
	}
	if got := deliveryModeCount(t, bus.commands, actors.DeliveryModeChatAction); got != 0 {
		t.Fatalf("chat action delivery commands = %d, want 0", got)
	}
	if got := deliveryModeCount(t, bus.commands, actors.DeliveryModeProgress); got != 1 {
		t.Fatalf("progress delivery commands = %d, want 1", got)
	}
	gotTexts := deliveryTextsFromCommands(t, bus.commands)
	wantTexts := []string{
		"Plan update\n- [in progress] Run tests",
		baldaRunTurnFinalAnswerText,
	}
	if strings.Join(gotTexts, "\n---\n") != strings.Join(wantTexts, "\n---\n") {
		t.Fatalf("delivery texts = %#v, want %#v", gotTexts, wantTexts)
	}
	for _, env := range bus.commands {
		if env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		if strings.TrimSpace(baldaexecution.EnvelopeJobID(env)) != "" {
			t.Fatalf("delivery env job_id = %q, want empty for direct telegram path", baldaexecution.EnvelopeJobID(env))
		}
		var payload actors.DeliveryPayload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		if strings.TrimSpace(payload.JobID) != "" {
			t.Fatalf("delivery payload job_id = %q, want empty for direct telegram path", payload.JobID)
		}
	}
	if len(tgClient.drafts) != 1 || len(tgClient.messages) != 1 || len(tgClient.chatActions) != 1 {
		t.Fatalf("telegram sends = drafts:%d messages:%d chat_actions:%d, want 1/1/1", len(tgClient.drafts), len(tgClient.messages), len(tgClient.chatActions))
	}
}

func TestBaldaSessionTurnRunner_DirectTelegramProgressDeliveriesComeFromSessionActor(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	bus, ok := h.actorDispatcher.(*recordingHandlerCommandBus)
	if !ok || bus == nil {
		t.Fatal("actorDispatcher is not a recording handler bus")
	}

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "in_progress"}),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	ts := newBaldaTopicSession(t, sessionID)
	setUnexportedField(t, ts, "runner", adkRunner)
	setUnexportedField(t, ts, "locator", locator)
	setUnexportedField(t, ts, "userID", "tg-101")
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	executor := &providerTurnExecutor{dispatcher: bus, logger: zerolog.Nop()}
	sessionRunner := sessionturn.New(sessionTurnSessionAccessor{manager: manager}, executor, nil, zerolog.Nop())

	err := sessionRunner.RunSessionTurnPayload(context.Background(), actors.SessionTurnPayload{
		Text:           "hello",
		Locator:        locator,
		UserID:         "tg-101",
		AgentSessionID: sessionID,
		MessageID:      41,
		TopicID:        77,
		DeliveryOptions: deliveryfmt.Options{
			ProgressPolicy: deliveryfmt.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true},
		},
		ProgressPolicy: baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true},
		Deliver:        true,
		Source:         "telegram",
	})
	if err != nil {
		t.Fatalf("RunSessionTurnPayload() error = %v", err)
	}

	for _, env := range bus.commands {
		if env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		if env.From.Target != baldaexecution.ActorTypeSession {
			t.Fatalf("delivery from target = %q, want %q", env.From.Target, baldaexecution.ActorTypeSession)
		}
		if env.From.Key != sessionID {
			t.Fatalf("delivery from key = %q, want %q", env.From.Key, sessionID)
		}
	}
	if len(tgClient.drafts) != 1 || len(tgClient.messages) != 1 || len(tgClient.chatActions) != 1 {
		t.Fatalf("telegram sends = drafts:%d messages:%d chat_actions:%d, want 1/1/1", len(tgClient.drafts), len(tgClient.messages), len(tgClient.chatActions))
	}
}

func TestRunTurn_SendsPlanUpdateMessagesInPublicChat(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, true)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		planOne := adksession.NewEvent(context.Background(), invocationID)
		planOne.Partial = true
		planOne.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "pending"}),
		}

		planTwo := adksession.NewEvent(context.Background(), invocationID)
		planTwo.Partial = true
		planTwo.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan": newBaldaPlanSnapshot(
				map[string]any{"content": "Run tests", "status": "completed"},
				map[string]any{"content": "Ship fix", "status": "in_progress"},
			),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{planOne, planTwo, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.messages) != 3 {
		t.Fatalf("message calls = %d, want 3", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != "Plan update\n- [pending] Run tests" {
		t.Fatalf("messages[0].text = %q, want first plan update", got)
	}
	if got := tgClient.messages[1].Text; got != "Plan update\n- [completed] Run tests\n- [in progress] Ship fix" {
		t.Fatalf("messages[1].text = %q, want second plan update", got)
	}
	if got := tgClient.messages[2].Text; got != baldaRunTurnFinalAnswerText {
		t.Fatalf("messages[2].text = %q, want final answer", got)
	}
	if tgClient.messages[0].ParseMode != nil || tgClient.messages[1].ParseMode != nil {
		t.Fatal("plan update messages must be plain text without parse_mode")
	}
}

func TestRunTurn_TaskBackedProgressUsesDeliveryActor(t *testing.T) {
	t.Parallel()

	h, tgClient, bus, tasks := newBaldaRunTurnTaskTestHandler(t)
	if _, err := tasks.Create(context.Background(), baldastate.JobRecord{
		ID:        "task-1",
		SessionID: "session-1",
		Objective: "run turn",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "in_progress"}),
		}

		partial := adksession.NewEvent(context.Background(), invocationID)
		partial.Partial = true
		partial.Content = genai.NewContentFromText("draft answer", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, partial, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurnWithDelivery(context.Background(), "hello", adkRunner, "tg-101", sessionID, "task-1", sessionID, locator, 41, baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}, true); err != nil {
		t.Fatalf("runTurnWithDelivery() error = %v", err)
	}

	if len(tgClient.drafts) != 0 || len(tgClient.chatActions) != 0 || len(tgClient.messages) != 0 {
		t.Fatalf("telegram direct sends = drafts:%d typing:%d messages:%d, want 0", len(tgClient.drafts), len(tgClient.chatActions), len(tgClient.messages))
	}
	if got := deliveryModeCount(t, bus.commands, actors.DeliveryModeChatAction); got != 0 {
		t.Fatalf("chat action delivery commands = %d, want 0", got)
	}
	if got := deliveryModeCount(t, bus.commands, actors.DeliveryModeProgress); got != 2 {
		t.Fatalf("progress delivery commands = %d, want 2", got)
	}
	gotTexts := deliveryTextsFromCommands(t, bus.commands)
	wantTexts := []string{
		"Plan update\n- [in progress] Run tests",
		baldaRunTurnFinalAnswerText,
	}
	if strings.Join(gotTexts, "\n---\n") != strings.Join(wantTexts, "\n---\n") {
		t.Fatalf("delivery texts = %#v, want %#v", gotTexts, wantTexts)
	}
	agentEvents := taskEventsOfType(bus.eventEnvs, baldajobs.JobEventAgentProgress, baldajobs.JobEventAgentResult)
	if len(agentEvents) != 2 {
		t.Fatalf("agent job events = %d, want 2", len(agentEvents))
	}
	if got := agentEvents[0].Meta["event_type"]; got != baldajobs.JobEventAgentProgress {
		t.Fatalf("event[0] type = %q, want %q", got, baldajobs.JobEventAgentProgress)
	}
	if got := taskEventPayload(t, agentEvents[0])["kind"]; got != "plan" {
		t.Fatalf("event[0] kind = %v, want plan", got)
	}
	if got := agentEvents[1].Meta["event_type"]; got != baldajobs.JobEventAgentResult {
		t.Fatalf("event[1] type = %q, want %q", got, baldajobs.JobEventAgentResult)
	}
	if got := taskEventPayload(t, agentEvents[1])["kind"]; got != nil {
		t.Fatalf("event[1] kind = %v, want nil", got)
	}
}

func TestRunTurn_TaskBackedVisibleOutputOnlySendsFinalReply(t *testing.T) {
	t.Parallel()

	h, _, bus, tasks := newBaldaRunTurnTaskTestHandler(t)
	if _, err := tasks.Create(context.Background(), baldastate.JobRecord{
		ID:        "task-2",
		SessionID: "session-1",
		Objective: "run turn",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		partialOne := adksession.NewEvent(context.Background(), invocationID)
		partialOne.Partial = true
		partialOne.Content = genai.NewContentFromText("Hello", genai.RoleModel)

		partialTwo := adksession.NewEvent(context.Background(), invocationID)
		partialTwo.Partial = true
		partialTwo.Content = genai.NewContentFromText("Hello there", genai.RoleModel)

		partialThree := adksession.NewEvent(context.Background(), invocationID)
		partialThree.Partial = true
		partialThree.Content = genai.NewContentFromText("Hello there friend", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText("Hello there friend!", genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{partialOne, partialTwo, partialThree, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	options := deliveryfmt.Options{Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatAuto, TelegramMode: "markdownv2"}}
	if err := h.runTurnWithDeliveryOptions(context.Background(), "hello", adkRunner, "tg-101", sessionID, "task-2", sessionID, locator, 41, options, true); err != nil {
		t.Fatalf("runTurnWithDeliveryOptions() error = %v", err)
	}

	gotTexts := deliveryTextsFromCommands(t, bus.commands)
	wantTexts := []string{"Hello there friend!"}
	if strings.Join(gotTexts, "\n---\n") != strings.Join(wantTexts, "\n---\n") {
		t.Fatalf("delivery texts = %#v, want %#v", gotTexts, wantTexts)
	}
	agentEvents := taskEventsOfType(bus.eventEnvs, baldajobs.JobEventAgentProgress, baldajobs.JobEventAgentResult)
	if len(agentEvents) != 1 {
		t.Fatalf("agent job events = %d, want 1", len(agentEvents))
	}
	if got := agentEvents[0].Meta["event_type"]; got != baldajobs.JobEventAgentResult {
		t.Fatalf("event[0] type = %q, want %q", got, baldajobs.JobEventAgentResult)
	}
	var payload actors.DeliveryPayload
	if err := actorlayer.UnmarshalPayload(bus.commands[len(bus.commands)-1].Payload, &payload); err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	if string(payload.Profile.Format) != string(options.Profile.Format) || payload.Profile.TelegramMode != options.Profile.TelegramMode {
		t.Fatalf("delivery profile = %+v, want %+v", payload.Profile, options.Profile)
	}
}

func TestRunTurn_TaskBackedDuplicatePartialAndFinalOnlyDeliversOnce(t *testing.T) {
	t.Parallel()

	h, _, bus, tasks := newBaldaRunTurnTaskTestHandler(t)
	if _, err := tasks.Create(context.Background(), baldastate.JobRecord{
		ID:        "task-3",
		SessionID: "session-1",
		Objective: "run turn",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		partial := adksession.NewEvent(context.Background(), invocationID)
		partial.Partial = true
		partial.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{partial, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurnWithDelivery(context.Background(), "hello", adkRunner, "tg-101", sessionID, "task-3", sessionID, locator, 41, baldachannel.ProgressPolicy{}, true); err != nil {
		t.Fatalf("runTurnWithDelivery() error = %v", err)
	}

	gotTexts := deliveryTextsFromCommands(t, bus.commands)
	wantTexts := []string{baldaRunTurnFinalAnswerText}
	if strings.Join(gotTexts, "\n---\n") != strings.Join(wantTexts, "\n---\n") {
		t.Fatalf("delivery texts = %#v, want %#v", gotTexts, wantTexts)
	}
}

func TestRunTurn_SendsProgressAndGenericMessageForNonThoughtEventsWithoutFinalReply(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		toolCall := adksession.NewEvent(context.Background(), invocationID)
		toolCall.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{FunctionCall: &genai.FunctionCall{Name: "tool.one"}},
			},
		}

		partial := adksession.NewEvent(context.Background(), invocationID)
		partial.Partial = true
		partial.Content = genai.NewContentFromText("visible", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{toolCall, partial, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.chatActions) != 2 {
		t.Fatalf("chat action calls = %d, want 2", len(tgClient.chatActions))
	}
	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnGenericEmptyTerminalMessage {
		t.Fatalf("message text = %q, want %q", got, baldaRunTurnGenericEmptyTerminalMessage)
	}
}

func TestRunTurn_SendsTypingAgainAfterThrottleInterval(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("markdownv2")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.chatActions) != 3 {
		t.Fatalf("chat action calls = %d, want 3", len(tgClient.chatActions))
	}
}

func TestRunTurn_SendsThinkingDraftForEachNonTerminalEvent(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Thinking: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 2 {
		t.Fatalf("draft calls = %d, want 2", len(tgClient.drafts))
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[0]); got != baldaRunTurnReasoningOne {
		t.Fatalf("draft[0].text = %q, want %s", got, baldaRunTurnReasoningOne)
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[1]); got != baldaRunTurnReasoningTwo {
		t.Fatalf("draft[1].text = %q, want %s", got, baldaRunTurnReasoningTwo)
	}
}

func TestRunTurn_DoesNotFallBackToThinkingAfterPlanDraftInDM(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		thought := adksession.NewEvent(context.Background(), invocationID)
		thought.Partial = true
		thought.Content = &genai.Content{
			Role:  genai.RoleModel,
			Parts: []*genai.Part{{Thought: true, Text: baldaRunTurnReasoningOne}},
		}

		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "in_progress"}),
		}

		thoughtTwo := adksession.NewEvent(context.Background(), invocationID)
		thoughtTwo.Partial = true
		thoughtTwo.Content = &genai.Content{
			Role:  genai.RoleModel,
			Parts: []*genai.Part{{Thought: true, Text: baldaRunTurnReasoningTwo}},
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{thought, plan, thoughtTwo, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 3 {
		t.Fatalf("draft calls = %d, want 3", len(tgClient.drafts))
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[0]); got != baldaRunTurnReasoningOne {
		t.Fatalf("draft[0].text = %q, want %s", got, baldaRunTurnReasoningOne)
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[1]); got != "Plan update\n- [in progress] Run tests" {
		t.Fatalf("draft[1].text = %q, want plan update text", got)
	}
	if got := baldaRunTurnDraftText(t, tgClient.drafts[2]); got != baldaRunTurnReasoningTwo {
		t.Fatalf("draft[2].text = %q, want %s", got, baldaRunTurnReasoningTwo)
	}
}

func TestRunTurn_SendsFinalResponseWithoutParseModeWhenConfiguredNone(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if tgClient.messages[0].ParseMode != nil {
		t.Fatalf("parse_mode = %v, want nil", *tgClient.messages[0].ParseMode)
	}
}

func TestRunTurn_SkipsExactDuplicateFinalAfterStreamedText(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		partial := adksession.NewEvent(context.Background(), invocationID)
		partial.Partial = true
		partial.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)

		final := adksession.NewEvent(context.Background(), invocationID)
		final.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{partial, final, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1", len(tgClient.richMessages))
	}
	gotMarkdown := tgClient.richMessages[0].RichMessage.Markdown
	if gotMarkdown == nil {
		t.Fatal("rich message markdown = nil, want final answer")
	}
	if got := strings.TrimSpace(*gotMarkdown); got != baldaRunTurnFinalAnswerText {
		t.Fatalf("rich markdown = %q, want final answer", *gotMarkdown)
	}
}

func TestRunTurn_MergesFinalResponseDeltaChunksOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		chunkOne := adksession.NewEvent(context.Background(), invocationID)
		chunkOne.Content = genai.NewContentFromText("Пункт списка1\n", genai.RoleModel)

		chunkTwo := adksession.NewEvent(context.Background(), invocationID)
		chunkTwo.Content = genai.NewContentFromText("- Пункт списка2\n", genai.RoleModel)

		chunkThree := adksession.NewEvent(context.Background(), invocationID)
		chunkThree.Content = genai.NewContentFromText("- Пункт списка3", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{chunkOne, chunkTwo, chunkThree, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	want := "Пункт списка1\n- Пункт списка2\n- Пункт списка3"
	if got := tgClient.messages[0].Text; got != want {
		t.Fatalf("message text = %q, want %q", got, want)
	}
}

func TestRunTurn_AppendsFinalResponseTextEventsOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		chunkOne := adksession.NewEvent(context.Background(), invocationID)
		chunkOne.Content = genai.NewContentFromText("Doing", genai.RoleModel)

		chunkTwo := adksession.NewEvent(context.Background(), invocationID)
		chunkTwo.Content = genai.NewContentFromText("Doing well", genai.RoleModel)

		chunkThree := adksession.NewEvent(context.Background(), invocationID)
		chunkThree.Content = genai.NewContentFromText("Doing well.", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{chunkOne, chunkTwo, chunkThree, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != "DoingDoing wellDoing well." {
		t.Fatalf("message text = %q, want appended chunks", got)
	}
}

func TestRunTurn_SendsGenericMessageWhenOnlyNonFinalTextExistsOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		nonFinalOne := adksession.NewEvent(context.Background(), invocationID)
		nonFinalOne.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{FunctionCall: &genai.FunctionCall{Name: "tool.one"}},
				{Text: "previous fallback"},
			},
		}

		nonFinalTwo := adksession.NewEvent(context.Background(), invocationID)
		nonFinalTwo.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{FunctionCall: &genai.FunctionCall{Name: "tool.two"}},
				{Text: "new fallback"},
			},
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{nonFinalOne, nonFinalTwo, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1", len(tgClient.richMessages))
	}
	gotHTML := tgClient.richMessages[0].RichMessage.Html
	if gotHTML == nil {
		t.Fatal("rich message html = nil, want generic terminal message")
	}
	if got := *gotHTML; got != baldaRunTurnGenericEmptyTerminalMessage {
		t.Fatalf("rich html = %q, want %q", got, baldaRunTurnGenericEmptyTerminalMessage)
	}
	if tgClient.richMessages[0].RichMessage.SkipEntityDetection == nil || !*tgClient.richMessages[0].RichMessage.SkipEntityDetection {
		t.Fatalf("skip_entity_detection = %v, want true", tgClient.richMessages[0].RichMessage.SkipEntityDetection)
	}
}

func TestRunTurn_DoesNotLeakNonFinalProgressTextInPublicChat(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		progressOne := adksession.NewEvent(context.Background(), invocationID)
		progressOne.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{FunctionCall: &genai.FunctionCall{Name: "approve"}},
				{Text: "Сделаю: поставлю Approve и добавлю комментарий."},
			},
		}

		progressTwo := adksession.NewEvent(context.Background(), invocationID)
		progressTwo.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{FunctionCall: &genai.FunctionCall{Name: "comment"}},
				{Text: "Ставлю Approve и добавляю общий комментарий."},
			},
		}

		final := adksession.NewEvent(context.Background(), invocationID)
		final.Content = genai.NewContentFromText("Готово.\n\n- В PR 1762 поставил Approved.\n- Добавил комментарий.", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{progressOne, progressTwo, final, done}
	})
	locator := baldatelegram.NewLocator(-5173524191, 0)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "approve PR", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.chatActions) != 3 {
		t.Fatalf("chat action calls = %d, want 3", len(tgClient.chatActions))
	}
	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	got := tgClient.messages[0].Text
	if strings.Contains(got, "Сделаю") || strings.Contains(got, "Ставлю Approve") {
		t.Fatalf("message text = %q, contains non-final progress text", got)
	}
	if !strings.Contains(got, "Готово.") || !strings.Contains(got, "Approved") {
		t.Fatalf("message text = %q, want final response", got)
	}
}

func TestRunTurn_SendsFinalTextFromTurnCompleteEvent(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		progress := adksession.NewEvent(context.Background(), invocationID)
		progress.Partial = true
		progress.Content = genai.NewContentFromText("working...", genai.RoleModel)

		toolUpdate := adksession.NewEvent(context.Background(), invocationID)
		toolUpdate.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{
					FunctionResponse: &genai.FunctionResponse{
						ID:   "tool-1",
						Name: "acp_tool_call_update",
						Response: map[string]any{
							"status": "completed",
						},
					},
				},
			},
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.FinishReason = genai.FinishReasonStop
		done.TurnComplete = true

		return []*adksession.Event{progress, toolUpdate, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnFinalAnswerText {
		t.Fatalf("message text = %q, want final answer", got)
	}
}

func TestRunTurn_UsesPlanStateDeltaFallback(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, true)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.Actions.StateDelta["acp_plan"] = newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "pending"})

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 2 {
		t.Fatalf("message calls = %d, want 2", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != "Plan update\n- [pending] Run tests" {
		t.Fatalf("messages[0].text = %q, want current plan update text", got)
	}
}

func TestRunTurn_DeduplicatesRepeatedPlanUpdates(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, true)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		planOne := adksession.NewEvent(context.Background(), invocationID)
		planOne.Partial = true
		planOne.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "pending"}),
		}

		planTwo := adksession.NewEvent(context.Background(), invocationID)
		planTwo.Partial = true
		planTwo.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "pending"}),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{planOne, planTwo, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 2 {
		t.Fatalf("message calls = %d, want 2", len(tgClient.messages))
	}
}

func TestRunTurn_PlanUpdatesDisabledDoesNotSendPlaceholderDraft(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, true)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan":        newBaldaPlanSnapshot(map[string]any{"content": "Run tests", "status": "pending"}),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Thinking: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.drafts) != 0 {
		t.Fatalf("draft calls = %d, want 0", len(tgClient.drafts))
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
}

func TestRunTurn_DoesNotSendWithoutTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		final := adksession.NewEvent(context.Background(), invocationID)
		final.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		return []*adksession.Event{final}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 0 {
		t.Fatalf("message calls = %d, want 0", len(tgClient.messages))
	}
}

func TestRunTurn_SendsGenericMessageWhenOnlyPartialTextExistsOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		partialOne := adksession.NewEvent(context.Background(), invocationID)
		partialOne.Partial = true
		partialOne.Content = genai.NewContentFromText("Doing", genai.RoleModel)

		partialTwo := adksession.NewEvent(context.Background(), invocationID)
		partialTwo.Partial = true
		partialTwo.Content = genai.NewContentFromText(" well", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{partialOne, partialTwo, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnGenericEmptyTerminalMessage {
		t.Fatalf("message text = %q, want %q", got, baldaRunTurnGenericEmptyTerminalMessage)
	}
}

func TestRunTurn_SendsGenericMessageWhenOnlyPartialMarkdownChunksExistOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		partialOne := adksession.NewEvent(context.Background(), invocationID)
		partialOne.Partial = true
		partialOne.Content = genai.NewContentFromText("**Статус задачи**", genai.RoleModel)

		partialTwo := adksession.NewEvent(context.Background(), invocationID)
		partialTwo.Partial = true
		partialTwo.Content = genai.NewContentFromText("\n", genai.RoleModel)

		partialThree := adksession.NewEvent(context.Background(), invocationID)
		partialThree.Partial = true
		partialThree.Content = genai.NewContentFromText("- **Task:** `balda-runtime`\n- **Status:** in progress", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{partialOne, partialTwo, partialThree, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnGenericEmptyTerminalMessage {
		t.Fatalf("message text = %q, want %q", got, baldaRunTurnGenericEmptyTerminalMessage)
	}
}

func TestRunTurn_SendsGenericMessageWhenOnlyThoughtOrPartialTextExistsOnTurnComplete(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		thought := adksession.NewEvent(context.Background(), invocationID)
		thought.Partial = true
		thought.Content = &genai.Content{
			Role: genai.RoleModel,
			Parts: []*genai.Part{
				{Thought: true, Text: "secret"},
			},
		}

		partial := adksession.NewEvent(context.Background(), invocationID)
		partial.Partial = true
		partial.Content = genai.NewContentFromText("visible", genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{thought, partial, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnGenericEmptyTerminalMessage {
		t.Fatalf("message text = %q, want %q", got, baldaRunTurnGenericEmptyTerminalMessage)
	}
}

func TestRunTurn_SendsFinishReasonMessageOnEmptyTurnComplete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		finishReason genai.FinishReason
		want         string
	}{
		{name: "empty", finishReason: genai.FinishReason(""), want: baldaRunTurnGenericEmptyTerminalMessage},
		{name: "unspecified", finishReason: genai.FinishReasonUnspecified, want: baldaRunTurnGenericEmptyTerminalMessage},
		{name: "stop", finishReason: genai.FinishReasonStop, want: baldaRunTurnGenericEmptyTerminalMessage},
		{name: "max tokens", finishReason: genai.FinishReasonMaxTokens, want: "The provider hit the output limit before producing a visible reply. Ask for a shorter answer or split the request."},
		{name: "safety", finishReason: genai.FinishReasonSafety, want: "The provider blocked this turn for safety reasons. Please rephrase and try again."},
		{name: "recitation", finishReason: genai.FinishReasonRecitation, want: "The provider blocked this turn because it may reproduce protected source material. Please rephrase and try again."},
		{name: "language", finishReason: genai.FinishReasonLanguage, want: "The provider could not answer because the request used an unsupported language. Please rephrase in a supported language and try again."},
		{name: "other", finishReason: genai.FinishReasonOther, want: baldaRunTurnGenericEmptyTerminalMessage},
		{name: "blocklist", finishReason: genai.FinishReasonBlocklist, want: "The provider blocked this turn because it matched restricted terms. Please rephrase and try again."},
		{name: "prohibited content", finishReason: genai.FinishReasonProhibitedContent, want: "The provider rejected this turn as prohibited content. Please rephrase and try again."},
		{name: "spii", finishReason: genai.FinishReasonSPII, want: "The provider blocked this turn because it may contain sensitive personal information. Please remove that information and try again."},
		{name: "malformed function call", finishReason: genai.FinishReasonMalformedFunctionCall, want: "The provider ended the turn with an invalid function call. Please try again."},
		{name: "unexpected tool call", finishReason: genai.FinishReasonUnexpectedToolCall, want: "The provider ended the turn with an unexpected tool call. Please try again."},
		{name: "image safety", finishReason: genai.FinishReasonImageSafety, want: "The provider blocked image generation for safety reasons. Please try a different request."},
		{name: "image prohibited content", finishReason: genai.FinishReasonImageProhibitedContent, want: "The provider rejected image generation as prohibited content. Please try a different request."},
		{name: "no image", finishReason: genai.FinishReasonNoImage, want: "The provider completed the turn without returning an image. Please try a different request."},
		{name: "image recitation", finishReason: genai.FinishReasonImageRecitation, want: "The provider blocked image generation because it may reproduce protected source material. Please try a different request."},
		{name: "image other", finishReason: genai.FinishReasonImageOther, want: "The provider ended image generation without a usable result. Please try again."},
		{name: "unknown", finishReason: genai.FinishReason("MYSTERY"), want: baldaRunTurnGenericEmptyTerminalMessage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, tgClient := newBaldaRunTurnTestHandler(t, false)
			adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
				done := adksession.NewEvent(context.Background(), invocationID)
				done.FinishReason = tt.finishReason
				done.TurnComplete = true

				return []*adksession.Event{done}
			})
			locator := baldatelegram.NewLocator(9001, 77)
			if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
				t.Fatalf("runTurn() error = %v", err)
			}

			if len(tgClient.messages) != 1 {
				t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
			}
			if got := tgClient.messages[0].Text; got != tt.want {
				t.Fatalf("message text = %q, want %q", got, tt.want)
			}
			if tgClient.messages[0].ParseMode != nil {
				t.Fatalf("parse_mode = %v, want nil", *tgClient.messages[0].ParseMode)
			}
		})
	}
}

func TestRunTurn_AppendsProviderMessageExcerptForEmptyTurnComplete(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		done := adksession.NewEvent(context.Background(), invocationID)
		done.FinishReason = genai.FinishReasonProhibitedContent
		done.TurnComplete = true

		return []*adksession.Event{done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	want := "The provider rejected this turn as prohibited content. Please rephrase and try again."
	if got := tgClient.messages[0].Text; got != want {
		t.Fatalf("message text = %q, want %q", got, want)
	}
}

func TestRunTurn_PrefersProviderErrorMessageOnEmptyTurnComplete(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		done := adksession.NewEvent(context.Background(), invocationID)
		done.ErrorCode = "provider_error"
		done.ErrorMessage = "model gpt-5.3-codex is not available for this account"
		done.FinishReason = genai.FinishReasonProhibitedContent
		done.TurnComplete = true

		return []*adksession.Event{done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	want := "Provider error: model gpt-5.3-codex is not available for this account"
	if got := tgClient.messages[0].Text; got != want {
		t.Fatalf("message text = %q, want %q", got, want)
	}
}

func TestRunTurn_FallsBackToLastNonRetryProviderErrorOnEmptyTurnComplete(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		retrying := adksession.NewEvent(context.Background(), invocationID)
		retrying.ErrorMessage = "Reconnecting... 1/5"

		terminal := adksession.NewEvent(context.Background(), invocationID)
		terminal.ErrorMessage = "unexpected status 401 Unauthorized: Missing bearer or basic authentication in header"

		done := adksession.NewEvent(context.Background(), invocationID)
		done.ErrorCode = "provider_error"
		done.TurnComplete = true

		return []*adksession.Event{retrying, terminal, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	want := "Provider error: unexpected status 401 Unauthorized: Missing bearer or basic authentication in header"
	if got := tgClient.messages[0].Text; got != want {
		t.Fatalf("message text = %q, want %q", got, want)
	}
}

func TestRunTurnTaskWithDelivery_HardFailureSuggestsReset(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, false)
	locator := baldatelegram.NewLocator(9001, 77)
	h.sessionManager = newBaldaSessionManagerWithSession(t, locator, newBaldaTopicSession(t, locator.SessionID))

	baldaAgent, err := adkagent.New(adkagent.Config{
		Name:        "BaldaRunTurnErrorAgent",
		Description: "Returns a terminal runner error",
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				_ = yield(nil, errors.New("agent run failed"))
			}
		},
	})
	if err != nil {
		t.Fatalf("agent.New() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	adkRunner, err := runner.New(runner.Config{
		AppName:        "balda-run-turn-error-test",
		Agent:          baldaAgent,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}

	if err := h.runTurnJobWithDelivery(
		context.Background(),
		"hello",
		adkRunner,
		"tg-101",
		locator.SessionID,
		"",
		locator.SessionID,
		locator,
		41,
		77,
		baldachannel.ProgressPolicy{},
		true,
	); err == nil {
		t.Fatal("runTurnJobWithDelivery() error = nil, want error")
	}

	if len(tgClient.messages) == 0 {
		t.Fatal("expected error message delivery")
	}
	want := "Agent execution failed. Use /reset or /restart to restart this session."
	if got := tgClient.messages[len(tgClient.messages)-1].Text; got != want {
		t.Fatalf("message text = %q, want %q", got, want)
	}
}

func TestRunTurnWithDelivery_AcceptsSlackLocator(t *testing.T) {
	t.Parallel()

	bus := &recordingHandlerCommandBus{}
	h := &BaldaHandler{
		actorDispatcher: bus,
		logger:          zerolog.Nop(),
		turnExecution:   sessionturnapp.NewTurnExecutionServiceWithJobEvents(bus, nil, nil, zerolog.Nop()),
	}
	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		reply := adksession.NewEvent(context.Background(), invocationID)
		reply.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true
		return []*adksession.Event{reply, done}
	})
	locator := baldaslack.NewThreadLocator("T123", "C456", "1712345678.000100")

	if err := h.runTurnWithDelivery(context.Background(), "hello", adkRunner, "tg-101", sessionID, "", sessionID, locator, 41, baldachannel.ProgressPolicy{}, true); err != nil {
		t.Fatalf("runTurnWithDelivery() error = %v", err)
	}
	if len(bus.commands) != 2 {
		t.Fatalf("dispatch calls = %d, want 2", len(bus.commands))
	}
	var payload actors.DeliveryPayload
	if err := actorlayer.UnmarshalPayload(bus.commands[len(bus.commands)-1].Payload, &payload); err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	if payload.Locator.ChannelType != baldastate.ChannelTypeSlackChat {
		t.Fatalf("delivery channel type = %q, want slack", payload.Locator.ChannelType)
	}
	if payload.Text != baldaRunTurnFinalAnswerText {
		t.Fatalf("delivery text = %q, want final answer", payload.Text)
	}
}

func TestRunTurn_DoesNotAppendFinishReasonMessageWhenFinalTextExists(t *testing.T) {
	t.Parallel()

	h, tgClient := newBaldaRunTurnTestHandler(t, true)
	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.FinishReason = genai.FinishReasonMaxTokens
		done.TurnComplete = true

		return []*adksession.Event{done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, baldachannel.ProgressPolicy{}); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("message calls = %d, want 1", len(tgClient.messages))
	}
	if got := tgClient.messages[0].Text; got != baldaRunTurnFinalAnswerText {
		t.Fatalf("message text = %q, want final answer", got)
	}
}

func newBaldaRunTurnTestRunner(t *testing.T) (*runner.Runner, string) {
	t.Helper()

	return newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		thoughtOne := adksession.NewEvent(context.Background(), invocationID)
		thoughtOne.Content = &genai.Content{
			Role:  genai.RoleModel,
			Parts: []*genai.Part{{Thought: true, Text: baldaRunTurnReasoningOne}},
		}

		thoughtTwo := adksession.NewEvent(context.Background(), invocationID)
		thoughtTwo.Content = &genai.Content{
			Role:  genai.RoleModel,
			Parts: []*genai.Part{{Thought: true, Text: baldaRunTurnReasoningTwo}},
		}

		reply := adksession.NewEvent(context.Background(), invocationID)
		reply.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)

		done := adksession.NewEvent(context.Background(), invocationID)
		done.TurnComplete = true

		return []*adksession.Event{thoughtOne, thoughtTwo, reply, done}
	})
}

func TestRunTurn_SendsRichMarkdownReasoningDraftsInDM(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("rich_markdown")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunner(t)
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.richDrafts) != 2 {
		t.Fatalf("rich draft calls = %d, want 2", len(tgClient.richDrafts))
	}
	if got := baldaRunTurnRichDraftMarkdown(t, tgClient.richDrafts[0]); got != baldaRunTurnReasoningOne {
		t.Fatalf("rich draft[0] markdown = %q, want reasoning payload without tg wrapper", got)
	}
	if got := baldaRunTurnRichDraftMarkdown(t, tgClient.richDrafts[1]); got != baldaRunTurnReasoningTwo {
		t.Fatalf("rich draft[1] markdown = %q, want reasoning payload without tg wrapper", got)
	}
}

func TestRunTurn_SendsRichMarkdownPlanUpdateDraftInDM(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("rich_markdown")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan": newBaldaPlanSnapshot(
				map[string]any{"content": "Check job_id handling", "status": "completed"},
				map[string]any{"content": "Fix Telegram retry path", "status": "in_progress"},
				map[string]any{"content": "Run actor tests", "status": "pending"},
			),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: true, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.richDrafts) != 1 {
		t.Fatalf("rich draft calls = %d, want 1", len(tgClient.richDrafts))
	}
	if got := baldaRunTurnRichDraftMarkdown(t, tgClient.richDrafts[0]); got != "# Plan update\n\n- [x] Check job_id handling\n- [ ] _In progress:_ Fix Telegram retry path\n- [ ] Run actor tests" {
		t.Fatalf("rich draft markdown = %q, want checklist payload", got)
	}
}

func TestRunTurn_SendsRichMarkdownPlanUpdateMessageInPublicChat(t *testing.T) {
	t.Parallel()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("rich_markdown")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	h := newBaldaRunTurnHandlerWithChannel(channel, nil)

	adkRunner, sessionID := newBaldaRunTurnTestRunnerWithEvents(t, func(invocationID string) []*adksession.Event {
		plan := adksession.NewEvent(context.Background(), invocationID)
		plan.Partial = true
		plan.CustomMetadata = map[string]any{
			"acp_update_kind": "plan",
			"acp_plan": newBaldaPlanSnapshot(
				map[string]any{"content": "Check job_id handling", "status": "completed"},
				map[string]any{"content": "Run actor tests", "status": "pending"},
			),
		}

		done := adksession.NewEvent(context.Background(), invocationID)
		done.Content = genai.NewContentFromText(baldaRunTurnFinalAnswerText, genai.RoleModel)
		done.TurnComplete = true

		return []*adksession.Event{plan, done}
	})
	locator := baldatelegram.NewLocator(9001, 77)
	progressPolicy := baldachannel.ProgressPolicy{Typing: true, Thinking: false, PlanUpdates: true}
	if err := h.runTurn(context.Background(), "hello", adkRunner, "tg-101", sessionID, sessionID, locator, 41, progressPolicy); err != nil {
		t.Fatalf("runTurn() error = %v", err)
	}

	if len(tgClient.richMessages) < 1 {
		t.Fatalf("rich message calls = %d, want at least 1", len(tgClient.richMessages))
	}
	if got := baldaRunTurnRichMessageMarkdown(t, tgClient.richMessages[0]); got != "# Plan update\n\n- [x] Check job_id handling\n- [ ] Run actor tests" {
		t.Fatalf("rich message markdown = %q, want checklist payload", got)
	}
}

func newBaldaRunTurnTestHandler(t *testing.T, agentReplyFormattingNone bool) (*BaldaHandler, *baldaRunTurnTelegramClient) {
	t.Helper()

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	if agentReplyFormattingNone {
		msg.SetAgentReplyFormattingMode("none")
	} else {
		msg.SetAgentReplyFormattingMode("markdownv2")
	}
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})

	return newBaldaRunTurnHandlerWithChannel(channel, nil), tgClient
}

func newBaldaRunTurnHandlerWithChannel(channel *baldatelegram.Adapter, now func() time.Time) *BaldaHandler {
	if channel == nil {
		return &BaldaHandler{logger: zerolog.Nop(), now: now}
	}
	bus := &recordingHandlerCommandBus{deliveryAdapter: channel}
	return &BaldaHandler{
		channel:         channel,
		actorDispatcher: bus,
		logger:          zerolog.Nop(),
		now:             now,
		turnExecution:   sessionturnapp.NewTurnExecutionServiceWithJobEvents(bus, nil, nil, zerolog.Nop()),
	}
}

type testJobServices struct {
	*baldajobs.JobLifecycleService
	*baldajobs.JobEventsService
	*baldajobs.DeliveryService
}

func newBaldaRunTurnTaskTestHandler(t *testing.T) (*BaldaHandler, *baldaRunTurnTelegramClient, *recordingHandlerCommandBus, *testJobServices) {
	t.Helper()

	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	bus := &recordingHandlerCommandBus{}
	lifecycle, err := baldajobs.NewJobLifecycleServiceForTests(provider.Jobs(), bus)
	if err != nil {
		t.Fatalf("NewJobLifecycleServiceForTests() error = %v", err)
	}
	eventsSvc, err := baldajobs.NewJobEventsServiceForTests(provider.Jobs(), bus)
	if err != nil {
		t.Fatalf("NewJobEventsServiceForTests() error = %v", err)
	}
	deliverySvc, err := baldajobs.NewDeliveryServiceForTests(provider.Jobs())
	if err != nil {
		t.Fatalf("NewDeliveryServiceForTests() error = %v", err)
	}
	tasks := &testJobServices{
		JobLifecycleService: lifecycle,
		JobEventsService:    eventsSvc,
		DeliveryService:     deliverySvc,
	}

	tgClient := &baldaRunTurnTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})

	return &BaldaHandler{
		channel:         channel,
		actorDispatcher: bus,
		jobEvents:       tasks,
		logger:          zerolog.Nop(),
		turnExecution:   sessionturnapp.NewTurnExecutionServiceWithJobEvents(bus, tasks, nil, zerolog.Nop()),
	}, tgClient, bus, tasks
}

func deliveryTextsFromCommands(t *testing.T, commands []actorlayer.Envelope) []string {
	t.Helper()

	texts := make([]string, 0, len(commands))
	for _, env := range commands {
		if env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		var payload actors.DeliveryPayload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		text := strings.TrimSpace(payload.Text)
		if payload.Progress != nil && strings.TrimSpace(payload.Progress.Text) != "" {
			text = strings.TrimSpace(payload.Progress.Text)
		}
		if text == "" {
			continue
		}
		texts = append(texts, text)
	}
	return texts
}

func taskEventsOfType(events []actorlayer.Envelope, types ...string) []actorlayer.Envelope {
	out := make([]actorlayer.Envelope, 0, len(events))
	for _, env := range events {
		eventType := env.Meta["event_type"]
		for _, want := range types {
			if eventType == want {
				out = append(out, env)
				break
			}
		}
	}
	return out
}

func taskEventPayload(t *testing.T, env actorlayer.Envelope) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		t.Fatalf("decode job event payload: %v", err)
	}
	return payload
}

func deliveryModeCount(t *testing.T, commands []actorlayer.Envelope, mode actors.DeliveryMode) int {
	t.Helper()

	count := 0
	for _, env := range commands {
		if env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		var payload actors.DeliveryPayload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		if payload.Mode == mode {
			count++
		}
	}
	return count
}

func newBaldaRunTurnTestRunnerWithEvents(
	t *testing.T,
	eventsFn func(invocationID string) []*adksession.Event,
) (*runner.Runner, string) {
	t.Helper()

	baldaAgent, err := adkagent.New(adkagent.Config{
		Name:        "BaldaRunTurnTestAgent",
		Description: "Emits scripted events for balda runTurn tests",
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				for _, ev := range eventsFn(ctx.InvocationID()) {
					if !yield(ev, nil) {
						return
					}
				}
			}
		},
	})
	if err != nil {
		t.Fatalf("agent.New() error = %v", err)
	}

	sessionService := adksession.InMemoryService()
	adkRunner, err := runner.New(runner.Config{
		AppName:        "balda-run-turn-test",
		Agent:          baldaAgent,
		SessionService: sessionService,
	})
	if err != nil {
		t.Fatalf("runner.New() error = %v", err)
	}

	sess, err := sessionService.Create(context.Background(), &adksession.CreateRequest{
		AppName: "balda-run-turn-test",
		UserID:  "tg-101",
	})
	if err != nil {
		t.Fatalf("session.Create() error = %v", err)
	}

	return adkRunner, sess.Session.ID()
}
