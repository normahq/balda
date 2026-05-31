package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
)

const (
	testProviderAlpha     = "alpha"
	testTelegramUserID101 = "tg-101"
	testParseModeMarkdown = "MarkdownV2"
)

func TestCommandHandlerOnCommand_CloseTopicAndStopSession(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 123
	err := handler.onCommand(context.Background(), newCommandEvent("close", "", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 1 {
		t.Fatalf("CloseTopic calls = %d, want 1", len(tgClient.closedTopicIDs))
	}
	if len(sm.resetCalls) != 1 {
		t.Fatalf("ResetSession calls = %d, want 1", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].Namespace != swarm.NamespaceTaskControl || turns.commands[0].Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control cancel", turns.commands[0])
	}
	if tgClient.closedTopicIDs[0] != topicID {
		t.Fatalf("CloseTopic call = %d, want topic=%d", tgClient.closedTopicIDs[0], topicID)
	}
	if sm.resetCalls[0].SessionID != "tg-9001-123" {
		t.Fatalf("ResetSession call = %+v, want session=tg-9001-123", sm.resetCalls[0])
	}
	assertLastSentContains(t, tgClient, "Closing this topic and resetting session history.")
}

func TestCommandHandlerOnCommand_CloseRootResetsSessionHistory(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("close", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	if len(sm.resetCalls) != 1 {
		t.Fatalf("ResetSession calls = %d, want 1", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].Namespace != swarm.NamespaceTaskControl || turns.commands[0].Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control cancel", turns.commands[0])
	}
	if sm.resetCalls[0].SessionID != "tg-9001-0" {
		t.Fatalf("ResetSession call = %+v, want session=tg-9001-0", sm.resetCalls[0])
	}
	assertLastSentContains(t, tgClient, "Session history reset.")
}

func TestCommandHandlerOnCommand_CloseWithArgsShowsUsage(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 11
	err := handler.onCommand(context.Background(), newCommandEvent("close", "now", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	if len(sm.resetCalls) != 0 {
		t.Fatalf("ResetSession calls = %d, want 0", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Usage: /close")
}

func TestCommandHandlerOnCommand_CloseUnauthorized(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 33
	err := handler.onCommand(context.Background(), newCommandEvent("close", "", 999, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	if len(sm.resetCalls) != 0 {
		t.Fatalf("ResetSession calls = %d, want 0", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Only the bot owner or collaborators can use this command.")
}

func TestCommandHandlerOnCommand_CloseCollaboratorAllowed(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 33
	err := handler.onCommand(context.Background(), newCommandEvent("close", "", 202, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 1 {
		t.Fatalf("CloseTopic calls = %d, want 1", len(tgClient.closedTopicIDs))
	}
	if len(sm.resetCalls) != 1 {
		t.Fatalf("ResetSession calls = %d, want 1", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].Namespace != swarm.NamespaceTaskControl || turns.commands[0].Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control cancel", turns.commands[0])
	}
}

func TestCommandHandlerOnCommand_CloseResetFailureDoesNotCloseTopic(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)
	sm.resetErr = errors.New("reset failed")

	topicID := 44
	err := handler.onCommand(context.Background(), newCommandEvent("close", "", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.resetCalls) != 1 {
		t.Fatalf("ResetSession calls = %d, want 1", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].Namespace != swarm.NamespaceTaskControl || turns.commands[0].Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control cancel", turns.commands[0])
	}
	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	assertLastSentContains(t, tgClient, "Could not close this topic.")
}

func TestCommandHandlerOnCommand_TopicInGroupChat_Rejects(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEventWithChatType("topic", "alpha", 101, 9001, nil, "supergroup"))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.createCalls) != 0 {
		t.Fatalf("CreateSession calls = %d, want 0", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "This command is only available in direct messages.")
}

func TestCommandHandlerOnCommand_CloseInGroupChat_Rejects(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 33
	err := handler.onCommand(context.Background(), newCommandEventWithChatType("close", "", 101, 9001, &topicID, "supergroup"))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "This command is only available in direct messages.")
}

func TestCommandHandlerOnCommand_TopicWithoutArgs_ShowsUsage(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("topic", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.createCalls) != 0 {
		t.Fatalf("CreateSession calls = %d, want 0", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Usage: /topic <name>")
}

func TestCommandHandlerOnCommand_TopicCreatesTopicSession(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)
	tgClient.nextTopicID = 456

	err := handler.onCommand(context.Background(), newCommandEvent("topic", "alpha", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.createdTopics) != 1 {
		t.Fatalf("CreateTopic calls = %d, want 1", len(tgClient.createdTopics))
	}
	if tgClient.createdTopics[0].Name != "Balda: alpha" {
		t.Fatalf("CreateTopic name = %q, want %q", tgClient.createdTopics[0].Name, "Balda: alpha")
	}
	if len(sm.createCalls) != 1 {
		t.Fatalf("CreateSession calls = %d, want 1", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	if sm.createCalls[0].SessionID != "tg-9001-456" || sm.createCalls[0].UserID != testTelegramUserID101 || sm.createCalls[0].AgentName != "alpha" {
		t.Fatalf("CreateSession call = %+v, want session=tg-9001-456 user=tg-101 agent=alpha", sm.createCalls[0])
	}
	assertLastSentContains(t, tgClient, "Name")
	assertLastSentContains(t, tgClient, "alpha")
	assertLastSentContains(t, tgClient, "tg\\-9001\\-456")
}

func TestCommandHandlerOnCommand_TopicCollaboratorAllowed(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)
	tgClient.nextTopicID = 457

	err := handler.onCommand(context.Background(), newCommandEvent("topic", "ops run", 202, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.createCalls) != 1 {
		t.Fatalf("CreateSession calls = %d, want 1", len(sm.createCalls))
	}
	if sm.createCalls[0].AgentName != "ops run" {
		t.Fatalf("CreateSession agent label = %q, want %q", sm.createCalls[0].AgentName, "ops run")
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Name")
	assertLastSentContains(t, tgClient, "ops run")
}

func TestCommandHandlerOnCommand_TopicNoBaldaProvider_ShowsError(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)
	sm.baldaProvider = ""

	err := handler.onCommand(context.Background(), newCommandEvent("topic", "alpha", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}
	if len(sm.createCalls) != 0 {
		t.Fatalf("CreateSession calls = %d, want 0", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Balda is not ready right now.")
}

func TestCommandHandlerOnCommand_NewIsIgnored(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("new", "alpha", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}
	if len(sm.createCalls) != 0 {
		t.Fatalf("CreateSession calls = %d, want 0", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerOnCommand_GoalStartsRun(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	bus := &recordingHandlerCommandBus{}
	handler.actorDispatcher = bus

	topicID := 99
	err := handler.onCommand(context.Background(), newCommandEvent("goal", "deploy release", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(bus.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(bus.commands))
	}
	cmd := bus.commands[0]
	if cmd.To.Target != swarm.ActorTypeGoalkeeper || cmd.Namespace != swarm.NamespaceGoalCommand || cmd.Kind != swarm.KindGoal {
		t.Fatalf("published command = %+v, want goalkeeper command", cmd)
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerSubmitGoalTask_PublishesJetStreamCommandOnly(t *testing.T) {
	ctx := context.Background()
	locator := session.SessionLocator{
		SessionID:   "tg-9001-99",
		ChannelType: "telegram",
		AddressKey:  "tg-9001-99",
	}

	bus := &recordingHandlerCommandBus{}
	handler := &CommandHandler{actorDispatcher: bus, goalMaxIterations: 7}

	started, err := handler.submitGoalTask(ctx, locator, "deploy release", testTelegramUserID101)
	if err != nil {
		t.Fatalf("submitGoalTask() error = %v", err)
	}
	if !started {
		t.Fatal("submitGoalTask() started = false, want true")
	}
	if len(bus.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(bus.commands))
	}
	var payload struct {
		Goal *struct {
			MaxIterations int `json:"max_iterations"`
		} `json:"goal"`
	}
	if err := json.Unmarshal([]byte(bus.commands[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode goal command payload: %v", err)
	}
	if payload.Goal == nil || payload.Goal.MaxIterations != 7 {
		t.Fatalf("goal payload = %+v, want max_iterations=7 from config", payload.Goal)
	}
}

func TestCommandHandlerOnCommand_GoalWithoutArgsShowsUsage(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "Usage: /goal <objective>")
}

func TestCommandHandlerOnCommand_CronIgnored(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cron", "add * * * * * check", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerOnCommand_CancelClearsQueueAndInFlight(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)
	turns.cancelHadInFlight = true
	turns.cancelDropped = 2

	topicID := 88
	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].Namespace != swarm.NamespaceTaskControl || turns.commands[0].Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control cancel", turns.commands[0])
	}
	assertLastSentContains(t, tgClient, "Cancel requested.")
}

func TestCommandHandlerOnCommand_CancelNoActiveTurns(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	assertLastSentContains(t, tgClient, "Cancel requested.")
}

func TestCommandHandlerOnCommand_CancelWithArgsShowsUsage(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "now", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Usage: /cancel")
}

func TestCommandHandlerOnCommand_CancelUnauthorized(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 999, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Only the bot owner or collaborators can use this command.")
}

func TestCommandHandlerOnCommand_CancelCollaboratorAllowed(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 202, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turns.cancelCalls))
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	assertLastSentContains(t, tgClient, "Cancel requested.")
}

func TestCommandHandlerOnCommand_RemovedCommandsAreSilent(t *testing.T) {
	removed := []struct {
		name string
		args string
	}{
		{name: "reset"},
		{name: "memory"},
		{name: "tasks"},
		{name: "task", args: "task-1"},
		{name: "swarm", args: "status"},
		{name: "queue", args: "status"},
		{name: "mailbox", args: "status"},
		{name: "projection", args: "status"},
		{name: "actors", args: "status"},
		{name: "dlq"},
	}

	for _, tt := range removed {
		t.Run(tt.name, func(t *testing.T) {
			handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

			if err := handler.onCommand(context.Background(), newCommandEvent(tt.name, tt.args, 101, 9001, nil)); err != nil {
				t.Fatalf("onCommand(%s) error = %v", tt.name, err)
			}

			if len(tgClient.messages) != 0 {
				t.Fatalf("messages = %d, want 0 for removed command %q", len(tgClient.messages), tt.name)
			}
			if len(sm.createCalls) != 0 {
				t.Fatalf("create calls = %d, want 0 for removed command %q", len(sm.createCalls), tt.name)
			}
			if len(sm.resetCalls) != 0 {
				t.Fatalf("reset calls = %d, want 0 for removed command %q", len(sm.resetCalls), tt.name)
			}
			if len(turns.commands) != 0 {
				t.Fatalf("published commands = %d, want 0 for removed command %q", len(turns.commands), tt.name)
			}
			if len(turns.cancelCalls) != 0 {
				t.Fatalf("cancel calls = %d, want 0 for removed command %q", len(turns.cancelCalls), tt.name)
			}
		})
	}
}

type fakeCommandSessionManager struct {
	resetCalls    []resetSessionCall
	createCalls   []createSessionCall
	baldaProvider string
	metadata      session.AgentMetadata
	sessionInfos  map[string]session.TopicSessionInfo
	resetErr      error
}

type createSessionCall struct {
	SessionID string
	UserID    string
	AgentName string
}

type resetSessionCall struct {
	SessionID string
}

type cancelSessionCall struct {
	SessionID   string
	ClearQueued bool
}

func (f *fakeCommandSessionManager) CreateSession(_ context.Context, sessionCtx session.SessionContext, agentName string) error {
	f.createCalls = append(f.createCalls, createSessionCall{
		SessionID: sessionCtx.Locator.SessionID,
		UserID:    sessionCtx.UserID,
		AgentName: agentName,
	})
	return nil
}

func (f *fakeCommandSessionManager) GetAgentMetadata(string) session.AgentMetadata {
	return f.metadata
}

func (f *fakeCommandSessionManager) BaldaProviderID() string {
	return f.baldaProvider
}

func (f *fakeCommandSessionManager) ResetSession(_ context.Context, locator session.SessionLocator) error {
	f.resetCalls = append(f.resetCalls, resetSessionCall{SessionID: locator.SessionID})
	return f.resetErr
}

func (f *fakeCommandSessionManager) GetSessionInfo(_ context.Context, sessionID string) (session.TopicSessionInfo, error) {
	if f.sessionInfos != nil {
		if info, ok := f.sessionInfos[sessionID]; ok {
			return info, nil
		}
	}
	return session.TopicSessionInfo{SessionID: sessionID}, nil
}

type fakeTurnDispatcher struct {
	commands          []swarm.Envelope
	cancelCalls       []cancelSessionCall
	enqueueCalls      []actors.TurnTask
	cancelHadInFlight bool
	cancelDropped     int
	cancelErr         error
}

func (f *fakeTurnDispatcher) Enqueue(task actors.TurnTask) (int, error) {
	f.enqueueCalls = append(f.enqueueCalls, task)
	return 0, nil
}

func (f *fakeTurnDispatcher) Dispatch(_ context.Context, env swarm.Envelope) (*swarm.DispatchReceipt, error) {
	f.commands = append(f.commands, env)
	return &swarm.DispatchReceipt{
		Stream:   swarm.DefaultCommandStream,
		Sequence: uint64(len(f.commands)),
		Subject:  swarm.SubjectForEnvelope(env),
		MsgID:    swarm.DedupeKeyOrID(env),
	}, nil
}

func (*fakeTurnDispatcher) PublishEvent(context.Context, string, swarm.Envelope) error { return nil }

func (f *fakeTurnDispatcher) CancelSession(locator session.SessionLocator, clearQueued bool) (bool, int, error) {
	f.cancelCalls = append(f.cancelCalls, cancelSessionCall{
		SessionID:   locator.SessionID,
		ClearQueued: clearQueued,
	})
	if f.cancelErr != nil {
		return false, 0, f.cancelErr
	}
	return f.cancelHadInFlight, f.cancelDropped, nil
}

func newCommandHandlerTestHarness(t *testing.T) (*CommandHandler, *fakeCommandSessionManager, *fakeTurnDispatcher, *fakeTelegramClient) {
	t.Helper()

	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	_, err = ownerStore.RegisterOwner(101, 9001, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	collaboratorStore := auth.NewCollaboratorStore(&fakeCollaboratorBackend{
		entries: map[string]auth.Collaborator{
			"202": {UserID: "202"},
		},
	})

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	sessionManager := &fakeCommandSessionManager{}
	turnDispatcher := &fakeTurnDispatcher{}
	sessionManager.baldaProvider = testProviderAlpha
	sessionManager.metadata = session.AgentMetadata{
		Type:       "opencode_acp",
		Model:      "gpt-5",
		MCPServers: []string{"provider_mcp"},
	}
	handler := &CommandHandler{
		ownerStore:        ownerStore,
		collaboratorStore: collaboratorStore,
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		sessionManager:    sessionManager,
		actorDispatcher:   turnDispatcher,
		goalMaxIterations: normalizeGoalMaxIterations(0),
	}
	return handler, sessionManager, turnDispatcher, tgClient
}

type recordingHandlerCommandBus struct {
	commands      []swarm.Envelope
	commandErrs   []error
	eventSubjects []string
	eventEnvs     []swarm.Envelope
	eventErrs     []error
}

func (b *recordingHandlerCommandBus) Dispatch(_ context.Context, env swarm.Envelope) (*swarm.DispatchReceipt, error) {
	if len(b.commandErrs) > 0 {
		err := b.commandErrs[0]
		b.commandErrs = b.commandErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	b.commands = append(b.commands, env)
	return &swarm.DispatchReceipt{Stream: swarm.DefaultCommandStream, Sequence: uint64(len(b.commands)), Subject: swarm.SubjectForEnvelope(env), MsgID: swarm.DedupeKeyOrID(env)}, nil
}

func (b *recordingHandlerCommandBus) PublishEvent(_ context.Context, subject string, env swarm.Envelope) error {
	b.eventSubjects = append(b.eventSubjects, subject)
	b.eventEnvs = append(b.eventEnvs, env)
	if len(b.eventErrs) > 0 {
		err := b.eventErrs[0]
		b.eventErrs = b.eventErrs[1:]
		return err
	}
	return nil
}

type fakeCollaboratorBackend struct {
	entries map[string]auth.Collaborator
}

func (f *fakeCollaboratorBackend) AddCollaborator(_ context.Context, c auth.Collaborator) error {
	if f.entries == nil {
		f.entries = make(map[string]auth.Collaborator)
	}
	f.entries[c.UserID] = c
	return nil
}

func (f *fakeCollaboratorBackend) RemoveCollaborator(_ context.Context, userID string) error {
	delete(f.entries, userID)
	return nil
}

func (f *fakeCollaboratorBackend) GetCollaborator(_ context.Context, userID string) (*auth.Collaborator, bool, error) {
	entry, ok := f.entries[userID]
	if !ok {
		return nil, false, nil
	}
	c := entry
	return &c, true, nil
}

func (f *fakeCollaboratorBackend) ListCollaborators(context.Context) ([]auth.Collaborator, error) {
	out := make([]auth.Collaborator, 0, len(f.entries))
	for _, entry := range f.entries {
		out = append(out, entry)
	}
	return out, nil
}

func newCommandEvent(command, args string, userID, chatID int64, topicID *int) *events.CommandEvent {
	return newCommandEventWithChatType(command, args, userID, chatID, topicID, "private")
}

func newCommandEventWithChatType(command, args string, userID, chatID int64, topicID *int, chatType string) *events.CommandEvent {
	text := "/" + command
	if trimmedArgs := strings.TrimSpace(args); trimmedArgs != "" {
		text += " " + trimmedArgs
	}
	msg := &client.Message{
		Chat: client.Chat{
			Id:   chatID,
			Type: chatType,
		},
		From: &client.User{
			Id:        userID,
			FirstName: "Test",
		},
		Text: &text,
	}
	if topicID != nil {
		msg.MessageThreadId = topicID
	}
	return &events.CommandEvent{
		Command: command,
		Args:    args,
		Message: msg,
	}
}
