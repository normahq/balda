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
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
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

func TestCommandHandlerOnCommand_UnknownCommandIsIgnored(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("unknown", "alpha", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}
	if len(sm.createCalls) != 0 {
		t.Fatalf("CreateSession calls = %d, want 0", len(sm.createCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
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
	if cmd.To.Target != swarm.ActorTypeGoalkeeper || cmd.Namespace != swarm.NamespaceGoalkeeperCommand || cmd.Kind != swarm.KindGoal {
		t.Fatalf("published command = %+v, want goal command", cmd)
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerSubmitGoalTask_PublishesDurableCommandOnly(t *testing.T) {
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

	assertLastSentContains(t, tgClient, "Usage:")
	assertLastSentContains(t, tgClient, "/goal <objective>")
	assertLastSentContains(t, tgClient, "/goal clear")
}

func TestCommandHandlerOnCommand_GoalClearPublishesControlCommand(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "clear", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	cmd := turns.commands[0]
	if cmd.Namespace != swarm.NamespaceTaskControl || cmd.Kind != swarm.KindCancel {
		t.Fatalf("published command = %+v, want task control command", cmd)
	}
	payload := decodeControlPayload(t, cmd.PayloadJSON)
	if payload.Action != "clear_goal" {
		t.Fatalf("control payload action = %q, want clear_goal", payload.Action)
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerOnCommand_GoalClearExtraStartsGoal(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)
	bus := &recordingHandlerCommandBus{}
	handler.actorDispatcher = bus

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "clear extra", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("control commands = %d, want 0", len(turns.commands))
	}
	if len(bus.commands) != 1 {
		t.Fatalf("goal commands = %d, want 1", len(bus.commands))
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerOnCommand_GoalRejectsWhenActiveGoalExists(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.taskService = fakeGoalTaskService{
		active: []baldastate.SwarmTaskRecord{{
			ID:        "goal-1",
			SessionID: "tg-9001-0",
			Status:    baldastate.SwarmTaskStatusRunning,
		}},
	}

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "deploy release", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "A goal run is already active for this session.")
}

func TestCommandHandlerSubmitGoalTask_RejectsWhenActiveGoalExists(t *testing.T) {
	ctx := context.Background()
	locator := session.SessionLocator{
		SessionID:   "tg-9001-99",
		ChannelType: "telegram",
		AddressKey:  "tg-9001-99",
	}

	bus := &recordingHandlerCommandBus{}
	handler := &CommandHandler{
		actorDispatcher:   bus,
		goalMaxIterations: 7,
		taskService: fakeGoalTaskService{
			active: []baldastate.SwarmTaskRecord{{
				ID:        "goal-active",
				SessionID: locator.SessionID,
				Status:    baldastate.SwarmTaskStatusRunning,
			}},
		},
	}

	started, err := handler.submitGoalTask(ctx, locator, "deploy release", testTelegramUserID101)
	if err != nil {
		t.Fatalf("submitGoalTask() error = %v", err)
	}
	if started {
		t.Fatal("submitGoalTask() started = true, want false")
	}
	if len(bus.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(bus.commands))
	}
}

func TestCommandHandlerOnCommand_CancelPublishesControlCommand(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

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
	payload := decodeControlPayload(t, turns.commands[0].PayloadJSON)
	if payload.Action != "cancel_turn" {
		t.Fatalf("control payload action = %q, want cancel_turn", payload.Action)
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

func TestCommandHandlerOnCommand_UserUsageShowsUserID(t *testing.T) {
	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	inviteStore, err := auth.NewInviteStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewInviteStore(): %v", err)
	}
	collaboratorStore := auth.NewCollaboratorStore(&fakeCollaboratorBackend{})
	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	msg.SetAgentReplyFormattingMode("none")
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	handler := &CommandHandler{
		ownerStore:        ownerStore,
		collaboratorStore: collaboratorStore,
		channel:           channel,
		userHandler: &userHandler{
			ownerStore:        ownerStore,
			inviteStore:       inviteStore,
			collaboratorStore: collaboratorStore,
			channel:           channel,
			tgClient:          tgClient,
		},
	}

	if err := handler.onCommand(context.Background(), newCommandEvent("user", "", 101, 9001, nil)); err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "/user remove <user_id>")
}

type fakeCommandSessionManager struct {
	resetCalls    []resetSessionCall
	createCalls   []createSessionCall
	baldaProvider string
	metadata      session.AgentMetadata
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

type fakeGoalTaskService struct {
	active []baldastate.SwarmTaskRecord
	err    error
}

func (f fakeGoalTaskService) ListActiveGoalTasksBySession(_ context.Context, sessionID string) ([]baldastate.SwarmTaskRecord, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []baldastate.SwarmTaskRecord
	for _, task := range f.active {
		if task.SessionID == sessionID {
			out = append(out, task)
		}
	}
	return out, nil
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

type fakeTurnDispatcher struct {
	commands    []swarm.Envelope
	cancelCalls []cancelSessionCall
}

func (*fakeTurnDispatcher) Enqueue(actors.TurnTask) (int, error) {
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
	return false, 0, nil
}

func newCommandHandlerTestHarness(t *testing.T) (*CommandHandler, *fakeCommandSessionManager, *fakeTurnDispatcher, *fakeTelegramClient) {
	t.Helper()

	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	_, err = ownerStore.RegisterOwner(101, 9001)
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

func decodeControlPayload(t *testing.T, payloadJSON string) struct {
	Action string `json:"action"`
} {
	t.Helper()

	var payload struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode control payload: %v", err)
	}
	return payload
}
