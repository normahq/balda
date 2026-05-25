package handlers

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
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
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if tgClient.closedTopicIDs[0] != topicID {
		t.Fatalf("CloseTopic call = %d, want topic=%d", tgClient.closedTopicIDs[0], topicID)
	}
	if sm.resetCalls[0].SessionID != "tg-9001-123" {
		t.Fatalf("ResetSession call = %+v, want session=tg-9001-123", sm.resetCalls[0])
	}
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if sm.resetCalls[0].SessionID != "tg-9001-0" {
		t.Fatalf("ResetSession call = %+v, want session=tg-9001-0", sm.resetCalls[0])
	}
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	assertLastSentContains(t, tgClient, "Failed to reset this session before close: reset failed")
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
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 33
	err := handler.onCommand(context.Background(), newCommandEventWithChatType("close", "", 101, 9001, &topicID, "supergroup"))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.closedTopicIDs) != 0 {
		t.Fatalf("CloseTopic calls = %d, want 0", len(tgClient.closedTopicIDs))
	}
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
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
	assertLastSentContains(t, tgClient, "balda.provider is not configured.")
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
	goal := handler.goalRunner.(*fakeGoalRunner)

	topicID := 99
	err := handler.onCommand(context.Background(), newCommandEvent("goal", "deploy release", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(goal.startCalls) != 1 {
		t.Fatalf("GoalRunner Start calls = %d, want 1", len(goal.startCalls))
	}
	call := goal.startCalls[0]
	if call.SessionID != "tg-9001-99" || call.Objective != "deploy release" || call.TransportUserID != testTelegramUserID101 {
		t.Fatalf("GoalRunner Start call = %+v, want session=tg-9001-99 objective='deploy release' user=tg-101", call)
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("sent messages = %d, want 0", len(tgClient.messages))
	}
}

func TestCommandHandlerSubmitGoalTask_CreatesTaskRecordInEveryMode(t *testing.T) {
	ctx := context.Background()
	locator := session.SessionLocator{
		SessionID:   "tg-9001-99",
		ChannelType: "telegram",
		AddressKey:  "tg-9001-99",
	}

	tests := []struct {
		name             string
		cfg              swarm.Config
		expectGoalRunner bool
		expectMailbox    bool
	}{
		{
			name:             "legacy",
			cfg:              swarm.Config{Enabled: true, Mode: swarm.ModeLegacy},
			expectGoalRunner: true,
		},
		{
			name:             "shadow",
			cfg:              swarm.Config{Enabled: true, Mode: swarm.ModeShadow, Shadow: swarm.ShadowConfig{Enabled: true}},
			expectGoalRunner: true,
		},
		{
			name:          "mailbox",
			cfg:           swarm.Config{Enabled: true, Mode: swarm.ModeMailbox},
			expectMailbox: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, coordinator, tasks := newCommandHandlerSwarmServices(t, ctx, tc.cfg)
			goal := &fakeGoalRunner{startResult: true, maxIters: 7}
			handler := &CommandHandler{
				swarmCoordinator: coordinator,
				tasks:            tasks,
				goalRunner:       goal,
			}

			started, err := handler.submitGoalTask(ctx, locator, "deploy release", testTelegramUserID101)
			if err != nil {
				t.Fatalf("submitGoalTask() error = %v", err)
			}
			if !started {
				t.Fatal("submitGoalTask() started = false, want true")
			}
			if got := len(goal.startCalls); got != boolToInt(tc.expectGoalRunner) {
				t.Fatalf("GoalRunner StartTask calls = %d, want %d", got, boolToInt(tc.expectGoalRunner))
			}

			active, err := provider.Swarm().ListActiveTasksBySession(ctx, locator.SessionID)
			if err != nil {
				t.Fatalf("ListActiveTasksBySession() error = %v", err)
			}
			if len(active) != 1 {
				t.Fatalf("active tasks = %+v, want one", active)
			}
			task := active[0]
			if task.Objective != "deploy release" || task.Status != baldastate.SwarmTaskStatusQueued || task.CreatedFrom != "goal" {
				t.Fatalf("task = %+v, want queued goal task", task)
			}

			claimed, err := provider.Swarm().Claim(ctx, "task:"+task.ID, "test-worker", 1, time.Minute)
			if err != nil {
				t.Fatalf("Claim(task mailbox) error = %v", err)
			}
			if tc.expectMailbox && len(claimed) != 1 {
				t.Fatalf("claimed mailbox messages = %+v, want one", claimed)
			}
			if !tc.expectMailbox && len(claimed) != 0 {
				t.Fatalf("claimed mailbox messages = %+v, want none", claimed)
			}
		})
	}
}

func TestCommandHandlerOnCommand_GoalRejectsConcurrentRun(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	goal := handler.goalRunner.(*fakeGoalRunner)
	goal.startResult = false

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "deploy release", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "A goal run is already active for this session.")
}

func TestCommandHandlerOnCommand_GoalRejectsConcurrentRun_UsesAgentReplyFormatting(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.messenger.SetAgentReplyFormattingMode("markdownv2")
	goal := handler.goalRunner.(*fakeGoalRunner)
	goal.startResult = false

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "deploy release", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(tgClient.messages) == 0 {
		t.Fatal("sent messages = 0, want concurrent-run message")
	}
	last := tgClient.messages[len(tgClient.messages)-1]
	if last.ParseMode == nil || *last.ParseMode != testParseModeMarkdown {
		t.Fatalf("parse_mode = %v, want MarkdownV2", last.ParseMode)
	}
	assertLastSentContains(t, tgClient, "A goal run is already active for this session")
}

func TestCommandHandlerOnCommand_GoalWithoutArgsShowsUsage(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	goal := handler.goalRunner.(*fakeGoalRunner)

	err := handler.onCommand(context.Background(), newCommandEvent("goal", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(goal.startCalls) != 0 {
		t.Fatalf("GoalRunner Start calls = %d, want 0", len(goal.startCalls))
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

	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if turns.cancelCalls[0].SessionID != "tg-9001-88" {
		t.Fatalf("CancelSession call = %+v, want session=tg-9001-88", turns.cancelCalls[0])
	}
	assertLastSentContains(t, tgClient, "Canceled current turn.")
	assertLastSentContains(t, tgClient, "Dropped 2 queued message(s).")
}

func TestCommandHandlerOnCommand_CancelNoActiveTurns(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "No running or queued turns for this session.")
}

func TestCommandHandlerOnCommand_CancelCancelsGoalRun(t *testing.T) {
	handler, _, turns, tgClient := newCommandHandlerTestHarness(t)
	goal := handler.goalRunner.(*fakeGoalRunner)
	goal.cancelResult = true

	err := handler.onCommand(context.Background(), newCommandEvent("cancel", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Canceled active goal run.")
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

	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "No running or queued turns for this session.")
}

func TestCommandHandlerOnCommand_ResetClearsSessionHistory(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	topicID := 88
	err := handler.onCommand(context.Background(), newCommandEvent("reset", "", 101, 9001, &topicID))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.resetCalls) != 1 {
		t.Fatalf("ResetSession calls = %d, want 1", len(sm.resetCalls))
	}
	if sm.resetCalls[0].SessionID != "tg-9001-88" {
		t.Fatalf("ResetSession call = %+v, want session=tg-9001-88", sm.resetCalls[0])
	}
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if len(sm.stopCalls) != 0 {
		t.Fatalf("StopSession calls = %d, want 0", len(sm.stopCalls))
	}
	assertLastSentContains(t, tgClient, "Session history reset.")
}

func TestCommandHandlerOnCommand_ResetWithArgsShowsUsage(t *testing.T) {
	handler, sm, turns, tgClient := newCommandHandlerTestHarness(t)

	err := handler.onCommand(context.Background(), newCommandEvent("reset", "now", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	if len(sm.resetCalls) != 0 {
		t.Fatalf("ResetSession calls = %d, want 0", len(sm.resetCalls))
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	assertLastSentContains(t, tgClient, "Usage: /reset")
}

func TestCommandHandlerOnCommand_MemoryReadsCurrentMemory(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.memoryStore = memory.NewStore(t.TempDir(), true)
	if err := handler.memoryStore.Remember(context.Background(), "project uses Balda memory"); err != nil {
		t.Fatalf("Remember() error = %v", err)
	}

	err := handler.onCommand(context.Background(), newCommandEvent("memory", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "project uses Balda memory")
}

func TestCommandHandlerOnCommand_MemoryRequiresDM(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.memoryStore = memory.NewStore(t.TempDir(), true)
	topicID := 10

	err := handler.onCommand(context.Background(), newCommandEventWithChatType("memory", "", 101, 9001, &topicID, "supergroup"))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "This command is only available in direct messages.")
}

func TestCommandHandlerOnCommand_MemoryDisabled(t *testing.T) {
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.memoryStore = memory.NewStore(t.TempDir(), false)

	err := handler.onCommand(context.Background(), newCommandEvent("memory", "", 101, 9001, nil))
	if err != nil {
		t.Fatalf("onCommand() error = %v", err)
	}

	assertLastSentContains(t, tgClient, "Memory is disabled.")
}

type fakeCommandSessionManager struct {
	stopCalls     []stopSessionCall
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

type stopSessionCall struct {
	SessionID string
}

type resetSessionCall struct {
	SessionID string
}

type cancelSessionCall struct {
	SessionID   string
	ClearQueued bool
}

type goalStartCall struct {
	SessionID       string
	Objective       string
	TransportUserID string
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

func (f *fakeCommandSessionManager) StopSession(locator session.SessionLocator) {
	f.stopCalls = append(f.stopCalls, stopSessionCall{SessionID: locator.SessionID})
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
	cancelCalls       []cancelSessionCall
	enqueueCalls      []TurnTask
	cancelHadInFlight bool
	cancelDropped     int
	cancelErr         error
}

type fakeGoalRunner struct {
	startCalls   []goalStartCall
	startResult  bool
	startErr     error
	cancelCalls  []string
	cancelResult bool
	maxIters     int
}

func (f *fakeTurnDispatcher) Enqueue(task TurnTask) (int, error) {
	f.enqueueCalls = append(f.enqueueCalls, task)
	return 0, nil
}

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

func (f *fakeGoalRunner) Start(
	_ context.Context,
	locator session.SessionLocator,
	objective string,
	transportUserID string,
) (bool, error) {
	return f.StartTask(context.Background(), "", locator, objective, transportUserID)
}

func (f *fakeGoalRunner) StartTask(
	_ context.Context,
	_ string,
	locator session.SessionLocator,
	objective string,
	transportUserID string,
) (bool, error) {
	f.startCalls = append(f.startCalls, goalStartCall{
		SessionID:       locator.SessionID,
		Objective:       objective,
		TransportUserID: transportUserID,
	})
	if f.startErr != nil {
		return false, f.startErr
	}
	return f.startResult, nil
}

func (f *fakeGoalRunner) MaxIterations() int {
	if f.maxIters <= 0 {
		return defaultGoalMaxIterations
	}
	return f.maxIters
}

func (f *fakeGoalRunner) Cancel(locator session.SessionLocator) bool {
	f.cancelCalls = append(f.cancelCalls, locator.SessionID)
	return f.cancelResult
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
	goalRunner := &fakeGoalRunner{startResult: true}
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
		sessionManager: sessionManager,
		turnDispatcher: turnDispatcher,
		goalRunner:     goalRunner,
		messenger:      msg,
		memoryStore:    memory.NewStore(t.TempDir(), true),
	}
	return handler, sessionManager, turnDispatcher, tgClient
}

func newCommandHandlerSwarmServices(
	t *testing.T,
	ctx context.Context,
	cfg swarm.Config,
) (baldastate.Provider, *swarm.Coordinator, *swarm.TaskService) {
	t.Helper()

	normalized, err := cfg.Normalized()
	if err != nil {
		t.Fatalf("Normalize swarm config: %v", err)
	}
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("provider.Close() error = %v", err)
		}
	})

	var coordinator *swarm.Coordinator
	var tasks *swarm.TaskService
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			fx.Annotate(handlerShadowWakeBus{}, fx.As(new(swarm.EventBus))),
			normalized,
		),
		fx.Provide(
			swarm.NewShadowMetrics,
			swarm.NewMailboxService,
			swarm.NewTaskService,
			swarm.NewCoordinator,
		),
		fx.Populate(&coordinator, &tasks),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, coordinator, tasks
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
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
