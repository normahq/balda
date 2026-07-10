package actors

import (
	"context"
	"encoding/json"
	"iter"
	"slices"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/progress"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
	adkagent "google.golang.org/adk/v2/agent"
	adkrunner "google.golang.org/adk/v2/runner"
	adksession "google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

func TestGoalKeeperActorRejectsMismatchedEnvelopeAndPayloadJobID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, dispatcher, tasks := newTaskActorDispatchServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: &fakeGoalRunPreparer{t: t, finalValidatorText: "verdict: pass"},
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   1,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "ship release", "101", 1)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	env.Meta = baldaexecution.WithJobIDMeta(nil, "other-job")

	err = actor.Handle(ctx, env)
	if err == nil {
		t.Fatal("Handle() error = nil, want policy error")
	}
	if got, want := actorlayer.ClassifyError(err), actorlayer.ErrorKindPolicy; got != want {
		t.Fatalf("Handle() error kind = %q, want %q (err=%v)", got, want, err)
	}
}

func TestGoalKeeperActorCompletesPassingRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{t: t, finalValidatorText: "verdict: pass\nvalidated"}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	profile := deliveryfmt.Profile{Format: deliveryfmt.FormatAuto, TelegramMode: "rich_markdown"}
	env, err := goalkeeper.GoalJobEnvelopeWithOptions(locator, deliveryfmt.Options{Profile: profile}, "ship release", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusCompleted)
	}
	if !strings.Contains(task.ResultJSON, `"goal_reached":true`) {
		t.Fatalf("job result = %s, want goal_reached true", task.ResultJSON)
	}
	if runtimeBuilder.exportedMessage == "" {
		t.Fatal("exportedMessage = empty, want generated commit message")
	}
	if runtimeBuilder.cleanupCalls != 1 {
		t.Fatalf("cleanupCalls = %d, want 1", runtimeBuilder.cleanupCalls)
	}
	if got := lastPublishedCommandTo(t, bus, baldaexecution.ActorTypeDelivery, locator.DeliveryActorKey()); got.Kind != jobPayloadKindDelivery {
		t.Fatalf("last delivery = %+v, want delivery command", got)
	}
	payloads := deliveryPayloadsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	if len(payloads) == 0 {
		t.Fatalf("delivery payloads = %v, want at least one", payloads)
	}
	if !strings.Contains(payloads[0].Text, "**Objective:** ship release") {
		t.Fatalf("start delivery text = %q, want objective label", payloads[0].Text)
	}
	for _, payload := range payloads {
		if payload.Mode != DeliveryModeAgentReply {
			t.Fatalf("delivery payload mode = %q, want %q", payload.Mode, DeliveryModeAgentReply)
		}
		if payload.Profile.Format != profile.Format || payload.Profile.TelegramMode != profile.TelegramMode {
			t.Fatalf("delivery profile = %+v, want %+v", payload.Profile, profile)
		}
	}
}

func TestGoalKeeperActorCompletesPassingRunWithoutWorkspaceExport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t:                  t,
		finalValidatorText: "verdict: pass\nvalidated",
		exportStatus:       "not_exported",
		exportReason:       "workspace_disabled",
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "ship release", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusCompleted)
	}
	for _, want := range []string{`"status":"not_exported"`, `"reason":"workspace_disabled"`} {
		if !strings.Contains(task.ResultJSON, want) {
			t.Fatalf("job result = %s, want %s", task.ResultJSON, want)
		}
	}
	if runtimeBuilder.exportedMessage != "" {
		t.Fatalf("exportedMessage = %q, want empty when export is skipped", runtimeBuilder.exportedMessage)
	}
	texts := deliveryTextsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	if got := countContains(texts, "Export: skipped (workspace mode disabled)."); got != 0 {
		t.Fatalf("skipped export deliveries = %d, want 0\n%v", got, texts)
	}
}

func TestGoalKeeperActorUsesLatestValidatorVerdictForCompletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t:      t,
		events: goalEventsAfterInitialFailure("verdict: pass\nEvidence: final pass"),
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "count lines", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusCompleted)
	}
	if runtimeBuilder.finalizeWorkerOutput != "final worker result" {
		t.Fatalf("Finalize worker output = %q, want latest worker result", runtimeBuilder.finalizeWorkerOutput)
	}
	if runtimeBuilder.finalizeValidatorOutput != "verdict: pass\nEvidence: final pass" {
		t.Fatalf("Finalize validator output = %q, want latest validator result", runtimeBuilder.finalizeValidatorOutput)
	}

	var result struct {
		GoalReached       bool   `json:"goal_reached"`
		ReviewerOutput    string `json:"reviewer_output"`
		ReviewableOutcome struct {
			WhatWasDone string `json:"what_was_done"`
			Validation  string `json:"validation_output"`
		} `json:"reviewable_outcome"`
	}
	if err := json.Unmarshal([]byte(task.ResultJSON), &result); err != nil {
		t.Fatalf("decode job result: %v\n%s", err, task.ResultJSON)
	}
	if !result.GoalReached {
		t.Fatalf("goal_reached = false, want true\n%s", task.ResultJSON)
	}
	if !strings.Contains(result.ReviewerOutput, "first failure") || !strings.Contains(result.ReviewerOutput, "final pass") {
		t.Fatalf("reviewer_output = %q, want full validator transcript", result.ReviewerOutput)
	}
	if result.ReviewableOutcome.WhatWasDone != "final worker result" {
		t.Fatalf("what_was_done = %q, want latest worker result", result.ReviewableOutcome.WhatWasDone)
	}
	if strings.Contains(result.ReviewableOutcome.Validation, "first failure") {
		t.Fatalf("reviewable validation = %q, want latest validator output only", result.ReviewableOutcome.Validation)
	}
	if !strings.Contains(result.ReviewableOutcome.Validation, "final pass") {
		t.Fatalf("reviewable validation = %q, want latest validator pass evidence", result.ReviewableOutcome.Validation)
	}
	finalPayloads := deliveryPayloadsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	finalText := finalPayloads[len(finalPayloads)-1].Text
	if !strings.Contains(finalText, "final worker result") {
		t.Fatalf("final delivery = %q, want latest worker result", finalText)
	}
	if strings.Contains(finalText, "first failure") {
		t.Fatalf("final delivery = %q, want no stale validator failure", finalText)
	}
}

func TestGoalKeeperActorFinalFailureUsesLatestValidatorOutput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t:      t,
		events: goalEventsAfterInitialFailure("verdict: fail\nEvidence: final failure"),
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   2,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "count lines", "101", 2)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusFailed {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusFailed)
	}

	var result struct {
		GoalReached       bool   `json:"goal_reached"`
		ReviewerOutput    string `json:"reviewer_output"`
		ReviewableOutcome struct {
			Validation string `json:"validation_output"`
		} `json:"reviewable_outcome"`
	}
	if err := json.Unmarshal([]byte(task.ResultJSON), &result); err != nil {
		t.Fatalf("decode job result: %v\n%s", err, task.ResultJSON)
	}
	if result.GoalReached {
		t.Fatalf("goal_reached = true, want false\n%s", task.ResultJSON)
	}
	if !strings.Contains(result.ReviewerOutput, "first failure") || !strings.Contains(result.ReviewerOutput, "final failure") {
		t.Fatalf("reviewer_output = %q, want full validator transcript", result.ReviewerOutput)
	}
	if strings.Contains(result.ReviewableOutcome.Validation, "first failure") {
		t.Fatalf("reviewable validation = %q, want latest validator output only", result.ReviewableOutcome.Validation)
	}
	if !strings.Contains(result.ReviewableOutcome.Validation, "final failure") {
		t.Fatalf("reviewable validation = %q, want latest validator failure evidence", result.ReviewableOutcome.Validation)
	}
	finalPayloads := deliveryPayloadsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	finalText := finalPayloads[len(finalPayloads)-1].Text
	if !strings.Contains(finalText, "final failure") {
		t.Fatalf("final delivery = %q, want latest validator failure", finalText)
	}
	if strings.Contains(finalText, "first failure") {
		t.Fatalf("final delivery = %q, want no stale validator failure", finalText)
	}
}

func TestGoalKeeperActorRejectsSecondActiveGoalInSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	if _, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:            "goal-existing",
		SessionID:     locator.SessionID,
		Objective:     "existing goal",
		Status:        baldastate.JobStatusRunning,
		OwnerActor:    baldaexecution.ActorTypeGoalkeeper + ":goal-existing",
		AssignedActor: baldaexecution.ActorTypeGoalkeeper + ":goal-existing",
	}, "test", nil); err != nil {
		t.Fatalf("Create existing goal job: %v", err)
	}
	runtimeBuilder := &fakeGoalRunPreparer{t: t}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusCanceled)
	}
	if runtimeBuilder.buildCalls != 0 {
		t.Fatalf("buildCalls = %d, want 0", runtimeBuilder.buildCalls)
	}
	texts := deliveryTextsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	if got := countMatches(texts, "A goal run is already active for this session."); got != 1 {
		t.Fatalf("already-active deliveries = %d, want 1\n%v", got, texts)
	}
}

func TestGoalKeeperActorDeliversWorkerProgressAndDedupesRepeatedOutput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t: t,
		events: []goalTestEvent{
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "worker completed"},
			{kind: "text", text: "worker completed"},
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "verdict: pass\nvalidated"},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
		},
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	texts := deliveryTextsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	joined := strings.Join(texts, "\n---\n")
	for _, want := range []string{
		"Goal iteration 1/3: worker started.",
		"Goal iteration 1/3: worker update.\n\nworker completed",
		"Goal iteration 1/3: worker completed.",
		"Goal iteration 1/3: validator started.",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("delivery texts missing %q\n%s", want, joined)
		}
	}
	if got := countMatches(texts, "Goal iteration 1/3: worker update.\n\nworker completed"); got != 1 {
		t.Fatalf("worker update deliveries = %d, want 1\n%v", got, texts)
	}
	var progressKinds []string
	for _, event := range bus.eventEnvs {
		if event.Meta["event_type"] != baldajobs.JobEventAgentProgress || baldaexecution.EnvelopeJobID(event) != baldaexecution.EnvelopeJobID(env) {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("progress payload decode: %v", err)
		}
		progressKinds = append(progressKinds, strings.TrimSpace(payload["kind"].(string)))
	}
	for _, want := range []string{"output", "completed"} {
		if !slices.Contains(progressKinds, want) {
			t.Fatalf("progress kinds = %v, want %q", progressKinds, want)
		}
	}
}

func TestGoalKeeperActorDeliversPlanUpdatesWhenEnabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorRuntimeServices(t, ctx)
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t: t,
		events: []goalTestEvent{
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
			{kind: "plan", planEntries: []map[string]any{{"content": "Run tests", "status": "in_progress"}}},
			{kind: "plan", planEntries: []map[string]any{{"content": "Run tests", "status": "in_progress"}}},
			{kind: "text", text: "worker completed"},
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "verdict: pass\nvalidated"},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
		},
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	payloads := deliveryPayloadsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
	planPayloads := 0
	for _, payload := range payloads {
		if payload.Mode != DeliveryModeProgress || payload.Progress == nil || payload.Progress.Kind != DeliveryProgressPlanUpdate {
			continue
		}
		planPayloads++
		if got := payload.Progress.Text; got != "Plan update\n- [in progress] Run tests" {
			t.Fatalf("plan progress text = %q, want plain plan text", got)
		}
		if payload.Progress.Plan == nil || len(payload.Progress.Plan.Entries) != 1 {
			t.Fatalf("plan snapshot = %+v, want 1 entry", payload.Progress.Plan)
		}
		entry := payload.Progress.Plan.Entries[0]
		if entry.Content != "Run tests" || entry.Status != "in progress" {
			t.Fatalf("plan entry = %+v, want normalized content/status", entry)
		}
	}
	if planPayloads != 1 {
		texts := deliveryTextsForTask(t, bus, baldaexecution.EnvelopeJobID(env))
		t.Fatalf("plan update deliveries = %d, want 1\n%v", planPayloads, texts)
	}
}

type fakeGoalRunPreparer struct {
	t                       *testing.T
	finalValidatorText      string
	commitMessage           string
	exportErr               error
	exportStatus            string
	exportReason            string
	cleanupCalls            int
	buildCalls              int
	exportedMessage         string
	finalizeWorkerOutput    string
	finalizeValidatorOutput string
	events                  []goalTestEvent
}

func (b *fakeGoalRunPreparer) PrepareGoalRun(ctx context.Context, cfg goalkeeper.GoalRunConfig) (goalkeeper.GoalRun, error) {
	b.t.Helper()
	b.buildCalls++
	if cfg.UserID == "" || cfg.SourceSessionID == "" || cfg.JobID == "" {
		b.t.Fatalf("PrepareGoalRun() cfg = %+v, want user/source session/task", cfg)
	}
	workspaceDir := b.t.TempDir()
	svc := adksession.InMemoryService()
	if _, err := svc.Create(ctx, &adksession.CreateRequest{
		AppName:   "goal-test",
		UserID:    cfg.UserID,
		SessionID: cfg.JobID,
	}); err != nil {
		return nil, err
	}
	ag, err := adkagent.New(adkagent.Config{
		Name:        "Goal",
		Description: "test goal",
		Run: func(inv adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				events := b.events
				if len(events) == 0 {
					events = []goalTestEvent{
						{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
						{kind: "text", text: "worker completed"},
						{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
						{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
						{kind: "text", text: b.finalValidatorText},
						{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
					}
				}
				for _, event := range events {
					ev := event.build(inv.InvocationID(), b.finalValidatorText)
					if !yield(ev, nil) {
						return
					}
				}
			}
		},
	})
	if err != nil {
		return nil, err
	}
	r, err := adkrunner.New(adkrunner.Config{AppName: "goal-test", Agent: ag, SessionService: svc})
	if err != nil {
		return nil, err
	}
	return fakeGoalRun{
		runner:       r,
		sessionID:    cfg.JobID,
		workspaceDir: workspaceDir,
		branchName:   "norma/balda/goal/" + cfg.JobID,
		finalizeFn: func(_ context.Context, _ string, workerOutput string, validatorOutput string) (goalkeeper.GoalFinalizationResult, error) {
			b.finalizeWorkerOutput = workerOutput
			b.finalizeValidatorOutput = validatorOutput
			commitMessage := strings.TrimSpace(b.commitMessage)
			if commitMessage == "" {
				commitMessage = "chore(goal): complete goal"
			}
			status := strings.TrimSpace(b.exportStatus)
			if status == "" {
				status = "exported"
			}
			if status == "exported" {
				b.exportedMessage = commitMessage
			}
			result := goalkeeper.GoalFinalizationResult{
				Status:        status,
				CommitMessage: commitMessage,
				Reason:        b.exportReason,
			}
			if b.exportErr != nil {
				result.Status = "export_failed"
				result.Error = b.exportErr.Error()
			}
			return result, b.exportErr
		},
		cleanupResourcesFn: func(context.Context) error {
			b.cleanupCalls++
			return nil
		},
	}, nil
}

func TestGoalKeeperActorPreservesWorkspaceOnExportFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = allocator
	locator := session.SessionLocator{ChannelType: "telegram", SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRunPreparer{
		t:                  t,
		finalValidatorText: "verdict: pass\nvalidated",
		exportErr:          context.DeadlineExceeded,
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		JobService:      tasks,
		Dispatcher:      dispatcher,
		SessionManager:  manager,
		GoalRunPreparer: runtimeBuilder,
		JobRuns:         NewJobRunRegistry(),
		MaxIterations:   3,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeeper.GoalJobEnvelope(locator, "ship release", "101", 3)
	if err != nil {
		t.Fatalf("GoalJobEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusFailed {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusFailed)
	}
	if !strings.Contains(task.ResultJSON, `"status":"export_failed"`) {
		t.Fatalf("job result = %s, want export_failed status", task.ResultJSON)
	}
	if runtimeBuilder.cleanupCalls != 0 {
		t.Fatalf("cleanupCalls = %d, want 0 when export fails", runtimeBuilder.cleanupCalls)
	}
}

type goalTestEvent struct {
	kind         string
	step         string
	eventType    string
	text         string
	turnComplete bool
	planEntries  []map[string]any
}

func goalEventsAfterInitialFailure(finalValidatorText string) []goalTestEvent {
	return []goalTestEvent{
		{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
		{kind: "text", text: "first worker result"},
		{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
		{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
		{kind: "text", text: "verdict: fail\nEvidence: first failure"},
		{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted},
		{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
		{kind: "text", text: "final worker result"},
		{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
		{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
		{kind: "text", text: finalValidatorText},
		{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
	}
}

func (e goalTestEvent) build(invocationID string, fallbackValidatorText string) *adksession.Event {
	ev := adksession.NewEvent(context.Background(), invocationID)
	switch e.kind {
	case "step":
		ev.CustomMetadata = map[string]any{
			goalkeeper.MetadataEventKey: e.eventType,
			goalkeeper.MetadataStepKey:  e.step,
		}
	case "text":
		text := e.text
		if text == "" {
			text = fallbackValidatorText
		}
		ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	case "plan":
		ev.Actions.StateDelta = map[string]any{}
		ev.Actions.StateDelta[progress.ACPPlanMetadataKey] = map[string]any{"entries": e.planEntries}
	}
	ev.TurnComplete = e.turnComplete
	return ev
}

type fakeGoalRun struct {
	runner             goalkeeper.GoalRunner
	sessionID          string
	workspaceDir       string
	branchName         string
	finalizeFn         func(context.Context, string, string, string) (goalkeeper.GoalFinalizationResult, error)
	cleanupResourcesFn func(context.Context) error
}

func (r fakeGoalRun) Runner() goalkeeper.GoalRunner { return r.runner }
func (r fakeGoalRun) SessionID() string             { return r.sessionID }
func (r fakeGoalRun) WorkspaceDir() string          { return r.workspaceDir }
func (r fakeGoalRun) BranchName() string            { return r.branchName }
func (r fakeGoalRun) Close() error                  { return nil }

func (r fakeGoalRun) CleanupResources(ctx context.Context) error {
	if r.cleanupResourcesFn == nil {
		return nil
	}
	return r.cleanupResourcesFn(ctx)
}

func (r fakeGoalRun) Finalize(
	ctx context.Context,
	objective string,
	workerOutput string,
	validatorOutput string,
) (goalkeeper.GoalFinalizationResult, error) {
	if r.finalizeFn == nil {
		return goalkeeper.GoalFinalizationResult{Status: "not_exported", Reason: "workspace_disabled"}, nil
	}
	return r.finalizeFn(ctx, objective, workerOutput, validatorOutput)
}

func deliveryTextsForTask(t *testing.T, bus *recordingHandlerCommandBus, taskID string) []string {
	t.Helper()
	payloads := deliveryPayloadsForTask(t, bus, taskID)
	var texts []string
	for _, payload := range payloads {
		text := payload.Text
		if strings.TrimSpace(text) == "" && payload.Progress != nil {
			text = payload.Progress.Text
		}
		texts = append(texts, text)
	}
	return texts
}

func deliveryPayloadsForTask(t *testing.T, bus *recordingHandlerCommandBus, taskID string) []DeliveryPayload {
	t.Helper()
	var payloads []DeliveryPayload
	for _, env := range bus.commands {
		if baldaexecution.EnvelopeJobID(env) != taskID || env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		var payload DeliveryPayload
		if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		payloads = append(payloads, payload)
	}
	return payloads
}

func countMatches(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func countContains(values []string, want string) int {
	count := 0
	for _, value := range values {
		if strings.Contains(value, want) {
			count++
		}
	}
	return count
}
