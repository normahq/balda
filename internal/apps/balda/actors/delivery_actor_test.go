package actors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryworkflow"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

func TestTaskDeliveryActorDeduplicatesSentDelivery(t *testing.T) {
	ctx := context.Background()
	actor, _, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	env, _ := deliveryEnvelopeForTest(t, "delivery-command-1", "task-1:delivery:started", "Goal started")

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() first error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() duplicate error = %v", err)
	}
	if got := len(tgClient.richMessages); got != 1 {
		t.Fatalf("sent rich telegram messages = %d, want 1", got)
	}
}

func TestTaskDeliveryActorDefersDuplicatePendingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-pending", "task-1:delivery:pending", "Goal started")
	if _, _, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending)); err != nil {
		t.Fatalf("ReserveDelivery() error = %v", err)
	}
	if err := actor.Handle(ctx, env); actorlayer.ClassifyError(err) != actorlayer.ErrorKindTransient {
		t.Fatalf("Handle() error kind = %s, want transient: %v", actorlayer.ClassifyError(err), err)
	}
	if got := len(tgClient.richMessages); got != 0 {
		t.Fatalf("sent rich telegram messages = %d, want 0 while duplicate is pending", got)
	}
}

func TestTaskDeliveryActorDoesNotRetryAmbiguousSendingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-sending", "task-1:delivery:completed", "Goal completed")
	if _, _, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusSending)); err != nil {
		t.Fatalf("ReserveDelivery() error = %v", err)
	}
	if err := actor.Handle(ctx, env); actorlayer.ClassifyError(err) != actorlayer.ErrorKindTransient {
		t.Fatalf("Handle() error kind = %s, want transient: %v", actorlayer.ClassifyError(err), err)
	}
	if got := len(tgClient.richMessages); got != 0 {
		t.Fatalf("sent rich telegram messages = %d, want 0 for ambiguous sending delivery", got)
	}
}

func TestDeliveryReadyForAttemptNeverRetriesSendingDelivery(t *testing.T) {
	record := baldastate.DeliveryRecord{
		Status:    baldastate.DeliveryStatusSending,
		UpdatedAt: time.Now().Add(-60 * time.Second),
	}
	if deliveryworkflow.ReadyForAttempt(record) {
		t.Fatal("ReadyForAttempt(sending) = true, want false because send outcome is ambiguous")
	}
}

func TestTaskDeliveryActorPublishesFailedEventOnSendError(t *testing.T) {
	ctx := context.Background()
	actor, _, tgClient, bus := newTaskDeliveryActorForTest(t, ctx)
	tgClient.sendErr = errors.New("telegram send failed")
	env, _ := deliveryEnvelopeForTest(t, "delivery-command-failed", "task-1:delivery:failed", "Goal failed")

	err := actor.Handle(ctx, env)
	if actorlayer.ClassifyError(err) != actorlayer.ErrorKindExternalDelivery {
		t.Fatalf("Handle() error kind = %s, want external_delivery: %v", actorlayer.ClassifyError(err), err)
	}

	if len(bus.eventSubjects) != 1 {
		t.Fatalf("published event subjects len = %d, want 1", len(bus.eventSubjects))
	}
	if got := bus.eventSubjects[0]; got != baldaexecution.SubjectEventDeliveryFailed {
		t.Fatalf("event subject = %q, want %q", got, baldaexecution.SubjectEventDeliveryFailed)
	}
	if len(bus.eventEnvs) != 1 {
		t.Fatalf("published event envelopes len = %d, want 1", len(bus.eventEnvs))
	}
	if got := bus.eventEnvs[0].Meta["event_type"]; got != baldajobs.JobEventDeliveryFailed {
		t.Fatalf("event type = %q, want %q", got, baldajobs.JobEventDeliveryFailed)
	}
}

func TestTaskDeliveryActorStoresProviderMessageIDOnSuccess(t *testing.T) {
	ctx := context.Background()
	actor, tasks, _, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-success", "task-1:delivery:success", "Goal success")

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending))
	if err != nil {
		t.Fatalf("ReserveDelivery() lookup error = %v", err)
	}
	if created {
		t.Fatal("ReserveDelivery() created = true, want existing delivery record")
	}
	if got := record.ProviderMessageID; got != "1" {
		t.Fatalf("provider_message_id = %q, want \"1\"", got)
	}
}

func TestTaskDeliveryActorSendsConversationalReplyWithoutPersistingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	locator := baldatelegram.NewLocator(9001, 99)
	env, err := AgentReplyDeliveryEnvelopeWithSettlement("", actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: locator.SessionID}, locator, deliverycmd.SettlementBypass, "session reply", "final")
	if err != nil {
		t.Fatalf("AgentReplyDeliveryEnvelopeWithSettlement() error = %v", err)
	}

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := len(tgClient.richMessages); got != 1 {
		t.Fatalf("sent rich telegram messages = %d, want 1", got)
	}
	payload := DeliveryPayload{Locator: locator, Mode: DeliveryModeAgentReply, Settlement: deliverycmd.SettlementBypass, Text: "session reply"}
	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending))
	if err != nil {
		t.Fatalf("ReserveDelivery() lookup error = %v", err)
	}
	if !created || record.Status != baldastate.DeliveryStatusPending {
		t.Fatalf("delivery record = %+v created=%t, want no persisted conversational delivery", record, created)
	}
}

func TestTaskDeliveryActorPersistsSessionOwnedTaskReplyWhenSettlementRequiresOutbox(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	locator := baldatelegram.NewLocator(9001, 99)
	env, err := AgentReplyDeliveryEnvelopeWithSettlement("task-1", actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: locator.SessionID}, locator, deliverycmd.SettlementOutbox, "session reply", "final")
	if err != nil {
		t.Fatalf("AgentReplyDeliveryEnvelopeWithSettlement() error = %v", err)
	}

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := len(tgClient.richMessages); got != 1 {
		t.Fatalf("sent rich telegram messages = %d, want 1", got)
	}
	payload := DeliveryPayload{JobID: "task-1", Locator: locator, Mode: DeliveryModeAgentReply, Settlement: deliverycmd.SettlementOutbox, Text: "session reply"}
	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending))
	if err != nil {
		t.Fatalf("ReserveDelivery() lookup error = %v", err)
	}
	if created {
		t.Fatal("ReserveDelivery() created = true, want existing persisted delivery record")
	}
	if record.Status != baldastate.DeliveryStatusSent {
		t.Fatalf("delivery status = %q, want %q", record.Status, baldastate.DeliveryStatusSent)
	}
}

func TestTaskDeliveryActorSendsDraftWithoutPersistingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	locator := baldatelegram.NewLocator(9001, 99)
	env, err := DraftPlainDeliveryEnvelope("task-1", actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"}, locator, 7, "draft text")
	if err != nil {
		t.Fatalf("DraftPlainDeliveryEnvelope() error = %v", err)
	}

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := len(tgClient.richDrafts); got != 1 {
		t.Fatalf("sent rich telegram drafts = %d, want 1", got)
	}
	payload := DeliveryPayload{JobID: "task-1", Locator: locator, Mode: DeliveryModeDraftPlain, Text: "draft text", DraftID: 7}
	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending))
	if err != nil {
		t.Fatalf("ReserveDelivery() lookup error = %v", err)
	}
	if !created || record.Status != baldastate.DeliveryStatusPending {
		t.Fatalf("delivery record = %+v created=%t, want no persisted draft delivery", record, created)
	}
}

func TestTaskDeliveryActorSendsChatActionWithoutPersistingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	locator := baldatelegram.NewLocator(9001, 99)
	env, err := ChatActionDeliveryEnvelope("task-1", actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"}, locator, "typing")
	if err != nil {
		t.Fatalf("ChatActionDeliveryEnvelope() error = %v", err)
	}

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := len(tgClient.chatActions); got != 1 {
		t.Fatalf("sent telegram chat actions = %d, want 1", got)
	}
	payload := DeliveryPayload{JobID: "task-1", Locator: locator, Mode: DeliveryModeChatAction, Action: "typing"}
	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.DeliveryStatusPending))
	if err != nil {
		t.Fatalf("ReserveDelivery() lookup error = %v", err)
	}
	if !created || record.Status != baldastate.DeliveryStatusPending {
		t.Fatalf("delivery record = %+v created=%t, want no persisted chat action delivery", record, created)
	}
}

func newTaskDeliveryActorForTest(t *testing.T, ctx context.Context) (*jobDeliveryActor, *testJobServices, *fakeTelegramClient, *recordingHandlerCommandBus) {
	t.Helper()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	tgAdapter := baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	router := baldachannel.NewRouter(map[string]deliverycmd.Adapter{
		baldatelegram.ChannelType: tgAdapter,
	})
	return &jobDeliveryActor{
		service: deliveryworkflow.New(deliveryworkflow.NewChannelDispatcher(router), tasks, tasks, zerolog.Nop()),
	}, tasks, tgClient, bus
}

func deliveryEnvelopeForTest(t *testing.T, id string, dedupeKey string, text string) (actorlayer.Envelope, DeliveryPayload) {
	t.Helper()
	locator := baldatelegram.NewLocator(9001, 99)
	payload := DeliveryPayload{JobID: "task-1", Locator: locator, Mode: DeliveryModeAgentReply, Text: text}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return actorlayer.Envelope{
		ID:        id,
		Namespace: baldaexecution.NamespaceAgentResult,
		Kind:      jobPayloadKindDelivery,
		From:      actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"},
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeDelivery, Key: locator.DeliveryActorKey()},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, payload.JobID), locator.SessionID),
		DedupeKey: dedupeKey,
		Payload: actorlayer.Payload{
			Encoding: actorlayer.EncodingJSON,
			Data:     data,
		},
	}, payload
}

func deliveryRecordForTest(env actorlayer.Envelope, payload DeliveryPayload, status string) baldastate.DeliveryRecord {
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortJobHash(env.Payload.String())
	}
	sum := sha256.Sum256(env.Payload.Data)

	return baldastate.DeliveryRecord{
		ID:          "delivery-record-" + env.ID,
		DeliveryKey: deliveryKey,
		SessionID:   payload.Locator.SessionID,
		Channel:     "telegram",
		AddressKey:  payload.Locator.AddressKey,
		Kind:        env.Kind,
		Payload:     env.Payload.String(),
		PayloadHash: hex.EncodeToString(sum[:]),
		Status:      status,
	}
}
