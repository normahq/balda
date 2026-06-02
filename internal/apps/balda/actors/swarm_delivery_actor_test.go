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

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
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
	if got := len(tgClient.messages); got != 1 {
		t.Fatalf("sent telegram messages = %d, want 1", got)
	}
}

func TestTaskDeliveryActorDefersDuplicatePendingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-pending", "task-1:delivery:pending", "Goal started")
	if _, _, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.SwarmDeliveryStatusPending)); err != nil {
		t.Fatalf("ReserveDelivery() error = %v", err)
	}
	if err := actor.Handle(ctx, env); swarm.ClassifyError(err) != swarm.ErrorKindTransient {
		t.Fatalf("Handle() error kind = %s, want transient: %v", swarm.ClassifyError(err), err)
	}
	if got := len(tgClient.messages); got != 0 {
		t.Fatalf("sent telegram messages = %d, want 0 while duplicate is pending", got)
	}
}

func TestTaskDeliveryActorDoesNotRetryAmbiguousSendingDelivery(t *testing.T) {
	ctx := context.Background()
	actor, tasks, tgClient, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-sending", "task-1:delivery:completed", "Goal completed")
	if _, _, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.SwarmDeliveryStatusSending)); err != nil {
		t.Fatalf("ReserveDelivery() error = %v", err)
	}
	if err := actor.Handle(ctx, env); swarm.ClassifyError(err) != swarm.ErrorKindTransient {
		t.Fatalf("Handle() error kind = %s, want transient: %v", swarm.ClassifyError(err), err)
	}
	if got := len(tgClient.messages); got != 0 {
		t.Fatalf("sent telegram messages = %d, want 0 for ambiguous sending delivery", got)
	}
}

func TestDeliveryReadyForAttemptNeverRetriesSendingDelivery(t *testing.T) {
	record := baldastate.SwarmDeliveryRecord{
		Status:    baldastate.SwarmDeliveryStatusSending,
		UpdatedAt: time.Now().Add(-2 * deliveryPendingRetryAfter),
	}
	if deliveryReadyForAttempt(record) {
		t.Fatal("deliveryReadyForAttempt(sending) = true, want false because send outcome is ambiguous")
	}
}

func TestTaskDeliveryActorPublishesFailedEventOnSendError(t *testing.T) {
	ctx := context.Background()
	actor, _, tgClient, bus := newTaskDeliveryActorForTest(t, ctx)
	tgClient.sendErr = errors.New("telegram send failed")
	env, _ := deliveryEnvelopeForTest(t, "delivery-command-failed", "task-1:delivery:failed", "Goal failed")

	err := actor.Handle(ctx, env)
	if swarm.ClassifyError(err) != swarm.ErrorKindExternalDelivery {
		t.Fatalf("Handle() error kind = %s, want external_delivery: %v", swarm.ClassifyError(err), err)
	}

	if len(bus.eventSubjects) != 1 {
		t.Fatalf("published event subjects len = %d, want 1", len(bus.eventSubjects))
	}
	if got := bus.eventSubjects[0]; got != swarm.SubjectEventDeliveryFailed {
		t.Fatalf("event subject = %q, want %q", got, swarm.SubjectEventDeliveryFailed)
	}
	if len(bus.eventEnvs) != 1 {
		t.Fatalf("published event envelopes len = %d, want 1", len(bus.eventEnvs))
	}
	if got := bus.eventEnvs[0].Meta["event_type"]; got != swarm.TaskEventDeliveryFailed {
		t.Fatalf("event type = %q, want %q", got, swarm.TaskEventDeliveryFailed)
	}
}

func TestTaskDeliveryActorStoresProviderMessageIDOnSuccess(t *testing.T) {
	ctx := context.Background()
	actor, tasks, _, _ := newTaskDeliveryActorForTest(t, ctx)
	env, payload := deliveryEnvelopeForTest(t, "delivery-command-success", "task-1:delivery:success", "Goal success")

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	record, created, err := tasks.ReserveDelivery(ctx, deliveryRecordForTest(env, payload, baldastate.SwarmDeliveryStatusPending))
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

func newTaskDeliveryActorForTest(t *testing.T, ctx context.Context) (*taskDeliveryActor, *swarm.TaskService, *fakeTelegramClient, *recordingHandlerCommandBus) {
	t.Helper()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	return &taskDeliveryActor{
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		tasks:  tasks,
		logger: zerolog.Nop(),
	}, tasks, tgClient, bus
}

func deliveryEnvelopeForTest(t *testing.T, id string, dedupeKey string, text string) (swarm.Envelope, DeliveryPayload) {
	t.Helper()
	locator := baldatelegram.NewLocator(9001, 99)
	payload := DeliveryPayload{TaskID: "task-1", Locator: locator, Text: text}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return swarm.Envelope{
		ID:          id,
		Namespace:   swarm.NamespaceAgentResult,
		Kind:        taskPayloadKindDelivery,
		From:        swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: "task-1"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeDelivery, Key: "9001:99"},
		SessionID:   locator.SessionID,
		TaskID:      "task-1",
		DedupeKey:   dedupeKey,
		PayloadJSON: string(data),
	}, payload
}

func deliveryRecordForTest(env swarm.Envelope, payload DeliveryPayload, status string) baldastate.SwarmDeliveryRecord {
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortTaskHash(env.PayloadJSON)
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(env.PayloadJSON)))

	return baldastate.SwarmDeliveryRecord{
		ID:          "delivery-record-" + env.ID,
		DeliveryKey: deliveryKey,
		TaskID:      payload.TaskID,
		SessionID:   payload.Locator.SessionID,
		Channel:     "telegram",
		AddressKey:  payload.Locator.AddressKey,
		Kind:        env.Kind,
		PayloadJSON: env.PayloadJSON,
		PayloadHash: hex.EncodeToString(sum[:]),
		Status:      status,
	}
}
