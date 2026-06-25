package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChannelTokenStoreConsumesOwnerBindTokenOnce(t *testing.T) {
	store, err := NewChannelTokenStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewChannelTokenStore() error = %v", err)
	}
	token, _, err := store.CreateOwnerBindToken(context.Background(), ChannelSlack, "telegram:101")
	if err != nil {
		t.Fatalf("CreateOwnerBindToken() error = %v", err)
	}
	if !LooksLikeChannelToken(token) {
		t.Fatalf("LooksLikeChannelToken(%q) = false, want true", token)
	}

	record, err := store.Consume(context.Background(), token, ChannelSlack)
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if record == nil || record.Purpose != ChannelTokenPurposeOwnerBind {
		t.Fatalf("Consume() = %#v, want owner bind record", record)
	}
	record, err = store.Consume(context.Background(), token, ChannelSlack)
	if err != nil {
		t.Fatalf("Consume(second) error = %v", err)
	}
	if record != nil {
		t.Fatalf("Consume(second) = %#v, want nil", record)
	}
}

func TestChannelTokenStoreConcurrentConsumeSucceedsOnce(t *testing.T) {
	ctx := context.Background()
	store, err := NewChannelTokenStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewChannelTokenStore() error = %v", err)
	}
	token, _, err := store.CreateOwnerBindToken(ctx, ChannelSlack, "telegram:101")
	if err != nil {
		t.Fatalf("CreateOwnerBindToken() error = %v", err)
	}

	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := store.Consume(ctx, token, ChannelSlack)
			if err != nil {
				t.Errorf("Consume() error = %v", err)
				return
			}
			if record != nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := successes.Load(); got != 1 {
		t.Fatalf("successful consumes = %d, want 1", got)
	}
}

func TestChannelTokenStoreRejectsWrongChannel(t *testing.T) {
	store, err := NewChannelTokenStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewChannelTokenStore() error = %v", err)
	}
	token, _, err := store.CreateOwnerBindToken(context.Background(), ChannelSlack, "telegram:101")
	if err != nil {
		t.Fatalf("CreateOwnerBindToken() error = %v", err)
	}

	record, err := store.Consume(context.Background(), token, ChannelZulip)
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if record != nil {
		t.Fatalf("Consume() = %#v, want nil for wrong channel", record)
	}
	record, err = store.Consume(context.Background(), token, ChannelSlack)
	if err != nil {
		t.Fatalf("Consume(slack) error = %v", err)
	}
	if record == nil {
		t.Fatal("Consume(slack) = nil, want record after wrong-channel attempt")
	}
}

func TestChannelAuthServiceBindsOwnerSubject(t *testing.T) {
	ctx := context.Background()
	owner, err := NewOwnerStore(newMemoryOwnerKV())
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := owner.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}
	tokenStore, err := NewChannelTokenStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewChannelTokenStore() error = %v", err)
	}
	tokenStore.now = func() time.Time { return time.Unix(100, 0) }
	service := NewChannelAuthService(tokenStore, owner)
	token, err := service.CreateOwnerBindToken(ctx, ChannelSlack, TelegramSubject(101))
	if err != nil {
		t.Fatalf("CreateOwnerBindToken() error = %v", err)
	}

	consumed, err := service.ConsumeOwnerBind(ctx, ChannelSlack, SlackSubject("T123", "U456"), token)
	if err != nil {
		t.Fatalf("ConsumeOwnerBind() error = %v", err)
	}
	if !consumed {
		t.Fatal("ConsumeOwnerBind() = false, want true")
	}
	if !owner.IsOwnerSubject("slack:T123:U456") {
		t.Fatal("owner.IsOwnerSubject(slack:T123:U456) = false, want true")
	}
}
