package dispatch_test

import (
	"context"
	"testing"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
)

type testActor struct {
	address string
}

func (a testActor) Address() string { return a.address }

func (testActor) Handle(context.Context, actorlayer.Envelope) error { return nil }

func TestMemoryRegistryZeroValueRegistersAndResolves(t *testing.T) {
	t.Parallel()

	var registry dispatch.MemoryRegistry
	actor := testActor{address: " Session:One "}
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, ok := registry.Resolve("session:one")
	if !ok {
		t.Fatal("Resolve() ok = false, want true")
	}
	if got.Address() != actor.Address() {
		t.Fatalf("Resolve() actor address = %q, want %q", got.Address(), actor.Address())
	}
}

func TestMemoryRegistryWildcardFallback(t *testing.T) {
	t.Parallel()

	registry := dispatch.NewMemoryRegistry()
	actor := testActor{address: "session:*"}
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, ok := registry.Resolve("SESSION:abc")
	if !ok {
		t.Fatal("Resolve() ok = false, want wildcard match")
	}
	if got.Address() != actor.Address() {
		t.Fatalf("Resolve() actor address = %q, want %q", got.Address(), actor.Address())
	}
}

func TestMemoryRegistryNilAndEmptyActorHandling(t *testing.T) {
	t.Parallel()

	registry := dispatch.NewMemoryRegistry()
	if err := registry.Register(nil); err != nil {
		t.Fatalf("Register(nil) error = %v, want nil", err)
	}
	if err := registry.Register(testActor{}); err == nil {
		t.Fatal("Register(empty address) error = nil, want error")
	}
	if _, ok := registry.Resolve(""); ok {
		t.Fatal("Resolve(empty) ok = true, want false")
	}
}
