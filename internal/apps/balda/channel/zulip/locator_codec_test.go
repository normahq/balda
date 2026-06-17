package zulip

import (
	"strings"
	"testing"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func TestLocatorFromAddressKey_RoundTripsStreamLocator(t *testing.T) {
	locator := NewStreamLocator(42, "ops/release:nightly")

	got, err := LocatorFromAddressKey(locator.AddressKey)
	if err != nil {
		t.Fatalf("LocatorFromAddressKey() error = %v", err)
	}
	if got != locator {
		t.Fatalf("LocatorFromAddressKey() = %#v, want %#v", got, locator)
	}

	address, ok, err := DecodeLocator(got)
	if err != nil {
		t.Fatalf("DecodeLocator() error = %v", err)
	}
	if !ok {
		t.Fatal("DecodeLocator() ok = false, want true")
	}
	if address.Type != addressTypeStream || address.StreamID != 42 || address.Topic != "ops/release:nightly" {
		t.Fatalf("decoded address = %#v, want stream 42 ops/release:nightly", address)
	}
}

func TestLocatorFromAddressKey_RoundTripsDMLocator(t *testing.T) {
	locator := NewDMLocator(101)

	got, err := LocatorFromAddressKey(locator.AddressKey)
	if err != nil {
		t.Fatalf("LocatorFromAddressKey() error = %v", err)
	}
	if got != locator {
		t.Fatalf("LocatorFromAddressKey() = %#v, want %#v", got, locator)
	}

	address, ok, err := DecodeLocator(got)
	if err != nil {
		t.Fatalf("DecodeLocator() error = %v", err)
	}
	if !ok {
		t.Fatal("DecodeLocator() ok = false, want true")
	}
	if address.Type != addressTypeDM || address.UserID != 101 {
		t.Fatalf("decoded address = %#v, want dm 101", address)
	}
}

func TestLocatorFromAddressKeyRejectsNonPositiveIDs(t *testing.T) {
	tests := []struct {
		name       string
		addressKey string
		wantMarker string
	}{
		{name: "zero stream", addressKey: "s:0:ops", wantMarker: "stream_id"},
		{name: "negative stream", addressKey: "s:-1:ops", wantMarker: "stream_id"},
		{name: "zero dm", addressKey: "dm:0", wantMarker: "user_id"},
		{name: "negative dm", addressKey: "dm:-1", wantMarker: "user_id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LocatorFromAddressKey(tt.addressKey)
			if err == nil {
				t.Fatal("LocatorFromAddressKey() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantMarker) {
				t.Fatalf("LocatorFromAddressKey() error = %q, want marker %q", err, tt.wantMarker)
			}
		})
	}
}

func TestDecodeLocatorRejectsInvalidAddress(t *testing.T) {
	tests := []struct {
		name        string
		addressJSON string
		wantMarker  string
	}{
		{name: "zero stream", addressJSON: `{"type":"stream","stream_id":0,"topic":"ops"}`, wantMarker: "stream_id"},
		{name: "zero dm", addressJSON: `{"type":"dm","user_id":0}`, wantMarker: "user_id"},
		{name: "unknown type", addressJSON: `{"type":"unknown"}`, wantMarker: "unsupported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeLocator(baldasession.SessionLocator{
				ChannelType: baldastate.ChannelTypeZulip,
				AddressKey:  "test",
				AddressJSON: tt.addressJSON,
				SessionID:   "zu-test",
			})
			if err == nil {
				t.Fatal("DecodeLocator() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantMarker) {
				t.Fatalf("DecodeLocator() error = %q, want marker %q", err, tt.wantMarker)
			}
		})
	}
}
