package slack

import (
	"testing"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func TestThreadLocatorRoundTrip(t *testing.T) {
	t.Parallel()

	locator := NewThreadLocator("T123", "C456", "1712345678.000100")
	if locator.ChannelType != baldastate.ChannelTypeSlack {
		t.Fatalf("ChannelType = %q, want slack", locator.ChannelType)
	}
	if locator.AddressKey != "t:T123:C456:1712345678.000100" {
		t.Fatalf("AddressKey = %q", locator.AddressKey)
	}

	got, err := LocatorFromAddressKey(locator.AddressKey)
	if err != nil {
		t.Fatalf("LocatorFromAddressKey() error = %v", err)
	}
	if got != locator {
		t.Fatalf("LocatorFromAddressKey() = %+v, want %+v", got, locator)
	}

	address, ok, err := DecodeLocator(locator)
	if err != nil {
		t.Fatalf("DecodeLocator() error = %v", err)
	}
	if !ok {
		t.Fatal("DecodeLocator() ok = false, want true")
	}
	if address.Type != addressTypeThread || address.TeamID != "T123" || address.Channel != "C456" || address.ThreadTS != "1712345678.000100" {
		t.Fatalf("DecodeLocator() address = %+v", address)
	}
}

func TestDMLocatorRoundTrip(t *testing.T) {
	t.Parallel()

	locator := NewDMLocator("T123", "D456")
	if locator.AddressKey != "dm:T123:D456" {
		t.Fatalf("AddressKey = %q", locator.AddressKey)
	}
	got, err := LocatorFromAddressKey(locator.AddressKey)
	if err != nil {
		t.Fatalf("LocatorFromAddressKey() error = %v", err)
	}
	if got != locator {
		t.Fatalf("LocatorFromAddressKey() = %+v, want %+v", got, locator)
	}
}

func TestUserID(t *testing.T) {
	t.Parallel()

	if got, want := UserID(" T123 ", " U456 "), "slack:T123:U456"; got != want {
		t.Fatalf("UserID() = %q, want %q", got, want)
	}
}
