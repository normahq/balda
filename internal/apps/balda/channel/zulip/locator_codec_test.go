package zulip

import "testing"

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
