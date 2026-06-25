package locatorref

import (
	"strings"
	"testing"

	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
)

func TestFormatTelegram(t *testing.T) {
	t.Parallel()

	locator := baldatelegram.NewLocator(-1002667079342, 8939)
	if got, want := Format(locator), "telegram:-1002667079342:8939"; got != want {
		t.Fatalf("Format() = %q, want %q", got, want)
	}
}

func TestParseTelegram(t *testing.T) {
	t.Parallel()

	got, err := Parse("telegram:-1002667079342:8939")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	want := baldatelegram.NewLocator(-1002667079342, 8939)
	if got != want {
		t.Fatalf("Parse() = %+v, want %+v", got, want)
	}
}

func TestParseRejectsMalformedRef(t *testing.T) {
	t.Parallel()

	_, err := Parse("telegram")
	if err == nil {
		t.Fatal("Parse() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "<channel_type>:<address_key>") {
		t.Fatalf("Parse() error = %v", err)
	}
}

func TestParseSlackThread(t *testing.T) {
	t.Parallel()

	got, err := Parse("slack:t:T123:C456:1712345678.000100")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	want := baldaslack.NewThreadLocator("T123", "C456", "1712345678.000100")
	if got != want {
		t.Fatalf("Parse() = %+v, want %+v", got, want)
	}
}

func TestParseRejectsUnknownTransport(t *testing.T) {
	t.Parallel()

	_, err := Parse("matrix:ops:deploy")
	if err == nil {
		t.Fatal("Parse() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), `unsupported locator transport "matrix"`) {
		t.Fatalf("Parse() error = %v", err)
	}
}
