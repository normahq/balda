package deliveryfmt

import (
	"testing"

	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
)

func TestNormalizeProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Profile
		want Profile
	}{
		{
			name: "empty defaults to auto",
			in:   Profile{},
			want: Profile{Format: FormatAuto},
		},
		{
			name: "neutral markdown",
			in:   Profile{Format: " MARKDOWN "},
			want: Profile{Format: FormatMarkdown},
		},
		{
			name: "legacy telegram html stays telegram mode",
			in:   Profile{FormattingMode: telegramfmt.ModeHTML},
			want: Profile{Format: FormatAuto, TelegramMode: telegramfmt.ModeHTML},
		},
		{
			name: "legacy plain becomes plain format",
			in:   Profile{FormattingMode: "plain"},
			want: Profile{Format: FormatPlain},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := NormalizeProfile(tt.in); got != tt.want {
				t.Fatalf("NormalizeProfile() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestEffectiveTelegramMode(t *testing.T) {
	t.Parallel()

	if got := EffectiveTelegramMode(Profile{Format: FormatPlain}, telegramfmt.ModeRichMarkdown); got != telegramfmt.ModeNone {
		t.Fatalf("EffectiveTelegramMode(plain) = %q, want %q", got, telegramfmt.ModeNone)
	}
	if got := EffectiveTelegramMode(Profile{TelegramMode: telegramfmt.ModeMarkdownV2}, telegramfmt.ModeRichMarkdown); got != telegramfmt.ModeMarkdownV2 {
		t.Fatalf("EffectiveTelegramMode(telegram mode) = %q, want %q", got, telegramfmt.ModeMarkdownV2)
	}
}
