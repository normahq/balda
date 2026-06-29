// Package deliveryfmt defines transport-neutral delivery presentation options.
package deliveryfmt

import (
	"strings"

	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
)

type Format string

const (
	FormatAuto     Format = "auto"
	FormatMarkdown Format = "markdown"
	FormatHTML     Format = "html"
	FormatPlain    Format = "plain"
)

// ProgressPolicy describes which progress indicators a delivery target supports.
type ProgressPolicy struct {
	Typing   bool `json:"typing,omitempty"`
	Thinking bool `json:"thinking,omitempty"`
}

// Profile snapshots delivery-target formatting attributes at request time.
type Profile struct {
	Format       Format `json:"format,omitempty"`
	TelegramMode string `json:"telegram_mode,omitempty"`

	// FormattingMode is accepted for compatibility with older queued payloads.
	FormattingMode string `json:"formatting_mode,omitempty"`
}

type Options struct {
	Profile        Profile        `json:"profile,omitempty,omitzero"`
	ProgressPolicy ProgressPolicy `json:"progress_policy,omitempty,omitzero"`
}

func NormalizeOptions(options Options) Options {
	return Options{
		Profile:        NormalizeProfile(options.Profile),
		ProgressPolicy: options.ProgressPolicy,
	}
}

func NormalizeProfile(profile Profile) Profile {
	format := Format(strings.ToLower(strings.TrimSpace(string(profile.Format))))
	telegramMode := strings.ToLower(strings.TrimSpace(profile.TelegramMode))
	legacy := strings.ToLower(strings.TrimSpace(profile.FormattingMode))

	if format == "" && legacy != "" {
		if isTelegramMode(legacy) {
			format = FormatAuto
			telegramMode = legacy
		} else if isDeliveryFormat(legacy) {
			format = Format(legacy)
		}
	}
	if format == "" {
		format = FormatAuto
	}
	if telegramMode == "" && isTelegramMode(legacy) {
		telegramMode = legacy
	}

	return Profile{
		Format:       format,
		TelegramMode: telegramMode,
	}
}

func isDeliveryFormat(value string) bool {
	switch value {
	case string(FormatAuto), string(FormatMarkdown), string(FormatHTML), string(FormatPlain):
		return true
	default:
		return false
	}
}

func isTelegramMode(value string) bool {
	switch value {
	case telegramfmt.ModeRichMarkdown, telegramfmt.ModeRichHTML, telegramfmt.ModeMarkdownV2, telegramfmt.ModeHTML, telegramfmt.ModeNone:
		return true
	default:
		return false
	}
}

func EffectiveTelegramMode(profile Profile, fallback string) string {
	normalized := NormalizeProfile(profile)
	if normalized.Format == FormatPlain {
		return telegramfmt.ModeNone
	}
	if normalized.TelegramMode != "" {
		return telegramfmt.NormalizeMode(normalized.TelegramMode)
	}
	return telegramfmt.NormalizeMode(fallback)
}
