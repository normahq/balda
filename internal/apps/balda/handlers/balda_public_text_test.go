package handlers

import (
	"strings"
	"testing"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/tgbotkit/client"
)

func TestNormalizePublicText_MentionEntityAnywhereBuildsStructuredInputWithReply(t *testing.T) {
	h := &BaldaHandler{}
	setUnexportedField(t, h, "botUsername", "testbot")

	text := "please @testbot review this"
	normalized, ok := h.normalizePublicText(baldatelegram.MessageContext{
		Text:         text,
		ReplyContent: "previous message body",
		Entities: []client.MessageEntity{
			{Type: "mention", Offset: 7, Length: len("@testbot")},
		},
	})

	if !ok {
		t.Fatal("normalizePublicText() ok = false, want true")
	}
	if !strings.Contains(normalized, "Reply context:\nprevious message body") {
		t.Fatalf("normalized text = %q, want reply context block", normalized)
	}
	if !strings.Contains(normalized, "User message:\nplease") || !strings.Contains(normalized, "review this") {
		t.Fatalf("normalized text = %q, want user message block without mention", normalized)
	}
	if strings.Contains(normalized, "@testbot") {
		t.Fatalf("normalized text = %q, want bot mention stripped", normalized)
	}
}

func TestNormalizePublicText_MentionOnlyWithReplyContentIsProcessed(t *testing.T) {
	h := &BaldaHandler{}
	setUnexportedField(t, h, "botUsername", "testbot")

	normalized, ok := h.normalizePublicText(baldatelegram.MessageContext{
		Text:         "@testbot",
		ReplyContent: "quoted context",
		Entities: []client.MessageEntity{
			{Type: "mention", Offset: 0, Length: len("@testbot")},
		},
	})

	if !ok {
		t.Fatal("normalizePublicText() ok = false, want true")
	}
	want := "Reply context:\nquoted context"
	if normalized != want {
		t.Fatalf("normalized text = %q, want %q", normalized, want)
	}
}

func TestNormalizePublicText_MentionOnlyWithoutReplyContentIsIgnored(t *testing.T) {
	h := &BaldaHandler{}
	setUnexportedField(t, h, "botUsername", "testbot")

	normalized, ok := h.normalizePublicText(baldatelegram.MessageContext{
		Text: "@testbot",
		Entities: []client.MessageEntity{
			{Type: "mention", Offset: 0, Length: len("@testbot")},
		},
	})

	if ok {
		t.Fatalf("normalizePublicText() ok = true, want false with normalized=%q", normalized)
	}
}

func TestNormalizePublicText_DirectReplyToBotIncludesReplyContext(t *testing.T) {
	h := &BaldaHandler{}
	setUnexportedField(t, h, "botUserID", int64(4242))
	setUnexportedField(t, h, "botUsername", "testbot")

	normalized, ok := h.normalizePublicText(baldatelegram.MessageContext{
		Text:          "follow up message",
		IsReply:       true,
		ReplyToIsBot:  true,
		ReplyToUserID: 4242,
		ReplyContent:  "bot previous response",
	})

	if !ok {
		t.Fatal("normalizePublicText() ok = false, want true")
	}
	want := "Reply context:\nbot previous response\n\nUser message:\nfollow up message"
	if normalized != want {
		t.Fatalf("normalized text = %q, want %q", normalized, want)
	}
}

func TestNormalizePublicText_ForwardedBotMessageIncludesForwardedContext(t *testing.T) {
	h := &BaldaHandler{}
	setUnexportedField(t, h, "botUserID", int64(4242))

	normalized, ok := h.normalizePublicText(baldatelegram.MessageContext{
		Text:             "bot previous response",
		IsForwarded:      true,
		ForwardedFromBot: true,
		ForwardedContent: "bot previous response",
	})

	if !ok {
		t.Fatal("normalizePublicText() ok = false, want true")
	}
	want := "Forwarded context:\nbot previous response"
	if normalized != want {
		t.Fatalf("normalized text = %q, want %q", normalized, want)
	}
}

func TestBotMentionEntityRanges_SupportsUTF16Offsets(t *testing.T) {
	text := "hi 😀 @testbot now"
	ranges := botMentionEntityRanges(text, []client.MessageEntity{
		{Type: "mention", Offset: 6, Length: len("@testbot")},
	}, "testbot")

	if len(ranges) != 1 {
		t.Fatalf("ranges len = %d, want 1", len(ranges))
	}
	if got := strings.TrimSpace(removeTextByUTF16Ranges(text, ranges)); got != "hi 😀  now" {
		t.Fatalf("text after mention removal = %q, want %q", got, "hi 😀  now")
	}
}
