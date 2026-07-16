package handlers

import (
	"fmt"
	"sort"
	"strings"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/tgbotkit/client"
)

func (h *BaldaHandler) normalizePublicText(messageCtx baldatelegram.MessageContext) (string, bool) {
	botUserID, botUsername := h.getBotIdentity()
	replyContent := strings.TrimSpace(messageCtx.ReplyContent)
	forwardedContent := strings.TrimSpace(messageCtx.ForwardedContent)

	if botUsername != "" {
		mentionRanges := botMentionEntityRanges(messageCtx.Text, messageCtx.Entities, botUsername)
		if len(mentionRanges) > 0 {
			userMessage := strings.TrimSpace(removeTextByUTF16Ranges(messageCtx.Text, mentionRanges))
			return composeContextAwareInput(userMessage, replyContent, forwardedContent)
		}
	}

	if messageCtx.IsForwarded && messageCtx.ForwardedFromBot {
		return composeContextAwareInput("", replyContent, forwardedContent)
	}

	if !messageCtx.IsReply || !messageCtx.ReplyToIsBot || botUserID == 0 {
		return "", false
	}

	if messageCtx.ReplyToUserID != botUserID {
		return "", false
	}

	return composeContextAwareInput(strings.TrimSpace(messageCtx.Text), replyContent, forwardedContent)
}

func (h *BaldaHandler) normalizeDMText(messageCtx baldatelegram.MessageContext) string {
	if !messageCtx.IsReply && !messageCtx.IsForwarded {
		return messageCtx.Text
	}
	if normalized, ok := composeContextAwareInput(
		strings.TrimSpace(messageCtx.Text),
		strings.TrimSpace(messageCtx.ReplyContent),
		strings.TrimSpace(messageCtx.ForwardedContent),
	); ok {
		return normalized
	}
	return messageCtx.Text
}

func composeContextAwareInput(userMessage, replyContent, forwardedContent string) (string, bool) {
	switch {
	case replyContent != "" && forwardedContent != "" && userMessage != "":
		return fmt.Sprintf("Reply context:\n%s\n\nForwarded context:\n%s\n\nUser message:\n%s", replyContent, forwardedContent, stripDuplicateUserMessage(userMessage, forwardedContent)), true
	case replyContent != "" && forwardedContent != "":
		return fmt.Sprintf("Reply context:\n%s\n\nForwarded context:\n%s", replyContent, forwardedContent), true
	case replyContent != "" && userMessage != "":
		return fmt.Sprintf("Reply context:\n%s\n\nUser message:\n%s", replyContent, userMessage), true
	case forwardedContent != "" && userMessage != "":
		return fmt.Sprintf("Forwarded context:\n%s\n\nUser message:\n%s", forwardedContent, stripDuplicateUserMessage(userMessage, forwardedContent)), true
	case replyContent != "":
		return fmt.Sprintf("Reply context:\n%s", replyContent), true
	case forwardedContent != "":
		return fmt.Sprintf("Forwarded context:\n%s", forwardedContent), true
	case userMessage != "":
		return userMessage, true
	default:
		return "", false
	}
}

func stripDuplicateUserMessage(userMessage, forwardedContent string) string {
	if strings.TrimSpace(userMessage) == strings.TrimSpace(forwardedContent) {
		return ""
	}
	return userMessage
}

type utf16Range struct {
	start int
	end   int
}

func botMentionEntityRanges(text string, entities []client.MessageEntity, botUsername string) []utf16Range {
	trimmedUsername := strings.TrimSpace(botUsername)
	if trimmedUsername == "" || len(entities) == 0 || strings.TrimSpace(text) == "" {
		return nil
	}
	expectedMention := "@" + trimmedUsername
	ranges := make([]utf16Range, 0, len(entities))
	for _, entity := range entities {
		if entity.Type != "mention" {
			continue
		}
		if entity.Length <= 0 || entity.Offset < 0 {
			continue
		}
		mentionText, ok := utf16TextSlice(text, entity.Offset, entity.Length)
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(mentionText), expectedMention) {
			continue
		}
		ranges = append(ranges, utf16Range{
			start: entity.Offset,
			end:   entity.Offset + entity.Length,
		})
	}
	return ranges
}

func removeTextByUTF16Ranges(text string, ranges []utf16Range) string {
	if len(ranges) == 0 || text == "" {
		return text
	}
	runes := []rune(text)
	type runeRange struct {
		start int
		end   int
	}
	runeRanges := make([]runeRange, 0, len(ranges))
	for _, r := range ranges {
		start, ok := utf16OffsetToRuneIndex(runes, r.start)
		if !ok {
			continue
		}
		end, ok := utf16OffsetToRuneIndex(runes, r.end)
		if !ok || end <= start {
			continue
		}
		runeRanges = append(runeRanges, runeRange{start: start, end: end})
	}
	if len(runeRanges) == 0 {
		return text
	}
	sort.Slice(runeRanges, func(i, j int) bool {
		if runeRanges[i].start == runeRanges[j].start {
			return runeRanges[i].end < runeRanges[j].end
		}
		return runeRanges[i].start < runeRanges[j].start
	})
	merged := make([]runeRange, 0, len(runeRanges))
	for _, rr := range runeRanges {
		if len(merged) == 0 {
			merged = append(merged, rr)
			continue
		}
		last := &merged[len(merged)-1]
		if rr.start > last.end {
			merged = append(merged, rr)
			continue
		}
		if rr.end > last.end {
			last.end = rr.end
		}
	}

	var out strings.Builder
	prevEnd := 0
	for _, rr := range merged {
		if rr.start > prevEnd {
			out.WriteString(string(runes[prevEnd:rr.start]))
		}
		prevEnd = rr.end
	}
	if prevEnd < len(runes) {
		out.WriteString(string(runes[prevEnd:]))
	}
	return out.String()
}

func utf16TextSlice(text string, offset, length int) (string, bool) {
	if length <= 0 || offset < 0 {
		return "", false
	}
	runes := []rune(text)
	start, ok := utf16OffsetToRuneIndex(runes, offset)
	if !ok {
		return "", false
	}
	end, ok := utf16OffsetToRuneIndex(runes, offset+length)
	if !ok || end < start {
		return "", false
	}
	return string(runes[start:end]), true
}

func utf16OffsetToRuneIndex(runes []rune, targetOffset int) (int, bool) {
	if targetOffset < 0 {
		return 0, false
	}
	units := 0
	for idx, r := range runes {
		if units == targetOffset {
			return idx, true
		}
		units += utf16UnitsForRune(r)
	}
	if units == targetOffset {
		return len(runes), true
	}
	return 0, false
}

func utf16UnitsForRune(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}
