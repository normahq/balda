package zulip

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
)

var (
	markdownImagePattern = regexp.MustCompile(`!\[([^\]]*)\]\(([^)]+)\)`)
	markdownLinkPattern  = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
)

// Adapter implements channel.ChannelAdapter for the Zulip transport.
type Adapter struct {
	client *Client
	logger zerolog.Logger
}

// NewAdapter creates a new Zulip channel adapter.
func NewAdapter(client *Client, logger zerolog.Logger) *Adapter {
	return &Adapter{
		client: client,
		logger: logger.With().Str("component", "balda.channel.zulip").Logger(),
	}
}

// SendPlain sends a plain text message to the locator.
func (a *Adapter) SendPlain(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	_, err := a.send(ctx, locator, text)
	return err
}

// SendMarkdown sends a Markdown message to the locator.
func (a *Adapter) SendMarkdown(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	_, err := a.sendWithPlainFallback(ctx, locator, text)
	return err
}

// SendAgentReply sends agent output to the locator.
func (a *Adapter) SendAgentReply(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	_, err := a.sendWithPlainFallback(ctx, locator, text)
	return err
}

// SendAgentReplyWithProviderMessageID sends agent output and returns the
// Zulip message ID as a string.
func (a *Adapter) SendAgentReplyWithProviderMessageID(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) (string, error) {
	msgID, err := a.sendWithPlainFallback(ctx, locator, text)
	if err != nil {
		return "", err
	}
	if msgID <= 0 {
		return "", nil
	}
	return strconv.Itoa(msgID), nil
}

// SendDraftPlain is a no-op for Zulip, which has no draft/edit-in-place concept.
func (a *Adapter) SendDraftPlain(
	_ context.Context,
	_ baldasession.SessionLocator,
	_ int,
	_ string,
) error {
	return nil
}

// SendTyping sends a typing indicator to the locator.
func (a *Adapter) SendTyping(
	ctx context.Context,
	locator baldasession.SessionLocator,
) error {
	address, ok, err := DecodeLocator(locator)
	if err != nil {
		return fmt.Errorf("decode zulip locator for typing: %w", err)
	}
	if !ok {
		return fmt.Errorf("unsupported channel type %q for zulip typing", locator.ChannelType)
	}
	switch address.Type {
	case addressTypeStream:
		return a.client.SendStreamTyping(ctx, address.StreamID, address.Topic)
	case addressTypeDM:
		return a.client.SendDirectTyping(ctx, address.UserID)
	default:
		return fmt.Errorf("unsupported zulip address type %q for typing", address.Type)
	}
}

func (a *Adapter) send(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) (int, error) {
	address, ok, err := DecodeLocator(locator)
	if err != nil {
		return 0, fmt.Errorf("decode zulip locator: %w", err)
	}
	if !ok {
		return 0, fmt.Errorf("unsupported channel type %q", locator.ChannelType)
	}
	switch address.Type {
	case addressTypeStream:
		return a.client.SendStreamMessage(ctx, address.StreamID, address.Topic, text)
	case addressTypeDM:
		return a.client.SendDirectMessage(ctx, address.UserID, text)
	default:
		return 0, fmt.Errorf("unsupported zulip address type %q", address.Type)
	}
}

func (a *Adapter) sendWithPlainFallback(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) (int, error) {
	msgID, err := a.send(ctx, locator, text)
	if err == nil {
		return msgID, nil
	}
	if !isContentRejectedError(err) {
		return 0, err
	}
	fallback := plainTextFallback(text)
	if strings.TrimSpace(fallback) == "" || fallback == text {
		return 0, err
	}
	a.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("zulip rejected markdown content, retrying as plain text")
	msgID, fallbackErr := a.send(ctx, locator, fallback)
	if fallbackErr != nil {
		return 0, fmt.Errorf("send zulip plain text fallback after content rejection: %w", fallbackErr)
	}
	return msgID, nil
}

func plainTextFallback(text string) string {
	fallback := markdownImagePattern.ReplaceAllString(text, "$1: $2")
	fallback = markdownLinkPattern.ReplaceAllString(fallback, "$1 ($2)")
	fallback = strings.ReplaceAll(fallback, "**", "")
	fallback = strings.ReplaceAll(fallback, "__", "")
	fallback = strings.ReplaceAll(fallback, "`", "")
	return strings.TrimSpace(fallback)
}
