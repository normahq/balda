package slack

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
)

// Adapter implements channel.ChannelAdapter for Slack.
type Adapter struct {
	client *Client
	logger zerolog.Logger
}

// NewAdapter creates a Slack channel adapter.
func NewAdapter(client *Client, logger zerolog.Logger) *Adapter {
	return &Adapter{
		client: client,
		logger: logger.With().Str("component", "balda.channel.slack").Logger(),
	}
}

// SendPlain sends a plain text Slack message.
func (a *Adapter) SendPlain(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	_, err := a.send(ctx, locator, text, false)
	return err
}

// SendMarkdown sends a Slack mrkdwn message.
func (a *Adapter) SendMarkdown(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	return a.SendMarkdownWithProfile(ctx, locator, deliverycmd.Profile{}, text)
}

// SendMarkdownWithProfile sends a Slack message using the requested formatting profile.
func (a *Adapter) SendMarkdownWithProfile(ctx context.Context, locator baldasession.SessionLocator, profile deliverycmd.Profile, text string) error {
	if deliveryfmt.NormalizeProfile(profile).Format == deliveryfmt.FormatHTML {
		return fmt.Errorf("slack delivery does not support html formatting")
	}
	if deliveryfmt.NormalizeProfile(profile).Format == deliveryfmt.FormatPlain {
		return a.SendPlain(ctx, locator, text)
	}
	_, err := a.send(ctx, locator, text, true)
	return err
}

// SendAgentReply sends agent output to Slack.
func (a *Adapter) SendAgentReply(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	_, err := a.SendAgentReplyWithProviderMessageID(ctx, locator, text)
	return err
}

// SendAgentReplyWithProviderMessageID sends agent output and returns the Slack message timestamp.
func (a *Adapter) SendAgentReplyWithProviderMessageID(ctx context.Context, locator baldasession.SessionLocator, text string) (string, error) {
	return a.SendAgentReplyWithProviderMessageIDAndProfile(ctx, locator, deliverycmd.Profile{}, text)
}

// SendAgentReplyWithProviderMessageIDAndProfile sends agent output using Slack mrkdwn unless plain is requested.
func (a *Adapter) SendAgentReplyWithProviderMessageIDAndProfile(ctx context.Context, locator baldasession.SessionLocator, profile deliverycmd.Profile, text string) (string, error) {
	normalized := deliveryfmt.NormalizeProfile(profile)
	if normalized.Format == deliveryfmt.FormatHTML {
		return "", fmt.Errorf("slack delivery does not support html formatting")
	}
	mrkdwn := normalized.Format != deliveryfmt.FormatPlain
	return a.send(ctx, locator, text, mrkdwn)
}

// SendDraftPlain is a no-op for Slack v1.
func (a *Adapter) SendDraftPlain(_ context.Context, _ baldasession.SessionLocator, _ int, _ string) error {
	return nil
}

// SendTyping is a no-op for Slack v1.
func (a *Adapter) SendTyping(_ context.Context, _ baldasession.SessionLocator) error {
	return nil
}

func (a *Adapter) send(ctx context.Context, locator baldasession.SessionLocator, text string, mrkdwn bool) (string, error) {
	if a == nil || a.client == nil {
		return "", fmt.Errorf("slack adapter client is required")
	}
	address, ok, err := DecodeLocator(locator)
	if err != nil {
		return "", fmt.Errorf("decode slack locator: %w", err)
	}
	if !ok {
		return "", fmt.Errorf("unsupported channel type %q for slack", locator.ChannelType)
	}
	threadTS := ""
	if address.Type == addressTypeThread {
		threadTS = address.ThreadTS
	}
	return a.client.PostMessage(ctx, address.Channel, threadTS, text, mrkdwn)
}
