package channel

import (
	"context"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

// ChannelAdapter is the transport-neutral interface for sending messages
// and typing indicators to a balda session.
type ChannelAdapter interface {
	SendPlain(ctx context.Context, locator baldasession.SessionLocator, text string) error
	SendMarkdown(ctx context.Context, locator baldasession.SessionLocator, text string) error
	SendAgentReply(ctx context.Context, locator baldasession.SessionLocator, text string) error
	SendAgentReplyWithProviderMessageID(ctx context.Context, locator baldasession.SessionLocator, text string) (string, error)
	SendDraftPlain(ctx context.Context, locator baldasession.SessionLocator, draftID int, text string) error
	SendTyping(ctx context.Context, locator baldasession.SessionLocator) error
}
