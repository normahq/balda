package channel

import (
	"context"
	"fmt"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

// Router routes outbound channel operations to the correct ChannelAdapter
// based on the locator's ChannelType.
type Router struct {
	adapters map[string]ChannelAdapter
}

// NewRouter creates a Router from a map of channel type to adapter.
func NewRouter(adapters map[string]ChannelAdapter) *Router {
	return &Router{adapters: adapters}
}

func (r *Router) adapterFor(locator baldasession.SessionLocator) (ChannelAdapter, error) {
	channelType := locator.ChannelType
	adapter, ok := r.adapters[channelType]
	if !ok {
		return nil, fmt.Errorf("no channel adapter for channel type %q", channelType)
	}
	return adapter, nil
}

// SendPlain sends a plain text message via the appropriate adapter.
func (r *Router) SendPlain(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	return adapter.SendPlain(ctx, locator, text)
}

// SendMarkdown sends a Markdown message via the appropriate adapter.
func (r *Router) SendMarkdown(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	return adapter.SendMarkdown(ctx, locator, text)
}

// SendAgentReply sends an agent reply via the appropriate adapter.
func (r *Router) SendAgentReply(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	return adapter.SendAgentReply(ctx, locator, text)
}

// SendAgentReplyWithProviderMessageID sends an agent reply and returns the
// provider message ID via the appropriate adapter.
func (r *Router) SendAgentReplyWithProviderMessageID(
	ctx context.Context,
	locator baldasession.SessionLocator,
	text string,
) (string, error) {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return "", err
	}
	return adapter.SendAgentReplyWithProviderMessageID(ctx, locator, text)
}

// SendDraftPlain sends a draft plain text message via the appropriate adapter.
func (r *Router) SendDraftPlain(
	ctx context.Context,
	locator baldasession.SessionLocator,
	draftID int,
	text string,
) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	return adapter.SendDraftPlain(ctx, locator, draftID, text)
}

// SendTyping sends a typing indicator via the appropriate adapter.
func (r *Router) SendTyping(
	ctx context.Context,
	locator baldasession.SessionLocator,
) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	return adapter.SendTyping(ctx, locator)
}
