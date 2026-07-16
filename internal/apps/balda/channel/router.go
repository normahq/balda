package channel

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
)

// Router routes outbound channel operations to the correct ChannelAdapter
// based on the locator's ChannelType.
type Router struct {
	adapters map[string]deliverycmd.Adapter
}

func NewRouter(adapters map[string]deliverycmd.Adapter) *Router { return &Router{adapters: adapters} }

func (r *Router) adapterFor(locator deliverycmd.Locator) (deliverycmd.Adapter, error) {
	adapter, ok := r.adapters[locator.ChannelType]
	if !ok {
		return nil, fmt.Errorf("no channel adapter for channel type %q", locator.ChannelType)
	}
	return adapter, nil
}

func (r *Router) SendPlain(ctx context.Context, locator deliverycmd.Locator, text string) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationPlain, Text: text})
	return err
}

func (r *Router) SendMarkdown(ctx context.Context, locator deliverycmd.Locator, text string) error {
	return r.SendMarkdownWithProfile(ctx, locator, deliverycmd.Profile{}, text)
}

func (r *Router) SendMarkdownWithProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationMarkdown, Profile: profile, Text: text})
	return err
}

func (r *Router) SendAgentReply(ctx context.Context, locator deliverycmd.Locator, text string) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationAgentReply, Text: text})
	return err
}

func (r *Router) SendAgentReplyWithProviderMessageID(ctx context.Context, locator deliverycmd.Locator, text string) (string, error) {
	return r.SendAgentReplyWithProviderMessageIDAndProfile(ctx, locator, deliverycmd.Profile{}, text)
}

func (r *Router) SendAgentReplyWithProviderMessageIDAndProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) (string, error) {
	return r.SendAgentReplyWithQuestion(ctx, locator, profile, text, nil)
}

func (r *Router) SendAgentReplyWithQuestion(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string, question *deliverycmd.Question) (string, error) {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return "", err
	}
	result, err := adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationAgentReply, Profile: profile, Text: text, Question: question})
	return result.ProviderMessageID, err
}

func (r *Router) ClearQuestionControls(ctx context.Context, locator deliverycmd.Locator, messageID, handle string) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationClearQuestionControls, MessageID: messageID, Handle: handle})
	return err
}

func (r *Router) SendDraftPlain(ctx context.Context, locator deliverycmd.Locator, draftID int, text string) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationDraft, DraftID: draftID, Text: text})
	return err
}

func (r *Router) SendTyping(ctx context.Context, locator deliverycmd.Locator) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationTyping})
	return err
}

func (r *Router) SendProgress(ctx context.Context, locator deliverycmd.Locator, progress deliverycmd.Progress) error {
	adapter, err := r.adapterFor(locator)
	if err != nil {
		return err
	}
	_, err = adapter.Deliver(ctx, locator, deliverycmd.Operation{Kind: deliverycmd.OperationProgress, Progress: progress})
	return err
}
