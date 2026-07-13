package deliverycmd

import "context"

// ChannelType identifies the transport backing a delivery locator.
type ChannelType string

const (
	ChannelTypeTelegram ChannelType = "telegram"
	ChannelTypeZulip    ChannelType = "zulip"
	ChannelTypeSlack    ChannelType = "slack"
)

type OperationKind string

const (
	OperationPlain      OperationKind = "plain"
	OperationMarkdown   OperationKind = "markdown"
	OperationAgentReply OperationKind = "agent_reply"
	OperationDraft      OperationKind = "draft"
	OperationTyping     OperationKind = "typing"
	OperationProgress   OperationKind = "progress"
)

// Operation describes one transport-neutral delivery side effect.
type Operation struct {
	Kind     OperationKind
	Profile  Profile
	Text     string
	DraftID  int
	Progress Progress
}

// Result contains transport metadata returned by a delivery.
type Result struct {
	ProviderMessageID string
}

// Adapter executes one semantic delivery operation.
type Adapter interface {
	Deliver(ctx context.Context, locator Locator, operation Operation) (Result, error)
}
