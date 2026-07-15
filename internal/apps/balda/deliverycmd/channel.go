package deliverycmd

import "context"

// ChannelType identifies the transport backing a delivery locator.
type ChannelType string

const (
	ChannelTypeTelegram   ChannelType = "telegram"
	ChannelTypeZulip      ChannelType = "zulip"
	ChannelTypeSlackChat  ChannelType = "slack"
	ChannelTypeSlackAgent ChannelType = "slack_agent"
)

type OperationKind string

const (
	OperationPlain                 OperationKind = "plain"
	OperationMarkdown              OperationKind = "markdown"
	OperationAgentReply            OperationKind = "agent_reply"
	OperationDraft                 OperationKind = "draft"
	OperationTyping                OperationKind = "typing"
	OperationProgress              OperationKind = "progress"
	OperationClearQuestionControls OperationKind = "clear_question_controls"
)

// Operation describes one transport-neutral delivery side effect.
type Operation struct {
	Kind      OperationKind
	Profile   Profile
	Text      string
	DraftID   int
	Progress  Progress
	Question  *Question
	MessageID string
}

// Result contains transport metadata returned by a delivery.
type Result struct {
	ProviderMessageID string
}

// Adapter executes one semantic delivery operation.
type Adapter interface {
	Deliver(ctx context.Context, locator Locator, operation Operation) (Result, error)
}
