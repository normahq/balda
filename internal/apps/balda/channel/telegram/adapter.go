package telegram

import (
	"context"
	"fmt"
	"strconv"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
	"go.uber.org/fx"
)

const chatTypePrivate = "private"

// Adapter maps Telegram runtime events and operations to balda session locators.
type Adapter struct {
	messenger *messenger.Messenger
	tgClient  client.ClientWithResponsesInterface
	logger    zerolog.Logger
}

// MessageContext is the balda-facing Telegram message shape.
type MessageContext struct {
	Locator        baldasession.SessionLocator
	ChatID         int64
	TopicID        int
	MessageID      int
	UserID         int64
	Entities       []client.MessageEntity
	IsReply        bool
	ReplyToUserID  int64
	ReplyToIsBot   bool
	ReplyContent   string
	Text           string
	HasCommand     bool
	ProgressPolicy baldachannel.ProgressPolicy
	IsDM           bool
}

// CommandContext is the balda-facing Telegram command shape.
type CommandContext struct {
	Locator baldasession.SessionLocator
	ChatID  int64
	TopicID int
	UserID  int64
	Command string
	Args    string
	IsDM    bool
}

// TopicLifecycleContext is the balda-facing Telegram topic lifecycle shape.
type TopicLifecycleContext struct {
	Locator   baldasession.SessionLocator
	ChatID    int64
	TopicID   int
	MessageID int
	UserID    int64
	Type      messagetype.MessageType
}

type AdapterParams struct {
	fx.In

	Messenger *messenger.Messenger
	TGClient  client.ClientWithResponsesInterface
	Logger    zerolog.Logger
}

// NewAdapter creates the Telegram balda adapter.
func NewAdapter(params AdapterParams) *Adapter {
	return &Adapter{
		messenger: params.Messenger,
		tgClient:  params.TGClient,
		logger:    params.Logger.With().Str("component", "balda.channel.telegram").Logger(),
	}
}

// MessageContextFromEvent converts a Telegram message event into balda channel context.
func (a *Adapter) MessageContextFromEvent(event *events.MessageEvent) (MessageContext, bool) {
	if event == nil || event.Message == nil || event.Message.From == nil {
		return MessageContext{}, false
	}

	topicID := a.topicIDFromMessage(event.Message)

	text := ""
	if event.Message.Text != nil {
		text = *event.Message.Text
	}
	var entities []client.MessageEntity
	if event.Message.Entities != nil {
		entities = append(entities, (*event.Message.Entities)...)
	}
	isReply := event.Message.ReplyToMessage != nil
	replyToUserID := int64(0)
	replyToIsBot := false
	replyContent := ""
	if event.Message.ReplyToMessage != nil && event.Message.ReplyToMessage.From != nil {
		replyToUserID = event.Message.ReplyToMessage.From.Id
		replyToIsBot = event.Message.ReplyToMessage.From.IsBot
	}
	if event.Message.ReplyToMessage != nil {
		if event.Message.ReplyToMessage.Text != nil {
			replyContent = *event.Message.ReplyToMessage.Text
		} else if event.Message.ReplyToMessage.Caption != nil {
			replyContent = *event.Message.ReplyToMessage.Caption
		}
	}

	hasCommand := false
	if event.Message.Entities != nil {
		for _, entity := range *event.Message.Entities {
			if entity.Type == "bot_command" {
				hasCommand = true
				break
			}
		}
	}

	return MessageContext{
		Locator:       NewLocator(event.Message.Chat.Id, topicID),
		ChatID:        event.Message.Chat.Id,
		TopicID:       topicID,
		MessageID:     event.Message.MessageId,
		UserID:        event.Message.From.Id,
		Entities:      entities,
		IsReply:       isReply,
		ReplyToUserID: replyToUserID,
		ReplyToIsBot:  replyToIsBot,
		ReplyContent:  replyContent,
		Text:          text,
		HasCommand:    hasCommand,
		ProgressPolicy: baldachannel.ProgressPolicy{
			Typing:   true,
			Thinking: event.Message.Chat.Type == chatTypePrivate,
		},
		IsDM: event.Message.Chat.Type == chatTypePrivate,
	}, true
}

// CommandContextFromEvent converts a Telegram command event into balda channel context.
func (a *Adapter) CommandContextFromEvent(event *events.CommandEvent) (CommandContext, bool) {
	if event == nil || event.Message == nil || event.Message.From == nil {
		return CommandContext{}, false
	}

	topicID := a.topicIDFromMessage(event.Message)

	return CommandContext{
		Locator: NewLocator(event.Message.Chat.Id, topicID),
		ChatID:  event.Message.Chat.Id,
		TopicID: topicID,
		UserID:  event.Message.From.Id,
		Command: event.Command,
		Args:    event.Args,
		IsDM:    event.Message.Chat.Type == chatTypePrivate,
	}, true
}

// TopicLifecycleFromEvent converts a Telegram topic lifecycle event into balda channel context.
func (a *Adapter) TopicLifecycleFromEvent(event *events.MessageEvent) (TopicLifecycleContext, bool) {
	if event == nil || event.Message == nil || event.Message.MessageThreadId == nil {
		return TopicLifecycleContext{}, false
	}
	if !isTopicMessage(event.Message) {
		a.logger.Debug().
			Str("chat_type", event.Message.Chat.Type).
			Int("message_thread_id", *event.Message.MessageThreadId).
			Msg("ignoring topic lifecycle event for non-topic message")
		return TopicLifecycleContext{}, false
	}

	topicID := *event.Message.MessageThreadId
	userID := int64(0)
	if event.Message.From != nil {
		userID = event.Message.From.Id
	}

	return TopicLifecycleContext{
		Locator:   NewLocator(event.Message.Chat.Id, topicID),
		ChatID:    event.Message.Chat.Id,
		TopicID:   topicID,
		MessageID: event.Message.MessageId,
		UserID:    userID,
		Type:      event.Type,
	}, true
}

// SendPlain sends a plain text reply to the locator.
func (a *Adapter) SendPlain(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendPlain(ctx, chatID, text, topicID)
}

// SendMarkdown sends a Markdown reply to the locator.
func (a *Adapter) SendMarkdown(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendMarkdown(ctx, chatID, text, topicID)
}

// SendAgentReply sends final agent output for the locator using configured formatting mode.
func (a *Adapter) SendAgentReply(ctx context.Context, locator baldasession.SessionLocator, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendAgentReply(ctx, chatID, text, topicID)
}

// SendAgentReplyWithProviderMessageID sends final agent output and returns the provider message ID when available.
func (a *Adapter) SendAgentReplyWithProviderMessageID(ctx context.Context, locator baldasession.SessionLocator, text string) (string, error) {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return "", err
	}
	result, err := a.messenger.SendAgentReplyWithResult(ctx, chatID, text, topicID)
	if err != nil {
		return "", err
	}
	if result.LastMessageID <= 0 {
		return "", nil
	}
	return strconv.Itoa(result.LastMessageID), nil
}

// SendDraftPlain updates a draft message for the locator.
func (a *Adapter) SendDraftPlain(ctx context.Context, locator baldasession.SessionLocator, draftID int, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendDraftPlain(ctx, chatID, draftID, text, topicID)
}

// SendTyping sends a typing chat action to the locator chat/topic.
func (a *Adapter) SendTyping(ctx context.Context, locator baldasession.SessionLocator) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendChatAction(ctx, chatID, topicID, "typing")
}

// CreateTopicLocator creates a Telegram forum topic and returns the balda locator for it.
func (a *Adapter) CreateTopicLocator(ctx context.Context, chatID int64, topicName string) (baldasession.SessionLocator, error) {
	createTopicResp, err := a.tgClient.CreateForumTopicWithResponse(ctx, client.CreateForumTopicJSONRequestBody{
		ChatId: chatID,
		Name:   topicName,
	})
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf("creating forum topic: %w", err)
	}
	if createTopicResp.JSON200 == nil {
		return baldasession.SessionLocator{}, fmt.Errorf("failed to create forum topic: %s", createTopicResp.Status())
	}

	return NewLocator(chatID, createTopicResp.JSON200.Result.MessageThreadId), nil
}

// Close removes a Telegram forum topic for the locator. Root locators are ignored.
func (a *Adapter) Close(ctx context.Context, locator baldasession.SessionLocator) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	if topicID == 0 {
		return nil
	}

	closeResp, err := a.tgClient.DeleteForumTopicWithResponse(ctx, client.DeleteForumTopicJSONRequestBody{
		ChatId:          chatID,
		MessageThreadId: topicID,
	})
	if err != nil {
		return fmt.Errorf("removing forum topic: %w", err)
	}
	if closeResp.JSON200 == nil {
		return fmt.Errorf("failed to remove forum topic: %s", closeResp.Status())
	}
	return nil
}

func telegramTuple(locator baldasession.SessionLocator) (int64, int, error) {
	address, ok, err := DecodeLocator(locator)
	if err != nil {
		return 0, 0, fmt.Errorf("decode telegram locator %q: %w", locator.SessionID, err)
	}
	if !ok {
		return 0, 0, fmt.Errorf("unsupported channel type %q", locator.ChannelType)
	}
	return address.ChatID, address.TopicID, nil
}

func (a *Adapter) topicIDFromMessage(msg *client.Message) int {
	if msg == nil || msg.MessageThreadId == nil {
		return 0
	}
	if msg.Chat.Type != chatTypePrivate {
		// In public chats, message_thread_id is the routing key for balda task
		// threads even when is_topic_message is omitted or false.
		return *msg.MessageThreadId
	}
	if !isTopicMessage(msg) {
		a.logger.Debug().
			Str("chat_type", msg.Chat.Type).
			Int("message_thread_id", *msg.MessageThreadId).
			Msg("ignoring message_thread_id for non-topic message")
		return 0
	}
	return *msg.MessageThreadId
}

func isTopicMessage(msg *client.Message) bool {
	if msg == nil || msg.MessageThreadId == nil {
		return false
	}
	if msg.IsTopicMessage != nil {
		return *msg.IsTopicMessage
	}
	// Fallback for payloads that omit is_topic_message: if Telegram sent a
	// message_thread_id, treat it as a topic/thread-scoped message.
	return true
}
