package telegram

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

const (
	telegramSessionIDPrefix = "tg"
	// ChannelType is the channel type string for the Telegram transport.
	ChannelType = "telegram"
)

// LocatorAddress is the Telegram-specific transport address payload.
type LocatorAddress struct {
	ChatID  int64 `json:"chat_id"`
	TopicID int   `json:"topic_id"`
}

// NewLocator builds a canonical session locator for Telegram transport.
func NewLocator(chatID int64, topicID int) baldasession.SessionLocator {
	address := LocatorAddress{ChatID: chatID, TopicID: topicID}
	raw, _ := json.Marshal(address)
	channelType := baldastate.ChannelTypeTelegram
	addressKey := fmt.Sprintf("%d:%d", chatID, topicID)
	addressJSON := string(raw)
	sessionID := fmt.Sprintf("%s-%d-%d", telegramSessionIDPrefix, chatID, topicID)

	locator, err := baldasession.NewSessionLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		// Generated values are deterministic and should always validate.
		return baldasession.SessionLocator{
			ChannelType: channelType,
			AddressKey:  addressKey,
			AddressJSON: addressJSON,
			SessionID:   sessionID,
		}
	}
	return locator
}

// LocatorFromAddressKey rebuilds a canonical Telegram locator from "<chat_id>:<topic_id>".
func LocatorFromAddressKey(addressKey string) (baldasession.SessionLocator, error) {
	trimmed := strings.TrimSpace(addressKey)
	chatPart, topicPart, ok := strings.Cut(trimmed, ":")
	if !ok {
		return baldasession.SessionLocator{}, fmt.Errorf("telegram address key %q must be <chat_id>:<topic_id>", addressKey)
	}
	chatID, err := strconv.ParseInt(strings.TrimSpace(chatPart), 10, 64)
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf("parse telegram chat_id from %q: %w", addressKey, err)
	}
	topicID, err := strconv.Atoi(strings.TrimSpace(topicPart))
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf("parse telegram topic_id from %q: %w", addressKey, err)
	}
	return NewLocator(chatID, topicID), nil
}

// DecodeLocator decodes a Telegram locator payload from canonical session locator fields.
func DecodeLocator(locator baldasession.SessionLocator) (LocatorAddress, bool, error) {
	if strings.TrimSpace(locator.ChannelType) != baldastate.ChannelTypeTelegram {
		return LocatorAddress{}, false, nil
	}

	var address LocatorAddress
	if err := json.Unmarshal([]byte(locator.AddressJSON), &address); err != nil {
		return LocatorAddress{}, true, fmt.Errorf("decode telegram address: %w", err)
	}
	return address, true, nil
}

// UserID returns a Telegram transport user identifier.
func UserID(userID int64) string {
	return fmt.Sprintf("%s-%d", telegramSessionIDPrefix, userID)
}
