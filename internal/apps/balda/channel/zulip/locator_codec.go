package zulip

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

const (
	zulipSessionIDPrefix = "zu"
	// ChannelType is the channel type string for the Zulip transport.
	ChannelType = "zulip"
)

// LocatorAddress is the Zulip-specific transport address payload.
type LocatorAddress struct {
	Type     string `json:"type"`
	StreamID int    `json:"stream_id,omitempty"`
	Topic    string `json:"topic,omitempty"`
	UserID   int    `json:"user_id,omitempty"`
}

// NewStreamLocator builds a canonical session locator for a Zulip stream/topic.
func NewStreamLocator(streamID int, topic string) baldasession.SessionLocator {
	address := LocatorAddress{
		Type:     "stream",
		StreamID: streamID,
		Topic:    topic,
	}
	raw, _ := json.Marshal(address)
	channelType := baldastate.ChannelTypeZulip
	escapedTopic := url.PathEscape(topic)
	addressKey := fmt.Sprintf("s:%d:%s", streamID, escapedTopic)
	addressJSON := string(raw)

	topicHash := sha256.Sum256([]byte(topic))
	hashStr := fmt.Sprintf("%x", topicHash[:4])
	sessionID := fmt.Sprintf("%s-s-%d-%s", zulipSessionIDPrefix, streamID, hashStr)

	locator, err := baldasession.NewSessionLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		return baldasession.SessionLocator{
			ChannelType: channelType,
			AddressKey:  addressKey,
			AddressJSON: addressJSON,
			SessionID:   sessionID,
		}
	}
	return locator
}

// NewDMLocator builds a canonical session locator for a Zulip direct message.
func NewDMLocator(userID int) baldasession.SessionLocator {
	address := LocatorAddress{
		Type:   "dm",
		UserID: userID,
	}
	raw, _ := json.Marshal(address)
	channelType := baldastate.ChannelTypeZulip
	addressKey := fmt.Sprintf("dm:%d", userID)
	addressJSON := string(raw)
	sessionID := fmt.Sprintf("%s-dm-%d", zulipSessionIDPrefix, userID)

	locator, err := baldasession.NewSessionLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		return baldasession.SessionLocator{
			ChannelType: channelType,
			AddressKey:  addressKey,
			AddressJSON: addressJSON,
			SessionID:   sessionID,
		}
	}
	return locator
}

// DecodeLocator decodes a Zulip locator payload from canonical session locator
// fields. Returns false if the locator is not a Zulip channel type.
func DecodeLocator(locator baldasession.SessionLocator) (LocatorAddress, bool, error) {
	if strings.TrimSpace(locator.ChannelType) != baldastate.ChannelTypeZulip {
		return LocatorAddress{}, false, nil
	}
	var address LocatorAddress
	if err := json.Unmarshal([]byte(locator.AddressJSON), &address); err != nil {
		return LocatorAddress{}, true, fmt.Errorf("decode zulip address: %w", err)
	}
	return address, true, nil
}

// LocatorFromAddressKey rebuilds a canonical Zulip locator from an address key.
// Stream format: "s:<stream_id>:<url-path-escaped topic>"
// DM format: "dm:<user_id>"
func LocatorFromAddressKey(addressKey string) (baldasession.SessionLocator, error) {
	trimmed := strings.TrimSpace(addressKey)
	if strings.HasPrefix(trimmed, "s:") {
		return streamLocatorFromAddressKey(trimmed)
	}
	if strings.HasPrefix(trimmed, "dm:") {
		return dmLocatorFromAddressKey(trimmed)
	}
	return baldasession.SessionLocator{}, fmt.Errorf(
		"zulip address key %q must start with \"s:\" (stream) or \"dm:\" (direct message)",
		addressKey,
	)
}

func streamLocatorFromAddressKey(addressKey string) (baldasession.SessionLocator, error) {
	rest := strings.TrimPrefix(addressKey, "s:")
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return baldasession.SessionLocator{}, fmt.Errorf(
			"zulip stream address key %q must be s:<stream_id>:<topic>",
			addressKey,
		)
	}
	streamIDStr := rest[:colonIdx]
	escapedTopic := rest[colonIdx+1:]
	streamID, err := strconv.Atoi(strings.TrimSpace(streamIDStr))
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf(
			"parse zulip stream_id from %q: %w",
			addressKey, err,
		)
	}
	topic, err := url.PathUnescape(escapedTopic)
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf(
			"unescape zulip topic from %q: %w",
			addressKey, err,
		)
	}
	return NewStreamLocator(streamID, topic), nil
}

func dmLocatorFromAddressKey(addressKey string) (baldasession.SessionLocator, error) {
	rest := strings.TrimPrefix(addressKey, "dm:")
	userID, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil {
		return baldasession.SessionLocator{}, fmt.Errorf(
			"parse zulip user_id from %q: %w",
			addressKey, err,
		)
	}
	return NewDMLocator(userID), nil
}

// UserID returns a Zulip transport user identifier string.
func UserID(userID int) string {
	return fmt.Sprintf("%s-%d", zulipSessionIDPrefix, userID)
}
