package zulip

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
)

const (
	zulipSessionIDPrefix = "zu"
	// ChannelType is the channel type string for the Zulip transport.
	ChannelType = "zulip"

	addressTypeStream = "stream"
	addressTypeDM     = "dm"
)

// LocatorAddress is the Zulip-specific transport address payload.
type LocatorAddress struct {
	Type     string `json:"type"`
	StreamID int    `json:"stream_id,omitempty"`
	Topic    string `json:"topic"`
	UserID   int    `json:"user_id,omitempty"`
}

// NewStreamLocator builds a canonical session locator for a Zulip stream/topic.
func NewStreamLocator(streamID int, topic string) deliverycmd.Locator {
	address := LocatorAddress{
		Type:     addressTypeStream,
		StreamID: streamID,
		Topic:    topic,
	}
	raw, _ := json.Marshal(address)
	channelType := string(deliverycmd.ChannelTypeZulip)
	escapedTopic := url.PathEscape(topic)
	addressKey := fmt.Sprintf("s:%d:%s", streamID, escapedTopic)
	addressJSON := string(raw)

	topicHash := sha256.Sum256([]byte(topic))
	hashStr := fmt.Sprintf("%x", topicHash[:4])
	sessionID := fmt.Sprintf("%s-s-%d-%s", zulipSessionIDPrefix, streamID, hashStr)

	locator, err := deliverycmd.NewLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		return deliverycmd.Locator{
			ChannelType: channelType,
			AddressKey:  addressKey,
			AddressJSON: addressJSON,
			SessionID:   sessionID,
		}
	}
	return locator
}

// NewDMLocator builds a canonical session locator for a Zulip direct message.
func NewDMLocator(userID int) deliverycmd.Locator {
	address := LocatorAddress{
		Type:   addressTypeDM,
		UserID: userID,
	}
	raw, _ := json.Marshal(address)
	channelType := string(deliverycmd.ChannelTypeZulip)
	addressKey := fmt.Sprintf("dm:%d", userID)
	addressJSON := string(raw)
	sessionID := fmt.Sprintf("%s-dm-%d", zulipSessionIDPrefix, userID)

	locator, err := deliverycmd.NewLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		return deliverycmd.Locator{
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
func DecodeLocator(locator deliverycmd.Locator) (LocatorAddress, bool, error) {
	if strings.TrimSpace(locator.ChannelType) != string(deliverycmd.ChannelTypeZulip) {
		return LocatorAddress{}, false, nil
	}
	var address LocatorAddress
	if err := json.Unmarshal([]byte(locator.AddressJSON), &address); err != nil {
		return LocatorAddress{}, true, fmt.Errorf("decode zulip address: %w", err)
	}
	if err := validateLocatorAddress(address); err != nil {
		return LocatorAddress{}, true, err
	}
	return address, true, nil
}

// LocatorFromAddressKey rebuilds a canonical Zulip locator from an address key.
// Stream format: "s:<stream_id>:<url-path-escaped topic>"
// DM format: "dm:<user_id>".
func LocatorFromAddressKey(addressKey string) (deliverycmd.Locator, error) {
	trimmed := strings.TrimSpace(addressKey)
	if strings.HasPrefix(trimmed, "s:") {
		return streamLocatorFromAddressKey(trimmed)
	}
	if strings.HasPrefix(trimmed, "dm:") {
		return dmLocatorFromAddressKey(trimmed)
	}
	return deliverycmd.Locator{}, fmt.Errorf(
		"zulip address key %q must start with \"s:\" (stream) or \"dm:\" (direct message)",
		addressKey,
	)
}

func streamLocatorFromAddressKey(addressKey string) (deliverycmd.Locator, error) {
	rest := strings.TrimPrefix(addressKey, "s:")
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return deliverycmd.Locator{}, fmt.Errorf(
			"zulip stream address key %q must be s:<stream_id>:<topic>",
			addressKey,
		)
	}
	streamIDStr := rest[:colonIdx]
	escapedTopic := rest[colonIdx+1:]
	streamID, err := strconv.Atoi(strings.TrimSpace(streamIDStr))
	if err != nil {
		return deliverycmd.Locator{}, fmt.Errorf(
			"parse zulip stream_id from %q: %w",
			addressKey, err,
		)
	}
	if streamID <= 0 {
		return deliverycmd.Locator{}, fmt.Errorf("zulip stream_id from %q must be positive", addressKey)
	}
	topic, err := url.PathUnescape(escapedTopic)
	if err != nil {
		return deliverycmd.Locator{}, fmt.Errorf(
			"unescape zulip topic from %q: %w",
			addressKey, err,
		)
	}
	return NewStreamLocator(streamID, topic), nil
}

func dmLocatorFromAddressKey(addressKey string) (deliverycmd.Locator, error) {
	rest := strings.TrimPrefix(addressKey, "dm:")
	userID, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil {
		return deliverycmd.Locator{}, fmt.Errorf(
			"parse zulip user_id from %q: %w",
			addressKey, err,
		)
	}
	if userID <= 0 {
		return deliverycmd.Locator{}, fmt.Errorf("zulip user_id from %q must be positive", addressKey)
	}
	return NewDMLocator(userID), nil
}

func validateLocatorAddress(address LocatorAddress) error {
	switch strings.TrimSpace(address.Type) {
	case addressTypeStream:
		if address.StreamID <= 0 {
			return fmt.Errorf("zulip stream locator requires positive stream_id")
		}
	case addressTypeDM:
		if address.UserID <= 0 {
			return fmt.Errorf("zulip dm locator requires positive user_id")
		}
	default:
		return fmt.Errorf("unsupported zulip address type %q", address.Type)
	}
	return nil
}

// StreamIDFromLocator extracts the stream ID from a Zulip stream locator.
// Returns (0, false) if the locator is not a Zulip stream locator.
func StreamIDFromLocator(locator deliverycmd.Locator) (int, bool) {
	addr, ok, err := DecodeLocator(locator)
	if !ok || err != nil || addr.Type != addressTypeStream {
		return 0, false
	}
	return addr.StreamID, true
}

// UserID returns a Zulip transport user identifier string.
func UserID(userID int) string {
	return fmt.Sprintf("%s-%d", zulipSessionIDPrefix, userID)
}
