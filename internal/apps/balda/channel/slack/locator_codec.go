package slack

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
)

const (
	slackSessionIDPrefix = "sl"
	// ChannelType is the channel type string for the Slack transport.
	ChannelType = "slack"

	addressTypeDM     = "dm"
	addressTypeThread = "thread"
)

// LocatorAddress is the Slack-specific transport address payload.
type LocatorAddress struct {
	Type     string `json:"type"`
	TeamID   string `json:"team_id"`
	Channel  string `json:"channel"`
	ThreadTS string `json:"thread_ts,omitempty"`
}

// NewDMLocator builds a canonical session locator for a Slack DM channel.
func NewDMLocator(teamID, channel string) deliverycmd.Locator {
	address := LocatorAddress{
		Type:    addressTypeDM,
		TeamID:  strings.TrimSpace(teamID),
		Channel: strings.TrimSpace(channel),
	}
	return newLocator(address, fmt.Sprintf("dm:%s:%s", address.TeamID, address.Channel))
}

// NewThreadLocator builds a canonical session locator for a Slack channel thread.
func NewThreadLocator(teamID, channel, threadTS string) deliverycmd.Locator {
	address := LocatorAddress{
		Type:     addressTypeThread,
		TeamID:   strings.TrimSpace(teamID),
		Channel:  strings.TrimSpace(channel),
		ThreadTS: strings.TrimSpace(threadTS),
	}
	return newLocator(address, fmt.Sprintf("t:%s:%s:%s", address.TeamID, address.Channel, address.ThreadTS))
}

func newLocator(address LocatorAddress, addressKey string) deliverycmd.Locator {
	raw, _ := json.Marshal(address)
	channelType := string(deliverycmd.ChannelTypeSlack)
	addressJSON := string(raw)
	sessionID := slackSessionID(addressKey)
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

func slackSessionID(addressKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(addressKey)))
	return fmt.Sprintf("%s-%x", slackSessionIDPrefix, sum[:8])
}

// DecodeLocator decodes a Slack locator payload from canonical session locator fields.
func DecodeLocator(locator deliverycmd.Locator) (LocatorAddress, bool, error) {
	if strings.TrimSpace(locator.ChannelType) != string(deliverycmd.ChannelTypeSlack) {
		return LocatorAddress{}, false, nil
	}
	var address LocatorAddress
	if err := json.Unmarshal([]byte(locator.AddressJSON), &address); err != nil {
		return LocatorAddress{}, true, fmt.Errorf("decode slack address: %w", err)
	}
	if err := validateLocatorAddress(address); err != nil {
		return LocatorAddress{}, true, err
	}
	return address, true, nil
}

// LocatorFromAddressKey rebuilds a canonical Slack locator from an address key.
// DM format: "dm:<team_id>:<channel_id>"
// Thread format: "t:<team_id>:<channel_id>:<thread_ts>".
func LocatorFromAddressKey(addressKey string) (deliverycmd.Locator, error) {
	parts := strings.Split(strings.TrimSpace(addressKey), ":")
	switch {
	case len(parts) == 3 && parts[0] == "dm":
		if parts[1] == "" || parts[2] == "" {
			return deliverycmd.Locator{}, fmt.Errorf("slack dm address key %q must be dm:<team_id>:<channel_id>", addressKey)
		}
		return NewDMLocator(parts[1], parts[2]), nil
	case len(parts) == 4 && parts[0] == "t":
		if parts[1] == "" || parts[2] == "" || parts[3] == "" {
			return deliverycmd.Locator{}, fmt.Errorf("slack thread address key %q must be t:<team_id>:<channel_id>:<thread_ts>", addressKey)
		}
		return NewThreadLocator(parts[1], parts[2], parts[3]), nil
	default:
		return deliverycmd.Locator{}, fmt.Errorf("slack address key %q must be dm:<team_id>:<channel_id> or t:<team_id>:<channel_id>:<thread_ts>", addressKey)
	}
}

func validateLocatorAddress(address LocatorAddress) error {
	if strings.TrimSpace(address.TeamID) == "" {
		return fmt.Errorf("slack locator requires team_id")
	}
	if strings.TrimSpace(address.Channel) == "" {
		return fmt.Errorf("slack locator requires channel")
	}
	switch strings.TrimSpace(address.Type) {
	case addressTypeDM:
		return nil
	case addressTypeThread:
		if strings.TrimSpace(address.ThreadTS) == "" {
			return fmt.Errorf("slack thread locator requires thread_ts")
		}
		return nil
	default:
		return fmt.Errorf("unsupported slack address type %q", address.Type)
	}
}

// UserID returns a Slack transport user subject.
func UserID(teamID, userID string) string {
	return fmt.Sprintf("slack:%s:%s", strings.TrimSpace(teamID), strings.TrimSpace(userID))
}
