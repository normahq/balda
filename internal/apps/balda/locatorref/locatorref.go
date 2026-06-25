package locatorref

import (
	"fmt"
	"strings"

	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

// Format returns the public locator reference form used in config.
func Format(locator baldasession.SessionLocator) string {
	channelType := strings.TrimSpace(locator.ChannelType)
	addressKey := strings.TrimSpace(locator.AddressKey)
	if channelType == "" || addressKey == "" {
		return ""
	}
	return channelType + ":" + addressKey
}

// Parse reconstructs a canonical session locator from a public locator ref.
func Parse(ref string) (baldasession.SessionLocator, error) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return baldasession.SessionLocator{}, fmt.Errorf("locator ref is required")
	}

	channelType, rawAddressKey, ok := strings.Cut(trimmed, ":")
	if !ok {
		return baldasession.SessionLocator{}, fmt.Errorf("locator ref %q must be <channel_type>:<address_key>", ref)
	}

	channelType = strings.ToLower(strings.TrimSpace(channelType))
	addressKey := strings.TrimSpace(rawAddressKey)
	if channelType == "" {
		return baldasession.SessionLocator{}, fmt.Errorf("locator ref channel_type is required")
	}
	if addressKey == "" {
		return baldasession.SessionLocator{}, fmt.Errorf("locator ref address_key is required")
	}

	switch channelType {
	case baldastate.ChannelTypeTelegram:
		locator, err := baldatelegram.LocatorFromAddressKey(addressKey)
		if err != nil {
			return baldasession.SessionLocator{}, err
		}
		return locator, nil
	case baldastate.ChannelTypeZulip:
		locator, err := baldazulip.LocatorFromAddressKey(addressKey)
		if err != nil {
			return baldasession.SessionLocator{}, err
		}
		return locator, nil
	case baldastate.ChannelTypeSlack:
		locator, err := baldaslack.LocatorFromAddressKey(addressKey)
		if err != nil {
			return baldasession.SessionLocator{}, err
		}
		return locator, nil
	default:
		return baldasession.SessionLocator{}, fmt.Errorf("unsupported locator transport %q", channelType)
	}
}
