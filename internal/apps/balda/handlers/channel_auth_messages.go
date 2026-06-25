package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/auth"
)

const ownerAlreadyRegisteredMessage = "You are already registered as the bot owner."

func firstFieldToken(text string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) != 1 {
		return "", false
	}
	token := strings.TrimSpace(fields[0])
	return token, auth.LooksLikeChannelToken(token)
}

func ownerBindTokenBundleMessage(ctx context.Context, authService *auth.ChannelAuthService, createdBy string) (string, bool) {
	if authService == nil {
		return "", false
	}
	tokens, err := authService.CreateMissingOwnerBindTokens(ctx, createdBy)
	if err != nil || len(tokens) == 0 {
		return "", false
	}
	lines := []string{"Connect your other Balda channels:"}
	for _, token := range tokens {
		switch token.Channel {
		case auth.ChannelTelegram:
			lines = append(lines, "", "Telegram:", fmt.Sprintf("DM Balda this command: /start %s", token.Token))
		case auth.ChannelSlack:
			lines = append(lines, "", "Slack:", "DM Balda this token:", token.Token)
		case auth.ChannelZulip:
			lines = append(lines, "", "Zulip:", "DM Balda this token:", token.Token)
		}
	}
	return strings.Join(lines, "\n"), true
}
