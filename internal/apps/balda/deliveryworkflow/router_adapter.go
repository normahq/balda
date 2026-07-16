package deliveryworkflow

import (
	"context"
	"fmt"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
)

type channelRouterDispatcher struct {
	router *baldachannel.Router
}

var _ Dispatcher = channelRouterDispatcher{}

func NewChannelDispatcher(router *baldachannel.Router) Dispatcher {
	return channelRouterDispatcher{router: router}
}

func (d channelRouterDispatcher) Dispatch(ctx context.Context, payload deliverycmd.Payload) (string, error) {
	if d.router == nil {
		return "", fmt.Errorf("channel router is required")
	}
	switch payload.Mode {
	case deliverycmd.ModeAgentReply:
		return d.router.SendAgentReplyWithQuestion(ctx, payload.Locator, payload.Profile, payload.Text, payload.Question)
	case deliverycmd.ModePlain:
		return "", d.router.SendPlain(ctx, payload.Locator, payload.Text)
	case deliverycmd.ModeMarkdown:
		return "", d.router.SendMarkdownWithProfile(ctx, payload.Locator, payload.Profile, payload.Text)
	case deliverycmd.ModeDraftPlain:
		return "", d.router.SendDraftPlain(ctx, payload.Locator, payload.DraftID, payload.Text)
	case deliverycmd.ModeChatAction:
		return "", d.router.SendTyping(ctx, payload.Locator)
	case deliverycmd.ModeProgress:
		if payload.Progress == nil {
			return "", fmt.Errorf("progress payload is required")
		}
		return "", d.router.SendProgress(ctx, payload.Locator, *payload.Progress)
	case deliverycmd.ModeClearQuestionControls:
		return "", d.router.ClearQuestionControls(ctx, payload.Locator, payload.MessageID, payload.Handle)
	default:
		return "", fmt.Errorf("unsupported delivery mode %q", payload.Mode)
	}
}
