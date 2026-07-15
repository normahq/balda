package deliveryfx

import (
	"context"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
)

type questionControlPublisher struct {
	dispatcher actortransport.Dispatcher
}

func (p questionControlPublisher) ClearQuestionControls(ctx context.Context, request questions.ControlClearRequest) error {
	// Controls are currently projected only by Telegram. Keeping this decision
	// in composition avoids leaking channel capabilities into question lifecycle.
	if !strings.EqualFold(strings.TrimSpace(request.Locator.ChannelType), string(deliverycmd.ChannelTypeTelegram)) {
		return nil
	}
	env, err := deliverycmd.ClearQuestionControlsEnvelope(
		actorlayer.SystemAddress("question"),
		request.Locator,
		request.QuestionID,
		request.ProviderMessageID,
	)
	if err != nil {
		return err
	}
	_, err = p.dispatcher.Dispatch(ctx, env)
	return err
}
