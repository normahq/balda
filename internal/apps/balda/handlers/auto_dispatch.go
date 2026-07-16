package handlers

import (
	"context"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/automodecmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

func dispatchAutoStateUpdate(
	ctx context.Context,
	dispatcher actortransport.Dispatcher,
	locator baldasession.SessionLocator,
	state map[string]any,
) error {
	if dispatcher == nil || len(state) == 0 {
		return nil
	}
	env, err := automodecmd.Envelope(automodecmd.Payload{
		Locator: locator,
		State:   state,
	})
	if err != nil {
		return err
	}
	_, err = dispatcher.Dispatch(ctx, env)
	return err
}
