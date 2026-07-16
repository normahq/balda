package automodecmd

import (
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

type Payload struct {
	Locator baldasession.SessionLocator `json:"locator"`
	State   map[string]any              `json:"state,omitempty"`
}

func Envelope(payload Payload) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("session id is required")
	}
	raw, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	return actorlayer.Envelope{
		Namespace: actorcmd.NamespaceAutoModeCommand,
		To: actorlayer.ActorAddress{
			Target: actorcmd.ActorTypeSession,
			Key:    payload.Locator.SessionID,
		},
		Payload: raw,
	}, nil
}
