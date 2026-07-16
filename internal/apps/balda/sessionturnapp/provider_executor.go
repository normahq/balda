package sessionturnapp

import (
	"context"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/sessionturn"
	"github.com/rs/zerolog"
)

type ProviderTurnExecutor struct {
	execution  *TurnExecutionService
	dispatcher actortransport.Dispatcher
	jobEvents  jobEventAppender
	logger     zerolog.Logger
}

func NewProviderTurnExecutorFromService(execution *TurnExecutionService) *ProviderTurnExecutor {
	if execution == nil {
		return &ProviderTurnExecutor{}
	}
	return &ProviderTurnExecutor{
		execution:  execution,
		dispatcher: execution.dispatcher,
		jobEvents:  execution.jobEvents,
		logger:     execution.logger,
	}
}

func NewProviderTurnExecutor(dispatcher actortransport.Dispatcher, jobEvents jobEventAppender, logger zerolog.Logger) *ProviderTurnExecutor {
	return &ProviderTurnExecutor{
		dispatcher: dispatcher,
		jobEvents:  jobEvents,
		logger:     logger,
	}
}

func (e *ProviderTurnExecutor) ExecuteSessionTurn(ctx context.Context, request sessionturn.Request) error {
	execution := e.execution
	if execution == nil {
		execution = &TurnExecutionService{
			dispatcher: e.dispatcher,
			jobEvents:  e.jobEvents,
			logger:     e.logger,
		}
	}
	payload := request.Payload
	from := actorlayer.ActorAddress{Target: actorcmd.ActorTypeSession, Key: request.Session.GetSessionID()}
	progressEmitter := NewSessionProgressDispatcher(
		execution.dispatcher,
		from,
		session.SessionLocator{
			SessionID:   request.DeliveryLocator.SessionID,
			ChannelType: request.DeliveryLocator.ChannelType,
			AddressKey:  request.DeliveryLocator.AddressKey,
			AddressJSON: request.DeliveryLocator.AddressJSON,
		},
		payload.JobID,
		payload.TopicID,
		request.DeliveryOptions.ProgressPolicy,
		strings.TrimSpace(payload.JobID) != "",
		execution.logger,
	)
	return execution.Execute(ctx, ExecutionRequest{
		Text:            payload.Text,
		Runner:          request.Session.GetRunner(),
		UserID:          request.UserID,
		RequesterUserID: payload.RequesterUserID,
		SessionID:       request.Session.GetSessionID(),
		JobID:           payload.JobID,
		AgentSessionID:  request.AgentSessionID,
		Locator: session.SessionLocator{
			SessionID:   request.DeliveryLocator.SessionID,
			ChannelType: request.DeliveryLocator.ChannelType,
			AddressKey:  request.DeliveryLocator.AddressKey,
			AddressJSON: request.DeliveryLocator.AddressJSON,
		},
		MessageID:       payload.MessageID,
		DeliveryOptions: request.DeliveryOptions,
		Deliver:         payload.Deliver,
		ProgressEmitter: progressEmitter,
		OutboundFrom:    from,
		RunOptions:      request.MemoryRunOptions,
		TurnSource:      payload.Source,
	})
}

var _ sessionturn.Executor = (*ProviderTurnExecutor)(nil)
