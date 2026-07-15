package tgbotkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/metalagman/appkit/lifecycle"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime"
	"github.com/tgbotkit/runtime/eventemitter"
	"github.com/tgbotkit/runtime/handlers"
	"github.com/tgbotkit/runtime/logger"
	"github.com/tgbotkit/runtime/messagetype"
	"github.com/tgbotkit/runtime/updatepoller"
	"github.com/tgbotkit/runtime/updatepoller/offsetstore"
	"github.com/tgbotkit/runtime/webhook"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_tgbotkit",
	fx.Provide(
		NewUpdateSource,
		NewBot,
		NewClient,
	),
	fx.Invoke(RegisterHandlers),
	fx.Invoke(func(*runtime.Bot) {
		// Placeholder to ensure bot is created
	}),
)

const (
	defaultWebhookListenAddr = "0.0.0.0:8080"
	defaultWebhookPath       = "/telegram/webhook"

	telegramHTTPClientTimeout = 90 * time.Second
	webhookReadHeaderTimeout  = 5 * time.Second
	webhookReadTimeout        = 10 * time.Second
	webhookWriteTimeout       = 30 * time.Second
	webhookIdleTimeout        = 60 * time.Second
)

var telegramAllowedUpdates = []string{"message", "callback_query"}

// NewClient creates a new Telegram API client.
func NewClient(cfg Config) (client.ClientWithResponsesInterface, error) {
	serverURL, err := client.NewServerUrlTelegramBotAPIEndpointSubstituteBotTokenWithYourBotToken(
		client.ServerUrlTelegramBotAPIEndpointSubstituteBotTokenWithYourBotTokenBotTokenVariable(cfg.Token),
	)
	if err != nil {
		return nil, err
	}

	return client.NewClientWithResponses(serverURL, client.WithHTTPClient(&http.Client{
		Timeout: telegramHTTPClientTimeout,
	}))
}

// NewBot creates a new Telegram bot runtime.
func NewBot(
	cfg Config,
	client client.ClientWithResponsesInterface,
	updateSource runtime.UpdateSource,
	l zerolog.Logger,
) (*runtime.Bot, error) {
	opts := []runtime.OptOptionsSetter{
		runtime.WithUpdateSource(updateSource),
		runtime.WithClient(client),
		runtime.WithLogger(logger.NewZerolog(l)),
	}
	if strings.TrimSpace(cfg.Token) == "" {
		// Skip GetMe API call when Telegram is not configured.
		opts = append(opts, runtime.WithDefaultListenersEnabled(false))
	}
	bot, err := runtime.New(runtime.NewOptions(cfg.Token, opts...))
	if err != nil {
		return nil, err
	}

	return bot, nil
}

// NewUpdateSource creates a new update source (webhook or polling).
func NewUpdateSource(
	cfg Config,
	client client.ClientWithResponsesInterface,
	persistedOffsetStore updatepoller.OffsetStore,
	l zerolog.Logger,
) (runtime.UpdateSource, error) {
	if cfg.Webhook.Enabled {
		if strings.TrimSpace(cfg.Webhook.URL) == "" {
			return nil, fmt.Errorf("balda.telegram.webhook.enabled=true requires balda.telegram.webhook.url")
		}
		if strings.TrimSpace(cfg.Webhook.AuthToken) == "" {
			return nil, fmt.Errorf("balda.telegram.webhook.enabled=true requires balda.telegram.webhook.auth_token")
		}

		wh, err := webhook.New(
			webhook.NewOptions(
				webhook.WithToken(strings.TrimSpace(cfg.Webhook.AuthToken)),
				webhook.WithUrl(strings.TrimSpace(cfg.Webhook.URL)),
				webhook.WithClient(client),
				webhook.WithAllowedUpdates(telegramAllowedUpdates),
			),
		)
		if err != nil {
			return nil, err
		}

		listenAddr := strings.TrimSpace(cfg.Webhook.ListenAddr)
		if listenAddr == "" {
			listenAddr = defaultWebhookListenAddr
		}
		path := strings.TrimSpace(cfg.Webhook.Path)
		if path == "" {
			path = defaultWebhookPath
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		return &webhookUpdateSource{
			webhookSource: wh,
			listenAddr:    listenAddr,
			path:          path,
			logger:        l,
			secretEnabled: strings.TrimSpace(cfg.Webhook.AuthToken) != "",
		}, nil
	}
	offsetStore := persistedOffsetStore
	if offsetStore == nil {
		offsetStore = offsetstore.NewInMemoryOffsetStore(0)
	}
	opts := updatepoller.NewOptions(
		client,
		updatepoller.WithOffsetStore(offsetStore),
		updatepoller.WithLogger(logger.NewZerolog(l)),
		updatepoller.WithAllowedUpdates(telegramAllowedUpdates),
	)
	return updatepoller.NewPoller(opts)
}

type webhookUpdateSource struct {
	webhookSource *webhook.Webhook
	listenAddr    string
	path          string
	logger        zerolog.Logger

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	started  bool

	secretEnabled bool
}

func (s *webhookUpdateSource) UpdateChan() <-chan client.Update {
	return s.webhookSource.UpdateChan()
}

func (s *webhookUpdateSource) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if err := s.webhookSource.Start(ctx); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("listen webhook endpoint on %q: %w", s.listenAddr, err)
	}

	mux := http.NewServeMux()
	mux.Handle(s.path, s.webhookSource)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: webhookReadHeaderTimeout,
		ReadTimeout:       webhookReadTimeout,
		WriteTimeout:      webhookWriteTimeout,
		IdleTimeout:       webhookIdleTimeout,
	}

	s.mu.Lock()
	s.listener = listener
	s.server = server
	s.started = true
	s.mu.Unlock()

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.logger.Error().Err(serveErr).Msg("balda webhook endpoint serve failed")
		}
	}()

	s.logger.Info().
		Str("listen_addr", listener.Addr().String()).
		Str("path", s.path).
		Bool("token_protection", s.secretEnabled).
		Msg("balda webhook endpoint started")

	return nil
}

func (s *webhookUpdateSource) Stop(ctx context.Context) error {
	s.mu.Lock()
	server := s.server
	s.server = nil
	s.listener = nil
	s.started = false
	s.mu.Unlock()

	if server != nil {
		if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("shutdown webhook endpoint: %w", err)
		}
	}

	return s.webhookSource.Stop(ctx)
}

// Registry is the subset of the tgbotkit handler registry Balda uses.
type Registry interface {
	OnCommand(handler handlers.CommandHandler) eventemitter.UnsubscribeFunc
	OnMessage(handler handlers.MessageHandler) eventemitter.UnsubscribeFunc
	OnMessageType(t messagetype.MessageType, handler handlers.MessageHandler) eventemitter.UnsubscribeFunc
	OnCallbackDataPrefix(prefix string, handler handlers.CallbackQueryHandler) eventemitter.UnsubscribeFunc
}

// Handler is a local interface for bot handlers.
type Handler interface {
	Register(registry Registry)
}

type handlerParams struct {
	fx.In

	Bot      *runtime.Bot
	Handlers []Handler `group:"bot_handlers"`
}

// RegisterHandlers registers all bot handlers.
func RegisterHandlers(params handlerParams) {
	for _, handler := range params.Handlers {
		handler.Register(params.Bot.Handlers())
	}
}

// lifecycleCheck ensures UpdateSource implements lifecycle.Lifecycle.
var _ lifecycle.Lifecycle = (runtime.UpdateSource)(nil)
