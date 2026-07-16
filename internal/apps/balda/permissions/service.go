// Package permissions implements Balda's generic agent permission review policy.
package permissions

import (
	"context"
	"fmt"
	"strings"
	"time"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
	"github.com/normahq/balda/internal/apps/balda/permissionfmt"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	"github.com/rs/zerolog"
)

const defaultTimeout = 2 * time.Minute

type Config struct {
	Mode    permissioncmd.Mode
	Timeout time.Duration
}

func ParseConfig(mode, timeout string) (Config, error) {
	parsedMode := permissioncmd.Mode(strings.ToLower(strings.TrimSpace(mode)))
	if parsedMode == "" {
		parsedMode = permissioncmd.ModeAllowAll
	}
	switch parsedMode {
	case permissioncmd.ModeAllowAll, permissioncmd.ModeAsk, permissioncmd.ModeDenyAll:
	default:
		return Config{}, fmt.Errorf("permissions mode %q must be allow_all, ask, or deny_all", mode)
	}
	parsedTimeout := defaultTimeout
	if strings.TrimSpace(timeout) != "" {
		var err error
		parsedTimeout, err = time.ParseDuration(strings.TrimSpace(timeout))
		if err != nil {
			return Config{}, fmt.Errorf("parse permissions timeout: %w", err)
		}
		if parsedTimeout <= 0 {
			return Config{}, fmt.Errorf("permissions timeout must be positive")
		}
	}
	return Config{Mode: parsedMode, Timeout: parsedTimeout}, nil
}

type Service struct {
	config     Config
	questions  *questions.Service
	dispatcher actortransport.Dispatcher
	logger     zerolog.Logger
}

func New(config Config, questionService *questions.Service, dispatcher actortransport.Dispatcher, logger zerolog.Logger) *Service {
	serviceLogger := logger.With().Str("component", "balda.permissions").Logger()
	serviceLogger.Info().
		Str("mode", string(config.Mode)).
		Str("timeout", config.Timeout.String()).
		Bool("questions_available", questionService != nil).
		Bool("dispatcher_available", dispatcher != nil).
		Msg("agent permission policy configured")
	return &Service{
		config:     config,
		questions:  questionService,
		dispatcher: dispatcher,
		logger:     serviceLogger,
	}
}

func (s *Service) Review(ctx context.Context, request permissioncmd.Request) (permissioncmd.Decision, error) {
	switch s.config.Mode {
	case permissioncmd.ModeAllowAll:
		return selectDecision(request.Options, true, "allow_all"), nil
	case permissioncmd.ModeDenyAll:
		return selectDecision(request.Options, false, "deny_all"), nil
	case permissioncmd.ModeAsk:
		return s.ask(ctx, request)
	default:
		return selectDecision(request.Options, false, "invalid_mode"), fmt.Errorf("unsupported permission mode %q", s.config.Mode)
	}
}

func (s *Service) ask(ctx context.Context, request permissioncmd.Request) (permissioncmd.Decision, error) {
	fallback := selectDecision(request.Options, false, "fail_closed")
	if s.questions == nil {
		return fallback, fmt.Errorf("interactive permission review is unavailable")
	}
	interaction := request.Interaction
	channel := strings.ToLower(strings.TrimSpace(interaction.Locator.ChannelType))
	if channel != "telegram" && channel != "slack_agent" {
		return fallback, fmt.Errorf("interactive permission review is unsupported for channel %q", channel)
	}
	if strings.TrimSpace(interaction.SessionID) == "" || strings.TrimSpace(interaction.RequestedBy.UserID) == "" {
		return fallback, fmt.Errorf("interactive permission review requires session and requester identity")
	}
	if len(request.Options) == 0 {
		return fallback, fmt.Errorf("permission request has no options")
	}

	options := make([]questions.SessionOption, 0, len(request.Options))
	for _, option := range request.Options {
		id := strings.TrimSpace(option.ID)
		label := strings.TrimSpace(option.Name)
		options = append(options, questions.SessionOption{ID: id, Label: label})
	}
	presentation := permissionfmt.Render(request)
	result, err := s.questions.AskSession(ctx, s.dispatcher, questions.SessionRequest{
		Interaction: interaction,
		Resume:      sessionquestionResumeTarget(interaction),
		Prompt:      presentation.Prompt,
		Options:     options,
		Timeout:     s.config.Timeout,
		Audience:    permissionQuestionAudience(interaction),
		Profile:     presentation.Profile,
	})
	if err != nil {
		return fallback, fmt.Errorf("run permission question: %w", err)
	}
	if result.Canceled || result.TimedOut {
		fallback.Source = firstNonEmpty(result.Source, "timeout")
		return fallback, nil
	}
	if hasOption(request.Options, result.OptionID) {
		return permissioncmd.Decision{OptionID: result.OptionID, Source: firstNonEmpty(result.Source, "user")}, nil
	}
	return fallback, fmt.Errorf("permission response selected unknown option %q", result.OptionID)
}

func (s *Service) Resolve(questionID string, decision permissioncmd.Decision) {
	if s.questions == nil {
		return
	}
	s.questions.ResolveSession(questionID, questions.SessionResult{
		QuestionID: strings.TrimSpace(questionID),
		OptionID:   strings.TrimSpace(decision.OptionID),
		Source:     strings.TrimSpace(decision.Source),
		Canceled:   decision.Canceled,
	})
}

func permissionQuestionAudience(interaction questioncmd.InteractionContext) deliverycmd.QuestionAudience {
	if !strings.EqualFold(strings.TrimSpace(interaction.Locator.ChannelType), string(deliverycmd.ChannelTypeTelegram)) {
		return deliverycmd.QuestionAudience{}
	}
	return deliverycmd.QuestionAudience{
		Visibility: deliverycmd.QuestionVisibilityPrivate,
		UserID:     strings.TrimSpace(interaction.RequestedBy.UserID),
	}
}

func sessionquestionResumeTarget(interaction questioncmd.InteractionContext) questioncmd.ResumeTarget {
	return questioncmd.ResumeTarget{
		To:        actorcmd.ActorTypePermission + ":" + strings.TrimSpace(interaction.SessionID),
		Namespace: actorcmd.NamespacePermissionCommand,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func selectDecision(options []permissioncmd.Option, allow bool, source string) permissioncmd.Decision {
	preferred := []string{"reject_once", "reject_always"}
	if allow {
		preferred = []string{"allow_once", "allow_always"}
	}
	for _, kind := range preferred {
		for _, option := range options {
			if strings.EqualFold(strings.TrimSpace(option.Kind), kind) && strings.TrimSpace(option.ID) != "" {
				return permissioncmd.Decision{OptionID: strings.TrimSpace(option.ID), Source: source}
			}
		}
	}
	return permissioncmd.Decision{Canceled: true, Source: source}
}

func hasOption(options []permissioncmd.Option, optionID string) bool {
	for _, option := range options {
		if strings.TrimSpace(option.ID) == strings.TrimSpace(optionID) && strings.TrimSpace(optionID) != "" {
			return true
		}
	}
	return false
}
