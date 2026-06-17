package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/welcome"
	"github.com/rs/zerolog/log"
)

// setOwner binds the handler to the owner. Pass chatID=0 when the chat
// is not yet known (it will be set from the first incoming message).
func (h *BaldaHandler) setOwner(ownerID, chatID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.Info().Int64("owner_id", ownerID).Int64("chat_id", chatID).Msg("Setting owner for balda")

	h.ownerID = ownerID
	if chatID != 0 {
		h.chatID = chatID
	}
}

func (h *BaldaHandler) activateOwner(ctx context.Context, ownerID, chatID int64) error {
	h.setOwner(ownerID, chatID)
	return h.bootstrapOwnerSession(ctx, ownerID, chatID)
}

func (h *BaldaHandler) bootstrapOwnerSession(ctx context.Context, ownerID, chatID int64) error {
	baldaProviderName := h.getProviderName()
	if baldaProviderName == "" {
		return fmt.Errorf("balda provider is not configured")
	}

	locator := baldatelegram.NewLocator(chatID, 0)
	transportUserID := baldatelegram.UserID(ownerID)

	ts, err := h.sessionManager.GetSession(locator)
	if err != nil {
		ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
			Locator: locator,
			UserID:  transportUserID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return fmt.Errorf("restore owner session: %w", err)
			}
			ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
				Locator: locator,
				UserID:  transportUserID,
			}, ownerSessionLabel)
			if err != nil {
				return fmt.Errorf("create owner session: %w", err)
			}
		}
	}

	metadata := h.sessionManager.GetAgentMetadata(baldaProviderName)
	welcomeMsg := welcome.BuildAgentWelcomeMessage(ownerSessionLabel, ts.GetSessionID(), metadata.Type, metadata.Model, metadata.MCPServers)
	_ = sendMarkdown(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, welcomeMsg)
	h.sendSessionStartupNotice(ctx, locator, ts.GetSessionID())

	h.logger.Info().
		Int64("owner_id", ownerID).
		Int64("chat_id", chatID).
		Str("agent", baldaProviderName).
		Msg("owner session bootstrapped")
	return nil
}

func (h *BaldaHandler) sendSessionStartupNotice(ctx context.Context, locator baldasession.SessionLocator, sessionID string) {
	if h.sessionManager == nil {
		return
	}

	notice := strings.TrimSpace(h.sessionManager.TakeStartupNotice(sessionID))
	if notice == "" {
		return
	}

	if err := sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, locator, notice); err != nil {
		h.logger.Warn().Err(err).Str("session_id", sessionID).Msg("failed to send session startup notice")
	}
}

func (h *BaldaHandler) getOwnerID() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ownerID
}

func (h *BaldaHandler) getChatID() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.chatID
}

func (h *BaldaHandler) setChatID(chatID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.chatID = chatID
}

func (h *BaldaHandler) onStart(ctx context.Context) error {
	if h.telegramConfigured && !h.telegramEnabled {
		return nil
	}
	if err := h.initializeBotUsername(ctx); err != nil {
		return fmt.Errorf("resolve balda telegram bot identity: %w", err)
	}
	h.logOwnerAuthIfNeeded()

	if !h.ownerStore.HasOwner() {
		return nil
	}
	owner := h.ownerStore.GetOwner()
	if owner == nil {
		return nil
	}
	if owner.ChatID == 0 {
		return fmt.Errorf("owner.chat_id is required for balda startup; run /start to bind owner chat")
	}

	h.setOwner(owner.UserID, owner.ChatID)

	if err := h.bootstrapOwnerSession(ctx, owner.UserID, owner.ChatID); err != nil {
		h.logger.Error().Err(err).Int64("owner_id", owner.UserID).Msg("failed to bootstrap owner session during startup")
		if sendErr := sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, baldatelegram.NewLocator(owner.UserID, 0), "Could not start owner session. Please try again."); sendErr != nil {
			h.logger.Warn().Err(sendErr).Msg("failed to send owner session failure message")
		}
		return fmt.Errorf("bootstrap owner session during startup: %w", err)
	}

	if err := sendPlain(ctx, h.actorDispatcher, baldaHandlerActorAddress, baldatelegram.NewLocator(owner.UserID, 0), "Boss, I'm online and ready to work."); err != nil {
		h.logger.Warn().Err(err).Int64("owner_id", owner.UserID).Msg("failed to send startup ready message to owner")
		return nil
	}
	h.logger.Info().Int64("owner_id", owner.UserID).Msg("startup ready message sent to owner")
	return nil
}

func (h *BaldaHandler) initializeBotUsername(ctx context.Context) error {
	if h.tgClient == nil {
		return fmt.Errorf("telegram client is required")
	}

	meResp, err := h.tgClient.GetMeWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("getMe: %w", err)
	}
	if meResp == nil {
		return fmt.Errorf("getMe response is nil")
	}
	if meResp.JSON200 == nil {
		if meResp.JSON401 != nil {
			return fmt.Errorf("getMe unauthorized: %s", strings.TrimSpace(meResp.JSON401.Description))
		}
		if meResp.JSON400 != nil {
			return fmt.Errorf("getMe bad request: %s", strings.TrimSpace(meResp.JSON400.Description))
		}
		return fmt.Errorf("getMe response missing result (status %s)", strings.TrimSpace(meResp.Status()))
	}
	botUserID := meResp.JSON200.Result.Id
	if botUserID == 0 {
		return fmt.Errorf("getMe returned empty bot id")
	}

	username := ""
	if meResp.JSON200.Result.Username != nil {
		username = strings.TrimSpace(*meResp.JSON200.Result.Username)
	}
	if username == "" {
		return fmt.Errorf("getMe returned empty username for bot id %d", botUserID)
	}

	h.mu.Lock()
	h.botUserID = botUserID
	h.botUsername = username
	h.mu.Unlock()

	if h.authToken != "" {
		h.logger.Info().Int64("bot_user_id", botUserID).Str("bot_username", username).Bool("owner_auth_available", true).Msg("balda owner auth available")
		return nil
	}
	h.logger.Info().Int64("bot_user_id", botUserID).Str("bot_username", username).Msg("balda bot identity loaded")
	return nil
}

func (h *BaldaHandler) logOwnerAuthIfNeeded() {
	if h.authToken == "" || h.ownerStore.HasOwner() {
		return
	}

	_, username := h.getBotIdentity()
	if strings.TrimSpace(username) == "" {
		return
	}

	h.logger.Info().
		Str("auth_command", auth.BuildOwnerAuthCommand(h.authToken)).
		Str("auth_link", auth.BuildOwnerAuthLink(username, h.authToken)).
		Msg("balda owner authentication required")
}

func (h *BaldaHandler) getProviderName() string {
	providerName := strings.TrimSpace(h.sessionManager.BaldaProviderID())
	if providerName == "" {
		h.mu.RLock()
		defer h.mu.RUnlock()
		providerName = strings.TrimSpace(h.baldaProviderName)
	}
	return providerName
}

func (h *BaldaHandler) welcomeDisplayName(messageCtx baldatelegram.MessageContext, ts *baldasession.TopicSession) string {
	if !messageCtx.IsDM {
		return ownerSessionLabel
	}
	if ts == nil {
		return ""
	}
	return ts.GetAgentName()
}

func (h *BaldaHandler) currentTime() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

func (h *BaldaHandler) getBotIdentity() (int64, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.botUserID, h.botUsername
}
