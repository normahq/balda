package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ChannelTelegram = "telegram"
	ChannelSlack    = "slack"
	ChannelZulip    = "zulip"

	ChannelTokenPrefix = "balda_"

	channelTokenKVPrefix = "channel_token:"
	channelTokenTTL      = 24 * time.Hour
	channelTokenBytes    = 24

	ChannelTokenPurposeOwnerBind = "owner_bind"
)

type channelTokenKVStore interface {
	GetJSON(ctx context.Context, key string) (value any, ok bool, err error)
	SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

type atomicChannelTokenKVStore interface {
	ConsumeJSON(ctx context.Context, key string, shouldConsume func(value any) (bool, error)) (value any, consumed bool, err error)
}

// ChannelTokenRecord stores metadata for a short-lived opaque auth token.
type ChannelTokenRecord struct {
	Purpose       string    `json:"purpose"`
	TargetChannel string    `json:"target_channel"`
	CreatedBy     string    `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// ChannelTokenStore persists hashed channel auth tokens.
type ChannelTokenStore struct {
	store channelTokenKVStore
	now   func() time.Time
}

// NewChannelTokenStore creates a store for short-lived channel authentication tokens.
func NewChannelTokenStore(store channelTokenKVStore) (*ChannelTokenStore, error) {
	if store == nil {
		return nil, fmt.Errorf("channel token store is required")
	}
	return &ChannelTokenStore{store: store, now: time.Now}, nil
}

// CreateOwnerBindToken creates a token that can bind the owner to another channel.
func (s *ChannelTokenStore) CreateOwnerBindToken(ctx context.Context, targetChannel, createdBy string) (string, *ChannelTokenRecord, error) {
	channel := strings.TrimSpace(targetChannel)
	if channel == "" {
		return "", nil, fmt.Errorf("target channel is required")
	}
	token, err := generateChannelToken()
	if err != nil {
		return "", nil, err
	}
	now := s.now().UTC()
	record := &ChannelTokenRecord{
		Purpose:       ChannelTokenPurposeOwnerBind,
		TargetChannel: channel,
		CreatedBy:     strings.TrimSpace(createdBy),
		CreatedAt:     now,
		ExpiresAt:     now.Add(channelTokenTTL),
	}
	if err := s.store.SetWithTTL(ctx, channelTokenKey(token), record, channelTokenTTL); err != nil {
		return "", nil, fmt.Errorf("store channel token: %w", err)
	}
	return token, record, nil
}

// Consume atomically validates and removes a matching channel auth token.
func (s *ChannelTokenStore) Consume(ctx context.Context, token, targetChannel string) (*ChannelTokenRecord, error) {
	trimmed := strings.TrimSpace(token)
	if !LooksLikeChannelToken(trimmed) {
		return nil, nil
	}
	key := channelTokenKey(trimmed)
	if store, ok := s.store.(atomicChannelTokenKVStore); ok {
		var record ChannelTokenRecord
		var matched bool
		_, consumed, err := store.ConsumeJSON(ctx, key, func(raw any) (bool, error) {
			decoded, err := decodeChannelTokenRecord(raw)
			if err != nil {
				return false, err
			}
			if !decoded.ExpiresAt.IsZero() && !decoded.ExpiresAt.After(s.now()) {
				return true, nil
			}
			if strings.TrimSpace(decoded.TargetChannel) != strings.TrimSpace(targetChannel) {
				return false, nil
			}
			record = *decoded
			matched = true
			return true, nil
		})
		if err != nil {
			return nil, fmt.Errorf("consume channel token: %w", err)
		}
		if !consumed || !matched {
			return nil, nil
		}
		return &record, nil
	}

	raw, ok, err := s.store.GetJSON(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get channel token: %w", err)
	}
	if !ok || raw == nil {
		return nil, nil
	}
	record, err := decodeChannelTokenRecord(raw)
	if err != nil {
		return nil, err
	}
	if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(s.now()) {
		_ = s.store.Delete(ctx, key)
		return nil, nil
	}
	if strings.TrimSpace(record.TargetChannel) != strings.TrimSpace(targetChannel) {
		return nil, nil
	}
	if err := s.store.Delete(ctx, key); err != nil {
		return nil, fmt.Errorf("delete consumed channel token: %w", err)
	}
	return record, nil
}

// LooksLikeChannelToken reports whether token has the Balda channel auth prefix.
func LooksLikeChannelToken(token string) bool {
	return strings.HasPrefix(strings.TrimSpace(token), ChannelTokenPrefix)
}

func generateChannelToken() (string, error) {
	buf := make([]byte, channelTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate channel token: %w", err)
	}
	return ChannelTokenPrefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

func channelTokenKey(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return channelTokenKVPrefix + hex.EncodeToString(sum[:])
}

// ChannelAuthService owns transparent channel token consumption.
type ChannelAuthService struct {
	tokens *ChannelTokenStore
	owner  *OwnerStore
}

// OwnerBindToken carries a token for binding the owner on a specific channel.
type OwnerBindToken struct {
	Channel string
	Token   string
}

// NewChannelAuthService creates transparent channel authentication service wiring.
func NewChannelAuthService(tokens *ChannelTokenStore, owner *OwnerStore) *ChannelAuthService {
	return &ChannelAuthService{tokens: tokens, owner: owner}
}

// ConsumeOwnerBind consumes a token and binds the subject as an owner on the channel.
func (s *ChannelAuthService) ConsumeOwnerBind(ctx context.Context, channel, subject, token string) (bool, error) {
	if s == nil || s.tokens == nil || s.owner == nil {
		return false, nil
	}
	if !s.owner.HasOwner() {
		return false, nil
	}
	record, err := s.tokens.Consume(ctx, token, channel)
	if err != nil {
		return false, err
	}
	if record == nil || record.Purpose != ChannelTokenPurposeOwnerBind {
		return false, nil
	}
	return true, s.owner.BindOwnerSubject(subject)
}

// CreateOwnerBindToken creates a channel-specific owner bind token.
func (s *ChannelAuthService) CreateOwnerBindToken(ctx context.Context, channel, createdBy string) (string, error) {
	if s == nil || s.tokens == nil {
		return "", fmt.Errorf("channel auth service is unavailable")
	}
	token, _, err := s.tokens.CreateOwnerBindToken(ctx, channel, createdBy)
	return token, err
}

// CreateMissingOwnerBindTokens creates owner bind tokens for channels not yet connected.
func (s *ChannelAuthService) CreateMissingOwnerBindTokens(ctx context.Context, createdBy string) ([]OwnerBindToken, error) {
	if s == nil || s.owner == nil || !s.owner.HasOwner() {
		return nil, nil
	}
	channels := []string{ChannelTelegram, ChannelSlack, ChannelZulip}
	out := make([]OwnerBindToken, 0, len(channels))
	for _, channel := range channels {
		if s.owner.HasOwnerSubject(channel) {
			continue
		}
		token, err := s.CreateOwnerBindToken(ctx, channel, createdBy)
		if err != nil {
			return nil, err
		}
		out = append(out, OwnerBindToken{Channel: channel, Token: token})
	}
	return out, nil
}

func decodeChannelTokenRecord(raw any) (*ChannelTokenRecord, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal channel token: %w", err)
	}
	var record ChannelTokenRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("unmarshal channel token: %w", err)
	}
	return &record, nil
}
