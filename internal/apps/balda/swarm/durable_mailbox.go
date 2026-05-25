package swarm

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SQLiteDurableMailbox adapts the existing SQLite mailbox store to the durable
// command mailbox contract. It auto-settles messages from handler results.
type SQLiteDurableMailbox struct {
	mailboxes *MailboxService
}

func NewSQLiteDurableMailbox(mailboxes *MailboxService) *SQLiteDurableMailbox {
	return &SQLiteDurableMailbox{mailboxes: mailboxes}
}

func (m *SQLiteDurableMailbox) PublishCommand(ctx context.Context, env Envelope) error {
	if m == nil || m.mailboxes == nil {
		return nil
	}
	_, err := m.mailboxes.Publish(ctx, env)
	return err
}

func (m *SQLiteDurableMailbox) ConsumeCommands(ctx context.Context, actorGroup string, handler EventHandler) error {
	if m == nil || m.mailboxes == nil || handler == nil {
		return nil
	}
	mailboxes, err := m.consumeMailboxes(ctx, actorGroup)
	if err != nil {
		return err
	}
	for _, mailbox := range mailboxes {
		batch, err := m.mailboxes.Claim(ctx, mailbox, "sqlite-durable-mailbox", DefaultClaimLimit, DefaultLeaseDuration)
		if err != nil {
			return err
		}
		for _, env := range batch {
			ref := MessageRef{Source: "sqlite", Subject: SubjectForEnvelope(env), Mailbox: mailbox, MessageID: env.ID}
			if err := handler(ctx, ref.Subject, env); err != nil {
				if settleErr := m.settleError(ctx, ref, env, err); settleErr != nil {
					return settleErr
				}
				continue
			}
			if err := m.Ack(ctx, ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *SQLiteDurableMailbox) Ack(ctx context.Context, msg MessageRef) error {
	if m == nil || m.mailboxes == nil {
		return nil
	}
	return m.mailboxes.Ack(ctx, msg.Mailbox, msg.MessageID)
}

func (m *SQLiteDurableMailbox) Retry(ctx context.Context, msg MessageRef, delay time.Duration, reason string) error {
	if m == nil || m.mailboxes == nil {
		return nil
	}
	return m.mailboxes.Retry(ctx, msg.Mailbox, msg.MessageID, time.Now().UTC().Add(delay), reason)
}

func (m *SQLiteDurableMailbox) DeadLetter(ctx context.Context, msg MessageRef, reason string) error {
	if m == nil || m.mailboxes == nil {
		return nil
	}
	return m.mailboxes.DeadLetter(ctx, msg.Mailbox, msg.MessageID, reason)
}

func (m *SQLiteDurableMailbox) consumeMailboxes(ctx context.Context, actorGroup string) ([]string, error) {
	trimmed := strings.TrimSpace(actorGroup)
	if trimmed != "" {
		return []string{trimmed}, nil
	}
	mailboxes, err := m.mailboxes.ListReadyMailboxes(ctx, 100)
	if err != nil {
		return nil, fmt.Errorf("list ready mailboxes: %w", err)
	}
	return mailboxes, nil
}

func (m *SQLiteDurableMailbox) settleError(ctx context.Context, ref MessageRef, env Envelope, err error) error {
	switch classifyError(err) {
	case ErrorKindDuplicate:
		return m.Ack(ctx, ref)
	case ErrorKindAuth, ErrorKindPolicy, ErrorKindPermanent:
		return m.DeadLetter(ctx, ref, err.Error())
	default:
		if env.MaxAttempts > 0 && env.Attempt >= env.MaxAttempts {
			return m.DeadLetter(ctx, ref, err.Error())
		}
		return m.Retry(ctx, ref, nextRetryAt(env.Attempt).Sub(time.Now().UTC()), err.Error())
	}
}
