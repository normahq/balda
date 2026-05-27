package natsbus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

func (b *Bus) GetDLQEntry(ctx context.Context, sequence uint64) (swarm.DLQEntry, error) {
	if sequence == 0 {
		return swarm.DLQEntry{}, fmt.Errorf("dlq sequence must be greater than zero")
	}
	stream, err := b.js.Stream(ctx, b.cfg.Swarm.DLQ.Stream)
	if err != nil {
		return swarm.DLQEntry{}, fmt.Errorf("open dlq stream %s: %w", b.cfg.Swarm.DLQ.Stream, err)
	}
	msg, err := stream.GetMsg(ctx, sequence)
	if err != nil {
		if isDLQMessageNotFound(err) {
			return swarm.DLQEntry{}, fmt.Errorf("%w: sequence=%d", swarm.ErrDLQEntryNotFound, sequence)
		}
		return swarm.DLQEntry{}, fmt.Errorf("read dlq entry sequence=%d: %w", sequence, err)
	}
	env, err := swarm.DecodeEnvelope(string(msg.Data))
	if err != nil {
		return swarm.DLQEntry{}, fmt.Errorf("decode dlq envelope sequence=%d: %w", sequence, err)
	}
	return swarm.DLQEntry{
		Stream:      b.cfg.Swarm.DLQ.Stream,
		Sequence:    msg.Sequence,
		Subject:     msg.Subject,
		PublishedAt: msg.Time,
		Reason:      strings.TrimSpace(msg.Header.Get("Balda-DLQ-Reason")),
		Envelope:    env,
	}, nil
}

func isDLQMessageNotFound(err error) bool {
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return true
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "message not found")
}
