package execution

import "testing"

func TestConfigNormalized_DefaultsToDurableRuntime(t *testing.T) {
	t.Parallel()

	got, err := (Config{}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if got.Commands.Stream != DefaultCommandStream {
		t.Fatalf("Commands.Stream = %q, want %q", got.Commands.Stream, DefaultCommandStream)
	}
	if got.Commands.Consumer != DefaultCommandConsumer {
		t.Fatalf("Commands.Consumer = %q, want %q", got.Commands.Consumer, DefaultCommandConsumer)
	}
	if got.Commands.AckWait != "5m" || got.Commands.MaxDeliver != 5 || got.Commands.FetchBatch != 16 {
		t.Fatalf("Commands defaults = %+v", got.Commands)
	}
	if got.Events.Stream != DefaultEventStream || got.DLQ.Stream != DefaultDLQStream {
		t.Fatalf("Events/DLQ defaults = %+v/%+v", got.Events, got.DLQ)
	}
}

func TestCommandConfigNormalized_ClampsFetchBatchToMaxAckPending(t *testing.T) {
	t.Parallel()

	got := (CommandConfig{MaxAckPending: 4, FetchBatch: 16}).Normalized()
	if got.MaxAckPending != 4 || got.FetchBatch != 4 {
		t.Fatalf("CommandConfig.Normalized() = %+v, want fetch_batch clamped to max_ack_pending", got)
	}

	got = (CommandConfig{FetchBatch: 128}).Normalized()
	if got.MaxAckPending != 64 || got.FetchBatch != 64 {
		t.Fatalf("CommandConfig.Normalized() = %+v, want fetch_batch clamped to default max_ack_pending", got)
	}
}
