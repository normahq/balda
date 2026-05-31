package swarm

import (
	"errors"
	"fmt"
	"testing"

	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
)

func TestClassifyErrorKinds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want ErrorKind
	}{
		{name: "transient", err: TransientError(errors.New("retry")), want: ErrorKindTransient},
		{name: "permanent", err: PermanentError(errors.New("perm")), want: ErrorKindPermanent},
		{name: "canceled", err: CanceledError(errors.New("cancel")), want: ErrorKindCanceled},
		{name: "decode", err: DecodeError(errors.New("decode")), want: ErrorKindDecode},
		{name: "external delivery", err: ExternalDeliveryError(errors.New("send failed")), want: ErrorKindExternalDelivery},
		{name: "fallback transient", err: errors.New("plain error"), want: ErrorKindTransient},
		{name: "nil", err: nil, want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("ClassifyError() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()

	t.Run("resolve error is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := &actorengine.ResolveError{Address: "session:missing"}
		if got := IsRetryableError(err); got {
			t.Fatalf("IsRetryableError() = true, want false")
		}
	})

	t.Run("wrapped resolve error is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("dispatch failed: %w", &actorengine.ResolveError{Address: "session:wrapped"})
		if got := IsRetryableError(err); got {
			t.Fatalf("IsRetryableError() = true, want false")
		}
	})

	t.Run("wrapped canonical actor not found is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("lookup failed: %w", actorengine.ErrActorNotFound)
		if got := IsRetryableError(err); got {
			t.Fatalf("IsRetryableError() = true, want false")
		}
	})

	t.Run("other errors stay classified by actor errors", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("%w", PermanentError(errors.New("persist failed")))
		if got := IsRetryableError(err); got {
			t.Fatalf("IsRetryableError() = true, want false")
		}
	})
}
