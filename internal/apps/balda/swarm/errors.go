package swarm

import (
	"errors"
	"fmt"

	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
)

type ErrorKind string

const (
	ErrorKindTransient        ErrorKind = "transient"
	ErrorKindQuota            ErrorKind = "quota"
	ErrorKindAuth             ErrorKind = "auth"
	ErrorKindPolicy           ErrorKind = "policy"
	ErrorKindDuplicate        ErrorKind = "duplicate"
	ErrorKindPermanent        ErrorKind = "permanent"
	ErrorKindCanceled         ErrorKind = "canceled"
	ErrorKindDecode           ErrorKind = "decode"
	ErrorKindExternalDelivery ErrorKind = "external_delivery"
)

type ActorError struct {
	Kind ErrorKind
	Err  error
}

func (e *ActorError) Error() string {
	if e == nil || e.Err == nil {
		return string(e.Kind)
	}
	return e.Err.Error()
}

func (e *ActorError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func TransientError(err error) error { return actorError(ErrorKindTransient, err) }
func QuotaError(err error) error     { return actorError(ErrorKindQuota, err) }
func AuthError(err error) error      { return actorError(ErrorKindAuth, err) }
func PolicyError(err error) error    { return actorError(ErrorKindPolicy, err) }
func DuplicateError(err error) error { return actorError(ErrorKindDuplicate, err) }
func PermanentError(err error) error { return actorError(ErrorKindPermanent, err) }
func CanceledError(err error) error  { return actorError(ErrorKindCanceled, err) }
func DecodeError(err error) error    { return actorError(ErrorKindDecode, err) }

func ExternalDeliveryError(err error) error {
	return actorError(ErrorKindExternalDelivery, err)
}

func actorError(kind ErrorKind, err error) error {
	if err == nil {
		err = fmt.Errorf("%s error", kind)
	}
	return &ActorError{Kind: kind, Err: err}
}

func classifyError(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var actorErr *ActorError
	if errors.As(err, &actorErr) && actorErr.Kind != "" {
		return actorErr.Kind
	}
	return ErrorKindTransient
}

func ClassifyError(err error) ErrorKind {
	return classifyError(err)
}

// IsRetryableError reports whether an error should be retried by runtime consumers.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, actorengine.ErrActorNotFound) {
		return false
	}
	switch ClassifyError(err) {
	case ErrorKindDuplicate, ErrorKindAuth, ErrorKindPolicy, ErrorKindPermanent, ErrorKindDecode, ErrorKindCanceled:
		return false
	default:
		return true
	}
}
