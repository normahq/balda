package execution

import (
	"strings"

	"github.com/normahq/balda/pkg/actorlayer"
)

const JobIDMetaKey = "job_id"

func EnvelopeJobID(env actorlayer.Envelope) string {
	if env.Meta == nil {
		return ""
	}
	return strings.TrimSpace(env.Meta[JobIDMetaKey])
}

func WithJobIDMeta(meta map[string]string, jobID string) map[string]string {
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return meta
	}
	out := make(map[string]string, len(meta)+1)
	for k, v := range meta {
		out[k] = v
	}
	out[JobIDMetaKey] = trimmed
	return out
}
