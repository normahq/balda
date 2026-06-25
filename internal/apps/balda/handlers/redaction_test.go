package handlers

import (
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	t.Parallel()

	slackToken := "xox" + "b-123456789012-ABCdefGhIjkLMNop"
	tests := []struct {
		name       string
		in         string
		want       string
		notContain []string
	}{
		{
			name:       "bearer header",
			in:         "Authorization: Bearer super-secret-token",
			want:       "Authorization: Bearer [REDACTED]",
			notContain: []string{"super-secret-token"},
		},
		{
			name:       "key value token",
			in:         "token=abcd1234",
			want:       "token=[REDACTED]",
			notContain: []string{"abcd1234"},
		},
		{
			name:       "pem block",
			in:         "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
			want:       "[REDACTED_PEM]",
			notContain: []string{"PRIVATE KEY", "abc"},
		},
		{
			name:       "telegram token",
			in:         "bot token 123456:ABCdefGhIjkLMNopQRST_uvwx",
			want:       "bot token [REDACTED_TOKEN]",
			notContain: []string{"123456:ABCdefGhIjkLMNopQRST_uvwx"},
		},
		{
			name:       "slack token",
			in:         "slack " + slackToken,
			want:       "slack [REDACTED_TOKEN]",
			notContain: []string{slackToken},
		},
		{
			name: "clean text unchanged",
			in:   "normal execution progress",
			want: "normal execution progress",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := redactSecrets(tc.in)
			if got != tc.want {
				t.Fatalf("redactSecrets() = %q, want %q", got, tc.want)
			}
			for _, forbidden := range tc.notContain {
				if forbidden != "" && strings.Contains(got, forbidden) {
					t.Fatalf("redactSecrets() = %q, still contains %q", got, forbidden)
				}
			}
		})
	}
}
