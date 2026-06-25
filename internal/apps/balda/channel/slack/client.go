package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultAPIBaseURL        = "https://slack.com/api"
	defaultHTTPClientTimeout = 15 * time.Second
	maxResponseBodyBytes     = 1 << 20
	maxErrorResponseBodyText = 4096
)

// Client is a small Slack Web API client for Balda's channel needs.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

type postMessageRequest struct {
	Channel  string `json:"channel"`
	Text     string `json:"text"`
	ThreadTS string `json:"thread_ts,omitempty"`
	Mrkdwn   bool   `json:"mrkdwn"`
}

type postMessageResponse struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
}

type authTestResponse struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	TeamID string `json:"team_id"`
	UserID string `json:"user_id"`
}

// APIError describes a Slack Web API error response.
type APIError struct {
	Method     string
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("slack %s returned HTTP %d (%s): %s", e.Method, e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("slack %s returned HTTP %d: %s", e.Method, e.StatusCode, e.Message)
}

// NewClient creates a Slack Web API client.
func NewClient(token string) *Client {
	return NewClientWithBaseURL(defaultAPIBaseURL, token)
}

// NewClientWithBaseURL creates a Slack Web API client using a custom API base URL.
func NewClientWithBaseURL(baseURL, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:   strings.TrimSpace(token),
		http:    &http.Client{Timeout: defaultHTTPClientTimeout},
	}
}

// AuthTest validates the configured bot token and returns bot identity fields.
func (c *Client) AuthTest(ctx context.Context) (teamID string, userID string, err error) {
	var result authTestResponse
	if err := c.postJSON(ctx, "auth.test", map[string]string{}, &result); err != nil {
		return "", "", err
	}
	if !result.OK {
		return "", "", &APIError{Method: "auth.test", StatusCode: http.StatusOK, Code: result.Error, Message: result.Error}
	}
	return result.TeamID, result.UserID, nil
}

// PostMessage posts a Slack message and returns the provider message timestamp.
func (c *Client) PostMessage(ctx context.Context, channel, threadTS, text string, mrkdwn bool) (string, error) {
	req := postMessageRequest{
		Channel:  strings.TrimSpace(channel),
		ThreadTS: strings.TrimSpace(threadTS),
		Text:     strings.TrimSpace(text),
		Mrkdwn:   mrkdwn,
	}
	if req.Channel == "" {
		return "", fmt.Errorf("slack channel is required")
	}
	if req.Text == "" {
		return "", fmt.Errorf("slack message text is required")
	}
	var result postMessageResponse
	if err := c.postJSON(ctx, "chat.postMessage", req, &result); err != nil {
		return "", err
	}
	if !result.OK {
		return "", &APIError{Method: "chat.postMessage", StatusCode: http.StatusOK, Code: result.Error, Message: result.Error}
	}
	if strings.TrimSpace(result.TS) == "" {
		return "", &APIError{Method: "chat.postMessage", StatusCode: http.StatusOK, Code: "malformed_response", Message: "missing ts"}
	}
	return result.TS, nil
}

func (c *Client) postJSON(ctx context.Context, method string, payload any, out any) error {
	if c == nil {
		return fmt.Errorf("slack client is required")
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return fmt.Errorf("slack api base url is required")
	}
	if strings.TrimSpace(c.token) == "" {
		return fmt.Errorf("slack bot token is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode slack %s request: %w", method, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build slack %s request: %w", method, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	httpClient := c.http
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPClientTimeout}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("slack %s request: %w", method, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := readLimitedResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read slack %s response body: %w", method, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Method: method, StatusCode: resp.StatusCode, Message: responseBodySnippet(data)}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode slack %s response: %w", method, err)
	}
	return nil
}

func readLimitedResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBodyBytes {
		return nil, fmt.Errorf("slack response body too large: limit %d bytes", maxResponseBodyBytes)
	}
	return data, nil
}

func responseBodySnippet(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) <= maxErrorResponseBodyText {
		return text
	}
	return text[:maxErrorResponseBodyText] + "...(truncated)"
}
