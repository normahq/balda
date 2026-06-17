package zulip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout       = 15 * time.Second
	maxResponseBodyBytes     = 1 << 20
	maxErrorResponseBodyText = 4096
)

// Client is a low-level Zulip REST client.
type Client struct {
	baseURL  string
	botEmail string
	apiKey   string
	http     *http.Client
}

// sendMessageResult holds the Zulip API response for a sent message.
type sendMessageResult struct {
	Result string `json:"result"`
	Msg    string `json:"msg"`
	Code   string `json:"code"`
	ID     int    `json:"id"`
}

// APIError describes a Zulip API response that rejected the request.
type APIError struct {
	Path       string
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("zulip %s returned HTTP %d: %s", e.Path, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("zulip %s returned HTTP %d (%s): %s", e.Path, e.StatusCode, e.Code, e.Message)
}

// NewClient creates a new Zulip API client.
func NewClient(baseURL, botEmail, apiKey string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		botEmail: strings.TrimSpace(botEmail),
		apiKey:   strings.TrimSpace(apiKey),
		http:     &http.Client{Timeout: defaultHTTPTimeout},
	}
}

// ValidateConfig validates the REST credentials needed to send Zulip replies.
func ValidateConfig(baseURL, botEmail, apiKey string) error {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		return fmt.Errorf("balda.zulip.server_url is required when Zulip webhook is enabled")
	}
	parsedBaseURL, err := url.Parse(trimmedBaseURL)
	if err != nil || parsedBaseURL.Scheme == "" || parsedBaseURL.Host == "" {
		return fmt.Errorf("balda.zulip.server_url must be an absolute http(s) URL")
	}
	switch parsedBaseURL.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("balda.zulip.server_url must use http or https")
	}
	if strings.TrimSpace(botEmail) == "" {
		return fmt.Errorf("balda.zulip.bot_email is required when Zulip webhook is enabled")
	}
	if strings.TrimSpace(apiKey) == "" {
		return fmt.Errorf("balda.zulip.api_key is required when Zulip webhook is enabled")
	}
	return nil
}

// SendStreamMessage sends a message to a Zulip stream topic.
// Returns the Zulip message ID on success.
func (c *Client) SendStreamMessage(
	ctx context.Context,
	streamID int,
	topic string,
	content string,
) (int, error) {
	toJSON, _ := json.Marshal(streamID)
	form := url.Values{}
	form.Set("type", "stream")
	form.Set("to", string(toJSON))
	form.Set("topic", topic)
	form.Set("content", content)
	return c.postMessage(ctx, form)
}

// SendDirectMessage sends a direct message to a Zulip user.
// Returns the Zulip message ID on success.
func (c *Client) SendDirectMessage(
	ctx context.Context,
	userID int,
	content string,
) (int, error) {
	toJSON, _ := json.Marshal([]int{userID})
	form := url.Values{}
	form.Set("type", "direct")
	form.Set("to", string(toJSON))
	form.Set("content", content)
	return c.postMessage(ctx, form)
}

// SendStreamTyping sends a typing indicator to a stream topic.
func (c *Client) SendStreamTyping(
	ctx context.Context,
	streamID int,
	topic string,
) error {
	type streamTarget struct {
		StreamID int    `json:"stream_id"`
		Topic    string `json:"topic"`
	}
	toJSON, _ := json.Marshal([]streamTarget{{StreamID: streamID, Topic: topic}})
	form := url.Values{}
	form.Set("op", "start")
	form.Set("type", "stream")
	form.Set("to", string(toJSON))
	return c.postTyping(ctx, form)
}

// SendDirectTyping sends a typing indicator to a direct message conversation.
func (c *Client) SendDirectTyping(ctx context.Context, userID int) error {
	toJSON, _ := json.Marshal([]int{userID})
	form := url.Values{}
	form.Set("op", "start")
	form.Set("type", "direct")
	form.Set("to", string(toJSON))
	return c.postTyping(ctx, form)
}

func (c *Client) postMessage(ctx context.Context, form url.Values) (int, error) {
	body, err := c.post(ctx, "/api/v1/messages", form)
	if err != nil {
		return 0, err
	}
	var result sendMessageResult
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode zulip send message response: %w", err)
	}
	if result.Result != "success" {
		return 0, &APIError{
			Path:       "/api/v1/messages",
			StatusCode: http.StatusOK,
			Code:       result.Code,
			Message:    result.Msg,
		}
	}
	return result.ID, nil
}

func (c *Client) postTyping(ctx context.Context, form url.Values) error {
	_, err := c.post(ctx, "/api/v1/typing", form)
	return err
}

func (c *Client) post(
	ctx context.Context,
	path string,
	form url.Values,
) ([]byte, error) {
	endpoint := c.baseURL + path
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("build zulip request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.botEmail, c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("zulip request to %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readLimitedResponseBody(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read zulip response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{
			Path:       path,
			StatusCode: resp.StatusCode,
			Message:    responseBodySnippet(body),
		}
	}
	return body, nil
}

func isContentRejectedError(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.StatusCode == http.StatusBadRequest {
		return true
	}
	return apiErr.StatusCode == http.StatusOK && strings.EqualFold(apiErr.Code, "BAD_REQUEST")
}

func readLimitedResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBodyBytes {
		return nil, fmt.Errorf("zulip response body too large: limit %d bytes", maxResponseBodyBytes)
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
