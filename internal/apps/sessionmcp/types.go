package sessionmcp

import "context"

type noInput struct{}

type SessionLocatorInput struct {
	SessionID   string `json:"session_id" jsonschema:"target session id, e.g. tg-123-0"`
	ChannelType string `json:"channel_type" jsonschema:"channel type, e.g. telegram"`
	AddressKey  string `json:"address_key" jsonschema:"canonical address key, e.g. 12345:0"`
	AddressJSON string `json:"address_json" jsonschema:"canonical address JSON for the target session locator"`
}

// ToolError represents an error from a tool operation.
type ToolError struct {
	Operation string `json:"operation" jsonschema:"tool name that produced the error"`
	Code      string `json:"code" jsonschema:"stable machine-readable error code"`
	Message   string `json:"message" jsonschema:"human-readable error message"`
}

// ToolOutcome represents the result of a tool operation.
type ToolOutcome struct {
	OK    bool       `json:"ok" jsonschema:"true when the tool completed successfully"`
	Error *ToolError `json:"error,omitempty" jsonschema:"error details when ok is false"`
}

type basicOutput struct {
	ToolOutcome
}

// Get input/output

type getKeyInput struct {
	Key string `json:"key" jsonschema:"state key to retrieve"`
}

type getKeyOutput struct {
	ToolOutcome
	Value string `json:"value,omitempty" jsonschema:"stored value for the key"`
	Found bool   `json:"found" jsonschema:"whether the key exists"`
}

// Set input/output

type setKeyInput struct {
	Key   string `json:"key" jsonschema:"state key"`
	Value string `json:"value" jsonschema:"state value (JSON string)"`
}

// Delete input/output

type deleteKeyInput struct {
	Key string `json:"key" jsonschema:"state key to delete"`
}

// List input/output

type listKeysInput struct {
	Prefix string `json:"prefix,omitempty" jsonschema:"optional key prefix filter"`
}

type listKeysOutput struct {
	ToolOutcome
	Keys []string `json:"keys,omitempty" jsonschema:"matching keys"`
}

// GetJSON input/output - returns parsed JSON value

type getJSONInput struct {
	Key string `json:"key" jsonschema:"state key to retrieve as JSON"`
}

type getJSONOutput struct {
	ToolOutcome
	Value interface{} `json:"value,omitempty" jsonschema:"parsed JSON value stored at the key"`
	Found bool        `json:"found" jsonschema:"whether the key exists"`
}

// SetJSON input/output - stores a value as JSON

type setJSONInput struct {
	Key   string      `json:"key" jsonschema:"state key"`
	Value interface{} `json:"value" jsonschema:"value to store as JSON"`
}

// MergeJSON input/output - merges JSON object into existing value

type mergeJSONInput struct {
	Key   string                 `json:"key" jsonschema:"state key (must contain JSON object)"`
	Value map[string]interface{} `json:"value" jsonschema:"fields to merge into existing object"`
}

type mergeJSONOutput struct {
	ToolOutcome
	Merged map[string]interface{} `json:"merged,omitempty" jsonschema:"merged JSON object after applying the update"`
}

// Keyspace input/output - for scoping keys to a session/agent

type keyspaceInput struct {
	Namespace string `json:"namespace" jsonschema:"namespace for key isolation (e.g., session-id or agent-name)"`
	Key       string `json:"key" jsonschema:"key within namespace"`
}

type keyspaceValueInput struct {
	Namespace string `json:"namespace" jsonschema:"namespace for key isolation"`
	Key       string `json:"key" jsonschema:"key within namespace"`
	Value     string `json:"value" jsonschema:"value to store"`
}

type keyspaceJSONInput struct {
	Namespace string      `json:"namespace" jsonschema:"namespace for key isolation"`
	Key       string      `json:"key" jsonschema:"key within namespace"`
	Value     interface{} `json:"value" jsonschema:"value to store as JSON"`
}

type namespaceOnlyInput struct {
	Namespace string `json:"namespace" jsonschema:"namespace to list keys from"`
}

type SessionWaitInput struct {
	Action       string              `json:"action,omitempty" jsonschema:"wait action: schedule, list, or cancel; defaults to schedule"`
	Locator      SessionLocatorInput `json:"locator" jsonschema:"target session locator for the wake-up"`
	Content      string              `json:"content,omitempty" jsonschema:"message or command text to run when the wait wakes"`
	DelaySeconds int                 `json:"delay_seconds,omitempty" jsonschema:"positive delay in seconds before wake-up"`
	JobID        string              `json:"job_id,omitempty" jsonschema:"optional stable wait id"`
	RequestedBy  string              `json:"requested_by,omitempty" jsonschema:"optional requester id for audit metadata"`
	Notify       bool                `json:"notify,omitempty" jsonschema:"whether to send an acknowledgement message to the session when the wait is scheduled"`
}

type sessionWaitOutput struct {
	ToolOutcome
	Accepted bool                  `json:"accepted,omitempty" jsonschema:"true when action=schedule and the wait command was published successfully"`
	Deleted  bool                  `json:"deleted,omitempty" jsonschema:"true when the wait job existed and was deleted"`
	Message  string                `json:"message,omitempty" jsonschema:"human-readable status message"`
	Items    []SessionWaitListItem `json:"items,omitempty" jsonschema:"scheduled wait jobs for the locator"`
}

type SessionWaitListItem struct {
	JobID        string `json:"job_id" jsonschema:"wait job id"`
	Content      string `json:"content" jsonschema:"message or command text to run when the wait wakes"`
	Status       string `json:"status" jsonschema:"scheduled job status"`
	ScheduleSpec string `json:"schedule_spec" jsonschema:"schedule mode specification"`
	Timezone     string `json:"timezone" jsonschema:"scheduled job timezone"`
	NextRunAt    string `json:"next_run_at,omitempty" jsonschema:"next scheduled run time in RFC3339"`
	CreatedAt    string `json:"created_at,omitempty" jsonschema:"creation time in RFC3339"`
	UpdatedAt    string `json:"updated_at,omitempty" jsonschema:"last update time in RFC3339"`
	LastError    string `json:"last_error,omitempty" jsonschema:"last execution error if any"`
}

type SessionWaitService interface {
	ScheduleSessionWait(ctx context.Context, in SessionWaitInput) error
	ListSessionWaits(ctx context.Context, locator SessionLocatorInput) ([]SessionWaitListItem, error)
	CancelSessionWait(ctx context.Context, locator SessionLocatorInput, jobID string) (bool, error)
}

type SessionQuestionOption struct {
	ID    string `json:"id" jsonschema:"stable option identifier returned when the option is chosen"`
	Label string `json:"label" jsonschema:"human-readable option label shown to the user"`
}

type SessionQuestionInput struct {
	Locator         SessionLocatorInput     `json:"locator" jsonschema:"target session locator for the question delivery"`
	Prompt          string                  `json:"prompt" jsonschema:"question text to deliver to the session"`
	Options         []SessionQuestionOption `json:"options,omitempty" jsonschema:"structured options rendered as transport-native buttons when available"`
	DefaultOptionID string                  `json:"default_option_id,omitempty" jsonschema:"optional option id to apply automatically after timeout"`
	AllowFreeText   bool                    `json:"allow_free_text,omitempty" jsonschema:"whether the user may answer with arbitrary text instead of only choosing an option"`
	TimeoutSeconds  int                     `json:"timeout_seconds,omitempty" jsonschema:"positive timeout in seconds before the question settles"`
	RequestedBy     string                  `json:"requested_by,omitempty" jsonschema:"optional requester user id used to restrict who may answer private questions"`
	Private         bool                    `json:"private,omitempty" jsonschema:"whether the question should be targeted privately to requested_by when supported"`
	Metadata        map[string]string       `json:"metadata,omitempty" jsonschema:"optional metadata for downstream question clients"`
}

type SessionQuestionOutput struct {
	ToolOutcome
	QuestionID string `json:"question_id,omitempty" jsonschema:"created question id"`
	OptionID   string `json:"option_id,omitempty" jsonschema:"selected option id when the question settled via structured choice"`
	Text       string `json:"text,omitempty" jsonschema:"free-text or selected option label returned by the user"`
	Source     string `json:"source,omitempty" jsonschema:"settlement source such as user, default, timeout, or delivery_failed"`
	TimedOut   bool   `json:"timed_out,omitempty" jsonschema:"true when the question timed out without a user answer"`
	Canceled   bool   `json:"canceled,omitempty" jsonschema:"true when the question was canceled or could not be completed interactively"`
}

type SessionQuestionService interface {
	AskSessionQuestion(ctx context.Context, in SessionQuestionInput) (SessionQuestionOutput, error)
}
