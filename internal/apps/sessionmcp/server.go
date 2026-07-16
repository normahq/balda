package sessionmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	codeValidationError = "validation_error"
	codeBackendError    = "backend_error"
)

// RegisterTools adds session-state MCP tools to an existing server.
func RegisterTools(server *mcp.Server, store Store, waitService SessionWaitService, questionService SessionQuestionService) {
	if server == nil || store == nil {
		return
	}
	svc := &service{store: store, waitService: waitService, questionService: questionService}
	svc.registerTools(server)
}

type service struct {
	store           Store
	waitService     SessionWaitService
	questionService SessionQuestionService
}

func (s *service) registerTools(server *mcp.Server) {
	// Basic key-value operations
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.get", Description: "Read a raw string value from persistent balda state by exact key."}, s.getKey)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.set", Description: "Write a raw string value to persistent balda state under an exact key."}, s.setKey)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.delete", Description: "Delete one exact key from persistent balda state."}, s.deleteKey)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.list", Description: "List persistent balda-state keys, optionally restricted to a prefix."}, s.listKeys)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.clear", Description: "Delete all keys stored by balda.state. This is destructive and affects every session using this state store."}, s.clearState)

	// JSON operations
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.get_json", Description: "Read a key from persistent balda state and return its parsed JSON value."}, s.getJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.set_json", Description: "Write a JSON value to persistent balda state under an exact key."}, s.setJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.merge_json", Description: "Merge object fields into an existing JSON object stored at a key and return the merged object."}, s.mergeJSON)

	// Namespaced operations for agent/session isolation
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.ns_get", Description: "Read a raw string value from a namespace-scoped key. Use namespaces such as session IDs or agent names to avoid collisions."}, s.nsGet)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.ns_set", Description: "Write a raw string value to a namespace-scoped key for session or agent isolation."}, s.nsSet)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.ns_set_json", Description: "Write a JSON value to a namespace-scoped key for session or agent isolation."}, s.nsSetJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "balda.state.ns_list", Description: "List keys stored inside one namespace without returning keys from other namespaces."}, s.nsList)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "balda.session.wait",
		Description: "Manage one-shot wake-ups for a specific Balda session. Use action=schedule, list, or cancel. This is a session-level action, not an instance-level control action.",
	}, s.sessionWait)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "balda.session.question",
		Description: "Ask a generic interactive question in a specific Balda session and wait for the settled answer. Supports transport-native buttons, optional default_option_id timeout fallback, and private requester-scoped questions.",
	}, s.sessionQuestion)
}

// nsKey builds a namespaced key for isolation.
func nsKey(namespace, key string) string {
	return fmt.Sprintf("ns:%s:%s", strings.TrimSpace(namespace), strings.TrimSpace(key))
}

// Basic key-value tools

func (s *service) getKey(ctx context.Context, _ *mcp.CallToolRequest, in getKeyInput) (*mcp.CallToolResult, getKeyOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.get", "key is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.Get(ctx, key)
	if err != nil {
		result, out := backendFailure("balda.state.get", err)
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	return nil, getKeyOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) setKey(ctx context.Context, _ *mcp.CallToolRequest, in setKeyInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.set", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Set(ctx, key, in.Value); err != nil {
		result, out := backendFailure("balda.state.set", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) deleteKey(ctx context.Context, _ *mcp.CallToolRequest, in deleteKeyInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.delete", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Delete(ctx, key); err != nil {
		result, out := backendFailure("balda.state.delete", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) listKeys(ctx context.Context, _ *mcp.CallToolRequest, in listKeysInput) (*mcp.CallToolResult, listKeysOutput, error) {
	prefix := strings.TrimSpace(in.Prefix)

	keys, err := s.store.List(ctx, prefix)
	if err != nil {
		result, out := backendFailure("balda.state.list", err)
		return result, listKeysOutput{ToolOutcome: out}, nil
	}
	return nil, listKeysOutput{ToolOutcome: okOutcome(), Keys: keys}, nil
}

func (s *service) clearState(ctx context.Context, _ *mcp.CallToolRequest, _ noInput) (*mcp.CallToolResult, basicOutput, error) {
	if err := s.store.Clear(ctx); err != nil {
		result, out := backendFailure("balda.state.clear", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

// JSON tools

func (s *service) getJSON(ctx context.Context, _ *mcp.CallToolRequest, in getJSONInput) (*mcp.CallToolResult, getJSONOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.get_json", "key is required")
		return result, getJSONOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.GetJSON(ctx, key)
	if err != nil {
		result, out := backendFailure("balda.state.get_json", err)
		return result, getJSONOutput{ToolOutcome: out}, nil
	}
	return nil, getJSONOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) setJSON(ctx context.Context, _ *mcp.CallToolRequest, in setJSONInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.set_json", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.SetJSON(ctx, key, in.Value); err != nil {
		result, out := backendFailure("balda.state.set_json", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) mergeJSON(ctx context.Context, _ *mcp.CallToolRequest, in mergeJSONInput) (*mcp.CallToolResult, mergeJSONOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.merge_json", "key is required")
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}
	if len(in.Value) == 0 {
		result, out := validationFailure("balda.state.merge_json", "value must have at least one field")
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}

	merged, err := s.store.MergeJSON(ctx, key, in.Value)
	if err != nil {
		result, out := backendFailure("balda.state.merge_json", err)
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}
	return nil, mergeJSONOutput{ToolOutcome: okOutcome(), Merged: merged}, nil
}

// Namespaced tools

func (s *service) nsGet(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceInput) (*mcp.CallToolResult, getKeyOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("balda.state.ns_get", "namespace is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.ns_get", "key is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.Get(ctx, nsKey(namespace, key))
	if err != nil {
		result, out := backendFailure("balda.state.ns_get", err)
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	return nil, getKeyOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) nsSet(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceValueInput) (*mcp.CallToolResult, basicOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("balda.state.ns_set", "namespace is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.ns_set", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Set(ctx, nsKey(namespace, key), in.Value); err != nil {
		result, out := backendFailure("balda.state.ns_set", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) nsSetJSON(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceJSONInput) (*mcp.CallToolResult, basicOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("balda.state.ns_set_json", "namespace is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("balda.state.ns_set_json", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.SetJSON(ctx, nsKey(namespace, key), in.Value); err != nil {
		result, out := backendFailure("balda.state.ns_set_json", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) nsList(ctx context.Context, _ *mcp.CallToolRequest, in namespaceOnlyInput) (*mcp.CallToolResult, listKeysOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("balda.state.ns_list", "namespace is required")
		return result, listKeysOutput{ToolOutcome: out}, nil
	}

	prefix := nsKey(namespace, "")
	keys, err := s.store.List(ctx, prefix)
	if err != nil {
		result, out := backendFailure("balda.state.ns_list", err)
		return result, listKeysOutput{ToolOutcome: out}, nil
	}

	// Strip prefix from returned keys
	stripped := make([]string, 0, len(keys))
	for _, k := range keys {
		if after, ok := strings.CutPrefix(k, prefix); ok {
			stripped = append(stripped, after)
		}
	}
	return nil, listKeysOutput{ToolOutcome: okOutcome(), Keys: stripped}, nil
}

func (s *service) sessionWait(ctx context.Context, _ *mcp.CallToolRequest, in SessionWaitInput) (*mcp.CallToolResult, sessionWaitOutput, error) {
	if s.waitService == nil {
		result, out := backendFailure("balda.session.wait", fmt.Errorf("wait scheduler is required"))
		return result, sessionWaitOutput{ToolOutcome: out}, nil
	}
	action := strings.TrimSpace(in.Action)
	if action == "" {
		action = "schedule"
	}
	if strings.TrimSpace(in.Locator.ChannelType) == "" || strings.TrimSpace(in.Locator.AddressKey) == "" {
		result, out := validationFailure("balda.session.wait", "locator.channel_type and locator.address_key are required")
		return result, sessionWaitOutput{ToolOutcome: out}, nil
	}
	switch action {
	case "schedule":
		if strings.TrimSpace(in.Content) == "" {
			result, out := validationFailure("balda.session.wait", "content is required for action=schedule")
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		if in.DelaySeconds <= 0 {
			result, out := validationFailure("balda.session.wait", "delay_seconds must be positive for action=schedule")
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		if err := s.waitService.ScheduleSessionWait(ctx, in); err != nil {
			result, out := backendFailure("balda.session.wait", err)
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		message := "wait scheduled"
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: message}}}, sessionWaitOutput{ToolOutcome: okOutcome(), Accepted: true, Message: message}, nil
	case "list":
		items, err := s.waitService.ListSessionWaits(ctx, in.Locator)
		if err != nil {
			result, out := backendFailure("balda.session.wait", err)
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		out := sessionWaitOutput{ToolOutcome: okOutcome(), Items: items}
		return jsonToolResult(out), out, nil
	case "cancel":
		if strings.TrimSpace(in.JobID) == "" {
			result, out := validationFailure("balda.session.wait", "job_id is required for action=cancel")
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		deleted, err := s.waitService.CancelSessionWait(ctx, in.Locator, in.JobID)
		if err != nil {
			result, out := backendFailure("balda.session.wait", err)
			return result, sessionWaitOutput{ToolOutcome: out}, nil
		}
		message := "wait not found"
		if deleted {
			message = "wait canceled"
		}
		out := sessionWaitOutput{ToolOutcome: okOutcome(), Deleted: deleted, Message: message}
		return jsonToolResult(out), out, nil
	default:
		result, out := validationFailure("balda.session.wait", "action must be one of: schedule, list, cancel")
		return result, sessionWaitOutput{ToolOutcome: out}, nil
	}
}

func (s *service) sessionQuestion(ctx context.Context, _ *mcp.CallToolRequest, in SessionQuestionInput) (*mcp.CallToolResult, SessionQuestionOutput, error) {
	if s.questionService == nil {
		result, out := backendFailure("balda.session.question", fmt.Errorf("session question service is required"))
		return result, SessionQuestionOutput{ToolOutcome: out}, nil
	}
	if strings.TrimSpace(in.Locator.SessionID) == "" || strings.TrimSpace(in.Locator.ChannelType) == "" || strings.TrimSpace(in.Locator.AddressKey) == "" {
		result, out := validationFailure("balda.session.question", "locator.session_id, locator.channel_type, and locator.address_key are required")
		return result, SessionQuestionOutput{ToolOutcome: out}, nil
	}
	if strings.TrimSpace(in.Prompt) == "" {
		result, out := validationFailure("balda.session.question", "prompt is required")
		return result, SessionQuestionOutput{ToolOutcome: out}, nil
	}
	if !in.AllowFreeText && len(in.Options) == 0 {
		result, out := validationFailure("balda.session.question", "options are required unless allow_free_text is true")
		return result, SessionQuestionOutput{ToolOutcome: out}, nil
	}
	if in.TimeoutSeconds <= 0 {
		result, out := validationFailure("balda.session.question", "timeout_seconds must be positive")
		return result, SessionQuestionOutput{ToolOutcome: out}, nil
	}

	out, err := s.questionService.AskSessionQuestion(ctx, in)
	if err != nil {
		result, toolOut := backendFailure("balda.session.question", err)
		return result, SessionQuestionOutput{ToolOutcome: toolOut}, nil
	}
	return jsonToolResult(out), out, nil
}

// Helpers

func okOutcome() ToolOutcome {
	return ToolOutcome{OK: true}
}

func validationFailure(operation string, message string) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, codeValidationError, message)
}

func backendFailure(operation string, err error) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, codeBackendError, err.Error())
}

func failure(operation string, code string, message string) (*mcp.CallToolResult, ToolOutcome) {
	return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: message}},
		}, ToolOutcome{
			OK: false,
			Error: &ToolError{
				Operation: operation,
				Code:      code,
				Message:   message,
			},
		}
}

func jsonToolResult(v any) *mcp.CallToolResult {
	data, err := json.Marshal(v)
	if err != nil {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "{}"}}}
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}
}
