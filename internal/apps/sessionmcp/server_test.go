package sessionmcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type recordingWaitScheduler struct {
	inputs []SessionWaitInput
	items  []SessionWaitListItem
	cancel map[string]bool
}

type recordingQuestionService struct {
	inputs []SessionQuestionInput
	output SessionQuestionOutput
}

func (r *recordingWaitScheduler) ScheduleSessionWait(_ context.Context, in SessionWaitInput) error {
	r.inputs = append(r.inputs, in)
	return nil
}

func (r *recordingWaitScheduler) ListSessionWaits(_ context.Context, _ SessionLocatorInput) ([]SessionWaitListItem, error) {
	return append([]SessionWaitListItem(nil), r.items...), nil
}

func (r *recordingWaitScheduler) CancelSessionWait(_ context.Context, _ SessionLocatorInput, jobID string) (bool, error) {
	if r.cancel == nil {
		return false, nil
	}
	return r.cancel[jobID], nil
}

func (r *recordingQuestionService) StartSessionQuestion(_ context.Context, in SessionQuestionInput) (SessionQuestionOutput, error) {
	r.inputs = append(r.inputs, in)
	if !r.output.OK {
		r.output.ToolOutcome = ToolOutcome{OK: true}
	}
	return r.output, nil
}

func TestSessionStateServerListsTools(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	got := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		got = append(got, tool.Name)
	}

	want := []string{
		"balda.state.clear",
		"balda.state.delete",
		"balda.state.get",
		"balda.state.get_json",
		"balda.state.list",
		"balda.state.merge_json",
		"balda.state.set",
		"balda.state.set_json",
		"balda.state.ns_get",
		"balda.state.ns_list",
		"balda.state.ns_set",
		"balda.state.ns_set_json",
		"balda.session.wait",
		"balda.session.question",
	}

	if len(got) != len(want) {
		t.Fatalf("tool count = %d, want %d\ngot: %v\nwant: %v", len(got), len(want), got, want)
	}
}

func TestSessionStateToolDescriptionsAndSchemas(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	toolByName := map[string]*mcp.Tool{}
	for _, tool := range tools.Tools {
		toolByName[tool.Name] = tool
	}

	if got := toolByName["balda.state.clear"].Description; !strings.Contains(got, "destructive") {
		t.Fatalf("balda.state.clear description = %q, want destructive warning", got)
	}
	if got := toolByName["balda.state.ns_set"].Description; !strings.Contains(got, "session or agent isolation") {
		t.Fatalf("balda.state.ns_set description = %q, want namespace guidance", got)
	}
	if got := toolByName["balda.session.wait"].Description; !strings.Contains(got, "session-level action") {
		t.Fatalf("balda.session.wait description = %q, want session-level wording", got)
	}
	if got := toolByName["balda.session.wait"].Description; !strings.Contains(got, "action=schedule, list, or cancel") {
		t.Fatalf("balda.session.wait description = %q, want action wording", got)
	}
	if got := toolByName["balda.session.question"].Description; !strings.Contains(got, "default_option_id") {
		t.Fatalf("balda.session.question description = %q, want default option wording", got)
	}
	questionTool := toolByName["balda.session.question"]
	if questionTool.Annotations == nil || questionTool.Annotations.ReadOnlyHint || questionTool.Annotations.DestructiveHint == nil || *questionTool.Annotations.DestructiveHint || questionTool.Annotations.OpenWorldHint == nil || *questionTool.Annotations.OpenWorldHint {
		t.Fatalf("balda.session.question annotations = %+v, want additive closed-session hints", questionTool.Annotations)
	}

	outSchema, ok := toolByName["balda.state.get"].OutputSchema.(map[string]any)
	if !ok {
		t.Fatalf("balda.state.get output schema type = %T, want map[string]any", toolByName["balda.state.get"].OutputSchema)
	}
	properties := outSchema["properties"].(map[string]any)
	found := properties["found"].(map[string]any)
	if got := found["description"]; got != "whether the key exists" {
		t.Fatalf("balda.state.get found description = %v, want whether the key exists", got)
	}
	waitSchema, ok := toolByName["balda.session.wait"].InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("balda.session.wait input schema type = %T, want map[string]any", toolByName["balda.session.wait"].InputSchema)
	}
	waitProps := waitSchema["properties"].(map[string]any)
	if _, ok := waitProps["locator"]; !ok {
		t.Fatal("balda.session.wait input schema missing locator property")
	}
	locatorSchema := waitProps["locator"].(map[string]any)
	requiredFields, _ := json.Marshal(locatorSchema["required"])
	if strings.Contains(string(requiredFields), `"address_json"`) {
		t.Fatalf("session locator required fields = %s, want address_json optional", requiredFields)
	}
	questionSchema, ok := toolByName["balda.session.question"].InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("balda.session.question input schema type = %T, want map[string]any", toolByName["balda.session.question"].InputSchema)
	}
	questionProps := questionSchema["properties"].(map[string]any)
	if _, ok := questionProps["options"]; !ok {
		t.Fatal("balda.session.question input schema missing options property")
	}
}

func TestSessionQuestionCallsService(t *testing.T) {
	questionService := &recordingQuestionService{
		output: SessionQuestionOutput{
			ToolOutcome: ToolOutcome{OK: true},
			QuestionID:  "question-1",
			OptionID:    "allow",
			Source:      "user",
		},
	}
	ctx, cleanup, session := newTestSessionWithQuestionService(t, NewMemoryStore(), nil, questionService)
	defer cleanup()
	_ = session.InitializeResult()

	result := callTool(t, ctx, session, "balda.session.question", map[string]any{
		"locator": map[string]any{
			"session_id":   "tg-1-0",
			"channel_type": "telegram",
			"address_key":  "1:0",
			"address_json": `{"chat_id":1,"topic_id":0}`,
		},
		"prompt":          "Continue?",
		"timeout_seconds": 30,
		"options": []map[string]any{
			{"id": "allow", "label": "Allow"},
			{"id": "reject", "label": "Reject"},
		},
		"default_option_id": "reject",
		"requested_by":      "tg-101",
		"private":           true,
	})
	assertOk(t, result)
	if len(questionService.inputs) != 1 {
		t.Fatalf("question service calls = %d, want 1", len(questionService.inputs))
	}
	if got := questionService.inputs[0].DefaultOptionID; got != "reject" {
		t.Fatalf("default_option_id = %q, want reject", got)
	}
}

func TestSessionWaitListReturnsItems(t *testing.T) {
	waitScheduler := &recordingWaitScheduler{
		items: []SessionWaitListItem{{JobID: "wait-1", Content: "wake me", Status: "active", ScheduleSpec: "@once", Timezone: "UTC"}},
	}
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), waitScheduler)
	defer cleanup()
	_ = session.InitializeResult()

	result := callTool(t, ctx, session, "balda.session.wait", map[string]any{
		"action": "list",
		"locator": map[string]any{
			"session_id":   "tg-1-0",
			"channel_type": "telegram",
			"address_key":  "1:0",
			"address_json": `{"chat_id":1,"topic_id":0}`,
		},
	})
	if result == nil || result.IsError {
		text := ""
		if len(result.Content) > 0 {
			if tc, ok := result.Content[0].(*mcp.TextContent); ok {
				text = tc.Text
			}
		}
		t.Fatalf("result = %#v, text=%q, want success", result, text)
	}
	if got := waitScheduler.items[0].JobID; got != "wait-1" {
		t.Fatalf("waitScheduler.items[0].JobID = %q, want wait-1", got)
	}
}

func TestSessionWaitCancelReturnsDeleted(t *testing.T) {
	waitScheduler := &recordingWaitScheduler{cancel: map[string]bool{"wait-1": true}}
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), waitScheduler)
	defer cleanup()
	_ = session.InitializeResult()

	result := callTool(t, ctx, session, "balda.session.wait", map[string]any{
		"action": "cancel",
		"locator": map[string]any{
			"session_id":   "tg-1-0",
			"channel_type": "telegram",
			"address_key":  "1:0",
			"address_json": `{"chat_id":1,"topic_id":0}`,
		},
		"job_id": "wait-1",
	})
	if result == nil || result.IsError {
		text := ""
		if len(result.Content) > 0 {
			if tc, ok := result.Content[0].(*mcp.TextContent); ok {
				text = tc.Text
			}
		}
		t.Fatalf("result = %#v, text=%q, want success", result, text)
	}
}

func TestSessionWaitPublishesControlCommand(t *testing.T) {
	waitScheduler := &recordingWaitScheduler{}
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), waitScheduler)
	defer cleanup()
	_ = session.InitializeResult()

	result := callTool(t, ctx, session, "balda.session.wait", map[string]any{
		"action": "schedule",
		"locator": map[string]any{
			"session_id":   "tg-1-0",
			"channel_type": "telegram",
			"address_key":  "1:0",
			"address_json": `{"chat_id":1,"topic_id":0}`,
		},
		"content":       "wake me",
		"delay_seconds": 60,
		"job_id":        "wait-1",
	})
	if result.IsError {
		t.Fatal("result.IsError = true, want false")
	}
	if len(waitScheduler.inputs) != 1 {
		t.Fatalf("scheduled waits = %d, want 1", len(waitScheduler.inputs))
	}
	if got := waitScheduler.inputs[0].Locator.SessionID; got != "tg-1-0" {
		t.Fatalf("locator.session_id = %q, want tg-1-0", got)
	}
}

func TestSetGetBasic(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	// Set a value
	setResult := callTool(t, ctx, session, "balda.state.set", map[string]any{
		"key":   "mykey",
		"value": "myvalue",
	})
	assertOk(t, setResult)

	// Get the value
	getResult := callTool(t, ctx, session, "balda.state.get", map[string]any{
		"key": "mykey",
	})
	payload := structuredResultMap(t, getResult)
	if payload["found"] != true {
		t.Fatal("found = false, want true")
	}
	if payload["value"] != "myvalue" {
		t.Fatalf("value = %v, want myvalue", payload["value"])
	}
}

func TestGetMissingKey(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	getResult := callTool(t, ctx, session, "balda.state.get", map[string]any{
		"key": "nonexistent",
	})
	payload := structuredResultMap(t, getResult)
	if payload["found"] != false {
		t.Fatal("found = true, want false")
	}
}

func TestSetGetJSON(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	// Set a JSON value
	setResult := callTool(t, ctx, session, "balda.state.set_json", map[string]any{
		"key": "config",
		"value": map[string]any{
			"timeout": 30,
			"retries": 3,
		},
	})
	assertOk(t, setResult)

	// Get as JSON
	getResult := callTool(t, ctx, session, "balda.state.get_json", map[string]any{
		"key": "config",
	})
	payload := structuredResultMap(t, getResult)
	if payload["found"] != true {
		t.Fatal("found = false, want true")
	}
	config, ok := payload["value"].(map[string]any)
	if !ok {
		t.Fatalf("value type = %T, want map[string]any", payload["value"])
	}
	if config["timeout"] != float64(30) {
		t.Fatalf("timeout = %v, want 30", config["timeout"])
	}
}

func TestMergeJSON(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	// Set initial value
	_ = callTool(t, ctx, session, "balda.state.set_json", map[string]any{
		"key": "state",
		"value": map[string]any{
			"count": 1,
			"name":  "test",
		},
	})

	// Merge new fields
	mergeResult := callTool(t, ctx, session, "balda.state.merge_json", map[string]any{
		"key": "state",
		"value": map[string]any{
			"count": 2,
			"extra": "field",
		},
	})
	payload := structuredResultMap(t, mergeResult)
	merged := payload["merged"].(map[string]any)
	if merged["count"] != float64(2) {
		t.Fatalf("count = %v, want 2", merged["count"])
	}
	if merged["name"] != "test" {
		t.Fatalf("name = %v, want test", merged["name"])
	}
	if merged["extra"] != "field" {
		t.Fatalf("extra = %v, want field", merged["extra"])
	}
}

func TestListKeys(t *testing.T) {
	ResetSharedStore()
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	// Set multiple keys
	for _, k := range []string{"a:1", "a:2", "b:1"} {
		_ = callTool(t, ctx, session, "balda.state.set", map[string]any{"key": k, "value": "v"})
	}

	// List all
	allResult := callTool(t, ctx, session, "balda.state.list", map[string]any{})
	allPayload := structuredResultMap(t, allResult)
	allKeys := allPayload["keys"].([]any)
	if len(allKeys) != 3 {
		t.Fatalf("all keys count = %d, want 3", len(allKeys))
	}

	// List with prefix
	prefixResult := callTool(t, ctx, session, "balda.state.list", map[string]any{"prefix": "a:"})
	prefixPayload := structuredResultMap(t, prefixResult)
	prefixKeys := prefixPayload["keys"].([]any)
	if len(prefixKeys) != 2 {
		t.Fatalf("a: prefix keys count = %d, want 2", len(prefixKeys))
	}
}

func TestDeleteAndClearAffectMemoryStore(t *testing.T) {
	ResetSharedStore()
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	assertOk(t, callTool(t, ctx, session, "balda.state.set", map[string]any{"key": "keep", "value": "v"}))
	assertOk(t, callTool(t, ctx, session, "balda.state.set", map[string]any{"key": "remove", "value": "v"}))
	assertOk(t, callTool(t, ctx, session, "balda.state.delete", map[string]any{"key": "remove"}))

	deleted := callTool(t, ctx, session, "balda.state.get", map[string]any{"key": "remove"})
	if payload := structuredResultMap(t, deleted); payload["found"] != false {
		t.Fatalf("deleted key found = %v, want false", payload["found"])
	}
	remaining := callTool(t, ctx, session, "balda.state.list", map[string]any{})
	remainingPayload := structuredResultMap(t, remaining)
	remainingKeys := remainingPayload["keys"].([]any)
	if len(remainingKeys) != 1 || remainingKeys[0] != "keep" {
		t.Fatalf("remaining keys = %v, want [keep]", remainingKeys)
	}

	assertOk(t, callTool(t, ctx, session, "balda.state.clear", map[string]any{}))
	cleared := callTool(t, ctx, session, "balda.state.list", map[string]any{})
	clearedPayload := structuredResultMap(t, cleared)
	if clearedKeys, ok := clearedPayload["keys"].([]any); ok && len(clearedKeys) != 0 {
		t.Fatalf("keys after clear = %v, want empty", clearedKeys)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	// Set in namespace "agent1"
	_ = callTool(t, ctx, session, "balda.state.ns_set", map[string]any{
		"namespace": "agent1",
		"key":       "state",
		"value":     "value1",
	})

	// Set in namespace "agent2"
	_ = callTool(t, ctx, session, "balda.state.ns_set", map[string]any{
		"namespace": "agent2",
		"key":       "state",
		"value":     "value2",
	})

	// Get from agent1
	get1 := callTool(t, ctx, session, "balda.state.ns_get", map[string]any{
		"namespace": "agent1",
		"key":       "state",
	})
	payload1 := structuredResultMap(t, get1)
	if payload1["value"] != "value1" {
		t.Fatalf("agent1 value = %v, want value1", payload1["value"])
	}

	// Get from agent2
	get2 := callTool(t, ctx, session, "balda.state.ns_get", map[string]any{
		"namespace": "agent2",
		"key":       "state",
	})
	payload2 := structuredResultMap(t, get2)
	if payload2["value"] != "value2" {
		t.Fatalf("agent2 value = %v, want value2", payload2["value"])
	}

	// List keys in agent1 namespace
	list1 := callTool(t, ctx, session, "balda.state.ns_list", map[string]any{
		"namespace": "agent1",
	})
	listPayload1 := structuredResultMap(t, list1)
	ns1Keys := listPayload1["keys"].([]any)
	if len(ns1Keys) != 1 || ns1Keys[0] != "state" {
		t.Fatalf("agent1 keys = %v, want [state]", ns1Keys)
	}
}

func TestValidationErrors(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore(), nil)
	defer cleanup()
	_ = session.InitializeResult()

	tests := []struct {
		name     string
		toolName string
		args     map[string]any
	}{
		{"get empty key", "balda.state.get", map[string]any{"key": "   "}},
		{"set empty key", "balda.state.set", map[string]any{"key": "  ", "value": "v"}},
		{"ns_get empty namespace", "balda.state.ns_get", map[string]any{"namespace": "  ", "key": "k"}},
		{"ns_set empty namespace", "balda.state.ns_set", map[string]any{"namespace": "  ", "key": "k", "value": "v"}},
		{"ns_list empty namespace", "balda.state.ns_list", map[string]any{"namespace": "  "}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := callTool(t, ctx, session, tc.toolName, tc.args)
			if !result.IsError {
				t.Fatalf("result.IsError = false, want true")
			}
			payload := structuredResultMap(t, result)
			errObj := payload["error"].(map[string]any)
			if errObj["code"] != codeValidationError {
				t.Fatalf("error.code = %v, want %q", errObj["code"], codeValidationError)
			}
		})
	}
}

func TestSharedStateAcrossStores(t *testing.T) {
	// Reset shared state for this test
	ResetSharedStore()

	store1 := NewMemoryStore()
	store2 := NewMemoryStore()

	ctx := context.Background()

	// Set value via store1
	if err := store1.Set(ctx, "shared-key", "shared-value"); err != nil {
		t.Fatalf("store1.Set() error = %v", err)
	}

	// Get value via store2 (different instance, same underlying state)
	val, ok, err := store2.Get(ctx, "shared-key")
	if err != nil {
		t.Fatalf("store2.Get() error = %v", err)
	}
	if !ok {
		t.Fatal("store2.Get() found = false, want true")
	}
	if val != "shared-value" {
		t.Fatalf("store2.Get() value = %q, want %q", val, "shared-value")
	}
}

// Test helpers

func newTestSession(t *testing.T, store Store, waitScheduler SessionWaitService) (context.Context, func(), *mcp.ClientSession) {
	return newTestSessionWithQuestionService(t, store, waitScheduler, nil)
}

func newTestSessionWithQuestionService(t *testing.T, store Store, waitScheduler SessionWaitService, questionService SessionQuestionService) (context.Context, func(), *mcp.ClientSession) {
	t.Helper()
	if store == nil {
		t.Fatal("store is required")
	}
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-session-state", Version: "1.0.0"},
		nil,
	)
	RegisterTools(server, store, waitScheduler, questionService)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = server.Run(ctx, serverTransport)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		cancel()
		t.Fatalf("client.Connect() error = %v", err)
	}

	cleanup := func() {
		cancel()
		_ = session.Close()
	}
	return ctx, cleanup, session
}

func callTool(t *testing.T, ctx context.Context, session *mcp.ClientSession, toolName string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: toolName, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s) error = %v", toolName, err)
	}
	return result
}

func structuredResultMap(t *testing.T, result *mcp.CallToolResult) map[string]any {
	t.Helper()
	if result == nil {
		t.Fatal("result is nil")
	}
	switch typed := result.StructuredContent.(type) {
	case map[string]any:
		return typed
	case json.RawMessage:
		var decoded map[string]any
		if err := json.Unmarshal(typed, &decoded); err != nil {
			t.Fatalf("json.Unmarshal(structured content) error = %v", err)
		}
		return decoded
	case nil:
		if len(result.Content) > 0 {
			if textContent, ok := result.Content[0].(*mcp.TextContent); ok {
				var decoded map[string]any
				if err := json.Unmarshal([]byte(textContent.Text), &decoded); err == nil {
					return decoded
				}
			}
		}
		t.Fatalf("result.StructuredContent is nil")
	default:
		t.Fatalf("unexpected structured content type %T", result.StructuredContent)
	}
	return nil
}

func assertOk(t *testing.T, result *mcp.CallToolResult) {
	t.Helper()
	if result.IsError {
		t.Fatalf("result.IsError = true, want false; content=%v", result.Content)
	}
	payload := structuredResultMap(t, result)
	if payload["ok"] != true {
		t.Fatalf("ok = %v, want true", payload["ok"])
	}
}
