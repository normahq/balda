package sessionmcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestNewServerRequiresStore(t *testing.T) {
	_, err := NewServer(nil)
	if err == nil {
		t.Fatal("NewServer(nil) error = nil, want non-nil")
	}
}

func TestSessionStateServerListsTools(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	initResult := session.InitializeResult()
	if !strings.Contains(initResult.Instructions, "persist relay state") {
		t.Fatalf("InitializeResult().Instructions = %q, want session-state guidance", initResult.Instructions)
	}

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	got := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		got = append(got, tool.Name)
	}

	want := []string{
		"relay.state.clear",
		"relay.state.delete",
		"relay.state.get",
		"relay.state.get_json",
		"relay.state.list",
		"relay.state.merge_json",
		"relay.state.set",
		"relay.state.set_json",
		"relay.state.ns_get",
		"relay.state.ns_list",
		"relay.state.ns_set",
		"relay.state.ns_set_json",
	}

	if len(got) != len(want) {
		t.Fatalf("tool count = %d, want %d\ngot: %v\nwant: %v", len(got), len(want), got, want)
	}
}

func TestSessionStateToolDescriptionsAndSchemas(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
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

	if got := toolByName["relay.state.clear"].Description; !strings.Contains(got, "destructive") {
		t.Fatalf("relay.state.clear description = %q, want destructive warning", got)
	}
	if got := toolByName["relay.state.ns_set"].Description; !strings.Contains(got, "session or agent isolation") {
		t.Fatalf("relay.state.ns_set description = %q, want namespace guidance", got)
	}

	outSchema, ok := toolByName["relay.state.get"].OutputSchema.(map[string]any)
	if !ok {
		t.Fatalf("relay.state.get output schema type = %T, want map[string]any", toolByName["relay.state.get"].OutputSchema)
	}
	properties := outSchema["properties"].(map[string]any)
	found := properties["found"].(map[string]any)
	if got := found["description"]; got != "whether the key exists" {
		t.Fatalf("relay.state.get found description = %v, want whether the key exists", got)
	}
}

func TestSetGetBasic(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	// Set a value
	setResult := callTool(t, ctx, session, "relay.state.set", map[string]any{
		"key":   "mykey",
		"value": "myvalue",
	})
	assertOk(t, setResult)

	// Get the value
	getResult := callTool(t, ctx, session, "relay.state.get", map[string]any{
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
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	getResult := callTool(t, ctx, session, "relay.state.get", map[string]any{
		"key": "nonexistent",
	})
	payload := structuredResultMap(t, getResult)
	if payload["found"] != false {
		t.Fatal("found = true, want false")
	}
}

func TestSetGetJSON(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	// Set a JSON value
	setResult := callTool(t, ctx, session, "relay.state.set_json", map[string]any{
		"key": "config",
		"value": map[string]any{
			"timeout": 30,
			"retries": 3,
		},
	})
	assertOk(t, setResult)

	// Get as JSON
	getResult := callTool(t, ctx, session, "relay.state.get_json", map[string]any{
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
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	// Set initial value
	_ = callTool(t, ctx, session, "relay.state.set_json", map[string]any{
		"key": "state",
		"value": map[string]any{
			"count": 1,
			"name":  "test",
		},
	})

	// Merge new fields
	mergeResult := callTool(t, ctx, session, "relay.state.merge_json", map[string]any{
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
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	// Set multiple keys
	for _, k := range []string{"a:1", "a:2", "b:1"} {
		_ = callTool(t, ctx, session, "relay.state.set", map[string]any{"key": k, "value": "v"})
	}

	// List all
	allResult := callTool(t, ctx, session, "relay.state.list", map[string]any{})
	allPayload := structuredResultMap(t, allResult)
	allKeys := allPayload["keys"].([]any)
	if len(allKeys) != 3 {
		t.Fatalf("all keys count = %d, want 3", len(allKeys))
	}

	// List with prefix
	prefixResult := callTool(t, ctx, session, "relay.state.list", map[string]any{"prefix": "a:"})
	prefixPayload := structuredResultMap(t, prefixResult)
	prefixKeys := prefixPayload["keys"].([]any)
	if len(prefixKeys) != 2 {
		t.Fatalf("a: prefix keys count = %d, want 2", len(prefixKeys))
	}
}

func TestNamespaceIsolation(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	// Set in namespace "agent1"
	_ = callTool(t, ctx, session, "relay.state.ns_set", map[string]any{
		"namespace": "agent1",
		"key":       "state",
		"value":     "value1",
	})

	// Set in namespace "agent2"
	_ = callTool(t, ctx, session, "relay.state.ns_set", map[string]any{
		"namespace": "agent2",
		"key":       "state",
		"value":     "value2",
	})

	// Get from agent1
	get1 := callTool(t, ctx, session, "relay.state.ns_get", map[string]any{
		"namespace": "agent1",
		"key":       "state",
	})
	payload1 := structuredResultMap(t, get1)
	if payload1["value"] != "value1" {
		t.Fatalf("agent1 value = %v, want value1", payload1["value"])
	}

	// Get from agent2
	get2 := callTool(t, ctx, session, "relay.state.ns_get", map[string]any{
		"namespace": "agent2",
		"key":       "state",
	})
	payload2 := structuredResultMap(t, get2)
	if payload2["value"] != "value2" {
		t.Fatalf("agent2 value = %v, want value2", payload2["value"])
	}

	// List keys in agent1 namespace
	list1 := callTool(t, ctx, session, "relay.state.ns_list", map[string]any{
		"namespace": "agent1",
	})
	listPayload1 := structuredResultMap(t, list1)
	ns1Keys := listPayload1["keys"].([]any)
	if len(ns1Keys) != 1 || ns1Keys[0] != "state" {
		t.Fatalf("agent1 keys = %v, want [state]", ns1Keys)
	}
}

func TestValidationErrors(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, NewMemoryStore())
	defer cleanup()
	_ = session.InitializeResult()

	tests := []struct {
		name     string
		toolName string
		args     map[string]any
	}{
		{"get empty key", "relay.state.get", map[string]any{"key": "   "}},
		{"set empty key", "relay.state.set", map[string]any{"key": "  ", "value": "v"}},
		{"ns_get empty namespace", "relay.state.ns_get", map[string]any{"namespace": "  ", "key": "k"}},
		{"ns_set empty namespace", "relay.state.ns_set", map[string]any{"namespace": "  ", "key": "k", "value": "v"}},
		{"ns_list empty namespace", "relay.state.ns_list", map[string]any{"namespace": "  "}},
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

func TestRunHTTPRequiresStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunHTTP(ctx, nil, "localhost:0")
	if err == nil {
		t.Fatal("RunHTTP(nil store) error = nil, want non-nil")
	}
}

func TestStartHTTPServerStartsAndReturnsAddr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := StartHTTPServer(ctx, NewMemoryStore(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	defer func() {
		if err := result.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if result.Addr == "" {
		t.Fatal("StartHTTPServer() Addr is empty")
	}
	if result.server == nil {
		t.Fatal("StartHTTPServer() server is nil")
	}
	if result.server.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", result.server.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if result.server.IdleTimeout != httpIdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", result.server.IdleTimeout, httpIdleTimeout)
	}
	if result.server.ReadTimeout != 0 || result.server.WriteTimeout != 0 {
		t.Fatalf("streaming MCP server read/write timeouts = %s/%s, want unset", result.server.ReadTimeout, result.server.WriteTimeout)
	}
}

// Test helpers

func newTestSession(t *testing.T, store Store) (context.Context, func(), *mcp.ClientSession) {
	t.Helper()

	server, err := NewServer(store)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

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
