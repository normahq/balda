package sessionmcp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	serverName    = "norma-session-state"
	serverVersion = "1.0.0"

	codeValidationError = "validation_error"
	codeBackendError    = "backend_error"

	httpReadHeaderTimeout = 5 * time.Second
	httpIdleTimeout       = 60 * time.Second
)

const serverInstructions = `Use this server to persist relay state in relay.db.

- relay.state.* reads and writes session or app key-value data.
- relay.state.ns_* scopes keys under an explicit namespace such as a session ID or agent name.
- relay.state.clear deletes all data owned by this server and is destructive.
- relay.state.get_json, set_json, and merge_json are for JSON values rather than raw strings.`

// Run serves the session state MCP server over stdio.
func Run(ctx context.Context, store Store) error {
	server, err := NewServer(store)
	if err != nil {
		return err
	}
	return server.Run(ctx, &mcp.StdioTransport{})
}

// RunHTTP serves the session state MCP server over HTTP.
func RunHTTP(ctx context.Context, store Store, addr string) error {
	result, err := StartHTTPServer(ctx, store, addr)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return result.Close()
}

// HTTPServerResult contains the address and cleanup function for an embedded HTTP server.
type HTTPServerResult struct {
	// Addr is the actual listen address (e.g., "127.0.0.1:54321").
	Addr string
	// Close shuts down the server.
	Close func() error

	server *http.Server
}

// StartHTTPServer starts an HTTP server on the given address and returns immediately.
// Use ":0" to let the OS assign a random port.
func StartHTTPServer(ctx context.Context, store Store, addr string) (*HTTPServerResult, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("address is required")
	}

	getServer := func(_ *http.Request) *mcp.Server {
		server, err := NewServer(store)
		if err != nil {
			return nil
		}
		return server
	}

	handler := mcp.NewStreamableHTTPHandler(getServer, &mcp.StreamableHTTPOptions{})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", addr, err)
	}

	actualAddr := listener.Addr().String()
	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	go func() {
		<-ctx.Done()
		_ = httpServer.Close()
	}()

	go func() {
		_ = httpServer.Serve(listener)
	}()

	return &HTTPServerResult{
		Addr:   actualAddr,
		server: httpServer,
		Close: func() error {
			return httpServer.Close()
		},
	}, nil
}

// NewServer builds the session state MCP server.
func NewServer(store Store) (*mcp.Server, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}

	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    serverName,
			Version: serverVersion,
		},
		&mcp.ServerOptions{Instructions: serverInstructions},
	)

	RegisterTools(server, store)
	return server, nil
}

// RegisterTools adds session-state MCP tools to an existing server.
func RegisterTools(server *mcp.Server, store Store) {
	if server == nil || store == nil {
		return
	}
	svc := &service{store: store}
	svc.registerTools(server)
}

type service struct {
	store Store
}

func (s *service) registerTools(server *mcp.Server) {
	// Basic key-value operations
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.get", Description: "Read a raw string value from persistent relay state by exact key."}, s.getKey)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.set", Description: "Write a raw string value to persistent relay state under an exact key."}, s.setKey)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.delete", Description: "Delete one exact key from persistent relay state."}, s.deleteKey)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.list", Description: "List persistent relay-state keys, optionally restricted to a prefix."}, s.listKeys)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.clear", Description: "Delete all keys stored by relay.state. This is destructive and affects every session using this state store."}, s.clearState)

	// JSON operations
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.get_json", Description: "Read a key from persistent relay state and return its parsed JSON value."}, s.getJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.set_json", Description: "Write a JSON value to persistent relay state under an exact key."}, s.setJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.merge_json", Description: "Merge object fields into an existing JSON object stored at a key and return the merged object."}, s.mergeJSON)

	// Namespaced operations for agent/session isolation
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.ns_get", Description: "Read a raw string value from a namespace-scoped key. Use namespaces such as session IDs or agent names to avoid collisions."}, s.nsGet)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.ns_set", Description: "Write a raw string value to a namespace-scoped key for session or agent isolation."}, s.nsSet)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.ns_set_json", Description: "Write a JSON value to a namespace-scoped key for session or agent isolation."}, s.nsSetJSON)
	mcp.AddTool(server, &mcp.Tool{Name: "relay.state.ns_list", Description: "List keys stored inside one namespace without returning keys from other namespaces."}, s.nsList)
}

// nsKey builds a namespaced key for isolation.
func nsKey(namespace, key string) string {
	return fmt.Sprintf("ns:%s:%s", strings.TrimSpace(namespace), strings.TrimSpace(key))
}

// Basic key-value tools

func (s *service) getKey(ctx context.Context, _ *mcp.CallToolRequest, in getKeyInput) (*mcp.CallToolResult, getKeyOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.get", "key is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.Get(ctx, key)
	if err != nil {
		result, out := backendFailure("relay.state.get", err)
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	return nil, getKeyOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) setKey(ctx context.Context, _ *mcp.CallToolRequest, in setKeyInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.set", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Set(ctx, key, in.Value); err != nil {
		result, out := backendFailure("relay.state.set", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) deleteKey(ctx context.Context, _ *mcp.CallToolRequest, in deleteKeyInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.delete", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Delete(ctx, key); err != nil {
		result, out := backendFailure("relay.state.delete", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) listKeys(ctx context.Context, _ *mcp.CallToolRequest, in listKeysInput) (*mcp.CallToolResult, listKeysOutput, error) {
	prefix := strings.TrimSpace(in.Prefix)

	keys, err := s.store.List(ctx, prefix)
	if err != nil {
		result, out := backendFailure("relay.state.list", err)
		return result, listKeysOutput{ToolOutcome: out}, nil
	}
	return nil, listKeysOutput{ToolOutcome: okOutcome(), Keys: keys}, nil
}

func (s *service) clearState(ctx context.Context, _ *mcp.CallToolRequest, _ noInput) (*mcp.CallToolResult, basicOutput, error) {
	if err := s.store.Clear(ctx); err != nil {
		result, out := backendFailure("relay.state.clear", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

// JSON tools

func (s *service) getJSON(ctx context.Context, _ *mcp.CallToolRequest, in getJSONInput) (*mcp.CallToolResult, getJSONOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.get_json", "key is required")
		return result, getJSONOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.GetJSON(ctx, key)
	if err != nil {
		result, out := backendFailure("relay.state.get_json", err)
		return result, getJSONOutput{ToolOutcome: out}, nil
	}
	return nil, getJSONOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) setJSON(ctx context.Context, _ *mcp.CallToolRequest, in setJSONInput) (*mcp.CallToolResult, basicOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.set_json", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.SetJSON(ctx, key, in.Value); err != nil {
		result, out := backendFailure("relay.state.set_json", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) mergeJSON(ctx context.Context, _ *mcp.CallToolRequest, in mergeJSONInput) (*mcp.CallToolResult, mergeJSONOutput, error) {
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.merge_json", "key is required")
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}
	if len(in.Value) == 0 {
		result, out := validationFailure("relay.state.merge_json", "value must have at least one field")
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}

	merged, err := s.store.MergeJSON(ctx, key, in.Value)
	if err != nil {
		result, out := backendFailure("relay.state.merge_json", err)
		return result, mergeJSONOutput{ToolOutcome: out}, nil
	}
	return nil, mergeJSONOutput{ToolOutcome: okOutcome(), Merged: merged}, nil
}

// Namespaced tools

func (s *service) nsGet(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceInput) (*mcp.CallToolResult, getKeyOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("relay.state.ns_get", "namespace is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.ns_get", "key is required")
		return result, getKeyOutput{ToolOutcome: out}, nil
	}

	value, ok, err := s.store.Get(ctx, nsKey(namespace, key))
	if err != nil {
		result, out := backendFailure("relay.state.ns_get", err)
		return result, getKeyOutput{ToolOutcome: out}, nil
	}
	return nil, getKeyOutput{ToolOutcome: okOutcome(), Value: value, Found: ok}, nil
}

func (s *service) nsSet(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceValueInput) (*mcp.CallToolResult, basicOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("relay.state.ns_set", "namespace is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.ns_set", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.Set(ctx, nsKey(namespace, key), in.Value); err != nil {
		result, out := backendFailure("relay.state.ns_set", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) nsSetJSON(ctx context.Context, _ *mcp.CallToolRequest, in keyspaceJSONInput) (*mcp.CallToolResult, basicOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("relay.state.ns_set_json", "namespace is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}
	key := strings.TrimSpace(in.Key)
	if key == "" {
		result, out := validationFailure("relay.state.ns_set_json", "key is required")
		return result, basicOutput{ToolOutcome: out}, nil
	}

	if err := s.store.SetJSON(ctx, nsKey(namespace, key), in.Value); err != nil {
		result, out := backendFailure("relay.state.ns_set_json", err)
		return result, basicOutput{ToolOutcome: out}, nil
	}
	return nil, basicOutput{ToolOutcome: okOutcome()}, nil
}

func (s *service) nsList(ctx context.Context, _ *mcp.CallToolRequest, in namespaceOnlyInput) (*mcp.CallToolResult, listKeysOutput, error) {
	namespace := strings.TrimSpace(in.Namespace)
	if namespace == "" {
		result, out := validationFailure("relay.state.ns_list", "namespace is required")
		return result, listKeysOutput{ToolOutcome: out}, nil
	}

	prefix := nsKey(namespace, "")
	keys, err := s.store.List(ctx, prefix)
	if err != nil {
		result, out := backendFailure("relay.state.ns_list", err)
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
