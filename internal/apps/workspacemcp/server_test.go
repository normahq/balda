package workspacemcp

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestWorkspaceServerPublishesInstructionsAndToolDescriptions(t *testing.T) {
	ctx, cleanup, session := newTestSession(t, fakeWorkspaceService{})
	defer cleanup()

	initResult := session.InitializeResult()
	if !strings.Contains(initResult.Instructions, "workspace mode is enabled") {
		t.Fatalf("InitializeResult().Instructions = %q, want workspace guidance", initResult.Instructions)
	}

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	toolByName := map[string]*mcp.Tool{}
	for _, tool := range tools.Tools {
		toolByName[tool.Name] = tool
	}

	if got := toolByName["relay.workspace.import"].Description; !strings.Contains(got, "discards uncommitted workspace changes") {
		t.Fatalf("relay.workspace.import description = %q, want destructive import guidance", got)
	}
	if got := toolByName["relay.workspace.export"].Description; !strings.Contains(got, "Conventional Commit") {
		t.Fatalf("relay.workspace.export description = %q, want commit-message guidance", got)
	}
}

type fakeWorkspaceService struct{}

func (fakeWorkspaceService) Import(_ context.Context, _ string) error {
	return nil
}

func (fakeWorkspaceService) Export(_ context.Context, _ string, _ string) error {
	return nil
}

func newTestSession(t *testing.T, svc WorkspaceService) (context.Context, func(), *mcp.ClientSession) {
	t.Helper()

	server, err := NewServer(svc)
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

func TestStartHTTPServerUsesStreamingSafeTimeouts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := StartHTTPServer(ctx, &fakeWorkspaceService{}, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	t.Cleanup(func() {
		_ = result.Close()
	})

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
