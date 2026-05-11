package workspacemcp

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
	serverName     = "norma-workspace"
	serverVersion  = "1.0.0"
	defaultAddress = "127.0.0.1:9091"

	httpReadHeaderTimeout = 5 * time.Second
	httpIdleTimeout       = 60 * time.Second
)

const serverInstructions = `Use this server to sync and land relay workspaces when relay workspace mode is enabled.

- relay.workspace.import rebases the session workspace branch onto the configured base branch.
- relay.workspace.import discards uncommitted workspace changes before rebasing.
- relay.workspace.export squash-merges the session workspace branch into the configured base branch.
- relay.workspace.export requires a Conventional Commit message.`

type ToolError struct {
	Operation string `json:"operation" jsonschema:"tool name that produced the error"`
	Code      string `json:"code" jsonschema:"stable machine-readable error code"`
	Message   string `json:"message" jsonschema:"human-readable error message"`
}

type ToolOutcome struct {
	OK    bool       `json:"ok" jsonschema:"true when the tool completed successfully"`
	Error *ToolError `json:"error,omitempty" jsonschema:"error details when ok is false"`
}

func okOutcome() ToolOutcome {
	return ToolOutcome{OK: true}
}

func validationFailure(operation string, message string) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, "validation_error", message)
}

func backendFailure(operation string, err error) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, "backend_error", err.Error())
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

// WorkspaceService defines workspace sync operations.
type WorkspaceService interface {
	Import(ctx context.Context, sessionID string) error
	Export(ctx context.Context, sessionID string, commitMessage string) error
}

func Run(ctx context.Context, svc WorkspaceService) error {
	server, err := NewServer(svc)
	if err != nil {
		return err
	}
	return server.Run(ctx, &mcp.StdioTransport{})
}

func RunHTTP(ctx context.Context, svc WorkspaceService, addr string) error {
	result, err := StartHTTPServer(ctx, svc, addr)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return result.Close()
}

type HTTPServerResult struct {
	Addr  string
	Close func() error

	server *http.Server
}

func StartHTTPServer(ctx context.Context, svc WorkspaceService, addr string) (*HTTPServerResult, error) {
	if svc == nil {
		return nil, fmt.Errorf("service is required")
	}
	address := strings.TrimSpace(addr)
	if address == "" {
		address = defaultAddress
	}

	getServer := func(_ *http.Request) *mcp.Server {
		server, err := NewServer(svc)
		if err != nil {
			return nil
		}
		return server
	}

	handler := mcp.NewStreamableHTTPHandler(getServer, &mcp.StreamableHTTPOptions{})

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", address, err)
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

func NewServer(svc WorkspaceService) (*mcp.Server, error) {
	if svc == nil {
		return nil, fmt.Errorf("service is required")
	}

	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    serverName,
			Version: serverVersion,
		},
		&mcp.ServerOptions{Instructions: serverInstructions},
	)

	RegisterTools(server, svc)
	return server, nil
}

// RegisterTools adds workspace MCP tools to an existing server.
func RegisterTools(server *mcp.Server, svc WorkspaceService) {
	if server == nil || svc == nil {
		return
	}
	srv := &service{svc: svc}
	srv.registerTools(server)
}

type service struct {
	svc WorkspaceService
}

func (s *service) registerTools(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "relay.workspace.import",
		Description: "Rebase the session workspace branch onto the configured base branch. Requires workspace mode and discards uncommitted workspace changes before rebasing.",
	}, s.importTool)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "relay.workspace.export",
		Description: "Squash-merge the session workspace branch into the configured base branch and create a commit using the provided Conventional Commit message. Requires workspace mode.",
	}, s.exportTool)
}

type importInput struct {
	SessionID string `json:"session_id" jsonschema:"relay session ID whose workspace should be rebased onto the configured base branch"`
}

type importOutput struct {
	ToolOutcome
}

func (s *service) importTool(ctx context.Context, _ *mcp.CallToolRequest, in importInput) (*mcp.CallToolResult, importOutput, error) {
	if strings.TrimSpace(in.SessionID) == "" {
		result, out := validationFailure("relay.workspace.import", "session_id is required")
		return result, importOutput{ToolOutcome: out}, nil
	}

	if err := s.svc.Import(ctx, in.SessionID); err != nil {
		result, out := backendFailure("relay.workspace.import", err)
		return result, importOutput{ToolOutcome: out}, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: "Workspace synced to base branch successfully"}},
	}, importOutput{ToolOutcome: okOutcome()}, nil
}

type exportInput struct {
	SessionID     string `json:"session_id" jsonschema:"relay session ID whose workspace should be exported to the configured base branch"`
	CommitMessage string `json:"commit_message" jsonschema:"Conventional Commit message for the squash-merge commit"`
}

type exportOutput struct {
	ToolOutcome
}

func (s *service) exportTool(ctx context.Context, _ *mcp.CallToolRequest, in exportInput) (*mcp.CallToolResult, exportOutput, error) {
	if strings.TrimSpace(in.SessionID) == "" {
		result, out := validationFailure("relay.workspace.export", "session_id is required")
		return result, exportOutput{ToolOutcome: out}, nil
	}
	if strings.TrimSpace(in.CommitMessage) == "" {
		result, out := validationFailure("relay.workspace.export", "commit_message is required")
		return result, exportOutput{ToolOutcome: out}, nil
	}

	if err := s.svc.Export(ctx, in.SessionID, in.CommitMessage); err != nil {
		result, out := backendFailure("relay.workspace.export", err)
		return result, exportOutput{ToolOutcome: out}, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: "Workspace exported to base branch successfully"}},
	}, exportOutput{ToolOutcome: okOutcome()}, nil
}
