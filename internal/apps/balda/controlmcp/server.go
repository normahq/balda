package controlmcp

import (
	"context"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"go.uber.org/fx"
)

const (
	codeValidationError = "validation_error"
	codeBackendError    = "backend_error"
	shutdownConfirmWord = "shutdown"
	shutdownSignalDelay = 100 * time.Millisecond
)

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

func validationFailure(operation, message string) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, codeValidationError, message)
}

func backendFailure(operation string, err error) (*mcp.CallToolResult, ToolOutcome) {
	return failure(operation, codeBackendError, err.Error())
}

func failure(operation, code, message string) (*mcp.CallToolResult, ToolOutcome) {
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

type shutdownOutput struct {
	ToolOutcome
	Requested bool   `json:"requested" jsonschema:"true when graceful shutdown was requested successfully"`
	Message   string `json:"message,omitempty" jsonschema:"human-readable status message"`
}

type shutdownInput struct {
	Confirm string `json:"confirm" jsonschema:"must be the exact word 'shutdown' to confirm graceful process shutdown"`
	Reason  string `json:"reason,omitempty" jsonschema:"optional short reason for the shutdown request"`
}

type service struct {
	shutdowner fx.Shutdowner
	dispatcher actortransport.Dispatcher
	terminate  func() error
}

// RegisterTools adds control MCP tools to an existing server.
func RegisterTools(server *mcp.Server, shutdowner fx.Shutdowner, dispatcher actortransport.Dispatcher) {
	registerTools(server, shutdowner, dispatcher, terminateCurrentProcess)
}

func registerTools(server *mcp.Server, shutdowner fx.Shutdowner, dispatcher actortransport.Dispatcher, terminate func() error) {
	if server == nil || shutdowner == nil {
		return
	}
	if terminate == nil {
		terminate = terminateCurrentProcess
	}
	srv := &service{shutdowner: shutdowner, dispatcher: dispatcher, terminate: terminate}
	mcp.AddTool(server, &mcp.Tool{
		Name:        "balda.control.shutdown",
		Description: "Gracefully stop the running Balda process. This affects the whole instance. Requires confirm='shutdown' and should only be used when the user explicitly asks for restart or shutdown. This is the preferred restart path after installing a new override binary; use kill -TERM 1 only as a fallback when graceful shutdown is unavailable or broken.",
	}, srv.shutdown)
}

func (s *service) shutdown(_ context.Context, _ *mcp.CallToolRequest, in shutdownInput) (*mcp.CallToolResult, shutdownOutput, error) {
	if strings.TrimSpace(in.Confirm) != shutdownConfirmWord {
		result, out := validationFailure("balda.control.shutdown", "confirm must be the exact word 'shutdown'")
		return result, shutdownOutput{ToolOutcome: out}, nil
	}

	if err := s.shutdowner.Shutdown(fx.ExitCode(0)); err != nil {
		result, out := backendFailure("balda.control.shutdown", err)
		return result, shutdownOutput{ToolOutcome: out}, nil
	}
	if s.terminate != nil {
		go func(terminate func() error) {
			time.Sleep(shutdownSignalDelay)
			_ = terminate()
		}(s.terminate)
	}

	message := "graceful shutdown requested"
	if reason := strings.TrimSpace(in.Reason); reason != "" {
		message += ": " + reason
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
	}, shutdownOutput{ToolOutcome: okOutcome(), Requested: true, Message: message}, nil
}

func terminateCurrentProcess() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Signal(syscall.SIGTERM)
}
