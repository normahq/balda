package sessionturnapp

import (
	"testing"

	"google.golang.org/genai"
)

func TestToolFailureFromFunctionResponseExtractsMCPFailure(t *testing.T) {
	t.Parallel()

	got, ok := toolFailureFromFunctionResponse(&genai.FunctionResponse{
		ID:   "call-1",
		Name: "tool_call_update",
		Response: map[string]any{
			"status": "failed",
			"rawOutput": map[string]any{
				"server": "balda",
				"tool":   "balda.session.wait",
				"status": "failed",
				"arguments": map[string]any{
					"token": "must-not-be-logged",
				},
				"result": map[string]any{
					"structuredContent": map[string]any{
						"error": map[string]any{
							"code":    "backend_error",
							"message": "address_json is required",
						},
					},
				},
			},
		},
	})
	if !ok {
		t.Fatal("toolFailureFromFunctionResponse() ok = false, want true")
	}
	if got.ToolName != "balda.session.wait" || got.Server != "balda" || got.Status != "failed" || got.Code != "backend_error" || got.Message != "address_json is required" {
		t.Fatalf("toolFailureFromFunctionResponse() = %+v", got)
	}
}

func TestToolFailureFromFunctionResponseIgnoresSuccess(t *testing.T) {
	t.Parallel()

	_, ok := toolFailureFromFunctionResponse(&genai.FunctionResponse{Response: map[string]any{
		"status": "completed",
		"rawOutput": map[string]any{
			"tool":   "balda.session.wait",
			"status": "completed",
		},
	}})
	if ok {
		t.Fatal("toolFailureFromFunctionResponse() ok = true, want false")
	}
}
