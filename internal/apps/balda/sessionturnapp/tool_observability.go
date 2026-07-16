package sessionturnapp

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/redaction"
	"google.golang.org/genai"
)

const maxToolErrorLogLength = 2048

type toolFailure struct {
	ToolName string
	Server   string
	Status   string
	Code     string
	Message  string
}

func toolFailureFromFunctionResponse(response *genai.FunctionResponse) (toolFailure, bool) {
	if response == nil || len(response.Response) == 0 {
		return toolFailure{}, false
	}
	failure := toolFailure{ToolName: strings.TrimSpace(response.Name), Status: stringValue(response.Response["status"])}
	errorValue := response.Response["error"]
	if rawOutput, ok := mapValue(response.Response["rawOutput"]); ok {
		failure.ToolName = stringValue(rawOutput["tool"])
		failure.Server = stringValue(rawOutput["server"])
		failure.Status = firstNonEmptyString(stringValue(rawOutput["status"]), failure.Status)
		errorValue = firstNonNil(rawOutput["error"], nestedValue(rawOutput, "result", "structuredContent", "error"), errorValue)
	}
	if errorValue == nil && !isFailureStatus(failure.Status) {
		return toolFailure{}, false
	}
	failure.Code, failure.Message = toolErrorFields(errorValue)
	failure.Message = truncateToolError(redaction.Secrets(failure.Message))
	return failure, true
}

func toolErrorFields(value any) (string, string) {
	if mapped, ok := mapValue(value); ok {
		code := stringValue(mapped["code"])
		message := firstNonEmptyString(stringValue(mapped["message"]), stringValue(mapped["error"]), stringValue(mapped["operation"]))
		if message != "" || code != "" {
			return code, message
		}
	}
	if text := stringValue(value); text != "" {
		return "", text
	}
	if value == nil {
		return "", ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Sprint(value)
	}
	return "", string(data)
}

func nestedValue(value map[string]any, path ...string) any {
	var current any = value
	for _, key := range path {
		mapped, ok := mapValue(current)
		if !ok {
			return nil
		}
		current = mapped[key]
	}
	return current
}

func mapValue(value any) (map[string]any, bool) {
	mapped, ok := value.(map[string]any)
	return mapped, ok
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func isFailureStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "error":
		return true
	default:
		return false
	}
}

func truncateToolError(message string) string {
	if len(message) <= maxToolErrorLogLength {
		return message
	}
	return message[:maxToolErrorLogLength] + "…"
}
