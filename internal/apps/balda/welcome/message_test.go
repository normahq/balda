package welcome

import (
	"testing"
)

func TestBuildAgentWelcomeMessage(t *testing.T) {
	tests := []struct {
		name       string
		agentName  string
		sessionID  string
		agentType  string
		model      string
		mcpServers []string
		want       string
	}{
		{
			name:       "full info",
			agentName:  "balda",
			sessionID:  "tg-1-0",
			agentType:  "opencode_acp",
			model:      "gpt-5",
			mcpServers: []string{" balda ", "workspace", "balda", ""},
			want:       "🚀 **Session Started** • **Name:** `balda` • **ID:** `tg-1-0` • **Model:** `gpt-5` • **Type:** `opencode_acp` • **MCP:** `balda, workspace` ",
		},
		{
			name:       "missing info uses none",
			agentName:  " ",
			sessionID:  " ",
			agentType:  " ",
			model:      " ",
			mcpServers: nil,
			want:       "🚀 **Session Started** • **Name:** `none` • **ID:** `none` • **Model:** `none` • **Type:** `none` • **MCP:** `none` ",
		},
		{
			name:       "escapes backticks",
			agentName:  "agent`name",
			sessionID:  "id",
			agentType:  "type",
			model:      "model",
			mcpServers: nil,
			want:       "🚀 **Session Started** • **Name:** `agent\\` name` • **ID:** `id` • **Model:** `model` • **Type:** `type` • **MCP:** `none` ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildAgentWelcomeMessage(tt.agentName, tt.sessionID, tt.agentType, tt.model, tt.mcpServers)
			if got != tt.want {
				t.Errorf("BuildAgentWelcomeMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}
