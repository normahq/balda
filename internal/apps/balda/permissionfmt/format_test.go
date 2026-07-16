package permissionfmt

import (
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
)

func TestRenderTelegramUsesStructuredContentAndOmitsOpaqueInput(t *testing.T) {
	presentation := Render(permissioncmd.Request{
		Interaction: questioncmd.InteractionContext{Locator: deliverycmd.Locator{ChannelType: "telegram"}},
		ToolCall: permissioncmd.ToolCall{
			Title:    "Command approval",
			Kind:     "execute",
			RawInput: `{"threadId":"internal-id","token":"secret-value"}`,
			Content: []permissioncmd.Content{{
				Kind: permissioncmd.ContentKindText,
				Text: "Run the test.\n\nCommand:\n```sh\nid\n```\n\nWorking directory: `/workspace`",
			}},
		},
		Options: []permissioncmd.Option{{ID: "opt-1", Name: "Allow once"}, {ID: "opt-2", Name: "Cancel"}},
	})
	if presentation.Profile.Format != deliverycmd.FormatMarkdown {
		t.Fatalf("profile = %+v", presentation.Profile)
	}
	for _, want := range []string{"**Permission required**", "Run the test.", "```sh\nid\n```", "`/workspace`"} {
		if !strings.Contains(presentation.Prompt, want) {
			t.Fatalf("prompt missing %q: %q", want, presentation.Prompt)
		}
	}
	for _, hidden := range []string{"threadId", "internal-id", "secret-value", "opt-1", "opt-2", "RawInput", "1. Allow once", "2. Cancel", "Choose an action below"} {
		if strings.Contains(presentation.Prompt, hidden) {
			t.Fatalf("prompt exposed %q: %q", hidden, presentation.Prompt)
		}
	}
}

func TestRenderSlackAgentKeepsTextOptions(t *testing.T) {
	presentation := Render(permissioncmd.Request{
		Interaction: questioncmd.InteractionContext{Locator: deliverycmd.Locator{ChannelType: string(deliverycmd.ChannelTypeSlackAgent)}},
		ToolCall:    permissioncmd.ToolCall{Title: "Command approval"},
		Options:     []permissioncmd.Option{{ID: "allow", Name: "Allow"}, {ID: "cancel", Name: "Cancel"}},
	})
	if !strings.Contains(presentation.Prompt, "1. Allow") || !strings.Contains(presentation.Prompt, "2. Cancel") {
		t.Fatalf("prompt = %q, want text options", presentation.Prompt)
	}
}

func TestRenderOmitsGenericOtherKind(t *testing.T) {
	t.Parallel()

	presentation := Render(permissioncmd.Request{
		Interaction: questioncmd.InteractionContext{Locator: deliverycmd.Locator{ChannelType: "telegram"}},
		ToolCall: permissioncmd.ToolCall{
			Title: "MCP elicitation request",
			Kind:  "other",
		},
	})
	if !strings.Contains(presentation.Prompt, "**Action:** MCP elicitation request") {
		t.Fatalf("prompt = %q, want action title", presentation.Prompt)
	}
	if strings.Contains(presentation.Prompt, "other") {
		t.Fatalf("prompt = %q, want generic kind omitted", presentation.Prompt)
	}
}

func TestRenderFallbackIsPlain(t *testing.T) {
	presentation := Render(permissioncmd.Request{
		Interaction: questioncmd.InteractionContext{Locator: deliverycmd.Locator{ChannelType: "zulip"}},
		ToolCall: permissioncmd.ToolCall{
			Title:   "Read file",
			Content: []permissioncmd.Content{{Kind: permissioncmd.ContentKindText, Text: "Inspect `config`"}},
		},
		Options: []permissioncmd.Option{{ID: "yes", Name: "Allow"}},
	})
	if presentation.Profile.Format != deliverycmd.FormatPlain {
		t.Fatalf("profile = %+v", presentation.Profile)
	}
	if strings.Contains(presentation.Prompt, "**") || !strings.Contains(presentation.Prompt, "Inspect config") || !strings.Contains(presentation.Prompt, "1. Allow") {
		t.Fatalf("prompt = %q", presentation.Prompt)
	}
}
