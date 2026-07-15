// Package permissionfmt renders structured permission requests for concrete
// delivery channels without inspecting opaque provider input.
package permissionfmt

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
)

const (
	maxContentLength = 4096
	maxLocations     = 10
)

type Presentation struct {
	Prompt  string
	Profile deliverycmd.Profile
}

func Render(request permissioncmd.Request) Presentation {
	switch strings.ToLower(strings.TrimSpace(request.Interaction.Locator.ChannelType)) {
	case string(deliverycmd.ChannelTypeTelegram):
		return Presentation{Prompt: renderTelegramMarkdown(request), Profile: deliverycmd.Profile{Format: deliverycmd.FormatMarkdown}}
	case string(deliverycmd.ChannelTypeSlackAgent):
		return Presentation{Prompt: renderMarkdown(request), Profile: deliverycmd.Profile{Format: deliverycmd.FormatMarkdown}}
	default:
		return Presentation{Prompt: renderPlain(request), Profile: deliverycmd.Profile{Format: deliverycmd.FormatPlain}}
	}
}

func renderTelegramMarkdown(request permissioncmd.Request) string {
	var out strings.Builder
	writeMarkdownRequest(&out, request)
	out.WriteString("\n\n_Choose an action below._")
	return out.String()
}

func renderMarkdown(request permissioncmd.Request) string {
	var out strings.Builder
	writeMarkdownRequest(&out, request)
	writeOptions(&out, request.Options, "\n\n**Choose:**")
	out.WriteString("\n\n_Reply with the number or option name._")
	return out.String()
}

func writeMarkdownRequest(out *strings.Builder, request permissioncmd.Request) {
	out.WriteString("🔐 **Permission required**")
	writeMarkdownAction(out, request.ToolCall)
	writeMarkdownContent(out, request.ToolCall.Content)
	writeMarkdownLocations(out, request.ToolCall.Locations)
}

func writeMarkdownAction(out *strings.Builder, toolCall permissioncmd.ToolCall) {
	title := displayValue(toolCall.Title)
	kind := displayValue(toolCall.Kind)
	if title == "" && kind == "" {
		return
	}
	out.WriteString("\n\n**Action:** ")
	if title != "" {
		out.WriteString(title)
	}
	if kind != "" {
		if title != "" {
			out.WriteString(" ")
		}
		out.WriteString("`")
		out.WriteString(strings.ReplaceAll(kind, "`", "'"))
		out.WriteString("`")
	}
}

func writeMarkdownContent(out *strings.Builder, content []permissioncmd.Content) {
	for _, item := range content {
		switch item.Kind {
		case permissioncmd.ContentKindText:
			if text := displayValue(item.Text); text != "" {
				out.WriteString("\n\n")
				out.WriteString(text)
			}
		case permissioncmd.ContentKindDiff:
			if path := displayValue(item.Path); path != "" {
				out.WriteString("\n\n**File change:** `")
				out.WriteString(strings.ReplaceAll(path, "`", "'"))
				out.WriteString("`")
			}
		case permissioncmd.ContentKindTerminal:
			if terminalID := displayValue(item.TerminalID); terminalID != "" {
				out.WriteString("\n\n**Terminal:** `")
				out.WriteString(strings.ReplaceAll(terminalID, "`", "'"))
				out.WriteString("`")
			}
		}
	}
}

func writeMarkdownLocations(out *strings.Builder, locations []permissioncmd.Location) {
	for index, location := range locations {
		if index >= maxLocations {
			break
		}
		path := displayValue(location.Path)
		if path == "" {
			continue
		}
		out.WriteString("\n\n**Affected:** `")
		out.WriteString(strings.ReplaceAll(path, "`", "'"))
		if location.Line != nil {
			out.WriteString(":")
			out.WriteString(strconv.Itoa(*location.Line))
		}
		out.WriteString("`")
	}
}

func renderPlain(request permissioncmd.Request) string {
	var out strings.Builder
	out.WriteString("Permission required")
	if title := displayValue(request.ToolCall.Title); title != "" {
		out.WriteString("\n\nAction: ")
		out.WriteString(title)
	}
	if kind := displayValue(request.ToolCall.Kind); kind != "" {
		out.WriteString("\nKind: ")
		out.WriteString(kind)
	}
	for _, item := range request.ToolCall.Content {
		if item.Kind != permissioncmd.ContentKindText {
			continue
		}
		if content := plainText(displayValue(item.Text)); content != "" {
			out.WriteString("\n\n")
			out.WriteString(content)
		}
	}
	for index, location := range request.ToolCall.Locations {
		if index >= maxLocations {
			break
		}
		path := displayValue(location.Path)
		if path == "" {
			continue
		}
		out.WriteString("\nAffected: ")
		out.WriteString(path)
		if location.Line != nil {
			out.WriteString(":")
			out.WriteString(strconv.Itoa(*location.Line))
		}
	}
	writeOptions(&out, request.Options, "\n\nChoose:")
	out.WriteString("\n\nReply with the number or option name.")
	return out.String()
}

func writeOptions(out *strings.Builder, options []permissioncmd.Option, heading string) {
	out.WriteString(heading)
	for index, option := range options {
		fmt.Fprintf(out, "\n%d. %s", index+1, displayValue(option.Name))
	}
}

func displayValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= maxContentLength {
		return value
	}
	return value[:maxContentLength] + "…"
}

func plainText(value string) string {
	value = strings.ReplaceAll(value, "```sh\n", "")
	value = strings.ReplaceAll(value, "```", "")
	return strings.ReplaceAll(value, "`", "")
}
