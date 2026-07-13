package goaldelivery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/redaction"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
)

const (
	DefaultNotVerifiedText       = "manual review still required"
	DefaultInspectNextAction     = "Inspect events and decide whether to continue, cancel, or ask a human."
	DefaultExportedNextAction    = "Review the exported result and continue with follow-up work if needed."
	DefaultNotExportedNextAction = "Review the direct working directory changes and commit or follow up manually if needed."

	GoalExportStatusExported    = "exported"
	GoalExportStatusFailed      = "export_failed"
	GoalExportStatusNotExported = "not_exported"
)

type messageStyle string

const (
	messageStylePlain    messageStyle = "plain"
	messageStyleMarkdown messageStyle = "markdown"
	messageStyleHTML     messageStyle = "html"
)

type messageTemplate string

const (
	templateStarted messageTemplate = "started"
	templateStep    messageTemplate = "step"
	templateStatus  messageTemplate = "status"
)

type messageData struct {
	MaxIterations int
	Iteration     int
	Objective     string
	Step          string
	Action        string
	Body          string
	Text          string
}

var messageTemplates = map[messageStyle]map[messageTemplate]*template.Template{
	messageStylePlain: mustTemplates(messageStylePlain, map[messageTemplate]string{
		templateStarted: "Goal run started. Max iterations: {{.MaxIterations}}.\n\nObjective: {{.Objective}}",
		templateStep:    "Goal iteration {{.Iteration}}/{{.MaxIterations}}: {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		templateStatus:  "{{.Text}}",
	}),
	messageStyleMarkdown: mustTemplates(messageStyleMarkdown, map[messageTemplate]string{
		templateStarted: "**Goal run started**\n\n- **Max iterations:** {{.MaxIterations}}\n- **Objective:** {{.Objective}}",
		templateStep:    "**Goal iteration {{.Iteration}}/{{.MaxIterations}}:** {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		templateStatus:  "**{{.Text}}**",
	}),
	messageStyleHTML: mustTemplates(messageStyleHTML, map[messageTemplate]string{
		templateStarted: "<b>Goal run started</b>\n\n<b>Max iterations:</b> {{.MaxIterations}}\n\n<b>Objective:</b> {{.Objective}}",
		templateStep:    "<b>Goal iteration {{.Iteration}}/{{.MaxIterations}}:</b> {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		templateStatus:  "<b>{{.Text}}</b>",
	}),
}

func mustTemplates(style messageStyle, sources map[messageTemplate]string) map[messageTemplate]*template.Template {
	out := make(map[messageTemplate]*template.Template, len(sources))
	for name, source := range sources {
		out[name] = template.Must(template.New(string(style) + "." + string(name)).Option("missingkey=error").Parse(source))
	}
	return out
}

func RenderStartedMessage(profile deliverycmd.Profile, maxIterations int, objective string) string {
	style := messageStyleForProfile(profile)
	return renderTemplate(style, templateStarted, messageData{MaxIterations: maxIterations, Objective: systemText(style, objective)})
}

func RenderStepMessage(profile deliverycmd.Profile, iteration int, maxIterations int, step string, action string, body string) string {
	style := messageStyleForProfile(profile)
	return renderTemplate(style, templateStep, messageData{MaxIterations: maxIterations, Iteration: iteration, Step: systemText(style, step), Action: systemText(style, action), Body: strings.TrimSpace(body)})
}

func RenderStatusMessage(profile deliverycmd.Profile, text string) string {
	style := messageStyleForProfile(profile)
	return renderTemplate(style, templateStatus, messageData{Text: systemText(style, text)})
}

func RenderReviewableOutcome(profile deliverycmd.Profile, task baldastate.JobRecord) string {
	var result map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.Result)), &result); err != nil {
		result = nil
	}
	parsedOutcome := struct {
		WhatWasDone string
		Validation  string
		Verified    string
		NotVerified string
		NextAction  string
	}{}
	hasOutcome := false
	if len(result) != 0 {
		if outcomeMap, ok := result["reviewable_outcome"].(map[string]any); ok {
			parsedOutcome.WhatWasDone = RedactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_done"])))
			parsedOutcome.Validation = RedactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["validation_output"])))
			parsedOutcome.Verified = RedactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_verified"])))
			parsedOutcome.NotVerified = RedactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_not_verified"])))
			parsedOutcome.NextAction = RedactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["next_action"])))
			hasOutcome = parsedOutcome.WhatWasDone != "" || parsedOutcome.Validation != "" || parsedOutcome.Verified != "" || parsedOutcome.NotVerified != "" || parsedOutcome.NextAction != ""
		}
	}
	goalReached := false
	switch typed := result["goal_reached"].(type) {
	case bool:
		goalReached = typed
	case string:
		goalReached = strings.EqualFold(strings.TrimSpace(typed), "true")
	}
	exportStatus, exportReason, exportError := "", "", ""
	if len(result) != 0 {
		if exportMap, ok := result["export"].(map[string]any); ok {
			exportStatus = RedactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["status"])))
			exportReason = RedactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["reason"])))
			exportError = RedactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["error"])))
		}
	}
	resultText := func(key string) string {
		if len(result) == 0 {
			return ""
		}
		value, ok := result[key]
		if !ok || value == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(value))
	}
	executorOutput := RedactSecrets(firstNonEmpty(resultText("executor_output"), resultText("final_text")))
	reviewerOutput := RedactSecrets(firstNonEmpty(resultText("reviewer_output"), resultText("reviewer_feedback")))
	whatWasDone := firstNonEmpty(executorOutput, task.Objective)
	if hasOutcome {
		whatWasDone = firstNonEmpty(parsedOutcome.WhatWasDone, whatWasDone)
	}
	if !goalReached && task.Status != baldastate.JobStatusCompleted && resultText("final_text") != "" {
		whatWasDone = RedactSecrets(resultText("final_text"))
	}
	validation := reviewerOutput
	if hasOutcome {
		validation = firstNonEmpty(parsedOutcome.Validation, validation)
	}
	routineSuccessfulOutcome := goalReached && exportStatusIsRoutineSuccess(exportStatus)
	verified := firstNonEmpty(parsedOutcome.Verified, "validator returned feedback")
	notVerified := firstNonEmpty(parsedOutcome.NotVerified, DefaultNotVerifiedText)
	nextAction := firstNonEmpty(parsedOutcome.NextAction, DefaultInspectNextAction)
	renderNotVerified := shouldRenderNotVerified(parsedOutcome.NotVerified)
	renderNextAction := shouldRenderNextAction(parsedOutcome.NextAction, goalReached, exportStatus)
	renderVerified := shouldRenderVerified(verified, routineSuccessfulOutcome)
	renderValidation := shouldRenderValidation(validation, goalReached)

	var parts []string
	if goalReached {
		parts = append(parts, outcomeLine(profile, "Result", "Goal completed."))
	} else {
		parts = append(parts, outcomeLine(profile, "Result", "Goal not completed."))
	}
	if goalReached && exportStatus != "" {
		switch exportStatus {
		case GoalExportStatusExported:
		case GoalExportStatusNotExported:
		case GoalExportStatusFailed:
			parts = append(parts, outcomeLine(profile, "Export", "failed: "+systemText(messageStyleForProfile(profile), firstNonEmpty(exportError, exportReason, "unknown error"))))
		default:
			parts = append(parts, outcomeLine(profile, "Export", systemText(messageStyleForProfile(profile), exportStatus)+"."))
		}
	}
	if whatWasDone != "" {
		if routineSuccessfulOutcome {
			parts = append(parts, strings.TrimSpace(whatWasDone))
		} else {
			parts = append(parts, outcomeBlock(profile, "What was done", whatWasDone))
		}
	}
	if renderValidation && validation != "" {
		parts = append(parts, outcomeBlock(profile, "Validation", validation))
	}
	if renderVerified && verified != "" {
		parts = append(parts, outcomeLine(profile, "Verified", verified))
	}
	if renderNotVerified && notVerified != "" {
		parts = append(parts, outcomeLine(profile, "Not verified", notVerified))
	}
	if renderNextAction && nextAction != "" {
		parts = append(parts, outcomeLine(profile, "Next action", nextAction))
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

func RedactSecrets(raw string) string {
	return redaction.Secrets(raw)
}

func messageStyleForProfile(profile deliverycmd.Profile) messageStyle {
	normalized := deliveryfmt.NormalizeProfile(deliveryfmt.Profile{
		Format:         deliveryfmt.Format(profile.Format),
		TelegramMode:   profile.TelegramMode,
		FormattingMode: profile.FormattingMode,
	})
	switch normalized.Format {
	case deliveryfmt.FormatMarkdown:
		return messageStyleMarkdown
	case deliveryfmt.FormatHTML:
		return messageStyleHTML
	}
	switch normalized.TelegramMode {
	case telegramfmt.ModeRichMarkdown, telegramfmt.ModeMarkdownV2:
		return messageStyleMarkdown
	case telegramfmt.ModeRichHTML, telegramfmt.ModeHTML:
		return messageStyleHTML
	default:
		return messageStylePlain
	}
}

func renderTemplate(style messageStyle, name messageTemplate, data messageData) string {
	templates := messageTemplates[style]
	if templates == nil {
		templates = messageTemplates[messageStylePlain]
	}
	tmpl := templates[name]
	if tmpl == nil {
		tmpl = messageTemplates[messageStylePlain][name]
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return ""
	}
	return strings.TrimSpace(out.String())
}

func systemText(style messageStyle, text string) string {
	text = strings.TrimSpace(text)
	if style == messageStyleHTML {
		return telegramfmt.HTML(text)
	}
	return text
}

func outcomeLine(profile deliverycmd.Profile, label string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	style := messageStyleForProfile(profile)
	switch style {
	case messageStyleMarkdown:
		return fmt.Sprintf("**%s:** %s", label, value)
	case messageStyleHTML:
		return fmt.Sprintf("<b>%s:</b> %s", telegramfmt.HTML(label), value)
	default:
		return label + ": " + value
	}
}

func outcomeBlock(profile deliverycmd.Profile, label string, body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	style := messageStyleForProfile(profile)
	switch style {
	case messageStyleMarkdown:
		return fmt.Sprintf("**%s:**\n%s", label, body)
	case messageStyleHTML:
		return fmt.Sprintf("<b>%s:</b>\n%s", telegramfmt.HTML(label), body)
	default:
		return label + ":\n" + body
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func shouldRenderNotVerified(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && !strings.EqualFold(trimmed, DefaultNotVerifiedText)
}

func shouldRenderVerified(value string, routineSuccessfulOutcome bool) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !routineSuccessfulOutcome {
		return true
	}
	return !strings.EqualFold(trimmed, "validator returned pass")
}

func shouldRenderValidation(value string, goalReached bool) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !goalReached {
		return true
	}
	return !validationIsRoutinePass(trimmed)
}

func validationIsRoutinePass(value string) bool {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if strings.Contains(lowered, "evidence:") || strings.Contains(lowered, "verdict: fail") || strings.Contains(lowered, "verdict fail") {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, ":", " ")))
	normalized = strings.Join(strings.Fields(normalized), " ")
	return strings.Contains(normalized, "verdict pass")
}

func shouldRenderNextAction(value string, goalReached bool, exportStatus string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !goalReached {
		return true
	}
	if !strings.EqualFold(trimmed, DefaultExportedNextAction) {
		if goalReached && strings.TrimSpace(exportStatus) == GoalExportStatusNotExported && strings.EqualFold(trimmed, DefaultNotExportedNextAction) {
			return false
		}
		return true
	}
	switch strings.TrimSpace(exportStatus) {
	case GoalExportStatusFailed, GoalExportStatusNotExported:
		return true
	default:
		return false
	}
}

func exportStatusIsRoutineSuccess(status string) bool {
	switch strings.TrimSpace(status) {
	case "", GoalExportStatusExported, GoalExportStatusNotExported:
		return true
	default:
		return false
	}
}
