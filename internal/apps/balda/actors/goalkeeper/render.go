package goalkeeper

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
)

type goalMessageStyle string

const (
	goalMessageStylePlain    goalMessageStyle = "plain"
	goalMessageStyleMarkdown goalMessageStyle = "markdown"
	goalMessageStyleHTML     goalMessageStyle = "html"
)

type goalMessageTemplate string

const (
	goalTemplateStarted goalMessageTemplate = "started"
	goalTemplateStep    goalMessageTemplate = "step"
	goalTemplateStatus  goalMessageTemplate = "status"
)

type goalMessageData struct {
	MaxIterations int
	Iteration     int
	Objective     string
	Step          string
	Action        string
	Body          string
	Text          string
}

var goalMessageTemplates = map[goalMessageStyle]map[goalMessageTemplate]*template.Template{
	goalMessageStylePlain: mustGoalMessageTemplates(goalMessageStylePlain, map[goalMessageTemplate]string{
		goalTemplateStarted: "Goal run started. Max iterations: {{.MaxIterations}}.\n\nObjective: {{.Objective}}",
		goalTemplateStep:    "Goal iteration {{.Iteration}}/{{.MaxIterations}}: {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		goalTemplateStatus:  "{{.Text}}",
	}),
	goalMessageStyleMarkdown: mustGoalMessageTemplates(goalMessageStyleMarkdown, map[goalMessageTemplate]string{
		goalTemplateStarted: "**Goal run started**\n\n- **Max iterations:** {{.MaxIterations}}\n- **Objective:** {{.Objective}}",
		goalTemplateStep:    "**Goal iteration {{.Iteration}}/{{.MaxIterations}}:** {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		goalTemplateStatus:  "**{{.Text}}**",
	}),
	goalMessageStyleHTML: mustGoalMessageTemplates(goalMessageStyleHTML, map[goalMessageTemplate]string{
		goalTemplateStarted: "<b>Goal run started</b>\n\n<b>Max iterations:</b> {{.MaxIterations}}\n\n<b>Objective:</b> {{.Objective}}",
		goalTemplateStep:    "<b>Goal iteration {{.Iteration}}/{{.MaxIterations}}:</b> {{.Step}} {{.Action}}.{{if .Body}}\n\n{{.Body}}{{end}}",
		goalTemplateStatus:  "<b>{{.Text}}</b>",
	}),
}

func mustGoalMessageTemplates(style goalMessageStyle, sources map[goalMessageTemplate]string) map[goalMessageTemplate]*template.Template {
	out := make(map[goalMessageTemplate]*template.Template, len(sources))
	for name, source := range sources {
		out[name] = template.Must(template.New(string(style) + "." + string(name)).Option("missingkey=error").Parse(source))
	}
	return out
}

func renderGoalStartedMessage(profile deliverycmd.Profile, maxIterations int, objective string) string {
	style := goalMessageStyleForProfile(profile)
	return renderGoalTemplate(style, goalTemplateStarted, goalMessageData{
		MaxIterations: maxIterations,
		Objective:     goalSystemText(style, objective),
	})
}

func renderGoalStepMessage(profile deliverycmd.Profile, iteration int, maxIterations int, step string, action string, body string) string {
	style := goalMessageStyleForProfile(profile)
	return renderGoalTemplate(style, goalTemplateStep, goalMessageData{
		MaxIterations: maxIterations,
		Iteration:     iteration,
		Step:          goalSystemText(style, step),
		Action:        goalSystemText(style, action),
		Body:          strings.TrimSpace(body),
	})
}

func renderGoalStatusMessage(profile deliverycmd.Profile, text string) string {
	style := goalMessageStyleForProfile(profile)
	return renderGoalTemplate(style, goalTemplateStatus, goalMessageData{
		Text: goalSystemText(style, text),
	})
}

func goalMessageStyleForProfile(profile deliverycmd.Profile) goalMessageStyle {
	normalized := deliveryfmt.NormalizeProfile(profile)
	switch normalized.Format {
	case deliveryfmt.FormatMarkdown:
		return goalMessageStyleMarkdown
	case deliveryfmt.FormatHTML:
		return goalMessageStyleHTML
	}
	switch normalized.TelegramMode {
	case telegramfmt.ModeRichMarkdown, telegramfmt.ModeMarkdownV2:
		return goalMessageStyleMarkdown
	case telegramfmt.ModeRichHTML, telegramfmt.ModeHTML:
		return goalMessageStyleHTML
	default:
		return goalMessageStylePlain
	}
}

func renderGoalTemplate(style goalMessageStyle, name goalMessageTemplate, data goalMessageData) string {
	templates := goalMessageTemplates[style]
	if templates == nil {
		templates = goalMessageTemplates[goalMessageStylePlain]
	}
	tmpl := templates[name]
	if tmpl == nil {
		tmpl = goalMessageTemplates[goalMessageStylePlain][name]
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return ""
	}
	return strings.TrimSpace(out.String())
}

func goalSystemText(style goalMessageStyle, text string) string {
	text = strings.TrimSpace(text)
	if style == goalMessageStyleHTML {
		return telegramfmt.HTML(text)
	}
	return text
}

func goalOutcomeLine(profile deliverycmd.Profile, label string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	style := goalMessageStyleForProfile(profile)
	switch style {
	case goalMessageStyleMarkdown:
		return fmt.Sprintf("**%s:** %s", label, value)
	case goalMessageStyleHTML:
		return fmt.Sprintf("<b>%s:</b> %s", telegramfmt.HTML(label), value)
	default:
		return label + ": " + value
	}
}

func goalOutcomeBlock(profile deliverycmd.Profile, label string, body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	style := goalMessageStyleForProfile(profile)
	switch style {
	case goalMessageStyleMarkdown:
		return fmt.Sprintf("**%s:**\n%s", label, body)
	case goalMessageStyleHTML:
		return fmt.Sprintf("<b>%s:</b>\n%s", telegramfmt.HTML(label), body)
	default:
		return label + ":\n" + body
	}
}
