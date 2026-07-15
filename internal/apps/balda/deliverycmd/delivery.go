package deliverycmd

import (
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
)

const jobPayloadKindDelivery = "delivery"

type Payload struct {
	JobID      string            `json:"job_id,omitempty"`
	Locator    Locator           `json:"locator"`
	Profile    Profile           `json:"profile,omitempty,omitzero"`
	Mode       Mode              `json:"mode"`
	Settlement SettlementPolicy  `json:"settlement,omitempty"`
	Refs       map[string]string `json:"refs,omitempty"`
	Question   *Question         `json:"question,omitempty"`
	Text       string            `json:"text,omitempty"`
	DraftID    int               `json:"draft_id,omitempty"`
	Action     string            `json:"action,omitempty"`
	MessageID  string            `json:"message_id,omitempty"`
	Progress   *Progress         `json:"progress,omitempty"`
}

type Mode string

type SettlementPolicy string

type ProgressKind string

type Format string

const (
	FormatAuto     Format = "auto"
	FormatMarkdown Format = "markdown"
	FormatHTML     Format = "html"
	FormatPlain    Format = "plain"
)

type Profile struct {
	Format         Format `json:"format,omitempty"`
	TelegramMode   string `json:"telegram_mode,omitempty"`
	FormattingMode string `json:"formatting_mode,omitempty"`
}

type ProgressPolicy struct {
	Typing      bool `json:"typing,omitempty"`
	Thinking    bool `json:"thinking,omitempty"`
	PlanUpdates bool `json:"plan_updates,omitempty"`
}

type PlanSnapshot struct {
	Entries []PlanEntry `json:"entries,omitempty"`
}

type PlanEntry struct {
	Content string `json:"content"`
	Status  string `json:"status,omitempty"`
}

// Question describes transport-neutral choices attached to a delivered prompt.
type Question struct {
	ID      string           `json:"id"`
	Options []QuestionOption `json:"options"`
}

// QuestionOption is one selectable value in a delivered question.
type QuestionOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type Progress struct {
	Kind     ProgressKind   `json:"kind"`
	Text     string         `json:"text,omitempty"`
	Plan     *PlanSnapshot  `json:"plan,omitempty"`
	Visible  bool           `json:"visible,omitempty"`
	Policy   ProgressPolicy `json:"policy,omitempty,omitzero"`
	Sequence int            `json:"sequence,omitempty"`
}

const (
	ModeAgentReply            Mode = "agent_reply"
	ModePlain                 Mode = "plain"
	ModeMarkdown              Mode = "markdown"
	ModeDraftPlain            Mode = "draft_plain"
	ModeChatAction            Mode = "chat_action"
	ModeProgress              Mode = "progress"
	ModeClearQuestionControls Mode = "clear_question_controls"
)

const (
	SettlementAuto   SettlementPolicy = "auto"
	SettlementBypass SettlementPolicy = "bypass"
	SettlementOutbox SettlementPolicy = "outbox"
)

const (
	ProgressActivity   ProgressKind = "activity"
	ProgressThinking   ProgressKind = "thinking"
	ProgressPlanUpdate ProgressKind = "plan_update"
)

func AgentReplyEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return AgentReplyEnvelopeWithProfile(jobID, from, locator, Profile{}, text, dedupeSuffix)
}

func AgentReplyEnvelopeWithSettlement(jobID string, from actorlayer.ActorAddress, locator Locator, settlement SettlementPolicy, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return AgentReplyEnvelopeWithProfileAndSettlement(jobID, from, locator, Profile{}, settlement, text, dedupeSuffix)
}

func AgentReplyEnvelopeWithProfile(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return AgentReplyEnvelopeWithProfileAndSettlement(jobID, from, locator, profile, SettlementAuto, text, dedupeSuffix)
}

func AgentReplyEnvelopeWithProfileAndSettlement(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, settlement SettlementPolicy, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return AgentReplyEnvelopeWithProfileAndSettlementAndRefs(jobID, from, locator, profile, settlement, text, dedupeSuffix, nil)
}

func AgentReplyEnvelopeWithProfileAndSettlementAndRefs(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, settlement SettlementPolicy, text string, dedupeSuffix string, refs map[string]string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:      strings.TrimSpace(jobID),
		Locator:    locator,
		Profile:    normalizeProfile(profile),
		Mode:       ModeAgentReply,
		Settlement: normalizeSettlementPolicy(settlement),
		Refs:       refs,
		Text:       strings.TrimSpace(text),
	}, dedupeSuffix)
}

// QuestionEnvelope builds an agent-reply delivery carrying generic selectable options.
func QuestionEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, settlement SettlementPolicy, text, questionID, dedupeSuffix string, options []QuestionOption) (actorlayer.Envelope, error) {
	questionID = strings.TrimSpace(questionID)
	return envelope(jobID, from, Payload{
		JobID:      strings.TrimSpace(jobID),
		Locator:    locator,
		Profile:    normalizeProfile(profile),
		Mode:       ModeAgentReply,
		Settlement: normalizeSettlementPolicy(settlement),
		Refs:       map[string]string{"question_id": questionID},
		Question: &Question{
			ID:      questionID,
			Options: append([]QuestionOption(nil), options...),
		},
		Text: strings.TrimSpace(text),
	}, dedupeSuffix)
}

// ClearQuestionControlsEnvelope removes channel-native controls from a settled question.
func ClearQuestionControlsEnvelope(from actorlayer.ActorAddress, locator Locator, questionID, messageID string) (actorlayer.Envelope, error) {
	questionID = strings.TrimSpace(questionID)
	env, err := envelope("", from, Payload{
		Locator:   locator,
		Mode:      ModeClearQuestionControls,
		Refs:      map[string]string{"question_id": questionID},
		MessageID: strings.TrimSpace(messageID),
	}, "question-controls-clear")
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	dedupeKey := "question:" + questionID + ":controls:clear"
	env.ID = dedupeKey
	env.DedupeKey = dedupeKey
	return env, nil
}

func PlainEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return PlainEnvelopeWithSettlement(jobID, from, locator, SettlementAuto, text, dedupeSuffix)
}

func PlainEnvelopeWithSettlement(jobID string, from actorlayer.ActorAddress, locator Locator, settlement SettlementPolicy, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:      strings.TrimSpace(jobID),
		Locator:    locator,
		Mode:       ModePlain,
		Settlement: normalizeSettlementPolicy(settlement),
		Text:       strings.TrimSpace(text),
	}, dedupeSuffix)
}

func MarkdownEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return MarkdownEnvelopeWithProfile(jobID, from, locator, Profile{}, text, dedupeSuffix)
}

func MarkdownEnvelopeWithSettlement(jobID string, from actorlayer.ActorAddress, locator Locator, settlement SettlementPolicy, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return MarkdownEnvelopeWithProfileAndSettlement(jobID, from, locator, Profile{}, settlement, text, dedupeSuffix)
}

func MarkdownEnvelopeWithProfile(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return MarkdownEnvelopeWithProfileAndSettlement(jobID, from, locator, profile, SettlementAuto, text, dedupeSuffix)
}

func MarkdownEnvelopeWithProfileAndSettlement(jobID string, from actorlayer.ActorAddress, locator Locator, profile Profile, settlement SettlementPolicy, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:      strings.TrimSpace(jobID),
		Locator:    locator,
		Profile:    normalizeProfile(profile),
		Mode:       ModeMarkdown,
		Settlement: normalizeSettlementPolicy(settlement),
		Text:       strings.TrimSpace(text),
	}, dedupeSuffix)
}

func DraftPlainEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, draftID int, text string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:   strings.TrimSpace(jobID),
		Locator: locator,
		Mode:    ModeDraftPlain,
		Text:    strings.TrimSpace(text),
		DraftID: draftID,
	}, "")
}

func ChatActionEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, action string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:   strings.TrimSpace(jobID),
		Locator: locator,
		Mode:    ModeChatAction,
		Action:  strings.TrimSpace(action),
	}, "")
}

func ProgressActivityEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, policy ProgressPolicy, sequence int, dedupeSuffix string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:   strings.TrimSpace(jobID),
		Locator: locator,
		Mode:    ModeProgress,
		Progress: &Progress{
			Kind:     ProgressActivity,
			Visible:  false,
			Policy:   policy,
			Sequence: sequence,
		},
	}, dedupeSuffix)
}

func ProgressThinkingEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, policy ProgressPolicy, visible bool, text string, sequence int, dedupeSuffix string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:   strings.TrimSpace(jobID),
		Locator: locator,
		Mode:    ModeProgress,
		Progress: &Progress{
			Kind:     ProgressThinking,
			Text:     strings.TrimSpace(text),
			Visible:  visible,
			Policy:   policy,
			Sequence: sequence,
		},
	}, dedupeSuffix)
}

func ProgressPlanUpdateEnvelope(jobID string, from actorlayer.ActorAddress, locator Locator, policy ProgressPolicy, visible bool, plan *PlanSnapshot, text string, dedupeSuffix string) (actorlayer.Envelope, error) {
	return envelope(jobID, from, Payload{
		JobID:   strings.TrimSpace(jobID),
		Locator: locator,
		Mode:    ModeProgress,
		Progress: &Progress{
			Kind:    ProgressPlanUpdate,
			Text:    strings.TrimSpace(text),
			Plan:    plan,
			Visible: visible,
			Policy:  policy,
		},
	}, dedupeSuffix)
}

func normalizeProfile(profile Profile) Profile {
	format := Format(strings.ToLower(strings.TrimSpace(string(profile.Format))))
	telegramMode := strings.ToLower(strings.TrimSpace(profile.TelegramMode))
	legacy := strings.ToLower(strings.TrimSpace(profile.FormattingMode))

	if format == "" && legacy != "" {
		switch legacy {
		case "auto", "markdown", "html", "plain":
			format = Format(legacy)
		default:
			format = FormatAuto
			telegramMode = legacy
		}
	}
	if format == "" {
		format = FormatAuto
	}
	if telegramMode == "" && legacy != "" && legacy != "auto" && legacy != "markdown" && legacy != "html" && legacy != "plain" {
		telegramMode = legacy
	}

	return Profile{
		Format:       format,
		TelegramMode: telegramMode,
	}
}

func normalizeSettlementPolicy(policy SettlementPolicy) SettlementPolicy {
	switch strings.TrimSpace(string(policy)) {
	case "", string(SettlementAuto):
		return SettlementAuto
	case string(SettlementBypass):
		return SettlementBypass
	case string(SettlementOutbox):
		return SettlementOutbox
	default:
		return SettlementPolicy(strings.TrimSpace(string(policy)))
	}
}

func Validate(payload Payload) error {
	switch payload.Mode {
	case ModeAgentReply, ModePlain, ModeMarkdown, ModeDraftPlain:
		if strings.TrimSpace(payload.Text) == "" {
			return fmt.Errorf("delivery text is required")
		}
		if payload.Question != nil {
			if payload.Mode != ModeAgentReply {
				return fmt.Errorf("question controls require agent reply mode")
			}
			if err := validateQuestion(*payload.Question); err != nil {
				return err
			}
		}
	case ModeChatAction:
		if strings.TrimSpace(payload.Action) == "" {
			return fmt.Errorf("delivery action is required")
		}
	case ModeProgress:
		if payload.Progress == nil {
			return fmt.Errorf("delivery progress is required")
		}
		switch payload.Progress.Kind {
		case ProgressActivity:
			return nil
		case ProgressThinking:
		case ProgressPlanUpdate:
			if strings.TrimSpace(payload.Progress.Text) == "" && (payload.Progress.Plan == nil || len(payload.Progress.Plan.Entries) == 0) {
				return fmt.Errorf("plan update progress text or plan snapshot is required")
			}
		default:
			return fmt.Errorf("unsupported progress kind %q", payload.Progress.Kind)
		}
	case ModeClearQuestionControls:
		if strings.TrimSpace(payload.Refs["question_id"]) == "" {
			return fmt.Errorf("question id is required")
		}
		if strings.TrimSpace(payload.MessageID) == "" {
			return fmt.Errorf("provider message id is required")
		}
	default:
		return fmt.Errorf("unsupported delivery mode %q", payload.Mode)
	}
	if payload.Mode == ModeDraftPlain && payload.DraftID <= 0 {
		return fmt.Errorf("draft id is required")
	}
	switch normalizeSettlementPolicy(payload.Settlement) {
	case SettlementAuto, SettlementBypass, SettlementOutbox:
	default:
		return fmt.Errorf("unsupported delivery settlement %q", payload.Settlement)
	}
	return nil
}

func validateQuestion(question Question) error {
	if strings.TrimSpace(question.ID) == "" {
		return fmt.Errorf("question id is required")
	}
	if len(question.Options) == 0 {
		return fmt.Errorf("question options are required")
	}
	seen := make(map[string]struct{}, len(question.Options))
	for _, option := range question.Options {
		id := strings.TrimSpace(option.ID)
		if id == "" {
			return fmt.Errorf("question option id is required")
		}
		if strings.TrimSpace(option.Label) == "" {
			return fmt.Errorf("question option label is required")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate question option id %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func envelope(jobID string, from actorlayer.ActorAddress, payload Payload, dedupeSuffix string) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Locator.ChannelType) == "" || strings.TrimSpace(payload.Locator.AddressKey) == "" || strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("delivery locator is required")
	}
	trimmedJobID := strings.TrimSpace(jobID)
	trimmedPayloadJobID := strings.TrimSpace(payload.JobID)
	switch {
	case trimmedJobID == "":
		payload.JobID = ""
	case trimmedPayloadJobID == "":
		payload.JobID = trimmedJobID
	case trimmedPayloadJobID != trimmedJobID:
		return actorlayer.Envelope{}, fmt.Errorf("delivery payload job id %q does not match job id %q", trimmedPayloadJobID, trimmedJobID)
	}
	if err := Validate(payload); err != nil {
		return actorlayer.Envelope{}, err
	}
	data, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode delivery payload: %w", err)
	}
	dedupeKey := deliveryDedupeKey(trimmedJobID, payload.Mode, dedupeSuffix)
	return actorlayer.Envelope{
		ID:            dedupeKey,
		Namespace:     namespaceAgentResult,
		Kind:          jobPayloadKindDelivery,
		From:          from,
		To:            actorlayer.ActorAddress{Target: actorTypeDelivery, Key: payload.Locator.DeliveryActorKey()},
		Meta:          withSessionIDMeta(withJobIDMeta(nil, trimmedJobID), payload.Locator.SessionID),
		CorrelationID: trimmedJobID,
		Priority:      70,
		DedupeKey:     dedupeKey,
		Payload:       data,
	}, nil
}

func deliveryDedupeKey(jobID string, mode Mode, dedupeSuffix string) string {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		id := "delivery:" + strings.ToLower(string(mode)) + ":" + uuid.NewString()
		if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
			return id + ":" + suffix
		}
		return id
	}
	if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
		return trimmedJobID + ":delivery:" + suffix
	}
	return trimmedJobID + ":delivery:" + strings.ToLower(string(mode)) + ":" + uuid.NewString()
}

const (
	namespaceAgentResult = "agent.result"
	actorTypeDelivery    = "delivery"
	jobIDMetaKey         = "job_id"
)

func withJobIDMeta(meta map[string]string, jobID string) map[string]string {
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return meta
	}
	out := make(map[string]string, len(meta)+1)
	for key, value := range meta {
		out[key] = value
	}
	out[jobIDMetaKey] = trimmed
	return out
}

func withSessionIDMeta(meta map[string]string, sessionID string) map[string]string {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return meta
	}
	out := make(map[string]string, len(meta)+1)
	for key, value := range meta {
		out[key] = value
	}
	out["session_id"] = trimmed
	return out
}
