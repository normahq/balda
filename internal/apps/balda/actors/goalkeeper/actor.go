package goalkeeper

import (
	"context"
	"fmt"
	"iter"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	"github.com/normahq/balda/internal/apps/balda/redaction"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	adkagent "google.golang.org/adk/v2/agent"
	adkrunner "google.golang.org/adk/v2/runner"
	adksession "google.golang.org/adk/v2/session"
	"google.golang.org/genai"
)

// actor.go contains the goal actor entrypoint and feature contracts.
const (
	actorName                    = "goalkeeper.actor"
	ownerSessionLabel            = "balda"
	defaultGoalMaxIterations     = 25
	goalExportStatusExported     = "exported"
	goalExportStatusFailed       = "export_failed"
	goalExportStatusNotExported  = "not_exported"
	goalExportReasonDisabled     = "workspace_disabled"
	MetadataEventKey             = "norma.goal.event"
	MetadataStepKey              = "norma.goal.step"
	StepStarted                  = "step_started"
	StepCompleted                = "step_completed"
	StepFailed                   = "step_failed"
	WorkerStep                   = "worker"
	ValidatorStep                = "validator"
	jobPayloadKindGoal           = "goal"
	jobResultSchemaVersionV1     = "job_result.v1"
	jobReviewableOutcomeSchemaV1 = "job_reviewable_outcome.v1"
	progressKindOutput           = "output"
)

type GoalRunPreparer interface {
	PrepareGoalRun(ctx context.Context, cfg GoalRunConfig) (GoalRun, error)
}

type GoalRunConfig struct {
	SourceSessionID string
	JobID           string
	UserID          string
	MaxIterations   uint
}

type GoalRunner interface {
	Run(ctx context.Context, userID string, sessionID string, userContent *genai.Content, cfg adkagent.RunConfig, opts ...adkrunner.RunOption) iter.Seq2[*adksession.Event, error]
}

type GoalRun interface {
	Runner() GoalRunner
	SessionID() string
	WorkspaceDir() string
	BranchName() string
	Close() error
	CleanupResources(ctx context.Context) error
	Finalize(ctx context.Context, objective string, workerOutput string, validatorOutput string) (GoalFinalizationResult, error)
}

type GoalFinalizationResult struct {
	Status        string
	CommitMessage string
	Reason        string
	Error         string
}

type JobRuns interface {
	Register(jobID string, cancel context.CancelFunc) string
	Unregister(jobID string, runID string)
}

type jobLifecycle interface {
	Create(ctx context.Context, record baldastate.JobRecord, actor string, payload any) (bool, error)
	Get(ctx context.Context, jobID string) (baldastate.JobRecord, bool, error)
	ListActiveGoalJobsBySession(ctx context.Context, sessionID string) ([]baldastate.JobRecord, error)
	MarkStatus(ctx context.Context, jobID string, status string, actor string, messageID string, reason string, payload any) error
	SetResult(ctx context.Context, jobID string, result any, status string, actor string, reason string) error
}

type jobEvents interface {
	AppendEvent(ctx context.Context, jobID string, eventType string, actor string, messageID string, payload any) error
}

type ActorParams struct {
	fx.In

	JobLifecycle    jobLifecycle
	JobEvents       jobEvents
	SessionManager  *baldasession.Manager
	GoalRunPreparer GoalRunPreparer
	JobRuns         JobRuns
	QuestionService *questions.Service
	MaxIterations   int `name:"balda_goal_max_iterations"`
	Dispatcher      actortransport.Dispatcher
	Logger          zerolog.Logger
}

type Actor struct {
	coordinator *coordinator
}

type goalJobPayload = goalkeepercmd.JobPayload
type goalQuestionPayload = goalkeepercmd.QuestionPayload

type goalArtifactResultV1 struct {
	WorkspaceDir string   `json:"workspace_dir,omitempty"`
	BranchName   string   `json:"branch_name,omitempty"`
	Commit       string   `json:"commit,omitempty"`
	ChangedFiles []string `json:"changed_files,omitempty"`
	GitError     string   `json:"git_error,omitempty"`
}

type goalExportResultV1 struct {
	Status        string `json:"status,omitempty"`
	CommitMessage string `json:"commit_message,omitempty"`
	BaseCommit    string `json:"base_commit,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Error         string `json:"error,omitempty"`
}

type goalReviewableOutcomeV1 struct {
	SchemaVersion string `json:"schema_version"`
	WhatWasDone   string `json:"what_was_done,omitempty"`
	Validation    string `json:"validation_output,omitempty"`
	Verified      string `json:"what_was_verified,omitempty"`
	NotVerified   string `json:"what_was_not_verified,omitempty"`
	NextAction    string `json:"next_action,omitempty"`
}

type goalResultPayloadV1 struct {
	SchemaVersion     string                  `json:"schema_version"`
	GoalReached       bool                    `json:"goal_reached"`
	Iterations        int                     `json:"iterations"`
	ExecutorOutput    string                  `json:"executor_output,omitempty"`
	ReviewerOutput    string                  `json:"reviewer_output,omitempty"`
	ReviewerNotes     string                  `json:"reviewer_feedback,omitempty"`
	Artifacts         *goalArtifactResultV1   `json:"artifacts,omitempty"`
	Export            *goalExportResultV1     `json:"export,omitempty"`
	ReviewableOutcome goalReviewableOutcomeV1 `json:"reviewable_outcome"`
}

type goalArtifactSnapshot struct {
	WorkspaceDir string
	BranchName   string
	Commit       string
	ChangedFiles []string
	GitError     string
}

type goalRunResult struct {
	payload               goalJobPayload
	iterations            int
	workerOutput          string
	validatorOutput       string
	latestWorkerOutput    string
	latestValidatorOutput string
	finalText             string
}

type stepProgressState struct {
	lastVisibleText string
	lastPlanText    string
	deliveredOutput bool
}

func NewActor(params ActorParams) *Actor {
	return &Actor{
		coordinator: newCoordinator(params),
	}
}

func (a *Actor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeGoalkeeper)
}

func (a *Actor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Namespace) != baldaexecution.NamespaceGoalkeeperCommand {
		return actorlayer.PolicyError(fmt.Errorf("unsupported goalkeeper namespace %q", env.Namespace))
	}
	var payload goalkeepercmd.EnvelopePayload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode goalkeeper payload: %w", err))
	}
	if a == nil || a.coordinator == nil {
		return actorlayer.TransientError(fmt.Errorf("goal coordinator is required"))
	}
	switch strings.TrimSpace(payload.Kind) {
	case goalkeepercmd.PayloadKindGoal:
		if payload.Goal == nil {
			return actorlayer.PolicyError(fmt.Errorf("goalkeeper payload is required"))
		}
		return a.coordinator.execute(ctx, env, *payload.Goal)
	case goalkeepercmd.PayloadKindQuestion:
		if payload.Question == nil {
			return actorlayer.PolicyError(fmt.Errorf("goalkeeper question payload is required"))
		}
		return a.coordinator.handleQuestionContinuation(ctx, env, *payload.Question)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported goalkeeper payload kind %q", payload.Kind))
	}
}

func GoalJobEnvelope(
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
	maxIterations int,
) (actorlayer.Envelope, error) {
	return goalkeepercmd.JobEnvelope(locator, objective, transportUserID, maxIterations)
}

func GoalJobEnvelopeWithOptions(
	locator baldasession.SessionLocator,
	deliveryOptions deliveryfmt.Options,
	objective string,
	transportUserID string,
	maxIterations int,
) (actorlayer.Envelope, error) {
	return goalkeepercmd.JobEnvelopeWithOptions(locator, deliveryOptions, objective, transportUserID, maxIterations)
}

func envelopeJobID(env actorlayer.Envelope) string {
	return strings.TrimSpace(baldaexecution.EnvelopeJobID(env))
}

func normalizeGoalDeliveryLocator(locator baldasession.SessionLocator) baldasession.SessionLocator {
	if strings.TrimSpace(locator.ChannelType) == "" {
		locator.ChannelType = "telegram"
	}
	return locator
}

func normalizeGoalDeliveryOptions(options deliveryfmt.Options) deliveryfmt.Options {
	return deliveryfmt.NormalizeOptions(options)
}

func goalDeliveryOptions(payload goalJobPayload) deliveryfmt.Options {
	return normalizeGoalDeliveryOptions(payload.DeliveryOptions)
}

func goalDeliveryProfile(payload goalJobPayload) deliverycmd.Profile {
	profile := goalDeliveryOptions(payload).Profile
	return deliverycmd.Profile{
		Format:         deliverycmd.Format(profile.Format),
		TelegramMode:   profile.TelegramMode,
		FormattingMode: profile.FormattingMode,
	}
}

func goalProgressPolicy(payload goalJobPayload) deliverycmd.ProgressPolicy {
	policy := goalDeliveryOptions(payload).ProgressPolicy
	return deliverycmd.ProgressPolicy{
		Typing:      policy.Typing,
		Thinking:    policy.Thinking,
		PlanUpdates: policy.PlanUpdates,
	}
}

func normalizeGoalMaxIterations(v int) int {
	if v <= 0 {
		return defaultGoalMaxIterations
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func redactSecrets(raw string) string {
	return redaction.Secrets(raw)
}

func nextDeliverySequence(value *int) int {
	if value == nil {
		return 0
	}
	*value++
	return *value
}
