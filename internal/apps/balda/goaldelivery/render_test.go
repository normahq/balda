package goaldelivery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func TestRenderGoalStartedMessagePlainMatchesLegacyText(t *testing.T) {
	t.Parallel()

	got := RenderStartedMessage(deliverycmd.Profile{}, 25, "count total go lines")
	want := "Goal run started. Max iterations: 25.\n\nObjective: count total go lines"
	if got != want {
		t.Fatalf("RenderStartedMessage() = %q, want %q", got, want)
	}
}

func TestRenderGoalStepMessageMarkdownFormatsHeaderAndPreservesBody(t *testing.T) {
	t.Parallel()

	body := "worker update\n---\n![bad](http://invalid/image.png)"
	got := RenderStepMessage(
		deliverycmd.Profile{FormattingMode: "rich_markdown"},
		1,
		25,
		"worker",
		"update",
		body,
	)
	wantPrefix := "**Goal iteration 1/25:** worker update."
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("RenderStepMessage() = %q, want prefix %q", got, wantPrefix)
	}
	if !strings.Contains(got, "\n\n"+body) {
		t.Fatalf("RenderStepMessage() = %q, want unchanged body %q", got, body)
	}
}

func TestRenderGoalStartedMessageHTMLEscapesSystemFields(t *testing.T) {
	t.Parallel()

	got := RenderStartedMessage(
		deliverycmd.Profile{FormattingMode: "rich_html"},
		3,
		"ship <release> & verify",
	)
	for _, want := range []string{
		"<b>Goal run started</b>",
		"<b>Objective:</b> ship &lt;release&gt; &amp; verify",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderStartedMessage() = %q, want %q", got, want)
		}
	}
}

func TestRenderGoalStartedMessageMarkdownUsesBlockSafeLayout(t *testing.T) {
	t.Parallel()

	got := RenderStartedMessage(deliverycmd.Profile{FormattingMode: "rich_markdown"}, 25, "count total go lines")
	for _, want := range []string{
		"**Goal run started**",
		"- **Max iterations:** 25",
		"- **Objective:** count total go lines",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderStartedMessage() = %q, want %q", got, want)
		}
	}
	if strings.Contains(got, "25 **Objective:**") {
		t.Fatalf("RenderStartedMessage() = %q, objective collapsed onto max iterations", got)
	}
}

func TestRenderGoalStepMessageHTMLPreservesBody(t *testing.T) {
	t.Parallel()

	body := "<b>validator</b>\n---\nplain"
	got := RenderStepMessage(
		deliverycmd.Profile{FormattingMode: "html"},
		2,
		5,
		"validator",
		"completed",
		body,
	)
	if !strings.HasPrefix(got, "<b>Goal iteration 2/5:</b> validator completed.") {
		t.Fatalf("RenderStepMessage() = %q, want HTML header", got)
	}
	if !strings.Contains(got, "\n\n"+body) {
		t.Fatalf("RenderStepMessage() = %q, want unchanged body %q", got, body)
	}
}

func TestRenderGoalStatusMessageUnknownModeFallsBackToPlain(t *testing.T) {
	t.Parallel()

	got := RenderStatusMessage(deliverycmd.Profile{FormattingMode: "unknown"}, "Goal run canceled.")
	if got != "Goal run canceled." {
		t.Fatalf("RenderStatusMessage() = %q, want plain fallback", got)
	}
}

func TestRenderReviewableOutcomeOmitsSuccessfulExportDefaults(t *testing.T) {
	t.Parallel()

	got := RenderReviewableOutcome(deliverycmd.Profile{}, taskRecordWithResult(t, true, GoalExportStatusExported, "", "", DefaultNotVerifiedText, DefaultExportedNextAction))
	for _, notWant := range []string{
		"Not verified:",
		"Next action:",
		DefaultNotVerifiedText,
		DefaultExportedNextAction,
	} {
		if strings.Contains(got, notWant) {
			t.Fatalf("RenderReviewableOutcome() = %q, did not want %q", got, notWant)
		}
	}
	if !strings.Contains(got, "Result: Goal completed.") {
		t.Fatalf("RenderReviewableOutcome() = %q, want result", got)
	}
}

func TestRenderReviewableOutcomeMarkdownSuccessfulExportIsConciseAndConsistent(t *testing.T) {
	t.Parallel()

	task := taskRecordWithOutcome(t, true, GoalExportStatusExported, map[string]string{
		"what_was_done":         "Result: 50,528 total lines across 218 *.go files.\nEvidence: find . -name '*.go' -type f -print0 | xargs -0 wc -l over the current workspace.",
		"validation_output":     "verdict: pass",
		"what_was_verified":     "validator returned pass",
		"what_was_not_verified": DefaultNotVerifiedText,
		"next_action":           DefaultExportedNextAction,
	})

	got := RenderReviewableOutcome(deliverycmd.Profile{FormattingMode: "rich_markdown"}, task)
	for _, want := range []string{
		"**Result:** Goal completed.",
		"Result: 50,528 total lines across 218 *.go files.",
		"Evidence: find . -name '*.go' -type f -print0 | xargs -0 wc -l over the current workspace.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderReviewableOutcome() = %q, want %q", got, want)
		}
	}
	for _, notWant := range []string{
		"**Export:** exported.",
		"**What was done:**",
		"**Validation:**\nverdict: pass",
		"**Verified:** validator returned pass",
		"**Not verified:**",
		"**Next action:**",
	} {
		if strings.Contains(got, notWant) {
			t.Fatalf("RenderReviewableOutcome() = %q, did not want %q", got, notWant)
		}
	}
}

func TestRenderReviewableOutcomeNotExportedSuccessHidesExportNoise(t *testing.T) {
	t.Parallel()

	task := taskRecordWithOutcome(t, true, GoalExportStatusNotExported, map[string]string{
		"what_was_done":         "Result: direct workspace complete.",
		"validation_output":     "verdict pass",
		"what_was_verified":     "validator returned pass",
		"what_was_not_verified": DefaultNotVerifiedText,
		"next_action":           DefaultNotExportedNextAction,
		"export_reason":         goalExportReasonDisabled,
	})

	got := RenderReviewableOutcome(deliverycmd.Profile{}, task)
	for _, want := range []string{
		"Result: Goal completed.",
		"Result: direct workspace complete.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderReviewableOutcome() = %q, want %q", got, want)
		}
	}
	for _, notWant := range []string{
		"Export:",
		"workspace mode disabled",
		"Validation: verdict pass",
		"Next action:",
	} {
		if strings.Contains(got, notWant) {
			t.Fatalf("RenderReviewableOutcome() = %q, did not want %q", got, notWant)
		}
	}
}

func TestRenderReviewableOutcomeFailureKeepsEvidence(t *testing.T) {
	t.Parallel()

	task := taskRecordWithOutcome(t, false, "", map[string]string{
		"what_was_done":         "worker tried\nEvidence: worker command output",
		"validation_output":     "verdict: fail\nEvidence: mismatch found",
		"what_was_verified":     "validator returned feedback",
		"what_was_not_verified": DefaultNotVerifiedText,
		"next_action":           "Review failure evidence and rerun /goal or assign a narrower follow-up task.",
	})

	got := RenderReviewableOutcome(deliverycmd.Profile{FormattingMode: "rich_markdown"}, task)
	for _, want := range []string{
		"Evidence: worker command output",
		"Evidence: mismatch found",
		"**Verified:** validator returned feedback",
		"**Next action:** Review failure evidence and rerun /goal or assign a narrower follow-up task.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("RenderReviewableOutcome() = %q, want %q", got, want)
		}
	}
}

func TestRenderReviewableOutcomeKeepsActionableNextActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		goalReached  bool
		exportStatus string
		nextAction   string
	}{
		{
			name:         "export failed",
			goalReached:  true,
			exportStatus: GoalExportStatusFailed,
			nextAction:   "Inspect the preserved goal workspace and retry export after resolving the base-branch issue.",
		},
		{
			name:        "not reached",
			goalReached: false,
			nextAction:  "Review failure evidence and rerun /goal or assign a narrower follow-up task.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := RenderReviewableOutcome(deliverycmd.Profile{}, taskRecordWithResult(t, tt.goalReached, tt.exportStatus, "", "", DefaultNotVerifiedText, tt.nextAction))
			if !strings.Contains(got, "Next action: "+tt.nextAction) {
				t.Fatalf("RenderReviewableOutcome() = %q, want next action %q", got, tt.nextAction)
			}
		})
	}
}

func TestRenderReviewableOutcomeKeepsExplicitNotVerified(t *testing.T) {
	t.Parallel()

	got := RenderReviewableOutcome(deliverycmd.Profile{}, taskRecordWithResult(t, true, GoalExportStatusExported, "logs were not inspected", "", "manual log review remains", DefaultExportedNextAction))
	if !strings.Contains(got, "Not verified: manual log review remains") {
		t.Fatalf("RenderReviewableOutcome() = %q, want explicit not verified", got)
	}
}

const goalExportReasonDisabled = "workspace_disabled"

func taskRecordWithResult(t *testing.T, goalReached bool, exportStatus string, notVerified string, exportReason string, outcomeNotVerified string, nextAction string) baldastate.JobRecord {
	t.Helper()

	return taskRecordWithOutcome(t, goalReached, exportStatus, map[string]string{
		"what_was_done":         "work completed",
		"validation_output":     "verdict: pass",
		"what_was_verified":     "validator returned pass",
		"what_was_not_verified": firstNonEmpty(outcomeNotVerified, notVerified),
		"next_action":           nextAction,
		"export_reason":         exportReason,
	})
}

func taskRecordWithOutcome(t *testing.T, goalReached bool, exportStatus string, outcome map[string]string) baldastate.JobRecord {
	t.Helper()

	result := map[string]any{
		"goal_reached": goalReached,
		"reviewable_outcome": map[string]any{
			"what_was_done":         outcome["what_was_done"],
			"validation_output":     outcome["validation_output"],
			"what_was_verified":     outcome["what_was_verified"],
			"what_was_not_verified": outcome["what_was_not_verified"],
			"next_action":           outcome["next_action"],
		},
	}
	if exportStatus != "" {
		result["export"] = map[string]any{
			"status": exportStatus,
			"reason": outcome["export_reason"],
		}
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	status := baldastate.JobStatusCompleted
	if !goalReached {
		status = baldastate.JobStatusFailed
	}
	return baldastate.JobRecord{
		Status:     status,
		Objective:  "objective",
		ResultJSON: string(data),
	}
}
