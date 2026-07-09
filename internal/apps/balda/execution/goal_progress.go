package execution

import (
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/progress"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

type GoalProgressKind string

const (
	GoalProgressKindPlan      GoalProgressKind = "plan"
	GoalProgressKindOutput    GoalProgressKind = "output"
	GoalProgressKindCompleted GoalProgressKind = "completed"
)

type GoalProgressUpdate struct {
	JobID         string
	Locator       baldasession.SessionLocator
	Profile       deliveryfmt.Profile
	Step          string
	Iteration     int
	MaxIterations int
	Kind          GoalProgressKind
	Text          string
	Plan          *progress.PlanSnapshot
	Sequence      int
}
