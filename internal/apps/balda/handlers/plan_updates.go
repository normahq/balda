package handlers

import (
	"github.com/normahq/balda/internal/apps/balda/progress"
	adksession "google.golang.org/adk/v2/session"
)

func baldaPlanProgressText(ev *adksession.Event) (string, bool) {
	return progress.PlanUpdateText(ev)
}
