package session

import (
	"fmt"
)

func (m *Manager) sessionBranchName(sessionID string) string {
	return fmt.Sprintf("norma/balda/%s", sessionID)
}
