package session

// SessionContext carries the channel locator plus the transport actor identity
// used to bind the underlying runtime session.
type SessionContext struct {
	Locator                    SessionLocator
	UserID                     string
	AllowBaldaProviderFallback bool
}
