package swarm

import "testing"

func TestConfigNormalized_DefaultsModesToShadow(t *testing.T) {
	t.Parallel()

	got, err := (Config{Enabled: true}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if got.Mode != ModeShadow {
		t.Fatalf("Mode = %q, want %q", got.Mode, ModeShadow)
	}
	if got.WebhookMode != ModeShadow {
		t.Fatalf("WebhookMode = %q, want %q", got.WebhookMode, ModeShadow)
	}
	if got.SchedulerMode != ModeShadow {
		t.Fatalf("SchedulerMode = %q, want %q", got.SchedulerMode, ModeShadow)
	}
	if got.Queue.DefaultMode != QueueModeFollowup {
		t.Fatalf("Queue.DefaultMode = %q, want %q", got.Queue.DefaultMode, QueueModeFollowup)
	}
	if got.Queue.ByNamespace[NamespaceTaskControl] != QueueModeInterrupt {
		t.Fatalf("Queue task.control mode = %q, want %q", got.Queue.ByNamespace[NamespaceTaskControl], QueueModeInterrupt)
	}
}

func TestConfigNormalized_RejectsInvalidModes(t *testing.T) {
	t.Parallel()

	for _, cfg := range []Config{
		{Mode: "invalid"},
		{WebhookMode: "invalid"},
		{SchedulerMode: "invalid"},
	} {
		if _, err := cfg.Normalized(); err == nil {
			t.Fatalf("Normalized(%+v) error = nil, want non-nil", cfg)
		}
	}
}

func TestConfigNormalized_RejectsInvalidQueuePolicy(t *testing.T) {
	t.Parallel()

	for _, cfg := range []Config{
		{Queue: QueueConfig{DefaultMode: "invalid"}},
		{Queue: QueueConfig{Drop: "invalid"}},
		{Queue: QueueConfig{ByNamespace: map[string]string{NamespaceTaskControl: "invalid"}}},
	} {
		if _, err := cfg.Normalized(); err == nil {
			t.Fatalf("Normalized(%+v) error = nil, want non-nil", cfg)
		}
	}
}

func TestQueueConfigPolicyForAppliesOverridesAndPriority(t *testing.T) {
	t.Parallel()

	cfg, err := (QueueConfig{
		DefaultMode: QueueModeFollowup,
		ByNamespace: map[string]string{
			NamespaceWebhookInbound: QueueModeInterrupt,
		},
	}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	policy := cfg.PolicyFor(NamespaceWebhookInbound)
	if policy.Mode != QueueModeInterrupt {
		t.Fatalf("Mode = %q, want %q", policy.Mode, QueueModeInterrupt)
	}
	if policy.Priority != 80 {
		t.Fatalf("Priority = %d, want 80", policy.Priority)
	}
	if policy.Cap != defaultQueueCap {
		t.Fatalf("Cap = %d, want %d", policy.Cap, defaultQueueCap)
	}
}

func TestConfigMailboxEnabled_WhenAnySourceUsesMailbox(t *testing.T) {
	t.Parallel()

	for _, cfg := range []Config{
		{Enabled: true, Mode: ModeMailbox},
		{Enabled: true, WebhookMode: ModeMailbox},
		{Enabled: true, SchedulerMode: ModeMailbox},
	} {
		if !cfg.MailboxEnabled() {
			t.Fatalf("MailboxEnabled(%+v) = false, want true", cfg)
		}
	}
	if (Config{Enabled: false, Mode: ModeMailbox}).MailboxEnabled() {
		t.Fatal("MailboxEnabled(disabled) = true, want false")
	}
}
