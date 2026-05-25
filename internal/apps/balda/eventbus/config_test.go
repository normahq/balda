package eventbus

import "testing"

func TestConfigNormalizedDefaultsToNATSCore(t *testing.T) {
	cfg, err := (Config{}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if cfg.Mode != ModeNATSCore {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, ModeNATSCore)
	}
	if !cfg.NATS.Embedded {
		t.Fatal("NATS.Embedded = false, want true")
	}
	if cfg.NATS.Host != defaultNATSHost || cfg.NATS.Port != defaultNATSPort {
		t.Fatalf("NATS address = %s:%d, want %s:%d", cfg.NATS.Host, cfg.NATS.Port, defaultNATSHost, defaultNATSPort)
	}
	if !cfg.NATS.JetStream {
		t.Fatal("NATS.JetStream = false, want true")
	}
}

func TestConfigNormalizedRejectsInvalidMode(t *testing.T) {
	_, err := (Config{Mode: "redis"}).Normalized()
	if err == nil {
		t.Fatal("Normalized() error = nil, want invalid mode error")
	}
}
