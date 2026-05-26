package balda

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestJetStreamArchitectureContract_Static(t *testing.T) {
	root := baldaPackageRoot(t)
	files := productionGoFiles(t, root)

	t.Run("turn dispatcher is only a session actor implementation detail", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\.Enqueue\s*\(`))
		assertOnlyAllowedFiles(t, matches, []string{"handlers/swarm_session_actor.go"})
		if len(matches) == 0 {
			t.Fatal("no TurnDispatcher.Enqueue call found; expected SessionActor to remain the only adapter to TurnDispatcher")
		}
	})

	t.Run("actors execute from the JetStream command consumer", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`RunCommandConsumer\s*\(`))
		assertOnlyAllowedFiles(t, matches, []string{
			"eventbus/nats/jetstream.go",
			"swarm/bus.go",
			"swarm/runtime.go",
		})
		runtimeSource := readSource(t, filepath.Join(root, "swarm/runtime.go"))
		if !strings.Contains(runtimeSource, "RunCommandConsumer(runCtx, r.HandleCommand)") {
			t.Fatal("swarm runtime must dispatch actor commands only from the JetStream command consumer")
		}
	})

	t.Run("sqlite mailbox and shadow transport vocabulary stays out of runtime code", func(t *testing.T) {
		forbidden := []string{
			"MailboxService",
			"SQLiteMailbox",
			"balda.v1.wakeup.mailbox",
			"wakeup.mailbox",
			"swarm_messages",
			"balda_mailbox_messages",
			"SQLiteCommandBus",
			"ShadowMode",
			"LegacyDirectPath",
			"sqlite_command_bus",
			"shadow_mode",
			"legacy_direct_path",
			"nats_core",
			"nats_jetstream",
			"webhook_mode",
			"scheduler_mode",
		}
		for _, needle := range forbidden {
			t.Run(needle, func(t *testing.T) {
				matches := findSourceMatches(t, root, files, regexp.MustCompile(regexp.QuoteMeta(needle)))
				if len(matches) > 0 {
					t.Fatalf("retired transport term %q found in production Go files:\n%s", needle, formatSourceMatches(matches))
				}
			})
		}
	})

	t.Run("ingress publishes commands before local state is advanced", func(t *testing.T) {
		schedulerSource := readSource(t, filepath.Join(root, "handlers/job_scheduler.go"))
		if !strings.Contains(schedulerSource, "s.coordinator.Submit(ctx, env)") {
			t.Fatal("scheduler ingress must publish the durable JetStream command through SwarmCoordinator")
		}
		if !strings.Contains(schedulerSource, "Mark the slot only after JetStream accepts the command.") {
			t.Fatal("scheduler must document and preserve publish-before-dispatch-state ordering")
		}

		webhookSource := readSource(t, filepath.Join(root, "handlers/inbound_webhook.go"))
		if !strings.Contains(webhookSource, "submitWebhookTask(") {
			t.Fatal("webhook ingress must publish a task command instead of executing directly")
		}

		telegramSource := readSource(t, filepath.Join(root, "handlers/balda.go"))
		if !strings.Contains(telegramSource, "h.enqueueTurn(") {
			t.Fatal("telegram ingress must route user messages through the session command publisher")
		}
	})
}

type sourceMatch struct {
	path string
	line int
	text string
}

func baldaPackageRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func productionGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "testdata":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production Go files: %v", err)
	}
	slices.Sort(files)
	return files
}

func findSourceMatches(t *testing.T, root string, files []string, pattern *regexp.Regexp) []sourceMatch {
	t.Helper()
	var matches []sourceMatch
	for _, rel := range files {
		source := readSource(t, filepath.Join(root, filepath.FromSlash(rel)))
		for idx, line := range strings.Split(source, "\n") {
			if pattern.MatchString(line) {
				matches = append(matches, sourceMatch{path: rel, line: idx + 1, text: strings.TrimSpace(line)})
			}
		}
	}
	return matches
}

func assertOnlyAllowedFiles(t *testing.T, matches []sourceMatch, allowed []string) {
	t.Helper()
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, path := range allowed {
		allowedSet[path] = struct{}{}
	}
	var unexpected []sourceMatch
	for _, match := range matches {
		if _, ok := allowedSet[match.path]; !ok {
			unexpected = append(unexpected, match)
		}
	}
	if len(unexpected) > 0 {
		t.Fatalf("unexpected architecture-contract matches:\n%s", formatSourceMatches(unexpected))
	}
}

func formatSourceMatches(matches []sourceMatch) string {
	var out strings.Builder
	for _, match := range matches {
		fmt.Fprintf(&out, "%s:%d: %s\n", match.path, match.line, match.text)
	}
	return strings.TrimRight(out.String(), "\n")
}

func readSource(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
