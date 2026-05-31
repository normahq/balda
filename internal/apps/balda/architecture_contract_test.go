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

func TestJetStreamArchitectureContractStatic(t *testing.T) {
	root := baldaPackageRoot(t)
	files := productionGoFiles(t, root)

	t.Run("turn dispatcher is only a session actor implementation detail", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\.Enqueue\s*\(`))
		assertOnlyAllowedFiles(t, matches, []string{"actors/swarm_session_actor.go"})
		if len(matches) == 0 {
			t.Fatal("no TurnDispatcher.Enqueue call found; expected the session-turn execution path to remain the only code path allowed to enqueue TurnDispatcher work")
		}
	})

	t.Run("handlers do not call turn dispatcher cancellation directly", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\.CancelSession\s*\(`))
		assertOnlyAllowedFiles(t, matches, []string{
			"actors/turn_dispatcher.go",
			"actors/swarm_control_actor.go",
			"actors/swarm_session_actor.go",
		})
	})

	t.Run("balda product actors live outside telegram handlers", func(t *testing.T) {
		forbiddenHandlers, err := filepath.Glob(filepath.Join(root, "handlers", "swarm_*_actor.go"))
		if err != nil {
			t.Fatalf("glob handler actor files: %v", err)
		}
		if len(forbiddenHandlers) > 0 {
			t.Fatalf("Balda product actors must live in internal/apps/balda/actors, found handlers files: %v", forbiddenHandlers)
		}
		handlersSource := readPackageSource(t, filepath.Join(root, "handlers"))
		if strings.Contains(handlersSource, `group:"balda_swarm_actors"`) {
			t.Fatal("handlers module must not provide balda_swarm_actors; actor registration belongs to internal/apps/balda/actors")
		}
		actorsSource := readPackageSource(t, filepath.Join(root, "actors"))
		if !strings.Contains(actorsSource, `group:"balda_swarm_actors"`) {
			t.Fatal("internal/apps/balda/actors must provide Balda product actors to balda_swarm_actors")
		}
	})

	t.Run("user command routing stays owned by command handler", func(t *testing.T) {
		handlerSource := readSource(t, filepath.Join(root, "handlers/command_handler.go"))
		if !strings.Contains(handlerSource, `case "user":`) {
			t.Fatal("command handler must own /user routing")
		}
		fxSource := readSource(t, filepath.Join(root, "handlers/fx.go"))
		if strings.Contains(fxSource, "registerUserHandler") {
			t.Fatal("user handler must not be registered as an independent bot handler")
		}
		userHandlerSource := readSource(t, filepath.Join(root, "handlers/user_handler.go"))
		if strings.Contains(userHandlerSource, "func (h *userHandler) Register(") {
			t.Fatal("user handler must not expose a standalone Register hook")
		}
	})

	t.Run("actors execute from actorlayer delivery source", func(t *testing.T) {
		runtimeSource := readSource(t, filepath.Join(root, "swarm/runtime.go"))
		if !strings.Contains(runtimeSource, "Source actorengine.Source") {
			t.Fatal("swarm runtime must depend on actorlayer Source, not a direct transport consumer")
		}
		if !strings.Contains(runtimeSource, "actorengine.NewDispatchRuntime") {
			t.Fatal("swarm runtime must use Norma actorengine.NewDispatchRuntime")
		}
		if !strings.Contains(runtimeSource, "runtimeSource{") || !strings.Contains(runtimeSource, "r.engine.Run(runCtx") {
			t.Fatal("swarm runtime must dispatch actor deliveries through actorlayer dispatch runtime")
		}
	})

	t.Run("balda coordinator layer is removed", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`swarm\.Coordinator|NewCoordinator|type Coordinator\b`))
		if len(matches) > 0 {
			t.Fatalf("Balda must inject ActorDispatcher directly instead of swarm.Coordinator:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("nats imports stay inside jetstream adapter", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`github\.com/nats-io/`))
		assertOnlyAllowedFiles(t, matches, []string{
			"eventbus/nats/connection.go",
			"eventbus/nats/embedded_server.go",
			"eventbus/nats/jetstream.go",
			"eventbus/nats/subjects.go",
		})
	})

	t.Run("product code does not use direct transport contracts", func(t *testing.T) {
		forbidden := []string{
			"CommandMessage",
			"CommandPublisher",
			"CommandConsumer",
			"CoordinatorBus",
			"RuntimeBus",
			"RunCommandConsumer",
			"PublishCommand(",
		}
		for _, needle := range forbidden {
			t.Run(needle, func(t *testing.T) {
				pattern := regexp.QuoteMeta(needle)
				if !strings.Contains(needle, "(") {
					pattern = `\b` + pattern + `\b`
				}
				matches := findSourceMatches(t, root, files, regexp.MustCompile(pattern))
				assertOnlyAllowedFiles(t, matches, []string{
					"architecture_contract_test.go",
				})
			})
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
			"sqlite command queue",
			"sqlite-backed command queue",
			"sqlite command bus",
			"ShadowMode",
			"LegacyDirectPath",
			"sqlite_command_bus",
			"shadow_mode",
			"legacy_direct_path",
			"nats_core",
			"nats_jetstream",
			"webhook_mode",
			"scheduler_mode",
			"JetStream-backed actor runtime",
			"embedded JetStream defaults",
			"durable JetStream events for read-model projection",
			"required JetStream command/event bus",
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

	t.Run("sqlite mailbox polling loop cannot be started by runtime code", func(t *testing.T) {
		forbiddenPollingSymbols := []string{
			"MailboxPoller",
			"MailboxPollingLoop",
			"RunMailboxLoop",
			"StartMailboxLoop",
			"RunSQLiteMailbox",
			"StartSQLiteMailbox",
			"ClaimRunnableCommand",
			"ClaimRunnableCommands",
			"PollRunnableCommand",
			"PollRunnableCommands",
		}
		for _, needle := range forbiddenPollingSymbols {
			t.Run(needle, func(t *testing.T) {
				matches := findSourceMatches(t, root, files, regexp.MustCompile(regexp.QuoteMeta(needle)))
				if len(matches) > 0 {
					t.Fatalf("removed sqlite mailbox polling symbol %q found in production Go files:\n%s", needle, formatSourceMatches(matches))
				}
			})
		}
	})

	t.Run("ingress publishes commands before local state is advanced", func(t *testing.T) {
		schedulerSource := readSource(t, filepath.Join(root, "handlers/scheduled_task_scheduler.go"))
		if !strings.Contains(schedulerSource, "s.dispatcher.Dispatch(ctx, env)") {
			t.Fatal("scheduler ingress must dispatch durable actor work through ActorDispatcher")
		}
		if !strings.Contains(schedulerSource, "Mark the slot only after durable command dispatch succeeds.") {
			t.Fatal("scheduler must document and preserve publish-before-dispatch-state ordering")
		}

		webhookSource := readSource(t, filepath.Join(root, "handlers/inbound_webhook.go"))
		if !strings.Contains(webhookSource, "submitWebhookTask(") {
			t.Fatal("webhook ingress must publish a task command instead of executing directly")
		}
		if strings.Contains(webhookSource, "runTurnTaskWithDelivery(") {
			t.Fatal("webhook ingress must not expose direct turn execution hooks")
		}

		telegramSource := readSource(t, filepath.Join(root, "handlers/balda.go"))
		if !strings.Contains(telegramSource, "h.enqueueTurn(") {
			t.Fatal("telegram ingress must route user messages through the session command publisher")
		}
	})

	t.Run("removed telegram debug commands are not routed", func(t *testing.T) {
		handlerSource := readSource(t, filepath.Join(root, "handlers/command_handler.go"))
		for _, removed := range []string{
			`case "reset"`,
			`case "tasks"`,
			`case "task"`,
			`case "swarm"`,
			`case "queue"`,
			`case "mailbox"`,
			`case "projection"`,
			`case "actors"`,
			`case "dlq"`,
			`case "memory"`,
		} {
			if strings.Contains(handlerSource, removed) {
				t.Fatalf("command handler still routes removed command %q", removed)
			}
		}
	})

	t.Run("removed runtime status and dlq inspection surfaces stay deleted", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\bActorRuntimeStatusProvider\b|\bDLQInspector\b|\bRuntimeStatus\b|\bConsumerStatus\b|\bStreamStatus\b|\bErrDLQEntryNotFound\b|\bGetDLQEntry\s*\(`))
		if len(matches) > 0 {
			t.Fatalf("removed runtime status or dlq inspection surface found in production Go files:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("runtime lane inspection stays out of the public balda surface", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\bRuntimeLaneStatus\b|\bLaneStatus\s*\(`))
		if len(matches) > 0 {
			t.Fatalf("removed lane inspection surface found in production Go files:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("test-only and package-local helper surfaces stay out of production api", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`\bPublishDLQ\s*\(|\bStarted\s*\(\)\s+bool|func\s+\(h\s+\*BaldaHandler\)\s+RunSessionTurnPayload\s*\(|func\s+\(h\s+\*BaldaHandler\)\s+SendToOwner\s*\(|func\s+\(s\s+\*ScheduledTaskScheduler\)\s+MarkSuccess\s*\(|func\s+\(s\s+\*ScheduledTaskScheduler\)\s+RecordExecutionFailure\s*\(|func\s+\(s\s+\*ScheduledTaskScheduler\)\s+executeTaskTurn\s*\(|func\s+NewMemoryActor\s*\(|func\s+NewMemoryActorWithStore\s*\(|func\s+NewDeliveryActor\s*\(|func\s+NewActorDispatcher\s*\(|func\s+NewEventPublisher\s*\(|func\s+NewBusDrainer\s*\(|func\s+NewActorDeliverySource\s*\(|func\s+NewEventConsumer\s*\(|func\s+\(m\s+\*Messenger\)\s+KeepTyping\s*\(`))
		if len(matches) > 0 {
			t.Fatalf("test-only helper surface found in production Go files:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("dead agent and session convenience exports stay deleted", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`func\s+\(m\s+\*Manager\)\s+SessionBranchName\s*\(|func\s+\(s\s+\*TopicSession\)\s+GetLocator\s*\(|func\s+\(m\s+\*Manager\)\s+GetAgentInfo\s*\(|func\s+\(m\s+\*Manager\)\s+ProviderIDs\s*\(|func\s+\(b\s+\*Builder\)\s+GetAgentInfo\s*\(|func\s+\(b\s+\*Builder\)\s+ProviderIDs\s*\(|func\s+\(m\s+\*Manager\)\s+ValidateAgent\s*\(|func\s+\(b\s+\*Builder\)\s+ValidateAgent\s*\(|func\s+\(m\s+\*Manager\)\s+HasSession\s*\(|func\s+\(m\s+\*Manager\)\s+ListSessions\s*\(|func\s+\(m\s+\*Manager\)\s+ListSessionInfos\s*\(|func\s+\(m\s+\*Manager\)\s+StopSessionByID\s*\(|func\s+\(m\s+\*Manager\)\s+StopAll\s*\(`))
		if len(matches) > 0 {
			t.Fatalf("dead agent/session convenience export found in production Go files:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("unused mcp server entrypoints stay deleted", func(t *testing.T) {
		paths := []string{
			filepath.Clean(filepath.Join(root, "../sessionmcp/server.go")),
			filepath.Clean(filepath.Join(root, "../workspacemcp/server.go")),
		}
		pattern := regexp.MustCompile(`type\s+HTTPServerResult\s+struct|func\s+RunHTTP?\s*\(|func\s+StartHTTPServer\s*\(|func\s+NewServer\s*\(`)
		for _, path := range paths {
			source := readSource(t, path)
			if pattern.FindStringIndex(source) != nil {
				t.Fatalf("%s still defines unused MCP server entrypoint", filepath.ToSlash(path))
			}
		}
	})

	t.Run("sessionmcp production store keeps test memory helpers out", func(t *testing.T) {
		path := filepath.Clean(filepath.Join(root, "../sessionmcp/store.go"))
		source := readSource(t, path)
		pattern := regexp.MustCompile(`type\s+MemoryStore\s+struct|func\s+NewMemoryStore\s*\(|func\s+ResetSharedStore\s*\(`)
		if pattern.FindStringIndex(source) != nil {
			t.Fatalf("%s still defines test-only in-memory store helpers", filepath.ToSlash(path))
		}
	})

	t.Run("handler wiring stays package-local", func(t *testing.T) {
		matches := findSourceMatches(t, root, files, regexp.MustCompile(`type\s+StartHandlerParams\s+struct|func\s+NewStartHandler\s*\(|func\s+\(h\s+\*StartHandler\)\s+SetBaldaHandler\s*\(|func\s+NewBaldaHandler\s*\(|func\s+\(h\s+\*BaldaHandler\)\s+SetOwner\s*\(|func\s+\(h\s+\*BaldaHandler\)\s+ActivateOwner\s*\(|func\s+NewCommandHandler\s*\(|func\s+NewUserHandler\s*\(|func\s+NewScheduledTaskScheduler\s*\(|func\s+NewInboundWebhookReceiver\s*\(|func\s+WireHandlers\s*\(`))
		if len(matches) > 0 {
			t.Fatalf("handler-local wiring surface must not be exported:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("handler package does not wrap the welcome formatter", func(t *testing.T) {
		source := readSource(t, filepath.Join(root, "handlers/command_handler.go"))
		if regexp.MustCompile(`func\s+BuildAgentWelcomeMessage\s*\(`).FindStringIndex(source) != nil {
			t.Fatal("handlers/command_handler.go must not define a BuildAgentWelcomeMessage wrapper")
		}
	})

	t.Run("swarm runtime stays always on", func(t *testing.T) {
		swarmConfigSource := readSource(t, filepath.Join(root, "swarm/config.go"))
		if regexp.MustCompile(`(?m)^\s*Enabled\b`).FindStringIndex(swarmConfigSource) != nil {
			t.Fatal("swarm.Config must not expose an Enabled field")
		}

		appConfigSource := readSource(t, filepath.Join(root, "config.go"))
		if regexp.MustCompile(`type SwarmConfig struct \{\s*Enabled\b`).FindStringIndex(appConfigSource) != nil {
			t.Fatal("BaldaConfig.SwarmConfig must not expose a removed enabled field")
		}
	})

	t.Run("runtime is initialized before ingress starts accepting transport input", func(t *testing.T) {
		appSource := readSource(t, filepath.Join(root, "app.go"))
		const runtimeInit = "runtimeManager.EnsureRuntime(ctx)"
		const botRun = "bot.Run(runCtx)"
		if !strings.Contains(appSource, runtimeInit) {
			t.Fatalf("app startup must initialize balda runtime via %q before ingress starts", runtimeInit)
		}
		if !strings.Contains(appSource, botRun) {
			t.Fatalf("app startup must run transport ingress via %q", botRun)
		}
		if strings.Index(appSource, runtimeInit) > strings.Index(appSource, botRun) {
			t.Fatal("transport ingress starts before runtime initialization")
		}

		busSource := readSource(t, filepath.Join(root, "eventbus/nats/connection.go"))
		const ensureRuntimeCall = "bus.ensureRuntime(context.Background())"
		const lifecycleHook = "params.LC.Append(fx.Hook{OnStop: bus.Drain})"
		if !strings.Contains(busSource, ensureRuntimeCall) {
			t.Fatalf("actor runtime transport must call %q during construction", ensureRuntimeCall)
		}
		if !strings.Contains(busSource, lifecycleHook) {
			t.Fatalf("actor runtime transport lifecycle hook %q is missing", lifecycleHook)
		}
		if strings.Index(busSource, ensureRuntimeCall) > strings.Index(busSource, lifecycleHook) {
			t.Fatal("actor runtime transport readiness checks must run before transport lifecycle can continue")
		}
	})

	t.Run("swarm runtime package does not import ingress handlers or ADK", func(t *testing.T) {
		swarmDir := filepath.Join(root, "swarm")
		swarmFiles := productionGoFiles(t, swarmDir)
		forbiddenImportPattern := regexp.MustCompile(`github\.com/normahq/balda/internal/apps/balda/handlers|google\.golang\.org/adk`)
		matches := findSourceMatches(t, swarmDir, swarmFiles, forbiddenImportPattern)
		if len(matches) > 0 {
			t.Fatalf("swarm runtime packages must not import ingress handlers or ADK:\n%s", formatSourceMatches(matches))
		}
	})

	t.Run("balda does not define local actor adapter packages or runtime selectors", func(t *testing.T) {
		forbiddenDirs := []string{
			"adapters",
			"norma",
			"actoradapter",
			"actoradapters",
		}
		for _, dir := range forbiddenDirs {
			path := filepath.Join(root, dir)
			if info, err := os.Stat(path); err == nil && info.IsDir() {
				t.Fatalf("Balda-local actor adapter package %q is forbidden; keep typed actor engine packages owned by Norma", filepath.ToSlash(path))
			} else if err != nil && !os.IsNotExist(err) {
				t.Fatalf("stat %s: %v", path, err)
			}
		}

		forbiddenSelectorTerms := []string{
			"execution_provider",
			"delivery_provider",
			"execution.provider",
			"delivery.provider",
			"RuntimeAdapterFingerprint",
		}
		for _, needle := range forbiddenSelectorTerms {
			t.Run(needle, func(t *testing.T) {
				matches := findSourceMatches(t, root, files, regexp.MustCompile(regexp.QuoteMeta(needle)))
				if len(matches) > 0 {
					t.Fatalf("Balda runtime selector term %q found in production Go files:\n%s", needle, formatSourceMatches(matches))
				}
			})
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
			if entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
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

func readPackageSource(t *testing.T, dir string) string {
	t.Helper()
	files := productionGoFiles(t, dir)
	var out strings.Builder
	for _, rel := range files {
		out.WriteString(readSource(t, filepath.Join(dir, filepath.FromSlash(rel))))
		out.WriteByte('\n')
	}
	return out.String()
}
