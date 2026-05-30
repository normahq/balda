package balda

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestDocumentationContract(t *testing.T) {
	repoRoot := repositoryRoot(t)

	t.Run("agents doc links exist", func(t *testing.T) {
		agentsPath := filepath.Join(repoRoot, "AGENTS.md")
		agents := readFile(t, agentsPath)
		linkRE := regexp.MustCompile("`([^`]+\\.md)`")
		matches := linkRE.FindAllStringSubmatch(agents, -1)
		if len(matches) == 0 {
			t.Fatal("AGENTS.md has no markdown file links in backticks")
		}
		for _, match := range matches {
			rel := match[1]
			p := filepath.Join(repoRoot, filepath.FromSlash(rel))
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("AGENTS.md reference %q does not exist: %v", rel, err)
			}
		}
	})

	t.Run("architecture docs have ownership and test linkage sections", func(t *testing.T) {
		dir := filepath.Join(repoRoot, "docs/architecture")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read docs/architecture: %v", err)
		}
		required := []string{
			"Owner:",
			"Status:",
			"## Invariants",
			"## Related tests",
			"## Related packages",
			"## Update triggers",
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			body := readFile(t, path)
			for _, marker := range required {
				if !strings.Contains(body, marker) {
					t.Fatalf("%s is missing required marker %q", filepath.ToSlash(path), marker)
				}
			}
			if !strings.Contains(body, "internal/apps/balda") {
				t.Fatalf("%s must reference at least one related test/package path", filepath.ToSlash(path))
			}
		}
	})

	t.Run("architecture docs only reference repo paths that exist", func(t *testing.T) {
		dir := filepath.Join(repoRoot, "docs/architecture")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read docs/architecture: %v", err)
		}
		pathRE := regexp.MustCompile("`((?:internal|cmd|docs)/[^`]+)`")
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			body := readFile(t, path)
			matches := pathRE.FindAllStringSubmatch(body, -1)
			for _, match := range matches {
				target := filepath.Join(repoRoot, filepath.FromSlash(match[1]))
				if _, err := os.Stat(target); err != nil {
					t.Fatalf("%s references missing path %q: %v", filepath.ToSlash(path), match[1], err)
				}
			}
		}
	})

	t.Run("stable command and event subjects in docs match code constants", func(t *testing.T) {
		subjectsPath := filepath.Join(repoRoot, "internal/apps/balda/swarm/subjects.go")
		docPath := filepath.Join(repoRoot, "docs/balda.md")
		codeSubjects := subjectConstantsFromFile(t, subjectsPath)
		docBody := readFile(t, docPath)

		for _, subject := range codeSubjects {
			if strings.HasSuffix(subject, ".>") {
				continue
			}
			if !strings.HasPrefix(subject, "balda.v1.cmd.") &&
				!strings.HasPrefix(subject, "balda.v1.evt.") &&
				!strings.HasPrefix(subject, "balda.v1.dlq.") {
				continue
			}
			if !strings.Contains(docBody, subject) {
				t.Fatalf("docs/balda.md missing stable subject %q", subject)
			}
		}
	})

	t.Run("active and completed exec plans carry status", func(t *testing.T) {
		activeDir := filepath.Join(repoRoot, "docs/exec-plans/active")
		completedDir := filepath.Join(repoRoot, "docs/exec-plans/completed")
		assertMarkdownStatus(t, activeDir, "Status: active")
		assertMarkdownStatus(t, completedDir, "Status: completed")
	})

	t.Run("completed exec plans are archived outside active directory", func(t *testing.T) {
		activeDir := filepath.Join(repoRoot, "docs/exec-plans/active")
		completedDir := filepath.Join(repoRoot, "docs/exec-plans/completed")

		activeFiles := markdownFilenames(t, activeDir)
		completedFiles := markdownFilenames(t, completedDir)
		for name := range activeFiles {
			if _, ok := completedFiles[name]; ok {
				t.Fatalf("exec plan %q exists in both active and completed directories", name)
			}
		}
	})

	t.Run("user-facing docs do not advertise removed telegram debug commands", func(t *testing.T) {
		paths := []string{
			filepath.Join(repoRoot, "README.md"),
			filepath.Join(repoRoot, "docs/balda.md"),
			filepath.Join(repoRoot, "cmd/balda/balda.yaml"),
		}
		forbidden := []*regexp.Regexp{
			regexp.MustCompile(`/tasks\b`),
			regexp.MustCompile(`/task <id>`),
			regexp.MustCompile(`/swarm status\b`),
			regexp.MustCompile(`/queue status\b`),
			regexp.MustCompile(`/mailbox status\b`),
			regexp.MustCompile(`/projection status\b`),
			regexp.MustCompile(`/actors status\b`),
			regexp.MustCompile(`/dlq\b`),
			regexp.MustCompile(`/reset\b`),
			regexp.MustCompile(`balda\.swarm\.enabled`),
		}
		for _, path := range paths {
			body := readFile(t, path)
			for _, pattern := range forbidden {
				if pattern.FindStringIndex(body) != nil {
					t.Fatalf("%s still advertises removed surface %q", filepath.ToSlash(path), pattern.String())
				}
			}
		}
	})

	t.Run("session close docs match current behavior", func(t *testing.T) {
		paths := []string{
			filepath.Join(repoRoot, "README.md"),
			filepath.Join(repoRoot, "docs/balda.md"),
			filepath.Join(repoRoot, "AGENTS.md"),
		}
		forbidden := []*regexp.Regexp{
			regexp.MustCompile(`restart the owner session`),
			regexp.MustCompile(`stop(?:s)? the owner session`),
		}
		for _, path := range paths {
			body := readFile(t, path)
			for _, pattern := range forbidden {
				if pattern.FindStringIndex(body) != nil {
					t.Fatalf("%s contains stale /close behavior text %q", filepath.ToSlash(path), pattern.String())
				}
			}
		}
	})

	t.Run("agent docs use merge pull workflow", func(t *testing.T) {
		path := filepath.Join(repoRoot, "AGENTS.md")
		body := readFile(t, path)
		if strings.Contains(body, "git pull --rebase") {
			t.Fatalf("%s contains stale rebase workflow", filepath.ToSlash(path))
		}
		if !strings.Contains(body, "git pull --no-rebase") {
			t.Fatalf("%s must document merge-based pull workflow", filepath.ToSlash(path))
		}
	})

	t.Run("user-facing config samples do not expose swarm.enabled", func(t *testing.T) {
		paths := []string{
			filepath.Join(repoRoot, "README.md"),
			filepath.Join(repoRoot, "docs/balda.md"),
			filepath.Join(repoRoot, "cmd/balda/balda.yaml"),
		}
		for _, path := range paths {
			body := readFile(t, path)
			if hasNestedConfigKey(body, "swarm", "enabled") {
				t.Fatalf("%s still exposes swarm.enabled in a config sample", filepath.ToSlash(path))
			}
		}
	})

	t.Run("architecture docs do not describe removed status interface surfaces", func(t *testing.T) {
		paths := []string{
			filepath.Join(repoRoot, "docs/architecture/index.md"),
			filepath.Join(repoRoot, "docs/architecture/runtime-contract.md"),
			filepath.Join(repoRoot, "docs/architecture/actor-runtime.md"),
		}
		forbidden := []*regexp.Regexp{
			regexp.MustCompile(`event projection/status`),
			regexp.MustCompile(`dispatch/event/status interfaces`),
			regexp.MustCompile(`projection/status integration`),
			regexp.MustCompile(`operator-facing status surfaces`),
		}
		for _, path := range paths {
			body := readFile(t, path)
			for _, pattern := range forbidden {
				if pattern.FindStringIndex(body) != nil {
					t.Fatalf("%s still describes removed status surface %q", filepath.ToSlash(path), pattern.String())
				}
			}
		}
	})
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	t.Fatalf("failed to locate repo root from %s", wd)
	return ""
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func hasNestedConfigKey(body string, parent string, child string) bool {
	lines := strings.Split(body, "\n")
	inParent := false
	parentIndent := 0
	for _, raw := range lines {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		if !inParent {
			if trimmed == parent+":" {
				inParent = true
				parentIndent = indent
			}
			continue
		}
		if indent <= parentIndent {
			inParent = false
			if trimmed == parent+":" {
				inParent = true
				parentIndent = indent
			}
			continue
		}
		if trimmed == child+":" || strings.HasPrefix(trimmed, child+": ") {
			return true
		}
	}
	return false
}

func subjectConstantsFromFile(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	quoted := regexp.MustCompile(`"([^"]+)"`)
	var values []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Values) != 1 {
				continue
			}
			lit, ok := vs.Values[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			m := quoted.FindStringSubmatch(lit.Value)
			if len(m) != 2 {
				continue
			}
			values = append(values, m[1])
		}
	}
	slices.Sort(values)
	return values
}

func assertMarkdownStatus(t *testing.T, dir string, statusLine string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.ToSlash(dir), err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		body := readFile(t, path)
		if !strings.Contains(body, statusLine) {
			t.Fatalf("%s missing required status marker %q", filepath.ToSlash(path), statusLine)
		}
	}
}

func markdownFilenames(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.ToSlash(dir), err)
	}
	out := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		out[entry.Name()] = struct{}{}
	}
	return out
}
