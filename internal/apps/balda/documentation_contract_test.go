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
