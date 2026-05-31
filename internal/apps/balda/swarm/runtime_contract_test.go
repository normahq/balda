package swarm

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
)

var (
	_ actorengine.Delivery = (*runtimeDelivery)(nil)
	_ actorengine.Source   = runtimeSource{}
)

// TestRuntimeCoreNoProviderImports verifies that the runtime core module does
// not introduce direct session SDK imports, which keeps the actorlayer engine
// boundary explicit.
func TestRuntimeCoreNoProviderImports(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(testFile)
	fset := token.NewFileSet()
	for _, file := range []string{"runtime.go"} {
		path := filepath.Join(root, file)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		parsed, err := parser.ParseFile(fset, path, src, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("ParseFile(%s) error = %v", path, err)
		}
		for _, imp := range parsed.Imports {
			pathValue := strings.Trim(imp.Path.Value, "\"")
			if strings.HasPrefix(pathValue, "google.golang.org/adk/") {
				t.Fatalf("runtime.go import %q is disallowed in runtime core", pathValue)
			}
		}
	}
}
