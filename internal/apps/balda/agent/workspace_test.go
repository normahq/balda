package agent

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const testWorkspaceBranchContent = "branch\n"

func TestWorkspaceImportDiscardsDirtyChangesAndSyncsToBaseBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "base.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "base.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-1-0"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "base.txt"), "dirty change\n")
	writeFile(t, filepath.Join(workspaceDir, "scratch.txt"), "scratch\n")

	writeFile(t, filepath.Join(workingDir, "base-branch-only.txt"), "base-branch-only\n")
	runGit(t, ctx, workingDir, "add", "base-branch-only.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: update base branch")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	if err := m.Import(ctx, workspaceDir); err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	status := runGit(t, ctx, workspaceDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		t.Fatalf("expected clean workspace after import, got:\n%s", status)
	}

	if _, err := os.Stat(filepath.Join(workspaceDir, "scratch.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected scratch.txt to be absent, stat err=%v", err)
	}

	if got := readFile(t, filepath.Join(workspaceDir, "base.txt")); got != "base\n" {
		t.Fatalf("base.txt mismatch: got %q", got)
	}
	if got := readFile(t, filepath.Join(workspaceDir, "base-branch-only.txt")); got != "base-branch-only\n" {
		t.Fatalf("base-branch-only.txt mismatch: got %q", got)
	}
}

func TestWorkspaceImportRebasesCleanBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "base.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "base.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-1-1"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "branch.txt"), testWorkspaceBranchContent)
	runGit(t, ctx, workspaceDir, "add", "branch.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch change")

	writeFile(t, filepath.Join(workingDir, "base-branch.txt"), "base-branch\n")
	runGit(t, ctx, workingDir, "add", "base-branch.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: base branch change")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	if err := m.Import(ctx, workspaceDir); err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	status := runGit(t, ctx, workspaceDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		t.Fatalf("expected clean workspace after import, got:\n%s", status)
	}

	if got := readFile(t, filepath.Join(workspaceDir, "branch.txt")); got != testWorkspaceBranchContent {
		t.Fatalf("branch.txt mismatch: got %q", got)
	}
	if got := readFile(t, filepath.Join(workspaceDir, "base-branch.txt")); got != "base-branch\n" {
		t.Fatalf("base-branch.txt mismatch: got %q", got)
	}
}

func TestWorkspaceImportUsesCurrentHeadBranchNotHardcodedMaster(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)
	runGit(t, ctx, workingDir, "branch", "-m", "main")

	writeFile(t, filepath.Join(workingDir, "base.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "base.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-2-1"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workingDir, "main-only.txt"), "main-only\n")
	runGit(t, ctx, workingDir, "add", "main-only.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: update main")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	if err := m.Import(ctx, workspaceDir); err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if got := readFile(t, filepath.Join(workspaceDir, "main-only.txt")); got != "main-only\n" {
		t.Fatalf("main-only.txt mismatch: got %q", got)
	}
}

func TestEnsureWorkspace_UsesStateDirSessionsRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	stateDir := t.TempDir()
	sessionID := "tg-4-5"
	branchName := "norma/balda/tg-4-5"

	m := NewWorkspaceManagerWithSessionsDir(workingDir, stateDir, currentBranch(t, ctx, workingDir), "")
	got, err := m.EnsureWorkspace(ctx, sessionID, branchName, "")
	if err != nil {
		t.Fatalf("EnsureWorkspace() error = %v", err)
	}
	workspaceDir := got.Dir
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	want := filepath.Join(stateDir, "sessions", sessionID)
	if workspaceDir != want {
		t.Fatalf("workspaceDir = %q, want %q", workspaceDir, want)
	}
	if _, err := os.Stat(workspaceDir); err != nil {
		t.Fatalf("stat workspaceDir %q: %v", workspaceDir, err)
	}
}

func TestEnsureWorkspace_RemountsExistingBranchWithoutSyncWhenImportConflicts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	sessionID := "tg-9-9"
	branchName := "norma/balda/tg-9-9"
	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "conflict.txt"), testWorkspaceBranchContent)
	runGit(t, ctx, workspaceDir, "add", "conflict.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch conflict")

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "main\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: main conflict")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	got, err := m.EnsureWorkspace(ctx, sessionID, branchName, workspaceDir)
	if err != nil {
		t.Fatalf("EnsureWorkspace() error = %v", err)
	}
	if !got.SyncSkipped {
		t.Fatal("EnsureWorkspace() SyncSkipped = false, want true")
	}
	if got.Dir != workspaceDir {
		t.Fatalf("EnsureWorkspace() dir = %q, want %q", got.Dir, workspaceDir)
	}

	status := runGit(t, ctx, workspaceDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		t.Fatalf("expected clean workspace after fallback remount, got:\n%s", status)
	}
	if got := readFile(t, filepath.Join(workspaceDir, "conflict.txt")); got != testWorkspaceBranchContent {
		t.Fatalf("conflict.txt mismatch after fallback remount: got %q", got)
	}
}

func TestEnsureWorkspace_RemountsCleanBranchWithoutSyncWhenFreshMountConflicts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	sessionID := "tg-10-10"
	branchName := "norma/balda/tg-10-10"
	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")

	writeFile(t, filepath.Join(workspaceDir, "conflict.txt"), testWorkspaceBranchContent)
	runGit(t, ctx, workspaceDir, "add", "conflict.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch conflict")

	if err := runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir); err != nil {
		t.Fatalf("remove worktree: %v", err)
	}
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "main\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: main conflict")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	got, err := m.EnsureWorkspace(ctx, sessionID, branchName, workspaceDir)
	if err != nil {
		t.Fatalf("EnsureWorkspace() error = %v", err)
	}
	if !got.SyncSkipped {
		t.Fatal("EnsureWorkspace() SyncSkipped = false, want true")
	}

	status := runGit(t, ctx, workspaceDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		t.Fatalf("expected clean workspace after fallback remount, got:\n%s", status)
	}
	if got := readFile(t, filepath.Join(workspaceDir, "conflict.txt")); got != testWorkspaceBranchContent {
		t.Fatalf("conflict.txt mismatch after fallback remount: got %q", got)
	}
}

func TestWorkspaceImportAbortsRebaseOnConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "base\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-1-2"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "conflict.txt"), testWorkspaceBranchContent)
	runGit(t, ctx, workspaceDir, "add", "conflict.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch conflict")

	writeFile(t, filepath.Join(workingDir, "conflict.txt"), "base-branch\n")
	runGit(t, ctx, workingDir, "add", "conflict.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: base branch conflict")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), "")
	err := m.Import(ctx, workspaceDir)
	if err == nil {
		t.Fatal("Import() error = nil, want conflict error")
	}
	if !strings.Contains(err.Error(), "rebase workspace onto ") {
		t.Fatalf("error = %q, want rebase-on-base-ref context", err)
	}

	rebaseMergePath := strings.TrimSpace(runGit(t, ctx, workspaceDir, "rev-parse", "--git-path", "rebase-merge"))
	if _, statErr := os.Stat(rebaseMergePath); !os.IsNotExist(statErr) {
		t.Fatalf("expected no rebase-merge state after abort, stat err=%v", statErr)
	}

	rebaseApplyPath := strings.TrimSpace(runGit(t, ctx, workspaceDir, "rev-parse", "--git-path", "rebase-apply"))
	if _, statErr := os.Stat(rebaseApplyPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected no rebase-apply state after abort, stat err=%v", statErr)
	}

	status := runGit(t, ctx, workspaceDir, "status", "--porcelain")
	if strings.TrimSpace(status) != "" {
		t.Fatalf("expected clean workspace after abort, got:\n%s", status)
	}
	if got := readFile(t, filepath.Join(workspaceDir, "conflict.txt")); got != testWorkspaceBranchContent {
		t.Fatalf("conflict.txt mismatch after abort: got %q", got)
	}
}

func TestEnsureWorkspace_DifferentSessionsUseDistinctWorkspaceDirs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	stateDir := t.TempDir()
	m := NewWorkspaceManagerWithSessionsDir(workingDir, stateDir, currentBranch(t, ctx, workingDir), "")

	first, err := m.EnsureWorkspace(ctx, "tg-1-1", "norma/balda/tg-1-1", "")
	if err != nil {
		t.Fatalf("EnsureWorkspace(first) error = %v", err)
	}
	second, err := m.EnsureWorkspace(ctx, "tg-2-2", "norma/balda/tg-2-2", "")
	if err != nil {
		t.Fatalf("EnsureWorkspace(second) error = %v", err)
	}
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", first.Dir)
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", second.Dir)
	})

	if first.Dir == second.Dir {
		t.Fatalf("workspace dirs are equal: %q", first.Dir)
	}
	if strings.Contains(first.Dir, "tg-2-2") {
		t.Fatalf("first workspace dir = %q, want isolated session path", first.Dir)
	}
	if strings.Contains(second.Dir, "tg-1-1") {
		t.Fatalf("second workspace dir = %q, want isolated session path", second.Dir)
	}
}

func TestEnsureWorkspace_RejectsWorkspacePathCollisionAcrossBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	stateDir := t.TempDir()
	m := NewWorkspaceManagerWithSessionsDir(workingDir, stateDir, currentBranch(t, ctx, workingDir), "")

	first, err := m.EnsureWorkspace(ctx, "tg-1-1", "norma/balda/tg-1-1", "")
	if err != nil {
		t.Fatalf("EnsureWorkspace(first) error = %v", err)
	}
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", first.Dir)
	})

	_, err = m.EnsureWorkspace(ctx, "tg-2-2", "norma/balda/tg-2-2", first.Dir)
	if err == nil {
		t.Fatal("EnsureWorkspace(collision) error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "workspace collision") {
		t.Fatalf("EnsureWorkspace(collision) error = %q, want workspace collision marker", err)
	}
}

func TestWorkspaceExportSquashMergesIntoConfiguredBaseBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")
	runGit(t, ctx, workingDir, "branch", "-M", "main")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-1-3"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "main")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "feature.txt"), "feature\n")
	runGit(t, ctx, workspaceDir, "add", "feature.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch feature")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), "main", "")
	if err := m.Export(ctx, workspaceDir, branchName, "feat: export balda changes"); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if got := readFile(t, filepath.Join(workingDir, "feature.txt")); got != "feature\n" {
		t.Fatalf("feature.txt mismatch in base branch: got %q", got)
	}
	if got := strings.TrimSpace(runGit(t, ctx, workingDir, "rev-parse", "--abbrev-ref", "HEAD")); got != "main" {
		t.Fatalf("base repo branch = %q, want main", got)
	}
	if got := strings.TrimSpace(runGit(t, ctx, workingDir, "log", "-1", "--pretty=%s")); got != "feat: export balda changes" {
		t.Fatalf("last commit subject = %q, want feat: export balda changes", got)
	}
}

func TestWorkspaceExportFailsWhenBaseBranchMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")
	runGit(t, ctx, workingDir, "branch", "-M", "main")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	branchName := "norma/balda/tg-1-4"
	runGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "main")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeFile(t, filepath.Join(workspaceDir, "feature.txt"), "feature\n")
	runGit(t, ctx, workspaceDir, "add", "feature.txt")
	runGit(t, ctx, workspaceDir, "commit", "-m", "feat: branch feature")

	runGit(t, ctx, workingDir, "checkout", "-b", "develop")

	m := NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), "main", "")
	err := m.Export(ctx, workspaceDir, branchName, "feat: export balda changes")
	if err == nil {
		t.Fatal("Export() error = nil, want branch mismatch error")
	}
	if !strings.Contains(err.Error(), `export requires repository branch "main", current branch is "develop"`) {
		t.Fatalf("Export() error = %q, want branch mismatch context", err)
	}
}

func initGitRepo(t *testing.T, ctx context.Context, workingDir string) {
	t.Helper()
	runGit(t, ctx, workingDir, "init")
	runGit(t, ctx, workingDir, "config", "user.name", "Balda Test")
	runGit(t, ctx, workingDir, "config", "user.email", "balda-test@example.com")
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return string(data)
}

func runGit(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func runGitAllowError(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	_, err := cmd.CombinedOutput()
	return err
}

func currentBranch(t *testing.T, ctx context.Context, dir string) string {
	t.Helper()
	return strings.TrimSpace(runGit(t, ctx, dir, "rev-parse", "--abbrev-ref", "HEAD"))
}
