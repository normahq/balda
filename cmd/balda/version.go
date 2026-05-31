package main

import (
	"fmt"
	buildinfo "runtime/debug"
	"strings"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func buildVersionString() string {
	resolvedVersion := strings.TrimSpace(version)
	if resolvedVersion == "" || resolvedVersion == "dev" {
		if info, ok := buildinfo.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			resolvedVersion = info.Main.Version
		}
	}
	if resolvedVersion == "" {
		resolvedVersion = "dev"
	}

	resolvedCommit := strings.TrimSpace(commit)
	if resolvedCommit == "" {
		resolvedCommit = "unknown"
	}
	resolvedDate := strings.TrimSpace(date)
	if resolvedDate == "" {
		resolvedDate = "unknown"
	}

	return fmt.Sprintf("balda %s (commit %s, built %s)", resolvedVersion, resolvedCommit, resolvedDate)
}
