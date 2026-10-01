// Package crashpoint lets subprocess tests kill the process at named points in
// the engine, to check that recovery copes with a crash at exactly that moment.
//
// Arm it with environment variables before starting the process:
//
//	ANCHORDB_CRASH_AT=commit.after-sync   name of the point
//	ANCHORDB_CRASH_AFTER=3                crash on the 3rd hit (default 1)
//
// A crash is os.Exit(ExitCode): deferred functions do not run and nothing is
// fsynced, but bytes already passed to write(2) stay in the OS page cache. That
// matches `kill -9` for file contents. It does NOT simulate power loss.
package crashpoint

import (
	"os"
	"strconv"
	"sync/atomic"
)

// ExitCode is the process exit status used for a simulated crash.
const ExitCode = 86

// names lists every crash point the engine defines (docs/architecture.md §7.2).
var names = []string{
	"commit.before-write",
	"commit.partial-write",
	"commit.after-write",
	"commit.after-sync",
	"commit.after-apply",
	"rotate.after-seal",
	"rotate.after-create-tmp",
	"rotate.after-rename",
	"checkpoint.after-rotate",
	"checkpoint.partial-write",
	"checkpoint.after-file-sync",
	"checkpoint.after-file-rename",
	"checkpoint.after-current-tmp",
	"checkpoint.after-current-rename",
	"checkpoint.after-publish",
	"checkpoint.mid-reclaim",
	"checkpoint.after-reclaim",
	"init.after-segment",
	"recovery.after-truncate",
	"recovery.mid-cleanup",
}

// Names returns every defined crash point.
func Names() []string { return append([]string(nil), names...) }

var (
	target    string
	remaining atomic.Int64
	armed     atomic.Bool
)

func init() {
	target = os.Getenv("ANCHORDB_CRASH_AT")
	if target == "" {
		return
	}
	n, err := strconv.Atoi(os.Getenv("ANCHORDB_CRASH_AFTER"))
	if err != nil || n < 1 {
		n = 1
	}
	remaining.Store(int64(n))
	armed.Store(true)
}

// Hit crashes the process if name is the armed point and its hit count is
// reached. Disarmed cost: one atomic load.
func Hit(name string) {
	if Should(name) {
		Crash()
	}
}

// Should reports whether reaching name now must crash the process. Callers
// that need to do something first (for example write half a record) call
// Should, do the partial work, and then call Crash.
func Should(name string) bool {
	if !armed.Load() || name != target {
		return false
	}
	return remaining.Add(-1) == 0
}

// Crash terminates the process immediately with ExitCode.
func Crash() { os.Exit(ExitCode) }
