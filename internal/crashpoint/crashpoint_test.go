package crashpoint_test

import (
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/rosaiju/anchordb/internal/crashpoint"
)

// specPoints is the table of spec §7.2, copied by hand.
var specPoints = []string{
	"commit.before-write", "commit.partial-write", "commit.after-write", "commit.after-sync",
	"commit.after-apply", "rotate.after-seal", "rotate.after-create-tmp", "rotate.after-rename",
	"checkpoint.after-rotate", "checkpoint.partial-write", "checkpoint.after-file-sync",
	"checkpoint.after-file-rename", "checkpoint.after-current-tmp", "checkpoint.after-current-rename",
	"checkpoint.after-publish", "checkpoint.mid-reclaim", "checkpoint.after-reclaim",
	"init.after-segment", "recovery.after-truncate", "recovery.mid-cleanup",
}

func TestNamesMatchSpec(t *testing.T) {
	got := crashpoint.Names()
	a := append([]string{}, got...)
	b := append([]string{}, specPoints...)
	sort.Strings(a)
	sort.Strings(b)
	if len(a) != len(b) {
		t.Fatalf("Names()=%v, spec=%v", got, specPoints)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("Names()=%v, spec=%v", got, specPoints)
		}
	}
	if crashpoint.ExitCode != 86 {
		t.Fatal("ExitCode")
	}
}

func TestDisarmed(t *testing.T) {
	if os.Getenv("ANCHORDB_CRASH_AT") != "" {
		t.Skip("armed environment")
	}
	for _, n := range specPoints {
		crashpoint.Hit(n) // must not exit
	}
}

// The n-th hit (ANCHORDB_CRASH_AFTER) exits with ExitCode; earlier hits don't.
func TestArmedInSubprocess(t *testing.T) {
	if os.Getenv("CP_CHILD") == "1" {
		crashpoint.Hit("other.point")
		crashpoint.Hit("commit.after-write")
		crashpoint.Hit("commit.after-write")
		os.Stdout.WriteString("reached-third\n")
		crashpoint.Hit("commit.after-write")
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestArmedInSubprocess$")
	cmd.Env = append(os.Environ(), "CP_CHILD=1", "ANCHORDB_CRASH_AT=commit.after-write", "ANCHORDB_CRASH_AFTER=3")
	out, err := cmd.Output()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 86 {
		t.Fatalf("want exit 86, got %v", err)
	}
	if string(out) != "reached-third\n" {
		t.Fatalf("output %q", out)
	}
}
