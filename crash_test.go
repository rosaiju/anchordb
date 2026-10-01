package anchordb_test

// Subprocess crash tests (spec §7.2). The parent re-executes this test binary
// with ANCHORDB_CRASH_AT / ANCHORDB_CRASH_AFTER set; the child runs a
// deterministic workload and reports every transaction it starts and every
// commit that was acknowledged on its stdout pipe, *before* and *after* the
// call, so the parent knows exactly what was acknowledged even though the
// child dies with os.Exit(86). The parent then reopens the directory and
// checks the recovered state against the reference model.
//
// A process exit is equivalent to kill -9 with respect to file contents; it
// does NOT simulate power loss (see docs/correctness.md).

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rosaiju/anchordb"
	"github.com/rosaiju/anchordb/internal/crashpoint"
	"github.com/rosaiju/anchordb/internal/refmodel"
)

// workloadOps returns the ops of transaction i (1-based). Some transactions
// delete keys, and keys are rewritten, so state depends on order.
func workloadOps(i int) []refmodel.Op {
	ops := []refmodel.Op{
		{Key: []byte(fmt.Sprintf("key-%d", i%5)), Value: []byte(fmt.Sprintf("val-%d-%s", i, strings.Repeat("x", 10+i%13)))},
		{Key: []byte("seq"), Value: []byte(strconv.Itoa(i))},
	}
	if i%3 == 0 {
		ops = append(ops, refmodel.Op{Key: []byte(fmt.Sprintf("key-%d", (i+1)%5)), Delete: true})
	}
	return ops
}

// workloadModel returns the model after transactions 1..n.
func workloadModel(n int) *refmodel.Model {
	md := refmodel.New()
	for i := 1; i <= n; i++ {
		tx := md.Begin()
		for _, op := range workloadOps(i) {
			if op.Delete {
				tx.Delete(op.Key)
			} else {
				tx.Put(op.Key, op.Value)
			}
		}
		tx.Commit()
	}
	return md
}

// frameLenOf is the encoded WAL frame size of transaction i with txid.
func frameLenOf(i int, txid uint64) int {
	tx := refmodel.New().Begin()
	for _, op := range workloadOps(i) {
		if op.Delete {
			tx.Delete(op.Key)
		} else {
			tx.Put(op.Key, op.Value)
		}
	}
	return len(refmodel.Frame(1, refmodel.CommitPayload(txid, tx.Ops())))
}

type childCfg struct {
	Mode      string // "workload", "open"
	N         int    // transactions
	SyncNone  bool
	SegSize   int64
	CkptEvery int // checkpoint after every CkptEvery-th tx (0 = never)
}

func (c childCfg) env() []string {
	return []string{
		"ANCHORDB_TEST_CHILD=1",
		"ANCHORDB_TEST_MODE=" + c.Mode,
		"ANCHORDB_TEST_N=" + strconv.Itoa(c.N),
		"ANCHORDB_TEST_SYNCNONE=" + strconv.FormatBool(c.SyncNone),
		"ANCHORDB_TEST_SEGSIZE=" + strconv.FormatInt(c.SegSize, 10),
		"ANCHORDB_TEST_CKPT=" + strconv.Itoa(c.CkptEvery),
	}
}

func say(format string, a ...any) {
	// os.Stdout.Write is an unbuffered system call: once it returns, the
	// parent can read the line even if we die immediately afterwards.
	os.Stdout.WriteString("@@ " + fmt.Sprintf(format, a...) + "\n")
}

// TestCrashChild is the child side; it does nothing in a normal run.
func TestCrashChild(t *testing.T) {
	if os.Getenv("ANCHORDB_TEST_CHILD") != "1" {
		t.Skip("child process helper")
	}
	dir := os.Getenv("ANCHORDB_TEST_DIR")
	n, _ := strconv.Atoi(os.Getenv("ANCHORDB_TEST_N"))
	seg, _ := strconv.ParseInt(os.Getenv("ANCHORDB_TEST_SEGSIZE"), 10, 64)
	ck, _ := strconv.Atoi(os.Getenv("ANCHORDB_TEST_CKPT"))
	opts := &anchordb.Options{SegmentSize: seg}
	if os.Getenv("ANCHORDB_TEST_SYNCNONE") == "true" {
		opts.SyncMode = anchordb.SyncNone
	}
	say("OPEN-BEGIN")
	db, err := anchordb.Open(dir, opts)
	if err != nil {
		say("ERROR open %v", err)
		os.Exit(3)
	}
	say("OPEN-OK")
	if os.Getenv("ANCHORDB_TEST_MODE") == "open" {
		db.Close()
		os.Exit(0)
	}
	s, _ := db.Stats()
	start := int(s.LastTxID)
	for i := start + 1; i <= start+n; i++ {
		say("BEGIN %d", i)
		err := db.Update(func(tx *anchordb.Tx) error {
			for _, op := range workloadOps(i) {
				var e error
				if op.Delete {
					e = tx.Delete(op.Key)
				} else {
					e = tx.Put(op.Key, op.Value)
				}
				if e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			say("ERROR commit %d %v", i, err)
			os.Exit(3)
		}
		say("ACK %d", i)
		if ck > 0 && i%ck == 0 {
			say("CKPT-BEGIN %d", i)
			if err := db.Checkpoint(); err != nil {
				say("ERROR checkpoint %v", err)
				os.Exit(3)
			}
			say("CKPT-OK %d", i)
		}
	}
	db.Close()
	say("DONE")
	os.Exit(0)
}

type childResult struct {
	exitCode  int
	lines     []string
	lastBegin int
	lastAck   int
	ckptBegun int // T of the last checkpoint started (-1 none)
	ckptOK    int // T of the last checkpoint completed (0 none)
	inCkpt    bool
}

func runChild(t *testing.T, dir string, cfg childCfg, crashAt string, after int) childResult {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), cfg.env()...)
	cmd.Env = append(cmd.Env, "ANCHORDB_TEST_DIR="+dir)
	if crashAt != "" {
		cmd.Env = append(cmd.Env, "ANCHORDB_CRASH_AT="+crashAt, "ANCHORDB_CRASH_AFTER="+strconv.Itoa(after))
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	res := childResult{ckptBegun: -1}
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "@@ ") {
			continue
		}
		line = line[3:]
		res.lines = append(res.lines, line)
		var v int
		switch {
		case strings.HasPrefix(line, "BEGIN "):
			fmt.Sscanf(line, "BEGIN %d", &v)
			res.lastBegin = v
		case strings.HasPrefix(line, "ACK "):
			fmt.Sscanf(line, "ACK %d", &v)
			res.lastAck = v
		case strings.HasPrefix(line, "CKPT-BEGIN "):
			fmt.Sscanf(line, "CKPT-BEGIN %d", &v)
			res.ckptBegun, res.inCkpt = v, true
		case strings.HasPrefix(line, "CKPT-OK "):
			fmt.Sscanf(line, "CKPT-OK %d", &v)
			res.ckptOK, res.inCkpt = v, false
		case strings.HasPrefix(line, "ERROR"):
			t.Errorf("child reported %s", line)
		}
	}
	err = cmd.Wait()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		res.exitCode = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if crashAt != "" && res.exitCode != crashpoint.ExitCode {
		t.Fatalf("child exit code %d, want %d (crash point %s ×%d never reached?)\nlines: %v\nstderr: %s",
			res.exitCode, crashpoint.ExitCode, crashAt, after, res.lines, stderr.String())
	}
	if crashAt == "" && res.exitCode != 0 {
		t.Fatalf("child failed: %d %s", res.exitCode, stderr.String())
	}
	return res
}

type expect struct {
	inflight   string // "absent", "present", "none" (no tx in flight)
	current    string // "old", "new", "" (don't care)
	removedMin int    // minimum RecoveryInfo.RemovedFiles
	truncHalf  bool   // TruncatedBytes == ⌊frame/2⌋
	finalEmpty bool   // last segment is header-only after recovery
	noTmpSeg   bool
	skipPost   bool // skip the post-crash write/checkpoint (when the dir is reused)
}

// verifyAfterCrash reopens dir and checks the recovered state.
func verifyAfterCrash(t *testing.T, dir string, res childResult, ex expect) {
	t.Helper()
	db := openT(t, dir, nil)
	s := statsT(t, db)
	got := dump(t, db)
	p := int(s.LastTxID)
	if p < res.lastAck {
		t.Fatalf("acknowledged commit lost: LastTxID=%d, last ack=%d", p, res.lastAck)
	}
	if p > res.lastBegin {
		t.Fatalf("recovered %d txs but only %d were attempted", p, res.lastBegin)
	}
	if !workloadModel(p).Equal(got) {
		t.Fatalf("recovered state is not the model after %d txs:\n got %s\nwant %s", p, kvString(got), kvString(workloadModel(p).All()))
	}
	inflight := res.lastBegin > res.lastAck && !res.inCkpt
	switch ex.inflight {
	case "absent":
		if !inflight {
			t.Fatalf("expected an in-flight tx (begin=%d ack=%d)", res.lastBegin, res.lastAck)
		}
		if p != res.lastAck {
			t.Fatalf("in-flight tx %d should be absent, LastTxID=%d", res.lastBegin, p)
		}
	case "present":
		if !inflight {
			t.Fatalf("expected an in-flight tx (begin=%d ack=%d)", res.lastBegin, res.lastAck)
		}
		if p != res.lastBegin {
			t.Fatalf("in-flight tx %d should be present, LastTxID=%d", res.lastBegin, p)
		}
	case "none":
		if p != res.lastAck {
			t.Fatalf("no tx in flight, but LastTxID=%d != last ack %d", p, res.lastAck)
		}
	}
	switch ex.current {
	case "old":
		if s.CheckpointTxID != uint64(res.ckptOK) {
			t.Fatalf("expected old CURRENT (checkpoint %d), got CheckpointTxID=%d", res.ckptOK, s.CheckpointTxID)
		}
	case "new":
		if s.CheckpointTxID != uint64(res.ckptBegun) {
			t.Fatalf("expected new CURRENT (checkpoint %d), got CheckpointTxID=%d", res.ckptBegun, s.CheckpointTxID)
		}
	}
	if s.Recovery.RemovedFiles < ex.removedMin {
		t.Fatalf("RemovedFiles=%d, want >= %d", s.Recovery.RemovedFiles, ex.removedMin)
	}
	if ex.truncHalf {
		want := int64(frameLenOf(res.lastBegin, uint64(res.lastBegin)) / 2)
		if s.Recovery.TruncatedBytes != want {
			t.Fatalf("TruncatedBytes=%d, want ⌊frame/2⌋=%d", s.Recovery.TruncatedBytes, want)
		}
	}
	if ex.finalEmpty {
		last := lastSegment(t, dir)
		if sz := fileSize(dir, filepath.Base(last)); sz != 32 {
			t.Fatalf("final segment %s is %d bytes, want header-only", filepath.Base(last), sz)
		}
	}
	// No garbage may survive recovery.
	for _, n := range listDir(t, dir) {
		if strings.HasSuffix(n, ".tmp") {
			t.Fatalf("temp file %s survived Open", n)
		}
	}
	if cks := checkpoints(t, dir); len(cks) > 1 || (len(cks) == 1 && cks[0] != refmodel.CheckpointName(s.CheckpointTxID)) {
		t.Fatalf("unreferenced checkpoint files survived Open: %v (CheckpointTxID=%d)", cks, s.CheckpointTxID)
	}
	for _, n := range segments(t, dir) {
		var seq uint64
		fmt.Sscanf(n, "wal-%d.log", &seq)
		if seq < s.WALStartSeq {
			t.Fatalf("segment %s below wal_start %d survived Open", n, s.WALStartSeq)
		}
	}
	if ex.skipPost {
		closeT(t, db)
		return
	}
	// The recovered DB is healthy: write, checkpoint, reopen, compare.
	md := workloadModel(p)
	putT(t, db, "post-crash", "ok")
	md.Put([]byte("post-crash"), []byte("ok"))
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after recovery: %v", err)
	}
	db = reopenT(t, db, dir, nil)
	assertModel(t, db, md)
	closeT(t, db)
}

type crashCase struct {
	point string
	after int
	cfg   childCfg
	ex    expect
	name  string
}

func crashMatrix() []crashCase {
	commitCfg := childCfg{Mode: "workload", N: 12}
	commitNone := childCfg{Mode: "workload", N: 12, SyncNone: true}
	rotCfg := childCfg{Mode: "workload", N: 12, SegSize: 150}
	ckCfg := childCfg{Mode: "workload", N: 12, CkptEvery: 4}
	return []crashCase{
		{"commit.before-write", 5, commitCfg, expect{inflight: "absent"}, ""},
		{"commit.partial-write", 5, commitCfg, expect{inflight: "absent", truncHalf: true}, ""},
		{"commit.after-write", 5, commitCfg, expect{inflight: "present"}, ""},
		{"commit.after-sync", 5, commitCfg, expect{inflight: "present"}, ""},
		{"commit.after-apply", 5, commitCfg, expect{inflight: "present"}, ""},
		{"commit.before-write", 5, commitNone, expect{inflight: "absent"}, "syncnone"},
		{"commit.partial-write", 5, commitNone, expect{inflight: "absent", truncHalf: true}, "syncnone"},
		{"commit.after-write", 5, commitNone, expect{inflight: "present"}, "syncnone"},
		{"commit.after-sync", 5, commitNone, expect{inflight: "present"}, "syncnone"},
		{"commit.after-apply", 5, commitNone, expect{inflight: "present"}, "syncnone"},
		{"commit.partial-write", 1, commitCfg, expect{inflight: "absent", truncHalf: true}, "first-tx"},
		{"rotate.after-seal", 2, rotCfg, expect{inflight: "absent"}, ""},
		{"rotate.after-create-tmp", 2, rotCfg, expect{inflight: "absent", removedMin: 1}, ""},
		{"rotate.after-rename", 2, rotCfg, expect{inflight: "absent", finalEmpty: true}, ""},
		{"rotate.after-rename", 1, ckCfg, expect{inflight: "none", current: "old", finalEmpty: true}, "via-checkpoint"},
		{"checkpoint.after-rotate", 2, ckCfg, expect{inflight: "none", current: "old"}, ""},
		{"checkpoint.partial-write", 2, ckCfg, expect{inflight: "none", current: "old", removedMin: 1}, ""},
		{"checkpoint.after-file-sync", 2, ckCfg, expect{inflight: "none", current: "old", removedMin: 1}, ""},
		{"checkpoint.after-file-rename", 2, ckCfg, expect{inflight: "none", current: "old", removedMin: 1}, ""},
		{"checkpoint.after-current-tmp", 2, ckCfg, expect{inflight: "none", current: "old", removedMin: 2}, ""},
		{"checkpoint.after-current-rename", 2, ckCfg, expect{inflight: "none", current: "new", removedMin: 1}, ""},
		{"checkpoint.after-publish", 2, ckCfg, expect{inflight: "none", current: "new", removedMin: 1}, ""},
		{"checkpoint.mid-reclaim", 2, ckCfg, expect{inflight: "none", current: "new", removedMin: 1}, ""},
		{"checkpoint.after-reclaim", 2, ckCfg, expect{inflight: "none", current: "new"}, ""},
		{"checkpoint.after-current-rename", 1, ckCfg, expect{inflight: "none", current: "new", removedMin: 1}, "first-checkpoint"},
		{"checkpoint.mid-reclaim", 1, ckCfg, expect{inflight: "none", current: "new", removedMin: 0}, "first-checkpoint"},
	}
}

func TestCrashPoints(t *testing.T) {
	for _, c := range crashMatrix() {
		c := c
		name := c.point
		if c.name != "" {
			name += "/" + c.name
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			res := runChild(t, dir, c.cfg, c.point, c.after)
			if c.point == "checkpoint.after-reclaim" {
				c.ex.removedMin = 0
			}
			verifyAfterCrash(t, dir, res, c.ex)
			if c.point == "checkpoint.after-reclaim" {
				// Nothing was left to remove (§7.2 table).
				db := openT(t, dir, nil)
				defer db.Close()
			}
		})
	}
}

// checkpoint.after-reclaim: "nothing left to remove" — verified on a fresh
// crash without the post-crash checkpoint interfering.
func TestCrashAfterReclaimLeavesNothing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	runChild(t, dir, childCfg{Mode: "workload", N: 12, CkptEvery: 4}, "checkpoint.after-reclaim", 2)
	db := openT(t, dir, nil)
	defer db.Close()
	if s := statsT(t, db); s.Recovery.RemovedFiles != 0 || s.CheckpointTxID != 8 {
		t.Fatalf("stats %+v", s)
	}
}

// checkpoint.partial-write on an empty DB (§7.2: crash just before the trailer).
func TestCrashCheckpointPartialWriteEmptyDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openT(t, dir, nil)
	putT(t, db, "a", "1")
	db.Delete([]byte("a"))
	closeT(t, db)
	// The child's workload with N=0 and CkptEvery would not checkpoint, so
	// use a dedicated config: one tx that deletes nothing would still add
	// keys; instead checkpoint via the "ckempty" mode below.
	res := runChildCkptOnly(t, dir, "checkpoint.partial-write")
	_ = res
	db = openT(t, dir, nil)
	defer db.Close()
	s := statsT(t, db)
	if s.Keys != 0 || s.LastTxID != 2 || s.CheckpointTxID != 0 || s.Recovery.RemovedFiles < 1 {
		t.Fatalf("stats %+v", s)
	}
}

// runChildCkptOnly opens dir in a child and calls Checkpoint once.
func runChildCkptOnly(t *testing.T, dir, point string) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChildCheckpoint$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ANCHORDB_TEST_CHILD=1", "ANCHORDB_TEST_DIR="+dir,
		"ANCHORDB_CRASH_AT="+point, "ANCHORDB_CRASH_AFTER=1")
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != crashpoint.ExitCode {
		t.Fatalf("child: %v (want exit %d)", err, crashpoint.ExitCode)
	}
	return ee.ExitCode()
}

func TestCrashChildCheckpoint(t *testing.T) {
	if os.Getenv("ANCHORDB_TEST_CHILD") != "1" {
		t.Skip("child process helper")
	}
	db, err := anchordb.Open(os.Getenv("ANCHORDB_TEST_DIR"), nil)
	if err != nil {
		os.Exit(3)
	}
	db.Checkpoint()
	db.Close()
	os.Exit(0)
}

func TestCrashInitAfterSegment(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "db")
	runChild(t, dir, childCfg{Mode: "open"}, "init.after-segment", 1)
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err == nil {
		t.Fatal("CURRENT exists although the crash was before publishing it")
	}
	db := openT(t, dir, nil)
	if s := statsT(t, db); s.Keys != 0 || s.LastTxID != 0 {
		t.Fatalf("stats %+v", s)
	}
	putT(t, db, "a", "1")
	closeT(t, db)
	if !bytes.Equal(readFile(t, filepath.Join(dir, "CURRENT")), refmodel.CurrentFile("", 0, 1)) {
		t.Fatal("CURRENT after init recovery differs from spec")
	}
}

func TestCrashRecoveryAfterTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	runChild(t, dir, childCfg{Mode: "workload", N: 6}, "", 0)
	seg := lastSegment(t, dir)
	b := readFile(t, seg)
	writeFile(t, seg, b[:len(b)-7])
	runChild(t, dir, childCfg{Mode: "open"}, "recovery.after-truncate", 1)
	db := openT(t, dir, nil)
	defer db.Close()
	s := statsT(t, db)
	if s.LastTxID != 5 || s.Recovery.TruncatedBytes != 0 {
		t.Fatalf("stats %+v", s)
	}
	assertModel(t, db, workloadModel(5))
}

func TestCrashRecoveryMidCleanup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	runChild(t, dir, childCfg{Mode: "workload", N: 6}, "", 0)
	garbage := []string{"CURRENT.tmp", refmodel.CheckpointName(3) + ".tmp", refmodel.SegmentName(9) + ".tmp", refmodel.CheckpointName(4)}
	for _, g := range garbage {
		writeFile(t, filepath.Join(dir, g), []byte("junk"))
	}
	runChild(t, dir, childCfg{Mode: "open"}, "recovery.mid-cleanup", 1)
	left := 0
	for _, g := range garbage {
		if _, err := os.Stat(filepath.Join(dir, g)); err == nil {
			left++
		}
	}
	if left != len(garbage)-1 {
		t.Fatalf("%d garbage files left after crash at mid-cleanup, want %d", left, len(garbage)-1)
	}
	db := openT(t, dir, nil)
	defer db.Close()
	s := statsT(t, db)
	if s.Recovery.RemovedFiles != left {
		t.Fatalf("RemovedFiles=%d want %d", s.Recovery.RemovedFiles, left)
	}
	assertModel(t, db, workloadModel(6))
	for _, g := range garbage {
		if _, err := os.Stat(filepath.Join(dir, g)); err == nil {
			t.Fatalf("garbage %s survived", g)
		}
	}
}

// Every crash point the engine defines is exercised by some test above.
func TestCrashPointsAllCovered(t *testing.T) {
	covered := map[string]bool{
		"init.after-segment":       true, // TestCrashInitAfterSegment
		"recovery.after-truncate":  true, // TestCrashRecoveryAfterTruncate
		"recovery.mid-cleanup":     true, // TestCrashRecoveryMidCleanup
		"checkpoint.partial-write": true,
	}
	for _, c := range crashMatrix() {
		covered[c.point] = true
	}
	for _, n := range crashpoint.Names() {
		if !covered[n] {
			t.Errorf("crash point %s has no subprocess test", n)
		}
	}
}

// Randomized crash test: crash at a random commit count in a random point,
// repeated with seeds; every acked commit must survive.
func TestCrashRandomized(t *testing.T) {
	points := []string{"commit.before-write", "commit.partial-write", "commit.after-write", "commit.after-sync", "commit.after-apply"}
	runs := iters(10, 3)
	for r := 0; r < runs; r++ {
		r := r
		t.Run(strconv.Itoa(r), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := childCfg{Mode: "workload", N: 30, SegSize: int64(100 + 50*r), CkptEvery: 7 + r%3, SyncNone: r%2 == 1}
			point := points[r%len(points)]
			after := 3 + (r*7)%20
			res := runChild(t, dir, cfg, point, after)
			verifyAfterCrash(t, dir, res, expect{skipPost: true})
			// Run again from the recovered state and crash again.
			res2 := runChild(t, dir, cfg, points[(r+2)%len(points)], 4)
			verifyAfterCrash(t, dir, res2, expect{})
		})
	}
}

// Directory lock held by another process (§8 Locking).
func TestLockAcrossProcesses(t *testing.T) {
	if os.Getenv("ANCHORDB_TEST_LOCKCHILD") == "1" {
		db, err := anchordb.Open(os.Getenv("ANCHORDB_TEST_DIR"), nil)
		if err != nil {
			os.Stdout.WriteString("@@ ERR\n")
			os.Exit(3)
		}
		os.Stdout.WriteString("@@ HELD\n")
		bufio.NewReader(os.Stdin).ReadString('\n')
		db.Close()
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockAcrossProcesses$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ANCHORDB_TEST_LOCKCHILD=1", "ANCHORDB_TEST_DIR="+dir)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	held := false
	for sc.Scan() {
		if sc.Text() == "@@ HELD" {
			held = true
			break
		}
	}
	if !held {
		t.Fatal("child never acquired the lock")
	}
	_, err := anchordb.Open(dir, nil)
	wantErr(t, err, anchordb.ErrLocked, "Open while another process holds the DB")
	// Kill the child (crash): the OS releases the lock.
	cmd.Process.Kill()
	stdin.Close()
	cmd.Wait()
	db := openT(t, dir, nil)
	closeT(t, db)
}
