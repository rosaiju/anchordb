// Command anchordb is a small command-line interface to an AnchorDB database.
//
//	anchordb [-db DIR] [-sync always|none] [-segment-size N] COMMAND [ARGS]
//
// Run `anchordb help` for the command list. Each invocation opens the
// database (running crash recovery), performs one command, and closes it.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/rosaiju/anchordb"
)

const usage = `usage: anchordb [-db DIR] [-sync always|none] [-segment-size BYTES] COMMAND [ARGS]

commands:
  put KEY VALUE                 store KEY=VALUE (one transaction)
  get KEY                       print the value (exit status 3 if missing)
  delete KEY                    delete KEY
  scan [-start K] [-end K] [-prefix P] [-limit N]
                                print "key<TAB>value" lines for keys in [start, end)
  tx [-rollback] OP...          run several ops in ONE transaction; OP is
                                  put KEY VALUE | del KEY
                                -rollback discards the transaction instead of committing
  transfer FROM TO AMOUNT       move AMOUNT between integer balances stored at
                                acct:FROM and acct:TO, atomically; fails (and
                                rolls back) on insufficient funds
  load -n N [-batch B] [-prefix P] [-value-size S]
                                insert N generated keys, B per transaction
  checkpoint                    write a checkpoint and reclaim old WAL segments
  stats                         print engine statistics (includes what recovery did)
  verify                        open (recover), scan everything, check key count

exit status: 0 ok, 1 error, 2 usage, 3 key not found,
             86 simulated crash (ANCHORDB_CRASH_AT, for tests and demos)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("anchordb", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	dir := fs.String("db", "anchordb-data", "database directory")
	syncMode := fs.String("sync", "always", "commit durability: always (fsync) or none")
	segSize := fs.Int64("segment-size", 0, "WAL segment rotation size in bytes (0 = default)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 || fs.Arg(0) == "help" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	opts := &anchordb.Options{SegmentSize: *segSize}
	switch *syncMode {
	case "always":
		opts.SyncMode = anchordb.SyncAlways
	case "none":
		opts.SyncMode = anchordb.SyncNone
	default:
		fmt.Fprintf(stderr, "anchordb: -sync must be always or none\n")
		return 2
	}

	cmd, cargs := fs.Arg(0), fs.Args()[1:]
	c, ok := commands[cmd]
	if !ok {
		fmt.Fprintf(stderr, "anchordb: unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	db, err := anchordb.Open(*dir, opts)
	if err != nil {
		fmt.Fprintf(stderr, "anchordb: open %s: %v\n", *dir, err)
		return 1
	}
	code := c(db, cargs, stdout, stderr)
	if err := db.Close(); err != nil && code == 0 {
		fmt.Fprintf(stderr, "anchordb: close: %v\n", err)
		return 1
	}
	return code
}

type command func(db *anchordb.DB, args []string, stdout, stderr io.Writer) int

var commands = map[string]command{
	"put":        cmdPut,
	"get":        cmdGet,
	"delete":     cmdDelete,
	"scan":       cmdScan,
	"tx":         cmdTx,
	"transfer":   cmdTransfer,
	"load":       cmdLoad,
	"checkpoint": cmdCheckpoint,
	"stats":      cmdStats,
	"verify":     cmdVerify,
}

func fail(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "anchordb: %v\n", err)
	return 1
}

func usageErr(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "anchordb: %s\n", msg)
	return 2
}

func cmdPut(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		return usageErr(stderr, "usage: put KEY VALUE")
	}
	if err := db.Put([]byte(args[0]), []byte(args[1])); err != nil {
		return fail(stderr, err)
	}
	return 0
}

func cmdGet(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		return usageErr(stderr, "usage: get KEY")
	}
	v, err := db.Get([]byte(args[0]))
	if errors.Is(err, anchordb.ErrNotFound) {
		fmt.Fprintf(stderr, "anchordb: %q not found\n", args[0])
		return 3
	}
	if err != nil {
		return fail(stderr, err)
	}
	fmt.Fprintf(stdout, "%s\n", v)
	return 0
}

func cmdDelete(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		return usageErr(stderr, "usage: delete KEY")
	}
	if err := db.Delete([]byte(args[0])); err != nil {
		return fail(stderr, err)
	}
	return 0
}

func cmdScan(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	start := fs.String("start", "", "first key (inclusive)")
	end := fs.String("end", "", "last key (exclusive)")
	prefix := fs.String("prefix", "", "only keys with this prefix")
	limit := fs.Int("limit", 0, "stop after N keys (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	lo, hi := []byte(*start), []byte(*end)
	if *prefix != "" {
		if lo == nil || bytes.Compare(lo, []byte(*prefix)) < 0 {
			lo = []byte(*prefix)
		}
		if pe := prefixEnd([]byte(*prefix)); pe != nil && (len(hi) == 0 || bytes.Compare(pe, hi) < 0) {
			hi = pe
		}
	}
	n := 0
	err := db.Scan(lo, hi, func(k, v []byte) bool {
		fmt.Fprintf(stdout, "%s\t%s\n", k, v)
		n++
		return *limit == 0 || n < *limit
	})
	if err != nil {
		return fail(stderr, err)
	}
	return 0
}

// prefixEnd returns the smallest key greater than every key with prefix p,
// or nil if there is none (p is all 0xff).
func prefixEnd(p []byte) []byte {
	e := append([]byte(nil), p...)
	for i := len(e) - 1; i >= 0; i-- {
		if e[i] < 0xff {
			e[i]++
			return e[:i+1]
		}
	}
	return nil
}

func cmdTx(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	rollback := false
	if len(args) > 0 && args[0] == "-rollback" {
		rollback, args = true, args[1:]
	}
	if len(args) == 0 {
		return usageErr(stderr, "usage: tx [-rollback] put KEY VALUE | del KEY ...")
	}
	tx, err := db.Begin(true)
	if err != nil {
		return fail(stderr, err)
	}
	defer tx.Rollback()
	for len(args) > 0 {
		switch {
		case args[0] == "put" && len(args) >= 3:
			err, args = tx.Put([]byte(args[1]), []byte(args[2])), args[3:]
		case args[0] == "del" && len(args) >= 2:
			err, args = tx.Delete([]byte(args[1])), args[2:]
		default:
			return usageErr(stderr, fmt.Sprintf("bad op near %q: expected put KEY VALUE or del KEY", args[0]))
		}
		if err != nil {
			return fail(stderr, err)
		}
	}
	if rollback {
		if err := tx.Rollback(); err != nil {
			return fail(stderr, err)
		}
		fmt.Fprintln(stdout, "rolled back")
		return 0
	}
	if err := tx.Commit(); err != nil {
		return fail(stderr, err)
	}
	fmt.Fprintln(stdout, "committed")
	return 0
}

func cmdTransfer(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	if len(args) != 3 {
		return usageErr(stderr, "usage: transfer FROM TO AMOUNT")
	}
	amount, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || amount <= 0 {
		return usageErr(stderr, "AMOUNT must be a positive integer")
	}
	from, to := []byte("acct:"+args[0]), []byte("acct:"+args[1])
	err = db.Update(func(tx *anchordb.Tx) error {
		fb, err := balance(tx, from)
		if err != nil {
			return err
		}
		tb, err := balance(tx, to)
		if err != nil {
			return err
		}
		if fb < amount {
			return fmt.Errorf("insufficient funds in %s: %d < %d (rolled back)", from, fb, amount)
		}
		if err := tx.Put(from, []byte(strconv.FormatInt(fb-amount, 10))); err != nil {
			return err
		}
		return tx.Put(to, []byte(strconv.FormatInt(tb+amount, 10)))
	})
	if err != nil {
		return fail(stderr, err)
	}
	fmt.Fprintln(stdout, "committed")
	return 0
}

func balance(tx *anchordb.Tx, key []byte) (int64, error) {
	v, err := tx.Get(key)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return strconv.ParseInt(string(v), 10, 64)
}

func cmdLoad(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	fs.SetOutput(stderr)
	n := fs.Int("n", 1000, "number of keys")
	batch := fs.Int("batch", 100, "keys per transaction")
	prefix := fs.String("prefix", "key:", "key prefix")
	valueSize := fs.Int("value-size", 16, "value size in bytes")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *n < 0 || *batch < 1 || *valueSize < 0 {
		return usageErr(stderr, "-n must be >= 0, -batch >= 1, -value-size >= 0")
	}
	for i := 0; i < *n; i += *batch {
		err := db.Update(func(tx *anchordb.Tx) error {
			for j := i; j < i+*batch && j < *n; j++ {
				v := strings.Repeat("v", *valueSize)
				if len(v) >= 8 {
					v = fmt.Sprintf("%08d", j) + v[8:]
				}
				if err := tx.Put([]byte(fmt.Sprintf("%s%08d", *prefix, j)), []byte(v)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fail(stderr, err)
		}
	}
	fmt.Fprintf(stdout, "loaded %d keys\n", *n)
	return 0
}

func cmdCheckpoint(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	if err := db.Checkpoint(); err != nil {
		return fail(stderr, err)
	}
	fmt.Fprintln(stdout, "checkpoint complete")
	return cmdStats(db, nil, stdout, stderr)
}

func cmdStats(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	st, err := db.Stats()
	if err != nil {
		return fail(stderr, err)
	}
	fmt.Fprintf(stdout, "keys             %d\n", st.Keys)
	fmt.Fprintf(stdout, "last_txid        %d\n", st.LastTxID)
	fmt.Fprintf(stdout, "checkpoint_txid  %d\n", st.CheckpointTxID)
	fmt.Fprintf(stdout, "wal_segments     %d..%d\n", st.WALStartSeq, st.ActiveSeq)
	fmt.Fprintf(stdout, "wal_bytes        %d\n", st.WALBytes)
	fmt.Fprintf(stdout, "recovery         replayed=%d truncated_bytes=%d removed_files=%d\n",
		st.Recovery.ReplayedTxs, st.Recovery.TruncatedBytes, st.Recovery.RemovedFiles)
	return 0
}

func cmdVerify(db *anchordb.DB, args []string, stdout, stderr io.Writer) int {
	st, err := db.Stats()
	if err != nil {
		return fail(stderr, err)
	}
	n := 0
	var prev []byte
	ordered := true
	if err := db.Scan(nil, nil, func(k, v []byte) bool {
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			ordered = false
		}
		prev, n = k, n+1
		return true
	}); err != nil {
		return fail(stderr, err)
	}
	if n != st.Keys || !ordered {
		fmt.Fprintf(stderr, "anchordb: verify FAILED: scanned %d keys (ordered=%v), stats say %d\n", n, ordered, st.Keys)
		return 1
	}
	fmt.Fprintf(stdout, "ok: recovery succeeded, %d keys in order, last_txid %d\n", n, st.LastTxID)
	return cmdStats(db, nil, stdout, stderr)
}
