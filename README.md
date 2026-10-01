# AnchorDB

AnchorDB is an **educational** transactional key-value storage engine written
in Go. It is a single-machine, embedded library (plus a small CLI) that shows,
in about 2,200 non-blank lines of engine code (plus about 5,600 lines of tests and test tooling), how a storage engine provides:

- `Get`, `Put`, `Delete`, and ordered range `Scan`
- `Begin` / `Commit` / `Rollback` with atomic multi-key transactions
- a write-ahead log with length-prefixed, CRC-32C-checksummed records
- an ordered in-memory index (a skip list written for this project)
- serializable isolation for concurrent callers
- crash recovery that distinguishes a torn final write from corruption
- checkpoints that become authoritative atomically, plus safe log reclamation
- a directory lock that prevents two processes from opening the same database

> **Not production software.** AnchorDB exists to be read, tested, and
> explained. It keeps the whole dataset in memory, serializes all writes, and
> has only been tested on Windows. See [Limitations](#limitations).

## Install

Requires Go 1.23+ (developed with Go 1.26). AnchorDB uses only the Go
standard library.

```sh
git clone <this repo> anchordb && cd anchordb
go build -o bin/anchordb ./cmd/anchordb      # CLI (bin/anchordb.exe on Windows)
go test ./...                                 # test suite
```

## Library quick start

```go
db, err := anchordb.Open("data", nil) // nil options: fsync on every commit
if err != nil { log.Fatal(err) }
defer db.Close()

// Atomic multi-key transaction.
err = db.Update(func(tx *anchordb.Tx) error {
    if err := tx.Put([]byte("acct:alice"), []byte("70")); err != nil {
        return err
    }
    return tx.Put([]byte("acct:bob"), []byte("130"))
}) // committed and durable when Update returns nil

v, err := db.Get([]byte("acct:alice")) // "70"; ErrNotFound if missing

// Ordered range scan over [start, end).
db.Scan([]byte("acct:"), []byte("acct;"), func(k, v []byte) bool {
    fmt.Printf("%s=%s\n", k, v)
    return true // false stops early
})

db.Checkpoint() // snapshot state, then delete WAL segments it makes obsolete
```

Explicit transactions: `tx, _ := db.Begin(true); defer tx.Rollback(); ...; tx.Commit()`.
`Rollback` after `Commit` is a harmless no-op, so `defer tx.Rollback()` is the idiom.

## CLI examples

```sh
A="bin/anchordb -db ./mydb"
$A put greeting hello
$A get greeting                                  # hello
$A tx put acct:alice 100 put acct:bob 50         # one atomic transaction
$A transfer alice bob 30                         # read-modify-write transaction
$A tx -rollback put acct:alice 0 del acct:bob    # built, then rolled back
$A scan -prefix acct:                            # acct:alice 70 / acct:bob 80
$A load -n 10000 -batch 1000                     # bulk load
$A checkpoint                                    # snapshot + log reclamation
$A stats                                         # includes what recovery did at open
$A verify                                        # recover, full scan, sanity checks

# Simulated crash at a named point (exit status 86), then recovery:
ANCHORDB_CRASH_AT=commit.partial-write $A transfer alice bob 5
$A stats      # recovery ... truncated_bytes=NN  (torn record dropped)
```

## Demonstration

One command builds the CLI, creates and populates a database, commits a
multi-key transaction, demonstrates rollback, crashes subprocesses at three
controlled points (mid-commit, after the commit point but before
acknowledgement, and mid-checkpoint), reopens the database, and automatically
checks every recovered balance:

```sh
bash scripts/demo.sh                                    # Linux/macOS/Git Bash
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1   # Windows PowerShell
```

The full verification suite (format, vet, tests, race detector when a C
compiler is available, a brief 15-second fuzzing run per decoder, demo):

```sh
bash scripts/check.sh
```

## How it works (one paragraph)

A read-write transaction takes an exclusive lock and buffers its writes in a
private skip list. On commit, the whole write set is encoded as **one** WAL
record (length + length-CRC + payload-CRC), appended to the active segment and
fsynced. Only then is it applied to the in-memory index and success returned.
On open, recovery loads the checkpoint named by the `CURRENT` file and replays
every later WAL record. An incomplete final record (a torn write) is truncated;
a complete record with a bad checksum stops recovery with a `CorruptionError`.
A checkpoint is written to a temporary file, fsynced, renamed, and becomes
authoritative only when `CURRENT` is atomically replaced; only then are old WAL
segments deleted. Details: [docs/architecture.md](docs/architecture.md).

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | the specification: API, file formats, protocols, invariants, tradeoffs |
| [docs/correctness.md](docs/correctness.md) | guarantees, failure assumptions, and the tests that support each one |
| [docs/learning-guide.md](docs/learning-guide.md) | indexing, transactions, logging, checkpoints, recovery, explained through this code |
| [docs/interview-guide.md](docs/interview-guide.md) | 30-second and 2-minute explanations, likely questions with code references |
| [BENCHMARKS.md](BENCHMARKS.md) | measured results, hardware, and reproduction commands |
| [STATUS.md](STATUS.md) | what is done, what is not, verification performed, next steps |

## Supported platforms

| Platform | Status |
|---|---|
| Windows 11 / NTFS / amd64 | **runtime-tested**: unit, crash, fault-injection, randomized, race detector, brief fuzzing, demos |
| Linux (ext4, xfs) | **static checks only**: compiles and passes `go vet` (`GOOS=linux`); no tests executed. `flock` and directory-fsync code is not runtime-tested |
| macOS (APFS) | **static checks only**: compiles and passes `go vet` (`GOOS=darwin`); no tests executed. Go's `File.Sync` uses `F_FULLFSYNC` |

Local disks only; network file systems are not supported.

## Limitations

- Dataset and each transaction (≤ 64 MiB encoded) must fit in RAM.
- All writes are serialized; with fsync-per-commit, write throughput is bounded
  by the device's flush latency (no group commit). A long transaction blocks
  every other writer, and a writer blocks readers.
- Checkpoints are manual (`db.Checkpoint()` / `anchordb checkpoint`), write a
  full snapshot, and block writers while the snapshot is written.
- A complete-length WAL record with a bad checksum makes `Open` fail; there is
  no repair tool.
- Loss of entire final WAL segments cannot be detected.
- Crash testing covers process termination, not power loss; it does not prove
  behaviour under any power-loss scenario. Power-loss durability rests on the file-system
  assumptions listed in [docs/architecture.md §8](docs/architecture.md#8-platforms-and-file-system-assumptions).
- No SQL, networking, replication, MVCC, compression, or secondary indexes, by design.

## Credits

Skip list: W. Pugh (1990). WAL framing inspired by LevelDB's log format.
Terminology from ARIES (Mohan et al., 1992). Crash-consistency pitfalls from
Pillai et al., OSDI 2014. fsync error handling informed by PostgreSQL's 2018
"fsyncgate" discussion. Full list in [docs/architecture.md §12](docs/architecture.md#12-references-and-credits).
No code from these projects is used.
