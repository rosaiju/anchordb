# AnchorDB Interview Guide

## 30-second explanation

> AnchorDB is an educational transactional key-value store I built in Go. It's
> embedded, like SQLite or BoltDB, but much simpler. The data lives in an
> in-memory skip list I implemented. Every committed transaction is written as
> one checksummed record to a write-ahead log and fsynced before the commit
> returns, so commits are atomic and durable. On startup it loads the latest
> checkpoint and replays the log. It can tell a half-written final record,
> which it drops, from real corruption, which makes it refuse to open. I tested
> it by killing processes at about twenty named points in the commit,
> checkpoint, and recovery code, and by injecting write and fsync failures.

## Two-minute walkthrough

1. **API.** `Open`, `Begin(writable)`, `Get/Put/Delete/Scan`, `Commit`,
   `Rollback`, `Checkpoint`. Range scans are ordered because the index is a
   skip list (`internal/index/skiplist.go`).
2. **Isolation.** I chose the simplest defensible model. A read-write
   transaction holds an exclusive `RWMutex` from `Begin` to `Commit`, and readers
   share it. That's serializable by construction. The tradeoff is that writes
   are fully serialized, and I document that (`docs/architecture.md` §3).
3. **Private writes.** Puts go into a per-transaction skip list, so rollback
   is just dropping it. Reads merge that buffer over the shared index, which
   gives read-your-writes (`Tx.Scan` in `tx.go`).
4. **Commit protocol** (`DB.commit` in `commit.go`). Encode the whole write set
   as one WAL record, append it, fsync, apply to memory, return. The commit
   point is "complete record in the log". Success is returned only after the
   fsync. If write or fsync fails, the outcome is uncertain, so the DB marks
   itself failed and you must reopen. Recovery then decides from what's on disk.
5. **Record format** (`internal/record`). Length, CRC of the length, CRC of the
   payload. The separate length CRC means a truncated final record (torn write,
   safe to drop) can be distinguished from a corrupted record (refuse to open).
6. **Checkpoints** (`checkpoint.go`). Rotate the log, write a snapshot to a temp
   file, fsync, rename, then atomically replace a small `CURRENT` file that
   names the snapshot and the first log segment needed. Old segments are
   deleted only after that. A crash at any step leaves either the old or the
   new state fully usable.
7. **Testing.** The tests were written by a separate agent from the
   specification, not from my code. They include subprocess crash tests at
   every named crash point, checked against externally recorded acknowledged
   commits and a reference model; fault injection for short writes and failed
   fsyncs; torn and corrupted log files; randomized operation sequences against
   a reference model; a brief 15-second fuzzing run per decoder; and the race
   detector. All runtime testing was on Windows; Linux and macOS only compile
   and pass `go vet`. The crash tests cover process termination, not power loss.

## Technical questions and answers

**Q: What exactly is your commit point, and when do you return success?**
The commit point is when the complete, checksum-valid record is in the log,
because recovery replays every such record. In `SyncAlways` mode we fsync
right after the write and return success only after the fsync *and* after
applying the writes to memory. See `DB.commit` in `commit.go` and
`docs/architecture.md` §5.2.

**Q: What if the process crashes after the fsync but before Commit returns?**
The transaction is committed even though the caller never heard back. That
uncertainty is unavoidable in any system. The standard fix is to make
operations idempotent, e.g. store a request id in the same transaction and
check it after reconnecting. The crash point `commit.after-sync` tests
exactly this case (§6.4).

**Q: What happens if fsync returns an error?**
We don't retry. After a failed fsync, Linux may have marked the dirty pages
clean, so a retry can report success without the data being durable
(PostgreSQL's "fsyncgate", 2018). We return `ErrCommitUncertain`, mark the DB
failed so every later call returns `ErrDBFailed`, and require a reopen.
Recovery then trusts only what's on disk. Tested via fault injection
(`internal/faultfs`).

**Q: How do you distinguish a torn write from corruption?**
`wal.ScanSegment` in `internal/wal/segment.go`. Only the final segment may end
badly. Earlier segments were fsynced before rotation, so any damage there is
corruption. At the end of the final segment, three cases count as torn: a frame
whose checksum-validated length runs past EOF, fewer than 16 bytes, or nothing
but zeros. All are truncated. A complete frame with a bad checksum is
corruption, and `Open` fails with a `CorruptionError` naming the file and
offset. We don't silently drop it because it might be an acknowledged
transaction.

**Q: Why does the length field have its own CRC?**
Without it, a corrupted length that points past EOF looks exactly like a torn
write, and we'd silently truncate valid data after it. See
`record.PayloadLen`.

**Q: Why is checkpoint publication safe?**
`CURRENT` is the single source of truth, and it is replaced by
write-temp → fsync → rename → directory sync (`manifest.Publish`). Before the
rename, the old `CURRENT` and all its files are intact. After it, the new
checkpoint is already fsynced. Deletion happens only after publication, and
only of files the new `CURRENT` doesn't reference. Crash points cover every
step: `checkpoint.after-file-rename`, `after-current-tmp`,
`after-current-rename`, `after-publish`, `mid-reclaim`.

**Q: What isolation level do you provide? What anomalies are possible?**
Serializable. With one writer at a time and no reader overlapping a writer,
dirty reads, non-repeatable reads, phantoms, lost updates, and write skew are
all impossible. Tested with concurrent counters and bank transfers whose total
balance never changes, plus readers checking totals mid-flight.

**Q: How would you scale writes?**
First, group commit: batch several transactions' records into one fsync, which
is the biggest win under `SyncAlways`. Then finer-grained concurrency: per-key
locks with two-phase locking, or MVCC so readers never block writers. Each
adds real complexity to recovery and isolation reasoning, which is why the
baseline doesn't do it.

**Q: Why a skip list and not a B-tree?**
The index is in memory only; the disk holds just the log and snapshots. A skip
list gives ordered iteration and O(log n) expected operations with no
rebalancing code. A B-tree would matter if the index lived on disk in pages.

**Q: How do you prevent two processes from opening the same database?**
`internal/lock`: `flock(LOCK_EX|LOCK_NB)` on Unix, or opening `LOCK` with share
mode 0 on Windows. The OS releases both when the process dies, so a crash never
leaves a stale lock.

**Q: What does your crash testing prove, and what doesn't it?**
The crash tests kill the process at named points (`internal/crashpoint`),
which behaves like `kill -9`: data already written stays in the OS page cache.
That proves the protocol handles process crashes. It does **not** prove
correctness under power loss, where unsynced data and directory entries can
vanish or reorder. For that we rely on stated file-system assumptions
(`docs/architecture.md` §8). I wouldn't claim more.

**Q: What would you do differently in a production engine?**
Group commit; an on-disk index (B-tree or LSM) so data can exceed RAM; MVCC;
incremental checkpoints; a repair tool; testing on Linux with real power-cut
or `dm-flakey`-style fault injection; and a fuzzed end-to-end recovery
harness.

## Code map for questions

| Topic | Where |
|---|---|
| Skip list search/insert/delete | `internal/index/skiplist.go`: `findGE`, `Set`, `Delete` |
| Transaction lifecycle, read-your-writes, scan merge | `tx.go`: `Begin`, `Tx.Get`, `Tx.Scan`, `Tx.Commit`, `Tx.Rollback` |
| Commit protocol, rotation | `commit.go`: `DB.commit`, `DB.rotate` |
| Record frame | `internal/record/record.go`: `Append`, `PayloadLen`, `Decode` |
| Commit payload codec | `internal/wal/commit.go`: `EncodeCommit`, `DecodeCommit` |
| Torn tail vs corruption | `internal/wal/segment.go`: `ScanSegment` |
| Recovery | `recovery.go`: `DB.recover` |
| Checkpoint + reclamation | `checkpoint.go`: `DB.Checkpoint`, `DB.removeObsolete` |
| Atomic manifest | `internal/manifest/manifest.go`: `Publish`, `Decode` |
| Crash points | `internal/crashpoint/crashpoint.go` |
| Directory lock | `internal/lock/` |
