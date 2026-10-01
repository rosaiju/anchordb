# AnchorDB Learning Guide

This guide explains the five core ideas of a storage engine — **indexing,
transactions, logging, checkpoints, and recovery** — using AnchorDB's actual
code. Read it with the code open. Each section ends with an exercise.

Suggested reading order of the code:

1. `internal/index/skiplist.go` (ordered index, ~170 lines)
2. `tx.go` (transactions, read-your-writes, scan merge)
3. `internal/record/record.go` and `internal/wal/` (log format)
4. `commit.go` (the commit protocol: the most important 60 lines)
5. `recovery.go` (how the disk is turned back into memory)
6. `checkpoint.go` + `internal/manifest/` (snapshots and safe deletion)

---

## 1. Indexing: the skip list

**Problem.** We need `Get` by key *and* ordered range scans. A hash map gives
fast `Get` but no order. A sorted array gives order but O(n) inserts.

**Idea.** A *skip list* (Pugh, 1990) is a sorted linked list with "express
lanes". Every node is on level 0. With probability 1/4 it is also on level 1,
with probability 1/16 on level 2, and so on. To find a key you start on the
highest lane and move right while the next key is smaller, then drop down a
lane. On average this visits O(log n) nodes, like a balanced tree, but the code
is much simpler: there are no rotations.

**In AnchorDB.** `SkipList.findGE` (`internal/index/skiplist.go`) is the whole
search algorithm. Notice the `prev` array. While searching, it records the
last node visited on each level. Those nodes are exactly where `Set` must
splice in a new node and where `Delete` must unlink one. The random height
comes from `randomLevel`, which uses a *seeded* generator. Results never depend
on the seed, but performance becomes reproducible.

**Range scans** are trivial once you have order: `Seek(start)` finds the
first key `>= start`, then you follow level-0 pointers until `key >= end`.

**What it is not.** A skip list is an in-memory structure. Disk-based engines
use B-trees (pages on disk) or LSM trees (sorted files). AnchorDB avoids that
problem by keeping everything in memory and using the disk only for
durability (log + snapshots). That is a deliberate simplification.

> **Exercise.** Insert 1,000 keys into `index.New(1)` and print how many nodes
> have each height. Compare with the expected 3/4, 3/16, 3/64… distribution.

---

## 2. Transactions and isolation

**Atomicity** means a transaction's writes happen all-or-nothing. **Isolation**
means concurrent transactions don't see each other's half-finished work.

**AnchorDB's isolation model is the simplest correct one.** The DB has one
`sync.RWMutex`. `Begin(true)` (read-write) takes the exclusive lock and holds
it **until Commit or Rollback**. `Begin(false)` (read-only) takes the shared
lock for its whole life. So:

- two writers never overlap → no lost updates, no write skew;
- a reader never overlaps a writer → no dirty reads, and repeatable reads.

That is **serializable** isolation, by construction. The price is concurrency:
writers run one at a time, and a long transaction blocks everyone. Real
engines use locking per key or MVCC (multiple versions) to do better. AnchorDB
deliberately doesn't (see `docs/architecture.md` §3).

**Private writes and read-your-writes.** `tx.Put` doesn't touch the shared
index. It writes into `tx.writes`, a *second* skip list owned by the
transaction (`tx.go`). A `nil` value in that buffer means "deleted in this
transaction". `tx.Get` checks the buffer first, then the shared index.
`tx.Scan` has to *merge* two sorted streams (the index and the buffer). Read
the loop in `Tx.Scan`: on equal keys the buffer wins, and a buffered delete
hides the committed key. This is the same merge step as in merge sort.

**Rollback is free.** Since nothing shared was modified, rollback just drops
the buffer and releases the lock (`Tx.end`). Database people call this
**no-steal**: uncommitted data is never written to the shared state or the
disk, so nothing ever has to be undone.

> **Exercise.** Write a test where two goroutines each run 1,000
> `Update`s that read a counter, add 1, and write it back. Explain why the
> final value is exactly 2,000. Then explain what would go wrong if
> `Begin(true)` took the lock only inside `Commit`.

---

## 3. Logging: the write-ahead log

**Rule of write-ahead logging:** before a change is considered done, a
description of it must be on stable storage. AnchorDB logs *redo* information
only: the final value of each key the transaction wrote.

**One record per transaction.** `commit.go` encodes the entire write set as one
`Commit` payload (`internal/wal/commit.go`) and wraps it in one frame
(`internal/record/record.go`):

```
[ N | crc(N) | crc(type+payload) | type | 000 ][ payload: txid, ops... ]
```

Because the transaction is one checksummed unit, recovery either sees a valid
record (the whole transaction) or not. No "BEGIN ... COMMIT" matching is
needed.

**Why does the length have its own checksum?** Suppose a bit flip turns `N`
into 2 GB. Without `crc(N)`, the reader would think "this record extends past
end-of-file, so it must be a torn write", and would silently throw away
everything after it. With `crc(N)`, a bad length is detected as *corruption*.
Only a *trustworthy* length that runs past EOF counts as a torn write.

**The commit protocol** (`DB.commit` in `commit.go`):

1. encode one record
2. `write` it to the end of the active segment
3. `fsync` (in `SyncAlways` mode)
4. apply the writes to the in-memory index
5. return success

The **commit point** is the moment the complete record is in the log. From
then on, recovery will replay it. Success is returned only after the fsync, so
an acknowledged transaction is durable. A crash *after* step 2 but before step
5 commits a transaction nobody was told about. That is normal and unavoidable
(see §6.4 of the architecture doc on uncertain outcomes).

**When fsync fails** we don't retry. After a failed fsync the OS may have
dropped the dirty pages, so a retry could "succeed" without the data being on
disk (PostgreSQL learned this in 2018). AnchorDB marks the DB failed and makes
you reopen, so recovery can read what is *actually* on disk.

**Segments.** The log is split into files (`wal-0000000000000001.log`, …).
Rotation (`DB.rotate`) fsyncs the old segment before creating the next one.
So only the *last* segment can ever contain a half-written record.

> **Exercise.** Use `wal.EncodeCommit` and `record.Append` to build a frame,
> then truncate it at every possible length and call `record.Decode`. Which
> lengths give `ErrIncomplete`, and which give a different error? Why?

---

## 4. Checkpoints and log reclamation

The log grows forever, and replaying it gets slower. A **checkpoint** is a
full snapshot of the state at some transaction id `T`. After a checkpoint,
log records `<= T` are no longer needed.

The hard part is **making the switch crash-safe** (`DB.Checkpoint` in
`checkpoint.go`):

1. **Rotate** the WAL, so every record after `T` goes into segment `S` or later.
2. Write `checkpoint-T.ckpt.tmp`, **fsync** it, and rename it to `checkpoint-T.ckpt`.
3. **Publish:** write `CURRENT.tmp` saying "checkpoint T, replay from segment
   S", fsync it, and **rename it over `CURRENT`**.
4. **Reclaim:** only now delete segments `< S` and old checkpoints.

`CURRENT` is the single source of truth. The rename in step 3 is atomic, so
after a crash `CURRENT` is either the old one (which references files we
haven't deleted yet) or the new one (whose checkpoint was already fsynced).
There is no moment where the authoritative state depends on a file we have
deleted. That is invariant I7: *a file is deleted only if no durable CURRENT
references it.*

A crash during step 4 just leaves unreferenced files. The next `Open`
deletes them, because it can see from `CURRENT` that nothing needs them.

> **Exercise.** For each of the crash points `checkpoint.after-file-rename`,
> `checkpoint.after-current-tmp`, and `checkpoint.mid-reclaim`, list the files
> on disk at the moment of the crash, and say which `CURRENT` recovery will use.
> Then check your answer with
> `ANCHORDB_CRASH_AT=<point> bin/anchordb -db d checkpoint` and `ls d`.

---

## 5. Recovery

`Open` calls `DB.recover` (`recovery.go`). It follows one rule: **validate
first, modify later.**

1. Read and verify `CURRENT`.
2. Load the checkpoint it names (every frame checksummed, keys strictly
   ascending, trailer present).
3. Check that WAL segments `wal_start, wal_start+1, …` all exist, with no gaps.
4. Replay each record, requiring txids to be consecutive (`T+1, T+2, …`).
5. Only now modify anything: truncate a torn tail, delete garbage files.

**Torn tail vs corruption** (`wal.ScanSegment`) is the subtle part:

| What recovery finds at the end of the *last* segment | Meaning | Action |
|---|---|---|
| a frame that runs past EOF (trustworthy length) | crash mid-`write` | truncate (safe: never acknowledged) |
| fewer than 16 bytes left | crash mid-header | truncate |
| only zero bytes left | file extended, data not written | truncate |
| a complete frame with a bad checksum | **damaged data** | refuse to open |

Why refuse? A complete record with a bad checksum might be an acknowledged
transaction that the disk damaged. Silently dropping it would lose committed
data without anyone noticing. AnchorDB stops and reports the file and offset.

> **Exercise.** Create a database, commit three transactions, close it, then
> flip one byte in the middle of the second record with a hex editor. What does
> `bin/anchordb -db d verify` print? Now instead truncate the file in the
> middle of the *third* record. What happens, and why is that different?

---

## Putting it together: the life of one transfer

`anchordb transfer alice bob 30`:

1. `Open` → `recover`: load checkpoint, replay WAL, truncate any torn tail.
2. `db.Update` → `Begin(true)`: exclusive lock.
3. `tx.Get(acct:alice)`, `tx.Get(acct:bob)`: buffer (empty) then index.
4. `tx.Put` × 2 → private buffer.
5. `tx.Commit` → `DB.commit`: one record with both puts → write → fsync →
   apply both to the index → unlock → success.
6. `Close`.

Crash anywhere before the record is fully written: neither balance changes.
Crash after: both change. Never one without the other. That is atomicity, and
`scripts/demo.sh` checks it automatically.
