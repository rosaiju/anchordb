# AnchorDB Architecture

AnchorDB is an **educational**, single-machine, embedded, transactional key-value
storage engine written in Go. It is not production software. This document is
the specification that both the implementation and the test suite are written
against. Where the code and this document disagree, that is a bug in one of them.

Spec version: **1.0** (see "Change log" at the end).

---

## 1. Design summary

| Concern | Decision |
|---|---|
| Data model | Byte-string keys → byte-string values, ordered by `bytes.Compare` |
| Storage model | Whole dataset held in memory in an ordered index; disk holds a **checkpoint** (full snapshot) + a **write-ahead log** (redo log) |
| Index | Skip list implemented in `internal/index` |
| Concurrency | One `sync.RWMutex` held for the **entire life** of a transaction: many read-only transactions *or* one read-write transaction |
| Isolation | **Serializable** (transactions are effectively executed one writer at a time; readers never overlap a writer) |
| Atomicity | Each committed transaction is exactly **one** WAL record protected by CRC-32C |
| Durability | `SyncAlways` (default): record is `fsync`ed before `Commit` returns |
| Logging style | Redo-only, no-steal: uncommitted data never reaches disk, so recovery never needs undo |
| Recovery | Load checkpoint named by `CURRENT`, replay WAL segments, truncate a torn final record, refuse to open on corruption |
| Checkpoint publication | Atomic replace of the `CURRENT` file (write temp → fsync → rename → sync directory) |
| Multi-process safety | Exclusive OS lock on `DIR/LOCK` for the lifetime of an open `DB` |

This is deliberately the simplest defensible design. It resembles the
"snapshot + append-only log" approach used by Redis (RDB + AOF) and the log
framing ideas of LevelDB, but none of their code or formats are reused.

### What AnchorDB is *not*

No SQL, networking, replication, MVCC, secondary indexes, compression, or
datasets larger than RAM. See §11 for limitations.

---

## 2. Public API (package `github.com/rosaiju/anchordb`)

```go
type SyncMode int
const (
    SyncAlways SyncMode = iota // default: fsync the WAL before Commit returns
    SyncNone                   // never fsync on commit (see §6.3)
)

type Options struct {
    SyncMode    SyncMode
    SegmentSize int64  // rotate WAL when active segment >= this many bytes; 0 → 8 MiB
    FS          vfs.FS // nil → vfs.OS; tests inject faults through this
}

func Open(dir string, opts *Options) (*DB, error) // opts may be nil
func (db *DB) Close() error

func (db *DB) Begin(writable bool) (*Tx, error)
func (db *DB) View(fn func(tx *Tx) error) error   // read-only tx, always rolled back
func (db *DB) Update(fn func(tx *Tx) error) error // commits if fn returns nil, else rolls back

// Single-operation convenience wrappers (each is its own transaction).
func (db *DB) Get(key []byte) ([]byte, error)
func (db *DB) Put(key, value []byte) error
func (db *DB) Delete(key []byte) error
func (db *DB) Scan(start, end []byte, fn func(key, value []byte) bool) error

func (db *DB) Checkpoint() error
func (db *DB) Stats() (Stats, error)

func (tx *Tx) Get(key []byte) ([]byte, error)
func (tx *Tx) Put(key, value []byte) error
func (tx *Tx) Delete(key []byte) error
func (tx *Tx) Scan(start, end []byte, fn func(key, value []byte) bool) error
func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error
func (tx *Tx) Writable() bool

type Stats struct {
    Keys           int
    LastTxID       uint64 // id of the last committed transaction (0 = none)
    CheckpointTxID uint64 // txid covered by the authoritative checkpoint (0 = none)
    WALStartSeq    uint64 // first WAL segment referenced by CURRENT
    ActiveSeq      uint64 // segment currently appended to
    WALBytes       int64  // total bytes of live WAL segments
    Recovery       RecoveryInfo
}
type RecoveryInfo struct {
    ReplayedTxs    int    // WAL records applied on top of the checkpoint at Open
    TruncatedBytes int64  // bytes of torn tail removed at Open
    RemovedFiles   int    // orphan/temp files deleted at Open
}
```

### 2.1 Errors

| Error | Meaning |
|---|---|
| `ErrNotFound` | `Get` on a missing (or deleted) key. Value is `nil`. |
| `ErrEmptyKey` | key has length 0 (empty keys are not allowed; empty values are) |
| `ErrKeyTooLarge` | key longer than `MaxKeySize` = 64 KiB |
| `ErrValueTooLarge` | value longer than `MaxValueSize` = 16 MiB |
| `ErrTxTooLarge` | the transaction's encoded WAL record would exceed `MaxTxBytes` = 64 MiB |
| `ErrTxClosed` | operation on a transaction that already committed or rolled back |
| `ErrTxReadOnly` | `Put`/`Delete` on a read-only transaction |
| `ErrScanInProgress` | `Put`/`Delete`/`Commit`/`Rollback` called from inside that tx's own `Scan` callback |
| `ErrClosed` | the DB has been closed |
| `ErrCommitUncertain` | the commit's WAL write or sync failed; the transaction **may or may not** be durable (§6.4). The DB is now failed. |
| `ErrDBFailed` | the DB hit an unrecoverable I/O error earlier; every new operation fails until `Close` + `Open` |
| `ErrLocked` | another process (or another `DB` in this process) holds `DIR/LOCK` |
| `ErrCorrupt` | on-disk data is corrupt; returned errors are `*CorruptionError{File, Offset, Reason}` and satisfy `errors.Is(err, ErrCorrupt)` |

Errors returned by the library may wrap these; callers use `errors.Is`.

### 2.2 Transaction lifecycle rules

1. `Begin(writable)` blocks until the lock is available. `Begin(true)` takes the
   exclusive lock; `Begin(false)` takes a shared lock. The lock is held until
   `Commit` or `Rollback` returns.
2. A `Tx` must be used by one goroutine at a time.
3. **Do not call `Begin` (or any `db.*` convenience method, `Checkpoint`, or
   `Close`) while the same goroutine holds an open transaction.** This
   deadlocks, exactly like re-locking a `sync.RWMutex`. It is documented, not
   detected.
4. `Put`/`Delete` go into a private, ordered write buffer. Other transactions
   cannot see them (they can't even run concurrently with a writer).
5. Read-your-writes: `tx.Get` and `tx.Scan` see the transaction's own buffered
   writes merged over the committed state.
6. `Put`/`Delete` validation errors (`ErrEmptyKey`, `ErrKeyTooLarge`, ...) do
   **not** close the transaction; the buffer is unchanged and the caller may
   continue or roll back.
7. `Commit` always ends the transaction and releases the lock, whether it
   returns `nil` or an error.
8. `Rollback` discards the buffer and releases the lock. Calling it on a closed
   transaction returns `ErrTxClosed` and has no other effect, so
   `defer tx.Rollback()` is always safe.
9. `Commit` on a read-only transaction, or on a writable transaction with an empty
   write buffer, writes nothing, consumes no txid, and returns `nil`. (Unless the
   DB has already failed, in which case it returns `ErrDBFailed`.)
10. `Update`/`View` roll back if `fn` panics, then re-panic.
11. `Close` waits for all open transactions to finish, then releases files and
    the directory lock. Methods called after `Close` return `ErrClosed`.
12. Byte slices passed to `Put` are copied. Slices returned by `Get` and passed
    to `Scan` callbacks are copies owned by the caller.
13. A delete of a key that does not exist is not an error.

### 2.3 Scan semantics

`Scan(start, end, fn)` visits keys `k` with `start <= k < end` in ascending
`bytes.Compare` order. `start == nil` (or empty) means "from the first key";
`end == nil` (or empty) means "to the last key". If `start >= end` (both non-empty)
nothing is visited. `fn` returning `false` stops the scan early; `Scan` then
returns `nil`. Inside a transaction, the scan reflects buffered writes
(puts appear, deletes are hidden).

---

## 3. Isolation model and concurrency

The DB owns one `sync.RWMutex` (`db.mu`).

* Read-write transaction: `mu.Lock()` from `Begin` to the end of `Commit`/`Rollback`.
* Read-only transaction: `mu.RLock()` for its whole life.
* `Checkpoint`: `mu.RLock()` (it reads the index; writers wait, readers continue).
* `Close`: `mu.Lock()`.

Consequences, stated precisely:

* **Serializable.** No two read-write transactions overlap, and no reader overlaps
  a writer. Every execution is equivalent to a serial order of transactions.
  Readers see only committed data and get repeatable reads for their whole life.
* **No lost updates, no dirty reads, no write skew, no phantoms** — trivially,
  because there is no concurrent writer.
* **Throughput cost.** Writes are completely serialized. One long transaction
  blocks all writers (and a long writer blocks all readers). With `SyncAlways`,
  write throughput is bounded by one `fsync` per transaction — there is no group
  commit. Go's `RWMutex` blocks new readers once a writer is waiting, so writers
  are not starved by a stream of readers.

This is "serial execution" in the sense of *Designing Data-Intensive
Applications* (Kleppmann, ch. 7), with readers allowed to share.

---

## 4. On-disk layout

```
DIR/
  LOCK                                   exclusive OS lock, content irrelevant
  CURRENT                                names the authoritative checkpoint + first WAL segment
  checkpoint-00000000000000000042.ckpt   snapshot of state after txid 42 (20-digit txid)
  wal-0000000000000007.log               WAL segment 7 (16-digit sequence number)
  wal-0000000000000008.log
  *.tmp                                  temporary files; never authoritative; deleted at Open
```

All integers are **little-endian**. All checksums are **CRC-32C (Castagnoli)**
via `hash/crc32`.

### 4.1 Record frame (shared by WAL and checkpoint files; `internal/record`)

```
offset size field
0      4    N            payload length (uint32)
4      4    lenCRC       crc32c(bytes[0:4])            -- protects N on its own
8      4    payloadCRC   crc32c(byte[12] ‖ payload)    -- protects type + payload
12     1    type         record type
13     3    reserved     must be zero
16     N    payload
```

Header size: 16 bytes. Maximum payload: `record.MaxPayload` = 64 MiB + 1 KiB
(enough for a maximum transaction plus its own headers).

The separate `lenCRC` lets a reader trust `N` *before* reading the payload, so a
corrupted length is detected as corruption instead of being mistaken for a
record that runs past end-of-file.

`record.Decode(buf []byte) (Frame, int, error)` decodes one frame from the start
of `buf` and returns the frame, the number of bytes consumed, or one of:

| Error | Condition (checked in this order) |
|---|---|
| `record.ErrIncomplete` | `len(buf) < 16` |
| `record.ErrBadHeader` | `lenCRC` mismatch, or reserved bytes non-zero |
| `record.ErrTooLarge` | `N > MaxPayload` |
| `record.ErrIncomplete` | `len(buf) < 16 + N` |
| `record.ErrBadChecksum` | `payloadCRC` mismatch |

`Decode` never allocates more than it returns (the payload aliases `buf`).
`record.Append(dst []byte, typ byte, payload []byte) []byte` encodes a frame.

### 4.2 WAL segment file `wal-%016d.log`

```
segment header (32 bytes)
0   8  magic      "ANCHWAL1"
8   4  version    1
12  4  reserved   0
16  8  seq        segment sequence number; must equal the number in the file name
24  4  headerCRC  crc32c(bytes[0:24])
28  4  reserved   0
32  …  zero or more record frames
```

Record types in a WAL segment:

| type | name | payload |
|---|---|---|
| 1 | `Commit` | see below |

Commit payload:

```
u64  txid
u32  opCount                    (>= 1)
repeat opCount:
  u8   op        1 = Put, 2 = Delete
  u32  keyLen    1..MaxKeySize
  key
  if Put: u32 valLen  0..MaxValueSize, value
```

Within one Commit record keys are **strictly ascending** (the write buffer is
ordered and each key appears once, last write wins). The decoder rejects
unknown op codes, out-of-range lengths, non-ascending keys, `opCount == 0`, and
trailing bytes. It validates every length against the remaining payload before
allocating, so allocation is bounded by the input size.
`wal.EncodeCommit(Commit) []byte` / `wal.DecodeCommit([]byte) (Commit, error)`.

Segments are created as `wal-%016d.log.tmp`, header written and fsynced, then
renamed into place and the directory synced. Therefore **every `.log` file has a
complete header**; a bad header is corruption.

### 4.3 Checkpoint file `checkpoint-%020d.ckpt`

```
file header (32 bytes)
0   8  magic      "ANCHCKP1"
8   4  version    1
12  4  reserved   0
16  8  txid       state after applying txids 1..txid; must equal file name
24  4  headerCRC  crc32c(bytes[0:24])
28  4  reserved   0
32  …  record frames:
       type 2 `Entries`: u32 count (>=1), then count × (u32 keyLen, key, u32 valLen, value)
                         batched up to ~1 MiB of payload per frame
       type 3 `Trailer`: u64 txid, u64 totalEntries     -- exactly one, last
```

A checkpoint is valid only if: the header is valid; every frame decodes with
correct checksums; keys are strictly ascending across the whole file; exactly
one Trailer exists, is the final frame, ends exactly at end-of-file, and its
`txid` and `totalEntries` match. Anything else is corruption. A checkpoint file
is written as `checkpoint-%020d.ckpt.tmp`, fsynced, and renamed **before**
`CURRENT` refers to it, so there is no "torn checkpoint" case for a referenced
file.

### 4.4 `CURRENT`

A small text file, so you can `cat` it while learning:

```
ANCHORDB-CURRENT 1
checkpoint none                                     (or checkpoint-00000000000000000042.ckpt)
checkpoint_txid 0
wal_start 1
crc32c 1a2b3c4d                                     (8 lowercase hex digits, crc32c of all preceding bytes)
```

Each line ends in `\n`. If `checkpoint` is `none`, `checkpoint_txid` must be 0.
Written as `CURRENT.tmp` → fsync → rename over `CURRENT` → sync directory.
Rename-over-existing is atomic on the supported file systems (§8), so a reader
sees either the complete old file or the complete new file.

---

## 5. Data flow

### 5.1 Read path

`tx.Get(k)`: look in the tx write buffer (a second skip list of
`{deleted bool, value}`); if absent, look in the main index. `tx.Scan` merges an
iterator over the write buffer with an iterator over the index, buffer winning
ties, hiding deletes.

### 5.2 Commit protocol (read-write tx with non-empty buffer)

Executed while holding the exclusive lock:

1. If the DB is failed → return `ErrDBFailed`.
2. If the active segment is `>= SegmentSize` bytes, **rotate** (§5.4). A
   rotation error is returned as-is; nothing of this transaction has been
   written, so it is cleanly *not* committed (unless the rotation itself failed
   the DB — see §5.4).
3. `txid := lastTxID + 1`; encode one Commit record.
4. `write` the whole frame to the end of the active segment.
5. If `SyncAlways`: `fsync` the segment.
6. Apply every op to the in-memory index; `lastTxID = txid`.
7. Release the lock; return `nil`.

**Commit point.** A transaction is committed if and only if its complete,
checksum-valid record is present in the log as read by recovery. Under
`SyncAlways` this is guaranteed (within §8's failure model) once step 5's
`fsync` returns successfully. **Success is returned only after** steps 4–6.

If step 4 or 5 returns an error, the DB is marked **failed**, the index is not
changed, and `Commit` returns an error wrapping both `ErrCommitUncertain` and
the cause. Every later operation returns `ErrDBFailed`. See §6.4.

### 5.3 Recovery (`Open`)

1. Create `DIR` if needed. Acquire `DIR/LOCK` or fail with `ErrLocked`.
2. If `CURRENT` is missing:
   * if the directory contains no `wal-*.log` and no `checkpoint-*.ckpt` files,
     initialize: write `CURRENT` (`checkpoint none`, `wal_start 1`), then create
     segment 1. (CURRENT first, so a crash leaves a state step 4 handles.)
   * otherwise → `ErrCorrupt` (data files without a manifest).
3. Parse and verify `CURRENT` (bad syntax or CRC → `ErrCorrupt`).
4. If `checkpoint` is not `none`, load and fully validate it (§4.3) into the
   index. `lastTxID = checkpoint_txid`.
5. List `wal-*.log` segments with `seq >= wal_start`. They must form a
   contiguous run `wal_start, wal_start+1, …, last`. A gap → `ErrCorrupt`.
   If there are none and `checkpoint_txid == 0` and checkpoint is `none`, create
   segment `wal_start` (crash during initialization). If there are none
   otherwise → `ErrCorrupt`.
6. Replay each segment in order. Each record must be a Commit with
   `txid == lastTxID + 1`; anything else → `ErrCorrupt`. Apply it.
7. Tail handling — see §6.2. Only the **last** segment may have a torn tail; it
   is truncated to the end of the last valid record and fsynced.
8. Delete garbage: `*.tmp`, segments with `seq < wal_start`, and checkpoint
   files other than the one `CURRENT` names. Sync the directory.
9. Open the last segment for appending.

Steps 8 deletes only files that the durable `CURRENT` no longer references, and
happens only after the rest of recovery succeeded.

### 5.4 Segment rotation

Rotation (only while writers are excluded):

1. `fsync` the active segment (always, even in `SyncNone`). Failure → DB failed.
2. Create `wal-<next>.log.tmp`, write header, fsync. Failure → delete temp,
   return the error; the DB keeps using the old segment (not failed).
3. Rename to `wal-<next>.log`. **Any failure from this point on fails the DB**
   (the name may or may not exist in the directory, and continuing to append to
   the old segment could later make a torn record appear in a non-final segment).
4. Sync the directory; switch the active segment.

### 5.5 Checkpoint and log reclamation

`db.Checkpoint()`:

1. Take the checkpoint mutex (one checkpoint at a time), then `mu.RLock()`.
   If the DB is failed → `ErrDBFailed`.
2. `T := lastTxID`. If `T == CURRENT.checkpoint_txid` → nothing to do, return `nil`.
3. Rotate the WAL (§5.4) so every record with `txid > T` will live in segments
   `>= S`, where `S` is the new active segment.
4. Write `checkpoint-T.ckpt.tmp` by iterating the index in key order; fsync it;
   rename to `checkpoint-T.ckpt`; sync directory.
5. `mu.RUnlock()` (writers may proceed; they append to segment `S`).
6. **Publish**: write `CURRENT.tmp` (`checkpoint-T.ckpt`, `T`, `wal_start S`),
   fsync, rename over `CURRENT`, sync directory. The successful directory sync
   is the **publication point**: from here on the new checkpoint is
   authoritative.
7. **Reclaim**: delete segments with `seq < S` and older checkpoint files; sync
   the directory.

Failure handling:

* Failure in steps 3–4 (other than rotation failures that fail the DB) or in
  step 6 *before the rename*: delete temp files, return the error. The old
  `CURRENT` remains authoritative and every file it needs still exists.
* Failure of the rename or directory sync in step 6: either `CURRENT` may be
  authoritative, and **both are complete and valid** because nothing has been
  deleted yet. Return the error and **skip reclamation**. The DB is not failed.
* Failure in step 7: some old files remain; they are unreferenced and are
  deleted at the next `Open` (§5.3 step 8). Return the error. The DB is not failed.

**Why reclamation is safe.** A file is deleted only after a durable `CURRENT`
exists that does not reference it, and the state it described is fully
contained in the new checkpoint (txids ≤ T) plus segments `>= S` (txids > T).

---

## 6. Durability, torn writes, and corruption

### 6.1 Invariants

* **I1** `CURRENT` is always a complete file with a valid CRC (atomic rename).
* **I2** Every segment `wal_start … active` exists, contiguously.
* **I3** The checkpoint named by `CURRENT` is exactly the state after txids `1..checkpoint_txid`.
* **I4** WAL records from `wal_start` onward have consecutive txids starting at `checkpoint_txid + 1`.
* **I5** Under `SyncAlways`, the record of an acknowledged commit was fsynced before `Commit` returned.
* **I6** Outside an in-progress commit, the in-memory index equals checkpoint + all complete WAL records.
* **I7** A file is deleted only if no durable `CURRENT` references it.
* **I8** At most one open `DB` holds `DIR/LOCK`.
* **I9** Non-final segments are fsynced before the next segment is created, so only the final segment can have an incomplete record.

### 6.2 Torn tail vs corruption

While replaying a segment at offset `off` with `R` bytes remaining:

| Situation | Final segment | Non-final segment |
|---|---|---|
| `R == 0` | clean end | clean end |
| `record.ErrIncomplete` (header or payload runs past EOF) | **torn tail** → truncate at `off` | corrupt |
| `record.ErrBadHeader` and **all** `R` remaining bytes are `0x00` | **torn tail** (zero-filled extension) → truncate at `off` | corrupt |
| `record.ErrBadHeader` otherwise | corrupt | corrupt |
| `record.ErrTooLarge` | corrupt | corrupt |
| `record.ErrBadChecksum` | corrupt | corrupt |
| valid frame but bad Commit payload, wrong type, or wrong txid | corrupt | corrupt |

A torn tail is what an interrupted `write` leaves behind: a correct prefix of
the bytes we meant to write. It is safe to drop because that transaction was
never acknowledged (§5.2: success is returned only after the whole record was
written and, in `SyncAlways`, synced).

A complete-length record with a bad checksum is **not** silently dropped, even
at the end of the log: it may be an acknowledged transaction damaged by the
storage device. `Open` returns a `*CorruptionError` naming the file and offset
and leaves the files untouched. AnchorDB has no automatic repair tool; a human
must decide (e.g. restore from backup, or truncate deliberately). This is
conservative: after some power failures a file system may expose a
full-length region of garbage at the end of a file, and AnchorDB will refuse
to open rather than guess.

### 6.3 Sync modes

* `SyncAlways`: acknowledged commits survive process crashes **and** (within §8)
  OS crashes / power loss.
* `SyncNone`: commit returns after `write` without `fsync`. Acknowledged commits
  survive a **process** crash (the data is in the OS page cache) but may be lost
  on OS crash or power loss; a suffix of transactions may disappear. Recovery is
  still atomic per transaction (a commit is all-or-nothing). Rotation,
  checkpoints, `CURRENT`, and `Close` still fsync in both modes.

### 6.4 Uncertain commit outcomes

There are two ways a caller can lack a definite answer:

1. **Crash between commit point and acknowledgement.** The process dies after the
   record reached the log (step 4/5) but before `Commit` returned. After restart
   the transaction **is** committed even though nobody saw success.
2. **`ErrCommitUncertain`.** `write` or `fsync` returned an error. The bytes may be
   fully on disk, partially written (a torn tail), or absent; after a failed
   `fsync` the kernel's page cache state is undefined (see PostgreSQL's 2018
   "fsyncgate"). AnchorDB therefore does not retry the fsync, does not update
   the index, and fails the DB. After `Close` and `Open`, recovery decides:
   a complete valid record → committed; a prefix → torn tail → not committed.

Guidance for callers: treat the outcome as unknown, reopen, and check. Making
transactions idempotent (e.g. writing a unique request-id key in the same
transaction) lets a client check whether it was applied.

An *unacknowledged* transaction may therefore end up committed or not; an
*acknowledged* one must survive (within the failure model).

---

## 7. Fault-injection and crash hooks

### 7.1 `internal/vfs`

```go
type File interface {
    io.Reader; io.Writer; io.ReaderAt; io.Closer
    Sync() error
    Truncate(size int64) error
    Stat() (os.FileInfo, error)
    Name() string
}
type FS interface {
    OpenFile(name string, flag int, perm os.FileMode) (File, error)
    Remove(name string) error
    Rename(oldpath, newpath string) error  // atomic replace; durable-on-return on Windows
    ReadDir(dir string) ([]os.DirEntry, error)
    MkdirAll(dir string, perm os.FileMode) error
    Stat(name string) (os.FileInfo, error)
    SyncDir(dir string) error              // fsync a directory (no-op on Windows, see §8)
}
var OS FS // real file system
```

All engine file I/O except the LOCK file goes through `Options.FS`, so a test
can wrap `vfs.OS` and fail chosen `Write`/`Sync`/`Rename`/... calls (including
short writes).

### 7.2 `internal/crashpoint`

For **subprocess** crash tests. When the environment variable
`ANCHORDB_CRASH_AT=<name>` is set, the *n*-th time (`ANCHORDB_CRASH_AFTER=<n>`,
default 1) the engine reaches that point, the process calls
`os.Exit(crashpoint.ExitCode)` (`ExitCode = 86`) immediately: no deferred
functions, no buffered-file flush, no fsync. Data already handed to the OS with
`write` stays in the page cache, so this is equivalent to `kill -9` with
respect to file contents. **It does not simulate power loss.**

| Name | Where |
|---|---|
| `commit.before-write` | §5.2 after encoding, before step 4 |
| `commit.partial-write` | step 4: writes the **first half** of the frame, then exits |
| `commit.after-write` | after step 4, before fsync |
| `commit.after-sync` | after step 5, before applying to the index |
| `commit.after-apply` | after step 6, before returning |
| `rotate.after-seal` | §5.4 after step 1 |
| `rotate.after-create-tmp` | §5.4 after step 2 |
| `rotate.after-rename` | §5.4 after step 3, before directory sync |
| `checkpoint.after-rotate` | §5.5 after step 3 |
| `checkpoint.partial-write` | step 4: after the first Entries frame of the temp file is written |
| `checkpoint.after-file-sync` | step 4 after fsync of temp, before its rename |
| `checkpoint.after-file-rename` | step 4 after the rename |
| `checkpoint.after-current-tmp` | step 6 after `CURRENT.tmp` is fsynced, before rename |
| `checkpoint.after-publish` | step 6 after directory sync, before reclamation |
| `checkpoint.mid-reclaim` | step 7 after the **first** file is deleted |
| `checkpoint.after-reclaim` | after step 7 |

Disarmed cost: one atomic load per crash point.

---

## 8. Platforms and file-system assumptions

**Tested**: Windows 11, NTFS, Go 1.26 (amd64). **Expected to work** (code paths
exist, not regularly tested by the author): Linux (ext4/xfs), macOS (APFS).

Assumptions the durability guarantee depends on:

1. `fsync` (`FlushFileBuffers` on Windows, `fsync` on Linux, `F_FULLFSYNC` via
   Go's `os.File.Sync` on macOS) returns only after the file's data has reached
   stable storage, and the device honours cache flushes.
2. `rename` over an existing file is atomic.
3. On Linux/macOS, syncing a directory makes prior creates/renames/deletes in it
   durable. On Windows, directories cannot be fsynced through Go's API;
   AnchorDB uses `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`
   for renames and relies on NTFS metadata journaling; `SyncDir` is a no-op.
4. Local disk only. Network file systems (SMB/NFS) are unsupported; their lock
   and fsync semantics differ.
5. A write that is interrupted leaves a prefix of the intended bytes, possibly
   followed by zeros (§6.2). Silent bit flips are detected by checksums but not
   corrected.

**Not claimed:** that subprocess-kill tests prove correctness under every power
loss scenario. They test the protocol against process crashes. Power-loss
behaviour relies on assumptions 1–5, which are only partially testable in
software (see `docs/correctness.md`).

**Locking:** `internal/lock` uses `flock(LOCK_EX|LOCK_NB)` on Unix and opens
`LOCK` with share mode 0 (`CreateFile`) on Windows. Both are released by the OS
when the process dies, so a crash never leaves a stale lock.

---

## 9. Package layout

```
anchordb.go, tx.go, commit.go, recovery.go, checkpoint.go, errors.go   engine (package anchordb)
internal/index       skip list (ordered in-memory index)
internal/record      frame encoding/decoding
internal/wal         segment files, Commit payload codec, segment reader
internal/checkpoint  checkpoint file writer/reader
internal/manifest    CURRENT file
internal/vfs         file-system interface (+ OS implementation)
internal/lock        directory lock
internal/crashpoint  subprocess crash hooks
cmd/anchordb         CLI
scripts/             demo scripts
```

### 9.1 Internal APIs that tests may use

```go
// internal/index
func New(seed uint64) *SkipList
func (s *SkipList) Get(key []byte) ([]byte, bool)
func (s *SkipList) Set(key, value []byte)      // stores the slices as given (caller copies)
func (s *SkipList) Delete(key []byte) bool     // reports whether the key existed
func (s *SkipList) Len() int
func (s *SkipList) Seek(key []byte) *Iterator  // first key >= key; nil key → first
type Iterator; func (it *Iterator) Valid() bool; Key() []byte; Value() []byte; Next()

// internal/record — §4.1
// internal/wal
type Op struct { Delete bool; Key, Value []byte }
type Commit struct { TxID uint64; Ops []Op }
func EncodeCommit(c Commit) []byte
func DecodeCommit(p []byte) (Commit, error)
// internal/checkpoint
func DecodeEntries(p []byte) ([]Entry, error) // Entry{Key, Value []byte}
func Read(fs vfs.FS, path string, wantTxID uint64, visit func(k, v []byte)) error
// internal/manifest
type Current struct { Checkpoint string; CheckpointTxID, WALStart uint64 }
func Encode(c Current) []byte
func Decode(b []byte) (Current, error)
```

---

## 10. Tradeoffs

| Choice | Benefit | Cost |
|---|---|---|
| Whole-tx RWMutex | Serializable by construction; tiny code; easy to reason about | No write concurrency; long tx blocks everyone |
| One WAL record per tx | Atomicity from one checksum; no BEGIN/COMMIT matching | Tx size capped at 64 MiB; whole tx buffered in RAM |
| Redo-only, no-steal | Recovery never undoes anything | Write buffer must fit in memory |
| Dataset in memory + full snapshots | Simple, fast reads; checkpoint is a sorted dump | Dataset ≤ RAM; checkpoint cost O(dataset); recovery reads whole snapshot |
| Skip list | Simple ordered structure; easy merge iteration | More pointer-chasing than a B-tree; no on-disk pages |
| fsync per commit, no group commit | Simple, exact durability point | Write throughput bounded by device fsync latency |
| Refuse to open on a bad checksum | Never silently loses acknowledged data | A power-loss-garbled tail requires manual intervention |
| Manual checkpoint | Explicit, testable | WAL grows until someone checkpoints |

---

## 11. Limitations

* Dataset and each transaction must fit in RAM.
* Writes are serialized; no group commit; checkpoints block writers while the
  snapshot is written.
* No automatic checkpointing, compaction scheduling, or repair tool.
* Single process per directory (enforced). No read-only shared opens.
* Durability limited by §8 assumptions; Linux/macOS are not regularly tested.
* Not production-ready; no fuzzing beyond the record/payload decoders.

## 12. References and credits

* W. Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees", CACM 1990 — index algorithm.
* LevelDB log format (`doc/log_format.md`) — inspiration for checksummed, length-prefixed WAL frames (format here is different and simpler).
* C. Mohan et al., "ARIES" (1992) — terminology (redo, steal/no-steal, force/no-force); AnchorDB is a deliberately tiny redo-only, no-steal subset.
* T. Pillai et al., "All File Systems Are Not Created Equal" (OSDI 2014) — crash-consistency pitfalls (rename, directory fsync).
* PostgreSQL fsync error-handling discussion ("fsyncgate", 2018) — why a failed fsync fails the DB instead of retrying.
* M. Kleppmann, *Designing Data-Intensive Applications* (2017), ch. 3 and 7 — logs, snapshots, serial execution.
* Redis persistence docs (RDB + AOF) — snapshot-plus-log model.
* CRC-32C (Castagnoli) via Go's `hash/crc32`.

## Change log

* 1.0 — initial specification.
