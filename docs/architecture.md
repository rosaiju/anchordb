# AnchorDB Architecture

AnchorDB is an **educational**, single-machine, embedded, transactional key-value
storage engine written in Go. It is not production software. This document is
the specification that both the implementation and the test suite are written
against. Where the code and this document disagree, that is a bug in one of them.

Spec version: **1.1** (see "Change log" at the end).

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

* Limits are **inclusive**: a 65536-byte key and a 16777216-byte value are legal.
  `MaxKeySize`, `MaxValueSize`, `MaxTxBytes` are exported constants.
* `ErrTxTooLarge` is detected in `Put`/`Delete` (rule 6 below applies) and is
  measured on the encoded Commit payload (§4.2), i.e. `12 + Σ opSize`.
* `ErrCommitUncertain` errors do **not** satisfy `errors.Is(err, ErrDBFailed)`.
* `*CorruptionError`: `File` is the base name (e.g. `wal-0000000000000003.log`,
  `CURRENT`), `Offset` is the byte offset of the bad frame (0 for file-header
  problems and for `CURRENT`).
* `Open` with an invalid `SyncMode` returns an error and opens nothing.
  `SegmentSize <= 0` means the default. Segment size includes the 32-byte header.

### 2.1.1 Behaviour after the DB has failed

| Call | Result on a failed DB |
|---|---|
| `Begin`, `View`, `Update`, `Get`, `Put`, `Delete`, `Scan`, `Checkpoint`, `Stats` | `ErrDBFailed` |
| `Get`/`Put`/`Delete`/`Scan` on a transaction that was already open | keep working (they touch only memory) |
| `Commit` on an already-open transaction | `ErrDBFailed`; the tx is closed |
| `Rollback` on an already-open transaction | works as usual |
| `Close` | releases files and the lock, performs **no** write, fsync, or truncate, returns `nil` |

A second `Close` returns `ErrClosed`. A failed `Open` releases the lock and
any files it opened.

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
   DB has already failed, in which case it returns `ErrDBFailed`.) Any
   *successful* `Put` or `Delete` makes the buffer non-empty, regardless of net
   effect (a `Delete` of a key that never existed is still logged).
10. `Update`/`View` roll back if `fn` panics, then re-panic. `Update` returns
    `fn`'s error unchanged (after rolling back) if it is non-nil; otherwise it
    returns `Commit`'s result. `View` always rolls back and returns `fn`'s error.
    `fn` should not call `Commit`/`Rollback` itself; if it does, `Update`'s own
    `Commit` returns `ErrTxClosed`.
11. `Close` waits for all open transactions **and any in-progress
    `Checkpoint`** to finish, then releases files and the directory lock.
    Methods called after `Close` return `ErrClosed` (including a `Begin` that
    was blocked while `Close` waited).
12. Byte slices passed to `Put` are copied. Slices returned by `Get` and passed
    to `Scan` callbacks are copies owned by the caller. `Get` of a key stored
    with an empty value returns a non-nil, zero-length slice.
13. A delete of a key that does not exist is not an error.
14. Keys passed to `Get`/`Delete` are validated like `Put` keys (`ErrEmptyKey`,
    `ErrKeyTooLarge`).
15. Error precedence for tx methods: `ErrTxClosed` > `ErrTxReadOnly` >
    `ErrScanInProgress` > argument validation (`ErrEmptyKey`, …) >
    `ErrTxTooLarge`. `Commit`/`Rollback`: `ErrTxClosed` > `ErrScanInProgress` >
    `ErrDBFailed`.
16. `Scan` may be called (nested) from inside a `Scan` callback of the same tx.
    The scan-in-progress state is cleared even if the callback panics.

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
* `Checkpoint`: `ckptMu` for its whole duration, plus `mu.RLock()` while it
  rotates the WAL and writes the snapshot (writers wait, readers continue).
* `Close`: `ckptMu`, then `mu.Lock()`.
* `metaMu` (a plain mutex, held only briefly) protects the fields `Stats` reads
  that a checkpoint can change while other readers hold `mu.RLock()`: the
  decoded `CURRENT`, the active segment pointer, and the live-WAL byte count.
  `lastTxID` and the index are written only under `mu.Lock()`.
* The failed flag is an atomic boolean.

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

Exact name patterns. Files that match none of them (including `LOCK`) are
ignored and never deleted:

| Kind | Pattern |
|---|---|
| WAL segment | `^wal-[0-9]{16}\.log$` |
| Checkpoint | `^checkpoint-[0-9]{20}\.ckpt$` |
| Manifest | `^CURRENT$` |
| Engine temp file | a segment or checkpoint name followed by `.tmp`, or `CURRENT.tmp` |

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
allocating, so allocation is bounded by the input size (target: total bytes
allocated ≤ 16·len(p) + 4 KiB; `opCount` is never used to preallocate beyond
`len(p)/6`). Decoded keys and values **alias** the payload; the engine copies
them before storing them in the index.
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
`txid` and `totalEntries` match. Anything else is corruption. An empty
checkpoint (zero Entries frames, `totalEntries == 0`) is valid. A single entry
larger than 1 MiB occupies a frame on its own. `DecodeEntries` checks key length
`1..MaxKeySize`, value length `≤ MaxValueSize`, strictly ascending keys within
the frame, `count >= 1`, and no trailing bytes; `EncodeEntries([]Entry) []byte`
is its inverse. The header txid, file-name txid, trailer txid, and `CURRENT`'s
`checkpoint_txid` must all agree. A checkpoint file
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

Each line ends in `\n`. If `checkpoint` is `none`, `checkpoint_txid` must be 0;
otherwise the name must be `checkpoint-<checkpoint_txid as 20 digits>.ckpt`.
`wal_start >= 1`. Decoding is **canonical**: `manifest.Decode` accepts `b` only if
`manifest.Encode(manifest.Decode(b)) == b` (no CRLF, no leading zeros, no
uppercase hex, no trailing bytes). `Current.Checkpoint` is `""` for `none`.
A `CURRENT` that names a missing checkpoint file is `ErrCorrupt`.
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
2. If the active segment is `>= SegmentSize` bytes **and contains at least one
   record**, **rotate** (§5.4). Nothing of this transaction has been written
   yet, so a rotation error means it is cleanly *not* committed. If the
   rotation failed the DB (§5.4), `Commit` returns an error wrapping
   `ErrDBFailed` and the cause (not `ErrCommitUncertain`); otherwise it returns
   the cause and the DB stays usable.
3. `txid := lastTxID + 1`; encode one Commit record.
4. `write` the whole frame to the end of the active segment.
5. If `SyncAlways`: `fsync` the segment.
6. Apply every op to the in-memory index; `lastTxID = txid`.
7. Release the lock; return `nil`.

**Commit point.** A transaction is committed if and only if its complete,
checksum-valid record is present in the log as read by recovery. Under
`SyncAlways` this is guaranteed (within §8's failure model) once step 5's
`fsync` returns successfully. **Success is returned only after** steps 4–6.

If step 4 or 5 returns an error — including a short write (`n < len` with a nil
error, treated as `io.ErrShortWrite`) — the DB is marked **failed**, the index
is not changed, and `Commit` returns an error wrapping both
`ErrCommitUncertain` and the cause. Every later operation follows §2.1.1. See §6.4.

### 5.3 Recovery (`Open`)

**Validate first, modify later.** Steps 2–6 only read. No file in the
directory is created, truncated, or deleted until every file recovery depends
on has been validated. The only exceptions are creating `DIR` and `LOCK`, and
the initialization path in step 2. A failed `Open` therefore leaves every
data file byte-for-byte unchanged.

1. Create `DIR` if needed. Acquire `DIR/LOCK` or fail with `ErrLocked`.
2. If `CURRENT` is missing:
   * If the directory contains no segment and no checkpoint (by the exact
     patterns in §4), **initialize**: create segment 1 durably (§5.4 steps 2–4,
     crash point `init.after-segment` after it), then publish `CURRENT`
     (`checkpoint none`, `checkpoint_txid 0`, `wal_start 1`) with the same
     temp-file protocol as §5.5 step 6.
   * Else, if the only data file is `wal-0000000000000001.log`, it has a valid
     header and **zero records**, and there is no checkpoint: this is a crash
     between the two initialization steps; publish `CURRENT` as above.
   * Otherwise → `ErrCorrupt` (data files without a manifest).
3. Parse and verify `CURRENT` (any deviation → `ErrCorrupt`, `File: "CURRENT"`).
4. If `checkpoint` is not `none`, load and fully validate it (§4.3) into the
   index (a missing file → `ErrCorrupt`). `lastTxID = checkpoint_txid`.
5. List segments with `seq >= wal_start`. The first must be exactly
   `wal_start`, and they must form a contiguous run `wal_start … last`.
   No segments, a missing `wal_start`, or a gap → `ErrCorrupt`.
   Header-only segments (zero records) are legal anywhere in the run.
6. Read and validate every segment in order. Each record must be a Commit with
   `txid == lastTxID + 1`; anything else → `ErrCorrupt`. Apply it to the index.
   Only the **last** segment may end in a torn tail (§6.2).
7. *(First modification.)* If the last segment has a torn tail, truncate it to
   the end of the last valid record and fsync it (crash point
   `recovery.after-truncate`).
8. Delete garbage: engine temp files, segments with `seq < wal_start`, and
   checkpoint files other than the one `CURRENT` names (crash point
   `recovery.mid-cleanup` after the first deletion). Sync the directory.
9. Open the last segment for appending with `O_APPEND`, so writes always go
   to the current end of file, even after the truncation in step 7.

Step 8 deletes only files that the durable `CURRENT` no longer references.
Recovery is idempotent: a crash anywhere in steps 7–8 leaves a directory that
the next `Open` recovers to the same state.

### 5.4 Segment rotation

Rotation (only while writers are excluded):

1. `fsync` the active segment (always, even in `SyncNone`). Failure → DB failed.
2. Create `wal-<next>.log.tmp`, write header, fsync. Failure → delete temp,
   return the error; the DB keeps using the old segment (not failed).
3. Rename to `wal-<next>.log`. **Any failure from this point on fails the DB**
   (the name may or may not exist in the directory, and continuing to append to
   the old segment could later make a torn record appear in a non-final segment).
4. Sync the directory; open the new segment with `O_APPEND`; close the old
   segment's handle (Windows cannot delete files with open handles); switch.

Rotation happens in two places, both covered by the `rotate.*` crash points
(counted together): before a commit (§5.2 step 2) and at the start of a
checkpoint (§5.5 step 3). Checkpoint rotation happens even if the active
segment holds zero records.

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
   authoritative, and the in-memory copy of `CURRENT` is updated.
7. **Reclaim**: list the directory and delete, in this order, every segment
   with `seq < S` (ascending) and then every checkpoint file other than
   `checkpoint-T.ckpt`; sync the directory. Listing (rather than deleting a
   remembered range) means leftovers of an earlier failed reclamation are
   cleaned up too.

Failure handling:

* Failure in steps 3–4 (other than rotation failures that fail the DB) or in
  step 6 *before the rename*: delete temp files, return the error. The old
  `CURRENT` remains authoritative and every file it needs still exists. A
  checkpoint file that was already renamed into place in step 4 is an
  unreferenced orphan; it is removed by the next successful reclamation or `Open`.
  The DB is not failed.
* Failure of the rename or directory sync in step 6: either `CURRENT` may be
  the durable one. Both are complete and valid, because nothing has been
  deleted yet, so the data is safe. But the engine no longer knows which
  manifest a restart would use, so it **fails the DB** and skips reclamation
  (like rotation step 3). Reopening resolves it.
* Failure in step 7: some old files remain; they are unreferenced and are
  deleted by the next checkpoint's reclamation or the next `Open` (§5.3 step 8).
  Return the error. The DB is not failed.

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

Consequences worth stating explicitly:

* Because `Decode` checks `len < 16` first, **any** 1–15 trailing bytes at the
  end of the final segment are a torn tail and are truncated, whatever their
  content. Damage *inside a complete frame* is corruption.
* A valid header followed by a partial payload and then zero bytes extending
  past the frame's declared end yields `ErrBadChecksum` → **corruption** (refuse
  to open). Only zeros that start exactly at a frame boundary count as torn.
* A `.log` file shorter than 32 bytes or with a bad segment header is
  corruption, never torn (segments are created via temp file + rename).
* Recovery cannot detect the loss of whole *final* segments, or a log cut
  exactly at a record boundary, because `CURRENT` does not record the last
  segment or txid. Under §8's assumptions this cannot happen to acknowledged
  data; it is listed in §11.

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

The environment is read once, at process start. `crashpoint.Names()` returns
every point below, so tests can check that all of them are covered.

In every row, the recovered key/value state equals all transactions committed
before the crash, plus the in-flight transaction where the table says
"present". "In-flight" means the transaction whose commit was running when the
process died.

| Name | Where | After restart |
|---|---|---|
| `commit.before-write` | §5.2 after encoding, before step 4 | in-flight tx absent |
| `commit.partial-write` | step 4: writes the first `⌊len/2⌋` bytes of the frame, then exits | absent; `TruncatedBytes == ⌊len/2⌋` |
| `commit.after-write` | after step 4, before fsync | present (bytes are in the page cache) |
| `commit.after-sync` | after step 5 (in `SyncNone`: right after step 4), before applying | present |
| `commit.after-apply` | after step 6, before returning | present |
| `rotate.after-seal` | §5.4 after step 1 | in-flight tx absent; no new segment |
| `rotate.after-create-tmp` | §5.4 after step 2 | absent; temp segment removed (`RemovedFiles >= 1`) |
| `rotate.after-rename` | §5.4 after step 3, before directory sync | absent; the new empty segment is the final segment |
| `checkpoint.after-rotate` | §5.5 after step 3 | old `CURRENT`; state intact |
| `checkpoint.partial-write` | step 4: after the first Entries frame is written (empty DB: just before the trailer) | old `CURRENT`; temp removed |
| `checkpoint.after-file-sync` | step 4 after fsync of temp, before its rename | old `CURRENT`; temp removed |
| `checkpoint.after-file-rename` | step 4 after the rename | old `CURRENT`; orphan checkpoint removed |
| `checkpoint.after-current-tmp` | step 6 after `CURRENT.tmp` is fsynced, before rename | old `CURRENT`; temp removed |
| `checkpoint.after-current-rename` | step 6 after rename, before directory sync | new `CURRENT` (a process crash cannot undo the rename); old files removed at Open |
| `checkpoint.after-publish` | step 6 after directory sync, before reclamation | new `CURRENT`; old segments/checkpoint removed at Open |
| `checkpoint.mid-reclaim` | step 7 after the first file is deleted | new `CURRENT`; remaining old files removed at Open |
| `checkpoint.after-reclaim` | after step 7 | new `CURRENT`; nothing left to remove |
| `init.after-segment` | §5.3 step 2 after segment 1 is created, before `CURRENT` | next `Open` publishes `CURRENT`; empty DB |
| `recovery.after-truncate` | §5.3 step 7 | next `Open`: same state, `TruncatedBytes == 0` |
| `recovery.mid-cleanup` | §5.3 step 8 after the first deletion | next `Open`: same state, remaining garbage removed |

"Old/new `CURRENT`" can be observed through `Stats().CheckpointTxID`.
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
5. An interrupted write leaves a prefix of the intended bytes. The file may
   also be extended with zeros, but only starting at a frame boundary; zeros
   inside a frame are classified as corruption (§6.2). Silent bit flips are
   detected by checksums but not corrected.
6. The engine calls `FS.SyncDir` at every point this document specifies, on
   every platform (it is a no-op inside `vfs.OS` on Windows), so fault
   injection can observe and fail those calls.

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
func DecodeEntries(p []byte) ([]Entry, error) // Entry{Key, Value []byte}; aliases p
func EncodeEntries(es []Entry) []byte
func Read(fs vfs.FS, path string, wantTxID uint64, visit func(k, v []byte)) error
// internal/manifest
type Current struct { Checkpoint string; CheckpointTxID, WALStart uint64 }
func Encode(c Current) []byte
func Decode(b []byte) (Current, error)
// internal/crashpoint
func Names() []string
const ExitCode = 86
// package anchordb
const MaxKeySize, MaxValueSize, MaxTxBytes
type CorruptionError struct { File string; Offset int64; Reason string }
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
* Loss of whole final WAL segments, or a log truncated exactly at a record
  boundary, is undetectable (`CURRENT` does not record the log end).
* A power-loss-garbled final record (full length, bad checksum) makes `Open`
  refuse to proceed instead of guessing; there is no repair command.
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
* 1.1 — after the independent test-agent review: initialization creates
  segment 1 before `CURRENT` (so a missing segment is never silently
  re-created); "validate first, modify later" recovery rule; exact file-name
  patterns; ambiguous `CURRENT` publication fails the DB; reclamation by
  directory listing; `Close` waits for checkpoints; failed-DB behaviour table;
  error precedence and identity; inclusive limits; canonical `CURRENT`
  decoding; per-crash-point expected outcomes; new crash points (`init.*`,
  `recovery.*`, `checkpoint.after-current-rename`); documented torn-tail edge
  cases and undetectable final-segment loss.
