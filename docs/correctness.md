# AnchorDB correctness evidence

This document maps each guarantee in `docs/architecture.md` (spec v1.1) to the
failure model it assumes and to the tests that check it. It also says plainly
what the tests do **not** prove.

The tests were written by a separate test engineer. Expected behaviour comes
from the spec, and expected *state* comes from an independent reference model
(`internal/refmodel`: a Go map plus sorting, and spec-derived format
encoders/decoders that share no code with the engine). Engine code was read
only to learn exported names and signatures.

## How to run

```
go test ./...                 # full suite (~20 s on the author's laptop)
go test -short ./...          # trimmed loops (~12 s)
go test -run TestRandomOps -v .            # randomized model test
ANCHORDB_SEED=42 go test -run TestRandomOps .   # reproduce one seed
go test ./internal/wal -run XXX -fuzz FuzzDecodeCommit -fuzztime 60s
```

**Race detector.** Run on Windows/amd64 with portable MinGW gcc 16.2
(`CGO_ENABLED=1 go test -race ./...`, with the MinGW `bin` directory on
`PATH`). The concurrency, bank, failed-DB and randomized tests pass under
`-race`, with no races reported.

Fuzz targets (the seed corpus runs on every `go test`; coverage-guided fuzzing
was run for 10–20 s per target during development):
`internal/record.FuzzDecode`, `internal/wal.FuzzDecodeCommit`,
`internal/wal.FuzzScanSegment`, `internal/checkpoint.FuzzDecodeEntries`,
`internal/manifest.FuzzDecode`.

## Guarantee → assumption → tests

| # | Guarantee (spec §) | Failure / environment assumption | Tests |
|---|---|---|---|
| 1 | Basic KV semantics: Get/Put/Delete, overwrite, missing key → `ErrNotFound` with nil value, delete of missing key is not an error but is logged (§2.1, §2.2 r9/r13) | none | `TestCRUD`, `TestEmptyValueAndBinaryKeys`, `TestRandomOpsAgainstModel` |
| 2 | Validation and inclusive limits; `ErrEmptyKey`/`ErrKeyTooLarge`/`ErrValueTooLarge`/`ErrTxTooLarge` measured on the encoded payload (§2.1, r14) | none | `TestKeyValueValidation`, `TestValidationErrorsDoNotCloseTx`, `TestTxTooLarge` (full mode only) |
| 3 | Returned and passed slices are copies; an empty value comes back as a non-nil empty slice (r12) | none | `TestReturnedSlicesAreCopies`, `TestEmptyValueAndBinaryKeys` |
| 4 | Scan: `[start,end)`, nil/empty bounds, `start>=end` visits nothing, early stop (§2.3) | none | `TestScanBoundaries`, `TestRandomOpsAgainstModel` (scans with limits) |
| 5 | Persistence across clean Close/Open, txid continuity, `ReplayedTxs` (§5.3) | clean shutdown | `TestReopenPersistence`, `TestMultiSegmentReplay`, `TestRandomOpsAgainstModel` (reopen interleaved) |
| 6 | Atomic multi-key commit, rollback, one txid per non-empty tx, empty commit consumes no txid and writes nothing (§2.2 r9, §5.2) | none | `TestMultiKeyCommitAndRollback`, `TestEmptyCommitConsumesNoTxid` |
| 7 | Read-your-writes in `tx.Get` and `tx.Scan` (r5) | none | `TestReadYourWrites`, `TestRandomOpsAgainstModel` |
| 8 | Lifecycle rules: `ErrTxClosed`, `ErrTxReadOnly`, `ErrScanInProgress`, error precedence, nested scan, scan flag cleared after a panic, Update/View semantics and panics (§2.2 r7–r16) | none | `TestLifecycleClosedTx`, `TestReadOnlyTx`, `TestScanInProgress`, `TestScanFlagClearedAfterPanic`, `TestUpdateViewSemantics` |
| 9 | Serializability: no lost updates, readers never see a partial tx, repeatable reads, writers exclude readers (§3) | Go memory model; race detector run as above | `TestConcurrentCounter`, `TestReadersNeverSeePartialTx`, `TestWriterExcludesOthers`, `TestBankConcurrentExactBalances` |
| 10 | Bank invariant: total preserved in every snapshot **and** the exact balance of every account (computed independently, so an engine that does nothing fails) | none | `TestBankConcurrentExactBalances` (commuting transfers, concurrent, checkpoint mid-run, reopen), `TestBankSequentialMatchesModel` (conditional transfers vs. model, reopens) |
| 11 | Checkpoint concurrent with writers, readers and Stats; Stats stays consistent (§3 metaMu, §5.5) | none | `TestCheckpointConcurrentWithWriters` |
| 12 | Close waits for open txs; a Begin blocked during Close gets `ErrClosed`; methods after Close return `ErrClosed` (r11) | none | `TestCloseWaitsForOpenTx`, `TestClosedDB` |
| 13 | Directory lock: second Open → `ErrLocked`, in-process and from another process; the lock is released when the holder dies; a failed Open releases it (§2.1.1, §8) | OS releases locks on process exit | `TestSecondOpenInProcessIsLocked`, `TestLockAcrossProcesses`, `internal/lock.TestExclusiveAcrossProcesses`, `openCorrupt` helper (used by every corruption test) |
| 14 | Commit point: under **process crash**, every acknowledged commit survives; the recovered state equals the model applied to a prefix of attempted transactions that contains every acked one (at most one extra, the in-flight tx) (§5.2, §6.4) | process kill (`os.Exit` at a crash point); page-cache contents survive | `TestCrashPoints/*`, `TestCrashRandomized` |
| 15 | Per-crash-point outcomes in the §7.2 table (in-flight present/absent, `TruncatedBytes == ⌊len/2⌋`, final empty segment after `rotate.after-rename`, old/new `CURRENT`, garbage removed at Open, nothing left after `after-reclaim`), in both sync modes for commit points | process kill | `TestCrashPoints/*` (all 20 points, several in two configurations), `TestCrashAfterReclaimLeavesNothing`, `TestCrashCheckpointPartialWriteEmptyDB`, `TestCrashInitAfterSegment`, `TestCrashRecoveryAfterTruncate`, `TestCrashRecoveryMidCleanup`, `TestCrashPointsAllCovered` (checks every name in `crashpoint.Names()` is covered) |
| 16 | Recovery is idempotent: crash during truncation or cleanup, reopen → same state (§5.3) | process kill | `TestCrashRecoveryAfterTruncate`, `TestCrashRecoveryMidCleanup`, `TestCrashRandomized` (crash, recover, crash again) |
| 17 | Torn tail: the final segment cut at **every** byte offset inside the last record → that record dropped, file truncated, `TruncatedBytes` exact, DB writable; zero-filled tails and 1–15 trailing bytes are torn (§6.2) | an interrupted write leaves a prefix, possibly followed by zeros starting at a frame boundary (§8 a5) | `TestTornTailEveryOffset`, `TestZeroFilledTail`, `TestShortGarbageTailIsTorn`, `internal/wal.TestScanTornTailEveryOffset`, `TestScanZeroTail`, `TestScanShortGarbageTail` |
| 18 | Appends after truncation go to the new end of file (O_APPEND, §5.3 step 9) | none | `TestAppendAfterTornTailTruncation` |
| 19 | Corruption is detected and refused, never silently dropped: any bit flip in a complete record (including the last one), zeros inside a frame, bad segment header, wrong record type, txid gap or duplicate, damage in a non-final segment, a missing first or middle segment, bad or missing `CURRENT`, a corrupt checkpoint → `*CorruptionError` with the right `File`/`Offset`, `errors.Is(err, ErrCorrupt)` (§6.2, §5.3) | CRC-32C detects the damage (any single-bit flip is detected) | `TestBitFlipIsCorruption`, `TestZerosInsideFrameIsCorruption`, `TestBadSegmentHeader`, `TestNonFinalSegmentDamageIsCorruption`, `TestMissingSegment`, `TestTxidDiscontinuityIsCorruption`, `TestWrongRecordTypeIsCorruption`, `TestBadCURRENT`, `TestCURRENTMissing`, `TestCheckpointFileCorruption`, plus the per-package bit-flip tests |
| 20 | "Validate first, modify later": a failed Open leaves every data file byte-for-byte unchanged, even when there is also a torn tail and garbage to clean up (§5.3) | none | `openCorrupt` helper (snapshot before/after on every corruption test), `TestFailedOpenModifiesNothing` |
| 21 | Garbage collection by exact name pattern: engine temp files, segments below `wal_start` and unreferenced checkpoints are deleted; non-matching files (`foo.tmp`, `wal-7.log`, `checkpoint-3.ckpt`, …) are never touched; `RemovedFiles` is exact (§4, §5.3 step 8) | none | `TestOpenRemovesGarbageOnly`, `TestCURRENTMissing/unrelated files only` |
| 22 | Initialization order (segment 1 before `CURRENT`) and its crash window (§5.3 step 2) | process kill | `TestOpenCreatesDirAndInitialLayout`, `TestCrashInitAfterSegment`, `TestCURRENTMissing/*` |
| 23 | `ErrCommitUncertain`: a WAL write that lets through k bytes (0, 1, 15, 16, half, all-but-1, all) or an fsync failure → error wraps `ErrCommitUncertain` and the cause, not `ErrDBFailed`; the DB is failed; after reopen the outcome is decided exactly by the bytes that reached the file (§5.2, §6.4) | faultfs injects the error; the bytes it lets through really reach the OS | `TestCommitWriteFailures/*`, `TestCommitFailureSyncNone` |
| 24 | Failed-DB behaviour table: all new ops → `ErrDBFailed`; an already-open tx keeps reading; its Commit → `ErrDBFailed`; Rollback works; Close returns nil and performs **no** write/fsync/truncate/rename/remove/dir-sync (checked by counting faultfs events); second Close → `ErrClosed` (§2.1.1) | faultfs | `assertFailedDB` / `closeFailed` helpers in all fault tests, `TestOpenTxSurvivesDBFailure` |
| 25 | Rotation failures: seal-fsync/rename/dir-sync fail the DB (the error wraps `ErrDBFailed`, not uncertain); temp create/write/fsync failures are clean "not committed" errors, the temp file is removed, and the DB keeps working (§5.2 step 2, §5.4) | faultfs | `TestRotationFailures/*` |
| 26 | Checkpoint failure at every step leaves the DB openable with the correct state: failures before the `CURRENT` rename → old `CURRENT`, temp files deleted, DB usable; `CURRENT` rename/dir-sync failure → DB failed; reclamation failure → published, not failed, and leftovers cleaned by the next checkpoint or Open (§5.5) | faultfs | `TestCheckpointFailures/*` (16 injection points) |
| 27 | Durability needs fsync (SyncAlways): after cutting every file to its last-fsynced length, all acked commits are still present; SyncNone loses at most a suffix and stays atomic per tx (§6.3) | **approximation**: faultfs `SyncedLengths` truncates file data to the last fsync; it does not model lost or reordered directory operations | `TestPowerLossApproxSyncAlways`, `TestPowerLossApproxSyncNoneIsPrefix` |
| 28 | On-disk formats exactly as specified (frame, Commit payload, segment/checkpoint headers, Entries/Trailer, `CURRENT`) in both directions: the engine reads hand-built directories and its own files decode with the reference decoders | none | `TestOpenHandBuiltDirectory`, `TestEngineFilesMatchReferenceFormat`, `TestAppendMatchesReference`, `TestEncodeCommitMatchesReference`, `TestSegmentHeaderMatchesReference`, `TestEncodeEntriesMatchesReference`, `TestWriteThenRead`, `TestEncodeMatchesSpec` |
| 29 | Decoders never panic, allocate at most 16·len(input)+4 KiB (spec §4.2 target; `record.Decode` allocates nothing), agree with the independent reference decoder on accept/reject, and accept only canonical encodings (re-encode == input) | adversarial input | `FuzzDecode` (record), `FuzzDecodeCommit`, `FuzzScanSegment`, `FuzzDecodeEntries`, `FuzzDecode` (manifest), `TestDecodeDoesNotAllocate`, `TestDecodeCommitHugeCountDoesNotAllocate`, `TestDecodeRejects`, `TestDecodeCommitRejects`, `TestDecodeEntriesRejects` |
| 30 | Frame decode check order and classification (§4.1) | none | `TestDecodeCheckOrder`, `TestDecodeEveryTruncationIsIncomplete`, `TestDecodeEveryBitFlip` |
| 31 | Randomized end-to-end equivalence with the model across reopens and checkpoints, with random segment sizes, binary keys, empty and 3 KB values | none | `TestRandomOpsAgainstModel` (8 seeds full, 3 short) |

## What is NOT proven

* **Process kill is not power loss.** The crash tests end the process with
  `os.Exit`. Everything already passed to `write` survives in the OS page
  cache. That shows the protocol never acknowledges before its commit point and
  that recovery handles every intermediate *file* state a process crash can
  leave. It says nothing about which writes the disk actually persisted.
* **The power-loss approximation is weak.** `faultfs` cuts file *data* back to
  the last fsync. It does not drop or reorder creates, renames or deletes. It
  does not model torn sectors, pages persisted out of order, or a disk that
  lies about cache flushes (§8 assumption 1). On Windows, `SyncDir` is a
  no-op, and the rename durability (`MOVEFILE_WRITE_THROUGH`) comes from NTFS
  and is not tested.
* **Only Windows 11 / NTFS was tested.** The Linux/macOS code paths, including
  directory fsync, are not exercised.
* **Undetectable loss is undetectable.** Losing whole final segments, or a log
  cut exactly at a record boundary, cannot be detected (§6.2, §11).
  `TestMissingFinalSegmentIsUndetectable` pins this documented behaviour; it is
  not a guarantee.
* **CRC-32C is not cryptographic.** The tests show that every single-bit flip
  in the tested records is detected. Multi-bit damage is detected with high
  probability only.
* **Concurrency is tested, not proven.** The tests use bounded schedules. The
  race detector reports only races that actually occur during the run.
* **Fuzzing is short.** It ran for seconds per target, not CPU-days.
* **Out of scope:** memory exhaustion, datasets near the RAM limit, and `Close`
  while a checkpoint is running. That last one is serialized by `ckptMu`
  according to the spec, but no test forces the interleaving.

## Known failing tests (engine bugs, reported to the engine owner)

| Test | Spec | Problem |
|---|---|---|
| `TestCheckpointFailures/ckpt_rename` | §5.5 "Failure in steps 3–4 …: delete temp files" | When renaming `checkpoint-T.ckpt.tmp` fails, the engine returns the error but never tries to remove the temp file. It stays until the next `Open`. |
