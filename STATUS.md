# AnchorDB Status

_Last updated: 2026-10-01._

All five milestones and the acceptance criteria in the original brief are
complete, within the stated failure model. **Runtime testing was on Windows 11
only**; Linux and macOS have static checks only (compile + `go vet`).
AnchorDB is an educational engine, not production software.

## Completed

- **Milestone 1:** specification `docs/architecture.md` v1.1. The test agent reviewed v1.0 independently and the review was integrated (see the spec's change log). Module `github.com/rosaiju/anchordb`, standard library only.
- **Milestone 2:** skip-list index, record framing, WAL segments, CRUD, ordered range scans, reopen.
- **Milestone 3:** transactions (private write buffer, read-your-writes, rollback), the fsync-before-success commit protocol, a failed-DB state after an uncertain I/O outcome, crash recovery with torn-tail/corruption classification, directory lock, 20 crash points.
- **Milestone 4:** checkpoint write → publication via an atomic `CURRENT` replace → reclamation; orphan cleanup at Open.
- **Milestone 5:**
  - Independent test suite (118 test/fuzz functions, written from the spec by a separate agent), plus `TestCloseWaitsForRunningCheckpoint`, added during integration.
  - Benchmarks (BENCHMARKS.md).
  - CLI.
  - Demo scripts (bash + PowerShell).
  - Docs: README, architecture, correctness, learning guide, interview guide.
- **Repository:** private `github.com/rosaiju/anchordb`, default branch `main`. The commit-and-push workflow is in `CLAUDE.md`.

## Bugs found by independent verification

- A checkpoint temp file was left behind when its rename failed (`TestCheckpointFailures/ckpt_rename`). Fixed in 2885da3; the test was not changed.
- Spec review (before implementation) found about 25 issues. Two were genuine safety problems: an initialization order that could hide a deleted first WAL segment, and `Close` racing a checkpoint's publication. Both were fixed in spec v1.1 and implemented.

## Verification performed (2026-10-01, Windows 11, Go 1.26.2)

**Runtime tests, Windows 11 / NTFS / amd64 only.** `bash scripts/check.sh` with `FUZZTIME=15s` and a portable MinGW-w64 gcc 16.2 on PATH (needed for `-race`):

| Step | Result |
|---|---|
| gofmt; `go vet` (windows) | pass |
| `go vet` with `GOOS=linux` and `GOOS=darwin` (**static check only; nothing executed on those OSes**) | pass |
| `go test -count=1 ./...` (~20 s) | pass |
| `go test -race -count=1 ./...` | pass, no races reported |
| Fuzzing: one **brief 15-second run** per decoder (`record.FuzzDecode`, `manifest.FuzzDecode`, `wal.FuzzDecodeCommit`, `wal.FuzzScanSegment`, `checkpoint.FuzzDecodeEntries`) | pass |
| `TestCloseWaitsForRunningCheckpoint` repeated: `-count=500`, and `-race -count=200` | pass, no races |
| `scripts/demo.sh` and `scripts/demo.ps1` | pass |

`TestCloseWaitsForRunningCheckpoint` (spec §2.2 rule 11) pauses a checkpoint at a chosen write with explicit channels, confirms through goroutine stacks that `Close` is parked on a lock, then releases it. It covers both the snapshot phase and the `CURRENT` publication phase. A mutation check (removing `ckptMu` from `Close`) makes the publication case fail. During development a first version hung because `Open` itself writes `CURRENT.tmp`; that was a test bug, fixed by arming the gate after `Open`. No engine change was needed.

The main agent reviewed the crash-test verification logic: acknowledged commits must survive, the recovered state must equal the reference model after a prefix of attempted transactions, and every crash point has an expected outcome. It also checked the bank test's exact-balance assertions. Only the `-short`/resource skips listed in `docs/correctness.md` exist (e.g. a 64 MiB transaction test under `-short`).

## Unresolved issues and limitations

- **Linux and macOS: static checks only.** The code compiles and passes `go vet` for both, but no test has run there. WSL has no Go or gcc installed.
- Crash testing covers process termination, not power loss; it does not prove power-loss safety. `faultfs.LoseUnsynced` is only an approximation (see `docs/correctness.md` "What is NOT proven").
- Benchmarks come from one laptop, on battery. The checkpoint-vs-WAL recovery benefit was not measured on an overwrite-heavy workload.
- No group commit, manual checkpoints only, dataset must fit in RAM, writes serialized (by design; see architecture §10–11).

## Exact next steps (optional future work)

1. Install Go (and gcc) in WSL; run `bash scripts/check.sh` on Linux ext4 and record the results.
2. Add a benchmark with many small overwriting transactions to measure the checkpoint's recovery benefit.
3. If extending: group commit first, then MVCC (keep the spec and correctness doc in step).
