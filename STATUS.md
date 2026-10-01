# AnchorDB Status

_Last updated: 2026-10-01._

All five milestones and the acceptance criteria in the original brief are
complete, within the stated failure model. Verified on **Windows 11 only**.
AnchorDB is an educational engine, not production software.

## Completed

- **Milestone 1:** specification `docs/architecture.md` v1.1. The test agent reviewed v1.0 independently and the review was integrated (see the spec's change log). Module `github.com/rosaiju/anchordb`, standard library only.
- **Milestone 2:** skip-list index, record framing, WAL segments, CRUD, ordered range scans, reopen.
- **Milestone 3:** transactions (private write buffer, read-your-writes, rollback), the fsync-before-success commit protocol, a failed-DB state after an uncertain I/O outcome, crash recovery with torn-tail/corruption classification, directory lock, 20 crash points.
- **Milestone 4:** checkpoint write → publication via an atomic `CURRENT` replace → reclamation; orphan cleanup at Open.
- **Milestone 5:**
  - Independent test suite (118 test/fuzz functions, written from the spec by a separate agent).
  - Benchmarks (BENCHMARKS.md).
  - CLI.
  - Demo scripts (bash + PowerShell).
  - Docs: README, architecture, correctness, learning guide, interview guide.
- **Repository:** private `github.com/rosaiju/anchordb`, default branch `main`. The commit-and-push workflow is in `CLAUDE.md`.

## Bugs found by independent verification

- A checkpoint temp file was left behind when its rename failed (`TestCheckpointFailures/ckpt_rename`). Fixed in 2885da3; the test was not changed.
- Spec review (before implementation) found about 25 issues. Two were genuine safety problems: an initialization order that could hide a deleted first WAL segment, and `Close` racing a checkpoint's publication. Both were fixed in spec v1.1 and implemented.

## Verification performed (2026-10-01, Windows 11, Go 1.26.2)

`bash scripts/check.sh` with `FUZZTIME=15s` and a portable MinGW-w64 gcc 16.2 on PATH (needed for `-race`):

| Step | Result |
|---|---|
| gofmt; `go vet` for windows, linux, darwin | pass |
| `go test -count=1 ./...` (~20 s) | pass |
| `go test -race -count=1 ./...` | pass, no races reported |
| Fuzzing, 15 s each: `record.FuzzDecode`, `manifest.FuzzDecode`, `wal.FuzzDecodeCommit`, `wal.FuzzScanSegment`, `checkpoint.FuzzDecodeEntries` | pass |
| `scripts/demo.sh` and `scripts/demo.ps1` | pass |

The main agent reviewed the crash-test verification logic: acknowledged commits must survive, the recovered state must equal the reference model after a prefix of attempted transactions, and every crash point has an expected outcome. It also checked the bank test's exact-balance assertions. Only the `-short`/resource skips listed in `docs/correctness.md` exist (e.g. a 64 MiB transaction test under `-short`).

## Unresolved issues and limitations

- **Linux and macOS are untested.** The code compiles and vets for both. WSL has no Go or gcc installed.
- Process-kill crash tests do not prove power-loss safety. `faultfs.LoseUnsynced` is only an approximation (see `docs/correctness.md` "What is NOT proven").
- Benchmarks come from one laptop, on battery. The checkpoint-vs-WAL recovery benefit was not measured on an overwrite-heavy workload.
- No group commit, manual checkpoints only, dataset must fit in RAM, writes serialized (by design; see architecture §10–11).
- `Close` racing a running checkpoint is serialized by `ckptMu`, but no test forces that interleaving.

## Exact next steps (optional future work)

1. Install Go (and gcc) in WSL; run `bash scripts/check.sh` on Linux ext4 and record the results.
2. Add a benchmark with many small overwriting transactions to measure the checkpoint's recovery benefit.
3. Add a test that forces `Close` to wait on an in-progress checkpoint (e.g. via a blocking faultfs rule).
4. If extending: group commit first, then MVCC (keep the spec and correctness doc in step).
