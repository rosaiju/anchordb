# AnchorDB Status

_Last updated: 2026-10-01._

> **Independent verification is still in progress.** The independent test
> suite has been delivered and integrated. The full suite (`go test ./...`)
> passes on Windows 11 after one engine bug it found was fixed. The full
> `scripts/check.sh` run (race detector, fuzzing, demo), the main agent's
> review of the test suite, and benchmarks are not finished yet. Treat every
> claim as provisional until this banner is removed.

## Completed

- Milestone 1: specification `docs/architecture.md` v1.1. The test agent reviewed v1.0 independently and its review was integrated (see the spec's change log). Project setup, module `github.com/rosaiju/anchordb`.
- Milestone 2: skip-list index, record framing, WAL segments, CRUD, ordered range scans, reopen.
- Milestone 3: transactions (private write buffer, read-your-writes, rollback), commit protocol with fsync, failed-DB state after an uncertain I/O outcome, crash recovery with torn-tail/corruption classification, directory lock, crash points.
- Milestone 4: checkpoint write → publication via `CURRENT` → reclamation; orphan cleanup at Open.
- CLI (`cmd/anchordb`), demo scripts (`scripts/demo.sh`, `scripts/demo.ps1`); both pass.
- Independent test suite (written from the spec by a separate agent): faultfs fault injection, reference model, unit + fuzz tests for every decoder, API/transaction/concurrency/bank tests, subprocess crash tests for every crash point, recovery/corruption tests, randomized model tests, `docs/correctness.md`.
- Bug found by that suite and fixed: a checkpoint temp file was left behind when its rename failed (`TestCheckpointFailures/ckpt_rename`).

## Verification performed so far

- `go test -count=1 ./...` on Windows 11 / Go 1.26.2: all packages pass (~20 s).
- The test agent's `-race` runs (portable MinGW gcc 16.2) of the root concurrency, bank, fault, and randomized tests: no races reported. A full race run by the main agent is pending.
- Both demo scripts: pass.

## Next steps

1. Main agent reviews the test suite for weak assertions; run `bash scripts/check.sh` with `ANCHORDB_GCC_DIR` set so `-race` runs.
2. Run benchmarks on an idle machine; write BENCHMARKS.md with real numbers.
3. Finalize README (line counts), correctness doc review, interview guide, and this file; remove the banner above.
