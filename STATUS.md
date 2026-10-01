# AnchorDB Status

_Last updated: 2026-10-01 (in progress — integration of the independent test suite pending)._

## Completed

- Milestone 1: specification `docs/architecture.md` v1.1 (reviewed independently by the test agent; review integrated, see its change log). Project setup, module `github.com/rosaiju/anchordb`.
- Milestone 2: skip-list index, record framing, WAL segments, CRUD, ordered range scans, reopen.
- Milestone 3: transactions (private write buffer, read-your-writes, rollback), commit protocol with fsync, failed-DB state after uncertain I/O, crash recovery with torn-tail/corruption classification, directory lock, crash points.
- Milestone 4: checkpoint write → publish via `CURRENT` → reclamation; orphan cleanup at Open.
- CLI (`cmd/anchordb`), demo scripts (`scripts/demo.sh`, `scripts/demo.ps1`) — both pass.
- Benchmarks (`bench_test.go`), verification runner (`scripts/check.sh`).
- README, learning guide, interview guide (drafts).

## In progress

- Independent test suite (test agent): faultfs, reference model, unit + fuzz tests for internal packages (passing so far), root-package correctness, crash, fault-injection, randomized, bank-transfer tests, `docs/correctness.md`.

## Next steps

1. Integrate the test agent's suite; fix engine bugs it finds (without weakening tests).
2. Run `scripts/check.sh` (with `ANCHORDB_GCC_DIR` set for `-race`).
3. Run benchmarks on an idle machine; write BENCHMARKS.md with real numbers.
4. Finalize STATUS.md, README line counts, correctness doc review.
