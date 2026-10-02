# AnchorDB project instructions

Educational transactional key-value storage engine in Go. `docs/architecture.md`
is the specification; code and tests must agree with it. Read `STATUS.md` first
for the current state and next steps.

## Commit-and-push workflow

Remote: `https://github.com/rosaiju/anchordb` (public since 2026-10-01, after a full-history scan for
secrets and personal data; never commit secrets or local databases such as `mydb/`). Default branch: `main`.

1. Commit each meaningful, completed change with a descriptive message that
   says what changed and why. Do not create empty commits, and do not split a
   change artificially to inflate the contribution graph.
2. Run the relevant checks **before** committing: at minimum `gofmt -l .`,
   `go vet ./...`, and `go test ./...` for code changes. Use `bash scripts/check.sh`
   (full suite, race detector when a C compiler is on PATH, fuzz smoke, demo)
   before merging larger work.
3. Push after committing. Verified work is merged into `main`; experimental or
   unverified work goes on a branch until its checks pass.
4. Preserve accurate authorship and real dates. Use the repository's configured
   identity (`Rohan Sainju <rosaiju@users.noreply.github.com>`, which GitHub
   links to the rosaiju account). Never rewrite published history, never
   change global Git config, and never use `--no-verify` or force-push to `main`.
5. AI-assisted commits end with the `Co-Authored-By:` trailer for the model used.
6. Never commit generated databases (`demo-data/`, test temp dirs), binaries
   (`bin/`), scratch files, downloaded toolchains, or credentials. Check
   `git status` before staging; stage files explicitly.
7. If another agent is working in the tree, stage only files you own and never
   commit its partially written files.

## Engineering rules

- Do not weaken a test or quietly change a documented guarantee to make a
  failure disappear; fix the code or update the spec explicitly (with a change-log entry).
- Keep scope fixed: no SQL, networking, replication, MVCC, auth, dashboard, ORM.
- Don't publish benchmark numbers that weren't measured; record hardware and commands in BENCHMARKS.md.
