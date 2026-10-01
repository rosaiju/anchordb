#!/usr/bin/env bash
# Runs the complete AnchorDB verification suite and prints a summary.
#
#   scripts/check.sh            full suite
#   scripts/check.sh -short     skip the heaviest randomized/crash loops
#
# The race detector needs cgo (a C compiler on PATH). If none is found, the
# race step is reported as SKIPPED, not passed. On Windows, set ANCHORDB_GCC_DIR
# to a MinGW-w64 bin directory to enable it.
set -u
cd "$(dirname "$0")/.."
SHORT="${1:-}"
FUZZTIME="${FUZZTIME:-10s}"
[ -n "${ANCHORDB_GCC_DIR:-}" ] && export PATH="$ANCHORDB_GCC_DIR:$PATH"

declare -a RESULTS
FAILED=0
step() { # step NAME COMMAND...
  local name="$1"; shift
  echo; echo "=== $name: $*"
  if "$@"; then RESULTS+=("PASS     $name"); else RESULTS+=("FAIL     $name"); FAILED=1; fi
}

step "gofmt" bash -c 'test -z "$(gofmt -l .)" || { gofmt -l .; false; }'
step "go vet" go vet ./...
step "go vet (linux)" env GOOS=linux go vet ./...
step "go vet (darwin)" env GOOS=darwin go vet ./...
step "tests" go test $SHORT -count=1 ./...

if command -v gcc >/dev/null 2>&1; then
  step "tests -race" env CGO_ENABLED=1 go test $SHORT -race -count=1 ./...
else
  RESULTS+=("SKIPPED  tests -race (no C compiler; race detector needs cgo)")
fi

# Short fuzzing pass over every persistent-format decoder (seed corpus + FUZZTIME of new inputs each).
while IFS=: read -r file target; do
  pkg=$(dirname "$file")
  step "fuzz $pkg.$target ($FUZZTIME)" go test "./$pkg" -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME"
done < <(grep -rHoE '^func Fuzz[A-Za-z0-9_]+' --include='*_test.go' internal | sed 's/:func /:/' | sort -u)

step "demo" bash scripts/demo.sh "${TMPDIR:-/tmp}/anchordb-check-demo"

echo; echo "=== summary"
printf '%s\n' "${RESULTS[@]}"
exit $FAILED
