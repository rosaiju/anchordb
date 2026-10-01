#!/usr/bin/env bash
# AnchorDB end-to-end demonstration. Every step is checked automatically;
# the script exits non-zero if any check fails.
#
#   1. create and populate a database
#   2. commit a multi-key transaction (a bank transfer)
#   3. demonstrate rollback (explicit, and a failed transfer)
#   4. crash a subprocess at controlled points (mid-commit, after the commit
#      point but before acknowledgement, and mid-checkpoint)
#   5. reopen the database (running recovery)
#   6. verify the recovered state against expected values
#
# Usage: scripts/demo.sh [DATA_DIR]      (works in Git Bash on Windows too)
set -u
cd "$(dirname "$0")/.."

EXE=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;; esac
BIN="bin/anchordb${EXE}"
DATA="${1:-demo-data}"

echo "== building CLI"
go build -o "$BIN" ./cmd/anchordb || exit 1
rm -rf "$DATA"

FAILS=0
db() { "$BIN" -db "$DATA" "$@"; }
check() { # check DESCRIPTION EXPECTED ACTUAL
  if [ "$2" = "$3" ]; then echo "  PASS  $1 ($3)"; else echo "  FAIL  $1: expected '$2', got '$3'"; FAILS=$((FAILS+1)); fi
}
bal() { db get "acct:$1"; }
total() { echo $(( $(bal alice) + $(bal bob) + $(bal carol) )); }

echo
echo "== 1. create and populate"
db tx put acct:alice 100 put acct:bob 100 put acct:carol 100 >/dev/null
db load -n 1000 -batch 250 -prefix item: >/dev/null
check "key count" "1003" "$(db scan | wc -l | tr -d ' ')"
check "total balance" "300" "$(total)"

echo
echo "== 2. multi-key transaction: transfer 30 alice -> bob"
db transfer alice bob 30 >/dev/null
check "alice" "70" "$(bal alice)"
check "bob" "130" "$(bal bob)"
check "total balance preserved" "300" "$(total)"

echo
echo "== 3. rollback"
db tx -rollback put acct:alice 0 del acct:bob >/dev/null
check "alice unchanged after explicit rollback" "70" "$(bal alice)"
check "bob unchanged after explicit rollback" "130" "$(bal bob)"
db transfer carol alice 5000 2>/dev/null
check "failed transfer exit status" "1" "$?"
check "carol unchanged after failed transfer" "100" "$(bal carol)"

echo
echo "== 4a. crash in the middle of writing a commit record (commit.partial-write)"
ANCHORDB_CRASH_AT=commit.partial-write db transfer bob carol 50
check "process crashed with exit status 86" "86" "$?"
echo "== 5a. reopen"
check "recovery truncated the torn record" "yes" "$(db stats | awk '/^recovery/{print ($3 != "truncated_bytes=0") ? "yes" : "no"}')"
echo "== 6a. verify: the unacknowledged, torn transfer is absent"
check "bob" "130" "$(bal bob)"
check "carol" "100" "$(bal carol)"
check "total balance preserved" "300" "$(total)"

echo
echo "== 4b. crash after the commit point, before acknowledgement (commit.after-sync)"
ANCHORDB_CRASH_AT=commit.after-sync db transfer carol alice 10
check "process crashed with exit status 86" "86" "$?"
echo "== 5b/6b. reopen and verify: the transfer IS committed (it reached the log)"
check "alice" "80" "$(bal alice)"
check "carol" "90" "$(bal carol)"
check "total balance preserved" "300" "$(total)"

echo
echo "== 4c. crash during checkpoint publication (checkpoint.after-current-tmp)"
ANCHORDB_CRASH_AT=checkpoint.after-current-tmp db checkpoint >/dev/null
check "process crashed with exit status 86" "86" "$?"
check "old CURRENT still authoritative" "checkpoint_txid  0" "$(db stats | grep checkpoint_txid)"
check "alice" "80" "$(bal alice)"
db checkpoint >/dev/null
stat() { db stats | awk -v k="$1" '$1 == k {print $2}'; }
check "a clean checkpoint now covers every transaction" "$(stat last_txid)" "$(stat checkpoint_txid)"
check "old WAL segments reclaimed (live WAL is one empty segment)" "32" "$(stat wal_bytes)"

echo
echo "== final verification after reopen"
db verify
check "verify exit status" "0" "$?"
check "key count" "1003" "$(db scan | wc -l | tr -d ' ')"
check "balances" "80 130 90" "$(bal alice) $(bal bob) $(bal carol)"

echo
if [ "$FAILS" -eq 0 ]; then echo "DEMO PASSED"; else echo "DEMO FAILED: $FAILS check(s)"; exit 1; fi
