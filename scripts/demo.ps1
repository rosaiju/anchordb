# AnchorDB end-to-end demonstration for Windows PowerShell (same steps and
# checks as scripts/demo.sh). Exits 1 if any check fails.
#
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 [-Data demo-data]
param([string]$Data = "demo-data")
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "== building CLI"
go build -o bin\anchordb.exe .\cmd\anchordb
if ($LASTEXITCODE -ne 0) { exit 1 }
if (Test-Path $Data) { Remove-Item -Recurse -Force $Data }

$script:Fails = 0
function Db { & .\bin\anchordb.exe -db $Data @args }
function Check($desc, $expected, $actual) {
    if ("$expected" -eq "$actual") { Write-Host "  PASS  $desc ($actual)" }
    else { Write-Host "  FAIL  ${desc}: expected '$expected', got '$actual'"; $script:Fails++ }
}
function Bal($name) { Db get "acct:$name" }
function Total { [int](Bal alice) + [int](Bal bob) + [int](Bal carol) }
function Stat($key) { (Db stats | Where-Object { ($_ -split '\s+')[0] -eq $key } | ForEach-Object { ($_ -split '\s+')[1] }) }
function Crash($point) {
    $env:ANCHORDB_CRASH_AT = $point
    try { Db @args | Out-Null; $code = $LASTEXITCODE } finally { Remove-Item Env:\ANCHORDB_CRASH_AT }
    return $code
}

Write-Host "`n== 1. create and populate"
Db tx put acct:alice 100 put acct:bob 100 put acct:carol 100 | Out-Null
Db load -n 1000 -batch 250 -prefix item: | Out-Null
Check "key count" 1003 (Db scan | Measure-Object -Line).Lines
Check "total balance" 300 (Total)

Write-Host "`n== 2. multi-key transaction: transfer 30 alice -> bob"
Db transfer alice bob 30 | Out-Null
Check "alice" 70 (Bal alice)
Check "bob" 130 (Bal bob)
Check "total balance preserved" 300 (Total)

Write-Host "`n== 3. rollback"
Db tx -rollback put acct:alice 0 del acct:bob | Out-Null
Check "alice unchanged after explicit rollback" 70 (Bal alice)
Check "bob unchanged after explicit rollback" 130 (Bal bob)
Db transfer carol alice 5000 2>$null | Out-Null
Check "failed transfer exit status" 1 $LASTEXITCODE
Check "carol unchanged after failed transfer" 100 (Bal carol)

Write-Host "`n== 4a. crash in the middle of writing a commit record (commit.partial-write)"
Check "process crashed with exit status 86" 86 (Crash commit.partial-write transfer bob carol 50)
Write-Host "== 5a/6a. reopen and verify: the torn, unacknowledged transfer is absent"
$rec = Db stats | Where-Object { $_ -like "recovery*" }
Check "recovery truncated the torn record" $true ($rec -notmatch "truncated_bytes=0 ")
Check "bob" 130 (Bal bob)
Check "carol" 100 (Bal carol)
Check "total balance preserved" 300 (Total)

Write-Host "`n== 4b. crash after the commit point, before acknowledgement (commit.after-sync)"
Check "process crashed with exit status 86" 86 (Crash commit.after-sync transfer carol alice 10)
Write-Host "== 5b/6b. reopen and verify: the transfer IS committed (it reached the log)"
Check "alice" 80 (Bal alice)
Check "carol" 90 (Bal carol)
Check "total balance preserved" 300 (Total)

Write-Host "`n== 4c. crash during checkpoint publication (checkpoint.after-current-tmp)"
Check "process crashed with exit status 86" 86 (Crash checkpoint.after-current-tmp checkpoint)
Check "old CURRENT still authoritative" 0 (Stat checkpoint_txid)
Check "alice" 80 (Bal alice)
Db checkpoint | Out-Null
Check "a clean checkpoint now covers every transaction" (Stat last_txid) (Stat checkpoint_txid)
Check "old WAL segments reclaimed (live WAL is one empty segment)" 32 (Stat wal_bytes)

Write-Host "`n== final verification after reopen"
Db verify
Check "verify exit status" 0 $LASTEXITCODE
Check "key count" 1003 (Db scan | Measure-Object -Line).Lines
Check "balances" "80 130 90" "$(Bal alice) $(Bal bob) $(Bal carol)"

if ($script:Fails -eq 0) { Write-Host "`nDEMO PASSED"; exit 0 }
Write-Host "`nDEMO FAILED: $($script:Fails) check(s)"; exit 1
