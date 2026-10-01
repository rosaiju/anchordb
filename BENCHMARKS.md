# AnchorDB Benchmarks

Measured once on 2026-10-01 with Go's `testing` benchmarks (`bench_test.go`).
These numbers describe **this engine on this laptop**. They are not a
comparison with any other database; no fair comparison was run.

## Environment

| Item | Value |
|---|---|
| CPU | Intel Core Ultra 7 258V (8 cores; Go reports `-8`) |
| RAM | 32 GB |
| Disk | SK hynix 1 TB NVMe SSD (PVC10), NTFS |
| OS | Windows 11 Home (10.0.26200) |
| Go | 1.26.2 windows/amd64 |
| Power | **on battery** during the run (OS power management may lower CPU clocks) |
| Other load | none from this project (test agent finished); normal desktop background |

## Command

```sh
go test -run '^$' -bench . -benchtime 2s -count 3 -timeout 30m .
```

Each benchmark ran 3 times. The tables show the range across the 3 runs.

## Dataset and settings

- Keys `key:%010d` (14 bytes); values 100 random lowercase bytes.
- Read, scan, recovery, and checkpoint benchmarks use a 100,000-key database
  (≈11 MB of key+value data), loaded in transactions of 1,000 keys.
- Write benchmarks pick keys at random from a space of 100,000 (inserts and overwrites mixed).
- Default `SegmentSize` (8 MiB). Temp directories live on the same NVMe disk.
- **durable** = `SyncAlways`: fsync (`FlushFileBuffers`) before every commit returns.
  **nondurable** = `SyncNone`: no fsync on commit; survives process crashes only.

## Results

### Reads (in memory)

| Benchmark | Result |
|---|---|
| `Get`, 1 goroutine, random key | 793–836 ns/op |
| `GetParallel`, 8 goroutines sharing the read lock | 215–427 ns/op (aggregate; noisy across runs) |

### Durable writes (`SyncAlways`)

| Benchmark | Per transaction | Throughput |
|---|---|---|
| `Put`: 1 key per transaction | 677–686 µs | ≈1,460 commits/s |
| `TxBatch`: 10 keys per transaction | 792–893 µs | 11,197–12,623 keys/s |
| `TxBatch`: 100 keys per transaction | 1.30–1.42 ms | 70,469–76,853 keys/s |
| `TxBatch`: 1,000 keys per transaction | 4.97–5.11 ms | 195,677–201,390 keys/s |

Durable single-key commit latency is dominated by the fsync (≈0.68 ms on this
SSD). Batching more keys into one transaction amortizes that fsync, which is
the main lever available, since AnchorDB has no group commit.

### Non-durable writes (`SyncNone`), reported separately

| Benchmark | Per transaction |
|---|---|
| `Put`: 1 key per transaction | 14.4–14.7 µs (≈68,000 commits/s) |

This mode does **not** give the same guarantee: acknowledged commits can be
lost on an OS crash or power loss. It is shown only to separate the cost of
fsync from the cost of the engine and the `write` system call.

### Range scans (100,000-key database)

| Benchmark | Per scan | Throughput |
|---|---|---|
| 100 consecutive keys, random start | 7.9–8.0 µs | ≈12.6 M keys/s |
| full scan, 100,000 keys | 6.15–6.16 ms | ≈16.2 M keys/s |

Every key and value passed to the callback is copied (API contract), so this
includes about 200,000 small allocations per full scan.

### Recovery (`Open` of a 100,000-key database)

| Benchmark | Time |
|---|---|
| WAL replay only (100 commit records × 1,000 keys) | 27.9–29.1 ms |
| Load from checkpoint (empty WAL) | 27.5–28.4 ms |

In this dataset every key is written exactly once in large transactions, so
the WAL is about as compact as the checkpoint, and both paths cost about the
same. A checkpoint pays off when the WAL contains many small transactions or
overwrites of the same keys. That workload was **not** measured here.

### Checkpoint

| Benchmark | Time |
|---|---|
| Write + fsync + publish a 100,000-key checkpoint | 22.0–26.1 ms |

Writers are blocked while the snapshot is written (§5.5 of the architecture doc).

## Limitations of these measurements

- One machine, one run session, on battery; numbers can vary by tens of percent.
- fsync latency depends heavily on the drive and its write cache. Consumer SSDs
  may acknowledge flushes from a volatile cache; we did not test power loss.
- In-memory read numbers mostly measure the skip list, the `RWMutex`, and copying.
- No mixed read/write workload, no datasets near RAM size, no Linux/macOS numbers.
