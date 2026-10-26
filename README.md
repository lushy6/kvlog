# kvlog

**C++20 · TCP sockets · RESP (Redis protocol) · append-only log · crash recovery · concurrency**

A small key-value server that speaks the Redis wire protocol, keeps data in a sharded in-memory map, and
makes it durable with a checksummed append-only log. The point of the project is the parts that are easy
to get subtly wrong: write ordering across threads, torn writes, failed writes, group commit, and an honest
benchmark against Redis.

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
build/kvlog-server --port 6380 --log kvlog.aof --fsync always      # or everysec (default) / no / --no-log
redis-cli -p 6380 SET greeting hello
```

Commands: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, `DBSIZE`, `COMPACT` (rewrite the log), `QUIT`. The CI workflow
checks `SET`/`GET`/`DEL`/`EXISTS` through `redis-cli`.

## How it works

| Part | What it does | Where |
|---|---|---|
| Protocol | Incremental RESP parser: arrays of bulk strings, binary-safe, per-argument and per-connection size limits; finds argument offsets first and copies only once a command is complete. | `include/kvlog/resp.hpp` |
| Store | 64 shards, each an `unordered_map` behind a `shared_mutex`. | `include/kvlog/store.hpp` |
| Log | Each SET/DEL is a record `len · crc32 · op · key · value`. | `include/kvlog/log.hpp` |
| Replies | Every command in one network read is executed, then the batch is made durable **once**, then all replies are sent. Nothing is acknowledged before its write is durable under the chosen policy. | `src/server.cpp` |
| Durability | `always`: fsync before replying; one fsync covers every record appended before it started (**group commit**), across connections and across a pipeline. `everysec` and `no`: the batch is handed to the OS before replying (as Redis does), plus a background fsync each second for `everysec`. | `LogWriter` |
| Write errors | A failed write, flush or fsync puts the log into a **sticky failed state**: the batch gets `-ERR`, and every later write is refused. Nothing is acknowledged after a write error, and nothing is appended after a possibly torn record. | `LogWriter::fail` |
| Recovery | Replays records and stops at the first short or corrupt one. If nothing valid follows, it is a torn tail and is truncated. If a valid record *does* follow, the damage is mid-log and the server **refuses to start** instead of deleting later data. A read error is never mistaken for a torn tail. | `recover()` |
| Compaction | `COMPACT` writes one SET per live key to a temp file, checks every write, fsyncs, reads the file back to verify it decodes completely, then replaces the log (`rename` + directory fsync on POSIX, `MoveFileExW` with write-through on Windows). On any error the old log stays in use. | `LogWriter::rewrite` |
| Server | One thread per connection, portable sockets (Winsock / BSD), no SIGPIPE, errors in one connection never stop the server. | `src/server.cpp` |
| Benchmark | A RESP load generator, so kvlog and Redis are measured by the same client with the same settings. | `src/bench.cpp`, `bench/run_vs_redis.sh` |

### The ordering problem

With a lock per shard and one shared log, two SETs to the same key can be applied to memory in one order
and appended to the log in the other. Memory then says `v2` while a replay says `v1`. kvlog appends to the
log *while holding the key's shard lock* (shard lock → log lock, a fixed order, so no deadlock) and does the
slow fsync after releasing it.

The first version of the test for this compared the replayed log with memory only at the end of a run, and a
deliberately broken store (log appended after unlocking) **still passed**: threads finish at different times, so
their last writes rarely race. The test now stops all threads at a `std::barrier` 100 times and compares at
every stop. The broken variant is kept in the build behind `KVLOG_BROKEN_ORDERING`, and ctest requires that
build to fail (`test_store_broken_must_fail`), so the claim is re-checked on every run.

## Tests

| Test | What it checks |
|---|---|
| `test_resp` | Whole and byte-by-byte input parse identically; CRLF and NUL inside values; eight kinds of malformed input rejected, including a 20-digit length that used to overflow; client text cannot inject extra reply lines; the per-connection buffer limit holds; replies match Redis's format. |
| `test_log` | A file cut at **every byte offset** recovers exactly the complete records before the cut; a damaged last record is truncated as a torn tail; damage with valid records after it is refused, and the file is left untouched; appending after a truncated recovery works; rewrite replaces the log with exactly the snapshot. On Linux, writing to `/dev/full` proves write errors are never acknowledged and stay sticky. |
| `test_store` | Replay equals memory at 100 synchronised checkpoints under 8-thread contention on 8 hot keys; compaction running concurrently with writers loses nothing; a delete of a missing key writes nothing. |
| `test_store_broken_must_fail` | The same test, built with the ordering bug reintroduced, must fail. |
| `scripts/crash_test.py` | Starts the real server, runs 8 writer clients (and in every third round a client running `COMPACT` continuously), kills the process without warning (SIGKILL / TerminateProcess) at a random moment, restarts it, and checks every acknowledged write. A write in flight when the process died may or may not survive. Local Windows run, `--fsync always`: 20 kills, 26,181 acknowledged writes, all 2,400 keys intact; `everysec` and `no` also pass. |
| Negative control | `--unsafe-reply-before-write` (a test-only server flag) replies before writing; the crash test must then report data loss with its dedicated exit code 3, not just any failure. Locally it lost 777 acknowledged keys in round 1. |

The workflow in `.github/workflows/ci.yml` runs all of this on Ubuntu and Windows; the first run (commit
`f293e95`, 2026-10-26) passed on both, including the crash test and its negative control.

## Benchmark vs Redis

`bench/run_vs_redis.sh` runs both servers on one machine with the same `kvlog-bench` client: 16 connections,
50% SET / 50% GET over 100,000 keys with 64-byte values (the `always` mode is 100% SET), pipeline 1 and 16,
in three matching modes (memory only; AOF `everysec`; AOF `always`). CI runs it on a GitHub-hosted Ubuntu
runner with Redis 7.0.15 from apt.

First CI run (commit `f293e95`, 2 vCPUs shared by both servers' threads and the 16 client threads; one run,
so treat differences under about 20% as noise):

| Mode | Pipeline | kvlog ops/s | Redis ops/s | kvlog p50 / p99 | Redis p50 / p99 |
|---|---:|---:|---:|---:|---:|
| memory only, 50/50 | 1 | 57,890 | 56,194 | 245 / 881 µs | 274 / 483 µs |
| memory only, 50/50 | 16 | 545,636 | 444,287 | 386 / 1,391 µs | 525 / 958 µs |
| `everysec`, 50/50 | 1 | 54,732 | 54,454 | 266 / 844 µs | 282 / 493 µs |
| `everysec`, 50/50 | 16 | 449,624 | 383,701 | 484 / 1,922 µs | 598 / 1,252 µs |
| `always`, all SET | 1 | 11,893 | 23,394 | 1,222 / 4,266 µs | 590 / 1,395 µs |
| `always`, all SET | 16 | 157,263 | 204,084 | 1,464 / 3,929 µs | 1,220 / 2,911 µs |

What this says:

- **Without fsync on the hot path, kvlog keeps up with Redis on throughput** (about equal unpipelined, ahead
  by 17–23% pipelined) **but has worse tail latency** (p99 about 1.5–1.8x Redis's). One thread per connection
  on 2 vCPUs means more context switches than Redis's single event loop.
- **Under `always`, Redis is faster: about 2x unpipelined and 1.3x pipelined.** Both group-commit, so the gap
  is in the sync itself and its scheduling. Likely contributors, not measured separately: kvlog calls `fsync`
  where Redis on Linux calls `fdatasync` (which skips metadata), and kvlog's fsync runs from a connection
  thread rather than an event loop that keeps accepting work.
- These numbers are from one run on shared CI hardware. They show the order of magnitude and the direction
  of each gap, not a precise ratio.

Local Windows run, kvlog only (24-thread desktop, Redis not installed):

| Mode | Pipeline | ops/s | p50 | p99 |
|---|---:|---:|---:|---:|
| `everysec`, 50/50 | 1 | 204,395 | 29 µs | 419 µs |
| `everysec`, 50/50 | 16 | 750,103 | 313 µs | 813 µs |
| `always`, all SET | 1 | 8,082 | 1.6 ms | 6.9 ms |
| `always`, all SET | 16 | 167,586 | 1.3 ms | 4.2 ms |

Pipelining under `always` is 20x faster because the whole batch shares one fsync.

**Known differences in this comparison:** kvlog is multi-threaded and Redis (with default `io-threads 1`)
is single-threaded, on a runner with 2 vCPUs that the 16 client threads also use; kvlog calls `fsync` where
Redis on Linux calls `fdatasync`. A follow-up worth measuring: switch to `fdatasync` on Linux and re-run.

## Design decisions and limitations

- **Thread per connection, not an event loop.** Simple and portable (no epoll/IOCP split), and the sharded
  store lets connections run in parallel. It does not scale to tens of thousands of connections the way an event
  loop does; `--max-connections` (default 1,024) bounds it.
- **Reads can see a write before it is durable.** Under `--fsync always` a *writer* is acknowledged only after
  fsync, but another client may read the new value between apply and fsync. **Redis does not allow this**: it
  fsyncs before sending any reply in the event-loop pass, so no client sees an unsynced value. Closing this gap
  would mean holding readers of a key until its pending write is durable.
- **Compaction blocks.** `COMPACT` holds every shard lock while it writes and verifies the snapshot, so all reads
  and writes pause for its duration. A background rewrite (snapshot plus a buffer of concurrent writes, as Redis
  does) is the next step.
- **The crash test kills the process, not the machine.** The OS page cache survives a process kill, so it checks
  the log format, torn-tail recovery, compaction and reply-after-write ordering, not whether fsync reaches the
  disk. Power-loss safety (fsync, directory fsync after rename, write-through rename on Windows) is implemented
  but not tested; that would need a VM or a disk that drops unsynced writes.
- **Mid-log corruption stops the server.** That is deliberate: deleting everything after a bad record to get
  running again would silently lose acknowledged data. An operator decides.
- **No expiry, eviction, replication or auth.** It binds to 127.0.0.1 by default for that reason.
- **Whole log in memory during recovery.** Fine while compaction keeps the log small; a very large log would need
  a streaming reader.

An independent code review found the write-error, compaction-error, SIGPIPE, buffer-limit, reply-injection and
benchmark-fairness problems in the first version; they are fixed and most are covered by the tests above.
Development used AI assistance (Claude). The decisions above are written down so they can be checked.
