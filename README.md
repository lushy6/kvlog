# kvlog

**C++20 · TCP sockets · RESP (Redis protocol) · append-only log · crash recovery · concurrency · CI on Linux and Windows**

A small key-value server that speaks the Redis wire protocol, keeps data in a sharded in-memory map, and
makes it durable with a checksummed append-only log. The point of the project is the parts that are easy
to get subtly wrong: write ordering across threads, torn writes on crash, group commit, and an honest
benchmark against Redis.

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
build/kvlog-server --port 6380 --log kvlog.aof --fsync always      # or everysec (default) / no / --no-log
redis-cli -p 6380 SET greeting hello                               # any Redis client works
```

Commands: `GET`, `SET`, `DEL`, `EXISTS`, `PING`, `DBSIZE`, `COMPACT` (rewrite the log), `QUIT`.

## How it works

| Part | What it does | Where |
|---|---|---|
| Protocol | Incremental RESP parser (arrays of bulk strings, binary-safe, size limits); replies for a whole network read are sent in one write, so pipelined clients are batched. | `include/kvlog/resp.hpp`, `commands.hpp` |
| Store | 64 shards, each an `unordered_map` behind a `shared_mutex`; readers of a shard share it, writers take it exclusively. | `include/kvlog/store.hpp` |
| Log | Each SET/DEL is a record `len · crc32 · op · key · value`. Recovery replays records and stops at the first short or corrupt one, then truncates the file there. | `include/kvlog/log.hpp` |
| Durability | `--fsync always`: a write is acknowledged only after an fsync covering it. **Group commit**: one fsync covers every record appended before it started, so concurrent writers share it. `everysec`: a background fsync each second. `no`: left to the OS. | `LogWriter` |
| Compaction | `COMPACT` writes one SET per live key to a temp file, fsyncs it and atomically renames it over the log. | `Store::compact`, `LogWriter::rewrite` |
| Server | One thread per connection, portable sockets (Winsock / BSD). | `src/server.cpp` |
| Benchmark | A RESP load generator, so kvlog and Redis are measured by the same client with the same settings. | `src/bench.cpp`, `bench/run_vs_redis.sh` |

### The ordering problem

With a lock per shard and one shared log, two SETs to the same key can be applied to memory in one order
and appended to the log in the other. Memory then says `v2` while a replay says `v1`. kvlog appends to the
log *while holding the key's shard lock* (shard lock → log lock, a fixed order, so no deadlock), and does
the slow fsync after releasing it.

The first version of the test for this compared the replayed log with memory only at the end of the run,
and a deliberately broken store (log appended after unlocking) **still passed**: threads finish at different
times, so their last writes rarely race. The test now stops all threads at a `std::barrier` 100 times and
compares at every stop; the broken store fails 5 of 10 checkpoints, the real one passes.

## Tests

| Test | What it proves |
|---|---|
| `test_resp` | Whole and byte-by-byte input parse identically; CRLF and NUL inside values; seven kinds of malformed input are rejected; replies match Redis's format. |
| `test_log` | A file cut at **every byte offset** recovers exactly the complete records before the cut and is truncated to that boundary; a flipped bit stops replay before the damaged record; appending after a truncated recovery works; rewrite is atomic. |
| `test_store` | Replay equals memory at 100 synchronised checkpoints under 8-thread contention on 8 hot keys (`fsync=no` and `always`); compaction running concurrently with writers loses nothing; group commit issues fewer fsyncs than writes (1,609 for 2,400 in one local run). |
| `scripts/crash_test.py` | Starts the real server with `--fsync always`, runs 8 writer clients, kills the process without warning (SIGKILL / TerminateProcess) at a random moment, restarts, and checks every acknowledged write: 20 kills, 20,002 acknowledged writes, all 2,400 keys intact (local Windows run). A write in flight when the process died may or may not survive; that is allowed. |
| Negative control | The same crash test with `--fsync no` **must fail** (acknowledged writes still in the user-space buffer die with the process), or the crash test is not detecting loss. Locally it fails in the first round and recovery reports a 20-byte torn record. CI enforces this. |

CI runs all of it on Ubuntu and Windows.

## Benchmark vs Redis

Run in CI on one GitHub-hosted Ubuntu runner: both servers, the same `kvlog-bench` client, 16 connections,
50% SET / 50% GET over 100,000 keys with 64-byte values (durable mode: 100% SET). Results:
**pending the first CI run; filled in from its job summary.**

Local Windows sanity run (kvlog only, 24-thread desktop, no Redis available): 123,751 ops/s at pipeline 1
(p50 119 µs, p99 310 µs), 1.31 M ops/s at pipeline 16, and 8,167 SET/s with `--fsync always`
(p50 1.6 ms).

## Design decisions and limitations

- **Thread per connection, not an event loop.** Simple and portable (no epoll/IOCP split), and the sharded
  store lets connections run in parallel. It does not scale to tens of thousands of idle connections the
  way Redis's event loop does; `--max-connections` (default 1,024) bounds it.
- **Reads can see a write before it is durable.** Under `--fsync always` the *writer* is acknowledged only
  after fsync, but another client may read the new value in the window between apply and fsync. Redis
  behaves the same way.
- **Compaction blocks.** `COMPACT` holds every shard lock while it writes the snapshot, so all reads and writes
  pause for its duration. A background rewrite (snapshot plus a buffer of concurrent writes, as Redis does)
  is the obvious next step.
- **The crash test kills the process, not the machine.** The OS page cache survives a process kill, so the test
  checks the log format, torn-tail recovery and reply-after-write ordering, not whether fsync reaches the disk.
  That would need power-loss testing (for example a VM with a disk that drops unsynced writes).
- **No expiry, no eviction, no replication, no auth.** It binds to 127.0.0.1 by default for that reason.
- **Whole log in memory during recovery.** Fine for a log that compaction keeps small; a streaming reader
  would be needed for very large logs.

Development used AI assistance (Claude). The decisions above are written down so they can be checked.
