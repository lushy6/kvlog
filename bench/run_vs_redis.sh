#!/usr/bin/env bash
# Same client, same machine, same settings: kvlog vs Redis in three matching durability modes.
#   bench/run_vs_redis.sh build/kvlog-bench build/kvlog-server [build/kvlog-server-fsync] > results.jsonl
# Needs redis-server on PATH. Each server gets a fresh data directory; nothing is reused between runs.
# With the optional third argument, the `always` mode also runs kvlog built with plain fsync instead of
# fdatasync, in the same rounds, so the two sync calls are compared on the same runner. `always` runs
# ROUNDS times (default 3) because a single CI run is noisy.
set -euo pipefail
BENCH="$1"; SERVER="$2"; SERVER_FSYNC="${3:-}"
CLIENTS="${CLIENTS:-16}"; REQS="${REQS:-20000}"; DURABLE_REQS="${DURABLE_REQS:-2000}"; ROUNDS="${ROUNDS:-3}"
work="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT

wait_port() { for _ in $(seq 1 100); do (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null && return 0; sleep 0.1; done; echo "port $1 never opened" >&2; exit 1; }

run_pair() {  # mode, kvlog args, redis args, requests, set-ratio
  local mode="$1" kv_args="$2" redis_args="$3" reqs="$4" ratio="$5"
  rm -rf "$work/kv" "$work/kvf" "$work/redis"; mkdir -p "$work/kv" "$work/kvf" "$work/redis"
  # shellcheck disable=SC2086
  "$SERVER" --port 6390 --log "$work/kv/bench.aof" $kv_args 2>/dev/null & local kv=$!
  # shellcheck disable=SC2086
  redis-server --port 6391 --dir "$work/redis" --save "" $redis_args --daemonize no >/dev/null & local rd=$!
  local kvf=""
  if [[ -n "$SERVER_FSYNC" && "$mode" == log-always* ]]; then
    # shellcheck disable=SC2086
    "$SERVER_FSYNC" --port 6392 --log "$work/kvf/bench.aof" $kv_args 2>/dev/null & kvf=$!
    wait_port 6392
  fi
  wait_port 6390; wait_port 6391
  for p in 1 16; do
    # A failed run still prints its JSON line (with failures > 0); keep going so the table shows every row.
    "$BENCH" --port 6390 --clients "$CLIENTS" --requests "$reqs" --pipeline "$p" --set-ratio "$ratio" --label "kvlog/$mode" || true
    if [[ -n "$kvf" ]]; then
      "$BENCH" --port 6392 --clients "$CLIENTS" --requests "$reqs" --pipeline "$p" --set-ratio "$ratio" --label "kvlog-fsync/$mode" || true
    fi
    "$BENCH" --port 6391 --clients "$CLIENTS" --requests "$reqs" --pipeline "$p" --set-ratio "$ratio" --label "redis/$mode" || true
  done
  kill "$kv" "$rd" $kvf; wait "$kv" "$rd" $kvf 2>/dev/null || true
}

run_pair "memory"         "--no-log"           "--appendonly no"                          "$REQS" 0.5
run_pair "log-everysec"   "--fsync everysec"   "--appendonly yes --appendfsync everysec"  "$REQS" 0.5
for r in $(seq 1 "$ROUNDS"); do
  run_pair "log-always#$r" "--fsync always"    "--appendonly yes --appendfsync always"    "$DURABLE_REQS" 1.0
done
