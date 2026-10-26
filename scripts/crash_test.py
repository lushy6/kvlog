"""Kill the server mid-write, restart it, and check that every acknowledged write survived.

    python scripts/crash_test.py --server build/kvlog-server [--rounds 20] [--clients 8]

Each round starts the server with --fsync always on the same log, runs writer clients that record every
SET the server acknowledged, kills the process without warning (SIGKILL / TerminateProcess) at a random
moment, restarts it, and reads every acknowledged key back. Writes that were in flight when the process
died may or may not be present; acknowledged ones must be, with the acknowledged value (or a later one
from the same key's sequence, since each round keeps writing the same keys).

What this does NOT test: a power cut or OS crash. Killing the process leaves the OS page cache intact, so
it checks the log format, torn-tail recovery and the reply-after-write ordering, not fsync itself.
Standard library only.
"""

from __future__ import annotations

import argparse
import shutil
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time


def command(*args: str) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


class Conn:
    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""

    def _line(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def call(self, *args: str):
        self.sock.sendall(command(*args))
        line = self._line()
        if line[:1] == b"$":
            n = int(line[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise ConnectionError("closed")
                self.buf += chunk
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data.decode()
        if line[:1] == b"-":
            raise RuntimeError(line.decode())
        return line[1:].decode()


def wait_for_port(port: int, proc: subprocess.Popen, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early: {proc.stderr.read().decode() if proc.stderr else ''}")
        try:
            Conn(port).call("PING")
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("server did not start")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


DATA_LOST = 3  # the only exit code that means "acknowledged writes were lost"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--fsync", default="always")
    ap.add_argument("--unsafe", action="store_true",
                    help="negative control: server replies before writing; this run is expected to LOSE data (exit 3)")
    args = ap.parse_args()
    rng = random.Random(args.seed)

    workdir = tempfile.mkdtemp(prefix="kvlog-crash-")
    server_cmd = lambda port: [args.server, "--port", str(port), "--log", log, "--fsync", args.fsync] + (
        ["--unsafe-reply-before-write"] if args.unsafe else [])
    log = os.path.join(workdir, "crash.aof")
    acked: dict[str, str] = {}      # key -> last acknowledged value
    inflight: dict[str, str] = {}   # key -> value sent but not acknowledged when the server died (at most one per client)
    history: dict[str, list[str]] = {}  # key -> every value ever acknowledged, in order
    lock = threading.Lock()
    total_acked = 0

    for rnd in range(args.rounds):
        port = free_port()
        proc = subprocess.Popen(server_cmd(port),
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        wait_for_port(port, proc)

        # 1) Everything acknowledged in earlier rounds must be readable now. A key may instead hold the write
        #    that was in flight when the server died: it reached the log, only the reply was lost.
        reader = Conn(port)
        missing = []
        for k, v in acked.items():
            got = reader.call("GET", k)
            if got == v:
                continue
            if got is not None and got == inflight.get(k):
                acked[k] = got  # durable after all; it is the new baseline
                continue
            missing.append(k)
        inflight.clear()
        if missing:
            detail = [(k, acked[k], reader.call("GET", k), history[k][-3:]) for k in missing[:3]]
            proc.kill()
            proc.wait()
            print(f"round {rnd}: {len(missing)} acknowledged keys lost or wrong")
            for k, want, got, hist in detail:
                print(f"  {k}: acknowledged {want!r}, server has {got!r}, last acks {hist}")
            print("  server said:", proc.stderr.read().decode().strip() if proc.stderr is not None else "")
            return DATA_LOST

        # 2) Write until killed.
        stop = threading.Event()

        def writer(cid: int) -> None:
            nonlocal total_acked
            try:
                c = Conn(port)
                i = 0
                while not stop.is_set():
                    key = f"c{cid}:k{i % 300}"
                    value = f"r{rnd}:c{cid}:{i}"
                    with lock:
                        inflight[key] = value
                    c.call("SET", key, value)          # returns only after the server replied OK
                    with lock:
                        inflight.pop(key, None)
                        acked[key] = value
                        history.setdefault(key, []).append(value)
                        total_acked += 1
                    i += 1
            except (OSError, ConnectionError):
                pass  # the server was killed mid-request: this write is not acknowledged

        def compactor() -> None:
            # Every third round also rewrites the log continuously, so some kills land mid-COMPACT.
            try:
                c = Conn(port)
                while not stop.is_set():
                    c.call("COMPACT")
                    time.sleep(0.02)
            except (OSError, ConnectionError):
                pass

        threads = [threading.Thread(target=writer, args=(c,), daemon=True) for c in range(args.clients)]
        if rnd % 3 == 2:
            threads.append(threading.Thread(target=compactor, daemon=True))
        for t in threads:
            t.start()
        time.sleep(rng.uniform(0.05, 0.6))
        proc.kill()  # SIGKILL on POSIX, TerminateProcess on Windows: no shutdown code runs
        proc.wait()
        stop.set()
        for t in threads:
            t.join(timeout=5)
        print(f"round {rnd:2d}: killed after {total_acked} acknowledged writes in total, {len(acked)} keys")

    # Final check after the last kill.
    port = free_port()
    proc = subprocess.Popen(server_cmd(port),
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    wait_for_port(port, proc)
    reader = Conn(port)
    lost = []
    for k, v in acked.items():
        got = reader.call("GET", k)
        if got != v and not (got is not None and got == inflight.get(k)):
            lost.append((k, v, got))
    proc.kill()
    proc.wait()
    recovery_line = proc.stderr.read().decode().strip().splitlines()[0] if proc.stderr is not None else ""
    if lost:
        print(f"FAIL: {len(lost)} of {len(acked)} acknowledged keys lost or stale, e.g. {lost[:3]}")
        return DATA_LOST
    print(f"PASS: {args.rounds} kills, {total_acked} acknowledged writes, all {len(acked)} keys intact")
    print(f"last restart: {recovery_line}")
    shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
