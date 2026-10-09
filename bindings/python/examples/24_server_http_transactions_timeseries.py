#!/usr/bin/env python3
"""Example 24: Transactions, Database Commands and Time-Series Writes over HTTP.

Three server features the embedded bindings do not wrap, because they belong
to the server's HTTP API, shown against the server that `create_server()`
starts in this process:

- a transaction spanning several requests, through the `arcadedb-session-id`
  header returned by `/api/v1/begin/{db}`, ended by `/commit` or `/rollback`
- server-level database commands (`close database`, `open database`) sent to
  `/api/v1/server`
- writes to a TIMESERIES type through `/api/v1/ts/{db}/write` in InfluxDB line
  protocol, and the same rows read back with SQL

Standard-library HTTP only, like example 23: no client layer is introduced.
"""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import socket
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import arcadedb_embedded as arcadedb

DB = "httpdemo"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--server-root",
        default=None,
        help="server root directory (default: a temp dir)",
    )
    parser.add_argument(
        "--password", default="password123", help="root password for the server"
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=2482,
        help="HTTP port to try first (default: 2482)",
    )
    return parser.parse_args()


def free_port(requested: int) -> int:
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", requested))
            return requested
        except OSError:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])


class Http:
    """A few lines of urllib so the example needs nothing beyond the wheel."""

    def __init__(self, base: str, user: str, password: str) -> None:
        self.base = base
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.auth = f"Basic {token}"

    def call(
        self, method: str, path: str, body=None, headers=None, raw: bytes | None = None
    ):
        data = (
            raw
            if raw is not None
            else (json.dumps(body).encode() if body is not None else None)
        )
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Authorization", self.auth)
        req.add_header(
            "Content-Type", "text/plain" if raw is not None else "application/json"
        )
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(
                req, timeout=60
            ) as resp:  # nosec B310 - local server URL built above
                text = resp.read().decode()
                return (
                    resp.status,
                    dict(resp.headers),
                    (json.loads(text) if text else {}),
                )
        except urllib.error.HTTPError as exc:
            raise SystemExit(
                f"{method} {path} -> HTTP {exc.code}: {exc.read().decode(errors='replace')[:300]}"
            )

    def sql(self, kind: str, command: str, headers=None, language: str = "sql"):
        _, _, payload = self.call(
            "POST",
            f"/api/v1/{kind}/{DB}",
            {"language": language, "command": command},
            headers,
        )
        return payload.get("result", [])


def main() -> None:
    args = parse_args()
    root = (
        Path(args.server_root)
        if args.server_root
        else Path(tempfile.mkdtemp(prefix="arcadedb_ex24_"))
    )
    # A directory the user named is never wiped: refuse one that holds anything, so unrelated data is not lost.
    if args.server_root and root.exists() and any(root.iterdir()):
        raise SystemExit(
            f"--server-root {root} is not empty; pass a new or empty directory"
        )
    if root.exists():
        shutil.rmtree(root)
    port = free_port(args.http_port)
    server = arcadedb.create_server(
        root_path=str(root),
        root_password=args.password,
        config={"host": "127.0.0.1", "http_port": port, "mode": "development"},
    )
    server.start()
    time.sleep(1)
    http = Http(f"http://127.0.0.1:{server.get_http_port()}", "root", args.password)
    try:
        server.create_database(DB)
        print(f"server up on port {server.get_http_port()}, database {DB!r} created")

        # 1. One transaction across several requests.
        http.sql("command", "CREATE DOCUMENT TYPE Person")
        _, headers, _ = http.call("POST", f"/api/v1/begin/{DB}")
        sid = headers.get("arcadedb-session-id")
        tx = {"arcadedb-session-id": sid}
        http.sql("command", "INSERT INTO Person SET name = 'ada'", tx)
        http.sql("command", "INSERT INTO Person SET name = 'grace'", tx)
        before = http.sql("query", "SELECT count(*) AS c FROM Person")[0]["c"]
        http.call("POST", f"/api/v1/commit/{DB}", headers=tx)
        after = http.sql("query", "SELECT count(*) AS c FROM Person")[0]["c"]
        print(f"transaction: visible before commit = {before}, after commit = {after}")

        _, headers, _ = http.call("POST", f"/api/v1/begin/{DB}")
        tx = {"arcadedb-session-id": headers.get("arcadedb-session-id")}
        http.sql("command", "INSERT INTO Person SET name = 'discarded'", tx)
        http.call("POST", f"/api/v1/rollback/{DB}", headers=tx)
        print(
            f"rollback: still {http.sql('query', 'SELECT count(*) AS c FROM Person')[0]['c']} people"
        )

        # 2. Close and reopen the database on the server.
        t0 = time.perf_counter()
        http.call("POST", "/api/v1/server", {"command": f"close database {DB}"})
        t1 = time.perf_counter()
        http.call("POST", "/api/v1/server", {"command": f"open database {DB}"})
        t2 = time.perf_counter()
        print(
            f"close database {(t1 - t0) * 1000:.1f} ms, open database {(t2 - t1) * 1000:.1f} ms, "
            f"rows survive: {http.sql('query', 'SELECT count(*) AS c FROM Person')[0]['c']}"
        )

        # 3. Time-series writes in line protocol, read back with SQL.
        http.sql(
            "command",
            "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)",
        )
        lines = "\n".join(
            f"Reading,sensor=s1 value={i / 10:.1f} {1700000000 + i}"
            for i in range(1000)
        )
        t0 = time.perf_counter()
        http.call("POST", f"/api/v1/ts/{DB}/write?precision=s", raw=lines.encode())
        t1 = time.perf_counter()
        # Seal the samples still in the mutable tail before reading them, rather
        # than waiting for the 60-second background pass (26.10.1,
        # ArcadeData/arcadedb#8574): 0 means everything is sealed.
        left = http.sql("command", "COMPACT TIMESERIES TYPE Reading")[0][
            "mutableSamples"
        ]
        n = http.sql("query", "SELECT count(*) AS n FROM Reading")[0]["n"]
        last = http.sql(
            "query",
            "SELECT ts, value FROM Reading WHERE sensor = 's1' ORDER BY ts DESC LIMIT 1",
        )[0]
        print(
            f"line protocol: 1,000 samples in {(t1 - t0) * 1000:.1f} ms, stored {n}, newest value {last['value']}"
        )
        print(f"compacted: {left} samples left in the mutable tail")
    finally:
        server.stop()
        if not args.server_root:
            shutil.rmtree(root, ignore_errors=True)
    print("done")


if __name__ == "__main__":
    main()
