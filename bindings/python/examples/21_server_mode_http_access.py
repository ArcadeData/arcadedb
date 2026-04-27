#!/usr/bin/env python3
"""Example 21: Server Mode And HTTP Access.

This example shows the narrow client-server story that already works in the
Python bindings today without adding a new remote client abstraction.

Workflow covered:
- start ArcadeDB server mode from the embedded Python package
- inspect server metadata over HTTP
- create a database through the server-managed Java API
- create schema through the HTTP command endpoint
- insert records through embedded access in the same Python process
- log in to the HTTP API and switch to bearer-token authentication
- query and update the same data over HTTP
- verify HTTP-side changes back through embedded access

Notes:
- This repo remains embedded-first. Server mode is an optional access pattern
  for multi-process or remote HTTP use.
- The example intentionally uses Python's standard library for HTTP instead of
  introducing a dedicated remote client layer.
"""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import socket
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import arcadedb_embedded as arcadedb

ROOT_PASSWORD = "example21pass"
ITEMS = [
    {
        "sku": "SKU-001",
        "name": "Arcade Notebook",
        "category": "office",
        "stock": 12,
        "warehouse": "north",
    },
    {
        "sku": "SKU-002",
        "name": "Graph Whiteboard",
        "category": "office",
        "stock": 4,
        "warehouse": "north",
    },
    {
        "sku": "SKU-003",
        "name": "Vector GPU Crate",
        "category": "hardware",
        "stock": 2,
        "warehouse": "south",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 21: server mode and HTTP access workflow"
    )
    parser.add_argument(
        "--server-root",
        default="./my_test_databases/server_mode_demo_root",
        help=(
            "Server root directory "
            "(default: ./my_test_databases/server_mode_demo_root)"
        ),
    )
    parser.add_argument(
        "--db-name",
        default="example21_server_demo",
        help="Database name to create inside the server root",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="HTTP host to bind the embedded server to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=2481,
        help="HTTP port for the embedded server (default: 2481)",
    )
    parser.add_argument(
        "--wait-for-enter",
        action="store_true",
        help=(
            "Keep the server running after the scripted steps finish until you "
            "press Enter"
        ),
    )
    return parser.parse_args()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def reset_server_root(server_root: Path) -> None:
    if server_root.exists():
        shutil.rmtree(server_root)
    server_root.parent.mkdir(parents=True, exist_ok=True)


def choose_http_port(host: str, requested_port: int) -> tuple[int, bool]:
    """Return a usable HTTP port for the example.

    Tries the requested port first. If it is unavailable, fall back to an
    ephemeral free port chosen by the OS.
    """

    def _can_bind(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                return False
        return True

    if requested_port > 0 and _can_bind(requested_port):
        return requested_port, False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        fallback_port = int(sock.getsockname()[1])

    return fallback_port, True


def basic_auth_header(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def bearer_auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def http_json_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> dict:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {method} {url}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"HTTP request failed for {method} {url}: {exc}") from exc

    if not body.strip():
        return {}

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Expected JSON from {url}, got: {body}") from exc


def wait_for_server(base_url: str, headers: dict[str, str], timeout_sec: float) -> dict:
    start = time.perf_counter()
    while True:
        try:
            return http_json_request(
                f"{base_url}/api/v1/server",
                headers=headers,
                timeout=5.0,
            )
        except RuntimeError:
            if time.perf_counter() - start > timeout_sec:
                raise
            time.sleep(0.25)


def print_rows(title: str, rows: list[dict]) -> None:
    print(title)
    if not rows:
        print("  (no rows)")
        print()
        return
    for row in rows:
        print(f"  - {row}")
    print()


def main() -> int:
    args = parse_args()
    server_root = Path(args.server_root)
    reset_server_root(server_root)

    http_port, used_fallback_port = choose_http_port(args.host, args.http_port)

    base_url = f"http://{args.host}:{http_port}"
    basic_headers = basic_auth_header("root", ROOT_PASSWORD)

    print("=" * 72)
    print("ArcadeDB Python - Example 21: Server Mode And HTTP Access")
    print("=" * 72)
    print()
    print("Positioning: embedded-first package, optional server mode when HTTP helps")
    print(f"Server root: {server_root}")
    print(f"Database name: {args.db_name}")
    print(f"Base URL: {base_url}")
    if used_fallback_port:
        print(
            f"Requested HTTP port {args.http_port} was unavailable; "
            f"using {http_port} instead."
        )
    print()

    with arcadedb.create_server(
        str(server_root),
        root_password=ROOT_PASSWORD,
        config={
            "host": args.host,
            "http_port": http_port,
            "mode": "development",
        },
    ) as server:
        print(f"Studio URL: {server.get_studio_url()}")

        server_info = wait_for_server(base_url, basic_headers, timeout_sec=15.0)
        print_rows(
            "Server metadata:",
            [
                {
                    "version": server_info.get("version"),
                    "serverName": server_info.get("serverName"),
                    "languages": server_info.get("languages"),
                }
            ],
        )

        login_payload = http_json_request(
            f"{base_url}/api/v1/login",
            method="POST",
            headers=basic_headers,
        )
        token = login_payload.get("token")
        require(bool(token), "Expected /api/v1/login to return a bearer token")
        bearer_headers = bearer_auth_header(token)
        print("Acquired bearer token via HTTP login.")
        print()

        db = server.create_database(args.db_name)
        print("Created database through server-managed Java API.")
        print()

        for statement in (
            "CREATE DOCUMENT TYPE InventoryItem",
            "CREATE PROPERTY InventoryItem.sku STRING",
            "CREATE PROPERTY InventoryItem.name STRING",
            "CREATE PROPERTY InventoryItem.category STRING",
            "CREATE PROPERTY InventoryItem.stock INTEGER",
            "CREATE PROPERTY InventoryItem.warehouse STRING",
            "CREATE INDEX ON InventoryItem (sku) UNIQUE_HASH",
        ):
            http_json_request(
                f"{base_url}/api/v1/command/{args.db_name}",
                method="POST",
                payload={"language": "sql", "command": statement},
                headers=bearer_headers,
            )
        print("Created schema through the HTTP command endpoint.")
        print()

        with db.transaction():
            for item in ITEMS:
                db.command(
                    "sql",
                    "INSERT INTO InventoryItem SET sku = ?, name = ?, category = ?, "
                    "stock = ?, warehouse = ?",
                    item["sku"],
                    item["name"],
                    item["category"],
                    item["stock"],
                    item["warehouse"],
                )
        print_rows("Inserted through embedded access:", ITEMS)

        http_list = http_json_request(
            f"{base_url}/api/v1/query/{args.db_name}",
            method="POST",
            payload={
                "language": "sql",
                "command": (
                    "SELECT sku, name, category, stock, warehouse "
                    "FROM InventoryItem ORDER BY sku"
                ),
            },
            headers=bearer_headers,
        )
        remote_rows = http_list.get("result", [])
        require(len(remote_rows) == len(ITEMS), "Expected HTTP query to see all items")
        print_rows("Queried through HTTP:", remote_rows)

        http_json_request(
            f"{base_url}/api/v1/command/{args.db_name}",
            method="POST",
            payload={
                "language": "sql",
                "command": ("UPDATE InventoryItem SET stock = 9 WHERE sku = 'SKU-002'"),
            },
            headers=bearer_headers,
        )
        http_json_request(
            f"{base_url}/api/v1/command/{args.db_name}",
            method="POST",
            payload={
                "language": "sql",
                "command": (
                    "INSERT INTO InventoryItem SET sku = 'SKU-004', "
                    "name = 'Remote Rack', category = 'hardware', "
                    "stock = 7, warehouse = 'west'"
                ),
            },
            headers=bearer_headers,
        )
        print("Applied one HTTP-side update and one HTTP-side insert.")
        print()

        embedded_rows = [
            {
                "sku": row.get("sku"),
                "name": row.get("name"),
                "category": row.get("category"),
                "stock": row.get("stock"),
                "warehouse": row.get("warehouse"),
            }
            for row in db.query(
                "sql",
                "SELECT sku, name, category, stock, warehouse "
                "FROM InventoryItem ORDER BY sku",
            )
        ]
        require(
            [row["sku"] for row in embedded_rows]
            == ["SKU-001", "SKU-002", "SKU-003", "SKU-004"],
            "Expected embedded verification to see all four SKUs",
        )
        require(
            next(row for row in embedded_rows if row["sku"] == "SKU-002")["stock"] == 9,
            "Expected embedded verification to see the HTTP-side stock update",
        )
        print_rows("Verified back through embedded access:", embedded_rows)

        aggregate_payload = http_json_request(
            f"{base_url}/api/v1/query/{args.db_name}",
            method="POST",
            payload={
                "language": "sql",
                "command": (
                    "SELECT category, sum(stock) AS total_stock, count(*) AS items "
                    "FROM InventoryItem GROUP BY category ORDER BY category"
                ),
            },
            headers=bearer_headers,
        )
        aggregate_rows = aggregate_payload.get("result", [])
        require(len(aggregate_rows) == 2, "Expected two category aggregates")
        print_rows("HTTP aggregate query:", aggregate_rows)

        if args.wait_for_enter:
            print(
                "Server is still running. Open Studio in your browser now if you "
                "want to inspect the database."
            )
            print(f"Studio URL: {server.get_studio_url()}")
            input("Press Enter to stop the server and exit the example...")

        db.close()

    print("Example 21 completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
