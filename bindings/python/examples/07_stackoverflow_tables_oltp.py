#!/usr/bin/env python3
"""
Example 07: Stack Overflow Tables (OLTP)

Loads Stack Overflow XML tables and runs a mixed OLTP workload (CRUD).
CRUD operations are point-oriented and randomly target one table per operation.

Threading note:
- Databases can exhibit different scaling behavior as thread count increases.
- For cross-database comparability, it is recommended to run with --threads 1.
"""

import argparse
import concurrent.futures
import csv
import json
import os
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:
    from lxml import etree
except ImportError:
    print("Missing dependency: lxml")
    print("Install with: uv pip install lxml")
    sys.exit(1)


def uv_bootstrap_commands(python_cmd: str) -> List[str]:
    return [
        (
            f'{python_cmd} -c "import json, pathlib, platform, shutil, tarfile, urllib.request; '
            "arch={'x86_64':'x86_64-unknown-linux-gnu','amd64':'x86_64-unknown-linux-gnu','aarch64':'aarch64-unknown-linux-gnu','arm64':'aarch64-unknown-linux-gnu'}[platform.machine().lower()]; "
            "req=urllib.request.Request('https://api.github.com/repos/astral-sh/uv/releases/latest', headers={'User-Agent':'arcadedb-bench'}); "
            "version=json.load(urllib.request.urlopen(req, timeout=30))['tag_name'].lstrip('v'); "
            "archive=pathlib.Path('/tmp/uv.tar.gz'); "
            "archive.write_bytes(urllib.request.urlopen(f'https://github.com/astral-sh/uv/releases/download/{version}/uv-{arch}.tar.gz', timeout=30).read()); "
            "target=pathlib.Path('/tmp/uv-extract'); target.mkdir(parents=True, exist_ok=True); "
            "tarfile.open(archive, 'r:gz').extractall(target); "
            "uv_bin=next(p for p in target.rglob('uv') if p.is_file()); "
            "pathlib.Path('/tmp/uv-bin').mkdir(parents=True, exist_ok=True); "
            "shutil.copy2(uv_bin, '/tmp/uv-bin/uv')\""
        ),
        'export PATH="/tmp/uv-bin:$PATH"',
    ]


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None or value == "":
        return None
    return value.lower() == "true"


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", ""))
    except ValueError:
        return None


def serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.isoformat()
    return value.astimezone(timezone.utc).isoformat()


def to_arcadedb_sql_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return serialize_datetime(value)
    return value


FieldDef = Tuple[str, str, Callable[[Optional[str]], Any]]

TABLE_DEFS: List[Dict[str, Any]] = [
    {
        "name": "User",
        "xml": "Users.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("Reputation", "INTEGER", parse_int),
            ("CreationDate", "DATETIME", parse_datetime),
            ("DisplayName", "STRING", lambda v: v),
            ("LastAccessDate", "DATETIME", parse_datetime),
            ("WebsiteUrl", "STRING", lambda v: v),
            ("Location", "STRING", lambda v: v),
            ("AboutMe", "STRING", lambda v: v),
            ("Views", "INTEGER", parse_int),
            ("UpVotes", "INTEGER", parse_int),
            ("DownVotes", "INTEGER", parse_int),
            ("AccountId", "INTEGER", parse_int),
        ],
    },
    {
        "name": "Post",
        "xml": "Posts.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("PostTypeId", "INTEGER", parse_int),
            ("ParentId", "INTEGER", parse_int),
            ("AcceptedAnswerId", "INTEGER", parse_int),
            ("CreationDate", "DATETIME", parse_datetime),
            ("Score", "INTEGER", parse_int),
            ("ViewCount", "INTEGER", parse_int),
            ("Body", "STRING", lambda v: v),
            ("OwnerUserId", "INTEGER", parse_int),
            ("LastEditorUserId", "INTEGER", parse_int),
            ("LastEditorDisplayName", "STRING", lambda v: v),
            ("LastEditDate", "DATETIME", parse_datetime),
            ("LastActivityDate", "DATETIME", parse_datetime),
            ("Title", "STRING", lambda v: v),
            ("Tags", "STRING", lambda v: v),
            ("AnswerCount", "INTEGER", parse_int),
            ("CommentCount", "INTEGER", parse_int),
            ("FavoriteCount", "INTEGER", parse_int),
            ("ClosedDate", "DATETIME", parse_datetime),
            ("CommunityOwnedDate", "DATETIME", parse_datetime),
        ],
    },
    {
        "name": "Comment",
        "xml": "Comments.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("PostId", "INTEGER", parse_int),
            ("Score", "INTEGER", parse_int),
            ("Text", "STRING", lambda v: v),
            ("CreationDate", "DATETIME", parse_datetime),
            ("UserId", "INTEGER", parse_int),
        ],
    },
    {
        "name": "Badge",
        "xml": "Badges.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("UserId", "INTEGER", parse_int),
            ("Name", "STRING", lambda v: v),
            ("Date", "DATETIME", parse_datetime),
            ("Class", "INTEGER", parse_int),
            ("TagBased", "BOOLEAN", parse_bool),
        ],
    },
    {
        "name": "Vote",
        "xml": "Votes.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("PostId", "INTEGER", parse_int),
            ("VoteTypeId", "INTEGER", parse_int),
            ("CreationDate", "DATETIME", parse_datetime),
            ("UserId", "INTEGER", parse_int),
            ("BountyAmount", "INTEGER", parse_int),
        ],
    },
    {
        "name": "PostLink",
        "xml": "PostLinks.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("CreationDate", "DATETIME", parse_datetime),
            ("PostId", "INTEGER", parse_int),
            ("RelatedPostId", "INTEGER", parse_int),
            ("LinkTypeId", "INTEGER", parse_int),
        ],
    },
    {
        "name": "Tag",
        "xml": "Tags.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("TagName", "STRING", lambda v: v),
            ("Count", "INTEGER", parse_int),
            ("ExcerptPostId", "INTEGER", parse_int),
            ("WikiPostId", "INTEGER", parse_int),
        ],
    },
    {
        "name": "PostHistory",
        "xml": "PostHistory.xml",
        "fields": [
            ("Id", "INTEGER", parse_int),
            ("PostHistoryTypeId", "INTEGER", parse_int),
            ("PostId", "INTEGER", parse_int),
            ("RevisionGUID", "STRING", lambda v: v),
            ("CreationDate", "DATETIME", parse_datetime),
            ("UserId", "INTEGER", parse_int),
            ("UserDisplayName", "STRING", lambda v: v),
            ("Comment", "STRING", lambda v: v),
            ("Text", "STRING", lambda v: v),
            ("CloseReasonId", "INTEGER", parse_int),
        ],
    },
]

EXPECTED_DATASETS = {
    "stackoverflow-tiny",
    "stackoverflow-small",
    "stackoverflow-medium",
    "stackoverflow-large",
    "stackoverflow-xlarge",
    "stackoverflow-full",
}

DEFAULT_OLTP_MIX = {"read": 0.60, "update": 0.20, "insert": 0.10, "delete": 0.10}
SQLITE_PROFILE_CHOICES = ["fair", "perf", "olap"]


def mem_limit_tag(mem_limit: str) -> str:
    normalized = re.sub(r"[^0-9a-z]+", "", mem_limit.lower())
    return f"mem{normalized}" if normalized else "memdefault"


def build_benchmark_db_name(
    dataset: str,
    db: str,
    run_label: Optional[str],
    mem_limit: Optional[str] = None,
) -> str:
    db_name = f"{dataset.replace('-', '_')}_tables_oltp_{db}"
    if mem_limit:
        db_name = f"{db_name}_{mem_limit_tag(mem_limit)}"
    if run_label:
        db_name = f"{db_name}_{run_label}"
    return db_name


def build_table_schema() -> Dict[str, dict]:
    return {
        table["name"]: {
            "columns": [field[0] for field in table["fields"]],
            "column_count": len(table["fields"]),
        }
        for table in TABLE_DEFS
    }


def get_table_def(name: str) -> Dict[str, Any]:
    for table in TABLE_DEFS:
        if table["name"] == name:
            return table
    raise KeyError(name)


def get_numeric_update_column(table: Dict[str, Any]) -> Optional[str]:
    for field_name, field_type, _ in table["fields"]:
        if field_name == "Id":
            continue
        if field_type in ("INTEGER", "BOOLEAN"):
            return field_name
    return None


def count_table_rows_arcadedb(db) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in TABLE_DEFS:
        rows = db.query(
            "sql", f"SELECT count(*) AS count FROM {table['name']}"
        ).to_list()
        counts[table["name"]] = int(rows[0].get("count", 0)) if rows else 0
    return counts


def count_table_rows_sql(conn) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in TABLE_DEFS:
        row = conn.execute(f'SELECT count(*) FROM "{table["name"]}"').fetchone()
        counts[table["name"]] = int(row[0]) if row else 0
    return counts


def quote_ident_pg(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def count_table_rows_postgres(conn) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    with conn.cursor() as cur:
        for table in TABLE_DEFS:
            cur.execute(f"SELECT count(*) FROM {quote_ident_pg(table['name'])}")
            row = cur.fetchone()
            counts[table["name"]] = int(row[0]) if row else 0
    return counts


def get_duckdb_module():
    try:
        import duckdb
    except ImportError:
        return None
    return duckdb


def get_arcadedb_module():
    try:
        import arcadedb_embedded as arcadedb
        from arcadedb_embedded.exceptions import ArcadeDBError
    except ImportError:
        return None, None
    return arcadedb, ArcadeDBError


def get_psycopg_module():
    try:
        import psycopg
    except ImportError:
        return None
    return psycopg


def get_rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
    except FileNotFoundError:
        return 0
    return 0


def read_proc_rss_kb(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return 0
    return 0


def read_postmaster_pid(pgdata: Path) -> Optional[int]:
    try:
        content = (pgdata / "postmaster.pid").read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    if not content:
        return None
    try:
        return int(content[0].strip())
    except ValueError:
        return None


def list_postgres_pids(pgdata: Path) -> List[int]:
    main_pid = read_postmaster_pid(pgdata)
    if not main_pid:
        return []
    pids = [main_pid]
    proc_dir = Path("/proc")
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == main_pid:
            continue
        try:
            stat_parts = (entry / "stat").read_text(encoding="utf-8").split()
        except FileNotFoundError:
            continue
        if len(stat_parts) >= 4:
            try:
                ppid = int(stat_parts[3])
            except ValueError:
                continue
            if ppid == main_pid:
                pids.append(pid)
    return pids


def get_dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = Path(root) / name
            try:
                total += file_path.stat().st_size
            except FileNotFoundError:
                continue
    return total


def find_postgres_bin_dir() -> Optional[Path]:
    candidates = []
    base = Path("/usr/lib/postgresql")
    if base.exists():
        for path in base.glob("*/bin/pg_ctl"):
            candidates.append(path.parent)
    if not candidates:
        return None
    return sorted(candidates, reverse=True)[0]


def format_bytes(value: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PB"


def format_bytes_binary(value: int) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PiB"


def percentiles(values: List[float], points: List[float]) -> Dict[float, float]:
    if not values:
        return {p: 0.0 for p in points}
    values_sorted = sorted(values)
    results = {}
    for p in points:
        k = int(round((p / 100.0) * (len(values_sorted) - 1)))
        results[p] = values_sorted[k]
    return results


def start_rss_sampler(
    interval_sec: float = 0.2,
) -> Tuple[threading.Event, dict, threading.Thread]:
    stop_event = threading.Event()
    state = {"max_kb": 0}

    def run():
        while not stop_event.is_set():
            rss_kb = get_rss_kb()
            if rss_kb > state["max_kb"]:
                state["max_kb"] = rss_kb
            time.sleep(interval_sec)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop_event, state, thread


def start_combined_rss_sampler(
    pid_provider,
    interval_sec: float = 0.2,
) -> Tuple[threading.Event, dict, threading.Thread]:
    stop_event = threading.Event()
    state = {"max_kb": 0, "client_max_kb": 0, "server_max_kb": 0}

    def run():
        while not stop_event.is_set():
            client_kb = get_rss_kb()
            server_kb = 0
            for pid in pid_provider():
                server_kb += read_proc_rss_kb(pid)
            if client_kb > state["client_max_kb"]:
                state["client_max_kb"] = client_kb
            if server_kb > state["server_max_kb"]:
                state["server_max_kb"] = server_kb
            total_kb = client_kb + server_kb
            if total_kb > state["max_kb"]:
                state["max_kb"] = total_kb
            time.sleep(interval_sec)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop_event, state, thread


def ensure_dataset(data_dir: Path):
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Dataset not found: {data_dir}. Run download_data.py first."
        )


def iter_xml_rows(
    xml_path: Path, limit: Optional[int] = None
) -> Iterable[Dict[str, str]]:
    context = etree.iterparse(str(xml_path), events=("end",), tag="row")
    seen = 0
    for _, elem in context:
        yield elem.attrib
        seen += 1
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        if limit is not None and seen >= limit:
            break


def parse_row(attrs: Dict[str, str], table: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    row: Dict[str, Any] = {}
    for field_name, _, parser in table["fields"]:
        row[field_name] = parser(attrs.get(field_name))
    if row.get("Id") is None:
        return None
    return row


def sqlite_type(field_type: str) -> str:
    if field_type == "INTEGER":
        return "INTEGER"
    if field_type == "BOOLEAN":
        return "INTEGER"
    return "TEXT"


def duckdb_type(field_type: str) -> str:
    if field_type == "INTEGER":
        return "INTEGER"
    if field_type == "BOOLEAN":
        return "BOOLEAN"
    if field_type == "DATETIME":
        return "TIMESTAMP"
    return "VARCHAR"


def postgres_type(field_type: str) -> str:
    if field_type == "INTEGER":
        return "INTEGER"
    if field_type == "BOOLEAN":
        return "BOOLEAN"
    if field_type == "DATETIME":
        return "TIMESTAMP"
    return "TEXT"


def create_schema_arcadedb(db):
    for table in TABLE_DEFS:
        db.command("sql", f"CREATE DOCUMENT TYPE {table['name']}")
        for field_name, field_type, _ in table["fields"]:
            arc_type = "STRING" if field_type == "BOOLEAN" else field_type
            db.command(
                "sql", f"CREATE PROPERTY {table['name']}.{field_name} {arc_type}"
            )


def create_arcadedb_id_indexes(db):
    for table in TABLE_DEFS:
        db.command("sql", f"CREATE INDEX ON {table['name']} (Id) UNIQUE_HASH")


def create_schema_sqlite(conn: sqlite3.Connection):
    for table in TABLE_DEFS:
        cols = []
        for field_name, field_type, _ in table["fields"]:
            col_type = sqlite_type(field_type)
            cols.append(f'"{field_name}" {col_type}')
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table["name"]}" ({", ".join(cols)})'
        )


def create_sqlite_id_indexes(conn: sqlite3.Connection):
    for table in TABLE_DEFS:
        conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table["name"].lower()}_id ON "{table["name"]}"("Id")'
        )


def create_schema_duckdb(conn):
    for table in TABLE_DEFS:
        cols = []
        for field_name, field_type, _ in table["fields"]:
            col_type = duckdb_type(field_type)
            cols.append(f'"{field_name}" {col_type}')
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table["name"]}" ({", ".join(cols)})'
        )


def create_duckdb_id_indexes(conn):
    print("Skipping manual DuckDB secondary indexes for this benchmark.")
    return 0.0


def create_schema_postgresql(conn):
    for table in TABLE_DEFS:
        cols = []
        for field_name, field_type, _ in table["fields"]:
            col_type = postgres_type(field_type)
            cols.append(f"{quote_ident_pg(field_name)} {col_type}")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {quote_ident_pg(table['name'])} ({', '.join(cols)})"
        )


def create_postgresql_id_indexes(conn):
    for table in TABLE_DEFS:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            f"idx_{table['name'].lower()}_id ON {quote_ident_pg(table['name'])} ({quote_ident_pg('Id')})"
        )


def sanitize_value(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "DATETIME":
        return serialize_datetime(value) if isinstance(value, datetime) else value
    if field_type == "BOOLEAN":
        return 1 if value else 0
    return value


def insert_batch_arcadedb(db, table: Dict[str, Any], rows: List[Dict[str, Any]]):
    if not rows:
        return
    columns = [f[0] for f in table["fields"]]
    placeholders = ", ".join([f"{col} = ?" for col in columns])
    sql = f"INSERT INTO {table['name']} SET {placeholders}"
    with db.transaction():
        for row in rows:
            args = []
            for field_name, field_type, _ in table["fields"]:
                value = sanitize_value(field_type, row.get(field_name))
                args.append(to_arcadedb_sql_value(value))
            db.command("sql", sql, args)


def configure_arcadedb_async_loader(db, batch_size: int, parallelism: int = 1):
    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, parallelism))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)
    return async_exec


def reset_arcadedb_async_loader(db, async_exec):
    async_exec.wait_completion()
    db.set_read_your_writes(True)
    async_exec.set_transaction_use_wal(True)
    async_exec.close()


def insert_batch_sqlite(conn, table: Dict[str, Any], rows: List[Dict[str, Any]]):
    if not rows:
        return
    columns = [f[0] for f in table["fields"]]
    placeholders = ", ".join(["?"] * len(columns))
    quoted_cols = ", ".join([f'"{c}"' for c in columns])
    stmt = f'INSERT INTO "{table["name"]}" ({quoted_cols}) VALUES ({placeholders})'
    payload = []
    for row in rows:
        payload.append(
            tuple(
                sanitize_value(field_type, row.get(field_name))
                for field_name, field_type, _ in table["fields"]
            )
        )
    conn.executemany(stmt, payload)


def insert_batch_duckdb(conn, table: Dict[str, Any], rows: List[Dict[str, Any]]):
    if not rows:
        return
    columns = [f[0] for f in table["fields"]]
    placeholders = ", ".join(["?"] * len(columns))
    quoted_cols = ", ".join([f'"{c}"' for c in columns])
    stmt = f'INSERT INTO "{table["name"]}" ({quoted_cols}) VALUES ({placeholders})'
    payload = []
    for row in rows:
        payload.append(
            tuple(
                sanitize_value(field_type, row.get(field_name))
                for field_name, field_type, _ in table["fields"]
            )
        )
    conn.executemany(stmt, payload)


def insert_batch_postgresql(conn, table: Dict[str, Any], rows: List[Dict[str, Any]]):
    if not rows:
        return
    columns = [f[0] for f in table["fields"]]
    placeholders = ", ".join(["%s"] * len(columns))
    quoted_cols = ", ".join([quote_ident_pg(c) for c in columns])
    stmt = f"INSERT INTO {quote_ident_pg(table['name'])} ({quoted_cols}) VALUES ({placeholders})"
    payload = []
    for row in rows:
        converted = []
        for field_name, field_type, _ in table["fields"]:
            value = sanitize_value(field_type, row.get(field_name))
            if field_type == "BOOLEAN" and value is not None:
                value = bool(value)
            converted.append(value)
        payload.append(tuple(converted))
    with conn.cursor() as cur:
        cur.executemany(stmt, payload)


def to_duckdb_csv_value(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "DATETIME":
        return serialize_datetime(value) if isinstance(value, datetime) else value
    if field_type == "BOOLEAN":
        return "true" if bool(value) else "false"
    return value


def to_arcadedb_csv_value(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "DATETIME":
        return serialize_datetime(value) if isinstance(value, datetime) else value
    if field_type == "BOOLEAN":
        return "1" if bool(value) else "0"
    return value


def load_tables_arcadedb_async(
    db,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, List[int]], Dict[str, int], float]:
    id_pools: Dict[str, List[int]] = {table["name"]: [] for table in TABLE_DEFS}
    next_ids: Dict[str, int] = {table["name"]: 1 for table in TABLE_DEFS}

    async_exec = configure_arcadedb_async_loader(db, batch_size, parallelism=1)
    errors: List[Exception] = []

    def on_error(exc: Exception):
        errors.append(exc)

    async_exec.on_error(on_error)
    start = time.time()
    try:
        for table in TABLE_DEFS:
            table_name = table["name"]
            xml_path = data_dir / table["xml"]
            if not xml_path.exists():
                raise FileNotFoundError(f"Missing XML file: {xml_path}")

            columns = [field[0] for field in table["fields"]]
            placeholders = ", ".join([f"{col} = ?" for col in columns])
            insert_sql = f"INSERT INTO {table_name} SET {placeholders}"

            max_id = 0
            initial_error_count = len(errors)
            for attrs in iter_xml_rows(xml_path):
                row = parse_row(attrs, table)
                if row is None:
                    continue

                row_id = int(row["Id"])
                id_pools[table_name].append(row_id)
                if row_id > max_id:
                    max_id = row_id

                payload = []
                for field_name, field_type, _ in table["fields"]:
                    value = sanitize_value(field_type, row.get(field_name))
                    payload.append(to_arcadedb_sql_value(value))

                async_exec.command("sql", insert_sql, args=payload)

            async_exec.wait_completion()
            if len(errors) > initial_error_count:
                raise RuntimeError(
                    f"Async preload failed for {table_name} "
                    f"(first error: {errors[initial_error_count]})"
                )

            next_ids[table_name] = max_id + 1

        return id_pools, next_ids, time.time() - start
    finally:
        reset_arcadedb_async_loader(db, async_exec)


def load_tables(
    insert_batch_fn,
    db_obj,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, List[int]], Dict[str, int], float]:
    id_pools: Dict[str, List[int]] = {table["name"]: [] for table in TABLE_DEFS}
    next_ids: Dict[str, int] = {table["name"]: 1 for table in TABLE_DEFS}

    start = time.time()
    for table in TABLE_DEFS:
        table_name = table["name"]
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML file: {xml_path}")

        batch: List[Dict[str, Any]] = []
        max_id = 0
        for attrs in iter_xml_rows(xml_path):
            row = parse_row(attrs, table)
            if row is None:
                continue
            row_id = int(row["Id"])
            id_pools[table_name].append(row_id)
            if row_id > max_id:
                max_id = row_id
            batch.append(row)
            if len(batch) >= batch_size:
                insert_batch_fn(db_obj, table, batch)
                batch = []
        if batch:
            insert_batch_fn(db_obj, table, batch)

        next_ids[table_name] = max_id + 1

    return id_pools, next_ids, time.time() - start


def load_tables_duckdb_copy(
    conn,
    data_dir: Path,
    db_dir: Path,
) -> Tuple[Dict[str, List[int]], Dict[str, int], float]:
    id_pools: Dict[str, List[int]] = {table["name"]: [] for table in TABLE_DEFS}
    next_ids: Dict[str, int] = {table["name"]: 1 for table in TABLE_DEFS}

    csv_dir = db_dir / "duckdb_csv_bulk"
    if csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    start = time.time()
    for table in TABLE_DEFS:
        table_name = table["name"]
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML file: {xml_path}")

        csv_path = csv_dir / f"{table_name}.csv"
        field_names = [field[0] for field in table["fields"]]

        max_id = 0
        row_count = 0
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=field_names)
            writer.writeheader()

            for attrs in iter_xml_rows(xml_path):
                row = parse_row(attrs, table)
                if row is None:
                    continue

                row_id = int(row["Id"])
                id_pools[table_name].append(row_id)
                if row_id > max_id:
                    max_id = row_id

                payload = {}
                for field_name, field_type, _ in table["fields"]:
                    payload[field_name] = to_duckdb_csv_value(
                        field_type,
                        row.get(field_name),
                    )
                writer.writerow(payload)
                row_count += 1

        csv_path_sql = csv_path.as_posix().replace("'", "''")
        print(f"  COPY {table_name}: {row_count:,} rows")
        conn.execute(
            f"COPY \"{table_name}\" FROM '{csv_path_sql}' (AUTO_DETECT TRUE, HEADER TRUE)"
        )

        next_ids[table_name] = max_id + 1

    return id_pools, next_ids, time.time() - start


def to_postgres_csv_value(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "DATETIME":
        return serialize_datetime(value) if isinstance(value, datetime) else value
    if field_type == "BOOLEAN":
        return "true" if bool(value) else "false"
    return value


def load_tables_postgresql_copy(
    conn,
    data_dir: Path,
    db_dir: Path,
) -> Tuple[Dict[str, List[int]], Dict[str, int], float]:
    id_pools: Dict[str, List[int]] = {table["name"]: [] for table in TABLE_DEFS}
    next_ids: Dict[str, int] = {table["name"]: 1 for table in TABLE_DEFS}

    csv_dir = db_dir / "postgres_csv_bulk"
    if csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    start = time.time()
    for table in TABLE_DEFS:
        table_name = table["name"]
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML file: {xml_path}")

        csv_path = csv_dir / f"{table_name}.csv"
        field_names = [field[0] for field in table["fields"]]

        max_id = 0
        row_count = 0
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=field_names)
            writer.writeheader()

            for attrs in iter_xml_rows(xml_path):
                row = parse_row(attrs, table)
                if row is None:
                    continue

                row_id = int(row["Id"])
                id_pools[table_name].append(row_id)
                if row_id > max_id:
                    max_id = row_id

                payload = {}
                for field_name, field_type, _ in table["fields"]:
                    payload[field_name] = to_postgres_csv_value(
                        field_type,
                        row.get(field_name),
                    )
                writer.writerow(payload)
                row_count += 1

        columns_sql = ", ".join(quote_ident_pg(name) for name in field_names)
        copy_sql = (
            f"COPY {quote_ident_pg(table_name)} ({columns_sql}) "
            "FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        )
        print(f"  COPY {table_name}: {row_count:,} rows")
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                with csv_path.open("r", encoding="utf-8") as source:
                    while True:
                        chunk = source.read(1024 * 1024)
                        if not chunk:
                            break
                        copy.write(chunk)

        next_ids[table_name] = max_id + 1

    return id_pools, next_ids, time.time() - start


def build_operation_plan(
    count: int,
    mix: Dict[str, float],
    seed: int,
    table_names: List[str],
) -> List[Tuple[str, str]]:
    rng = random.Random(seed)
    ops = rng.choices(
        population=["read", "update", "insert", "delete"],
        weights=[mix["read"], mix["update"], mix["insert"], mix["delete"]],
        k=count,
    )
    return [(op, rng.choice(table_names)) for op in ops]


def simulate_expected_counts_single_thread(
    preload_counts: Dict[str, int],
    op_plan: List[Tuple[str, str]],
) -> Dict[str, int]:
    counts = {name: int(value) for name, value in preload_counts.items()}

    for op, table_name in op_plan:
        if op == "insert":
            counts[table_name] = counts.get(table_name, 0) + 1
        elif op == "delete":
            current = counts.get(table_name, 0)
            if current > 0:
                counts[table_name] = current - 1

    return counts


def assert_counts_match(
    expected: Dict[str, int], actual: Dict[str, int], db_label: str
):
    diffs = []
    all_tables = sorted(set(expected.keys()) | set(actual.keys()))
    for table_name in all_tables:
        exp = int(expected.get(table_name, 0))
        got = int(actual.get(table_name, 0))
        if exp != got:
            diffs.append(f"{table_name}: expected={exp}, actual={got}")

    if diffs:
        raise RuntimeError(
            f"Deterministic single-thread CRUD verification failed for {db_label}:\n- "
            + "\n- ".join(diffs)
        )


def run_with_retry(
    action,
    error_class,
    max_retries: int = 100,
    base_delay: float = 0.01,
    max_delay: float = 0.5,
):
    for attempt in range(max_retries):
        try:
            return action()
        except error_class as exc:
            message = str(exc)
            if "ConcurrentModificationException" not in message:
                raise
            if attempt == max_retries - 1:
                raise
            delay = min(max_delay, base_delay * (2**attempt))
            delay *= 1.0 + (random.random() * 0.1)
            time.sleep(delay)


def get_read_projection(table: Dict[str, Any], rng: random.Random) -> str:
    cols = [f[0] for f in table["fields"] if f[0] != "Id"]
    if not cols:
        return "Id"
    picks = ["Id"] + cols[:2]
    if rng.random() > 0.5 and len(cols) > 2:
        picks = ["Id", cols[0], cols[2]]
    return ", ".join(picks)


def build_synthetic_row(
    table: Dict[str, Any], new_id: int, rng: random.Random
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    row: Dict[str, Any] = {}
    for field_name, field_type, _ in table["fields"]:
        if field_name == "Id":
            row[field_name] = new_id
        elif field_type == "INTEGER":
            row[field_name] = rng.randint(1, 1000)
        elif field_type == "BOOLEAN":
            row[field_name] = bool(rng.randint(0, 1))
        elif field_type == "DATETIME":
            row[field_name] = now
        else:
            row[field_name] = f"synthetic_{table['name']}_{field_name}_{new_id}"
    return row


def build_table_meta() -> Dict[str, Dict[str, Any]]:
    meta: Dict[str, Dict[str, Any]] = {}
    for table in TABLE_DEFS:
        update_col = get_numeric_update_column(table)
        meta[table["name"]] = {
            "table": table,
            "update_col": update_col,
        }
    return meta


def run_oltp_arcadedb(
    db_path: Path,
    data_dir: Path,
    transactions: int,
    batch_size: int,
    threads: int,
    seed: int,
    jvm_kwargs: dict,
    verify_single_thread_series: bool = False,
) -> dict:
    arcadedb, arcade_error = get_arcadedb_module()
    if arcadedb is None or arcade_error is None:
        raise RuntimeError("arcadedb-embedded is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)

    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)
    create_schema_arcadedb(db)

    ingest_started_at = datetime.now(timezone.utc).isoformat()
    print(f"Ingest start (arcadedb, UTC): {ingest_started_at}")
    id_pools, next_ids, preload_time = load_tables_arcadedb_async(
        db=db,
        data_dir=data_dir,
        batch_size=batch_size,
    )
    ingest_ended_at = datetime.now(timezone.utc).isoformat()
    print(
        f"Ingest end   (arcadedb, UTC): {ingest_ended_at} "
        f"(elapsed={preload_time:.2f}s)"
    )

    index_start = time.time()
    create_arcadedb_id_indexes(db)
    index_time = time.time() - index_start

    load_counts_start = time.time()
    preload_counts = count_table_rows_arcadedb(db)
    load_counts_time = time.time() - load_counts_start

    disk_after_preload = get_dir_size_bytes(db_path)
    table_meta = build_table_meta()
    table_names = [table["name"] for table in TABLE_DEFS]
    id_lock = threading.Lock()

    def worker(
        op_plan_chunk: List[Tuple[str, str]], worker_id: int
    ) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}

        for op, table_name in op_plan_chunk:
            start = time.perf_counter()
            meta = table_meta[table_name]
            table = meta["table"]
            update_col = meta["update_col"]

            if op == "read":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:
                    projection = get_read_projection(table, rng)
                    _ = list(
                        db.query(
                            "sql",
                            f"SELECT {projection} FROM {table_name} WHERE Id = {target_id}",
                        )
                    )
            elif op == "update":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None and update_col is not None:

                    def do_update():
                        with db.transaction():
                            db.command(
                                "sql",
                                f"UPDATE {table_name} SET {update_col} = coalesce({update_col}, 0) + 1 WHERE Id = {target_id}",
                            )

                    run_with_retry(do_update, arcade_error)
            elif op == "insert":
                with id_lock:
                    new_id = next_ids[table_name]
                    next_ids[table_name] += 1
                row = build_synthetic_row(table, new_id, rng)

                def do_insert():
                    insert_batch_arcadedb(db, table, [row])

                run_with_retry(do_insert, arcade_error)
                with id_lock:
                    id_pools[table_name].append(new_id)
            elif op == "delete":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:

                    def do_delete():
                        with db.transaction():
                            db.command(
                                "sql",
                                f"DELETE FROM {table_name} WHERE Id = {target_id}",
                            )

                    run_with_retry(do_delete, arcade_error)
                    with id_lock:
                        try:
                            id_pools[table_name].remove(target_id)
                        except ValueError:
                            pass

            latencies[op].append(time.perf_counter() - start)

        return latencies

    op_plan = build_operation_plan(transactions, DEFAULT_OLTP_MIX, seed, table_names)
    expected_counts = None
    if verify_single_thread_series:
        expected_counts = simulate_expected_counts_single_thread(
            preload_counts,
            op_plan,
        )
    chunks = [op_plan[i::threads] for i in range(threads)]

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results = {"read": [], "update": [], "insert": [], "delete": []}
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(worker, chunk, i) for i, chunk in enumerate(chunks)]
        for future in concurrent.futures.as_completed(futures):
            thread_latencies = future.result()
            for op_name, vals in thread_latencies.items():
                results[op_name].extend(vals)

    total_time = time.perf_counter() - start_time
    stop_event.set()
    rss_thread.join()

    disk_after_oltp = get_dir_size_bytes(db_path)

    counts_start = time.time()
    final_counts = count_table_rows_arcadedb(db)
    counts_time = time.time() - counts_start

    if verify_single_thread_series and expected_counts is not None:
        assert_counts_match(expected_counts, final_counts, "arcadedb")

    db.close()

    total_ops = sum(len(v) for v in results.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "ingest_mode": "bulk_tuned_insert",
        "preload_time_s": preload_time,
        "index_time_s": index_time,
        "load_counts_time_s": load_counts_time,
        "preload_counts": preload_counts,
        "counts_time_s": counts_time,
        "final_counts": final_counts,
        "disk_after_preload_bytes": disk_after_preload,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
    }


def configure_sqlite(conn: sqlite3.Connection, profile: str) -> Dict[str, Any]:
    profile = (profile or "perf").lower()
    if profile not in SQLITE_PROFILE_CHOICES:
        raise ValueError(
            f"Unsupported sqlite profile: {profile}. "
            f"Expected one of: {', '.join(SQLITE_PROFILE_CHOICES)}"
        )

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA temp_store=MEMORY")

    if profile == "fair":
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
    elif profile == "perf":
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
    else:
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute("PRAGMA mmap_size=268435456")

    return {
        "profile": profile,
        "journal_mode": "WAL",
        "synchronous": "FULL" if profile == "fair" else "NORMAL",
        "foreign_keys": "ON",
        "temp_store": "MEMORY",
        "busy_timeout_ms": 30000,
        "cache_size_kib": 200000 if profile == "olap" else None,
        "mmap_size_bytes": 268435456 if profile == "olap" else None,
    }


def run_oltp_sqlite(
    db_dir: Path,
    data_dir: Path,
    transactions: int,
    batch_size: int,
    threads: int,
    seed: int,
    sqlite_profile: str = "perf",
    verify_single_thread_series: bool = False,
) -> dict:
    if db_dir.exists():
        shutil.rmtree(db_dir)
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / "sqlite.db"

    conn = sqlite3.connect(db_file, isolation_level=None)
    sqlite_pragmas = configure_sqlite(conn, sqlite_profile)
    create_schema_sqlite(conn)

    # SQLite note: we intentionally keep preload as transaction-batched
    # executemany inserts for portability and stable semantics in this Python
    # benchmark. The sqlite shell's .import path is CLI-specific and would add
    # extra external tooling complexity here.
    ingest_started_at = datetime.now(timezone.utc).isoformat()
    print(f"Ingest start (sqlite, UTC): {ingest_started_at}")
    id_pools, next_ids, preload_time = load_tables(
        insert_batch_fn=insert_batch_sqlite,
        db_obj=conn,
        data_dir=data_dir,
        batch_size=batch_size,
    )
    ingest_ended_at = datetime.now(timezone.utc).isoformat()
    print(
        f"Ingest end   (sqlite, UTC): {ingest_ended_at} "
        f"(elapsed={preload_time:.2f}s)"
    )
    index_start = time.time()
    create_sqlite_id_indexes(conn)
    index_time = time.time() - index_start

    load_counts_start = time.time()
    preload_counts = count_table_rows_sql(conn)
    load_counts_time = time.time() - load_counts_start

    disk_after_preload = get_dir_size_bytes(db_dir)
    table_meta = build_table_meta()
    table_names = [table["name"] for table in TABLE_DEFS]
    id_lock = threading.Lock()

    def worker(
        op_plan_chunk: List[Tuple[str, str]], worker_id: int
    ) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        local_conn = sqlite3.connect(
            db_file, isolation_level=None, check_same_thread=False, timeout=5.0
        )
        configure_sqlite(local_conn, sqlite_profile)

        for op, table_name in op_plan_chunk:
            start = time.perf_counter()
            meta = table_meta[table_name]
            table = meta["table"]
            update_col = meta["update_col"]

            if op == "read":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:
                    projection = get_read_projection(table, rng)
                    local_conn.execute(
                        f'SELECT {projection} FROM "{table_name}" WHERE "Id" = ?',
                        (target_id,),
                    ).fetchone()
            elif op == "update":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None and update_col is not None:
                    local_conn.execute("BEGIN")
                    local_conn.execute(
                        f'UPDATE "{table_name}" SET "{update_col}" = coalesce("{update_col}", 0) + 1 WHERE "Id" = ?',
                        (target_id,),
                    )
                    local_conn.execute("COMMIT")
            elif op == "insert":
                with id_lock:
                    new_id = next_ids[table_name]
                    next_ids[table_name] += 1
                row = build_synthetic_row(table, new_id, rng)
                local_conn.execute("BEGIN")
                insert_batch_sqlite(local_conn, table, [row])
                local_conn.execute("COMMIT")
                with id_lock:
                    id_pools[table_name].append(new_id)
            elif op == "delete":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:
                    local_conn.execute("BEGIN")
                    local_conn.execute(
                        f'DELETE FROM "{table_name}" WHERE "Id" = ?',
                        (target_id,),
                    )
                    local_conn.execute("COMMIT")
                    with id_lock:
                        try:
                            id_pools[table_name].remove(target_id)
                        except ValueError:
                            pass

            latencies[op].append(time.perf_counter() - start)

        local_conn.close()
        return latencies

    op_plan = build_operation_plan(transactions, DEFAULT_OLTP_MIX, seed, table_names)
    expected_counts = None
    if verify_single_thread_series:
        expected_counts = simulate_expected_counts_single_thread(
            preload_counts,
            op_plan,
        )
    chunks = [op_plan[i::threads] for i in range(threads)]

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results = {"read": [], "update": [], "insert": [], "delete": []}
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(worker, chunk, i) for i, chunk in enumerate(chunks)]
        for future in concurrent.futures.as_completed(futures):
            thread_latencies = future.result()
            for op_name, vals in thread_latencies.items():
                results[op_name].extend(vals)

    total_time = time.perf_counter() - start_time
    stop_event.set()
    rss_thread.join()

    disk_after_oltp = get_dir_size_bytes(db_dir)

    counts_start = time.time()
    final_counts = count_table_rows_sql(conn)
    counts_time = time.time() - counts_start

    if verify_single_thread_series and expected_counts is not None:
        assert_counts_match(expected_counts, final_counts, "sqlite")

    conn.close()

    total_ops = sum(len(v) for v in results.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "preload_time_s": preload_time,
        "index_time_s": index_time,
        "load_counts_time_s": load_counts_time,
        "preload_counts": preload_counts,
        "counts_time_s": counts_time,
        "final_counts": final_counts,
        "disk_after_preload_bytes": disk_after_preload,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": sqlite_pragmas,
    }


def configure_duckdb(conn):
    pass


def run_oltp_duckdb(
    db_dir: Path,
    data_dir: Path,
    transactions: int,
    batch_size: int,
    threads: int,
    seed: int,
    verify_single_thread_series: bool = False,
) -> dict:
    duckdb = get_duckdb_module()
    if duckdb is None:
        raise RuntimeError("duckdb is not installed")

    if db_dir.exists():
        shutil.rmtree(db_dir)
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / "duckdb.db"

    conn = duckdb.connect(str(db_file))
    configure_duckdb(conn)
    create_schema_duckdb(conn)

    ingest_started_at = datetime.now(timezone.utc).isoformat()
    print(f"Ingest start (duckdb, UTC): {ingest_started_at}")
    id_pools, next_ids, preload_time = load_tables_duckdb_copy(
        conn=conn,
        data_dir=data_dir,
        db_dir=db_dir,
    )
    ingest_ended_at = datetime.now(timezone.utc).isoformat()
    print(
        f"Ingest end   (duckdb, UTC): {ingest_ended_at} "
        f"(elapsed={preload_time:.2f}s)"
    )
    index_time = create_duckdb_id_indexes(conn)

    load_counts_start = time.time()
    preload_counts = count_table_rows_sql(conn)
    load_counts_time = time.time() - load_counts_start

    disk_after_preload = get_dir_size_bytes(db_dir)
    table_meta = build_table_meta()
    table_names = [table["name"] for table in TABLE_DEFS]
    id_lock = threading.Lock()

    def worker(
        op_plan_chunk: List[Tuple[str, str]], worker_id: int
    ) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        local_conn = duckdb.connect(str(db_file))
        configure_duckdb(local_conn)

        for op, table_name in op_plan_chunk:
            start = time.perf_counter()
            meta = table_meta[table_name]
            table = meta["table"]
            update_col = meta["update_col"]

            if op == "read":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:
                    projection = get_read_projection(table, rng)
                    local_conn.execute(
                        f'SELECT {projection} FROM "{table_name}" WHERE "Id" = ?',
                        (target_id,),
                    ).fetchone()
            elif op == "update":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None and update_col is not None:
                    local_conn.execute("BEGIN TRANSACTION")
                    local_conn.execute(
                        f'UPDATE "{table_name}" SET "{update_col}" = coalesce("{update_col}", 0) + 1 WHERE "Id" = ?',
                        (target_id,),
                    )
                    local_conn.execute("COMMIT")
            elif op == "insert":
                with id_lock:
                    new_id = next_ids[table_name]
                    next_ids[table_name] += 1
                row = build_synthetic_row(table, new_id, rng)
                local_conn.execute("BEGIN TRANSACTION")
                insert_batch_duckdb(local_conn, table, [row])
                local_conn.execute("COMMIT")
                with id_lock:
                    id_pools[table_name].append(new_id)
            elif op == "delete":
                with id_lock:
                    target_id = (
                        rng.choice(id_pools[table_name])
                        if id_pools[table_name]
                        else None
                    )
                if target_id is not None:
                    local_conn.execute("BEGIN TRANSACTION")
                    local_conn.execute(
                        f'DELETE FROM "{table_name}" WHERE "Id" = ?',
                        (target_id,),
                    )
                    local_conn.execute("COMMIT")
                    with id_lock:
                        try:
                            id_pools[table_name].remove(target_id)
                        except ValueError:
                            pass

            latencies[op].append(time.perf_counter() - start)

        local_conn.close()
        return latencies

    op_plan = build_operation_plan(transactions, DEFAULT_OLTP_MIX, seed, table_names)
    expected_counts = None
    if verify_single_thread_series:
        expected_counts = simulate_expected_counts_single_thread(
            preload_counts,
            op_plan,
        )
    chunks = [op_plan[i::threads] for i in range(threads)]

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results = {"read": [], "update": [], "insert": [], "delete": []}
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(worker, chunk, i) for i, chunk in enumerate(chunks)]
        for future in concurrent.futures.as_completed(futures):
            thread_latencies = future.result()
            for op_name, vals in thread_latencies.items():
                results[op_name].extend(vals)

    total_time = time.perf_counter() - start_time
    stop_event.set()
    rss_thread.join()

    disk_after_oltp = get_dir_size_bytes(db_dir)

    counts_start = time.time()
    final_counts = count_table_rows_sql(conn)
    counts_time = time.time() - counts_start

    if verify_single_thread_series and expected_counts is not None:
        assert_counts_match(expected_counts, final_counts, "duckdb")

    conn.close()

    total_ops = sum(len(v) for v in results.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "ingest_mode": "sql",
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "preload_time_s": preload_time,
        "index_time_s": index_time,
        "load_counts_time_s": load_counts_time,
        "preload_counts": preload_counts,
        "counts_time_s": counts_time,
        "final_counts": final_counts,
        "disk_after_preload_bytes": disk_after_preload,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
    }


def run_oltp_postgresql(
    db_dir: Path,
    data_dir: Path,
    transactions: int,
    batch_size: int,
    threads: int,
    seed: int,
    verify_single_thread_series: bool = False,
) -> dict:
    psycopg = get_psycopg_module()
    if psycopg is None:
        raise RuntimeError("psycopg is not installed")

    deadlock_error = psycopg.errors.DeadlockDetected
    serialization_error = psycopg.errors.SerializationFailure

    pg_bin = find_postgres_bin_dir()
    if pg_bin is None:
        raise RuntimeError("PostgreSQL binaries not found")

    initdb = pg_bin / "initdb"
    pg_ctl = pg_bin / "pg_ctl"

    if db_dir.exists():
        shutil.rmtree(db_dir)
    db_dir.mkdir(parents=True, exist_ok=True)

    pgdata = db_dir / "pgdata"
    pgdata.mkdir(parents=True, exist_ok=True)

    subprocess.run(["chown", "-R", "postgres:postgres", str(pgdata)], check=True)
    subprocess.run(
        [
            "runuser",
            "-u",
            "postgres",
            "--",
            str(initdb),
            "-D",
            str(pgdata),
            "-U",
            "postgres",
            "--auth=trust",
        ],
        check=True,
    )

    subprocess.run(
        [
            "runuser",
            "-u",
            "postgres",
            "--",
            str(pg_ctl),
            "-D",
            str(pgdata),
            "-o",
            "-c listen_addresses='127.0.0.1' -p 5432",
            "-w",
            "start",
        ],
        check=True,
    )

    postgres_version = None
    try:
        admin_conn = psycopg.connect("dbname=postgres user=postgres host=127.0.0.1")
        admin_conn.autocommit = True
        admin_conn.execute("DROP DATABASE IF EXISTS bench")
        admin_conn.execute("CREATE DATABASE bench")
        admin_conn.close()

        version_conn = psycopg.connect("dbname=postgres user=postgres host=127.0.0.1")
        postgres_version = version_conn.execute("SELECT version()").fetchone()[0]
        version_conn.close()

        conn = psycopg.connect("dbname=bench user=postgres host=127.0.0.1")
        create_schema_postgresql(conn)
        conn.commit()

        ingest_started_at = datetime.now(timezone.utc).isoformat()
        print(f"Ingest start (postgresql, UTC): {ingest_started_at}")
        id_pools, next_ids, preload_time = load_tables_postgresql_copy(
            conn=conn,
            data_dir=data_dir,
            db_dir=db_dir,
        )
        ingest_ended_at = datetime.now(timezone.utc).isoformat()
        print(
            f"Ingest end   (postgresql, UTC): {ingest_ended_at} "
            f"(elapsed={preload_time:.2f}s)"
        )
        index_start = time.time()
        create_postgresql_id_indexes(conn)
        conn.commit()
        index_time = time.time() - index_start

        load_counts_start = time.time()
        preload_counts = count_table_rows_postgres(conn)
        load_counts_time = time.time() - load_counts_start

        disk_after_preload = get_dir_size_bytes(db_dir)
        table_meta = build_table_meta()
        table_names = [table["name"] for table in TABLE_DEFS]
        id_lock = threading.Lock()

        def run_pg_with_retry(action, max_retries: int = 20):
            for attempt in range(max_retries):
                try:
                    return action()
                except (deadlock_error, serialization_error):
                    if attempt == max_retries - 1:
                        raise
                    delay = min(0.5, 0.01 * (2**attempt))
                    delay *= 1.0 + (random.random() * 0.1)
                    time.sleep(delay)

        def worker(
            op_plan_chunk: List[Tuple[str, str]], worker_id: int
        ) -> Dict[str, List[float]]:
            rng = random.Random(seed + worker_id)
            latencies = {"read": [], "update": [], "insert": [], "delete": []}
            local_conn = psycopg.connect("dbname=bench user=postgres host=127.0.0.1")
            local_conn.autocommit = True

            for op, table_name in op_plan_chunk:
                start_time = time.perf_counter()
                meta = table_meta[table_name]
                table = meta["table"]
                update_col = meta["update_col"]

                if op == "read":
                    with id_lock:
                        target_id = (
                            rng.choice(id_pools[table_name])
                            if id_pools[table_name]
                            else None
                        )
                    if target_id is not None:
                        projection = get_read_projection(table, rng)
                        projection_sql = ", ".join(
                            quote_ident_pg(col.strip())
                            for col in projection.split(",")
                            if col.strip()
                        )
                        local_conn.execute(
                            f"SELECT {projection_sql} FROM {quote_ident_pg(table_name)} WHERE {quote_ident_pg('Id')} = %s",
                            (target_id,),
                        ).fetchone()
                elif op == "update":
                    with id_lock:
                        target_id = (
                            rng.choice(id_pools[table_name])
                            if id_pools[table_name]
                            else None
                        )
                    if target_id is not None and update_col is not None:

                        def do_update():
                            with local_conn.transaction():
                                local_conn.execute(
                                    f"UPDATE {quote_ident_pg(table_name)} SET {quote_ident_pg(update_col)} = coalesce({quote_ident_pg(update_col)}, 0) + 1 WHERE {quote_ident_pg('Id')} = %s",
                                    (target_id,),
                                )

                        run_pg_with_retry(do_update)
                elif op == "insert":
                    with id_lock:
                        new_id = next_ids[table_name]
                        next_ids[table_name] += 1
                    row = build_synthetic_row(table, new_id, rng)

                    def do_insert():
                        with local_conn.transaction():
                            insert_batch_postgresql(local_conn, table, [row])

                    run_pg_with_retry(do_insert)
                    with id_lock:
                        id_pools[table_name].append(new_id)
                elif op == "delete":
                    with id_lock:
                        target_id = (
                            rng.choice(id_pools[table_name])
                            if id_pools[table_name]
                            else None
                        )
                    if target_id is not None:

                        def do_delete():
                            with local_conn.transaction():
                                local_conn.execute(
                                    f"DELETE FROM {quote_ident_pg(table_name)} WHERE {quote_ident_pg('Id')} = %s",
                                    (target_id,),
                                )

                        run_pg_with_retry(do_delete)
                        with id_lock:
                            try:
                                id_pools[table_name].remove(target_id)
                            except ValueError:
                                pass

                latencies[op].append(time.perf_counter() - start_time)

            local_conn.close()
            return latencies

        op_plan = build_operation_plan(
            transactions, DEFAULT_OLTP_MIX, seed, table_names
        )
        expected_counts = None
        if verify_single_thread_series:
            expected_counts = simulate_expected_counts_single_thread(
                preload_counts,
                op_plan,
            )
        chunks = [op_plan[i::threads] for i in range(threads)]

        def pid_provider() -> List[int]:
            return list_postgres_pids(pgdata)

        stop_event, rss_state, rss_thread = start_combined_rss_sampler(pid_provider)
        start_time = time.perf_counter()

        results = {"read": [], "update": [], "insert": [], "delete": []}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, threads)
        ) as executor:
            futures = [
                executor.submit(worker, chunk, i) for i, chunk in enumerate(chunks)
            ]
            for future in concurrent.futures.as_completed(futures):
                thread_latencies = future.result()
                for op_name, vals in thread_latencies.items():
                    results[op_name].extend(vals)

        total_time = time.perf_counter() - start_time
        stop_event.set()
        rss_thread.join()

        disk_after_oltp = get_dir_size_bytes(db_dir)

        counts_start = time.time()
        final_counts = count_table_rows_postgres(conn)
        counts_time = time.time() - counts_start

        if verify_single_thread_series and expected_counts is not None:
            assert_counts_match(expected_counts, final_counts, "postgresql")

        conn.close()

        total_ops = sum(len(v) for v in results.values())
        throughput = total_ops / total_time if total_time > 0 else 0

        return {
            "ingest_mode": "copy",
            "total_ops": total_ops,
            "total_time_s": total_time,
            "throughput_ops_s": throughput,
            "preload_time_s": preload_time,
            "index_time_s": index_time,
            "load_counts_time_s": load_counts_time,
            "preload_counts": preload_counts,
            "counts_time_s": counts_time,
            "final_counts": final_counts,
            "disk_after_preload_bytes": disk_after_preload,
            "disk_after_oltp_bytes": disk_after_oltp,
            "rss_peak_kb": rss_state["max_kb"],
            "rss_client_peak_kb": rss_state["client_max_kb"],
            "rss_server_peak_kb": rss_state["server_max_kb"],
            "latencies": results,
            "postgres_version": postgres_version,
        }
    finally:
        subprocess.run(
            [
                "runuser",
                "-u",
                "postgres",
                "--",
                str(pg_ctl),
                "-D",
                str(pgdata),
                "-m",
                "fast",
                "stop",
            ],
            check=False,
        )


def print_latency_summary(latencies: Dict[str, List[float]]):
    points = [50, 95, 99]
    for op_name in ["read", "update", "insert", "delete"]:
        values = latencies.get(op_name, [])
        if not values:
            continue
        stats = percentiles(values, points)
        print(
            f"{op_name:>6}  p50={stats[50]*1000:.2f}ms "
            f"p95={stats[95]*1000:.2f}ms p99={stats[99]*1000:.2f}ms "
            f"(n={len(values):,})"
        )


def build_latency_summary(latencies: Dict[str, List[float]]) -> dict:
    points = [50, 95, 99]
    summary = {"ops": {}, "overall": None}

    for op_name in ["read", "update", "insert", "delete"]:
        values = latencies.get(op_name, [])
        if not values:
            continue
        stats = percentiles(values, points)
        summary["ops"][op_name] = {
            "count": len(values),
            "p50_ms": stats[50] * 1000,
            "p95_ms": stats[95] * 1000,
            "p99_ms": stats[99] * 1000,
        }

    all_values = [v for vals in latencies.values() for v in vals]
    if all_values:
        stats = percentiles(all_values, points)
        summary["overall"] = {
            "count": len(all_values),
            "p50_ms": stats[50] * 1000,
            "p95_ms": stats[95] * 1000,
            "p99_ms": stats[99] * 1000,
        }

    return summary


def write_results(db_path: Path, args: argparse.Namespace, summary: dict):
    results_path = db_path / (
        f"results_{args.run_label}.json" if args.run_label else "results.json"
    )
    duckdb_module = get_duckdb_module()
    duckdb_version = duckdb_module.__version__ if duckdb_module is not None else None
    arcadedb_module, _ = get_arcadedb_module()
    arcadedb_version = (
        getattr(arcadedb_module, "__version__", None)
        if arcadedb_module is not None
        else None
    )

    payload = {
        "dataset": args.dataset,
        "db": args.db,
        "ingest_mode": summary.get("ingest_mode"),
        "threads": args.threads,
        "transactions": args.transactions,
        "batch_size": args.batch_size,
        "mem_limit": args.mem_limit,
        "heap_size": args.heap_size_effective,
        "arcadedb_version": arcadedb_version,
        "duckdb_version": duckdb_version,
        "docker_image": args.docker_image,
        "sqlite_version": sqlite3.sqlite_version,
        "sqlite_profile": summary.get("sqlite_profile"),
        "sqlite_pragmas": summary.get("sqlite_pragmas"),
        "duckdb_runtime_version": duckdb_version,
        "postgres_version": summary.get("postgres_version"),
        "seed": args.seed,
        "run_label": args.run_label,
        "table_schema": build_table_schema(),
        "throughput_ops_s": summary["throughput_ops_s"],
        "total_time_s": summary["total_time_s"],
        "preload_time_s": summary["preload_time_s"],
        "index_time_s": summary.get("index_time_s", 0.0),
        "load_counts_time_s": summary.get("load_counts_time_s", 0.0),
        "preload_counts": summary.get("preload_counts"),
        "counts_time_s": summary.get("counts_time_s", 0.0),
        "final_counts": summary.get("final_counts"),
        "disk_after_preload_bytes": summary["disk_after_preload_bytes"],
        "disk_after_preload_human": format_bytes_binary(
            summary["disk_after_preload_bytes"]
        ),
        "disk_after_oltp_bytes": summary["disk_after_oltp_bytes"],
        "disk_after_oltp_human": format_bytes_binary(summary["disk_after_oltp_bytes"]),
        "rss_peak_kb": summary["rss_peak_kb"],
        "rss_peak_human": format_bytes_binary(summary["rss_peak_kb"] * 1024),
        "rss_client_peak_kb": summary.get("rss_client_peak_kb", summary["rss_peak_kb"]),
        "rss_client_peak_human": format_bytes_binary(
            summary.get("rss_client_peak_kb", summary["rss_peak_kb"]) * 1024
        ),
        "rss_server_peak_kb": summary.get("rss_server_peak_kb", 0),
        "rss_server_peak_human": format_bytes_binary(
            summary.get("rss_server_peak_kb", 0) * 1024
        ),
        "latency_summary": build_latency_summary(summary["latencies"]),
        "run_status": summary.get("run_status", "success"),
        "error_type": summary.get("error_type"),
        "error_message": summary.get("error_message"),
    }
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with open(results_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    print(f"Results saved to: {results_path}")


def is_running_in_docker() -> bool:
    if Path("/.dockerenv").exists():
        return True
    try:
        with open("/proc/1/cgroup", "r", encoding="utf-8") as handle:
            content = handle.read()
        return "docker" in content or "containerd" in content
    except FileNotFoundError:
        return False


SIZE_TOKEN_RE = re.compile(
    r"^\s*([0-9]*\.?[0-9]+)\s*([kmgt]?)(?:i?b)?\s*$", re.IGNORECASE
)


def parse_size_to_mib(value: str) -> int:
    match = SIZE_TOKEN_RE.match(value)
    if not match:
        raise ValueError(f"Invalid size: {value}")
    amount = float(match.group(1))
    unit = (match.group(2) or "m").lower()
    scale = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024, "": 1}[unit]
    return max(1, int(amount * scale))


def resolve_arcadedb_heap_size(mem_limit: str, heap_fraction: float) -> str:
    if heap_fraction <= 0 or heap_fraction > 1:
        raise ValueError(f"Invalid heap fraction: {heap_fraction}")
    total_mib = parse_size_to_mib(mem_limit)
    heap_mib = max(256, int(total_mib * heap_fraction))
    return f"{heap_mib}m"


def run_in_docker(args):
    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        return False

    if os.name == "nt":
        return False

    if is_running_in_docker():
        return False

    docker = shutil.which("docker")
    if not docker:
        return False

    repo_root = Path(__file__).resolve().parents[3]
    host_uid = os.getuid() if hasattr(os, "getuid") else None
    host_gid = os.getgid() if hasattr(os, "getgid") else None
    user_spec = (
        f"{host_uid}:{host_gid}"
        if host_uid is not None and host_gid is not None
        else None
    )

    filtered_args = []
    skip_next = False
    for arg in sys.argv[1:]:
        if skip_next:
            skip_next = False
            continue
        if arg == "--docker-image":
            skip_next = True
            continue
        if arg.startswith("--docker-image="):
            continue
        filtered_args.append(arg)

    arcadedb_wheel_mount_path = None
    if args.db == "arcadedb_sql":
        wheel_candidates = sorted(
            (repo_root / "bindings/python/dist").glob("*embed*.whl")
        )
        if not wheel_candidates:
            if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
                raise RuntimeError(
                    "ArcadeDB wheel not found in bindings/python/dist during GitHub Actions run"
                )
            print(
                "[info] No local ArcadeDB wheel found in bindings/python/dist; "
                "skipping Docker wrapper and running natively."
            )
            return False
        arcadedb_wheel_mount_path = (
            f"/workspace/bindings/python/dist/{wheel_candidates[0].name}"
        )

    packages = ["lxml"]
    if args.db == "duckdb":
        packages.append("duckdb")
    if args.db == "postgresql":
        packages.append("psycopg[binary]")

    packages_str = " ".join(packages)

    inner_cmd_parts = []
    python_cmd = "python"
    if args.db == "postgresql":
        inner_cmd_parts.append(
            "apt-get update && apt-get install -y postgresql python3 python3-venv && rm -rf /var/lib/apt/lists/*"
        )
        python_cmd = "python3"
    inner_cmd_parts.append(f"{python_cmd} -m venv /tmp/bench-venv")
    inner_cmd_parts.append(". /tmp/bench-venv/bin/activate")
    inner_cmd_parts.extend(uv_bootstrap_commands(python_cmd))
    inner_cmd_parts.append(f"uv pip install {packages_str}")
    if arcadedb_wheel_mount_path is not None:
        inner_cmd_parts.append(f'uv pip install "{arcadedb_wheel_mount_path}"')
    inner_cmd_parts.append("echo 'Starting benchmark...'")
    if args.db == "postgresql":
        db_name = build_benchmark_db_name(
            args.dataset,
            args.db,
            args.run_label,
            args.mem_limit,
        )
        inner_cmd_parts.append(
            "status=0; "
            f"{python_cmd} -u 07_stackoverflow_tables_oltp.py {' '.join(filtered_args)} "
            "|| status=$?; "
            "chown -R $HOST_UID:$HOST_GID "
            f"/workspace/bindings/python/examples/my_test_databases/{db_name} || true; "
            "exit $status"
        )
    else:
        inner_cmd_parts.append(
            f"{python_cmd} -u 07_stackoverflow_tables_oltp.py {' '.join(filtered_args)}"
        )

    inner_cmd = " && ".join(inner_cmd_parts)

    docker_image = args.docker_image
    if arcadedb_wheel_mount_path is not None and docker_image == "python:3.12-slim":
        docker_image = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"
    if args.db == "postgresql" and docker_image == "python:3.12-slim":
        docker_image = "postgres:latest"

    cmd = [docker, "run", "--rm"]
    if args.db != "postgresql" and user_spec is not None:
        cmd.extend(["--user", user_spec])
    elif args.db == "postgresql":
        if host_uid is not None:
            cmd.extend(["-e", f"HOST_UID={host_uid}"])
        if host_gid is not None:
            cmd.extend(["-e", f"HOST_GID={host_gid}"])

    cmd.extend(
        [
            "--memory",
            args.mem_limit,
            "--cpus",
            str(args.threads),
            "-e",
            "UV_CACHE_DIR=/tmp/uv-cache",
            "-e",
            "XDG_CACHE_HOME=/tmp",
            "-v",
            f"{repo_root}:/workspace",
            "-w",
            "/workspace/bindings/python/examples",
            docker_image,
            "sh",
            "-lc",
            inner_cmd,
        ]
    )

    print("Launching Docker container...")
    subprocess.run(cmd, check=True)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Example 07: Stack Overflow Tables (OLTP)"
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(EXPECTED_DATASETS),
        default="stackoverflow-tiny",
        help="Dataset size to use (default: stackoverflow-tiny)",
    )
    parser.add_argument(
        "--db",
        choices=["arcadedb_sql", "sqlite", "duckdb", "postgresql"],
        default="arcadedb_sql",
        help="Database to test (default: arcadedb_sql)",
    )
    parser.add_argument(
        "--threads", type=int, default=4, help="Number of worker threads (default: 4)"
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=100_000,
        help="Number of OLTP transactions (default: 100000)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10_000, help="Batch size for load inserts"
    )
    parser.add_argument(
        "--mem-limit",
        type=str,
        default="4g",
        help="Memory limit for Docker and JVM heap",
    )
    parser.add_argument(
        "--jvm-heap-fraction",
        type=float,
        default=0.80,
        help="JVM heap fraction of --mem-limit (default: 0.80)",
    )
    parser.add_argument(
        "--docker-image", type=str, default="python:3.12-slim", help="Docker image"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--verify-single-thread-series",
        action="store_true",
        help="For threads=1 only, assert final table counts match deterministic CRUD simulation",
    )
    parser.add_argument(
        "--run-label", type=str, default=None, help="Optional run label"
    )
    parser.add_argument(
        "--sqlite-profile",
        choices=SQLITE_PROFILE_CHOICES,
        default="perf",
        help="SQLite profile: fair (WAL+FULL+FK ON), perf (WAL+NORMAL+FK ON), olap (perf + large cache/mmap)",
    )

    args = parser.parse_args()
    if args.run_label:
        args.run_label = args.run_label.strip().replace("/", "-").replace(" ", "_")
    if args.jvm_heap_fraction <= 0 or args.jvm_heap_fraction > 1:
        parser.error("--jvm-heap-fraction must be > 0 and <= 1")
    if args.verify_single_thread_series and args.threads != 1:
        parser.error("--verify-single-thread-series requires --threads 1")

    ran = run_in_docker(args)
    if ran:
        return

    heap_size = (
        resolve_arcadedb_heap_size(args.mem_limit, args.jvm_heap_fraction)
        if args.db == "arcadedb_sql"
        else None
    )
    args.heap_size_effective = heap_size
    jvm_kwargs = {"heap_size": heap_size}

    data_dir = Path(__file__).parent / "data" / args.dataset
    ensure_dataset(data_dir)

    for table in TABLE_DEFS:
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"{table['xml']} not found in {data_dir}")

    db_name = build_benchmark_db_name(
        args.dataset,
        args.db,
        args.run_label,
        args.mem_limit,
    )
    db_path = Path("./my_test_databases") / db_name

    print("=" * 80)
    print("Stack Overflow Tables - OLTP (Multi-table)")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"DB: {args.db}")
    if args.db == "sqlite":
        print(f"SQLite profile: {args.sqlite_profile}")
    print(f"Threads: {args.threads}")
    print(f"Transactions: {args.transactions:,}")
    if args.db == "arcadedb_sql":
        print(f"JVM heap size: {heap_size}")
    print(f"DB path: {db_path}")
    print()

    if args.db == "arcadedb_sql":
        summary = run_oltp_arcadedb(
            db_path=db_path,
            data_dir=data_dir,
            transactions=args.transactions,
            batch_size=args.batch_size,
            threads=args.threads,
            seed=args.seed,
            jvm_kwargs=jvm_kwargs,
            verify_single_thread_series=args.verify_single_thread_series,
        )
    elif args.db == "sqlite":
        summary = run_oltp_sqlite(
            db_dir=db_path,
            data_dir=data_dir,
            transactions=args.transactions,
            batch_size=args.batch_size,
            threads=args.threads,
            seed=args.seed,
            sqlite_profile=args.sqlite_profile,
            verify_single_thread_series=args.verify_single_thread_series,
        )
    elif args.db == "duckdb":
        summary = run_oltp_duckdb(
            db_dir=db_path,
            data_dir=data_dir,
            transactions=args.transactions,
            batch_size=args.batch_size,
            threads=args.threads,
            seed=args.seed,
            verify_single_thread_series=args.verify_single_thread_series,
        )
    elif args.db == "postgresql":
        summary = run_oltp_postgresql(
            db_dir=db_path,
            data_dir=data_dir,
            transactions=args.transactions,
            batch_size=args.batch_size,
            threads=args.threads,
            seed=args.seed,
            verify_single_thread_series=args.verify_single_thread_series,
        )
    else:
        raise NotImplementedError("Unsupported DB")

    print("\nResults")
    print("-" * 80)
    print(f"Throughput: {summary['throughput_ops_s']:.1f} ops/s")
    print(f"Total time: {summary['total_time_s']:.2f}s")
    print(f"Load time: {summary['preload_time_s']:.2f}s")
    print(f"Disk after load: {format_bytes(summary['disk_after_preload_bytes'])}")
    print(f"Disk after OLTP: {format_bytes(summary['disk_after_oltp_bytes'])}")
    print(f"Peak RSS (total): {summary['rss_peak_kb'] / 1024:.1f} MB")
    server_peak = summary.get("rss_server_peak_kb", 0)
    client_peak = summary.get("rss_client_peak_kb")
    if server_peak:
        if client_peak is not None:
            print(f"Peak RSS (client): {client_peak / 1024:.1f} MB")
        print(f"Peak RSS (server): {server_peak / 1024:.1f} MB")
    if summary.get("postgres_version"):
        print(f"PostgreSQL version: {summary['postgres_version']}")
    print()
    print_latency_summary(summary["latencies"])
    print()
    write_results(db_path, args, summary)


if __name__ == "__main__":
    main()
