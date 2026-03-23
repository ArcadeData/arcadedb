#!/usr/bin/env python3
"""
Example 08: Stack Overflow Tables (OLAP)

Loads all Stack Overflow XML tables and runs a fixed OLAP query suite.
All work happens inside a Docker container when launched on the host.
"""

import argparse
import csv
import hashlib
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


EXPECTED_DATASETS = {
    "stackoverflow-tiny",
    "stackoverflow-small",
    "stackoverflow-medium",
    "stackoverflow-large",
    "stackoverflow-xlarge",
    "stackoverflow-full",
}

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
    db_name = f"{dataset.replace('-', '_')}_tables_olap_{db}"
    if mem_limit:
        db_name = f"{db_name}_{mem_limit_tag(mem_limit)}"
    if run_label:
        db_name = f"{db_name}_{run_label}"
    return db_name


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


def get_duckdb_module():
    try:
        import duckdb
    except ImportError:
        return None
    return duckdb


def quote_ident_pg(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


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


def to_arcadedb_sql_value(value):
    if isinstance(value, datetime):
        return serialize_datetime(value)
    return value


def get_rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        return 0
    return 0


def read_proc_rss_kb(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return 0
    return 0


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


def find_postgres_bin_dir() -> Optional[Path]:
    candidates = []
    base = Path("/usr/lib/postgresql")
    if base.exists():
        for path in base.glob("*/bin/pg_ctl"):
            candidates.append(path.parent)
    if not candidates:
        return None
    return sorted(candidates, reverse=True)[0]


def format_bytes_binary(value: int) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PiB"


def ensure_dataset(data_dir: Path):
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Dataset not found: {data_dir}. Run download_data.py first."
        )


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


def build_table_schema() -> Dict[str, dict]:
    return {
        table["name"]: {
            "columns": [field[0] for field in table["fields"]],
            "column_count": len(table["fields"]),
        }
        for table in TABLE_DEFS
    }


def count_table_rows_arcadedb(db) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in TABLE_DEFS:
        rows = db.query(
            "sql",
            f"SELECT count(*) AS count FROM {table['name']}",
        ).to_list()
        counts[table["name"]] = int(rows[0].get("count", 0)) if rows else 0
    return counts


def count_table_rows_sql(conn) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in TABLE_DEFS:
        row = conn.execute(f"SELECT count(*) FROM {table['name']}").fetchone()
        counts[table["name"]] = int(row[0]) if row else 0
    return counts


def count_table_rows_postgres(conn) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    with conn.cursor() as cur:
        for table in TABLE_DEFS:
            table_name = quote_ident_pg(table["name"])
            cur.execute(f"SELECT count(*) FROM {table_name}")
            row = cur.fetchone()
            counts[table["name"]] = int(row[0]) if row else 0
    return counts


INDEX_DEFS: List[Tuple[str, List[str], bool]] = [
    ("User", ["Id"], True),
    ("User", ["Reputation"], False),
    ("Post", ["Id"], True),
    ("Post", ["PostTypeId"], False),
    ("Post", ["OwnerUserId"], False),
    ("Post", ["Score"], False),
    ("Post", ["CreationDate"], False),
    ("Comment", ["Id"], True),
    ("Comment", ["PostId"], False),
    ("Badge", ["Id"], True),
    ("Badge", ["Name"], False),
    ("Vote", ["Id"], True),
    ("Vote", ["VoteTypeId"], False),
    ("PostLink", ["Id"], True),
    ("PostLink", ["LinkTypeId"], False),
    ("Tag", ["Id"], True),
    ("Tag", ["TagName"], False),
    ("PostHistory", ["Id"], True),
    ("PostHistory", ["PostHistoryTypeId"], False),
]

HASH_INDEX_FIELDS = {"Id"}


QUERY_DEFS: List[Dict[str, str]] = [
    {
        "name": "post_type_counts",
        "sql": """
            SELECT PostTypeId, count(*) as count
            FROM Post
            GROUP BY PostTypeId
            ORDER BY PostTypeId
        """,
    },
    {
        "name": "top_users_by_reputation",
        "sql": """
            SELECT Id, DisplayName, Reputation
            FROM User
            WHERE Reputation IS NOT NULL
            ORDER BY Reputation DESC, Id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_questions_by_score",
        "sql": """
            SELECT Id, Score, ViewCount
            FROM Post
            WHERE PostTypeId = 1
            ORDER BY Score DESC, Id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_answers_by_score",
        "sql": """
            SELECT Id, Score
            FROM Post
            WHERE PostTypeId = 2
            ORDER BY Score DESC, Id ASC
            LIMIT 10
        """,
    },
    {
        "name": "most_commented_posts",
        "sql": """
            SELECT PostId, count(*) as comment_count
            FROM Comment
            GROUP BY PostId
            ORDER BY comment_count DESC, PostId ASC
            LIMIT 10
        """,
    },
    {
        "name": "votes_by_type",
        "sql": """
            SELECT VoteTypeId, count(*) as count
            FROM Vote
            GROUP BY VoteTypeId
            ORDER BY VoteTypeId
        """,
    },
    {
        "name": "top_badges",
        "sql": """
            SELECT Name, count(*) as count
            FROM Badge
            GROUP BY Name
            ORDER BY count DESC, Name ASC
            LIMIT 10
        """,
    },
    {
        "name": "postlinks_by_type",
        "sql": """
            SELECT LinkTypeId, count(*) as count
            FROM PostLink
            GROUP BY LinkTypeId
            ORDER BY LinkTypeId
        """,
    },
    {
        "name": "posthistory_by_type",
        "sql": """
            SELECT PostHistoryTypeId, count(*) as count
            FROM PostHistory
            GROUP BY PostHistoryTypeId
            ORDER BY PostHistoryTypeId
        """,
    },
    {
        "name": "top_tags_by_count",
        "sql": """
            SELECT TagName, Count
            FROM Tag
            ORDER BY Count DESC, TagName ASC
            LIMIT 10
        """,
    },
]


def iter_xml_rows(xml_path: Path) -> Iterable[Dict[str, str]]:
    context = etree.iterparse(str(xml_path), events=("end",), tag="row")
    for _, elem in context:
        attrs = elem.attrib
        yield attrs
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def create_schema(db):
    for table in TABLE_DEFS:
        db.command("sql", f"CREATE DOCUMENT TYPE {table['name']}")
        for field_name, field_type, _ in table["fields"]:
            db.command(
                "sql", f"CREATE PROPERTY {table['name']}.{field_name} {field_type}"
            )


def insert_batch(db, table_name: str, batch: List[Dict[str, Any]]):
    if not batch:
        return
    columns = list(batch[0].keys())
    assignment_sql = ", ".join(f"{col} = ?" for col in columns)
    sql = f"INSERT INTO {table_name} SET {assignment_sql}"
    with db.transaction():
        for record in batch:
            db.command(
                "sql",
                sql,
                [to_arcadedb_sql_value(record.get(col)) for col in columns],
            )


def configure_arcadedb_async_loader(db, batch_size: int, parallelism: int = 1):
    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, parallelism))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)
    return async_exec


def reset_arcadedb_async_loader(db):
    db.async_executor().wait_completion()
    db.set_read_your_writes(True)
    db.async_executor().set_transaction_use_wal(True)


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


def create_schema_sqlite(conn: sqlite3.Connection):
    for table in TABLE_DEFS:
        columns = []
        for field_name, field_type, _ in table["fields"]:
            columns.append(f"{field_name} {sqlite_type(field_type)}")
        ddl = f"CREATE TABLE IF NOT EXISTS {table['name']} ({', '.join(columns)})"
        conn.execute(ddl)


def configure_sqlite(conn: sqlite3.Connection, profile: str) -> Dict[str, Any]:
    profile = (profile or "olap").lower()
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


def create_schema_duckdb(conn):
    for table in TABLE_DEFS:
        columns = []
        for field_name, field_type, _ in table["fields"]:
            columns.append(f"{field_name} {duckdb_type(field_type)}")
        ddl = f"CREATE TABLE IF NOT EXISTS {table['name']} ({', '.join(columns)})"
        conn.execute(ddl)


def create_schema_postgresql(conn):
    for table in TABLE_DEFS:
        columns = []
        for field_name, field_type, _ in table["fields"]:
            columns.append(f"{quote_ident_pg(field_name)} {postgres_type(field_type)}")
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {quote_ident_pg(table['name'])} "
            f"({', '.join(columns)})"
        )
        conn.execute(ddl)


def insert_batch_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    columns: List[str],
    rows: List[Tuple[Any, ...]],
):
    placeholders = ", ".join(["?"] * len(columns))
    stmt = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    conn.executemany(stmt, rows)


def insert_batch_postgresql(
    conn,
    table_name: str,
    columns: List[str],
    rows: List[Tuple[Any, ...]],
):
    placeholders = ", ".join(["%s"] * len(columns))
    quoted_table = quote_ident_pg(table_name)
    quoted_cols = ", ".join(quote_ident_pg(col) for col in columns)
    stmt = f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"
    with conn.cursor() as cursor:
        cursor.executemany(stmt, rows)
    conn.commit()


def quote_ident_duckdb(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def copy_csv_into_duckdb(conn, table_name: str, columns: List[str], csv_path: Path):
    quoted_table = quote_ident_duckdb(table_name)
    quoted_columns = ", ".join(quote_ident_duckdb(col) for col in columns)
    csv_literal = csv_path.as_posix().replace("'", "''")
    conn.execute(
        f"COPY {quoted_table} ({quoted_columns}) " f"FROM '{csv_literal}' (HEADER TRUE)"
    )


def load_table_arcadedb_async(
    async_exec,
    errors: List[Exception],
    xml_path: Path,
    table_def: Dict[str, Any],
) -> Tuple[int, float]:
    total = 0
    fields: List[FieldDef] = table_def["fields"]
    columns = [field_name for field_name, _, _ in fields]
    start = time.time()

    assignment_sql = ", ".join(f"{col} = ?" for col in columns)
    sql = f"INSERT INTO {table_def['name']} SET {assignment_sql}"
    initial_error_count = len(errors)

    for attrs in iter_xml_rows(xml_path):
        payload: List[Any] = []
        for field_name, field_type, parser in fields:
            value = parser(attrs.get(field_name))
            if field_type == "BOOLEAN" and value is not None:
                value = 1 if value else 0
            payload.append(to_arcadedb_sql_value(value))
        async_exec.command("sql", sql, args=payload)
        total += 1

    async_exec.wait_completion()
    if len(errors) > initial_error_count:
        raise RuntimeError(
            f"Async preload failed for {table_def['name']} "
            f"(first error: {errors[initial_error_count]})"
        )

    return total, time.time() - start


def load_table(
    db, xml_path: Path, table_def: Dict[str, Any], batch_size: int
) -> Tuple[int, float]:
    total = 0
    batch: List[Dict[str, Any]] = []
    fields: List[FieldDef] = table_def["fields"]
    start = time.time()

    for attrs in iter_xml_rows(xml_path):
        row: Dict[str, Any] = {}
        for field_name, _, parser in fields:
            row[field_name] = parser(attrs.get(field_name))
        batch.append(row)
        if len(batch) >= batch_size:
            insert_batch(db, table_def["name"], batch)
            total += len(batch)
            batch = []

    if batch:
        insert_batch(db, table_def["name"], batch)
        total += len(batch)

    elapsed = time.time() - start
    return total, elapsed


def load_table_sqlite(
    conn: sqlite3.Connection,
    xml_path: Path,
    table_def: Dict[str, Any],
    batch_size: int,
) -> Tuple[int, float]:
    total = 0
    batch: List[Tuple[Any, ...]] = []
    fields: List[FieldDef] = table_def["fields"]
    columns = [field_name for field_name, _, _ in fields]
    start = time.time()

    for attrs in iter_xml_rows(xml_path):
        row: List[Any] = []
        for field_name, _, parser in fields:
            value = parser(attrs.get(field_name))
            if isinstance(value, datetime):
                value = serialize_datetime(value)
            row.append(value)
        batch.append(tuple(row))
        if len(batch) >= batch_size:
            conn.execute("BEGIN")
            insert_batch_sqlite(conn, table_def["name"], columns, batch)
            conn.execute("COMMIT")
            total += len(batch)
            batch = []

    if batch:
        conn.execute("BEGIN")
        insert_batch_sqlite(conn, table_def["name"], columns, batch)
        conn.execute("COMMIT")
        total += len(batch)

    elapsed = time.time() - start
    return total, elapsed


def load_table_duckdb(
    conn,
    xml_path: Path,
    table_def: Dict[str, Any],
    csv_dir: Path,
) -> Tuple[int, float]:
    total = 0
    fields: List[FieldDef] = table_def["fields"]
    columns = [field_name for field_name, _, _ in fields]
    start = time.time()

    csv_path = csv_dir / f"{table_def['name']}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()

        for attrs in iter_xml_rows(xml_path):
            payload: Dict[str, Any] = {}
            for field_name, _, parser in fields:
                value = parser(attrs.get(field_name))
                if isinstance(value, datetime):
                    value = serialize_datetime(value)
                if value is None:
                    payload[field_name] = ""
                elif isinstance(value, str):
                    payload[field_name] = value.replace("\r", " ").replace("\n", " ")
                else:
                    payload[field_name] = value
            writer.writerow(payload)
            total += 1

    copy_csv_into_duckdb(conn, table_def["name"], columns, csv_path)

    elapsed = time.time() - start
    return total, elapsed


def load_table_postgresql(
    conn,
    xml_path: Path,
    table_def: Dict[str, Any],
    batch_size: int,
) -> Tuple[int, float]:
    total = 0
    batch: List[Tuple[Any, ...]] = []
    fields: List[FieldDef] = table_def["fields"]
    columns = [field_name for field_name, _, _ in fields]
    start = time.time()

    for attrs in iter_xml_rows(xml_path):
        row: List[Any] = []
        for field_name, _, parser in fields:
            value = parser(attrs.get(field_name))
            row.append(value)
        batch.append(tuple(row))
        if len(batch) >= batch_size:
            insert_batch_postgresql(conn, table_def["name"], columns, batch)
            total += len(batch)
            batch = []

    if batch:
        insert_batch_postgresql(conn, table_def["name"], columns, batch)
        total += len(batch)

    elapsed = time.time() - start
    return total, elapsed


def to_postgres_csv_value(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == "DATETIME":
        return serialize_datetime(value) if isinstance(value, datetime) else value
    if field_type == "BOOLEAN":
        return "true" if bool(value) else "false"
    return value


def load_table_postgresql_copy(
    conn,
    xml_path: Path,
    table_def: Dict[str, Any],
    csv_dir: Path,
) -> Tuple[int, float]:
    total = 0
    fields: List[FieldDef] = table_def["fields"]
    columns = [field_name for field_name, _, _ in fields]
    start = time.time()

    csv_path = csv_dir / f"{table_def['name']}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()

        for attrs in iter_xml_rows(xml_path):
            payload: Dict[str, Any] = {}
            for field_name, field_type, parser in fields:
                payload[field_name] = to_postgres_csv_value(
                    field_type,
                    parser(attrs.get(field_name)),
                )
            writer.writerow(payload)
            total += 1

    quoted_table = quote_ident_pg(table_def["name"])
    quoted_cols = ", ".join(quote_ident_pg(col) for col in columns)
    copy_sql = (
        f"COPY {quoted_table} ({quoted_cols}) "
        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    )

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with csv_path.open("r", encoding="utf-8") as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    copy.write(chunk)
    conn.commit()

    elapsed = time.time() - start
    return total, elapsed


def get_retry_config(dataset_size: str) -> Dict[str, int]:
    size = dataset_size.split("-")[-1] if "-" in dataset_size else dataset_size
    configs = {
        "tiny": {"retry_delay": 10, "max_retries": 60},
        "small": {"retry_delay": 60, "max_retries": 120},
        "medium": {"retry_delay": 180, "max_retries": 200},
        "large": {"retry_delay": 300, "max_retries": 200},
    }
    return configs.get(size, configs["tiny"])


def create_indexes(db, retry_delay: int = 10, max_retries: int = 60) -> float:
    start = time.time()
    failed_indexes = []

    for idx, (table, props, unique) in enumerate(INDEX_DEFS, 1):
        created = False
        for attempt in range(1, max_retries + 1):
            try:
                use_hash = unique and len(props) == 1 and props[0] in HASH_INDEX_FIELDS
                if use_hash:
                    unique_clause = "UNIQUE_HASH" if unique else "NOTUNIQUE_HASH"
                else:
                    unique_clause = "UNIQUE" if unique else "NOTUNIQUE"
                props_clause = ", ".join(props)
                db.command(
                    "sql",
                    f"CREATE INDEX ON {table} ({props_clause}) {unique_clause}",
                )
                created = True
                break
            except Exception as exc:
                error_msg = str(exc)
                retryable = (
                    "NeedRetryException" in error_msg
                    and "asynchronous tasks" in error_msg
                )
                if retryable and attempt < max_retries:
                    elapsed = attempt * retry_delay
                    print(
                        "    Waiting for background tasks to finish "
                        f"(index {idx}/{len(INDEX_DEFS)}, "
                        f"attempt {attempt}/{max_retries}, "
                        f"{elapsed}s elapsed)..."
                    )
                    time.sleep(retry_delay)
                    continue

                failed_indexes.append((table, props, error_msg))
                break

        if not created:
            continue

    async_exec = db.async_executor()
    async_exec.wait_completion()
    elapsed = time.time() - start
    if failed_indexes:
        table, props, error_msg = failed_indexes[0]
        prop_list = ",".join(props)
        raise RuntimeError(
            "Failed to create "
            f"{len(failed_indexes)} indexes. "
            f"First failure: {table}[{prop_list}]: {error_msg}"
        )
    return elapsed


def create_indexes_sqlite(conn: sqlite3.Connection) -> float:
    start = time.time()
    for table, props, unique in INDEX_DEFS:
        suffix = "_".join(props)
        index_name = f"idx_{table}_{suffix}"
        unique_sql = "UNIQUE " if unique else ""
        ddl = (
            f"CREATE {unique_sql}INDEX IF NOT EXISTS {index_name} "
            f"ON {table} ({', '.join(props)})"
        )
        conn.execute(ddl)
    conn.commit()
    return time.time() - start


def create_indexes_duckdb(conn) -> float:
    print("Skipping manual DuckDB secondary indexes for this benchmark.", flush=True)
    return 0.0


def create_indexes_postgresql(conn) -> float:
    start = time.time()
    for table, props, unique in INDEX_DEFS:
        suffix = "_".join(props).lower()
        index_name = f"idx_{table.lower()}_{suffix}"
        unique_sql = "UNIQUE " if unique else ""
        quoted_table = quote_ident_pg(table)
        quoted_cols = ", ".join(quote_ident_pg(col) for col in props)
        ddl = (
            f"CREATE {unique_sql}INDEX IF NOT EXISTS {index_name} "
            f"ON {quoted_table} ({quoted_cols})"
        )
        conn.execute(ddl)
    conn.commit()
    return time.time() - start


def normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return serialize_datetime(value)
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}
    return value


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for row in rows:
        cleaned = {str(k): normalize_value(v) for k, v in row.items()}
        normalized.append(cleaned)
    return normalized


def hash_rows(rows: List[Dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def translate_sql_for_postgresql(sql: str) -> str:
    identifiers = [
        "User",
        "Post",
        "Comment",
        "Badge",
        "Vote",
        "PostLink",
        "Tag",
        "PostHistory",
        "Id",
        "Reputation",
        "CreationDate",
        "DisplayName",
        "PostTypeId",
        "Score",
        "ViewCount",
        "PostId",
        "VoteTypeId",
        "Name",
        "LinkTypeId",
        "PostHistoryTypeId",
        "TagName",
        "Count",
    ]
    translated = sql
    for ident in identifiers:
        pattern = rf"\b{ident}\b"
        if ident == "Count":
            pattern = rf"\b{ident}\b(?!\s*\()"
        translated = re.sub(pattern, quote_ident_pg(ident), translated)
    return translated


def extract_limit(sql: str) -> Optional[int]:
    match = re.search(r"\bLIMIT\s+(\d+)", sql, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def run_queries(
    query_runner: Callable[[str], List[Dict[str, Any]]],
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], float]:
    if query_runs < 1:
        raise ValueError("query_runs must be >= 1")

    if query_order not in {"fixed", "shuffled"}:
        raise ValueError("query_order must be 'fixed' or 'shuffled'")

    per_query: Dict[str, Dict[str, Any]] = {
        item["name"]: {
            "name": item["name"],
            "elapsed_runs_s": [],
            "row_count": None,
            "result_hash": None,
            "sample_rows": [],
            "consistent_across_runs": True,
        }
        for item in QUERY_DEFS
    }

    total_start = time.time()

    for run_idx in range(query_runs):
        run_items = list(QUERY_DEFS)
        if query_order == "shuffled":
            rng = random.Random(seed + run_idx)
            rng.shuffle(run_items)

        for item in run_items:
            query = item["sql"].strip()
            sample_limit = extract_limit(query) or 5
            start = time.time()
            rows = query_runner(query)
            elapsed = time.time() - start

            normalized = normalize_rows(rows)
            result_hash = hash_rows(normalized)

            entry = per_query[item["name"]]
            entry["elapsed_runs_s"].append(elapsed)

            if entry["row_count"] is None:
                entry["row_count"] = len(rows)
                entry["result_hash"] = result_hash
                entry["sample_rows"] = normalized[:sample_limit]
            else:
                if (
                    entry["row_count"] != len(rows)
                    or entry["result_hash"] != result_hash
                ):
                    entry["consistent_across_runs"] = False

    results = []
    for item in QUERY_DEFS:
        entry = per_query[item["name"]]
        runs = entry["elapsed_runs_s"]
        runs_sorted = sorted(runs)
        median_elapsed = runs_sorted[len(runs_sorted) // 2]
        mean_elapsed = sum(runs) / len(runs)
        results.append(
            {
                "name": entry["name"],
                "elapsed_s": median_elapsed,
                "elapsed_runs_s": runs,
                "elapsed_mean_s": mean_elapsed,
                "elapsed_min_s": min(runs),
                "elapsed_max_s": max(runs),
                "row_count": entry["row_count"],
                "result_hash": entry["result_hash"],
                "sample_rows": entry["sample_rows"],
                "consistent_across_runs": entry["consistent_across_runs"],
            }
        )

    total_elapsed = time.time() - total_start
    return results, total_elapsed


def build_query_telemetry(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    cold_times = []
    warm_means = []
    hash_stable = True
    row_count_stable = True

    for item in items:
        runs = item.get("elapsed_runs_s") or []
        if runs:
            cold_times.append(float(runs[0]))
        if len(runs) > 1:
            warm_means.append(float(sum(runs[1:]) / len(runs[1:])))

        if item.get("consistent_across_runs") is False:
            hash_stable = False
            row_count_stable = False

    return {
        "query_cold_time_s": (
            (sum(cold_times) / len(cold_times)) if cold_times else None
        ),
        "query_warm_mean_s": (
            (sum(warm_means) / len(warm_means)) if warm_means else None
        ),
        "query_result_hash_stable": hash_stable,
        "query_row_count_stable": row_count_stable,
    }


def write_results(db_path: Path, args: argparse.Namespace, summary: dict):
    if args.run_label:
        results_path = db_path / f"results_{args.run_label}.json"
    else:
        results_path = db_path / "results.json"
    duckdb_module = get_duckdb_module()
    duckdb_runtime_version = duckdb_module.__version__ if duckdb_module else None
    arcadedb_module, _ = get_arcadedb_module()
    arcadedb_runtime_version = (
        getattr(arcadedb_module, "__version__", None)
        if arcadedb_module is not None
        else None
    )
    query_telemetry = build_query_telemetry(summary.get("queries", {}).get("items", []))
    payload = {
        "dataset": args.dataset,
        "db": args.db,
        "ingest_mode": summary.get("ingest_mode"),
        "batch_size": args.batch_size,
        "mem_limit": args.mem_limit,
        "heap_size": args.heap_size_effective,
        "arcadedb_version": arcadedb_runtime_version,
        "duckdb_version": duckdb_runtime_version,
        "docker_image": args.docker_image,
        "seed": args.seed,
        "run_label": args.run_label,
        "query_runs": args.query_runs,
        "query_order": args.query_order,
        "sqlite_version": sqlite3.sqlite_version,
        "sqlite_profile": summary.get("sqlite_profile"),
        "sqlite_pragmas": summary.get("sqlite_pragmas"),
        "duckdb_runtime_version": duckdb_runtime_version,
        "postgres_version": summary.get("postgres_version"),
        "schema": summary["schema"],
        "load": summary["load"],
        "table_schema": build_table_schema(),
        "table_counts": summary.get("table_counts"),
        "index": summary["index"],
        "queries": summary["queries"],
        "disk_after_load_bytes": summary["disk_after_load_bytes"],
        "disk_after_load_human": format_bytes_binary(summary["disk_after_load_bytes"]),
        "disk_after_index_bytes": summary["disk_after_index_bytes"],
        "disk_after_index_human": format_bytes_binary(
            summary["disk_after_index_bytes"]
        ),
        "disk_after_queries_bytes": summary["disk_after_queries_bytes"],
        "disk_after_queries_human": format_bytes_binary(
            summary["disk_after_queries_bytes"]
        ),
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
        "total_time_s": summary["total_time_s"],
        "run_status": summary.get("run_status", "success"),
        "error_type": summary.get("error_type"),
        "error_message": summary.get("error_message"),
        "db_create_time_s": summary.get("db_create_time_s"),
        "db_open_time_s": summary.get("db_open_time_s"),
        "db_close_time_s": summary.get("db_close_time_s"),
        "query_cold_time_s": query_telemetry.get("query_cold_time_s"),
        "query_warm_mean_s": query_telemetry.get("query_warm_mean_s"),
        "query_result_hash_stable": query_telemetry.get("query_result_hash_stable"),
        "query_row_count_stable": query_telemetry.get("query_row_count_stable"),
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
    r"^\s*([0-9]*\.?[0-9]+)\s*([kmgt]?)(?:i?b)?\s*$",
    re.IGNORECASE,
)


def parse_size_to_mib(value: str) -> int:
    match = SIZE_TOKEN_RE.match(value)
    if not match:
        raise ValueError(f"Invalid size: {value}")

    amount = float(match.group(1))
    unit = (match.group(2) or "m").lower()
    scale = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024, "": 1}[unit]
    return max(1, int(amount * scale))


def resolve_arcadedb_heap_size(
    mem_limit: str,
    heap_fraction: float,
) -> str:
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
    inner_cmd_parts.append(
        f"{python_cmd} -u 08_stackoverflow_tables_olap.py {' '.join(filtered_args)}"
    )
    if args.db == "postgresql":
        db_name = build_benchmark_db_name(
            args.dataset,
            args.db,
            args.run_label,
            args.mem_limit,
        )
        inner_cmd_parts.append(
            "chown -R $HOST_UID:$HOST_GID "
            f"/workspace/bindings/python/examples/my_test_databases/{db_name}"
        )

    inner_cmd = " && ".join(inner_cmd_parts)

    docker_image = args.docker_image
    if arcadedb_wheel_mount_path is not None and docker_image == "python:3.12-slim":
        docker_image = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"
    if args.db == "postgresql" and docker_image == "python:3.12-slim":
        docker_image = "postgres:latest"

    cmd = [
        docker,
        "run",
        "--rm",
    ]

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


def run_olap_arcadedb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    jvm_kwargs: dict,
    dataset_name: str,
    query_runs: int,
    query_order: str,
    seed: int,
) -> dict:
    arcadedb, arcade_error = get_arcadedb_module()
    if arcadedb is None or arcade_error is None:
        raise RuntimeError("arcadedb-embedded is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)

    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)

    print("Creating schema...")
    schema_start = time.time()
    create_schema(db)
    schema_elapsed = time.time() - schema_start

    print("Loading XML tables...")
    load_stats = []
    load_start = time.time()
    async_exec = configure_arcadedb_async_loader(db, batch_size, parallelism=1)
    errors: List[Exception] = []

    def on_error(exc: Exception):
        errors.append(exc)

    async_exec.on_error(on_error)

    try:
        for table in TABLE_DEFS:
            xml_path = data_dir / table["xml"]
            if not xml_path.exists():
                raise FileNotFoundError(f"Missing XML file: {xml_path}")
            print(f"  -> {table['name']} ({xml_path.name})")
            count, elapsed = load_table_arcadedb_async(
                async_exec,
                errors,
                xml_path,
                table,
            )
            load_stats.append(
                {
                    "table": table["name"],
                    "rows": count,
                    "elapsed_s": elapsed,
                }
            )
            print(f"     {count:,} rows in {elapsed:.2f}s")
        load_total = time.time() - load_start
    finally:
        reset_arcadedb_async_loader(db)

    load_counts_start = time.time()
    table_counts_after_load = count_table_rows_arcadedb(db)
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)

    print("Creating indexes...")
    # ArcadeDB builds indexes asynchronously, so we retry and wait for completion.
    retry_config = get_retry_config(dataset_name)
    print(
        "  Retry: "
        f"{retry_config['retry_delay']}s delay, "
        f"{retry_config['max_retries']} max attempts"
    )
    index_elapsed = create_indexes(
        db,
        retry_delay=retry_config["retry_delay"],
        max_retries=retry_config["max_retries"],
    )

    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_elapsed = run_queries(
        lambda sql: db.query("sql", sql).to_list(),
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    table_counts_after_queries = count_table_rows_arcadedb(db)
    counts_time = time.time() - counts_start

    db.close()

    return {
        "ingest_mode": "bulk_tuned_insert",
        "schema": {
            "total_s": schema_elapsed,
        },
        "load": {
            "tables": load_stats,
            "total_s": load_total,
        },
        "table_counts": {
            "after_load": table_counts_after_load,
            "after_queries": table_counts_after_queries,
            "load_counts_time_s": load_counts_time,
            "counts_time_s": counts_time,
        },
        "index": {
            "total_s": index_elapsed,
            "indexes": [],
        },
        "queries": {
            "total_s": query_elapsed,
            "items": query_results,
        },
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
    }


def run_olap_sqlite(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    query_runs: int,
    query_order: str,
    seed: int,
    sqlite_profile: str,
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    db_file = db_path / "sqlite.db"
    conn = sqlite3.connect(db_file)
    sqlite_pragmas = configure_sqlite(conn, sqlite_profile)

    print("Creating schema...")
    schema_start = time.time()
    create_schema_sqlite(conn)
    schema_elapsed = time.time() - schema_start

    print("Loading XML tables...")
    load_stats = []
    load_start = time.time()
    for table in TABLE_DEFS:
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML file: {xml_path}")
        print(f"  -> {table['name']} ({xml_path.name})")
        count, elapsed = load_table_sqlite(conn, xml_path, table, batch_size)
        load_stats.append(
            {
                "table": table["name"],
                "rows": count,
                "elapsed_s": elapsed,
            }
        )
        print(f"     {count:,} rows in {elapsed:.2f}s")
    load_total = time.time() - load_start

    load_counts_start = time.time()
    table_counts_after_load = count_table_rows_sql(conn)
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)

    print("Creating indexes...")
    index_elapsed = create_indexes_sqlite(conn)

    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")

    def query_runner(sql: str) -> List[Dict[str, Any]]:
        cursor = conn.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    query_results, query_elapsed = run_queries(
        query_runner,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    table_counts_after_queries = count_table_rows_sql(conn)
    counts_time = time.time() - counts_start

    conn.close()

    return {
        "ingest_mode": "executemany",
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": sqlite_pragmas,
        "schema": {
            "total_s": schema_elapsed,
        },
        "load": {
            "tables": load_stats,
            "total_s": load_total,
        },
        "table_counts": {
            "after_load": table_counts_after_load,
            "after_queries": table_counts_after_queries,
            "load_counts_time_s": load_counts_time,
            "counts_time_s": counts_time,
        },
        "index": {
            "total_s": index_elapsed,
            "indexes": [
                {"table": t, "properties": props, "unique": unique}
                for t, props, unique in INDEX_DEFS
            ],
        },
        "queries": {
            "total_s": query_elapsed,
            "items": query_results,
        },
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
    }


def run_olap_duckdb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    query_runs: int,
    query_order: str,
    seed: int,
) -> dict:
    duckdb = get_duckdb_module()
    if duckdb is None:
        raise RuntimeError("duckdb is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    db_file = db_path / "duckdb.db"
    conn = duckdb.connect(str(db_file))

    print("Creating schema...")
    schema_start = time.time()
    create_schema_duckdb(conn)
    schema_elapsed = time.time() - schema_start

    print("Loading XML tables...")
    csv_dir = db_path / "duckdb_csv_bulk"
    csv_dir.mkdir(parents=True, exist_ok=True)
    load_stats = []
    load_start = time.time()
    for table in TABLE_DEFS:
        xml_path = data_dir / table["xml"]
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML file: {xml_path}")
        print(f"  -> {table['name']} ({xml_path.name})")
        count, elapsed = load_table_duckdb(conn, xml_path, table, csv_dir)
        load_stats.append(
            {
                "table": table["name"],
                "rows": count,
                "elapsed_s": elapsed,
            }
        )
        print(f"     {count:,} rows in {elapsed:.2f}s")
    load_total = time.time() - load_start

    load_counts_start = time.time()
    table_counts_after_load = count_table_rows_sql(conn)
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)

    print("Creating indexes...")
    index_elapsed = create_indexes_duckdb(conn)

    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")

    def query_runner(sql: str) -> List[Dict[str, Any]]:
        cursor = conn.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    query_results, query_elapsed = run_queries(
        query_runner,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    table_counts_after_queries = count_table_rows_sql(conn)
    counts_time = time.time() - counts_start

    conn.close()

    return {
        "ingest_mode": "copy_csv",
        "schema": {
            "total_s": schema_elapsed,
        },
        "load": {
            "tables": load_stats,
            "total_s": load_total,
        },
        "table_counts": {
            "after_load": table_counts_after_load,
            "after_queries": table_counts_after_queries,
            "load_counts_time_s": load_counts_time,
            "counts_time_s": counts_time,
        },
        "index": {
            "total_s": index_elapsed,
            "indexes": [
                {"table": t, "properties": props, "unique": unique}
                for t, props, unique in INDEX_DEFS
            ],
        },
        "queries": {
            "total_s": query_elapsed,
            "items": query_results,
        },
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
    }


def run_olap_postgresql(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    query_runs: int,
    query_order: str,
    seed: int,
) -> dict:
    psycopg = get_psycopg_module()
    if psycopg is None:
        raise RuntimeError("psycopg is not installed")

    pg_bin = find_postgres_bin_dir()
    if pg_bin is None:
        raise RuntimeError("PostgreSQL binaries not found")

    initdb = pg_bin / "initdb"
    pg_ctl = pg_bin / "pg_ctl"

    print("Creating PostgreSQL database...")
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    pgdata = db_path / "pgdata"
    pgdata.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        ["chown", "-R", "postgres:postgres", str(pgdata)],
        check=True,
    )
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

    # PostgreSQL runs as a separate server process; track server + client RSS.
    postgres_version = None
    stop_event = None
    rss_state = None
    rss_thread = None

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

        pid_provider = lambda: list_postgres_pids(pgdata)
        stop_event, rss_state, rss_thread = start_combined_rss_sampler(pid_provider)

        print("Creating schema...")
        schema_start = time.time()
        create_schema_postgresql(conn)
        conn.commit()
        schema_elapsed = time.time() - schema_start

        print("Loading XML tables...")
        csv_dir = db_path / "postgres_csv_bulk"
        csv_dir.mkdir(parents=True, exist_ok=True)
        load_stats = []
        load_start = time.time()
        for table in TABLE_DEFS:
            xml_path = data_dir / table["xml"]
            if not xml_path.exists():
                raise FileNotFoundError(f"Missing XML file: {xml_path}")
            print(f"  -> {table['name']} ({xml_path.name})")
            count, elapsed = load_table_postgresql_copy(conn, xml_path, table, csv_dir)
            load_stats.append(
                {
                    "table": table["name"],
                    "rows": count,
                    "elapsed_s": elapsed,
                }
            )
            print(f"     {count:,} rows in {elapsed:.2f}s")
        load_total = time.time() - load_start

        load_counts_start = time.time()
        table_counts_after_load = count_table_rows_postgres(conn)
        load_counts_time = time.time() - load_counts_start

        disk_after_load = get_dir_size_bytes(db_path)

        print("Creating indexes...")
        index_elapsed = create_indexes_postgresql(conn)

        disk_after_index = get_dir_size_bytes(db_path)

        print("Running OLAP queries...")

        def query_runner(sql: str) -> List[Dict[str, Any]]:
            translated = translate_sql_for_postgresql(sql)
            cursor = conn.execute(translated)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]

        query_results, query_elapsed = run_queries(
            query_runner,
            query_runs=query_runs,
            query_order=query_order,
            seed=seed,
        )

        disk_after_queries = get_dir_size_bytes(db_path)

        counts_start = time.time()
        table_counts_after_queries = count_table_rows_postgres(conn)
        counts_time = time.time() - counts_start

        conn.close()

        return {
            "ingest_mode": "copy_from_stdin",
            "schema": {
                "total_s": schema_elapsed,
            },
            "load": {
                "tables": load_stats,
                "total_s": load_total,
            },
            "table_counts": {
                "after_load": table_counts_after_load,
                "after_queries": table_counts_after_queries,
                "load_counts_time_s": load_counts_time,
                "counts_time_s": counts_time,
            },
            "index": {
                "total_s": index_elapsed,
                "indexes": [
                    {"table": t, "properties": props, "unique": unique}
                    for t, props, unique in INDEX_DEFS
                ],
            },
            "queries": {
                "total_s": query_elapsed,
                "items": query_results,
            },
            "disk_after_load_bytes": disk_after_load,
            "disk_after_index_bytes": disk_after_index,
            "disk_after_queries_bytes": disk_after_queries,
            "rss_peak_kb": rss_state["max_kb"] if rss_state else 0,
            "rss_client_peak_kb": rss_state["client_max_kb"] if rss_state else 0,
            "rss_server_peak_kb": rss_state["server_max_kb"] if rss_state else 0,
            "postgres_version": postgres_version,
        }
    finally:
        if stop_event is not None:
            stop_event.set()
        if rss_thread is not None:
            rss_thread.join()
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


def main():
    parser = argparse.ArgumentParser(
        description="Example 08: Stack Overflow Tables (OLAP)",
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
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size for XML inserts (default: 10000)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Docker CPU limit (default: 4)",
    )
    parser.add_argument(
        "--mem-limit",
        type=str,
        default="4g",
        help="Memory limit for Docker and JVM heap (default: 4g)",
    )
    parser.add_argument(
        "--jvm-heap-fraction",
        type=float,
        default=0.80,
        help="JVM heap fraction of --mem-limit (default: 0.80)",
    )
    parser.add_argument(
        "--docker-image",
        type=str,
        default="python:3.12-slim",
        help="Docker image to use (default: python:3.12-slim)",
    )
    parser.add_argument(
        "--query-runs",
        type=int,
        default=1,
        help="Number of measured executions per query (default: 1)",
    )
    parser.add_argument(
        "--query-order",
        choices=["fixed", "shuffled"],
        default="shuffled",
        help="Query execution order across repeated runs (default: shuffled)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used for deterministic shuffled query order (default: 42)",
    )
    parser.add_argument(
        "--sqlite-profile",
        choices=SQLITE_PROFILE_CHOICES,
        default="olap",
        help="SQLite profile: fair (WAL+FULL+FK ON), perf (WAL+NORMAL+FK ON), olap (perf + large cache/mmap)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to DB directory and result filename",
    )
    parser.add_argument(
        "--arcadedb-query-max-heap-elements",
        type=int,
        default=1_000_000,
        help=(
            "Value for ArcadeDB setting arcadedb.queryMaxHeapElementsAllowedPerOp "
            "(default: 1000000). Use -1 to disable the guardrail."
        ),
    )

    args = parser.parse_args()
    if args.run_label:
        args.run_label = args.run_label.strip().replace("/", "-").replace(" ", "_")
    if args.jvm_heap_fraction <= 0 or args.jvm_heap_fraction > 1:
        parser.error("--jvm-heap-fraction must be > 0 and <= 1")

    ran = run_in_docker(args)
    if ran:
        return

    heap_size = (
        resolve_arcadedb_heap_size(
            args.mem_limit,
            args.jvm_heap_fraction,
        )
        if args.db == "arcadedb_sql"
        else args.mem_limit
    )
    args.heap_size_effective = heap_size
    jvm_kwargs = {"heap_size": heap_size}
    if args.db == "arcadedb_sql":
        jvm_kwargs["jvm_args"] = (
            "-Darcadedb.queryMaxHeapElementsAllowedPerOp="
            f"{args.arcadedb_query_max_heap_elements}"
        )

    data_dir = Path(__file__).parent / "data" / args.dataset
    ensure_dataset(data_dir)

    db_name = build_benchmark_db_name(
        args.dataset,
        args.db,
        args.run_label,
        args.mem_limit,
    )
    db_path = Path("./my_test_databases") / db_name

    print("=" * 80)
    print("Stack Overflow Tables - OLAP")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"DB: {args.db}")
    if args.db == "sqlite":
        print(f"SQLite profile: {args.sqlite_profile}")
    print(f"Batch size: {args.batch_size}")
    print(f"Query runs: {args.query_runs}")
    print(f"Query order: {args.query_order}")
    print(f"Seed: {args.seed}")
    if args.db == "arcadedb_sql":
        print(f"JVM heap size: {heap_size}")
        print(
            "ArcadeDB query max heap elements/op: "
            f"{args.arcadedb_query_max_heap_elements}"
        )
    print(f"DB path: {db_path}")
    print()

    start_time = time.perf_counter()

    if args.db == "arcadedb_sql":
        stop_event, rss_state, rss_thread = start_rss_sampler()
        summary = run_olap_arcadedb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            jvm_kwargs=jvm_kwargs,
            dataset_name=args.dataset,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
        total_time = time.perf_counter() - start_time
        stop_event.set()
        rss_thread.join()
        summary["rss_peak_kb"] = rss_state["max_kb"]
    elif args.db == "sqlite":
        stop_event, rss_state, rss_thread = start_rss_sampler()
        summary = run_olap_sqlite(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
            sqlite_profile=args.sqlite_profile,
        )
        total_time = time.perf_counter() - start_time
        stop_event.set()
        rss_thread.join()
        summary["rss_peak_kb"] = rss_state["max_kb"]
    elif args.db == "duckdb":
        stop_event, rss_state, rss_thread = start_rss_sampler()
        summary = run_olap_duckdb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
        total_time = time.perf_counter() - start_time
        stop_event.set()
        rss_thread.join()
        summary["rss_peak_kb"] = rss_state["max_kb"]
    elif args.db == "postgresql":
        summary = run_olap_postgresql(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
        total_time = time.perf_counter() - start_time
    else:
        raise NotImplementedError(
            "Only arcadedb_sql, sqlite, duckdb, and postgresql are supported for now"
        )

    summary["total_time_s"] = total_time

    print("\nResults")
    print("-" * 80)
    print(f"Schema time: {summary['schema']['total_s']:.2f}s")
    print(f"Load time: {summary['load']['total_s']:.2f}s")
    print(f"Index time: {summary['index']['total_s']:.2f}s")
    print(f"Query time: {summary['queries']['total_s']:.2f}s")
    print(f"Total time: {summary['total_time_s']:.2f}s")
    print(f"Disk after load: {format_bytes_binary(summary['disk_after_load_bytes'])}")
    print(f"Disk after index: {format_bytes_binary(summary['disk_after_index_bytes'])}")
    print(
        f"Disk after queries: {format_bytes_binary(summary['disk_after_queries_bytes'])}"
    )
    print(f"Peak RSS: {summary['rss_peak_kb'] / 1024:.1f} MB")
    server_peak = summary.get("rss_server_peak_kb", 0)
    client_peak = summary.get("rss_client_peak_kb")
    if server_peak:
        if client_peak is not None:
            print(f"Peak RSS (client): {client_peak / 1024:.1f} MB")
        print(f"Peak RSS (server): {server_peak / 1024:.1f} MB")
    if summary.get("postgres_version"):
        print(f"PostgreSQL version: {summary['postgres_version']}")
    print()

    for item in summary["queries"]["items"]:
        print(
            f"{item['name']}: {item['elapsed_s']:.3f}s, "
            f"rows={item['row_count']}, hash={item['result_hash'][:12]}"
        )

    print()
    write_results(db_path, args, summary)


if __name__ == "__main__":
    main()
