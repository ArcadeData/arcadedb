#!/usr/bin/env python3
"""
Example 10: Stack Overflow Graph (OLAP)

Builds a Stack Overflow property graph (Phase 2 schema) and runs a fixed
OLAP query suite using OpenCypher.
"""

import argparse
import collections
import csv
import hashlib
import json
import os
import pickle
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


BENCHMARK_SCOPE_NOTE = (
    "Scope: OLAP query fairness on a common query suite. "
    "Ingestion paths differ by engine (ArcadeDB uses Cypher inserts, Ladybug uses staged CSV + COPY), "
    "so load/index timings are not a same-path ingest comparison."
)

INDEX_DEFS: List[Tuple[str, List[str], bool]] = [
    ("User", ["Id"], True),
    ("Question", ["Id"], True),
    ("Answer", ["Id"], True),
    ("Tag", ["Id"], True),
    ("Badge", ["Id"], True),
    ("Comment", ["Id"], True),
]

VERTEX_TYPES = ["User", "Question", "Answer", "Tag", "Badge", "Comment"]
EDGE_TYPES = [
    "ASKED",
    "ANSWERED",
    "HAS_ANSWER",
    "ACCEPTED_ANSWER",
    "TAGGED_WITH",
    "COMMENTED_ON",
    "COMMENTED_ON_ANSWER",
    "EARNED",
    "LINKED_TO",
]

QUERY_DEFS: List[Dict[str, str]] = [
    {
        "name": "top_askers",
        "cypher": """
            MATCH (u:User)-[:ASKED]->(q:Question)
            RETURN u.Id AS user_id, u.DisplayName AS name, count(q) AS questions
            ORDER BY questions DESC, user_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_answerers",
        "cypher": """
            MATCH (u:User)-[:ANSWERED]->(a:Answer)
            RETURN u.Id AS user_id, u.DisplayName AS name, count(a) AS answers
            ORDER BY answers DESC, user_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_accepted_answerers",
        "cypher": """
            MATCH (q:Question)-[:ACCEPTED_ANSWER]->(a:Answer)
            MATCH (u:User)-[:ANSWERED]->(a)
            WITH u, count(*) AS accepted
            RETURN u.Id AS user_id, u.DisplayName AS name, accepted
            ORDER BY accepted DESC, user_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_tags_by_questions",
        "cypher": """
            MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
            RETURN t.Id AS tag_id, t.TagName AS tag, count(q) AS questions
            ORDER BY questions DESC, tag_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "tag_cooccurrence",
        "cypher": """
            MATCH (q:Question)-[:TAGGED_WITH]->(t1:Tag)
            MATCH (q)-[:TAGGED_WITH]->(t2:Tag)
            WHERE t1.Id < t2.Id
            RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs
            ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_questions_by_score",
        "cypher": """
            MATCH (q:Question)
            RETURN q.Id AS question_id, q.Score AS score
            ORDER BY score DESC, question_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "questions_with_most_answers",
        "cypher": """
            MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)
            RETURN q.Id AS question_id, count(a) AS answers
            ORDER BY answers DESC, question_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "asker_answerer_pairs",
        "cypher": """
            MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User)
            WHERE asker.Id <> answerer.Id
            WITH asker, answerer, count(*) AS interactions
            RETURN asker.Id AS asker_id, answerer.Id AS answerer_id, interactions
            ORDER BY interactions DESC, asker_id ASC, answerer_id ASC
            LIMIT 10
        """,
    },
    {
        "name": "top_badges",
        "cypher": """
            MATCH (:User)-[:EARNED]->(b:Badge)
            RETURN b.Name AS badge, count(*) AS earned
            ORDER BY earned DESC, badge ASC
            LIMIT 10
        """,
    },
    # Luca is investigating the COUNT() optimization regression that breaks this query.
    {
        "name": "top_questions_by_total_comments",
        "cypher": """
            MATCH (q:Question)
            OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
            WITH q, count(c1) AS direct_comments
            OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
            OPTIONAL MATCH (c2:Comment)-[:COMMENTED_ON_ANSWER]->(a)
            WITH q, direct_comments, count(c2) AS answer_comments
            RETURN q.Id AS question_id, direct_comments + answer_comments AS total_comments
            ORDER BY total_comments DESC, question_id ASC
            LIMIT 10
        """,
    },
]


def get_arcadedb_module():
    try:
        import arcadedb_embedded as arcadedb
        from arcadedb_embedded.exceptions import ArcadeDBError
    except ImportError:
        return None, None
    return arcadedb, ArcadeDBError


def get_ladybug_module():
    try:
        import real_ladybug as lb
    except ImportError:
        return None
    return lb


def get_graphqlite_module():
    try:
        import graphqlite
    except ImportError:
        return None
    return graphqlite


def get_duckdb_module():
    try:
        import duckdb
    except ImportError:
        return None
    return duckdb


def configure_sqlite_profile(conn: sqlite3.Connection, profile: str) -> Dict[str, Any]:
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


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", ""))
    except ValueError:
        return None


def to_epoch_millis(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def format_bytes_binary(value: int) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PiB"


def get_dir_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for root, _, files in os.walk(path):
        for name in files:
            total += (Path(root) / name).stat().st_size
    return total


def start_rss_sampler(
    interval_sec: float = 0.2,
) -> Tuple[threading.Event, dict, threading.Thread]:
    stop_event = threading.Event()
    state = {"max_kb": 0}

    def run():
        while not stop_event.is_set():
            try:
                with open("/proc/self/status", "r", encoding="utf-8") as handle:
                    for line in handle:
                        if line.startswith("VmRSS:"):
                            parts = line.split()
                            if len(parts) >= 2:
                                rss_kb = int(parts[1])
                                state["max_kb"] = max(state["max_kb"], rss_kb)
                            break
            except FileNotFoundError:
                pass
            time.sleep(interval_sec)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop_event, state, thread


class CsvTableWriter:
    def __init__(self, path: Path, fieldnames: List[str], label: str):
        self.path = path
        self.fieldnames = fieldnames
        self.label = label
        self.row_count = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.handle, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        for row in rows:
            payload = {}
            for key in self.fieldnames:
                value = row.get(key)
                if value is None:
                    payload[key] = ""
                elif isinstance(value, str):
                    payload[key] = value.replace("\r", " ").replace("\n", " ")
                else:
                    payload[key] = value
            self.writer.writerow(payload)
            self.row_count += 1

    def close(self) -> None:
        self.handle.close()


def iter_xml_rows(xml_path: Path) -> Iterable[Dict[str, str]]:
    context = etree.iterparse(str(xml_path), events=("end",), tag="row")
    for _, elem in context:
        yield elem.attrib
        elem.clear()


def parse_tags(tags_str: Optional[str]) -> List[str]:
    if not tags_str:
        return []
    if "|" in tags_str:
        return [t for t in tags_str.split("|") if t]
    return [
        t
        for t in tags_str.replace(
            "><",
            "|",
        )
        .replace("<", "")
        .replace(">", "")
        .split("|")
        if t
    ]


def get_retry_config(dataset_size: str) -> Dict[str, int]:
    size = dataset_size.split("-")[-1] if "-" in dataset_size else dataset_size
    configs = {
        "tiny": {"retry_delay": 10, "max_retries": 60},
        "small": {"retry_delay": 60, "max_retries": 120},
        "medium": {"retry_delay": 180, "max_retries": 200},
        "large": {"retry_delay": 300, "max_retries": 200},
    }
    return configs.get(size, configs["tiny"])


def count_arcadedb_by_type(db) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts: Dict[str, int] = {}
    edge_counts: Dict[str, int] = {}
    for label in VERTEX_TYPES:
        rows = db.query(
            "opencypher",
            f"MATCH (n:{label}) RETURN count(n) AS count",
        ).to_list()
        node_counts[label] = int(rows[0].get("count", 0)) if rows else 0
    for label in EDGE_TYPES:
        rows = db.query(
            "opencypher",
            f"MATCH ()-[r:{label}]->() RETURN count(r) AS count",
        ).to_list()
        edge_counts[label] = int(rows[0].get("count", 0)) if rows else 0
    return node_counts, edge_counts


def count_ladybug_by_type(conn) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts: Dict[str, int] = {}
    edge_counts: Dict[str, int] = {}
    for label in VERTEX_TYPES:
        rows = conn.execute(f"MATCH (n:{label}) RETURN count(n) AS count").get_all()
        node_counts[label] = int(rows[0][0]) if rows else 0
    for label in EDGE_TYPES:
        rows = conn.execute(
            f"MATCH ()-[r:{label}]->() RETURN count(r) AS count"
        ).get_all()
        edge_counts[label] = int(rows[0][0]) if rows else 0
    return node_counts, edge_counts


def collect_graph_counts_arcadedb(db) -> Dict[str, object]:
    node_rows = db.query("opencypher", "MATCH (n) RETURN count(n) AS count").to_list()
    edge_rows = db.query(
        "opencypher", "MATCH ()-[r]->() RETURN count(r) AS count"
    ).to_list()
    node_total = int(node_rows[0].get("count", 0)) if node_rows else 0
    edge_total = int(edge_rows[0].get("count", 0)) if edge_rows else 0
    node_counts, edge_counts = count_arcadedb_by_type(db)
    return {
        "node_total": node_total,
        "edge_total": edge_total,
        "node_counts_by_type": node_counts,
        "edge_counts_by_type": edge_counts,
    }


def collect_graph_counts_ladybug(conn) -> Dict[str, object]:
    node_rows = conn.execute("MATCH (n) RETURN count(n) AS count").get_all()
    edge_rows = conn.execute("MATCH ()-[r]->() RETURN count(r) AS count").get_all()
    node_total = int(node_rows[0][0]) if node_rows else 0
    edge_total = int(edge_rows[0][0]) if edge_rows else 0
    node_counts, edge_counts = count_ladybug_by_type(conn)
    return {
        "node_total": node_total,
        "edge_total": edge_total,
        "node_counts_by_type": node_counts,
        "edge_counts_by_type": edge_counts,
    }


def create_arcadedb_schema(db):
    for vertex_type in ("User", "Question", "Answer", "Tag", "Badge", "Comment"):
        db.command("sql", f"CREATE VERTEX TYPE {vertex_type}")
        db.command("sql", f"CREATE PROPERTY {vertex_type}.Id LONG")

    for edge_type in (
        "ASKED",
        "ANSWERED",
        "HAS_ANSWER",
        "ACCEPTED_ANSWER",
        "TAGGED_WITH",
        "COMMENTED_ON",
        "COMMENTED_ON_ANSWER",
        "EARNED",
        "LINKED_TO",
    ):
        db.command("sql", f"CREATE EDGE TYPE {edge_type} UNIDIRECTIONAL")

    db.async_executor().wait_completion()


def create_arcadedb_indexes(db, retry_delay: int = 10, max_retries: int = 60) -> float:
    start = time.time()
    failed_indexes: List[Tuple[str, List[str], str]] = []

    for idx, (table, props, unique) in enumerate(INDEX_DEFS, 1):
        created = False
        for attempt in range(1, max_retries + 1):
            try:
                unique_clause = " UNIQUE_HASH" if unique else ""
                props_csv = ", ".join(props)
                db.command(
                    "sql",
                    f"CREATE INDEX ON {table} ({props_csv}){unique_clause}",
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


def create_ladybug_schema(conn):
    conn.execute(
        "CREATE NODE TABLE User(Id INT64 PRIMARY KEY, DisplayName STRING, Reputation INT64, CreationDate INT64, Views INT64, UpVotes INT64, DownVotes INT64)"
    )
    conn.execute(
        "CREATE NODE TABLE Question(Id INT64 PRIMARY KEY, Title STRING, Body STRING, Score INT64, ViewCount INT64, CreationDate INT64, AnswerCount INT64, CommentCount INT64, FavoriteCount INT64)"
    )
    conn.execute(
        "CREATE NODE TABLE Answer(Id INT64 PRIMARY KEY, Body STRING, Score INT64, CreationDate INT64, CommentCount INT64)"
    )
    conn.execute(
        "CREATE NODE TABLE Tag(Id INT64 PRIMARY KEY, TagName STRING, Count INT64)"
    )
    conn.execute(
        "CREATE NODE TABLE Badge(Id INT64 PRIMARY KEY, Name STRING, Date INT64, Class INT64)"
    )
    conn.execute(
        "CREATE NODE TABLE Comment(Id INT64 PRIMARY KEY, Text STRING, Score INT64, CreationDate INT64)"
    )

    conn.execute("CREATE REL TABLE ASKED(FROM User TO Question, CreationDate INT64)")
    conn.execute("CREATE REL TABLE ANSWERED(FROM User TO Answer, CreationDate INT64)")
    conn.execute("CREATE REL TABLE HAS_ANSWER(FROM Question TO Answer)")
    conn.execute("CREATE REL TABLE ACCEPTED_ANSWER(FROM Question TO Answer)")
    conn.execute("CREATE REL TABLE TAGGED_WITH(FROM Question TO Tag)")
    conn.execute(
        "CREATE REL TABLE COMMENTED_ON(FROM Comment TO Question, CreationDate INT64, Score INT64)"
    )
    conn.execute(
        "CREATE REL TABLE COMMENTED_ON_ANSWER(FROM Comment TO Answer, CreationDate INT64, Score INT64)"
    )
    conn.execute("CREATE REL TABLE EARNED(FROM User TO Badge, Date INT64, Class INT64)")
    conn.execute(
        "CREATE REL TABLE LINKED_TO(FROM Question TO Question, LinkTypeId INT64, CreationDate INT64)"
    )


def arcadedb_insert_vertices(db, vertex_type: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    parallel_flush = db.async_executor().get_parallel_level() > 1
    with db.graph_batch(
        batch_size=max(1, len(rows)),
        expected_edge_count=0,
        bidirectional=False,
        commit_every=max(1, len(rows)),
        use_wal=False,
        parallel_flush=parallel_flush,
    ) as batch:
        batch.create_vertices(vertex_type, rows)


def cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    text = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def format_cypher_props(props: Dict[str, Any]) -> str:
    if not props:
        return ""
    items = [f"{key}: {cypher_literal(value)}" for key, value in props.items()]
    return "{" + ", ".join(items) + "}"


def arcadedb_insert_edges(
    db,
    edge_type: str,
    from_label: str,
    to_label: str,
    rows: List[Dict[str, Any]],
    prop_keys: List[str],
):
    if not rows:
        return
    parallel_flush = db.async_executor().get_parallel_level() > 1
    with db.graph_batch(
        batch_size=max(1, len(rows)),
        expected_edge_count=max(1, len(rows)),
        bidirectional=False,
        commit_every=max(1, len(rows)),
        use_wal=False,
        parallel_flush=parallel_flush,
    ) as batch:
        for row in rows:
            from_rid = row.get("from_rid")
            to_rid = row.get("to_rid")
            if from_rid is None or to_rid is None:
                raise RuntimeError(
                    "RID-only edge insert requires from_rid/to_rid " f"for {edge_type}"
                )
            payload = {
                key: row.get(key) for key in prop_keys if row.get(key) is not None
            }
            batch.new_edge(from_rid, edge_type, to_rid, **payload)


def build_arcadedb_rid_lookup(db, vertex_type: str) -> Dict[int, str]:
    rows = db.query("sql", f"SELECT Id, @rid as rid FROM {vertex_type}").to_list()
    rid_lookup: Dict[int, str] = {}
    for row in rows:
        row_id = row.get("Id")
        row_rid = row.get("rid")
        if row_rid is None:
            row_rid = row.get("@rid")
        if row_id is None or row_rid is None:
            continue
        try:
            rid_lookup[int(row_id)] = str(row_rid)
        except Exception:
            continue
    return rid_lookup


def ladybug_insert_vertices(conn, label: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    statements = []
    for row in rows:
        props = format_cypher_props(row)
        statements.append(f"CREATE (n:{label} {props})")
    conn.execute(";".join(statements))


def ladybug_insert_edges(
    conn,
    edge_type: str,
    from_label: str,
    to_label: str,
    rows: List[Dict[str, Any]],
    prop_keys: List[str],
):
    if not rows:
        return
    statements = []
    for row in rows:
        props = {key: row.get(key) for key in prop_keys}
        prop_sql = format_cypher_props(props)
        if prop_sql:
            prop_sql = " " + prop_sql
        from_id = cypher_literal(row.get("from_id"))
        to_id = cypher_literal(row.get("to_id"))
        statements.append(
            f"MATCH (a:{from_label} {{Id: {from_id}}}), "
            f"(b:{to_label} {{Id: {to_id}}}) "
            f"CREATE (a)-[:{edge_type}{prop_sql}]->(b)"
        )
    conn.execute(";".join(statements))


EDGE_COPY_COLUMNS = {
    "ASKED": ["from_id", "to_id", "CreationDate"],
    "ANSWERED": ["from_id", "to_id", "CreationDate"],
    "HAS_ANSWER": ["from_id", "to_id"],
    "ACCEPTED_ANSWER": ["from_id", "to_id"],
    "TAGGED_WITH": ["from_id", "to_id"],
    "COMMENTED_ON": ["from_id", "to_id", "CreationDate", "Score"],
    "COMMENTED_ON_ANSWER": ["from_id", "to_id", "CreationDate", "Score"],
    "EARNED": ["from_id", "to_id", "Date", "Class"],
    "LINKED_TO": ["from_id", "to_id", "LinkTypeId", "CreationDate"],
}


def copy_csv_table(
    conn,
    table_name: str,
    csv_path: Path,
    row_count: int,
    columns: Optional[List[str]] = None,
) -> float:
    print(f"  COPY {table_name}: {row_count:,} rows")
    start = time.time()
    conn.execute(f"COPY {table_name} FROM '{csv_path.as_posix()}'")
    elapsed = time.time() - start
    print(f"  COPY {table_name} done in {elapsed:.2f}s")
    return elapsed


def copy_csv_table_duckdb(
    conn,
    table_name: str,
    csv_path: Path,
    row_count: int,
    columns: Optional[List[str]] = None,
) -> float:
    print(f"  COPY {table_name}: {row_count:,} rows")
    start = time.time()
    column_sql = f" ({', '.join(columns)})" if columns else ""
    conn.execute(
        f"COPY {table_name}{column_sql} FROM '{csv_path.as_posix()}' "
        "(FORMAT CSV, HEADER TRUE)"
    )
    elapsed = time.time() - start
    print(f"  COPY {table_name} done in {elapsed:.2f}s")
    return elapsed


def load_graph_arcadedb(
    db,
    data_dir: Path,
    batch_size: int,
    retry_config: Dict[str, int],
    graph_parallelism: int,
) -> Tuple[dict, dict, float]:
    stats = {"nodes": {}, "edges": {}}
    ids = {"users": [], "questions": [], "answers": []}
    max_ids = {"user": 0, "question": 0}
    tag_map: Dict[str, int] = {}
    question_ids: set[int] = set()
    answer_ids: set[int] = set()
    db.async_executor().set_parallel_level(max(1, graph_parallelism))

    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Tags.xml"):
        tag_id = parse_int(attrs.get("Id"))
        tag_name = attrs.get("TagName")
        if tag_id is None or tag_name is None:
            continue
        tag_map[tag_name] = tag_id
        batch.append(
            {
                "Id": tag_id,
                "TagName": tag_name,
                "Count": parse_int(attrs.get("Count")),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_vertices(db, "Tag", batch)
            batch = []
    if batch:
        arcadedb_insert_vertices(db, "Tag", batch)
    stats["nodes"]["Tag"] = time.time() - start

    start = time.time()
    batch = []
    for attrs in iter_xml_rows(data_dir / "Users.xml"):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
        max_ids["user"] = max(max_ids["user"], user_id)
        ids["users"].append(user_id)
        batch.append(
            {
                "Id": user_id,
                "DisplayName": attrs.get("DisplayName"),
                "Reputation": parse_int(attrs.get("Reputation")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
                "Views": parse_int(attrs.get("Views")),
                "UpVotes": parse_int(attrs.get("UpVotes")),
                "DownVotes": parse_int(attrs.get("DownVotes")),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_vertices(db, "User", batch)
            batch = []
    if batch:
        arcadedb_insert_vertices(db, "User", batch)
    stats["nodes"]["User"] = time.time() - start

    start = time.time()
    batch_questions: List[Dict[str, Any]] = []
    batch_answers: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_id = parse_int(attrs.get("Id"))
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_id is None or post_type is None:
            continue
        if post_type == 1:
            max_ids["question"] = max(max_ids["question"], post_id)
            ids["questions"].append(post_id)
            question_ids.add(post_id)
            batch_questions.append(
                {
                    "Id": post_id,
                    "Title": attrs.get("Title"),
                    "Body": attrs.get("Body"),
                    "Score": parse_int(attrs.get("Score")),
                    "ViewCount": parse_int(attrs.get("ViewCount")),
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                    "AnswerCount": parse_int(attrs.get("AnswerCount")),
                    "CommentCount": parse_int(attrs.get("CommentCount")),
                    "FavoriteCount": parse_int(attrs.get("FavoriteCount")),
                }
            )
        elif post_type == 2:
            answer_ids.add(post_id)
            batch_answers.append(
                {
                    "Id": post_id,
                    "Body": attrs.get("Body"),
                    "Score": parse_int(attrs.get("Score")),
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                    "CommentCount": parse_int(attrs.get("CommentCount")),
                }
            )
        if len(batch_questions) >= batch_size:
            arcadedb_insert_vertices(db, "Question", batch_questions)
            batch_questions = []
        if len(batch_answers) >= batch_size:
            arcadedb_insert_vertices(db, "Answer", batch_answers)
            batch_answers = []
    if batch_questions:
        arcadedb_insert_vertices(db, "Question", batch_questions)
    if batch_answers:
        arcadedb_insert_vertices(db, "Answer", batch_answers)
    stats["nodes"]["Post"] = time.time() - start

    start = time.time()
    batch = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        badge_id = parse_int(attrs.get("Id"))
        if badge_id is None:
            continue
        batch.append(
            {
                "Id": badge_id,
                "Name": attrs.get("Name"),
                "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                "Class": parse_int(attrs.get("Class")),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_vertices(db, "Badge", batch)
            batch = []
    if batch:
        arcadedb_insert_vertices(db, "Badge", batch)
    stats["nodes"]["Badge"] = time.time() - start

    start = time.time()
    batch = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        if comment_id is None:
            continue
        batch.append(
            {
                "Id": comment_id,
                "Text": attrs.get("Text"),
                "Score": parse_int(attrs.get("Score")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_vertices(db, "Comment", batch)
            batch = []
    if batch:
        arcadedb_insert_vertices(db, "Comment", batch)
    stats["nodes"]["Comment"] = time.time() - start

    # Ensure all async vertex writes are fully visible before creating indexes
    # and issuing MATCH-by-Id edge inserts.
    db.async_executor().wait_completion()

    # Build indexes before edge creation so batched MATCH-by-Id edge inserts
    # can resolve endpoints efficiently.
    print("Building indexes...")
    print(
        "  Retry: "
        f"{retry_config['retry_delay']}s delay, "
        f"{retry_config['max_retries']} max attempts"
    )
    index_time = create_arcadedb_indexes(
        db,
        retry_delay=retry_config["retry_delay"],
        max_retries=retry_config["max_retries"],
    )

    rid_lookup_start = time.time()
    rid_lookups = {
        "User": build_arcadedb_rid_lookup(db, "User"),
        "Question": build_arcadedb_rid_lookup(db, "Question"),
        "Answer": build_arcadedb_rid_lookup(db, "Answer"),
        "Tag": build_arcadedb_rid_lookup(db, "Tag"),
        "Badge": build_arcadedb_rid_lookup(db, "Badge"),
        "Comment": build_arcadedb_rid_lookup(db, "Comment"),
    }
    stats["rid_lookup_s"] = time.time() - rid_lookup_start

    stats["edges"]["ASKED"] = create_edges_arcadedb_asked(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["ANSWERED"] = create_edges_arcadedb_answered(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["HAS_ANSWER"] = create_edges_arcadedb_has_answer(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["ACCEPTED_ANSWER"] = create_edges_arcadedb_accepted_answer(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["TAGGED_WITH"] = create_edges_arcadedb_tagged_with(
        db, data_dir, tag_map, batch_size, rid_lookups
    )
    commented_stats = create_edges_arcadedb_commented_on(
        db, data_dir, question_ids, answer_ids, batch_size, rid_lookups
    )
    stats["edges"].update(commented_stats)
    stats["edges"]["EARNED"] = create_edges_arcadedb_earned(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["LINKED_TO"] = create_edges_arcadedb_linked_to(
        db, data_dir, question_ids, batch_size, rid_lookups
    )

    # Drain pending async edge writes before returning load stats.
    db.async_executor().wait_completion()

    return stats, {"ids": ids, "max_ids": max_ids}, index_time


def create_edges_arcadedb_asked(
    db,
    data_dir: Path,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        from_rid = rid_lookups["User"].get(user_id)
        to_rid = rid_lookups["Question"].get(post_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": post_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "ASKED",
                "User",
                "Question",
                batch,
                ["CreationDate"],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "ASKED",
            "User",
            "Question",
            batch,
            ["CreationDate"],
        )
    return time.time() - start


def create_edges_arcadedb_answered(
    db,
    data_dir: Path,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        from_rid = rid_lookups["User"].get(user_id)
        to_rid = rid_lookups["Answer"].get(post_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": post_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "ANSWERED",
                "User",
                "Answer",
                batch,
                ["CreationDate"],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "ANSWERED",
            "User",
            "Answer",
            batch,
            ["CreationDate"],
        )
    return time.time() - start


def create_edges_arcadedb_has_answer(
    db,
    data_dir: Path,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        parent_id = parse_int(attrs.get("ParentId"))
        answer_id = parse_int(attrs.get("Id"))
        if parent_id is None or answer_id is None:
            continue
        from_rid = rid_lookups["Question"].get(parent_id)
        to_rid = rid_lookups["Answer"].get(answer_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": parent_id,
                "to_id": answer_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "HAS_ANSWER",
                "Question",
                "Answer",
                batch,
                [],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "HAS_ANSWER",
            "Question",
            "Answer",
            batch,
            [],
        )
    return time.time() - start


def create_edges_arcadedb_accepted_answer(
    db,
    data_dir: Path,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        answer_id = parse_int(attrs.get("AcceptedAnswerId"))
        if question_id is None or answer_id is None:
            continue
        from_rid = rid_lookups["Question"].get(question_id)
        to_rid = rid_lookups["Answer"].get(answer_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": question_id,
                "to_id": answer_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "ACCEPTED_ANSWER",
                "Question",
                "Answer",
                batch,
                [],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "ACCEPTED_ANSWER",
            "Question",
            "Answer",
            batch,
            [],
        )
    return time.time() - start


def create_edges_arcadedb_tagged_with(
    db,
    data_dir: Path,
    tag_map: Dict[str, int],
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        if question_id is None:
            continue
        tags = parse_tags(attrs.get("Tags"))
        for tag in tags:
            tag_id = tag_map.get(tag)
            if tag_id is None:
                continue
            from_rid = rid_lookups["Question"].get(question_id)
            to_rid = rid_lookups["Tag"].get(tag_id)
            if from_rid is None or to_rid is None:
                continue
            batch.append(
                {
                    "from_id": question_id,
                    "to_id": tag_id,
                    "from_rid": from_rid,
                    "to_rid": to_rid,
                }
            )
            if len(batch) >= batch_size:
                arcadedb_insert_edges(
                    db,
                    "TAGGED_WITH",
                    "Question",
                    "Tag",
                    batch,
                    [],
                )
                batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "TAGGED_WITH",
            "Question",
            "Tag",
            batch,
            [],
        )
    return time.time() - start


def create_edges_arcadedb_commented_on(
    db,
    data_dir: Path,
    question_ids: set,
    answer_ids: set,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> Dict[str, float]:
    start = time.time()
    batch_question: List[Dict[str, Any]] = []
    batch_answer: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None or post_id is None:
            continue
        edge_type = None
        target_id = None
        if post_id in question_ids:
            edge_type = "COMMENTED_ON"
            target_id = post_id
        elif post_id in answer_ids:
            edge_type = "COMMENTED_ON_ANSWER"
            target_id = post_id
        if target_id is None:
            continue
        from_rid = rid_lookups["Comment"].get(comment_id)
        if from_rid is None:
            continue
        if edge_type == "COMMENTED_ON":
            to_rid = rid_lookups["Question"].get(target_id)
        else:
            to_rid = rid_lookups["Answer"].get(target_id)
        if to_rid is None:
            continue
        payload = {
            "from_id": comment_id,
            "to_id": target_id,
            "from_rid": from_rid,
            "to_rid": to_rid,
            "CreationDate": to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
            "Score": parse_int(attrs.get("Score")),
        }
        if edge_type == "COMMENTED_ON":
            batch_question.append(payload)
        elif edge_type == "COMMENTED_ON_ANSWER":
            batch_answer.append(payload)
        if len(batch_question) >= batch_size:
            arcadedb_insert_edges(
                db,
                "COMMENTED_ON",
                "Comment",
                "Question",
                batch_question,
                ["CreationDate", "Score"],
            )
            batch_question = []
        if len(batch_answer) >= batch_size:
            arcadedb_insert_edges(
                db,
                "COMMENTED_ON_ANSWER",
                "Comment",
                "Answer",
                batch_answer,
                ["CreationDate", "Score"],
            )
            batch_answer = []
    if batch_question:
        arcadedb_insert_edges(
            db,
            "COMMENTED_ON",
            "Comment",
            "Question",
            batch_question,
            ["CreationDate", "Score"],
        )
    if batch_answer:
        arcadedb_insert_edges(
            db,
            "COMMENTED_ON_ANSWER",
            "Comment",
            "Answer",
            batch_answer,
            ["CreationDate", "Score"],
        )
    elapsed = time.time() - start
    return {
        "COMMENTED_ON": elapsed,
        "COMMENTED_ON_ANSWER": elapsed,
    }


def create_edges_arcadedb_earned(
    db,
    data_dir: Path,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        user_id = parse_int(attrs.get("UserId"))
        badge_id = parse_int(attrs.get("Id"))
        if user_id is None or badge_id is None:
            continue
        from_rid = rid_lookups["User"].get(user_id)
        to_rid = rid_lookups["Badge"].get(badge_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": badge_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
                "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                "Class": parse_int(attrs.get("Class")),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "EARNED",
                "User",
                "Badge",
                batch,
                ["Date", "Class"],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "EARNED",
            "User",
            "Badge",
            batch,
            ["Date", "Class"],
        )
    return time.time() - start


def create_edges_arcadedb_linked_to(
    db,
    data_dir: Path,
    question_ids: set,
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
        post_id = parse_int(attrs.get("PostId"))
        related_id = parse_int(attrs.get("RelatedPostId"))
        if post_id is None or related_id is None:
            continue
        if post_id not in question_ids or related_id not in question_ids:
            continue
        from_rid = rid_lookups["Question"].get(post_id)
        to_rid = rid_lookups["Question"].get(related_id)
        if from_rid is None or to_rid is None:
            continue
        batch.append(
            {
                "from_id": post_id,
                "to_id": related_id,
                "from_rid": from_rid,
                "to_rid": to_rid,
                "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            arcadedb_insert_edges(
                db,
                "LINKED_TO",
                "Question",
                "Question",
                batch,
                ["LinkTypeId", "CreationDate"],
            )
            batch = []
    if batch:
        arcadedb_insert_edges(
            db,
            "LINKED_TO",
            "Question",
            "Question",
            batch,
            ["LinkTypeId", "CreationDate"],
        )
    return time.time() - start


def load_graph_ladybug(
    conn,
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    copy_fn: Callable[
        [Any, str, Path, int, Optional[List[str]]], float
    ] = copy_csv_table,
    csv_dir_name: str = "ladybug_csv_bulk",
) -> Tuple[dict, dict]:
    # Default to native bulk ingest path for Ladybug: staged CSV + COPY.
    stats = {"nodes": {}, "edges": {}}
    ids = {"users": [], "questions": [], "answers": []}
    max_ids = {"user": 0, "question": 0}
    tag_map: Dict[str, int] = {}
    question_ids: set[int] = set()
    answer_ids: set[int] = set()
    user_ids: set[int] = set()
    badge_ids: set[int] = set()
    comment_ids: set[int] = set()

    csv_dir = db_path / csv_dir_name
    if csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    writers = {
        "Tag": CsvTableWriter(csv_dir / "Tag.csv", ["Id", "TagName", "Count"], "Tag"),
        "User": CsvTableWriter(
            csv_dir / "User.csv",
            [
                "Id",
                "DisplayName",
                "Reputation",
                "CreationDate",
                "Views",
                "UpVotes",
                "DownVotes",
            ],
            "User",
        ),
        "Question": CsvTableWriter(
            csv_dir / "Question.csv",
            [
                "Id",
                "Title",
                "Body",
                "Score",
                "ViewCount",
                "CreationDate",
                "AnswerCount",
                "CommentCount",
                "FavoriteCount",
            ],
            "Question",
        ),
        "Answer": CsvTableWriter(
            csv_dir / "Answer.csv",
            ["Id", "Body", "Score", "CreationDate", "CommentCount"],
            "Answer",
        ),
        "Badge": CsvTableWriter(
            csv_dir / "Badge.csv",
            ["Id", "Name", "Date", "Class"],
            "Badge",
        ),
        "Comment": CsvTableWriter(
            csv_dir / "Comment.csv",
            ["Id", "Text", "Score", "CreationDate"],
            "Comment",
        ),
        "ASKED": CsvTableWriter(
            csv_dir / "ASKED.csv", ["FROM", "TO", "CreationDate"], "ASKED"
        ),
        "ANSWERED": CsvTableWriter(
            csv_dir / "ANSWERED.csv", ["FROM", "TO", "CreationDate"], "ANSWERED"
        ),
        "HAS_ANSWER": CsvTableWriter(
            csv_dir / "HAS_ANSWER.csv", ["FROM", "TO"], "HAS_ANSWER"
        ),
        "ACCEPTED_ANSWER": CsvTableWriter(
            csv_dir / "ACCEPTED_ANSWER.csv", ["FROM", "TO"], "ACCEPTED_ANSWER"
        ),
        "TAGGED_WITH": CsvTableWriter(
            csv_dir / "TAGGED_WITH.csv", ["FROM", "TO"], "TAGGED_WITH"
        ),
        "COMMENTED_ON": CsvTableWriter(
            csv_dir / "COMMENTED_ON.csv",
            ["FROM", "TO", "CreationDate", "Score"],
            "COMMENTED_ON",
        ),
        "COMMENTED_ON_ANSWER": CsvTableWriter(
            csv_dir / "COMMENTED_ON_ANSWER.csv",
            ["FROM", "TO", "CreationDate", "Score"],
            "COMMENTED_ON_ANSWER",
        ),
        "EARNED": CsvTableWriter(
            csv_dir / "EARNED.csv", ["FROM", "TO", "Date", "Class"], "EARNED"
        ),
        "LINKED_TO": CsvTableWriter(
            csv_dir / "LINKED_TO.csv",
            ["FROM", "TO", "LinkTypeId", "CreationDate"],
            "LINKED_TO",
        ),
    }

    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Tags.xml"):
        tag_id = parse_int(attrs.get("Id"))
        tag_name = attrs.get("TagName")
        if tag_id is None or tag_name is None:
            continue
        tag_map[tag_name] = tag_id
        batch.append(
            {
                "Id": tag_id,
                "TagName": tag_name,
                "Count": parse_int(attrs.get("Count")),
            }
        )
        if len(batch) >= batch_size:
            writers["Tag"].write_rows(batch)
            batch = []
    if batch:
        writers["Tag"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Users.xml"):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
        max_ids["user"] = max(max_ids["user"], user_id)
        ids["users"].append(user_id)
        user_ids.add(user_id)
        batch.append(
            {
                "Id": user_id,
                "DisplayName": attrs.get("DisplayName"),
                "Reputation": parse_int(attrs.get("Reputation")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
                "Views": parse_int(attrs.get("Views")),
                "UpVotes": parse_int(attrs.get("UpVotes")),
                "DownVotes": parse_int(attrs.get("DownVotes")),
            }
        )
        if len(batch) >= batch_size:
            writers["User"].write_rows(batch)
            batch = []
    if batch:
        writers["User"].write_rows(batch)

    batch_questions: List[Dict[str, Any]] = []
    batch_answers: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_id = parse_int(attrs.get("Id"))
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_id is None or post_type is None:
            continue
        if post_type == 1:
            max_ids["question"] = max(max_ids["question"], post_id)
            ids["questions"].append(post_id)
            question_ids.add(post_id)
            batch_questions.append(
                {
                    "Id": post_id,
                    "Title": attrs.get("Title"),
                    "Body": attrs.get("Body"),
                    "Score": parse_int(attrs.get("Score")),
                    "ViewCount": parse_int(attrs.get("ViewCount")),
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                    "AnswerCount": parse_int(attrs.get("AnswerCount")),
                    "CommentCount": parse_int(attrs.get("CommentCount")),
                    "FavoriteCount": parse_int(attrs.get("FavoriteCount")),
                }
            )
        elif post_type == 2:
            answer_ids.add(post_id)
            batch_answers.append(
                {
                    "Id": post_id,
                    "Body": attrs.get("Body"),
                    "Score": parse_int(attrs.get("Score")),
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                    "CommentCount": parse_int(attrs.get("CommentCount")),
                }
            )
        if len(batch_questions) >= batch_size:
            writers["Question"].write_rows(batch_questions)
            batch_questions = []
        if len(batch_answers) >= batch_size:
            writers["Answer"].write_rows(batch_answers)
            batch_answers = []
    if batch_questions:
        writers["Question"].write_rows(batch_questions)
    if batch_answers:
        writers["Answer"].write_rows(batch_answers)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        badge_id = parse_int(attrs.get("Id"))
        if badge_id is None:
            continue
        badge_ids.add(badge_id)
        batch.append(
            {
                "Id": badge_id,
                "Name": attrs.get("Name"),
                "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                "Class": parse_int(attrs.get("Class")),
            }
        )
        if len(batch) >= batch_size:
            writers["Badge"].write_rows(batch)
            batch = []
    if batch:
        writers["Badge"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        if comment_id is None:
            continue
        comment_ids.add(comment_id)
        batch.append(
            {
                "Id": comment_id,
                "Text": attrs.get("Text"),
                "Score": parse_int(attrs.get("Score")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            writers["Comment"].write_rows(batch)
            batch = []
    if batch:
        writers["Comment"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_type not in (1, 2):
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        if user_id not in user_ids:
            continue
        if post_type == 1 and post_id in question_ids:
            batch.append(
                {
                    "FROM": user_id,
                    "TO": post_id,
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                }
            )
            if len(batch) >= batch_size:
                writers["ASKED"].write_rows(batch)
                batch = []
    if batch:
        writers["ASKED"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        if user_id in user_ids and post_id in answer_ids:
            batch.append(
                {
                    "FROM": user_id,
                    "TO": post_id,
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                }
            )
            if len(batch) >= batch_size:
                writers["ANSWERED"].write_rows(batch)
                batch = []
    if batch:
        writers["ANSWERED"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        parent_id = parse_int(attrs.get("ParentId"))
        answer_id = parse_int(attrs.get("Id"))
        if parent_id is None or answer_id is None:
            continue
        if parent_id in question_ids and answer_id in answer_ids:
            batch.append({"FROM": parent_id, "TO": answer_id})
            if len(batch) >= batch_size:
                writers["HAS_ANSWER"].write_rows(batch)
                batch = []
    if batch:
        writers["HAS_ANSWER"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        answer_id = parse_int(attrs.get("AcceptedAnswerId"))
        if question_id is None or answer_id is None:
            continue
        if question_id in question_ids and answer_id in answer_ids:
            batch.append({"FROM": question_id, "TO": answer_id})
            if len(batch) >= batch_size:
                writers["ACCEPTED_ANSWER"].write_rows(batch)
                batch = []
    if batch:
        writers["ACCEPTED_ANSWER"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        if question_id is None or question_id not in question_ids:
            continue
        for tag in parse_tags(attrs.get("Tags")):
            tag_id = tag_map.get(tag)
            if tag_id is None:
                continue
            batch.append({"FROM": question_id, "TO": tag_id})
            if len(batch) >= batch_size:
                writers["TAGGED_WITH"].write_rows(batch)
                batch = []
    if batch:
        writers["TAGGED_WITH"].write_rows(batch)

    batch_question: List[Dict[str, Any]] = []
    batch_answer: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None or post_id is None:
            continue
        if comment_id not in comment_ids:
            continue
        payload = {
            "FROM": comment_id,
            "TO": post_id,
            "CreationDate": to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
            "Score": parse_int(attrs.get("Score")),
        }
        if post_id in question_ids:
            batch_question.append(payload)
        elif post_id in answer_ids:
            batch_answer.append(payload)
        if len(batch_question) >= batch_size:
            writers["COMMENTED_ON"].write_rows(batch_question)
            batch_question = []
        if len(batch_answer) >= batch_size:
            writers["COMMENTED_ON_ANSWER"].write_rows(batch_answer)
            batch_answer = []
    if batch_question:
        writers["COMMENTED_ON"].write_rows(batch_question)
    if batch_answer:
        writers["COMMENTED_ON_ANSWER"].write_rows(batch_answer)

    batch = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        user_id = parse_int(attrs.get("UserId"))
        badge_id = parse_int(attrs.get("Id"))
        if user_id is None or badge_id is None:
            continue
        if user_id in user_ids and badge_id in badge_ids:
            batch.append(
                {
                    "FROM": user_id,
                    "TO": badge_id,
                    "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                    "Class": parse_int(attrs.get("Class")),
                }
            )
            if len(batch) >= batch_size:
                writers["EARNED"].write_rows(batch)
                batch = []
    if batch:
        writers["EARNED"].write_rows(batch)

    batch = []
    for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
        post_id = parse_int(attrs.get("PostId"))
        related_id = parse_int(attrs.get("RelatedPostId"))
        if post_id is None or related_id is None:
            continue
        if post_id in question_ids and related_id in question_ids:
            batch.append(
                {
                    "FROM": post_id,
                    "TO": related_id,
                    "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                    "CreationDate": to_epoch_millis(
                        parse_datetime(attrs.get("CreationDate"))
                    ),
                }
            )
            if len(batch) >= batch_size:
                writers["LINKED_TO"].write_rows(batch)
                batch = []
    if batch:
        writers["LINKED_TO"].write_rows(batch)

    for writer in writers.values():
        writer.close()

    stats["nodes"]["Tag"] = copy_fn(
        conn, "Tag", writers["Tag"].path, writers["Tag"].row_count
    )
    stats["nodes"]["User"] = copy_fn(
        conn, "User", writers["User"].path, writers["User"].row_count
    )
    stats["nodes"]["Question"] = copy_fn(
        conn,
        "Question",
        writers["Question"].path,
        writers["Question"].row_count,
    )
    stats["nodes"]["Answer"] = copy_fn(
        conn, "Answer", writers["Answer"].path, writers["Answer"].row_count
    )
    stats["nodes"]["Badge"] = copy_fn(
        conn, "Badge", writers["Badge"].path, writers["Badge"].row_count
    )
    stats["nodes"]["Comment"] = copy_fn(
        conn,
        "Comment",
        writers["Comment"].path,
        writers["Comment"].row_count,
    )
    stats["nodes"]["Post"] = stats["nodes"]["Question"] + stats["nodes"]["Answer"]

    stats["edges"]["ASKED"] = copy_fn(
        conn,
        "ASKED",
        writers["ASKED"].path,
        writers["ASKED"].row_count,
        EDGE_COPY_COLUMNS["ASKED"],
    )
    stats["edges"]["ANSWERED"] = copy_fn(
        conn,
        "ANSWERED",
        writers["ANSWERED"].path,
        writers["ANSWERED"].row_count,
        EDGE_COPY_COLUMNS["ANSWERED"],
    )
    stats["edges"]["HAS_ANSWER"] = copy_fn(
        conn,
        "HAS_ANSWER",
        writers["HAS_ANSWER"].path,
        writers["HAS_ANSWER"].row_count,
        EDGE_COPY_COLUMNS["HAS_ANSWER"],
    )
    stats["edges"]["ACCEPTED_ANSWER"] = copy_fn(
        conn,
        "ACCEPTED_ANSWER",
        writers["ACCEPTED_ANSWER"].path,
        writers["ACCEPTED_ANSWER"].row_count,
        EDGE_COPY_COLUMNS["ACCEPTED_ANSWER"],
    )
    stats["edges"]["TAGGED_WITH"] = copy_fn(
        conn,
        "TAGGED_WITH",
        writers["TAGGED_WITH"].path,
        writers["TAGGED_WITH"].row_count,
        EDGE_COPY_COLUMNS["TAGGED_WITH"],
    )
    stats["edges"]["COMMENTED_ON"] = copy_fn(
        conn,
        "COMMENTED_ON",
        writers["COMMENTED_ON"].path,
        writers["COMMENTED_ON"].row_count,
        EDGE_COPY_COLUMNS["COMMENTED_ON"],
    )
    stats["edges"]["COMMENTED_ON_ANSWER"] = copy_fn(
        conn,
        "COMMENTED_ON_ANSWER",
        writers["COMMENTED_ON_ANSWER"].path,
        writers["COMMENTED_ON_ANSWER"].row_count,
        EDGE_COPY_COLUMNS["COMMENTED_ON_ANSWER"],
    )
    stats["edges"]["EARNED"] = copy_fn(
        conn,
        "EARNED",
        writers["EARNED"].path,
        writers["EARNED"].row_count,
        EDGE_COPY_COLUMNS["EARNED"],
    )
    stats["edges"]["LINKED_TO"] = copy_fn(
        conn,
        "LINKED_TO",
        writers["LINKED_TO"].path,
        writers["LINKED_TO"].row_count,
        EDGE_COPY_COLUMNS["LINKED_TO"],
    )

    return stats, {"ids": ids, "max_ids": max_ids}


def create_edges_ladybug_asked(
    conn,
    data_dir: Path,
    user_ids: set[int],
    question_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        if user_id not in user_ids or post_id not in question_ids:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": post_id,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "ASKED",
                "User",
                "Question",
                batch,
                ["CreationDate"],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "ASKED",
            "User",
            "Question",
            batch,
            ["CreationDate"],
        )
    return time.time() - start


def create_edges_ladybug_answered(
    conn,
    data_dir: Path,
    user_ids: set[int],
    answer_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        user_id = parse_int(attrs.get("OwnerUserId"))
        post_id = parse_int(attrs.get("Id"))
        if user_id is None or post_id is None:
            continue
        if user_id not in user_ids or post_id not in answer_ids:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": post_id,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "ANSWERED",
                "User",
                "Answer",
                batch,
                ["CreationDate"],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "ANSWERED",
            "User",
            "Answer",
            batch,
            ["CreationDate"],
        )
    return time.time() - start


def create_edges_ladybug_has_answer(
    conn,
    data_dir: Path,
    question_ids: set[int],
    answer_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 2:
            continue
        parent_id = parse_int(attrs.get("ParentId"))
        answer_id = parse_int(attrs.get("Id"))
        if parent_id is None or answer_id is None:
            continue
        if parent_id not in question_ids or answer_id not in answer_ids:
            continue
        batch.append({"from_id": parent_id, "to_id": answer_id})
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "HAS_ANSWER",
                "Question",
                "Answer",
                batch,
                [],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "HAS_ANSWER",
            "Question",
            "Answer",
            batch,
            [],
        )
    return time.time() - start


def create_edges_ladybug_accepted_answer(
    conn,
    data_dir: Path,
    question_ids: set[int],
    answer_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        answer_id = parse_int(attrs.get("AcceptedAnswerId"))
        if question_id is None or answer_id is None:
            continue
        if question_id not in question_ids or answer_id not in answer_ids:
            continue
        batch.append({"from_id": question_id, "to_id": answer_id})
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "ACCEPTED_ANSWER",
                "Question",
                "Answer",
                batch,
                [],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "ACCEPTED_ANSWER",
            "Question",
            "Answer",
            batch,
            [],
        )
    return time.time() - start


def create_edges_ladybug_tagged_with(
    conn,
    data_dir: Path,
    tag_map: Dict[str, int],
    question_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        if question_id is None:
            continue
        if question_id not in question_ids:
            continue
        tags = parse_tags(attrs.get("Tags"))
        for tag in tags:
            tag_id = tag_map.get(tag)
            if tag_id is None:
                continue
            batch.append({"from_id": question_id, "to_id": tag_id})
            if len(batch) >= batch_size:
                ladybug_insert_edges(
                    conn,
                    "TAGGED_WITH",
                    "Question",
                    "Tag",
                    batch,
                    [],
                )
                batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "TAGGED_WITH",
            "Question",
            "Tag",
            batch,
            [],
        )
    return time.time() - start


def create_edges_ladybug_commented_on(
    conn,
    data_dir: Path,
    comment_ids: set[int],
    question_ids: set,
    answer_ids: set,
    batch_size: int,
) -> Dict[str, float]:
    # Ladybug requires edge endpoints to match table types, so split COMMENTED_ON by target type.
    start = time.time()
    batch_question: List[Dict[str, Any]] = []
    batch_answer: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None or post_id is None:
            continue
        if comment_id not in comment_ids:
            continue
        payload = {
            "from_id": comment_id,
            "to_id": post_id,
            "CreationDate": to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
            "Score": parse_int(attrs.get("Score")),
        }
        if post_id in question_ids:
            batch_question.append(payload)
        elif post_id in answer_ids:
            batch_answer.append(payload)
        else:
            continue
        if len(batch_question) >= batch_size:
            ladybug_insert_edges(
                conn,
                "COMMENTED_ON",
                "Comment",
                "Question",
                batch_question,
                ["CreationDate", "Score"],
            )
            batch_question = []
        if len(batch_answer) >= batch_size:
            ladybug_insert_edges(
                conn,
                "COMMENTED_ON_ANSWER",
                "Comment",
                "Answer",
                batch_answer,
                ["CreationDate", "Score"],
            )
            batch_answer = []
    if batch_question:
        ladybug_insert_edges(
            conn,
            "COMMENTED_ON",
            "Comment",
            "Question",
            batch_question,
            ["CreationDate", "Score"],
        )
    if batch_answer:
        ladybug_insert_edges(
            conn,
            "COMMENTED_ON_ANSWER",
            "Comment",
            "Answer",
            batch_answer,
            ["CreationDate", "Score"],
        )
    elapsed = time.time() - start
    return {
        "COMMENTED_ON": elapsed,
        "COMMENTED_ON_ANSWER": elapsed,
    }


def create_edges_ladybug_earned(
    conn,
    data_dir: Path,
    user_ids: set[int],
    badge_ids: set[int],
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        user_id = parse_int(attrs.get("UserId"))
        badge_id = parse_int(attrs.get("Id"))
        if user_id is None or badge_id is None:
            continue
        if user_id not in user_ids or badge_id not in badge_ids:
            continue
        batch.append(
            {
                "from_id": user_id,
                "to_id": badge_id,
                "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                "Class": parse_int(attrs.get("Class")),
            }
        )
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "EARNED",
                "User",
                "Badge",
                batch,
                ["Date", "Class"],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "EARNED",
            "User",
            "Badge",
            batch,
            ["Date", "Class"],
        )
    return time.time() - start


def create_edges_ladybug_linked_to(
    conn,
    data_dir: Path,
    question_ids: set,
    batch_size: int,
) -> float:
    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
        post_id = parse_int(attrs.get("PostId"))
        related_id = parse_int(attrs.get("RelatedPostId"))
        if post_id is None or related_id is None:
            continue
        if post_id not in question_ids or related_id not in question_ids:
            continue
        batch.append(
            {
                "from_id": post_id,
                "to_id": related_id,
                "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            ladybug_insert_edges(
                conn,
                "LINKED_TO",
                "Question",
                "Question",
                batch,
                ["LinkTypeId", "CreationDate"],
            )
            batch = []
    if batch:
        ladybug_insert_edges(
            conn,
            "LINKED_TO",
            "Question",
            "Question",
            batch,
            ["LinkTypeId", "CreationDate"],
        )
    return time.time() - start


def normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    if isinstance(value, tuple):
        return [normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): normalize_value(v) for k, v in value.items()}
    return value


def extract_return_aliases(cypher: str) -> List[str]:
    match = re.search(r"\bRETURN\b(.*)", cypher, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    return_clause = match.group(1)
    return_clause = re.split(
        r"\bORDER\s+BY\b|\bLIMIT\b|\bSKIP\b",
        return_clause,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    parts = [part.strip() for part in return_clause.split(",") if part.strip()]
    aliases: List[str] = []
    for part in parts:
        alias_match = re.search(
            r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", part, flags=re.IGNORECASE
        )
        if alias_match:
            aliases.append(alias_match.group(1))
            continue
        last_token = part.split(".")[-1].strip()
        aliases.append(last_token)
    return aliases


def extract_limit(cypher: str) -> Optional[int]:
    match = re.search(r"\bLIMIT\s+(\d+)", cypher, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def normalize_rows(
    rows: List[Any], alias_order: Optional[List[str]] = None
) -> List[Any]:
    normalized: List[Any] = []
    for row in rows:
        if alias_order and isinstance(row, dict):
            normalized.append(
                {alias: normalize_value(row.get(alias)) for alias in alias_order}
            )
            continue
        if alias_order and isinstance(row, (list, tuple)):
            payload = {}
            for idx, alias in enumerate(alias_order):
                payload[alias] = normalize_value(row[idx]) if idx < len(row) else None
            normalized.append(payload)
            continue
        if isinstance(row, (list, tuple)):
            normalized.append([normalize_value(value) for value in row])
            continue
        normalized.append(normalize_value(row))
    return normalized


def row_sort_key(row: Any) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def hash_rows(rows: List[Any]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_count_rows(rows: List[Any]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for row in rows:
        if isinstance(row, dict):
            question_id = row.get("question_id")
            count_value = row.get("count")
        else:
            question_id = row[0] if len(row) > 0 else None
            count_value = row[1] if len(row) > 1 else None
        if question_id is None:
            continue
        counts[int(question_id)] = int(count_value or 0)
    return counts


def compute_manual_total_comments(query_runner) -> Dict[str, Any]:
    start = time.time()
    direct_rows = query_runner("""
        MATCH (q:Question)
        OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q)
        RETURN q.Id AS question_id, count(c) AS count
        """.strip())
    answer_rows = query_runner("""
        MATCH (q:Question)
        OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)<-[:COMMENTED_ON_ANSWER]-(c:Comment)
        RETURN q.Id AS question_id, count(c) AS count
        """.strip())
    direct_counts = _normalize_count_rows(direct_rows)
    answer_counts = _normalize_count_rows(answer_rows)
    totals: List[Dict[str, int]] = []
    for question_id in sorted(set(direct_counts) | set(answer_counts)):
        total_comments = direct_counts.get(question_id, 0) + answer_counts.get(
            question_id, 0
        )
        totals.append({"question_id": question_id, "total_comments": total_comments})
    totals_sorted = sorted(
        totals,
        key=lambda row: (-row["total_comments"], row["question_id"]),
    )
    top_rows = totals_sorted[:10]
    elapsed = time.time() - start
    result_hash = hash_rows(top_rows)
    return {
        "name": "top_questions_by_total_comments_manual",
        "elapsed_s": elapsed,
        "row_count": len(top_rows),
        "result_hash": result_hash,
        "sample_rows": top_rows,
    }


def run_queries(
    query_runner,
    only_query: Optional[str] = None,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], float]:
    if query_runs < 1:
        raise ValueError("query_runs must be >= 1")

    if query_order not in {"fixed", "shuffled"}:
        raise ValueError("query_order must be 'fixed' or 'shuffled'")

    total_start = time.time()

    items = QUERY_DEFS
    if only_query:
        items = [item for item in QUERY_DEFS if item["name"] == only_query]
        if not items:
            raise ValueError(f"Unknown query name: {only_query}")

    per_query: Dict[str, Dict[str, Any]] = {
        item["name"]: {
            "name": item["name"],
            "elapsed_runs_s": [],
            "row_count": None,
            "result_hash": None,
            "sample_rows": [],
            "consistent_across_runs": True,
        }
        for item in items
    }

    for run_idx in range(query_runs):
        run_items = list(items)
        if query_order == "shuffled":
            rng = random.Random(seed + run_idx)
            rng.shuffle(run_items)

        for idx, item in enumerate(run_items, 1):
            query = item["cypher"].strip()
            alias_order = extract_return_aliases(query)
            sample_limit = extract_limit(query) or 5
            print(
                f"  Run {run_idx + 1}/{query_runs} - "
                f"Query {idx}/{len(run_items)}: {item['name']}..."
            )
            start = time.time()
            rows = query_runner(query)
            elapsed = time.time() - start
            print(f"    Done in {elapsed:.3f}s (rows={len(rows)})")

            normalized = normalize_rows(rows, alias_order=alias_order)
            normalized_sorted = sorted(normalized, key=row_sort_key)
            result_hash = hash_rows(normalized_sorted)

            entry = per_query[item["name"]]
            entry["elapsed_runs_s"].append(elapsed)

            if entry["row_count"] is None:
                entry["row_count"] = len(rows)
                entry["result_hash"] = result_hash
                entry["sample_rows"] = normalized_sorted[:sample_limit]
            else:
                if (
                    entry["row_count"] != len(rows)
                    or entry["result_hash"] != result_hash
                ):
                    entry["consistent_across_runs"] = False

    results: List[Dict[str, Any]] = []
    for item in items:
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


QUERY_NAME_BY_CYPHER = {item["cypher"].strip(): item["name"] for item in QUERY_DEFS}
QUERY_CYPHER_BY_NAME = {item["name"]: item["cypher"].strip() for item in QUERY_DEFS}


def query_name_from_cypher(cypher: str) -> str:
    query = cypher.strip()
    name = QUERY_NAME_BY_CYPHER.get(query)
    if name:
        return name
    if (
        "OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q)" in query
        and "count(c) AS count" in query
    ):
        return "__manual_direct_comments"
    if (
        "OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)<-[:COMMENTED_ON_ANSWER]-(c:Comment)"
        in query
        and "count(c) AS count" in query
    ):
        return "__manual_answer_comments"
    raise ValueError("Unsupported query for this backend")


def execute_sqlite_olap_query(
    conn: sqlite3.Connection, query_name: str
) -> List[Dict[str, Any]]:
    sql_by_name = {
        "top_askers": """
            SELECT u.Id AS user_id, u.DisplayName AS name, count(*) AS questions
            FROM ASKED a
            JOIN User u ON u.Id = a.from_id
            GROUP BY u.Id, u.DisplayName
            ORDER BY questions DESC, user_id ASC
            LIMIT 10
        """,
        "top_answerers": """
            SELECT u.Id AS user_id, u.DisplayName AS name, count(*) AS answers
            FROM ANSWERED a
            JOIN User u ON u.Id = a.from_id
            GROUP BY u.Id, u.DisplayName
            ORDER BY answers DESC, user_id ASC
            LIMIT 10
        """,
        "top_accepted_answerers": """
            SELECT u.Id AS user_id, u.DisplayName AS name, count(*) AS accepted
            FROM ACCEPTED_ANSWER ca
            JOIN ANSWERED ans ON ans.to_id = ca.to_id
            JOIN User u ON u.Id = ans.from_id
            GROUP BY u.Id, u.DisplayName
            ORDER BY accepted DESC, user_id ASC
            LIMIT 10
        """,
        "top_tags_by_questions": """
            SELECT t.Id AS tag_id, t.TagName AS tag, count(*) AS questions
            FROM TAGGED_WITH tw
            JOIN Tag t ON t.Id = tw.to_id
            GROUP BY t.Id, t.TagName
            ORDER BY questions DESC, tag_id ASC
            LIMIT 10
        """,
        "tag_cooccurrence": """
            SELECT t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs
            FROM TAGGED_WITH tw1
            JOIN TAGGED_WITH tw2 ON tw1.from_id = tw2.from_id AND tw1.to_id < tw2.to_id
            JOIN Tag t1 ON t1.Id = tw1.to_id
            JOIN Tag t2 ON t2.Id = tw2.to_id
            GROUP BY t1.TagName, t2.TagName
            ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC
            LIMIT 10
        """,
        "top_questions_by_score": """
            SELECT q.Id AS question_id, q.Score AS score
            FROM Question q
            ORDER BY score DESC, question_id ASC
            LIMIT 10
        """,
        "questions_with_most_answers": """
            SELECT ha.from_id AS question_id, count(*) AS answers
            FROM HAS_ANSWER ha
            GROUP BY ha.from_id
            ORDER BY answers DESC, question_id ASC
            LIMIT 10
        """,
        "asker_answerer_pairs": """
            SELECT ask.from_id AS asker_id, ans.from_id AS answerer_id, count(*) AS interactions
            FROM ASKED ask
            JOIN HAS_ANSWER ha ON ha.from_id = ask.to_id
            JOIN ANSWERED ans ON ans.to_id = ha.to_id
            WHERE ask.from_id <> ans.from_id
            GROUP BY ask.from_id, ans.from_id
            ORDER BY interactions DESC, asker_id ASC, answerer_id ASC
            LIMIT 10
        """,
        "top_badges": """
            SELECT b.Name AS badge, count(*) AS earned
            FROM EARNED e
            JOIN Badge b ON b.Id = e.to_id
            GROUP BY b.Name
            ORDER BY earned DESC, badge ASC
            LIMIT 10
        """,
        "top_questions_by_total_comments": """
            WITH direct_comments AS (
                SELECT q.Id AS question_id, count(cq.from_id) AS direct_comments
                FROM Question q
                LEFT JOIN COMMENTED_ON cq ON cq.to_id = q.Id
                GROUP BY q.Id
            ),
            answer_comments AS (
                SELECT q.Id AS question_id, count(ca.from_id) AS answer_comments
                FROM Question q
                LEFT JOIN HAS_ANSWER ha ON ha.from_id = q.Id
                LEFT JOIN COMMENTED_ON_ANSWER ca ON ca.to_id = ha.to_id
                GROUP BY q.Id
            )
            SELECT q.Id AS question_id,
                   coalesce(dc.direct_comments, 0) + coalesce(ac.answer_comments, 0) AS total_comments
            FROM Question q
            LEFT JOIN direct_comments dc ON dc.question_id = q.Id
            LEFT JOIN answer_comments ac ON ac.question_id = q.Id
            ORDER BY total_comments DESC, question_id ASC
            LIMIT 10
        """,
        "__manual_direct_comments": """
            SELECT q.Id AS question_id, count(c.from_id) AS count
            FROM Question q
            LEFT JOIN COMMENTED_ON c ON c.to_id = q.Id
            GROUP BY q.Id
        """,
        "__manual_answer_comments": """
            SELECT q.Id AS question_id, count(c.from_id) AS count
            FROM Question q
            LEFT JOIN HAS_ANSWER ha ON ha.from_id = q.Id
            LEFT JOIN COMMENTED_ON_ANSWER c ON c.to_id = ha.to_id
            GROUP BY q.Id
        """,
    }
    sql = sql_by_name.get(query_name)
    if not sql:
        raise ValueError(f"Unsupported sqlite query: {query_name}")
    cursor = conn.execute(sql)
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _edge_items_with_count(store: Dict[str, Dict[str, Any]], edge_type: str):
    for (from_id, to_id), props in store["edges"][edge_type].items():
        count = int((props or {}).get("count") or 1)
        yield from_id, to_id, max(1, count)


def execute_python_memory_olap_query(
    store: Dict[str, Dict[str, Any]],
    query_name: str,
) -> List[Dict[str, Any]]:
    users = store["nodes"]["User"]
    questions = store["nodes"]["Question"]
    tags = store["nodes"]["Tag"]
    badges = store["nodes"]["Badge"]

    if query_name == "top_askers":
        counts = collections.defaultdict(int)
        for uid, _qid, mult in _edge_items_with_count(store, "ASKED"):
            counts[uid] += mult
        rows = [
            {
                "user_id": uid,
                "name": users.get(uid, {}).get("DisplayName"),
                "questions": count,
            }
            for uid, count in counts.items()
        ]
        return sorted(rows, key=lambda r: (-int(r["questions"]), int(r["user_id"])))[
            :10
        ]

    if query_name == "top_answerers":
        counts = collections.defaultdict(int)
        for uid, _aid, mult in _edge_items_with_count(store, "ANSWERED"):
            counts[uid] += mult
        rows = [
            {
                "user_id": uid,
                "name": users.get(uid, {}).get("DisplayName"),
                "answers": count,
            }
            for uid, count in counts.items()
        ]
        return sorted(rows, key=lambda r: (-int(r["answers"]), int(r["user_id"])))[:10]

    if query_name == "top_accepted_answerers":
        answerers = collections.defaultdict(list)
        for uid, aid, mult in _edge_items_with_count(store, "ANSWERED"):
            answerers[aid].append((uid, mult))
        accepted = collections.defaultdict(int)
        for _qid, aid, mult_acc in _edge_items_with_count(store, "ACCEPTED_ANSWER"):
            for uid, mult_ans in answerers.get(aid, []):
                accepted[uid] += mult_acc * mult_ans
        rows = [
            {
                "user_id": uid,
                "name": users.get(uid, {}).get("DisplayName"),
                "accepted": count,
            }
            for uid, count in accepted.items()
        ]
        return sorted(rows, key=lambda r: (-int(r["accepted"]), int(r["user_id"])))[:10]

    if query_name == "top_tags_by_questions":
        counts = collections.defaultdict(int)
        for _qid, tid, mult in _edge_items_with_count(store, "TAGGED_WITH"):
            counts[tid] += mult
        rows = [
            {"tag_id": tid, "tag": tags.get(tid, {}).get("TagName"), "questions": count}
            for tid, count in counts.items()
        ]
        return sorted(rows, key=lambda r: (-int(r["questions"]), int(r["tag_id"])))[:10]

    if query_name == "tag_cooccurrence":
        by_question: Dict[int, Dict[int, int]] = collections.defaultdict(
            lambda: collections.defaultdict(int)
        )
        for qid, tid, mult in _edge_items_with_count(store, "TAGGED_WITH"):
            by_question[qid][tid] += mult
        pair_counts = collections.defaultdict(int)
        for tag_mult in by_question.values():
            tag_ids = sorted(tag_mult.keys())
            for i, t1 in enumerate(tag_ids):
                for t2 in tag_ids[i + 1 :]:
                    pair_counts[(t1, t2)] += tag_mult[t1] * tag_mult[t2]
        rows = [
            {
                "tag1": tags.get(t1, {}).get("TagName"),
                "tag2": tags.get(t2, {}).get("TagName"),
                "cooccurs": count,
            }
            for (t1, t2), count in pair_counts.items()
        ]
        return sorted(
            rows, key=lambda r: (-int(r["cooccurs"]), str(r["tag1"]), str(r["tag2"]))
        )[:10]

    if query_name == "top_questions_by_score":
        rows = [
            {"question_id": qid, "score": int((props or {}).get("Score") or 0)}
            for qid, props in questions.items()
        ]
        return sorted(rows, key=lambda r: (-int(r["score"]), int(r["question_id"])))[
            :10
        ]

    if query_name == "questions_with_most_answers":
        counts = collections.defaultdict(int)
        for qid, _aid, mult in _edge_items_with_count(store, "HAS_ANSWER"):
            counts[qid] += mult
        rows = [{"question_id": qid, "answers": count} for qid, count in counts.items()]
        return sorted(rows, key=lambda r: (-int(r["answers"]), int(r["question_id"])))[
            :10
        ]

    if query_name == "asker_answerer_pairs":
        askers_by_question = collections.defaultdict(list)
        for uid, qid, mult in _edge_items_with_count(store, "ASKED"):
            askers_by_question[qid].append((uid, mult))
        answerers_by_answer = collections.defaultdict(list)
        for uid, aid, mult in _edge_items_with_count(store, "ANSWERED"):
            answerers_by_answer[aid].append((uid, mult))
        interactions = collections.defaultdict(int)
        for qid, aid, mult_has in _edge_items_with_count(store, "HAS_ANSWER"):
            for asker_id, mult_ask in askers_by_question.get(qid, []):
                for answerer_id, mult_ans in answerers_by_answer.get(aid, []):
                    if asker_id == answerer_id:
                        continue
                    interactions[(asker_id, answerer_id)] += (
                        mult_has * mult_ask * mult_ans
                    )
        rows = [
            {"asker_id": a, "answerer_id": b, "interactions": count}
            for (a, b), count in interactions.items()
        ]
        return sorted(
            rows,
            key=lambda r: (
                -int(r["interactions"]),
                int(r["asker_id"]),
                int(r["answerer_id"]),
            ),
        )[:10]

    if query_name == "top_badges":
        counts = collections.defaultdict(int)
        for _uid, bid, mult in _edge_items_with_count(store, "EARNED"):
            badge_name = badges.get(bid, {}).get("Name")
            counts[badge_name] += mult
        rows = [{"badge": badge, "earned": count} for badge, count in counts.items()]
        return sorted(rows, key=lambda r: (-int(r["earned"]), str(r["badge"])))[:10]

    if (
        query_name == "top_questions_by_total_comments"
        or query_name == "__manual_direct_comments"
        or query_name == "__manual_answer_comments"
    ):
        direct = collections.defaultdict(int)
        for _cid, qid, mult in _edge_items_with_count(store, "COMMENTED_ON"):
            direct[qid] += mult
        answer_comment_by_answer = collections.defaultdict(int)
        for _cid, aid, mult in _edge_items_with_count(store, "COMMENTED_ON_ANSWER"):
            answer_comment_by_answer[aid] += mult
        via_answer = collections.defaultdict(int)
        for qid, aid, mult_ha in _edge_items_with_count(store, "HAS_ANSWER"):
            via_answer[qid] += mult_ha * answer_comment_by_answer.get(aid, 0)

        if query_name == "__manual_direct_comments":
            return [
                {"question_id": qid, "count": int(direct.get(qid, 0))}
                for qid in sorted(questions.keys())
            ]
        if query_name == "__manual_answer_comments":
            return [
                {"question_id": qid, "count": int(via_answer.get(qid, 0))}
                for qid in sorted(questions.keys())
            ]

        rows = [
            {
                "question_id": qid,
                "total_comments": int(direct.get(qid, 0) + via_answer.get(qid, 0)),
            }
            for qid in questions.keys()
        ]
        return sorted(
            rows, key=lambda r: (-int(r["total_comments"]), int(r["question_id"]))
        )[:10]

    raise ValueError(f"Unsupported python-memory query: {query_name}")


def execute_arcadedb_fast_asker_answerer_pairs(db) -> List[Dict[str, Any]]:
    question_to_askers: Dict[int, List[int]] = {}
    for row in db.query(
        "sql",
        "SELECT @out.Id AS asker_id, @in.Id AS question_id FROM ASKED",
    ).to_list():
        asker_id = row.get("asker_id")
        question_id = row.get("question_id")
        if asker_id is None or question_id is None:
            continue
        asker = int(asker_id)
        question = int(question_id)
        question_to_askers.setdefault(question, []).append(asker)

    answer_to_answerers: Dict[int, List[int]] = {}
    for row in db.query(
        "sql",
        "SELECT @out.Id AS answerer_id, @in.Id AS answer_id FROM ANSWERED",
    ).to_list():
        answerer_id = row.get("answerer_id")
        answer_id = row.get("answer_id")
        if answerer_id is None or answer_id is None:
            continue
        answerer = int(answerer_id)
        answer = int(answer_id)
        answer_to_answerers.setdefault(answer, []).append(answerer)

    interactions: Dict[Tuple[int, int], int] = {}
    for row in db.query(
        "sql",
        "SELECT @out.Id AS question_id, @in.Id AS answer_id FROM HAS_ANSWER",
    ).to_list():
        question_id = row.get("question_id")
        answer_id = row.get("answer_id")
        if question_id is None or answer_id is None:
            continue
        askers = question_to_askers.get(int(question_id), [])
        answerers = answer_to_answerers.get(int(answer_id), [])
        if not askers or not answerers:
            continue
        for asker in askers:
            for answerer in answerers:
                if asker == answerer:
                    continue
                key = (asker, answerer)
                interactions[key] = interactions.get(key, 0) + 1

    top_rows = sorted(
        (
            {
                "asker_id": asker,
                "answerer_id": answerer,
                "interactions": count,
            }
            for (asker, answerer), count in interactions.items()
        ),
        key=lambda row: (
            -int(row["interactions"]),
            int(row["asker_id"]),
            int(row["answerer_id"]),
        ),
    )
    return top_rows[:10]


def execute_arcadedb_fast_top_questions_by_total_comments(db) -> List[Dict[str, Any]]:
    question_ids: List[int] = []
    for row in db.query("sql", "SELECT Id AS question_id FROM Question").to_list():
        qid = row.get("question_id")
        if qid is None:
            continue
        question_ids.append(int(qid))

    direct_counts: Dict[int, int] = {}
    for row in db.query(
        "sql",
        "SELECT @in.Id AS question_id, count(*) AS count FROM COMMENTED_ON GROUP BY @in.Id",
    ).to_list():
        qid = row.get("question_id")
        count = row.get("count")
        if qid is None:
            continue
        direct_counts[int(qid)] = int(count or 0)

    answer_comment_counts: Dict[int, int] = {}
    for row in db.query(
        "sql",
        "SELECT @in.Id AS answer_id, count(*) AS count FROM COMMENTED_ON_ANSWER GROUP BY @in.Id",
    ).to_list():
        aid = row.get("answer_id")
        count = row.get("count")
        if aid is None:
            continue
        answer_comment_counts[int(aid)] = int(count or 0)

    via_answer_counts: Dict[int, int] = {}
    for row in db.query(
        "sql",
        "SELECT @out.Id AS question_id, @in.Id AS answer_id FROM HAS_ANSWER",
    ).to_list():
        qid = row.get("question_id")
        aid = row.get("answer_id")
        if qid is None or aid is None:
            continue
        qid_i = int(qid)
        aid_i = int(aid)
        via_answer_counts[qid_i] = via_answer_counts.get(
            qid_i, 0
        ) + answer_comment_counts.get(aid_i, 0)

    rows: List[Dict[str, int]] = []
    for qid in question_ids:
        total_comments = direct_counts.get(qid, 0) + via_answer_counts.get(qid, 0)
        rows.append({"question_id": qid, "total_comments": int(total_comments)})

    rows.sort(key=lambda row: (-int(row["total_comments"]), int(row["question_id"])))
    return rows[:10]


def execute_arcadedb_cypher_olap_query(db, cypher: str) -> List[Dict[str, Any]]:
    return db.query("opencypher", cypher).to_list()


def run_olap_arcadedb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    threads: int,
    jvm_kwargs: dict,
    dataset_name: str,
    olap_language: str = "cypher",
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    arcadedb, arcade_error = get_arcadedb_module()
    if arcadedb is None or arcade_error is None:
        raise RuntimeError("arcadedb-embedded is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)

    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)
    retry_config = get_retry_config(dataset_name)

    print("Creating schema...")
    schema_start = time.time()
    create_arcadedb_schema(db)
    schema_time = time.time() - schema_start

    print("Loading graph...")
    load_start = time.time()
    load_stats, _, index_time = load_graph_arcadedb(
        db,
        data_dir,
        batch_size,
        retry_config,
        threads,
    )
    load_time_including_index = time.time() - load_start
    load_time = max(0.0, load_time_including_index - index_time)

    load_counts_start = time.time()
    load_counts = collect_graph_counts_arcadedb(db)
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)
    disk_after_index = disk_after_load

    print("Running OLAP queries...")
    query_language = (olap_language or "cypher").strip().lower()
    if query_language != "cypher":
        raise ValueError(
            "ArcadeDB SQL mode is disabled for Example 10. Use cypher mode only."
        )

    query_results, query_time = run_queries(
        lambda cypher: execute_arcadedb_cypher_olap_query(db, cypher),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(
                (lambda cypher: execute_arcadedb_cypher_olap_query(db, cypher))
            )
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    final_counts = collect_graph_counts_arcadedb(db)
    counts_time = time.time() - counts_start

    db.close()

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_counts["node_total"],
        "load_edge_count": load_counts["edge_total"],
        "load_node_counts_by_type": load_counts["node_counts_by_type"],
        "load_edge_counts_by_type": load_counts["edge_counts_by_type"],
        "counts_time_s": counts_time,
        "node_count": final_counts["node_total"],
        "edge_count": final_counts["edge_total"],
        "node_counts_by_type": final_counts["node_counts_by_type"],
        "edge_counts_by_type": final_counts["edge_counts_by_type"],
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
        "arcadedb_olap_language": query_language,
    }


def run_olap_ladybug(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    lb = get_ladybug_module()
    if lb is None:
        raise RuntimeError("real_ladybug is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    db_file = db_path / "ladybug.lbug"

    db = lb.Database(str(db_file))
    conn = lb.Connection(db)

    print("Creating schema...")
    schema_start = time.time()
    create_ladybug_schema(conn)
    schema_time = time.time() - schema_start

    print("Loading graph...")
    load_start = time.time()
    load_stats, _ = load_graph_ladybug(
        conn,
        db_path,
        data_dir,
        batch_size,
    )
    load_time_including_index = time.time() - load_start
    load_time = load_time_including_index

    load_counts_start = time.time()
    load_counts = collect_graph_counts_ladybug(conn)
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)

    print("Building indexes...")
    # Ladybug primary keys are created with the schema; treat this stage as a no-op.
    index_time = 0.0

    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_time = run_queries(
        lambda cypher: conn.execute(cypher).get_all(),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(lambda cypher: conn.execute(cypher).get_all())
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    final_counts = collect_graph_counts_ladybug(conn)
    counts_time = time.time() - counts_start

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_counts["node_total"],
        "load_edge_count": load_counts["edge_total"],
        "load_node_counts_by_type": load_counts["node_counts_by_type"],
        "load_edge_counts_by_type": load_counts["edge_counts_by_type"],
        "counts_time_s": counts_time,
        "node_count": final_counts["node_total"],
        "edge_count": final_counts["edge_total"],
        "node_counts_by_type": final_counts["node_counts_by_type"],
        "edge_counts_by_type": final_counts["edge_counts_by_type"],
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
    }


def _row_count_value(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    row = rows[0] or {}
    if "count" in row:
        return int(row["count"] or 0)
    first_value = next(iter(row.values()), 0)
    return int(first_value or 0)


def _clean_props(props: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in props.items() if value is not None}


def _connect_sqlite(
    db_file: Path,
    sqlite_profile: str,
) -> Tuple[sqlite3.Connection, Dict[str, Any]]:
    conn = sqlite3.connect(str(db_file), timeout=30.0, check_same_thread=False)
    pragma_config = configure_sqlite_profile(conn, sqlite_profile)
    return conn, pragma_config


def _connect_duckdb(db_file: Path) -> Tuple[Any, Dict[str, Any]]:
    duckdb_module = get_duckdb_module()
    if duckdb_module is None:
        raise ImportError("duckdb is required for the duckdb backend")
    conn = duckdb_module.connect(str(db_file))
    return conn, {
        "duckdb_version": getattr(duckdb_module, "__version__", None),
        "duckdb_runtime_version": getattr(duckdb_module, "__version__", None),
    }


def _load_stackoverflow_duckdb(
    conn,
    db_path: Path,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    load_stats, load_info = load_graph_ladybug(
        conn,
        db_path,
        data_dir,
        batch_size,
        copy_fn=copy_csv_table_duckdb,
        csv_dir_name="duckdb_csv_bulk",
    )
    return load_info, load_stats


def _create_sqlite_schema(conn: sqlite3.Connection) -> float:
    statements = [
        "CREATE TABLE IF NOT EXISTS User(Id INTEGER PRIMARY KEY, DisplayName TEXT, Reputation INTEGER, CreationDate INTEGER, Views INTEGER, UpVotes INTEGER, DownVotes INTEGER)",
        "CREATE TABLE IF NOT EXISTS Question(Id INTEGER PRIMARY KEY, Title TEXT, Body TEXT, Score INTEGER, ViewCount INTEGER, CreationDate INTEGER, AnswerCount INTEGER, CommentCount INTEGER, FavoriteCount INTEGER)",
        "CREATE TABLE IF NOT EXISTS Answer(Id INTEGER PRIMARY KEY, Body TEXT, Score INTEGER, CreationDate INTEGER, CommentCount INTEGER)",
        "CREATE TABLE IF NOT EXISTS Tag(Id INTEGER PRIMARY KEY, TagName TEXT, Count INTEGER)",
        "CREATE TABLE IF NOT EXISTS Badge(Id INTEGER PRIMARY KEY, Name TEXT, Date INTEGER, Class INTEGER)",
        "CREATE TABLE IF NOT EXISTS Comment(Id INTEGER PRIMARY KEY, Text TEXT, Score INTEGER, CreationDate INTEGER)",
        "CREATE TABLE IF NOT EXISTS ASKED(from_id INTEGER, to_id INTEGER, CreationDate INTEGER)",
        "CREATE TABLE IF NOT EXISTS ANSWERED(from_id INTEGER, to_id INTEGER, CreationDate INTEGER)",
        "CREATE TABLE IF NOT EXISTS HAS_ANSWER(from_id INTEGER, to_id INTEGER)",
        "CREATE TABLE IF NOT EXISTS ACCEPTED_ANSWER(from_id INTEGER, to_id INTEGER)",
        "CREATE TABLE IF NOT EXISTS TAGGED_WITH(from_id INTEGER, to_id INTEGER)",
        "CREATE TABLE IF NOT EXISTS COMMENTED_ON(from_id INTEGER, to_id INTEGER, CreationDate INTEGER, Score INTEGER)",
        "CREATE TABLE IF NOT EXISTS COMMENTED_ON_ANSWER(from_id INTEGER, to_id INTEGER, CreationDate INTEGER, Score INTEGER)",
        "CREATE TABLE IF NOT EXISTS EARNED(from_id INTEGER, to_id INTEGER, Date INTEGER, Class INTEGER)",
        "CREATE TABLE IF NOT EXISTS LINKED_TO(from_id INTEGER, to_id INTEGER, LinkTypeId INTEGER, CreationDate INTEGER)",
        "CREATE INDEX IF NOT EXISTS idx_asked_from_to ON ASKED(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_asked_to ON ASKED(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_answered_from_to ON ANSWERED(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_answered_to ON ANSWERED(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_has_answer_from_to ON HAS_ANSWER(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_has_answer_to ON HAS_ANSWER(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_accepted_answer_from_to ON ACCEPTED_ANSWER(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_accepted_answer_to ON ACCEPTED_ANSWER(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_tagged_with_from_to ON TAGGED_WITH(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_tagged_with_to ON TAGGED_WITH(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_commented_on_from_to ON COMMENTED_ON(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_commented_on_to ON COMMENTED_ON(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_commented_on_answer_from_to ON COMMENTED_ON_ANSWER(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_commented_on_answer_to ON COMMENTED_ON_ANSWER(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_earned_from_to ON EARNED(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_earned_to ON EARNED(to_id)",
        "CREATE INDEX IF NOT EXISTS idx_linked_to_from_to ON LINKED_TO(from_id, to_id)",
        "CREATE INDEX IF NOT EXISTS idx_linked_to_to ON LINKED_TO(to_id)",
    ]
    index_time_s = 0.0
    for statement in statements:
        started = time.perf_counter()
        conn.execute(statement)
        if statement.startswith("CREATE INDEX"):
            index_time_s += time.perf_counter() - started
    conn.commit()
    return index_time_s


def _create_duckdb_schema(conn) -> float:
    statements = [
        "CREATE TABLE IF NOT EXISTS User(Id BIGINT PRIMARY KEY, DisplayName TEXT, Reputation BIGINT, CreationDate BIGINT, Views BIGINT, UpVotes BIGINT, DownVotes BIGINT)",
        "CREATE TABLE IF NOT EXISTS Question(Id BIGINT PRIMARY KEY, Title TEXT, Body TEXT, Score BIGINT, ViewCount BIGINT, CreationDate BIGINT, AnswerCount BIGINT, CommentCount BIGINT, FavoriteCount BIGINT)",
        "CREATE TABLE IF NOT EXISTS Answer(Id BIGINT PRIMARY KEY, Body TEXT, Score BIGINT, CreationDate BIGINT, CommentCount BIGINT)",
        "CREATE TABLE IF NOT EXISTS Tag(Id BIGINT PRIMARY KEY, TagName TEXT, Count BIGINT)",
        "CREATE TABLE IF NOT EXISTS Badge(Id BIGINT PRIMARY KEY, Name TEXT, Date BIGINT, Class BIGINT)",
        "CREATE TABLE IF NOT EXISTS Comment(Id BIGINT PRIMARY KEY, Text TEXT, Score BIGINT, CreationDate BIGINT)",
        "CREATE TABLE IF NOT EXISTS ASKED(from_id BIGINT, to_id BIGINT, CreationDate BIGINT)",
        "CREATE TABLE IF NOT EXISTS ANSWERED(from_id BIGINT, to_id BIGINT, CreationDate BIGINT)",
        "CREATE TABLE IF NOT EXISTS HAS_ANSWER(from_id BIGINT, to_id BIGINT)",
        "CREATE TABLE IF NOT EXISTS ACCEPTED_ANSWER(from_id BIGINT, to_id BIGINT)",
        "CREATE TABLE IF NOT EXISTS TAGGED_WITH(from_id BIGINT, to_id BIGINT)",
        "CREATE TABLE IF NOT EXISTS COMMENTED_ON(from_id BIGINT, to_id BIGINT, CreationDate BIGINT, Score BIGINT)",
        "CREATE TABLE IF NOT EXISTS COMMENTED_ON_ANSWER(from_id BIGINT, to_id BIGINT, CreationDate BIGINT, Score BIGINT)",
        "CREATE TABLE IF NOT EXISTS EARNED(from_id BIGINT, to_id BIGINT, Date BIGINT, Class BIGINT)",
        "CREATE TABLE IF NOT EXISTS LINKED_TO(from_id BIGINT, to_id BIGINT, LinkTypeId BIGINT, CreationDate BIGINT)",
    ]
    print("Skipping manual DuckDB secondary indexes for this benchmark.")
    for statement in statements:
        conn.execute(statement)
    conn.commit()
    return 0.0


def _load_stackoverflow_sqlite(
    conn: sqlite3.Connection,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    user_ids: List[int] = []
    question_ids: List[int] = []
    answer_ids: List[int] = []
    tag_ids: List[int] = []
    badge_ids: List[int] = []
    comment_ids: List[int] = []

    user_id_set: set[int] = set()
    question_id_set: set[int] = set()
    answer_id_set: set[int] = set()

    tag_name_to_id: Dict[str, int] = {}

    edge_counts = {
        "ASKED": 0,
        "ANSWERED": 0,
        "HAS_ANSWER": 0,
        "ACCEPTED_ANSWER": 0,
        "TAGGED_WITH": 0,
        "COMMENTED_ON": 0,
        "COMMENTED_ON_ANSWER": 0,
        "EARNED": 0,
        "LINKED_TO": 0,
    }
    max_ids = {
        "user": 0,
        "question": 0,
        "answer": 0,
        "tag": 0,
        "badge": 0,
        "comment": 0,
    }

    def flush(batch: List[Tuple[Any, ...]], table: str, columns: List[str]) -> None:
        if not batch:
            return
        placeholders = ",".join(["?"] * len(columns))
        conn.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            batch,
        )
        batch.clear()

    conn.execute("BEGIN")
    try:
        tag_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Tags.xml"):
            tag_id = parse_int(attrs.get("Id"))
            tag_name = attrs.get("TagName")
            if tag_id is None or tag_name is None:
                continue
            tag_ids.append(tag_id)
            max_ids["tag"] = max(max_ids["tag"], tag_id)
            tag_name_to_id[tag_name] = tag_id
            tag_batch.append((tag_id, tag_name, parse_int(attrs.get("Count"))))
            if len(tag_batch) >= batch_size:
                flush(tag_batch, "Tag", ["Id", "TagName", "Count"])
        flush(tag_batch, "Tag", ["Id", "TagName", "Count"])

        user_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Users.xml"):
            user_id = parse_int(attrs.get("Id"))
            if user_id is None:
                continue
            user_ids.append(user_id)
            user_id_set.add(user_id)
            max_ids["user"] = max(max_ids["user"], user_id)
            user_batch.append(
                (
                    user_id,
                    attrs.get("DisplayName"),
                    parse_int(attrs.get("Reputation")),
                    to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                    parse_int(attrs.get("Views")),
                    parse_int(attrs.get("UpVotes")),
                    parse_int(attrs.get("DownVotes")),
                )
            )
            if len(user_batch) >= batch_size:
                flush(
                    user_batch,
                    "User",
                    [
                        "Id",
                        "DisplayName",
                        "Reputation",
                        "CreationDate",
                        "Views",
                        "UpVotes",
                        "DownVotes",
                    ],
                )
        flush(
            user_batch,
            "User",
            [
                "Id",
                "DisplayName",
                "Reputation",
                "CreationDate",
                "Views",
                "UpVotes",
                "DownVotes",
            ],
        )

        question_batch: List[Tuple[Any, ...]] = []
        answer_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Posts.xml"):
            post_id = parse_int(attrs.get("Id"))
            post_type = parse_int(attrs.get("PostTypeId"))
            if post_id is None or post_type is None:
                continue
            if post_type == 1:
                question_ids.append(post_id)
                question_id_set.add(post_id)
                max_ids["question"] = max(max_ids["question"], post_id)
                question_batch.append(
                    (
                        post_id,
                        attrs.get("Title"),
                        attrs.get("Body"),
                        parse_int(attrs.get("Score")),
                        parse_int(attrs.get("ViewCount")),
                        to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                        parse_int(attrs.get("AnswerCount")),
                        parse_int(attrs.get("CommentCount")),
                        parse_int(attrs.get("FavoriteCount")),
                    )
                )
            elif post_type == 2:
                answer_ids.append(post_id)
                answer_id_set.add(post_id)
                max_ids["answer"] = max(max_ids["answer"], post_id)
                answer_batch.append(
                    (
                        post_id,
                        attrs.get("Body"),
                        parse_int(attrs.get("Score")),
                        to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                        parse_int(attrs.get("CommentCount")),
                    )
                )

            if len(question_batch) >= batch_size:
                flush(
                    question_batch,
                    "Question",
                    [
                        "Id",
                        "Title",
                        "Body",
                        "Score",
                        "ViewCount",
                        "CreationDate",
                        "AnswerCount",
                        "CommentCount",
                        "FavoriteCount",
                    ],
                )
            if len(answer_batch) >= batch_size:
                flush(
                    answer_batch,
                    "Answer",
                    ["Id", "Body", "Score", "CreationDate", "CommentCount"],
                )

        flush(
            question_batch,
            "Question",
            [
                "Id",
                "Title",
                "Body",
                "Score",
                "ViewCount",
                "CreationDate",
                "AnswerCount",
                "CommentCount",
                "FavoriteCount",
            ],
        )
        flush(
            answer_batch,
            "Answer",
            ["Id", "Body", "Score", "CreationDate", "CommentCount"],
        )

        asked_batch: List[Tuple[Any, ...]] = []
        answered_batch: List[Tuple[Any, ...]] = []
        has_answer_batch: List[Tuple[Any, ...]] = []
        accepted_answer_batch: List[Tuple[Any, ...]] = []
        tagged_with_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Posts.xml"):
            post_id = parse_int(attrs.get("Id"))
            post_type = parse_int(attrs.get("PostTypeId"))
            if post_id is None or post_type is None:
                continue

            if post_type == 1:
                owner_user_id = parse_int(attrs.get("OwnerUserId"))
                if owner_user_id is not None and owner_user_id in user_id_set:
                    asked_batch.append(
                        (
                            owner_user_id,
                            post_id,
                            to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                        )
                    )
                    edge_counts["ASKED"] += 1

                accepted_answer_id = parse_int(attrs.get("AcceptedAnswerId"))
                if (
                    accepted_answer_id is not None
                    and accepted_answer_id in answer_id_set
                ):
                    accepted_answer_batch.append((post_id, accepted_answer_id))
                    edge_counts["ACCEPTED_ANSWER"] += 1

                for tag_name in parse_tags(attrs.get("Tags")):
                    tag_id = tag_name_to_id.get(tag_name)
                    if tag_id is not None:
                        tagged_with_batch.append((post_id, tag_id))
                        edge_counts["TAGGED_WITH"] += 1

            elif post_type == 2:
                owner_user_id = parse_int(attrs.get("OwnerUserId"))
                if owner_user_id is not None and owner_user_id in user_id_set:
                    answered_batch.append(
                        (
                            owner_user_id,
                            post_id,
                            to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                        )
                    )
                    edge_counts["ANSWERED"] += 1

                parent_id = parse_int(attrs.get("ParentId"))
                if parent_id is not None and parent_id in question_id_set:
                    has_answer_batch.append((parent_id, post_id))
                    edge_counts["HAS_ANSWER"] += 1

            if len(asked_batch) >= batch_size:
                flush(asked_batch, "ASKED", ["from_id", "to_id", "CreationDate"])
            if len(answered_batch) >= batch_size:
                flush(
                    answered_batch,
                    "ANSWERED",
                    ["from_id", "to_id", "CreationDate"],
                )
            if len(has_answer_batch) >= batch_size:
                flush(has_answer_batch, "HAS_ANSWER", ["from_id", "to_id"])
            if len(accepted_answer_batch) >= batch_size:
                flush(
                    accepted_answer_batch,
                    "ACCEPTED_ANSWER",
                    ["from_id", "to_id"],
                )
            if len(tagged_with_batch) >= batch_size:
                flush(tagged_with_batch, "TAGGED_WITH", ["from_id", "to_id"])

        flush(asked_batch, "ASKED", ["from_id", "to_id", "CreationDate"])
        flush(answered_batch, "ANSWERED", ["from_id", "to_id", "CreationDate"])
        flush(has_answer_batch, "HAS_ANSWER", ["from_id", "to_id"])
        flush(accepted_answer_batch, "ACCEPTED_ANSWER", ["from_id", "to_id"])
        flush(tagged_with_batch, "TAGGED_WITH", ["from_id", "to_id"])

        badge_batch: List[Tuple[Any, ...]] = []
        earned_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Badges.xml"):
            badge_id = parse_int(attrs.get("Id"))
            user_id = parse_int(attrs.get("UserId"))
            if badge_id is None:
                continue
            badge_ids.append(badge_id)
            max_ids["badge"] = max(max_ids["badge"], badge_id)
            badge_batch.append(
                (
                    badge_id,
                    attrs.get("Name"),
                    to_epoch_millis(parse_datetime(attrs.get("Date"))),
                    parse_int(attrs.get("Class")),
                )
            )
            if user_id is not None and user_id in user_id_set:
                earned_batch.append(
                    (
                        user_id,
                        badge_id,
                        to_epoch_millis(parse_datetime(attrs.get("Date"))),
                        parse_int(attrs.get("Class")),
                    )
                )
                edge_counts["EARNED"] += 1

            if len(badge_batch) >= batch_size:
                flush(badge_batch, "Badge", ["Id", "Name", "Date", "Class"])
            if len(earned_batch) >= batch_size:
                flush(
                    earned_batch,
                    "EARNED",
                    ["from_id", "to_id", "Date", "Class"],
                )

        flush(badge_batch, "Badge", ["Id", "Name", "Date", "Class"])
        flush(earned_batch, "EARNED", ["from_id", "to_id", "Date", "Class"])

        comment_batch: List[Tuple[Any, ...]] = []
        commented_on_batch: List[Tuple[Any, ...]] = []
        commented_on_answer_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "Comments.xml"):
            comment_id = parse_int(attrs.get("Id"))
            post_id = parse_int(attrs.get("PostId"))
            if comment_id is None:
                continue
            comment_ids.append(comment_id)
            max_ids["comment"] = max(max_ids["comment"], comment_id)
            created_ms = to_epoch_millis(parse_datetime(attrs.get("CreationDate")))
            score = parse_int(attrs.get("Score"))
            comment_batch.append((comment_id, attrs.get("Text"), score, created_ms))

            if post_id is not None and post_id in question_id_set:
                commented_on_batch.append((comment_id, post_id, created_ms, score))
                edge_counts["COMMENTED_ON"] += 1
            elif post_id is not None and post_id in answer_id_set:
                commented_on_answer_batch.append(
                    (comment_id, post_id, created_ms, score)
                )
                edge_counts["COMMENTED_ON_ANSWER"] += 1

            if len(comment_batch) >= batch_size:
                flush(
                    comment_batch,
                    "Comment",
                    ["Id", "Text", "Score", "CreationDate"],
                )
            if len(commented_on_batch) >= batch_size:
                flush(
                    commented_on_batch,
                    "COMMENTED_ON",
                    ["from_id", "to_id", "CreationDate", "Score"],
                )
            if len(commented_on_answer_batch) >= batch_size:
                flush(
                    commented_on_answer_batch,
                    "COMMENTED_ON_ANSWER",
                    ["from_id", "to_id", "CreationDate", "Score"],
                )

        flush(comment_batch, "Comment", ["Id", "Text", "Score", "CreationDate"])
        flush(
            commented_on_batch,
            "COMMENTED_ON",
            ["from_id", "to_id", "CreationDate", "Score"],
        )
        flush(
            commented_on_answer_batch,
            "COMMENTED_ON_ANSWER",
            ["from_id", "to_id", "CreationDate", "Score"],
        )

        linked_to_batch: List[Tuple[Any, ...]] = []
        for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
            post_id = parse_int(attrs.get("PostId"))
            related_id = parse_int(attrs.get("RelatedPostId"))
            if post_id is None or related_id is None:
                continue
            if post_id not in question_id_set or related_id not in question_id_set:
                continue
            linked_to_batch.append(
                (
                    post_id,
                    related_id,
                    parse_int(attrs.get("LinkTypeId")),
                    to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
                )
            )
            edge_counts["LINKED_TO"] += 1
            if len(linked_to_batch) >= batch_size:
                flush(
                    linked_to_batch,
                    "LINKED_TO",
                    ["from_id", "to_id", "LinkTypeId", "CreationDate"],
                )

        flush(
            linked_to_batch,
            "LINKED_TO",
            ["from_id", "to_id", "LinkTypeId", "CreationDate"],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    load_info = {
        "ids": {
            "users": user_ids,
            "questions": question_ids,
            "answers": answer_ids,
            "tags": tag_ids,
            "badges": badge_ids,
            "comments": comment_ids,
        },
        "max_ids": max_ids,
    }
    load_stats = {
        "nodes": {
            "User": len(user_ids),
            "Question": len(question_ids),
            "Answer": len(answer_ids),
            "Tag": len(tag_ids),
            "Badge": len(badge_ids),
            "Comment": len(comment_ids),
        },
        "edges": edge_counts,
    }
    return load_info, load_stats


def _count_sqlite_by_type(
    conn: sqlite3.Connection,
    vertex_types: List[str],
    edge_types: List[str],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts: Dict[str, int] = {}
    edge_counts: Dict[str, int] = {}
    for label in vertex_types:
        node_counts[label] = int(
            conn.execute(f"SELECT count(*) FROM {label}").fetchone()[0]
        )
    for label in edge_types:
        edge_counts[label] = int(
            conn.execute(f"SELECT count(*) FROM {label}").fetchone()[0]
        )
    return node_counts, edge_counts


def _load_stackoverflow_graphqlite_bulk(
    graph,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, List[int]], Dict[str, int]]:
    user_ids: List[int] = []
    question_ids: List[int] = []
    answer_ids: List[int] = []
    tag_ids: List[int] = []
    badge_ids: List[int] = []
    comment_ids: List[int] = []
    user_id_set = set()
    question_id_set: set[int] = set()
    answer_id_set: set[int] = set()
    tag_name_to_id: Dict[str, int] = {}

    edge_counts = {
        "ASKED": 0,
        "ANSWERED": 0,
        "HAS_ANSWER": 0,
        "ACCEPTED_ANSWER": 0,
        "TAGGED_WITH": 0,
        "COMMENTED_ON": 0,
        "COMMENTED_ON_ANSWER": 0,
        "EARNED": 0,
        "LINKED_TO": 0,
    }

    max_ids = {
        "user": 0,
        "question": 0,
        "answer": 0,
        "tag": 0,
        "badge": 0,
        "comment": 0,
    }

    id_map_all: Dict[str, int] = {}

    def flush_nodes(batch: List[Tuple[str, Dict[str, Any], str]]) -> None:
        if not batch:
            return
        id_map_all.update(graph.insert_nodes_bulk(batch))
        batch.clear()

    def flush_edges(batch: List[Tuple[str, str, Dict[str, Any], str]]) -> None:
        if not batch:
            return
        graph.insert_edges_bulk(batch, id_map_all)
        batch.clear()

    node_batch: List[Tuple[str, Dict[str, Any], str]] = []
    for attrs in iter_xml_rows(data_dir / "Tags.xml"):
        tag_id = parse_int(attrs.get("Id"))
        tag_name = attrs.get("TagName")
        if tag_id is None or tag_name is None:
            continue
        tag_ids.append(tag_id)
        max_ids["tag"] = max(max_ids["tag"], tag_id)
        tag_name_to_id[tag_name] = tag_id
        node_batch.append(
            (
                f"t:{tag_id}",
                _clean_props(
                    {
                        "Id": tag_id,
                        "TagName": tag_name,
                        "Count": parse_int(attrs.get("Count")),
                    }
                ),
                "Tag",
            )
        )
        if len(node_batch) >= batch_size:
            flush_nodes(node_batch)

    for attrs in iter_xml_rows(data_dir / "Users.xml"):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
        node_batch.append(
            (
                f"u:{user_id}",
                _clean_props(
                    {
                        "Id": user_id,
                        "DisplayName": attrs.get("DisplayName"),
                        "Reputation": parse_int(attrs.get("Reputation")),
                        "CreationDate": to_epoch_millis(
                            parse_datetime(attrs.get("CreationDate"))
                        ),
                        "Views": parse_int(attrs.get("Views")),
                        "UpVotes": parse_int(attrs.get("UpVotes")),
                        "DownVotes": parse_int(attrs.get("DownVotes")),
                    }
                ),
                "User",
            )
        )
        user_ids.append(user_id)
        user_id_set.add(user_id)
        max_ids["user"] = max(max_ids["user"], user_id)
        if len(node_batch) >= batch_size:
            flush_nodes(node_batch)

    flush_nodes(node_batch)

    question_batch: List[Tuple[str, Dict[str, Any], str]] = []
    answer_batch: List[Tuple[str, Dict[str, Any], str]] = []
    edge_batch: List[Tuple[str, str, Dict[str, Any], str]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_type = parse_int(attrs.get("PostTypeId"))
        post_id = parse_int(attrs.get("Id"))
        if post_type is None or post_id is None:
            continue
        if post_type == 1:
            question_ids.append(post_id)
            question_id_set.add(post_id)
            max_ids["question"] = max(max_ids["question"], post_id)
            question_batch.append(
                (
                    f"q:{post_id}",
                    _clean_props(
                        {
                            "Id": post_id,
                            "Title": attrs.get("Title"),
                            "Body": attrs.get("Body"),
                            "Score": parse_int(attrs.get("Score")),
                            "ViewCount": parse_int(attrs.get("ViewCount")),
                            "CreationDate": to_epoch_millis(
                                parse_datetime(attrs.get("CreationDate"))
                            ),
                            "AnswerCount": parse_int(attrs.get("AnswerCount")),
                            "CommentCount": parse_int(attrs.get("CommentCount")),
                            "FavoriteCount": parse_int(attrs.get("FavoriteCount")),
                        }
                    ),
                    "Question",
                )
            )
            owner_user_id = parse_int(attrs.get("OwnerUserId"))
            if owner_user_id is not None and owner_user_id in user_id_set:
                edge_batch.append(
                    (
                        f"u:{owner_user_id}",
                        f"q:{post_id}",
                        _clean_props(
                            {
                                "CreationDate": to_epoch_millis(
                                    parse_datetime(attrs.get("CreationDate"))
                                )
                            }
                        ),
                        "ASKED",
                    )
                )
                edge_counts["ASKED"] += 1
        elif post_type == 2:
            answer_ids.append(post_id)
            answer_id_set.add(post_id)
            max_ids["answer"] = max(max_ids["answer"], post_id)
            answer_batch.append(
                (
                    f"a:{post_id}",
                    _clean_props(
                        {
                            "Id": post_id,
                            "Body": attrs.get("Body"),
                            "Score": parse_int(attrs.get("Score")),
                            "CreationDate": to_epoch_millis(
                                parse_datetime(attrs.get("CreationDate"))
                            ),
                            "CommentCount": parse_int(attrs.get("CommentCount")),
                        }
                    ),
                    "Answer",
                )
            )
            owner_user_id = parse_int(attrs.get("OwnerUserId"))
            parent_id = parse_int(attrs.get("ParentId"))
            if owner_user_id is not None and owner_user_id in user_id_set:
                edge_batch.append(
                    (
                        f"u:{owner_user_id}",
                        f"a:{post_id}",
                        _clean_props(
                            {
                                "CreationDate": to_epoch_millis(
                                    parse_datetime(attrs.get("CreationDate"))
                                )
                            }
                        ),
                        "ANSWERED",
                    )
                )
                edge_counts["ANSWERED"] += 1
            if parent_id is not None and parent_id in question_id_set:
                edge_batch.append((f"q:{parent_id}", f"a:{post_id}", {}, "HAS_ANSWER"))
                edge_counts["HAS_ANSWER"] += 1

        if len(question_batch) >= batch_size:
            flush_nodes(question_batch)
        if len(answer_batch) >= batch_size:
            flush_nodes(answer_batch)
        if len(edge_batch) >= batch_size:
            flush_nodes(question_batch)
            flush_nodes(answer_batch)
            flush_edges(edge_batch)

    flush_nodes(question_batch)
    flush_nodes(answer_batch)

    node_batch = []
    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        badge_id = parse_int(attrs.get("Id"))
        if badge_id is None:
            continue
        badge_ids.append(badge_id)
        max_ids["badge"] = max(max_ids["badge"], badge_id)
        node_batch.append(
            (
                f"b:{badge_id}",
                _clean_props(
                    {
                        "Id": badge_id,
                        "Name": attrs.get("Name"),
                        "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                        "Class": parse_int(attrs.get("Class")),
                    }
                ),
                "Badge",
            )
        )
        user_id = parse_int(attrs.get("UserId"))
        if user_id is not None and user_id in user_id_set:
            edge_batch.append(
                (
                    f"u:{user_id}",
                    f"b:{badge_id}",
                    _clean_props(
                        {
                            "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                            "Class": parse_int(attrs.get("Class")),
                        }
                    ),
                    "EARNED",
                )
            )
            edge_counts["EARNED"] += 1
        if len(node_batch) >= batch_size:
            flush_nodes(node_batch)
        if len(edge_batch) >= batch_size:
            flush_nodes(node_batch)
            flush_edges(edge_batch)

    flush_nodes(node_batch)

    node_batch = []
    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None:
            continue
        comment_ids.append(comment_id)
        max_ids["comment"] = max(max_ids["comment"], comment_id)
        node_batch.append(
            (
                f"c:{comment_id}",
                _clean_props(
                    {
                        "Id": comment_id,
                        "Text": attrs.get("Text"),
                        "Score": parse_int(attrs.get("Score")),
                        "CreationDate": to_epoch_millis(
                            parse_datetime(attrs.get("CreationDate"))
                        ),
                    }
                ),
                "Comment",
            )
        )
        if post_id is not None and post_id in question_id_set:
            edge_batch.append(
                (
                    f"c:{comment_id}",
                    f"q:{post_id}",
                    _clean_props(
                        {
                            "CreationDate": to_epoch_millis(
                                parse_datetime(attrs.get("CreationDate"))
                            ),
                            "Score": parse_int(attrs.get("Score")),
                        }
                    ),
                    "COMMENTED_ON",
                )
            )
            edge_counts["COMMENTED_ON"] += 1
        elif post_id is not None and post_id in answer_id_set:
            edge_batch.append(
                (
                    f"c:{comment_id}",
                    f"a:{post_id}",
                    _clean_props(
                        {
                            "CreationDate": to_epoch_millis(
                                parse_datetime(attrs.get("CreationDate"))
                            ),
                            "Score": parse_int(attrs.get("Score")),
                        }
                    ),
                    "COMMENTED_ON_ANSWER",
                )
            )
            edge_counts["COMMENTED_ON_ANSWER"] += 1

        if len(node_batch) >= batch_size:
            flush_nodes(node_batch)
        if len(edge_batch) >= batch_size:
            flush_nodes(node_batch)
            flush_edges(edge_batch)

    flush_nodes(node_batch)

    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        if question_id is None or question_id not in question_id_set:
            continue
        accepted_answer_id = parse_int(attrs.get("AcceptedAnswerId"))
        if accepted_answer_id is not None and accepted_answer_id in answer_id_set:
            edge_batch.append(
                (f"q:{question_id}", f"a:{accepted_answer_id}", {}, "ACCEPTED_ANSWER")
            )
            edge_counts["ACCEPTED_ANSWER"] += 1
        for tag in parse_tags(attrs.get("Tags")):
            tag_id = tag_name_to_id.get(tag)
            if tag_id is None:
                continue
            edge_batch.append((f"q:{question_id}", f"t:{tag_id}", {}, "TAGGED_WITH"))
            edge_counts["TAGGED_WITH"] += 1
        if len(edge_batch) >= batch_size:
            flush_edges(edge_batch)

    for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
        post_id = parse_int(attrs.get("PostId"))
        related_id = parse_int(attrs.get("RelatedPostId"))
        if post_id is None or related_id is None:
            continue
        if post_id not in question_id_set or related_id not in question_id_set:
            continue
        edge_batch.append(
            (
                f"q:{post_id}",
                f"q:{related_id}",
                _clean_props(
                    {
                        "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                        "CreationDate": to_epoch_millis(
                            parse_datetime(attrs.get("CreationDate"))
                        ),
                    }
                ),
                "LINKED_TO",
            )
        )
        edge_counts["LINKED_TO"] += 1
        if len(edge_batch) >= batch_size:
            flush_edges(edge_batch)

    flush_edges(edge_batch)

    load_stats = {
        "nodes": {
            "User": len(user_ids),
            "Question": len(question_ids),
            "Answer": len(answer_ids),
            "Tag": len(tag_ids),
            "Badge": len(badge_ids),
            "Comment": len(comment_ids),
        },
        "edges": edge_counts,
    }
    load_info = {
        "ids": {
            "users": user_ids,
            "questions": question_ids,
            "answers": answer_ids,
            "tags": tag_ids,
            "badges": badge_ids,
            "comments": comment_ids,
        },
        "max_ids": max_ids,
    }
    return load_info, load_stats


def _create_python_memory_store() -> Dict[str, Dict[str, Any]]:
    return {
        "nodes": {label: {} for label in VERTEX_TYPES},
        "edges": {label: {} for label in EDGE_TYPES},
    }


def _python_memory_add_edge(
    store: Dict[str, Dict[str, Any]],
    edge_type: str,
    from_id: int,
    to_id: int,
    props: Optional[Dict[str, Any]] = None,
) -> None:
    edges = store["edges"][edge_type]
    key = (from_id, to_id)
    existing = edges.get(key)
    if existing is None:
        entry = {"count": 1}
        if props:
            entry.update(props)
        edges[key] = entry
        return
    existing["count"] = int(existing.get("count") or 0) + 1
    if props:
        existing.update(props)


def _python_memory_count_by_type(
    store: Dict[str, Dict[str, Any]],
    vertex_types: List[str],
    edge_types: List[str],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts = {label: len(store["nodes"][label]) for label in vertex_types}
    edge_counts = {
        label: sum(int(v.get("count") or 0) for v in store["edges"][label].values())
        for label in edge_types
    }
    return node_counts, edge_counts


def _persist_python_memory_store(
    store: Dict[str, Dict[str, Any]], output_path: Path
) -> None:
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with open(temp_path, "wb") as handle:
        pickle.dump(store, handle, protocol=pickle.HIGHEST_PROTOCOL)
    temp_path.replace(output_path)


def _load_stackoverflow_python_memory(
    store: Dict[str, Dict[str, Any]],
    data_dir: Path,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    user_ids: List[int] = []
    question_ids: List[int] = []
    answer_ids: List[int] = []
    tag_ids: List[int] = []
    badge_ids: List[int] = []
    comment_ids: List[int] = []

    user_id_set: set[int] = set()
    question_id_set: set[int] = set()
    answer_id_set: set[int] = set()

    tag_name_to_id: Dict[str, int] = {}
    edge_counts = collections.defaultdict(int)
    max_ids = {
        "user": 0,
        "question": 0,
        "answer": 0,
        "tag": 0,
        "badge": 0,
        "comment": 0,
    }

    for attrs in iter_xml_rows(data_dir / "Tags.xml"):
        tag_id = parse_int(attrs.get("Id"))
        tag_name = attrs.get("TagName")
        if tag_id is None or tag_name is None:
            continue
        store["nodes"]["Tag"][tag_id] = {
            "Id": tag_id,
            "TagName": tag_name,
            "Count": parse_int(attrs.get("Count")),
        }
        tag_ids.append(tag_id)
        max_ids["tag"] = max(max_ids["tag"], tag_id)
        tag_name_to_id[tag_name] = tag_id

    for attrs in iter_xml_rows(data_dir / "Users.xml"):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
        store["nodes"]["User"][user_id] = {
            "Id": user_id,
            "DisplayName": attrs.get("DisplayName"),
            "Reputation": parse_int(attrs.get("Reputation")),
            "CreationDate": to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
            "Views": parse_int(attrs.get("Views")),
            "UpVotes": parse_int(attrs.get("UpVotes")),
            "DownVotes": parse_int(attrs.get("DownVotes")),
        }
        user_ids.append(user_id)
        user_id_set.add(user_id)
        max_ids["user"] = max(max_ids["user"], user_id)

    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_id = parse_int(attrs.get("Id"))
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_id is None or post_type is None:
            continue
        if post_type == 1:
            store["nodes"]["Question"][post_id] = {
                "Id": post_id,
                "Title": attrs.get("Title"),
                "Body": attrs.get("Body"),
                "Score": parse_int(attrs.get("Score")),
                "ViewCount": parse_int(attrs.get("ViewCount")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
                "AnswerCount": parse_int(attrs.get("AnswerCount")),
                "CommentCount": parse_int(attrs.get("CommentCount")),
                "FavoriteCount": parse_int(attrs.get("FavoriteCount")),
            }
            question_ids.append(post_id)
            question_id_set.add(post_id)
            max_ids["question"] = max(max_ids["question"], post_id)
        elif post_type == 2:
            store["nodes"]["Answer"][post_id] = {
                "Id": post_id,
                "Body": attrs.get("Body"),
                "Score": parse_int(attrs.get("Score")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
                "CommentCount": parse_int(attrs.get("CommentCount")),
            }
            answer_ids.append(post_id)
            answer_id_set.add(post_id)
            max_ids["answer"] = max(max_ids["answer"], post_id)

    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_id = parse_int(attrs.get("Id"))
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_id is None or post_type is None:
            continue

        if post_type == 1:
            owner_user_id = parse_int(attrs.get("OwnerUserId"))
            if owner_user_id is not None and owner_user_id in user_id_set:
                _python_memory_add_edge(
                    store,
                    "ASKED",
                    owner_user_id,
                    post_id,
                    {
                        "CreationDate": to_epoch_millis(
                            parse_datetime(attrs.get("CreationDate"))
                        )
                    },
                )
                edge_counts["ASKED"] += 1

            accepted_answer_id = parse_int(attrs.get("AcceptedAnswerId"))
            if accepted_answer_id is not None and accepted_answer_id in answer_id_set:
                _python_memory_add_edge(
                    store, "ACCEPTED_ANSWER", post_id, accepted_answer_id
                )
                edge_counts["ACCEPTED_ANSWER"] += 1

            for tag_name in parse_tags(attrs.get("Tags")):
                tag_id = tag_name_to_id.get(tag_name)
                if tag_id is not None:
                    _python_memory_add_edge(store, "TAGGED_WITH", post_id, tag_id)
                    edge_counts["TAGGED_WITH"] += 1

        elif post_type == 2:
            owner_user_id = parse_int(attrs.get("OwnerUserId"))
            if owner_user_id is not None and owner_user_id in user_id_set:
                _python_memory_add_edge(
                    store,
                    "ANSWERED",
                    owner_user_id,
                    post_id,
                    {
                        "CreationDate": to_epoch_millis(
                            parse_datetime(attrs.get("CreationDate"))
                        )
                    },
                )
                edge_counts["ANSWERED"] += 1

            parent_id = parse_int(attrs.get("ParentId"))
            if parent_id is not None and parent_id in question_id_set:
                _python_memory_add_edge(store, "HAS_ANSWER", parent_id, post_id)
                edge_counts["HAS_ANSWER"] += 1

    for attrs in iter_xml_rows(data_dir / "Badges.xml"):
        badge_id = parse_int(attrs.get("Id"))
        user_id = parse_int(attrs.get("UserId"))
        if badge_id is None:
            continue
        badge_props = {
            "Id": badge_id,
            "Name": attrs.get("Name"),
            "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
            "Class": parse_int(attrs.get("Class")),
        }
        store["nodes"]["Badge"][badge_id] = badge_props
        badge_ids.append(badge_id)
        max_ids["badge"] = max(max_ids["badge"], badge_id)

        if user_id is not None and user_id in user_id_set:
            _python_memory_add_edge(
                store,
                "EARNED",
                user_id,
                badge_id,
                {"Date": badge_props.get("Date"), "Class": badge_props.get("Class")},
            )
            edge_counts["EARNED"] += 1

    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None:
            continue
        created_ms = to_epoch_millis(parse_datetime(attrs.get("CreationDate")))
        score = parse_int(attrs.get("Score"))
        store["nodes"]["Comment"][comment_id] = {
            "Id": comment_id,
            "Text": attrs.get("Text"),
            "Score": score,
            "CreationDate": created_ms,
        }
        comment_ids.append(comment_id)
        max_ids["comment"] = max(max_ids["comment"], comment_id)

        if post_id is not None and post_id in question_id_set:
            _python_memory_add_edge(
                store,
                "COMMENTED_ON",
                comment_id,
                post_id,
                {"CreationDate": created_ms, "Score": score},
            )
            edge_counts["COMMENTED_ON"] += 1
        elif post_id is not None and post_id in answer_id_set:
            _python_memory_add_edge(
                store,
                "COMMENTED_ON_ANSWER",
                comment_id,
                post_id,
                {"CreationDate": created_ms, "Score": score},
            )
            edge_counts["COMMENTED_ON_ANSWER"] += 1

    for attrs in iter_xml_rows(data_dir / "PostLinks.xml"):
        post_id = parse_int(attrs.get("PostId"))
        related_id = parse_int(attrs.get("RelatedPostId"))
        if post_id is None or related_id is None:
            continue
        if post_id not in question_id_set or related_id not in question_id_set:
            continue
        _python_memory_add_edge(
            store,
            "LINKED_TO",
            post_id,
            related_id,
            {
                "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            },
        )
        edge_counts["LINKED_TO"] += 1

    load_info = {
        "ids": {
            "users": user_ids,
            "questions": question_ids,
            "answers": answer_ids,
            "tags": tag_ids,
            "badges": badge_ids,
            "comments": comment_ids,
        },
        "max_ids": max_ids,
    }
    load_stats = {
        "nodes": {
            "User": len(user_ids),
            "Question": len(question_ids),
            "Answer": len(answer_ids),
            "Tag": len(tag_ids),
            "Badge": len(badge_ids),
            "Comment": len(comment_ids),
        },
        "edges": {
            edge_type: int(edge_counts.get(edge_type, 0)) for edge_type in EDGE_TYPES
        },
        "indexes": {"id_unique": 0.0},
    }
    return load_info, load_stats


def run_olap_sqlite(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    sqlite_profile: str,
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    db_file = db_path / "sqlite.sqlite"

    conn, sqlite_pragmas = _connect_sqlite(db_file, sqlite_profile)

    print("Creating schema...")
    schema_start = time.time()
    index_time = _create_sqlite_schema(conn)
    schema_total_time = time.time() - schema_start
    schema_time = max(0.0, schema_total_time - index_time)

    print("Loading graph...")
    load_start = time.time()
    _, load_stats = _load_stackoverflow_sqlite(
        conn,
        data_dir,
        batch_size=max(1, batch_size),
    )
    load_time_including_index = time.time() - load_start
    load_time = load_time_including_index

    load_counts_start = time.time()
    load_node_counts_by_type, load_edge_counts_by_type = _count_sqlite_by_type(
        conn,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    load_counts_time = time.time() - load_counts_start

    sqlite_version = conn.execute("SELECT sqlite_version() AS version").fetchone()[0]
    disk_after_load = get_dir_size_bytes(db_path)
    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_time = run_queries(
        lambda cypher: execute_sqlite_olap_query(conn, query_name_from_cypher(cypher)),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(
                lambda cypher: execute_sqlite_olap_query(
                    conn, query_name_from_cypher(cypher)
                )
            )
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    node_counts_by_type, edge_counts_by_type = _count_sqlite_by_type(
        conn,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())
    counts_time = time.time() - counts_start

    conn.close()

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_node_count,
        "load_edge_count": load_edge_count,
        "load_node_counts_by_type": load_node_counts_by_type,
        "load_edge_counts_by_type": load_edge_counts_by_type,
        "counts_time_s": counts_time,
        "node_count": node_count,
        "edge_count": edge_count,
        "node_counts_by_type": node_counts_by_type,
        "edge_counts_by_type": edge_counts_by_type,
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": sqlite_pragmas,
        "sqlite_version": sqlite_version,
    }


def run_olap_duckdb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    db_file = db_path / "duckdb.duckdb"

    conn, duckdb_meta = _connect_duckdb(db_file)

    print("Creating schema...")
    schema_start = time.time()
    index_time = _create_duckdb_schema(conn)
    schema_total_time = time.time() - schema_start
    schema_time = max(0.0, schema_total_time - index_time)

    print("Loading graph...")
    load_start = time.time()
    _, load_stats = _load_stackoverflow_duckdb(
        conn,
        db_path,
        data_dir,
        batch_size=max(1, batch_size),
    )
    load_time_including_index = time.time() - load_start
    load_time = load_time_including_index

    load_counts_start = time.time()
    load_node_counts_by_type, load_edge_counts_by_type = _count_sqlite_by_type(
        conn,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)
    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_time = run_queries(
        lambda cypher: execute_sqlite_olap_query(conn, query_name_from_cypher(cypher)),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(
                lambda cypher: execute_sqlite_olap_query(
                    conn, query_name_from_cypher(cypher)
                )
            )
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    node_counts_by_type, edge_counts_by_type = _count_sqlite_by_type(
        conn,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())
    counts_time = time.time() - counts_start

    conn.close()

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_node_count,
        "load_edge_count": load_edge_count,
        "load_node_counts_by_type": load_node_counts_by_type,
        "load_edge_counts_by_type": load_edge_counts_by_type,
        "counts_time_s": counts_time,
        "node_count": node_count,
        "edge_count": edge_count,
        "node_counts_by_type": node_counts_by_type,
        "edge_counts_by_type": edge_counts_by_type,
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
        "duckdb_version": duckdb_meta.get("duckdb_version"),
        "duckdb_runtime_version": duckdb_meta.get("duckdb_runtime_version"),
    }


def run_olap_graphqlite(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    sqlite_profile: str,
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    graphqlite = get_graphqlite_module()
    if graphqlite is None:
        raise RuntimeError("graphqlite is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    db_file = db_path / "graphqlite.sqlite"

    bootstrap_conn = sqlite3.connect(str(db_file), timeout=30.0)
    sqlite_pragmas = configure_sqlite_profile(bootstrap_conn, sqlite_profile)
    sqlite_version = bootstrap_conn.execute(
        "SELECT sqlite_version() AS version"
    ).fetchone()[0]
    bootstrap_conn.close()

    graph = graphqlite.Graph(str(db_file))

    def execute_cypher(query: str, parameters: Optional[Dict[str, Any]] = None):
        result = graph.connection.cypher(query, parameters)
        if hasattr(result, "to_list"):
            return result.to_list()
        return list(result or [])

    print("Creating schema...")
    schema_time = 0.0

    print("Loading graph...")
    load_start = time.time()
    _, load_stats = _load_stackoverflow_graphqlite_bulk(
        graph,
        data_dir,
        batch_size=max(1, batch_size),
    )
    load_time_including_index = time.time() - load_start
    load_time = load_time_including_index

    load_counts_start = time.time()
    load_node_counts_by_type = {
        label: _row_count_value(
            execute_cypher(f"MATCH (n:{label}) RETURN count(n) AS count")
        )
        for label in VERTEX_TYPES
    }
    load_edge_counts_by_type = {
        label: _row_count_value(
            execute_cypher(f"MATCH ()-[r:{label}]->() RETURN count(r) AS count")
        )
        for label in EDGE_TYPES
    }
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)
    index_time = 0.0
    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_time = run_queries(
        lambda cypher: execute_cypher(cypher),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(lambda cypher: execute_cypher(cypher))
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    node_counts_by_type = {
        label: _row_count_value(
            execute_cypher(f"MATCH (n:{label}) RETURN count(n) AS count")
        )
        for label in VERTEX_TYPES
    }
    edge_counts_by_type = {
        label: _row_count_value(
            execute_cypher(f"MATCH ()-[r:{label}]->() RETURN count(r) AS count")
        )
        for label in EDGE_TYPES
    }
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())
    counts_time = time.time() - counts_start

    graph.close()

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_node_count,
        "load_edge_count": load_edge_count,
        "load_node_counts_by_type": load_node_counts_by_type,
        "load_edge_counts_by_type": load_edge_counts_by_type,
        "counts_time_s": counts_time,
        "node_count": node_count,
        "edge_count": edge_count,
        "node_counts_by_type": node_counts_by_type,
        "edge_counts_by_type": edge_counts_by_type,
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": sqlite_pragmas,
        "sqlite_version": sqlite_version,
    }


def run_olap_python_memory(
    db_path: Path,
    data_dir: Path,
    sqlite_profile: str,
    only_query: Optional[str] = None,
    manual_checks: bool = False,
    query_runs: int = 1,
    query_order: str = "fixed",
    seed: int = 42,
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    snapshot_path = db_path / "python_memory_store.pkl"

    store = _create_python_memory_store()

    print("Creating schema...")
    schema_time = 0.0

    print("Loading graph...")
    load_start = time.time()
    _, load_stats = _load_stackoverflow_python_memory(store, data_dir)
    load_time_including_index = time.time() - load_start
    load_time = load_time_including_index

    _persist_python_memory_store(store, snapshot_path)

    load_counts_start = time.time()
    load_node_counts_by_type, load_edge_counts_by_type = _python_memory_count_by_type(
        store,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    load_counts_time = time.time() - load_counts_start

    disk_after_load = get_dir_size_bytes(db_path)
    index_time = 0.0
    disk_after_index = get_dir_size_bytes(db_path)

    print("Running OLAP queries...")
    query_results, query_time = run_queries(
        lambda cypher: execute_python_memory_olap_query(
            store, query_name_from_cypher(cypher)
        ),
        only_query=only_query,
        query_runs=query_runs,
        query_order=query_order,
        seed=seed,
    )
    manual_results = None
    if manual_checks:
        manual_results = [
            compute_manual_total_comments(
                lambda cypher: execute_python_memory_olap_query(
                    store, query_name_from_cypher(cypher)
                )
            )
        ]

    disk_after_queries = get_dir_size_bytes(db_path)

    counts_start = time.time()
    node_counts_by_type, edge_counts_by_type = _python_memory_count_by_type(
        store,
        VERTEX_TYPES,
        EDGE_TYPES,
    )
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())
    counts_time = time.time() - counts_start

    return {
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "load_time_including_index_s": load_time_including_index,
        "index_time_s": index_time,
        "query_time_s": query_time,
        "load_counts_time_s": load_counts_time,
        "load_node_count": load_node_count,
        "load_edge_count": load_edge_count,
        "load_node_counts_by_type": load_node_counts_by_type,
        "load_edge_counts_by_type": load_edge_counts_by_type,
        "counts_time_s": counts_time,
        "node_count": node_count,
        "edge_count": edge_count,
        "node_counts_by_type": node_counts_by_type,
        "edge_counts_by_type": edge_counts_by_type,
        "load_stats": load_stats,
        "queries": query_results,
        "manual_checks": manual_results,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_index_bytes": disk_after_index,
        "disk_after_queries_bytes": disk_after_queries,
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": {
            "profile": sqlite_profile,
            "storage": "python_memory_snapshot",
        },
        "sqlite_version": sqlite3.sqlite_version,
    }


def write_results(db_path: Path, args: argparse.Namespace, summary: dict):
    if args.run_label:
        results_path = db_path / f"results_{args.run_label}.json"
    else:
        results_path = db_path / "results.json"
    query_telemetry = build_query_telemetry(summary.get("queries", []))
    arcadedb_module, _ = get_arcadedb_module()
    ladybug_module = get_ladybug_module()
    graphqlite_module = get_graphqlite_module()
    duckdb_module = get_duckdb_module()
    payload = {
        "dataset": args.dataset,
        "db": args.db,
        "batch_size": args.batch_size,
        "mem_limit": args.mem_limit,
        "heap_size": args.heap_size_effective,
        "arcadedb_version": (
            getattr(arcadedb_module, "__version__", None)
            if arcadedb_module is not None
            else None
        ),
        "arcadedb_olap_language": summary.get(
            "arcadedb_olap_language",
            (
                args.arcadedb_olap_language
                if args.db in ("arcadedb_sql", "arcadedb_cypher")
                else None
            ),
        ),
        "ladybug_version": (
            getattr(ladybug_module, "__version__", None)
            if ladybug_module is not None
            else None
        ),
        "graphqlite_version": (
            getattr(graphqlite_module, "__version__", None)
            if graphqlite_module is not None
            else None
        ),
        "duckdb_version": summary.get(
            "duckdb_version",
            (
                getattr(duckdb_module, "__version__", None)
                if duckdb_module is not None
                else None
            ),
        ),
        "sqlite_profile": args.sqlite_profile,
        "docker_image": args.docker_image,
        "threads": args.threads,
        "seed": args.seed,
        "run_label": args.run_label,
        "query_runs": args.query_runs,
        "query_order": args.query_order,
        "schema_time_s": summary["schema_time_s"],
        "load_time_s": summary["load_time_s"],
        "load_time_including_index_s": summary.get("load_time_including_index_s"),
        "index_time_s": summary["index_time_s"],
        "query_time_s": summary["query_time_s"],
        "load_counts_time_s": summary.get("load_counts_time_s", 0.0),
        "load_node_count": summary.get("load_node_count"),
        "load_edge_count": summary.get("load_edge_count"),
        "load_node_counts_by_type": summary.get("load_node_counts_by_type"),
        "load_edge_counts_by_type": summary.get("load_edge_counts_by_type"),
        "counts_time_s": summary.get("counts_time_s", 0.0),
        "node_count": summary.get("node_count"),
        "edge_count": summary.get("edge_count"),
        "node_counts_by_type": summary.get("node_counts_by_type"),
        "edge_counts_by_type": summary.get("edge_counts_by_type"),
        "load_stats": summary["load_stats"],
        "queries": summary["queries"],
        "manual_checks": summary.get("manual_checks"),
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
        "benchmark_scope_note": summary.get("benchmark_scope_note"),
        "sqlite_pragmas": summary.get("sqlite_pragmas"),
        "sqlite_version": summary.get("sqlite_version"),
        "duckdb_runtime_version": summary.get(
            "duckdb_runtime_version",
            (
                getattr(duckdb_module, "__version__", None)
                if duckdb_module is not None
                else None
            ),
        ),
    }
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with open(results_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    print(f"Results saved to: {results_path}")


def is_running_in_docker() -> bool:
    if Path("/.dockerenv").exists():
        return True
    try:
        content = Path("/proc/1/cgroup").read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return "docker" in content or "containerd" in content


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


def run_in_docker(args) -> bool:
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
        if arg in {"--docker-image"}:
            skip_next = True
            continue
        filtered_args.append(arg)

    arcadedb_wheel_mount_path = None
    if args.db in ("arcadedb_sql", "arcadedb_cypher"):
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
    if args.db in ("ladybug", "ladybugdb"):
        packages.append("real_ladybug")
    if args.db == "graphqlite":
        packages.append("graphqlite")
    if args.db == "duckdb":
        packages.append("duckdb")

    packages_str = " ".join(packages)

    inner_cmd_parts = []
    python_cmd = "python"
    inner_cmd_parts.append(f"{python_cmd} -m venv /tmp/bench-venv")
    inner_cmd_parts.append(". /tmp/bench-venv/bin/activate")
    inner_cmd_parts.extend(uv_bootstrap_commands(python_cmd))
    inner_cmd_parts.append(f"uv pip install {packages_str}")
    if arcadedb_wheel_mount_path is not None:
        inner_cmd_parts.append(f'uv pip install "{arcadedb_wheel_mount_path}"')
    inner_cmd_parts.append("echo 'Starting benchmark...'")
    inner_cmd_parts.append(
        f"{python_cmd} -u 10_stackoverflow_graph_olap.py {' '.join(filtered_args)}"
    )

    inner_cmd = " && ".join(inner_cmd_parts)

    docker_image = args.docker_image
    if arcadedb_wheel_mount_path is not None and docker_image == "python:3.12-slim":
        docker_image = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"

    cmd = [
        docker,
        "run",
        "--rm",
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

    if user_spec is not None:
        cmd[3:3] = ["-u", user_spec]

    print("Launching Docker container...")
    subprocess.run(cmd, check=True)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Example 10: Stack Overflow Graph (OLAP)",
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(EXPECTED_DATASETS),
        default="stackoverflow-tiny",
        help="Dataset size to use (default: stackoverflow-tiny)",
    )
    parser.add_argument(
        "--db",
        choices=[
            "arcadedb_sql",
            "arcadedb_cypher",
            "ladybug",
            "ladybugdb",
            "graphqlite",
            "duckdb",
            "sqlite",
            "python_memory",
        ],
        default="arcadedb_cypher",
        help="Database to test (default: arcadedb_cypher)",
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
        "--sqlite-profile",
        choices=SQLITE_PROFILE_CHOICES,
        default="olap",
        help="SQLite profile for sqlite/graphqlite backends (default: olap)",
    )
    parser.add_argument(
        "--docker-image",
        type=str,
        default="python:3.12-slim",
        help="Docker image to use (default: python:3.12-slim)",
    )
    parser.add_argument(
        "--only-query",
        type=str,
        default=None,
        help="Run only a single query by name (default: run all)",
    )
    parser.add_argument(
        "--manual-checks",
        action="store_true",
        help="Compute manual cross-check queries for validation",
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
        help="Query execution order across repeated runs (default: fixed)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used for deterministic shuffled query order (default: 42)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to DB directory and result filename",
    )

    args = parser.parse_args()
    if args.run_label:
        args.run_label = args.run_label.strip().replace("/", "-").replace(" ", "_")
    if args.jvm_heap_fraction <= 0 or args.jvm_heap_fraction > 1:
        parser.error("--jvm-heap-fraction must be > 0 and <= 1")

    args.arcadedb_olap_language = None
    if args.db.startswith("arcadedb_"):
        args.arcadedb_olap_language = args.db.removeprefix("arcadedb_")

    ran = run_in_docker(args)
    if ran:
        return

    heap_size = (
        resolve_arcadedb_heap_size(
            args.mem_limit,
            args.jvm_heap_fraction,
        )
        if args.db in ("arcadedb_sql", "arcadedb_cypher")
        else args.mem_limit
    )
    args.heap_size_effective = heap_size
    jvm_kwargs = {"heap_size": heap_size}

    data_dir = Path(__file__).parent / "data" / args.dataset
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Dataset not found: {data_dir}. Run download_data.py first."
        )

    db_name = (
        f"{args.dataset.replace('-', '_')}_graph_olap_{args.db}_"
        f"{mem_limit_tag(args.mem_limit)}"
    )
    if args.run_label:
        db_name = f"{db_name}_{args.run_label}"
    db_path = Path("./my_test_databases") / db_name

    print("=" * 80)
    print("Stack Overflow Graph - OLAP")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"DB: {args.db}")
    if args.db in ("arcadedb_sql", "arcadedb_cypher"):
        print(f"ArcadeDB OLAP language: {args.arcadedb_olap_language}")
    if args.db in ("sqlite", "graphqlite", "python_memory"):
        print(f"SQLite profile: {args.sqlite_profile}")
    print(f"Batch size: {args.batch_size}")
    print(f"Query runs: {args.query_runs}")
    print(f"Query order: {args.query_order}")
    print(f"Seed: {args.seed}")
    print(f"JVM heap size: {heap_size}")
    print(f"DB path: {db_path}")
    print(BENCHMARK_SCOPE_NOTE)
    print()

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    if args.db in ("arcadedb_sql", "arcadedb_cypher"):
        summary = run_olap_arcadedb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            threads=args.threads,
            jvm_kwargs=jvm_kwargs,
            dataset_name=args.dataset,
            olap_language=args.arcadedb_olap_language,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    elif args.db in ("ladybug", "ladybugdb"):
        summary = run_olap_ladybug(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    elif args.db == "sqlite":
        summary = run_olap_sqlite(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            sqlite_profile=args.sqlite_profile,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    elif args.db == "duckdb":
        summary = run_olap_duckdb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    elif args.db == "graphqlite":
        summary = run_olap_graphqlite(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            sqlite_profile=args.sqlite_profile,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    elif args.db == "python_memory":
        summary = run_olap_python_memory(
            db_path=db_path,
            data_dir=data_dir,
            sqlite_profile=args.sqlite_profile,
            only_query=args.only_query,
            manual_checks=args.manual_checks,
            query_runs=args.query_runs,
            query_order=args.query_order,
            seed=args.seed,
        )
    else:
        raise NotImplementedError(f"Unsupported db: {args.db}")

    total_time = time.perf_counter() - start_time
    stop_event.set()
    rss_thread.join()

    summary["rss_peak_kb"] = rss_state["max_kb"]
    summary["total_time_s"] = total_time

    print("\nResults")
    print("-" * 80)
    print(f"Schema time: {summary['schema_time_s']:.2f}s")
    print(f"Load time: {summary['load_time_s']:.2f}s")
    print(f"Index time: {summary['index_time_s']:.2f}s")
    print(f"Query time: {summary['query_time_s']:.2f}s")
    print(f"Total time: {summary['total_time_s']:.2f}s")
    print(f"Disk after load: {format_bytes_binary(summary['disk_after_load_bytes'])}")
    print(f"Disk after index: {format_bytes_binary(summary['disk_after_index_bytes'])}")
    print(
        f"Disk after queries: {format_bytes_binary(summary['disk_after_queries_bytes'])}"
    )
    print(f"Peak RSS: {summary['rss_peak_kb'] / 1024:.1f} MB")
    print(BENCHMARK_SCOPE_NOTE)
    print()

    for item in summary["queries"]:
        print(
            f"{item['name']}: {item['elapsed_s']:.3f}s, "
            f"rows={item['row_count']}, hash={item['result_hash'][:12]}"
        )

    print()
    summary["benchmark_scope_note"] = BENCHMARK_SCOPE_NOTE
    write_results(db_path, args, summary)


if __name__ == "__main__":
    main()
