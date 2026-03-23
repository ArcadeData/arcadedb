#!/usr/bin/env python3
"""
Example 09: Stack Overflow Graph (OLTP)

Builds a Stack Overflow property graph (Phase 2 schema) and runs an OLTP
workload on ArcadeDB and LadybugDB with the same operation mix as Example 08.

Threading note: Databases can exhibit different scaling behavior as thread
count increases. For cross-database comparability, run with --threads 1.
"""

import argparse
import collections
import concurrent.futures
import contextlib
import csv
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

BENCHMARK_SCOPE_NOTE = (
    "Scope: OLTP throughput/stability benchmark. "
    "ArcadeDB, Ladybug, GraphQLite, SQLite native, and Python in-memory use the "
    "same logical schema, "
    "ID indexing, ingestion dataset/relationships, and CRUD operation mix. "
    "Query language and execution path remain engine-native."
)

DEFAULT_OLTP_MIX = {
    "read": 0.60,
    "update": 0.20,
    "insert": 0.10,
    "delete": 0.10,
}
SQLITE_PROFILE_CHOICES = ["fair", "perf", "olap"]


def mem_limit_tag(mem_limit: str) -> str:
    normalized = re.sub(r"[^0-9a-z]+", "", mem_limit.lower())
    return f"mem{normalized}" if normalized else "memdefault"


READ_TARGET_KINDS = [
    "user",
    "question",
    "answer",
    "badge",
    "tag",
    "comment",
    "edge_sample",
]

UPDATE_TARGET_KINDS = [
    "question",
    "answer",
    "comment",
    "user",
    "tag",
    "asked_edge",
    "answered_edge",
    "commented_on_edge",
    "commented_on_answer_edge",
    "earned_edge",
    "linked_to_edge",
]

INSERT_TARGET_KINDS = [
    "user_question",
    "answer",
    "comment",
    "badge",
    "tag_link",
    "post_link",
    "accepted_answer",
]

DELETE_TARGET_KINDS = [
    "question",
    "answer",
    "comment",
    "badge",
    "user",
    "tag",
    "asked_edge",
    "answered_edge",
    "has_answer_edge",
    "accepted_answer_edge",
    "tagged_with_edge",
    "commented_on_edge",
    "commented_on_answer_edge",
    "earned_edge",
    "linked_to_edge",
]

ARCADE_VERTEX_TYPES = ["User", "Question", "Answer", "Tag", "Badge", "Comment"]
ARCADE_EDGE_TYPES = [
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
LADYBUG_VERTEX_TYPES = ARCADE_VERTEX_TYPES
LADYBUG_EDGE_TYPES = [
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


def choose_ops(count: int, mix: Dict[str, float], seed: int) -> List[str]:
    rng = random.Random(seed)
    return rng.choices(
        population=["read", "update", "insert", "delete"],
        weights=[mix["read"], mix["update"], mix["insert"], mix["delete"]],
        k=count,
    )


def run_with_retry(
    action,
    error_class,
    max_retries: int = 100,
    base_delay: float = 0.01,
    max_delay: float = 0.5,
):
    delay = base_delay
    for attempt in range(max_retries):
        try:
            return action()
        except error_class as exc:
            if is_transient_record_not_found_error(exc):
                raise
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay = min(max_delay, delay * 2)


def is_transient_record_not_found_error(exc: Exception) -> bool:
    def looks_like_not_found(text: str) -> bool:
        lower = (text or "").lower()
        if "recordnotfoundexception" in lower:
            return True
        if "record #" in lower and "not found" in lower:
            return True
        if "record not found" in lower:
            return True
        if "commandexecutionexception" in lower and "not found" in lower:
            return True
        return False

    visited = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in visited:
        visited.add(id(current))

        if looks_like_not_found(str(current)):
            return True

        for arg in getattr(current, "args", ()):
            if looks_like_not_found(str(arg)):
                return True

        current = getattr(current, "__cause__", None) or getattr(
            current,
            "__context__",
            None,
        )

    return False


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


def create_arcadedb_indexes(db):
    db.command("sql", "CREATE INDEX ON User (Id) UNIQUE_HASH")
    db.command("sql", "CREATE INDEX ON Question (Id) UNIQUE_HASH")
    db.command("sql", "CREATE INDEX ON Answer (Id) UNIQUE_HASH")
    db.command("sql", "CREATE INDEX ON Tag (Id) UNIQUE_HASH")
    db.command("sql", "CREATE INDEX ON Badge (Id) UNIQUE_HASH")
    db.command("sql", "CREATE INDEX ON Comment (Id) UNIQUE_HASH")

    db.async_executor().wait_completion()


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


def format_counts_by_type(label: str, counts: Dict[str, int]) -> str:
    parts = [f"{name}={value:,}" for name, value in counts.items()]
    return f"{label}: " + ", ".join(parts)


VERIFY_SINGLE_THREAD_PROFILE = "v1_single_thread_series"


def normalize_count_dict(values: Optional[Dict[str, int]]) -> Dict[str, int]:
    if not isinstance(values, dict):
        return {}
    normalized: Dict[str, int] = {}
    for key, value in values.items():
        try:
            normalized[str(key)] = int(value)
        except Exception:
            normalized[str(key)] = 0
    return normalized


def assert_count_dicts_match(
    expected: Dict[str, int], actual: Dict[str, int], section: str, db_label: str
):
    diffs = []
    keys = sorted(set(expected.keys()) | set(actual.keys()))
    for key in keys:
        exp = int(expected.get(key, 0))
        got = int(actual.get(key, 0))
        if exp != got:
            diffs.append(f"{section}.{key}: expected={exp}, actual={got}")
    if diffs:
        raise RuntimeError(
            f"Deterministic single-thread CRUD verification failed for {db_label}:\n- "
            + "\n- ".join(diffs)
        )


def verify_single_thread_series(
    args: argparse.Namespace,
    summary: Dict[str, Any],
    db_label: str,
    db_path: Path,
) -> Optional[Path]:
    verify_dir = db_path
    verify_dir.mkdir(parents=True, exist_ok=True)
    dataset_tag = args.dataset.replace("-", "_")
    baseline_path = (
        verify_dir
        / f"verify_09_graph_oltp_{db_label}_{dataset_tag}_tx{args.transactions}_seed{args.seed}.json"
    )

    current_payload = {
        "verification_profile": VERIFY_SINGLE_THREAD_PROFILE,
        "dataset": args.dataset,
        "transactions": int(args.transactions),
        "seed": int(args.seed),
        "threads": int(args.threads),
        "node_count": int(summary.get("node_count") or 0),
        "edge_count": int(summary.get("edge_count") or 0),
        "node_counts_by_type": normalize_count_dict(summary.get("node_counts_by_type")),
        "edge_counts_by_type": normalize_count_dict(summary.get("edge_counts_by_type")),
        "op_counts": normalize_count_dict(summary.get("op_counts")),
    }

    def write_baseline() -> None:
        baseline_payload = {
            "meta": {
                "created_at_utc": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "source_db": db_label,
            },
            "baseline": current_payload,
        }
        with open(baseline_path, "w", encoding="utf-8") as handle:
            json.dump(baseline_payload, handle, indent=2, sort_keys=True)

    if not baseline_path.exists():
        write_baseline()
        return baseline_path

    with open(baseline_path, "r", encoding="utf-8") as handle:
        existing = json.load(handle)
    baseline = (existing or {}).get("baseline") or {}

    if baseline.get("verification_profile") != VERIFY_SINGLE_THREAD_PROFILE:
        write_baseline()
        return baseline_path

    if baseline.get("dataset") != current_payload["dataset"]:
        raise RuntimeError(
            "Deterministic verification baseline dataset mismatch: "
            f"expected={baseline.get('dataset')} actual={current_payload['dataset']}"
        )
    if int(baseline.get("transactions") or 0) != current_payload["transactions"]:
        raise RuntimeError(
            "Deterministic verification baseline transactions mismatch: "
            f"expected={baseline.get('transactions')} actual={current_payload['transactions']}"
        )
    if int(baseline.get("seed") or 0) != current_payload["seed"]:
        raise RuntimeError(
            "Deterministic verification baseline seed mismatch: "
            f"expected={baseline.get('seed')} actual={current_payload['seed']}"
        )

    baseline_node_count = int(baseline.get("node_count") or 0)
    baseline_edge_count = int(baseline.get("edge_count") or 0)
    if baseline_node_count != current_payload["node_count"]:
        raise RuntimeError(
            "Deterministic single-thread CRUD verification failed "
            f"for {db_label}: node_count expected={baseline_node_count}, actual={current_payload['node_count']}"
        )
    if baseline_edge_count != current_payload["edge_count"]:
        raise RuntimeError(
            "Deterministic single-thread CRUD verification failed "
            f"for {db_label}: edge_count expected={baseline_edge_count}, actual={current_payload['edge_count']}"
        )

    assert_count_dicts_match(
        normalize_count_dict(baseline.get("node_counts_by_type")),
        current_payload["node_counts_by_type"],
        "node_counts_by_type",
        db_label,
    )
    assert_count_dicts_match(
        normalize_count_dict(baseline.get("edge_counts_by_type")),
        current_payload["edge_counts_by_type"],
        "edge_counts_by_type",
        db_label,
    )
    assert_count_dicts_match(
        normalize_count_dict(baseline.get("op_counts")),
        current_payload["op_counts"],
        "op_counts",
        db_label,
    )

    return baseline_path


def count_arcadedb_by_type(
    db, vertex_types: List[str], edge_types: List[str]
) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts: Dict[str, int] = {}
    edge_counts: Dict[str, int] = {}
    for label in vertex_types:
        rows = db.query("sql", f"SELECT count(*) AS count FROM {label}").to_list()
        node_counts[label] = int(rows[0].get("count", 0)) if rows else 0
    for label in edge_types:
        rows = db.query("sql", f"SELECT count(*) AS count FROM {label}").to_list()
        edge_counts[label] = int(rows[0].get("count", 0)) if rows else 0
    return node_counts, edge_counts


def count_ladybug_by_type(
    conn, vertex_types: List[str], edge_types: List[str]
) -> Tuple[Dict[str, int], Dict[str, int]]:
    node_counts: Dict[str, int] = {}
    edge_counts: Dict[str, int] = {}
    for label in vertex_types:
        rows = conn.execute(f"MATCH (n:{label}) RETURN count(n) AS count").get_all()
        node_counts[label] = int(rows[0][0]) if rows else 0
    for label in edge_types:
        rows = conn.execute(
            f"MATCH ()-[r:{label}]->() RETURN count(r) AS count"
        ).get_all()
        edge_counts[label] = int(rows[0][0]) if rows else 0
    return node_counts, edge_counts


def ladybug_insert_vertices(conn, label: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    statements = []
    for row in rows:
        props = format_cypher_props(row)
        statements.append(f"CREATE (n:{label} {props})")
    conn.execute(";".join(statements))


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
                continue
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


def load_graph_arcadedb(
    db,
    data_dir: Path,
    batch_size: int,
    graph_parallelism: int,
) -> Tuple[dict, dict]:
    stats = {"nodes": {}, "edges": {}}
    ids = {
        "users": [],
        "questions": [],
        "answers": [],
        "tags": [],
        "badges": [],
        "comments": [],
    }
    max_ids = {
        "user": 0,
        "question": 0,
        "answer": 0,
        "tag": 0,
        "badge": 0,
        "comment": 0,
    }
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
        ids["tags"].append(tag_id)
        max_ids["tag"] = max(max_ids["tag"], tag_id)
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
            max_ids["answer"] = max(max_ids["answer"], post_id)
            ids["answers"].append(post_id)
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
        ids["badges"].append(badge_id)
        max_ids["badge"] = max(max_ids["badge"], badge_id)
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
        ids["comments"].append(comment_id)
        max_ids["comment"] = max(max_ids["comment"], comment_id)
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

    index_start = time.time()
    create_arcadedb_indexes(db)
    stats["indexes"] = {"id_unique": time.time() - index_start}

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

    return stats, {"ids": ids, "max_ids": max_ids}


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
    ids = {
        "users": [],
        "questions": [],
        "answers": [],
        "tags": [],
        "badges": [],
        "comments": [],
    }
    max_ids = {
        "user": 0,
        "question": 0,
        "answer": 0,
        "tag": 0,
        "badge": 0,
        "comment": 0,
    }
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
        ids["tags"].append(tag_id)
        max_ids["tag"] = max(max_ids["tag"], tag_id)
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
            max_ids["answer"] = max(max_ids["answer"], post_id)
            ids["answers"].append(post_id)
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
        ids["badges"].append(badge_id)
        max_ids["badge"] = max(max_ids["badge"], badge_id)
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
        ids["comments"].append(comment_id)
        max_ids["comment"] = max(max_ids["comment"], comment_id)
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


def run_graph_oltp_arcadedb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
    jvm_kwargs: dict,
    oltp_language: str = "cypher",
) -> dict:
    arcadedb, arcade_error = get_arcadedb_module()
    if arcadedb is None or arcade_error is None:
        raise RuntimeError("arcadedb-embedded is not installed")

    if db_path.exists():
        shutil.rmtree(db_path)

    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)

    print("Creating schema...")
    schema_start = time.time()
    create_arcadedb_schema(db)
    schema_time = time.time() - schema_start

    print("Loading graph...")
    load_start = time.time()
    load_stats, load_info = load_graph_arcadedb(db, data_dir, batch_size, threads)
    load_time = time.time() - load_start

    disk_after_load = get_dir_size_bytes(db_path)

    load_counts_start = time.time()
    load_node_counts_by_type, load_edge_counts_by_type = count_arcadedb_by_type(
        db,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )
    load_counts_time = time.time() - load_counts_start
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    print(
        "Load counts: "
        f"nodes={load_node_count:,}, "
        f"edges={load_edge_count:,} "
        f"(time={load_counts_time:.2f}s)"
    )
    print(format_counts_by_type("Load nodes by type", load_node_counts_by_type))
    print(format_counts_by_type("Load edges by type", load_edge_counts_by_type))

    id_lock = threading.Lock()
    # No global write serialization lock: allow concurrent C/U/D operations.
    write_lock = contextlib.nullcontext()
    user_ids = load_info["ids"]["users"]
    question_ids = load_info["ids"]["questions"]
    answer_ids = load_info["ids"]["answers"]
    tag_ids = load_info["ids"]["tags"]
    badge_ids = load_info["ids"]["badges"]
    comment_ids = load_info["ids"]["comments"]
    next_user_id = load_info["max_ids"]["user"] + 1
    next_question_id = load_info["max_ids"]["question"] + 1
    next_answer_id = load_info["max_ids"]["answer"] + 1
    next_badge_id = load_info["max_ids"]["badge"] + 1
    next_comment_id = load_info["max_ids"]["comment"] + 1
    arcadedb_mode = (oltp_language or "cypher").strip().lower()

    def worker(ops: List[str], worker_id: int) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        nonlocal next_user_id
        nonlocal next_question_id
        nonlocal next_answer_id
        nonlocal next_badge_id
        nonlocal next_comment_id

        for op in ops:
            start_time = time.perf_counter()

            if arcadedb_mode == "sql":
                try:
                    if op == "read":
                        read_kind = rng.choice(READ_TARGET_KINDS)
                        if read_kind == "edge_sample":
                            edge_type = rng.choice(ARCADE_EDGE_TYPES)
                            db.query(
                                "sql", f"SELECT FROM {edge_type} LIMIT 1"
                            ).to_list()
                        else:
                            with id_lock:
                                if read_kind == "user":
                                    target_id = (
                                        rng.choice(user_ids) if user_ids else None
                                    )
                                    target_type = "User"
                                elif read_kind == "question":
                                    target_id = (
                                        rng.choice(question_ids)
                                        if question_ids
                                        else None
                                    )
                                    target_type = "Question"
                                elif read_kind == "answer":
                                    target_id = (
                                        rng.choice(answer_ids) if answer_ids else None
                                    )
                                    target_type = "Answer"
                                elif read_kind == "tag":
                                    target_id = rng.choice(tag_ids) if tag_ids else None
                                    target_type = "Tag"
                                elif read_kind == "comment":
                                    target_id = (
                                        rng.choice(comment_ids) if comment_ids else None
                                    )
                                    target_type = "Comment"
                                else:
                                    target_id = (
                                        rng.choice(badge_ids) if badge_ids else None
                                    )
                                    target_type = "Badge"

                            if target_id is not None:
                                db.query(
                                    "sql",
                                    f"SELECT Id FROM {target_type} WHERE Id = :id LIMIT 1",
                                    {"id": target_id},
                                ).to_list()

                    elif op == "update":
                        update_kind = rng.choice(
                            ["question", "answer", "comment", "user", "tag"]
                        )
                        with id_lock:
                            if update_kind == "question":
                                target_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                            elif update_kind == "answer":
                                target_id = (
                                    rng.choice(answer_ids) if answer_ids else None
                                )
                            elif update_kind == "comment":
                                target_id = (
                                    rng.choice(comment_ids) if comment_ids else None
                                )
                            elif update_kind == "tag":
                                target_id = rng.choice(tag_ids) if tag_ids else None
                            else:
                                target_id = rng.choice(user_ids) if user_ids else None

                        if target_id is not None:
                            with db.transaction():
                                if update_kind == "question":
                                    db.command(
                                        "sql",
                                        "UPDATE Question SET Score = coalesce(Score, 0) + 1 WHERE Id = :id",
                                        {"id": target_id},
                                    )
                                elif update_kind == "answer":
                                    db.command(
                                        "sql",
                                        "UPDATE Answer SET Score = coalesce(Score, 0) + 1 WHERE Id = :id",
                                        {"id": target_id},
                                    )
                                elif update_kind == "comment":
                                    db.command(
                                        "sql",
                                        "UPDATE Comment SET Score = coalesce(Score, 0) + 1 WHERE Id = :id",
                                        {"id": target_id},
                                    )
                                elif update_kind == "tag":
                                    db.command(
                                        "sql",
                                        "UPDATE Tag SET Count = coalesce(Count, 0) + 1 WHERE Id = :id",
                                        {"id": target_id},
                                    )
                                else:
                                    db.command(
                                        "sql",
                                        "UPDATE User SET Reputation = coalesce(Reputation, 0) + 1 WHERE Id = :id",
                                        {"id": target_id},
                                    )

                    elif op == "insert":
                        insert_kind = rng.choice(INSERT_TARGET_KINDS)
                        now_ms = int(time.time() * 1000)
                        new_user_id: Optional[int] = None
                        new_question_id: Optional[int] = None
                        new_answer_id: Optional[int] = None
                        new_comment_id: Optional[int] = None
                        new_badge_id: Optional[int] = None
                        user_id: Optional[int] = None
                        question_id: Optional[int] = None
                        tag_id: Optional[int] = None
                        second_question_id: Optional[int] = None
                        answer_id: Optional[int] = None
                        target_kind: Optional[str] = None
                        target_id: Optional[int] = None

                        with id_lock:
                            if insert_kind == "user_question":
                                new_user_id = next_user_id
                                next_user_id += 1
                                new_question_id = next_question_id
                                next_question_id += 1
                            elif insert_kind == "answer":
                                new_answer_id = next_answer_id
                                next_answer_id += 1
                                user_id = rng.choice(user_ids) if user_ids else None
                                question_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                            elif insert_kind == "comment":
                                new_comment_id = next_comment_id
                                next_comment_id += 1
                                if question_ids and (
                                    not answer_ids or rng.random() < 0.6
                                ):
                                    target_kind = "question"
                                    target_id = rng.choice(question_ids)
                                elif answer_ids:
                                    target_kind = "answer"
                                    target_id = rng.choice(answer_ids)
                            elif insert_kind == "tag_link":
                                question_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                                tag_id = rng.choice(tag_ids) if tag_ids else None
                            elif insert_kind == "post_link":
                                question_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                                second_question_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                            elif insert_kind == "accepted_answer":
                                question_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                                answer_id = (
                                    rng.choice(answer_ids) if answer_ids else None
                                )
                            else:
                                new_badge_id = next_badge_id
                                next_badge_id += 1
                                user_id = rng.choice(user_ids) if user_ids else None

                        with db.transaction():
                            if (
                                insert_kind == "user_question"
                                and new_user_id is not None
                                and new_question_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "INSERT INTO User SET Id = :id, DisplayName = 'Synthetic', Reputation = 0, CreationDate = :ts",
                                    {"id": new_user_id, "ts": now_ms},
                                )
                                db.command(
                                    "sql",
                                    "INSERT INTO Question SET Id = :id, Title = 'Synthetic', Body = 'Synthetic body', Score = 0, CreationDate = :ts",
                                    {"id": new_question_id, "ts": now_ms},
                                )
                                db.command(
                                    "sql",
                                    "CREATE EDGE ASKED FROM (SELECT FROM User WHERE Id = :uid LIMIT 1) TO (SELECT FROM Question WHERE Id = :qid LIMIT 1) SET CreationDate = :ts",
                                    {
                                        "uid": new_user_id,
                                        "qid": new_question_id,
                                        "ts": now_ms,
                                    },
                                )
                            elif (
                                insert_kind == "answer"
                                and new_answer_id is not None
                                and user_id is not None
                                and question_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "INSERT INTO Answer SET Id = :id, Body = 'Synthetic answer', Score = 0, CreationDate = :ts, CommentCount = 0",
                                    {"id": new_answer_id, "ts": now_ms},
                                )
                                db.command(
                                    "sql",
                                    "CREATE EDGE ANSWERED FROM (SELECT FROM User WHERE Id = :uid LIMIT 1) TO (SELECT FROM Answer WHERE Id = :aid LIMIT 1) SET CreationDate = :ts",
                                    {
                                        "uid": user_id,
                                        "aid": new_answer_id,
                                        "ts": now_ms,
                                    },
                                )
                                db.command(
                                    "sql",
                                    "CREATE EDGE HAS_ANSWER FROM (SELECT FROM Question WHERE Id = :qid LIMIT 1) TO (SELECT FROM Answer WHERE Id = :aid LIMIT 1)",
                                    {"qid": question_id, "aid": new_answer_id},
                                )
                            elif (
                                insert_kind == "comment"
                                and new_comment_id is not None
                                and target_kind is not None
                                and target_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "INSERT INTO Comment SET Id = :id, Text = 'Synthetic comment', Score = 0, CreationDate = :ts",
                                    {"id": new_comment_id, "ts": now_ms},
                                )
                                if target_kind == "question":
                                    db.command(
                                        "sql",
                                        "CREATE EDGE COMMENTED_ON FROM (SELECT FROM Comment WHERE Id = :cid LIMIT 1) TO (SELECT FROM Question WHERE Id = :qid LIMIT 1) SET CreationDate = :ts, Score = 0",
                                        {
                                            "cid": new_comment_id,
                                            "qid": target_id,
                                            "ts": now_ms,
                                        },
                                    )
                                else:
                                    db.command(
                                        "sql",
                                        "CREATE EDGE COMMENTED_ON_ANSWER FROM (SELECT FROM Comment WHERE Id = :cid LIMIT 1) TO (SELECT FROM Answer WHERE Id = :aid LIMIT 1) SET CreationDate = :ts, Score = 0",
                                        {
                                            "cid": new_comment_id,
                                            "aid": target_id,
                                            "ts": now_ms,
                                        },
                                    )
                            elif (
                                insert_kind == "tag_link"
                                and question_id is not None
                                and tag_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE Id = :qid LIMIT 1) TO (SELECT FROM Tag WHERE Id = :tid LIMIT 1)",
                                    {"qid": question_id, "tid": tag_id},
                                )
                            elif (
                                insert_kind == "post_link"
                                and question_id is not None
                                and second_question_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "CREATE EDGE LINKED_TO FROM (SELECT FROM Question WHERE Id = :qid LIMIT 1) TO (SELECT FROM Question WHERE Id = :rid LIMIT 1) SET LinkTypeId = 1, CreationDate = :ts",
                                    {
                                        "qid": question_id,
                                        "rid": second_question_id,
                                        "ts": now_ms,
                                    },
                                )
                            elif (
                                insert_kind == "accepted_answer"
                                and question_id is not None
                                and answer_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "CREATE EDGE ACCEPTED_ANSWER FROM (SELECT FROM Question WHERE Id = :qid LIMIT 1) TO (SELECT FROM Answer WHERE Id = :aid LIMIT 1)",
                                    {"qid": question_id, "aid": answer_id},
                                )
                            elif (
                                insert_kind == "badge"
                                and new_badge_id is not None
                                and user_id is not None
                            ):
                                db.command(
                                    "sql",
                                    "INSERT INTO Badge SET Id = :id, Name = 'SyntheticBadge', Date = :ts, Class = 1",
                                    {"id": new_badge_id, "ts": now_ms},
                                )
                                db.command(
                                    "sql",
                                    "CREATE EDGE EARNED FROM (SELECT FROM User WHERE Id = :uid LIMIT 1) TO (SELECT FROM Badge WHERE Id = :bid LIMIT 1) SET Date = :ts, Class = 1",
                                    {"uid": user_id, "bid": new_badge_id, "ts": now_ms},
                                )

                        with id_lock:
                            if new_user_id is not None:
                                user_ids.append(new_user_id)
                            if new_question_id is not None:
                                question_ids.append(new_question_id)
                            if new_answer_id is not None:
                                answer_ids.append(new_answer_id)
                            if new_comment_id is not None:
                                comment_ids.append(new_comment_id)
                            if new_badge_id is not None:
                                badge_ids.append(new_badge_id)

                    elif op == "delete":
                        delete_kind = rng.choice(
                            ["question", "answer", "comment", "badge", "user", "tag"]
                        )
                        with id_lock:
                            if delete_kind == "question":
                                target_id = (
                                    rng.choice(question_ids) if question_ids else None
                                )
                                target_type = "Question"
                                target_list = question_ids
                            elif delete_kind == "answer":
                                target_id = (
                                    rng.choice(answer_ids) if answer_ids else None
                                )
                                target_type = "Answer"
                                target_list = answer_ids
                            elif delete_kind == "comment":
                                target_id = (
                                    rng.choice(comment_ids) if comment_ids else None
                                )
                                target_type = "Comment"
                                target_list = comment_ids
                            elif delete_kind == "badge":
                                target_id = rng.choice(badge_ids) if badge_ids else None
                                target_type = "Badge"
                                target_list = badge_ids
                            elif delete_kind == "tag":
                                target_id = rng.choice(tag_ids) if tag_ids else None
                                target_type = "Tag"
                                target_list = tag_ids
                            else:
                                target_id = rng.choice(user_ids) if user_ids else None
                                target_type = "User"
                                target_list = user_ids

                        if target_id is not None:
                            with db.transaction():
                                db.command(
                                    "sql",
                                    f"DELETE FROM {target_type} WHERE Id = :id",
                                    {"id": target_id},
                                )
                            with id_lock:
                                try:
                                    target_list.remove(target_id)
                                except ValueError:
                                    pass
                except Exception as exc:
                    if not is_transient_record_not_found_error(exc):
                        raise

                elapsed = time.perf_counter() - start_time
                latencies[op].append(elapsed)
                continue

            if op == "read":
                read_kind = rng.choice(READ_TARGET_KINDS)
                try:
                    if read_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (u:User {Id: %d})-[:ASKED|ANSWERED]->(p)
                                RETURN p.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                    elif read_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (q:Question {Id: %d})-[:TAGGED_WITH]->(t:Tag)
                                RETURN t.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                    elif read_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (a:Answer {Id: %d})<-[:COMMENTED_ON_ANSWER]-(c:Comment)
                                RETURN c.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                    elif read_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag {Id: %d})
                                RETURN q.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                    elif read_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (c:Comment {Id: %d})-[r:COMMENTED_ON|COMMENTED_ON_ANSWER]->(p)
                                RETURN p.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                    elif read_kind == "edge_sample":
                        db.query(
                            "opencypher",
                            """
                            MATCH ()-[r:ASKED|ANSWERED|HAS_ANSWER|ACCEPTED_ANSWER|TAGGED_WITH|COMMENTED_ON|COMMENTED_ON_ANSWER|EARNED|LINKED_TO]->()
                            RETURN r
                            LIMIT 1
                            """,
                        ).to_list()
                    else:
                        with id_lock:
                            target_id = rng.choice(badge_ids) if badge_ids else None
                        if target_id is not None:
                            db.query(
                                "opencypher",
                                """
                                MATCH (u:User)-[:EARNED]->(b:Badge {Id: %d})
                                RETURN u.Id
                                LIMIT 1
                                """ % target_id,
                            ).to_list()
                except Exception as exc:
                    if not is_transient_record_not_found_error(exc):
                        raise
            elif op == "update":
                with write_lock:
                    update_kind = rng.choice(UPDATE_TARGET_KINDS)
                    with id_lock:
                        if update_kind == "question":
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif update_kind == "answer":
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        elif update_kind == "comment":
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        elif update_kind == "tag":
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        else:
                            target_id = rng.choice(user_ids) if user_ids else None

                    if target_id is not None:

                        def do_update():
                            with db.transaction():
                                if update_kind == "question":
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (q:Question {Id: %d})
                                        SET q.Score = coalesce(q.Score, 0) + 1
                                        """ % target_id,
                                    )
                                elif update_kind == "answer":
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (a:Answer {Id: %d})
                                        SET a.Score = coalesce(a.Score, 0) + 1
                                        """ % target_id,
                                    )
                                elif update_kind == "comment":
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (c:Comment {Id: %d})
                                        SET c.Score = coalesce(c.Score, 0) + 1
                                        """ % target_id,
                                    )
                                elif update_kind == "tag":
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (t:Tag {Id: %d})
                                        SET t.Count = coalesce(t.Count, 0) + 1
                                        """ % target_id,
                                    )
                                else:
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (u:User {Id: %d})
                                        SET u.Reputation = coalesce(u.Reputation, 0) + 1
                                        """ % target_id,
                                    )

                        try:
                            run_with_retry(do_update, arcade_error)
                        except arcade_error as exc:
                            if not is_transient_record_not_found_error(exc):
                                raise
                    elif update_kind == "asked_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if user_id is not None and question_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (u:User {Id: %d})-[r:ASKED]->(q:Question {Id: %d})
                                        SET r.CreationDate = coalesce(r.CreationDate, 0) + 1
                                        """ % (user_id, question_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif update_kind == "answered_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if user_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (u:User {Id: %d})-[r:ANSWERED]->(a:Answer {Id: %d})
                                        SET r.CreationDate = coalesce(r.CreationDate, 0) + 1
                                        """ % (user_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif update_kind == "commented_on_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if comment_id is not None and question_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (c:Comment {Id: %d})-[r:COMMENTED_ON]->(q:Question {Id: %d})
                                        SET r.Score = coalesce(r.Score, 0) + 1
                                        """ % (comment_id, question_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif update_kind == "commented_on_answer_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if comment_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (c:Comment {Id: %d})-[r:COMMENTED_ON_ANSWER]->(a:Answer {Id: %d})
                                        SET r.Score = coalesce(r.Score, 0) + 1
                                        """ % (comment_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif update_kind == "earned_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            badge_id = rng.choice(badge_ids) if badge_ids else None
                        if user_id is not None and badge_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (u:User {Id: %d})-[r:EARNED]->(b:Badge {Id: %d})
                                        SET r.Class = coalesce(r.Class, 0) + 1
                                        """ % (user_id, badge_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif update_kind == "linked_to_edge":
                        with id_lock:
                            post_id = rng.choice(question_ids) if question_ids else None
                            related_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if post_id is not None and related_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        """
                                        MATCH (q1:Question {Id: %d})-[r:LINKED_TO]->(q2:Question {Id: %d})
                                        SET r.LinkTypeId = coalesce(r.LinkTypeId, 0) + 1
                                        """ % (post_id, related_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
            elif op == "insert":
                with write_lock:
                    insert_kind = rng.choice(INSERT_TARGET_KINDS)
                    now_ms = int(time.time() * 1000)
                    new_user_id: Optional[int] = None
                    new_question_id: Optional[int] = None
                    new_answer_id: Optional[int] = None
                    new_comment_id: Optional[int] = None
                    new_badge_id: Optional[int] = None
                    user_id: Optional[int] = None
                    question_id: Optional[int] = None
                    tag_id: Optional[int] = None
                    second_question_id: Optional[int] = None
                    answer_id: Optional[int] = None
                    target_kind: Optional[str] = None
                    target_id: Optional[int] = None

                    with id_lock:
                        if insert_kind == "user_question":
                            new_user_id = next_user_id
                            next_user_id += 1
                            new_question_id = next_question_id
                            next_question_id += 1
                        elif insert_kind == "answer":
                            new_answer_id = next_answer_id
                            next_answer_id += 1
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif insert_kind == "comment":
                            new_comment_id = next_comment_id
                            next_comment_id += 1
                            if question_ids and (not answer_ids or rng.random() < 0.6):
                                target_kind = "question"
                                target_id = rng.choice(question_ids)
                            elif answer_ids:
                                target_kind = "answer"
                                target_id = rng.choice(answer_ids)
                            else:
                                target_kind = None
                                target_id = None
                        elif insert_kind == "tag_link":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            tag_id = rng.choice(tag_ids) if tag_ids else None
                        elif insert_kind == "post_link":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            second_question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif insert_kind == "accepted_answer":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        else:
                            new_badge_id = next_badge_id
                            next_badge_id += 1
                            user_id = rng.choice(user_ids) if user_ids else None

                    def do_insert():
                        with db.transaction():
                            if (
                                insert_kind == "user_question"
                                and new_user_id is not None
                                and new_question_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    CREATE (u:User {
                                        Id: %d,
                                        DisplayName: 'Synthetic',
                                        Reputation: 0,
                                        CreationDate: %d
                                    })
                                    CREATE (q:Question {
                                        Id: %d,
                                        Title: 'Synthetic',
                                        Body: 'Synthetic body',
                                        Score: 0,
                                        CreationDate: %d
                                    })
                                    CREATE (u)-[:ASKED {CreationDate: %d}]->(q)
                                    """
                                    % (
                                        new_user_id,
                                        now_ms,
                                        new_question_id,
                                        now_ms,
                                        now_ms,
                                    ),
                                )
                            elif (
                                insert_kind == "answer"
                                and new_answer_id is not None
                                and user_id is not None
                                and question_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    MATCH (u:User {Id: %d}), (q:Question {Id: %d})
                                    CREATE (a:Answer {
                                        Id: %d,
                                        Body: 'Synthetic answer',
                                        Score: 0,
                                        CreationDate: %d,
                                        CommentCount: 0
                                    })
                                    CREATE (u)-[:ANSWERED {CreationDate: %d}]->(a)
                                    CREATE (q)-[:HAS_ANSWER]->(a)
                                    """
                                    % (
                                        user_id,
                                        question_id,
                                        new_answer_id,
                                        now_ms,
                                        now_ms,
                                    ),
                                )
                            elif (
                                insert_kind == "comment"
                                and new_comment_id is not None
                                and target_kind is not None
                                and target_id is not None
                            ):
                                if target_kind == "question":
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (q:Question {Id: %d})
                                        CREATE (c:Comment {
                                            Id: %d,
                                            Text: 'Synthetic comment',
                                            Score: 0,
                                            CreationDate: %d
                                        })
                                        CREATE (c)-[:COMMENTED_ON {CreationDate: %d, Score: 0}]->(q)
                                        """
                                        % (target_id, new_comment_id, now_ms, now_ms),
                                    )
                                else:
                                    db.command(
                                        "opencypher",
                                        """
                                        MATCH (a:Answer {Id: %d})
                                        CREATE (c:Comment {
                                            Id: %d,
                                            Text: 'Synthetic comment',
                                            Score: 0,
                                            CreationDate: %d
                                        })
                                        CREATE (c)-[:COMMENTED_ON_ANSWER {CreationDate: %d, Score: 0}]->(a)
                                        """
                                        % (target_id, new_comment_id, now_ms, now_ms),
                                    )
                            elif (
                                insert_kind == "tag_link"
                                and question_id is not None
                                and tag_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    MATCH (q:Question {Id: %d}), (t:Tag {Id: %d})
                                    CREATE (q)-[:TAGGED_WITH]->(t)
                                    """ % (question_id, tag_id),
                                )
                            elif (
                                insert_kind == "post_link"
                                and question_id is not None
                                and second_question_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    MATCH (q1:Question {Id: %d}), (q2:Question {Id: %d})
                                    CREATE (q1)-[:LINKED_TO {LinkTypeId: 1, CreationDate: %d}]->(q2)
                                    """ % (question_id, second_question_id, now_ms),
                                )
                            elif (
                                insert_kind == "accepted_answer"
                                and question_id is not None
                                and answer_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    MATCH (q:Question {Id: %d}), (a:Answer {Id: %d})
                                    CREATE (q)-[:ACCEPTED_ANSWER]->(a)
                                    """ % (question_id, answer_id),
                                )
                            elif (
                                insert_kind == "badge"
                                and new_badge_id is not None
                                and user_id is not None
                            ):
                                db.command(
                                    "opencypher",
                                    """
                                    MATCH (u:User {Id: %d})
                                    CREATE (b:Badge {
                                        Id: %d,
                                        Name: 'SyntheticBadge',
                                        Date: %d,
                                        Class: 1
                                    })
                                    CREATE (u)-[:EARNED {Date: %d, Class: 1}]->(b)
                                    """ % (user_id, new_badge_id, now_ms, now_ms),
                                )

                    try:
                        run_with_retry(do_insert, arcade_error)
                    except arcade_error as exc:
                        if not is_transient_record_not_found_error(exc):
                            raise
                    else:
                        with id_lock:
                            if new_user_id is not None:
                                user_ids.append(new_user_id)
                            if new_question_id is not None:
                                question_ids.append(new_question_id)
                            if new_answer_id is not None:
                                answer_ids.append(new_answer_id)
                            if new_comment_id is not None:
                                comment_ids.append(new_comment_id)
                            if new_badge_id is not None:
                                badge_ids.append(new_badge_id)
            elif op == "delete":
                with write_lock:
                    delete_kind = rng.choice(DELETE_TARGET_KINDS)
                    node_delete_labels = {
                        "question": "Question",
                        "answer": "Answer",
                        "comment": "Comment",
                        "badge": "Badge",
                        "user": "User",
                        "tag": "Tag",
                    }
                    node_id_lists = {
                        "question": question_ids,
                        "answer": answer_ids,
                        "comment": comment_ids,
                        "badge": badge_ids,
                        "user": user_ids,
                        "tag": tag_ids,
                    }

                    if delete_kind in node_delete_labels:
                        with id_lock:
                            target_list = node_id_lists[delete_kind]
                            target_id = rng.choice(target_list) if target_list else None

                        if target_id is not None:
                            node_label = node_delete_labels[delete_kind]

                            def do_delete():
                                with db.transaction():
                                    db.command(
                                        "opencypher",
                                        "MATCH (n:%s {Id: %d}) DETACH DELETE n"
                                        % (node_label, target_id),
                                    )

                            try:
                                run_with_retry(do_delete, arcade_error)
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                            else:
                                with id_lock:
                                    try:
                                        target_list.remove(target_id)
                                    except ValueError:
                                        pass
                    elif delete_kind == "asked_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if user_id is not None and question_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (u:User {Id: %d})-[r:ASKED]->(q:Question {Id: %d}) DELETE r"
                                        % (user_id, question_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "answered_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if user_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (u:User {Id: %d})-[r:ANSWERED]->(a:Answer {Id: %d}) DELETE r"
                                        % (user_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "has_answer_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if question_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (q:Question {Id: %d})-[r:HAS_ANSWER]->(a:Answer {Id: %d}) DELETE r"
                                        % (question_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "accepted_answer_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if question_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (q:Question {Id: %d})-[r:ACCEPTED_ANSWER]->(a:Answer {Id: %d}) DELETE r"
                                        % (question_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "tagged_with_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            tag_id = rng.choice(tag_ids) if tag_ids else None
                        if question_id is not None and tag_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (q:Question {Id: %d})-[r:TAGGED_WITH]->(t:Tag {Id: %d}) DELETE r"
                                        % (question_id, tag_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "commented_on_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if comment_id is not None and question_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (c:Comment {Id: %d})-[r:COMMENTED_ON]->(q:Question {Id: %d}) DELETE r"
                                        % (comment_id, question_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "commented_on_answer_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if comment_id is not None and answer_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (c:Comment {Id: %d})-[r:COMMENTED_ON_ANSWER]->(a:Answer {Id: %d}) DELETE r"
                                        % (comment_id, answer_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "earned_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            badge_id = rng.choice(badge_ids) if badge_ids else None
                        if user_id is not None and badge_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (u:User {Id: %d})-[r:EARNED]->(b:Badge {Id: %d}) DELETE r"
                                        % (user_id, badge_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise
                    elif delete_kind == "linked_to_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            related_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if question_id is not None and related_id is not None:
                            try:
                                run_with_retry(
                                    lambda: db.command(
                                        "opencypher",
                                        "MATCH (q1:Question {Id: %d})-[r:LINKED_TO]->(q2:Question {Id: %d}) DELETE r"
                                        % (question_id, related_id),
                                    ),
                                    arcade_error,
                                )
                            except arcade_error as exc:
                                if not is_transient_record_not_found_error(exc):
                                    raise

            elapsed = time.perf_counter() - start_time
            latencies[op].append(elapsed)

        return latencies

    ops = choose_ops(transactions, DEFAULT_OLTP_MIX, seed)
    chunks = [ops[i::threads] for i in range(threads)]

    print(f"Running OLTP workload ({transactions:,} ops, {threads} threads)...")

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results: Dict[str, List[float]] = {
        "read": [],
        "update": [],
        "insert": [],
        "delete": [],
    }

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
    node_counts_by_type, edge_counts_by_type = count_arcadedb_by_type(
        db,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )
    counts_time = time.time() - counts_start
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())

    db.close()

    op_counts = {op_name: len(vals) for op_name, vals in results.items()}
    total_ops = sum(op_counts.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "load_time_s": load_time,
        "schema_time_s": schema_time,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "op_counts": op_counts,
        "load_stats": load_stats,
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
    }


def run_graph_oltp_ladybug(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
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
    load_stats, load_info = load_graph_ladybug(
        conn,
        db_path,
        data_dir,
        batch_size,
    )
    load_time = time.time() - load_start

    disk_after_load = get_dir_size_bytes(db_path)

    load_counts_start = time.time()
    load_node_rows = conn.execute("MATCH (n) RETURN count(n) AS count").get_all()
    load_edge_rows = conn.execute("MATCH ()-[r]->() RETURN count(r) AS count").get_all()
    load_counts_time = time.time() - load_counts_start
    load_node_count = int(load_node_rows[0][0]) if load_node_rows else 0
    load_edge_count = int(load_edge_rows[0][0]) if load_edge_rows else 0
    load_node_counts_by_type, load_edge_counts_by_type = count_ladybug_by_type(
        conn,
        LADYBUG_VERTEX_TYPES,
        LADYBUG_EDGE_TYPES,
    )
    print(
        "Load counts: "
        f"nodes={load_node_count:,}, "
        f"edges={load_edge_count:,} "
        f"(time={load_counts_time:.2f}s)"
    )
    print(format_counts_by_type("Load nodes by type", load_node_counts_by_type))
    print(format_counts_by_type("Load edges by type", load_edge_counts_by_type))

    id_lock = threading.Lock()
    # No global write serialization lock: allow concurrent C/U/D operations.
    write_lock = contextlib.nullcontext()
    user_ids = load_info["ids"]["users"]
    question_ids = load_info["ids"]["questions"]
    answer_ids = load_info["ids"]["answers"]
    tag_ids = load_info["ids"]["tags"]
    badge_ids = load_info["ids"]["badges"]
    comment_ids = load_info["ids"]["comments"]
    next_user_id = load_info["max_ids"]["user"] + 1
    next_question_id = load_info["max_ids"]["question"] + 1
    next_answer_id = load_info["max_ids"]["answer"] + 1
    next_badge_id = load_info["max_ids"]["badge"] + 1
    next_comment_id = load_info["max_ids"]["comment"] + 1

    def worker(ops: List[str], worker_id: int) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        nonlocal next_user_id
        nonlocal next_question_id
        nonlocal next_answer_id
        nonlocal next_badge_id
        nonlocal next_comment_id
        local_conn = lb.Connection(db)

        for op in ops:
            start_time = time.perf_counter()
            if op == "read":
                read_kind = rng.choice(READ_TARGET_KINDS)
                if read_kind == "user":
                    with id_lock:
                        target_id = rng.choice(user_ids) if user_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (u:User {Id: $id})-[:ASKED|ANSWERED]->(p) RETURN p.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
                elif read_kind == "question":
                    with id_lock:
                        target_id = rng.choice(question_ids) if question_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (q:Question {Id: $id})-[:TAGGED_WITH]->(t:Tag) RETURN t.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
                elif read_kind == "answer":
                    with id_lock:
                        target_id = rng.choice(answer_ids) if answer_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (a:Answer {Id: $id})<-[:COMMENTED_ON_ANSWER]-(c:Comment) RETURN c.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
                elif read_kind == "tag":
                    with id_lock:
                        target_id = rng.choice(tag_ids) if tag_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag {Id: $id}) RETURN q.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
                elif read_kind == "comment":
                    with id_lock:
                        target_id = rng.choice(comment_ids) if comment_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (c:Comment {Id: $id})-[r:COMMENTED_ON|COMMENTED_ON_ANSWER]->(p) RETURN p.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
                elif read_kind == "edge_sample":
                    local_conn.execute(
                        "MATCH ()-[r:ASKED|ANSWERED|HAS_ANSWER|ACCEPTED_ANSWER|TAGGED_WITH|COMMENTED_ON|COMMENTED_ON_ANSWER|EARNED|LINKED_TO]->() RETURN r LIMIT 1"
                    )
                else:
                    with id_lock:
                        target_id = rng.choice(badge_ids) if badge_ids else None
                    if target_id is not None:
                        local_conn.execute(
                            "MATCH (u:User)-[:EARNED]->(b:Badge {Id: $id}) RETURN u.Id LIMIT 1",
                            parameters={"id": target_id},
                        )
            elif op == "update":
                with write_lock:
                    update_kind = rng.choice(UPDATE_TARGET_KINDS)
                    with id_lock:
                        if update_kind == "question":
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif update_kind == "answer":
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        elif update_kind == "comment":
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        elif update_kind == "tag":
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        else:
                            target_id = rng.choice(user_ids) if user_ids else None
                    if target_id is not None:
                        if update_kind == "question":
                            local_conn.execute(
                                "MATCH (q:Question {Id: $id}) SET q.Score = coalesce(q.Score, 0) + 1",
                                parameters={"id": target_id},
                            )
                        elif update_kind == "answer":
                            local_conn.execute(
                                "MATCH (a:Answer {Id: $id}) SET a.Score = coalesce(a.Score, 0) + 1",
                                parameters={"id": target_id},
                            )
                        elif update_kind == "comment":
                            local_conn.execute(
                                "MATCH (c:Comment {Id: $id}) SET c.Score = coalesce(c.Score, 0) + 1",
                                parameters={"id": target_id},
                            )
                        elif update_kind == "tag":
                            local_conn.execute(
                                "MATCH (t:Tag {Id: $id}) SET t.Count = coalesce(t.Count, 0) + 1",
                                parameters={"id": target_id},
                            )
                        else:
                            local_conn.execute(
                                "MATCH (u:User {Id: $id}) SET u.Reputation = coalesce(u.Reputation, 0) + 1",
                                parameters={"id": target_id},
                            )
                    elif update_kind == "asked_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if user_id is not None and question_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:ASKED]->(q:Question {Id: $qid}) SET r.CreationDate = coalesce(r.CreationDate, 0) + 1",
                                parameters={"uid": user_id, "qid": question_id},
                            )
                    elif update_kind == "answered_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if user_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:ANSWERED]->(a:Answer {Id: $aid}) SET r.CreationDate = coalesce(r.CreationDate, 0) + 1",
                                parameters={"uid": user_id, "aid": answer_id},
                            )
                    elif update_kind == "commented_on_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if comment_id is not None and question_id is not None:
                            local_conn.execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON]->(q:Question {Id: $qid}) SET r.Score = coalesce(r.Score, 0) + 1",
                                parameters={"cid": comment_id, "qid": question_id},
                            )
                    elif update_kind == "commented_on_answer_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if comment_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON_ANSWER]->(a:Answer {Id: $aid}) SET r.Score = coalesce(r.Score, 0) + 1",
                                parameters={"cid": comment_id, "aid": answer_id},
                            )
                    elif update_kind == "earned_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            badge_id = rng.choice(badge_ids) if badge_ids else None
                        if user_id is not None and badge_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:EARNED]->(b:Badge {Id: $bid}) SET r.Class = coalesce(r.Class, 0) + 1",
                                parameters={"uid": user_id, "bid": badge_id},
                            )
                    elif update_kind == "linked_to_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            related_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if question_id is not None and related_id is not None:
                            local_conn.execute(
                                "MATCH (q1:Question {Id: $qid})-[r:LINKED_TO]->(q2:Question {Id: $rid}) SET r.LinkTypeId = coalesce(r.LinkTypeId, 0) + 1",
                                parameters={"qid": question_id, "rid": related_id},
                            )
            elif op == "insert":
                with write_lock:
                    insert_kind = rng.choice(INSERT_TARGET_KINDS)
                    now_ms = int(time.time() * 1000)
                    new_user_id: Optional[int] = None
                    new_question_id: Optional[int] = None
                    new_answer_id: Optional[int] = None
                    new_comment_id: Optional[int] = None
                    new_badge_id: Optional[int] = None
                    user_id: Optional[int] = None
                    question_id: Optional[int] = None
                    tag_id: Optional[int] = None
                    second_question_id: Optional[int] = None
                    answer_id: Optional[int] = None
                    target_kind: Optional[str] = None
                    target_id: Optional[int] = None

                    with id_lock:
                        if insert_kind == "user_question":
                            new_user_id = next_user_id
                            next_user_id += 1
                            new_question_id = next_question_id
                            next_question_id += 1
                        elif insert_kind == "answer":
                            new_answer_id = next_answer_id
                            next_answer_id += 1
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif insert_kind == "comment":
                            new_comment_id = next_comment_id
                            next_comment_id += 1
                            if question_ids and (not answer_ids or rng.random() < 0.6):
                                target_kind = "question"
                                target_id = rng.choice(question_ids)
                            elif answer_ids:
                                target_kind = "answer"
                                target_id = rng.choice(answer_ids)
                        elif insert_kind == "tag_link":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            tag_id = rng.choice(tag_ids) if tag_ids else None
                        elif insert_kind == "post_link":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            second_question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        elif insert_kind == "accepted_answer":
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        else:
                            new_badge_id = next_badge_id
                            next_badge_id += 1
                            user_id = rng.choice(user_ids) if user_ids else None

                    if (
                        insert_kind == "user_question"
                        and new_user_id is not None
                        and new_question_id is not None
                    ):
                        local_conn.execute(
                            """
                            CREATE (u:User {Id: $uid, DisplayName: 'Synthetic', Reputation: 0, CreationDate: $ts})
                            CREATE (q:Question {Id: $qid, Title: 'Synthetic', Body: 'Synthetic body', Score: 0, CreationDate: $ts})
                            CREATE (u)-[:ASKED {CreationDate: $ts}]->(q)
                            """,
                            parameters={
                                "uid": new_user_id,
                                "qid": new_question_id,
                                "ts": now_ms,
                            },
                        )
                    elif (
                        insert_kind == "answer"
                        and new_answer_id is not None
                        and user_id is not None
                        and question_id is not None
                    ):
                        local_conn.execute(
                            """
                            MATCH (u:User {Id: $uid}), (q:Question {Id: $qid})
                            CREATE (a:Answer {Id: $aid, Body: 'Synthetic answer', Score: 0, CreationDate: $ts, CommentCount: 0})
                            CREATE (u)-[:ANSWERED {CreationDate: $ts}]->(a)
                            CREATE (q)-[:HAS_ANSWER]->(a)
                            """,
                            parameters={
                                "uid": user_id,
                                "qid": question_id,
                                "aid": new_answer_id,
                                "ts": now_ms,
                            },
                        )
                    elif (
                        insert_kind == "comment"
                        and new_comment_id is not None
                        and target_kind is not None
                        and target_id is not None
                    ):
                        if target_kind == "question":
                            local_conn.execute(
                                """
                                MATCH (q:Question {Id: $target_id})
                                CREATE (c:Comment {Id: $cid, Text: 'Synthetic comment', Score: 0, CreationDate: $ts})
                                CREATE (c)-[:COMMENTED_ON {CreationDate: $ts, Score: 0}]->(q)
                                """,
                                parameters={
                                    "target_id": target_id,
                                    "cid": new_comment_id,
                                    "ts": now_ms,
                                },
                            )
                        else:
                            local_conn.execute(
                                """
                                MATCH (a:Answer {Id: $target_id})
                                CREATE (c:Comment {Id: $cid, Text: 'Synthetic comment', Score: 0, CreationDate: $ts})
                                CREATE (c)-[:COMMENTED_ON_ANSWER {CreationDate: $ts, Score: 0}]->(a)
                                """,
                                parameters={
                                    "target_id": target_id,
                                    "cid": new_comment_id,
                                    "ts": now_ms,
                                },
                            )
                    elif (
                        insert_kind == "tag_link"
                        and question_id is not None
                        and tag_id is not None
                    ):
                        local_conn.execute(
                            """
                            MATCH (q:Question {Id: $qid}), (t:Tag {Id: $tid})
                            CREATE (q)-[:TAGGED_WITH]->(t)
                            """,
                            parameters={"qid": question_id, "tid": tag_id},
                        )
                    elif (
                        insert_kind == "post_link"
                        and question_id is not None
                        and second_question_id is not None
                    ):
                        local_conn.execute(
                            """
                            MATCH (q1:Question {Id: $qid}), (q2:Question {Id: $rid})
                            CREATE (q1)-[:LINKED_TO {LinkTypeId: 1, CreationDate: $ts}]->(q2)
                            """,
                            parameters={
                                "qid": question_id,
                                "rid": second_question_id,
                                "ts": now_ms,
                            },
                        )
                    elif (
                        insert_kind == "accepted_answer"
                        and question_id is not None
                        and answer_id is not None
                    ):
                        local_conn.execute(
                            """
                            MATCH (q:Question {Id: $qid}), (a:Answer {Id: $aid})
                            CREATE (q)-[:ACCEPTED_ANSWER]->(a)
                            """,
                            parameters={"qid": question_id, "aid": answer_id},
                        )
                    elif (
                        insert_kind == "badge"
                        and new_badge_id is not None
                        and user_id is not None
                    ):
                        local_conn.execute(
                            """
                            MATCH (u:User {Id: $uid})
                            CREATE (b:Badge {Id: $bid, Name: 'SyntheticBadge', Date: $ts, Class: 1})
                            CREATE (u)-[:EARNED {Date: $ts, Class: 1}]->(b)
                            """,
                            parameters={
                                "uid": user_id,
                                "bid": new_badge_id,
                                "ts": now_ms,
                            },
                        )

                    with id_lock:
                        if new_user_id is not None:
                            user_ids.append(new_user_id)
                        if new_question_id is not None:
                            question_ids.append(new_question_id)
                        if new_answer_id is not None:
                            answer_ids.append(new_answer_id)
                        if new_comment_id is not None:
                            comment_ids.append(new_comment_id)
                        if new_badge_id is not None:
                            badge_ids.append(new_badge_id)
            elif op == "delete":
                with write_lock:
                    delete_kind = rng.choice(DELETE_TARGET_KINDS)
                    node_delete_labels = {
                        "question": "Question",
                        "answer": "Answer",
                        "comment": "Comment",
                        "badge": "Badge",
                        "user": "User",
                        "tag": "Tag",
                    }
                    node_id_lists = {
                        "question": question_ids,
                        "answer": answer_ids,
                        "comment": comment_ids,
                        "badge": badge_ids,
                        "user": user_ids,
                        "tag": tag_ids,
                    }
                    if delete_kind in node_delete_labels:
                        with id_lock:
                            target_list = node_id_lists[delete_kind]
                            target_id = rng.choice(target_list) if target_list else None
                        if target_id is not None:
                            node_label = node_delete_labels[delete_kind]
                            local_conn.execute(
                                "MATCH (n:%s {Id: $id}) DETACH DELETE n" % node_label,
                                parameters={"id": target_id},
                            )
                            with id_lock:
                                try:
                                    target_list.remove(target_id)
                                except ValueError:
                                    pass
                    elif delete_kind == "asked_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if user_id is not None and question_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:ASKED]->(q:Question {Id: $qid}) DELETE r",
                                parameters={"uid": user_id, "qid": question_id},
                            )
                    elif delete_kind == "answered_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if user_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:ANSWERED]->(a:Answer {Id: $aid}) DELETE r",
                                parameters={"uid": user_id, "aid": answer_id},
                            )
                    elif delete_kind == "has_answer_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if question_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (q:Question {Id: $qid})-[r:HAS_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                parameters={"qid": question_id, "aid": answer_id},
                            )
                    elif delete_kind == "accepted_answer_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if question_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (q:Question {Id: $qid})-[r:ACCEPTED_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                parameters={"qid": question_id, "aid": answer_id},
                            )
                    elif delete_kind == "tagged_with_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            tag_id = rng.choice(tag_ids) if tag_ids else None
                        if question_id is not None and tag_id is not None:
                            local_conn.execute(
                                "MATCH (q:Question {Id: $qid})-[r:TAGGED_WITH]->(t:Tag {Id: $tid}) DELETE r",
                                parameters={"qid": question_id, "tid": tag_id},
                            )
                    elif delete_kind == "commented_on_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if comment_id is not None and question_id is not None:
                            local_conn.execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON]->(q:Question {Id: $qid}) DELETE r",
                                parameters={"cid": comment_id, "qid": question_id},
                            )
                    elif delete_kind == "commented_on_answer_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if comment_id is not None and answer_id is not None:
                            local_conn.execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                parameters={"cid": comment_id, "aid": answer_id},
                            )
                    elif delete_kind == "earned_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            badge_id = rng.choice(badge_ids) if badge_ids else None
                        if user_id is not None and badge_id is not None:
                            local_conn.execute(
                                "MATCH (u:User {Id: $uid})-[r:EARNED]->(b:Badge {Id: $bid}) DELETE r",
                                parameters={"uid": user_id, "bid": badge_id},
                            )
                    elif delete_kind == "linked_to_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            related_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if question_id is not None and related_id is not None:
                            local_conn.execute(
                                "MATCH (q1:Question {Id: $qid})-[r:LINKED_TO]->(q2:Question {Id: $rid}) DELETE r",
                                parameters={"qid": question_id, "rid": related_id},
                            )

            elapsed = time.perf_counter() - start_time
            latencies[op].append(elapsed)

        return latencies

    ops = choose_ops(transactions, DEFAULT_OLTP_MIX, seed)
    chunks = [ops[i::threads] for i in range(threads)]

    print(f"Running OLTP workload ({transactions:,} ops, {threads} threads)...")

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results: Dict[str, List[float]] = {
        "read": [],
        "update": [],
        "insert": [],
        "delete": [],
    }

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
    node_rows = conn.execute("MATCH (n) RETURN count(n) AS count").get_all()
    edge_rows = conn.execute("MATCH ()-[r]->() RETURN count(r) AS count").get_all()
    counts_time = time.time() - counts_start
    node_count = int(node_rows[0][0]) if node_rows else 0
    edge_count = int(edge_rows[0][0]) if edge_rows else 0
    node_counts_by_type, edge_counts_by_type = count_ladybug_by_type(
        conn,
        LADYBUG_VERTEX_TYPES,
        LADYBUG_EDGE_TYPES,
    )

    op_counts = {op_name: len(vals) for op_name, vals in results.items()}
    total_ops = sum(op_counts.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "load_time_s": load_time,
        "schema_time_s": schema_time,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "op_counts": op_counts,
        "load_stats": load_stats,
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
    }


def _cypher_count(rows: Any) -> int:
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        for key, value in first.items():
            key_lower = str(key).lower()
            if key_lower.startswith("count") or "count(" in key_lower:
                try:
                    return int(value)
                except Exception:
                    return 0
        if first:
            try:
                return int(next(iter(first.values())))
            except Exception:
                return 0
    if isinstance(first, (list, tuple)) and first:
        try:
            return int(first[0])
        except Exception:
            return 0
    return 0


def _load_stackoverflow_minimal_cypher(
    execute_cypher,
    data_dir: Path,
) -> Tuple[Dict[str, List[int]], Dict[str, int]]:
    users_path = data_dir / "Users.xml"
    posts_path = data_dir / "Posts.xml"

    user_ids: List[int] = []
    question_ids: List[int] = []
    asked_edges = 0
    max_user_id = 0
    max_question_id = 0

    for attrs in iter_xml_rows(users_path):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
        reputation = parse_int(attrs.get("Reputation")) or 0
        execute_cypher(f"CREATE (u:User {{Id: {user_id}, Reputation: {reputation}}})")
        user_ids.append(user_id)
        max_user_id = max(max_user_id, user_id)

    user_id_set = set(user_ids)
    for attrs in iter_xml_rows(posts_path):
        if parse_int(attrs.get("PostTypeId")) != 1:
            continue
        question_id = parse_int(attrs.get("Id"))
        owner_user_id = parse_int(attrs.get("OwnerUserId"))
        if question_id is None or owner_user_id is None:
            continue
        if owner_user_id not in user_id_set:
            continue
        score = parse_int(attrs.get("Score")) or 0
        execute_cypher(f"CREATE (q:Question {{Id: {question_id}, Score: {score}}})")
        execute_cypher(
            "MATCH (u:User {Id: %d}), (q:Question {Id: %d}) CREATE (u)-[:ASKED]->(q)"
            % (owner_user_id, question_id)
        )
        question_ids.append(question_id)
        asked_edges += 1
        max_question_id = max(max_question_id, question_id)

    load_stats = {
        "users": len(user_ids),
        "questions": len(question_ids),
        "asked_edges": asked_edges,
    }
    load_info = {
        "ids": {"users": user_ids, "questions": question_ids},
        "max_ids": {"user": max_user_id, "question": max_question_id},
    }
    return load_info, load_stats


def _clean_props(props: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in props.items() if value is not None}


def _load_stackoverflow_graphqlite_bulk(
    graph,
    data_dir: Path,
    batch_size: int,
) -> Tuple[Dict[str, List[int]], Dict[str, int]]:
    users_path = data_dir / "Users.xml"
    posts_path = data_dir / "Posts.xml"

    user_ids: List[int] = []
    question_ids: List[int] = []
    answer_ids: List[int] = []
    tag_ids: List[int] = []
    badge_ids: List[int] = []
    comment_ids: List[int] = []
    user_id_set = set()
    question_id_set: set[int] = set()
    answer_id_set: set[int] = set()
    badge_id_set: set[int] = set()
    comment_id_set: set[int] = set()
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

    max_user_id = 0
    max_question_id = 0

    for attrs in iter_xml_rows(users_path):
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
        max_user_id = max(max_user_id, user_id)
        if len(node_batch) >= batch_size:
            flush_nodes(node_batch)

    flush_nodes(node_batch)

    question_batch: List[Tuple[str, Dict[str, Any], str]] = []
    answer_batch: List[Tuple[str, Dict[str, Any], str]] = []
    edge_batch: List[Tuple[str, str, Dict[str, Any], str]] = []
    for attrs in iter_xml_rows(posts_path):
        post_type = parse_int(attrs.get("PostTypeId"))
        post_id = parse_int(attrs.get("Id"))
        if post_type is None or post_id is None:
            continue
        if post_type == 1:
            question_ids.append(post_id)
            question_id_set.add(post_id)
            max_question_id = max(max_question_id, post_id)
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
            if parent_id is not None:
                if parent_id in question_id_set:
                    edge_batch.append(
                        (f"q:{parent_id}", f"a:{post_id}", {}, "HAS_ANSWER")
                    )
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
        badge_id_set.add(badge_id)
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
        comment_id_set.add(comment_id)
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

    for attrs in iter_xml_rows(posts_path):
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
            flush_nodes(question_batch)
            flush_nodes(answer_batch)
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
            flush_nodes(question_batch)
            flush_nodes(answer_batch)
            flush_edges(edge_batch)

    flush_nodes(node_batch)
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
        "max_ids": {
            "user": max_user_id,
            "question": max(max_question_id, max_ids["question"]),
            "answer": max_ids["answer"],
            "tag": max_ids["tag"],
            "badge": max_ids["badge"],
            "comment": max_ids["comment"],
        },
    }
    return load_info, load_stats


def _configure_sqlite_connection(
    conn: sqlite3.Connection, profile: str
) -> Dict[str, Any]:
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


def _connect_sqlite(
    db_file: Path,
    sqlite_profile: str,
) -> Tuple[sqlite3.Connection, Dict[str, Any]]:
    conn = sqlite3.connect(str(db_file), timeout=30.0, check_same_thread=False)
    pragma_config = _configure_sqlite_connection(conn, sqlite_profile)
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


def _create_python_memory_store() -> Dict[str, Dict[str, Any]]:
    return {
        "nodes": {label: {} for label in ARCADE_VERTEX_TYPES},
        "edges": {label: {} for label in ARCADE_EDGE_TYPES},
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


def _python_memory_delete_edges_where(
    store: Dict[str, Dict[str, Any]],
    edge_type: str,
    predicate,
) -> None:
    edges = store["edges"][edge_type]
    delete_keys = [key for key in edges.keys() if predicate(key)]
    for key in delete_keys:
        edges.pop(key, None)


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
                    store,
                    "ACCEPTED_ANSWER",
                    post_id,
                    accepted_answer_id,
                )
                edge_counts["ACCEPTED_ANSWER"] += 1

            for tag_name in parse_tags(attrs.get("Tags")):
                tag_id = tag_name_to_id.get(tag_name)
                if tag_id is not None:
                    _python_memory_add_edge(
                        store,
                        "TAGGED_WITH",
                        post_id,
                        tag_id,
                    )
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
            edge_type: int(edge_counts.get(edge_type, 0))
            for edge_type in ARCADE_EDGE_TYPES
        },
        "indexes": {"id_unique": 0.0},
    }
    return load_info, load_stats


def run_graph_oltp_python_memory(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
) -> dict:
    del batch_size
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    snapshot_file = db_path / "python_memory_store.pkl"

    store = _create_python_memory_store()

    print("Creating schema...")
    schema_start = time.time()
    schema_time = time.time() - schema_start

    print("Loading graph...")
    load_start = time.time()
    load_info, load_stats = _load_stackoverflow_python_memory(store, data_dir)
    _persist_python_memory_store(store, snapshot_file)
    load_time = time.time() - load_start

    disk_after_load = get_dir_size_bytes(db_path)

    load_counts_start = time.time()
    load_node_counts_by_type, load_edge_counts_by_type = _python_memory_count_by_type(
        store,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )
    load_node_count = sum(load_node_counts_by_type.values())
    load_edge_count = sum(load_edge_counts_by_type.values())
    load_counts_time = time.time() - load_counts_start

    id_lock = threading.Lock()
    graph_lock = threading.Lock()
    user_ids = load_info["ids"]["users"]
    question_ids = load_info["ids"]["questions"]
    answer_ids = load_info["ids"]["answers"]
    tag_ids = load_info["ids"]["tags"]
    badge_ids = load_info["ids"]["badges"]
    comment_ids = load_info["ids"]["comments"]
    next_user_id = load_info["max_ids"]["user"] + 1
    next_question_id = load_info["max_ids"]["question"] + 1
    next_answer_id = load_info["max_ids"]["answer"] + 1
    next_badge_id = load_info["max_ids"]["badge"] + 1
    next_comment_id = load_info["max_ids"]["comment"] + 1

    def worker(ops: List[str], worker_id: int) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        nonlocal next_user_id
        nonlocal next_question_id
        nonlocal next_answer_id
        nonlocal next_badge_id
        nonlocal next_comment_id

        for op in ops:
            start_time = time.perf_counter()
            if op == "read":
                read_kind = rng.choice(READ_TARGET_KINDS)
                with graph_lock:
                    if read_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            any(
                                src == target_id
                                for (src, _), _props in store["edges"]["ASKED"].items()
                            )
                    elif read_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            any(
                                src == target_id
                                for (src, _), _props in store["edges"][
                                    "TAGGED_WITH"
                                ].items()
                            )
                    elif read_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            any(
                                dst == target_id
                                for (_src, dst), _props in store["edges"][
                                    "COMMENTED_ON_ANSWER"
                                ].items()
                            )
                    elif read_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            any(
                                dst == target_id
                                for (_src, dst), _props in store["edges"][
                                    "TAGGED_WITH"
                                ].items()
                            )
                    elif read_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            any(
                                src == target_id
                                for (src, _dst), _props in store["edges"][
                                    "COMMENTED_ON"
                                ].items()
                            ) or any(
                                src == target_id
                                for (src, _dst), _props in store["edges"][
                                    "COMMENTED_ON_ANSWER"
                                ].items()
                            )
                    elif read_kind == "edge_sample":
                        edge_table = rng.choice(ARCADE_EDGE_TYPES)
                        bool(store["edges"][edge_table])
                    else:
                        with id_lock:
                            target_id = rng.choice(badge_ids) if badge_ids else None
                        if target_id is not None:
                            any(
                                dst == target_id
                                for (_src, dst), _props in store["edges"][
                                    "EARNED"
                                ].items()
                            )

            elif op == "update":
                update_kind = rng.choice(UPDATE_TARGET_KINDS)
                with graph_lock:
                    if update_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id in store["nodes"]["Question"]:
                            node = store["nodes"]["Question"][target_id]
                            node["Score"] = int(node.get("Score") or 0) + 1
                    elif update_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id in store["nodes"]["Answer"]:
                            node = store["nodes"]["Answer"][target_id]
                            node["Score"] = int(node.get("Score") or 0) + 1
                    elif update_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id in store["nodes"]["Comment"]:
                            node = store["nodes"]["Comment"][target_id]
                            node["Score"] = int(node.get("Score") or 0) + 1
                    elif update_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id in store["nodes"]["User"]:
                            node = store["nodes"]["User"][target_id]
                            node["Reputation"] = int(node.get("Reputation") or 0) + 1
                    elif update_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id in store["nodes"]["Tag"]:
                            node = store["nodes"]["Tag"][target_id]
                            node["Count"] = int(node.get("Count") or 0) + 1
                    else:
                        edge_map = {
                            "asked_edge": (
                                "ASKED",
                                "CreationDate",
                                user_ids,
                                question_ids,
                            ),
                            "answered_edge": (
                                "ANSWERED",
                                "CreationDate",
                                user_ids,
                                answer_ids,
                            ),
                            "commented_on_edge": (
                                "COMMENTED_ON",
                                "Score",
                                comment_ids,
                                question_ids,
                            ),
                            "commented_on_answer_edge": (
                                "COMMENTED_ON_ANSWER",
                                "Score",
                                comment_ids,
                                answer_ids,
                            ),
                            "earned_edge": ("EARNED", "Class", user_ids, badge_ids),
                            "linked_to_edge": (
                                "LINKED_TO",
                                "LinkTypeId",
                                question_ids,
                                question_ids,
                            ),
                        }
                        edge_type, field, from_list, to_list = edge_map[update_kind]
                        with id_lock:
                            from_id = rng.choice(from_list) if from_list else None
                            to_id = rng.choice(to_list) if to_list else None
                        if from_id is not None and to_id is not None:
                            edge = store["edges"][edge_type].get((from_id, to_id))
                            if edge is not None:
                                edge[field] = int(edge.get(field) or 0) + 1

            elif op == "insert":
                with id_lock:
                    insert_kind = rng.choice(INSERT_TARGET_KINDS)
                    now_ms = int(time.time() * 1000)
                    new_user_id: Optional[int] = None
                    new_question_id: Optional[int] = None
                    new_answer_id: Optional[int] = None
                    new_comment_id: Optional[int] = None
                    new_badge_id: Optional[int] = None
                    user_id = rng.choice(user_ids) if user_ids else None
                    question_id = rng.choice(question_ids) if question_ids else None
                    answer_id = rng.choice(answer_ids) if answer_ids else None
                    tag_id = rng.choice(tag_ids) if tag_ids else None
                    second_question_id = (
                        rng.choice(question_ids) if question_ids else None
                    )
                    if insert_kind == "user_question":
                        new_user_id = next_user_id
                        next_user_id += 1
                        new_question_id = next_question_id
                        next_question_id += 1
                    elif insert_kind == "answer":
                        new_answer_id = next_answer_id
                        next_answer_id += 1
                    elif insert_kind == "comment":
                        new_comment_id = next_comment_id
                        next_comment_id += 1
                    elif insert_kind == "badge":
                        new_badge_id = next_badge_id
                        next_badge_id += 1

                with graph_lock:
                    if (
                        insert_kind == "user_question"
                        and new_user_id is not None
                        and new_question_id is not None
                    ):
                        store["nodes"]["User"][new_user_id] = {
                            "Id": new_user_id,
                            "DisplayName": "Synthetic",
                            "Reputation": 0,
                            "CreationDate": now_ms,
                        }
                        store["nodes"]["Question"][new_question_id] = {
                            "Id": new_question_id,
                            "Title": "Synthetic",
                            "Body": "Synthetic body",
                            "Score": 0,
                            "CreationDate": now_ms,
                        }
                        _python_memory_add_edge(
                            store,
                            "ASKED",
                            new_user_id,
                            new_question_id,
                            {"CreationDate": now_ms},
                        )
                        with id_lock:
                            user_ids.append(new_user_id)
                            question_ids.append(new_question_id)
                    elif (
                        insert_kind == "answer"
                        and new_answer_id is not None
                        and user_id is not None
                        and question_id is not None
                    ):
                        store["nodes"]["Answer"][new_answer_id] = {
                            "Id": new_answer_id,
                            "Body": "Synthetic answer",
                            "Score": 0,
                            "CreationDate": now_ms,
                            "CommentCount": 0,
                        }
                        _python_memory_add_edge(
                            store,
                            "ANSWERED",
                            user_id,
                            new_answer_id,
                            {"CreationDate": now_ms},
                        )
                        _python_memory_add_edge(
                            store, "HAS_ANSWER", question_id, new_answer_id
                        )
                        with id_lock:
                            answer_ids.append(new_answer_id)
                    elif insert_kind == "comment" and new_comment_id is not None:
                        store["nodes"]["Comment"][new_comment_id] = {
                            "Id": new_comment_id,
                            "Text": "Synthetic comment",
                            "Score": 0,
                            "CreationDate": now_ms,
                        }
                        if question_id is not None:
                            _python_memory_add_edge(
                                store,
                                "COMMENTED_ON",
                                new_comment_id,
                                question_id,
                                {"CreationDate": now_ms, "Score": 0},
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                        elif answer_id is not None:
                            _python_memory_add_edge(
                                store,
                                "COMMENTED_ON_ANSWER",
                                new_comment_id,
                                answer_id,
                                {"CreationDate": now_ms, "Score": 0},
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                        else:
                            store["nodes"]["Comment"].pop(new_comment_id, None)
                    elif (
                        insert_kind == "badge"
                        and new_badge_id is not None
                        and user_id is not None
                    ):
                        store["nodes"]["Badge"][new_badge_id] = {
                            "Id": new_badge_id,
                            "Name": "SyntheticBadge",
                            "Date": now_ms,
                            "Class": 1,
                        }
                        _python_memory_add_edge(
                            store,
                            "EARNED",
                            user_id,
                            new_badge_id,
                            {"Date": now_ms, "Class": 1},
                        )
                        with id_lock:
                            badge_ids.append(new_badge_id)
                    elif (
                        insert_kind == "tag_link"
                        and question_id is not None
                        and tag_id is not None
                    ):
                        _python_memory_add_edge(
                            store, "TAGGED_WITH", question_id, tag_id
                        )
                    elif (
                        insert_kind == "post_link"
                        and question_id is not None
                        and second_question_id is not None
                    ):
                        _python_memory_add_edge(
                            store,
                            "LINKED_TO",
                            question_id,
                            second_question_id,
                            {"LinkTypeId": 1, "CreationDate": now_ms},
                        )
                    elif (
                        insert_kind == "accepted_answer"
                        and question_id is not None
                        and answer_id is not None
                    ):
                        _python_memory_add_edge(
                            store,
                            "ACCEPTED_ANSWER",
                            question_id,
                            answer_id,
                        )

            else:
                delete_kind = rng.choice(DELETE_TARGET_KINDS)
                with graph_lock:
                    if delete_kind in {
                        "question",
                        "answer",
                        "comment",
                        "badge",
                        "user",
                        "tag",
                    }:
                        mapping = {
                            "question": (question_ids, "Question"),
                            "answer": (answer_ids, "Answer"),
                            "comment": (comment_ids, "Comment"),
                            "badge": (badge_ids, "Badge"),
                            "user": (user_ids, "User"),
                            "tag": (tag_ids, "Tag"),
                        }
                        target_list, label = mapping[delete_kind]
                        with id_lock:
                            target_id = rng.choice(target_list) if target_list else None
                        if target_id is not None:
                            if label == "Question":
                                _python_memory_delete_edges_where(
                                    store, "ASKED", lambda k: k[1] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store, "HAS_ANSWER", lambda k: k[0] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store,
                                    "ACCEPTED_ANSWER",
                                    lambda k: k[0] == target_id,
                                )
                                _python_memory_delete_edges_where(
                                    store, "TAGGED_WITH", lambda k: k[0] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store, "COMMENTED_ON", lambda k: k[1] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store,
                                    "LINKED_TO",
                                    lambda k: k[0] == target_id or k[1] == target_id,
                                )
                            elif label == "Answer":
                                _python_memory_delete_edges_where(
                                    store, "ANSWERED", lambda k: k[1] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store, "HAS_ANSWER", lambda k: k[1] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store,
                                    "ACCEPTED_ANSWER",
                                    lambda k: k[1] == target_id,
                                )
                                _python_memory_delete_edges_where(
                                    store,
                                    "COMMENTED_ON_ANSWER",
                                    lambda k: k[1] == target_id,
                                )
                            elif label == "Comment":
                                _python_memory_delete_edges_where(
                                    store, "COMMENTED_ON", lambda k: k[0] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store,
                                    "COMMENTED_ON_ANSWER",
                                    lambda k: k[0] == target_id,
                                )
                            elif label == "Badge":
                                _python_memory_delete_edges_where(
                                    store, "EARNED", lambda k: k[1] == target_id
                                )
                            elif label == "User":
                                _python_memory_delete_edges_where(
                                    store, "ASKED", lambda k: k[0] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store, "ANSWERED", lambda k: k[0] == target_id
                                )
                                _python_memory_delete_edges_where(
                                    store, "EARNED", lambda k: k[0] == target_id
                                )
                            elif label == "Tag":
                                _python_memory_delete_edges_where(
                                    store, "TAGGED_WITH", lambda k: k[1] == target_id
                                )

                            store["nodes"][label].pop(target_id, None)
                            with id_lock:
                                with contextlib.suppress(ValueError):
                                    target_list.remove(target_id)
                    else:
                        edge_mapping = {
                            "asked_edge": ("ASKED", user_ids, question_ids),
                            "answered_edge": ("ANSWERED", user_ids, answer_ids),
                            "has_answer_edge": ("HAS_ANSWER", question_ids, answer_ids),
                            "accepted_answer_edge": (
                                "ACCEPTED_ANSWER",
                                question_ids,
                                answer_ids,
                            ),
                            "tagged_with_edge": ("TAGGED_WITH", question_ids, tag_ids),
                            "commented_on_edge": (
                                "COMMENTED_ON",
                                comment_ids,
                                question_ids,
                            ),
                            "commented_on_answer_edge": (
                                "COMMENTED_ON_ANSWER",
                                comment_ids,
                                answer_ids,
                            ),
                            "earned_edge": ("EARNED", user_ids, badge_ids),
                            "linked_to_edge": ("LINKED_TO", question_ids, question_ids),
                        }
                        edge_type, from_list, to_list = edge_mapping[delete_kind]
                        with id_lock:
                            from_id = rng.choice(from_list) if from_list else None
                            to_id = rng.choice(to_list) if to_list else None
                        if from_id is not None and to_id is not None:
                            store["edges"][edge_type].pop((from_id, to_id), None)

            elapsed = time.perf_counter() - start_time
            latencies[op].append(elapsed)

        return latencies

    ops = choose_ops(transactions, DEFAULT_OLTP_MIX, seed)
    chunks = [ops[i::threads] for i in range(threads)]

    print(f"Running OLTP workload ({transactions:,} ops, {threads} threads)...")

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results: Dict[str, List[float]] = {
        "read": [],
        "update": [],
        "insert": [],
        "delete": [],
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(worker, chunk, i) for i, chunk in enumerate(chunks)]
        for future in concurrent.futures.as_completed(futures):
            thread_latencies = future.result()
            for op_name, vals in thread_latencies.items():
                results[op_name].extend(vals)

    total_time = time.perf_counter() - start_time
    stop_event.set()
    rss_thread.join()

    _persist_python_memory_store(store, snapshot_file)
    disk_after_oltp = get_dir_size_bytes(db_path)

    counts_start = time.time()
    node_counts_by_type, edge_counts_by_type = _python_memory_count_by_type(
        store,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )
    node_count = sum(node_counts_by_type.values())
    edge_count = sum(edge_counts_by_type.values())
    counts_time = time.time() - counts_start

    op_counts = {op_name: len(vals) for op_name, vals in results.items()}
    total_ops = sum(op_counts.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "load_time_s": load_time,
        "schema_time_s": schema_time,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "op_counts": op_counts,
        "load_stats": load_stats,
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
        "benchmark_scope_note": BENCHMARK_SCOPE_NOTE,
    }


def _run_graph_oltp_relational(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
    db_file_name: str,
    connect_fn: Callable[[Path], Tuple[Any, Dict[str, Any]]],
    create_schema_fn: Callable[[Any], float],
    load_fn: Callable[[Any], Tuple[Dict[str, Any], Dict[str, Any]]],
    build_summary_extras: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    db_file = db_path / db_file_name
    conn, backend_meta = connect_fn(db_file)

    print("Creating schema...")
    schema_start = time.time()
    index_time_s = create_schema_fn(conn)
    schema_total_time = time.time() - schema_start
    schema_time = max(0.0, schema_total_time - index_time_s)

    print("Loading graph...")
    load_start = time.time()
    load_info, load_stats = load_fn(conn)
    load_stats["indexes"] = {"id_unique": index_time_s}
    load_time = time.time() - load_start

    disk_after_load = get_dir_size_bytes(db_path)

    load_counts_start = time.time()
    load_node_count = int(
        conn.execute(
            "SELECT count(*) FROM (SELECT Id FROM User UNION ALL SELECT Id FROM Question UNION ALL SELECT Id FROM Answer UNION ALL SELECT Id FROM Tag UNION ALL SELECT Id FROM Badge UNION ALL SELECT Id FROM Comment)"
        ).fetchone()[0]
    )
    load_edge_count = int(
        conn.execute(
            "SELECT (SELECT count(*) FROM ASKED) + (SELECT count(*) FROM ANSWERED) + (SELECT count(*) FROM HAS_ANSWER) + (SELECT count(*) FROM ACCEPTED_ANSWER) + (SELECT count(*) FROM TAGGED_WITH) + (SELECT count(*) FROM COMMENTED_ON) + (SELECT count(*) FROM COMMENTED_ON_ANSWER) + (SELECT count(*) FROM EARNED) + (SELECT count(*) FROM LINKED_TO)"
        ).fetchone()[0]
    )
    load_counts_time = time.time() - load_counts_start

    load_node_counts_by_type, load_edge_counts_by_type = _count_sqlite_by_type(
        conn,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )

    id_lock = threading.Lock()
    user_ids = load_info["ids"]["users"]
    question_ids = load_info["ids"]["questions"]
    answer_ids = load_info["ids"]["answers"]
    tag_ids = load_info["ids"]["tags"]
    badge_ids = load_info["ids"]["badges"]
    comment_ids = load_info["ids"]["comments"]
    next_user_id = load_info["max_ids"]["user"] + 1
    next_question_id = load_info["max_ids"]["question"] + 1
    next_answer_id = load_info["max_ids"]["answer"] + 1
    next_badge_id = load_info["max_ids"]["badge"] + 1
    next_comment_id = load_info["max_ids"]["comment"] + 1

    def worker(ops: List[str], worker_id: int) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        nonlocal next_user_id
        nonlocal next_question_id
        nonlocal next_answer_id
        nonlocal next_badge_id
        nonlocal next_comment_id
        local_conn, _ = connect_fn(db_file)

        def safe_execute(statement: str, params: Tuple[Any, ...] = ()) -> None:
            try:
                local_conn.execute(statement, params)
            except Exception:
                pass

        try:
            for op in ops:
                start_time = time.perf_counter()
                if op == "read":
                    read_kind = rng.choice(READ_TARGET_KINDS)
                    if read_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            local_conn.execute(
                                "SELECT q.Id FROM ASKED a JOIN Question q ON q.Id = a.to_id WHERE a.from_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()
                    elif read_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            local_conn.execute(
                                "SELECT t.Id FROM TAGGED_WITH tw JOIN Tag t ON t.Id = tw.to_id WHERE tw.from_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()
                    elif read_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            local_conn.execute(
                                "SELECT c.Id FROM COMMENTED_ON_ANSWER coa JOIN Comment c ON c.Id = coa.from_id WHERE coa.to_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()
                    elif read_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            local_conn.execute(
                                "SELECT q.Id FROM TAGGED_WITH tw JOIN Question q ON q.Id = tw.from_id WHERE tw.to_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()
                    elif read_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            row = local_conn.execute(
                                "SELECT to_id FROM COMMENTED_ON WHERE from_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()
                            if row is None:
                                local_conn.execute(
                                    "SELECT to_id FROM COMMENTED_ON_ANSWER WHERE from_id = ? LIMIT 1",
                                    (target_id,),
                                ).fetchone()
                    elif read_kind == "edge_sample":
                        edge_table = rng.choice(ARCADE_EDGE_TYPES)
                        local_conn.execute(
                            f"SELECT 1 FROM {edge_table} LIMIT 1"
                        ).fetchone()
                    else:
                        with id_lock:
                            target_id = rng.choice(badge_ids) if badge_ids else None
                        if target_id is not None:
                            local_conn.execute(
                                "SELECT u.Id FROM EARNED e JOIN User u ON u.Id = e.from_id WHERE e.to_id = ? LIMIT 1",
                                (target_id,),
                            ).fetchone()

                elif op == "update":
                    update_kind = rng.choice(UPDATE_TARGET_KINDS)
                    if update_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            safe_execute(
                                "UPDATE Question SET Score = coalesce(Score, 0) + 1 WHERE Id = ?",
                                (target_id,),
                            )
                    elif update_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            safe_execute(
                                "UPDATE Answer SET Score = coalesce(Score, 0) + 1 WHERE Id = ?",
                                (target_id,),
                            )
                    elif update_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            safe_execute(
                                "UPDATE Comment SET Score = coalesce(Score, 0) + 1 WHERE Id = ?",
                                (target_id,),
                            )
                    elif update_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            safe_execute(
                                "UPDATE User SET Reputation = coalesce(Reputation, 0) + 1 WHERE Id = ?",
                                (target_id,),
                            )
                    elif update_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            safe_execute(
                                "UPDATE Tag SET Count = coalesce(Count, 0) + 1 WHERE Id = ?",
                                (target_id,),
                            )
                    elif update_kind == "asked_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if user_id is not None and question_id is not None:
                            safe_execute(
                                "UPDATE ASKED SET CreationDate = coalesce(CreationDate, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (user_id, question_id),
                            )
                    elif update_kind == "answered_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if user_id is not None and answer_id is not None:
                            safe_execute(
                                "UPDATE ANSWERED SET CreationDate = coalesce(CreationDate, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (user_id, answer_id),
                            )
                    elif update_kind == "commented_on_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if comment_id is not None and question_id is not None:
                            safe_execute(
                                "UPDATE COMMENTED_ON SET Score = coalesce(Score, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (comment_id, question_id),
                            )
                    elif update_kind == "commented_on_answer_edge":
                        with id_lock:
                            comment_id = (
                                rng.choice(comment_ids) if comment_ids else None
                            )
                            answer_id = rng.choice(answer_ids) if answer_ids else None
                        if comment_id is not None and answer_id is not None:
                            safe_execute(
                                "UPDATE COMMENTED_ON_ANSWER SET Score = coalesce(Score, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (comment_id, answer_id),
                            )
                    elif update_kind == "earned_edge":
                        with id_lock:
                            user_id = rng.choice(user_ids) if user_ids else None
                            badge_id = rng.choice(badge_ids) if badge_ids else None
                        if user_id is not None and badge_id is not None:
                            safe_execute(
                                "UPDATE EARNED SET Class = coalesce(Class, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (user_id, badge_id),
                            )
                    elif update_kind == "linked_to_edge":
                        with id_lock:
                            question_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                            related_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if question_id is not None and related_id is not None:
                            safe_execute(
                                "UPDATE LINKED_TO SET LinkTypeId = coalesce(LinkTypeId, 0) + 1 WHERE from_id = ? AND to_id = ?",
                                (question_id, related_id),
                            )

                elif op == "insert":
                    with id_lock:
                        insert_kind = rng.choice(INSERT_TARGET_KINDS)
                        now_ms = int(time.time() * 1000)
                        new_user_id: Optional[int] = None
                        new_question_id: Optional[int] = None
                        new_answer_id: Optional[int] = None
                        new_comment_id: Optional[int] = None
                        new_badge_id: Optional[int] = None
                        user_id = rng.choice(user_ids) if user_ids else None
                        question_id = rng.choice(question_ids) if question_ids else None
                        answer_id = rng.choice(answer_ids) if answer_ids else None
                        tag_id = rng.choice(tag_ids) if tag_ids else None
                        second_question_id = (
                            rng.choice(question_ids) if question_ids else None
                        )
                        if insert_kind == "user_question":
                            new_user_id = next_user_id
                            next_user_id += 1
                            new_question_id = next_question_id
                            next_question_id += 1
                        elif insert_kind == "answer":
                            new_answer_id = next_answer_id
                            next_answer_id += 1
                        elif insert_kind == "comment":
                            new_comment_id = next_comment_id
                            next_comment_id += 1
                        elif insert_kind == "badge":
                            new_badge_id = next_badge_id
                            next_badge_id += 1

                    if (
                        insert_kind == "user_question"
                        and new_user_id is not None
                        and new_question_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO User(Id, DisplayName, Reputation, CreationDate) VALUES (?, 'Synthetic', 0, ?)",
                            (new_user_id, now_ms),
                        )
                        safe_execute(
                            "INSERT INTO Question(Id, Title, Body, Score, CreationDate) VALUES (?, 'Synthetic', 'Synthetic body', 0, ?)",
                            (new_question_id, now_ms),
                        )
                        safe_execute(
                            "INSERT INTO ASKED(from_id, to_id, CreationDate) VALUES (?, ?, ?)",
                            (new_user_id, new_question_id, now_ms),
                        )
                        with id_lock:
                            user_ids.append(new_user_id)
                            question_ids.append(new_question_id)
                    elif (
                        insert_kind == "answer"
                        and new_answer_id is not None
                        and user_id is not None
                        and question_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO Answer(Id, Body, Score, CreationDate, CommentCount) VALUES (?, 'Synthetic answer', 0, ?, 0)",
                            (new_answer_id, now_ms),
                        )
                        safe_execute(
                            "INSERT INTO ANSWERED(from_id, to_id, CreationDate) VALUES (?, ?, ?)",
                            (user_id, new_answer_id, now_ms),
                        )
                        safe_execute(
                            "INSERT INTO HAS_ANSWER(from_id, to_id) VALUES (?, ?)",
                            (question_id, new_answer_id),
                        )
                        with id_lock:
                            answer_ids.append(new_answer_id)
                    elif insert_kind == "comment" and new_comment_id is not None:
                        if question_id is not None:
                            safe_execute(
                                "INSERT INTO Comment(Id, Text, Score, CreationDate) VALUES (?, 'Synthetic comment', 0, ?)",
                                (new_comment_id, now_ms),
                            )
                            safe_execute(
                                "INSERT INTO COMMENTED_ON(from_id, to_id, CreationDate, Score) VALUES (?, ?, ?, 0)",
                                (new_comment_id, question_id, now_ms),
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                        elif answer_id is not None:
                            safe_execute(
                                "INSERT INTO Comment(Id, Text, Score, CreationDate) VALUES (?, 'Synthetic comment', 0, ?)",
                                (new_comment_id, now_ms),
                            )
                            safe_execute(
                                "INSERT INTO COMMENTED_ON_ANSWER(from_id, to_id, CreationDate, Score) VALUES (?, ?, ?, 0)",
                                (new_comment_id, answer_id, now_ms),
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                    elif (
                        insert_kind == "badge"
                        and new_badge_id is not None
                        and user_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO Badge(Id, Name, Date, Class) VALUES (?, 'SyntheticBadge', ?, 1)",
                            (new_badge_id, now_ms),
                        )
                        safe_execute(
                            "INSERT INTO EARNED(from_id, to_id, Date, Class) VALUES (?, ?, ?, 1)",
                            (user_id, new_badge_id, now_ms),
                        )
                        with id_lock:
                            badge_ids.append(new_badge_id)
                    elif (
                        insert_kind == "tag_link"
                        and question_id is not None
                        and tag_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO TAGGED_WITH(from_id, to_id) VALUES (?, ?)",
                            (question_id, tag_id),
                        )
                    elif (
                        insert_kind == "post_link"
                        and question_id is not None
                        and second_question_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO LINKED_TO(from_id, to_id, LinkTypeId, CreationDate) VALUES (?, ?, 1, ?)",
                            (question_id, second_question_id, now_ms),
                        )
                    elif (
                        insert_kind == "accepted_answer"
                        and question_id is not None
                        and answer_id is not None
                    ):
                        safe_execute(
                            "INSERT INTO ACCEPTED_ANSWER(from_id, to_id) VALUES (?, ?)",
                            (question_id, answer_id),
                        )

                else:
                    delete_kind = rng.choice(DELETE_TARGET_KINDS)
                    if delete_kind in {
                        "question",
                        "answer",
                        "comment",
                        "badge",
                        "user",
                        "tag",
                    }:
                        mapping = {
                            "question": (question_ids, "Question"),
                            "answer": (answer_ids, "Answer"),
                            "comment": (comment_ids, "Comment"),
                            "badge": (badge_ids, "Badge"),
                            "user": (user_ids, "User"),
                            "tag": (tag_ids, "Tag"),
                        }
                        target_list, label = mapping[delete_kind]
                        with id_lock:
                            target_id = rng.choice(target_list) if target_list else None
                        if target_id is not None:
                            if label == "Question":
                                safe_execute(
                                    "DELETE FROM ASKED WHERE to_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM HAS_ANSWER WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM ACCEPTED_ANSWER WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM TAGGED_WITH WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM COMMENTED_ON WHERE to_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM LINKED_TO WHERE from_id = ? OR to_id = ?",
                                    (target_id, target_id),
                                )
                            elif label == "Answer":
                                safe_execute(
                                    "DELETE FROM ANSWERED WHERE to_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM HAS_ANSWER WHERE to_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM ACCEPTED_ANSWER WHERE to_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM COMMENTED_ON_ANSWER WHERE to_id = ?",
                                    (target_id,),
                                )
                            elif label == "Comment":
                                safe_execute(
                                    "DELETE FROM COMMENTED_ON WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM COMMENTED_ON_ANSWER WHERE from_id = ?",
                                    (target_id,),
                                )
                            elif label == "Badge":
                                safe_execute(
                                    "DELETE FROM EARNED WHERE to_id = ?",
                                    (target_id,),
                                )
                            elif label == "User":
                                safe_execute(
                                    "DELETE FROM ASKED WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM ANSWERED WHERE from_id = ?",
                                    (target_id,),
                                )
                                safe_execute(
                                    "DELETE FROM EARNED WHERE from_id = ?",
                                    (target_id,),
                                )
                            elif label == "Tag":
                                safe_execute(
                                    "DELETE FROM TAGGED_WITH WHERE to_id = ?",
                                    (target_id,),
                                )

                            safe_execute(
                                f"DELETE FROM {label} WHERE Id = ?", (target_id,)
                            )
                            with id_lock:
                                try:
                                    target_list.remove(target_id)
                                except ValueError:
                                    pass
                    elif delete_kind == "asked_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            qid = rng.choice(question_ids) if question_ids else None
                        if uid is not None and qid is not None:
                            safe_execute(
                                "DELETE FROM ASKED WHERE from_id = ? AND to_id = ?",
                                (uid, qid),
                            )
                    elif delete_kind == "answered_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if uid is not None and aid is not None:
                            safe_execute(
                                "DELETE FROM ANSWERED WHERE from_id = ? AND to_id = ?",
                                (uid, aid),
                            )
                    elif delete_kind == "has_answer_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if qid is not None and aid is not None:
                            safe_execute(
                                "DELETE FROM HAS_ANSWER WHERE from_id = ? AND to_id = ?",
                                (qid, aid),
                            )
                    elif delete_kind == "accepted_answer_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if qid is not None and aid is not None:
                            safe_execute(
                                "DELETE FROM ACCEPTED_ANSWER WHERE from_id = ? AND to_id = ?",
                                (qid, aid),
                            )
                    elif delete_kind == "tagged_with_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            tid = rng.choice(tag_ids) if tag_ids else None
                        if qid is not None and tid is not None:
                            safe_execute(
                                "DELETE FROM TAGGED_WITH WHERE from_id = ? AND to_id = ?",
                                (qid, tid),
                            )
                    elif delete_kind == "commented_on_edge":
                        with id_lock:
                            cid = rng.choice(comment_ids) if comment_ids else None
                            qid = rng.choice(question_ids) if question_ids else None
                        if cid is not None and qid is not None:
                            safe_execute(
                                "DELETE FROM COMMENTED_ON WHERE from_id = ? AND to_id = ?",
                                (cid, qid),
                            )
                    elif delete_kind == "commented_on_answer_edge":
                        with id_lock:
                            cid = rng.choice(comment_ids) if comment_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if cid is not None and aid is not None:
                            safe_execute(
                                "DELETE FROM COMMENTED_ON_ANSWER WHERE from_id = ? AND to_id = ?",
                                (cid, aid),
                            )
                    elif delete_kind == "earned_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            bid = rng.choice(badge_ids) if badge_ids else None
                        if uid is not None and bid is not None:
                            safe_execute(
                                "DELETE FROM EARNED WHERE from_id = ? AND to_id = ?",
                                (uid, bid),
                            )
                    elif delete_kind == "linked_to_edge":
                        with id_lock:
                            q1 = rng.choice(question_ids) if question_ids else None
                            q2 = rng.choice(question_ids) if question_ids else None
                        if q1 is not None and q2 is not None:
                            safe_execute(
                                "DELETE FROM LINKED_TO WHERE from_id = ? AND to_id = ?",
                                (q1, q2),
                            )

                elapsed = time.perf_counter() - start_time
                latencies[op].append(elapsed)
        finally:
            local_conn.commit()
            local_conn.close()

        return latencies

    ops = choose_ops(transactions, DEFAULT_OLTP_MIX, seed)
    chunks = [ops[i::threads] for i in range(threads)]

    print(f"Running OLTP workload ({transactions:,} ops, {threads} threads)...")

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results: Dict[str, List[float]] = {
        "read": [],
        "update": [],
        "insert": [],
        "delete": [],
    }

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
    node_count = int(
        conn.execute(
            "SELECT count(*) FROM (SELECT Id FROM User UNION ALL SELECT Id FROM Question UNION ALL SELECT Id FROM Answer UNION ALL SELECT Id FROM Tag UNION ALL SELECT Id FROM Badge UNION ALL SELECT Id FROM Comment)"
        ).fetchone()[0]
    )
    edge_count = int(
        conn.execute(
            "SELECT (SELECT count(*) FROM ASKED) + (SELECT count(*) FROM ANSWERED) + (SELECT count(*) FROM HAS_ANSWER) + (SELECT count(*) FROM ACCEPTED_ANSWER) + (SELECT count(*) FROM TAGGED_WITH) + (SELECT count(*) FROM COMMENTED_ON) + (SELECT count(*) FROM COMMENTED_ON_ANSWER) + (SELECT count(*) FROM EARNED) + (SELECT count(*) FROM LINKED_TO)"
        ).fetchone()[0]
    )
    counts_time = time.time() - counts_start

    node_counts_by_type, edge_counts_by_type = _count_sqlite_by_type(
        conn,
        ARCADE_VERTEX_TYPES,
        ARCADE_EDGE_TYPES,
    )

    conn.commit()
    conn.close()

    op_counts = {op_name: len(vals) for op_name, vals in results.items()}
    total_ops = sum(op_counts.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    summary_extras = build_summary_extras(backend_meta) if build_summary_extras else {}

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "load_time_s": load_time,
        "schema_time_s": schema_time,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "op_counts": op_counts,
        "load_stats": load_stats,
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
        "benchmark_scope_note": BENCHMARK_SCOPE_NOTE,
        **summary_extras,
    }


def run_graph_oltp_sqlite(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
    sqlite_profile: str = "perf",
) -> dict:
    return _run_graph_oltp_relational(
        db_path=db_path,
        data_dir=data_dir,
        batch_size=batch_size,
        transactions=transactions,
        threads=threads,
        seed=seed,
        db_file_name="sqlite.sqlite",
        connect_fn=lambda db_file: _connect_sqlite(db_file, sqlite_profile),
        create_schema_fn=_create_sqlite_schema,
        load_fn=lambda conn: _load_stackoverflow_sqlite(
            conn,
            data_dir,
            batch_size=max(1, batch_size),
        ),
        build_summary_extras=lambda meta: {
            "sqlite_profile": sqlite_profile,
            "sqlite_pragmas": meta,
        },
    )


def run_graph_oltp_duckdb(
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
) -> dict:
    return _run_graph_oltp_relational(
        db_path=db_path,
        data_dir=data_dir,
        batch_size=batch_size,
        transactions=transactions,
        threads=threads,
        seed=seed,
        db_file_name="duckdb.duckdb",
        connect_fn=_connect_duckdb,
        create_schema_fn=_create_duckdb_schema,
        load_fn=lambda conn: _load_stackoverflow_duckdb(
            conn,
            db_path,
            data_dir,
            batch_size=max(1, batch_size),
        ),
        build_summary_extras=lambda meta: {
            "duckdb_version": meta.get("duckdb_version"),
            "duckdb_runtime_version": meta.get("duckdb_runtime_version"),
        },
    )


def run_graph_oltp_sqlite_layer(
    db_kind: str,
    db_path: Path,
    data_dir: Path,
    batch_size: int,
    transactions: int,
    threads: int,
    seed: int,
    sqlite_profile: str = "perf",
) -> dict:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    db_file = db_path / f"{db_kind}.sqlite"
    close_fn = None
    open_worker_executor = None
    execute_cypher = None

    if db_kind == "graphqlite":
        graphqlite = get_graphqlite_module()
        if graphqlite is None:
            raise RuntimeError("graphqlite is not installed")
        bootstrap_conn = sqlite3.connect(str(db_file), timeout=30.0)
        sqlite_pragmas = _configure_sqlite_connection(bootstrap_conn, sqlite_profile)
        bootstrap_conn.close()
        graph = graphqlite.Graph(str(db_file))

        def execute_cypher(
            query: str,
            parameters: Optional[Dict[str, Any]] = None,
        ) -> List[Dict[str, Any]]:
            result = graph.connection.cypher(query, parameters)
            if hasattr(result, "to_list"):
                return result.to_list()
            return list(result or [])

        def open_worker_executor():
            worker_db = graphqlite.connect(str(db_file), check_same_thread=False)

            def worker_execute(
                query: str,
                parameters: Optional[Dict[str, Any]] = None,
            ) -> List[Dict[str, Any]]:
                result = worker_db.cypher(query, parameters)
                if hasattr(result, "to_list"):
                    return result.to_list()
                return list(result or [])

            return worker_execute, worker_db.close

        close_fn = graph.close
    else:
        raise ValueError("Only graphqlite is supported for SQLite graph-layer backend")

    print("Creating schema...")
    schema_start = time.time()
    schema_time = time.time() - schema_start

    print("Loading graph...")
    load_start = time.time()
    load_info, load_stats = _load_stackoverflow_graphqlite_bulk(
        graph,
        data_dir,
        batch_size=max(1, batch_size),
    )
    load_time = time.time() - load_start

    disk_after_load = get_dir_size_bytes(db_path)

    load_counts_start = time.time()
    load_node_count = _cypher_count(
        execute_cypher("MATCH (n) RETURN count(n) AS count")
    )
    load_edge_count = _cypher_count(
        execute_cypher("MATCH ()-[r]->() RETURN count(r) AS count")
    )
    load_counts_time = time.time() - load_counts_start

    load_node_counts_by_type = {
        "User": _cypher_count(
            execute_cypher("MATCH (n:User) RETURN count(n) AS count")
        ),
        "Question": _cypher_count(
            execute_cypher("MATCH (n:Question) RETURN count(n) AS count")
        ),
        "Answer": _cypher_count(
            execute_cypher("MATCH (n:Answer) RETURN count(n) AS count")
        ),
        "Tag": _cypher_count(execute_cypher("MATCH (n:Tag) RETURN count(n) AS count")),
        "Badge": _cypher_count(
            execute_cypher("MATCH (n:Badge) RETURN count(n) AS count")
        ),
        "Comment": _cypher_count(
            execute_cypher("MATCH (n:Comment) RETURN count(n) AS count")
        ),
    }
    load_edge_counts_by_type = {
        "ASKED": _cypher_count(
            execute_cypher("MATCH ()-[r:ASKED]->() RETURN count(r) AS count")
        ),
        "ANSWERED": _cypher_count(
            execute_cypher("MATCH ()-[r:ANSWERED]->() RETURN count(r) AS count")
        ),
        "HAS_ANSWER": _cypher_count(
            execute_cypher("MATCH ()-[r:HAS_ANSWER]->() RETURN count(r) AS count")
        ),
        "ACCEPTED_ANSWER": _cypher_count(
            execute_cypher("MATCH ()-[r:ACCEPTED_ANSWER]->() RETURN count(r) AS count")
        ),
        "TAGGED_WITH": _cypher_count(
            execute_cypher("MATCH ()-[r:TAGGED_WITH]->() RETURN count(r) AS count")
        ),
        "COMMENTED_ON": _cypher_count(
            execute_cypher("MATCH ()-[r:COMMENTED_ON]->() RETURN count(r) AS count")
        ),
        "COMMENTED_ON_ANSWER": _cypher_count(
            execute_cypher(
                "MATCH ()-[r:COMMENTED_ON_ANSWER]->() RETURN count(r) AS count"
            )
        ),
        "EARNED": _cypher_count(
            execute_cypher("MATCH ()-[r:EARNED]->() RETURN count(r) AS count")
        ),
        "LINKED_TO": _cypher_count(
            execute_cypher("MATCH ()-[r:LINKED_TO]->() RETURN count(r) AS count")
        ),
    }

    id_lock = threading.Lock()
    user_ids = load_info["ids"]["users"]
    question_ids = load_info["ids"]["questions"]
    answer_ids = load_info["ids"]["answers"]
    tag_ids = load_info["ids"]["tags"]
    badge_ids = load_info["ids"]["badges"]
    comment_ids = load_info["ids"]["comments"]
    next_user_id = load_info["max_ids"]["user"] + 1
    next_question_id = load_info["max_ids"]["question"] + 1
    next_answer_id = load_info["max_ids"]["answer"] + 1
    next_badge_id = load_info["max_ids"]["badge"] + 1
    next_comment_id = load_info["max_ids"]["comment"] + 1

    def worker(ops: List[str], worker_id: int) -> Dict[str, List[float]]:
        rng = random.Random(seed + worker_id)
        latencies = {"read": [], "update": [], "insert": [], "delete": []}
        nonlocal next_user_id
        nonlocal next_question_id
        nonlocal next_answer_id
        nonlocal next_badge_id
        nonlocal next_comment_id
        worker_execute_cypher, worker_close = open_worker_executor()

        def safe_execute(query: str, parameters: Optional[Dict[str, Any]] = None):
            try:
                if parameters:
                    return worker_execute_cypher(query, parameters)
                return worker_execute_cypher(query)
            except Exception:
                return []

        try:
            for op in ops:
                start_time = time.perf_counter()
                if op == "read":
                    read_kind = rng.choice(READ_TARGET_KINDS)
                    if read_kind == "user":
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (u:User {Id: $id})-[:ASKED|ANSWERED]->(p) RETURN p.Id LIMIT 1",
                                {"id": target_id},
                            )
                    elif read_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            safe_execute(
                                "MATCH (q:Question {Id: $id})-[:TAGGED_WITH]->(t:Tag) RETURN t.Id LIMIT 1",
                                {"id": target_id},
                            )
                    elif read_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (a:Answer {Id: $id})<-[:COMMENTED_ON_ANSWER]-(c:Comment) RETURN c.Id LIMIT 1",
                                {"id": target_id},
                            )
                    elif read_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag {Id: $id}) RETURN q.Id LIMIT 1",
                                {"id": target_id},
                            )
                    elif read_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (c:Comment {Id: $id})-[r:COMMENTED_ON|COMMENTED_ON_ANSWER]->(p) RETURN p.Id LIMIT 1",
                                {"id": target_id},
                            )
                    elif read_kind == "edge_sample":
                        safe_execute(
                            "MATCH ()-[r:ASKED|ANSWERED|HAS_ANSWER|ACCEPTED_ANSWER|TAGGED_WITH|COMMENTED_ON|COMMENTED_ON_ANSWER|EARNED|LINKED_TO]->() RETURN r LIMIT 1"
                        )
                    else:
                        with id_lock:
                            target_id = rng.choice(badge_ids) if badge_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (u:User)-[:EARNED]->(b:Badge {Id: $id}) RETURN u.Id LIMIT 1",
                                {"id": target_id},
                            )
                elif op == "update":
                    update_kind = rng.choice(UPDATE_TARGET_KINDS)
                    set_value = rng.randint(1, 1_000_000)
                    if update_kind == "question":
                        with id_lock:
                            target_id = (
                                rng.choice(question_ids) if question_ids else None
                            )
                        if target_id is not None:
                            safe_execute(
                                "MATCH (q:Question {Id: $id}) SET q.Score = $val",
                                {"id": target_id, "val": set_value},
                            )
                    elif update_kind == "answer":
                        with id_lock:
                            target_id = rng.choice(answer_ids) if answer_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (a:Answer {Id: $id}) SET a.Score = $val",
                                {"id": target_id, "val": set_value},
                            )
                    elif update_kind == "comment":
                        with id_lock:
                            target_id = rng.choice(comment_ids) if comment_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (c:Comment {Id: $id}) SET c.Score = $val",
                                {"id": target_id, "val": set_value},
                            )
                    elif update_kind == "tag":
                        with id_lock:
                            target_id = rng.choice(tag_ids) if tag_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (t:Tag {Id: $id}) SET t.Count = $val",
                                {"id": target_id, "val": set_value},
                            )
                    else:
                        with id_lock:
                            target_id = rng.choice(user_ids) if user_ids else None
                        if target_id is not None:
                            safe_execute(
                                "MATCH (u:User {Id: $id}) SET u.Reputation = $val",
                                {"id": target_id, "val": set_value},
                            )
                elif op == "insert":
                    with id_lock:
                        insert_kind = rng.choice(INSERT_TARGET_KINDS)
                        now_ms = int(time.time() * 1000)
                        new_user_id = None
                        new_question_id = None
                        new_answer_id = None
                        new_comment_id = None
                        new_badge_id = None
                        user_id = rng.choice(user_ids) if user_ids else None
                        question_id = rng.choice(question_ids) if question_ids else None
                        answer_id = rng.choice(answer_ids) if answer_ids else None
                        tag_id = rng.choice(tag_ids) if tag_ids else None
                        second_question_id = (
                            rng.choice(question_ids) if question_ids else None
                        )
                        if insert_kind == "user_question":
                            new_user_id = next_user_id
                            next_user_id += 1
                            new_question_id = next_question_id
                            next_question_id += 1
                        elif insert_kind == "answer":
                            new_answer_id = next_answer_id
                            next_answer_id += 1
                        elif insert_kind == "comment":
                            new_comment_id = next_comment_id
                            next_comment_id += 1
                        elif insert_kind == "badge":
                            new_badge_id = next_badge_id
                            next_badge_id += 1

                    if (
                        insert_kind == "user_question"
                        and new_user_id is not None
                        and new_question_id is not None
                    ):
                        safe_execute(
                            "CREATE (u:User {Id: $uid, DisplayName: 'Synthetic', Reputation: 0, CreationDate: $ts})",
                            {"uid": new_user_id, "ts": now_ms},
                        )
                        safe_execute(
                            "CREATE (q:Question {Id: $qid, Title: 'Synthetic', Body: 'Synthetic body', Score: 0, CreationDate: $ts})",
                            {"qid": new_question_id, "ts": now_ms},
                        )
                        safe_execute(
                            "MATCH (u:User {Id: $uid}), (q:Question {Id: $qid}) CREATE (u)-[:ASKED {CreationDate: $ts}]->(q)",
                            {"uid": new_user_id, "qid": new_question_id, "ts": now_ms},
                        )
                        with id_lock:
                            user_ids.append(new_user_id)
                            question_ids.append(new_question_id)
                    elif (
                        insert_kind == "answer"
                        and new_answer_id is not None
                        and user_id is not None
                        and question_id is not None
                    ):
                        safe_execute(
                            "CREATE (a:Answer {Id: $aid, Body: 'Synthetic answer', Score: 0, CreationDate: $ts, CommentCount: 0})",
                            {"aid": new_answer_id, "ts": now_ms},
                        )
                        safe_execute(
                            "MATCH (u:User {Id: $uid}), (a:Answer {Id: $aid}) CREATE (u)-[:ANSWERED {CreationDate: $ts}]->(a)",
                            {"uid": user_id, "aid": new_answer_id, "ts": now_ms},
                        )
                        safe_execute(
                            "MATCH (q:Question {Id: $qid}), (a:Answer {Id: $aid}) CREATE (q)-[:HAS_ANSWER]->(a)",
                            {"qid": question_id, "aid": new_answer_id},
                        )
                        with id_lock:
                            answer_ids.append(new_answer_id)
                    elif insert_kind == "comment" and new_comment_id is not None:
                        if question_id is not None:
                            safe_execute(
                                "CREATE (c:Comment {Id: $cid, Text: 'Synthetic comment', Score: 0, CreationDate: $ts})",
                                {"cid": new_comment_id, "ts": now_ms},
                            )
                            safe_execute(
                                "MATCH (c:Comment {Id: $cid}), (q:Question {Id: $qid}) CREATE (c)-[:COMMENTED_ON {CreationDate: $ts, Score: 0}]->(q)",
                                {
                                    "cid": new_comment_id,
                                    "qid": question_id,
                                    "ts": now_ms,
                                },
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                        elif answer_id is not None:
                            safe_execute(
                                "CREATE (c:Comment {Id: $cid, Text: 'Synthetic comment', Score: 0, CreationDate: $ts})",
                                {"cid": new_comment_id, "ts": now_ms},
                            )
                            safe_execute(
                                "MATCH (c:Comment {Id: $cid}), (a:Answer {Id: $aid}) CREATE (c)-[:COMMENTED_ON_ANSWER {CreationDate: $ts, Score: 0}]->(a)",
                                {"cid": new_comment_id, "aid": answer_id, "ts": now_ms},
                            )
                            with id_lock:
                                comment_ids.append(new_comment_id)
                    elif (
                        insert_kind == "badge"
                        and new_badge_id is not None
                        and user_id is not None
                    ):
                        safe_execute(
                            "CREATE (b:Badge {Id: $bid, Name: 'SyntheticBadge', Date: $ts, Class: 1})",
                            {"bid": new_badge_id, "ts": now_ms},
                        )
                        safe_execute(
                            "MATCH (u:User {Id: $uid}), (b:Badge {Id: $bid}) CREATE (u)-[:EARNED {Date: $ts, Class: 1}]->(b)",
                            {"uid": user_id, "bid": new_badge_id, "ts": now_ms},
                        )
                        with id_lock:
                            badge_ids.append(new_badge_id)
                    elif (
                        insert_kind == "tag_link"
                        and question_id is not None
                        and tag_id is not None
                    ):
                        safe_execute(
                            "MATCH (q:Question {Id: $qid}), (t:Tag {Id: $tid}) CREATE (q)-[:TAGGED_WITH]->(t)",
                            {"qid": question_id, "tid": tag_id},
                        )
                    elif (
                        insert_kind == "post_link"
                        and question_id is not None
                        and second_question_id is not None
                    ):
                        safe_execute(
                            "MATCH (q1:Question {Id: $qid}), (q2:Question {Id: $rid}) CREATE (q1)-[:LINKED_TO {LinkTypeId: 1, CreationDate: $ts}]->(q2)",
                            {
                                "qid": question_id,
                                "rid": second_question_id,
                                "ts": now_ms,
                            },
                        )
                    elif (
                        insert_kind == "accepted_answer"
                        and question_id is not None
                        and answer_id is not None
                    ):
                        safe_execute(
                            "MATCH (q:Question {Id: $qid}), (a:Answer {Id: $aid}) CREATE (q)-[:ACCEPTED_ANSWER]->(a)",
                            {"qid": question_id, "aid": answer_id},
                        )
                else:
                    delete_kind = rng.choice(DELETE_TARGET_KINDS)
                    if delete_kind in {
                        "question",
                        "answer",
                        "comment",
                        "badge",
                        "user",
                        "tag",
                    }:
                        mapping = {
                            "question": (question_ids, "Question"),
                            "answer": (answer_ids, "Answer"),
                            "comment": (comment_ids, "Comment"),
                            "badge": (badge_ids, "Badge"),
                            "user": (user_ids, "User"),
                            "tag": (tag_ids, "Tag"),
                        }
                        target_list, label = mapping[delete_kind]
                        with id_lock:
                            target_id = rng.choice(target_list) if target_list else None
                        if target_id is not None:
                            safe_execute(
                                f"MATCH (n:{label} {{Id: $id}})-[r]->() DELETE r",
                                {"id": target_id},
                            )
                            safe_execute(
                                f"MATCH ()-[r]->(n:{label} {{Id: $id}}) DELETE r",
                                {"id": target_id},
                            )
                            safe_execute(
                                f"MATCH (n:{label} {{Id: $id}}) DELETE n",
                                {"id": target_id},
                            )
                            with id_lock:
                                try:
                                    target_list.remove(target_id)
                                except ValueError:
                                    pass
                    elif delete_kind == "asked_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            qid = rng.choice(question_ids) if question_ids else None
                        if uid is not None and qid is not None:
                            safe_execute(
                                "MATCH (u:User {Id: $uid})-[r:ASKED]->(q:Question {Id: $qid}) DELETE r",
                                {"uid": uid, "qid": qid},
                            )
                    elif delete_kind == "answered_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if uid is not None and aid is not None:
                            safe_execute(
                                "MATCH (u:User {Id: $uid})-[r:ANSWERED]->(a:Answer {Id: $aid}) DELETE r",
                                {"uid": uid, "aid": aid},
                            )
                    elif delete_kind == "has_answer_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if qid is not None and aid is not None:
                            safe_execute(
                                "MATCH (q:Question {Id: $qid})-[r:HAS_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                {"qid": qid, "aid": aid},
                            )
                    elif delete_kind == "accepted_answer_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if qid is not None and aid is not None:
                            safe_execute(
                                "MATCH (q:Question {Id: $qid})-[r:ACCEPTED_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                {"qid": qid, "aid": aid},
                            )
                    elif delete_kind == "tagged_with_edge":
                        with id_lock:
                            qid = rng.choice(question_ids) if question_ids else None
                            tid = rng.choice(tag_ids) if tag_ids else None
                        if qid is not None and tid is not None:
                            safe_execute(
                                "MATCH (q:Question {Id: $qid})-[r:TAGGED_WITH]->(t:Tag {Id: $tid}) DELETE r",
                                {"qid": qid, "tid": tid},
                            )
                    elif delete_kind == "commented_on_edge":
                        with id_lock:
                            cid = rng.choice(comment_ids) if comment_ids else None
                            qid = rng.choice(question_ids) if question_ids else None
                        if cid is not None and qid is not None:
                            safe_execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON]->(q:Question {Id: $qid}) DELETE r",
                                {"cid": cid, "qid": qid},
                            )
                    elif delete_kind == "commented_on_answer_edge":
                        with id_lock:
                            cid = rng.choice(comment_ids) if comment_ids else None
                            aid = rng.choice(answer_ids) if answer_ids else None
                        if cid is not None and aid is not None:
                            safe_execute(
                                "MATCH (c:Comment {Id: $cid})-[r:COMMENTED_ON_ANSWER]->(a:Answer {Id: $aid}) DELETE r",
                                {"cid": cid, "aid": aid},
                            )
                    elif delete_kind == "earned_edge":
                        with id_lock:
                            uid = rng.choice(user_ids) if user_ids else None
                            bid = rng.choice(badge_ids) if badge_ids else None
                        if uid is not None and bid is not None:
                            safe_execute(
                                "MATCH (u:User {Id: $uid})-[r:EARNED]->(b:Badge {Id: $bid}) DELETE r",
                                {"uid": uid, "bid": bid},
                            )
                    elif delete_kind == "linked_to_edge":
                        with id_lock:
                            q1 = rng.choice(question_ids) if question_ids else None
                            q2 = rng.choice(question_ids) if question_ids else None
                        if q1 is not None and q2 is not None:
                            safe_execute(
                                "MATCH (q1:Question {Id: $q1})-[r:LINKED_TO]->(q2:Question {Id: $q2}) DELETE r",
                                {"q1": q1, "q2": q2},
                            )

                elapsed = time.perf_counter() - start_time
                latencies[op].append(elapsed)
        finally:
            worker_close()

        return latencies

    ops = choose_ops(transactions, DEFAULT_OLTP_MIX, seed)
    chunks = [ops[i::threads] for i in range(threads)]

    print(f"Running OLTP workload ({transactions:,} ops, {threads} threads)...")

    stop_event, rss_state, rss_thread = start_rss_sampler()
    start_time = time.perf_counter()

    results: Dict[str, List[float]] = {
        "read": [],
        "update": [],
        "insert": [],
        "delete": [],
    }

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
    node_count = _cypher_count(execute_cypher("MATCH (n) RETURN count(n) AS count"))
    edge_count = _cypher_count(
        execute_cypher("MATCH ()-[r]->() RETURN count(r) AS count")
    )
    counts_time = time.time() - counts_start

    node_counts_by_type = {
        "User": _cypher_count(
            execute_cypher("MATCH (n:User) RETURN count(n) AS count")
        ),
        "Question": _cypher_count(
            execute_cypher("MATCH (n:Question) RETURN count(n) AS count")
        ),
        "Answer": _cypher_count(
            execute_cypher("MATCH (n:Answer) RETURN count(n) AS count")
        ),
        "Tag": _cypher_count(execute_cypher("MATCH (n:Tag) RETURN count(n) AS count")),
        "Badge": _cypher_count(
            execute_cypher("MATCH (n:Badge) RETURN count(n) AS count")
        ),
        "Comment": _cypher_count(
            execute_cypher("MATCH (n:Comment) RETURN count(n) AS count")
        ),
    }
    edge_counts_by_type = {
        "ASKED": _cypher_count(
            execute_cypher("MATCH ()-[r:ASKED]->() RETURN count(r) AS count")
        ),
        "ANSWERED": _cypher_count(
            execute_cypher("MATCH ()-[r:ANSWERED]->() RETURN count(r) AS count")
        ),
        "HAS_ANSWER": _cypher_count(
            execute_cypher("MATCH ()-[r:HAS_ANSWER]->() RETURN count(r) AS count")
        ),
        "ACCEPTED_ANSWER": _cypher_count(
            execute_cypher("MATCH ()-[r:ACCEPTED_ANSWER]->() RETURN count(r) AS count")
        ),
        "TAGGED_WITH": _cypher_count(
            execute_cypher("MATCH ()-[r:TAGGED_WITH]->() RETURN count(r) AS count")
        ),
        "COMMENTED_ON": _cypher_count(
            execute_cypher("MATCH ()-[r:COMMENTED_ON]->() RETURN count(r) AS count")
        ),
        "COMMENTED_ON_ANSWER": _cypher_count(
            execute_cypher(
                "MATCH ()-[r:COMMENTED_ON_ANSWER]->() RETURN count(r) AS count"
            )
        ),
        "EARNED": _cypher_count(
            execute_cypher("MATCH ()-[r:EARNED]->() RETURN count(r) AS count")
        ),
        "LINKED_TO": _cypher_count(
            execute_cypher("MATCH ()-[r:LINKED_TO]->() RETURN count(r) AS count")
        ),
    }

    close_fn()

    op_counts = {op_name: len(vals) for op_name, vals in results.items()}
    total_ops = sum(op_counts.values())
    throughput = total_ops / total_time if total_time > 0 else 0

    return {
        "total_ops": total_ops,
        "total_time_s": total_time,
        "throughput_ops_s": throughput,
        "load_time_s": load_time,
        "schema_time_s": schema_time,
        "disk_after_load_bytes": disk_after_load,
        "disk_after_oltp_bytes": disk_after_oltp,
        "rss_peak_kb": rss_state["max_kb"],
        "latencies": results,
        "op_counts": op_counts,
        "load_stats": load_stats,
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
        "sqlite_profile": sqlite_profile,
        "sqlite_pragmas": sqlite_pragmas,
        "benchmark_scope_note": BENCHMARK_SCOPE_NOTE,
    }


def build_latency_summary(latencies: Dict[str, List[float]]) -> dict:
    points = [50, 95, 99]
    summary = {"ops": {}, "overall": None}

    for op_name in ["read", "update", "insert", "delete"]:
        values = latencies.get(op_name, [])
        values_sorted = sorted(values)
        if values_sorted:
            op_summary = {}
            for p in points:
                idx = int(round((p / 100.0) * (len(values_sorted) - 1)))
                op_summary[str(p)] = values_sorted[idx]
            summary["ops"][op_name] = op_summary
        else:
            summary["ops"][op_name] = {str(p): 0.0 for p in points}

    all_values = [v for vals in latencies.values() for v in vals]
    if all_values:
        all_values_sorted = sorted(all_values)
        overall = {}
        for p in points:
            idx = int(round((p / 100.0) * (len(all_values_sorted) - 1)))
            overall[str(p)] = all_values_sorted[idx]
        summary["overall"] = overall

    return summary


def write_results(db_path: Path, args: argparse.Namespace, summary: dict):
    if args.run_label:
        results_path = db_path / f"results_{args.run_label}.json"
    else:
        results_path = db_path / "results.json"
    arcadedb_module, _ = get_arcadedb_module()
    ladybug_module = get_ladybug_module()
    graphqlite_module = get_graphqlite_module()
    duckdb_module = get_duckdb_module()
    payload = {
        "dataset": args.dataset,
        "db": args.db,
        "threads": args.threads,
        "transactions": args.transactions,
        "batch_size": args.batch_size,
        "mem_limit": args.mem_limit,
        "heap_size": args.heap_size_effective,
        "arcadedb_version": (
            getattr(arcadedb_module, "__version__", None)
            if arcadedb_module is not None
            else None
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
        "sqlite_profile": summary.get("sqlite_profile"),
        "sqlite_pragmas": summary.get("sqlite_pragmas"),
        "sqlite_version": sqlite3.sqlite_version,
        "duckdb_runtime_version": summary.get(
            "duckdb_runtime_version",
            (
                getattr(duckdb_module, "__version__", None)
                if duckdb_module is not None
                else None
            ),
        ),
        "docker_image": args.docker_image,
        "seed": args.seed,
        "run_label": args.run_label,
        "throughput_ops_s": summary["throughput_ops_s"],
        "total_time_s": summary["total_time_s"],
        "schema_time_s": summary["schema_time_s"],
        "load_time_s": summary["load_time_s"],
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
        "disk_after_load_bytes": summary["disk_after_load_bytes"],
        "disk_after_load_human": format_bytes_binary(summary["disk_after_load_bytes"]),
        "disk_after_oltp_bytes": summary["disk_after_oltp_bytes"],
        "disk_after_oltp_human": format_bytes_binary(summary["disk_after_oltp_bytes"]),
        "rss_peak_kb": summary["rss_peak_kb"],
        "rss_peak_human": format_bytes_binary(summary["rss_peak_kb"] * 1024),
        "latency_summary": build_latency_summary(summary["latencies"]),
        "op_counts": summary.get("op_counts"),
        "load_stats": summary.get("load_stats"),
        "run_status": summary.get("run_status", "success"),
        "error_type": summary.get("error_type"),
        "error_message": summary.get("error_message"),
        "db_create_time_s": summary.get("db_create_time_s"),
        "db_open_time_s": summary.get("db_open_time_s"),
        "db_close_time_s": summary.get("db_close_time_s"),
        "query_cold_time_s": summary.get("query_cold_time_s"),
        "query_warm_mean_s": summary.get("query_warm_mean_s"),
        "query_result_hash_stable": summary.get("query_result_hash_stable"),
        "query_row_count_stable": summary.get("query_row_count_stable"),
        "benchmark_scope_note": summary.get("benchmark_scope_note"),
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
    if args.db.startswith("arcadedb_"):
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
        f"{python_cmd} -u 09_stackoverflow_graph_oltp.py {' '.join(filtered_args)}"
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
        description="Example 09: Stack Overflow Graph (OLTP)",
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
        "--threads",
        type=int,
        default=4,
        help="Number of worker threads (default: 4)",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=100_000,
        help="Number of OLTP operations (default: 100000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size for XML inserts (default: 10000)",
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
        "--seed",
        type=int,
        default=42,
        help="Random seed (default: 42)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to DB directory and result filename",
    )
    parser.add_argument(
        "--verify-single-thread-series",
        action="store_true",
        help="For threads=1 only, assert final graph counts match deterministic single-thread baseline",
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
        resolve_arcadedb_heap_size(
            args.mem_limit,
            args.jvm_heap_fraction,
        )
        if args.db.startswith("arcadedb_")
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
        f"{args.dataset.replace('-', '_')}_graph_oltp_{args.db}_"
        f"{mem_limit_tag(args.mem_limit)}"
    )
    if args.run_label:
        db_name = f"{db_name}_{args.run_label}"
    db_path = Path("./my_test_databases") / db_name

    print("=" * 80)
    print("Stack Overflow Graph - OLTP")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"DB: {args.db}")
    if args.db in ("sqlite", "graphqlite"):
        print(f"SQLite profile: {args.sqlite_profile}")
    print(f"Threads: {args.threads}")
    print(f"Operations: {args.transactions:,}")
    print(f"Batch size: {args.batch_size}")
    if args.db.startswith("arcadedb_"):
        print(f"JVM heap size: {heap_size}")
        print(f"ArcadeDB OLTP language: {args.db.removeprefix('arcadedb_')}")
    print(f"DB path: {db_path}")
    print(BENCHMARK_SCOPE_NOTE)
    print()

    if args.db.startswith("arcadedb_"):
        summary = run_graph_oltp_arcadedb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
            jvm_kwargs=jvm_kwargs,
            oltp_language=args.db.removeprefix("arcadedb_"),
        )
    elif args.db in ("ladybug", "ladybugdb"):
        summary = run_graph_oltp_ladybug(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
        )
    elif args.db == "graphqlite":
        summary = run_graph_oltp_sqlite_layer(
            db_kind=args.db,
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
            sqlite_profile=args.sqlite_profile,
        )
    elif args.db == "sqlite":
        summary = run_graph_oltp_sqlite(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
            sqlite_profile=args.sqlite_profile,
        )
    elif args.db == "duckdb":
        summary = run_graph_oltp_duckdb(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
        )
    elif args.db == "python_memory":
        summary = run_graph_oltp_python_memory(
            db_path=db_path,
            data_dir=data_dir,
            batch_size=args.batch_size,
            transactions=args.transactions,
            threads=args.threads,
            seed=args.seed,
        )
    else:
        raise NotImplementedError(
            "Only arcadedb, ladybugdb, graphqlite, duckdb, sqlite, and python_memory are supported"
        )

    print("\nResults")
    print("-" * 80)
    print(f"Throughput: {summary['throughput_ops_s']:.1f} ops/s")
    print(f"Total time: {summary['total_time_s']:.2f}s")
    print(f"Schema time: {summary['schema_time_s']:.2f}s")
    print(f"Load time: {summary['load_time_s']:.2f}s")
    if summary.get("load_node_count") is not None:
        print(
            "Load counts: "
            f"nodes={summary['load_node_count']:,}, "
            f"edges={summary['load_edge_count']:,} "
            f"(time={summary.get('load_counts_time_s', 0.0):.2f}s)"
        )
        if summary.get("load_node_counts_by_type"):
            print(
                format_counts_by_type(
                    "Load nodes by type",
                    summary["load_node_counts_by_type"],
                )
            )
        if summary.get("load_edge_counts_by_type"):
            print(
                format_counts_by_type(
                    "Load edges by type",
                    summary["load_edge_counts_by_type"],
                )
            )
    print(f"Count time: {summary['counts_time_s']:.2f}s")
    print(
        "Counts: "
        f"nodes={summary['node_count']:,}, "
        f"edges={summary['edge_count']:,}"
    )
    if summary.get("node_counts_by_type"):
        print(
            format_counts_by_type(
                "Nodes by type",
                summary["node_counts_by_type"],
            )
        )
    if summary.get("edge_counts_by_type"):
        print(
            format_counts_by_type(
                "Edges by type",
                summary["edge_counts_by_type"],
            )
        )
    if summary.get("op_counts"):
        print(format_counts_by_type("Ops by type", summary["op_counts"]))
    if args.verify_single_thread_series:
        baseline_path = verify_single_thread_series(args, summary, args.db, db_path)
        print(f"Deterministic single-thread CRUD verification passed: {baseline_path}")
    print(f"Disk after load: {format_bytes_binary(summary['disk_after_load_bytes'])}")
    print(f"Disk after OLTP: {format_bytes_binary(summary['disk_after_oltp_bytes'])}")
    print(f"Peak RSS: {summary['rss_peak_kb'] / 1024:.1f} MB")
    print(summary.get("benchmark_scope_note") or BENCHMARK_SCOPE_NOTE)
    print()

    summary["benchmark_scope_note"] = (
        summary.get("benchmark_scope_note") or BENCHMARK_SCOPE_NOTE
    )

    write_results(db_path, args, summary)


if __name__ == "__main__":
    main()
