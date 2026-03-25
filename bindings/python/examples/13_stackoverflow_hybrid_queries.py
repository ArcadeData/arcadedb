#!/usr/bin/env python3
"""
Example 13: Stack Overflow Hybrid Queries (Standalone)

Native-only ArcadeDB pipeline with four steps:
1) Phase 1: XML -> document tables (+ indexes)
2) Phase 2: XML -> graph (+ indexes)
3) Phase 3: embeddings + vector indexes on Question/Answer/Comment
4) Phase 4: hybrid queries combining SQL, OpenCypher, and vector search

This script is intentionally compact and does not use Docker.

Heap size testing
- tiny: At least 2G
- small: At least 4G
- medium: At least 8G
- large: At least 16G
- xlarge: At least 64G
- full: At least ???
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
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

EXPECTED_DATASETS = {
    "stackoverflow-tiny",
    "stackoverflow-small",
    "stackoverflow-medium",
    "stackoverflow-large",
    "stackoverflow-xlarge",
    "stackoverflow-full",
}


def get_arcadedb_module():
    try:
        import arcadedb_embedded as arcadedb
    except ImportError:
        return None
    return arcadedb


def get_sentence_transformer_class():
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None
    return SentenceTransformer


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


def parse_string(value: Optional[str]) -> Optional[str]:
    return value


def to_epoch_millis(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def format_bytes_binary(value: int) -> str:
    current = float(value)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if current < 1024:
            return f"{current:.1f}{unit}"
        current /= 1024
    return f"{current:.1f}PiB"


def configure_reproducibility(seed: int) -> None:
    random.seed(seed)

    try:
        import numpy as np

        np.random.seed(seed)
    except ImportError:
        pass

    try:
        import torch

        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    except ImportError:
        pass


def get_dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


def read_current_rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
                    break
    except FileNotFoundError:
        pass
    return 0


def start_rss_sampler(interval_sec: float = 0.2, profile_every_sec: float = 5.0):
    stop_event = threading.Event()
    state = {
        "max_kb": 0,
        "start_ts": time.time(),
        "samples": [],
    }

    next_profile_at = time.time()

    def run():
        nonlocal next_profile_at
        while not stop_event.is_set():
            current_kb = read_current_rss_kb()
            if current_kb > 0:
                state["max_kb"] = max(state["max_kb"], current_kb)

                now = time.time()
                if profile_every_sec > 0 and now >= next_profile_at:
                    state["samples"].append(
                        {
                            "t_s": round(now - state["start_ts"], 3),
                            "rss_kb": current_kb,
                            "rss_human": format_bytes_binary(current_kb * 1024),
                        }
                    )
                    next_profile_at = now + profile_every_sec

            time.sleep(interval_sec)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop_event, state, thread


def record_rss_checkpoint(state: Dict[str, Any], label: str) -> None:
    current_kb = read_current_rss_kb()
    if current_kb <= 0:
        return

    checkpoints = state.setdefault("checkpoints", [])
    checkpoints.append(
        {
            "label": label,
            "t_s": round(time.time() - state.get("start_ts", time.time()), 3),
            "rss_kb": current_kb,
            "rss_human": format_bytes_binary(current_kb * 1024),
        }
    )
    state["max_kb"] = max(state.get("max_kb", 0), current_kb)


def iter_xml_rows(xml_path: Path) -> Iterable[Dict[str, str]]:
    context = etree.iterparse(str(xml_path), events=("end",), tag="row")
    for _, elem in context:
        yield elem.attrib
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def infer_value_type(value: Optional[str]) -> Optional[str]:
    if value is None or value == "":
        return None

    normalized = value.strip()
    if normalized.lower() in ("true", "false"):
        return "BOOLEAN"
    if parse_int(normalized) is not None:
        return "INTEGER"
    if parse_datetime(normalized) is not None:
        return "DATETIME"
    return "STRING"


def resolve_inferred_type(observed_types: set[str], fallback: str) -> str:
    if not observed_types:
        return fallback
    if len(observed_types) == 1:
        return next(iter(observed_types))
    if "STRING" in observed_types:
        return "STRING"
    if "DATETIME" in observed_types and len(observed_types) > 1:
        return "STRING"
    return "STRING"


def parser_for_field_type(field_type: str) -> Callable[[Optional[str]], Any]:
    parser_map: Dict[str, Callable[[Optional[str]], Any]] = {
        "INTEGER": parse_int,
        "BOOLEAN": parse_bool,
        "DATETIME": parse_datetime,
        "STRING": parse_string,
    }
    return parser_map.get(field_type, parse_string)


FORCED_STRING_FIELD_NAMES = {
    "displayname",
    "lasteditordisplayname",
    "ownerdisplayname",
    "userdisplayname",
    "title",
    "body",
    "text",
    "tags",
    "tagname",
    "name",
    "location",
    "aboutme",
    "websiteurl",
    "comment",
    "revisionguid",
    "contentlicense",
}


def is_forced_string_field(field_name: str) -> bool:
    normalized = field_name.strip().lower()
    return normalized in FORCED_STRING_FIELD_NAMES or normalized.endswith("name")


def infer_table_defs_from_xml(
    data_dir: Path,
    sample_limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    effective_limit = max(1, int(sample_limit))
    inferred_defs: List[Dict[str, Any]] = []
    report: Dict[str, Any] = {
        "enabled": True,
        "sample_limit_per_table": effective_limit,
        "tables": {},
    }

    for table in TABLE_SPECS:
        observed_types: Dict[str, set[str]] = {}
        non_null_counts: Dict[str, int] = {}
        field_order: Dict[str, int] = {}
        sampled_rows = 0

        for attrs in iter_xml_rows(data_dir / table["xml"]):
            sampled_rows += 1
            for field_name, raw_value in attrs.items():
                if field_name not in field_order:
                    field_order[field_name] = len(field_order)
                    observed_types[field_name] = set()
                    non_null_counts[field_name] = 0

                detected = infer_value_type(raw_value)
                if detected is None:
                    continue
                observed_types[field_name].add(detected)
                non_null_counts[field_name] += 1
            if sampled_rows >= effective_limit:
                break

        new_fields: List[FieldDef] = []
        field_report: Dict[str, Any] = {}

        ordered_fields = [
            name for name, _idx in sorted(field_order.items(), key=lambda item: item[1])
        ]

        for field_name in ordered_fields:
            forced_string = is_forced_string_field(field_name)
            inferred_type = (
                "STRING"
                if forced_string
                else resolve_inferred_type(
                    observed_types.get(field_name, set()),
                    "STRING",
                )
            )
            new_fields.append(
                (
                    field_name,
                    inferred_type,
                    parser_for_field_type(inferred_type),
                )
            )

            field_report[field_name] = {
                "inferred_type": inferred_type,
                "non_null_sampled": non_null_counts.get(field_name, 0),
                "observed_types": (
                    ["STRING"]
                    if forced_string
                    else sorted(observed_types.get(field_name, set()))
                ),
                "forced_string": forced_string,
            }

        inferred_defs.append(
            {
                "name": table["name"],
                "xml": table["xml"],
                "fields": new_fields,
            }
        )

        report["tables"][table["name"]] = {
            "sampled_rows": sampled_rows,
            "field_count": len(ordered_fields),
            "fields": field_report,
        }

    return inferred_defs, report


FieldDef = Tuple[str, str, Callable[[Optional[str]], Any]]

TABLE_SPECS: List[Dict[str, str]] = [
    {"name": "User", "xml": "Users.xml"},
    {"name": "Post", "xml": "Posts.xml"},
    {"name": "Comment", "xml": "Comments.xml"},
    {"name": "Badge", "xml": "Badges.xml"},
    {"name": "Vote", "xml": "Votes.xml"},
    {"name": "PostLink", "xml": "PostLinks.xml"},
    {"name": "Tag", "xml": "Tags.xml"},
    {"name": "PostHistory", "xml": "PostHistory.xml"},
]

TABLE_INDEX_DEFS: List[Tuple[str, List[str], bool]] = [
    ("User", ["Id"], True),
    ("User", ["Reputation"], False),
    ("Post", ["Id"], True),
    ("Post", ["PostTypeId"], False),
    ("Post", ["OwnerUserId"], False),
    ("Comment", ["Id"], True),
    ("Comment", ["PostId"], False),
    ("Badge", ["Id"], True),
    ("Badge", ["UserId"], False),
    ("Tag", ["Id"], True),
    ("Tag", ["TagName"], False),
]

HASH_INDEX_FIELDS = {"Id"}

GRAPH_VERTEX_TYPES = ["User", "Question", "Answer", "Tag", "Badge", "Comment"]
GRAPH_EDGE_TYPES = [
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


def ensure_dataset(data_dir: Path) -> None:
    required = [table["xml"] for table in TABLE_SPECS]
    missing = [name for name in required if not (data_dir / name).exists()]
    if missing:
        raise FileNotFoundError(
            "Missing dataset XML files: " + ", ".join(missing) + f" (under {data_dir})"
        )


def create_indexes_with_retry(
    db,
    index_defs: List[Tuple[str, List[str], bool]],
    retry_delay: int,
    max_retries: int,
) -> float:
    start = time.time()
    for table, props, unique in index_defs:
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
                    and attempt < max_retries
                )
                if retryable:
                    time.sleep(retry_delay)
                    continue
                raise RuntimeError(
                    f"Failed creating index {table}[{','.join(props)}]: {error_msg}"
                )
        if not created:
            raise RuntimeError(f"Failed creating index {table}[{','.join(props)}]")

    db.async_executor().wait_completion()
    return time.time() - start


def create_table_schema(db, table_defs: List[Dict[str, Any]]) -> None:
    for table in table_defs:
        db.command("sql", f"CREATE DOCUMENT TYPE {table['name']}")
        for field_name, field_type, _ in table["fields"]:
            db.command(
                "sql", f"CREATE PROPERTY {table['name']}.{field_name} {field_type}"
            )


def to_arcadedb_sql_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return serialize_datetime(value)
    return value


def configure_arcadedb_async_loader(db, batch_size: int, parallelism: int = 1):
    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, parallelism))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)
    return async_exec


def reset_arcadedb_async_loader(db, async_exec):
    async_exec.wait_completion()
    async_exec.close()
    db.set_read_your_writes(True)
    async_exec.set_transaction_use_wal(True)


def load_table_arcadedb_async(
    async_exec,
    errors: List[Exception],
    xml_path: Path,
    table_def: Dict[str, Any],
) -> Dict[str, Any]:
    fields: List[FieldDef] = table_def["fields"]
    total = 0
    start = time.time()
    columns = [field_name for field_name, _, _ in fields]
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

    return {
        "table": table_def["name"],
        "rows": total,
        "elapsed_s": time.time() - start,
    }


def insert_table_batch(db, table_name: str, batch: List[Dict[str, Any]]) -> None:
    from arcadedb_embedded.type_conversion import convert_python_to_java

    with db.transaction():
        for record in batch:
            columns = [key for key, value in record.items() if value is not None]
            if not columns:
                continue
            assignments = ", ".join(f"{col} = ?" for col in columns)
            values = [convert_python_to_java(record[col]) for col in columns]
            db.command("sql", f"INSERT INTO {table_name} SET {assignments}", *values)


def load_table(
    db, data_dir: Path, table_def: Dict[str, Any], batch_size: int
) -> Dict[str, Any]:
    xml_path = data_dir / table_def["xml"]
    fields: List[FieldDef] = table_def["fields"]
    total = 0
    batch: List[Dict[str, Any]] = []
    start = time.time()

    for attrs in iter_xml_rows(xml_path):
        row: Dict[str, Any] = {}
        for field_name, _, parser in fields:
            row[field_name] = parser(attrs.get(field_name))
        batch.append(row)
        if len(batch) >= batch_size:
            insert_table_batch(db, table_def["name"], batch)
            total += len(batch)
            batch = []

    if batch:
        insert_table_batch(db, table_def["name"], batch)
        total += len(batch)

    return {
        "table": table_def["name"],
        "rows": total,
        "elapsed_s": time.time() - start,
    }


def count_table_rows(db, table_defs: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in table_defs:
        rows = db.query(
            "sql", f"SELECT count(*) AS count FROM {table['name']}"
        ).to_list()
        counts[table["name"]] = int(rows[0].get("count", 0)) if rows else 0
    return counts


def parse_tags(tags_str: Optional[str]) -> List[str]:
    if not tags_str:
        return []
    if "|" in tags_str:
        return [part for part in tags_str.split("|") if part]
    return [
        t
        for t in tags_str.replace("><", "|")
        .replace("<", "")
        .replace(">", "")
        .split("|")
        if t
    ]


def create_graph_schema(db) -> None:
    for vertex_type in GRAPH_VERTEX_TYPES:
        db.command("sql", f"CREATE VERTEX TYPE {vertex_type}")
        db.command("sql", f"CREATE PROPERTY {vertex_type}.Id LONG")

    for edge_type in GRAPH_EDGE_TYPES:
        db.command("sql", f"CREATE EDGE TYPE {edge_type} UNIDIRECTIONAL")


def create_graph_indexes(db) -> None:
    for vertex_type in GRAPH_VERTEX_TYPES:
        db.command("sql", f"CREATE INDEX ON {vertex_type} (Id) UNIQUE_HASH")

    db.async_executor().wait_completion()


def insert_vertices(db, vertex_type: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with db.graph_batch(
        batch_size=max(1, len(rows)),
        expected_edge_count=0,
        bidirectional=False,
        commit_every=max(1, len(rows)),
        use_wal=False,
    ) as batch:
        batch.create_vertices(vertex_type, rows)


def build_rid_lookup(db, vertex_type: str) -> Dict[int, str]:
    rows = db.query(
        "sql",
        f"SELECT Id AS id, @rid AS rid FROM {vertex_type} WHERE Id IS NOT NULL",
    ).to_list()
    lookup: Dict[int, str] = {}
    for row in rows:
        entity_id = row.get("id")
        rid = row.get("rid")
        if entity_id is None or rid is None:
            continue
        lookup[int(entity_id)] = str(rid)
    return lookup


def insert_edges(
    db,
    edge_type: str,
    rows: List[Dict[str, Any]],
    rid_lookups: Dict[str, Dict[int, str]],
) -> None:
    if not rows:
        return

    with db.graph_batch(
        batch_size=max(1, len(rows)),
        expected_edge_count=max(1, len(rows)),
        bidirectional=False,
        commit_every=max(1, len(rows)),
        use_wal=False,
    ) as batch:
        for row in rows:
            from_type = row["from_type"]
            to_type = row["to_type"]
            from_id = row["from_id"]
            to_id = row["to_id"]

            from_rid = rid_lookups.get(from_type, {}).get(int(from_id))
            to_rid = rid_lookups.get(to_type, {}).get(int(to_id))
            if from_rid is None or to_rid is None:
                continue

            props = {
                key: value
                for key, value in row.items()
                if key not in ("from_type", "from_id", "to_type", "to_id")
                and value is not None
            }
            batch.new_edge(from_rid, edge_type, to_rid, **props)


def load_graph(db, data_dir: Path, batch_size: int) -> Dict[str, Any]:
    stats: Dict[str, Dict[str, float]] = {"nodes": {}, "edges": {}}
    tag_map: Dict[str, int] = {}
    question_ids: set[int] = set()
    answer_ids: set[int] = set()

    start = time.time()
    batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Tags.xml"):
        tag_id = parse_int(attrs.get("Id"))
        tag_name = attrs.get("TagName")
        if tag_id is None or not tag_name:
            continue
        tag_map[tag_name] = tag_id
        batch.append(
            {"Id": tag_id, "TagName": tag_name, "Count": parse_int(attrs.get("Count"))}
        )
        if len(batch) >= batch_size:
            insert_vertices(db, "Tag", batch)
            batch = []
    if batch:
        insert_vertices(db, "Tag", batch)
    stats["nodes"]["Tag"] = time.time() - start

    start = time.time()
    batch = []
    for attrs in iter_xml_rows(data_dir / "Users.xml"):
        user_id = parse_int(attrs.get("Id"))
        if user_id is None:
            continue
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
            insert_vertices(db, "User", batch)
            batch = []
    if batch:
        insert_vertices(db, "User", batch)
    stats["nodes"]["User"] = time.time() - start

    start = time.time()
    q_batch: List[Dict[str, Any]] = []
    a_batch: List[Dict[str, Any]] = []
    for attrs in iter_xml_rows(data_dir / "Posts.xml"):
        post_id = parse_int(attrs.get("Id"))
        post_type = parse_int(attrs.get("PostTypeId"))
        if post_id is None or post_type is None:
            continue

        if post_type == 1:
            question_ids.add(post_id)
            q_batch.append(
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
            a_batch.append(
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

        if len(q_batch) >= batch_size:
            insert_vertices(db, "Question", q_batch)
            q_batch = []
        if len(a_batch) >= batch_size:
            insert_vertices(db, "Answer", a_batch)
            a_batch = []

    if q_batch:
        insert_vertices(db, "Question", q_batch)
    if a_batch:
        insert_vertices(db, "Answer", a_batch)
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
            insert_vertices(db, "Badge", batch)
            batch = []
    if batch:
        insert_vertices(db, "Badge", batch)
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
            insert_vertices(db, "Comment", batch)
            batch = []
    if batch:
        insert_vertices(db, "Comment", batch)
    stats["nodes"]["Comment"] = time.time() - start

    rid_lookups = {
        "User": build_rid_lookup(db, "User"),
        "Question": build_rid_lookup(db, "Question"),
        "Answer": build_rid_lookup(db, "Answer"),
        "Tag": build_rid_lookup(db, "Tag"),
        "Badge": build_rid_lookup(db, "Badge"),
        "Comment": build_rid_lookup(db, "Comment"),
    }

    stats["edges"]["ASKED"] = create_edge_asked(db, data_dir, batch_size, rid_lookups)
    stats["edges"]["ANSWERED"] = create_edge_answered(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["HAS_ANSWER"] = create_edge_has_answer(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["ACCEPTED_ANSWER"] = create_edge_accepted_answer(
        db, data_dir, batch_size, rid_lookups
    )
    stats["edges"]["TAGGED_WITH"] = create_edge_tagged_with(
        db, data_dir, tag_map, batch_size, rid_lookups
    )
    comments_stats = create_edge_commented_on(
        db, data_dir, question_ids, answer_ids, batch_size, rid_lookups
    )
    stats["edges"].update(comments_stats)
    stats["edges"]["EARNED"] = create_edge_earned(db, data_dir, batch_size, rid_lookups)
    stats["edges"]["LINKED_TO"] = create_edge_linked_to(
        db, data_dir, question_ids, answer_ids, batch_size, rid_lookups
    )

    return stats


def create_edge_asked(
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
        batch.append(
            {
                "from_type": "User",
                "from_id": user_id,
                "to_type": "Question",
                "to_id": post_id,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            insert_edges(db, "ASKED", batch, rid_lookups)
            batch = []
    if batch:
        insert_edges(db, "ASKED", batch, rid_lookups)
    return time.time() - start


def create_edge_answered(
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
        answer_id = parse_int(attrs.get("Id"))
        if user_id is None or answer_id is None:
            continue
        batch.append(
            {
                "from_type": "User",
                "from_id": user_id,
                "to_type": "Answer",
                "to_id": answer_id,
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )
        if len(batch) >= batch_size:
            insert_edges(db, "ANSWERED", batch, rid_lookups)
            batch = []
    if batch:
        insert_edges(db, "ANSWERED", batch, rid_lookups)
    return time.time() - start


def create_edge_has_answer(
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
        batch.append(
            {
                "from_type": "Question",
                "from_id": parent_id,
                "to_type": "Answer",
                "to_id": answer_id,
            }
        )
        if len(batch) >= batch_size:
            insert_edges(db, "HAS_ANSWER", batch, rid_lookups)
            batch = []
    if batch:
        insert_edges(db, "HAS_ANSWER", batch, rid_lookups)
    return time.time() - start


def create_edge_accepted_answer(
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
        batch.append(
            {
                "from_type": "Question",
                "from_id": question_id,
                "to_type": "Answer",
                "to_id": answer_id,
            }
        )
        if len(batch) >= batch_size:
            insert_edges(db, "ACCEPTED_ANSWER", batch, rid_lookups)
            batch = []
    if batch:
        insert_edges(db, "ACCEPTED_ANSWER", batch, rid_lookups)
    return time.time() - start


def create_edge_tagged_with(
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
        for tag in parse_tags(attrs.get("Tags")):
            tag_id = tag_map.get(tag)
            if tag_id is None:
                continue
            batch.append(
                {
                    "from_type": "Question",
                    "from_id": question_id,
                    "to_type": "Tag",
                    "to_id": tag_id,
                }
            )
            if len(batch) >= batch_size:
                insert_edges(db, "TAGGED_WITH", batch, rid_lookups)
                batch = []
    if batch:
        insert_edges(db, "TAGGED_WITH", batch, rid_lookups)
    return time.time() - start


def create_edge_commented_on(
    db,
    data_dir: Path,
    question_ids: set[int],
    answer_ids: set[int],
    batch_size: int,
    rid_lookups: Dict[str, Dict[int, str]],
) -> Dict[str, float]:
    start = time.time()
    question_batch: List[Dict[str, Any]] = []
    answer_batch: List[Dict[str, Any]] = []

    for attrs in iter_xml_rows(data_dir / "Comments.xml"):
        comment_id = parse_int(attrs.get("Id"))
        post_id = parse_int(attrs.get("PostId"))
        if comment_id is None or post_id is None:
            continue

        target_type = None
        target_edge = None
        if post_id in question_ids:
            target_type = "Question"
            target_edge = "COMMENTED_ON"
        elif post_id in answer_ids:
            target_type = "Answer"
            target_edge = "COMMENTED_ON_ANSWER"

        if not target_type:
            continue

        payload = {
            "from_type": "Comment",
            "from_id": comment_id,
            "to_type": target_type,
            "to_id": post_id,
            "CreationDate": to_epoch_millis(parse_datetime(attrs.get("CreationDate"))),
            "Score": parse_int(attrs.get("Score")),
        }

        if target_edge == "COMMENTED_ON":
            question_batch.append(payload)
        else:
            answer_batch.append(payload)

        if len(question_batch) >= batch_size:
            insert_edges(db, "COMMENTED_ON", question_batch, rid_lookups)
            question_batch = []
        if len(answer_batch) >= batch_size:
            insert_edges(db, "COMMENTED_ON_ANSWER", answer_batch, rid_lookups)
            answer_batch = []

    if question_batch:
        insert_edges(db, "COMMENTED_ON", question_batch, rid_lookups)
    if answer_batch:
        insert_edges(db, "COMMENTED_ON_ANSWER", answer_batch, rid_lookups)

    elapsed = time.time() - start
    return {"COMMENTED_ON": elapsed, "COMMENTED_ON_ANSWER": elapsed}


def create_edge_earned(
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
        batch.append(
            {
                "from_type": "User",
                "from_id": user_id,
                "to_type": "Badge",
                "to_id": badge_id,
                "Date": to_epoch_millis(parse_datetime(attrs.get("Date"))),
                "Class": parse_int(attrs.get("Class")),
            }
        )
        if len(batch) >= batch_size:
            insert_edges(db, "EARNED", batch, rid_lookups)
            batch = []
    if batch:
        insert_edges(db, "EARNED", batch, rid_lookups)
    return time.time() - start


def create_edge_linked_to(
    db,
    data_dir: Path,
    question_ids: set[int],
    answer_ids: set[int],
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

        from_type = None
        to_type = None

        if post_id in question_ids:
            from_type = "Question"
        elif post_id in answer_ids:
            from_type = "Answer"

        if related_id in question_ids:
            to_type = "Question"
        elif related_id in answer_ids:
            to_type = "Answer"

        if not from_type or not to_type:
            continue

        batch.append(
            {
                "from_type": from_type,
                "from_id": post_id,
                "to_type": to_type,
                "to_id": related_id,
                "LinkTypeId": parse_int(attrs.get("LinkTypeId")),
                "CreationDate": to_epoch_millis(
                    parse_datetime(attrs.get("CreationDate"))
                ),
            }
        )

        if len(batch) >= batch_size:
            insert_edges(db, "LINKED_TO", batch, rid_lookups)
            batch = []

    if batch:
        insert_edges(db, "LINKED_TO", batch, rid_lookups)

    return time.time() - start


def count_graph(db) -> Dict[str, Any]:
    node_rows = db.query("opencypher", "MATCH (n) RETURN count(n) AS count").to_list()
    edge_rows = db.query(
        "opencypher", "MATCH ()-[r]->() RETURN count(r) AS count"
    ).to_list()
    node_total = int(node_rows[0].get("count", 0)) if node_rows else 0
    edge_total = int(edge_rows[0].get("count", 0)) if edge_rows else 0

    node_by_type: Dict[str, int] = {}
    edge_by_type: Dict[str, int] = {}
    for label in GRAPH_VERTEX_TYPES:
        rows = db.query(
            "opencypher", f"MATCH (n:{label}) RETURN count(n) AS count"
        ).to_list()
        node_by_type[label] = int(rows[0].get("count", 0)) if rows else 0
    for label in GRAPH_EDGE_TYPES:
        rows = db.query(
            "opencypher", f"MATCH ()-[r:{label}]->() RETURN count(r) AS count"
        ).to_list()
        edge_by_type[label] = int(rows[0].get("count", 0)) if rows else 0

    return {
        "node_total": node_total,
        "edge_total": edge_total,
        "node_counts_by_type": node_by_type,
        "edge_counts_by_type": edge_by_type,
    }


def ensure_embedding_properties(db) -> None:
    for vertex_type in ("Question", "Answer", "Comment"):
        for prop_name, prop_type in (
            ("embedding", "ARRAY_OF_FLOATS"),
            ("vector_id", "STRING"),
        ):
            try:
                db.command(
                    "sql", f"CREATE PROPERTY {vertex_type}.{prop_name} {prop_type}"
                )
            except Exception:
                pass


def embed_vertex_type(
    arcadedb,
    db,
    model,
    vertex_type: str,
    text_builder: Callable[[Any], str],
    select_fields: str,
    batch_size: int,
    encode_batch_size: int,
) -> Dict[str, Any]:
    total_rows = db.query(
        "sql", f"SELECT count(*) AS count FROM {vertex_type}"
    ).to_list()
    total = int(total_rows[0].get("count", 0)) if total_rows else 0
    if total == 0:
        return {"vertex_type": vertex_type, "processed": 0, "elapsed_s": 0.0}

    start = time.time()
    processed = 0
    last_rid = "#-1:-1"

    while True:
        query = (
            f"SELECT Id, {select_fields}, @rid as rid FROM {vertex_type} "
            f"WHERE @rid > {last_rid} LIMIT {batch_size}"
        )
        batch = list(db.query("sql", query))
        if not batch:
            break

        ids: List[int] = []
        texts: List[str] = []
        for row in batch:
            entity_id = row.get("Id")
            if entity_id is None:
                continue
            text = text_builder(row).strip()
            ids.append(int(entity_id))
            texts.append(text if text else " ")

        embeddings = model.encode(
            texts,
            batch_size=encode_batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

        with db.transaction():
            for entity_id, emb in zip(ids, embeddings):
                db.command(
                    "sql",
                    f"UPDATE {vertex_type} SET embedding = ?, vector_id = ? WHERE Id = ?",
                    [arcadedb.to_java_float_array(emb), str(entity_id), entity_id],
                )

        processed += len(ids)
        last_rid = batch[-1].get("rid")

    return {
        "vertex_type": vertex_type,
        "processed": processed,
        "elapsed_s": time.time() - start,
    }


def create_vector_index(db, vertex_type: str) -> Tuple[Any, float]:
    start = time.time()
    index = db.create_vector_index(
        vertex_type=vertex_type,
        vector_property="embedding",
        dimensions=384,
        distance_function="cosine",
        max_connections=16,
        beam_width=100,
        quantization="INT8",
        store_vectors_in_graph=False,
        add_hierarchy=True,
    )
    return index, time.time() - start


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 7)
    if isinstance(value, list):
        return [normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in sorted(value.items())}
    return value


def hash_rows(rows: List[Dict[str, Any]]) -> str:
    payload = json.dumps(
        [normalize_value(row) for row in rows], sort_keys=True, ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_sql(db, query: str) -> List[Dict[str, Any]]:
    return list(db.query("sql", query))


def run_cypher(db, query: str) -> List[Dict[str, Any]]:
    return list(db.query("opencypher", query))


def quote_cypher_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def timed_step(name: str, fn: Callable[[], Any]) -> Dict[str, Any]:
    start = time.perf_counter()
    result = fn()
    return {"name": name, "elapsed_s": time.perf_counter() - start, "result": result}


def run_hybrid_queries(
    db,
    model,
    question_index,
    top_k: int,
    candidate_limit: int,
    min_reputation: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    # Q1: SQL -> Vector
    steps = []
    step = timed_step(
        "sql_candidates",
        lambda: run_sql(
            db,
            f"""
            SELECT Id, Title, Score, @rid as rid
            FROM Question
            WHERE Title IS NOT NULL AND Score IS NOT NULL
            ORDER BY Score DESC, Id ASC
            LIMIT {candidate_limit}
            """,
        ),
    )
    steps.append(step)
    candidate_ids = {
        int(row.get("Id")) for row in step["result"] if row.get("Id") is not None
    }
    allowed_rids = [
        str(row.get("rid")) for row in step["result"] if row.get("rid") is not None
    ]
    step2 = timed_step(
        "vector_search",
        lambda: question_index.find_nearest(
            model.encode(
                ["How do I parse JSON in Python?"],
                show_progress_bar=False,
                convert_to_numpy=True,
            )[0],
            k=top_k,
            ef_search=max(top_k, 100),
            allowed_rids=allowed_rids,
        ),
    )
    steps.append(step2)
    rows = []
    for vertex, distance in step2["result"]:
        qid = vertex.get("Id")
        if qid is None or int(qid) not in candidate_ids:
            continue
        rows.append(
            {
                "question_id": int(qid),
                "title": vertex.get("Title"),
                "score": vertex.get("Score"),
                "distance": float(distance),
            }
        )
        if len(rows) >= top_k:
            break
    results.append(
        {
            "name": "q1_sql_to_vector",
            "description": "SQL score-based candidate filter + vector rerank",
            "elapsed_s": sum(s["elapsed_s"] for s in steps),
            "steps": [{"name": s["name"], "elapsed_s": s["elapsed_s"]} for s in steps],
            "row_count": len(rows),
            "result_hash": hash_rows(rows),
            "rows": rows,
        }
    )

    # Q2: SQL -> Cypher
    steps = []
    top_tags_step = timed_step(
        "sql_top_tags",
        lambda: run_sql(
            db,
            """
            SELECT TagName, Count
            FROM Tag
            WHERE TagName IS NOT NULL
            ORDER BY Count DESC, TagName ASC
            LIMIT 5
            """,
        ),
    )
    steps.append(top_tags_step)
    tags = [
        str(row.get("TagName")) for row in top_tags_step["result"] if row.get("TagName")
    ]
    if tags:
        cypher_tags = "[" + ", ".join(quote_cypher_string(tag) for tag in tags) + "]"
    else:
        cypher_tags = "[]"
    step = timed_step(
        "cypher_expand",
        lambda: run_cypher(
            db,
            f"""
            MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
            MATCH (u:User)-[:ASKED]->(q)
            WHERE t.TagName IN {cypher_tags}
            RETURN t.TagName AS tag, u.Id AS user_id, u.DisplayName AS name, count(q) AS questions
            ORDER BY questions DESC, user_id ASC
            LIMIT {top_k}
            """,
        ),
    )
    steps.append(step)
    rows = [
        {
            "tag": row.get("tag"),
            "user_id": row.get("user_id"),
            "name": row.get("name"),
            "questions": row.get("questions"),
        }
        for row in step["result"]
    ]
    results.append(
        {
            "name": "q2_sql_to_cypher",
            "description": "SQL top tags + Cypher expert expansion",
            "elapsed_s": sum(s["elapsed_s"] for s in steps),
            "steps": [{"name": s["name"], "elapsed_s": s["elapsed_s"]} for s in steps],
            "row_count": len(rows),
            "result_hash": hash_rows(rows),
            "rows": rows,
        }
    )

    # Q3: Cypher -> SQL
    steps = []
    step = timed_step(
        "cypher_seed_users",
        lambda: run_cypher(
            db,
            """
            MATCH (u:User)-[:ANSWERED]->(a:Answer)<-[:ACCEPTED_ANSWER]-(:Question)
            RETURN u.Id AS user_id, count(a) AS accepted
            ORDER BY accepted DESC, user_id ASC
            LIMIT 25
            """,
        ),
    )
    steps.append(step)
    user_ids = [
        int(row.get("user_id"))
        for row in step["result"]
        if row.get("user_id") is not None
    ]
    if user_ids:
        ids_sql = ",".join(str(v) for v in user_ids)
        step2 = timed_step(
            "sql_profile_rank",
            lambda: run_sql(
                db,
                f"""
                SELECT Id, DisplayName, Reputation
                FROM User
                WHERE Id IN [{ids_sql}]
                ORDER BY Reputation DESC, Id ASC
                LIMIT {top_k}
                """,
            ),
        )
        steps.append(step2)
        rows = [
            {
                "user_id": row.get("Id"),
                "name": row.get("DisplayName"),
                "reputation": row.get("Reputation"),
            }
            for row in step2["result"]
        ]
    else:
        rows = []
    results.append(
        {
            "name": "q3_cypher_to_sql",
            "description": "Cypher accepted-answer experts + SQL profile ranking",
            "elapsed_s": sum(s["elapsed_s"] for s in steps),
            "steps": [{"name": s["name"], "elapsed_s": s["elapsed_s"]} for s in steps],
            "row_count": len(rows),
            "result_hash": hash_rows(rows),
            "rows": rows,
        }
    )

    # Q4: Vector -> Cypher
    steps = []
    step = timed_step(
        "vector_seed",
        lambda: question_index.find_nearest(
            model.encode(
                ["database indexing best practices for performance"],
                show_progress_bar=False,
                convert_to_numpy=True,
            )[0],
            k=top_k,
            ef_search=max(top_k, 100),
        ),
    )
    steps.append(step)
    seed_ids = [
        int(vertex.get("Id"))
        for vertex, _distance in step["result"]
        if vertex.get("Id") is not None
    ]
    if seed_ids:
        cypher_ids = "[" + ", ".join(str(v) for v in seed_ids) + "]"
        step2 = timed_step(
            "cypher_expand",
            lambda: run_cypher(
                db,
                f"""
                MATCH (u:User)-[:ASKED]->(q:Question)
                OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
                WHERE q.Id IN {cypher_ids}
                RETURN q.Id AS question_id, q.Title AS title, u.Id AS asker_id, count(a) AS answer_count
                ORDER BY answer_count DESC, question_id ASC
                LIMIT {top_k}
                """,
            ),
        )
        steps.append(step2)
        rows = [
            {
                "question_id": row.get("question_id"),
                "title": row.get("title"),
                "asker_id": row.get("asker_id"),
                "answer_count": row.get("answer_count"),
            }
            for row in step2["result"]
        ]
    else:
        rows = []
    results.append(
        {
            "name": "q4_vector_to_cypher",
            "description": "Vector seed + Cypher neighborhood expansion",
            "elapsed_s": sum(s["elapsed_s"] for s in steps),
            "steps": [{"name": s["name"], "elapsed_s": s["elapsed_s"]} for s in steps],
            "row_count": len(rows),
            "result_hash": hash_rows(rows),
            "rows": rows,
        }
    )

    # Q5: Vector -> Cypher -> SQL
    steps = []
    step = timed_step(
        "vector_seed",
        lambda: question_index.find_nearest(
            model.encode(
                ["concurrency control and transaction isolation"],
                show_progress_bar=False,
                convert_to_numpy=True,
            )[0],
            k=top_k,
            ef_search=max(top_k, 100),
        ),
    )
    steps.append(step)
    qids = [
        int(vertex.get("Id"))
        for vertex, _distance in step["result"]
        if vertex.get("Id") is not None
    ]
    if qids:
        cypher_ids = "[" + ", ".join(str(v) for v in qids) + "]"
        step2 = timed_step(
            "cypher_expand_users",
            lambda: run_cypher(
                db,
                f"""
                MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(u:User)
                WHERE q.Id IN {cypher_ids}
                RETURN DISTINCT u.Id AS user_id
                LIMIT 200
                """,
            ),
        )
        steps.append(step2)
        user_ids = [
            int(row.get("user_id"))
            for row in step2["result"]
            if row.get("user_id") is not None
        ]
        if user_ids:
            ids_sql = ",".join(str(v) for v in user_ids)
            step3 = timed_step(
                "sql_post_filter",
                lambda: run_sql(
                    db,
                    f"""
                    SELECT Id, DisplayName, Reputation, Views
                    FROM User
                    WHERE Id IN [{ids_sql}] AND Reputation IS NOT NULL AND Reputation >= {int(min_reputation)}
                    ORDER BY Reputation DESC, Views DESC, Id ASC
                    LIMIT {top_k}
                    """,
                ),
            )
            steps.append(step3)
            rows = [
                {
                    "user_id": row.get("Id"),
                    "name": row.get("DisplayName"),
                    "reputation": row.get("Reputation"),
                    "views": row.get("Views"),
                }
                for row in step3["result"]
            ]
        else:
            rows = []
    else:
        rows = []
    results.append(
        {
            "name": "q5_vector_to_cypher_to_sql",
            "description": "Vector seed + Cypher expansion + SQL post-filter",
            "elapsed_s": sum(s["elapsed_s"] for s in steps),
            "steps": [{"name": s["name"], "elapsed_s": s["elapsed_s"]} for s in steps],
            "row_count": len(rows),
            "result_hash": hash_rows(rows),
            "rows": rows,
        }
    )

    return results


def phase1_tables(
    arcadedb,
    data_dir: Path,
    db_path: Path,
    batch_size: int,
    jvm_kwargs: Dict[str, str],
    infer_sample_limit: int,
) -> Dict[str, Any]:
    if db_path.exists():
        shutil.rmtree(db_path)

    start = time.time()
    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)
    try:
        infer_start = time.time()
        table_defs, inference_report = infer_table_defs_from_xml(
            data_dir,
            sample_limit=infer_sample_limit,
        )
        inference_report["elapsed_s"] = time.time() - infer_start

        schema_start = time.time()
        create_table_schema(db, table_defs)
        schema_time = time.time() - schema_start

        load_start = time.time()
        async_exec = configure_arcadedb_async_loader(db, batch_size, parallelism=1)
        errors: List[Exception] = []

        def on_error(exc: Exception):
            errors.append(exc)

        async_exec.on_error(on_error)

        try:
            table_stats = []
            for table in table_defs:
                xml_path = data_dir / table["xml"]
                table_stats.append(
                    load_table_arcadedb_async(
                        async_exec,
                        errors,
                        xml_path,
                        table,
                    )
                )
            load_time = time.time() - load_start
        finally:
            reset_arcadedb_async_loader(db, async_exec)

        index_time = create_indexes_with_retry(
            db,
            TABLE_INDEX_DEFS,
            retry_delay=10,
            max_retries=60,
        )

        counts = count_table_rows(db, table_defs)
    finally:
        db.close()

    return {
        "db_path": str(db_path),
        "elapsed_s": time.time() - start,
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "index_time_s": index_time,
        "table_load_stats": table_stats,
        "table_counts": counts,
        "type_inference": inference_report,
        "disk_bytes": get_dir_size_bytes(db_path),
    }


def phase2_graph(
    arcadedb, data_dir: Path, db_path: Path, batch_size: int, jvm_kwargs: Dict[str, str]
) -> Dict[str, Any]:
    if db_path.exists():
        shutil.rmtree(db_path)

    start = time.time()
    db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)
    try:
        schema_start = time.time()
        create_graph_schema(db)
        schema_time = time.time() - schema_start

        load_start = time.time()
        load_stats = load_graph(db, data_dir, batch_size)
        load_time = time.time() - load_start

        index_start = time.time()
        create_graph_indexes(db)
        index_time = time.time() - index_start

        graph_counts = count_graph(db)
    finally:
        db.close()

    return {
        "db_path": str(db_path),
        "elapsed_s": time.time() - start,
        "schema_time_s": schema_time,
        "load_time_s": load_time,
        "index_time_s": index_time,
        "load_stats": load_stats,
        "graph_counts": graph_counts,
        "disk_bytes": get_dir_size_bytes(db_path),
    }


def phase3_and_phase4(
    arcadedb,
    SentenceTransformer,
    graph_db_path: Path,
    model_name: str,
    batch_size: int,
    encode_batch_size: int,
    top_k: int,
    candidate_limit: int,
    min_reputation: int,
    jvm_kwargs: Dict[str, str],
) -> Dict[str, Any]:
    start = time.time()

    model_start = time.time()
    model = SentenceTransformer(model_name)
    model_load_time = time.time() - model_start

    db = arcadedb.open_database(str(graph_db_path), jvm_kwargs=jvm_kwargs)
    try:
        ensure_embedding_properties(db)

        embed_stats = []
        embed_stats.append(
            embed_vertex_type(
                arcadedb,
                db,
                model,
                "Question",
                text_builder=lambda row: f"{row.get('Title') or ''} {row.get('Body') or ''}",
                select_fields="Title, Body",
                batch_size=batch_size,
                encode_batch_size=encode_batch_size,
            )
        )
        embed_stats.append(
            embed_vertex_type(
                arcadedb,
                db,
                model,
                "Answer",
                text_builder=lambda row: str(row.get("Body") or ""),
                select_fields="Body",
                batch_size=batch_size,
                encode_batch_size=encode_batch_size,
            )
        )
        embed_stats.append(
            embed_vertex_type(
                arcadedb,
                db,
                model,
                "Comment",
                text_builder=lambda row: str(row.get("Text") or ""),
                select_fields="Text",
                batch_size=batch_size,
                encode_batch_size=encode_batch_size,
            )
        )

        index_stats = {}
        question_index, q_time = create_vector_index(db, "Question")
        index_stats["Question"] = q_time
        _, a_time = create_vector_index(db, "Answer")
        index_stats["Answer"] = a_time
        _, c_time = create_vector_index(db, "Comment")
        index_stats["Comment"] = c_time

        hybrid_queries = run_hybrid_queries(
            db,
            model,
            question_index,
            top_k=top_k,
            candidate_limit=candidate_limit,
            min_reputation=min_reputation,
        )
    finally:
        db.close()

    return {
        "elapsed_s": time.time() - start,
        "model_load_time_s": model_load_time,
        "embedding_stats": embed_stats,
        "vector_index_time_s": index_stats,
        "hybrid_queries": hybrid_queries,
        "disk_bytes": get_dir_size_bytes(graph_db_path),
    }


def write_results(
    graph_db_path: Path, payload: Dict[str, Any], run_label: str | None
) -> Path:
    out_dir = graph_db_path / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if run_label:
        out_path = out_dir / f"hybrid_pipeline_{run_label}_{timestamp}.json"
    else:
        out_path = out_dir / f"hybrid_pipeline_{timestamp}.json"
    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Example 13: Stack Overflow Hybrid Queries (Standalone)"
    )
    parser.add_argument(
        "--dataset", choices=sorted(EXPECTED_DATASETS), default="stackoverflow-tiny"
    )
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--encode-batch-size", type=int, default=256)
    parser.add_argument("--model", default="all-MiniLM-L6-v2")
    parser.add_argument("--heap-size", default="4g")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--candidate-limit", type=int, default=500)
    parser.add_argument("--min-reputation", type=int, default=1000)
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to DB directories and result filename",
    )
    parser.add_argument(
        "--infer-sample-limit",
        type=int,
        default=100_000,
        help="Max XML rows sampled per table for schema inference.",
    )
    args = parser.parse_args()
    if args.run_label:
        args.run_label = args.run_label.strip().replace("/", "-").replace(" ", "_")
    configure_reproducibility(args.seed)

    arcadedb = get_arcadedb_module()
    if arcadedb is None:
        print("Missing dependency: arcadedb-embedded")
        print("Install with: uv pip install arcadedb-embedded")
        sys.exit(1)

    SentenceTransformer = get_sentence_transformer_class()
    if SentenceTransformer is None:
        print("Missing dependency: sentence-transformers")
        print("Install with: uv pip install sentence-transformers")
        sys.exit(1)

    data_dir = Path(__file__).parent / "data" / args.dataset
    ensure_dataset(data_dir)

    tables_name = f"{args.dataset.replace('-', '_')}_hybrid_tables"
    graph_name = f"{args.dataset.replace('-', '_')}_hybrid_graph"
    if args.run_label:
        tables_name = f"{tables_name}_{args.run_label}"
        graph_name = f"{graph_name}_{args.run_label}"

    tables_db_path = Path("./my_test_databases") / tables_name
    graph_db_path = Path("./my_test_databases") / graph_name

    jvm_kwargs = {"heap_size": args.heap_size}

    print("=" * 80)
    print("Stack Overflow Hybrid Pipeline (Standalone)")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"Tables DB: {tables_db_path}")
    print(f"Graph DB: {graph_db_path}")
    print(f"Heap: {args.heap_size}")
    print("Type inference: on")
    print(f"Type inference sample limit/table: {args.infer_sample_limit}")
    print()

    run_start = time.time()
    stop_event, rss_state, rss_thread = start_rss_sampler(
        interval_sec=0.2,
        profile_every_sec=5.0,
    )
    record_rss_checkpoint(rss_state, "run_start")

    try:
        print("[Phase 1] XML -> Tables")
        phase1 = phase1_tables(
            arcadedb,
            data_dir,
            tables_db_path,
            batch_size=args.batch_size,
            jvm_kwargs=jvm_kwargs,
            infer_sample_limit=args.infer_sample_limit,
        )
        print(
            f"  done in {phase1['elapsed_s']:.2f}s | disk={format_bytes_binary(phase1['disk_bytes'])}"
        )
        record_rss_checkpoint(rss_state, "after_phase1_tables")

        print("[Phase 2] XML -> Graph")
        phase2 = phase2_graph(
            arcadedb,
            data_dir,
            graph_db_path,
            batch_size=args.batch_size,
            jvm_kwargs=jvm_kwargs,
        )
        print(
            f"  done in {phase2['elapsed_s']:.2f}s | disk={format_bytes_binary(phase2['disk_bytes'])}"
        )
        record_rss_checkpoint(rss_state, "after_phase2_graph")

        print("[Phase 3 + 4] Embeddings/Vector + Hybrid Queries")
        phase34 = phase3_and_phase4(
            arcadedb,
            SentenceTransformer,
            graph_db_path=graph_db_path,
            model_name=args.model,
            batch_size=args.batch_size,
            encode_batch_size=args.encode_batch_size,
            top_k=args.top_k,
            candidate_limit=args.candidate_limit,
            min_reputation=args.min_reputation,
            jvm_kwargs=jvm_kwargs,
        )
        print(
            f"  done in {phase34['elapsed_s']:.2f}s | disk={format_bytes_binary(phase34['disk_bytes'])}"
        )
        record_rss_checkpoint(rss_state, "after_phase3_4_hybrid")
    finally:
        record_rss_checkpoint(rss_state, "run_end")
        stop_event.set()
        rss_thread.join()

    total_elapsed = time.time() - run_start

    payload = {
        "script": Path(__file__).name,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config": {
            "dataset": args.dataset,
            "batch_size": args.batch_size,
            "encode_batch_size": args.encode_batch_size,
            "model": args.model,
            "heap_size": args.heap_size,
            "top_k": args.top_k,
            "candidate_limit": args.candidate_limit,
            "min_reputation": args.min_reputation,
            "seed": args.seed,
            "run_label": args.run_label,
            "infer_types": True,
            "infer_sample_limit": args.infer_sample_limit,
        },
        "phase1_tables": phase1,
        "phase2_graph": phase2,
        "phase3_4_hybrid": phase34,
        "runtime": {
            "total_time_s": total_elapsed,
            "rss_peak_kb": rss_state["max_kb"],
            "rss_peak_human": format_bytes_binary(rss_state["max_kb"] * 1024),
            "rss_checkpoints": rss_state.get("checkpoints", []),
            "rss_samples": rss_state.get("samples", []),
        },
        "telemetry": {
            "run_status": "success",
            "error_type": None,
            "error_message": None,
            "db_create_time_s": (
                phase1.get("elapsed_s") if isinstance(phase1, dict) else None
            ),
            "db_open_time_s": None,
            "db_close_time_s": None,
            "query_cold_time_s": None,
            "query_warm_mean_s": None,
            "query_result_hash_stable": None,
            "query_row_count_stable": None,
        },
    }

    out_path = write_results(graph_db_path, payload, args.run_label)

    print()
    print("Results Summary")
    print("-" * 80)
    print(f"Phase 1: {phase1['elapsed_s']:.2f}s")
    print(f"Phase 2: {phase2['elapsed_s']:.2f}s")
    print(f"Phase 3+4: {phase34['elapsed_s']:.2f}s")
    print(f"Total: {total_elapsed:.2f}s")
    print(f"Peak RSS: {payload['runtime']['rss_peak_human']}")
    if payload["runtime"].get("rss_checkpoints"):
        print("RSS checkpoints:")
        for cp in payload["runtime"]["rss_checkpoints"]:
            print(f"  - {cp['label']}: {cp['rss_human']} @ +{cp['t_s']:.2f}s")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
