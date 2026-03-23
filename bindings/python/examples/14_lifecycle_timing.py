#!/usr/bin/env python3
"""
Example 14: Lifecycle Timing Benchmark

Measures elapsed time for:
1) JVM start
2) Database create + open
3) Mixed transaction workload (table + graph + vector data)
4) Mixed query workload (table + graph + vector queries)
5) Database close + reopen checks

Usage:
    python3 14_lifecycle_timing.py
    python3 14_lifecycle_timing.py --runs 5 --table-records 10000 --graph-vertices 1000
"""

from __future__ import annotations

import argparse
import random
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict, List

import arcadedb_embedded as arcadedb
from arcadedb_embedded.jvm import start_jvm


def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _build_dummy_vector(index: int, dimensions: int) -> List[float]:
    rng = random.Random(index * 7919 + dimensions * 104729)
    return [rng.uniform(-1.0, 1.0) for _ in range(dimensions)]


def create_schema(db, vector_dimensions: int) -> None:
    db.command("sql", "CREATE DOCUMENT TYPE TableRecord")
    db.command("sql", "CREATE PROPERTY TableRecord.name STRING")
    db.command("sql", "CREATE PROPERTY TableRecord.value INTEGER")
    db.command("sql", "CREATE INDEX ON TableRecord (value) NOTUNIQUE")

    db.command("sql", "CREATE VERTEX TYPE Node")
    db.command("sql", "CREATE PROPERTY Node.node_id INTEGER")
    db.command("sql", "CREATE PROPERTY Node.group_name STRING")
    db.command("sql", "CREATE INDEX ON Node (node_id) UNIQUE_HASH")

    db.command("sql", "CREATE EDGE TYPE CONNECTED_TO UNIDIRECTIONAL")
    db.command("sql", "CREATE PROPERTY CONNECTED_TO.weight INTEGER")

    db.command("sql", "CREATE VERTEX TYPE VectorDoc")
    db.command("sql", "CREATE PROPERTY VectorDoc.doc_id STRING")
    db.command("sql", "CREATE PROPERTY VectorDoc.embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE INDEX ON VectorDoc (doc_id) UNIQUE_HASH")

    db.create_vector_index(
        vertex_type="VectorDoc",
        vector_property="embedding",
        dimensions=vector_dimensions,
        distance_function="cosine",
    )


def run_single_iteration(
    db_path: Path,
    table_records: int,
    graph_vertices: int,
    vector_records: int,
    vector_dimensions: int,
    query_runs: int,
    jvm_kwargs: Dict[str, str],
) -> Dict[str, float]:
    if db_path.exists():
        shutil.rmtree(db_path)

    create_start = time.perf_counter()
    created_db = arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs)
    create_time_s = time.perf_counter() - create_start

    schema_start = time.perf_counter()
    create_schema(created_db, vector_dimensions=vector_dimensions)
    schema_time_s = time.perf_counter() - schema_start
    created_db.close()

    open_start = time.perf_counter()
    db = arcadedb.open_database(str(db_path), jvm_kwargs=jvm_kwargs)
    open_time_s = time.perf_counter() - open_start

    transaction_start = time.perf_counter()
    transaction_breakdown_start = time.perf_counter()
    with db.transaction():
        for index in range(table_records):
            db.command(
                "sql",
                "INSERT INTO TableRecord SET name = ?, value = ?",
                f"record-{index}",
                index,
            )

        for index in range(graph_vertices):
            db.command(
                "sql",
                "INSERT INTO Node SET node_id = ?, group_name = ?",
                index,
                f"group-{index % 10}",
            )

        for index in range(1, graph_vertices):
            db.command(
                "sql",
                "CREATE EDGE CONNECTED_TO "
                "FROM (SELECT FROM Node WHERE node_id = ?) "
                "TO (SELECT FROM Node WHERE node_id = ?) "
                "SET weight = ?",
                index - 1,
                index,
                (index % 100),
            )

        for index in range(vector_records):
            db.command(
                "sql",
                "INSERT INTO VectorDoc SET doc_id = ?, embedding = ?",
                f"vec-{index}",
                arcadedb.to_java_float_array(
                    _build_dummy_vector(index=index, dimensions=vector_dimensions)
                ),
            )
    transaction_time_s = time.perf_counter() - transaction_start
    load_time_s = time.perf_counter() - transaction_breakdown_start

    query_start = time.perf_counter()
    for query_index in range(query_runs):
        db.query(
            "sql",
            "SELECT count(*) AS count FROM TableRecord WHERE value >= ?",
            query_index % max(1, table_records),
        ).first()

    db.query(
        "sql",
        "SELECT count(*) AS count FROM Node",
    ).first()

    db.query(
        "sql",
        "SELECT count(*) AS count FROM CONNECTED_TO",
    ).first()

    query_vector = _build_dummy_vector(index=999_999, dimensions=vector_dimensions)
    query_vector_sql = "[" + ", ".join(str(v) for v in query_vector) + "]"
    for _ in range(query_runs):
        db.query(
            "sql",
            f"SELECT vectorNeighbors('VectorDoc[embedding]', {query_vector_sql}, 5) AS res",
        ).first()

    query_time_s = time.perf_counter() - query_start

    close_start = time.perf_counter()
    db.close()
    close_time_s = time.perf_counter() - close_start

    reopen_start = time.perf_counter()
    reopened_db = arcadedb.open_database(str(db_path), jvm_kwargs=jvm_kwargs)
    reopen_time_s = time.perf_counter() - reopen_start

    reopen_query_start = time.perf_counter()
    for query_index in range(query_runs):
        reopened_db.query(
            "sql",
            "SELECT count(*) AS count FROM TableRecord WHERE value >= ?",
            query_index % max(1, table_records),
        ).first()

    reopened_db.query("sql", "SELECT count(*) AS count FROM Node").first()
    reopened_db.query("sql", "SELECT count(*) AS count FROM CONNECTED_TO").first()

    for _ in range(query_runs):
        reopened_db.query(
            "sql",
            f"SELECT vectorNeighbors('VectorDoc[embedding]', {query_vector_sql}, 5) AS res",
        ).first()

    reopen_query_time_s = time.perf_counter() - reopen_query_start

    reopen_close_start = time.perf_counter()
    reopened_db.close()
    reopen_close_time_s = time.perf_counter() - reopen_close_start

    return {
        "create_time_s": create_time_s,
        "schema_time_s": schema_time_s,
        "open_time_s": open_time_s,
        "transaction_time_s": transaction_time_s,
        "load_time_s": load_time_s,
        "query_time_s": query_time_s,
        "close_time_s": close_time_s,
        "reopen_time_s": reopen_time_s,
        "reopen_query_time_s": reopen_query_time_s,
        "reopen_close_time_s": reopen_close_time_s,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 14: Lifecycle Timing Benchmark"
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional benchmark database directory path. If omitted, uses random /tmp path.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of repeated runs (default: 5)",
    )
    parser.add_argument(
        "--table-records",
        type=int,
        default=50000,
        help="Number of table records inserted in transaction (default: 50000)",
    )
    parser.add_argument(
        "--graph-vertices",
        type=int,
        default=10000,
        help="Number of graph vertices inserted in transaction (default: 10000)",
    )
    parser.add_argument(
        "--vector-records",
        type=int,
        default=10000,
        help="Number of vector vertices inserted in transaction (default: 10000)",
    )
    parser.add_argument(
        "--vector-dimensions",
        type=int,
        default=64,
        help="Dummy vector dimensions (default: 64)",
    )
    parser.add_argument(
        "--query-runs",
        type=int,
        default=100,
        help="Number of repeated table/vector queries per run (default: 100)",
    )
    parser.add_argument(
        "--jvm-heap",
        default="2g",
        help="Heap size passed to JVM startup (default: 2g)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        db_path = Path(tempfile.gettempdir()) / f"arcadedb_lifecycle_{uuid.uuid4().hex}"

    if args.runs < 1:
        raise ValueError("--runs must be >= 1")
    if args.table_records < 1:
        raise ValueError("--table-records must be >= 1")
    if args.graph_vertices < 2:
        raise ValueError("--graph-vertices must be >= 2")
    if args.vector_records < 1:
        raise ValueError("--vector-records must be >= 1")
    if args.vector_dimensions < 2:
        raise ValueError("--vector-dimensions must be >= 2")
    if args.query_runs < 1:
        raise ValueError("--query-runs must be >= 1")

    try:
        print("=" * 70)
        print("🎮 ArcadeDB Python - Example 14: Lifecycle Timing Benchmark")
        print("=" * 70)
        print(f"DB path: {db_path}")
        print(f"Runs: {args.runs}")
        print(f"Table records: {args.table_records}")
        print(f"Graph vertices: {args.graph_vertices}")
        print(f"Vector records: {args.vector_records}")
        print(f"Vector dimensions: {args.vector_dimensions}")
        print(f"Query runs: {args.query_runs}")
        print()

        jvm_start_begin = time.perf_counter()
        start_jvm(heap_size=args.jvm_heap)
        jvm_start_time_s = time.perf_counter() - jvm_start_begin
        print(f"JVM start time: {jvm_start_time_s:.6f}s")
        print()

        runs: List[Dict[str, float]] = []
        jvm_kwargs = {"heap_size": args.jvm_heap}
        for run_index in range(1, args.runs + 1):
            timings = run_single_iteration(
                db_path=db_path,
                table_records=args.table_records,
                graph_vertices=args.graph_vertices,
                vector_records=args.vector_records,
                vector_dimensions=args.vector_dimensions,
                query_runs=args.query_runs,
                jvm_kwargs=jvm_kwargs,
            )
            runs.append(timings)
            print(
                f"Run {run_index:02d} | "
                f"create: {timings['create_time_s']:.6f}s | "
                f"schema: {timings['schema_time_s']:.6f}s | "
                f"open: {timings['open_time_s']:.6f}s | "
                f"transaction: {timings['transaction_time_s']:.6f}s | "
                f"query: {timings['query_time_s']:.6f}s | "
                f"close: {timings['close_time_s']:.6f}s | "
                f"reopen: {timings['reopen_time_s']:.6f}s | "
                f"reopen_query: {timings['reopen_query_time_s']:.6f}s | "
                f"reopen_close: {timings['reopen_close_time_s']:.6f}s"
            )

        summary = {
            "jvm_start_time_s": jvm_start_time_s,
            "create_time_s_avg": _avg([row["create_time_s"] for row in runs]),
            "schema_time_s_avg": _avg([row["schema_time_s"] for row in runs]),
            "open_time_s_avg": _avg([row["open_time_s"] for row in runs]),
            "transaction_time_s_avg": _avg([row["transaction_time_s"] for row in runs]),
            "load_time_s_avg": _avg([row["load_time_s"] for row in runs]),
            "query_time_s_avg": _avg([row["query_time_s"] for row in runs]),
            "close_time_s_avg": _avg([row["close_time_s"] for row in runs]),
            "reopen_time_s_avg": _avg([row["reopen_time_s"] for row in runs]),
            "reopen_query_time_s_avg": _avg(
                [row["reopen_query_time_s"] for row in runs]
            ),
            "reopen_close_time_s_avg": _avg(
                [row["reopen_close_time_s"] for row in runs]
            ),
        }

        print()
        print("Averages")
        print(f"  jvm start:   {summary['jvm_start_time_s']:.6f}s")
        print(f"  create:      {summary['create_time_s_avg']:.6f}s")
        print(f"  schema:      {summary['schema_time_s_avg']:.6f}s")
        print(f"  open:        {summary['open_time_s_avg']:.6f}s")
        print(f"  transaction: {summary['transaction_time_s_avg']:.6f}s")
        print(f"  load:        {summary['load_time_s_avg']:.6f}s")
        print(f"  query:       {summary['query_time_s_avg']:.6f}s")
        print(f"  close:       {summary['close_time_s_avg']:.6f}s")
        print(f"  reopen:      {summary['reopen_time_s_avg']:.6f}s")
        print(f"  reopen query:{summary['reopen_query_time_s_avg']:.6f}s")
        print(f"  reopen close:{summary['reopen_close_time_s_avg']:.6f}s")
    finally:
        if db_path.exists():
            shutil.rmtree(db_path)
            print(f"Removed benchmark database: {db_path}")


if __name__ == "__main__":
    main()
