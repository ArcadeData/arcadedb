#!/usr/bin/env python3
"""
Example 16: Import Database vs Transactional Graph Ingest.

This script generates synthetic graph CSV datasets (vertices + edges) and then
loads them into separate ArcadeDB databases using:
1) Transactional graph INSERT/CREATE EDGE in batches
2) Embedded GraphBatch API for buffered vertex/edge loading
3) Async SQL graph INSERT/CREATE EDGE via async executor
4) SQL IMPORT DATABASE for vertices and edges

Goal: compare graph ingest speed for equivalent synthetic data shape.

Observed benchmark result (2026-03-19):
For:
- vertices=2,000,000
- edges=2,000,000
- vertex-int-props=10
- vertex-str-props=10
- edge-int-props=10
- edge-str-props=10
- string-size=64
- batch-size=10,000
- heap-size=8g

Measured ingest times:
- Transactional (`single-threaded`): 253.615s
- GraphBatch (`single-threaded`, `--parallel 1`): 177.150s
- GraphBatch (`4 threads`, `--parallel 4`): 187.681s
- GraphBatch (`8 threads`, `--parallel 8`): 134.836s
- Async SQL (`single-threaded`, `--async-parallel 1`): 230.192s
- IMPORT DATABASE (`single-threaded`, `--parallel 1`): 444.357s
- IMPORT DATABASE (`4 threads`, `--parallel 4`): 336.206s

Known limitation:
Current `IMPORT DATABASE` behavior can vary by import path and data shape. In some
CSV-heavy scenarios, transaction splitting via `commitEvery` may not behave as expected,
so very large imports can still hit transaction-buffer limits.

Important:
When running IMPORT DATABASE from Python against an already-open database,
explicitly configure async/import settings on the DB handle before import:
- set_read_your_writes(False)
- async_executor().set_parallel_level(parallel)
- async_executor().set_commit_every(batch_size)
- async_executor().set_transaction_use_wal(False)

Without this, import may not use the intended runtime settings in this code path, which
can produce misleading performance and/or parity behavior.

Note:
This example compares embedded ingestion paths. It does not benchmark the newer
server-side gRPC GraphBatchLoad transport.
"""

import argparse
import shutil
import time
from pathlib import Path
from typing import Dict, List, Tuple

import arcadedb_embedded as arcadedb

ColumnDef = Tuple[str, str]

NUMERIC_COLUMN_TYPES = {"INTEGER", "LONG"}


def result_int(row, *keys: str) -> int:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return int(value)
    return 0


def build_column_defs(prefix: str, int_count: int, str_count: int) -> List[ColumnDef]:
    columns: List[ColumnDef] = []

    for idx in range(1, int_count + 1):
        columns.append((f"{prefix}i{idx}", "LONG"))

    for idx in range(1, str_count + 1):
        columns.append((f"{prefix}s{idx}", "STRING"))

    return columns


def make_string_value(kind: str, column_name: str, row_id: int, size: int) -> str:
    base = f"{kind}_{column_name}_{row_id}"
    if len(base) >= size:
        return base[:size]

    padding = "x" * (size - len(base))
    return base + padding


def recreate_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def print_faster_statement(
    faster_name: str,
    faster_seconds: float,
    slower_name: str,
    slower_seconds: float,
) -> None:
    if faster_seconds <= 0 or slower_seconds <= 0:
        return
    ratio = slower_seconds / faster_seconds
    pct_less_time = (1.0 - (faster_seconds / slower_seconds)) * 100.0
    print(
        (
            f"- {faster_name} is {ratio:.2f}x faster than {slower_name} "
            f"({slower_seconds:.3f}s -> {faster_seconds:.3f}s, "
            f"{pct_less_time:.1f}% less time)"
        )
    )


def edge_endpoints(edge_id: int, vertex_count: int) -> Tuple[int, int]:
    idx = edge_id - 1
    from_id = (idx % vertex_count) + 1
    hop = ((idx // vertex_count) % (vertex_count - 1)) + 1
    to_id = ((from_id - 1 + hop) % vertex_count) + 1
    return from_id, to_id


def build_rid_lookup_for_vertex_type(db, vertex_type: str) -> Dict[int, str]:
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
        except (TypeError, ValueError):
            continue
    return rid_lookup


def sample_ids(total: int) -> List[int]:
    values = {1, max(1, total // 2), total}
    return sorted(values)


def query_one_or_none(result_set):
    try:
        return result_set.one()
    except ValueError as exc:
        if str(exc) == "Query returned no results":
            return None
        raise


def collect_vertex_sample(
    db, vertex_type: str, vertex_id: int, props: List[ColumnDef]
) -> dict:
    row = query_one_or_none(
        db.query("sql", f"SELECT FROM {vertex_type} WHERE Id = {vertex_id}")
    )
    if row is None:
        return {"Id": vertex_id, "missing": True}

    sample = {"Id": int(row.get("Id"))}
    for column_name, _ in props:
        sample[column_name] = row.get(column_name)
    return sample


def collect_edge_sample(
    db, edge_type: str, edge_id: int, props: List[ColumnDef]
) -> dict:
    projections = [
        "a.Id AS from_id",
        "b.Id AS to_id",
        "r.Id AS Id",
    ] + [f"r.{column_name} AS {column_name}" for column_name, _ in props]

    row = query_one_or_none(
        db.query(
            "opencypher",
            (
                f"MATCH (a)-[r:{edge_type}]->(b) "
                f"WHERE r.Id = {edge_id} "
                f"RETURN {', '.join(projections)}"
            ),
        )
    )
    if row is None:
        return {"Id": edge_id, "missing": True}

    sample = {
        "from_id": int(row.get("from_id")),
        "to_id": int(row.get("to_id")),
        "Id": int(row.get("Id")),
    }
    for column_name, _ in props:
        sample[column_name] = row.get(column_name)
    return sample


def collect_graph_signature(
    db_path: Path,
    heap_size: str,
    vertex_type: str,
    edge_type: str,
    vertex_props: List[ColumnDef],
    edge_props: List[ColumnDef],
    vertex_count: int,
    edge_count: int,
) -> dict:
    with arcadedb.open_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    ) as db:
        vertex_int_props = [
            name for name, kind in vertex_props if kind in NUMERIC_COLUMN_TYPES
        ]
        edge_int_props = [
            name for name, kind in edge_props if kind in NUMERIC_COLUMN_TYPES
        ]

        vertex_aggregate_fields = [
            "count(*) AS count",
            "sum(Id) AS sum_id",
            "min(Id) AS min_id",
            "max(Id) AS max_id",
        ] + [f"sum({name}) AS sum_{name}" for name in vertex_int_props]

        edge_match_aggregate_fields = [
            "count(r) AS count",
            "sum(r.Id) AS sum_id",
            "min(r.Id) AS min_id",
            "max(r.Id) AS max_id",
        ] + [f"sum(r.{name}) AS sum_{name}" for name in edge_int_props]

        vertex_aggregate = query_one_or_none(
            db.query(
                "sql",
                f"SELECT {', '.join(vertex_aggregate_fields)} FROM {vertex_type}",
            )
        )
        edge_aggregate = query_one_or_none(
            db.query(
                "opencypher",
                (
                    f"MATCH ()-[r:{edge_type}]->() "
                    f"RETURN {', '.join(edge_match_aggregate_fields)}"
                ),
            )
        )

        if vertex_aggregate is None:
            vertex_aggregate = {}
        if edge_aggregate is None:
            edge_aggregate = {}

        return {
            "vertex_aggregate": {
                "count": result_int(vertex_aggregate, "count"),
                "sum_id": result_int(vertex_aggregate, "sum_id"),
                "min_id": result_int(vertex_aggregate, "min_id"),
                "max_id": result_int(vertex_aggregate, "max_id"),
                **{
                    f"sum_{name}": result_int(vertex_aggregate, f"sum_{name}")
                    for name in vertex_int_props
                },
            },
            "edge_aggregate": {
                "count": result_int(edge_aggregate, "count"),
                "sum_id": result_int(edge_aggregate, "sum_id"),
                "min_id": result_int(edge_aggregate, "min_id"),
                "max_id": result_int(edge_aggregate, "max_id"),
                **{
                    f"sum_{name}": result_int(edge_aggregate, f"sum_{name}")
                    for name in edge_int_props
                },
            },
            "vertex_samples": {
                vertex_id: collect_vertex_sample(
                    db, vertex_type, vertex_id, vertex_props
                )
                for vertex_id in sample_ids(vertex_count)
            },
            "edge_samples": {
                edge_id: collect_edge_sample(db, edge_type, edge_id, edge_props)
                for edge_id in sample_ids(edge_count)
            },
        }


def validate_graph_signatures_or_fail(signatures: Dict[str, dict]) -> None:
    baseline_name = next(iter(signatures))
    baseline = signatures[baseline_name]
    problems: List[str] = []

    for name, signature in signatures.items():
        if signature != baseline:
            problems.append(f"{name} signature differs from {baseline_name}")

    if problems:
        raise RuntimeError("Logical graph output differs:\n- " + "\n- ".join(problems))


def build_vertex_csv(
    csv_path: Path,
    vertex_count: int,
    vertex_props: List[ColumnDef],
    string_size: int,
) -> None:
    header = ["Id"] + [column_name for column_name, _ in vertex_props]

    with csv_path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(header) + "\n")

        for vertex_id in range(1, vertex_count + 1):
            values: List[str] = [str(vertex_id)]
            for column_name, column_type in vertex_props:
                if column_type in NUMERIC_COLUMN_TYPES:
                    values.append(str((vertex_id * 37 + len(column_name)) % 1000000))
                else:
                    values.append(
                        make_string_value("vertex", column_name, vertex_id, string_size)
                    )

            handle.write(",".join(values) + "\n")


def build_edge_csv(
    csv_path: Path,
    edge_count: int,
    vertex_count: int,
    edge_props: List[ColumnDef],
    string_size: int,
) -> None:
    header = ["Id", "From", "To"] + [column_name for column_name, _ in edge_props]

    with csv_path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(header) + "\n")

        for edge_id in range(1, edge_count + 1):
            from_id, to_id = edge_endpoints(edge_id, vertex_count)
            values: List[str] = [str(edge_id), str(from_id), str(to_id)]

            for column_name, column_type in edge_props:
                if column_type in NUMERIC_COLUMN_TYPES:
                    values.append(str((edge_id * 41 + len(column_name)) % 1000000))
                else:
                    values.append(
                        make_string_value("edge", column_name, edge_id, string_size)
                    )

            handle.write(",".join(values) + "\n")


def run_transactional_graph_load(
    db_path: Path,
    vertex_count: int,
    edge_count: int,
    vertex_props: List[ColumnDef],
    edge_props: List[ColumnDef],
    string_size: int,
    batch_size: int,
    heap_size: str,
    vertex_type: str,
    edge_type: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    start = time.perf_counter()

    db.command("sql", f"CREATE VERTEX TYPE {vertex_type}")
    db.command("sql", f"CREATE PROPERTY {vertex_type}.Id LONG")
    db.command("sql", f"CREATE EDGE TYPE {edge_type} UNIDIRECTIONAL")

    vertex_setters = ", ".join(["Id = ?"] + [f"{name} = ?" for name, _ in vertex_props])
    vertex_insert_sql = f"INSERT INTO {vertex_type} SET {vertex_setters}"

    for begin in range(1, vertex_count + 1, batch_size):
        end = min(begin + batch_size - 1, vertex_count)
        with db.transaction():
            for vertex_id in range(begin, end + 1):
                payload: List[object] = [vertex_id]
                for column_name, column_type in vertex_props:
                    if column_type in NUMERIC_COLUMN_TYPES:
                        payload.append((vertex_id * 37 + len(column_name)) % 1000000)
                    else:
                        payload.append(
                            make_string_value(
                                "vertex",
                                column_name,
                                vertex_id,
                                string_size,
                            )
                        )

                db.command("sql", vertex_insert_sql, payload)

    db.command("sql", f"CREATE INDEX ON {vertex_type} (Id) UNIQUE_HASH")
    rid_lookup = build_rid_lookup_for_vertex_type(db, vertex_type)
    edge_setters = ", ".join(["Id = ?"] + [f"{name} = ?" for name, _ in edge_props])

    for begin in range(1, edge_count + 1, batch_size):
        end = min(begin + batch_size - 1, edge_count)
        with db.transaction():
            for edge_id in range(begin, end + 1):
                from_id, to_id = edge_endpoints(edge_id, vertex_count)

                from_rid = rid_lookup.get(from_id)
                to_rid = rid_lookup.get(to_id)
                if from_rid is None or to_rid is None:
                    raise RuntimeError(
                        "Missing RID endpoint for edge "
                        f"{edge_id}: from_id={from_id}, to_id={to_id}"
                    )

                edge_insert_sql = (
                    f"CREATE EDGE {edge_type} "
                    f"FROM {from_rid} "
                    f"TO {to_rid} "
                    f"SET {edge_setters}"
                )

                payload = [edge_id]
                for column_name, column_type in edge_props:
                    if column_type in NUMERIC_COLUMN_TYPES:
                        payload.append((edge_id * 41 + len(column_name)) % 1000000)
                    else:
                        payload.append(
                            make_string_value("edge", column_name, edge_id, string_size)
                        )

                db.command("sql", edge_insert_sql, payload)

    vertex_loaded = (
        db.query("opencypher", "MATCH (n) RETURN count(n) AS c").one().get("c") or 0
    )
    edge_loaded = (
        db.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c").one().get("c")
        or 0
    )

    elapsed = time.perf_counter() - start
    db.close()

    return {
        "vertices": int(vertex_loaded),
        "edges": int(edge_loaded),
        "seconds": elapsed,
    }


def run_async_sql_graph_load(
    db_path: Path,
    vertex_count: int,
    edge_count: int,
    vertex_props: List[ColumnDef],
    edge_props: List[ColumnDef],
    string_size: int,
    batch_size: int,
    async_parallel: int,
    heap_size: str,
    vertex_type: str,
    edge_type: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(max(1, async_parallel))
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)

    errors: List[Exception] = []

    def on_error(exc: Exception):
        errors.append(exc)

    async_exec.on_error(on_error)

    start = time.perf_counter()

    try:
        db.command("sql", f"CREATE VERTEX TYPE {vertex_type}")
        db.command("sql", f"CREATE PROPERTY {vertex_type}.Id LONG")
        db.command("sql", f"CREATE EDGE TYPE {edge_type} UNIDIRECTIONAL")

        vertex_setters = ", ".join(
            ["Id = ?"] + [f"{name} = ?" for name, _ in vertex_props]
        )
        vertex_insert_sql = f"INSERT INTO {vertex_type} SET {vertex_setters}"

        for vertex_id in range(1, vertex_count + 1):
            payload: List[object] = [vertex_id]
            for column_name, column_type in vertex_props:
                if column_type in NUMERIC_COLUMN_TYPES:
                    payload.append((vertex_id * 37 + len(column_name)) % 1000000)
                else:
                    payload.append(
                        make_string_value(
                            "vertex",
                            column_name,
                            vertex_id,
                            string_size,
                        )
                    )

            async_exec.command("sql", vertex_insert_sql, args=payload)

        async_exec.wait_completion()

        db.command("sql", f"CREATE INDEX ON {vertex_type} (Id) UNIQUE_HASH")
        rid_lookup = build_rid_lookup_for_vertex_type(db, vertex_type)
        edge_setters = ", ".join(["Id = ?"] + [f"{name} = ?" for name, _ in edge_props])

        for edge_id in range(1, edge_count + 1):
            from_id, to_id = edge_endpoints(edge_id, vertex_count)

            from_rid = rid_lookup.get(from_id)
            to_rid = rid_lookup.get(to_id)
            if from_rid is None or to_rid is None:
                raise RuntimeError(
                    "Missing RID endpoint for edge "
                    f"{edge_id}: from_id={from_id}, to_id={to_id}"
                )

            edge_insert_sql = (
                f"CREATE EDGE {edge_type} "
                f"FROM {from_rid} "
                f"TO {to_rid} "
                f"SET {edge_setters}"
            )

            payload = [edge_id]
            for column_name, column_type in edge_props:
                if column_type in NUMERIC_COLUMN_TYPES:
                    payload.append((edge_id * 41 + len(column_name)) % 1000000)
                else:
                    payload.append(
                        make_string_value("edge", column_name, edge_id, string_size)
                    )

            async_exec.command("sql", edge_insert_sql, args=payload)

        async_exec.wait_completion()

        if errors:
            raise RuntimeError(
                f"Async SQL ingest reported {len(errors)} errors (first: {errors[0]})"
            )

        vertex_loaded = (
            db.query("opencypher", "MATCH (n) RETURN count(n) AS c").one().get("c") or 0
        )
        edge_loaded = (
            db.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")
            .one()
            .get("c")
            or 0
        )

        elapsed = time.perf_counter() - start
    finally:
        async_exec.wait_completion()
        async_exec.close()
        db.set_read_your_writes(True)
        async_exec.set_transaction_use_wal(True)
        db.close()

    return {
        "vertices": int(vertex_loaded),
        "edges": int(edge_loaded),
        "seconds": elapsed,
    }


def run_graph_batch_graph_load(
    db_path: Path,
    vertex_count: int,
    edge_count: int,
    vertex_props: List[ColumnDef],
    edge_props: List[ColumnDef],
    string_size: int,
    batch_size: int,
    parallel: int,
    heap_size: str,
    vertex_type: str,
    edge_type: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    db.async_executor().set_parallel_level(max(1, parallel))

    start = time.perf_counter()

    try:
        db.command("sql", f"CREATE VERTEX TYPE {vertex_type}")
        db.command("sql", f"CREATE PROPERTY {vertex_type}.Id LONG")
        db.command("sql", f"CREATE EDGE TYPE {edge_type} UNIDIRECTIONAL")

        rid_lookup: Dict[int, str] = {}

        with db.graph_batch(
            batch_size=batch_size,
            expected_edge_count=edge_count,
            bidirectional=False,
            commit_every=batch_size,
            use_wal=False,
            parallel_flush=parallel > 1,
        ) as batch:
            for begin in range(1, vertex_count + 1, batch_size):
                end = min(begin + batch_size - 1, vertex_count)
                vertex_rows: List[dict] = []
                vertex_ids: List[int] = []

                for vertex_id in range(begin, end + 1):
                    row = {"Id": vertex_id}
                    for column_name, column_type in vertex_props:
                        if column_type in NUMERIC_COLUMN_TYPES:
                            row[column_name] = (
                                vertex_id * 37 + len(column_name)
                            ) % 1000000
                        else:
                            row[column_name] = make_string_value(
                                "vertex",
                                column_name,
                                vertex_id,
                                string_size,
                            )
                    vertex_rows.append(row)
                    vertex_ids.append(vertex_id)

                created_rids = batch.create_vertices(vertex_type, vertex_rows)
                for vertex_id, rid in zip(vertex_ids, created_rids):
                    rid_lookup[vertex_id] = rid

            for edge_id in range(1, edge_count + 1):
                from_id, to_id = edge_endpoints(edge_id, vertex_count)

                from_rid = rid_lookup.get(from_id)
                to_rid = rid_lookup.get(to_id)
                if from_rid is None or to_rid is None:
                    raise RuntimeError(
                        "Missing RID endpoint for edge "
                        f"{edge_id}: from_id={from_id}, to_id={to_id}"
                    )

                payload = {"Id": edge_id}
                for column_name, column_type in edge_props:
                    if column_type in NUMERIC_COLUMN_TYPES:
                        payload[column_name] = (
                            edge_id * 41 + len(column_name)
                        ) % 1000000
                    else:
                        payload[column_name] = make_string_value(
                            "edge",
                            column_name,
                            edge_id,
                            string_size,
                        )

                batch.new_edge(from_rid, edge_type, to_rid, **payload)

        db.command("sql", f"CREATE INDEX ON {vertex_type} (Id) UNIQUE_HASH")

        vertex_loaded = (
            db.query("opencypher", "MATCH (n) RETURN count(n) AS c").one().get("c") or 0
        )
        edge_loaded = (
            db.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")
            .one()
            .get("c")
            or 0
        )

        elapsed = time.perf_counter() - start
    finally:
        db.close()

    return {
        "vertices": int(vertex_loaded),
        "edges": int(edge_loaded),
        "seconds": elapsed,
    }


def run_import_database_graph_load(
    db_path: Path,
    vertices_csv_path: Path,
    edges_csv_path: Path,
    batch_size: int,
    heap_size: str,
    parallel: int,
    vertex_type: str,
    edge_type: str,
) -> dict:
    recreate_dir(db_path)

    db = arcadedb.create_database(
        str(db_path),
        jvm_kwargs={"heap_size": heap_size} if heap_size else None,
    )

    db.set_read_your_writes(False)
    async_exec = db.async_executor()
    async_exec.set_parallel_level(parallel)
    async_exec.set_commit_every(batch_size)
    async_exec.set_transaction_use_wal(False)

    start = time.perf_counter()

    vertices_url = f"file://{vertices_csv_path.resolve().as_posix()}"
    edges_url = f"file://{edges_csv_path.resolve().as_posix()}"

    try:
        import_result = db.command(
            "sql",
            (
                "IMPORT DATABASE WITH "
                f"vertices = '{vertices_url}', "
                f"edges = '{edges_url}', "
                f"vertexType = '{vertex_type}', "
                f"edgeType = '{edge_type}', "
                "typeIdProperty = 'Id', "
                "typeIdType = 'Long', "
                "typeIdUnique = true, "
                "edgeFromField = 'From', "
                "edgeToField = 'To', "
                "edgeBidirectional = false, "
                f"commitEvery = {batch_size}, "
                f"parallel = {parallel}"
            ),
        ).one()

        db.async_executor().wait_completion()

        vertex_loaded = (
            db.query("opencypher", "MATCH (n) RETURN count(n) AS c").one().get("c") or 0
        )
        edge_loaded = (
            db.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")
            .one()
            .get("c")
            or 0
        )

        elapsed = time.perf_counter() - start
    finally:
        db.async_executor().wait_completion()
        db.set_read_your_writes(True)
        async_exec.set_transaction_use_wal(True)
        db.close()

    imported_vertices = result_int(
        import_result,
        "createdVertices",
        "totalVertices",
        "vertices",
    )
    imported_edges = result_int(
        import_result,
        "createdEdges",
        "totalEdges",
        "linkedEdges",
        "edges",
    )

    return {
        "vertices": int(vertex_loaded),
        "edges": int(edge_loaded),
        "vertices_reported": imported_vertices,
        "edges_reported": imported_edges,
        "seconds": elapsed,
        "errors": int(import_result.get("errors") or 0),
    }


def validate_parity_or_fail(
    args: argparse.Namespace,
    txn: dict,
    graph_batch: dict,
    async_sql: dict,
    imp: dict,
) -> None:
    problems: List[str] = []

    if txn["vertices"] != args.vertices or txn["edges"] != args.edges:
        problems.append(
            "transactional counts do not match expected shape "
            f"(expected V={args.vertices:,}, E={args.edges:,}; "
            f"got V={txn['vertices']:,}, E={txn['edges']:,})"
        )

    if imp["vertices"] != args.vertices or imp["edges"] != args.edges:
        problems.append(
            "IMPORT DATABASE query counts do not match expected shape "
            f"(expected V={args.vertices:,}, E={args.edges:,}; "
            f"got V={imp['vertices']:,}, E={imp['edges']:,})"
        )

    if graph_batch["vertices"] != args.vertices or graph_batch["edges"] != args.edges:
        problems.append(
            "graphbatch counts do not match expected shape "
            f"(expected V={args.vertices:,}, E={args.edges:,}; "
            f"got V={graph_batch['vertices']:,}, E={graph_batch['edges']:,})"
        )

    if async_sql["vertices"] != args.vertices or async_sql["edges"] != args.edges:
        problems.append(
            "async-sql counts do not match expected shape "
            f"(expected V={args.vertices:,}, E={args.edges:,}; "
            f"got V={async_sql['vertices']:,}, E={async_sql['edges']:,})"
        )

    if txn["vertices"] != imp["vertices"] or txn["edges"] != imp["edges"]:
        problems.append(
            "transactional and IMPORT DATABASE query counts differ "
            f"(txn V={txn['vertices']:,}, E={txn['edges']:,}; "
            f"import V={imp['vertices']:,}, E={imp['edges']:,})"
        )

    if (
        graph_batch["vertices"] != imp["vertices"]
        or graph_batch["edges"] != imp["edges"]
    ):
        problems.append(
            (
                "graphbatch and IMPORT DATABASE query counts differ "
                f"(graphbatch V={graph_batch['vertices']:,}, "
                f"E={graph_batch['edges']:,}; "
                f"import V={imp['vertices']:,}, E={imp['edges']:,})"
            )
        )

    if async_sql["vertices"] != imp["vertices"] or async_sql["edges"] != imp["edges"]:
        problems.append(
            "async-sql and IMPORT DATABASE query counts differ "
            f"(async-sql V={async_sql['vertices']:,}, "
            f"E={async_sql['edges']:,}; "
            f"import V={imp['vertices']:,}, E={imp['edges']:,})"
        )

    if problems:
        raise RuntimeError("Invalid benchmark run:\n- " + "\n- ".join(problems))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Example 16: Import Database vs Transactional Graph Ingest"
    )
    parser.add_argument(
        "--vertices", type=int, default=100_000, help="Number of vertices"
    )
    parser.add_argument("--edges", type=int, default=300_000, help="Number of edges")
    parser.add_argument(
        "--vertex-int-props",
        type=int,
        default=6,
        help="INTEGER properties per vertex",
    )
    parser.add_argument(
        "--vertex-str-props",
        type=int,
        default=4,
        help="STRING properties per vertex",
    )
    parser.add_argument(
        "--edge-int-props",
        type=int,
        default=2,
        help="INTEGER properties per edge",
    )
    parser.add_argument(
        "--edge-str-props",
        type=int,
        default=1,
        help="STRING properties per edge",
    )
    parser.add_argument(
        "--string-size",
        type=int,
        default=64,
        help="Size in bytes for generated STRING values",
    )
    parser.add_argument("--batch-size", type=int, default=10_000, help="Batch size")
    parser.add_argument(
        "--async-parallel",
        type=int,
        default=1,
        help="Parallel workers for async SQL path (default 1 for stability)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Parallel workers for SQL import and GraphBatch parallel flush",
    )
    parser.add_argument("--heap-size", type=str, default="4g", help="JVM heap size")
    parser.add_argument(
        "--work-dir",
        type=str,
        default="./my_test_databases/import_vs_txn_graph",
        help="Output working directory",
    )
    args = parser.parse_args()

    if args.vertices < 2:
        raise ValueError("--vertices must be >= 2")
    if args.edges < 1:
        raise ValueError("--edges must be >= 1")

    work_dir = Path(args.work_dir)
    recreate_dir(work_dir)

    data_dir = work_dir / "csv"
    recreate_dir(data_dir)

    vertex_type = "Node"
    edge_type = "Relationship"

    vertex_props = build_column_defs("v", args.vertex_int_props, args.vertex_str_props)
    edge_props = build_column_defs("e", args.edge_int_props, args.edge_str_props)

    vertices_csv_path = data_dir / "vertices.csv"
    edges_csv_path = data_dir / "edges.csv"

    print("Generating graph CSV files...")
    print(
        f"Dataset shape: vertices={args.vertices:,}, edges={args.edges:,}, "
        f"vertex props={len(vertex_props)}, edge props={len(edge_props)}"
    )
    print(f"String payload size: {args.string_size} bytes")

    build_vertex_csv(
        csv_path=vertices_csv_path,
        vertex_count=args.vertices,
        vertex_props=vertex_props,
        string_size=args.string_size,
    )
    build_edge_csv(
        csv_path=edges_csv_path,
        edge_count=args.edges,
        vertex_count=args.vertices,
        edge_props=edge_props,
        string_size=args.string_size,
    )

    txn_db = work_dir / "db_transactional"
    graph_batch_db = work_dir / "db_graph_batch"
    async_sql_db = work_dir / "db_async_sql"
    imp_db = work_dir / "db_import_database"

    print("\nRunning transactional graph ingest benchmark...")
    txn = run_transactional_graph_load(
        db_path=txn_db,
        vertex_count=args.vertices,
        edge_count=args.edges,
        vertex_props=vertex_props,
        edge_props=edge_props,
        string_size=args.string_size,
        batch_size=args.batch_size,
        heap_size=args.heap_size,
        vertex_type=vertex_type,
        edge_type=edge_type,
    )

    print("Running GraphBatch graph ingest benchmark...")
    graph_batch = run_graph_batch_graph_load(
        db_path=graph_batch_db,
        vertex_count=args.vertices,
        edge_count=args.edges,
        vertex_props=vertex_props,
        edge_props=edge_props,
        string_size=args.string_size,
        batch_size=args.batch_size,
        parallel=args.parallel,
        heap_size=args.heap_size,
        vertex_type=vertex_type,
        edge_type=edge_type,
    )

    print("Running async SQL graph ingest benchmark...")
    async_sql = run_async_sql_graph_load(
        db_path=async_sql_db,
        vertex_count=args.vertices,
        edge_count=args.edges,
        vertex_props=vertex_props,
        edge_props=edge_props,
        string_size=args.string_size,
        batch_size=args.batch_size,
        async_parallel=args.async_parallel,
        heap_size=args.heap_size,
        vertex_type=vertex_type,
        edge_type=edge_type,
    )

    print("Running IMPORT DATABASE graph benchmark...")
    imp = run_import_database_graph_load(
        db_path=imp_db,
        vertices_csv_path=vertices_csv_path,
        edges_csv_path=edges_csv_path,
        batch_size=args.batch_size,
        heap_size=args.heap_size,
        parallel=args.parallel,
        vertex_type=vertex_type,
        edge_type=edge_type,
    )

    signatures = {
        "transactional": collect_graph_signature(
            txn_db,
            args.heap_size,
            vertex_type,
            edge_type,
            vertex_props,
            edge_props,
            args.vertices,
            args.edges,
        ),
        "graphbatch": collect_graph_signature(
            graph_batch_db,
            args.heap_size,
            vertex_type,
            edge_type,
            vertex_props,
            edge_props,
            args.vertices,
            args.edges,
        ),
        "async_sql": collect_graph_signature(
            async_sql_db,
            args.heap_size,
            vertex_type,
            edge_type,
            vertex_props,
            edge_props,
            args.vertices,
            args.edges,
        ),
        "import_database": collect_graph_signature(
            imp_db,
            args.heap_size,
            vertex_type,
            edge_type,
            vertex_props,
            edge_props,
            args.vertices,
            args.edges,
        ),
    }

    print("\n=== Results ===")
    print(f"Expected vertices: {args.vertices:,}")
    print(f"Expected edges:    {args.edges:,}")
    print(
        f"Transactional:     {txn['vertices']:,} vertices, {txn['edges']:,} edges "
        f"in {txn['seconds']:.3f}s"
    )
    print(
        f"GraphBatch:       {graph_batch['vertices']:,} vertices, "
        f"{graph_batch['edges']:,} edges in {graph_batch['seconds']:.3f}s"
    )
    print(
        "Async SQL:         "
        f"{async_sql['vertices']:,} vertices, {async_sql['edges']:,} edges "
        f"in {async_sql['seconds']:.3f}s"
    )
    print(
        f"IMPORT DATABASE:   {imp['vertices']:,} vertices, {imp['edges']:,} edges "
        f"in {imp['seconds']:.3f}s (errors={imp['errors']})"
    )
    print(
        f"IMPORT reported:    {imp['vertices_reported']:,} vertices, "
        f"{imp['edges_reported']:,} edges"
    )

    validate_parity_or_fail(args, txn, graph_batch, async_sql, imp)
    validate_graph_signatures_or_fail(signatures)

    print("Logical parity:    all four methods produced the same graph output")

    print("\nPerformance comparison:")
    print_faster_statement(
        "GraphBatch",
        graph_batch["seconds"],
        "Transactional",
        txn["seconds"],
    )
    print_faster_statement(
        "GraphBatch",
        graph_batch["seconds"],
        "Async SQL",
        async_sql["seconds"],
    )
    print_faster_statement(
        "GraphBatch",
        graph_batch["seconds"],
        "IMPORT DATABASE",
        imp["seconds"],
    )
    print_faster_statement(
        "Async SQL",
        async_sql["seconds"],
        "Transactional",
        txn["seconds"],
    )
    print_faster_statement(
        "IMPORT DATABASE",
        imp["seconds"],
        "Transactional",
        txn["seconds"],
    )
    print_faster_statement(
        "IMPORT DATABASE",
        imp["seconds"],
        "Async SQL",
        async_sql["seconds"],
    )

    print(f"\nArtifacts kept in: {work_dir}")


if __name__ == "__main__":
    main()
