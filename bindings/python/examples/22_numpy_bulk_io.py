#!/usr/bin/env python3
"""Example 22: numpy in, numpy out - bulk I/O paths.

Every path in this example crosses the Python/Java boundary once per BATCH
instead of once per value, which is what makes them fast: per-row FFI calls
cap document ingest around 30k rows/s regardless of engine speed, while the
batched paths below reach hundreds of thousands of rows or points per
second on the same machine.

Workflow covered:
- bulk document ingest with Database.insert_many (rows as one JSON batch)
- the same rows through the async parallel writers (insert_many parallel=True)
- time-series ingest straight from numpy arrays via
  AsyncExecutor.append_samples (buffer-protocol bulk copy per column)
- time-bucketed aggregation over the native TIMESERIES type
- columnar export with to_columns(): scalar columns as 1-D numpy arrays and
  embedding columns as a contiguous 2-D array, ready for scikit-learn or
  faiss without per-row conversion
- the same export via to_arrow(), which keeps NULLs typed instead of
  widening a nullable INTEGER to float64/NaN the way numpy must

Requirements:
- numpy (pip install numpy)
- pyarrow, optional, only for the to_arrow section
  (pip install 'arcadedb-embedded[arrow]')
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import arcadedb_embedded as arcadedb
import numpy as np


def bulk_documents(db, n_rows: int) -> None:
    print(f"\n=== insert_many: {n_rows:,} order documents ===")
    db.command("sql", "CREATE DOCUMENT TYPE BulkOrder")
    rows = [
        {
            "oid": i,
            "customer": f"cust_{i % 997}",
            "amount": round((i * 37) % 100_000 / 100.0, 2),
            "status": ("new", "paid", "shipped")[i % 3],
        }
        for i in range(n_rows)
    ]
    t0 = time.perf_counter()
    inserted = db.insert_many("BulkOrder", rows, commit_every=10_000)
    dt = time.perf_counter() - t0
    print(
        f"synchronous batches: {inserted:,} rows in {dt:.2f}s "
        f"({inserted / dt:,.0f} rows/s)"
    )

    db.command("sql", "CREATE DOCUMENT TYPE BulkOrderP")
    t0 = time.perf_counter()
    inserted = db.insert_many("BulkOrderP", rows, parallel=True)
    dt = time.perf_counter() - t0
    print(
        f"async parallel writers: {inserted:,} rows in {dt:.2f}s "
        f"({inserted / dt:,.0f} rows/s)"
    )


def timeseries_from_numpy(db, n_points: int) -> None:
    print(f"\n=== append_samples: {n_points:,} sensor points from numpy ===")
    db.command(
        "sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts "
        "TAGS (host STRING) FIELDS (cpu DOUBLE, mem DOUBLE) SHARDS 4",
    )
    base_ms = 1_767_225_600_000  # 2026-01-01T00:00:00Z
    ts = base_ms + np.arange(n_points, dtype=np.int64) * 1_000
    cpu = 50.0 + 40.0 * np.sin(np.arange(n_points) / 300.0)
    mem = np.random.default_rng(7).uniform(20.0, 90.0, n_points)
    hosts = [f"host_{i % 8}" for i in range(n_points)]

    ex = db.async_executor()
    t0 = time.perf_counter()
    ex.append_samples("Sensor", ts, hosts, cpu, mem)
    ex.wait_completion()
    dt = time.perf_counter() - t0
    print(f"ingested {n_points:,} points in {dt:.2f}s " f"({n_points / dt:,.0f} pts/s)")

    buckets = db.query(
        "sql",
        "SELECT ts.timeBucket('1m', ts) AS minute, avg(cpu) AS cpu "
        f"FROM Sensor WHERE host = 'host_3' AND ts BETWEEN {base_ms} AND "
        f"{base_ms + 600_000 - 1} GROUP BY minute ORDER BY minute",
    ).to_list()
    print(
        f"first 10 minutes of host_3, per-minute avg cpu: "
        f"{[round(float(b['cpu']), 1) for b in buckets]}"
    )


def embeddings_to_numpy(db, n_vectors: int, dim: int) -> None:
    print(f"\n=== to_columns: {n_vectors:,} x {dim} embeddings out ===")
    db.command("sql", "CREATE DOCUMENT TYPE Chunk")
    db.command("sql", "CREATE PROPERTY Chunk.cid INTEGER")
    db.command("sql", "CREATE PROPERTY Chunk.embedding ARRAY_OF_FLOATS")
    rng = np.random.default_rng(42)
    vecs = rng.standard_normal((n_vectors, dim), dtype=np.float32)
    with db.transaction():
        for i in range(n_vectors):
            db.command(
                "sql",
                "INSERT INTO Chunk SET cid = :c, embedding = :e",
                {"c": i, "e": arcadedb.to_java_float_array(vecs[i])},
            )

    t0 = time.perf_counter()
    cols = db.query("sql", "SELECT cid, embedding FROM Chunk ORDER BY cid").to_columns()
    dt = time.perf_counter() - t0
    emb = cols["embedding"]
    print(
        f"exported in {dt:.3f}s: cid -> {cols['cid'].dtype} "
        f"{cols['cid'].shape}, embedding -> {emb.dtype} {emb.shape}"
    )
    # the array is contiguous and immediately usable, e.g. cosine vs a query
    q = vecs[0]
    sims = emb @ q / (np.linalg.norm(emb, axis=1) * np.linalg.norm(q))
    print(f"nearest to chunk 0 by cosine: {np.argsort(-sims)[:5].tolist()}")
    assert np.allclose(emb[:3], vecs[:3], atol=1e-6)


def nulls_to_arrow(db, n_rows: int) -> None:
    print(f"\n=== to_arrow vs to_columns: {n_rows:,} rows with NULLs ===")
    db.command("sql", "CREATE DOCUMENT TYPE Reading")
    db.command("sql", "CREATE PROPERTY Reading.rid INTEGER")
    db.command("sql", "CREATE PROPERTY Reading.count INTEGER")
    db.command("sql", "CREATE PROPERTY Reading.ok BOOLEAN")
    # Every third row leaves count and ok unset, which is the case that
    # separates the two export paths.
    rows = [
        {"rid": i, "count": i * 2, "ok": i % 2 == 0} if i % 3 else {"rid": i}
        for i in range(n_rows)
    ]
    db.insert_many("Reading", rows)

    q = "SELECT rid, count, ok FROM Reading ORDER BY rid"

    # to_columns() has no way to say "missing" inside a plain numpy array, so a
    # nullable INTEGER widens to float64 with NaN holes and a nullable BOOLEAN
    # falls back to a Python list. That is a property of numpy, not a defect.
    cols = db.query("sql", q).to_columns()
    print(
        f"to_columns : count -> {cols['count'].dtype}, "
        f"ok -> {type(cols['ok']).__name__}"
    )

    table = db.query("sql", q).to_arrow()
    if table is None:
        print(
            "to_arrow   : pyarrow not installed, skipping "
            "(pip install 'arcadedb-embedded[arrow]')"
        )
        return

    print(
        f"to_arrow   : count -> {table['count'].type}, "
        f"ok -> {table['ok'].type}, {table.num_rows:,} rows"
    )

    # The integers stay integers and the nulls stay null, so downstream
    # arithmetic is not silently done in floating point.
    assert table["count"].type == "int64"
    assert table["ok"].type == "bool"
    expected_nulls = len(range(0, n_rows, 3))
    assert table["count"].null_count == expected_nulls
    assert table["ok"].null_count == expected_nulls
    assert np.isnan(cols["count"]).sum() == expected_nulls
    print(
        f"both agree on {expected_nulls:,} missing values; "
        f"only to_arrow keeps them typed"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="./my_test_databases/numpy_bulk_io")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--points", type=int, default=500_000)
    parser.add_argument("--vectors", type=int, default=20_000)
    parser.add_argument("--dim", type=int, default=64)
    args = parser.parse_args()

    path = Path(args.db_path)
    if path.exists():
        shutil.rmtree(path)
    db = arcadedb.create_database(str(path))
    try:
        bulk_documents(db, args.rows)
        timeseries_from_numpy(db, args.points)
        embeddings_to_numpy(db, args.vectors, args.dim)
        nulls_to_arrow(db, args.rows // 20)
    finally:
        db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
