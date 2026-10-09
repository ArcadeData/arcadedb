#!/usr/bin/env python3
"""Example 26: Cross-Model Transaction Atomicity.

One operation that touches three models: a vector search finds the nearest
products, a graph hop expands to the products related to the best hit, and a
document update bumps a counter on all of them. In ArcadeDB the three run in
ONE transaction, so an interruption between the writes leaves nothing behind.
A stack composed of separate engines has no transaction spanning them, and the
same interruption leaves the counters half-updated, which is what the project
page measures as "torn results".

This example builds a small synthetic catalogue, runs the operation cleanly,
then injects a failure between the writes N times in two ways: inside a
transaction (rolled back, never torn) and without one (each write committed
on its own, torn every time). Standard library plus the wheel; no data
download.
"""

from __future__ import annotations

import argparse
import random
import shutil
import time

import arcadedb_embedded as arcadedb

DIM = 16
K = 5


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--products", type=int, default=2_000)
    p.add_argument("--trials", type=int, default=20)
    p.add_argument("--db-path", default="./my_test_databases/cross_model_atomicity")
    return p.parse_args()


def build(db, n: int) -> None:
    rnd = random.Random(20260908)  # nosec B311 - synthetic data
    for ddl in (
        "CREATE VERTEX TYPE Product",
        "CREATE PROPERTY Product.pid INTEGER",
        "CREATE PROPERTY Product.views INTEGER",
        "CREATE PROPERTY Product.embedding ARRAY_OF_FLOATS",
        "CREATE INDEX ON Product (pid) UNIQUE_HASH",  # pid: equality only (#9169)
        "CREATE EDGE TYPE RELATED",
    ):
        db.command("sql", ddl)
    with db.transaction():
        for i in range(n):
            emb = [round(rnd.random(), 5) for _ in range(DIM)]
            db.command(
                "sql",
                "CREATE VERTEX Product SET pid = ?, views = 0, embedding = ?",
                i,
                arcadedb.to_java_float_array(emb),
            )
    with db.transaction():
        for i in range(n):
            for f in (1, 2, 3):
                j = (i + f * 7919) % n
                db.command(
                    "sql",
                    "CREATE EDGE RELATED FROM (SELECT FROM Product WHERE pid = :src) "
                    "TO (SELECT FROM Product WHERE pid = :dst)",
                    {"src": i, "dst": j},
                )
    db.command(
        "sql",
        'CREATE INDEX ON Product (embedding) LSM_VECTOR METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}'
        % DIM,
    )


def touched_products(db, qvec: list[float]) -> list[int]:
    rows = db.query(
        "sql",
        "SELECT pid FROM (SELECT expand(vectorNeighbors('Product[embedding]', ?, ?)))",
        arcadedb.to_java_float_array(qvec),
        K,
    ).to_list()
    pids = [int(r["pid"]) for r in rows]
    rel = db.query(
        "sql", "SELECT expand(out('RELATED')) FROM Product WHERE pid = ?", pids[0]
    ).to_list()
    return sorted(set(pids[:3] + [int(r["pid"]) for r in rel[:3]]))


def hybrid_op(
    db, qvec: list[float], *, crash_after: int | None = None, transactional: bool = True
) -> int:
    """Search, expand, bump. With crash_after=k the operation raises after k
    of its writes, which is the interruption the page measures."""
    pids = touched_products(db, qvec)

    def one_write(p):
        db.command("sql", "UPDATE Product SET views = views + 1 WHERE pid = ?", p)

    if transactional:
        with db.transaction():  # rolls back on the exception
            for n, p in enumerate(pids, 1):
                one_write(p)
                if crash_after is not None and n == crash_after:
                    raise RuntimeError("injected failure between the writes")
    else:
        # The composed-stack shape: every write is its own commit, nothing
        # spans them, so the ones before the failure stay.
        for n, p in enumerate(pids, 1):
            with db.transaction():
                one_write(p)
            if crash_after is not None and n == crash_after:
                raise RuntimeError("injected failure between the writes")
    return len(pids)


def total_views(db) -> int:
    return int(
        db.query("sql", "SELECT sum(views) AS s FROM Product").to_list()[0]["s"] or 0
    )


def main() -> None:
    args = parse_args()
    shutil.rmtree(args.db_path, ignore_errors=True)
    rnd = random.Random(7)  # nosec B311 - synthetic queries
    with arcadedb.create_database(args.db_path) as db:
        t0 = time.perf_counter()
        build(db, args.products)
        print(
            f"built {args.products:,} products with {3 * args.products:,} RELATED edges and a vector index in {time.perf_counter() - t0:.1f}s"
        )

        queries = [[rnd.random() for _ in range(DIM)] for _ in range(args.trials)]
        lat = []
        for q in queries:
            t0 = time.perf_counter()
            hybrid_op(db, q)
            lat.append((time.perf_counter() - t0) * 1000)
        lat.sort()
        print(
            f"clean operations: {len(lat)}, p50 {lat[len(lat) // 2]:.2f} ms, total views now {total_views(db):,}"
        )

        for label, transactional in (
            ("one transaction", True),
            ("no shared transaction", False),
        ):
            torn = 0
            for q in queries:
                before = total_views(db)
                try:
                    hybrid_op(db, q, crash_after=2, transactional=transactional)
                except RuntimeError:
                    pass  # the injected failure; what matters is the state left behind
                torn += int(total_views(db) != before)
            print(
                f"{label:22}: interrupted {len(queries)} times, torn {torn} of {len(queries)}"
            )
    print(f"database kept at {args.db_path}")


if __name__ == "__main__":
    main()
