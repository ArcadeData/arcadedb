"""The project page's cross-model story, at test size: search, hop and update
in one transaction survive an interruption between the writes with nothing
torn; the same writes without a transaction are torn every time."""

import random

import arcadedb_embedded as arcadedb

DIM = 8


def _build(db, n=200):
    rnd = random.Random(3)  # nosec B311
    for ddl in (
        "CREATE VERTEX TYPE Product",
        "CREATE PROPERTY Product.pid INTEGER",
        "CREATE PROPERTY Product.views INTEGER",
        "CREATE PROPERTY Product.embedding ARRAY_OF_FLOATS",
        "CREATE INDEX ON Product (pid) UNIQUE",
        "CREATE EDGE TYPE RELATED",
    ):
        db.command("sql", ddl)
    with db.transaction():
        for i in range(n):
            emb = [round(rnd.random(), 5) for _ in range(DIM)]
            db.command(
                "sql",
                f"CREATE VERTEX Product SET pid = {i}, views = 0, embedding = {emb}",
            )
    with db.transaction():
        for i in range(n):
            db.command(
                "sql",
                f"CREATE EDGE RELATED FROM (SELECT FROM Product WHERE pid = {i}) "  # nosec B608
                f"TO (SELECT FROM Product WHERE pid = {(i + 7) % n})",
            )
    db.command(
        "sql",
        'CREATE INDEX ON Product (embedding) LSM_VECTOR METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}'
        % DIM,
    )


def _views(db):
    return int(
        db.query("sql", "SELECT sum(views) AS s FROM Product").to_list()[0]["s"] or 0
    )


def _op(db, q, transactional, crash_after=2):
    rows = db.query(
        "sql",
        "SELECT pid FROM (SELECT expand(vectorNeighbors('Product[embedding]', ?, ?)))",
        arcadedb.to_java_float_array(q),
        3,
    ).to_list()
    pids = [int(r["pid"]) for r in rows]
    rel = db.query(
        "sql",
        f"SELECT expand(out('RELATED')) FROM Product WHERE pid = {pids[0]}",  # nosec B608
    ).to_list()
    touched = sorted(set(pids + [int(r["pid"]) for r in rel]))

    def one_write(p):
        db.command(
            "sql", f"UPDATE Product SET views = views + 1 WHERE pid = {p}"  # nosec B608
        )  # nosec B608

    if transactional:
        with db.transaction():
            for n, p in enumerate(touched, 1):
                one_write(p)
                if n == crash_after:
                    raise RuntimeError("injected")
    else:
        for n, p in enumerate(touched, 1):
            with db.transaction():
                one_write(p)
            if n == crash_after:
                raise RuntimeError("injected")


def test_one_transaction_is_never_torn_and_no_transaction_always_is(temp_db):
    _build(temp_db)
    rnd = random.Random(11)  # nosec B311
    queries = [[rnd.random() for _ in range(DIM)] for _ in range(5)]
    for transactional, expect_torn in ((True, 0), (False, 5)):
        torn = 0
        for q in queries:
            before = _views(temp_db)
            try:
                _op(temp_db, q, transactional)
            except RuntimeError:
                pass  # the injected interruption
            torn += int(_views(temp_db) != before)
        assert torn == expect_torn, (transactional, torn)
