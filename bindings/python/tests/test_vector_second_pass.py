"""A repeated query set returns the same neighbours as its first pass: the
warm second pass the project page reports is a cache effect, not a different
answer."""

import random

import arcadedb_embedded as arcadedb


def test_second_pass_returns_identical_neighbours(temp_db):
    rnd = random.Random(5)  # nosec B311
    temp_db.command("sql", "CREATE DOCUMENT TYPE V")
    temp_db.command("sql", "CREATE PROPERTY V.id INTEGER")
    temp_db.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS")
    with temp_db.transaction():
        for i in range(500):
            temp_db.command(
                "sql",
                f"INSERT INTO V SET id = {i}, emb = {[round(rnd.random(), 5) for _ in range(16)]}",
            )
    temp_db.command(
        "sql",
        'CREATE INDEX ON V (emb) LSM_VECTOR METADATA {"dimensions": 16, "similarity": "COSINE"}',
    )
    queries = [[rnd.random() for _ in range(16)] for _ in range(10)]

    def run():
        out = []
        for q in queries:
            rows = temp_db.query(
                "sql",
                "SELECT id FROM (SELECT expand(vectorNeighbors('V[emb]', ?, ?)))",
                arcadedb.to_java_float_array(q),
                5,
            ).to_list()
            out.append([int(r["id"]) for r in rows])
        return out

    first, second = run(), run()
    assert first == second
    assert all(len(h) == 5 for h in first)
