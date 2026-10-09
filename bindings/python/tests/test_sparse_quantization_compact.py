"""Sparse index weight precision and the settle step, plus the dense search
beam argument: three engine features the vector guide documents and the
benchmark harness depends on (guide/vectors.md, 2026-09-07)."""

import arcadedb_embedded as arcadedb
import jpype.types as jtypes
import pytest

DIMS = 512


def _sparse_db(db, quant):
    db.command("sql", "CREATE DOCUMENT TYPE SDoc")
    db.command("sql", "CREATE PROPERTY SDoc.id INTEGER")
    db.command("sql", "CREATE PROPERTY SDoc.tokens ARRAY_OF_INTEGERS")
    db.command("sql", "CREATE PROPERTY SDoc.weights ARRAY_OF_FLOATS")
    meta = (
        f'{{"dimensions": {DIMS}'
        + (f', "weightQuantization": "{quant}"' if quant else "")
        + "}"
    )
    db.command(
        "sql",
        f"CREATE INDEX ON SDoc (tokens, weights) LSM_SPARSE_VECTOR METADATA {meta}",
    )
    with db.transaction():
        for i in range(200):
            toks = sorted({(i * 7 + j * 13) % DIMS for j in range(8)})
            wts = [round(0.1 + ((i + j) % 10) / 10.0, 3) for j in range(len(toks))]
            db.command(
                "sql",
                f"INSERT INTO SDoc SET id = {i}, tokens = {toks}, weights = {wts}",
            )


def _neighbours(db, toks, wts, k=5):
    rows = db.query(
        "sql",
        "SELECT id FROM (SELECT expand(`vector.sparseNeighbors`('SDoc[tokens,weights]', ?, ?, ?)))",
        jtypes.JArray(jtypes.JInt)(toks),
        arcadedb.to_java_float_array(wts),
        k,
    ).to_list()
    return [int(r["id"]) for r in rows]


@pytest.mark.parametrize("quant", [None, "FP32"])
def test_sparse_weight_precision_and_compact(temp_db, quant):
    _sparse_db(temp_db, quant)
    toks = sorted({(3 * 7 + j * 13) % DIMS for j in range(8)})
    wts = [1.0] * len(toks)
    before = _neighbours(temp_db, toks, wts)
    assert len(before) == 5 and 3 in before
    # The settle step: synchronous, and the answer does not change.
    temp_db.command("sql", "COMPACT INDEX `SDoc[tokens,weights]`")
    after = _neighbours(temp_db, toks, wts)
    assert len(after) == 5 and 3 in after


def test_dense_search_beam_argument(temp_db):
    temp_db.command("sql", "CREATE DOCUMENT TYPE VDoc")
    temp_db.command("sql", "CREATE PROPERTY VDoc.id INTEGER")
    temp_db.command("sql", "CREATE PROPERTY VDoc.emb ARRAY_OF_FLOATS")
    # Distinct random vectors: the first version used ((i*(j+1)) % 17)/17, which
    # produced duplicates the engine dropped ("Building graph with 282 vectors" of
    # 300 in CI), so id 7 could be absent from every answer.
    import random

    _rnd = random.Random(7)  # nosec B311 - test data, not cryptography
    _vecs = {}
    with temp_db.transaction():
        for i in range(300):
            v = [round(_rnd.random(), 6) for _ in range(16)]
            _vecs[i] = v
            # A literal array on purpose: this test exercises the index over the vector as SQL stores a
            # literal list; a bound Python list takes a different conversion path.
            temp_db.command("sql", f"INSERT INTO VDoc SET id = {i}, emb = {v}")
    temp_db.command(
        "sql",
        'CREATE INDEX ON VDoc (emb) LSM_VECTOR METADATA {"dimensions": 16, "similarity": "COSINE"}',
    )
    q = _vecs[7]
    for beam in (16, 200):
        rows = temp_db.query(
            "sql",
            "SELECT id FROM (SELECT expand(vectorNeighbors('VDoc[emb]', ?, ?, ?)))",
            arcadedb.to_java_float_array(q),
            10,
            beam,
        ).to_list()
        assert len(rows) == 10, (beam, rows)
        if beam == 200:
            # The query IS vector 7; with a wide beam the exact match is in the top 10.
            assert 7 in [int(r["id"]) for r in rows], rows


def _scores_against_exact(db, type_name, meta):
    """Load one seeded corpus, compact (INT8 weights are written at segment
    flush), query, and return (score, exact dot product) per hit."""
    import numpy as np

    db.command("sql", f"CREATE DOCUMENT TYPE {type_name}")
    db.command("sql", f"CREATE PROPERTY {type_name}.id INTEGER")
    db.command("sql", f"CREATE PROPERTY {type_name}.tokens ARRAY_OF_INTEGERS")
    db.command("sql", f"CREATE PROPERTY {type_name}.weights ARRAY_OF_FLOATS")
    db.command(
        "sql",
        f"CREATE INDEX ON {type_name} (tokens, weights) LSM_SPARSE_VECTOR "
        f"METADATA {meta}",
    )
    rng = np.random.default_rng(1)
    docs = {}
    with db.transaction():
        for i in range(300):
            toks = np.sort(rng.choice(DIMS, 12, replace=False)).astype(np.int32)
            wts = rng.uniform(0.01, 3.0, 12).round(4).astype(np.float32)
            docs[i] = dict(zip(toks.tolist(), wts.tolist()))
            db.command(
                "sql",
                f"INSERT INTO {type_name} SET id = ?, tokens = ?, weights = ?",
                i,
                jtypes.JArray(jtypes.JInt)(toks),
                arcadedb.to_java_float_array(wts),
            )
    db.command("sql", f"COMPACT INDEX `{type_name}[tokens,weights]`")
    query = {t: 1.0 for t in list(docs[7])[:6]}
    rows = db.query(
        "sql",
        "SELECT id, score FROM (SELECT expand(`vector.sparseNeighbors`("
        f"'{type_name}[tokens,weights]', ?, ?, ?)))",  # nosec B608 - test type name
        jtypes.JArray(jtypes.JInt)(list(query)),
        arcadedb.to_java_float_array(list(query.values())),
        5,
    ).to_list()
    assert len(rows) == 5
    return [
        (
            float(r["score"]),
            sum(docs[int(r["id"])].get(t, 0.0) * w for t, w in query.items()),
        )
        for r in rows
    ]


def test_int8_scores_are_rescored_to_the_exact_dot_product(temp_db):
    # From 26.10.1 (ArcadeData/arcadedb#8576) the INT8 postings only pick the
    # candidates and the records' own weights score them, so the index returns
    # the exact dot product; "rescoreOversample": 0 turns that off and the
    # quantized score shows again. That second index is what proves the
    # postings went through INT8 at all: before 26.10.1, compact() left a
    # corpus this small in the in-memory table, unquantized, and every score
    # was exact for that reason (on 26.9.1 this test fails at the metadata key).
    def worst(pairs):
        return max(abs(s - e) / e for s, e in pairs)

    int8 = f'"dimensions": {DIMS}, "weightQuantization": "INT8"'
    rescored = _scores_against_exact(temp_db, "SDocR", "{" + int8 + "}")
    assert worst(rescored) < 1e-5
    quantized = _scores_against_exact(
        temp_db, "SDocQ", "{" + int8 + ', "rescoreOversample": 0}'
    )
    assert worst(quantized) > 1e-5
