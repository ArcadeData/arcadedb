"""The hybrid model: vectors written after a build are searchable before any rebuild.

LSM_VECTOR does not patch a built graph. A write queues into an in-memory
delta buffer -- the engine's own comment says it is "Skipping expensive
O(log n) HNSW graph inserts" -- and the graph is only updated when pending
mutations cross max(100, min(graphSize * 0.2, 50_000)). Between those points
the query path runs `mergeWithDeltaScan`: the graph's approximate results,
plus an EXHAUSTIVE scan of the buffer, merged and deduped, with tombstoned
entries filtered out.

That design is why the October dense table can say what it says about
mutation cost. It rests on two properties:

  CORRECTNESS  a vector in the buffer is found exactly, because brute force
               is exact -- not "eventually", not "after a rebuild".
  COST         the buffer is scanned per query, so the work is deferred into
               a per-query tax rather than skipped.

Only CORRECTNESS is asserted, and only below the rebuild threshold, the one
place the delta path can be told apart: above it a rebuild fires and a
passing search proves nothing about the delta path. COST is not measured:
when the index reports its counters, the test checks only that no rebuild
fired, the graph node count did not change, and the delta buffer is not empty.
"""

import math as _math
import random as _random

import arcadedb_embedded as arcadedb
import pytest

DIM = 8
BASE = 300  # threshold = max(100, min(300*0.2, 50_000)) = 100
ADDED = 50  # deliberately under it, so no rebuild can fire


def _vec(i):
    """A deterministic UNIT vector whose direction is unique to `i`.

    The obvious corpus -- dimension 0 carrying the id -- is degenerate here
    and cost me a wrong conclusion before this comment existed: the index is
    DOT_PRODUCT, so [19, 0, ...] and [320, 0, ...] are PARALLEL and score
    identically. Querying for one returned the other at distance 0.0, which
    looks exactly like a delta scan missing its entry and is nothing of the
    kind. Distinct directions, and unit length because DOT_PRODUCT documents
    unit vectors as its expectation and warns otherwise.
    """
    rnd = _random.Random(i)  # nosec B311 - deterministic test corpus, not security
    v = [rnd.uniform(-1.0, 1.0) for _ in range(DIM)]
    norm = _math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / norm for x in v]


@pytest.fixture
def db(tmp_path):
    d = arcadedb.create_database(str(tmp_path / "delta_visibility"))
    yield d
    d.drop()


def _stats(index):
    try:
        return index.get_stats() or {}
    except Exception:  # an engine build without the counters
        return {}


def test_delta_vectors_are_searchable_before_any_rebuild(db):
    db.command("sql", "CREATE VERTEX TYPE Doc")
    db.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS")
    with db.transaction():
        for i in range(BASE):
            db.command(
                "sql",
                "INSERT INTO Doc SET vid = ?, embedding = ?",
                i,
                arcadedb.to_java_float_array(_vec(i)),
            )
    index = db.create_vector_index("Doc", "embedding", dimensions=DIM)

    before = _stats(index)
    rebuilds_before = before.get("graphRebuildCount")
    nodes_before = before.get("graphNodeCount")

    # Write ADDED new vectors. Under the threshold, so the graph must not move.
    with db.transaction():
        for i in range(BASE, BASE + ADDED):
            db.command(
                "sql",
                "INSERT INTO Doc SET vid = ?, embedding = ?",
                i,
                arcadedb.to_java_float_array(_vec(i)),
            )

    after = _stats(index)
    if after.get("graphRebuildCount") is not None and rebuilds_before is not None:
        assert after["graphRebuildCount"] == rebuilds_before, (
            "a rebuild fired below the threshold, so this test no longer "
            "exercises the delta path it exists for"
        )
    if after.get("graphNodeCount") is not None and nodes_before is not None:
        assert after["graphNodeCount"] == nodes_before, (
            "the graph absorbed the writes; LSM_VECTOR is expected to buffer "
            "them until a rebuild"
        )
    if after.get("deltaVectorsCount") is not None:
        assert (
            after["deltaVectorsCount"] > 0
        ), "writes reached neither the graph nor the delta buffer"

    # CORRECTNESS: every buffered vector is its own nearest neighbour.
    for i in range(BASE, BASE + ADDED):
        hits = index.find_nearest(_vec(i), k=1)
        assert hits, f"vector {i} is in the buffer and was not returned at all"
        # find_nearest returns [(record, score), ...]
        record = hits[0][0]
        got = record.get("vid")
        assert got == i, (
            f"buffered vector {i} was not found exactly; the delta scan is "
            f"meant to be brute force, which cannot miss. Got {got!r}"
        )


def test_deleted_vectors_are_filtered_from_delta_results(db):
    """A tombstoned vector must not come back, buffer or no buffer."""
    db.command("sql", "CREATE VERTEX TYPE Doc")
    db.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS")
    with db.transaction():
        for i in range(BASE):
            db.command(
                "sql",
                "INSERT INTO Doc SET vid = ?, embedding = ?",
                i,
                arcadedb.to_java_float_array(_vec(i)),
            )
    index = db.create_vector_index("Doc", "embedding", dimensions=DIM)

    victim = BASE - 1
    with db.transaction():
        db.command("sql", "DELETE FROM Doc WHERE vid = ?", victim)

    hits = index.find_nearest(_vec(victim), k=5)
    ids = [rec.get("vid") for rec, _score in (hits or ())]
    assert victim not in ids, (
        f"deleted vector {victim} was returned; mergeWithDeltaScan filters "
        f"tombstones via isDeleted() and this asserts it stays that way"
    )
