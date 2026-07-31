"""Example 11 compares ArcadeDB against hnswlib-derived vector backends.

ArcadeDB applies maxConnections verbatim to every graph layer. hnswlib-derived
indexes (faiss, lancedb, pgvector, qdrant, milvus) allocate 2*M links at the
base layer. Passing one number to both families unchanged makes the benchmark
compare different graph densities, so the example converts between them.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

EXAMPLE_PATH = (
    Path(__file__).resolve().parents[1] / "examples" / "11_vector_index_build.py"
)

pytestmark = pytest.mark.skipif(
    not EXAMPLE_PATH.exists(),
    reason="bindings/python/examples/11_vector_index_build.py is not present",
)


@pytest.fixture(scope="module")
def example11():
    spec = importlib.util.spec_from_file_location("example11", EXAMPLE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_hnsw_m_is_half_the_vamana_degree(example11):
    assert example11.hnsw_m_from_max_connections(32) == 16
    assert example11.hnsw_m_from_max_connections(64) == 32


def test_hnsw_m_never_drops_below_one(example11):
    assert example11.hnsw_m_from_max_connections(1) == 1
    assert example11.hnsw_m_from_max_connections(0) == 1


def test_hnsw_m_accepts_string_input(example11):
    assert example11.hnsw_m_from_max_connections("32") == 16
