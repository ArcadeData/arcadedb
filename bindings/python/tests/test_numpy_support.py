"""
Tests for NumPy support in ArcadeDB Python bindings.
"""

import arcadedb_embedded as arcadedb
import pytest

try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


@pytest.mark.skipif(not HAS_NUMPY, reason="NumPy not installed")
def test_numpy_array_conversion_in_command(temp_db):
    """Test automatic conversion of NumPy arrays in db.command()."""
    db = temp_db

    # Schema operations are auto-transactional
    db.schema.create_vertex_type("VectorData")
    db.schema.create_property("VectorData", "vector", "ARRAY_OF_FLOATS")

    # Create a NumPy array
    vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)

    # Insert using command with NumPy array
    with db.transaction():
        db.command("sql", "INSERT INTO VectorData SET vector = ?", vec)

    # Verify
    result = db.query("sql", "SELECT FROM VectorData").first()
    stored_vec = result.get("vector")

    # Check values (approximate for float)
    assert len(stored_vec) == 3
    assert abs(stored_vec[0] - 0.1) < 0.0001
    assert abs(stored_vec[1] - 0.2) < 0.0001
    assert abs(stored_vec[2] - 0.3) < 0.0001


@pytest.mark.skipif(not HAS_NUMPY, reason="NumPy not installed")
def test_numpy_array_conversion_in_query(temp_db):
    """Test automatic conversion of NumPy arrays in db.query()."""
    db = temp_db

    # Schema operations are auto-transactional
    db.schema.create_vertex_type("VectorData")
    db.schema.create_property("VectorData", "vector", "ARRAY_OF_FLOATS")

    # Insert data manually first
    with db.transaction():
        db.command("sql", "INSERT INTO VectorData SET vector = ?", [0.1, 0.2, 0.3])

    vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)

    # Ensure the call succeeds with NumPy array as parameter
    try:
        db.query("sql", "SELECT FROM VectorData WHERE vector = ?", vec)
    except Exception as e:
        pytest.fail(f"Query with NumPy array failed: {e}")


@pytest.mark.skipif(not HAS_NUMPY, reason="NumPy not installed")
def test_numpy_array_conversion_in_transaction(temp_db):
    """Test NumPy array conversion in regular transactions (no batch context)."""
    db = temp_db

    # Schema operations are auto-transactional
    db.schema.create_vertex_type("VectorData")
    db.schema.create_property("VectorData", "vector", "ARRAY_OF_FLOATS")
    db.schema.create_document_type("DocData")
    db.schema.create_property("DocData", "embedding", "ARRAY_OF_FLOATS")

    vec1 = np.array([0.1, 0.2, 0.3], dtype=np.float32)
    vec2 = np.array([0.4, 0.5, 0.6], dtype=np.float32)
    vec1_java = arcadedb.to_java_float_array(vec1)
    vec2_java = arcadedb.to_java_float_array(vec2)

    with db.transaction():
        vertex = db.new_vertex("VectorData")
        vertex.set("vector", vec1_java)
        vertex.save()

        doc = db.new_document("DocData")
        doc.set("embedding", vec2_java)
        doc.save()

    # Verify Vertex
    result = db.query("sql", "SELECT FROM VectorData").first()
    stored_vec = result.get("vector")
    assert len(stored_vec) == 3
    assert abs(stored_vec[0] - 0.1) < 0.0001

    # Verify Document
    result = db.query("sql", "SELECT FROM DocData").first()
    stored_emb = result.get("embedding")
    assert len(stored_emb) == 3
    assert abs(stored_emb[0] - 0.4) < 0.0001
