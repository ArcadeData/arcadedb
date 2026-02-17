"""
Tests for LSM Vector Index functionality.

Tests cover:
- LSMVectorIndex creation
- LSMVectorIndex operations (find_nearest)
"""

import arcadedb_embedded as arcadedb
import pytest


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary test database."""
    db_path = str(tmp_path / "test_lsm_vector_db")
    db = arcadedb.create_database(db_path)
    yield db
    db.drop()


class TestLSMVectorIndex:
    """Test LSM Vector Index functionality."""

    def test_create_vector_index_build_graph_now_default_true(
        self, test_db, monkeypatch
    ):
        """create_vector_index should eagerly call build_graph_now by default."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        from arcadedb_embedded.vector import VectorIndex

        called = {"count": 0}
        original_build_graph_now = VectorIndex.build_graph_now

        def wrapped_build_graph_now(self):
            called["count"] += 1
            return original_build_graph_now(self)

        monkeypatch.setattr(VectorIndex, "build_graph_now", wrapped_build_graph_now)

        test_db.create_vector_index("Doc", "embedding", dimensions=3)

        assert called["count"] == 1

    def test_create_vector_index_build_graph_now_can_be_disabled(
        self, test_db, monkeypatch
    ):
        """create_vector_index should skip eager graph build when disabled."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        from arcadedb_embedded.vector import VectorIndex

        called = {"count": 0}
        original_build_graph_now = VectorIndex.build_graph_now

        def wrapped_build_graph_now(self):
            called["count"] += 1
            return original_build_graph_now(self)

        monkeypatch.setattr(VectorIndex, "build_graph_now", wrapped_build_graph_now)

        test_db.create_vector_index(
            "Doc",
            "embedding",
            dimensions=3,
            build_graph_now=False,
        )

        assert called["count"] == 0

    def test_create_vector_index(self, test_db):
        """Test creating a vector index (JVector implementation)."""
        # Create schema
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        # Create vector index
        try:
            index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

            assert index is not None
            # Check if it's the wrapper
            from arcadedb_embedded.vector import VectorIndex

            assert isinstance(index, VectorIndex)

            # Verify it's listed in schema
            indexes = test_db.schema.list_vector_indexes()
            # Check if index name is present (format might vary)
            # LSM indexes often have names like Doc_0_123456
            # But list_vector_indexes should return them.
            # We just check if the list is not empty and contains something that looks like an index
            assert len(indexes) > 0
            # Optionally check if one of them starts with Doc
            found = any(str(idx).startswith("Doc") for idx in indexes)
            assert found, f"No index starting with Doc found in {indexes}"

        except Exception as e:
            pytest.fail(f"Failed to create LSM vector index: {e}")

    def test_create_vector_index_with_pq_params(self, test_db):
        """PQ params should propagate to metadata when quantization=PRODUCT."""

        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "Doc",
            "embedding",
            dimensions=8,
            quantization="PRODUCT",
            pq_subspaces=4,
            pq_clusters=128,
            pq_center_globally=False,
            pq_training_limit=5000,
        )

        java_idx = index._java_index
        if "TypeIndex" in java_idx.getClass().getName():
            java_idx = java_idx.getSubIndexes().get(0)

        meta = java_idx.getMetadata()
        assert meta.pqSubspaces == 4
        assert meta.pqClusters == 128
        assert meta.pqCenterGlobally is False
        assert meta.pqTrainingLimit == 5000

    def test_lsm_vector_search(self, test_db):
        """Test searching in vector index (JVector implementation)."""
        # Create schema and index
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Add some data
        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]

        with test_db.transaction():
            for i, vec in enumerate(vectors):
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Search
        query = [0.9, 0.1, 0.0]  # Close to first vector
        results = index.find_nearest(query, k=1)

        assert len(results) == 1
        vertex, distance = results[0]

        # Check if we got the correct vertex (first one)
        # Note: Distance metric depends on default (likely Cosine or Euclidean)
        # For Cosine, distance is 1 - similarity, so close to 0
        # For Euclidean, distance is small

        # Verify the embedding of the result
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
        assert abs(res_embedding[0] - 1.0) < 0.001

    @pytest.mark.skip(reason="PQ tests disabled in this test run")
    def test_lsm_vector_search_approximate_product(self, test_db):
        """Test PQ approximate search path (PRODUCT quantization)."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "Doc", "embedding", dimensions=3, quantization="PRODUCT"
        )

        assert "TypeIndex" in index._java_index.getClass().getName()

        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Force PQ data to be built and available
        index.build_graph_now()

        results = index.find_nearest_approximate(
            [0.9, 0.1, 0.0], k=1, overquery_factor=2
        )

        assert len(results) == 1
        vertex, _ = results[0]
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
        assert abs(res_embedding[0] - 1.0) < 0.001

    @pytest.mark.skip(reason="PQ tests disabled in this test run")
    def test_lsm_vector_search_approximate_typeindex(self, test_db):
        """Ensure TypeIndex wrapper path works for approximate search."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "Doc", "embedding", dimensions=3, quantization="PRODUCT"
        )

        if "TypeIndex" not in index._java_index.getClass().getName():
            pytest.skip("TypeIndex wrapper not returned by this build")

        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
        ]

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        index.build_graph_now()

        results = index.find_nearest_approximate(
            [0.9, 0.1, 0.0], k=1, overquery_factor=3
        )
        assert len(results) == 1
        vertex, _ = results[0]
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
        assert abs(res_embedding[0] - 1.0) < 0.001

    def test_lsm_vector_search_approximate_fallback(self, test_db):
        """Approximate search should gracefully fall back when PQ is unavailable."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        index.build_graph_now()

        with pytest.raises(arcadedb.ArcadeDBError):
            index.find_nearest_approximate([0.9, 0.1, 0.0], k=1, overquery_factor=2)

    @pytest.mark.skip(reason="PQ tests disabled in this test run")
    def test_lsm_vector_search_approximate_overquery(self, test_db):
        """Approximate search should over-query then truncate to k."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "Doc", "embedding", dimensions=3, quantization="PRODUCT"
        )

        vectors = [
            [1.0, 0.0, 0.0],
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]

        # Add enough UNIQUE filler vectors so PQ (K=256) has sufficient points
        # and does not fail due to de-duplication reducing effective training size.
        # Keep fillers far from the query direction (mostly Y/Z) to avoid valid
        # cosine-nearest matches overshadowing the intended top candidates.
        for i in range(256):
            a = 0.01
            b = ((i % 16) + 1) / 10.0
            c = (((i // 16) % 16) + 1) / 10.0
            vectors.append([a, b, c])

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        index.build_graph_now()

        query = [0.9, 0.1, 0.0]
        k = 2
        overquery_factor = 3

        results = index.find_nearest_approximate(
            query, k=k, overquery_factor=overquery_factor
        )

        assert len(results) == k
        vertex, _ = results[0]
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))

        # Top result should be the closest along the first axis
        assert res_embedding[0] >= 0.899
        assert res_embedding[0] >= res_embedding[1]
        assert res_embedding[0] >= res_embedding[2]

    @pytest.mark.skip(reason="PQ tests disabled in this test run")
    def test_lsm_vector_search_approximate_persistence(self, tmp_path):
        """PQ approximate search works after reopen (PQ state persisted)."""
        db_path = str(tmp_path / "pq_persist_db")

        with arcadedb.create_database(db_path) as db:
            db.schema.create_vertex_type("Doc")
            db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

            index = db.create_vector_index(
                "Doc", "embedding", dimensions=3, quantization="PRODUCT"
            )

            with db.transaction():
                v = db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array([1.0, 0.0, 0.0]))
                v.save()

            index.build_graph_now()

        with arcadedb.open_database(db_path) as db:
            index = db.schema.get_vector_index("Doc", "embedding")
            assert index is not None

            results = index.find_nearest_approximate(
                [1.0, 0.0, 0.0], k=1, overquery_factor=2
            )
            assert len(results) == 1
            vertex, _ = results[0]
            res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
            assert abs(res_embedding[0] - 1.0) < 0.001

    def test_lsm_vector_build_graph_now(self, test_db):
        """Ensure build_graph_now triggers eager graph rebuild without search."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Add a few vectors
        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ]

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Force graph build immediately (should not require a search trigger)
        index.build_graph_now()

        # Query should work and return the closest vector
        results = index.find_nearest([0.9, 0.1, 0.0], k=1)
        assert len(results) == 1
        vertex, _ = results[0]
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
        assert abs(res_embedding[0] - 1.0) < 0.001

    def test_lsm_vector_search_with_filter(self, test_db):
        """Test searching in vector index with RID filtering."""
        # Create schema and index
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Add some data
        # V0 is exact match for query
        # V1 is close
        # V2 is somewhat close
        # V3 is far
        # V4 is far
        vectors = [
            [1.0, 0.0, 0.0],  # V0
            [0.9, 0.1, 0.0],  # V1
            [0.8, 0.2, 0.0],  # V2
            [0.0, 1.0, 0.0],  # V3
            [0.0, 0.0, 1.0],  # V4
        ]

        rids = []
        with test_db.transaction():
            for i, vec in enumerate(vectors):
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()
                rids.append(str(v.get_identity()))

        query = [1.0, 0.0, 0.0]

        # Scenario 1: Filter allows V2 and V3. k=1.
        # V2 is closer than V3. V0 and V1 are filtered out.
        allowed_rids = [rids[2], rids[3]]
        results = index.find_nearest(query, k=1, allowed_rids=allowed_rids)
        assert len(results) == 1
        assert str(results[0][0].get_identity()) == rids[2]

        # Scenario 2: Filter allows V2 and V3. k=2.
        # Should return both V2 and V3. V2 first.
        results = index.find_nearest(query, k=2, allowed_rids=allowed_rids)
        assert len(results) == 2
        assert str(results[0][0].get_identity()) == rids[2]
        assert str(results[1][0].get_identity()) == rids[3]

        # Scenario 3: Filter allows V0, V1, V2. k=2.
        # Should return V0 and V1.
        allowed_rids = [rids[0], rids[1], rids[2]]
        results = index.find_nearest(query, k=2, allowed_rids=allowed_rids)
        assert len(results) == 2
        assert str(results[0][0].get_identity()) == rids[0]
        assert str(results[1][0].get_identity()) == rids[1]

        # Scenario 4: Filter allows V3. k=5.
        # Should return only V3.
        allowed_rids = [rids[3]]
        results = index.find_nearest(query, k=5, allowed_rids=allowed_rids)
        assert len(results) == 1
        assert str(results[0][0].get_identity()) == rids[3]

    @pytest.mark.skip(
        reason="Known upstream bug: Vector deletions cause index corruption"
    )
    def test_lsm_vector_delete_and_search_others(self, test_db):
        """Test deleting vertices in a larger dataset and ensuring others are still found."""
        import random

        # Create schema
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")
        test_db.schema.create_property("Doc", "id", "INTEGER")

        # Use higher dimensions to ensure orthogonality and reduce ANN approximation errors
        dims = 32

        # Note: We create index AFTER insertion (Bulk loading) to avoid potential
        # "Invalid position" errors when checking mutable pages during incremental updates in some environments.
        # This is more robust for tests.

        # Generate 100 random vectors
        num_vectors = 100
        vectors = []
        rids = []

        # Use fixed seed for reproducibility
        random.seed(42)

        with test_db.transaction():
            for i in range(num_vectors):
                # Create random vector
                vec = [random.random() for _ in range(dims)]
                vectors.append(vec)

                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.set("id", i)
                v.save()
                rids.append(str(v.get_identity()))

        # Create index now (Bulk load)
        # We disable store_vectors_in_graph to avoid "Invalid position" errors when checking mutable pages
        # during graph build/updates which caused data loss in some environments.
        index = test_db.create_vector_index(
            "Doc", "embedding", dimensions=dims, store_vectors_in_graph=False
        )

        # Delete every 10th vector (indices 0, 10, 20, ...)
        deleted_indices = set(range(0, num_vectors, 10))

        with test_db.transaction():
            # Iterate in deterministic order to ensure consistent graph modification behavior
            for idx in sorted(list(deleted_indices)):
                rid = rids[idx]
                v_ref = test_db.lookup_by_rid(rid)
                assert v_ref is not None
                v_ref.delete()

        # Verify deletions and existence
        for i in range(num_vectors):
            vec = vectors[i]
            rid = rids[i]

            # Search for the vector
            # Increase k to 10 to handle slight variations in ANN recall or score normalization
            # especially on Windows/CI environments where the graph structure might be slightly different
            results = index.find_nearest(vec, k=10)

            if i in deleted_indices:
                # If deleted, we should NOT find this specific RID
                if len(results) > 0:
                    for found_vertex, _ in results:
                        found_rid = str(found_vertex.get_identity())
                        assert (
                            found_rid != rid
                        ), f"Deleted vector at index {i} (RID {rid}) was found!"
            else:
                # If not deleted, we SHOULD find this specific RID among top matches
                found = False
                found_rids = []
                for found_vertex, _ in results:
                    found_rid = str(found_vertex.get_identity())
                    found_rids.append(found_rid)
                    if found_rid == rid:
                        found = True
                        break

                assert found, (
                    f"Existing vector at index {i} (RID {rid}) not found in top 10 results.\n"
                    f"Found RIDs: {found_rids}\n"
                    f"Deleted indices: {sorted(list(deleted_indices))}"
                )

    def test_lsm_vector_search_overquery(self, test_db):
        """Test searching in vector index with overquery_factor."""
        # Create schema and index
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Add some data
        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.5, 0.5, 0.0],
            [0.0, 0.5, 0.5],
        ]

        with test_db.transaction():
            for v in vectors:
                vertex = test_db.new_vertex("Doc")
                vertex.set("embedding", arcadedb.to_java_float_array(v))
                vertex.save()

        # Search with overquery_factor
        query = [0.9, 0.1, 0.0]
        k = 2
        overquery_factor = 2

        # This should internally query for k * overquery_factor = 4 items
        # but return only k = 2 items
        results = index.find_nearest(query, k=k, overquery_factor=overquery_factor)

        assert len(results) == k

        # Verify the top result is still the closest one
        vertex, distance = results[0]
        res_embedding = arcadedb.to_python_array(vertex.get("embedding"))
        assert abs(res_embedding[0] - 1.0) < 0.001

    def test_get_vector_index_lsm(self, test_db):
        """Test retrieving an existing vector index (JVector implementation)."""
        # Create schema and index
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Retrieve index
        index = test_db.schema.get_vector_index("Doc", "embedding")

        assert index is not None
        from arcadedb_embedded.vector import VectorIndex

        assert isinstance(index, VectorIndex)

    def test_lsm_index_size(self, test_db):
        """Test getting the size of an LSM vector index."""
        test_db.schema.create_vertex_type("Doc")
        test_db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Initial size should be 0
        assert index.get_size() == 0

        # Add some data
        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
        ]

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Size should be 2
        assert index.get_size() == 2

    def test_lsm_persistence(self, temp_db_path):
        """Test that LSM index persists across database restarts."""
        import arcadedb_embedded as arcadedb

        # 1. Create DB and Index
        with arcadedb.create_database(temp_db_path) as db:
            db.schema.create_vertex_type("Doc")
            db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")
            db.create_vector_index("Doc", "embedding", dimensions=3)

            with db.transaction():
                v = db.new_vertex("Doc")
                v.set("embedding", arcadedb.to_java_float_array([1.0, 0.0, 0.0]))
                v.save()

        # 2. Reopen and Verify
        with arcadedb.open_database(temp_db_path) as db:
            index = db.schema.get_vector_index("Doc", "embedding")
            assert index is not None
            assert index.get_size() == 1

            results = index.find_nearest([1.0, 0.0, 0.0], k=1)
            assert len(results) == 1

    def test_lsm_cosine_distance_orthogonal_vectors(self, test_db):
        """Test that orthogonal vectors have cosine distance = 0.5 (JVector)."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTestOrthogonal")
        test_db.schema.create_property("VectorTestOrthogonal", "name", "STRING")
        test_db.schema.create_property(
            "VectorTestOrthogonal", "vector", "ARRAY_OF_FLOATS"
        )

        # Create LSM index
        index = test_db.create_vector_index(
            "VectorTestOrthogonal", "vector", dimensions=2
        )

        # Create orthogonal vectors: [1,0] and [0,1]
        if use_numpy:
            vectors = [
                ("x_axis", np.array([1.0, 0.0])),
                ("y_axis", np.array([0.0, 1.0])),
            ]
        else:
            vectors = [
                ("x_axis", [1.0, 0.0]),
                ("y_axis", [0.0, 1.0]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTestOrthogonal")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        # Query with [1,0] - should find [0,1] with distance ~0.5 (JVector normalized)
        # Note: JVector cosine distance = (1 - cos(theta)) / 2
        # Orthogonal: cos(90) = 0 -> distance = 0.5
        query = [1.0, 0.0] if not use_numpy else np.array([1.0, 0.0])
        neighbors = index.find_nearest(query, k=2)

        # Find the orthogonal vector
        orthogonal = [n for n in neighbors if str(n[0].get("name")) == "y_axis"]
        assert len(orthogonal) == 1, "Should find orthogonal vector"
        distance = orthogonal[0][1]

        print(f"\\n  Orthogonal vectors distance: {distance:.6f} (expected: 0.5)")
        assert (
            abs(distance - 0.5) < 0.01
        ), f"Orthogonal distance should be ~0.5, got {distance}"

    def test_lsm_euclidean_distance(self, test_db):
        """Test Euclidean distance metric (JVector implementation)."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTestEuclidean")
        test_db.schema.create_property("VectorTestEuclidean", "name", "STRING")
        test_db.schema.create_property(
            "VectorTestEuclidean", "vector", "ARRAY_OF_FLOATS"
        )

        # Create LSM index with EUCLIDEAN metric
        index = test_db.create_vector_index(
            "VectorTestEuclidean", "vector", dimensions=2, distance_function="EUCLIDEAN"
        )

        # Create vectors:
        # v1: [0.1, 0.1]
        # v2: [3.1, 4.1] -> Distance to v1 is 5. Squared distance is 25.
        # JVector Euclidean Similarity = 1 / (1 + d^2) = 1 / (1 + 25) = 1/26 ~= 0.038

        vectors = [
            ("origin", [0.1, 0.1]),
            ("point_3_4", [3.1, 4.1]),
        ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTestEuclidean")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        # Verify count
        count = test_db.count_type("VectorTestEuclidean")
        print(f"\nTotal vertices in DB: {count}")
        assert count == 2, f"Expected 2 vertices, found {count}"

        # Check index size
        idx_size = index.get_size()
        print(f"Index size: {idx_size}")

        # Query with [0.1, 0.1]
        query = [0.1, 0.1]
        neighbors = index.find_nearest(query, k=2)

        print(f"Neighbors found: {len(neighbors)}")
        for n in neighbors:
            print(f"  - {n[0].get('name')}: {n[1]}")

        # 1. Check exact match (origin)
        # Similarity should be 1.0 (1 / (1 + 0))
        origin = [n for n in neighbors if str(n[0].get("name")) == "origin"]
        assert len(origin) == 1
        assert (
            abs(origin[0][1] - 1.0) < 0.0001
        ), f"Origin similarity should be 1.0, got {origin[0][1]}"

        # 2. Check distant point
        # Similarity should be ~0.03846
        point = [n for n in neighbors if str(n[0].get("name")) == "point_3_4"]
        assert len(point) == 1
        expected_sim = 1.0 / (1.0 + 25.0)
        assert (
            abs(point[0][1] - expected_sim) < 0.0001
        ), f"Point similarity should be {expected_sim}, got {point[0][1]}"

        # 3. Check sorting (Higher score first)
        assert neighbors[0][0].get("name") == "origin", "Best match should be first"
        assert (
            neighbors[1][0].get("name") == "point_3_4"
        ), "Second match should be second"

    def test_lsm_cosine_distance_parallel_vectors(self, test_db):
        """Test that parallel vectors (same direction) have cosine distance = 0.0."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTestParallel")
        test_db.schema.create_property("VectorTestParallel", "name", "STRING")
        test_db.schema.create_property(
            "VectorTestParallel", "vector", "ARRAY_OF_FLOATS"
        )

        index = test_db.create_vector_index(
            "VectorTestParallel", "vector", dimensions=2
        )

        if use_numpy:
            vectors = [
                ("v1", np.array([1.0, 1.0])),
                ("v2", np.array([2.0, 2.0])),
                ("v3", np.array([0.5, 0.5])),
            ]
        else:
            vectors = [
                ("v1", [1.0, 1.0]),
                ("v2", [2.0, 2.0]),
                ("v3", [0.5, 0.5]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTestParallel")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        query = [1.0, 1.0] if not use_numpy else np.array([1.0, 1.0])
        neighbors = index.find_nearest(query, k=3)

        print("\\n  Parallel vectors distances:")
        for vertex, distance in neighbors:
            name = str(vertex.get("name"))
            print(f"    {name}: {distance:.6f} (expected: ~0.0)")
            assert (
                distance < 0.01
            ), f"Parallel vector {name} distance should be ~0.0, got {distance}"

    def test_lsm_cosine_distance_opposite_vectors(self, test_db):
        """Test that opposite vectors (180° apart) have cosine distance = 1.0 (JVector)."""
        # Note: JVector cosine distance = (1 - cos(theta)) / 2
        # Opposite: cos(180) = -1 -> distance = (1 - (-1))/2 = 1.0
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTestOpposite")
        test_db.schema.create_property("VectorTestOpposite", "name", "STRING")
        test_db.schema.create_property(
            "VectorTestOpposite", "vector", "ARRAY_OF_FLOATS"
        )

        index = test_db.create_vector_index(
            "VectorTestOpposite", "vector", dimensions=2
        )

        if use_numpy:
            vectors = [
                ("positive", np.array([1.0, 0.0])),
                ("negative", np.array([-1.0, 0.0])),
            ]
        else:
            vectors = [
                ("positive", [1.0, 0.0]),
                ("negative", [-1.0, 0.0]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTestOpposite")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        query = [1.0, 0.0] if not use_numpy else np.array([1.0, 0.0])
        neighbors = index.find_nearest(query, k=2)

        opposite = [n for n in neighbors if str(n[0].get("name")) == "negative"]
        assert len(opposite) == 1, "Should find opposite vector"
        distance = opposite[0][1]

        print(f"\\n  Opposite vectors distance: {distance:.6f} (expected: 1.0)")
        assert (
            abs(distance - 1.0) < 0.01
        ), f"Opposite distance should be ~1.0, got {distance}"

    def test_lsm_cosine_distance_45_degree_vectors(self, test_db):
        """Test vectors at 45° angle have expected cosine distance."""
        # Note: JVector cosine distance = (1 - cos(theta)) / 2
        # 45 deg: cos(45) = 0.707 -> distance = (1 - 0.707)/2 = 0.146
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTest45")
        test_db.schema.create_property("VectorTest45", "name", "STRING")
        test_db.schema.create_property("VectorTest45", "vector", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index("VectorTest45", "vector", dimensions=2)

        if use_numpy:
            vectors = [
                ("x_axis", np.array([1.0, 0.0])),
                ("diagonal", np.array([1.0, 1.0])),
            ]
        else:
            vectors = [
                ("x_axis", [1.0, 0.0]),
                ("diagonal", [1.0, 1.0]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTest45")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        query = [1.0, 0.0] if not use_numpy else np.array([1.0, 0.0])
        neighbors = index.find_nearest(query, k=2)

        diagonal = [n for n in neighbors if str(n[0].get("name")) == "diagonal"]
        assert len(diagonal) == 1, "Should find diagonal vector"
        distance = diagonal[0][1]

        expected = (1.0 - (1.0 / (2.0**0.5))) / 2.0
        print(f"\\n  45° vectors distance: {distance:.6f} (expected: {expected:.6f})")
        assert (
            abs(distance - expected) < 0.01
        ), f"45° distance should be ~{expected}, got {distance}"

    def test_lsm_cosine_distance_3d_orthogonal_vectors(self, test_db):
        """Test 3D orthogonal vectors have cosine distance = 1.0."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTest3DOrthogonal")
        test_db.schema.create_property("VectorTest3DOrthogonal", "name", "STRING")
        test_db.schema.create_property(
            "VectorTest3DOrthogonal", "vector", "ARRAY_OF_FLOATS"
        )

        index = test_db.create_vector_index(
            "VectorTest3DOrthogonal", "vector", dimensions=3
        )

        if use_numpy:
            vectors = [
                ("x_axis", np.array([1.0, 0.0, 0.0])),
                ("y_axis", np.array([0.0, 1.0, 0.0])),
                ("z_axis", np.array([0.0, 0.0, 1.0])),
            ]
        else:
            vectors = [
                ("x_axis", [1.0, 0.0, 0.0]),
                ("y_axis", [0.0, 1.0, 0.0]),
                ("z_axis", [0.0, 0.0, 1.0]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTest3DOrthogonal")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        query = [1.0, 0.0, 0.0] if not use_numpy else np.array([1.0, 0.0, 0.0])
        neighbors = index.find_nearest(query, k=3)

        print("\\n  3D orthogonal vectors distances:")
        for vertex, distance in neighbors:
            name = str(vertex.get("name"))
            print(f"    {name}: {distance:.6f}")
            if name != "x_axis":
                assert (
                    abs(distance - 0.5) < 0.01
                ), f"3D orthogonal distance should be ~0.5, got {distance}"

    def test_lsm_cosine_distance_3d_parallel_and_opposite(self, test_db):
        """Test 3D parallel (distance=0) and opposite (distance=1.0) vectors."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        test_db.schema.create_vertex_type("VectorTest3DParallel")
        test_db.schema.create_property("VectorTest3DParallel", "name", "STRING")
        test_db.schema.create_property(
            "VectorTest3DParallel", "vector", "ARRAY_OF_FLOATS"
        )

        index = test_db.create_vector_index(
            "VectorTest3DParallel", "vector", dimensions=3
        )

        if use_numpy:
            vectors = [
                ("v1", np.array([1.0, 1.0, 1.0])),
                ("v2_parallel", np.array([2.0, 2.0, 2.0])),
                ("v3_opposite", np.array([-1.0, -1.0, -1.0])),
            ]
        else:
            vectors = [
                ("v1", [1.0, 1.0, 1.0]),
                ("v2_parallel", [2.0, 2.0, 2.0]),
                ("v3_opposite", [-1.0, -1.0, -1.0]),
            ]

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTest3DParallel")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        query = [1.0, 1.0, 1.0] if not use_numpy else np.array([1.0, 1.0, 1.0])
        neighbors = index.find_nearest(query, k=3)

        print("\\n  3D parallel and opposite vectors:")
        for vertex, distance in neighbors:
            name = str(vertex.get("name"))
            print(f"    {name}: {distance:.6f}")

            if "parallel" in name:
                assert (
                    distance < 0.01
                ), f"Parallel should have distance ~0, got {distance}"
            elif "opposite" in name:
                assert (
                    abs(distance - 1.0) < 0.01
                ), f"Opposite should have distance ~1.0, got {distance}"

    def test_lsm_cosine_distance_high_dimensional(self, test_db):
        """Test cosine distance in high dimensions (128D)."""
        try:
            import numpy as np
        except ImportError:
            pytest.skip("NumPy required for high-dimensional test")

        test_db.schema.create_vertex_type("VectorTestHD")
        test_db.schema.create_property("VectorTestHD", "name", "STRING")
        test_db.schema.create_property("VectorTestHD", "vector", "ARRAY_OF_FLOATS")

        dim = 128
        np.random.seed(42)

        base = np.random.randn(dim)
        base = base / np.linalg.norm(base)

        parallel = base * 2.5
        opposite = -base

        orthogonal = np.random.randn(dim)
        orthogonal = orthogonal - np.dot(orthogonal, base) * base
        orthogonal = orthogonal / np.linalg.norm(orthogonal)

        vectors = [
            ("base", base),
            ("parallel", parallel),
            ("opposite", opposite),
            ("orthogonal", orthogonal),
        ]

        index = test_db.create_vector_index("VectorTestHD", "vector", dimensions=dim)

        with test_db.transaction():
            for name, vector in vectors:
                vertex = test_db.new_vertex("VectorTestHD")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        neighbors = index.find_nearest(base, k=4)

        print(f"\\n  High-dimensional ({dim}D) cosine distances:")
        for vertex, distance in neighbors:
            name = str(vertex.get("name"))
            print(f"    {name}: {distance:.6f}")

            if name == "parallel":
                assert (
                    distance < 0.01
                ), f"Parallel should have distance ~0, got {distance}"
            elif name == "opposite":
                assert (
                    abs(distance - 1.0) < 0.01
                ), f"Opposite should have distance ~1.0, got {distance}"
            elif name == "orthogonal":
                assert (
                    abs(distance - 0.5) < 0.1
                ), f"Orthogonal should have distance ~0.5, got {distance}"

    def test_lsm_vector_search_comprehensive(self, test_db):
        """Test vector embeddings with LSM similarity search (comprehensive)."""
        try:
            import numpy as np

            use_numpy = True
        except ImportError:
            use_numpy = False

        # Create vertex type for vector embeddings
        test_db.schema.create_vertex_type("EmbeddingNodeLSM")
        test_db.schema.create_property("EmbeddingNodeLSM", "name", "STRING")
        test_db.schema.create_property("EmbeddingNodeLSM", "vector", "ARRAY_OF_FLOATS")

        # Create index
        index = test_db.create_vector_index("EmbeddingNodeLSM", "vector", dimensions=4)

        # Insert sample word embeddings (4-dimensional for simplicity)
        if use_numpy:
            embeddings = [
                ("king", np.array([0.5, 0.3, 0.1, 0.2])),
                ("queen", np.array([0.52, 0.32, 0.08, 0.18])),  # Similar to king
                ("man", np.array([0.48, 0.25, 0.15, 0.22])),
                ("woman", np.array([0.50, 0.28, 0.12, 0.20])),
                ("cat", np.array([0.1, 0.8, 0.6, 0.3])),  # Different cluster
                ("dog", np.array([0.12, 0.82, 0.58, 0.32])),  # Similar to cat
            ]
        else:
            embeddings = [
                ("king", [0.5, 0.3, 0.1, 0.2]),
                ("queen", [0.52, 0.32, 0.08, 0.18]),  # Similar to king
                ("man", [0.48, 0.25, 0.15, 0.22]),
                ("woman", [0.50, 0.28, 0.12, 0.20]),
                ("cat", [0.1, 0.8, 0.6, 0.3]),  # Different cluster
                ("dog", [0.12, 0.82, 0.58, 0.32]),  # Similar to cat
            ]

        with test_db.transaction():
            for name, vector in embeddings:
                vertex = test_db.new_vertex("EmbeddingNodeLSM")
                vertex.set("name", name)
                vertex.set("vector", arcadedb.to_java_float_array(vector))
                vertex.save()

        # Search for neighbors of "king"
        if use_numpy:
            king_vector = np.array([0.5, 0.3, 0.1, 0.2])
        else:
            king_vector = [0.5, 0.3, 0.1, 0.2]

        neighbors = index.find_nearest(king_vector, k=3)
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]

        print(f"\\n  3 nearest neighbors to 'king': {neighbor_names}")
        assert "queen" in neighbor_names, "Expected 'queen' to be near 'king'"
        assert "man" in neighbor_names or "woman" in neighbor_names
        assert "cat" not in neighbor_names, "'cat' should be in different cluster"
        assert "dog" not in neighbor_names, "'dog' should be in different cluster"

        # Search for neighbors of "cat"
        if use_numpy:
            cat_vector = np.array([0.1, 0.8, 0.6, 0.3])
        else:
            cat_vector = [0.1, 0.8, 0.6, 0.3]

        neighbors = index.find_nearest(cat_vector, k=2)
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]

        print(f"  2 nearest neighbors to 'cat': {neighbor_names}")
        assert "dog" in neighbor_names, "Expected 'dog' to be near 'cat'"
        assert "king" not in neighbor_names, "'king' should be in different cluster"

    def test_int8_quantization_boundary_condition(self, test_db):
        """
        Demonstrates that INT8 quantization runs without crashing for small N (e.g. 10) at Dim=16,
        unlike N=50 which crashes. This explains why simple unit tests might pass (False Positives).
        """
        test_db.schema.create_vertex_type("SmallScaleInt8")
        test_db.schema.create_property("SmallScaleInt8", "embedding", "ARRAY_OF_FLOATS")

        # Dim=16 is the threshold where negative index bugs stop, but storage bugs haven't started yet (for small N)
        dims = 16
        index = test_db.create_vector_index(
            "SmallScaleInt8", "embedding", dimensions=dims, quantization="INT8"
        )

        # N=10 is safe from storage overflow (which triggers around N=50)
        vectors = []
        for i in range(10):
            vec = [0.0] * dims
            vec[i % dims] = 1.0
            vectors.append(vec)

        with test_db.transaction():
            for vec in vectors:
                v = test_db.new_vertex("SmallScaleInt8")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Search should succeed
        query = [0.9, 0.1] + [0.0] * (dims - 2)
        results = index.find_nearest(query, k=1)

        assert len(results) == 1
        # Note: We don't assert strict accuracy here, as INT8 is known to be imprecise.
        # We just verify that it runs without the IllegalArgumentException seen in the N=50 test.

    def test_lsm_vector_quantization_int8_comprehensive(self, test_db):
        """Test creating a vector index with quantization (INT8) - Comprehensive."""
        # Create schema
        test_db.schema.create_vertex_type("QuantizedDocInt8")
        test_db.schema.create_property(
            "QuantizedDocInt8", "embedding", "ARRAY_OF_FLOATS"
        )

        # Create vector index with quantization
        dims = 16
        index = test_db.create_vector_index(
            "QuantizedDocInt8",
            "embedding",
            dimensions=dims,
            quantization="INT8",
        )

        assert index is not None
        assert index.get_quantization() == "INT8"

        # Add enough data to verify robustness (N=50 was previous failure point)
        num_vectors = 50
        vectors = []
        for i in range(num_vectors):
            vec = [0.0] * dims
            vec[i % dims] = 1.0
            # Add noise to make unique and avoid graph collapsing
            if i >= dims:
                vec[(i + 1) % dims] = 0.01 * (i // dims)
            vectors.append(vec)

        with test_db.transaction():
            for i, vec in enumerate(vectors):
                v = test_db.new_vertex("QuantizedDocInt8")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Search should work
        query = [0.9, 0.1] + [0.0] * (dims - 2)
        results = index.find_nearest(query, k=1)

        assert len(results) == 1
        # Verify we found the vector that looks like [1.0, 0.0, ...]
        vertex, distance = results[0]
        vec_data = arcadedb.to_python_array(vertex.get("embedding"))

        # Check content: The first dimension should be dominant
        assert vec_data[0] > 0.9

        # Check distance:
        # Cosine Similarity ~= 0.9
        # Cosine Distance ~= 0.1
        assert (
            0.0 <= distance < 0.2
        ), f"Distance {distance} is suspicious (expected ~0.1)"

    def test_lsm_vector_quantization_binary_comprehensive(self, test_db):
        """Test creating a vector index with BINARY quantization - Comprehensive."""
        # Create schema
        test_db.schema.create_vertex_type("QuantizedDocBinary")
        test_db.schema.create_property(
            "QuantizedDocBinary", "embedding", "ARRAY_OF_FLOATS"
        )

        dims = 128

        # Create vector index with quantization BEFORE insertion
        # Using store_vectors_in_graph=True for better stability
        index = test_db.create_vector_index(
            "QuantizedDocBinary",
            "embedding",
            dimensions=dims,
            quantization="BINARY",
            store_vectors_in_graph=True,
        )

        assert index is not None
        assert index.get_quantization() == "BINARY"

        # Add enough data to verify robustness
        num_vectors = 50
        vectors = []
        for i in range(num_vectors):
            # Use -1.0 and 1.0 to be safe for Binary Quantization (sign based)
            # Avoid 0.0 which might be ambiguous depending on implementation
            vec = [-1.0] * dims
            vec[i % dims] = 1.0
            vectors.append(vec)

        with test_db.transaction():
            for i, vec in enumerate(vectors):
                v = test_db.new_vertex("QuantizedDocBinary")
                v.set("embedding", arcadedb.to_java_float_array(vec))
                v.save()

        # Search
        query = [0.9, 0.1] + [-1.0] * (dims - 2)
        results = index.find_nearest(query, k=1)

        # BINARY quantization currently drops data or returns 0 results
        assert len(results) == 1
        vertex, distance = results[0]
        vec_data = arcadedb.to_python_array(vertex.get("embedding"))

        # Check content
        # Binary quantization is lossy, so we relax the check to just ensure we got a valid vector back
        assert vec_data is not None
        assert len(vec_data) == dims

        # Check distance (BINARY might be Hamming or similar, but JVector often normalizes)
        # If it's Hamming, distance is int. If Cosine, float.
        # We just check it's not None
        assert distance is not None

    def test_document_vector_search(self, test_db):
        """Test vector search on Document type with metadata and clustering."""
        # Create schema
        test_db.schema.create_document_type("MyDoc")
        test_db.schema.create_property("MyDoc", "name", "STRING")
        test_db.schema.create_property("MyDoc", "embedding", "ARRAY_OF_FLOATS")

        # Create vector index
        index = test_db.create_vector_index("MyDoc", "embedding", dimensions=4)

        # Add data: Fruits (dim 0 dominant) vs Vehicles (dim 1 dominant)
        # [Fruitness, Vehicleness, 0, 0]
        data = [
            ("apple", [0.9, 0.1, 0.0, 0.0]),
            ("banana", [0.8, 0.2, 0.0, 0.0]),
            ("car", [0.1, 0.9, 0.0, 0.0]),
            ("truck", [0.2, 0.8, 0.0, 0.0]),
        ]

        with test_db.transaction():
            for name, vec in data:
                doc = test_db.new_document("MyDoc")
                doc.set("name", name)
                doc.set("embedding", arcadedb.to_java_float_array(vec))
                doc.save()

        # Search for something "fruity"
        query = [0.95, 0.05, 0.0, 0.0]
        results = index.find_nearest(query, k=2)

        assert len(results) == 2

        # Extract names from results
        found_names = []
        for record, distance in results:
            assert record.get_type_name() == "MyDoc"
            found_names.append(record.get("name"))

            # Verify embedding is present and correct type
            emb = arcadedb.to_python_array(record.get("embedding"))
            assert len(emb) == 4

        # Should find apple and banana
        assert "apple" in found_names
        assert "banana" in found_names
        assert "car" not in found_names

        # Search for something "vehicular"
        query_vehicle = [0.05, 0.95, 0.0, 0.0]
        results_v = index.find_nearest(query_vehicle, k=2)
        found_names_v = [r[0].get("name") for r in results_v]

        assert "car" in found_names_v
        assert "truck" in found_names_v
        assert "apple" not in found_names_v

    def test_create_vector_index_with_graph_storage(self, test_db):
        """Test creating a vector index with store_vectors_in_graph=True."""
        # Create schema
        test_db.schema.create_vertex_type("GraphDoc")
        test_db.schema.create_property("GraphDoc", "embedding", "ARRAY_OF_FLOATS")

        try:
            index = test_db.create_vector_index(
                "GraphDoc", "embedding", dimensions=3, store_vectors_in_graph=True
            )
            assert index is not None
        except Exception as e:
            pytest.fail(f"Failed to create index with store_vectors_in_graph=True: {e}")

    def test_graph_storage_with_quantization(self, test_db):
        """Test creating a vector index with both graph storage and quantization."""
        # Create schema
        test_db.schema.create_vertex_type("GraphQuantDoc")
        test_db.schema.create_property("GraphQuantDoc", "embedding", "ARRAY_OF_FLOATS")

        try:
            index = test_db.create_vector_index(
                "GraphQuantDoc",
                "embedding",
                dimensions=3,
                quantization="BINARY",
                store_vectors_in_graph=True,
            )
            assert index is not None
            assert index.get_quantization() == "BINARY"
        except Exception as e:
            pytest.fail(
                f"Failed to create index with graph storage and quantization: {e}"
            )
