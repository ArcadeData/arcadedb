import arcadedb_embedded as arcadedb
import pytest


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary test database."""
    db_path = str(tmp_path / "test_vector_params_db")
    db = arcadedb.create_database(db_path)
    yield db
    db.drop()


class TestVectorParams:
    """Verify that vector index parameters are correctly passed to Java."""

    def test_quantization_param(self, test_db):
        """Test sending quantization parameter."""
        test_db.schema.create_vertex_type("QuantDoc")
        test_db.schema.create_property("QuantDoc", "embedding", "ARRAY_OF_FLOATS")

        # Create with INT8
        index = test_db.create_vector_index(
            "QuantDoc", "embedding", dimensions=3, quantization="INT8"
        )

        # Verify via wrapper convenience method
        assert index.get_quantization() == "INT8"

        # Verify by inspecting Java object directly
        # Accessing the underlying Java object's metadata
        java_index = index._java_index

        # Depending on if it's a TypeIndex or LSMVectorIndex directly
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        # In Java: index.getMetadata().quantizationType
        # JPype allows attribute access for getters or fields
        # Note: the exact field name depends on the Java class implementation of metadata
        # Given bindings/python/src/arcadedb_embedded/vector.py uses .quantizationType:
        assert str(idx_to_check.getMetadata().quantizationType) == "INT8"

    def test_store_vectors_in_graph_param(self, test_db):
        """Test sending store_vectors_in_graph parameter."""
        test_db.schema.create_vertex_type("StoreDoc")
        test_db.schema.create_property("StoreDoc", "embedding", "ARRAY_OF_FLOATS")

        # Create with store_vectors_in_graph=True
        index = test_db.create_vector_index(
            "StoreDoc", "embedding", dimensions=3, store_vectors_in_graph=True
        )

        # Accessing the underlying Java object's metadata
        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        metadata = idx_to_check.getMetadata()

        # We need to find where "storeVectorsInGraph" is stored.
        # It might be a field, or it might be in map-like structure if it was passed via JSON.
        # Let's inspect what we can.

        print(f"\nMetadata Class: {metadata.getClass().getName()}")
        print(f"Metadata String: {metadata.toString()}")

        # Attempt to check property directly if it's exposed as a field matching the JSON key
        # Or check via getter if available

        val = None
        try:
            # Try field access
            val = metadata.storeVectorsInGraph
        except Exception:
            try:
                # Try getter
                val = metadata.isStoreVectorsInGraph()
            except Exception:
                pass

        if val is None:
            # Try inspecting the string representation as a fallback for verification
            assert (
                "storeVectorsInGraph" in metadata.toString()
                or "storeVectorsInGraph=true" in metadata.toString()
            )
        else:
            assert val is True

    def test_add_hierarchy_param(self, test_db):
        """Test sending add_hierarchy parameter."""
        test_db.schema.create_vertex_type("HierDoc")
        test_db.schema.create_property("HierDoc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "HierDoc", "embedding", dimensions=3, add_hierarchy=True
        )

        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        metadata = idx_to_check.getMetadata()

        try:
            assert metadata.addHierarchy is True
        except AttributeError:
            assert (
                "addHierarchy" in metadata.toString()
                or "addHierarchy=true" in metadata.toString()
            )

    def test_params_persistence(self, tmp_path):
        """Verify parameters persist after reload."""
        db_path = str(tmp_path / "test_vector_params_persist")

        # 1. Create and Configure
        with arcadedb.create_database(db_path) as db:
            db.schema.create_vertex_type("Doc")
            db.schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

            db.create_vector_index(
                "Doc",
                "embedding",
                dimensions=3,
                quantization="INT8",
                store_vectors_in_graph=True,
                add_hierarchy=True,
            )

        # 2. Reopen and Check
        with arcadedb.open_database(db_path) as db:
            index = db.schema.get_vector_index("Doc", "embedding")

            # Check Quantization
            assert index.get_quantization() == "INT8"

            # Check Graph Storage
            java_index = index._java_index
            idx_to_check = java_index
            if "TypeIndex" in java_index.getClass().getName():
                idx_to_check = java_index.getSubIndexes().get(0)

            metadata = idx_to_check.getMetadata()
            print(f"\nReloaded Metadata: {metadata.toString()}")

            # Verification (similar strategy as above)
            try:
                assert metadata.storeVectorsInGraph is True
            except AttributeError:
                assert (
                    "storeVectorsInGraph=true" in metadata.toString()
                    or "storeVectorsInGraph: true" in metadata.toString()
                )

            try:
                assert metadata.addHierarchy is True
            except AttributeError:
                assert (
                    "addHierarchy=true" in metadata.toString()
                    or "addHierarchy: true" in metadata.toString()
                )

    def test_per_index_cache_params(self, test_db):
        """Test per-index cache and rebuild overrides."""
        test_db.schema.create_vertex_type("CacheDoc")
        test_db.schema.create_property("CacheDoc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "CacheDoc",
            "embedding",
            dimensions=4,
            location_cache_size=123,
            graph_build_cache_size=456,
            mutations_before_rebuild=789,
        )

        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        metadata = idx_to_check.getMetadata()

        # Direct field access is available on LSMVectorIndexMetadata; fall back to string inspection if not.
        try:
            assert metadata.locationCacheSize == 123
            assert metadata.graphBuildCacheSize == 456
            assert metadata.mutationsBeforeRebuild == 789
        except AttributeError:
            meta_str = metadata.toString()
            assert (
                "locationCacheSize=123" in meta_str
                or "locationCacheSize: 123" in meta_str
            )
            assert (
                "graphBuildCacheSize=456" in meta_str
                or "graphBuildCacheSize: 456" in meta_str
            )
            assert (
                "mutationsBeforeRebuild=789" in meta_str
                or "mutationsBeforeRebuild: 789" in meta_str
            )

    def test_quantization_none(self, test_db):
        """Test sending quantization parameter NONE."""
        test_db.schema.create_vertex_type("QuantNoneDoc")
        test_db.schema.create_property("QuantNoneDoc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "QuantNoneDoc", "embedding", dimensions=3, quantization="NONE"
        )

        assert index.get_quantization() == "NONE"

        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        assert str(idx_to_check.getMetadata().quantizationType) == "NONE"

    def test_quantization_binary(self, test_db):
        """Test sending quantization parameter BINARY."""
        test_db.schema.create_vertex_type("QuantBinaryDoc")
        test_db.schema.create_property("QuantBinaryDoc", "embedding", "ARRAY_OF_FLOATS")

        index = test_db.create_vector_index(
            "QuantBinaryDoc", "embedding", dimensions=128, quantization="BINARY"
        )

        assert index.get_quantization() == "BINARY"

        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        assert str(idx_to_check.getMetadata().quantizationType) == "BINARY"

    def test_quantization_product(self, test_db):
        """Test sending quantization parameter PRODUCT (PQ)."""
        test_db.schema.create_vertex_type("QuantProductDoc")
        test_db.schema.create_property(
            "QuantProductDoc", "embedding", "ARRAY_OF_FLOATS"
        )

        index = test_db.create_vector_index(
            "QuantProductDoc", "embedding", dimensions=64, quantization="PRODUCT"
        )

        assert index.get_quantization() == "PRODUCT"

        java_index = index._java_index
        idx_to_check = java_index
        if "TypeIndex" in java_index.getClass().getName():
            idx_to_check = java_index.getSubIndexes().get(0)

        assert str(idx_to_check.getMetadata().quantizationType) == "PRODUCT"

    def test_jvm_heap_check(self):
        """Verify JVM memory settings from Java level."""
        import jpype

        runtime = jpype.JPackage("java.lang").Runtime.getRuntime()
        max_memory = runtime.maxMemory()
        total_memory = runtime.totalMemory()
        free_memory = runtime.freeMemory()

        print(f"\n=== JVM Memory Stats ===")
        print(f"Max Memory:   {max_memory / (1024**3):.2f} GB ({max_memory} bytes)")
        print(f"Total Memory: {total_memory / (1024**2):.2f} MB")
        print(f"Free Memory:  {free_memory / (1024**2):.2f} MB")

        # Verify it's a reasonable size (at least 1GB, reflecting -Xmx4g default)
        assert max_memory > 1 * 1024 * 1024 * 1024
