"""
Tests for Schema API functionality.

Tests cover:
- Type creation (document, vertex, edge)
- Property creation (simple and complex types)
- Index creation (unique, composite, full-text)
- get_or_create methods
- drop methods
- exists methods
- Error handling
"""

import pytest
from arcadedb_embedded import ArcadeDBError, IndexType, PropertyType, create_database


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary test database."""
    db_path = str(tmp_path / "test_schema_db")
    db = create_database(db_path)
    yield db
    db.drop()


class TestTypeCreation:
    """Test type creation methods."""

    def test_create_document_type(self, test_db):
        """Test creating a document type."""
        schema = test_db.schema
        doc_type = schema.create_document_type("Document")

        assert doc_type is not None
        assert schema.exists_type("Document")
        assert schema.get_type("Document") is not None

    def test_create_vertex_type(self, test_db):
        """Test creating a vertex type."""
        schema = test_db.schema
        vertex_type = schema.create_vertex_type("Person")

        assert vertex_type is not None
        assert schema.exists_type("Person")
        assert schema.get_type("Person") is not None

    def test_create_edge_type(self, test_db):
        """Test creating an edge type."""
        schema = test_db.schema
        edge_type = schema.create_edge_type("Knows")

        assert edge_type is not None
        assert schema.exists_type("Knows")
        assert schema.get_type("Knows") is not None

    def test_create_type_with_buckets(self, test_db):
        """Test creating a type with specific bucket count."""
        schema = test_db.schema
        doc_type = schema.create_document_type("DocWithBuckets", buckets=5)

        assert doc_type is not None
        assert schema.exists_type("DocWithBuckets")

    def test_duplicate_type_creation_fails(self, test_db):
        """Test that creating duplicate type raises error."""
        schema = test_db.schema
        schema.create_document_type("Duplicate")

        with pytest.raises(ArcadeDBError):
            schema.create_document_type("Duplicate")

    def test_get_or_create_document_type_new(self, test_db):
        """Test get_or_create for new document type."""
        schema = test_db.schema
        doc_type = schema.get_or_create_document_type("NewDoc")

        assert doc_type is not None
        assert schema.exists_type("NewDoc")

    def test_get_or_create_document_type_existing(self, test_db):
        """Test get_or_create for existing document type."""
        schema = test_db.schema
        schema.create_document_type("ExistingDoc")
        doc_type = schema.get_or_create_document_type("ExistingDoc")

        assert doc_type is not None
        assert schema.exists_type("ExistingDoc")

    def test_get_or_create_vertex_type_new(self, test_db):
        """Test get_or_create for new vertex type."""
        schema = test_db.schema
        vertex_type = schema.get_or_create_vertex_type("NewVertex")

        assert vertex_type is not None
        assert schema.exists_type("NewVertex")

    def test_get_or_create_vertex_type_existing(self, test_db):
        """Test get_or_create for existing vertex type."""
        schema = test_db.schema
        schema.create_vertex_type("ExistingVertex")
        vertex_type = schema.get_or_create_vertex_type("ExistingVertex")

        assert vertex_type is not None
        assert schema.exists_type("ExistingVertex")

    def test_get_or_create_edge_type_new(self, test_db):
        """Test get_or_create for new edge type."""
        schema = test_db.schema
        edge_type = schema.get_or_create_edge_type("NewEdge")

        assert edge_type is not None
        assert schema.exists_type("NewEdge")

    def test_get_or_create_edge_type_existing(self, test_db):
        """Test get_or_create for existing edge type."""
        schema = test_db.schema
        schema.create_edge_type("ExistingEdge")
        edge_type = schema.get_or_create_edge_type("ExistingEdge")

        assert edge_type is not None
        assert schema.exists_type("ExistingEdge")


class TestTypeQueries:
    """Test type query methods."""

    def test_exists_type_true(self, test_db):
        """Test exists_type returns True for existing type."""
        schema = test_db.schema
        schema.create_document_type("ExistingType")

        assert schema.exists_type("ExistingType") is True

    def test_exists_type_false(self, test_db):
        """Test exists_type returns False for non-existent type."""
        schema = test_db.schema

        assert schema.exists_type("NonExistentType") is False

    def test_get_type_existing(self, test_db):
        """Test get_type for existing type."""
        schema = test_db.schema
        schema.create_vertex_type("Person")

        person_type = schema.get_type("Person")
        assert person_type is not None

    def test_get_type_non_existent(self, test_db):
        """Test get_type for non-existent type returns None."""
        schema = test_db.schema

        result = schema.get_type("NonExistent")
        assert result is None

    def test_get_types(self, test_db):
        """Test get_types returns all types."""
        schema = test_db.schema
        schema.create_document_type("Doc1")
        schema.create_vertex_type("Vertex1")
        schema.create_edge_type("Edge1")

        types = schema.get_types()
        type_names = [t.getName() for t in types]

        assert "Doc1" in type_names
        assert "Vertex1" in type_names
        assert "Edge1" in type_names


class TestTypeDeletion:
    """Test type deletion methods."""

    def test_drop_type(self, test_db):
        """Test dropping a type."""
        schema = test_db.schema
        schema.create_document_type("TempType")
        assert schema.exists_type("TempType")

        schema.drop_type("TempType")
        assert not schema.exists_type("TempType")

    def test_drop_non_existent_type_fails(self, test_db):
        """Test dropping non-existent type raises error."""
        schema = test_db.schema

        with pytest.raises(ArcadeDBError):
            schema.drop_type("NonExistent")


class TestPropertyCreation:
    """Test property creation methods."""

    def test_create_simple_property(self, test_db):
        """Test creating a simple property."""
        schema = test_db.schema
        schema.create_vertex_type("User")

        prop = schema.create_property("User", "name", PropertyType.STRING)
        assert prop is not None

    def test_create_integer_property(self, test_db):
        """Test creating an integer property."""
        schema = test_db.schema
        schema.create_vertex_type("User")

        prop = schema.create_property("User", "age", PropertyType.INTEGER)
        assert prop is not None

    def test_create_list_property(self, test_db):
        """Test creating a list property with of_type."""
        schema = test_db.schema
        schema.create_vertex_type("User")

        prop = schema.create_property(
            "User", "tags", PropertyType.LIST, of_type=PropertyType.STRING
        )
        assert prop is not None

    def test_create_multiple_properties(self, test_db):
        """Test creating multiple properties on same type."""
        schema = test_db.schema
        schema.create_vertex_type("Person")

        schema.create_property("Person", "name", PropertyType.STRING)
        schema.create_property("Person", "age", PropertyType.INTEGER)
        schema.create_property("Person", "email", PropertyType.STRING)

        # All properties should exist
        person_type = schema.get_type("Person")
        assert person_type is not None

    def test_create_property_on_non_existent_type_fails(self, test_db):
        """Test creating property on non-existent type raises error."""
        schema = test_db.schema

        with pytest.raises(ArcadeDBError):
            schema.create_property("NonExistent", "prop", PropertyType.STRING)

    def test_get_or_create_property_new(self, test_db):
        """Test get_or_create for new property."""
        schema = test_db.schema
        schema.create_vertex_type("User")

        prop = schema.get_or_create_property("User", "email", PropertyType.STRING)
        assert prop is not None

    def test_get_or_create_property_existing(self, test_db):
        """Test get_or_create for existing property."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "name", PropertyType.STRING)

        prop = schema.get_or_create_property("User", "name", PropertyType.STRING)
        assert prop is not None


class TestPropertyDeletion:
    """Test property deletion methods."""

    def test_drop_property(self, test_db):
        """Test dropping a property."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "temp", PropertyType.STRING)

        schema.drop_property("User", "temp")

        # Property should no longer exist (verify by trying to create again)
        prop = schema.create_property("User", "temp", PropertyType.STRING)
        assert prop is not None

    def test_drop_property_from_non_existent_type_fails(self, test_db):
        """Test dropping property from non-existent type raises error."""
        schema = test_db.schema

        with pytest.raises(ArcadeDBError):
            schema.drop_property("NonExistent", "prop")


class TestIndexCreation:
    """Test index creation methods."""

    def test_create_simple_index(self, test_db):
        """Test creating a simple index on single property."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "email", PropertyType.STRING)

        idx = schema.create_index("User", ["email"])
        assert idx is not None

    def test_create_unique_index(self, test_db):
        """Test creating a unique index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "username", PropertyType.STRING)

        idx = schema.create_index("User", ["username"], unique=True)
        assert idx is not None

    def test_create_composite_index(self, test_db):
        """Test creating a composite index on multiple properties."""
        schema = test_db.schema
        schema.create_vertex_type("Person")
        schema.create_property("Person", "firstName", PropertyType.STRING)
        schema.create_property("Person", "lastName", PropertyType.STRING)

        idx = schema.create_index("Person", ["firstName", "lastName"])
        assert idx is not None

    def test_create_full_text_index(self, test_db):
        """Test creating a full-text index."""
        schema = test_db.schema
        schema.create_document_type("Article")
        schema.create_property("Article", "content", PropertyType.STRING)

        idx = schema.create_index(
            "Article", ["content"], index_type=IndexType.FULL_TEXT
        )
        assert idx is not None

    def test_create_lsm_tree_index(self, test_db):
        """Test creating an LSM tree index explicitly."""
        schema = test_db.schema
        schema.create_vertex_type("Product")
        schema.create_property("Product", "sku", PropertyType.STRING)

        idx = schema.create_index("Product", ["sku"], index_type=IndexType.LSM_TREE)
        assert idx is not None

    def test_get_or_create_index_new(self, test_db):
        """Test get_or_create for new index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "id", PropertyType.INTEGER)

        idx = schema.get_or_create_index("User", ["id"], unique=True)
        assert idx is not None

    def test_get_or_create_index_existing(self, test_db):
        """Test get_or_create for existing index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "email", PropertyType.STRING)
        schema.create_index("User", ["email"], unique=True)

        idx = schema.get_or_create_index("User", ["email"], unique=True)
        assert idx is not None


class TestIndexQueries:
    """Test index query methods."""

    def test_exists_index_true(self, test_db):
        """Test exists_index returns True for existing index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "id", PropertyType.INTEGER)
        schema.create_index("User", ["id"], unique=True)

        # Get the index name (format: Type[property,property])
        indexes = schema.get_indexes()
        index_name = None
        for idx in indexes:
            if "User" in idx.getTypeName():
                index_name = idx.getName()
                break

        assert index_name is not None
        assert schema.exists_index(index_name) is True

    def test_exists_index_false(self, test_db):
        """Test exists_index returns False for non-existent index."""
        schema = test_db.schema

        assert schema.exists_index("NonExistentIndex") is False

    def test_get_indexes(self, test_db):
        """Test get_indexes returns all indexes."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "email", PropertyType.STRING)
        schema.create_property("User", "username", PropertyType.STRING)

        schema.create_index("User", ["email"])
        schema.create_index("User", ["username"], unique=True)

        indexes = schema.get_indexes()
        assert len(indexes) >= 2  # At least our 2 indexes


class TestIndexDeletion:
    """Test index deletion methods."""

    def test_drop_index(self, test_db):
        """Test dropping an index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "temp", PropertyType.STRING)
        schema.create_index("User", ["temp"])

        # Get the index name
        indexes = schema.get_indexes()
        index_name = None
        for idx in indexes:
            if "User" in idx.getTypeName() and "temp" in idx.getName():
                index_name = idx.getName()
                break

        assert index_name is not None
        assert schema.exists_index(index_name) is True

        schema.drop_index(index_name)
        assert schema.exists_index(index_name) is False

    def test_drop_non_existent_index_fails(self, test_db):
        """Test dropping non-existent index raises error."""
        schema = test_db.schema

        with pytest.raises(ArcadeDBError):
            schema.drop_index("NonExistentIndex")


class TestPropertyTypes:
    """Test all PropertyType enum values."""

    def test_all_property_types(self, test_db):
        """Test creating properties with all supported types."""
        schema = test_db.schema
        schema.create_document_type("AllTypes")

        # Test each property type
        type_tests = [
            ("boolProp", PropertyType.BOOLEAN),
            ("byteProp", PropertyType.BYTE),
            ("shortProp", PropertyType.SHORT),
            ("intProp", PropertyType.INTEGER),
            ("longProp", PropertyType.LONG),
            ("floatProp", PropertyType.FLOAT),
            ("doubleProp", PropertyType.DOUBLE),
            ("decimalProp", PropertyType.DECIMAL),
            ("stringProp", PropertyType.STRING),
            ("binaryProp", PropertyType.BINARY),
            ("dateProp", PropertyType.DATE),
            ("datetimeProp", PropertyType.DATETIME),
        ]

        for prop_name, prop_type in type_tests:
            prop = schema.create_property("AllTypes", prop_name, prop_type)
            assert prop is not None

    def test_complex_property_types(self, test_db):
        """Test creating complex property types (LIST, MAP, etc.)."""
        schema = test_db.schema
        schema.create_document_type("Complex")

        # List of strings
        schema.create_property(
            "Complex",
            "stringList",
            PropertyType.LIST,
            of_type=PropertyType.STRING,
        )

        # List of integers
        schema.create_property(
            "Complex",
            "intList",
            PropertyType.LIST,
            of_type=PropertyType.INTEGER,
        )

        # Map
        schema.create_property("Complex", "metadata", PropertyType.MAP)

        complex_type = schema.get_type("Complex")
        assert complex_type is not None


class TestVectorIndexSchemaOps:
    """Test vector index schema operations (retrieval and listing via Schema API)."""

    def test_get_index_by_name_existing(self, test_db):
        """Test get_index_by_name for an existing index."""
        schema = test_db.schema
        schema.create_vertex_type("User")
        schema.create_property("User", "email", PropertyType.STRING)
        schema.create_index("User", ["email"], unique=True)

        # Get the index name (format: Type[property])
        indexes = schema.get_indexes()
        index_name = None
        for idx in indexes:
            if "User" in idx.getTypeName() and "email" in idx.getName():
                index_name = idx.getName()
                break

        assert index_name is not None
        retrieved_index = schema.get_index_by_name(index_name)
        assert retrieved_index is not None
        assert retrieved_index.getName() == index_name

    def test_get_index_by_name_non_existent(self, test_db):
        """Test get_index_by_name returns None for non-existent index."""
        schema = test_db.schema
        result = schema.get_index_by_name("NonExistentIndex")
        assert result is None

    def test_list_vector_indexes_empty(self, test_db):
        """Test list_vector_indexes returns empty list when no vector indexes exist."""
        schema = test_db.schema

        # Create some regular indexes
        schema.create_vertex_type("User")
        schema.create_property("User", "name", PropertyType.STRING)
        schema.create_index("User", ["name"])

        vector_indexes = schema.list_vector_indexes()
        assert isinstance(vector_indexes, list)
        assert len(vector_indexes) == 0

    def test_get_vector_index_non_existent(self, test_db):
        """Test get_vector_index returns None for non-existent index."""
        schema = test_db.schema

        # Create vertex type but no index
        schema.create_vertex_type("NoIndex")
        schema.create_property("NoIndex", "embedding", "ARRAY_OF_FLOATS")

        # Try to get non-existent index
        result = schema.get_vector_index(
            vertex_type="NoIndex", vector_property="embedding"
        )

        assert result is None

    def test_get_vector_index_wrong_type(self, test_db):
        """Test get_vector_index returns None for non-vector index."""
        schema = test_db.schema

        # Create regular index (not vector)
        schema.create_vertex_type("RegularIndex")
        schema.create_property("RegularIndex", "name", PropertyType.STRING)
        schema.create_index("RegularIndex", ["name"])

        # Try to get it as vector index (should return None)
        result = schema.get_vector_index(
            vertex_type="RegularIndex", vector_property="name"
        )

        assert result is None


class TestIntegration:
    """Integration tests combining multiple schema operations."""

    def test_create_complete_graph_schema(self, test_db):
        """Test creating a complete graph schema."""
        schema = test_db.schema

        # Create vertex types
        schema.create_vertex_type("User")
        schema.create_property("User", "username", PropertyType.STRING)
        schema.create_property("User", "email", PropertyType.STRING)
        schema.create_property("User", "age", PropertyType.INTEGER)
        schema.create_index("User", ["username"], unique=True)
        schema.create_index("User", ["email"], unique=True)

        schema.create_vertex_type("Post")
        schema.create_property("Post", "title", PropertyType.STRING)
        schema.create_property("Post", "content", PropertyType.STRING)
        schema.create_property("Post", "created", PropertyType.DATETIME)
        schema.create_index("Post", ["content"], index_type=IndexType.FULL_TEXT)

        # Create edge types
        schema.create_edge_type("Follows")
        schema.create_edge_type("Wrote")
        schema.create_property("Wrote", "publishedAt", PropertyType.DATETIME)

        # Verify all types exist
        assert schema.exists_type("User")
        assert schema.exists_type("Post")
        assert schema.exists_type("Follows")
        assert schema.exists_type("Wrote")

    def test_schema_modification_workflow(self, test_db):
        """Test a typical schema modification workflow."""
        schema = test_db.schema

        # Initial schema
        schema.create_vertex_type("Product")
        schema.create_property("Product", "name", PropertyType.STRING)
        schema.create_property("Product", "price", PropertyType.DECIMAL)

        # Add new property
        schema.create_property("Product", "sku", PropertyType.STRING)
        schema.create_index("Product", ["sku"], unique=True)

        # Add composite index
        schema.create_property("Product", "category", PropertyType.STRING)
        schema.create_index("Product", ["category", "name"])

        # Verify final schema
        product_type = schema.get_type("Product")
        assert product_type is not None

    def test_get_or_create_idempotent_schema_creation(self, test_db):
        """Test that get_or_create methods are idempotent."""
        schema = test_db.schema

        # Create schema multiple times (should not error)
        for _ in range(3):
            schema.get_or_create_vertex_type("User")
            schema.get_or_create_property("User", "name", PropertyType.STRING)
            schema.get_or_create_index("User", ["name"])

        # Should still have just one type
        assert schema.exists_type("User")


class TestLSMVectorIndexSchemaOps:
    """Test LSM vector index schema operations."""

    def test_list_lsm_vector_indexes(self, test_db):
        """Test list_vector_indexes includes LSM vector indexes."""
        schema = test_db.schema
        schema.create_vertex_type("Doc")
        schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        test_db.create_vector_index("Doc", "embedding", dimensions=3)

        indexes = schema.list_vector_indexes()
        assert len(indexes) > 0
        # LSM indexes usually have auto-generated names, but we check if we found something
        # The name usually contains the type name
        found = any("Doc" in idx for idx in indexes)
        assert found

    def test_get_lsm_vector_index_existing(self, test_db):
        """Test get_vector_index retrieves an existing LSM index."""
        schema = test_db.schema
        schema.create_vertex_type("Doc")
        schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Retrieve using auto detection
        index = schema.get_vector_index("Doc", "embedding")
        assert index is not None
        from arcadedb_embedded.vector import VectorIndex

        assert isinstance(index, VectorIndex)

    def test_get_lsm_vector_index_persistence(self, test_db):
        """Test that get_vector_index can load persisted LSM indexes."""
        import arcadedb_embedded as arcadedb

        schema = test_db.schema
        schema.create_vertex_type("Doc")
        schema.create_property("Doc", "embedding", "ARRAY_OF_FLOATS")

        test_db.create_vector_index("Doc", "embedding", dimensions=3)

        # Add data
        with test_db.transaction():
            v = test_db.new_vertex("Doc")
            v.set("embedding", arcadedb.to_java_float_array([1.0, 0.0, 0.0]))
            v.save()

        # Retrieve
        index = schema.get_vector_index("Doc", "embedding")
        assert index is not None
        assert index.get_size() == 1
