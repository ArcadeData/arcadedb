"""
Core functionality tests for ArcadeDB Python bindings.
These tests work with ALL distributions (headless, minimal, full).
"""

import pytest
import arcadedb_embedded as arcadedb


def test_database_creation(temp_db_path):
    """Test creating a new database."""
    db = arcadedb.create_database(temp_db_path)
    assert db.is_open()
    db.close()
    assert not db.is_open()


def test_database_operations(temp_db_path):
    """Test basic database operations."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create a document type
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE TestDoc")
        
        # Insert data
        with db.transaction():
            db.command("sql", "INSERT INTO TestDoc SET name = 'test', value = 42")
        
        # Query data
        result = db.query("sql", "SELECT FROM TestDoc WHERE name = 'test'")
        records = list(result)
        
        assert len(records) == 1
        record = records[0]
        assert record.get_property('name') == 'test'
        assert record.get_property('value') == 42


def test_transactions(temp_db_path):
    """Test transaction support."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE TransactionTest")
        
        # Test successful transaction
        with db.transaction():
            db.command("sql", "INSERT INTO TransactionTest SET id = 1")
            db.command("sql", "INSERT INTO TransactionTest SET id = 2")
        
        # Verify data was committed
        result = db.query("sql", "SELECT count(*) as count FROM TransactionTest")
        count = list(result)[0].get_property('count')
        assert count == 2
        
        # Test transaction rollback
        try:
            with db.transaction():
                db.command("sql", "INSERT INTO TransactionTest SET id = 3")
                raise Exception("Intentional error")
        except Exception:
            pass  # Expected
        
        # Verify rollback worked
        result = db.query("sql", "SELECT count(*) as count FROM TransactionTest")
        count = list(result)[0].get_property('count')
        assert count == 2  # Should still be 2


def test_graph_operations(temp_db_path):
    """Test graph operations."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Knows")
        
        # Create vertices
        with db.transaction():
            db.command("sql", "CREATE VERTEX Person SET name = 'Alice'")
            db.command("sql", "CREATE VERTEX Person SET name = 'Bob'")
        
        # Create edge
        with db.transaction():
            db.command("sql", """
                CREATE EDGE Knows
                FROM (SELECT FROM Person WHERE name = 'Alice')
                TO (SELECT FROM Person WHERE name = 'Bob')
            """)
        
        # Test graph traversal
        result = db.query("sql", """
            SELECT expand(out('Knows').name)
            FROM Person
            WHERE name = 'Alice'
        """)
        
        names = [record.get_property('value') for record in result]
        assert 'Bob' in names


def test_error_handling():
    """Test error handling."""
    # Test with invalid path
    with pytest.raises(arcadedb.ArcadeDBError):
        arcadedb.open_database("/invalid/path/that/does/not/exist")


def test_result_methods(temp_db_path):
    """Test Result object methods."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE ResultTest")
            db.command("sql", """
                INSERT INTO ResultTest SET
                name = 'test',
                number = 42,
                flag = true,
                nested = {'key': 'value'}
            """)
        
        result = db.query("sql", "SELECT FROM ResultTest")
        record = list(result)[0]
        
        # Test property access
        assert record.has_property('name')
        assert record.get_property('name') == 'test'
        assert not record.has_property('nonexistent')
        
        # Test property names
        prop_names = record.get_property_names()
        assert 'name' in prop_names
        assert 'number' in prop_names
        
        # Test to_dict
        data = record.to_dict()
        assert isinstance(data, dict)
        assert data['name'] == 'test'
        assert data['number'] == 42
        
        # Test to_json
        json_str = record.to_json()
        assert isinstance(json_str, str)
        assert 'test' in json_str


def test_cypher_queries(temp_db_path):
    """Test Cypher query language support."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE FRIEND_OF")
        
        # Insert data using Cypher (if available)
        try:
            with db.transaction():
                db.command("cypher", "CREATE (p:Person {name: 'Alice', age: 30})")
                db.command("cypher", "CREATE (p:Person {name: 'Bob', age: 25})")
            
            # Query using Cypher
            result = db.query("cypher",
                            "MATCH (p:Person) WHERE p.age > 20 RETURN p.name as name")
            names = [record.get_property('name') for record in result]
            
            assert len(names) == 2
            assert 'Alice' in names
            assert 'Bob' in names
        except arcadedb.ArcadeDBError as e:
            if "Query engine 'cypher' was not found" in str(e):
                pytest.skip("Cypher query engine not available in this distribution")
            raise


def test_vector_search(temp_db_path):
    """Test vector embeddings with HNSW similarity search.
    
    This test creates an HNSW vector index using the simplified Python API
    to enable nearest-neighbor similarity search on vector embeddings.
    Works with NumPy arrays (preferred) or plain Python lists.
    """
    # Try to use NumPy if available
    try:
        import numpy as np
        use_numpy = True
    except ImportError:
        use_numpy = False
    
    with arcadedb.create_database(temp_db_path) as db:
        # Create vertex type for vector embeddings
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE EmbeddingNode")
            db.command("sql", "CREATE PROPERTY EmbeddingNode.name STRING")
            # Use ARRAY_OF_FLOATS for vector property (required for HNSW)
            db.command("sql", "CREATE PROPERTY EmbeddingNode.vector ARRAY_OF_FLOATS")
            # Create unique index on name for lookups
            db.command("sql", "CREATE INDEX ON EmbeddingNode (name) UNIQUE")
        
        # Insert sample word embeddings (4-dimensional for simplicity)
        # In reality, embeddings would be 300-1536 dimensions
        if use_numpy:
            embeddings = [
                ("king", np.array([0.5, 0.3, 0.1, 0.2])),
                ("queen", np.array([0.52, 0.32, 0.08, 0.18])),  # Similar to king
                ("man", np.array([0.48, 0.25, 0.15, 0.22])),
                ("woman", np.array([0.50, 0.28, 0.12, 0.20])),
                ("cat", np.array([0.1, 0.8, 0.6, 0.3])),        # Different cluster
                ("dog", np.array([0.12, 0.82, 0.58, 0.32])),    # Similar to cat
            ]
        else:
            embeddings = [
                ("king", [0.5, 0.3, 0.1, 0.2]),
                ("queen", [0.52, 0.32, 0.08, 0.18]),  # Similar to king
                ("man", [0.48, 0.25, 0.15, 0.22]),
                ("woman", [0.50, 0.28, 0.12, 0.20]),
                ("cat", [0.1, 0.8, 0.6, 0.3]),        # Different cluster
                ("dog", [0.12, 0.82, 0.58, 0.32]),    # Similar to cat
            ]
        
        with db.transaction():
            for name, vector in embeddings:
                # Use the helper function to convert to Java array
                java_vector = arcadedb.to_java_float_array(vector)
                
                # Create vertex with Java array as vector property
                java_db = db._java_db
                vertex = java_db.newVertex("EmbeddingNode")
                vertex.set("name", name)
                vertex.set("vector", java_vector)
                vertex.save()
        
        # Test 1: Verify we can query and retrieve stored vectors
        result = db.query("sql",
                          "SELECT FROM EmbeddingNode WHERE name = 'king'")
        records = list(result)
        assert len(records) == 1
        
        king_node = records[0]
        assert king_node.has_property('vector')
        assert king_node.has_property('name')
        assert king_node.get_property('name') == 'king'
        
        vector = king_node.get_property('vector')
        # Convert to Python/NumPy array
        vector = arcadedb.to_python_array(vector, use_numpy=use_numpy)
        
        if use_numpy:
            assert isinstance(vector, np.ndarray)
            assert vector.shape == (4,)
        else:
            assert isinstance(vector, list)
            assert len(vector) == 4
        
        assert abs(vector[0] - 0.5) < 0.01  # Verify first component
        
        # Test 2: Create HNSW vector index using simplified API
        print("\nCreating HNSW vector index...")
        
        # Create index with simplified API
        with db.transaction():
            index = db.create_vector_index(
                vertex_type="EmbeddingNode",
                vector_property="vector",
                dimensions=4,
                id_property="name",  # Use name as ID
                distance_function="cosine",
                m=16,
                ef=128,
                max_items=1000
            )
        
        print("  ✓ Created vector index")
        
        # Index existing vertices
        print("  Indexing existing vertices...")
        result = db.query("sql", "SELECT FROM EmbeddingNode")
        indexed_count = 0
        
        with db.transaction():
            for record in result:
                vertex = record._java_result.getElement().get().asVertex()
                index.add_vertex(vertex)
                indexed_count += 1
        
        print(f"  ✓ Indexed {indexed_count} vertices")
        
        # Test 3: Perform similarity search with NumPy/list arrays
        print("\nTesting nearest-neighbor similarity search...")
        
        # Search for neighbors of "king" - should find "queen", "man", "woman"
        if use_numpy:
            king_vector = np.array([0.5, 0.3, 0.1, 0.2])
        else:
            king_vector = [0.5, 0.3, 0.1, 0.2]
        
        neighbors = index.find_nearest(king_vector, k=3)
        
        # Extract names from results
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]
        
        print(f"  3 nearest neighbors to 'king': {neighbor_names}")
        assert "queen" in neighbor_names, "Expected 'queen' to be near 'king'"
        assert "man" in neighbor_names or "woman" in neighbor_names
        assert "cat" not in neighbor_names, "'cat' should be in different cluster"
        assert "dog" not in neighbor_names, "'dog' should be in different cluster"
        print("  ✓ Similarity search returns correct neighbors!")
        
        # Search for neighbors of "cat" - should find "dog"
        if use_numpy:
            cat_vector = np.array([0.1, 0.8, 0.6, 0.3])
        else:
            cat_vector = [0.1, 0.8, 0.6, 0.3]
        
        neighbors = index.find_nearest(cat_vector, k=2)
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]
        
        print(f"  2 nearest neighbors to 'cat': {neighbor_names}")
        assert "dog" in neighbor_names, "Expected 'dog' to be near 'cat'"
        assert "king" not in neighbor_names, "'king' should be in different cluster"
        print("  ✓ Different cluster correctly separated!")
        
        print("\n✓ HNSW vector index fully functional with NumPy support!")
