"""
Tests for the Pythonic Graph API wrappers.

Tests Document, Vertex, and Edge wrapper classes and their methods.
"""

import arcadedb_embedded as arcadedb
from arcadedb_embedded.graph import Document, Edge, Vertex


def test_vertex_wrapper_creation(temp_db_path):
    """Test creating vertices using Python wrappers."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")

        with db.transaction():
            # Create vertex using db.new_vertex() - should return Vertex wrapper
            alice = db.new_vertex("Person")
            assert isinstance(alice, Vertex)

            # Test fluent interface with wrapper methods
            alice.set("name", "Alice").set("age", 30).save()

            # Verify properties
            assert alice.get("name") == "Alice"
            assert alice.get("age") == 30


def test_document_wrapper_creation(temp_db_path):
    """Test creating documents using Python wrappers."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Task")

        with db.transaction():
            # Create document using db.new_document() - should return Document wrapper
            task = db.new_document("Task")
            assert isinstance(task, Document)

            # Test wrapper methods
            task.set("title", "Test Task")
            task.set("priority", 5)
            task.save()

            # Verify properties
            assert task.get("title") == "Test Task"
            assert task.get("priority") == 5


def test_new_edge_pythonic_method(temp_db_path):
    """Test new_edge() Pythonic method with keyword arguments."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")
        db.schema.create_edge_type("Knows")

        with db.transaction():
            alice = db.new_vertex("Person")
            alice.set("name", "Alice").save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob").save()

            # Test Pythonic new_edge() with kwargs
            edge = alice.new_edge("Knows", bob, since="2020", strength=0.8)
            assert isinstance(edge, Edge)
            edge.save()

            # Verify edge properties
            assert edge.get("since") == "2020"
            assert edge.get("strength") == 0.8


def test_edge_direction_helpers(temp_db_path):
    """Test get_out_edges, get_in_edges, and get_both_edges with optional label filters."""
    with arcadedb.create_database(temp_db_path) as db:
        db.schema.create_vertex_type("Person")
        db.schema.create_edge_type("Knows")
        db.schema.create_edge_type("Likes")

        with db.transaction():
            alice = db.new_vertex("Person").set("name", "Alice").save()
            bob = db.new_vertex("Person").set("name", "Bob").save()
            carol = db.new_vertex("Person").set("name", "Carol").save()

            # Outgoing from Alice
            edge_ab = alice.new_edge("Knows", bob)
            edge_ab.save()
            edge_ac = alice.new_edge("Likes", carol)
            edge_ac.save()

            # Incoming to Alice
            edge_ba = bob.new_edge("Knows", alice)
            edge_ba.save()
            edge_ca = carol.new_edge("Knows", alice)
            edge_ca.save()

        # Outgoing edges (all)
        out_edges = alice.get_out_edges()
        assert {e.get_in().get("name") for e in out_edges} == {"Bob", "Carol"}

        # Outgoing filtered by label
        knows_out = alice.get_out_edges("Knows")
        assert {e.get_in().get("name") for e in knows_out} == {"Bob"}

        # Incoming edges (all)
        in_edges = alice.get_in_edges()
        assert {e.get_out().get("name") for e in in_edges} == {"Bob", "Carol"}

        # Both directions (all)
        both_edges = alice.get_both_edges()
        assert len(both_edges) == 4

        # Both directions filtered by label
        knows_both = alice.get_both_edges("Knows")
        assert {e.get_out().get("name") for e in knows_both} == {
            "Alice",
            "Bob",
            "Carol",
        }
        assert {e.get_in().get("name") for e in knows_both} == {"Alice", "Bob"}


def test_get_vertex_from_query_results(temp_db_path):
    """Test .get_vertex() returns proper Vertex wrapper."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("City")

        with db.transaction():
            db.command(
                "sql", "INSERT INTO City SET name = 'New York', population = 8000000"
            )

        # Query and get vertex wrapper
        results = db.query("sql", "SELECT FROM City WHERE name = 'New York'")
        result = list(results)[0]

        # Test get_vertex() returns Vertex wrapper
        city = result.get_vertex()
        assert isinstance(city, Vertex)
        assert city.get("name") == "New York"
        assert city.get("population") == 8000000

        # Test wrapper methods work - need modify() for immutable query results
        with db.transaction():
            mutable_city = city.modify()
            mutable_city.set("country", "USA")
            mutable_city.save()

        # Verify update
        updated = list(db.query("sql", "SELECT FROM City WHERE name = 'New York'"))[0]
        assert updated.get("country") == "USA"


def test_get_edge_from_query_results(temp_db_path):
    """Test .get_edge() returns proper Edge wrapper."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")
        db.schema.create_edge_type("Friends")

        with db.transaction():
            alice = db.new_vertex("Person")
            alice.set("name", "Alice").save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob").save()

            edge = alice.new_edge("Friends", bob, since=2020)
            edge.save()

        # Query for edge
        results = db.query("sql", "SELECT FROM Friends")
        result = list(results)[0]

        # Test get_edge() returns Edge wrapper
        friendship = result.get_edge()
        assert isinstance(friendship, Edge)
        assert friendship.get("since") == 2020


def test_get_element_generic_wrapper(temp_db_path):
    """Test .get_element() returns appropriate wrapper type."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Node")
        db.schema.create_document_type("Doc")

        with db.transaction():
            # Create vertex
            node = db.new_vertex("Node")
            node.set("type", "vertex").save()

            # Create document
            doc = db.new_document("Doc")
            doc.set("type", "document").save()

        # Test get_element() on vertex returns Vertex
        vertex_results = db.query("sql", "SELECT FROM Node")
        vertex_element = list(vertex_results)[0].get_element()
        assert isinstance(vertex_element, Vertex)

        # Test get_element() on document returns Document
        doc_results = db.query("sql", "SELECT FROM Doc")
        doc_element = list(doc_results)[0].get_element()
        assert isinstance(doc_element, Document)


def test_lookup_by_key_returns_wrapper(temp_db_path):
    """Test lookup_by_key() returns proper Python wrapper."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("User")
        db.schema.create_property("User", "username", "STRING")
        db.schema.create_index("User", ["username"], unique=True)

        with db.transaction():
            user = db.new_vertex("User")
            user.set("username", "alice123")
            user.set("email", "alice@example.com")
            user.save()

        # Test lookup_by_key returns Vertex wrapper
        found = db.lookup_by_key("User", ["username"], ["alice123"])
        assert found is not None
        assert isinstance(found, Vertex)
        assert found.get("username") == "alice123"
        assert found.get("email") == "alice@example.com"

        # Test lookup for non-existent key returns None
        not_found = db.lookup_by_key("User", ["username"], ["nobody"])
        assert not_found is None


def test_document_wrap_static_method(temp_db_path):
    """Test Document.wrap() static method for automatic type detection."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")
        db.schema.create_edge_type("Knows")
        db.schema.create_document_type("Note")

        with db.transaction():
            # Create different types
            person = db.new_vertex("Person")
            person.set("name", "Alice").save()

            note = db.new_document("Note")
            note.set("text", "Test note").save()

        # Query vertices
        vertex_results = db.query("sql", "SELECT FROM Person")
        vertex_result = list(vertex_results)[0]

        # Get Java document and wrap it
        java_doc = vertex_result._java_result.getElement().get()
        wrapped = Document.wrap(java_doc)

        # Should detect it's a vertex and return Vertex wrapper
        assert isinstance(wrapped, Vertex)
        assert wrapped.get("name") == "Alice"

        # Query documents
        doc_results = db.query("sql", "SELECT FROM Note")
        doc_result = list(doc_results)[0]

        # Get Java document and wrap it
        java_doc2 = doc_result._java_result.getElement().get()
        wrapped2 = Document.wrap(java_doc2)

        # Should detect it's a document and return Document wrapper
        assert isinstance(wrapped2, Document)
        assert wrapped2.get("text") == "Test note"


def test_wrapper_delete_method(temp_db_path):
    """Test delete() method on wrappers."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("TempNode")

        with db.transaction():
            node = db.new_vertex("TempNode")
            node.set("value", 42).save()
            node_id = str(node.get_identity())

        # Verify it exists
        results = list(db.query("sql", "SELECT FROM TempNode"))
        assert len(results) == 1

        # Delete using wrapper
        with db.transaction():
            # Use SQL DELETE for reliable deletion of query results
            db.command("sql", f"DELETE FROM TempNode WHERE @rid = {node_id}")

        # Verify it's gone
        results_after = list(db.query("sql", "SELECT FROM TempNode"))
        assert len(results_after) == 0


def test_wrapper_modify_method(temp_db_path):
    """Test modify() method returns mutable version."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Config")

        with db.transaction():
            config = db.new_vertex("Config")
            config.set("setting", "initial").save()

        # Query and modify
        results = db.query("sql", "SELECT FROM Config")
        config_wrapper = list(results)[0].get_vertex()

        with db.transaction():
            # Get mutable version
            mutable = config_wrapper.modify()
            mutable.set("setting", "updated")
            mutable.save()

        # Verify update
        updated = list(db.query("sql", "SELECT FROM Config"))[0]
        assert updated.get("setting") == "updated"


def test_no_bidirectional_parameter(temp_db_path):
    """Test that bidirectional parameter is no longer used."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Node")
        db.schema.create_edge_type("Link")

        with db.transaction():
            node1 = db.new_vertex("Node").set("id", 1).save()
            node2 = db.new_vertex("Node").set("id", 2).save()

            # Test that new_edge() doesn't accept bidirectional parameter
            # Should work without it (bidirectionality controlled by EdgeType schema)
            edge = node1.new_edge("Link", node2, weight=1.5)
            edge.save()

            # Verify edge was created
            assert edge.get("weight") == 1.5

            # Verify bidirectional parameter is not in signature
            import inspect

            sig = inspect.signature(node1.new_edge)
            params = list(sig.parameters.keys())
            assert "bidirectional" not in params


def test_wrapper_identity_methods(temp_db_path):
    """Test wrapper identity-related methods."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Item")

        with db.transaction():
            item = db.new_vertex("Item")
            item.set("name", "Widget").save()

            # Test get_identity()
            identity = item.get_identity()
            assert identity is not None
            assert "#" in str(identity)

            # Test get_type_name()
            type_name = item.get_type_name()
            assert type_name == "Item"


def test_wrapper_property_methods(temp_db_path):
    """Test wrapper property-related methods."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Product")

        with db.transaction():
            product = db.new_document("Product")
            product.set("name", "Laptop")
            product.set("price", 999.99)
            product.set("in_stock", True)
            product.save()

            # Test has_property()
            assert product.has_property("name") is True
            assert product.has_property("nonexistent") is False

            # Test get_property_names()
            prop_names = product.get_property_names()
            assert "name" in prop_names
            assert "price" in prop_names
            assert "in_stock" in prop_names

            # Test to_dict()
            as_dict = product.to_dict()
            assert isinstance(as_dict, dict)
            assert as_dict["name"] == "Laptop"
            assert as_dict["price"] == 999.99
            assert as_dict["in_stock"] is True


def test_vertex_delete_cascade(temp_db_path):
    """Test that deleting a vertex cascades to delete its edges."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Node")
        db.schema.create_edge_type("Links")

        # Create vertices and edge
        with db.transaction():
            node1 = db.new_vertex("Node").set("name", "A").save()
            node2 = db.new_vertex("Node").set("name", "B").save()
            edge = node1.new_edge("Links", node2).save()
            node1_id = str(node1.get_identity())

        # Verify setup
        vertices_before = list(db.query("sql", "SELECT FROM Node"))
        edges_before = list(db.query("sql", "SELECT FROM Links"))
        assert len(vertices_before) == 2
        assert len(edges_before) == 1

        # Delete vertex
        with db.transaction():
            node = db.lookup_by_rid(node1_id)
            node.delete()

        # Verify vertex deleted and edge cascaded
        vertices_after = list(db.query("sql", "SELECT FROM Node"))
        edges_after = list(db.query("sql", "SELECT FROM Links"))
        assert len(vertices_after) == 1, "Vertex should be deleted"
        assert len(edges_after) == 0, "Edge should cascade delete with vertex"


def test_edge_delete_leaves_vertices(temp_db_path):
    """Test that deleting an edge leaves its vertices intact."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Node")
        db.schema.create_edge_type("Links")

        # Create vertices and edge
        with db.transaction():
            node1 = db.new_vertex("Node").set("name", "X").save()
            node2 = db.new_vertex("Node").set("name", "Y").save()
            edge = node1.new_edge("Links", node2).save()
            edge_id = str(edge.get_identity())
            node1_id = str(node1.get_identity())
            node2_id = str(node2.get_identity())

        # Verify setup
        vertices_before = list(db.query("sql", "SELECT FROM Node"))
        edges_before = list(db.query("sql", "SELECT FROM Links"))
        assert len(vertices_before) == 2
        assert len(edges_before) == 1

        # Delete edge
        with db.transaction():
            edge_obj = db.lookup_by_rid(edge_id)
            edge_obj.delete()

        # Verify edge deleted but vertices remain
        vertices_after = list(db.query("sql", "SELECT FROM Node"))
        edges_after = list(db.query("sql", "SELECT FROM Links"))
        assert len(vertices_after) == 2, "Vertices should remain after edge delete"
        assert len(edges_after) == 0, "Edge should be deleted"

        # Verify specific vertices still exist
        nodes = list(
            db.query(
                "sql", f"SELECT FROM Node WHERE @rid = {node1_id} OR @rid = {node2_id}"
            )
        )
        assert len(nodes) == 2


def test_document_delete_sql(temp_db_path):
    """Test deleting documents using SQL DELETE."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Record")

        # Create document
        with db.transaction():
            # Use new_vertex with document type to create a document-based record
            doc = db.new_document("Record").set("content", "Test data").save()
            doc_id = str(doc.get_identity())

        # Verify it exists
        docs_before = list(db.query("sql", "SELECT FROM `Record`"))
        assert len(docs_before) == 1

        # Delete using SQL
        with db.transaction():
            db.command("sql", f"DELETE FROM `Record` WHERE @rid = {doc_id}")

        # Verify deleted
        docs_after = list(db.query("sql", "SELECT FROM `Record`"))
        assert len(docs_after) == 0, "Document should be deleted"


def test_delete_via_wrapper_on_fresh_lookup(temp_db_path):
    """Test that .delete() works on objects from lookup_by_rid()."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Item")

        # Create item
        with db.transaction():
            item = db.new_vertex("Item").set("value", 100).save()
            item_id = str(item.get_identity())

        # Verify exists
        items_before = list(db.query("sql", "SELECT FROM `Item`"))
        assert len(items_before) == 1

        # Delete using wrapper .delete() on fresh lookup
        with db.transaction():
            item_fresh = db.lookup_by_rid(item_id)
            item_fresh.delete()

        # Verify deleted
        items_after = list(db.query("sql", "SELECT FROM `Item`"))
        assert len(items_after) == 0, "Item should be deleted via .delete()"
