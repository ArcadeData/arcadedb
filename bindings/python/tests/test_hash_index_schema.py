"""Hash index schema API coverage for Python bindings."""

import arcadedb_embedded as arcadedb
import pytest


def test_hash_index_schema_create_discover_and_query(temp_db_path):
    """Create HASH index through schema API and verify index + query behavior."""
    with arcadedb.create_database(temp_db_path) as db:
        schema = db.schema
        schema.create_document_type("Product")
        schema.create_property("Product", "sku", arcadedb.PropertyType.STRING)

        with db.transaction():
            db.command("sql", "INSERT INTO Product SET sku = 'A-001', price = 10")
            db.command("sql", "INSERT INTO Product SET sku = 'A-002', price = 20")
            db.command("sql", "INSERT INTO Product SET sku = 'A-003', price = 30")

        try:
            created_index = schema.create_index(
                "Product",
                ["sku"],
                unique=False,
                index_type=arcadedb.IndexType.HASH,
            )
        except arcadedb.ArcadeDBError as e:
            if "Invalid index type 'HASH'" in str(e):
                pytest.skip("HASH index type is not available in this packaged runtime")
            raise
        assert created_index is not None

        indexes = schema.get_indexes()
        product_sku_indexes = [
            idx
            for idx in indexes
            if idx.getTypeName() == "Product" and "sku" in idx.getName()
        ]
        assert product_sku_indexes, "Expected Product[sku] HASH index to exist"
        assert any(str(idx.getType()) == "HASH" for idx in product_sku_indexes)

        for idx in product_sku_indexes:
            assert schema.exists_index(idx.getName()) is True
            assert schema.get_index_by_name(idx.getName()) is not None

        rows = list(db.query("sql", "SELECT FROM Product WHERE sku = 'A-002'"))
        assert len(rows) == 1
        assert rows[0].get("sku") == "A-002"
        assert rows[0].get("price") == 20


def test_hash_index_schema_get_or_create_and_force_drop(temp_db_path):
    """HASH index creation is idempotent through get_or_create and removable via force drop."""
    with arcadedb.create_database(temp_db_path) as db:
        schema = db.schema
        schema.create_document_type("Inventory")
        schema.create_property("Inventory", "code", arcadedb.PropertyType.STRING)

        try:
            idx1 = schema.get_or_create_index(
                "Inventory",
                ["code"],
                unique=False,
                index_type=arcadedb.IndexType.HASH,
            )
            idx2 = schema.get_or_create_index(
                "Inventory",
                ["code"],
                unique=False,
                index_type=arcadedb.IndexType.HASH,
            )
        except arcadedb.ArcadeDBError as e:
            if "Invalid index type 'HASH'" in str(e):
                pytest.skip("HASH index type is not available in this packaged runtime")
            raise

        assert idx1 is not None
        assert idx2 is not None
        assert idx1.getName() == idx2.getName()

        index_name = idx1.getName()
        assert schema.exists_index(index_name)

        schema.drop_index(index_name, force=True)
        assert not schema.exists_index(index_name)
