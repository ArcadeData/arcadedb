"""Schema statements apply immediately, and many of them batch in one transaction (ArcadeData/arcadedb#8635).

The docs said schema operations were "auto-transactional" and told users not to wrap them in a transaction. Upstream's
answer on #8635 is the other way round on both points: a schema statement is not transactional (it takes effect at
once and a rollback does not undo it), and running many of them inside one transaction is the recommended way to
create many types, because the schema is then written to disk once when the transaction ends.
"""

import arcadedb_embedded as arcadedb
import pytest


def test_many_schema_statements_in_one_transaction_all_take_effect(temp_db_path):
    """Types, properties, and indexes created inside one transaction all exist after it, and survive a reopen."""
    names = [f"Part{i}" for i in range(12)]
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            for name in names:
                db.command("sql", f"CREATE DOCUMENT TYPE {name}")
                db.command("sql", f"CREATE PROPERTY {name}.id LONG")
                db.command("sql", f"CREATE INDEX ON {name} (id) UNIQUE")
        assert [n for n in names if not db.schema.exists_type(n)] == []

        # the unique index is live: a duplicate id is refused
        with db.transaction():
            db.command("sql", "INSERT INTO Part3 SET id = 7")
        with pytest.raises(arcadedb.ArcadeDBError):
            with db.transaction():
                db.command("sql", "INSERT INTO Part3 SET id = 7")

    with arcadedb.open_database(temp_db_path) as db:
        assert [n for n in names if not db.schema.exists_type(n)] == []
        rows = db.query("sql", "SELECT count(*) AS n FROM Part3").to_list()
        assert rows[0]["n"] == 1


def test_a_rollback_does_not_undo_a_schema_statement(temp_db_path):
    """A type created inside a transaction that rolls back is still there, before and after a reopen."""

    class Boom(Exception):
        pass

    with arcadedb.create_database(temp_db_path) as db:
        with pytest.raises(Boom):
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Kept")
                db.command("sql", "INSERT INTO Kept SET id = 1")
                raise Boom()
        assert db.schema.exists_type("Kept")
        # the data write in the same block was rolled back; the schema statement was not
        rows = db.query("sql", "SELECT count(*) AS n FROM Kept").to_list()
        assert rows[0]["n"] == 0

    with arcadedb.open_database(temp_db_path) as db:
        assert db.schema.exists_type("Kept")
