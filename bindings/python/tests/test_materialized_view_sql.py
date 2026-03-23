"""Materialized view SQL coverage for Python bindings."""

import arcadedb_embedded as arcadedb


def test_materialized_view_sql_end_to_end(temp_db_path):
    """Create, refresh, inspect metadata, and drop a materialized view."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Account")

        with db.transaction():
            db.command("sql", "INSERT INTO Account SET name = 'Alice', active = true")
            db.command("sql", "INSERT INTO Account SET name = 'Bob', active = false")

        db.command(
            "sql",
            "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true",
        )
        db.command(
            "sql",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS ActiveAccounts AS SELECT name FROM Account WHERE active = true",
        )

        initial_rows = list(db.query("sql", "SELECT FROM ActiveAccounts"))
        assert len(initial_rows) == 1
        assert initial_rows[0].get("name") == "Alice"

        metadata = list(
            db.query(
                "sql",
                "SELECT FROM schema:materializedViews WHERE name = 'ActiveAccounts'",
            )
        )
        assert len(metadata) == 1
        assert metadata[0].get("name") == "ActiveAccounts"
        assert metadata[0].get("status") in ("VALID", "BUILDING")

        with db.transaction():
            db.command("sql", "INSERT INTO Account SET name = 'Charlie', active = true")

        db.command("sql", "REFRESH MATERIALIZED VIEW ActiveAccounts")
        refreshed_names = {
            row.get("name") for row in db.query("sql", "SELECT FROM ActiveAccounts")
        }
        assert refreshed_names == {"Alice", "Charlie"}

        db.command("sql", "DROP MATERIALIZED VIEW ActiveAccounts")
        db.command("sql", "DROP MATERIALIZED VIEW IF EXISTS ActiveAccounts")


def test_materialized_view_sql_alter_refresh_mode_and_manual_refresh(temp_db_path):
    """Alter refresh mode and verify manual refresh updates rows."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Account")

        with db.transaction():
            db.command("sql", "INSERT INTO Account SET name = 'Alice', active = true")
            db.command("sql", "INSERT INTO Account SET name = 'Bob', active = false")

        db.command(
            "sql",
            "CREATE MATERIALIZED VIEW MVRefresh AS SELECT name FROM Account WHERE active = true",
        )
        db.command("sql", "ALTER MATERIALIZED VIEW MVRefresh REFRESH MANUAL")

        metadata = list(
            db.query(
                "sql",
                "SELECT FROM schema:materializedViews WHERE name = 'MVRefresh'",
            )
        )
        assert len(metadata) == 1
        assert metadata[0].get("refreshMode") == "MANUAL"

        with db.transaction():
            db.command("sql", "INSERT INTO Account SET name = 'Charlie', active = true")

        before_refresh = {
            row.get("name") for row in db.query("sql", "SELECT FROM MVRefresh")
        }
        assert before_refresh == {"Alice"}

        db.command("sql", "REFRESH MATERIALIZED VIEW MVRefresh")
        after_refresh = {
            row.get("name") for row in db.query("sql", "SELECT FROM MVRefresh")
        }
        assert after_refresh == {"Alice", "Charlie"}

        db.command("sql", "DROP MATERIALIZED VIEW MVRefresh")


def test_materialized_view_sql_drop_if_exists_is_idempotent(temp_db_path):
    """`DROP MATERIALIZED VIEW IF EXISTS` should be safely repeatable."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "DROP MATERIALIZED VIEW IF EXISTS DoesNotExist")
        db.command("sql", "DROP MATERIALIZED VIEW IF EXISTS DoesNotExist")
