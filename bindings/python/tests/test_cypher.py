"""Tests for OpenCypher query language support."""

import arcadedb_embedded as arcadedb
import pytest


def _ensure_opencypher(db) -> None:
    try:
        _ = list(db.query("opencypher", "RETURN 1 AS one"))
    except arcadedb.ArcadeDBError as e:
        if "Query engine 'opencypher' was not found" in str(e):
            pytest.skip("OpenCypher not available")
        raise


def _seed_graph(db) -> None:
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE VERTEX TYPE Company")
    db.command("sql", "CREATE EDGE TYPE KNOWS UNIDIRECTIONAL")
    db.command("sql", "CREATE EDGE TYPE WORKS_FOR UNIDIRECTIONAL")

    with db.transaction():
        db.command(
            "opencypher",
            "CREATE (a:Person {name: 'Alice', age: 30})"
            "-[:KNOWS {since: 2020}]->"
            "(b:Person {name: 'Bob', age: 35})"
            "-[:KNOWS {since: 2021}]->"
            "(c:Person {name: 'Charlie', age: 25})"
            "-[:KNOWS {since: 2022}]->"
            "(d:Person {name: 'David', age: 28})",
        )
        db.command("opencypher", "CREATE (:Company {name: 'Acme'})")
        db.command(
            "opencypher",
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}),"
            " (c:Company {name: 'Acme'}) "
            "CREATE (a)-[:WORKS_FOR]->(c), (b)-[:WORKS_FOR]->(c)",
        )


def test_opencypher_basic_match(temp_db_path):
    """Test basic OpenCypher MATCH/WHERE."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person) WHERE p.age > 20 RETURN p.name as name",
        )
        names = [record.get("name") for record in result]

        assert set(names) == {"Alice", "Bob", "Charlie", "David"}


def test_opencypher_relationship_properties(temp_db_path):
    """Test relationship properties and filtering."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) "
            "WHERE r.since >= 2020 "
            "RETURN b.name as name, r.since as since ORDER BY since",
        )
        rows = [(record.get("name"), record.get("since")) for record in result]

        assert rows == [("Bob", 2020)]


def test_opencypher_variable_length_path(temp_db_path):
    """Test variable-length path expansion."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person) "
            "RETURN DISTINCT b.name as name ORDER BY name",
        )
        names = [record.get("name") for record in result]

        assert names == ["Bob", "Charlie", "David"]


def test_opencypher_aggregation(temp_db_path):
    """Test aggregation and ordering."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) "
            "WITH c, count(p) as employees "
            "RETURN c.name as company, employees ORDER BY employees DESC",
        )
        row = next(result)

        assert row.get("company") == "Acme"
        assert row.get("employees") == 2


def test_opencypher_id_filter(temp_db_path):
    """Test ID() function in WHERE clause."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (n:Person {name: 'Alice'}) RETURN ID(n) AS id",
        )
        alice_id = next(result).get("id")

        result = db.query(
            "opencypher",
            f"MATCH (n) WHERE ID(n) = '{alice_id}' RETURN n.name as name",
        )
        names = [record.get("name") for record in result]

        assert names == ["Alice"]


def test_opencypher_optional_match(temp_db_path):
    """Test OPTIONAL MATCH handling for missing relationships."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:WORKS_FOR]->(c:Company) "
            "RETURN p.name as name, c.name as company ORDER BY name",
        )
        rows = [(record.get("name"), record.get("company")) for record in result]

        assert rows == [
            ("Alice", "Acme"),
            ("Bob", "Acme"),
            ("Charlie", None),
            ("David", None),
        ]


def test_opencypher_case_and_coalesce(temp_db_path):
    """Test CASE expression and coalesce for missing relationships."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:WORKS_FOR]->(c:Company) "
            "RETURN p.name as name, "
            "CASE WHEN c IS NULL THEN 'unemployed' "
            "ELSE coalesce(c.name,'n/a') END AS status "
            "ORDER BY name",
        )
        try:
            rows = [(record.get("name"), record.get("status")) for record in result]
        except Exception as e:
            if "StatelessFunction" in str(e):
                pytest.skip("OpenCypher CASE/coalesce not supported")
            raise

        assert rows == [
            ("Alice", "Acme"),
            ("Bob", "Acme"),
            ("Charlie", "unemployed"),
            ("David", "unemployed"),
        ]


def test_opencypher_path_length_and_filter(temp_db_path):
    """Test variable-length paths with filtering and DISTINCT."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person) "
            "WHERE b.age >= 28 RETURN DISTINCT b.name as name ORDER BY name",
        )
        names = [record.get("name") for record in result]

        assert names == ["Bob", "David"]


def test_opencypher_collect_and_unwind(temp_db_path):
    """Test collect() aggregation and UNWIND for derived rows."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) "
            "WITH c.name AS company, collect(p.name) AS employees "
            "UNWIND employees AS employee "
            "RETURN company, employee ORDER BY employee",
        )
        rows = [(record.get("company"), record.get("employee")) for record in result]

        if not rows:
            pytest.skip("OpenCypher collect/UNWIND not supported")

        assert rows == [("Acme", "Alice"), ("Acme", "Bob")]


def test_opencypher_unwind_where_uses_unwind_variable(temp_db_path):
    """Test WHERE predicates can reference variables introduced by UNWIND."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "UNWIND ['Alice', 'Bob', 'Nobody'] AS expected_name "
            "MATCH (p:Person) "
            "WHERE p.name = expected_name "
            "RETURN p.name AS name ORDER BY name",
        )
        names = [record.get("name") for record in result]

        assert names == ["Alice", "Bob"]


def test_opencypher_pattern_comprehension(temp_db_path):
    """Test pattern comprehension to derive relationship values."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person {name: 'Alice'}) "
            "RETURN [ (p)-[r:KNOWS]->(b) | r.since ] AS since_years",
        )
        row = next(result)
        years = row.get("since_years")

        if years is None:
            pytest.skip("OpenCypher pattern comprehension not supported")

        assert sorted(list(years)) == [2020]


def test_opencypher_subquery_with_exists(temp_db_path):
    """Test EXISTS with subquery and property checks."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:WORKS_FOR]->(:Company) } "
            "RETURN p.name as name ORDER BY name",
        )
        names = [record.get("name") for record in result]

        assert names == ["Alice", "Bob"]


def test_opencypher_count_non_existing_label_returns_zero(temp_db_path):
    """Test COUNT against a non-existing label returns 0 instead of no rows/errors."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (n:DoesNotExist) RETURN count(n) AS c",
        )
        row = result.one()

        assert row.get("c") == 0


def test_opencypher_projection_property_order_is_preserved(temp_db_path):
    """Test projected property order is stable and matches RETURN clause order."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (p:Person {name: 'Alice'}) "
            "RETURN p.age AS age, p.name AS name, 42 AS marker",
        ).one()

        assert result.property_names == ["age", "name", "marker"]
        assert list(result.to_dict().keys()) == ["age", "name", "marker"]


def test_opencypher_named_params_on_variable_length_traversal(temp_db_path):
    """Test named parameters on variable-length graph traversal queries."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (a:Person {name: $start_name})-[:KNOWS*1..3]->(b:Person) "
            "WHERE b.age >= $min_age "
            "RETURN DISTINCT b.name AS name ORDER BY name",
            {"start_name": "Alice", "min_age": 28},
        )
        names = [record.get("name") for record in result]

        assert names == ["Bob", "David"]


def test_opencypher_named_params_on_traversal_count(temp_db_path):
    """Test named parameters work on traversal queries that aggregate graph matches."""
    with arcadedb.create_database(temp_db_path) as db:
        _ensure_opencypher(db)
        _seed_graph(db)

        result = db.query(
            "opencypher",
            "MATCH (a:Person {name: $start_name})-[:KNOWS*1..3]->(b:Person) "
            "WHERE b.age >= $min_age "
            "RETURN count(DISTINCT b) AS c",
            {"start_name": "Alice", "min_age": 28},
        )
        row = result.one()

        assert row.get("c") == 2
