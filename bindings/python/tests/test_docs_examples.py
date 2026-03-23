from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from tests.conftest import has_server_support

DOCS_ROOT = Path(__file__).resolve().parents[1] / "docs"
PYTHON_BLOCK_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)
pytestmark = pytest.mark.skipif(
    not DOCS_ROOT.exists(),
    reason="bindings/python/docs is not present on this branch",
)


def _doc_path(relative_path: str) -> Path:
    return DOCS_ROOT / relative_path


def _python_blocks(relative_path: str) -> list[str]:
    content = _doc_path(relative_path).read_text(encoding="utf-8")
    return [
        textwrap.dedent(block).strip() for block in PYTHON_BLOCK_RE.findall(content)
    ]


def _block_containing(relative_path: str, needle: str, occurrence: int = 1) -> str:
    matches = [block for block in _python_blocks(relative_path) if needle in block]
    if len(matches) < occurrence:
        raise AssertionError(
            "Could not find python block "
            f"#{occurrence} containing {needle!r} in {relative_path}"
        )
    return matches[occurrence - 1]


def _run_snippet_subprocess(
    snippet: str,
    workdir: Path,
    preamble: str = "",
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    workdir.mkdir(parents=True, exist_ok=True)
    combined_source = "\n".join([preamble, snippet])
    parts = [
        "from pathlib import Path",
        "import os",
        "",
        f"os.chdir({str(workdir)!r})",
        f'temp_db_path = str(Path({str(workdir)!r}) / "temp_db")',
    ]
    if (
        "arcadedb." in combined_source
        and "import arcadedb_embedded as arcadedb" not in combined_source
        and "from arcadedb_embedded import" not in combined_source
    ):
        parts.extend(["", "import arcadedb_embedded as arcadedb"])
    if preamble.strip():
        parts.extend(["", textwrap.dedent(preamble).strip()])
    if snippet.strip():
        parts.extend(["", textwrap.dedent(snippet).strip()])
    code = "\n".join(parts) + "\n"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=workdir,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    if expect_success and result.returncode != 0:
        raise AssertionError(
            "Snippet failed\n"
            f"stdout:\n{result.stdout}\n\n"
            f"stderr:\n{result.stderr}"
        )
    if not expect_success and result.returncode == 0:
        raise AssertionError(
            "Snippet unexpectedly succeeded\n"
            f"stdout:\n{result.stdout}\n\n"
            f"stderr:\n{result.stderr}"
        )

    return result


def _run_doc_block(
    relative_path: str,
    needle: str,
    workdir: Path,
    preamble: str = "",
    occurrence: int = 1,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    return _run_snippet_subprocess(
        _block_containing(relative_path, needle, occurrence=occurrence),
        workdir,
        preamble=preamble,
        expect_success=expect_success,
    )


def _run_doc_block_with_open_db(
    relative_path: str,
    needle: str,
    workdir: Path,
    setup: str = "",
    occurrence: int = 1,
    db_name: str = "snippet_db",
) -> subprocess.CompletedProcess[str]:
    block = _block_containing(relative_path, needle, occurrence=occurrence)
    setup_code = textwrap.indent(textwrap.dedent(setup).strip(), "    ")
    if not setup_code:
        setup_code = "    pass"
    wrapped = "\n".join(
        [
            "import arcadedb_embedded as arcadedb",
            "",
            f'with arcadedb.create_database("./{db_name}") as db:',
            setup_code,
            textwrap.indent(block, "    "),
            "",
        ]
    )
    return _run_snippet_subprocess(wrapped, workdir)


def _seed_social_network_setup() -> str:
    return textwrap.dedent("""
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.name STRING")
        db.command("sql", "CREATE PROPERTY Person.age INTEGER")
        db.command("sql", "CREATE PROPERTY Person.city STRING")
        db.command("sql", "CREATE PROPERTY Person.email STRING")
        db.command("sql", "CREATE PROPERTY Person.phone STRING")
        db.command("sql", "CREATE PROPERTY Person.verified BOOLEAN")
        db.command("sql", "CREATE PROPERTY Person.reputation FLOAT")
        db.command("sql", "CREATE EDGE TYPE FRIEND_OF UNIDIRECTIONAL")
        with db.transaction():
            for person in [
                ("Alice Johnson", 30, "Seattle", "alice@example.com", None, True, 9.5),
                ("Bob Smith", 34, "Portland", "bob@example.com", "555-0001", True, 8.0),
                ("Carol White", 28, "Seattle", None, None, False, 7.2),
                ("Dave Miller", 40, "Boston", "dave@example.com", None, True, None),
            ]:
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = ?, age = ?, city = ?, "
                        "email = ?, phone = ?, verified = ?, reputation = ?"
                    ),
                    *person,
                )

            edges = [
                ("Alice Johnson", "Bob Smith"),
                ("Alice Johnson", "Carol White"),
                ("Bob Smith", "Carol White"),
                ("Bob Smith", "Dave Miller"),
                ("Carol White", "Dave Miller"),
            ]
            for source, target in edges:
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = ?) TO "
                        "(SELECT FROM Person WHERE name = ?)"
                    ),
                    source,
                    target,
                )
        """)


def _seed_person_task_setup() -> str:
    return textwrap.dedent("""
        db.command("sql", "CREATE DOCUMENT TYPE Task")
        db.command("sql", "CREATE PROPERTY Task.title STRING")
        db.command("sql", "CREATE PROPERTY Task.completed BOOLEAN")
        db.command("sql", "CREATE PROPERTY Task.priority STRING")
        db.command("sql", "CREATE PROPERTY Task.tags LIST")
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.name STRING")
        db.command("sql", "CREATE PROPERTY Person.age INTEGER")
        db.command("sql", "CREATE PROPERTY Person.city STRING")
        db.command("sql", "CREATE PROPERTY Person.email STRING")
        with db.transaction():
            db.command(
                "sql",
                (
                    "INSERT INTO Task SET title = 'Buy groceries', "
                    "completed = false, priority = 'high', "
                    "tags = ['shopping', 'urgent']"
                ),
            )
            for name, age, city, email in [
                ("Alice", 30, "Seattle", None),
                ("Bob", 25, "Portland", "bob@example.com"),
                ("Carol", 35, "Seattle", None),
            ]:
                db.command(
                    "sql",
                    "INSERT INTO Person SET name = ?, age = ?, city = ?, email = ?",
                    name,
                    age,
                    city,
                    email,
                )
        """)


def _seed_knows_graph_setup() -> str:
    return textwrap.dedent("""
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")
        db.command("sql", "CREATE PROPERTY Person.name STRING")
        db.command("sql", "CREATE PROPERTY Person.age INTEGER")
        db.command("sql", "CREATE PROPERTY Person.email STRING")
        with db.transaction():
            db.command(
                "sql",
                (
                    "INSERT INTO Person SET name = 'Alice', age = 30, "
                    "email = 'alice@example.com'"
                ),
            )
            db.command(
                "sql",
                (
                    "INSERT INTO Person SET name = 'Bob', age = 25, "
                    "email = 'bob@example.com'"
                ),
            )
            db.command(
                "sql",
                (
                    "INSERT INTO Person SET name = 'Charlie', age = 35, "
                    "email = 'charlie@example.com'"
                ),
            )
            db.command(
                "sql",
                (
                    "CREATE EDGE Knows FROM "
                    "(SELECT FROM Person WHERE name='Alice') TO "
                    "(SELECT FROM Person WHERE name='Bob')"
                ),
            )
            db.command(
                "sql",
                (
                    "CREATE EDGE Knows FROM "
                    "(SELECT FROM Person WHERE name='Bob') TO "
                    "(SELECT FROM Person WHERE name='Charlie')"
                ),
            )
        """)


def test_docs_installation_and_distribution_examples(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_installation_"))

    _run_doc_block(
        "getting-started/installation.md",
        "ArcadeDB Python bindings version:",
        base_dir / "installation_verify",
    )
    _run_doc_block(
        "getting-started/installation.md",
        "from arcadedb_embedded.jvm import start_jvm",
        base_dir / "installation_jvm",
    )
    _run_doc_block(
        "getting-started/distributions.md",
        "Version: {arcadedb.__version__}",
        base_dir / "distributions_check",
    )
    _run_doc_block(
        "getting-started/installation.md",
        'with arcadedb.create_database("./db", jvm_kwargs={"heap_size": "8g"}) as db:',
        base_dir / "installation_jvm_kwargs",
    )
    _run_doc_block(
        "getting-started/distributions.md",
        "System: {platform.system()}",
        base_dir / "distributions_platform",
    )


def test_docs_index_and_quickstart_examples(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_quickstart_"))

    _run_doc_block("index.md", "SELECT FROM Person WHERE age > 25", base_dir / "index")
    _run_doc_block(
        "getting-started/quickstart.md",
        "# Direct database access (SQL)",
        base_dir / "embedded_sql",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        'print(f"Created database at: {db.get_database_path()}")',
        base_dir / "create_database",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        'print("Schema created!")',
        base_dir / "create_schema",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        'print("Inserted 3 records")',
        base_dir / "insert_data",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        'print(f"Name: {name}, Age: {age}")',
        base_dir / "query_data",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        "def main():",
        base_dir / "complete_example",
    )

    existing_db_preamble = textwrap.dedent("""
        import arcadedb_embedded as arcadedb
        with arcadedb.create_database("./quickstart") as db:
            db.command("sql", "CREATE DOCUMENT TYPE Person")
            with db.transaction():
                db.command("sql", "INSERT INTO Person SET name = 'Alice'")
        """)
    _run_doc_block(
        "getting-started/quickstart.md",
        'with arcadedb.open_database("./quickstart") as db:',
        base_dir / "existing_database",
        preamble=existing_db_preamble,
    )
    _run_snippet_subprocess(
        textwrap.dedent("""
            import arcadedb_embedded as arcadedb

            with arcadedb.create_database("./mydb") as db:
                db.command("sql", "CREATE DOCUMENT TYPE Person")
                db.command("sql", "CREATE PROPERTY Person.name STRING")
                db.command("sql", "CREATE PROPERTY Person.age INTEGER")

                with db.transaction():
                    for i in range(100):
                        db.command(
                            "sql",
                            "INSERT INTO Person SET name = ?, age = ?",
                            f"User{i}",
                            20 + i,
                        )
            """),
        base_dir / "batch_insert",
    )
    _run_doc_block(
        "getting-started/quickstart.md",
        (
            'with arcadedb.create_database("./mydb") as db:\n'
            '        db.command("sql", "CREATE DOCUMENT TYPE User")'
        ),
        base_dir / "error_handling",
    )


@pytest.mark.server
@pytest.mark.skipif(not has_server_support(), reason="Requires server support")
def test_docs_api_access_examples(temp_dir_factory):
    pytest.importorskip("requests")
    base_dir = Path(temp_dir_factory("docs_api_access_"))

    _run_doc_block(
        "api-access-methods.md",
        "# Direct database access - NO server needed",
        base_dir / "embedded",
    )
    _run_doc_block(
        "api-access-methods.md",
        '# "mydb" will be created at ./server_data/databases/mydb',
        base_dir / "server_managed",
    )
    _run_doc_block(
        "api-access-methods.md",
        "# Get server details",
        base_dir / "http_api",
    )
    _run_doc_block(
        "api-access-methods.md",
        "# Create database using Java API (fastest)",
        base_dir / "hybrid",
    )


def test_docs_transaction_examples(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_transactions_"))

    _run_doc_block(
        "guide/core/transactions.md",
        'db.command("sql", "CREATE DOCUMENT TYPE TransactionTest")',
        base_dir / "commit_and_rollback",
    )
    _run_doc_block(
        "guide/core/transactions.md",
        'db.command("sql", "CREATE DOCUMENT TYPE TestDoc")',
        base_dir / "schema_auto_transactional",
    )
    _run_doc_block(
        "guide/core/transactions.md",
        'db.command("sql", "CREATE VERTEX TYPE Person")',
        base_dir / "sql_inserts",
    )
    _run_doc_block(
        "guide/core/transactions.md",
        'db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")',
        base_dir / "edge_creation",
    )
    _run_doc_block(
        "guide/core/transactions.md",
        'db.command("sql", "CREATE VERTEX TYPE City")',
        base_dir / "update_after_querying",
    )


def test_docs_example_pages(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_examples_pages_"))

    simple_setup = textwrap.dedent("""
        db.command("sql", "CREATE DOCUMENT TYPE Task")
        db.command("sql", "CREATE PROPERTY Task.title STRING")
        db.command("sql", "CREATE PROPERTY Task.priority STRING")
        db.command("sql", "CREATE PROPERTY Task.completed BOOLEAN")
        db.command("sql", "CREATE PROPERTY Task.tags LIST")
        db.command("sql", "CREATE PROPERTY Task.created_date DATE")
        db.command("sql", "CREATE PROPERTY Task.due_datetime DATETIME")
        db.command("sql", "CREATE PROPERTY Task.estimated_hours FLOAT")
        db.command("sql", "CREATE PROPERTY Task.priority_score INTEGER")
        db.command("sql", "CREATE PROPERTY Task.cost DECIMAL")
        db.command("sql", "CREATE PROPERTY Task.task_id STRING")
        """)
    _run_doc_block_with_open_db(
        "examples/01_simple_document_store.md",
        "INSERT INTO Task SET",
        base_dir / "simple_document_store",
        setup=simple_setup,
    )
    social_network_script = textwrap.dedent('''
        import arcadedb_embedded as arcadedb

        with arcadedb.create_database("./social_network_db") as db:
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE PROPERTY Person.name STRING")
            db.command("sql", "CREATE PROPERTY Person.age INTEGER")
            db.command("sql", "CREATE PROPERTY Person.city STRING")
            db.command("sql", "CREATE PROPERTY Person.joined_date DATE")
            db.command("sql", "CREATE PROPERTY Person.email STRING")
            db.command("sql", "CREATE PROPERTY Person.phone STRING")
            db.command("sql", "CREATE PROPERTY Person.verified BOOLEAN")
            db.command("sql", "CREATE PROPERTY Person.reputation FLOAT")
            db.command("sql", "CREATE EDGE TYPE FRIEND_OF UNIDIRECTIONAL")
            db.command("sql", "CREATE PROPERTY FRIEND_OF.since DATE")
            db.command("sql", "CREATE PROPERTY FRIEND_OF.closeness STRING")
            db.command("sql", "CREATE INDEX ON Person (name) NOTUNIQUE_HASH")

            with db.transaction():
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = 'Alice Johnson', age = 30, "
                        "city = 'Seattle', joined_date = date('2021-01-10'), "
                        "email = 'alice@example.com', phone = null, "
                        "verified = true, reputation = 9.5"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = 'Bob Smith', age = 34, "
                        "city = 'Portland', joined_date = date('2021-02-10'), "
                        "email = 'bob@example.com', phone = '555-0001', "
                        "verified = true, reputation = 8.0"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = 'Carol White', age = 28, "
                        "city = 'Seattle', joined_date = date('2022-03-15'), "
                        "email = null, phone = null, verified = false, "
                        "reputation = 7.2"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = 'Dave Miller', age = 40, "
                        "city = 'Boston', joined_date = date('2020-07-01'), "
                        "email = 'dave@example.com', phone = null, "
                        "verified = true, reputation = null"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE VERTEX Person SET name = 'Eve Brown', age = 29, "
                        "city = 'Seattle', joined_date = date('2020-08-22'), "
                        "email = null, phone = null, verified = false, "
                        "reputation = 3.2"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = 'Alice Johnson') TO "
                        "(SELECT FROM Person WHERE name = 'Bob Smith') "
                        "SET since = date('2021-06-01'), closeness = 'close'"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = 'Alice Johnson') TO "
                        "(SELECT FROM Person WHERE name = 'Carol White') "
                        "SET since = date('2021-07-01'), closeness = 'close'"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = 'Bob Smith') TO "
                        "(SELECT FROM Person WHERE name = 'Carol White') "
                        "SET since = date('2021-08-01'), closeness = 'casual'"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = 'Carol White') TO "
                        "(SELECT FROM Person WHERE name = 'Dave Miller') "
                        "SET since = date('2022-01-01'), closeness = 'close'"
                    ),
                )
                db.command(
                    "sql",
                    (
                        "CREATE EDGE FRIEND_OF FROM "
                        "(SELECT FROM Person WHERE name = 'Bob Smith') TO "
                        "(SELECT FROM Person WHERE name = 'Dave Miller') "
                        "SET since = date('2022-02-01'), closeness = 'close'"
                    ),
                )

            sql_rows = db.query(
                "sql",
                """
                MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
                      -FRIEND_OF->
                      {type: Person, as: friend}
                RETURN friend.name as name, friend.city as city
                ORDER BY friend.name
                """,
            ).to_list()
            cypher_rows = db.query(
                "opencypher",
                """
                    MATCH (alice:Person {name: 'Alice Johnson'})
                        -[:FRIEND_OF]->(friend:Person)
                MATCH (friend)-[:FRIEND_OF]->(fof:Person)
                WHERE fof.name <> 'Alice Johnson'
                RETURN DISTINCT fof.name as name, friend.name as through_friend
                ORDER BY name
                """,
            ).to_list()

        assert [(row.get("name"), row.get("city")) for row in sql_rows] == [
            ("Bob Smith", "Portland"),
            ("Carol White", "Seattle"),
        ]
        assert [
            (row.get("name"), row.get("through_friend"))
            for row in cypher_rows
        ] == [
            ("Carol White", "Bob Smith"),
            ("Dave Miller", "Carol White"),
            ("Dave Miller", "Bob Smith"),
        ]
        ''')
    _run_snippet_subprocess(
        social_network_script,
        base_dir / "social_network_queries",
    )


def test_docs_core_query_examples(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_queries_"))

    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        'result = db.command("sqlscript", script)',
        base_dir / "sqlscript",
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "INSERT INTO JsonArrayDoc CONTENT",
        base_dir / "json_array_update",
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        'db.command("sql", "CREATE DOCUMENT TYPE BucketDoc BUCKETS 1")',
        base_dir / "truncate_bucket",
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "# Find friends using MATCH",
        base_dir / "graph_traversal",
        setup=_seed_social_network_setup(),
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "# Group by with statistics",
        base_dir / "aggregations",
        setup=_seed_person_task_setup() + textwrap.dedent("""
            db.command("sql", "CREATE DOCUMENT TYPE Test")
            with db.transaction():
                db.command("sql", "INSERT INTO Test SET value = 1")
                db.command("sql", "INSERT INTO Test SET value = 2")
            """),
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "# Create full-text index",
        base_dir / "full_text",
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "# first() - get first result",
        base_dir / "resultset_methods",
        setup=textwrap.dedent("""
            db.command("sql", "CREATE DOCUMENT TYPE Person")
            db.command("sql", "CREATE PROPERTY Person.name STRING")
            with db.transaction():
                db.command("sql", "INSERT INTO Person SET name = 'Alice'")
                db.command("sql", "INSERT INTO Person SET name = 'Bob'")
            """),
    )
    _run_doc_block_with_open_db(
        "guide/core/queries.md",
        "# Named parameters (recommended)",
        base_dir / "parameters",
        setup=textwrap.dedent("""
            db.command("sql", "CREATE DOCUMENT TYPE Person")
            db.command("sql", "CREATE PROPERTY Person.name STRING")
            db.command("sql", "CREATE PROPERTY Person.age INTEGER")
            with db.transaction():
                db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
                db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 20")
            """),
    )


def test_docs_graph_guide_examples(temp_dir_factory):
    base_dir = Path(temp_dir_factory("docs_graphs_"))

    _run_doc_block(
        "guide/graphs.md",
        'print("✅ Graph schema created")',
        base_dir / "schema_creation",
    )
    _run_doc_block_with_open_db(
        "guide/graphs.md",
        "# Create vertices and edge using OpenCypher",
        base_dir / "cypher_create_edge",
        setup=textwrap.dedent("""
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")
            """),
    )
    _run_doc_block_with_open_db(
        "guide/graphs.md",
        "MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'})",
        base_dir / "cypher_connect_existing",
        setup=_seed_knows_graph_setup(),
    )
    _run_doc_block(
        "guide/graphs.md",
        "def create_social_network():",
        base_dir / "complete_social_network",
    )
    _run_doc_block_with_open_db(
        "guide/graphs.md",
        "# Verify setup",
        base_dir / "vertex_delete_cascade",
        setup=textwrap.dedent("""
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")
            """),
    )
    _run_doc_block_with_open_db(
        "guide/graphs.md",
        "# Delete edge",
        base_dir / "edge_delete",
        setup=textwrap.dedent("""
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")
            """),
    )
    _run_doc_block(
        "guide/graphs.md",
        'with arcadedb.create_database("./graph_db") as db:',
        base_dir / "opencypher_usage",
    )
    _run_doc_block_with_open_db(
        "guide/graphs.md",
        "# Find all vertices",
        base_dir / "common_patterns",
        setup=_seed_knows_graph_setup(),
    )
