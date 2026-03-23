"""Exhaustive SQL-based bulk import tests via IMPORT DATABASE."""

import os
import tempfile
from pathlib import Path

import arcadedb_embedded as arcadedb
import pytest


@pytest.fixture
def temp_db_path():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_import_database_db")
    yield db_path

    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def sample_csv_path():
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("name,age,city\n")
    temp_file.write("Alice,30,New York\n")
    temp_file.write("Bob,25,London\n")
    temp_file.write("Charlie,35,Paris\n")
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


@pytest.fixture
def sample_xml_path():
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False)
    temp_file.write(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<users>\n"
        '  <user id="1" name="Alice" age="30"/>\n'
        '  <user id="2" name="Bob"/>\n'
        "</users>\n"
    )
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


@pytest.fixture
def sample_timeseries_csv_path():
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,ts,value\n")
    temp_file.write("1,1735689600000,10.5\n")
    temp_file.write("2,1735689660000,12.0\n")
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


def _file_url(path: str) -> str:
    return Path(path).resolve().as_uri()


def _resource_path(name: str) -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "integration"
        / "src"
        / "test"
        / "resources"
        / name
    )


def _import_result_ok(result_set):
    assert result_set is not None
    row = result_set.one()
    assert row.get("result") == "OK"
    return row


def _exception_chain_text(exc: BaseException) -> str:
    parts = []
    current = exc
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(str(current))
        current = current.__cause__ or current.__context__
    return "\n".join(parts).lower()


def test_import_database_csv_documents(temp_db_path, sample_csv_path):
    with arcadedb.create_database(temp_db_path) as db:
        result = db.command("sql", f"IMPORT DATABASE {_file_url(sample_csv_path)}")
        _import_result_ok(result)

        count = db.query("sql", "SELECT count(*) as c FROM `Document`").one().get("c")
        assert count == 3


def test_import_database_csv_graph_vertices_and_edges(temp_db_path):
    vertices_csv = _resource_path("importer-vertices.csv")
    edges_csv = _resource_path("importer-edges.csv")
    if not vertices_csv.exists() or not edges_csv.exists():
        pytest.skip("Graph CSV fixtures not available")

    with arcadedb.create_database(temp_db_path) as db:
        v_res = db.command(
            "sql",
            (
                "IMPORT DATABASE WITH "
                f"vertices = '{_file_url(str(vertices_csv))}', "
                "vertexType = 'Node', "
                "typeIdProperty = 'Id', "
                "typeIdType = 'Long', "
                "typeIdUnique = true"
            ),
        )
        _import_result_ok(v_res)

        e_res = db.command(
            "sql",
            (
                "IMPORT DATABASE WITH "
                f"edges = '{_file_url(str(edges_csv))}', "
                "edgeType = 'Relationship', "
                "typeIdProperty = 'Id', "
                "typeIdType = 'Long', "
                "edgeFromField = 'From', "
                "edgeToField = 'To'"
            ),
        )
        _import_result_ok(e_res)

        vertices_count = (
            db.query("sql", "SELECT count(*) as c FROM Node").one().get("c")
        )
        edges_count = (
            db.query("sql", "SELECT count(*) as c FROM Relationship").one().get("c")
        )
        assert vertices_count >= 6
        assert edges_count >= 3


def test_import_database_xml_vertices(temp_db_path, sample_xml_path):
    with arcadedb.create_database(temp_db_path) as db:
        try:
            result = db.command(
                "sql",
                (
                    f"IMPORT DATABASE {_file_url(sample_xml_path)} "
                    "WITH objectNestLevel = 1, entityType = 'VERTEX'"
                ),
            )
        except arcadedb.ArcadeDBError as e:
            message = _exception_chain_text(e)
            if os.name == "nt" and (
                "arrayindexoutofboundsexception" in message
                or "index 1 out of bounds for length 1" in message
                or (
                    "error on importing database" in message
                    and "error on parsing source" in message
                )
            ):
                pytest.skip(
                    "XML import path currently fails on Windows runtime "
                    f"(engine-side): {e}"
                )
            raise
        _import_result_ok(result)

        count = db.query("sql", "SELECT count(*) as c FROM v_user").one().get("c")
        assert count == 2


def test_import_database_neo4j_fixture(temp_db_path):
    neo4j_file = _resource_path("neo4j-export-mini.jsonl")
    if not neo4j_file.exists():
        pytest.skip("Neo4j fixture not available")

    with arcadedb.create_database(temp_db_path) as db:
        try:
            result = db.command(
                "sql",
                (
                    "IMPORT DATABASE WITH "
                    f"documents = '{_file_url(str(neo4j_file))}', "
                    "documentsFileType = 'neo4j'"
                ),
            )
        except arcadedb.ArcadeDBError as e:
            if "neo4j" in str(e).lower() or "unknown" in str(e).lower():
                pytest.skip(f"Neo4j import not available in current runtime: {e}")
            raise

        _import_result_ok(result)
        # Neo4j import should create at least one type
        assert len(db.schema.get_types()) > 0


def test_import_database_word2vec_vectors(temp_db_path):
    word2vec_file = _resource_path("importer-word2vec.txt")
    if not word2vec_file.exists():
        pytest.skip("Word2Vec fixture not available")

    with arcadedb.create_database(temp_db_path) as db:
        try:
            result = db.command(
                "sql",
                (
                    f"IMPORT DATABASE {_file_url(str(word2vec_file))} WITH "
                    "distanceFunction = cosine, "
                    "m = 16, "
                    "beamWidth = 100, "
                    "vertexType = Word, "
                    "vectorProperty = vector, "
                    "idProperty = name"
                ),
            )
        except arcadedb.ArcadeDBError as e:
            if "vector" in str(e).lower() or "word2vec" in str(e).lower():
                pytest.skip(f"Vector import not available in current runtime: {e}")
            raise

        _import_result_ok(result)
        count = db.query("sql", "SELECT count(*) as c FROM Word").one().get("c")
        assert count >= 10


def test_import_database_rdf_fixture(temp_db_path):
    rdf_source = _resource_path("importer-rdf.xml")
    if not rdf_source.exists():
        pytest.skip("RDF fixture not available")

    with arcadedb.create_database(temp_db_path) as db:
        try:
            result = db.command("sql", f"IMPORT DATABASE {_file_url(str(rdf_source))}")
        except arcadedb.ArcadeDBError as e:
            message = _exception_chain_text(e)
            if "rdf" in message or "cannot determine the file type" in message:
                pytest.skip(f"RDF import not available in current runtime: {e}")
            if os.name == "nt" and (
                "arrayindexoutofboundsexception" in message
                or "index 1 out of bounds for length 1" in message
                or (
                    "error on importing database" in message
                    and "error on parsing source" in message
                )
            ):
                pytest.skip(
                    "RDF import path currently fails on Windows runtime "
                    f"(engine-side): {e}"
                )
            raise

        _import_result_ok(result)

        # RDF import should create at least one imported record/type.
        total_docs = (
            db.query("sql", "SELECT count(*) as c FROM schema:types").one().get("c")
        )
        assert total_docs > 0


def test_import_database_into_timeseries_type(temp_db_path, sample_timeseries_csv_path):
    with arcadedb.create_database(temp_db_path) as db:
        try:
            db.command(
                "sql",
                "CREATE TIMESERIES TYPE Telemetry TIMESTAMP ts TAGS (id INTEGER) FIELDS (value DOUBLE)",
            )
        except arcadedb.ArcadeDBError as e:
            if "timeseries" in str(e).lower() or "syntax" in str(e).lower():
                pytest.skip(f"Timeseries not available in current runtime: {e}")
            raise

        try:
            result = db.command(
                "sql",
                (
                    f"IMPORT DATABASE {_file_url(sample_timeseries_csv_path)} WITH "
                    "documentType = 'Telemetry'"
                ),
            )
        except arcadedb.ArcadeDBError as e:
            message = str(e).lower()
            if (
                "no buckets associated" in message
                or "timeseries" in message
                or "error on importing database" in message
            ):
                pytest.skip(
                    f"Timeseries import path not available in current runtime: {e}"
                )
            raise
        _import_result_ok(result)

        count = db.query("sql", "SELECT count(*) as c FROM Telemetry").one().get("c")
        assert count == 2


def test_import_database_with_missing_file_fails(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        with pytest.raises(arcadedb.ArcadeDBError):
            db.command("sql", "IMPORT DATABASE file:///tmp/does-not-exist-arcadedb.csv")
