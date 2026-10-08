"""The wire protocols the wheel bundles are actually reachable.

The wheel ships arcadedb-postgresw, arcadedb-redisw and arcadedb-bolt as
shaded jars. Until 2026-08-01 nothing connected over any of them: the server
suite covered lifecycle, threading, jar presence and HTTP/Studio, so three
bundled protocols rested on the jars being in the archive. That is the same
shape as shipping a feature and testing that its file exists.

Each test speaks the real protocol with the real client, because the failure
these guard against is not "the port is closed" but "the plugin loaded and
then disagreed with its own client". A socket probe would pass on a half-built
plugin.

The plugins are opt-in, which is itself worth pinning: a default install
starts HTTP only, and a test that assumed otherwise would quietly stop testing
anything the day the default changed.
"""

import socket
import time
from urllib.parse import quote

import pytest

pytestmark = pytest.mark.server_wire

PG_PLUGIN = "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin"
REDIS_PLUGIN = "Redis:com.arcadedb.redis.RedisProtocolPlugin"
BOLT_PLUGIN = "Bolt:com.arcadedb.bolt.BoltProtocolPlugin"

# The shared fixture password, not a second hardcoded one. Defining our own
# tripped bandit B105 and, worse, diverged from what every other server test
# already imports.
from tests.conftest import TEST_PASSWORD as ROOT_PASSWORD  # noqa: E402


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _free_ports(*names):
    """Distinct free ports, one per name. Each socket stays bound until all are
    chosen, so the OS cannot hand the same port out twice (one port per call,
    closed before the next, let the Postgres plugin find its port taken on a
    macOS runner on 2026-10-02)."""
    socks = []
    try:
        for _ in names:
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return {n: s.getsockname()[1] for n, s in zip(names, socks)}
    finally:
        for s in socks:
            s.close()


def _wait(port, timeout=60.0):
    """Wait for a listener, then give the plugin a moment to finish binding."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.25)
    return False


@pytest.fixture
def wire_server(tmp_path):
    """A server with all three bundled wire plugins enabled."""
    from arcadedb_embedded import create_server

    ports = _free_ports("http", "postgres", "redis", "bolt")
    server = create_server(
        root_path=str(tmp_path / "databases"),
        root_password=ROOT_PASSWORD,
        config={
            "http_port": ports["http"],
            "server_plugins": ",".join([PG_PLUGIN, REDIS_PLUGIN, BOLT_PLUGIN]),
            "postgres_port": ports["postgres"],
            "redis_port": ports["redis"],
            "bolt_port": ports["bolt"],
        },
    )
    server.start()
    db = server.create_database("wiretest")
    # VERTEX, not DOCUMENT: Cypher's MATCH (i:Item) matches vertices, so a
    # document type makes the Bolt test return [] while SQL still works. That
    # is a property of the query language, not of the wire protocol, and it
    # cost a debugging round to see. A vertex is a record, so the SQL and
    # Postgres paths read it the same either way.
    db.command("sql", "CREATE VERTEX TYPE Item")
    with db.transaction():
        db.command("sql", "INSERT INTO Item SET id = 1, name = 'alpha'")
    try:
        yield server, ports
    finally:
        server.stop()


def test_plugins_are_opt_in(tmp_path):
    """A default server starts HTTP and nothing else.

    Checked against the REAL default ports rather than a random unused one:
    an earlier version of this test bound a free port and asserted it was
    closed, which would have passed whether or not the plugins were running.
    Verified 2026-08-01 that a default server logs
    "with plugins [AutoBackupSchedulerPlugin]" and leaves 5432/6379/7687 shut.

    Pinned because the other tests only mean something if enabling the
    plugins is what turns them on.
    """
    from arcadedb_embedded import create_server

    http = _free_port()
    server = create_server(
        root_path=str(tmp_path / "default"),
        root_password=ROOT_PASSWORD,
        config={"http_port": http},
    )
    server.start()
    try:
        assert _wait(http), "HTTP should serve on a default server"
        for name, port in (("postgres", 5432), ("redis", 6379), ("bolt", 7687)):
            with pytest.raises(OSError):
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    pass
    finally:
        server.stop()


def test_postgres_wire_answers_a_query(wire_server):
    """Postgres wire is the binary protocol the wheel actually ships."""
    psycopg = pytest.importorskip("psycopg")
    _, ports = wire_server
    assert _wait(ports["postgres"]), "postgres plugin never bound its port"

    with psycopg.connect(
        host="127.0.0.1",
        port=ports["postgres"],
        dbname="wiretest",
        user="root",
        password=ROOT_PASSWORD,
        connect_timeout=15,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM Item")
            rows = cur.fetchall()
    assert any("alpha" in str(r) for r in rows), rows


def test_postgres_wire_runs_cypher_with_a_bound_parameter(wire_server):
    """The `{cypher}` prefix and a bound parameter work over the Postgres wire.

    docs/guide/server.md ("Choosing a Protocol from Python") recommends this as
    the fastest route for single-row openCypher from Python, so the claim that
    the route exists and returns the right row is pinned here, not only written
    down. psycopg writes its placeholder as `%s` and sends it as `$1`.
    """
    psycopg = pytest.importorskip("psycopg")
    _, ports = wire_server
    assert _wait(ports["postgres"]), "postgres plugin never bound its port"

    with psycopg.connect(
        host="127.0.0.1",
        port=ports["postgres"],
        dbname="wiretest",
        user="root",
        password=ROOT_PASSWORD,
        connect_timeout=15,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("{cypher}MATCH (i:Item) WHERE i.id = %s RETURN i.name", (1,))
            hit = cur.fetchall()
            cur.execute("{cypher}MATCH (i:Item) WHERE i.id = %s RETURN i.name", (2,))
            miss = cur.fetchall()
    assert [str(r[0]) for r in hit] == ["alpha"], hit
    assert miss == [], miss


def test_postgres_wire_answers_arrow_adbc(wire_server):
    """Arrow's native PostgreSQL ADBC driver connects and fetches typed columns.

    Needs 26.10.1: on 26.9.1 the driver cannot connect at all ("Expected 5 or 6
    columns from type resolver pg_type query but got 0", ArcadeDB #7178).

    Declared schema properties arrive as their Arrow types, and so does a
    COMPUTED column (count(*) as int64): until 2026-09-24 the server described a
    prepared statement's computed columns as varchar before execution, and the
    driver builds its Arrow schema from that describe, so they arrived as
    strings (ArcadeDB #8285, fixed for 26.10.1). The last assertion pins the
    fixed behaviour; docs/guide/server.md states it.
    """
    pytest.importorskip(
        "pyarrow"
    )  # fetch_arrow_table needs it; the driver alone imports fine
    dbapi = pytest.importorskip("adbc_driver_postgresql.dbapi")
    server, ports = wire_server
    assert _wait(ports["postgres"]), "postgres plugin never bound its port"

    db = server.get_database("wiretest")
    db.command("sql", "CREATE DOCUMENT TYPE Typed")
    for name, kind in (
        ("n", "LONG"),
        ("s", "STRING"),
        ("x", "DOUBLE"),
        ("b", "BOOLEAN"),
    ):
        db.command("sql", f"CREATE PROPERTY Typed.{name} {kind}")
    rows = [{"n": i, "s": f"v{i}", "x": i * 0.5, "b": i % 2 == 0} for i in range(3)]
    db.insert_many("Typed", rows)

    uri = (
        f"postgresql://root:{quote(ROOT_PASSWORD, safe='')}"
        f"@127.0.0.1:{ports['postgres']}/wiretest"
    )
    with dbapi.connect(uri) as conn, conn.cursor() as cur:
        cur.execute("SELECT n, s, x, b FROM Typed ORDER BY n")
        table = cur.fetch_arrow_table()
        assert [str(f.type) for f in table.schema] == [
            "int64",
            "string",
            "double",
            "bool",
        ]
        assert table.to_pylist() == rows

        cur.execute("SELECT s FROM Typed WHERE n = $1", parameters=(2,))
        assert cur.fetchone()[0] == "v2"

        cur.execute("SELECT count(*) AS c FROM Typed")
        table = cur.fetch_arrow_table()
        assert (str(table.schema.field(0).type), table.column(0)[0].as_py()) == (
            "int64",
            3,
        ), "a computed column no longer arrives typed over ADBC (#8285): update docs/guide/server.md"


def test_redis_port_setting_is_honored(wire_server):
    """arcadedb.redis.port is honoured, like the Postgres and Bolt ports.

    This was an xfail(strict) until 2026-08-11. Redis ignored the setting and
    always bound 6379, while Postgres and Bolt honoured theirs through the
    same passthrough: ServerPlugin.configure() is handed the server's
    ContextConfiguration, and the Redis plugin dropped the argument and read
    the static GlobalConfiguration default at startService(). Bolt had carried
    the identical bug until #3809, so two plugins were converted and two were
    left behind.

    Filed as ArcadeDB #5796 on 2026-08-03, fixed upstream and closed
    2026-08-07. The strict marker is what reported the fix: the test began
    passing and CI turned red on the XPASS rather than going quietly green.
    It now guards the fix instead of the bug.

    MongoDB has the same plugin shape and is untested here, since that jar is
    excluded from the wheel.
    """
    _, ports = wire_server
    assert _wait(
        ports["redis"], timeout=20
    ), f"redis did not bind the requested port {ports['redis']}"


def test_bolt_wire_answers_a_cypher_query(wire_server):
    neo4j = pytest.importorskip("neo4j")
    _, ports = wire_server
    assert _wait(ports["bolt"]), "bolt plugin never bound its port"

    driver = neo4j.GraphDatabase.driver(
        f"bolt://127.0.0.1:{ports['bolt']}",
        auth=("root", ROOT_PASSWORD),
    )
    try:
        with driver.session(database="wiretest") as session:
            got = session.run("MATCH (i:Item) RETURN i.name AS name").data()
    finally:
        driver.close()
    assert any(r.get("name") == "alpha" for r in got), got
