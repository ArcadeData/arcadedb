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

    ports = {k: _free_port() for k in ("http", "postgres", "redis", "bolt")}
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


@pytest.mark.xfail(
    reason="engine ignores arcadedb.redis.port; binds 6379 (ArcadeDB #5796)",
    strict=True,
)
def test_redis_port_setting_is_honored(wire_server):
    """arcadedb.redis.port is accepted and ignored.

    Measured 2026-08-01 on 26.8.1.dev24. With all three plugins enabled and a
    distinct port passed to each, the startup log reads:

        [PostgresNetworkListener] Listening ... on 0.0.0.0:56857   <- ours
        [BoltNetworkListener]     Listening ... on 0.0.0.0:38377   <- ours
        [RedisNetworkListener]    Listening ... on 0.0.0.0:6379    <- default

    So the setting reaches the engine (Postgres and Bolt honour theirs by the
    same passthrough) and the Redis listener does not read it. xfail(strict)
    so this turns into a failure the day it is fixed, rather than sitting here
    as a permanently green skip.

    Root-caused and filed as ArcadeDB #5796 on 2026-08-03. ServerPlugin
    .configure() is handed the server's ContextConfiguration; Postgres and
    Bolt read the port from it, while Redis drops the argument and reads the
    static GlobalConfiguration default at startService(). MongoDB has the same
    shape (untested here, that jar is excluded from the wheel). Bolt carried
    the identical bug until #3809 fixed it, so two plugins were converted and
    two were left behind.
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
