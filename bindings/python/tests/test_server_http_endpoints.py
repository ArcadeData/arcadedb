"""The three server HTTP features the bindings document but do not wrap
(guide/server.md, "Transactions, Database Commands and Time-Series Writes over
HTTP"): a transaction spanning several requests through arcadedb-session-id,
server-level database commands, and line-protocol writes to a TIMESERIES type.

Pinned from the bindings' side so a server change surfaces here rather than in
a benchmark harness that reads the server's Java sources to find out.
"""

import time

import arcadedb_embedded as arcadedb
import pytest
from tests.conftest import TEST_PASSWORD


@pytest.fixture
def http_server(tmp_path):
    requests = pytest.importorskip("requests")
    server = arcadedb.create_server(
        root_path=str(tmp_path / "srv"), root_password=TEST_PASSWORD
    )
    server.start()
    time.sleep(1)
    base = f"http://localhost:{server.get_http_port()}"
    s = requests.Session()
    s.auth = ("root", TEST_PASSWORD)
    for _ in range(30):
        try:
            if s.get(f"{base}/api/v1/ready", timeout=5).status_code == 204:
                break
        except Exception:  # noqa: BLE001
            # Readiness poll: the server is not listening yet; keep waiting.
            time.sleep(0.5)
            continue
        time.sleep(0.5)
    server.create_database("httpx")
    try:
        yield s, base, server
    finally:
        s.close()
        try:
            server.stop()
        except Exception as exc:  # noqa: BLE001
            import warnings

            warnings.warn(f"server fixture: stop failed: {exc!r}", stacklevel=1)


def _cmd(s, base, db, sql, headers=None, kind="command", language="sql"):
    r = s.post(
        f"{base}/api/v1/{kind}/{db}",
        json={"language": language, "command": sql},
        headers=headers,
        timeout=30,
    )
    assert r.status_code == 200, r.text
    return r.json().get("result", [])


def test_transaction_spans_requests_and_rolls_back(http_server):
    s, base, _ = http_server
    _cmd(s, base, "httpx", "CREATE DOCUMENT TYPE T")
    r = s.post(f"{base}/api/v1/begin/httpx", timeout=30)
    assert r.status_code in (200, 204), r.text
    sid = r.headers.get("arcadedb-session-id")
    assert sid, "begin must return arcadedb-session-id"
    h = {"arcadedb-session-id": sid}
    _cmd(s, base, "httpx", "INSERT INTO T SET n = 1", headers=h)
    _cmd(s, base, "httpx", "INSERT INTO T SET n = 2", headers=h)
    # Not committed yet: a session-less read sees nothing.
    assert (
        _cmd(s, base, "httpx", "SELECT count(*) AS c FROM T", kind="query")[0]["c"] == 0
    )
    assert s.post(
        f"{base}/api/v1/rollback/httpx", headers=h, timeout=30
    ).status_code in (200, 204)
    assert (
        _cmd(s, base, "httpx", "SELECT count(*) AS c FROM T", kind="query")[0]["c"] == 0
    )
    # And the committing form lands both rows at once.
    sid2 = s.post(f"{base}/api/v1/begin/httpx", timeout=30).headers[
        "arcadedb-session-id"
    ]
    h2 = {"arcadedb-session-id": sid2}
    _cmd(s, base, "httpx", "INSERT INTO T SET n = 3", headers=h2)
    _cmd(s, base, "httpx", "INSERT INTO T SET n = 4", headers=h2)
    assert s.post(
        f"{base}/api/v1/commit/httpx", headers=h2, timeout=30
    ).status_code in (200, 204)
    assert (
        _cmd(s, base, "httpx", "SELECT count(*) AS c FROM T", kind="query")[0]["c"] == 2
    )


def test_close_and_open_database_commands(http_server):
    s, base, _ = http_server
    _cmd(s, base, "httpx", "CREATE DOCUMENT TYPE U")
    _cmd(s, base, "httpx", "INSERT INTO U SET n = 1")
    r = s.post(
        f"{base}/api/v1/server", json={"command": "close database httpx"}, timeout=30
    )
    assert r.status_code == 200, r.text
    r = s.post(
        f"{base}/api/v1/server", json={"command": "open database httpx"}, timeout=30
    )
    assert r.status_code == 200, r.text
    assert (
        _cmd(s, base, "httpx", "SELECT count(*) AS c FROM U", kind="query")[0]["c"] == 1
    )


def test_timeseries_line_protocol_write(http_server):
    s, base, _ = http_server
    _cmd(
        s,
        base,
        "httpx",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts "
        "TAGS (sensor STRING) FIELDS (value DOUBLE)",
    )
    body = "\n".join(
        f"Reading,sensor=s1 value={float(i)} {1700000000 + i}" for i in range(100)
    )
    r = s.post(
        f"{base}/api/v1/ts/httpx/write?precision=s",
        data=body.encode(),
        headers={"Content-Type": "text/plain"},
        timeout=30,
    )
    assert r.status_code in (200, 204), r.text
    rows = _cmd(s, base, "httpx", "SELECT count(*) AS n FROM Reading", kind="query")
    assert rows and int(rows[0]["n"]) == 100, rows
    last = _cmd(
        s,
        base,
        "httpx",
        "SELECT ts, value FROM Reading WHERE sensor = 's1' ORDER BY ts DESC LIMIT 1",
        kind="query",
    )
    assert last and float(last[0]["value"]) == 99.0, last


def test_embedded_and_http_projections_agree(http_server):
    """The decomposition in example 23 rests on both paths answering the same
    rows; the wire format may cost time, never content."""
    s, base, server = http_server
    _cmd(s, base, "httpx", "CREATE DOCUMENT TYPE R")
    _cmd(s, base, "httpx", "CREATE PROPERTY R.id LONG")
    _cmd(s, base, "httpx", "CREATE PROPERTY R.amount DOUBLE")
    _cmd(
        s,
        base,
        "httpx",
        ";".join(f"INSERT INTO R SET id = {i}, amount = {i / 10}" for i in range(300)),
        kind="command",
        language="sqlscript",
    )
    via_http = _cmd(
        s, base, "httpx", "SELECT id, amount FROM R ORDER BY id LIMIT 100", kind="query"
    )
    assert len(via_http) == 100 and via_http[0]["id"] == 0 and via_http[-1]["id"] == 99
    # The same statement through the embedded handle of the served database.
    embedded = (
        server.get_database("httpx")
        .query("sql", "SELECT id, amount FROM R ORDER BY id LIMIT 100")
        .to_list()
    )
    assert [(r["id"], r["amount"]) for r in embedded] == [
        (r["id"], r["amount"]) for r in via_http
    ]
