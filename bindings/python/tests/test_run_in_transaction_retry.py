"""
``Database.run_in_transaction`` decides what to retry by Java TYPE (#9629).

It used to search the error text for two class names, ``ConcurrentModificationException``
and ``NeedRetryException``, so every other ``NeedRetryException`` subclass escaped on
its first attempt with the retry budget untouched - a ``LockTimeoutException`` from a
contended commit above all - and a ``DuplicatedKeyException`` was never given the one
retry ``LocalDatabase.transaction()`` gives it. The decision now matches the Java loop:
any ``NeedRetryException`` anywhere in the cause chain is retried, a
``DuplicatedKeyException`` once, a block that already committed part of its work
(``BATCH n``) never, and the wait a refusal asks for is honored.

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import threading
import time

import arcadedb_embedded as arcadedb
import jpype
import pytest
from arcadedb_embedded import ArcadeDBError


def _java(name):
    return jpype.JClass(f"com.arcadedb.exception.{name}")


@pytest.fixture
def db(temp_db_path):
    database = arcadedb.create_database(temp_db_path)
    database.command("sql", "CREATE DOCUMENT TYPE R")
    database.command("sql", "CREATE PROPERTY R.k INTEGER")
    database.command("sql", "CREATE PROPERTY R.v INTEGER")
    yield database
    database.drop()


def _failing(db, failures, make_error):
    """A block that inserts a row, then fails ``failures`` times with ``make_error()``."""
    attempts = []

    def fn():
        attempts.append(1)
        db.command("sql", "INSERT INTO R SET k = 1, v = 0")
        if len(attempts) <= failures:
            raise make_error()
        return "done"

    return fn, attempts


def _count(db):
    return db.query("sql", "SELECT count(*) AS c FROM R").first().get("c")


def _wrapped(java_error):
    """The shape every Database method raises: an ArcadeDBError from the Java exception."""
    try:
        raise java_error
    except Exception as e:
        try:
            raise ArcadeDBError(f"Failed to commit transaction: {e}") from e
        except ArcadeDBError as wrapped:
            return wrapped


@pytest.mark.parametrize(
    "make_error",
    [
        pytest.param(
            lambda: _wrapped(
                _java("LockTimeoutException")("Timeout on locking file 1")
            ),
            id="lock-timeout",
        ),
        pytest.param(
            lambda: _java("LockTimeoutException")("raised as is"), id="raw-lock-timeout"
        ),
        pytest.param(
            lambda: _wrapped(_java("ConcurrentModificationException")("conflict")),
            id="cme",
        ),
        pytest.param(
            lambda: _wrapped(
                _java("CommandExecutionException")(
                    "Error on executing the statement",
                    _java("ConcurrentModificationException")("inside"),
                )
            ),
            id="cme-wrapped-by-the-engine",
        ),
    ],
)
def test_every_need_retry_exception_is_retried(db, make_error):
    fn, attempts = _failing(db, 3, make_error)
    assert db.run_in_transaction(fn, retries=5, backoff_s=0) == "done"
    assert len(attempts) == 4
    assert _count(db) == 1  # the failed attempts were rolled back
    assert db.is_transaction_active() is False


def test_the_budget_is_spent_before_the_conflict_reaches_the_caller(db):
    fn, attempts = _failing(
        db, 100, lambda: _wrapped(_java("LockTimeoutException")("timeout"))
    )
    with pytest.raises(ArcadeDBError, match="LockTimeoutException"):
        db.run_in_transaction(fn, retries=4, backoff_s=0)
    assert len(attempts) == 5
    assert _count(db) == 0


def test_a_duplicated_key_is_retried_once_as_the_engine_does(db):
    db.command("sql", "CREATE INDEX ON R (k) UNIQUE")
    with db.transaction():
        db.command("sql", "INSERT INTO R SET k = 7, v = 0")
    attempts = []

    def fn():
        attempts.append(1)
        db.command("sql", "INSERT INTO R SET k = 7, v = 1")

    with pytest.raises(ArcadeDBError) as raised:
        db.run_in_transaction(fn, retries=10, backoff_s=0)
    # A genuine duplicate fails the same way every time: one retry, not ten.
    assert len(attempts) == 2
    assert "DuplicatedKeyException" in str(raised.value)
    assert _count(db) == 1


def test_a_class_name_in_the_message_is_not_a_verdict(db):
    """The text no longer decides: an error with no Java conflict behind it is not retried."""
    fn, attempts = _failing(
        db, 100, lambda: ArcadeDBError("looks like a NeedRetryException, is not one")
    )
    with pytest.raises(ArcadeDBError):
        db.run_in_transaction(fn, retries=5, backoff_s=0)
    assert len(attempts) == 1


def test_an_ordinary_error_is_not_retried(db):
    fn, attempts = _failing(db, 100, lambda: KeyError("boom"))
    with pytest.raises(KeyError):
        db.run_in_transaction(fn, retries=5, backoff_s=0)
    assert len(attempts) == 1
    assert _count(db) == 0


def _refusing_with(retry_after_ms):
    def refusal():
        error = _java("NeedRetryException")("not ready yet")
        error.setRetryAfterMs(retry_after_ms)
        return _wrapped(error)

    return refusal


@pytest.fixture
def recorded_sleeps(monkeypatch):
    """The waits run_in_transaction asks for, recorded instead of slept.

    The REQUESTED wait is what is under test. Measuring the elapsed time
    instead failed on Windows, whose clock read a 300 ms sleep as 297 ms.
    """
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


def test_the_wait_the_refusing_side_asked_for_is_honored(db, recorded_sleeps):
    fn, attempts = _failing(db, 1, _refusing_with(300))
    assert db.run_in_transaction(fn, retries=3, backoff_s=0.001) == "done"
    assert len(attempts) == 2
    # the refusal's 300 ms, not the 1 ms backoff
    assert recorded_sleeps == [0.3]


def test_the_wait_is_capped_by_retry_after_max_wait(db, recorded_sleeps):
    """A peer asking for an hour does not park the caller (#8617)."""
    cap_ms = int(
        jpype.JClass(
            "com.arcadedb.GlobalConfiguration"
        ).NETWORK_RETRY_AFTER_MAX_WAIT.getValueAsLong()
    )
    fn, attempts = _failing(db, 1, _refusing_with(3_600_000))
    assert db.run_in_transaction(fn, retries=3, backoff_s=0) == "done"
    assert len(attempts) == 2
    assert recorded_sleeps == [cap_ms / 1000.0]
    assert cap_ms < 3_600_000


def test_a_block_that_committed_part_of_its_work_is_not_replayed(db):
    """#7916 on the Python side: a BATCH boundary already made rows durable, so a replay would apply them twice."""
    with db.transaction():
        for _ in range(25):
            db.command("sql", "INSERT INTO R SET k = 0, v = 0")
    attempts = []

    def fn():
        attempts.append(1)
        db.command("sql", "UPDATE R SET v = v + 1 BATCH 10")
        raise _wrapped(
            _java("ConcurrentModificationException")(
                "simulated conflict after the batch boundary"
            )
        )

    with pytest.raises(ArcadeDBError, match="ConcurrentModificationException"):
        db.run_in_transaction(fn, retries=5, backoff_s=0)
    assert len(attempts) == 1
    assert (
        db.query("sql", "SELECT count(*) AS c FROM R WHERE v = 2").first().get("c") == 0
    )
    assert (
        db.query("sql", "SELECT count(*) AS c FROM R WHERE v = 1").first().get("c")
        == 20
    )


def test_contended_commits_spend_their_budget_before_failing(db):
    """The issue's workload: no call may fail with retries left, whatever it failed with.

    A 1 ms commit lock timeout makes LockTimeoutException the common outcome of the
    contention; before the fix most calls raised it on their first attempt.
    """
    db._java_db.getConfiguration().setValue("arcadedb.commitLockTimeout", 1)
    with db.transaction():
        for i in range(100):
            db.command("sql", "INSERT INTO R SET k = ?, v = 0", i)

    retries = 10
    escaped = []
    committed = []
    lock = threading.Lock()

    def worker():
        for _ in range(30):
            attempts = []

            def fn():
                attempts.append(1)
                db.command("sql", "UPDATE R SET v = v + 1")

            try:
                db.run_in_transaction(fn, retries=retries, backoff_s=0.001)
                with lock:
                    committed.append(1)
            except ArcadeDBError as e:
                with lock:
                    escaped.append((str(e), len(attempts)))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    early = [(error, made) for error, made in escaped if made < retries + 1]
    assert early == [], f"{len(early)} calls failed with retries left, e.g. {early[:3]}"
    assert len(committed) + len(escaped) == 180
    # every committed call added exactly 1 to every row
    assert db.query(
        "sql", "SELECT min(v) AS lo, max(v) AS hi FROM R"
    ).first().to_dict() == {
        "lo": len(committed),
        "hi": len(committed),
    }
