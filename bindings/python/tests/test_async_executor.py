"""Tests for AsyncExecutor with SQL/Cypher-first usage."""

import shutil
import tempfile
import threading
import time
from pathlib import Path

import arcadedb_embedded as arcadedb
import pytest

# Submissions used by the record-loss tests. 9,742 is the count from the
# original report: 9,742 Movie vertices submitted, 2,436 stored.
LOSS_REPRO_ROWS = 9_742


def test_async_executor_sql_command_insert_is_exact_at_parallel_one():
    """Every command submitted at parallel level 1 becomes a row.

    This used to run at parallel level 4 and assert `count > 0`, which is the
    assertion shape that let ArcadeData/arcadedb#7615 through: at level 4 the
    executor stored a quarter of what it was given (before 26.10.1, fixed in
    #7625) and `count > 0` still passed.
    """
    db_path = Path(tempfile.mkdtemp()) / "test_async_sql_insert"

    try:
        db = arcadedb.create_database(str(db_path))
        db.command("sql", "CREATE DOCUMENT TYPE Item")

        async_exec = db.async_executor().set_parallel_level(1).set_commit_every(1)

        for i in range(200):
            async_exec.command(
                "sql",
                "INSERT INTO Item SET id = ?, name = ?",
                callback=lambda _r: None,
                args=(i, f"Item{i}"),
            )

        async_exec.wait_completion()
        async_exec.close()

        count = db.query("sql", "SELECT count(*) as c FROM Item").first().get("c")
        assert int(count) == 200
        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_bulk_command_is_exact_at_parallel_one(temp_db):
    """A bulk load through the async command path, at the size that lost rows.

    Parallel level 1 was the only level at which this path was exact before
    26.10.1 (ArcadeData/arcadedb#7615, fixed in #7625), which is one reason the
    recommended bulk paths are `Database.insert_many` and
    `Database.graph_batch` instead.
    """
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Bulk")

    errors = []
    async_exec = db.async_executor().set_parallel_level(1).set_commit_every(1_000)
    async_exec.on_error(errors.append)

    for i in range(LOSS_REPRO_ROWS):
        async_exec.command("sql", "INSERT INTO Bulk SET id = :id", id=i)

    async_exec.wait_completion()
    async_exec.close()

    stored = int(db.query("sql", "SELECT count(*) AS c FROM Bulk").one().get("c"))
    assert stored == LOSS_REPRO_ROWS
    assert errors == []


def test_async_executor_bulk_command_is_exact_at_parallel_four(temp_db):
    """The same load at parallel level 4, which is where the records went missing.

    This is the regression test for ArcadeData/arcadedb#7615, fixed upstream in
    #7625 (26.10.1): a periodic batch commit that hit a page conflict with
    another worker is now retried, and a batch that still cannot commit is
    reported through every command's error callback instead of being dropped.
    It was skipped until that fix landed. Observed on arcadedb-engine 26.9.1
    and 26.6.1, measured 2026-09-15: of 9,742 submitted, 2,436, 5,742, and
    7,742 stored across runs, with nothing raised, nothing logged, and
    `wait_completion()` returning normally. Only the executor-wide `on_error`
    handler fired, one ConcurrentModificationException per rolled-back batch.
    The assertion is exact on purpose: how much was lost varied, so any
    tolerance would let the defect back through. What it holds exact is the
    fixed contract, "nothing is lost without a report": every row is stored
    or its batch is reported. Under load a batch commit can still use up its
    retries (they run back to back, ArcadeData/arcadedb#9529, about 1 run in
    100 on two cores) and is then rolled back and reported: one executor-wide
    on_error per rolled-back batch of commit_every rows. So the rows stored
    plus commit_every per reported batch must equal the rows submitted, and
    every report must be the commit conflict. A batch dropped with no report
    (#7615 back) breaks the equality.
    """
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Bulk4")

    errors = []
    async_exec = db.async_executor().set_parallel_level(4).set_commit_every(1_000)
    async_exec.on_error(errors.append)

    for i in range(LOSS_REPRO_ROWS):
        async_exec.command("sql", "INSERT INTO Bulk4 SET id = :id", id=i)

    async_exec.wait_completion()
    async_exec.close()

    stored = int(db.query("sql", "SELECT count(*) AS c FROM Bulk4").one().get("c"))
    # Says which failure it is: a batch dropped with no callback (#7615 back) or a
    # batch rolled back and reported after its retries (#7625's design, under load).
    # A darwin/arm64 runner stored 8742 of 9742 once on 2026-10-02 and the old
    # message could not tell the two apart.
    reported = [str(e)[:200] for e in errors[:3]]
    assert stored + 1_000 * len(errors) == LOSS_REPRO_ROWS, (
        f"stored {stored} of {LOSS_REPRO_ROWS} with {len(errors)} reported batch(es) of 1,000: "
        f"{LOSS_REPRO_ROWS - stored - 1_000 * len(errors)} row(s) lost without a report; on_error: {reported}"
    )
    assert all("ConcurrentModification" in str(e) for e in errors), reported


def test_async_executor_query_callback_collects_rows(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    with db.transaction():
        for i in range(20):
            db.command(
                "sql",
                "INSERT INTO Person SET id = :id, name = :name",
                {"id": i, "name": f"Person{i}"},
            )

    seen = []

    def on_row(row):
        seen.append(row.get("id"))

    async_exec = db.async_executor().set_parallel_level(2).set_commit_every(10)
    async_exec.query("sql", "SELECT id FROM Person ORDER BY id", on_row)
    async_exec.wait_completion()
    async_exec.close()

    assert seen == list(range(20))


def test_database_close_closes_owned_async_executor(temp_db_path):
    db = arcadedb.create_database(temp_db_path)
    db.command("sql", "CREATE DOCUMENT TYPE Msg")

    async_exec = db.async_executor().set_commit_every(1)
    async_exec.command("sql", "INSERT INTO Msg SET id = :id", id=1)
    async_exec.wait_completion()

    db.close()

    assert async_exec.is_closed() is True

    async_exec.close()


def test_async_executor_close_is_idempotent(temp_db):
    async_exec = temp_db.async_executor()

    async_exec.close()
    async_exec.close()

    assert async_exec.is_closed() is True


def test_async_executor_pending_and_processing_flags(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Msg")

    async_exec = db.async_executor().set_parallel_level(1).set_commit_every(100)
    assert not async_exec.is_pending()
    assert not async_exec.is_processing()

    # A result callback that holds its command until released gives a window
    # in which the executor has work in flight, whatever the machine's speed.
    # (This test once polled with waitCompletion(0), which the engine treats
    # as an unbounded wait, and never asserted what it polled for.)
    entered, release = threading.Event(), threading.Event()

    def hold(_result):
        entered.set()
        release.wait(30)

    async_exec.command("sql", "INSERT INTO Msg SET id = 0", callback=hold)
    try:
        assert entered.wait(30), "the async command never ran"
        assert async_exec.is_processing()
        assert async_exec.is_pending()
    finally:
        release.set()

    async_exec.wait_completion()
    assert not async_exec.is_pending()
    assert not async_exec.is_processing()
    assert db.count_type("Msg") == 1

    async_exec.close()


def test_async_executor_is_pending_true_while_queued(temp_db):
    """Regression test for #7107: is_pending() must poll without blocking.

    It used to call waitCompletion(0), which the engine clamps to an infinite
    wait, so it always blocked until the queue drained and then reported
    False - never True, even while work was still queued.
    """
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Msg")

    # commit_every equal to the row count means the queue's own commit boundary lands
    # on the very last row, so there is always a real commit (page writes, WAL flush)
    # in flight - not just an idle queue - at the moment the check below runs. Tried
    # widening this to a much larger row count with commit_every set past it instead
    # (so the queue would simply stay non-empty for longer): that made the test LESS
    # reliable, not more, because JPype's per-call submission overhead from Python
    # dominates the single background worker's per-row insert cost, so a larger
    # backlog gives the worker more real time to catch up and fully drain the queue
    # before the check runs (code review follow-up on #7107).
    async_exec = db.async_executor().set_parallel_level(1).set_commit_every(2000)
    assert async_exec.is_pending() is False

    # HOLD THE WORKER, RATHER THAN OUT-RUNNING IT. Submitting a backlog and
    # hoping the single worker has not drained it is a race the test loses on a
    # fast machine: CI failed here on 2026-09-15 while the same test passed
    # three times in a row locally. A sleep submitted first occupies the one
    # worker for a known interval, so the rows behind it are certainly still
    # queued when is_pending() is asked, and the assertion is about the API
    # rather than about who won.
    #
    # The hold is SQL script's SLEEP statement. Until 2026-10-04 this was
    # `SELECT sleep(1500)`, but ArcadeDB SQL has no sleep() function: the
    # command failed on the worker with "Unknown function name 'sleep'", the
    # async executor swallowed the error, nothing was held, and the test was the
    # race it describes (it failed on CI's macOS Python 3.13 runner, run
    # 37199978708). So the hold is now proven below, not assumed.
    start = time.time()
    async_exec.command("sqlscript", "SLEEP 1500")  # 1.5 s on the worker
    for i in range(50):
        async_exec.command("sql", "INSERT INTO Msg SET id = :id", id=i)

    asked = time.time()
    pending = async_exec.is_pending()
    elapsed = time.time() - asked

    assert (
        elapsed < 1.0
    ), "is_pending() must answer immediately, not wait for the queue to drain"
    assert pending is True

    # The hold really held: the queue cannot drain before the sleep ends.
    async_exec.wait_completion()
    drained = time.time() - start
    assert (
        drained >= 1.2
    ), f"the queue drained in {drained:.2f}s; the 1.5 s hold did not hold"
    assert db.query("sql", "SELECT count(*) AS n FROM Msg").to_json_list() == [
        {"n": 50}
    ]

    async_exec.wait_completion()
    assert async_exec.is_pending() is False

    async_exec.close()


def test_async_executor_getters_and_sync_modes(temp_db):
    db = temp_db
    async_exec = db.async_executor()

    async_exec.set_parallel_level(3)
    async_exec.set_commit_every(123)
    async_exec.set_back_pressure(40)
    async_exec.set_transaction_use_wal(False)
    async_exec.set_transaction_sync("yes_nometadata")

    assert async_exec.get_parallel_level() == 3
    assert async_exec.get_commit_every() == 123
    assert async_exec.get_back_pressure() >= 0
    assert async_exec.is_transaction_use_wal() is False
    assert async_exec.get_transaction_sync() == "yes_nometadata"
    assert async_exec.get_thread_count() >= 1

    async_exec.close()


def test_async_executor_parallel_level_has_no_upper_cap(temp_db):
    # The engine's own default is cores - 1, 19 on a 20-thread host, and the
    # bucket rule (ArcadeData/arcadedb#8478) wants that many writers; the
    # package once refused anything above 16.
    async_exec = temp_db.async_executor()
    async_exec.set_parallel_level(17)
    assert async_exec.get_parallel_level() == 17
    with pytest.raises(ValueError):
        async_exec.set_parallel_level(0)
    async_exec.close()


def test_async_executor_command_error_callback(temp_db):
    db = temp_db
    errors = []

    def on_error(exc):
        errors.append(str(exc))

    async_exec = db.async_executor()
    async_exec.command(
        "sql",
        "INSERT INTO MissingType SET id = :id",
        error_callback=on_error,
        id=1,
    )
    async_exec.wait_completion()
    async_exec.close()

    assert errors, "Expected async error callback to be invoked"


def test_async_executor_global_callbacks(temp_db):
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Log")

    ok_calls = {"count": 0}
    err_calls = {"count": 0}

    def on_ok():
        ok_calls["count"] += 1

    def on_error(_exc):
        err_calls["count"] += 1

    async_exec = db.async_executor().on_ok(on_ok).on_error(on_error)

    for i in range(5):
        async_exec.command("sql", "INSERT INTO Log SET id = :id", id=i)

    async_exec.wait_completion()
    async_exec.close()

    assert ok_calls["count"] >= 1
    assert err_calls["count"] >= 0


def test_create_record_reports_a_rejected_record_to_its_error_callback(temp_db):
    # A record the writers reject reached only the executor-wide on_error
    # handler, so without one it was lost silently (#14).
    db = temp_db
    db.command("sql", "CREATE DOCUMENT TYPE Dup")
    db.command("sql", "CREATE PROPERTY Dup.k LONG")
    db.command("sql", "CREATE INDEX ON Dup (k) UNIQUE")

    created, errors = [], []
    ex = db.async_executor().set_parallel_level(1).set_commit_every(1)
    for _ in range(3):
        doc = db.new_document("Dup")
        doc.set("k", 7)
        ex.create_record(doc, callback=created.append, error_callback=errors.append)
    ex.wait_completion()
    ex.close()

    assert db.count_type("Dup") == 1
    # `callback` runs when the writer creates the record, before its batch
    # commits, so it has fired for all three; only error_callback says which
    # two the commit rejected.
    assert len(created) == 3
    assert len(errors) == 2
    assert all("Duplicate" in str(e) or "duplicate" in str(e) for e in errors), errors


def _block_async_worker(async_exec):
    """Park the executor's worker inside a query callback until the returned event is set.

    Returns ``(started, release)``. Once ``started`` is set the executor is provably busy
    and stays busy until ``release`` is set, so a "not done yet" answer does not depend on
    racing the worker against the clock.
    """
    started = threading.Event()
    release = threading.Event()

    def on_row(_row):
        started.set()
        release.wait(30)

    async_exec.query("sql", "SELECT 1 AS one", on_row)
    assert started.wait(30), "the async query callback never ran"
    return started, release


def test_async_executor_wait_completion_zero_does_not_block_while_busy(temp_db):
    """Regression test for #7883: wait_completion(0) must poll, not block.

    The engine's waitCompletion(long) clamps any timeout <= 0 to an infinite wait,
    so passing 0 through blocked until the queue drained and then returned normally,
    never raising TimeoutError.
    """
    async_exec = temp_db.async_executor().set_parallel_level(1)
    _, release = _block_async_worker(async_exec)

    outcome = {}

    def call():
        try:
            async_exec.wait_completion(0)
            outcome["result"] = "returned"
        except TimeoutError:
            outcome["result"] = "timeout"
        except Exception as exc:  # pragma: no cover - reported by the assertion below
            outcome["result"] = repr(exc)

    caller = threading.Thread(target=call, daemon=True)
    try:
        caller.start()
        # The worker is parked until `release` is set, so a wait_completion(0) that still
        # reached the engine's clamped infinite wait would never come back from this join.
        caller.join(10)
        blocked = caller.is_alive()
    finally:
        release.set()
        async_exec.wait_completion()
        caller.join(10)
        async_exec.close()

    assert blocked is False, "wait_completion(0) blocked on a busy executor"
    assert outcome.get("result") == "timeout"


def test_async_executor_wait_completion_zero_returns_when_idle(temp_db):
    async_exec = temp_db.async_executor()
    try:
        async_exec.wait_completion()

        assert async_exec.wait_completion(0) is None
    finally:
        async_exec.close()


def test_async_executor_wait_completion_rejects_negative_timeout(temp_db):
    async_exec = temp_db.async_executor().set_parallel_level(1)
    _, release = _block_async_worker(async_exec)

    try:
        with pytest.raises(ValueError):
            async_exec.wait_completion(-1)
    finally:
        release.set()
        async_exec.wait_completion()
        async_exec.close()


def test_async_executor_wait_completion_positive_timeout_still_times_out(temp_db):
    async_exec = temp_db.async_executor().set_parallel_level(1)
    _, release = _block_async_worker(async_exec)

    try:
        try:
            with pytest.raises(TimeoutError):
                async_exec.wait_completion(50)
        finally:
            release.set()
            async_exec.wait_completion()

        assert async_exec.wait_completion(30000) is None
    finally:
        async_exec.close()
