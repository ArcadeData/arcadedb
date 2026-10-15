"""
A ^C must not leave state behind in the two places the interrupt-hygiene sweep of #7108/#7882
had not reached (#9813).

* ``Database.run_in_transaction`` called ``begin()`` above the ``try`` whose handler rolls
  back, so an interrupt landing after the transaction was opened but before ``begin()``
  returned left it open for the next caller on the thread.
* ``ArcadeDBServer.start()`` cleaned up after ``Exception`` only, so a ``KeyboardInterrupt``
  during startup left the JVM-side server running while the Python object never recorded
  that it had started: ``stop()`` refused to stop it and the process could not exit.

Inside a caller's own transaction ``begin()`` pushes a NESTED level, so an interrupt after that push
must pop it and nothing else, in ``run_in_transaction`` and ``with db.transaction():`` alike (#9970).

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded import ArcadeDBError
from arcadedb_embedded.core import Database
from arcadedb_embedded.server import ArcadeDBServer


@pytest.fixture
def db(temp_db_path):
    database = arcadedb.create_database(temp_db_path)
    database.command("sql", "CREATE DOCUMENT TYPE R")
    yield database
    # a failed assertion can leave levels open, and drop() refuses inside a transaction
    try:
        database._java_db.rollbackAllNested()
    except Exception:  # nosec B110 - teardown must not hide the test failure
        pass
    database.drop()


def _count(database):
    return database.query("sql", "SELECT count(*) AS c FROM R").first().get("c")


def _depth(database):
    """How many transaction levels are on this thread's stack (nesting depth)."""
    return int(database._java_db.getNestedTransactions())


class TestRunInTransactionBeginHygiene:
    def test_interrupt_after_the_transaction_opened_rolls_it_back(
        self, db, monkeypatch
    ):
        real_begin = Database.begin

        def begin_then_interrupt(self):
            real_begin(self)
            # the transaction is open on the Java side; the interrupt lands
            # before begin() hands control back to run_in_transaction
            raise KeyboardInterrupt

        monkeypatch.setattr(Database, "begin", begin_then_interrupt)

        with pytest.raises(KeyboardInterrupt):
            db.run_in_transaction(lambda: None)

        monkeypatch.undo()
        assert db.is_transaction_active() is False

    def test_interrupt_taking_the_commit_mark_rolls_the_transaction_back(
        self, db, monkeypatch
    ):
        def interrupted_mark(self):
            raise KeyboardInterrupt

        monkeypatch.setattr(Database, "_transaction_commit_mark", interrupted_mark)

        with pytest.raises(KeyboardInterrupt):
            db.run_in_transaction(lambda: None)

        monkeypatch.undo()
        assert db.is_transaction_active() is False

    def test_a_begin_that_refuses_leaves_the_callers_transaction_alone(
        self, db, monkeypatch
    ):
        db.begin()
        db.new_document("R").set("k", "caller").save()

        def refusing_begin(self):
            raise ArcadeDBError("Failed to begin transaction: refused")

        monkeypatch.setattr(Database, "begin", refusing_begin)
        with pytest.raises(ArcadeDBError):
            db.run_in_transaction(lambda: None)
        monkeypatch.undo()

        # the transaction was the caller's: not opened by the call, so not its to end
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 1

    def test_interrupt_after_a_nested_begin_pops_only_that_level(self, db, monkeypatch):
        # begin() inside a caller's transaction pushes a NESTED level; an
        # interrupt after the push used to leave it on the stack, so the
        # caller's commit committed that empty level and left their own work
        # uncommitted (#9970)
        db.begin()
        db.new_document("R").set("k", "caller").save()
        assert _depth(db) == 1

        real_begin = Database.begin

        def begin_then_interrupt(self):
            real_begin(self)
            assert _depth(self) == 2
            raise KeyboardInterrupt

        monkeypatch.setattr(Database, "begin", begin_then_interrupt)
        with pytest.raises(KeyboardInterrupt):
            db.run_in_transaction(lambda: None)
        monkeypatch.undo()

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert db.is_transaction_active() is False
        assert _count(db) == 1

    def test_a_failing_block_inside_a_callers_transaction_rolls_back_its_level(
        self, db
    ):
        db.begin()
        db.new_document("R").set("k", "caller").save()

        def failing():
            db.new_document("R").set("k", "nested").save()
            raise ValueError("boom")

        with pytest.raises(ValueError):
            db.run_in_transaction(failing)

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 1

    def test_a_failed_nested_commit_leaves_the_callers_transaction_open(self, db):
        # LocalDatabase.commit() pops the nested level in its finally, failed
        # commit or not; the level left on top is the caller's, not this call's
        # to roll back (#9970)
        db.command("sql", "CREATE PROPERTY R.k STRING")
        db.command("sql", "CREATE INDEX ON R (k) UNIQUE")
        db.run_in_transaction(lambda: db.new_document("R").set("k", "dup").save())

        db.begin()
        db.new_document("R").set("k", "caller").save()

        with pytest.raises(ArcadeDBError):
            db.run_in_transaction(lambda: db.new_document("R").set("k", "dup").save())

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 2

    def test_a_failing_rollback_does_not_skip_the_levels_below_it(
        self, db, monkeypatch
    ):
        db.begin()
        db.new_document("R").set("k", "caller").save()

        def opens_another_level_then_fails():
            db.begin()
            raise ValueError("boom")

        real_rollback = Database.rollback
        calls = []

        def first_rollback_fails_after_popping(self):
            real_rollback(self)
            calls.append(1)
            if len(calls) == 1:
                raise ArcadeDBError("Failed to rollback transaction: boom")

        monkeypatch.setattr(Database, "rollback", first_rollback_fails_after_popping)
        with pytest.raises(ValueError):
            db.run_in_transaction(opens_another_level_then_fails)
        monkeypatch.undo()

        assert len(calls) == 2
        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 1

    def test_the_normal_path_still_commits_and_rolls_back(self, db):
        assert db.run_in_transaction(lambda: db.new_document("R").set("k", 1).save())
        assert db.is_transaction_active() is False
        assert _count(db) == 1

        def failing():
            db.new_document("R").set("k", 2).save()
            raise ValueError("boom")

        with pytest.raises(ValueError):
            db.run_in_transaction(failing)
        assert db.is_transaction_active() is False
        assert _count(db) == 1


class TestTransactionContextBeginHygiene:
    """``with db.transaction():`` calls begin() in __enter__, and an exception
    out of __enter__ means __exit__ never runs, so __enter__ cleans up (#9970)."""

    def test_interrupt_after_begin_rolls_the_transaction_back(self, db, monkeypatch):
        real_begin = Database.begin

        def begin_then_interrupt(self):
            real_begin(self)
            raise KeyboardInterrupt

        monkeypatch.setattr(Database, "begin", begin_then_interrupt)
        with pytest.raises(KeyboardInterrupt):
            with db.transaction():
                pass
        monkeypatch.undo()

        assert db.is_transaction_active() is False

    def test_interrupt_after_a_nested_begin_pops_only_that_level(self, db, monkeypatch):
        db.begin()
        db.new_document("R").set("k", "caller").save()

        real_begin = Database.begin

        def begin_then_interrupt(self):
            real_begin(self)
            assert _depth(self) == 2
            raise KeyboardInterrupt

        monkeypatch.setattr(Database, "begin", begin_then_interrupt)
        with pytest.raises(KeyboardInterrupt):
            with db.transaction():
                pass
        monkeypatch.undo()

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert db.is_transaction_active() is False
        assert _count(db) == 1

    def test_a_begin_that_refuses_leaves_the_callers_transaction_alone(
        self, db, monkeypatch
    ):
        db.begin()
        db.new_document("R").set("k", "caller").save()

        def refusing_begin(self):
            raise ArcadeDBError("Failed to begin transaction: refused")

        monkeypatch.setattr(Database, "begin", refusing_begin)
        with pytest.raises(ArcadeDBError):
            with db.transaction():
                pass
        monkeypatch.undo()

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 1

    def test_a_failing_block_inside_a_callers_transaction_rolls_back_its_level(
        self, db
    ):
        # __exit__ is unchanged: it rolls back the level __enter__ opened
        db.begin()
        db.new_document("R").set("k", "caller").save()

        with pytest.raises(ValueError):
            with db.transaction():
                db.new_document("R").set("k", "nested").save()
                raise ValueError("boom")

        assert _depth(db) == 1
        assert db.is_transaction_active() is True
        db.commit()
        assert _count(db) == 1


class _FakeJavaServer:
    """Stands in for the JVM-side server: start() raises, stop() is counted."""

    def __init__(self, error):
        self.error = error
        self.stops = 0

    def start(self):
        raise self.error

    def stop(self):
        self.stops += 1


def _server_with(java_server):
    """An ArcadeDBServer around a stand-in, without starting a real one."""
    server = object.__new__(ArcadeDBServer)
    server._started = False
    server._java_server = java_server
    return server


class TestServerStartInterrupt:
    def test_interrupt_during_start_stops_the_java_server_and_propagates(self):
        java_server = _FakeJavaServer(KeyboardInterrupt())
        server = _server_with(java_server)

        with pytest.raises(KeyboardInterrupt):
            server.start()

        assert java_server.stops == 1
        assert server._started is False

    def test_with_block_interrupted_in_start_leaves_the_java_server_stopped(self):
        # __exit__ never runs when __enter__ raises, so start() is the only place that can stop it
        java_server = _FakeJavaServer(KeyboardInterrupt())
        server = _server_with(java_server)

        with pytest.raises(KeyboardInterrupt):
            with server:
                pass

        assert java_server.stops == 1

    def test_system_exit_during_start_is_cleaned_up_too(self):
        java_server = _FakeJavaServer(SystemExit(1))
        server = _server_with(java_server)

        with pytest.raises(SystemExit):
            server.start()

        assert java_server.stops == 1

    def test_a_failed_start_is_still_reported_as_a_start_failure(self):
        java_server = _FakeJavaServer(RuntimeError("port taken"))
        server = _server_with(java_server)

        with pytest.raises(ArcadeDBError, match="Failed to start server: port taken"):
            server.start()

        assert java_server.stops == 1
        assert server._started is False

    def test_a_stop_that_fails_during_cleanup_does_not_hide_the_interrupt(self):
        java_server = _FakeJavaServer(KeyboardInterrupt())

        def failing_stop():
            java_server.stops += 1
            raise RuntimeError("already down")

        java_server.stop = failing_stop
        server = _server_with(java_server)

        with pytest.raises(KeyboardInterrupt):
            server.start()

        assert java_server.stops == 1
