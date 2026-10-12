"""
A ^C must not leave state behind in the two places the interrupt-hygiene sweep of #7108/#7882
had not reached (#9813).

* ``Database.run_in_transaction`` called ``begin()`` above the ``try`` whose handler rolls
  back, so an interrupt landing after the transaction was opened but before ``begin()``
  returned left it open for the next caller on the thread.
* ``ArcadeDBServer.start()`` cleaned up after ``Exception`` only, so a ``KeyboardInterrupt``
  during startup left the JVM-side server running while the Python object never recorded
  that it had started: ``stop()`` refused to stop it and the process could not exit.

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
    database.drop()


def _count(database):
    return database.query("sql", "SELECT count(*) AS c FROM R").first().get("c")


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
