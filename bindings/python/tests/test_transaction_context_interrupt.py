"""
Regression test for issue #9321: TransactionContext.__enter__ must not leave the transaction it opened when it is
interrupted between begin() and the flag that records it.
"""

import pytest
from arcadedb_embedded.transactions import TransactionContext


class _FakeDatabase:
    """Stands in for the database; the interrupt lands right after begin() has opened the transaction."""

    def __init__(self):
        self.active = False
        self.rollbacks = 0

    def begin(self):
        self.active = True
        raise KeyboardInterrupt()

    def is_transaction_active(self):
        return self.active

    def rollback(self):
        self.active = False
        self.rollbacks += 1

    def commit(self):
        self.active = False


def test_interrupt_after_begin_rolls_back():
    db = _FakeDatabase()
    context = TransactionContext(db)
    with pytest.raises(KeyboardInterrupt):
        with context:
            pytest.fail("the body must not run")
    assert db.active is False
    assert db.rollbacks == 1
    assert context.started is False


def test_failed_begin_without_open_transaction_does_not_roll_back():
    db = _FakeDatabase()
    db.begin = lambda: (_ for _ in ()).throw(RuntimeError("begin failed"))
    with pytest.raises(RuntimeError):
        with TransactionContext(db):
            pass
    assert db.rollbacks == 0


def test_failed_begin_leaves_a_preexisting_transaction_alone():
    db = _FakeDatabase()
    db.active = True
    db.begin = lambda: (_ for _ in ()).throw(RuntimeError("already active"))
    with pytest.raises(RuntimeError):
        with TransactionContext(db):
            pass
    assert db.active is True
    assert db.rollbacks == 0


def test_normal_flow_commits():
    db = _FakeDatabase()
    db.begin = lambda: setattr(db, "active", True)
    with TransactionContext(db):
        assert db.active is True
    assert db.active is False
