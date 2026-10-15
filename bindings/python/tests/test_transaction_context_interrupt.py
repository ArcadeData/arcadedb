"""
Regression test for issue #9321: TransactionContext.__enter__ must not leave the transaction it opened when it is
interrupted between begin() and the flag that records it.
"""

import pytest
from arcadedb_embedded.transactions import TransactionContext


class _FakeDatabase:
    """Stands in for the database; the interrupt lands right after begin() has opened the transaction.

    Mirrors LocalDatabase's stack: begin() with no active transaction reuses the base level, with one active it pushes
    a nested level (``depth``), and rollback() pops the top level.
    """

    def __init__(self):
        self.active = False
        self.depth = 0
        self.rollbacks = 0

    def begin(self):
        if self.active:
            self.depth += 1
        self.active = True
        raise KeyboardInterrupt()

    def is_transaction_active(self):
        return self.active

    def rollback(self):
        if self.depth > 0:
            self.depth -= 1
        else:
            self.active = False
        self.rollbacks += 1

    def commit(self):
        self.active = False

    def _transaction_stack_mark(self):
        return self.active, self.depth

    def _end_transactions_opened_since(self, mark):
        was_active, depth_before = mark
        for _ in range(self.depth - depth_before):
            self.rollback()
        if not was_active and self.active:
            self.rollback()


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


def test_interrupted_nested_begin_pops_only_its_own_level():
    db = _FakeDatabase()
    db.active = True
    with pytest.raises(KeyboardInterrupt):
        with TransactionContext(db):
            pytest.fail("the body must not run")
    assert db.active is True
    assert db.depth == 0
    assert db.rollbacks == 1
