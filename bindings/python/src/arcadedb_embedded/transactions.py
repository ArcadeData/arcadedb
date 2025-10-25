"""
ArcadeDB Python Bindings - Transaction Management

Transaction context manager and related utilities.
"""


class TransactionContext:
    """Context manager for ArcadeDB transactions."""

    def __init__(self, database):
        self.database = database
        self.started = False

    def __enter__(self):
        self.database.begin()
        self.started = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.started:
            if exc_type is None:
                self.database.commit()
            else:
                self.database.rollback()
