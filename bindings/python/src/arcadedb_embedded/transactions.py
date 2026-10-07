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
        # A transaction that was already open is not ours to roll back if begin() refuses to nest
        was_active = self.database.is_transaction_active()
        try:
            # begin() inside the try: an interrupt landing right after it must still reach the rollback,
            # because an exception out of __enter__ means __exit__ never runs
            self.database.begin()
            self.started = True
            return self
        except BaseException:
            self.started = False
            try:
                if not was_active and self.database.is_transaction_active():
                    self.database.rollback()
            except Exception:  # nosec B110 - best-effort rollback
                pass
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.started:
            if exc_type is None:
                self.database.commit()
            else:
                self.database.rollback()
