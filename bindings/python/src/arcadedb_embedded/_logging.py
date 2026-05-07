"""Internal logging helpers for arcadedb_embedded.

Centralises the pattern for swallowing exceptions in finalizers and
best-effort cleanup paths so the suppression is observable at DEBUG level
instead of being silently dropped.
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger for an arcadedb_embedded submodule."""
    return logging.getLogger(name)


def log_swallowed_exception(logger: logging.Logger, context: str) -> None:
    """Log the currently-handled exception at DEBUG with full traceback.

    Use only inside an `except` block where the caller has decided the
    error is non-fatal (e.g. JVM finalizer paths, optional best-effort
    rollback) and continuing is the right behaviour.
    """
    logger.debug("Swallowed exception %s", context, exc_info=True)
