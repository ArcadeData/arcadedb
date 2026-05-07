"""Tests for the internal _logging helper."""

import logging

from arcadedb_embedded._logging import get_logger, log_swallowed_exception


def test_get_logger_returns_namespaced_logger():
    logger = get_logger("arcadedb_embedded.foo")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "arcadedb_embedded.foo"


def test_log_swallowed_exception_emits_debug(caplog):
    logger = get_logger("arcadedb_embedded.test")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            log_swallowed_exception(logger, "during shutdown")

    records = [r for r in caplog.records if r.name == logger.name]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert "during shutdown" in records[0].getMessage()
    assert records[0].exc_info is not None
