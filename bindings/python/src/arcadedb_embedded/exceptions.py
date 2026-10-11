"""
ArcadeDB Python Bindings - Exception Classes

All custom exceptions for the ArcadeDB Python bindings.
"""


def _java_root_cause(exc):
    """The innermost cause of a Java exception, or None.

    None when ``exc`` is not a Java exception or has no nested cause (its own
    message is then already in the wrapping text).
    """
    get_cause = getattr(exc, "getCause", None)
    if get_cause is None:
        return None
    root, seen = None, set()
    cause = get_cause()
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        root = cause
        cause = cause.getCause()
    return root


_MAX_CAUSE_DEPTH = 64
_RETRY_CLASSES = None


def _retry_classes():
    """``(NeedRetryException, DuplicatedKeyException, GlobalConfiguration)``.

    Resolved once, by name through the JVM rather than the ``com`` import
    hook, which a ``com/`` folder on ``sys.path`` shadows. Two threads racing
    the first call resolve the same classes, so the unlocked assignment is
    harmless. None while the JVM is down: no Java exception can exist then.
    """
    global _RETRY_CLASSES
    if _RETRY_CLASSES is None:
        import jpype

        if not jpype.isJVMStarted():
            return None
        _RETRY_CLASSES = (
            jpype.JClass("com.arcadedb.exception.NeedRetryException"),
            jpype.JClass("com.arcadedb.exception.DuplicatedKeyException"),
            jpype.JClass("com.arcadedb.GlobalConfiguration"),
        )
    return _RETRY_CLASSES


def _java_exceptions(exc):
    """Every Java exception ``exc`` carries, outermost first.

    Follows the Python ``__cause__`` chain the binding builds with
    ``raise ArcadeDBError(...) from e`` down to the first Java exception, then
    that exception's own ``getCause()`` chain: the engine wraps a conflict in a
    generic outer exception on some paths (#9487), and the type that decides
    whether to retry is the one inside.
    """
    # Bounded as well as deduplicated: JPype may hand out a new Python wrapper
    # for the same Java object on every getCause(), so id() alone cannot see a
    # cycle in a Java cause chain.
    seen = set()
    for _ in range(_MAX_CAUSE_DEPTH):
        if exc is None or id(exc) in seen:
            return
        seen.add(id(exc))
        if hasattr(exc, "getCause") and hasattr(exc, "getClass"):
            yield exc
            exc = exc.getCause()
        else:
            exc = exc.__cause__


def _bounded_retry_after_ms(retry_after_ms, global_configuration):
    """The part of a refusal's wait a retry loop honors (#8617).

    All of it up to ``arcadedb.network.retryAfterMaxWait``, the cap the Java
    client applies, so a misbehaving peer cannot park the caller.
    """
    if retry_after_ms <= 0:
        return 0
    cap = int(global_configuration.NETWORK_RETRY_AFTER_MAX_WAIT.getValueAsLong())
    return min(retry_after_ms, max(0, cap))


def _retry_verdict(exc):
    """Whether ``exc`` is a conflict a transaction retry can absorb.

    Returns ``(kind, retry_after_ms)``: ``kind`` is ``"need_retry"`` for any
    ``NeedRetryException`` (``ConcurrentModificationException``,
    ``LockTimeoutException``, ...), ``"duplicated_key"`` for a
    ``DuplicatedKeyException`` and None for everything else;
    ``retry_after_ms`` is the wait the refusing side asked for
    (``NeedRetryException.getRetryAfterMs()``, #8617), 0 when it did not say.

    The decision is by Java TYPE, the way ``LocalDatabase.transaction()``
    makes it with ``catch (NeedRetryException | DuplicatedKeyException e)``,
    so a subclass is covered without being named. Searching the message for
    class names missed every subclass that was not listed, which is how a
    ``LockTimeoutException`` escaped ``run_in_transaction`` on its first
    attempt (#9629).
    """
    classes = _retry_classes()
    if classes is None:
        return None, 0
    need_retry, duplicated_key, global_configuration = classes
    for java_exc in _java_exceptions(exc):
        if isinstance(java_exc, need_retry):
            return "need_retry", _bounded_retry_after_ms(
                int(java_exc.getRetryAfterMs()), global_configuration
            )
        if isinstance(java_exc, duplicated_key):
            return "duplicated_key", 0
    return None, 0


class ArcadeDBError(Exception):
    """Base exception for ArcadeDB errors.

    When raised from a Java exception, ``str()`` also names the Java root
    cause: the engine often wraps the reason (for example "is locked by
    another process") in a generic outer exception.
    """

    def __str__(self):
        text = super().__str__()
        try:
            root = _java_root_cause(self.__cause__)
            if root is not None:
                message = root.getMessage()
                message = "" if message is None else str(message)
                if not message or message not in text:
                    name = str(root.getClass().getName())
                    detail = f"{name}: {message}" if message else name
                    text = f"{text} (caused by {detail})"
        except Exception:  # nosec B110 - never let str() of an error fail
            pass
        return text
