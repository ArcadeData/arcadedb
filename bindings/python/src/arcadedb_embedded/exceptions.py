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
