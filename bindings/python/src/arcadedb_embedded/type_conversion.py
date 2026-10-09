"""
Type conversion utilities for Java to Python type mapping.

Handles automatic conversion of Java objects to native Python types for better
developer experience and integration with Python ecosystem (pandas, numpy, etc.).
"""

import json
import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, NamedTuple

import jpype


class _JavaTimeTypes(NamedTuple):
    instant: Any
    local_date: Any
    local_datetime: Any
    zoned_datetime: Any
    offset_datetime: Any


class _JavaCoreTypes(NamedTuple):
    boolean: Any
    byte: Any
    character: Any
    double: Any
    float_type: Any
    integer: Any
    long_type: Any
    short: Any
    string: Any
    big_decimal: Any
    big_integer: Any
    java_date: Any


class _JavaCollectionTypes(NamedTuple):
    collection: Any
    list_type: Any
    map_type: Any
    set_type: Any


class _PythonToJavaTypes(NamedTuple):
    array_list: Any
    arrays: Any
    big_decimal: Any
    hash_map: Any
    hash_set: Any
    java_date: Any
    local_date: Any


_UNSET = object()

# Element types convert_python_to_java hands to the JVM as one Object[].
_BULK_SCALAR_TYPES = frozenset((int, float, str, bool, type(None)))

_TYPE_CACHE = {
    "java_core": None,
    "java_collections": None,
    "python_to_java": None,
    "java_time": None,
}


def _jclasses(*names):
    """Resolve Java classes by fully qualified name, straight from the JVM.

    NOT through JPype's ``java`` import hook. That hook resolves the top-level
    name ``java`` through ``sys.path`` like any other Python import, so a
    directory called ``java/`` anywhere on the path shadows it: a mixed
    Java/Python project's own source folder, or ``bindings/python/src/java``
    when running from a checkout of this repository. ``from java.lang import
    String`` then raised ImportError, the type lookups below returned None,
    every typed branch in ``_convert_and_register`` was skipped, and a Java
    String fell through to the generic sequence fallback -- which iterated it.
    ``to_list()`` returned ``[{'name': ['A', 'd', 'a']}]``, with no error.

    ``jpype.JClass`` asks the JVM for the class by name and has no path to be
    shadowed by. Returns None only when the JVM is not running, which is the
    one case the old ImportError branch was meant for.
    """
    if not jpype.isJVMStarted():
        return None
    return tuple(jpype.JClass(name) for name in names)


def _get_java_time_types():
    if _TYPE_CACHE["java_time"] is not None:
        return _TYPE_CACHE["java_time"]

    got = _jclasses(
        "java.time.Instant",
        "java.time.LocalDate",
        "java.time.LocalDateTime",
        "java.time.OffsetDateTime",
        "java.time.ZonedDateTime",
    )
    if got is None:
        return None
    Instant, LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime = got

    _TYPE_CACHE["java_time"] = _JavaTimeTypes(
        instant=Instant,
        local_date=LocalDate,
        local_datetime=LocalDateTime,
        zoned_datetime=ZonedDateTime,
        offset_datetime=OffsetDateTime,
    )
    return _TYPE_CACHE["java_time"]


def _get_java_core_types():
    if (
        _TYPE_CACHE["java_core"] is not None
        and _TYPE_CACHE["java_collections"] is not None
    ):
        return _TYPE_CACHE["java_core"], _TYPE_CACHE["java_collections"]

    got = _jclasses(
        "java.lang.Boolean",
        "java.lang.Byte",
        "java.lang.Character",
        "java.lang.Double",
        "java.lang.Float",
        "java.lang.Integer",
        "java.lang.Long",
        "java.lang.Short",
        "java.lang.String",
        "java.math.BigDecimal",
        "java.math.BigInteger",
        "java.util.Collection",
        "java.util.Date",
        "java.util.List",
        "java.util.Map",
        "java.util.Set",
    )
    if got is None:
        return None, None
    (
        Boolean,
        Byte,
        Character,
        Double,
        Float,
        Integer,
        Long,
        Short,
        String,
        BigDecimal,
        BigInteger,
        JavaCollection,
        JavaDate,
        JavaList,
        JavaMap,
        JavaSet,
    ) = got

    _TYPE_CACHE["java_core"] = _JavaCoreTypes(
        boolean=Boolean,
        byte=Byte,
        character=Character,
        double=Double,
        float_type=Float,
        integer=Integer,
        long_type=Long,
        short=Short,
        string=String,
        big_decimal=BigDecimal,
        big_integer=BigInteger,
        java_date=JavaDate,
    )
    _TYPE_CACHE["java_collections"] = _JavaCollectionTypes(
        collection=JavaCollection,
        list_type=JavaList,
        map_type=JavaMap,
        set_type=JavaSet,
    )
    return _TYPE_CACHE["java_core"], _TYPE_CACHE["java_collections"]


def _get_java_python_types():
    if _TYPE_CACHE["python_to_java"] is not None:
        return _TYPE_CACHE["python_to_java"]

    got = _jclasses(
        "java.math.BigDecimal",
        "java.time.LocalDate",
        "java.util.ArrayList",
        "java.util.Arrays",
        "java.util.Date",
        "java.util.HashMap",
        "java.util.HashSet",
    )
    if got is None:
        return None
    BigDecimal, LocalDate, ArrayList, Arrays, JavaDate, HashMap, HashSet = got

    _TYPE_CACHE["python_to_java"] = _PythonToJavaTypes(
        array_list=ArrayList,
        arrays=Arrays,
        big_decimal=BigDecimal,
        hash_map=HashMap,
        hash_set=HashSet,
        java_date=JavaDate,
        local_date=LocalDate,
    )
    return _TYPE_CACHE["python_to_java"]


def convert_java_to_python(value: Any) -> Any:
    """
    Convert Java objects to native Python types.

    Handles:
    - Primitives: Boolean, Integer, Long, Float, Double
    - Numeric: BigDecimal, BigInteger
    - Temporal: Date, LocalDate, LocalDateTime, Instant, ZonedDateTime
    - Collections: List, Set, Map
    - Arrays: byte[], int[], float[], etc.
    - Special: null → None

    Args:
        value: Java object to convert

    Returns:
        Python native type or original value if no conversion available

    Examples:
        >>> convert_java_to_python(java.lang.Integer(42))
        42
        >>> convert_java_to_python(java.math.BigDecimal("3.14"))
        Decimal('3.14')
        >>> convert_java_to_python(java.util.ArrayList([1, 2, 3]))
        [1, 2, 3]
    """
    if value is None:
        return None

    # Exact-type dispatch cache: JPype wrapper classes are stable per process,
    # so after a value's type has been resolved once through the isinstance
    # chain (in _convert_and_register), every later value of the same type
    # converts via a single dict lookup (~5x faster than the chain, dominant
    # in per-row result materialization).
    converter = _CONVERTER_CACHE.get(type(value))
    if converter is not None:
        return converter(value)
    return _convert_and_register(value)


_CONVERTER_CACHE: dict = {}


def _conv_identity(value):
    return value


def _conv_bool(value):
    return bool(value)


def _conv_str(value):
    return str(value)


def _conv_int(value):
    return int(value)


def _conv_float(value):
    return float(value)


def _conv_decimal(value):
    return Decimal(str(value))


def _conv_bigint(value):
    return int(str(value))


def _conv_java_date(value):
    return datetime.fromtimestamp(value.getTime() / 1000.0)


def _conv_local_date(value):
    return date(value.getYear(), value.getMonthValue(), value.getDayOfMonth())


def _conv_local_datetime(value):
    return datetime(
        value.getYear(),
        value.getMonthValue(),
        value.getDayOfMonth(),
        value.getHour(),
        value.getMinute(),
        value.getSecond(),
        value.getNano() // 1000,
    )


_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _utc_from_instant(instant):
    # Integer arithmetic: a float of epoch seconds has 15 to 16 significant
    # digits, so the microseconds were lost after the year 2262 and the last
    # instant of year 9999 rounded up into year 10000 and raised.
    return _EPOCH_UTC + timedelta(
        seconds=int(instant.getEpochSecond()),
        microseconds=int(instant.getNano()) // 1000,
    )


def _conv_instant(value):
    return _utc_from_instant(value)


def _conv_zoned_datetime(value):
    return _utc_from_instant(value.toInstant())


# OffsetDateTime is a storable DATETIME since engine 26.7.2 (#4922); same
# toInstant() shape as ZonedDateTime.
_conv_offset_datetime = _conv_zoned_datetime


def _conv_map(value):
    return {
        convert_java_to_python(k): convert_java_to_python(v) for k, v in value.items()
    }


def _conv_set(value):
    return {convert_java_to_python(item) for item in value}


def _conv_iter_to_list(value):
    return [convert_java_to_python(item) for item in value]


def _conv_primitive_array(value):
    # Bulk copy through the buffer protocol: ~200x faster than per-element
    # recursion for a 384-float vector.
    mv = memoryview(value)
    if (
        mv.format.startswith("=")
        or mv.format.startswith("<")
        or mv.format.startswith(">")
    ):
        # CPython's memoryview.tolist() rejects standard-size-prefixed
        # formats ('=i' for int[], '=q' for long[]) with NotImplementedError
        # (issue #4). Recast through bytes to the native format char.
        mv = mv.cast("b").cast(mv.format[1:])
    return mv.tolist()


def _register(value, converter):
    # Convert FIRST: caching before the first successful conversion pins a
    # broken converter for the process lifetime (issue #4).
    result = converter(value)
    _CONVERTER_CACHE[type(value)] = converter
    return result


def _convert_and_register(value):
    """Resolve the converter for a not-yet-seen type, cache it, convert."""
    if isinstance(value, jpype.JArray):
        try:
            memoryview(value)
        except TypeError:
            # object array (String[], Object[], ...): convert per element
            return _register(value, _conv_iter_to_list)
        return _register(value, _conv_primitive_array)

    java_core_types, java_collection_types = _get_java_core_types()
    if java_core_types is not None and java_collection_types is not None:
        if isinstance(value, java_core_types.boolean):
            return _register(value, _conv_bool)
        if isinstance(value, java_core_types.string):
            return _register(value, _conv_str)
        if isinstance(
            value,
            (
                java_core_types.integer,
                java_core_types.long_type,
                java_core_types.short,
                java_core_types.byte,
            ),
        ):
            return _register(value, _conv_int)
        if isinstance(value, (java_core_types.float_type, java_core_types.double)):
            return _register(value, _conv_float)
        if isinstance(value, java_core_types.character):
            return _register(value, _conv_str)

        if isinstance(value, java_core_types.big_decimal):
            return _register(value, _conv_decimal)
        if isinstance(value, java_core_types.big_integer):
            return _register(value, _conv_bigint)

        if isinstance(value, java_core_types.java_date):
            return _register(value, _conv_java_date)

        java_time_types = _get_java_time_types()
        if java_time_types is not None:
            if isinstance(value, java_time_types.local_date):
                return _register(value, _conv_local_date)
            if isinstance(value, java_time_types.local_datetime):
                return _register(value, _conv_local_datetime)
            if isinstance(value, java_time_types.instant):
                return _register(value, _conv_instant)
            if isinstance(value, java_time_types.zoned_datetime):
                return _register(value, _conv_zoned_datetime)
            if isinstance(value, java_time_types.offset_datetime):
                return _register(value, _conv_offset_datetime)

        if isinstance(value, java_collection_types.map_type):
            return _register(value, _conv_map)
        if isinstance(value, java_collection_types.set_type):
            return _register(value, _conv_set)
        if isinstance(value, java_collection_types.list_type):
            return _register(value, _conv_iter_to_list)
        if isinstance(value, java_collection_types.collection):
            return _register(value, _conv_iter_to_list)

    if (
        hasattr(value, "__len__")
        and hasattr(value, "__getitem__")
        and not isinstance(value, (str, bytes))
    ):
        try:
            result = [convert_java_to_python(item) for item in value]
        except (TypeError, AttributeError):
            pass
        else:
            _CONVERTER_CACHE[type(value)] = _conv_iter_to_list
            return result

    # No conversion available: Java objects like Vertex, Edge, Document pass
    # through unchanged. Cached only when the JVM type system was consulted,
    # so a pre-JVM call can't pin a wrong converter.
    if java_core_types is not None:
        _CONVERTER_CACHE[type(value)] = _conv_identity
    return value


def _is_numpy_bool(value: Any) -> bool:
    """numpy.bool_ (named `bool` in numpy 2), without importing numpy. It is not
    a subclass of `bool`, so JPype would read it as a number."""
    kind = type(value)
    return kind.__module__ == "numpy" and kind.__name__ in ("bool", "bool_")


def convert_python_to_java(value: Any) -> Any:
    """
    Convert Python objects to Java types when needed.

    This is mainly used for setting properties on records.
    Most conversions happen automatically via JPype, but some
    need explicit handling.

    Args:
        value: Python object to convert

    Returns:
        Java object or original value
    """
    if value is None:
        return None

    if _is_numpy_bool(value):
        # Not a bool subclass: JPype would store it as the Double 1.0 or 0.0.
        return bool(value)

    java_python_types = _get_java_python_types()

    if isinstance(value, Decimal):
        if java_python_types is None:
            return str(value)
        return java_python_types.big_decimal(str(value))

    if isinstance(value, set):
        if java_python_types is None:
            return list(value)
        java_set = java_python_types.hash_set()
        for item in value:
            java_set.add(convert_python_to_java(item))
        return java_set

    if isinstance(value, dict):
        if java_python_types is None:
            return value
        java_map = java_python_types.hash_map()
        for key, item in value.items():
            java_map.put(convert_python_to_java(key), convert_python_to_java(item))
        return java_map

    if isinstance(value, (list, tuple)):
        if java_python_types is None:
            return value
        # A LIST OF PLAIN SCALARS CROSSES AS ONE ARRAY (2026-09-27). Adding
        # element by element is one JVM call per element: 126 us for 39 ids
        # and 1.9 ms for 1,000 on the laptop, against 28 us and 0.48 ms as an
        # Object[]. JPype boxes each element exactly as add() would (int ->
        # Long, float -> Double, str, bool -> Boolean, None -> null) and
        # raises the same OverflowError past 64 bits; anything else (nested
        # collections, dates, Decimal, numpy scalars) takes the loop below.
        if all(type(item) in _BULK_SCALAR_TYPES for item in value):
            return java_python_types.array_list(
                java_python_types.arrays.asList(jpype.JArray(jpype.JObject)(value))
            )
        java_list = java_python_types.array_list()
        for item in value:
            java_list.add(convert_python_to_java(item))
        return java_list

    if isinstance(value, datetime):
        if java_python_types is None:
            return value
        # The engine stores DATETIME as a UTC wall clock, whatever the host's
        # or the database's time zone. A naive value is taken as that wall
        # clock as it stands, so it reads back unchanged on every host; an
        # aware one is converted to UTC first, so its instant is kept. The
        # java.util.Date this used to build read a naive value as local time
        # and kept milliseconds, so DATETIME_MICROS and DATETIME_NANOS lost
        # the rest and a lookup by the same value never matched (#58).
        utc = value.astimezone(timezone.utc) if value.utcoffset() is not None else value
        return _get_java_time_types().local_datetime.of(
            utc.year,
            utc.month,
            utc.day,
            utc.hour,
            utc.minute,
            utc.second,
            utc.microsecond * 1000,
        )

    if isinstance(value, date):
        if java_python_types is not None:
            return java_python_types.local_date.of(value.year, value.month, value.day)
        dt = datetime.combine(value, time.min)
        return convert_python_to_java(dt)

    if isinstance(value, (bytes, bytearray)):
        # Left to JPype, bytes reach an Object parameter as a Java String:
        # b"Hello" was stored as "Hello" and non-UTF-8 bytes as "", silently.
        # A byte[] keeps every byte; it reads back as a list of signed ints.
        if java_python_types is None:
            return value
        return jpype.JArray(jpype.JByte)(bytes(value))

    # Return as-is for other types (JPype will handle them)
    return value


# ---------------------------------------------------------------------------
# The bulk JSON paths (insert_many, GraphBatch.create_vertices, new_edges)
#
# They send their rows to the JVM as one JSON string, and the engine reads it
# with its own JSON parser. That parser changes some values the per-value
# entry points (Document.set, create_vertex, new_edge) store exactly or
# refuse: an integer beyond 64 bits keeps only its low 64 bits, NaN and the
# infinities become strings, a non-str dict key becomes its text, and a lone
# surrogate becomes "?". A value the JSON text cannot carry unchanged keeps the
# call off the bulk path: the per-value path stores it exactly or raises.

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _str_survives_json(value: str) -> bool:
    """False for a string UTF-8 cannot encode (a lone surrogate)."""
    if value.isascii():
        return True
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def json_bulk_scalar_ok(value: Any) -> bool:
    """Whether a scalar reaches the engine through a JSON bulk path unchanged.

    True for None, bool, an int that fits 64 bits, a finite float, and a str
    UTF-8 can encode. Everything else, other types included, is False: the
    caller falls back to the per-value path. The exact types are tested first
    because every value of a large load passes through here.
    """
    kind = type(value)
    if kind is str:
        return value.isascii() or _str_survives_json(value)
    if kind is int:
        return _INT64_MIN <= value <= _INT64_MAX
    if kind is float:
        return math.isfinite(value)
    if kind is bool or value is None:
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return _INT64_MIN <= value <= _INT64_MAX
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, str):
        return _str_survives_json(value)
    return False


def _json_bulk_check(value: Any) -> None:
    """Raise ValueError for a value JSON would change on the way to the engine.

    Walks nested lists and dicts. A type json.dumps cannot encode at all is
    left to it (TypeError), which is what insert_many's fallback already handles.
    """
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise ValueError(
                f"an integer beyond 64 bits cannot go through the JSON bulk path: {value}"
            )
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN and Infinity cannot go through the JSON bulk path")
    elif isinstance(value, str):
        if not _str_survives_json(value):
            raise ValueError(
                "a string with a lone surrogate cannot go through the JSON bulk path"
            )
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(
                    "a non-str dict key cannot go through the JSON bulk path"
                )
            _json_bulk_check(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _json_bulk_check(item)


def json_bulk_dumps(rows: Any) -> str:
    """json.dumps for the bulk paths: raises ValueError (or TypeError) when
    any value in `rows` would change on the way to the engine.

    `rows` is a list of dicts (one per record). The common value types are
    checked in this loop without a call per value, because every value of a
    large load passes through it; floats need no check here, since
    `allow_nan=False` makes json.dumps itself refuse NaN and the infinities.
    """
    int_min, int_max = _INT64_MIN, _INT64_MAX
    for row in rows:
        for key in row:
            if type(key) is not str:
                raise ValueError("a non-str key cannot go through the JSON bulk path")
        for value in row.values():
            kind = type(value)
            if kind is str:
                if not value.isascii() and not _str_survives_json(value):
                    raise ValueError(
                        "a string with a lone surrogate cannot go through the JSON bulk path"
                    )
            elif kind is int:
                if not int_min <= value <= int_max:
                    raise ValueError(
                        f"an integer beyond 64 bits cannot go through the JSON bulk path: {value}"
                    )
            elif kind is float or kind is bool or value is None:
                continue
            else:
                _json_bulk_check(
                    value
                )  # nested lists and dicts, and subclasses of the types above
    return json.dumps(rows, allow_nan=False)
