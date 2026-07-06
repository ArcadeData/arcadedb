"""
Type conversion utilities for Java to Python type mapping.

Handles automatic conversion of Java objects to native Python types for better
developer experience and integration with Python ecosystem (pandas, numpy, etc.).
"""

from datetime import date, datetime, time, timezone
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
    big_decimal: Any
    hash_map: Any
    hash_set: Any
    java_date: Any
    local_date: Any


_UNSET = object()

_TYPE_CACHE = {
    "java_core": None,
    "java_collections": None,
    "python_to_java": None,
    "java_time": None,
}


def _get_java_time_types():
    if _TYPE_CACHE["java_time"] is not None:
        return _TYPE_CACHE["java_time"]

    try:
        from java.time import (
            Instant,
            LocalDate,
            LocalDateTime,
            OffsetDateTime,
            ZonedDateTime,
        )
    except ImportError:
        return None

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

    try:
        from java.lang import (
            Boolean,
            Byte,
            Character,
            Double,
            Float,
            Integer,
            Long,
            Short,
            String,
        )
        from java.math import BigDecimal, BigInteger
        from java.util import Collection as JavaCollection
        from java.util import Date as JavaDate
        from java.util import List as JavaList
        from java.util import Map as JavaMap
        from java.util import Set as JavaSet
    except ImportError:
        return None, None

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

    try:
        from java.math import BigDecimal
        from java.time import LocalDate
        from java.util import ArrayList
        from java.util import Date as JavaDate
        from java.util import HashMap, HashSet
    except ImportError:
        return None

    _TYPE_CACHE["python_to_java"] = _PythonToJavaTypes(
        array_list=ArrayList,
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


def _conv_instant(value):
    return datetime.fromtimestamp(
        value.getEpochSecond() + value.getNano() / 1_000_000_000.0,
        tz=timezone.utc,
    )


def _conv_zoned_datetime(value):
    instant = value.toInstant()
    return datetime.fromtimestamp(
        instant.getEpochSecond() + instant.getNano() / 1_000_000_000.0,
        tz=timezone.utc,
    )


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
    return memoryview(value).tolist()


def _register(value, converter):
    _CONVERTER_CACHE[type(value)] = converter
    return converter(value)


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
        java_list = java_python_types.array_list()
        for item in value:
            java_list.add(convert_python_to_java(item))
        return java_list

    if isinstance(value, datetime):
        if java_python_types is None:
            return value
        timestamp_ms = int(value.timestamp() * 1000)
        return java_python_types.java_date(timestamp_ms)

    if isinstance(value, date):
        if java_python_types is not None:
            return java_python_types.local_date.of(value.year, value.month, value.day)
        dt = datetime.combine(value, time.min)
        return convert_python_to_java(dt)

    # Return as-is for other types (JPype will handle them)
    return value
