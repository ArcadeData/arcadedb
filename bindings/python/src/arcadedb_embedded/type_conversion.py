"""
Type conversion utilities for Java to Python type mapping.

Handles automatic conversion of Java objects to native Python types for better
developer experience and integration with Python ecosystem (pandas, numpy, etc.).
"""

from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, NamedTuple


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
}


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

    java_core_types, java_collection_types = _get_java_core_types()
    if java_core_types is not None and java_collection_types is not None:
        if isinstance(value, java_core_types.boolean):
            return bool(value)
        if isinstance(value, java_core_types.string):
            return str(value)
        if isinstance(
            value,
            (
                java_core_types.integer,
                java_core_types.long_type,
                java_core_types.short,
                java_core_types.byte,
            ),
        ):
            return int(value)
        if isinstance(value, (java_core_types.float_type, java_core_types.double)):
            return float(value)
        if isinstance(value, java_core_types.character):
            return str(value)

        if isinstance(value, java_core_types.big_decimal):
            return Decimal(str(value))
        if isinstance(value, java_core_types.big_integer):
            return int(str(value))

        if isinstance(value, java_core_types.java_date):
            return datetime.fromtimestamp(value.getTime() / 1000.0)

        try:
            from java.time import Instant, LocalDate, LocalDateTime, ZonedDateTime

            if isinstance(value, LocalDate):
                return date(
                    value.getYear(), value.getMonthValue(), value.getDayOfMonth()
                )

            if isinstance(value, LocalDateTime):
                return datetime(
                    value.getYear(),
                    value.getMonthValue(),
                    value.getDayOfMonth(),
                    value.getHour(),
                    value.getMinute(),
                    value.getSecond(),
                    value.getNano() // 1000,
                )

            if isinstance(value, Instant):
                return datetime.fromtimestamp(
                    value.getEpochSecond() + value.getNano() / 1_000_000_000.0,
                    tz=timezone.utc,
                )

            if isinstance(value, ZonedDateTime):
                instant = value.toInstant()
                return datetime.fromtimestamp(
                    instant.getEpochSecond() + instant.getNano() / 1_000_000_000.0,
                    tz=timezone.utc,
                )
        except ImportError:
            pass

        if isinstance(value, java_collection_types.map_type):
            return {
                convert_java_to_python(k): convert_java_to_python(v)
                for k, v in value.items()
            }

        if isinstance(value, java_collection_types.set_type):
            return {convert_java_to_python(item) for item in value}

        if isinstance(value, java_collection_types.list_type):
            return [convert_java_to_python(item) for item in value]

        if isinstance(value, java_collection_types.collection):
            return [convert_java_to_python(item) for item in value]

    if (
        hasattr(value, "__len__")
        and hasattr(value, "__getitem__")
        and not isinstance(value, (str, bytes))
    ):
        try:
            return [convert_java_to_python(item) for item in value]
        except (TypeError, AttributeError):
            pass

    # Return as-is if no conversion available
    # This could be a Java object like Vertex, Edge, Document, etc.
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
