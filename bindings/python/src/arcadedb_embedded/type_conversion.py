"""
Type conversion utilities for Java to Python type mapping.

Handles automatic conversion of Java objects to native Python types for better
developer experience and integration with Python ecosystem (pandas, numpy, etc.).
"""

from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Optional


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

    try:
        # Import Java types only when needed (after JVM is started)
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

        # Primitives and wrappers
        if isinstance(value, Boolean):
            return bool(value)
        if isinstance(value, String):
            return str(value)
        if isinstance(value, (Integer, Long, Short, Byte)):
            return int(value)
        if isinstance(value, (Float, Double)):
            return float(value)
        if isinstance(value, Character):
            return str(value)

        # Numeric types
        if isinstance(value, BigDecimal):
            return Decimal(str(value))
        if isinstance(value, BigInteger):
            return int(str(value))

        # Temporal types
        if isinstance(value, JavaDate):
            # Java Date uses milliseconds since epoch
            return datetime.fromtimestamp(value.getTime() / 1000.0)

        # Try to import java.time classes (may not be available in all JVMs)
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
                    value.getNano() // 1000,  # Convert nanoseconds to microseconds
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
            pass  # java.time not available

        # Collections - process recursively
        if isinstance(value, JavaMap):
            return {
                convert_java_to_python(k): convert_java_to_python(v)
                for k, v in value.items()
            }

        if isinstance(value, JavaSet):
            return {convert_java_to_python(item) for item in value}

        if isinstance(value, JavaList):
            return [convert_java_to_python(item) for item in value]

        # Generic collection fallback
        if isinstance(value, JavaCollection):
            return [convert_java_to_python(item) for item in value]

        # Arrays (JPype converts these to Python sequences)
        # Check if it looks like a sequence but isn't a string
        if (
            hasattr(value, "__len__")
            and hasattr(value, "__getitem__")
            and not isinstance(value, (str, bytes))
        ):
            try:
                return [convert_java_to_python(item) for item in value]
            except (TypeError, AttributeError):
                pass  # Not iterable after all

    except ImportError:
        # JVM not started yet or Java classes not available
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

    # Most basic types are auto-converted by JPype
    # Special cases that need handling:

    if isinstance(value, Decimal):
        try:
            from java.math import BigDecimal

            return BigDecimal(str(value))
        except ImportError:
            return str(value)

    if isinstance(value, set):
        # Convert set to list (Java doesn't have a direct set literal)
        try:
            from java.util import HashSet

            java_set = HashSet()
            for item in value:
                java_set.add(convert_python_to_java(item))
            return java_set
        except ImportError:
            return list(value)

    if isinstance(value, dict):
        # Convert dict to Java HashMap
        try:
            from java.util import HashMap

            java_map = HashMap()
            for k, v in value.items():
                java_map.put(convert_python_to_java(k), convert_python_to_java(v))
            return java_map
        except ImportError:
            return value

    if isinstance(value, (list, tuple)):
        # Convert to Java ArrayList
        try:
            from java.util import ArrayList

            java_list = ArrayList()
            for item in value:
                java_list.add(convert_python_to_java(item))
            return java_list
        except ImportError:
            return value

    if isinstance(value, datetime):
        # Convert to Java Date
        try:
            from java.util import Date as JavaDate

            # Convert to milliseconds since epoch
            timestamp_ms = int(value.timestamp() * 1000)
            return JavaDate(timestamp_ms)
        except ImportError:
            return value

    if isinstance(value, date):
        # Convert to LocalDate if available
        try:
            from java.time import LocalDate

            return LocalDate.of(value.year, value.month, value.day)
        except ImportError:
            # Fallback: convert to datetime then to Java Date
            dt = datetime.combine(value, time.min)
            return convert_python_to_java(dt)

    # Return as-is for other types (JPype will handle them)
    return value
