"""
ArcadeDB Python Bindings - Result Classes

Classes for handling query results.
"""

from typing import Any, Dict, List

from .exceptions import ArcadeDBError


class ResultSet:
    """Wrapper for ArcadeDB ResultSet."""

    def __init__(self, java_resultset):
        self._java_resultset = java_resultset

    def __iter__(self):
        return self

    def __next__(self):
        if self._java_resultset.hasNext():
            return Result(self._java_resultset.next())
        raise StopIteration

    def has_next(self) -> bool:
        """Check if there are more results."""
        return self._java_resultset.hasNext()

    def next(self) -> "Result":
        """Get the next result."""
        if self.has_next():
            return Result(self._java_resultset.next())
        raise StopIteration

    def close(self):
        """Close the result set."""
        if hasattr(self._java_resultset, "close"):
            self._java_resultset.close()


class Result:
    """Wrapper for ArcadeDB Result."""

    def __init__(self, java_result):
        self._java_result = java_result

    def get_property(self, name: str) -> Any:
        """Get a property value."""
        try:
            value = self._java_result.getProperty(name)
            # Convert Java types to Python types
            if value is not None and hasattr(value, "getClass"):
                java_class_name = value.getClass().getName()
                if "Boolean" in java_class_name:
                    return bool(value)
                elif "String" in java_class_name:
                    return str(value)
            return value
        except Exception as e:
            raise ArcadeDBError(f"Failed to get property '{name}': {e}") from e

    def has_property(self, name: str) -> bool:
        """Check if property exists."""
        return self._java_result.hasProperty(name)

    def get_property_names(self) -> List[str]:
        """Get all property names."""
        return list(self._java_result.getPropertyNames())

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        result_dict = {}
        for prop_name in self.get_property_names():
            result_dict[prop_name] = self.get_property(prop_name)
        return result_dict

    def to_json(self) -> str:
        """Convert result to JSON string."""
        return str(self._java_result.toJSON())
