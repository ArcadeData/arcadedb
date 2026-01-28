"""
ArcadeDB Python Bindings - Result Set Wrappers

ResultSet and Result classes for wrapping query results.
"""

from typing import Any, Dict, Iterator, List, Optional

import jpype

from .graph import Document, Edge, Vertex
from .type_conversion import convert_java_to_python


class ResultSet:
    """Iterator wrapper for ArcadeDB query results."""

    def __init__(self, java_result_set):
        self._java_result_set = java_result_set

    def __iter__(self) -> Iterator["Result"]:
        return self

    def __next__(self) -> "Result":
        if self._java_result_set.hasNext():
            return Result(self._java_result_set.next())
        raise StopIteration

    def to_list(self, convert_types: bool = True) -> List[Dict[str, Any]]:
        """
        Convert all results to list of dictionaries.

        More efficient than iterating manually as it processes in bulk.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            List of dictionaries with result data

        Example:
            >>> results = db.query("sql", "SELECT FROM User LIMIT 10")
            >>> users = results.to_list()
            >>> print(users[0])
            {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
        """
        return [r.to_dict(convert_types=convert_types) for r in self]

    def to_dataframe(self, convert_types: bool = True):
        """
        Convert results to pandas DataFrame.

        Requires pandas to be installed.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            pandas DataFrame

        Raises:
            ImportError: If pandas is not installed

        Example:
            >>> results = db.query("sql", "SELECT FROM User")
            >>> df = results.to_dataframe()
            >>> print(df.describe())
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install with: uv pip install pandas"
            )

        return pd.DataFrame(self.to_list(convert_types=convert_types))

    def iter_chunks(
        self, size: int = 1000, convert_types: bool = True
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Iterate results in chunks for memory-efficient processing.

        Useful for processing large result sets without loading
        everything into memory at once.

        Args:
            size: Chunk size (default: 1000)
            convert_types: Convert Java types to Python (default: True)

        Yields:
            List of dictionaries (up to size elements)

        Example:
            >>> results = db.query("sql", "SELECT FROM User")
            >>> for chunk in results.iter_chunks(size=1000):
            ...     process_batch(chunk)  # chunk is list of dicts
        """
        chunk = []
        for result in self:
            chunk.append(result.to_dict(convert_types=convert_types))
            if len(chunk) >= size:
                yield chunk
                chunk = []

        if chunk:  # Yield remaining items
            yield chunk

    def count(self) -> int:
        """
        Count results without loading all into memory.

        Returns:
            Number of results

        Example:
            >>> count = db.query("sql", "SELECT FROM User").count()
            >>> print(f"Found {count} users")
        """
        count = 0
        for _ in self:
            count += 1
        return count

    def first(self) -> Optional["Result"]:
        """
        Get first result or None if no results.

        Returns:
            First Result or None

        Example:
            >>> user = db.query("sql", "SELECT FROM User WHERE id = 1").first()
            >>> if user:
            ...     print(user.get("name"))
        """
        try:
            return next(iter(self))
        except StopIteration:
            return None

    def one(self) -> "Result":
        """
        Get single result, raise error if not exactly one.

        Returns:
            The single Result

        Raises:
            ValueError: If zero or multiple results

        Example:
            >>> user = db.query("sql", "SELECT FROM User WHERE email = ?",
            ...                 "alice@example.com").one()
            >>> print(user.get("name"))
        """
        iterator = iter(self)
        try:
            result = next(iterator)
        except StopIteration:
            raise ValueError("Query returned no results")

        try:
            next(iterator)
            raise ValueError("Query returned multiple results")
        except StopIteration:
            return result


class Result:
    """Wrapper for a single result from a query."""

    def __init__(self, java_result):
        self._java_result = java_result

    def has_property(self, name: str) -> bool:
        """
        Check if a property exists in the result.

        Args:
            name: Property name

        Returns:
            True if property exists, False otherwise

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> if result.has_property("email"):
            ...     print(result.get("email"))
        """
        return self._java_result.hasProperty(name)

    def get(self, name: str) -> Any:
        """
        Get a property value from the result with automatic type conversion.

        Args:
            name: Property name

        Returns:
            Property value as Python type (int, str, Decimal, datetime, etc.), or None if not found

        Example:
            >>> result = db.query("sql", "SELECT FROM User WHERE id = 1").first()
            >>> age = result.get("age")  # Returns Python int
            >>> email = result.get("email")  # Returns Python str
            >>> phone = result.get("phone")  # Returns None if not found
            >>> phone = result.get("phone") or "unknown"  # Use default pattern
        """
        if not self.has_property(name):
            return None
        value = self._java_result.getProperty(name)
        return convert_java_to_python(value)

    def get_rid(self) -> Optional[str]:
        """
        Get the Record ID (RID) if available.

        Returns:
            RID string (e.g., "#10:5") or None

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.get_rid())
            #10:5
        """
        identity = self._java_result.getIdentity()
        if identity.isPresent():
            return str(identity.get().toString())
        return None

    def get_vertex(self) -> Optional[Vertex]:
        """
        Get the underlying Vertex object if available.

        Returns:
            Vertex object or None
        """
        vertex = self._java_result.getVertex()
        if vertex.isPresent():
            return Vertex(vertex.get())
        return None

    def get_edge(self) -> Optional[Edge]:
        """
        Get the underlying Edge object if available.

        Returns:
            Edge object or None
        """
        edge = self._java_result.getEdge()
        if edge.isPresent():
            return Edge(edge.get())
        return None

    def get_element(self) -> Optional[Document]:
        """
        Get the underlying Element (Document, Vertex, or Edge) if available.

        Returns:
            Document, Vertex, or Edge object or None
        """
        element = self._java_result.getElement()
        if element.isPresent():
            java_element = element.get()
            # Determine specific type
            if isinstance(java_element, jpype.JClass("com.arcadedb.graph.Vertex")):
                return Vertex(java_element)
            elif isinstance(java_element, jpype.JClass("com.arcadedb.graph.Edge")):
                return Edge(java_element)
            return Document(java_element)
        return None

    def get_property_names(self) -> List[str]:
        """
        Get all property names (alternative to property_names property).

        Returns:
            List of property names

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> names = result.get_property_names()
            >>> print(names)
            ['name', 'email', 'age', 'created_at']
        """
        return list(self._java_result.getPropertyNames())

    @property
    def property_names(self) -> List[str]:
        """
        Get all property names in this result.

        Returns:
            List of property names

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.property_names)
            ['name', 'email', 'age', 'created_at']
        """
        return list(self._java_result.getPropertyNames())

    def to_dict(self, convert_types: bool = True) -> Dict[str, Any]:
        """
        Convert result to dictionary.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Returns:
            Dictionary with all properties

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> user_dict = result.to_dict()
            >>> print(user_dict)
            {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
        """
        if not convert_types:
            return {
                name: self._java_result.getProperty(name)
                for name in self.property_names
            }

        return {name: self.get(name) for name in self.property_names}

    def to_json(self) -> str:
        """
        Convert result to JSON string.

        Returns:
            JSON string representation

        Example:
            >>> result = db.query("sql", "SELECT FROM User LIMIT 1").first()
            >>> print(result.to_json())
            {"name": "Alice", "age": 30, "email": "alice@example.com"}
        """
        return str(self._java_result.toJSON())

    def __repr__(self) -> str:
        """String representation of the result."""
        try:
            props = self.to_dict()
            props_str = ", ".join(f"{k}={v!r}" for k, v in list(props.items())[:3])
            if len(props) > 3:
                props_str += ", ..."
            return f"Result({props_str})"
        except Exception:
            return f"Result({self._java_result})"
