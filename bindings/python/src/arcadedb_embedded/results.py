"""
ArcadeDB Python Bindings - Result Set Wrappers

ResultSet and Result classes for wrapping query results.
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple

from .graph import Document, Edge, Vertex
from .type_conversion import convert_java_to_python


def _java_class_name(value: Any) -> str:
    return str(value.getClass().getName())


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
        return list(self.iter_dicts(convert_types=convert_types))

    def iter_dicts(self, convert_types: bool = True) -> Iterator[Dict[str, Any]]:
        """
        Iterate results as dictionaries.

        Args:
            convert_types: Convert Java types to Python (default: True)

        Yields:
            Result rows as dictionaries
        """
        for result in self:
            yield result.to_dict(convert_types=convert_types)

    def close(self) -> None:
        """
        Close the underlying Java result set.

        Optional: memory-benchmarked as GC-safe to omit (drained or abandoned
        result sets are collected without measurable heap growth), but closing
        deterministically matches the Java API's try-with-resources idiom and
        releases any engine-side iteration state immediately.
        """
        try:
            self._java_result_set.close()
        except Exception:  # nosec B110 - close() is best-effort hygiene
            pass

    def __enter__(self) -> "ResultSet":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def to_json_list(self, batch_size: int = 10_000) -> List[Dict[str, Any]]:
        """
        Bulk-materialize all rows via batched Java-side JSON serialization.

        The fast path for large result sets: rows are serialized to JSON in
        batches on the Java side (one JPype crossing per batch instead of
        several per row) and parsed with the C json module — measured ~6x
        faster than ``to_list()`` on wide 100k-row scans.

        Trade-off: values carry JSON-native types. Numbers, strings, booleans,
        lists and nested maps convert as expected, but temporal values arrive
        as ISO strings (not ``datetime``) and DECIMALs as floats. Use
        ``to_list()`` when full Python-type fidelity matters more than speed.

        Args:
            batch_size: Rows serialized per Java crossing (default 10000)

        Returns:
            List of dictionaries with JSON-native values

        Example:
            >>> rows = db.query("sql", "SELECT FROM Doc").to_json_list()
        """
        rows: List[Dict[str, Any]] = []
        for batch in self.iter_json_batches(batch_size=batch_size):
            rows.extend(batch)
        return rows

    def iter_json_batches(
        self, batch_size: int = 10_000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield rows as lists of dicts, one Java-serialized batch at a time.

        Streaming counterpart of :meth:`to_json_list` with the same JSON-native
        type semantics; bounds memory to one batch. Falls back to chunked
        per-row conversion when the bridge jar is unavailable.
        """
        import json

        import jpype

        try:
            row_batcher = jpype.JClass("com.arcadedb.python.RowBatcher")
        except Exception:
            chunk: List[Dict[str, Any]] = []
            for row in self.iter_dicts():
                chunk.append(row)
                if len(chunk) >= batch_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
            return

        while True:
            batch = json.loads(
                str(row_batcher.nextJsonBatch(self._java_result_set, int(batch_size)))
            )
            if not batch:
                return
            yield batch

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
        except ImportError as exc:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install with: pip install pandas"
            ) from exc

        if convert_types:
            # fast path: columnar binary transport straight into typed
            # columns (numpy dtypes, real datetime64) — measured ~2x the JSON
            # row path and ~6x the per-row path on 100k-row scans
            columns = self.to_columns()
            if columns is not None:
                return pd.DataFrame(columns)

        return pd.DataFrame(self.to_list(convert_types=convert_types))

    def to_columns(self, batch_size: int = 25_000):
        """
        Bulk-materialize all rows as columns: dict of column name -> numpy
        array (int64/float64/bool/datetime64[ms]) or Python list (strings and
        JSON-typed values). The fastest bulk path (~1.2x Java-native scans,
        measured), ideal for feeding pandas/numpy.

        Null handling follows pandas conventions: int/datetime columns with
        nulls are promoted to float64 with NaN / datetime64 NaT; string and
        JSON columns use None.

        Returns None when numpy or the bridge jar is unavailable (callers
        fall back to row-based paths).
        """
        try:
            import numpy as np
        except ImportError:
            return None

        import json

        import jpype

        try:
            column_batcher = jpype.JClass("com.arcadedb.python.ColumnBatcher")
        except Exception:
            return None

        # column order comes from the first row; empty result -> {}
        first_names = None
        merged: Dict[str, list] = {}

        def decode_batch(buf):
            nonlocal first_names
            hlen = int.from_bytes(buf[:4], "little")
            header = json.loads(bytes(buf[4 : 4 + hlen]))
            count = header["count"]
            if count == 0:
                return 0
            pos = 4 + hlen
            for col in header["cols"]:
                name, ctype = col["name"], col["type"]
                nulls_len = col["nulls"]
                null_bits = np.frombuffer(buf[pos : pos + nulls_len], dtype=np.uint8)
                has_nulls = bool(null_bits.any())
                if has_nulls:
                    mask = np.unpackbits(null_bits, bitorder="little")[:count].astype(
                        bool
                    )
                pos += nulls_len
                data = buf[pos : pos + col["bytes"]]
                pos += col["bytes"]

                if ctype == "i8":
                    arr = np.frombuffer(data, dtype="<i8")
                    if has_nulls:
                        arr = arr.astype(np.float64)
                        arr[mask] = np.nan
                    values = arr
                elif ctype == "f8":
                    arr = np.frombuffer(data, dtype="<f8").copy()
                    if has_nulls:
                        arr[mask] = np.nan
                    values = arr
                elif ctype == "dt":
                    arr = np.frombuffer(data, dtype="<i8").astype("datetime64[ms]")
                    if has_nulls:
                        arr[mask] = np.datetime64("NaT", "ms")
                    values = arr
                elif ctype == "b1":
                    arr = np.frombuffer(data, dtype=np.uint8).astype(bool)
                    if has_nulls:
                        values = [
                            None if mask[i] else bool(arr[i]) for i in range(count)
                        ]
                    else:
                        values = arr
                elif ctype == "json":
                    values = json.loads(bytes(data))
                else:  # str
                    offs = np.frombuffer(data[: (count + 1) * 4], dtype="<i4")
                    chars = bytes(data[(count + 1) * 4 :])
                    if has_nulls:
                        values = [
                            (
                                None
                                if mask[i]
                                else chars[offs[i] : offs[i + 1]].decode("utf-8")
                            )
                            for i in range(count)
                        ]
                    else:
                        values = [
                            chars[offs[i] : offs[i + 1]].decode("utf-8")
                            for i in range(count)
                        ]
                merged.setdefault(name, []).append(values)
            if first_names is None:
                first_names = [c["name"] for c in header["cols"]]
            return count

        # Java derives the column set from the first row (empty spec) and
        # every batch reports it in its header
        joined = ""
        total = 0
        while True:
            buf = memoryview(
                bytes(
                    column_batcher.nextColumnBatch(
                        self._java_result_set, int(batch_size), joined
                    )
                )
            )
            count = decode_batch(buf)
            if count == 0:
                break
            total += count
            if first_names is not None and not joined:
                joined = ";".join(first_names)  # keep column set stable

        if total == 0:
            return {}

        out = {}
        for name in first_names or []:
            parts = merged.get(name, [])
            np_parts = [p for p in parts if not isinstance(p, list)]
            if parts and len(np_parts) == len(parts):
                try:
                    out[name] = np.concatenate(parts) if len(parts) > 1 else parts[0]
                    continue
                except Exception:  # nosec B110 - fall through to generic merge
                    pass
            column = []
            for p in parts:
                column.extend(p if isinstance(p, list) else p.tolist())
            out[name] = column
        return out

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
        for row in self.iter_dicts(convert_types=convert_types):
            chunk.append(row)
            if len(chunk) >= size:
                yield chunk
                chunk = []

        if chunk:  # Yield remaining items
            yield chunk

    def count(self) -> int:
        """
        Count the remaining results without building a list.

        Note:
            This consumes the remaining rows from the current result set.
            After calling `count()`, the iterator is exhausted.

        Returns:
            Number of remaining results

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
        except StopIteration as exc:
            raise ValueError("Query returned no results") from exc

        try:
            next(iterator)
            raise ValueError("Query returned multiple results")
        except StopIteration:
            return result


class Result:
    """Wrapper for a single result from a query."""

    def __init__(self, java_result):
        self._java_result = java_result
        self._property_names_cache: Optional[Tuple[str, ...]] = None

    def _property_names_tuple(self) -> Tuple[str, ...]:
        if self._property_names_cache is None:
            self._property_names_cache = tuple(
                str(name) for name in self._java_result.getPropertyNames()
            )
        return self._property_names_cache

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

    def get(self, name: str, convert_types: bool = True) -> Any:
        """
        Get a property value from the result with automatic type conversion.

        Args:
            name: Property name

        Returns:
            Property value as a Python type, or None if not found

        Example:
            >>> result = db.query("sql", "SELECT FROM User WHERE id = 1").first()
            >>> age = result.get("age")  # Returns Python int
            >>> email = result.get("email")  # Returns Python str
            >>> phone = result.get("phone")  # Returns None if not found
            >>> phone = result.get("phone") or "unknown"  # Use default pattern
        """
        value = self.get_raw(name)
        if convert_types:
            return convert_java_to_python(value)
        return value

    def get_raw(self, name: str) -> Any:
        """
        Get a property value without Java-to-Python conversion.

        Args:
            name: Property name

        Returns:
            Raw Java-backed property value or None if not found
        """
        if not self.has_property(name):
            return None
        return self._java_result.getProperty(name)

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
            return Document.wrap(element.get())
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
        return list(self._property_names_tuple())

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
        return list(self._property_names_tuple())

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
        property_names = self._property_names_tuple()
        if not convert_types:
            return {
                name: self._java_result.getProperty(name) for name in property_names
            }

        return {
            name: convert_java_to_python(self._java_result.getProperty(name))
            for name in property_names
        }

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
            rid = self.get_rid()
            property_names = self._property_names_tuple()
            names_preview = ", ".join(repr(name) for name in property_names[:3])
            if len(property_names) > 3:
                names_preview += ", ..."
            if rid is not None:
                return f"Result(rid={rid!r}, properties=[{names_preview}])"
            return f"Result(properties=[{names_preview}])"
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return f"Result({self._java_result})"
