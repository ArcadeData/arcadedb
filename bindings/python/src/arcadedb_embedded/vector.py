"""
ArcadeDB Python Bindings - Vector Search

Vector index and array conversion utilities for similarity search.
"""

import jpype.types as jtypes

from .exceptions import ArcadeDBError


def to_java_float_array(vector):
    """
    Convert a Python array-like object to a Java float array.

    Accepts:
    - Python lists: [0.1, 0.2, 0.3]
    - NumPy arrays: np.array([0.1, 0.2, 0.3])
    - Any array-like object with __iter__

    Args:
        vector: Array-like object containing float values

    Returns:
        Java float array compatible with ArcadeDB HNSW indexes
    """
    # Handle NumPy arrays
    try:
        import numpy as np

        if isinstance(vector, np.ndarray):
            vector = vector.tolist()
    except ImportError:
        pass

    # Convert to Python list if needed
    if not isinstance(vector, list):
        vector = list(vector)

    # Create Java float array
    return jtypes.JArray(jtypes.JFloat)(vector)


def to_python_array(java_vector, use_numpy=True):
    """
    Convert a Java array or ArrayList to a Python array.

    Args:
        java_vector: Java array or ArrayList of floats
        use_numpy: If True and NumPy is available, return np.array.
                   If False or NumPy unavailable, return Python list.

    Returns:
        NumPy array if use_numpy=True and NumPy is available, else Python list
    """
    # Convert to Python list first
    if hasattr(java_vector, "__iter__"):
        py_list = list(java_vector)
    else:
        py_list = [java_vector]

    # Try to return NumPy array if requested
    if use_numpy:
        try:
            import numpy as np

            return np.array(py_list, dtype=np.float32)
        except ImportError:
            pass

    return py_list


class VectorIndex:
    """
    Wrapper for ArcadeDB HNSW vector index.

    Provides a Pythonic interface for creating and searching vector indexes,
    with native support for NumPy arrays.
    """

    def __init__(self, java_index, database):
        """
        Initialize VectorIndex wrapper.

        Args:
            java_index: Java HnswVectorIndex object
            database: Parent Database object
        """
        self._java_index = java_index
        self._database = database

    def find_nearest(self, query_vector, k=10, use_numpy=True):
        """
        Find k nearest neighbors to the query vector.

        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (default: 10)
            use_numpy: Return vectors as NumPy arrays if available (default: True)

        Returns:
            List of tuples: [(vertex, distance), ...]
            where vertex is the matched vertex object and distance is the
            similarity score
        """
        # Convert query vector to Java float array
        java_vector = to_java_float_array(query_vector)

        # Perform search (None = no filtering callback)
        results = self._java_index.findNearest(java_vector, k, None)

        # Convert results to Python format
        neighbors = []
        for result in results:
            vertex = result.item()
            distance = result.distance()
            neighbors.append((vertex, float(distance)))

        return neighbors

    def add_vertex(self, vertex):
        """
        Add a single vertex to the index.

        Args:
            vertex: Vertex object to add (must have vector property)
        """
        try:
            self._java_index.add(vertex)
        except Exception as e:
            raise ArcadeDBError(f"Failed to add vertex to index: {e}") from e

    def remove_vertex(self, vertex_id):
        """
        Remove a vertex from the index.

        Args:
            vertex_id: ID of the vertex to remove
        """
        try:
            self._java_index.remove(vertex_id)
        except Exception as e:
            raise ArcadeDBError(f"Failed to remove vertex from index: {e}") from e
