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
        Java float array compatible with ArcadeDB vector indexes
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
    Wrapper for ArcadeDB vector index (JVector-based implementation).

    This is the default and recommended implementation for vector search.
    It uses JVector (a graph index combining HNSW hierarchy with Vamana/DiskANN
    algorithms) which provides:
    - No max_items limit (grows dynamically)
    - Faster index construction
    - Automatic indexing of existing records
    - Concurrent construction support

    Provides a Pythonic interface for creating and searching vector indexes,
    with native support for NumPy arrays.

    Distance Calculation:
        The metric used depends on the `distance_function` parameter during index creation:

        1. **EUCLIDEAN** (Default):
           - Returns **Similarity Score** (Higher is better).
           - Formula: $1 / (1 + d^2)$ where $d$ is Euclidean distance.
           - Range: (0.0, 1.0]
           - 1.0: Identical vectors
           - ~0.0: Very distant vectors

        2. **COSINE**:
           - Returns **Normalized Distance** (Lower is better).
           - Formula: $(1 - \\cos(\\theta)) / 2$
           - Range: [0.0, 1.0]
           - 0.0: Identical vectors (angle 0)
           - 0.5: Orthogonal vectors (angle 90)
           - 1.0: Opposite vectors (angle 180)

        3. **DOT_PRODUCT**:
           - Returns **Negative Dot Product** (Lower is better).
           - Formula: $- (A \\cdot B)$
           - Range: (-inf, +inf)
           - Lower values indicate higher similarity (larger positive dot product).

    Quantization:
        The index supports vector quantization to reduce memory usage and speed up search:
        - **NONE** (Default): Full precision (float32).
        - **INT8**: 8-bit integer quantization.
        - **BINARY**: 1-bit quantization (for high-dimensional binary vectors).
    """

    def __init__(self, java_index, database):
        """
        Initialize VectorIndex wrapper.

        Args:
            java_index: Java LSMVectorIndex object
            database: Parent Database object
        """
        self._java_index = java_index
        self._database = database

    def find_nearest(
        self,
        query_vector,
        k=10,
        overquery_factor=4,
        allowed_rids=None,
    ):
        """
        Find k nearest neighbors to the query vector.

        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (final k). Default is 10.
            overquery_factor: Multiplier for search-time over-querying (implicit efSearch).
                              Default is 4, chosen based on benchmarks to balance recall and speed.
            allowed_rids: Optional list of RID strings (e.g. ["#1:0", "#2:5"]) to restrict search

        Returns:
            List of tuples: [(record, score), ...]
            - record: The matching ArcadeDB record (Vertex, Document, or Edge)
            - score: The similarity score/distance
        """
        try:
            # Convert query vector to Java float array
            java_vector = to_java_float_array(query_vector)

            # Import Document wrapper and Java classes once
            from com.arcadedb.database import RID
            from java.util import HashSet

            from .graph import Document

            # Prepare RID filter if provided
            allowed_rids_set = None
            if allowed_rids:
                allowed_rids_set = HashSet()
                for rid_str in allowed_rids:
                    allowed_rids_set.add(RID(self._database._java_db, rid_str))

            # Search-time over-querying
            search_k = k * max(1, int(overquery_factor))

            all_results = []

            def process_index(idx):
                if "LSMVectorIndex" in idx.getClass().getName():
                    if allowed_rids_set:
                        pairs = idx.findNeighborsFromVector(
                            java_vector, search_k, allowed_rids_set
                        )
                    else:
                        pairs = idx.findNeighborsFromVector(java_vector, search_k)

                    for pair in pairs:
                        rid = pair.getFirst()
                        score = pair.getSecond()
                        record = self._database._java_db.lookupByRID(rid, True)
                        # Wrap the record in Python wrapper
                        wrapped_record = Document.wrap(record)
                        all_results.append((wrapped_record, float(score)))

            # Handle TypeIndex (multiple buckets)
            if "TypeIndex" in self._java_index.getClass().getName():
                for sub in self._java_index.getSubIndexes():
                    process_index(sub)
            else:
                process_index(self._java_index)

            # Determine sort order
            reverse_sort = False
            try:
                idx_to_check = None
                if "LSMVectorIndex" in self._java_index.getClass().getName():
                    idx_to_check = self._java_index
                elif "TypeIndex" in self._java_index.getClass().getName():
                    subs = self._java_index.getSubIndexes()
                    if not subs.isEmpty():
                        idx_to_check = subs[0]

                if idx_to_check:
                    if str(idx_to_check.getSimilarityFunction()) == "EUCLIDEAN":
                        reverse_sort = True
            except Exception:
                pass

            all_results.sort(key=lambda x: x[1], reverse=reverse_sort)

            # Final k truncation
            return all_results[:k]

        except Exception as e:
            # print(f"Debug VectorIndex: ERROR in find_nearest: {e}")
            # import traceback
            # traceback.print_exc()
            return []

    def get_size(self):
        """
        Get the current number of items in the index.

        Returns:
            int: Number of items currently indexed
        """
        try:
            # Both TypeIndex and LSMVectorIndex support countEntries()
            return self._java_index.countEntries()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get index size: {e}") from e

    def find_nearest_approximate(
        self,
        query_vector,
        k=10,
        overquery_factor=4,
        allowed_rids=None,
    ):
        """
        Find k nearest neighbors using Product Quantization (PQ) approximate search.

        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (final k)
            overquery_factor: Multiplier for over-querying before truncation (approx efSearch analogue).
            allowed_rids: Optional list of RID strings (e.g. ["#1:0", "#2:5"]) to restrict search

        Returns:
            List of tuples: [(record, score), ...]
        """
        try:
            quantization = self.get_quantization()
            if quantization != "PRODUCT":
                raise ArcadeDBError(
                    "Approximate search requires quantization=PRODUCT (PQ)"
                )

            java_vector = to_java_float_array(query_vector)
            search_k = k * max(1, int(overquery_factor))

            from com.arcadedb.database import RID
            from java.util import HashSet

            from .graph import Document

            allowed_rids_set = None
            if allowed_rids:
                allowed_rids_set = HashSet()
                for rid_str in allowed_rids:
                    allowed_rids_set.add(RID(self._database._java_db, rid_str))

            all_results = []

            def process_index(idx):
                if "LSMVectorIndex" in idx.getClass().getName():
                    if allowed_rids_set:
                        pairs = idx.findNeighborsFromVectorApproximate(
                            java_vector, search_k, allowed_rids_set
                        )
                    else:
                        pairs = idx.findNeighborsFromVectorApproximate(
                            java_vector, search_k
                        )

                    for pair in pairs:
                        rid = pair.getFirst()
                        score = pair.getSecond()
                        record = self._database._java_db.lookupByRID(rid, True)
                        wrapped_record = Document.wrap(record)
                        all_results.append((wrapped_record, float(score)))

            if "TypeIndex" in self._java_index.getClass().getName():
                for sub in self._java_index.getSubIndexes():
                    process_index(sub)
            else:
                process_index(self._java_index)

            reverse_sort = False
            try:
                idx_to_check = None
                if "LSMVectorIndex" in self._java_index.getClass().getName():
                    idx_to_check = self._java_index
                elif "TypeIndex" in self._java_index.getClass().getName():
                    subs = self._java_index.getSubIndexes()
                    if not subs.isEmpty():
                        idx_to_check = subs[0]

                if idx_to_check:
                    if str(idx_to_check.getSimilarityFunction()) == "EUCLIDEAN":
                        reverse_sort = True
            except Exception:
                pass

            all_results.sort(key=lambda x: x[1], reverse=reverse_sort)
            return all_results[:k]

        except Exception as e:
            raise ArcadeDBError(f"Approximate search failed: {e}") from e

    def get_quantization(self):
        """
        Get the quantization type of the index.

        Returns:
            str: "NONE", "INT8", or "BINARY"
        """
        try:
            idx_to_check = None
            if "LSMVectorIndex" in self._java_index.getClass().getName():
                idx_to_check = self._java_index
            elif "TypeIndex" in self._java_index.getClass().getName():
                sub_indexes = self._java_index.getSubIndexes()
                if not sub_indexes.isEmpty():
                    idx_to_check = sub_indexes.get(0)

            if idx_to_check:
                return str(idx_to_check.getMetadata().quantizationType)
            return "NONE"
        except Exception:
            return "NONE"

    def build_graph_now(self):
        """
        Trigger an immediate rebuild of the underlying vector graph.

        Useful after bulk inserts/updates to avoid waiting for a search-triggered lazy
        build. For TypeIndex wrappers, rebuilds all underlying LSMVectorIndex instances.
        """
        try:
            if "LSMVectorIndex" in self._java_index.getClass().getName():
                self._java_index.buildVectorGraphNow()
                return

            if "TypeIndex" in self._java_index.getClass().getName():
                sub_indexes = self._java_index.getSubIndexes()
                if sub_indexes and not sub_indexes.isEmpty():
                    for sub in sub_indexes:
                        if "LSMVectorIndex" in sub.getClass().getName():
                            sub.buildVectorGraphNow()
                return

            raise ArcadeDBError(
                "Underlying index does not support vector graph rebuild"
            )
        except Exception as e:
            raise ArcadeDBError(f"Failed to rebuild vector graph: {e}") from e
