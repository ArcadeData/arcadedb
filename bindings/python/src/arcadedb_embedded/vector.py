"""
ArcadeDB Python Bindings - Vector Search

Vector index and array conversion utilities for similarity search.
"""

import jpype
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
        - Returns **Cosine Distance** (Lower is better).
        - Formula: $1 - \\cos(\\theta)$
        - Range: [0.0, 2.0]
            - 0.0: Identical vectors (angle 0)
            - 1.0: Orthogonal vectors (angle 90)
            - 2.0: Opposite vectors (angle 180)

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

    def _iter_lsm_indexes(self):
        if "LSMVectorIndex" in self._java_index.getClass().getName():
            yield self._java_index
            return

        if "TypeIndex" in self._java_index.getClass().getName():
            sub_indexes = self._java_index.getSubIndexes()
            if sub_indexes and not sub_indexes.isEmpty():
                for sub in sub_indexes:
                    if "LSMVectorIndex" in sub.getClass().getName():
                        yield sub

    def _get_primary_lsm_index(self):
        for index in self._iter_lsm_indexes():
            return index
        return None

    def _ensure_product_quantization_ready(self):
        for index in self._iter_lsm_indexes():
            meta = index.getMetadata()
            if str(meta.quantizationType) != "PRODUCT":
                continue

            vector_count = int(index.countEntries())
            pq_clusters = int(meta.pqClusters)

            if 0 < vector_count < pq_clusters:
                raise ArcadeDBError(
                    "PRODUCT quantization in this ArcadeDB version requires at least "
                    "pq_clusters indexed vectors per bucket before graph build. "
                    f"Current bucket has {vector_count} vector(s) but "
                    f"pq_clusters={pq_clusters}. "
                    "Use a smaller pq_clusters value, insert more vectors, "
                    "or use INT8/BINARY/NONE."
                )

    def _normalize_ef_search(self, ef_search):
        if ef_search is None:
            return None

        if isinstance(ef_search, bool):
            raise ArcadeDBError("ef_search must be a positive integer")

        try:
            normalized = int(ef_search)
        except (TypeError, ValueError) as exc:
            raise ArcadeDBError("ef_search must be a positive integer") from exc

        if normalized < 1 or normalized != ef_search:
            raise ArcadeDBError("ef_search must be a positive integer")

        return normalized

    def _get_primary_metadata(self):
        index = self._get_primary_lsm_index()
        if index is None:
            raise ArcadeDBError("Underlying index does not expose vector metadata")
        return index.getMetadata()

    def _get_type_name(self):
        index = self._get_primary_lsm_index()
        if index is None:
            raise ArcadeDBError("Underlying index does not expose vector type metadata")
        return str(index.getTypeName())

    def _get_vector_property_name(self):
        index = self._get_primary_lsm_index()
        if index is None:
            raise ArcadeDBError(
                "Underlying index does not expose vector property metadata"
            )

        property_names = index.getPropertyNames()
        if hasattr(property_names, "get"):
            return str(property_names.get(0))
        return str(property_names[0])

    def _get_id_property_name(self):
        index = self._get_primary_lsm_index()
        if index is None:
            raise ArcadeDBError("Underlying index does not expose id-property metadata")
        return str(index.getIdPropertyName())

    def _lookup_query_vector_by_key(self, key):
        vector_property = self._get_vector_property_name()
        type_name = self._get_type_name()
        id_property = self._get_id_property_name()

        result = None
        try:
            result = self._database.lookup_by_key(type_name, [id_property], [key])
        except ArcadeDBError:
            result = None

        if result is None:
            result = self._database.query(
                "sql",
                (
                    f"SELECT {vector_property} FROM {type_name} "
                    f"WHERE {id_property} = ? LIMIT 1"
                ),
                key,
            ).first()

        if result is None:
            raise ArcadeDBError(
                f"No record found in type '{type_name}' where {id_property} = {key!r}"
            )

        query_vector = result.get(vector_property)
        if query_vector is None:
            raise ArcadeDBError(
                f"Record found for {id_property} = {key!r} "
                f"but property '{vector_property}' is null"
            )

        return query_vector

    def _build_allowed_rids_set(self, allowed_rids):
        if not allowed_rids:
            return None

        HashSet = jpype.JClass("java.util.HashSet")

        allowed_rids_set = HashSet()
        for rid_value in allowed_rids:
            allowed_rids_set.add(self._database.to_java_rid(rid_value))
        return allowed_rids_set

    def _lookup_record_by_rid(self, rid):
        record = self._database.lookup_by_rid(str(rid))
        if record is None:
            raise ArcadeDBError(f"Vector search returned missing RID: {rid}")
        return record

    def _wrap_pair_results(self, pairs):
        wrapped_results = []
        for pair in pairs:
            rid = pair.getFirst()
            score = pair.getSecond()
            wrapped_results.append((self._lookup_record_by_rid(rid), float(score)))

        return wrapped_results

    def _uses_reverse_score_sort(self):
        try:
            idx_to_check = self._get_primary_lsm_index()
            return bool(
                idx_to_check
                and str(idx_to_check.getSimilarityFunction()) == "EUCLIDEAN"
            )
        except Exception:
            return False

    def _find_neighbor_pairs(
        self,
        idx,
        *,
        java_vector,
        k,
        allowed_rids_set,
        approximate,
        ef_search=None,
    ):
        if approximate:
            if allowed_rids_set is not None:
                return idx.findNeighborsFromVectorApproximate(
                    java_vector, k, allowed_rids_set
                )
            return idx.findNeighborsFromVectorApproximate(java_vector, k)

        if allowed_rids_set is not None and ef_search is not None:
            return idx.findNeighborsFromVector(
                java_vector, k, ef_search, allowed_rids_set
            )
        if allowed_rids_set is not None:
            return idx.findNeighborsFromVector(java_vector, k, allowed_rids_set)
        if ef_search is not None:
            return idx.findNeighborsFromVector(java_vector, k, ef_search)
        return idx.findNeighborsFromVector(java_vector, k)

    def _sort_results(self, results):
        results.sort(key=lambda item: item[1], reverse=self._uses_reverse_score_sort())
        return results

    def _collect_search_results(
        self,
        *,
        java_vector,
        k,
        allowed_rids_set,
        approximate,
        ef_search=None,
    ):
        all_results = []

        for idx in self._iter_lsm_indexes():
            pairs = self._find_neighbor_pairs(
                idx,
                java_vector=java_vector,
                k=k,
                allowed_rids_set=allowed_rids_set,
                approximate=approximate,
                ef_search=ef_search,
            )

            all_results.extend(self._wrap_pair_results(pairs))

        return self._sort_results(all_results)[:k]

    def _run_search(
        self,
        *,
        query_vector,
        k,
        allowed_rids,
        approximate,
        ef_search=None,
    ):
        java_vector = to_java_float_array(query_vector)
        allowed_rids_set = self._build_allowed_rids_set(allowed_rids)

        return self._collect_search_results(
            java_vector=java_vector,
            k=k,
            allowed_rids_set=allowed_rids_set,
            approximate=approximate,
            ef_search=ef_search,
        )

    def find_nearest(
        self,
        query_vector,
        k=10,
        ef_search=None,
        allowed_rids=None,
    ):
        """
        Find k nearest neighbors to the query vector.

        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (final k). Default is 10.
            ef_search: Optional search beam width override for exact graph search. None
                uses the Java index default/adaptive behavior.
            allowed_rids: Optional list of RID strings (e.g. ["#1:0", "#2:5"]) to
                restrict search

        Returns:
            List of tuples: [(record, score), ...]
            - record: The matching ArcadeDB record (Vertex, Document, or Edge)
            - score: The similarity score/distance
        """
        effective_ef_search = self._normalize_ef_search(ef_search)

        try:
            self._ensure_product_quantization_ready()
            return self._run_search(
                query_vector=query_vector,
                k=k,
                allowed_rids=allowed_rids,
                approximate=False,
                ef_search=effective_ef_search,
            )

        except ArcadeDBError:
            raise
        except Exception as e:
            raise ArcadeDBError(f"Vector search failed: {e}") from e

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

    def get_metadata(self):
        """
        Get stable metadata for the vector index.

        Returns:
            dict: Index metadata including names, dimensions, similarity function,
            id property, quantization, and selected tuning settings.
        """
        try:
            index = self._get_primary_lsm_index()
            meta = self._get_primary_metadata()

            return {
                "index_name": str(self._java_index.getName()),
                "bucket_index_name": str(index.getName()),
                "type_name": str(index.getTypeName()),
                "vector_property": self._get_vector_property_name(),
                "dimensions": int(meta.dimensions),
                "similarity_function": str(meta.similarityFunction),
                "id_property": str(meta.idPropertyName),
                "quantization": str(meta.quantizationType),
                "max_connections": int(meta.maxConnections),
                "beam_width": int(meta.beamWidth),
                "ef_search": int(meta.efSearch),
                "location_cache_size": int(meta.locationCacheSize),
                "graph_build_cache_size": int(meta.graphBuildCacheSize),
                "mutations_before_rebuild": int(meta.mutationsBeforeRebuild),
                "store_vectors_in_graph": bool(meta.storeVectorsInGraph),
                "add_hierarchy": bool(meta.addHierarchy),
                "build_state": str(meta.buildState),
                "pq_subspaces": int(meta.pqSubspaces),
                "pq_clusters": int(meta.pqClusters),
                "pq_center_globally": bool(meta.pqCenterGlobally),
                "pq_training_limit": int(meta.pqTrainingLimit),
            }
        except Exception as e:
            raise ArcadeDBError(f"Failed to read vector index metadata: {e}") from e

    def find_nearest_by_key(
        self,
        key,
        k=10,
        ef_search=None,
        allowed_rids=None,
    ):
        """
        Find nearest neighbors using the indexed vector from an existing record.

        Args:
            key: Value of the configured id property for the source record.
            k: Number of nearest neighbors to return (default: 10).
            ef_search: Optional search beam width override for exact graph search.
            allowed_rids: Optional RID whitelist to restrict the final search.

        Returns:
            List of tuples: [(record, score), ...]
        """
        query_vector = self._lookup_query_vector_by_key(key)
        return self.find_nearest(
            query_vector,
            k=k,
            ef_search=ef_search,
            allowed_rids=allowed_rids,
        )

    def find_nearest_approximate(
        self,
        query_vector,
        k=10,
        allowed_rids=None,
    ):
        """
        Find k nearest neighbors using Product Quantization (PQ) approximate search.

        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (final k)
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

            self._ensure_product_quantization_ready()

            return self._run_search(
                query_vector=query_vector,
                k=k,
                allowed_rids=allowed_rids,
                approximate=True,
            )

        except ArcadeDBError:
            raise
        except Exception as e:
            raise ArcadeDBError(f"Approximate search failed: {e}") from e

    def get_quantization(self):
        """
        Get the quantization type of the index.

        Returns:
            str: "NONE", "INT8", "BINARY", or "PRODUCT"
        """
        try:
            idx_to_check = self._get_primary_lsm_index()

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
            self._ensure_product_quantization_ready()

            built_any = False
            for index in self._iter_lsm_indexes():
                index.buildVectorGraphNow()
                built_any = True

            if built_any:
                return

            raise ArcadeDBError(
                "Underlying index does not support vector graph rebuild"
            )
        except Exception as e:
            raise ArcadeDBError(f"Failed to rebuild vector graph: {e}") from e
