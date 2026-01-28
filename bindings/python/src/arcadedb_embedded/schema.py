"""
Schema manipulation API for ArcadeDB.

This module provides a Pythonic, type-safe interface for schema operations
instead of requiring SQL strings.

Example:
    >>> db.schema.create_vertex_type("User")
    >>> db.schema.create_property("User", "email", "STRING")
    >>> db.schema.create_index("User", ["email"], unique=True)
"""

from enum import Enum
from typing import Any, List, Optional, Union

import jpype

from .exceptions import ArcadeDBError


class IndexType(Enum):
    """Index types supported by ArcadeDB."""

    LSM_TREE = "LSM_TREE"
    FULL_TEXT = "FULL_TEXT"
    LSM_VECTOR = "LSM_VECTOR"


class PropertyType(Enum):
    """Property types supported by ArcadeDB."""

    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INTEGER = "INTEGER"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    STRING = "STRING"
    BINARY = "BINARY"
    DATE = "DATE"
    DATETIME = "DATETIME"
    EMBEDDED = "EMBEDDED"
    LIST = "LIST"
    MAP = "MAP"
    LINK = "LINK"
    ARRAY_OF_FLOATS = "ARRAY_OF_FLOATS"


class Schema:
    """Schema manipulation API for ArcadeDB.

    Provides type-safe methods for creating types, properties, and indexes
    instead of using SQL strings.

    This class wraps the Java Schema interface and provides a Pythonic API.

    Attributes:
        _java_schema: The underlying Java Schema object
        _db: Reference to the parent Database instance
    """

    def __init__(self, java_schema, database):
        """Initialize Schema wrapper.

        Args:
            java_schema: Java Schema object
            database: Parent Database instance
        """
        self._java_schema = java_schema
        self._db = database

    # Type Management
    def create_document_type(self, name: str, buckets: Optional[int] = None) -> Any:
        """Create a new document type.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3)

        Returns:
            Java DocumentType object

        Raises:
            ArcadeDBError: If type already exists or database is closed

        Example:
            >>> db.schema.create_document_type("Product")
            >>> db.schema.create_document_type("LargeDataset", buckets=10)
        """
        self._db._check_not_closed()

        try:
            if buckets is None:
                return self._java_schema.createDocumentType(name)
            else:
                return self._java_schema.createDocumentType(name, buckets)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create document type '{name}': {e}") from e

    def create_vertex_type(self, name: str, buckets: Optional[int] = None) -> Any:
        """Create a new vertex type.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3)

        Returns:
            Java VertexType object

        Raises:
            ArcadeDBError: If type already exists or database is closed

        Example:
            >>> db.schema.create_vertex_type("User")
            >>> db.schema.create_vertex_type("Company", buckets=5)
        """

        self._db._check_not_closed()

        try:
            if buckets is None:
                return self._java_schema.createVertexType(name)
            else:
                return self._java_schema.createVertexType(name, buckets)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create vertex type '{name}': {e}") from e

    def create_edge_type(self, name: str, buckets: Optional[int] = None) -> Any:
        """Create a new edge type.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3)

        Returns:
            Java EdgeType object

        Raises:
            ArcadeDBError: If type already exists or database is closed

        Example:
            >>> db.schema.create_edge_type("Follows")
            >>> db.schema.create_edge_type("Purchased", buckets=5)
        """
        self._db._check_not_closed()

        try:
            if buckets is None:
                return self._java_schema.createEdgeType(name)
            else:
                return self._java_schema.createEdgeType(name, buckets)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create edge type '{name}': {e}") from e

    def get_or_create_document_type(
        self, name: str, buckets: Optional[int] = None
    ) -> Any:
        """Get existing document type or create if it doesn't exist.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3) if creating new type

        Returns:
            Java DocumentType object

        Example:
            >>> doc_type = db.schema.get_or_create_document_type("Product")
        """
        self._db._check_not_closed()

        if buckets is None:
            return self._java_schema.getOrCreateDocumentType(name)
        else:
            return self._java_schema.getOrCreateDocumentType(name, buckets)

    def get_or_create_vertex_type(
        self, name: str, buckets: Optional[int] = None
    ) -> Any:
        """Get existing vertex type or create if it doesn't exist.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3) if creating new type

        Returns:
            Java VertexType object

        Example:
            >>> vertex_type = db.schema.get_or_create_vertex_type("User")
        """
        self._db._check_not_closed()

        if buckets is None:
            return self._java_schema.getOrCreateVertexType(name)
        else:
            return self._java_schema.getOrCreateVertexType(name, buckets)

    def get_or_create_edge_type(self, name: str, buckets: Optional[int] = None) -> Any:
        """Get existing edge type or create if it doesn't exist.

        Args:
            name: Type name
            buckets: Number of buckets (default: 3) if creating new type

        Returns:
            Java EdgeType object

        Example:
            >>> edge_type = db.schema.get_or_create_edge_type("Follows")
        """
        self._db._check_not_closed()

        if buckets is None:
            return self._java_schema.getOrCreateEdgeType(name)
        else:
            return self._java_schema.getOrCreateEdgeType(name, buckets)

    def drop_type(self, name: str):
        """Drop a type and all its data.

        Args:
            name: Type name to drop

        Raises:
            ArcadeDBError: If type doesn't exist or database is closed

        Example:
            >>> db.schema.drop_type("OldType")
        """
        self._db._check_not_closed()

        try:
            self._java_schema.dropType(name)
        except Exception as e:
            raise ArcadeDBError(f"Failed to drop type '{name}': {e}") from e

    def get_type(self, name: str) -> Any:
        """Get type by name.

        Args:
            name: Type name

        Returns:
            Java DocumentType/VertexType/EdgeType object

        Raises:
            ArcadeDBError: If type doesn't exist

        Example:
            >>> user_type = db.schema.get_type("User")
        """
        self._db._check_not_closed()

        try:
            return self._java_schema.getType(name)
        except Exception:
            # Java throws exception if type doesn't exist, we return None
            return None

    def exists_type(self, name: str) -> bool:
        """Check if type exists.

        Args:
            name: Type name

        Returns:
            True if type exists, False otherwise

        Example:
            >>> if db.schema.exists_type("User"):
            ...     print("User type exists")
        """
        self._db._check_not_closed()
        return self._java_schema.existsType(name)

    def get_types(self) -> List[Any]:
        """Get all types.

        Returns:
            List of Java DocumentType/VertexType/EdgeType objects

        Example:
            >>> all_types = db.schema.get_types()
            >>> for type_obj in all_types:
            ...     print(type_obj.getName())
        """
        self._db._check_not_closed()
        return list(self._java_schema.getTypes())

    # Property Management
    def create_property(
        self,
        type_name: str,
        property_name: str,
        property_type: Union[str, PropertyType],
        of_type: Optional[str] = None,
    ) -> Any:
        """Create a property on a type.

        Args:
            type_name: Name of the type
            property_name: Name of the property
            property_type: Property type (STRING, INTEGER, LIST, etc.)
            of_type: For LIST/MAP types, the type of elements (optional)

        Returns:
            Java Property object

        Raises:
            ArcadeDBError: If type doesn't exist or property already exists

        Example:
            >>> db.schema.create_property("User", "email", "STRING")
            >>> db.schema.create_property("User", "age", PropertyType.INTEGER)
            >>> db.schema.create_property("User", "tags", "LIST", of_type="STRING")
        """
        import jpype

        self._db._check_not_closed()

        # Convert enum to string
        if isinstance(property_type, PropertyType):
            property_type = property_type.value

        if isinstance(of_type, PropertyType):
            of_type = of_type.value

        try:
            doc_type = self._java_schema.getType(type_name)

            # ARRAY_OF_* types require Type enum, not string
            array_types = (
                "ARRAY_OF_FLOATS",
                "ARRAY_OF_SHORTS",
                "ARRAY_OF_INTEGERS",
                "ARRAY_OF_LONGS",
                "ARRAY_OF_DOUBLES",
            )
            if property_type.upper() in array_types:
                # Use Type enum for array types
                Type = jpype.JPackage("com").arcadedb.schema.Type
                java_type = getattr(Type, property_type.upper())
                return doc_type.createProperty(property_name, java_type)
            elif of_type is not None:
                # For LIST/MAP types with of_type
                # createProperty(String name, Type type, String ofType)
                Type = jpype.JPackage("com").arcadedb.schema.Type
                java_type = getattr(Type, property_type.upper())
                return doc_type.createProperty(
                    property_name, java_type, of_type.upper()
                )
            else:
                # For simple types: createProperty(String name, String typeName)
                return doc_type.createProperty(property_name, property_type.upper())
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create property '{property_name}' "
                f"on type '{type_name}': {e}"
            ) from e

    def get_or_create_property(
        self,
        type_name: str,
        property_name: str,
        property_type: Union[str, PropertyType],
        of_type: Optional[str] = None,
    ) -> Any:
        """Get existing property or create if it doesn't exist.

        Args:
            type_name: Name of the type
            property_name: Name of the property
            property_type: Property type (STRING, INTEGER, LIST, etc.)
            of_type: For LIST/MAP types, the type of elements (optional)

        Returns:
            Java Property object

        Example:
            >>> prop = db.schema.get_or_create_property("User", "email", "STRING")
        """
        self._db._check_not_closed()

        # Convert enum to string
        if isinstance(property_type, PropertyType):
            property_type = property_type.value

        try:
            doc_type = self._java_schema.getType(type_name)

            # For array types, we need to use the Java Type enum
            # (getOrCreateProperty doesn't handle ARRAY_OF_* strings properly)
            if property_type.upper().startswith("ARRAY_OF_"):
                Type = jpype.JPackage("com").arcadedb.schema.Type
                java_type = getattr(Type, property_type.upper())
                if of_type is not None:
                    return doc_type.getOrCreateProperty(
                        property_name, java_type, of_type
                    )
                else:
                    return doc_type.getOrCreateProperty(property_name, java_type)
            else:
                if of_type is not None:
                    return doc_type.getOrCreateProperty(
                        property_name, property_type, of_type
                    )
                else:
                    return doc_type.getOrCreateProperty(property_name, property_type)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to get/create property '{property_name}' on type '{type_name}': {e}"
            ) from e

    def drop_property(self, type_name: str, property_name: str):
        """Drop a property from a type.

        Args:
            type_name: Name of the type
            property_name: Name of the property

        Raises:
            ArcadeDBError: If type or property doesn't exist

        Example:
            >>> db.schema.drop_property("User", "old_field")
        """

        self._db._check_not_closed()

        try:
            doc_type = self._java_schema.getType(type_name)
            doc_type.dropProperty(property_name)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to drop property '{property_name}' from type '{type_name}': {e}"
            ) from e

    # Index Management
    def create_index(
        self,
        type_name: str,
        property_names: List[str],
        unique: bool = False,
        index_type: Union[str, IndexType] = IndexType.LSM_TREE,
    ) -> Any:
        """Create an index on one or more properties.

        Args:
            type_name: Name of the type
            property_names: List of property names to index
            unique: Whether the index should enforce uniqueness
            index_type: Type of index (LSM_TREE or FULL_TEXT)

        Returns:
            Java Index object

        Raises:
            ArcadeDBError: If type doesn't exist or index creation fails

        Example:
            >>> # Simple index
            >>> db.schema.create_index("User", ["email"], unique=True)
            >>>
            >>> # Composite index
            >>> db.schema.create_index("Order", ["customerId", "orderDate"])
            >>>
            >>> # Full-text index
            >>> db.schema.create_index("Article", ["content"],
            ...                     index_type=IndexType.FULL_TEXT)
        """
        self._db._check_not_closed()

        # Convert enum to string
        if isinstance(index_type, IndexType):
            index_type_str = index_type.value
        else:
            index_type_str = index_type

        # Get Java enum value
        try:
            INDEX_TYPE = jpype.JPackage("com").arcadedb.schema.Schema.INDEX_TYPE
            java_index_type = getattr(INDEX_TYPE, index_type_str)
        except Exception as e:
            raise ArcadeDBError(f"Invalid index type '{index_type_str}': {e}") from e

        try:
            # Convert Python list to Java String array
            java_property_names = jpype.JArray(jpype.JString)(len(property_names))
            for i, prop in enumerate(property_names):
                java_property_names[i] = prop

            return self._java_schema.createTypeIndex(
                java_index_type, unique, type_name, java_property_names
            )
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create index on '{type_name}[{','.join(property_names)}]': {e}"
            ) from e

    def get_or_create_index(
        self,
        type_name: str,
        property_names: List[str],
        unique: bool = False,
        index_type: Union[str, IndexType] = IndexType.LSM_TREE,
    ) -> Any:
        """Get existing index or create if it doesn't exist.

        Args:
            type_name: Name of the type
            property_names: List of property names to index
            unique: Whether the index should enforce uniqueness
            index_type: Type of index (LSM_TREE or FULL_TEXT)

        Returns:
            Java Index object

        Example:
            >>> idx = db.schema.get_or_create_index("User", ["email"], unique=True)
        """

        self._db._check_not_closed()

        # Convert enum to string
        if isinstance(index_type, IndexType):
            index_type_str = index_type.value
        else:
            index_type_str = index_type

        # Get Java enum value
        try:
            INDEX_TYPE = jpype.JPackage("com").arcadedb.schema.Schema.INDEX_TYPE
            java_index_type = getattr(INDEX_TYPE, index_type_str)
        except Exception as e:
            raise ArcadeDBError(f"Invalid index type '{index_type_str}': {e}") from e

        try:
            # Convert Python list to Java String array
            java_property_names = jpype.JArray(jpype.JString)(len(property_names))
            for i, prop in enumerate(property_names):
                java_property_names[i] = prop

            return self._java_schema.getOrCreateTypeIndex(
                java_index_type, unique, type_name, java_property_names
            )
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to get/create index on '{type_name}[{','.join(property_names)}]': {e}"
            ) from e

    def drop_index(self, index_name: str, force: bool = False):
        """Drop an index.

        Args:
            index_name: Name of the index to drop
            force: If True, skip existence check (useful for corrupted/partial indexes)

        Raises:
            ArcadeDBError: If index doesn't exist (when force=False) or drop fails

        Example:
            >>> db.schema.drop_index("User[email]")
            >>> # Force drop corrupted index
            >>> db.schema.drop_index("User[email]", force=True)
        """

        self._db._check_not_closed()

        # Check if index exists first (unless force=True)
        if not force and not self.exists_index(index_name):
            raise ArcadeDBError(f"Index '{index_name}' does not exist")

        try:
            self._java_schema.dropIndex(index_name)
        except Exception as e:
            raise ArcadeDBError(f"Failed to drop index '{index_name}': {e}") from e

    def get_indexes(self) -> List[Any]:
        """Get all indexes.

        Returns:
            List of Java Index objects

        Example:
            >>> all_indexes = db.schema.get_indexes()
            >>> for idx in all_indexes:
            ...     print(idx.getName())
        """
        self._db._check_not_closed()
        java_indexes = self._java_schema.getIndexes()
        return list(java_indexes) if java_indexes is not None else []

    def exists_index(self, index_name: str) -> bool:
        """Check if index exists.

        Args:
            index_name: Name of the index

        Returns:
            True if index exists, False otherwise

        Example:
            >>> if db.schema.exists_index("User[email]"):
            ...     print("Index exists")
        """
        self._db._check_not_closed()
        return self._java_schema.existsIndex(index_name)

    def get_index_by_name(self, index_name: str) -> Optional[Any]:
        """Get an index by name.

        Args:
            index_name: Name of the index

        Returns:
            Java Index object or None if not found

        Example:
            >>> idx = db.schema.get_index_by_name("User[email]")
            >>> if idx:
            ...     print(f"Index type: {idx.getType()}")
        """
        self._db._check_not_closed()

        try:
            return self._java_schema.getIndexByName(index_name)
        except Exception:
            return None

    def get_vector_index(
        self,
        vertex_type: str,
        vector_property: str,
    ) -> Optional[Any]:
        """Get an existing vector index.

        Args:
            vertex_type: Name of the vertex type
            vector_property: Name of the vector property

        Returns:
            VectorIndex object, or None if not found
        """
        from .vector import VectorIndex

        self._db._check_not_closed()

        try:
            # Try LSM Vector Index (native implementation)
            # Since the name is auto-generated, we need to search for it
            try:
                # Get the type
                doc_type = self._java_schema.getType(vertex_type)
                # Get all indexes for this type
                type_indexes = doc_type.getAllIndexes(True)

                for java_index in type_indexes:
                    index_class_name = java_index.getClass().getName()

                    # Check if it's a TypeIndex (wrapper)
                    if "TypeIndex" in index_class_name:
                        # Check sub-indexes to see if they are LSMVectorIndex (JVector)
                        try:
                            sub_indexes = java_index.getSubIndexes()
                            if sub_indexes and not sub_indexes.isEmpty():
                                # Check the first sub-index
                                first_sub = sub_indexes.get(0)
                                if "LSMVectorIndex" in first_sub.getClass().getName():
                                    # Check properties on the wrapper
                                    props = java_index.getPropertyNames()
                                    if len(props) == 1 and props[0] == vector_property:
                                        return VectorIndex(java_index, self._db)
                        except Exception:
                            pass

                    # Check if it's directly an LSM vector index (JVector)
                    elif "LSMVectorIndex" in index_class_name:
                        # Check if it covers the requested property
                        props = java_index.getPropertyNames()
                        if len(props) == 1 and props[0] == vector_property:
                            return VectorIndex(java_index, self._db)
            except Exception:
                pass

            return None

        except Exception:
            return None

    def list_vector_indexes(self) -> List[str]:
        """List all vector index names in the database.

        Returns:
            List of vector index names
        """
        self._db._check_not_closed()

        try:
            indexes = []
            for java_index in self._java_schema.getIndexes():
                index_class_name = java_index.getClass().getName()

                # Check for LSM Vector (direct)
                if "LSMVectorIndex" in index_class_name:
                    indexes.append(java_index.getName())

                # Check for TypeIndex (wrapper)
                elif "TypeIndex" in index_class_name:
                    try:
                        sub_indexes = java_index.getSubIndexes()
                        if sub_indexes and not sub_indexes.isEmpty():
                            first_sub = sub_indexes.get(0)
                            if "LSMVectorIndex" in first_sub.getClass().getName():
                                indexes.append(java_index.getName())
                    except Exception:
                        pass

            return indexes
        except Exception:
            return []
