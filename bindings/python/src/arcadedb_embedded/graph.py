"""
ArcadeDB Python Bindings - Graph API Wrappers

Wrappers for Document, Vertex, and Edge objects.
"""

from typing import Any, Dict, List, Optional, Tuple

import jpype

from .exceptions import ArcadeDBError
from .type_conversion import convert_java_to_python, convert_python_to_java

_GRAPH_CLASSES = None


def _graph_classes():
    """``(com.arcadedb.graph.Vertex, com.arcadedb.graph.Edge)``, resolved on first use."""
    global _GRAPH_CLASSES
    if _GRAPH_CLASSES is None:
        _GRAPH_CLASSES = (
            jpype.JClass("com.arcadedb.graph.Vertex"),
            jpype.JClass("com.arcadedb.graph.Edge"),
        )
    return _GRAPH_CLASSES


_GRAPH_CALLS = None


def _graph_calls():
    """``com.arcadedb.python.GraphCalls`` from the bridge jar; None (with the
    bridge's usual one warning) when the jar is missing, which sends the caller
    down the plain path."""
    global _GRAPH_CALLS
    if _GRAPH_CALLS is None:
        from .results import _bridge_class

        _GRAPH_CALLS = _bridge_class("GraphCalls") or False
    return _GRAPH_CALLS or None


class Document:
    """Wrapper for ArcadeDB Document.

    A record read from the database keeps the ``Database`` it came from alive:
    the engine loads a record's properties lazily, so the record is only as
    good as its database. Once that database is closed, reading the record
    raises ArcadeDBError instead of returning empty values.
    """

    def __init__(self, java_document, database=None):
        self._java_document = java_document
        self._database = database  # strong reference, see the class docstring
        self._property_names_cache: Optional[Tuple[str, ...]] = None

    def _check_open(self) -> None:
        database = self._database
        if database is not None and database._closed:
            raise ArcadeDBError(
                "Database is closed: a record cannot be read after its "
                "database was closed. Read what you need before closing it."
            )

    def _property_names_tuple(self) -> Tuple[str, ...]:
        self._check_open()
        if self._property_names_cache is None:
            self._property_names_cache = tuple(
                str(name) for name in self._java_document.getPropertyNames()
            )
        return self._property_names_cache

    @staticmethod
    def wrap(java_record, database=None):
        """
        Wrap a Java Record object in the appropriate Python wrapper.

        Args:
            java_record: Java Record, Vertex, Edge, or Document object
            database: The ``Database`` the record was read from, kept alive by
                the wrapper and checked on every read (None: no check)

        Returns:
            Document, Vertex, or Edge wrapper
        """
        if java_record is None:
            return None

        # Class handles are resolved once: the JClass lookup and the getClass().getName()
        # crossings this used to repeat per record cost more than the wrapping itself.
        # A record that is an instance of the Java interface is a Vertex or an Edge whatever
        # its concrete class is called.
        vertex_class, edge_class = _graph_classes()
        if isinstance(java_record, vertex_class):
            return Vertex(java_record, database)
        if isinstance(java_record, edge_class):
            return Edge(java_record, database)
        return Document(java_record, database)

    def get(self, name: str, convert_types: bool = True) -> Any:
        """Get property value."""
        value = self.get_raw(name)
        if convert_types:
            return convert_java_to_python(value)
        return value

    def get_raw(self, name: str) -> Any:
        """Get property value without Java-to-Python conversion."""
        self._check_open()
        if not self._java_document.has(name):
            return None
        return self._java_document.get(name)

    def set(self, name: str, value: Any) -> "Document":
        """Set property value. If object is immutable, raises an error."""
        # Check if document is mutable
        if not hasattr(self._java_document, "set"):
            raise AttributeError(
                f"{type(self._java_document).__name__} is immutable. "
                "Call .modify() first to get a mutable version."
            )
        self._java_document.set(name, convert_python_to_java(value))
        return self

    def save(self) -> "Document":
        """Save the document."""
        self._java_document.save()
        return self

    def delete(self):
        """Delete the record. Like every write, it needs an active transaction.

        A query row is a ``Result``, not a record: reach the record with
        ``result.get_element()`` (or ``db.lookup_by_rid()``) and delete that.
        """
        self._java_document.delete()

    def modify(self) -> "Document":
        """Get mutable version for updates."""
        self._check_open()
        return Document(self._java_document.modify(), self._database)

    def has_property(self, name: str) -> bool:
        """Check if property exists."""
        self._check_open()
        return self._java_document.has(name)

    def get_property_names(self) -> List[str]:
        """Get all property names."""
        return list(self._property_names_tuple())

    def get_identity(self):
        """Get record identity (RID)."""
        return self._java_document.getIdentity()

    def to_dict(self, convert_types: bool = True) -> Dict[str, Any]:
        """Convert to dictionary."""
        property_names = self._property_names_tuple()  # checks the database
        if not convert_types:
            return {name: self._java_document.get(name) for name in property_names}
        return {
            name: convert_java_to_python(self._java_document.get(name))
            for name in property_names
        }

    def get_rid(self) -> str:
        """Get Record ID."""
        return str(self._java_document.getIdentity())

    def get_java_document(self):
        """Expose the wrapped Java document for internal integrations."""
        return self._java_document

    def get_type_name(self) -> str:
        """Get type name."""
        self._check_open()
        return self._java_document.getTypeName()

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} rid={self.get_rid()} "
            f"type={self.get_type_name()}>"
        )


class Vertex(Document):
    """Wrapper for ArcadeDB Vertex."""

    def modify(self) -> "Vertex":
        """Get mutable version for updates."""
        self._check_open()
        return Vertex(self._java_document.modify(), self._database)

    def new_edge(self, label: str, target: "Vertex", **kwargs) -> "Edge":
        """
        Create an edge to another vertex.

        Note: Whether the edge is bidirectional is determined by the EdgeType schema
        definition, not by a per-call parameter. Use schema.create_edge_type() to
        control bidirectionality at the type level.

        Args:
            label: Edge label (type)
            target: Target vertex (Python Vertex or Java vertex object)
            **kwargs: Edge properties

        Returns:
            The created Edge object

        Example:
            >>> edge = alice.new_edge("Follows", bob, since="2024-01-01")
        """
        self._check_open()
        # Extract Java vertex if Python wrapper is provided
        target_java = (
            target.get_java_document() if isinstance(target, Vertex) else target
        )

        # Convert kwargs to flat list of [key, value, key, value...]
        props = []
        for k, v in kwargs.items():
            props.append(k)
            props.append(convert_python_to_java(v))

        # bidirectional is determined by the EdgeType schema
        java_edge = self._java_document.newEdge(label, target_java, *props)
        return Edge(java_edge, self._database)

    def get_out_edges(self, *labels: str) -> List["Edge"]:
        """Get outgoing edges."""
        self._check_open()
        calls = _graph_calls()
        if calls is not None:
            # every edge in one crossing instead of a hasNext and a next each
            database = self._database
            return [
                Edge(edge, database)
                for edge in calls.outEdges(self._java_document, *labels)
            ]
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").OUT
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge, self._database) for edge in java_edges]

    def get_in_edges(self, *labels: str) -> List["Edge"]:
        """Get incoming edges."""
        self._check_open()
        calls = _graph_calls()
        if calls is not None:
            # every edge in one crossing instead of a hasNext and a next each
            database = self._database
            return [
                Edge(edge, database)
                for edge in calls.inEdges(self._java_document, *labels)
            ]
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").IN
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge, self._database) for edge in java_edges]

    def get_both_edges(self, *labels: str) -> List["Edge"]:
        """Get both incoming and outgoing edges."""
        self._check_open()
        calls = _graph_calls()
        if calls is not None:
            # every edge in one crossing instead of a hasNext and a next each
            database = self._database
            return [
                Edge(edge, database)
                for edge in calls.bothEdges(self._java_document, *labels)
            ]
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").BOTH
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge, self._database) for edge in java_edges]


class Edge(Document):
    """Wrapper for ArcadeDB Edge."""

    def modify(self) -> "Edge":
        """Get mutable version for updates."""
        self._check_open()
        return Edge(self._java_document.modify(), self._database)

    def get_in(self) -> Vertex:
        """Get incoming vertex."""
        self._check_open()
        return Vertex(self._java_document.getInVertex(), self._database)

    def get_out(self) -> Vertex:
        """Get outgoing vertex."""
        self._check_open()
        return Vertex(self._java_document.getOutVertex(), self._database)
