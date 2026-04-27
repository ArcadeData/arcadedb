"""
ArcadeDB Python Bindings - Graph API Wrappers

Wrappers for Document, Vertex, and Edge objects.
"""

from typing import Any, Dict, List, Optional, Tuple

import jpype

from .type_conversion import convert_java_to_python, convert_python_to_java


def _java_class_name(value: Any) -> str:
    return str(value.getClass().getName())


def _is_java_instance(value: Any, class_name: str) -> bool:
    try:
        return isinstance(value, jpype.JClass(class_name))
    except (RuntimeError, TypeError):
        return False


class Document:
    """Wrapper for ArcadeDB Document."""

    def __init__(self, java_document):
        self._java_document = java_document
        self._property_names_cache: Optional[Tuple[str, ...]] = None

    def _property_names_tuple(self) -> Tuple[str, ...]:
        if self._property_names_cache is None:
            self._property_names_cache = tuple(
                str(name) for name in self._java_document.getPropertyNames()
            )
        return self._property_names_cache

    @staticmethod
    def wrap(java_record):
        """
        Wrap a Java Record object in the appropriate Python wrapper.

        Args:
            java_record: Java Record, Vertex, Edge, or Document object

        Returns:
            Document, Vertex, or Edge wrapper
        """
        if java_record is None:
            return None

        class_name = _java_class_name(java_record)
        if _is_java_instance(
            java_record, "com.arcadedb.graph.Vertex"
        ) or class_name in {
            "com.arcadedb.graph.Vertex",
            "com.arcadedb.graph.MutableVertex",
            "com.arcadedb.graph.ImmutableVertex",
        }:
            return Vertex(java_record)
        if _is_java_instance(java_record, "com.arcadedb.graph.Edge") or class_name in {
            "com.arcadedb.graph.Edge",
            "com.arcadedb.graph.MutableEdge",
            "com.arcadedb.graph.ImmutableEdge",
        }:
            return Edge(java_record)
        return Document(java_record)

    def get(self, name: str, convert_types: bool = True) -> Any:
        """Get property value."""
        value = self.get_raw(name)
        if convert_types:
            return convert_java_to_python(value)
        return value

    def get_raw(self, name: str) -> Any:
        """Get property value without Java-to-Python conversion."""
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
        """Delete the document.

        Works reliably on records from lookup_by_rid(), less reliably on query results.
        """
        self._java_document.delete()

    def modify(self) -> "Document":
        """Get mutable version for updates."""
        return Document(self._java_document.modify())

    def has_property(self, name: str) -> bool:
        """Check if property exists."""
        return self._java_document.has(name)

    def get_property_names(self) -> List[str]:
        """Get all property names."""
        return list(self._property_names_tuple())

    def get_identity(self):
        """Get record identity (RID)."""
        return self._java_document.getIdentity()

    def to_dict(self, convert_types: bool = True) -> Dict[str, Any]:
        """Convert to dictionary."""
        property_names = self._property_names_tuple()
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
        return Vertex(self._java_document.modify())

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
        # Extract Java vertex if Python wrapper is provided
        target_java = (
            target.get_java_document() if isinstance(target, Vertex) else target
        )

        # Convert kwargs to flat list of [key, value, key, value...]
        props = []
        for k, v in kwargs.items():
            props.append(k)
            props.append(convert_python_to_java(v))

        # Use non-deprecated Java API (bidirectional is determined by EdgeType schema)
        java_edge = self._java_document.newEdge(label, target_java, *props)
        return Edge(java_edge)

    def get_out_edges(self, *labels: str) -> List["Edge"]:
        """Get outgoing edges."""
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").OUT
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge) for edge in java_edges]

    def get_in_edges(self, *labels: str) -> List["Edge"]:
        """Get incoming edges."""
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").IN
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge) for edge in java_edges]

    def get_both_edges(self, *labels: str) -> List["Edge"]:
        """Get both incoming and outgoing edges."""
        direction = jpype.JClass("com.arcadedb.graph.Vertex$DIRECTION").BOTH
        java_edges = (
            self._java_document.getEdges(direction, *labels)
            if labels
            else self._java_document.getEdges(direction)
        )
        return [Edge(edge) for edge in java_edges]


class Edge(Document):
    """Wrapper for ArcadeDB Edge."""

    def modify(self) -> "Edge":
        """Get mutable version for updates."""
        return Edge(self._java_document.modify())

    def get_in(self) -> Vertex:
        """Get incoming vertex."""
        return Vertex(self._java_document.getInVertex())

    def get_out(self) -> Vertex:
        """Get outgoing vertex."""
        return Vertex(self._java_document.getOutVertex())
