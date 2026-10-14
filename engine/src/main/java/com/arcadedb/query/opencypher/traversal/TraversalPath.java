/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a path through a graph during traversal.
 * Contains the sequence of vertices and edges that form the path.
 */
public class TraversalPath {
  public static final String TYPE_KEY          = "_type";
  public static final String PATH_TYPE         = "path";
  public static final String NODES_KEY         = "nodes";
  public static final String RELATIONSHIPS_KEY = "relationships";
  public static final String LENGTH_KEY        = "length";

  private final List<Vertex> vertices;
  private final List<Edge> edges;

  public TraversalPath() {
    this.vertices = new ArrayList<>();
    this.edges = new ArrayList<>();
  }

  public TraversalPath(final Vertex startVertex) {
    this();
    vertices.add(startVertex);
  }

  /**
   * Creates a path by extending an existing path with a new edge and vertex.
   *
   * @param parent   parent path
   * @param edge     edge to add
   * @param vertex   vertex to add
   */
  public TraversalPath(final TraversalPath parent, final Edge edge, final Vertex vertex) {
    this.vertices = new ArrayList<>(parent.vertices);
    this.edges = new ArrayList<>(parent.edges);
    this.edges.add(edge);
    this.vertices.add(vertex);
  }

  /**
   * Creates a path by extending an existing path with another path's edges and vertices.
   * The extension path's start vertex is assumed to be the same as this path's end vertex,
   * so it is not duplicated.
   *
   * @param base      base path to extend
   * @param extension path to append (its first vertex is skipped since it equals base's last vertex)
   */
  public TraversalPath(final TraversalPath base, final TraversalPath extension) {
    this.vertices = new ArrayList<>(base.vertices);
    this.edges = new ArrayList<>(base.edges);
    // Append all edges from the extension
    this.edges.addAll(extension.edges);
    // Append vertices from the extension, skipping the first (it's the same as our last)
    for (int i = 1; i < extension.vertices.size(); i++)
      this.vertices.add(extension.vertices.get(i));
  }

  private TraversalPath(final ArrayList<Vertex> vertices, final ArrayList<Edge> edges) {
    this.vertices = vertices;
    this.edges = edges;
  }

  /**
   * Returns the path a value holds, or {@code null} when the value is not a path.
   * <p>
   * {@code TraversalPath} is the one runtime path value: every Cypher path - a MATCH pattern, {@code shortestPath()}, a
   * procedure's {@code path}, an {@code apoc.path.*} function - is one (issue #9913). One other shape reaches the path
   * functions from outside the engine and is read the same way: the {@code {_type: "path", nodes, relationships}} map
   * {@link #toMap()} writes, which is what a client that received a path and sends it back as a parameter holds. Its
   * elements may be the records or their RIDs. A list is never a path, even one that alternates nodes and relationships,
   * exactly as {@code IS :: PATH} answers: it carries no marker that says it is one, and reading a one-element list of a
   * node as a zero-length path would make {@code length([n])} answer instead of failing.
   * <p>
   * The map is a path only when it is one: {@code n} nodes and {@code n - 1} relationships, each relationship joining the
   * two nodes beside it, in either direction. Anything else answers {@code null}, so a caller can tell a path from a
   * value that merely looks like one.
   */
  public static TraversalPath of(final Object value) {
    if (value instanceof TraversalPath path)
      return path;

    if (value instanceof Map<?, ?> map) {
      if (!PATH_TYPE.equals(map.get(TYPE_KEY)) || !(map.get(NODES_KEY) instanceof List<?> nodes)
          || !(map.get(RELATIONSHIPS_KEY) instanceof List<?> relationships))
        return null;
      return of(nodes, relationships);
    }

    return null;
  }

  /**
   * Returns the path of the {@code [v0, e0, v1, ...]} elements a traversal walked. The caller vouches for the elements
   * being a path, so elements that are not one are a bug in the walk and raise rather than answer {@code null}.
   *
   * @throws IllegalStateException when the elements do not form a path
   */
  public static TraversalPath ofWalk(final List<?> elements) {
    // [v0, e0, v1, ..., vn]: an odd number of elements, the vertices at the even positions. One pass into the path's own
    // lists: this is the shape every walk hands over, so it costs no intermediate copy
    if (elements.size() % 2 == 1) {
      final ArrayList<Vertex> pathVertices = new ArrayList<>(elements.size() / 2 + 1);
      final ArrayList<Edge> pathEdges = new ArrayList<>(elements.size() / 2);
      boolean shaped = true;
      for (int i = 0; i < elements.size(); i++) {
        if (i % 2 == 0) {
          final Vertex vertex = asVertex(elements.get(i));
          if (vertex == null) {
            shaped = false;
            break;
          }
          pathVertices.add(vertex);
        } else {
          final Edge edge = asEdge(elements.get(i));
          if (edge == null) {
            shaped = false;
            break;
          }
          pathEdges.add(edge);
        }
      }
      if (shaped && joined(pathVertices, pathEdges))
        return new TraversalPath(pathVertices, pathEdges);
    }
    throw new IllegalStateException("The " + elements.size() + " elements walked do not form a path of alternating "
        + "vertices and the edges joining them");
  }

  /**
   * Returns the path made of {@code nodes} and the {@code relationships} joining them, or {@code null} when they do not
   * make one (see {@link #of(Object)}). The elements may be records or RIDs; a RID is loaded.
   */
  public static TraversalPath of(final List<?> nodes, final List<?> relationships) {
    if (nodes.size() != relationships.size() + 1)
      return null;

    final ArrayList<Vertex> pathVertices = new ArrayList<>(nodes.size());
    for (final Object node : nodes) {
      final Vertex vertex = asVertex(node);
      if (vertex == null)
        return null;
      pathVertices.add(vertex);
    }

    final ArrayList<Edge> pathEdges = new ArrayList<>(relationships.size());
    for (final Object relationship : relationships) {
      final Edge edge = asEdge(relationship);
      if (edge == null)
        return null;
      pathEdges.add(edge);
    }

    return joined(pathVertices, pathEdges) ? new TraversalPath(pathVertices, pathEdges) : null;
  }

  /** True when every edge joins the two vertices beside it, in either direction. */
  private static boolean joined(final List<Vertex> pathVertices, final List<Edge> pathEdges) {
    for (int i = 0; i < pathEdges.size(); i++) {
      final Edge edge = pathEdges.get(i);
      final RID from = pathVertices.get(i).getIdentity();
      final RID to = pathVertices.get(i + 1).getIdentity();
      final RID out = edge.getOut();
      final RID in = edge.getIn();
      if (!(from.equals(out) && to.equals(in)) && !(from.equals(in) && to.equals(out)))
        return false;
    }
    return true;
  }

  private static Vertex asVertex(final Object value) {
    if (value instanceof Vertex vertex)
      return vertex;
    return value instanceof Identifiable identifiable && !(value instanceof Document)
        && load(identifiable) instanceof Vertex vertex ? vertex : null;
  }

  private static Edge asEdge(final Object value) {
    if (value instanceof Edge edge)
      return edge;
    return value instanceof Identifiable identifiable && !(value instanceof Document)
        && load(identifiable) instanceof Edge edge ? edge : null;
  }

  /**
   * The record a RID a client sent points to, or null when there is none: a path map naming a record that was deleted,
   * or never existed, is not a path, which the caller reports as a type error rather than a RecordNotFoundException.
   */
  private static Object load(final Identifiable identifiable) {
    try {
      return identifiable.getRecord();
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Returns the list of vertices in this path.
   *
   * @return unmodifiable list of vertices
   */
  public List<Vertex> getVertices() {
    return Collections.unmodifiableList(vertices);
  }

  /**
   * Returns the list of edges in this path.
   *
   * @return unmodifiable list of edges
   */
  public List<Edge> getEdges() {
    return Collections.unmodifiableList(edges);
  }

  /**
   * Returns the starting vertex of this path.
   *
   * @return first vertex
   */
  public Vertex getStartVertex() {
    return vertices.isEmpty() ? null : vertices.get(0);
  }

  /**
   * Returns the ending vertex of this path.
   *
   * @return last vertex
   */
  public Vertex getEndVertex() {
    return vertices.isEmpty() ? null : vertices.get(vertices.size() - 1);
  }

  /**
   * Returns the length of this path (number of edges).
   *
   * @return path length
   */
  public int length() {
    return edges.size();
  }

  /**
   * Returns true if this path contains the specified vertex.
   *
   * @param vertex vertex to check
   * @return true if path contains vertex
   */
  public boolean containsVertex(final Vertex vertex) {
    // a plain loop, as containsEdge: this runs on every extension of a variable-length walk
    final RID vertexRid = vertex.getIdentity();
    for (int i = 0; i < vertices.size(); i++)
      if (vertices.get(i).getIdentity().equals(vertexRid))
        return true;
    return false;
  }

  /**
   * Returns true if this path already traverses the specified edge.
   * <p>
   * Compared with {@link LightEdgeRID#isSameEdge}, not with RID equality: two parallel lightweight edges of one type over
   * the same ordered pair share their RID but are two relationships, so a path may walk one and then its twin (issue
   * #9983). A linear scan rather than a set: a path is copied on every extension, its length is bounded by the hop
   * count, and the comparison walks an edge list only when two lightweight edges with the same endpoints meet.
   *
   * @param edge edge to check
   * @return true if path contains edge
   */
  public boolean containsEdge(final Edge edge) {
    final RID edgeRid = edge.getIdentity();
    for (int i = 0; i < edges.size(); i++)
      if (LightEdgeRID.isSameEdge(edges.get(i).getIdentity(), edgeRid))
        return true;
    return false;
  }

  /**
   * Adds a step to this path (mutates the path).
   *
   * @param edge   edge to add
   * @param vertex vertex to add
   */
  public void addStep(final Edge edge, final Vertex vertex) {
    edges.add(edge);
    vertices.add(vertex);
  }

  /**
   * Returns the part of this path between its relationships {@code fromEdge} (inclusive) and {@code toEdge} (exclusive):
   * those relationships and the nodes they join. {@code fromEdge == toEdge} is the zero-length path at that node.
   *
   * @throws IndexOutOfBoundsException unless {@code 0 <= fromEdge <= toEdge <= length()}
   */
  public TraversalPath subPath(final int fromEdge, final int toEdge) {
    if (fromEdge < 0 || toEdge > edges.size() || fromEdge > toEdge)
      throw new IndexOutOfBoundsException("Sub-path [" + fromEdge + ", " + toEdge + ") of a path of length " + edges.size());
    return new TraversalPath(new ArrayList<>(vertices.subList(fromEdge, toEdge + 1)),
        new ArrayList<>(edges.subList(fromEdge, toEdge)));
  }

  /**
   * Returns this path in the opposite traversal order.
   */
  public TraversalPath reversed() {
    if (vertices.isEmpty())
      return new TraversalPath();

    final TraversalPath reversed = new TraversalPath(vertices.get(vertices.size() - 1));
    for (int i = edges.size() - 1; i >= 0; i--)
      reversed.addStep(edges.get(i), vertices.get(i));
    return reversed;
  }

  /**
   * Returns the path as a flat list of alternating vertices and edges: [v0, e0, v1, e1, v2, ...].
   */
  public List<Object> toAlternatingList() {
    final List<Object> list = new ArrayList<>(vertices.size() + edges.size());
    for (int i = 0; i < vertices.size(); i++) {
      list.add(vertices.get(i));
      if (i < edges.size())
        list.add(edges.get(i));
    }
    return list;
  }

  /**
   * Returns the path as the {@code {_type: "path", nodes, relationships, length}} map, the shape a path takes where the
   * output has no path type of its own: JSON, the gRPC and PostgreSQL wire protocols. {@link #of(Object)} reads it back.
   */
  public Map<String, Object> toMap() {
    final Map<String, Object> map = new LinkedHashMap<>(6);
    map.put(TYPE_KEY, PATH_TYPE);
    map.put(NODES_KEY, getVertices());
    map.put(RELATIONSHIPS_KEY, getEdges());
    map.put(LENGTH_KEY, edges.size());
    return map;
  }

  /**
   * Reads one entry of {@link #toMap()} without building it, or {@code null} for any other name.
   * <p>
   * The procedures yielded a path as that map before they yielded a {@code TraversalPath} (issue #9913), so queries
   * written against them read {@code path.nodes}, {@code path.relationships} and {@code path.length}; property access
   * on a path value answers through here to keep them working. A path bound by a MATCH pattern is refused by the
   * semantic validator before it gets here, as in Neo4j.
   */
  public Object getMember(final String name) {
    return switch (name) {
      case NODES_KEY -> getVertices();
      case RELATIONSHIPS_KEY -> getEdges();
      case LENGTH_KEY -> edges.size();
      case TYPE_KEY -> PATH_TYPE;
      default -> null;
    };
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof TraversalPath other))
      return false;
    if (edges.size() != other.edges.size() || vertices.size() != other.vertices.size())
      return false;
    for (int i = 0; i < vertices.size(); i++)
      if (!vertices.get(i).getIdentity().equals(other.vertices.get(i).getIdentity()))
        return false;
    // LightEdgeRID.isSameEdge rather than RID equality: two parallel lightweight edges of one type over the same
    // ordered pair share their RID and are two relationships, so two paths through different twins are two paths
    // (issue #10008). hashCode() stays on the RID, which twins share: equal paths still hash alike
    for (int i = 0; i < edges.size(); i++)
      if (!LightEdgeRID.isSameEdge(edges.get(i).getIdentity(), other.edges.get(i).getIdentity()))
        return false;
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for (final Vertex v : vertices)
      hash = 31 * hash + v.getIdentity().hashCode();
    for (final Edge e : edges)
      hash = 31 * hash + e.getIdentity().hashCode();
    return hash;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vertices.size(); i++) {
      sb.append("(").append(vertices.get(i).getIdentity()).append(")");
      if (i < edges.size()) {
        sb.append("-[").append(edges.get(i).getIdentity()).append("]->");
      }
    }
    return sb.toString();
  }
}
