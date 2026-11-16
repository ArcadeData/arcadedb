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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.IterableGraph;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Vertex type used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteVertex {
  /**
   * Default number of records fetched per HTTP call by {@link #getEdges} and {@link #getVertices}.
   * Callers that need a different trade-off between round-trips and response size can use
   * {@link #getEdgesPaged} / {@link #getVerticesPaged} with an explicit page size.
   */
  public static final int DEFAULT_PAGE_SIZE = 2_000;

  public final Vertex         vertex;
  public final RemoteDatabase remoteDatabase;

  protected RemoteVertex(final Vertex vertex, final RemoteDatabase remoteDatabase) {
    this.vertex = vertex;
    this.remoteDatabase = remoteDatabase;
  }

  public long countEdges(final Vertex.DIRECTION direction, final String... edgeTypes) {
    final StringBuilder query = new StringBuilder("select " + direction.toString().toLowerCase(Locale.ENGLISH) + "(");
    final Map<String, Object> params = new HashMap<>();
    appendBoundTypes(query, edgeTypes, params);

    query.append(").size() as count from ").append(vertex.getIdentity());
    try (final ResultSet resultSet = remoteDatabase.queryUncounted("sql", query.toString(), params)) {
      // the query is anchored to the vertex's own RID: it answers nothing only when the vertex no longer exists
      if (!resultSet.hasNext())
        throw new RecordNotFoundException("Record " + vertex.getIdentity() + " not found", vertex.getIdentity());

      return resultSet.next().<Number>getProperty("count").longValue();
    }
  }

  /**
   * Appends the traversal's type filter as BOUND parameters rather than as single-quoted literals.
   * <p>
   * A type name carrying a quote used to end the literal early and have its remainder parsed as more SQL. That is
   * the defect #7914 is about, and these two call sites survived both passes over this class: the first swept for
   * hand-rolled back-ticks, the second for {@code "'" + name + "'"} string concatenation, and these build the
   * literal with chained {@code append} calls instead (PR #7942 review). The traversal functions read their labels
   * from already-evaluated argument values, so a parameter is exactly as good as a literal there.
   */
  private static void appendBoundTypes(final StringBuilder query, final String[] types,
      final Map<String, Object> params) {
    if (types == null)
      return;

    for (int i = 0; i < types.length; i++) {
      if (i > 0)
        query.append(", ");

      final String name = "t" + i;
      query.append(':').append(name);
      params.put(name, types[i]);
    }
  }

  public IterableGraph<Edge> getEdges() {
    return getEdges(Vertex.DIRECTION.BOTH);
  }

  /**
   * Returns all edges for this vertex, transparently paginating over multiple HTTP calls using
   * {@link #DEFAULT_PAGE_SIZE}. Use {@link #getEdgesPaged} to control the page size explicitly.
   */
  public IterableGraph<Edge> getEdges(final Vertex.DIRECTION direction, final String... edgeTypes) {
    return getEdgesPaged(direction, DEFAULT_PAGE_SIZE, edgeTypes);
  }

  /**
   * Returns a single page of edges starting at {@code skip}, up to {@code limit} edges per call.
   * Prefer {@link #getEdges} or {@link #getEdgesPaged} for full traversal.
   */
  public IterableGraph<Edge> getEdges(final Vertex.DIRECTION direction, final int limit, final int skip,
      final String... edgeTypes) {
    final ResultSet resultSet = fetch("E", direction, edgeTypes, limit, skip);
    return new IterableGraph<>() {
      @Override
      public Iterator<Edge> iterator() {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return resultSet.hasNext();
          }

          @Override
          public Edge next() {
            return resultSet.next().getEdge().get();
          }
        };
      }

      @Override
      public Class<? extends Document> getEntryType() {
        return Edge.class;
      }
    };
  }

  /**
   * Returns a lazy iterator that transparently fetches edges in pages of {@code pageSize} elements over multiple HTTP calls.
   * Each page is one SQL query that resumes after the RID of the last edge of the previous page (issue #12191): the edges
   * the caller deletes while iterating, or another client deletes, do not move the next page, as an offset into the edge list
   * did. {@link Vertex.DIRECTION#BOTH} reads the outgoing edges, then the incoming ones, as the embedded API does.
   */
  public IterableGraph<Edge> getEdgesPaged(final Vertex.DIRECTION direction, final int pageSize, final String... edgeTypes) {
    checkPageSize(pageSize);
    return new IterableGraph<>() {
      @Override
      public Iterator<Edge> iterator() {
        return new EdgePagesIterator<>(direction, pageSize, edgeTypes) {
          @Override
          protected List<Edge> elementsOf(final Vertex.DIRECTION pageDirection, final List<Edge> edges) {
            return edges;
          }
        };
      }

      @Override
      public Class<? extends Document> getEntryType() {
        return Edge.class;
      }
    };
  }

  public IterableGraph<Vertex> getVertices() {
    return getVertices(Vertex.DIRECTION.BOTH);
  }

  /**
   * Returns all neighbor vertices, transparently paginating over multiple HTTP calls using
   * {@link #DEFAULT_PAGE_SIZE}. Use {@link #getVerticesPaged} to control the page size explicitly.
   */
  public IterableGraph<Vertex> getVertices(final Vertex.DIRECTION direction, final String... edgeTypes) {
    return getVerticesPaged(direction, DEFAULT_PAGE_SIZE, edgeTypes);
  }

  /**
   * Returns a single page of neighbor vertices starting at {@code skip}, up to {@code limit} per call.
   * Prefer {@link #getVertices} or {@link #getVerticesPaged} for full traversal.
   */
  public IterableGraph<Vertex> getVertices(final Vertex.DIRECTION direction, final int limit, final int skip,
      final String... edgeTypes) {
    final ResultSet resultSet = fetch("", direction, edgeTypes, limit, skip);
    return new IterableGraph<>() {
      @Override
      public Iterator<Vertex> iterator() {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return resultSet.hasNext();
          }

          @Override
          public Vertex next() {
            return resultSet.next().getVertex().get();
          }
        };
      }

      @Override
      public Class<? extends Document> getEntryType() {
        return Vertex.class;
      }
    };
  }

  /**
   * Returns a lazy iterator that transparently fetches neighbor vertices in pages of {@code pageSize} elements over multiple
   * HTTP calls: the page of edges {@link #getEdgesPaged} reads, then their vertices on the other side in one query by RID, so
   * deleting the vertices while iterating does not skip any either (issue #12191).
   */
  public IterableGraph<Vertex> getVerticesPaged(final Vertex.DIRECTION direction, final int pageSize,
      final String... edgeTypes) {
    checkPageSize(pageSize);
    return new IterableGraph<>() {
      @Override
      public Iterator<Vertex> iterator() {
        return new EdgePagesIterator<>(direction, pageSize, edgeTypes) {
          @Override
          protected List<Vertex> elementsOf(final Vertex.DIRECTION pageDirection, final List<Edge> edges) {
            return otherVertices(pageDirection, edges);
          }
        };
      }

      @Override
      public Class<? extends Document> getEntryType() {
        return Vertex.class;
      }
    };
  }

  private static void checkPageSize(final int pageSize) {
    if (pageSize < 1)
      throw new IllegalArgumentException("The page size must be positive, it is " + pageSize);
  }

  /**
   * Walks the edges of the vertex page by page, in RID order, each page resuming after the last RID read: the outgoing edges,
   * then the incoming ones for {@link Vertex.DIRECTION#BOTH}. Hands each page of edges to {@link #elementsOf} for what the
   * iterator returns.
   */
  private abstract class EdgePagesIterator<T> implements Iterator<T> {
    private final Vertex.DIRECTION[] directions;
    private final int                pageSize;
    private final String[]           edgeTypes;
    private       int                directionIndex = 0;
    private       RID                lastEdge       = null;
    private       Iterator<T>        currentPage    = Collections.emptyIterator();

    private EdgePagesIterator(final Vertex.DIRECTION direction, final int pageSize, final String[] edgeTypes) {
      this.directions = direction == Vertex.DIRECTION.BOTH ?
          new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
          new Vertex.DIRECTION[] { direction };
      this.pageSize = pageSize;
      this.edgeTypes = edgeTypes;
    }

    protected abstract List<T> elementsOf(Vertex.DIRECTION pageDirection, List<Edge> edges);

    @Override
    public boolean hasNext() {
      // a loop, not one load: a page of vertices can come back empty when every vertex of its edges was deleted meanwhile
      while (!currentPage.hasNext()) {
        if (directionIndex >= directions.length)
          return false;

        final Vertex.DIRECTION pageDirection = directions[directionIndex];
        final List<Edge> edges = fetchEdgesAfter(pageDirection, edgeTypes, lastEdge, pageSize);
        if (edges.size() < pageSize) {
          // the last page of this direction
          ++directionIndex;
          lastEdge = null;
        } else
          lastEdge = edges.getLast().getIdentity();

        currentPage = elementsOf(pageDirection, edges).iterator();
      }
      return true;
    }

    @Override
    public T next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return currentPage.next();
    }
  }

  /**
   * Up to {@code limit} edges of {@code direction} ({@code OUT} or {@code IN}) in RID order, the ones after {@code after}, or
   * from the first one when it is null.
   */
  private List<Edge> fetchEdgesAfter(final Vertex.DIRECTION direction, final String[] types, final RID after,
      final int limit) {
    final StringBuilder query = new StringBuilder("select from ( select expand( ")
        .append(direction == Vertex.DIRECTION.OUT ? "outE" : "inE").append("(");
    final Map<String, Object> params = new HashMap<>();
    appendBoundTypes(query, types, params);

    query.append(") ) from ").append(vertex.getIdentity()).append(" )");
    // a RID literal, not a parameter: a RID sent as a JSON parameter is bound as a string
    if (after != null)
      query.append(" WHERE @rid > ").append(after);
    query.append(" ORDER BY @rid LIMIT ").append(limit);

    final List<Edge> edges = new ArrayList<>(limit);
    try (final ResultSet rs = remoteDatabase.queryUncounted("sql", query.toString(), params)) {
      while (rs.hasNext())
        edges.add(rs.next().getEdge().get());
    }
    return edges;
  }

  /**
   * The vertices on the other side of {@code edges}, in the order of the edges, read in one query. A vertex deleted since its
   * edge was read is left out.
   */
  private List<Vertex> otherVertices(final Vertex.DIRECTION direction, final List<Edge> edges) {
    if (edges.isEmpty())
      return Collections.emptyList();

    final Set<RID> rids = new LinkedHashSet<>(edges.size());
    for (final Edge edge : edges)
      rids.add(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());

    final StringBuilder query = new StringBuilder("select from [");
    boolean first = true;
    for (final RID rid : rids) {
      if (!first)
        query.append(", ");
      query.append(rid);
      first = false;
    }
    query.append("]");

    final Map<RID, Vertex> byRid = new HashMap<>(rids.size());
    try (final ResultSet rs = remoteDatabase.queryUncounted("sql", query.toString(), Collections.emptyMap())) {
      while (rs.hasNext()) {
        final Vertex v = rs.next().getVertex().get();
        byRid.put(v.getIdentity(), v);
      }
    }

    final List<Vertex> vertices = new ArrayList<>(edges.size());
    for (final Edge edge : edges) {
      final Vertex v = byRid.get(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());
      if (v != null)
        vertices.add(v);
    }
    return vertices;
  }

  public boolean isConnectedTo(final Identifiable toVertex) {
    final String query =
        "select from ( select both() as vertices from " + vertex.getIdentity() + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.queryUncounted("sql", query, null);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction) {
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "() as vertices from " + vertex.getIdentity()
            + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.queryUncounted("sql", query, null);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction, final String edgeType) {
    // The edge type is BOUND rather than interpolated into the string literal: a type name carrying a quote used to
    // end the literal early and have its remainder parsed as more SQL (issue #7914).
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "(:edgeType) as vertices from "
            + vertex.getIdentity() + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.queryUncounted("sql", query, Map.of("edgeType", edgeType));
    return resultSet.hasNext();
  }

  public RID moveTo(final String targetType, final String targetBucket) {
    final StringBuilder command = new StringBuilder("move vertex ").append(vertex.getIdentity()).append(" to");

    if (targetType != null)
      command.append(" TYPE:").append(Identifier.quote(targetType));
    if (targetBucket != null)
      command.append(" BUCKET:").append(Identifier.quote(targetBucket));

    try (final ResultSet resultSet = remoteDatabase.command("sql", command.toString())) {
      // MOVE VERTEX answers a row per vertex it moved: none means the source vertex does not exist
      if (!resultSet.hasNext())
        throw new RecordNotFoundException("Record " + vertex.getIdentity() + " not found", vertex.getIdentity());

      return resultSet.next().getVertex().get().getIdentity();
    }
  }

  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional && ((EdgeType) remoteDatabase.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage(edgeType));

    return newEdge(edgeType, toVertex, properties);
  }

  public MutableEdge newEdge(String edgeType, final Identifiable toVertex, Object... properties) {
    // WHAT THE CALLER ASKED FOR, KEPT FOR THE MESSAGE OF A REFUSAL: edgeType is rewritten below when it names a bucket
    final String requestedType = edgeType;

    final String bucketName;
    if (edgeType.startsWith("bucket:")) {
      bucketName = edgeType.substring("bucket:".length());
      final DocumentType type = remoteDatabase.getSchema().getTypeByBucketName(bucketName);
      if (type == null)
        edgeType = null;
      else
        edgeType = type.getName();
    } else
      bucketName = null;

    final StringBuilder query = new StringBuilder("create edge").append(
        edgeType != null ? " " + Identifier.quote(edgeType) : "");

    if (bucketName != null)
      query.append(" bucket ").append(Identifier.quote(bucketName));

    query.append(" from " + vertex.getIdentity() + " to " + toVertex.getIdentity());

    final Map<String, Object> params = new HashMap<>();

    if (properties != null && properties.length > 0) {
      query.append(" set ");

      if (properties.length == 1 && properties[0] instanceof Map) {
        // GET PROPERTIES FROM THE MAP
        final Map<String, Object> map = (Map<String, Object>) properties[0];

        properties = new Object[map.size() * 2];
        int i = 0;
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
          properties[i++] = entry.getKey();
          properties[i++] = entry.getValue();
        }

      } else {
        if (properties.length % 2 != 0)
          throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
      }

      for (int i = 0; i < properties.length; i += 2) {
        final String propName = (String) properties[i];
        final Object propValue = properties[i + 1];
        final String paramName = ":p" + (i / 2);

        if (i > 0)
          query.append(", ");

        query.append(Identifier.quote(propName));
        query.append(" = ");
        query.append(paramName);

        params.put(paramName, propValue);
      }
    }
    try (final ResultSet resultSet = remoteDatabase.command("sql", query.toString(), params)) {
      // A CREATE EDGE a before-create listener vetoed answers no row at all (SaveElementStep drops it, and the plan has no CountStep).
      // A syntax or type error would have come back as a server error, so an empty answer is the veto - the exception the embedded
      // engine raises for it (issue #10373)
      if (!resultSet.hasNext())
        throw RecordVetoedException.ofCreateEdge(requestedType, vertex, toVertex);

      return new RemoteMutableEdge((RemoteImmutableEdge) resultSet.next().getEdge().get());
    }
  }

  /**
   * TODO
   */
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  public void delete() {
    // through the driver's delete, so a vertex deletes, counts and fails as every other remote record does (issue #10230)
    remoteDatabase.deleteRecord(vertex);
  }

  private ResultSet fetch(final String suffix, final Vertex.DIRECTION direction, final String[] types, final int limit,
      final int skip) {
    final StringBuilder query = new StringBuilder(
        "select expand( " + direction.toString().toLowerCase(Locale.ENGLISH) + suffix + "(");
    final Map<String, Object> params = new HashMap<>();
    appendBoundTypes(query, types, params);

    query.append(") ) from ").append(vertex.getIdentity());
    if (skip > 0)
      query.append(" SKIP ").append(skip);
    if (limit > 0)
      query.append(" LIMIT ").append(limit);
    return remoteDatabase.queryUncounted("sql", query.toString(), params);
  }
}
