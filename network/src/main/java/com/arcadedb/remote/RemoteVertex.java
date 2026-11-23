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

  /**
   * Most vertex RIDs {@link #getVerticesPaged} puts in one {@code select from [...]} query: a larger page of edges reads its
   * vertices in several queries, so the query text and the request body stay bounded whatever page size the caller picks.
   */
  static final int MAX_RIDS_PER_QUERY = DEFAULT_PAGE_SIZE;

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
   * did. {@link Vertex.DIRECTION#BOTH} reads the outgoing edges, then the incoming ones, as the embedded API does, so a
   * self-loop is returned twice, once from each list. The edges come in RID order, not in the order of the edge list.
   * <p>
   * A lightweight edge has no RID of its own, so a page resumes after it by the vertex at its other end and skips only the
   * edges of the same type between the same two vertices (twins) already read: deleting any other edge already read cannot
   * hide a live one, deleting an already read twin can.
   * <p>
   * Cost: the server cannot seek into the edge list of a vertex by RID, so every page reads all the edges of the vertex in
   * the direction, filters them on the RID bound and keeps the first {@code pageSize} in a bounded top-K heap: O(E) record
   * reads and O(E log pageSize) comparisons per page, O(E^2 / pageSize) reads over a full walk, the same reads per page as
   * the SKIP form it replaces. A larger page size means fewer passes on a large hub.
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
    // the vertex at the other end of the last lightweight edge read: set only when lastEdge is the bound of the lightweight
    // edges of one bucket, null when it is the RID of a record-backed edge
    private       RID                lastFar        = null;
    // lightweight edges between the same two vertices as the last one (its twins) already read, to be skipped
    private       int                skip           = 0;
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
        final List<Edge> edges = fetchEdgesAfter(pageDirection, edgeTypes, lastEdge, lastFar, skip, pageSize);
        if (edges.size() < pageSize) {
          // the last page of this direction
          ++directionIndex;
          lastEdge = null;
          lastFar = null;
          skip = 0;
        } else
          advanceCursor(pageDirection, edges);

        currentPage = elementsOf(pageDirection, edges).iterator();
      }
      return true;
    }

    /**
     * Moves the cursor past a full page. A record-backed last edge is the bound itself. A lightweight one is not: every
     * lightweight edge of a bucket prints as {@code #<bucket>:-1}, and the server orders them by their endpoints, which the
     * literal does not carry. In the list of one vertex one endpoint is always that vertex, so the order of the lightweight
     * edges of a bucket is the order of the vertex at their other end. The cursor is then the bucket's {@code #<bucket>:-1}
     * plus that vertex ({@code lastFar}): the next page starts at the edges to it or after it, and skips only the twins of the
     * last edge (same type and same two vertices, so indistinguishable from it) already read. Deleting a lightweight edge
     * already read moves nothing but the twins, which are the same edge by definition.
     */
    private void advanceCursor(final Vertex.DIRECTION pageDirection, final List<Edge> edges) {
      final Edge lastRead = edges.getLast();
      final RID last = lastRead.getIdentity();
      if (last.getPosition() >= 0) {
        lastEdge = last;
        lastFar = null;
        skip = 0;
        return;
      }

      final int bucketId = last.getBucketId();
      final RID far = otherEnd(pageDirection, lastRead);
      int twins = 0;
      for (final Edge edge : edges) {
        final RID rid = edge.getIdentity();
        if (rid.getPosition() < 0 && rid.getBucketId() == bucketId && far.equals(otherEnd(pageDirection, edge)))
          ++twins;
      }

      // the page started past the twins already read when the last edge is one of the same twins as before
      skip = far.equals(lastFar) && lastEdge.getBucketId() == bucketId ? skip + twins : twins;
      lastEdge = new RID(bucketId, -1L);
      lastFar = far;
    }

    @Override
    public T next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return currentPage.next();
    }
  }

  /** The vertex at the other end of {@code edge} from the vertex of an edge list read in {@code direction}. */
  private static RID otherEnd(final Vertex.DIRECTION direction, final Edge edge) {
    return direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
  }

  /**
   * Up to {@code limit} edges of {@code direction} ({@code OUT} or {@code IN}) in RID order, the ones after {@code after}
   * less the first {@code skip} of them, or from the first one when it is null. The edge list carries no RID order the
   * engine could seek on (it is newest first), so the server expands every edge and ORDER BY ... LIMIT keeps the smallest
   * {@code limit} of the ones after the bound in a bounded heap rather than sorting them all.
   * <p>
   * Lightweight edges share one RID per bucket, {@code #<bucket>:-1}: {@code after} is then that RID and {@code afterFar}
   * the vertex at the other end of the last one read, and the edges returned are the record-backed ones of the bucket and
   * of the buckets after it, and the lightweight ones of the bucket to {@code afterFar} or after it, in the order the server
   * sorts them (by the endpoints, so by that vertex). {@code skip} is then the number of twins of the last one already read.
   * Both are null/zero past a record-backed edge (see {@code advanceCursor}).
   */
  private List<Edge> fetchEdgesAfter(final Vertex.DIRECTION direction, final String[] types, final RID after,
      final RID afterFar, final int skip, final int limit) {
    final StringBuilder query = new StringBuilder("select from ( select expand( ")
        .append(direction == Vertex.DIRECTION.OUT ? "outE" : "inE").append("(");
    final Map<String, Object> params = new HashMap<>();
    appendBoundTypes(query, types, params);

    query.append(") ) from ").append(vertex.getIdentity()).append(" )");
    // RID literals, not parameters: a RID sent as a JSON parameter is bound as a string. The literals are safe to inline:
    // RID#toString always prints the controlled "#<bucket>:<position>" form of two numbers, nothing a caller can shape
    if (after != null) {
      query.append(" WHERE @rid > ").append(after);
      if (afterFar != null)
        // the lightweight edges of the bucket sort before its first record (#<bucket>:0): of those, only the ones to the
        // vertex of the last one read, or after it
        query.append(" AND (@rid >= ").append(new RID(after.getBucketId(), 0L)).append(" OR ")
            .append(direction == Vertex.DIRECTION.OUT ? "@in" : "@out").append(" >= ").append(afterFar).append(')');
    }
    query.append(" ORDER BY @rid");
    if (skip > 0)
      query.append(" SKIP ").append(skip);
    query.append(" LIMIT ").append(limit);

    final List<Edge> edges = new ArrayList<>(limit);
    try (final ResultSet rs = remoteDatabase.queryUncounted("sql", query.toString(), params)) {
      while (rs.hasNext())
        edges.add(rs.next().getEdge().get());
    }
    return edges;
  }

  /**
   * The vertices on the other side of {@code edges}, in the order of the edges, read by RID at most
   * {@link #MAX_RIDS_PER_QUERY} at a time so a large caller page size does not build an unbounded query. A vertex deleted
   * since its edge was read is left out.
   */
  private List<Vertex> otherVertices(final Vertex.DIRECTION direction, final List<Edge> edges) {
    if (edges.isEmpty())
      return Collections.emptyList();

    final Set<RID> rids = new LinkedHashSet<>(edges.size());
    for (final Edge edge : edges)
      rids.add(otherEnd(direction, edge));

    final Map<RID, Vertex> byRid = new HashMap<>(rids.size());
    final StringBuilder query = new StringBuilder();
    int inQuery = 0;
    for (final RID rid : rids) {
      // the same controlled "#<bucket>:<position>" literal as in fetchEdgesAfter
      query.append(inQuery == 0 ? "select from [" : ", ").append(rid);
      if (++inQuery == MAX_RIDS_PER_QUERY) {
        readVertices(query, byRid);
        inQuery = 0;
      }
    }
    if (inQuery > 0)
      readVertices(query, byRid);

    final List<Vertex> vertices = new ArrayList<>(edges.size());
    for (final Edge edge : edges) {
      final Vertex v = byRid.get(otherEnd(direction, edge));
      if (v != null)
        vertices.add(v);
    }
    return vertices;
  }

  /** Runs the {@code select from [rids} query built so far, closing the list, then empties the builder. */
  private void readVertices(final StringBuilder query, final Map<RID, Vertex> byRid) {
    query.append(']');
    try (final ResultSet rs = remoteDatabase.queryUncounted("sql", query.toString(), Collections.emptyMap())) {
      while (rs.hasNext()) {
        final Vertex v = rs.next().getVertex().get();
        byRid.put(v.getIdentity(), v);
      }
    }
    query.setLength(0);
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
