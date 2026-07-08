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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.IterableGraph;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

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
    StringBuilder query = new StringBuilder("select " + direction.toString().toLowerCase(Locale.ENGLISH) + "(");
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (int i = 0; i < edgeTypes.length; i++) {
        if (i > 0)
          query.append(", ");
        query.append("'").append(edgeTypes[i]).append("'");
      }
    }

    query.append(").size() as count from ").append(vertex.getIdentity());
    final ResultSet resultSet = remoteDatabase.query("sql", query.toString());
    return resultSet.next().<Number>getProperty("count").longValue();
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
   * Returns a lazy iterator that transparently fetches edges in pages of {@code pageSize} elements
   * over multiple HTTP calls. Each page is one SQL query with SKIP/LIMIT.
   */
  public IterableGraph<Edge> getEdgesPaged(final Vertex.DIRECTION direction, final int pageSize, final String... edgeTypes) {
    return new IterableGraph<>() {
      @Override
      public Iterator<Edge> iterator() {
        return new Iterator<>() {
          private int currentSkip = 0;
          private Iterator<Edge> currentPage = Collections.emptyIterator();
          private boolean done = false;

          private void loadNextPage() {
            if (done)
              return;
            final ResultSet rs = fetch("E", direction, edgeTypes, pageSize, currentSkip);
            final List<Edge> page = new ArrayList<>(pageSize);
            while (rs.hasNext())
              page.add(rs.next().getEdge().get());
            if (page.size() < pageSize)
              done = true;
            currentSkip += page.size();
            currentPage = page.iterator();
          }

          @Override
          public boolean hasNext() {
            if (currentPage.hasNext())
              return true;
            if (done)
              return false;
            loadNextPage();
            return currentPage.hasNext();
          }

          @Override
          public Edge next() {
            if (!hasNext())
              throw new NoSuchElementException();
            return currentPage.next();
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
   * Returns a lazy iterator that transparently fetches neighbor vertices in pages of {@code pageSize}
   * elements over multiple HTTP calls. Each page is one SQL query with SKIP/LIMIT.
   */
  public IterableGraph<Vertex> getVerticesPaged(final Vertex.DIRECTION direction, final int pageSize,
      final String... edgeTypes) {
    return new IterableGraph<>() {
      @Override
      public Iterator<Vertex> iterator() {
        return new Iterator<>() {
          private int currentSkip = 0;
          private Iterator<Vertex> currentPage = Collections.emptyIterator();
          private boolean done = false;

          private void loadNextPage() {
            if (done)
              return;
            final ResultSet rs = fetch("", direction, edgeTypes, pageSize, currentSkip);
            final List<Vertex> page = new ArrayList<>(pageSize);
            while (rs.hasNext())
              page.add(rs.next().getVertex().get());
            if (page.size() < pageSize)
              done = true;
            currentSkip += page.size();
            currentPage = page.iterator();
          }

          @Override
          public boolean hasNext() {
            if (currentPage.hasNext())
              return true;
            if (done)
              return false;
            loadNextPage();
            return currentPage.hasNext();
          }

          @Override
          public Vertex next() {
            if (!hasNext())
              throw new NoSuchElementException();
            return currentPage.next();
          }
        };
      }

      @Override
      public Class<? extends Document> getEntryType() {
        return Vertex.class;
      }
    };
  }

  public boolean isConnectedTo(final Identifiable toVertex) {
    final String query =
        "select from ( select both() as vertices from " + vertex.getIdentity() + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction) {
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "() as vertices from " + vertex.getIdentity()
            + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction, final String edgeType) {
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "('" + edgeType + "') as vertices from "
            + vertex.getIdentity() + " ) where vertices contains " + toVertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public RID moveTo(final String targetType, final String targetBucket) {
    final StringBuilder command = new StringBuilder("move vertex ").append(vertex.getIdentity()).append(" to");

    if (targetType != null)
      command.append(" TYPE:`").append(targetType).append("`");
    if (targetBucket != null)
      command.append(" `").append(targetBucket).append("`");

    final ResultSet resultSet = remoteDatabase.command("sql", command.toString());

    return resultSet.nextIfAvailable().getVertex().get().getIdentity();
  }

  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional && ((EdgeType) remoteDatabase.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return newEdge(edgeType, toVertex, properties);
  }

  public MutableEdge newEdge(String edgeType, final Identifiable toVertex, Object... properties) {

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

    StringBuilder query = new StringBuilder("create edge" + (edgeType != null ? " `" + edgeType + "`" : ""));

    if (bucketName != null) {
      query.append(" bucket `");
      query.append(bucketName);
      query.append("`");
    }

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

        query.append("`");
        query.append(propName);
        query.append("` = ");
        query.append(paramName);

        params.put(paramName, propValue);
      }
    }
    final ResultSet resultSet = remoteDatabase.command("sql", query.toString(), params);

    return new RemoteMutableEdge((RemoteImmutableEdge) resultSet.next().getEdge().get());
  }

  /**
   * TODO
   */
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  public void delete() {
    remoteDatabase.command("sql", "delete from " + vertex.getIdentity());
  }

  private ResultSet fetch(final String suffix, final Vertex.DIRECTION direction, final String[] types, final int limit,
      final int skip) {
    final StringBuilder query = new StringBuilder(
        "select expand( " + direction.toString().toLowerCase(Locale.ENGLISH) + suffix + "(");
    for (int i = 0; i < types.length; ++i) {
      if (i > 0)
        query.append(",");
      query.append("'").append(types[i]).append("'");
    }
    query.append(") ) from ").append(vertex.getIdentity());
    if (skip > 0)
      query.append(" SKIP ").append(skip);
    if (limit > 0)
      query.append(" LIMIT ").append(limit);
    return remoteDatabase.query("sql", query.toString());
  }
}
