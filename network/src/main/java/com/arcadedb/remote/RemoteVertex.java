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

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

/**
 * Vertex type used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteVertex {
  public final Vertex         vertex;
  public final RemoteDatabase remoteDatabase;

  protected RemoteVertex(final Vertex vertex, final RemoteDatabase remoteDatabase) {
    this.vertex = vertex;
    this.remoteDatabase = remoteDatabase;
  }

  public long countEdges(final Vertex.DIRECTION direction, final String edgeType) {
    String query = "select " + direction.toString().toLowerCase(Locale.ENGLISH) + "(";
    if (edgeType != null)
      query += "'" + edgeType + "'";

    query += ").size() as count from " + vertex.getIdentity();
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return ((Number) resultSet.next().getProperty("count")).longValue();
  }

  public Iterable<Edge> getEdges() {
    return getEdges(Vertex.DIRECTION.BOTH);
  }

  public Iterable<Edge> getEdges(final Vertex.DIRECTION direction, final String... edgeTypes) {
    final ResultSet resultSet = fetch("E", direction, edgeTypes);
    return () -> new Iterator<>() {
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

  public Iterable<Vertex> getVertices() {
    return getVertices(Vertex.DIRECTION.BOTH);
  }

  public Iterable<Vertex> getVertices(final Vertex.DIRECTION direction, final String... edgeTypes) {
    final ResultSet resultSet = fetch("", direction, edgeTypes);
    return () -> new Iterator<>() {
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

  public boolean isConnectedTo(final Identifiable toVertex) {
    final String query =
        "select from ( select both() as vertices from " + vertex.getIdentity() + " ) where vertices contains " + toVertex;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction) {
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "() as vertices from " + vertex.getIdentity()
            + " ) where vertices contains " + toVertex;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public boolean isConnectedTo(final Identifiable toVertex, final Vertex.DIRECTION direction, final String edgeType) {
    final String query =
        "select from ( select " + direction.toString().toLowerCase(Locale.ENGLISH) + "('" + edgeType + "') as vertices from "
            + vertex.getIdentity() + " ) where vertices contains " + toVertex;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional)
      throw new UnsupportedOperationException("Creating unidirectional edges is not supported from remote database");

    String query = "create edge " + edgeType + " from " + vertex.getIdentity() + " to " + toVertex.getIdentity();
    if (properties.length > 0) {
      query += " set ";
      for (int i = 0; i < properties.length; i += 2) {
        final String propName = (String) properties[i];
        final Object propValue = properties[i + 1];

        if (i > 0)
          query += ", ";

        query += propName + " = ";

        if (propValue instanceof String)
          query += "'";
        query += propValue;
        if (propValue instanceof String)
          query += "'";
      }
    }
    final ResultSet resultSet = remoteDatabase.command("sql", query);

    return new RemoteMutableEdge((RemoteImmutableEdge) resultSet.next().getEdge().get());
  }

  /**
   * TODO
   */
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  public void delete() {
    remoteDatabase.command("sql", "delete from " + vertex.getIdentity());
  }

  private ResultSet fetch(final String suffix, final Vertex.DIRECTION direction, final String[] types) {
    String query = "select expand( " + direction.toString().toLowerCase(Locale.ENGLISH) + suffix + "(";
    for (int i = 0; i < types.length; ++i) {
      if (i > 0)
        query += ",";
      query += "'" + types[i] + "'";
    }
    query += ") ) from " + vertex.getIdentity();
    return remoteDatabase.query("sql", query);
  }
}
