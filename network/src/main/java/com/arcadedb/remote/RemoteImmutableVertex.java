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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class RemoteImmutableVertex extends RemoteImmutableDocument implements Vertex {
  protected RemoteImmutableVertex(final RemoteDatabase database, final Map<String, Object> properties) {
    super(database, properties);
  }

  @Override
  public long countEdges(final DIRECTION direction, final String edgeType) {
    String query = "select " + direction.toString().toLowerCase() + "(";
    if (edgeType != null)
      query += "'" + edgeType + "'";

    query += ").size() as count from " + rid;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return ((Number) resultSet.next().getProperty("count")).longValue();
  }

  @Override
  public Iterable<Edge> getEdges() {
    return getEdges(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
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

  @Override
  public Iterable<Vertex> getVertices() {
    return getVertices(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
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

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    String query = "select from ( select both() as vertices from " + rid + " ) where vertices contains " + toVertex;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    String query = "select from ( select " + direction.toString().toLowerCase() + "() as vertices from " + rid + " ) where vertices contains " + toVertex;
    final ResultSet resultSet = remoteDatabase.query("sql", query);
    return resultSet.hasNext();
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional, final Object... properties) {
    if (!bidirectional)
      throw new UnsupportedOperationException("Creating unidirectional edges is not supported from remote database");

    String query = "create edge " + edgeType + " from " + rid + " to " + toVertex.getIdentity();
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
  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    throw new UnsupportedOperationException("Creating light edges is not supported from remote database");
  }

  @Override
  public synchronized MutableVertex modify() {
    return new RemoteMutableVertex(this);
  }
  @Override
  public void delete() {
    remoteDatabase.command("sql", "delete from " + rid);
  }

  private ResultSet fetch(final String suffix, final DIRECTION direction, final String[] types) {
    String query = "select expand( " + direction.toString().toLowerCase() + suffix + "(";
    for (int i = 0; i < types.length; ++i) {
      if (i > 0)
        query += ",";
      query += "'" + types[i] + "'";
    }
    query += ") ) from " + rid;
    return remoteDatabase.query("sql", query);
  }
}
