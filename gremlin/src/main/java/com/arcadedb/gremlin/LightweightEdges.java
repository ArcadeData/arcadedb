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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads the edges of a LIGHTWEIGHT edge type, which are a pair of pointers inside the two vertices and have no record, so the
 * bucket of their type is empty by construction and a scan of it answers nothing (issue #9142). The SQL planner already knows
 * how to reach them, by walking the vertices (issue #7477); this goes through it rather than repeating the walk, so the access
 * checks and the planner's own decisions stay in one place.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LightweightEdges {
  private LightweightEdges() {
  }

  /** Whether the type, or one of its sub-types, stores its edges without a record of their own. */
  static boolean isHeldBy(final DocumentType type) {
    return EdgeType.holdsLightweightEdges(type);
  }

  /** Every edge of the type, polymorphically: its records first, then its lightweight edges. Close the iterator if not drained. */
  static CloseableIterator<Edge> ofType(final BasicDatabase database, final String typeName) {
    return new ClosingEdgeIterator(database, typeName);
  }

  /** The number of edges of the type, polymorphically, lightweight ones included. */
  static long countOfType(final BasicDatabase database, final String typeName) {
    try (final ResultSet resultSet = database.query("sql",
        "select count(*) as count from " + Identifier.quote(typeName))) {
      return resultSet.hasNext() ? ((Number) resultSet.next().getProperty("count")).longValue() : 0L;
    }
  }

  /** The edge types that declare LIGHTWEIGHT: the only ones whose own edges have no record. */
  private static List<String> lightweightTypeNames(final BasicDatabase database) {
    final List<String> names = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType edgeType && edgeType.isLightweight())
        names.add(type.getName());
    return names;
  }

  /**
   * Every lightweight edge of the database, each exactly once: an edge is taken from the scan of its own concrete type only, and
   * only the types that declare LIGHTWEIGHT are scanned, so a hierarchy does not repeat the edges of its sub-types nor read the
   * records of a regular super type. A scan is opened when the previous one is drained, and the open one is released on close().
   */
  static CloseableIterator<Edge> all(final BasicDatabase database) {
    final List<String> typeNames = lightweightTypeNames(database);

    return new CloseableIterator<>() {
      private int                    index = 0;
      private CloseableIterator<Edge> scan;
      private Edge                   next;

      @Override
      public boolean hasNext() {
        while (next == null) {
          if (scan == null) {
            if (index >= typeNames.size())
              return false;
            scan = ofType(database, typeNames.get(index));
          }
          if (scan.hasNext()) {
            final Edge edge = scan.next();
            if (edge.getIdentity() != null && edge.getIdentity().getPosition() < 0 && typeNames.get(index).equals(edge.getTypeName()))
              next = edge;
          } else {
            scan.close();
            scan = null;
            ++index;
          }
        }
        return true;
      }

      @Override
      public Edge next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Edge result = next;
        next = null;
        return result;
      }

      @Override
      public void close() {
        if (scan != null) {
          scan.close();
          scan = null;
        }
        index = typeNames.size();
      }
    };
  }

  /** Whether any edge type declares LIGHTWEIGHT, which a schema walk answers without opening a query. */
  static boolean exist(final BasicDatabase database) {
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType edgeType && edgeType.isLightweight())
        return true;
    return false;
  }

  /** The SQL scan of a type, opened on the first read and released when drained or closed. */
  private static final class ClosingEdgeIterator implements CloseableIterator<Edge> {
    private final BasicDatabase database;
    private final String        typeName;
    private       ResultSet     resultSet;
    private       boolean       closed;

    private ClosingEdgeIterator(final BasicDatabase database, final String typeName) {
      this.database = database;
      this.typeName = typeName;
    }

    @Override
    public boolean hasNext() {
      if (closed)
        return false;
      if (resultSet == null)
        resultSet = database.query("sql", "select from " + Identifier.quote(typeName));
      if (resultSet.hasNext())
        return true;
      close();
      return false;
    }

    @Override
    public Edge next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return (Edge) resultSet.next().toElement();
    }

    @Override
    public void close() {
      closed = true;
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
      }
    }
  }
}
