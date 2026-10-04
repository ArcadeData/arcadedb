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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
  static Iterator<Edge> ofType(final BasicDatabase database, final String typeName) {
    final ResultSet resultSet = database.query("sql", "select from " + Identifier.quote(typeName));
    return new ClosingEdgeIterator(resultSet);
  }

  /** The number of edges of the type, polymorphically, lightweight ones included. */
  static long countOfType(final BasicDatabase database, final String typeName) {
    try (final ResultSet resultSet = database.query("sql",
        "select count(*) as count from " + Identifier.quote(typeName))) {
      return resultSet.hasNext() ? ((Number) resultSet.next().getProperty("count")).longValue() : 0L;
    }
  }

  /**
   * Every lightweight edge of the database, each exactly once: an edge is taken from the scan of its own concrete type only, so a
   * hierarchy of lightweight types does not repeat the edges of its sub-types.
   */
  static Iterator<Edge> all(final BasicDatabase database) {
    final List<Iterator<Edge>> scans = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType && isHeldBy(type)) {
        final String typeName = type.getName();
        final Iterator<Edge> scan = ofType(database, typeName);
        scans.add(new Iterator<>() {
          private Edge next;

          @Override
          public boolean hasNext() {
            while (next == null && scan.hasNext()) {
              final Edge edge = scan.next();
              if (edge.getIdentity() != null && edge.getIdentity().getPosition() < 0 && typeName.equals(edge.getTypeName()))
                next = edge;
            }
            return next != null;
          }

          @Override
          public Edge next() {
            if (!hasNext())
              throw new NoSuchElementException();
            final Edge result = next;
            next = null;
            return result;
          }
        });
      }

    if (scans.isEmpty())
      return Collections.emptyIterator();

    return new Iterator<>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        while (index < scans.size()) {
          if (scans.get(index).hasNext())
            return true;
          ++index;
        }
        return false;
      }

      @Override
      public Edge next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return scans.get(index).next();
      }
    };
  }

  private static final class ClosingEdgeIterator implements Iterator<Edge> {
    private final ResultSet resultSet;

    private ClosingEdgeIterator(final ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
      if (resultSet.hasNext())
        return true;
      resultSet.close();
      return false;
    }

    @Override
    public Edge next() {
      final Result result = resultSet.next();
      return (Edge) result.toElement();
    }
  }
}
