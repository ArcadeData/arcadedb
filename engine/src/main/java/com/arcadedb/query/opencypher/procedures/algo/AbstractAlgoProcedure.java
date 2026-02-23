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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;

import java.util.*;

/**
 * Abstract base class for algorithm procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractAlgoProcedure implements CypherProcedure {

  protected Vertex extractVertex(final Object arg, final String paramName) {
    return switch (arg) {
      case null -> throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
      case Vertex vertex -> vertex;
      case Document doc when doc instanceof Vertex v -> v;
      default -> throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
    };
  }

  protected String extractString(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    return switch (arg) {
      case null -> null;
      case String s -> new String[]{s};
      case Collection<?> coll -> coll.stream().map(Object::toString).toArray(String[]::new);
      default -> new String[]{arg.toString()};
    };
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    else if (arg instanceof Map)
      return (Map<String, Object>) arg;

    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
  }

  /** @see GraphEngine#getAllVertices(Database, String[]) */
  protected Iterator<Vertex> getAllVertices(final Database db, final String[] nodeLabels) {
    return GraphEngine.getAllVertices(db, nodeLabels);
  }

  /** @see GraphEngine#buildRidIndex(List) */
  protected Map<RID, Integer> buildRidIndex(final List<Vertex> vertices) {
    return GraphEngine.buildRidIndex(vertices);
  }

  /** @see GraphEngine#neighborRid(Edge, RID, Vertex.DIRECTION) */
  protected RID neighborRid(final Edge edge, final RID sourceRid, final Vertex.DIRECTION dir) {
    return GraphEngine.neighborRid(edge, sourceRid, dir);
  }

  /** @see GraphEngine#parseDirection(String) */
  protected Vertex.DIRECTION parseDirection(final String dir) {
    return GraphEngine.parseDirection(dir);
  }

  /** @see GraphEngine#buildAdjacencyList(List, Map, Vertex.DIRECTION, String[]) */
  protected int[][] buildAdjacencyList(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx,
      final Vertex.DIRECTION dir, final String[] relTypes) {
    return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes);
  }

  /**
   * Builds a path representation from a list of RIDs.
   */
  protected Map<String, Object> buildPath(final List<RID> rids, final Database database) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final RID rid : rids) {
      final Document doc = database.lookupByRID(rid, true).asDocument();
      if (doc instanceof Vertex)
        nodes.add(doc);
      else if (doc instanceof Edge)
        relationships.add(doc);
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }
}
