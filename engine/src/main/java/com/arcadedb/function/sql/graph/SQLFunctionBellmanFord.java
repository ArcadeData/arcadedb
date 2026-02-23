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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.math.SQLFunctionMathAbstract;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bellman-Ford algorithm for finding the shortest path between two nodes, supporting graphs
 * with negative edge weights. Unlike Dijkstra, this algorithm handles negative weights
 * and can detect negative-weight cycles.
 * <p>
 * Syntax: {@code bellmanFord(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName> [, <direction>])}
 * <p>
 * Returns a list of RIDs representing the path from source to destination, or an empty list
 * if no path exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionBellmanFord extends SQLFunctionMathAbstract {
  public static final String NAME = "bellmanFord";

  public SQLFunctionBellmanFord() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    if (params.length < 3)
      throw new IllegalArgumentException("bellmanFord() requires at least 3 parameters: source, destination, weightProperty");

    final Vertex sourceVertex = toVertex(params[0], context);
    final Vertex destVertex = toVertex(params[1], context);
    final String weightProperty = FileUtils.getStringContent(params[2]);
    final String direction = params.length > 3 ? params[3].toString().toUpperCase() : "BOTH";

    if (sourceVertex == null || destVertex == null)
      return new LinkedList<>();

    final Database db = context.getDatabase();
    final List<Vertex> vertices = getAllVertices(db);

    if (vertices.isEmpty())
      return new LinkedList<>();

    final int n = vertices.size();
    final Map<Vertex, Integer> vertexIndex = new HashMap<>(n);
    for (int i = 0; i < n; i++)
      vertexIndex.put(vertices.get(i), i);

    final Integer startIdx = vertexIndex.get(sourceVertex);
    final Integer endIdx = vertexIndex.get(destVertex);
    if (startIdx == null || endIdx == null)
      return new LinkedList<>();

    final double[] dist = new double[n];
    final int[] prev = new int[n];
    Arrays.fill(dist, Double.MAX_VALUE);
    Arrays.fill(prev, -1);
    dist[startIdx] = 0.0;

    // Collect all edges based on direction
    final List<int[]> edgeList = new ArrayList<>();
    final List<Double> edgeWeights = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      final Vertex.DIRECTION dir = parseDirection(direction);
      for (final Edge edge : v.getEdges(dir)) {
        final Vertex neighbor;
        if (dir == Vertex.DIRECTION.OUT)
          neighbor = edge.getInVertex();
        else if (dir == Vertex.DIRECTION.IN)
          neighbor = edge.getOutVertex();
        else
          neighbor = edge.getOut().equals(v.getIdentity()) ? edge.getInVertex() : edge.getOutVertex();

        final Integer j = vertexIndex.get(neighbor);
        if (j == null)
          continue;

        double w = 1.0;
        if (weightProperty != null && !weightProperty.isEmpty()) {
          final Object wObj = edge.get(weightProperty);
          if (wObj instanceof Number num)
            w = num.doubleValue();
        }
        edgeList.add(new int[] { i, j });
        edgeWeights.add(w);
      }
    }

    // Bellman-Ford: V-1 relaxation iterations
    for (int iter = 0; iter < n - 1; iter++) {
      boolean relaxed = false;
      for (int e = 0; e < edgeList.size(); e++) {
        final int u = edgeList.get(e)[0];
        final int v = edgeList.get(e)[1];
        final double w = edgeWeights.get(e);
        if (dist[u] != Double.MAX_VALUE && dist[u] + w < dist[v]) {
          dist[v] = dist[u] + w;
          prev[v] = u;
          relaxed = true;
        }
      }
      if (!relaxed)
        break;
    }

    if (dist[endIdx] == Double.MAX_VALUE)
      return new LinkedList<>();

    // Reconstruct path with cycle guard (negative cycles may corrupt prev[])
    final LinkedList<Vertex> path = new LinkedList<>();
    int current = endIdx;
    final Set<Integer> visited = new HashSet<>();
    while (current != -1 && current != startIdx) {
      if (!visited.add(current))
        return new LinkedList<>(); // negative cycle detected in path
      path.addFirst(vertices.get(current));
      current = prev[current];
    }
    path.addFirst(vertices.get(startIdx));

    // Return as list of RIDs (consistent with other path functions)
    final LinkedList<Object> result = new LinkedList<>();
    for (final Vertex v : path)
      result.add(v.getIdentity());
    return result;
  }

  private Vertex toVertex(final Object arg, final CommandContext context) {
    if (arg instanceof Vertex v)
      return v;
    if (arg instanceof Identifiable id) {
      final Record rec = id.getRecord();
      if (rec instanceof Vertex v)
        return v;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private List<Vertex> getAllVertices(final Database db) {
    final MultiIterator<Vertex> multiIter = new MultiIterator<>();
    for (final DocumentType type : db.getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      multiIter.addIterator((Iterator<Vertex>) (Iterator<?>) db.iterateType(type.getName(), false));
    }
    final List<Vertex> vertices = new ArrayList<>();
    while (multiIter.hasNext())
      vertices.add(multiIter.next());
    return vertices;
  }

  private Vertex.DIRECTION parseDirection(final String direction) {
    return switch (direction) {
      case "OUT" -> Vertex.DIRECTION.OUT;
      case "IN" -> Vertex.DIRECTION.IN;
      default -> Vertex.DIRECTION.BOTH;
    };
  }

  @Override
  public String getSyntax() {
    return "bellmanFord(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName> [, <direction>])";
  }
}
