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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.labelpropagation([config])
 * <p>
 * Detects communities using the Label Propagation Algorithm (LPA). Each node adopts the
 * most common label among its neighbors in each iteration. Very fast and scalable; useful
 * for real-time community detection. Results may be non-deterministic due to tie-breaking.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>maxIterations (int, default 10): maximum number of propagation iterations</li>
 *   <li>direction (string, default "BOTH"): edge direction to follow (IN, OUT, BOTH)</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.labelpropagation({maxIterations: 10})
 * YIELD node, communityId
 * RETURN communityId, count(*) AS size ORDER BY size DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLabelPropagation extends AbstractAlgoProcedure {
  public static final String NAME = "algo.labelpropagation";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Detects communities using the Label Propagation Algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "communityId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        n.intValue() : 10;
    final String directionStr = config != null && config.get("direction") instanceof String s ? s : "BOTH";
    final Vertex.DIRECTION direction = parseDirection(directionStr);

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> vertIter = getAllVertices(db, null);
    while (vertIter.hasNext())
      vertices.add(vertIter.next());
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<Vertex, Integer> vertexIndex = new HashMap<>(n);
    for (int i = 0; i < n; i++)
      vertexIndex.put(vertices.get(i), i);

    // Initialize: each node gets a unique label (its index)
    final int[] label = new int[n];
    for (int i = 0; i < n; i++)
      label[i] = i;

    // Create a mutable list of indices for shuffling
    final List<Integer> order = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      order.add(i);

    for (int iter = 0; iter < maxIterations; iter++) {
      Collections.shuffle(order);
      boolean changed = false;

      for (final int i : order) {
        final Vertex v = vertices.get(i);

        // Count labels of neighbors
        final Map<Integer, Integer> labelCount = new HashMap<>();
        for (final Edge edge : v.getEdges(direction)) {
          final Vertex neighbor;
          if (direction == Vertex.DIRECTION.OUT)
            neighbor = edge.getInVertex();
          else if (direction == Vertex.DIRECTION.IN)
            neighbor = edge.getOutVertex();
          else
            neighbor = edge.getOut().equals(v.getIdentity()) ? edge.getInVertex() : edge.getOutVertex();

          final Integer neighborIdx = vertexIndex.get(neighbor);
          if (neighborIdx == null)
            continue;
          labelCount.merge(label[neighborIdx], 1, Integer::sum);
        }

        if (labelCount.isEmpty())
          continue;

        // Find most frequent label (ties broken by smallest label)
        int bestLabel = label[i];
        int bestCount = 0;
        for (final Map.Entry<Integer, Integer> entry : labelCount.entrySet()) {
          if (entry.getValue() > bestCount || (entry.getValue() == bestCount && entry.getKey() < bestLabel)) {
            bestCount = entry.getValue();
            bestLabel = entry.getKey();
          }
        }

        if (bestLabel != label[i]) {
          label[i] = bestLabel;
          changed = true;
        }
      }

      if (!changed)
        break;
    }

    // Remap community IDs to be sequential
    final Map<Integer, Integer> communityRemap = new HashMap<>();
    int nextId = 0;
    for (int i = 0; i < n; i++) {
      if (!communityRemap.containsKey(label[i]))
        communityRemap.put(label[i], nextId++);
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i));
      result.setProperty("communityId", communityRemap.get(label[i]));
      results.add(result);
    }
    return results.stream();
  }

  private Vertex.DIRECTION parseDirection(final String direction) {
    return switch (direction.toUpperCase()) {
      case "OUT" -> Vertex.DIRECTION.OUT;
      case "IN" -> Vertex.DIRECTION.IN;
      default -> Vertex.DIRECTION.BOTH;
    };
  }
}
