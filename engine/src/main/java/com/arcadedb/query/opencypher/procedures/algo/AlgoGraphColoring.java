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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.graphColoring(relTypes?)
 * <p>
 * Assigns colors to vertices using a greedy algorithm so that no two adjacent vertices
 * share the same color. Returns the color assigned to each node (0-based integer) and
 * the total chromatic number (number of distinct colors used).
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.graphColoring()
 * YIELD node, color, chromaticNumber
 * RETURN node.name, color, chromaticNumber ORDER BY color
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoGraphColoring extends AbstractAlgoProcedure {
  public static final String NAME = "algo.graphColoring";

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
    return "Assign colors to vertices using greedy graph coloring";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "color", "chromaticNumber");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    final int[] color = new int[n];
    Arrays.fill(color, -1);
    // Reused forbidden array — no per-vertex allocation in the loop
    final boolean[] forbidden = new boolean[n];

    for (int v = 0; v < n; v++) {
      // Reset forbidden for this vertex
      Arrays.fill(forbidden, false);
      for (final int u : adj[v])
        if (color[u] != -1)
          forbidden[color[u]] = true;
      // Assign smallest non-forbidden color
      int c = 0;
      while (forbidden[c])
        c++;
      color[v] = c;
    }

    int chromaticNumber = 0;
    for (int i = 0; i < n; i++)
      if (color[i] + 1 > chromaticNumber)
        chromaticNumber = color[i] + 1;

    final int finalChromaticNumber = chromaticNumber;

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", vertices.get(i));
      r.setProperty("color", color[i]);
      r.setProperty("chromaticNumber", finalChromaticNumber);
      return (Result) r;
    });
  }
}
