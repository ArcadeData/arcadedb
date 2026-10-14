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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.biconnectedComponents(relTypes?)
 * <p>
 * Finds all biconnected components (2-edge-connected subgraphs) in the graph using a
 * non-recursive DFS with an explicit edge stack. A biconnected component is a maximal
 * subgraph that has no articulation point (i.e. it cannot be disconnected by removing a
 * single vertex).
 * </p>
 * <p>
 * Each node is yielded once per biconnected component it belongs to; nodes belonging to more
 * than one component (articulation points) appear multiple times with different componentIds.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.biconnectedComponents()
 * YIELD node, componentId
 * RETURN node.name, componentId ORDER BY componentId
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBiconnectedComponents extends AbstractAlgoProcedure {
  public static final String NAME = "algo.biconnectedComponents";

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
    return "Finds all biconnected components using DFS with edge stack";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "componentId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);

    final Database db = context.getDatabase();

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    final UndirectedLowPointDfs dfs = new UndirectedLowPointDfs(adj);

    // EVERY (node, componentId) MEMBERSHIP IN THE ORDER IT IS FOUND: COMPONENT IDS GROW, SO EACH NODE'S IDS ARE ASCENDING
    final IntPairStack memberships = new IntPairStack();

    // THE EDGES OF THE COMPONENTS STILL OPEN, AS TWO PARALLEL STACKS OF ENDPOINTS. EACH EDGE IS PUSHED ONCE: A TREE EDGE
    // WHEN IT DISCOVERS ITS CHILD, A BACK EDGE FROM ITS DESCENDANT'S SIDE
    final IntPairStack edgeStack = new IntPairStack();
    // THE LAST COMPONENT EACH NODE WAS ADDED TO, SO A NODE MET ON SEVERAL EDGES OF ONE COMPONENT IS ADDED ONCE
    final int[] lastComponent = new int[n];
    Arrays.fill(lastComponent, -1);

    dfs.walk(new UndirectedLowPointDfs.Visitor() {
      private int nextComponentId = 0;

      @Override
      public void treeEdge(final int parent, final int child) {
        edgeStack.push(parent, child);
      }

      @Override
      public void backEdge(final int descendant, final int ancestor) {
        edgeStack.push(descendant, ancestor);
      }

      @Override
      public void childFinished(final int parent, final int child) {
        // THE CHILD'S SUBTREE CANNOT CLIMB ABOVE THE PARENT: THE EDGES DOWN TO THE TREE EDGE (parent, child) ARE ONE COMPONENT
        if (dfs.low[child] >= dfs.disc[parent])
          popComponent(parent, child);
      }

      @Override
      public void rootFinished(final int root) {
        // EVERY CHILD OF A ROOT CLOSES ITS OWN COMPONENT ABOVE, SO THIS ONLY GUARDS AGAINST EDGES LEFT BEHIND
        if (!edgeStack.isEmpty())
          popComponent(-1, -1);
      }

      private void popComponent(final int parent, final int child) {
        final int cid = nextComponentId++;
        while (!edgeStack.isEmpty()) {
          final int u = edgeStack.topU();
          final int v = edgeStack.topV();
          edgeStack.pop();
          addToComponent(u, cid);
          addToComponent(v, cid);
          if (u == parent && v == child)
            break;
        }
      }

      private void addToComponent(final int node, final int cid) {
        if (lastComponent[node] != cid) {
          lastComponent[node] = cid;
          memberships.push(node, cid);
        }
      }
    });

    // GROUPED BY NODE WITH A STABLE COUNTING SORT, WHICH KEEPS EACH NODE'S COMPONENT IDS IN THEIR ASCENDING FIND ORDER
    final int total = memberships.size;
    final int[] start = new int[n + 1];
    for (int m = 0; m < total; m++)
      start[memberships.us[m] + 1]++;
    for (int i = 0; i < n; i++)
      start[i + 1] += start[i];
    final int[] fill = Arrays.copyOf(start, n);
    final int[] componentIds = new int[total];
    for (int m = 0; m < total; m++)
      componentIds[fill[memberships.us[m]]++] = memberships.vs[m];

    final List<Result> results = new ArrayList<>(total);
    for (int i = 0; i < n; i++) {
      for (int m = start[i]; m < start[i + 1]; m++) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("node", graph.getRID(i));
        r.setProperty("componentId", componentIds[m]);
        results.add(r);
      }
    }
    return results.stream();
  }

  /** A stack of (u, v) int pairs over two growable primitive arrays: no array or boxed pair per entry. */
  private static final class IntPairStack {
    private int[] us   = new int[64];
    private int[] vs   = new int[64];
    private int   size = 0;

    void push(final int u, final int v) {
      if (size == us.length) {
        us = Arrays.copyOf(us, size << 1);
        vs = Arrays.copyOf(vs, size << 1);
      }
      us[size] = u;
      vs[size] = v;
      size++;
    }

    int topU() {
      return us[size - 1];
    }

    int topV() {
      return vs[size - 1];
    }

    void pop() {
      size--;
    }

    boolean isEmpty() {
      return size == 0;
    }
  }
}
