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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Fused multi-hop MATCH step that uses CSR arrays to traverse a chain of consecutive
 * out/in hops in a single DFS pass, without materializing intermediate Result objects.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * <p>
 * For example, in a MATCH pattern like:
 * <pre>
 *   {as: a} -KNOWS-> {as: b} -FOLLOWS-> {as: c} -LIKES-> {as: d}
 * </pre>
 * Instead of creating 3 separate MatchStep/MatchEdgeTraverser instances (each materializing
 * a full ResultInternal per hop), this step traverses the entire chain using int[] nodeId
 * arrays and only materializes vertex results at each alias binding point.
 */
class MatchGAVFusedStep extends AbstractExecutionStep {
  private final List<EdgeTraversal>    edges;
  private final GraphTraversalProvider provider;
  private       ResultSet              upstream;
  private       Result                 lastUpstreamRecord;
  private       List<ResultInternal>   bufferedResults;
  private       int                    bufferIndex;

  MatchGAVFusedStep(final CommandContext context, final List<EdgeTraversal> edges, final GraphTraversalProvider provider) {
    super(context);
    this.edges = edges;
    this.provider = provider;
  }

  @Override
  public void reset() {
    this.upstream = null;
    this.lastUpstreamRecord = null;
    this.bufferedResults = null;
    this.bufferIndex = 0;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords)
          return false;
        return fetchNextIfNeeded();
      }

      @Override
      public Result next() {
        if (localCount >= nRecords)
          throw new NoSuchElementException();
        if (!fetchNextIfNeeded())
          throw new NoSuchElementException();
        localCount++;
        return bufferedResults.get(bufferIndex++);
      }
    };
  }

  private boolean fetchNextIfNeeded() {
    // If we have buffered results remaining, return them
    if (bufferedResults != null && bufferIndex < bufferedResults.size())
      return true;

    // Fetch next upstream record and compute all fused results
    while (true) {
      if (upstream == null || !upstream.hasNext())
        upstream = getPrev().syncPull(context, 100);
      if (!upstream.hasNext())
        return false;

      lastUpstreamRecord = upstream.next();
      bufferedResults = computeFusedResults(lastUpstreamRecord);
      bufferIndex = 0;

      if (!bufferedResults.isEmpty())
        return true;
    }
  }

  /**
   * Executes the fused chain traversal for a single upstream record using CSR DFS.
   */
  private List<ResultInternal> computeFusedResults(final Result upstream) {
    final List<ResultInternal> results = new ArrayList<>();

    // Get the starting point from the first edge's source alias
    final String startAlias = edges.get(0).out ? edges.get(0).edge.out.alias : edges.get(0).edge.in.alias;
    Identifiable startingElem = upstream.getElementProperty(startAlias);
    if (startingElem instanceof Result r)
      startingElem = r.getElement().orElse(null);
    if (startingElem == null)
      return results;

    final int startNodeId = provider.getNodeId(startingElem.getIdentity());
    if (startNodeId < 0)
      return results; // Not in CSR, caller should fall back to standard path

    // Build direction and edge label arrays for the chain
    final int chainLen = edges.size();
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[chainLen];
    final String[][] edgeLabelsPerHop = new String[chainLen][];
    final String[] aliases = new String[chainLen]; // alias bound at each hop's endpoint

    for (int i = 0; i < chainLen; i++) {
      final EdgeTraversal et = edges.get(i);
      directions[i] = et.out ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.IN;
      // Extract edge labels from the method call parameters
      final var methodParams = et.edge.item.getMethod().params;
      if (methodParams != null && !methodParams.isEmpty()) {
        final String[] labels = new String[methodParams.size()];
        for (int p = 0; p < methodParams.size(); p++)
          labels[p] = methodParams.get(p).toString().replace("'", "").replace("\"", "").trim();
        edgeLabelsPerHop[i] = labels;
      } else {
        edgeLabelsPerHop[i] = new String[0];
      }
      // The alias for this hop's endpoint
      aliases[i] = et.out ? et.edge.in.alias : et.edge.out.alias;
    }

    // DFS through CSR, collecting path nodeIds
    final int[] pathNodeIds = new int[chainLen + 1];
    pathNodeIds[0] = startNodeId;
    dfs(pathNodeIds, 0, chainLen, directions, edgeLabelsPerHop, aliases, upstream, results);

    return results;
  }

  /**
   * Recursive DFS through CSR arrays. Materializes a ResultInternal only at the final hop,
   * carrying all intermediate alias bindings.
   */
  private void dfs(final int[] pathNodeIds, final int hop, final int chainLen,
      final Vertex.DIRECTION[] directions, final String[][] edgeLabelsPerHop,
      final String[] aliases, final Result upstream, final List<ResultInternal> results) {
    final int currentNodeId = pathNodeIds[hop];
    final int[] neighbors = edgeLabelsPerHop[hop].length == 0
        ? provider.getNeighborIds(currentNodeId, directions[hop])
        : provider.getNeighborIds(currentNodeId, directions[hop], edgeLabelsPerHop[hop]);

    for (final int neighborId : neighbors) {
      pathNodeIds[hop + 1] = neighborId;

      if (hop == chainLen - 1) {
        // Final hop: materialize the full result with all alias bindings
        final ResultInternal result = buildResult(pathNodeIds, aliases, upstream);
        if (result != null)
          results.add(result);
      } else {
        // Intermediate hop: continue DFS with raw int IDs
        dfs(pathNodeIds, hop + 1, chainLen, directions, edgeLabelsPerHop, aliases, upstream, results);
      }
    }
  }

  /**
   * Builds a ResultInternal from the CSR path, binding each alias to its vertex.
   */
  private ResultInternal buildResult(final int[] pathNodeIds, final String[] aliases, final Result upstream) {
    final ResultInternal result = new ResultInternal(context.getDatabase());

    // Copy all properties from upstream
    for (final String prop : upstream.getPropertyNames())
      result.setProperty(prop, upstream.getProperty(prop));

    // Bind each hop's endpoint alias
    for (int i = 0; i < aliases.length; i++) {
      final RID rid = provider.getRID(pathNodeIds[i + 1]);
      if (rid == null)
        return null;
      try {
        final Document doc = (Document) rid.getRecord();
        // Check if this alias was already bound — if so, verify it matches
        final Object prevValue = upstream.getProperty(aliases[i]);
        if (prevValue != null) {
          Identifiable prevElem = null;
          if (prevValue instanceof Identifiable id)
            prevElem = id;
          else if (prevValue instanceof Result r)
            prevElem = r.getElement().orElse(null);
          if (prevElem != null && !prevElem.getIdentity().equals(rid))
            return null; // Doesn't match bound value
        }
        result.setProperty(aliases[i], new ResultInternal(doc));
      } catch (final RecordNotFoundException e) {
        return null; // Deleted since CSR build
      }
    }

    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder sb = new StringBuilder();
    sb.append(spaces).append("+ MATCH GAV FUSED (").append(edges.size()).append(" hops)\n");
    for (final EdgeTraversal edge : edges) {
      sb.append(spaces).append("  ");
      sb.append("{").append(edge.edge.out.alias).append("}");
      sb.append(edge.edge.item.getMethod());
      sb.append("{").append(edge.edge.in.alias).append("}\n");
    }
    return sb.toString();
  }
}
