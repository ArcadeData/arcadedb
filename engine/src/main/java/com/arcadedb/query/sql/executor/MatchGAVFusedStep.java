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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
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
  // The view as the plan holds it, and as this execution reads it: pinned on first use, for the whole statement (issue #10415)
  private final GraphTraversalProvider liveProvider;
  // Per hop, the type and the bucket the pattern gives the node the hop reaches, null where it gives none (issue #10435)
  private final String[]               hopTypes;
  private final String[]               hopBuckets;
  private       GraphTraversalProvider provider;
  // Per hop, the sorted ids of the buckets the reached node may live in, null where the hop is unconstrained. Resolved with the
  // pin, so a type or a bucket created after the plan was cached is the one the statement reads
  private       int[][]                hopBucketIds;
  private       ResultSet              upstream;
  private       Result                 lastUpstreamRecord;
  private       List<ResultInternal>   bufferedResults;
  private       int                    bufferIndex;

  MatchGAVFusedStep(final CommandContext context, final List<EdgeTraversal> edges, final GraphTraversalProvider provider,
      final String[] hopTypes, final String[] hopBuckets) {
    super(context);
    this.edges = edges;
    this.liveProvider = provider;
    this.hopTypes = hopTypes;
    this.hopBuckets = hopBuckets;
  }

  @Override
  public void reset() {
    this.upstream = null;
    this.lastUpstreamRecord = null;
    this.bufferedResults = null;
    this.bufferIndex = 0;
    this.provider = null;
    this.hopBucketIds = null;
  }

  /**
   * The ids of the buckets each hop's node must live in: those of the type the pattern names, sub-types included as the regular
   * step's {@code isSubTypeOf} takes them, narrowed to the bucket it names. A type or a bucket that does not exist matches nothing,
   * as it does there.
   */
  private int[][] resolveHopBuckets() {
    final Schema schema = context.getDatabase().getSchema();
    final int[][] resolved = new int[edges.size()][];
    for (int hop = 0; hop < resolved.length; hop++) {
      final String type = hopTypes[hop];
      final String bucket = hopBuckets[hop];
      if (type == null && bucket == null)
        continue;

      int[] accepted = null;
      if (type != null) {
        final List<Integer> ids = schema.existsType(type) ? schema.getType(type).getBucketIds(true) : List.of();
        accepted = new int[ids.size()];
        for (int i = 0; i < accepted.length; i++)
          accepted[i] = ids.get(i);
        Arrays.sort(accepted);
      }
      if (bucket != null) {
        final Bucket named = schema.getBucketByNameIfExists(bucket);
        final int bucketId = named == null ? -1 : named.getFileId();
        accepted = named != null && (accepted == null || Arrays.binarySearch(accepted, bucketId) >= 0) ? new int[] { bucketId } : new int[0];
      }
      resolved[hop] = accepted;
    }
    return resolved;
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
    if (provider == null)
      provider = GraphViewPins.pin(context, liveProvider);
    if (hopBucketIds == null)
      hopBucketIds = resolveHopBuckets();

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
      // et.out is the schedule's forward/reverse flag, not the pattern's actual out()/in()/both() method - a
      // reverse-scheduled in()/both() hop needs the same out<->in flip MatchReverseEdgeTraverser applies to its
      // single-hop expand-into fast path (#6670). isFusibleEdge() already guarantees a plain out/in/both method,
      // so this can never fall back to null here.
      directions[i] = MatchExecutionPlanner.directionFor(et.edge.item, et.out);
      edgeLabelsPerHop[i] = MatchExecutionPlanner.extractEdgeLabels(et);
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

    final int[] acceptedBuckets = hopBucketIds[hop];
    for (final int neighborId : neighbors) {
      // The type and the bucket the pattern gives this node, which the walk would otherwise bind without asking (issue #10435)
      // (the bucket id alone: no RID is built in the loop over every neighbour, and -1 for a node the view does not map is in no bucket)
      if (acceptedBuckets != null && Arrays.binarySearch(acceptedBuckets, provider.getBucketId(neighborId)) < 0)
        continue;
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
