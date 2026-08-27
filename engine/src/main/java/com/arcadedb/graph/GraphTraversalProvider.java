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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.function.IntConsumer;

/**
 * SPI for accelerated graph traversal providers (e.g., Graph Analytical Views backed by CSR).
 * <p>
 * The query planner discovers registered providers via {@link GraphTraversalProviderRegistry}
 * and uses them for neighbor expansion when:
 * <ul>
 *   <li>The provider is ready ({@link #isReady()})</li>
 *   <li>The provider covers the required edge types ({@link #coversEdgeType(String)})</li>
 *   <li>The query does not need the edge object itself (no edge variable captured)</li>
 * </ul>
 * <p>
 * Providers map ArcadeDB RIDs to dense integer IDs for O(1) neighbor lookup
 * via CSR (Compressed Sparse Row) arrays, bypassing the OLTP edge linked lists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface GraphTraversalProvider {

  /**
   * Returns the total number of nodes in this provider's CSR structure.
   * Dense node IDs range from 0 (inclusive) to getNodeCount() (exclusive).
   */
  int getNodeCount();

  /**
   * Returns true if this provider is ready to serve queries.
   * Providers that are still building should return false.
   */
  boolean isReady();

  /**
   * Returns the name of this provider.
   */
  String getName();

  /**
   * Returns true if this provider covers the given vertex type.
   * A null type name means "all types", which returns true only if the provider includes all vertex types.
   */
  boolean coversVertexType(String typeName);

  /**
   * Returns true if this provider covers the given edge type.
   * A null type name means "all types", which returns true only if the provider includes all edge types.
   */
  boolean coversEdgeType(String edgeTypeName);

  /**
   * Returns the dense node ID for a RID, or -1 if not mapped.
   */
  int getNodeId(RID rid);

  /**
   * Returns the RID for a dense node ID.
   */
  RID getRID(int nodeId);

  /**
   * Returns neighbor dense node IDs for a given node, direction, and edge types.
   * This is the primary acceleration method — O(1) array access vs O(n) linked list traversal.
   */
  int[] getNeighborIds(int nodeId, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Returns the edge count for a node, direction, and edge types.
   * Avoids materializing the neighbor array when only the count is needed.
   */
  long countEdges(int nodeId, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Checks if nodeA is connected to nodeB via the given direction and edge types.
   * Uses binary search on sorted CSR arrays — O(log(degree)).
   */
  boolean isConnectedTo(int nodeA, int nodeB, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Counts the edges joining nodeA to nodeB in the given direction and of the given edge types.
   * <p>
   * This is the multiplicity {@link #isConnectedTo} collapses to a boolean: a pattern relationship
   * matches once per edge, so a pair joined by parallel edges contributes one row per edge. Under
   * {@link Vertex.DIRECTION#BOTH} a self-loop counts once, matching how the OLTP expansion
   * de-duplicates the relationship it reaches from both adjacency lists.
   * <p>
   * <b>A negative result means the provider cannot answer exactly</b> and the caller must fall back
   * to the edge list. A count is a stronger claim than a boolean, and a provider that tracks its
   * pending changes at a coarser granularity than the individual edge can hold the boolean while
   * losing the count - saying "unknown" is then the only honest answer. The caller has the two
   * vertices in hand and can always walk the edges itself.
   * <p>
   * The default implementation counts occurrences in {@link #getNeighborIds} and never answers
   * unknown; a CSR-backed provider overrides it with an equal-range scan on the sorted adjacency
   * array.
   * <p>
   * <b>The default halves the count of a {@link Vertex.DIRECTION#BOTH} self-loop</b>, because
   * {@link #getNeighborIds} is specified to return the raw adjacency entries and a self-loop
   * contributes one to each of the two lists - which is why the callers of that method de-duplicate
   * it themselves (see {@code SelfLoops.deduplicate}). A provider whose {@code getNeighborIds}
   * already de-duplicates would halve a count that was never doubled, and must override this method.
   */
  default long countEdgesBetween(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    long count = 0;
    for (final int neighbor : getNeighborIds(nodeA, direction, edgeTypes))
      if (neighbor == nodeB)
        ++count;
    // A self-loop contributes one entry to each adjacency list, so a BOTH walk sees every one of them twice
    if (direction == Vertex.DIRECTION.BOTH && nodeA == nodeB)
      count /= 2;
    return count;
  }

  /**
   * Returns a property value from columnar storage, or null if not materialized.
   */
  Object getProperty(int nodeId, String propertyName);

  /**
   * Estimates the mean number of parallel edges joining a connected pair of vertices for an edge type,
   * i.e. the same statistic {@link com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider
   * #getMeanEdgesPerConnectedPair} samples from the OLTP edge list. A provider backed by a structure that
   * can answer this exactly (e.g. a CSR adjacency index) lets the query planner skip that sample.
   * <p>
   * <b>A negative result means the provider cannot answer exactly</b> and the caller must fall back to
   * sampling - the same "negative means unknown" convention {@link #countEdgesBetween} uses, and for the
   * same reason: a provider whose backing structure does not currently reflect every committed edge (or
   * has none for this type) can only honestly say "unknown", never a value it cannot stand behind.
   * <p>
   * The default always answers unknown; a CSR-backed provider overrides it with an exact computation.
   *
   * @param edgeType the edge type name
   * @return the exact mean edges per connected pair, or a negative value if this provider cannot answer
   */
  default double getMeanEdgesPerConnectedPair(final String edgeType) {
    return -1.0;
  }

  /**
   * Returns the edge types this provider actually holds, or {@code null} when it cannot enumerate them.
   * <p>
   * {@link #edgeWeightsForSlice} is addressed per edge type, so a caller that was given no type filter but
   * needs edge properties has to ask which types exist rather than pass "all types" down. A provider that answers
   * {@code null} simply cannot serve such a caller, which then falls back to reading the edge records.
   */
  default String[] getMaterializedEdgeTypes() {
    return null;
  }

  /**
   * Returns true if this provider has edge property values it can serve at all, through
   * {@link #edgeWeightsOf}.
   * <p>
   * A provider that cannot pair every edge it reports in {@link #getNeighborIds} with that edge's own property
   * value - because its columns have gone out of step with its adjacency, say - must answer {@code false} rather
   * than hand back a value belonging to another edge. The caller holds the edge records and can always read the
   * property itself; a plausible wrong number is the one outcome it cannot recover from. Same "say unknown rather
   * than guess" convention as {@link #countEdgesBetween} and {@link #getMeanEdgesPerConnectedPair}.
   */
  default boolean hasEdgeProperties() {
    return false;
  }

  /**
   * Returns true if this provider can serve {@code propertyName} for {@code edgeType} through
   * {@link #edgeWeightsOf}.
   * <p>
   * {@link #hasEdgeProperties()} answers the coarser question - are there edge property columns at all - and a
   * caller that asks only that one gets the default weight back for every edge when the view happens to
   * materialise a <em>different</em> property than the one requested. Since that default is also the ordinary
   * way of saying "this edge has no value for the property", the caller cannot tell the two apart and silently
   * treats the whole graph as unweighted. That is the same failure as reading a weight that belongs to
   * another edge, arrived at from the other direction, so the question a weighted algorithm has to ask is this
   * one: can you serve <em>this</em> property?
   *
   * @param edgeType     the edge type name
   * @param propertyName the property the caller intends to read
   */
  default boolean hasEdgeProperty(final String edgeType, final String propertyName) {
    return false;
  }

  /**
   * Returns true if this provider can serve {@code propertyName} for every one of {@code edgeTypes} - or, when
   * none are given, for every type it materialises.
   * <p>
   * The question a weighted algorithm actually has to ask, and the reason it is answered here rather than
   * open-coded per caller: "all types" has to be resolved against what the provider holds before the per-type
   * check means anything, and a caller that resolves it differently gets a different answer to the same
   * question. Answers {@code false} for a provider that cannot enumerate its types, since it then cannot
   * promise anything about the ones it was not told about.
   *
   * @param propertyName the property the caller intends to read
   * @param edgeTypes    the types in play, empty or null for every materialised one
   */
  default boolean servesEdgeProperty(final String propertyName, final String... edgeTypes) {
    final String[] types = edgeTypes != null && edgeTypes.length > 0 ? edgeTypes : getMaterializedEdgeTypes();
    if (types == null || types.length == 0)
      return false;
    for (final String type : types)
      if (!hasEdgeProperty(type, propertyName))
        return false;
    return true;
  }

  /**
   * Returns one node's neighbours of a single edge type and direction, together with the weight of the edge
   * reaching each of them - or {@code null} when this provider cannot answer exactly.
   * <p>
   * <b>This is the provider's only edge-property primitive, and it deliberately hands back no index.</b> It used
   * to be {@code getEdgeProperty(nodeId, neighborIndex, direction, edgeType, propertyName)}, addressing an edge
   * by its position in the node's neighbour list, which meant something only while that position identified the
   * same edge {@link #getNeighborIds} reported there. It stopped identifying it as soon as a provider served
   * pending changes from a side structure - a deleted edge dropped from the neighbour list shifts every entry
   * after it, so the n-th neighbour is no longer the n-th column slot - and the method had no way to notice:
   * it returned a weight belonging to a different edge, which is a wrong shortest path, a wrong MST, a wrong
   * Steiner tree, and never an exception (issues #6301 and #6315). The rule that kept it correct lived in its
   * callers, where the next one written against this SPI could not discover it.
   * <p>
   * Returning the two arrays together removes the rule instead of restating it: the pairing is made where the
   * provider's own adjacency is walked, by whoever knows how that adjacency is put together, and nothing
   * downstream can re-derive it wrongly. Answering "I cannot" costs the caller a read of the edge records,
   * which are exact; answering with a plausible wrong number costs it the result.
   *
   * {@code edgeCheckpoint}, when not {@code null}, must be called once per edge as the slice is built, with a
   * counter that restarts at zero for every call. That is what keeps a supernode from being one unabortable
   * unit of work: this method returns a fully-built row, so a caller walking every node of the graph has no
   * other place to notice that it should stop (issue #6715). Restarting the counter rather than threading a
   * running total through is the same choice {@link com.arcadedb.query.sql.executor.WorkGuard#checkPeriodically}
   * documents for a loop whose counter restarts - it bounds the worst case to about one checkpoint stride into
   * the largest slice, whatever the slice before it looked like.
   *
   * @param nodeId         the node whose edges to read
   * @param direction      {@link Vertex.DIRECTION#OUT} or {@link Vertex.DIRECTION#IN}, never
   *                       {@link Vertex.DIRECTION#BOTH} - a direction has an adjacency slice, {@code BOTH} does
   *                       not, and {@link #edgeWeightsOf} splits it before calling here
   * @param edgeType       a single materialised edge type, never "all types"
   * @param propertyName   the edge property to read
   * @param defaultWeight  value for an edge carrying no numeric value for that property
   * @param edgeCheckpoint called with the edge index within this slice (0-based), or {@code null} to skip it
   *
   * @return the neighbours - the same ones, in the same order, {@code getNeighborIds(nodeId, direction,
   * edgeType)} reports - each beside its own edge's weight, or {@code null} if this provider cannot serve the
   * property for this type
   */
  default NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
      final String edgeType, final String propertyName, final double defaultWeight,
      final IntConsumer edgeCheckpoint) {
    return null;
  }

  /**
   * Returns one node's neighbours together with the edge-property value of each edge reaching them, or
   * {@code null} when this provider cannot serve {@code propertyName} for every type in play - or cannot
   * answer exactly for this particular node, which a caller reaching this method across a long walk has to
   * handle too: a provider that absorbs committed changes as it goes can lose the ability to answer partway
   * through one.
   * <p>
   * <b>This is the only way in, and the reason the composition lives here rather than in each caller.</b>
   * {@link #edgeWeightsForSlice} answers for one (type, direction) pair, and two things break a caller that
   * tries to assemble the whole neighbourhood itself:
   * <ul>
   *   <li>a multi-type neighbour list is <em>merged and sorted</em> across types, so entry {@code j} of it is
   *       not entry {@code j} of any one type's slice - the weight lands on another edge, or on none;</li>
   *   <li>{@link Vertex.DIRECTION#BOTH} has no adjacency slice of its own at all. A provider resolves
   *       {@code OUT} and {@code IN}, so a {@code BOTH} lookup would answer for neither and the caller would
   *       silently read the whole neighbourhood at its default weight.</li>
   * </ul>
   * Both are handled here once: the walk takes one slice per (type, direction) pair, each of which arrives with
   * its weights already paired, and concatenates them.
   *
   * @param nodeId        the node whose edges to read
   * @param direction     traversal direction; {@code BOTH} walks the outgoing and incoming slices in turn
   * @param propertyName  the edge property to read
   * @param defaultWeight value for an edge carrying no numeric value for that property
   * @param edgeTypes     the types in play, empty or null for every materialised one
   */
  default NodeEdgeWeights edgeWeightsOf(final int nodeId, final Vertex.DIRECTION direction,
      final String propertyName, final double defaultWeight, final String... edgeTypes) {
    return edgeWeightsOf(nodeId, direction, propertyName, defaultWeight, null, edgeTypes);
  }

  /**
   * Same as {@link #edgeWeightsOf(int, Vertex.DIRECTION, String, double, String...)}, with one addition:
   * {@code edgeCheckpoint}, if not {@code null}, is handed to {@link #edgeWeightsForSlice} for each
   * (type, direction) slice, which calls it once per edge as that slice is built, with a counter that restarts
   * at zero for every slice.
   * <p>
   * A caller that visits every node of the graph in turn - {@code AbstractAlgoProcedure}'s columnar adjacency
   * build does, once per node - has no other way to stay abortable mid-node: this method already returns one
   * fully-built {@link NodeEdgeWeights} per call, so a single node with a very large degree (a supernode) is
   * otherwise one unabortable unit of work regardless of how often the caller checkpoints between nodes (issue
   * #6715). Restarting the counter at zero rather than threading a running total through is deliberate, the same
   * choice {@link com.arcadedb.query.sql.executor.WorkGuard#checkPeriodically} documents for a loop whose counter
   * restarts: it bounds the worst-case latency to about one checkpoint stride into the largest node, whatever the
   * node before it looked like, and it keeps this method free of any dependency on where the caller's own count
   * comes from.
   * <p>
   * A caller that does not visit many nodes in a row - a single-source search that reads one node's edges per
   * step, already bounded by its own outer loop - passes {@code null} and pays nothing beyond one comparison per
   * edge.
   * <p>
   * {@code edgeTypes} is a plain array here rather than the other overload's varargs, deliberately: a fifth
   * argument of a bare {@code null} would otherwise be ambiguous between binding to {@code edgeCheckpoint} here
   * and to the other overload's {@code String... edgeTypes} (both are reference types, and neither overload is
   * more specific than the other from a {@code null} literal), breaking any existing five-argument call this
   * SPI already has. Requiring six arguments to reach this overload at all keeps every four- and five-argument
   * call - including a bare {@code null} - resolving to the other one, unchanged.
   *
   * @param edgeCheckpoint called with the edge index within this call (0-based), or {@code null} to skip it
   */
  default NodeEdgeWeights edgeWeightsOf(final int nodeId, final Vertex.DIRECTION direction,
      final String propertyName, final double defaultWeight, final IntConsumer edgeCheckpoint,
      final String[] edgeTypes) {
    if (!servesEdgeProperty(propertyName, edgeTypes))
      return null;

    final String[] types = edgeTypes != null && edgeTypes.length > 0 ? edgeTypes : getMaterializedEdgeTypes();
    final Vertex.DIRECTION[] directions = direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };

    final NodeEdgeWeights[] slices = new NodeEdgeWeights[types.length * directions.length];
    int degree = 0;
    int s = 0;
    for (final String type : types)
      for (final Vertex.DIRECTION d : directions) {
        final NodeEdgeWeights slice = edgeWeightsForSlice(nodeId, d, type, propertyName, defaultWeight, edgeCheckpoint);
        // One slice this provider cannot answer makes the whole node unanswerable: a partial row would be a
        // neighbourhood with edges missing from it, which reads as a graph that is simply shaped differently -
        // the one failure the caller cannot detect. Same convention as this method's own contract.
        if (slice == null)
          return null;
        slices[s++] = slice;
        degree += slice.neighbors().length;
      }

    // A single slice is already exactly what this method returns, and it was built for this call alone - and
    // its edges were checkpointed as it was built - so it can be handed straight back.
    if (slices.length == 1)
      return slices[0];

    final int[] neighbors = new int[degree];
    final double[] weights = new double[degree];
    int pos = 0;
    for (final NodeEdgeWeights slice : slices) {
      final int[] sliceNeighbors = slice.neighbors();
      System.arraycopy(sliceNeighbors, 0, neighbors, pos, sliceNeighbors.length);
      System.arraycopy(slice.weights(), 0, weights, pos, sliceNeighbors.length);
      pos += sliceNeighbors.length;
    }
    return new NodeEdgeWeights(neighbors, weights);
  }

  /**
   * Returns true if this provider's data is stale (not reflecting latest committed changes).
   * A provider may still be ready ({@link #isReady()}) while stale, if configured to serve stale data.
   */
  default boolean isStale() {
    return false;
  }

  /**
   * Returns a packed {@link NeighborView} for zero-allocation iteration over all nodes' neighbors,
   * or {@code null} if this provider does not support the optimization (e.g., when overlays are active).
   * <p>
   * When available, algorithms should prefer this over per-node {@link #getNeighborIds} calls
   * to avoid O(N) array allocations.
   */
  default NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    return null;
  }

  /**
   * Bulk degree computation: fills {@code degrees[nodeId]} with the edge count for each node.
   * <p>
   * Default implementation calls {@link #countEdges} per node. CSR-backed providers override
   * this to compute degrees directly from offset arrays in a single pass, avoiding per-node
   * HashMap lookups and method dispatch overhead.
   * <p>
   * For star-join queries (Q4/Q7), this reduces 5M × 150ns/call = 750ms to a single
   * array scan at ~5M × 2ns = 10ms per edge type.
   */
  default void getDegrees(final int[] degrees, final Vertex.DIRECTION direction, final String edgeType) {
    for (int v = 0; v < degrees.length; v++)
      degrees[v] = (int) countEdges(v, direction, edgeType);
  }
}
