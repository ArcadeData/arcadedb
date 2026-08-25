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
   * {@link #getEdgeProperty} is addressed per edge type, so a caller that was given no type filter but needs
   * edge properties has to ask which types exist rather than pass "all types" down. A provider that answers
   * {@code null} simply cannot serve such a caller, which then falls back to reading the edge records.
   */
  default String[] getMaterializedEdgeTypes() {
    return null;
  }

  /**
   * Returns true if this provider has edge property columns materialized <em>and</em> the positional mapping
   * {@link #getEdgeProperty} relies on is exact.
   * <p>
   * The second half is what makes the answer usable. {@code getEdgeProperty} addresses an edge by its position
   * in the node's neighbour list for a direction, so it means something only while that position identifies the
   * same edge {@link #getNeighborIds} reports there. A provider whose neighbour lists no longer line up with its
   * property columns - because pending changes are served from a side structure, say - must answer {@code false}
   * rather than hand back a weight belonging to another edge. The caller holds the edge records and can always
   * read the property itself; a plausible wrong number is the one outcome it cannot recover from. Same
   * "say unknown rather than guess" convention as {@link #countEdgesBetween} and
   * {@link #getMeanEdgesPerConnectedPair}.
   */
  default boolean hasEdgeProperties() {
    return false;
  }

  /**
   * Returns true if this provider can serve {@code propertyName} for {@code edgeType} through
   * {@link #getEdgeProperty}.
   * <p>
   * {@link #hasEdgeProperties()} answers the coarser question - are there edge property columns at all - and a
   * caller that asks only that one gets {@code null} back from every {@link #getEdgeProperty} call when the view
   * happens to materialise a <em>different</em> property than the one requested. Since a {@code null} property
   * value is also the ordinary way of saying "this edge has no value", the caller cannot tell the two apart and
   * silently treats the whole graph as unweighted. That is the same failure as reading a weight that belongs to
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
   * Returns an edge property value from columnar storage, or null if not materialized.
   * <p>
   * The {@code neighborIndex} is the position within the node's adjacency list for the given direction
   * (0-based, matching the order returned by {@link #getNeighborIds}).
   *
   * @param nodeId         the source node's dense ID
   * @param neighborIndex  the index within the node's neighbor list for the given direction
   * @param direction      OUT or IN
   * @param edgeType       the edge type name
   * @param propertyName   the property to retrieve
   *
   * @return the property value, or null if not available
   */
  default Object getEdgeProperty(final int nodeId, final int neighborIndex,
      final Vertex.DIRECTION direction, final String edgeType, final String propertyName) {
    return null;
  }

  /**
   * Returns one node's neighbours together with the edge-property value of each edge reaching them, or
   * {@code null} when this provider cannot serve {@code propertyName} for every type in play.
   * <p>
   * <b>This is the only correct way to read an edge property positionally, and the reason it lives here rather
   * than in each caller.</b> {@link #getEdgeProperty} is addressed by (type, direction, position), and two things
   * break a caller that pairs it with {@link #getNeighborIds} by hand:
   * <ul>
   *   <li>a multi-type neighbour list is <em>merged and sorted</em> across types, so position {@code j} in it is
   *       not position {@code j} in any one type's column - the weight lands on another edge, or on none;</li>
   *   <li>{@link Vertex.DIRECTION#BOTH} has no column of its own at all. A provider resolves {@code OUT} and
   *       {@code IN}, so a {@code BOTH} lookup answers {@code null} for every edge and the caller silently reads
   *       the whole neighbourhood at its default weight.</li>
   * </ul>
   * Both are handled here once: the walk takes one slice per (type, direction) pair, each of which <em>is</em>
   * positional, and concatenates them.
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
   * {@code edgeCheckpoint}, if not {@code null}, is called once per edge as the per-(type, direction) slices are
   * copied into the returned arrays, with a counter that restarts at zero for every call to this method.
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
   *
   * @param edgeCheckpoint called with the edge index within this call (0-based), or {@code null} to skip it
   */
  default NodeEdgeWeights edgeWeightsOf(final int nodeId, final Vertex.DIRECTION direction,
      final String propertyName, final double defaultWeight, final IntConsumer edgeCheckpoint,
      final String... edgeTypes) {
    if (!servesEdgeProperty(propertyName, edgeTypes))
      return null;

    final String[] types = edgeTypes != null && edgeTypes.length > 0 ? edgeTypes : getMaterializedEdgeTypes();
    final Vertex.DIRECTION[] directions = direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };

    final int[][] slices = new int[types.length * directions.length][];
    int degree = 0;
    int s = 0;
    for (final String type : types)
      for (final Vertex.DIRECTION d : directions) {
        final int[] slice = getNeighborIds(nodeId, d, type);
        slices[s++] = slice;
        degree += slice.length;
      }

    final int[] neighbors = new int[degree];
    final double[] weights = new double[degree];
    int pos = 0;
    int edgeCount = 0;
    s = 0;
    for (final String type : types)
      for (final Vertex.DIRECTION d : directions) {
        final int[] slice = slices[s++];
        for (int j = 0; j < slice.length; j++) {
          if (edgeCheckpoint != null)
            edgeCheckpoint.accept(edgeCount++);
          neighbors[pos] = slice[j];
          final Object value = getEdgeProperty(nodeId, j, d, type, propertyName);
          weights[pos] = value instanceof Number num ? num.doubleValue() : defaultWeight;
          pos++;
        }
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
