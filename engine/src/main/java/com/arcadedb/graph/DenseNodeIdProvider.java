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

import java.util.Arrays;
import java.util.function.IntConsumer;

/**
 * A {@link GraphTraversalProvider} whose node ID space has no holes in it, wrapped around one whose has.
 * <p>
 * A provider that allocates node IDs monotonically and does not renumber when a node is deleted - which is what
 * a Graph Analytical View's delta overlay does - reports a live node count smaller than the exclusive bound of
 * its own ID space. Every consumer that indexes an array by node ID then has two ways to be wrong at once, and
 * neither of them is an error it can attribute (issue #6792):
 * <ul>
 *   <li>sizing the array from {@link GraphTraversalProvider#getNodeCount()} leaves the highest live nodes
 *       outside it, so they are silently absent from the answer;</li>
 *   <li>sizing it from {@link GraphTraversalProvider#getNodeIdUpperBound()} covers them, but then every array
 *       carries a slot per deleted node and every enumeration of it has to remember to skip those slots -
 *       a rule that has to hold in every one of the fifty-odd {@code algo.*} procedures to be worth anything.</li>
 * </ul>
 * This removes the choice rather than restating it. The holes are renumbered away once, here, and the whole
 * graph is handed on as the compact {@code 0..getNodeCount()} space the SPI documents - so a consumer needs no
 * rule at all, and one written before overlays existed is correct in front of one.
 * <p>
 * {@link #wrap(GraphTraversalProvider)} returns the provider unchanged when its IDs are already compact, so the
 * translation costs nothing on the ordinary path: holes exist only while an overlay is holding deletions.
 * <p>
 * The mapping is built once, from the ID space as it stood at construction, and every answer this instance gives
 * is consistent with that one reading. A node deleted afterwards keeps its dense ID and resolves to a RID that
 * no longer loads - the same "deleted since the graph was loaded" case every consumer of this SPI already
 * handles - rather than shifting the IDs of its neighbours underneath a running algorithm.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DenseNodeIdProvider implements GraphTraversalProvider {
  private static final int[] EMPTY_NEIGHBORS = new int[0];

  private final GraphTraversalProvider delegate;
  /** Dense ID -> delegate ID, one entry per live node. */
  private final int[]                  toDelegate;
  /** Delegate ID -> dense ID, {@code -1} for a hole. */
  private final int[]                  toDense;

  /**
   * Returns {@code provider} itself when its node IDs are already compact, and a renumbering wrapper around it
   * when they are not.
   * <p>
   * Asking is one comparison, so a caller that may be handed either shape calls this unconditionally rather than
   * testing for a Graph Analytical View, or for an overlay, at the call site: the question is about the ID space,
   * not about who produced it.
   */
  public static GraphTraversalProvider wrap(final GraphTraversalProvider provider) {
    if (provider == null)
      return null;
    final int bound = provider.getNodeIdUpperBound();
    if (bound == provider.getNodeCount())
      // Compact already: every ID below the bound is a live node, which is the space this class would build.
      return provider;
    return new DenseNodeIdProvider(provider, bound);
  }

  private DenseNodeIdProvider(final GraphTraversalProvider delegate, final int bound) {
    this.delegate = delegate;
    this.toDense = new int[bound];
    int live = 0;
    for (int id = 0; id < bound; id++)
      toDense[id] = delegate.isNodeLive(id) ? live++ : -1;

    // Sized from the scan rather than from getNodeCount(): the two must agree, and if a provider ever reports a
    // count its own liveness predicate does not back up, the scan is the one this class can stand behind.
    this.toDelegate = new int[live];
    for (int id = 0; id < bound; id++)
      if (toDense[id] >= 0)
        toDelegate[toDense[id]] = id;
  }

  /** The provider being renumbered, for a caller that has to reach past the dense ID space. */
  public GraphTraversalProvider getDelegate() {
    return delegate;
  }

  private int delegateId(final int denseId) {
    return denseId >= 0 && denseId < toDelegate.length ? toDelegate[denseId] : -1;
  }

  private int denseId(final int delegateId) {
    return delegateId >= 0 && delegateId < toDense.length ? toDense[delegateId] : -1;
  }

  /**
   * Renumbers a neighbour row into a new array, compacting away any entry pointing at a hole.
   * <p>
   * A new array rather than the delegate's own: a CSR-backed provider is free to hand back a slice of its
   * adjacency arrays when it has nothing to merge into them, and renumbering that in place would rewrite the
   * graph itself. The copy is what makes this class safe to put in front of any provider rather than only in
   * front of the ones whose rows are known to be fresh.
   * <p>
   * A hole cannot normally appear in a row - deleting a vertex deletes its edges, so the overlay drops both
   * together - but a row that did carry one would otherwise renumber it to {@code -1} and hand the caller a
   * negative array index, which is the very failure this class exists to remove.
   */
  private int[] translate(final int[] neighbors) {
    if (neighbors == null || neighbors.length == 0)
      return EMPTY_NEIGHBORS;
    final int[] dense = new int[neighbors.length];
    int kept = 0;
    for (final int neighbor : neighbors) {
      final int id = denseId(neighbor);
      if (id >= 0)
        dense[kept++] = id;
    }
    return kept == dense.length ? dense : Arrays.copyOf(dense, kept);
  }

  @Override
  public int getNodeCount() {
    return toDelegate.length;
  }

  @Override
  public int getNodeIdUpperBound() {
    return toDelegate.length;
  }

  @Override
  public boolean isNodeLive(final int nodeId) {
    return nodeId >= 0 && nodeId < toDelegate.length;
  }

  @Override
  public boolean hasPendingChanges() {
    // Renumbering the IDs says nothing about whether the delegate's own CSR arrays are the whole graph, and a
    // caller reaching for those arrays has to get the delegate's honest answer, not a reassuring one.
    return delegate.hasPendingChanges();
  }

  @Override
  public boolean isReady() {
    return delegate.isReady();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean coversVertexType(final String typeName) {
    return delegate.coversVertexType(typeName);
  }

  @Override
  public boolean coversEdgeType(final String edgeTypeName) {
    return delegate.coversEdgeType(edgeTypeName);
  }

  @Override
  public int getNodeId(final RID rid) {
    return denseId(delegate.getNodeId(rid));
  }

  @Override
  public RID getRID(final int nodeId) {
    final int id = delegateId(nodeId);
    return id < 0 ? null : delegate.getRID(id);
  }

  @Override
  public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final int id = delegateId(nodeId);
    if (id < 0)
      return EMPTY_NEIGHBORS;
    return translate(delegate.getNeighborIds(id, direction, edgeTypes));
  }

  @Override
  public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final int id = delegateId(nodeId);
    return id < 0 ? 0 : delegate.countEdges(id, direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final int a = delegateId(nodeA);
    final int b = delegateId(nodeB);
    return a >= 0 && b >= 0 && delegate.isConnectedTo(a, b, direction, edgeTypes);
  }

  @Override
  public long countEdgesBetween(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final int a = delegateId(nodeA);
    final int b = delegateId(nodeB);
    return a < 0 || b < 0 ? 0 : delegate.countEdgesBetween(a, b, direction, edgeTypes);
  }

  @Override
  public Object getProperty(final int nodeId, final String propertyName) {
    final int id = delegateId(nodeId);
    return id < 0 ? null : delegate.getProperty(id, propertyName);
  }

  @Override
  public double getMeanEdgesPerConnectedPair(final String edgeType) {
    return delegate.getMeanEdgesPerConnectedPair(edgeType);
  }

  @Override
  public String[] getMaterializedEdgeTypes() {
    return delegate.getMaterializedEdgeTypes();
  }

  @Override
  public boolean hasEdgeProperties() {
    return delegate.hasEdgeProperties();
  }

  @Override
  public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
    return delegate.hasEdgeProperty(edgeType, propertyName);
  }

  @Override
  public boolean servesEdgeProperty(final String propertyName, final String... edgeTypes) {
    return delegate.servesEdgeProperty(propertyName, edgeTypes);
  }

  @Override
  public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
      final String edgeType, final String propertyName, final double defaultWeight, final IntConsumer edgeCheckpoint) {
    final int id = delegateId(nodeId);
    if (id < 0)
      return null;
    return translate(
        delegate.edgeWeightsForSlice(id, direction, edgeType, propertyName, defaultWeight, edgeCheckpoint));
  }

  @Override
  public NodeEdgeWeights edgeWeightsOf(final int nodeId, final Vertex.DIRECTION direction,
      final String propertyName, final double defaultWeight, final IntConsumer edgeCheckpoint,
      final String[] edgeTypes) {
    final int id = delegateId(nodeId);
    if (id < 0)
      return null;
    return translate(
        delegate.edgeWeightsOf(id, direction, propertyName, defaultWeight, edgeCheckpoint, edgeTypes));
  }

  /**
   * Renumbers a weighted row, dropping a {@code (neighbour, weight)} pair whose neighbour is a hole - both
   * halves of it, so the two arrays stay paired by construction the way the SPI promises.
   */
  private NodeEdgeWeights translate(final NodeEdgeWeights row) {
    if (row == null)
      return null;
    final int[] neighbors = row.neighbors();
    final double[] sourceWeights = row.weights();
    final int[] denseNeighbors = new int[neighbors.length];
    final double[] weights = new double[neighbors.length];
    int kept = 0;
    for (int i = 0; i < neighbors.length; i++) {
      final int dense = denseId(neighbors[i]);
      if (dense >= 0) {
        denseNeighbors[kept] = dense;
        weights[kept] = sourceWeights[i];
        kept++;
      }
    }
    if (kept == denseNeighbors.length)
      return new NodeEdgeWeights(denseNeighbors, weights);
    return new NodeEdgeWeights(Arrays.copyOf(denseNeighbors, kept), Arrays.copyOf(weights, kept));
  }

  @Override
  public boolean isStale() {
    return delegate.isStale();
  }

  @Override
  public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    // A packed view IS the delegate's own CSR arrays, addressed by its own node IDs. Renumbering it would mean
    // copying the whole graph, which is the allocation the view exists to avoid, so the honest answer is the one
    // the SPI already reserves for "I cannot serve this": the caller falls back to per-node lookups, which this
    // class does renumber.
    return null;
  }

  @Override
  public void getDegrees(final int[] degrees, final Vertex.DIRECTION direction, final String edgeType) {
    final int[] scratch = new int[toDense.length];
    delegate.getDegrees(scratch, direction, edgeType);
    Arrays.fill(degrees, 0);
    final int n = Math.min(degrees.length, toDelegate.length);
    for (int dense = 0; dense < n; dense++)
      degrees[dense] = scratch[toDelegate[dense]];
  }

  @Override
  public String toString() {
    return "DenseNodeIdProvider(" + delegate.getName() + ", " + toDelegate.length + " of " + toDense.length + ")";
  }
}
