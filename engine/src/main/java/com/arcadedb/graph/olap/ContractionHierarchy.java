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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;

/**
 * A Customizable Contraction Hierarchy (issue #9437) kept on a {@link GraphAnalyticalView} for one weight property and
 * one set of edge types, answering point-to-point shortest paths in a time that depends on the depth of its elimination
 * tree instead of on the distance between the two vertices.
 * <p>
 * <b>Kept in step with the view.</b> Every snapshot the view publishes (a build, a restore, a commit absorbed into the
 * overlay, a compaction) schedules a background preparation for it, on the view's own build executor and under its
 * build permits:
 * <ol>
 *   <li>the routed arcs and their weights are read out of the snapshot, overlay included;</li>
 *   <li>if every one of them is an arc of the current topology's supergraph - a weight change, a deletion, an edge
 *       added next to an existing one, or one the contraction had already created as a shortcut - the topology is kept
 *       and only re-customized (or not even that, when the costs it starts from are unchanged: a vertex property update
 *       publishes a new snapshot too). A new base CSR, whose dense ids are renumbered, is mapped onto the topology by
 *       RID;</li>
 *   <li>otherwise a new topology is ordered and contracted from scratch.</li>
 * </ol>
 * A query answers only from a preparation made for the very snapshot the view is serving: anything else - a
 * preparation still running, a transaction holding uncommitted changes, a view that is not ready - makes
 * {@link #shortestPath} return null and the caller answers through bidirectional Dijkstra instead
 * ({@link ShortestPathFinder}). An answer is therefore never older than the view itself.
 * <p>
 * <b>Refused when unsuitable.</b> Graphs without small separators need a supergraph many times larger than themselves.
 * The topology build stops at {@link GlobalConfiguration#GAV_CCH_MAX_ARCS_PER_EDGE} arcs per edge, the hierarchy reports
 * {@link Status#UNSUITABLE} and is not attempted again until the view is rebuilt from scratch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ContractionHierarchy {
  public enum Status {
    /** Not prepared yet for the snapshot the view is serving. */
    PREPARING,
    /** Prepared for the snapshot the view is serving. */
    READY,
    /** The graph needs more supergraph arcs than allowed: queries use bidirectional Dijkstra. */
    UNSUITABLE,
    /** The view cannot hand out its arcs exactly right now (e.g. its edge columns are being rebuilt). */
    UNAVAILABLE
  }

  private static final int  MODE_DIRECTED   = 1;
  private static final int  MODE_UNDIRECTED = 2;
  private static final long MIN_ARC_BUDGET  = 100_000;

  private final GraphAnalyticalView view;
  private final String              weightProperty;
  private final String[]            edgeTypes; // null = every edge type of the view

  private volatile Prepared      prepared;
  private volatile TopologyRoot  root;
  private volatile Status        status = Status.PREPARING;
  private volatile String        statusReason;
  private final    AtomicInteger requestedModes = new AtomicInteger(MODE_DIRECTED);
  private volatile boolean       closed;
  private final    AtomicBoolean running        = new AtomicBoolean();
  private volatile boolean       rerunRequested;
  // the snapshot the last preparation was attempted for, whatever its outcome
  private volatile GraphAnalyticalView.Snapshot lastAttempt;
  private final    Object        readyMonitor   = new Object();

  private final LongAdder topologyBuilds      = new LongAdder();
  private final LongAdder topologyRestores    = new LongAdder();
  private final LongAdder customizations      = new LongAdder();
  private final LongAdder customizationsSaved = new LongAdder();
  private final LongAdder queries             = new LongAdder();
  private final LongAdder fallbacks           = new LongAdder();
  private volatile long   lastTopologyBuildMs;
  private volatile long   lastCustomizationMs;

  /** The routed arcs of one snapshot, in its dense id space. */
  record Input(int nodeCount, int[] tails, int[] heads, double[] weights, int count) {
  }

  /**
   * A topology plus what is needed to map any later snapshot's vertices onto its node space: the node space is the dense
   * id space of the snapshot it was built from, so its base mapping and overlay are what resolve a RID into it.
   */
  private record TopologyRoot(CCHTopology topology, NodeIdMapping mapping, DeltaOverlay overlay) {
    int nodeOf(final RID rid) {
      if (rid == null)
        return -1;
      final int node = overlay != null ? overlay.resolveNodeId(rid, mapping) : mapping.getGlobalId(rid);
      return node < topology.nodeCount ? node : -1;
    }
  }

  /** Everything a query needs, for exactly one snapshot of the view. */
  private record Prepared(GraphAnalyticalView.Snapshot snapshot, TopologyRoot root, int[] denseToNode, int[] nodeToDense,
                          CCHMetric directed, CCHMetric undirected) {
    CCHMetric metric(final int mode) {
      return mode == MODE_UNDIRECTED ? undirected : directed;
    }
  }

  /** A shortest path: its vertices in walking order and its total weight. */
  public record Path(List<RID> vertices, double weight) {
  }

  /** The answer for two vertices the hierarchy proves are not connected. */
  public static final Path NO_PATH = new Path(List.of(), Double.POSITIVE_INFINITY);

  ContractionHierarchy(final GraphAnalyticalView view, final String weightProperty, final String[] edgeTypes) {
    if (weightProperty == null || weightProperty.isEmpty())
      throw new IllegalArgumentException("A contraction hierarchy needs a weight property");
    this.view = view;
    this.weightProperty = weightProperty;
    this.edgeTypes = edgeTypes == null || edgeTypes.length == 0 ? null : edgeTypes.clone();
  }

  public String getWeightProperty() {
    return weightProperty;
  }

  /** The edge types this hierarchy routes over; null for every type of the view. */
  public String[] getEdgeTypes() {
    return edgeTypes == null ? null : edgeTypes.clone();
  }

  public Status getStatus() {
    final Status s = status;
    if (s == Status.READY) {
      final Prepared p = prepared;
      if (p == null || p.snapshot != view.currentSnapshot())
        return Status.PREPARING;
    }
    return s;
  }

  /** Why the hierarchy is {@link Status#UNSUITABLE} or {@link Status#UNAVAILABLE}; null otherwise. */
  public String getStatusReason() {
    return statusReason;
  }

  /**
   * Finds a hierarchy that can answer for {@code weightProperty} over exactly {@code edgeTypes} (null or empty: every edge
   * type of the database) on a ready view, or null. Never returns one while the calling thread's transaction holds
   * uncommitted changes, which the view cannot see.
   */
  public static ContractionHierarchy find(final Database database, final String weightProperty, final String... edgeTypes) {
    if (!GraphTraversalProviderRegistry.hasAnyProviders() || GraphTraversalProviderRegistry.isWithheld(database))
      return null;
    for (final GraphTraversalProvider provider : GraphTraversalProviderRegistry.getProviders(database)) {
      if (!(provider instanceof GraphAnalyticalView view) || !view.hasContractionHierarchies())
        continue;
      final ContractionHierarchy hierarchy = view.getContractionHierarchy(weightProperty, edgeTypes);
      if (hierarchy != null && provider.coversVertexType(null) && provider.isReady())
        return hierarchy;
    }
    return null;
  }

  /** True when this hierarchy routes over exactly the types {@code requested} names on its view. */
  boolean matches(final String weight, final String[] requested) {
    if (!weightProperty.equals(weight))
      return false;
    final boolean all = requested == null || requested.length == 0;
    if (edgeTypes == null)
      return all ? view.coversEdgeType(null) : sameTypes(view.getMaterializedEdgeTypes(), view.resolveEdgeTypes(requested));
    if (all)
      return false;
    return sameTypes(view.resolveEdgeTypes(edgeTypes), view.resolveEdgeTypes(requested));
  }

  private static boolean sameTypes(final String[] a, final String[] b) {
    if (a == null || b == null)
      return false;
    return Set.of(a).equals(Set.of(b));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Query

  /**
   * A shortest path from {@code source} to {@code target} on the snapshot the view is serving.
   *
   * @param direction OUT follows edges forward, IN backward, BOTH ignores their direction
   *
   * @return the path, {@link #NO_PATH} when there is none, or null when this hierarchy cannot answer for the view's
   * current snapshot (the caller must compute the path itself)
   */
  public Path shortestPath(final RID source, final RID target, final Vertex.DIRECTION direction) {
    queries.increment();
    final Path path = answer(source, target, direction);
    if (path == null)
      fallbacks.increment();
    return path;
  }

  private Path answer(final RID source, final RID target, final Vertex.DIRECTION direction) {
    if (closed || source == null || target == null)
      return null;
    final GraphAnalyticalView.Snapshot snap = view.currentSnapshot();
    if (snap == null)
      return null;
    final int mode = direction == Vertex.DIRECTION.BOTH ? MODE_UNDIRECTED : MODE_DIRECTED;
    final Prepared p = prepared;
    if (p == null || p.snapshot != snap || p.metric(mode) == null) {
      request(mode);
      return null;
    }

    final int sourceDense = view.nodeIdOf(snap, source);
    final int targetDense = view.nodeIdOf(snap, target);
    // a vertex the view does not hold: the caller reads the records, which can answer for it
    if (sourceDense < 0 || targetDense < 0)
      return null;
    if (sourceDense == targetDense)
      return new Path(List.of(source), 0);

    final int sourceNode = sourceDense < p.denseToNode.length ? p.denseToNode[sourceDense] : -1;
    final int targetNode = targetDense < p.denseToNode.length ? p.denseToNode[targetDense] : -1;
    // a vertex the topology does not know has no routed arcs: preparing this snapshot would have failed otherwise
    if (sourceNode < 0 || targetNode < 0)
      return NO_PATH;

    final CCHMetric metric = p.metric(mode);
    final boolean reverse = direction == Vertex.DIRECTION.IN;
    final CCHMetric.PathResult result = reverse ?
        metric.shortestPathWithDistance(targetNode, sourceNode) :
        metric.shortestPathWithDistance(sourceNode, targetNode);
    if (result == null)
      return NO_PATH;

    final int[] nodes = result.nodes();
    final List<RID> vertices = new ArrayList<>(nodes.length);
    for (int i = 0; i < nodes.length; i++) {
      final int node = nodes[reverse ? nodes.length - 1 - i : i];
      final RID rid = view.ridOf(snap, p.nodeToDense[node]);
      if (rid == null)
        return null; // never expected: the preparation mapped every routed vertex
      vertices.add(rid);
    }
    return new Path(vertices, result.distance());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Preparation

  /** Asks for the metric of {@code mode} too, and for a preparation of the view's current snapshot. */
  private void request(final int mode) {
    requestedModes.getAndUpdate(modes -> modes | mode);
    schedule();
  }

  /** Called by the view whenever it publishes a new snapshot. */
  void onSnapshotPublished() {
    if (!closed && status != Status.UNSUITABLE)
      schedule();
  }

  /** Called by the view when it is built from scratch on request: an unsuitable graph may have become suitable. */
  void onRebuildRequested() {
    if (status == Status.UNSUITABLE) {
      status = Status.PREPARING;
      statusReason = null;
    }
  }

  private void schedule() {
    if (closed || status == Status.UNSUITABLE)
      return;
    if (!running.compareAndSet(false, true)) {
      rerunRequested = true;
      return;
    }
    rerunRequested = false;
    if (!view.dispatchBackgroundTask(this::prepareLoop))
      running.set(false);
  }

  private void prepareLoop() {
    try {
      do {
        rerunRequested = false;
        final GraphAnalyticalView.Snapshot snap = view.currentSnapshot();
        if (snap == null || closed)
          break;
        final Prepared current = prepared;
        if (current != null && current.snapshot == snap && hasRequestedModes(current))
          continue;
        prepare(snap);
      } while (rerunRequested && !closed && status != Status.UNSUITABLE);
    } catch (final Throwable e) {
      statusReason = e.toString();
      status = Status.UNAVAILABLE;
      LogManager.instance().log(this, Level.WARNING, "Contraction hierarchy on '%s' of view '%s' could not be prepared", e,
          weightProperty, view.getName());
    } finally {
      running.set(false);
      synchronized (readyMonitor) {
        readyMonitor.notifyAll();
      }
      // a request that landed between the last check of the loop and the release above would otherwise be lost
      if (rerunRequested && !closed && status != Status.UNSUITABLE)
        schedule();
    }
  }

  private boolean hasRequestedModes(final Prepared p) {
    final int modes = requestedModes.get();
    return ((modes & MODE_DIRECTED) == 0 || p.directed != null) && ((modes & MODE_UNDIRECTED) == 0 || p.undirected != null);
  }

  private void prepare(final GraphAnalyticalView.Snapshot snap) {
    lastAttempt = snap;
    final Input input = view.extractWeightedArcs(snap, edgeTypes, weightProperty, this::isClosed);
    if (closed)
      return;
    if (input == null) {
      // what was prepared before answers for a snapshot the view no longer serves: holding it would pin that
      // snapshot's CSR until this hierarchy can be prepared again, which may be never
      prepared = null;
      status = Status.UNAVAILABLE;
      statusReason = "the view cannot serve '" + weightProperty + "' exactly for its current snapshot";
      return;
    }

    TopologyRoot topologyRoot = root;
    int[] denseToNode = null;
    double[][] directedInput = null;
    double[][] undirectedInput = null;
    final int modes = requestedModes.get();

    if (topologyRoot != null) {
      denseToNode = mapOnto(topologyRoot, snap, input.nodeCount());
      final int[] tails = translate(input.tails(), input.count(), denseToNode);
      final int[] heads = tails == null ? null : translate(input.heads(), input.count(), denseToNode);
      if (heads != null) {
        if ((modes & MODE_DIRECTED) != 0)
          directedInput = CCHMetric.inputCosts(topologyRoot.topology, tails, heads, input.weights(), input.count(), false);
        if ((modes & MODE_UNDIRECTED) != 0 && ((modes & MODE_DIRECTED) == 0 || directedInput != null))
          undirectedInput = CCHMetric.inputCosts(topologyRoot.topology, tails, heads, input.weights(), input.count(), true);
      }
      final boolean fits = heads != null && ((modes & MODE_DIRECTED) == 0 || directedInput != null)
          && ((modes & MODE_UNDIRECTED) == 0 || undirectedInput != null);
      if (!fits) {
        topologyRoot = null;
        directedInput = null;
        undirectedInput = null;
      }
    }

    if (topologyRoot == null) {
      final long begin = System.nanoTime();
      final long budget = Math.max(MIN_ARC_BUDGET,
          (long) input.count() * view.getDatabaseConfiguration().getValueAsInteger(GlobalConfiguration.GAV_CCH_MAX_ARCS_PER_EDGE));
      // a CSR restored from disk with nothing committed since may come with the order computed for it before the close
      final int[] persistedOrder = root == null && snap.restoredFromDisk && snap.overlay == null && view.getName() != null ?
          CCHOrderPersistence.load(view.getDatabase(), orderFile(), snap.asOfTransactionId) : null;
      final boolean reusesOrder = persistedOrder != null && persistedOrder.length == input.nodeCount();
      final CCHTopology topology = CCHTopology.build(input.nodeCount(), input.tails(), input.heads(), input.count(), budget,
          reusesOrder ? persistedOrder : null, this::isClosed);
      if (closed)
        return;
      if (topology == null) {
        status = Status.UNSUITABLE;
        statusReason = "the graph needs more than " + budget + " supergraph arcs (" + input.count() + " routed edges): it "
            + "has no small separators. Shortest paths are answered by bidirectional Dijkstra";
        prepared = null;
        LogManager.instance().log(this, Level.WARNING, "Contraction hierarchy on '%s' of view '%s' not built: %s",
            weightProperty, view.getName(), statusReason);
        return;
      }
      lastTopologyBuildMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
      if (reusesOrder)
        topologyRestores.increment();
      else
        topologyBuilds.increment();
      topologyRoot = new TopologyRoot(topology, snap.nodeMapping, snap.overlay);
      root = topologyRoot;
      denseToNode = identity(input.nodeCount());
      if ((modes & MODE_DIRECTED) != 0)
        directedInput = CCHMetric.inputCosts(topology, input.tails(), input.heads(), input.weights(), input.count(), false);
      if ((modes & MODE_UNDIRECTED) != 0)
        undirectedInput = CCHMetric.inputCosts(topology, input.tails(), input.heads(), input.weights(), input.count(), true);
    }

    final Prepared previous = prepared;
    final CCHMetric directed = directedInput == null ? null : metric(previous, topologyRoot, directedInput, false);
    final CCHMetric undirected = undirectedInput == null ? null : metric(previous, topologyRoot, undirectedInput, true);
    if (closed || (directedInput != null && directed == null) || (undirectedInput != null && undirected == null))
      return;

    final int[] nodeToDense = new int[topologyRoot.topology.nodeCount];
    Arrays.fill(nodeToDense, -1);
    for (int d = 0; d < denseToNode.length; d++)
      if (denseToNode[d] >= 0)
        nodeToDense[denseToNode[d]] = d;

    prepared = new Prepared(snap, topologyRoot, denseToNode, nodeToDense, directed, undirected);
    statusReason = null;
    status = Status.READY;
  }

  /** The previous metric when its input costs are unchanged, else a fresh customization. */
  private CCHMetric metric(final Prepared previous, final TopologyRoot topologyRoot, final double[][] input,
      final boolean undirected) {
    if (previous != null && previous.root == topologyRoot) {
      final CCHMetric old = undirected ? previous.undirected : previous.directed;
      if (old != null && old.hasInput(input[0], input[1], undirected)) {
        customizationsSaved.increment();
        return old;
      }
    }
    final long begin = System.nanoTime();
    final CCHMetric metric = CCHMetric.customize(topologyRoot.topology, input[0], input[1], undirected, this::isClosed);
    if (metric != null) {
      lastCustomizationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
      customizations.increment();
    }
    return metric;
  }

  /**
   * The topology node of each dense id of {@code snap}, -1 for one it does not know. A snapshot over the base the
   * topology was built from shares its ids; any other base renumbered them, and is mapped by RID.
   */
  private int[] mapOnto(final TopologyRoot topologyRoot, final GraphAnalyticalView.Snapshot snap, final int nodeCount) {
    final int known = topologyRoot.topology.nodeCount;
    if (snap.nodeMapping == topologyRoot.mapping) {
      final int[] result = new int[nodeCount];
      for (int d = 0; d < nodeCount; d++)
        result[d] = d < known ? d : -1;
      return result;
    }
    final int[] result = new int[nodeCount];
    final boolean[] taken = new boolean[known];
    for (int d = 0; d < nodeCount; d++) {
      result[d] = -1;
      if (!view.isNodeLive(snap, d))
        continue;
      final int node = topologyRoot.nodeOf(view.ridOf(snap, d));
      if (node >= 0 && !taken[node]) {
        taken[node] = true;
        result[d] = node;
      }
    }
    return result;
  }

  private static int[] translate(final int[] ids, final int count, final int[] map) {
    final int[] result = new int[count];
    for (int i = 0; i < count; i++) {
      final int id = ids[i];
      final int mapped = id < map.length ? map[id] : -1;
      if (mapped < 0)
        return null;
      result[i] = mapped;
    }
    return result;
  }

  private static int[] identity(final int n) {
    final int[] result = new int[n];
    for (int i = 0; i < n; i++)
      result[i] = i;
    return result;
  }

  private boolean isClosed() {
    return closed;
  }

  /** Where the view persists this hierarchy's vertex order. */
  File orderFile() {
    return CCHOrderPersistence.fileFor(view.getDatabase(), view.getName(), weightProperty, edgeTypes);
  }

  /**
   * The vertex order of the topology, when it was computed on exactly the base CSR of {@code snap} with no overlay
   * changes on top: the only case a persisted CSR certificate describes. Null otherwise.
   */
  int[] orderFor(final GraphAnalyticalView.Snapshot snap) {
    final TopologyRoot r = root;
    if (r == null || r.mapping != snap.nodeMapping || (r.overlay != null && r.overlay.hasChanges()))
      return null;
    return r.topology.nodeAt;
  }

  /** Stops preparing and answering. What was prepared stays readable, for the view to persist the order on close. */
  void close() {
    closed = true;
    synchronized (readyMonitor) {
      readyMonitor.notifyAll();
    }
  }

  /**
   * Waits until this hierarchy is prepared for the snapshot the view is serving (for the directed metric, and for the
   * undirected one too when {@code undirected} is set), or can tell it never will be.
   *
   * @return true when ready, false on timeout or when the hierarchy is unsuitable or unavailable
   */
  public boolean awaitReady(final boolean undirected, final long timeout, final TimeUnit unit) {
    if (undirected)
      request(MODE_UNDIRECTED);
    final long deadline = System.nanoTime() + unit.toNanos(timeout);
    while (true) {
      if (closed || status == Status.UNSUITABLE)
        return false;
      final GraphAnalyticalView.Snapshot snap = view.currentSnapshot();
      final Prepared p = prepared;
      if (snap != null && p != null && p.snapshot == snap && hasRequestedModes(p) && status == Status.READY)
        return true;
      if (!running.get()) {
        // the last attempt was for this very snapshot and could not be completed: waiting longer changes nothing,
        // unless the view is about to replace it (its edge columns are being rebuilt after a weight update)
        if (status == Status.UNAVAILABLE && lastAttempt == snap && !view.isSnapshotBeingReplaced(snap))
          return false;
        schedule();
      }
      final long remaining = deadline - System.nanoTime();
      if (remaining <= 0)
        return false;
      synchronized (readyMonitor) {
        try {
          readyMonitor.wait(Math.max(1, Math.min(50, TimeUnit.NANOSECONDS.toMillis(remaining))));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Introspection

  public long getTopologyBuildCount() {
    return topologyBuilds.sum();
  }

  /** Topologies contracted in an order persisted at the previous close, instead of a freshly computed one. */
  public long getTopologyRestoreCount() {
    return topologyRestores.sum();
  }

  public long getCustomizationCount() {
    return customizations.sum();
  }

  public long getQueryCount() {
    return queries.sum();
  }

  public long getFallbackCount() {
    return fallbacks.sum();
  }

  long getMemoryUsageBytes() {
    final Prepared p = prepared;
    long bytes = 0;
    final TopologyRoot r = root;
    if (r != null)
      bytes += r.topology.getMemoryUsageBytes();
    if (p != null) {
      bytes += 4L * (p.denseToNode.length + p.nodeToDense.length);
      if (p.directed != null)
        bytes += p.directed.getMemoryUsageBytes();
      if (p.undirected != null)
        bytes += p.undirected.getMemoryUsageBytes();
    }
    return bytes;
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("weightProperty", weightProperty);
    stats.put("edgeTypes", edgeTypes == null ? null : List.of(edgeTypes));
    stats.put("status", getStatus().name());
    if (statusReason != null)
      stats.put("statusReason", statusReason);
    final TopologyRoot r = root;
    if (r != null) {
      stats.put("nodes", r.topology.nodeCount);
      stats.put("arcs", r.topology.arcCount());
    }
    stats.put("memoryUsageBytes", getMemoryUsageBytes());
    stats.put("topologyBuilds", topologyBuilds.sum());
    stats.put("topologyRestores", topologyRestores.sum());
    stats.put("lastTopologyBuildMs", lastTopologyBuildMs);
    stats.put("customizations", customizations.sum());
    stats.put("customizationsReused", customizationsSaved.sum());
    stats.put("lastCustomizationMs", lastCustomizationMs);
    stats.put("queries", queries.sum());
    stats.put("fallbacks", fallbacks.sum());
    return stats;
  }
}
