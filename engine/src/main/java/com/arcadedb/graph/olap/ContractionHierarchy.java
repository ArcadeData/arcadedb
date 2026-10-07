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
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.IntIntHashMap;

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
  // A partial customization pays for each changed arc what a full one pays per arc, plus the propagation: past an
  // eighth of the arcs, a full one is cheaper
  private static final int  PARTIAL_LIMIT_MIN     = 1_024;
  private static final int  PARTIAL_LIMIT_DIVISOR = 8;

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

  private final LongAdder topologyBuilds         = new LongAdder();
  private final LongAdder topologyRestores       = new LongAdder();
  private final LongAdder topologyRecontractions = new LongAdder();
  private final LongAdder customizations         = new LongAdder();
  private final LongAdder customizationsSaved    = new LongAdder();
  private final LongAdder partialCustomizations  = new LongAdder();
  private volatile long   lastPartialCustomizationMicros;
  private volatile int    lastPartialArcs;
  private volatile long   lastCatchUpMicros;
  private final LongAdder queries                = new LongAdder();
  private final LongAdder fallbacks              = new LongAdder();
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

    /** The same preparation, no longer tied to (and no longer keeping alive) the snapshot it was made for. */
    Prepared detached() {
      return new Prepared(null, root, denseToNode, nodeToDense, directed, undirected);
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
    // a waiter in awaitReady() re-reads the snapshot it is waiting for right away instead of at its next slice
    synchronized (readyMonitor) {
      readyMonitor.notifyAll();
    }
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
    } catch (final OutOfMemoryError e) {
      // Caught on purpose, like the CSR and order persistence do: the arc budget bounds the supergraph, but a large one
      // can still exhaust a tight heap, and the view itself must stay usable. The topology goes too, at the price of
      // ordering again: the heap needs it back more.
      root = null;
      failed(e);
    } catch (final Exception e) {
      failed(e);
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

  /**
   * A preparation that failed: what was prepared is let go so the collector can take it back, and queries answer through
   * Dijkstra meanwhile. Every later snapshot retries, so a cause that keeps repeating is logged once at WARNING, then at
   * FINE.
   */
  private void failed(final Throwable e) {
    prepared = null;
    final String reason = e.toString();
    final boolean repeated = reason.equals(statusReason) && status == Status.UNAVAILABLE;
    statusReason = reason;
    status = Status.UNAVAILABLE;
    LogManager.instance().log(this, repeated ? Level.FINE : Level.WARNING,
        "Contraction hierarchy on '%s' of view '%s' could not be prepared", e, weightProperty, view.getName());
  }

  private boolean hasRequestedModes(final Prepared p) {
    final int modes = requestedModes.get();
    return ((modes & MODE_DIRECTED) == 0 || p.directed != null) && ((modes & MODE_UNDIRECTED) == 0 || p.undirected != null);
  }

  private void prepare(final GraphAnalyticalView.Snapshot snap) {
    lastAttempt = snap;
    // A commit on the base the last preparation already covered changed a few pairs of vertices: redo those, not the graph
    final Prepared previous = prepared;
    if (previous != null && previous.snapshot != null && previous.root == root && previous.snapshot.nodeMapping == snap.nodeMapping
        && hasRequestedModes(previous) && catchUp(previous, snap))
      return;

    final Input input = view.extractWeightedArcs(snap, edgeTypes, weightProperty, this::isClosed);
    if (closed)
      return;
    if (input == null) {
      // what was prepared before answers for a snapshot the view no longer serves: holding the snapshot would pin its
      // CSR until this hierarchy can be prepared again, which may be never. The topology and the metrics stay, so the
      // next preparation - typically right after the rebuild the view is running - is a partial customization again
      prepared = previous == null ? null : previous.detached();
      status = Status.UNAVAILABLE;
      statusReason = "the view cannot serve '" + weightProperty + "' exactly for its current snapshot";
      return;
    }

    TopologyRoot topologyRoot = root;
    int[] denseToNode = null;
    double[][] directedInput = null;
    double[][] undirectedInput = null;
    final int modes = requestedModes.get();

    // the order of the topology the change made unusable: still a valid order, see keptOrder()
    int[] keptOrder = null;
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
        keptOrder = keptOrder(topologyRoot, denseToNode, input.nodeCount());
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
      final int[] persistedOrder = keptOrder == null && root == null && snap.restoredFromDisk && snap.overlay == null
          && view.getName() != null ? CCHOrderPersistence.load(view.getDatabase(), orderFile(), snap.asOfTransactionId) : null;
      final int[] order = keptOrder != null ? keptOrder : persistedOrder;
      final boolean reusesOrder = order != null && order.length == input.nodeCount();
      CCHTopology topology = CCHTopology.build(input.nodeCount(), input.tails(), input.heads(), input.count(), budget,
          reusesOrder ? order : null, this::isClosed);
      final boolean builtFromOrder = reusesOrder && topology != null;
      // the kept order may have degraded past the budget under the changes it absorbed: a fresh one may not
      if (topology == null && keptOrder != null && !closed)
        topology = CCHTopology.build(input.nodeCount(), input.tails(), input.heads(), input.count(), budget, null,
            this::isClosed);
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
      if (builtFromOrder && keptOrder != null)
        topologyRecontractions.increment();
      else if (builtFromOrder)
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

  /**
   * The previous metric, brought up to these input costs by a partial customization of the arcs that differ (none: as
   * it is), or a fresh full customization when there is none for this topology or too much has changed for a partial
   * one to be cheaper. Comparing the costs walks every arc; this is the path of a full re-read of the view (a new base
   * after a compaction or a rebuild), which costs that much already - commits on the same base take catchUp() instead.
   */
  private CCHMetric metric(final Prepared previous, final TopologyRoot topologyRoot, final double[][] input,
      final boolean undirected) {
    if (previous != null && previous.root == topologyRoot) {
      final CCHMetric old = undirected ? previous.undirected : previous.directed;
      // a metric whose update failed half way has inputs that may already match these: only a full customization is exact
      if (old != null && !old.isBroken()) {
        final int arcs = topologyRoot.topology.arcCount();
        final int limit = partialLimit(arcs);
        int[] changed = new int[16];
        int count = 0;
        for (int a = 0; a < arcs && count <= limit; a++)
          if (Double.compare(input[0][a], old.inputUp[a]) != 0
              || (!undirected && Double.compare(input[1][a], old.inputDown[a]) != 0)) {
            if (count == changed.length)
              changed = Arrays.copyOf(changed, count * 2);
            changed[count++] = a;
          }
        if (count == 0) {
          customizationsSaved.increment();
          return old;
        }
        if (count <= limit) {
          final double[] ups = new double[count];
          final double[] downs = new double[count];
          for (int i = 0; i < count; i++) {
            ups[i] = input[0][changed[i]];
            if (!undirected)
              downs[i] = input[1][changed[i]];
          }
          partialUpdate(old, changed, ups, downs, count);
          return old;
        }
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
   * Brings the previous preparation up to {@code snap}, which sits on the same base CSR, by re-pricing only the pairs
   * of vertices whose edges the overlay difference between the two snapshots names, then partially customizing the arcs
   * that changed. That is what keeps a weight update, a closed road or a removed edge in the milliseconds.
   * <p>
   * The metrics are updated in place, so they belong to the hierarchy rather than to one snapshot: a query that started
   * on the previous preparation and overlaps the update reads either the costs before it or the costs after it, never a
   * mix (see {@link CCHMetric#update}), and from the moment the view publishes the new snapshot until this preparation
   * is published for it, queries are answered by Dijkstra as with any other preparation.
   *
   * @return false when the change cannot be handled this way - a vertex deleted, an edge between two vertices the
   * topology does not join, an exact answer refused - and the whole graph has to be read again
   */
  private boolean catchUp(final Prepared previous, final GraphAnalyticalView.Snapshot snap) {
    final long begin = System.nanoTime();
    final LongPairs pairs = new LongPairs();
    if (!DeltaOverlay.changedPairs(previous.snapshot.overlay, snap.overlay, pairs::add))
      return false;

    final CCHTopology topology = previous.root.topology;
    // more changed pairs than a partial customization would take arcs: the full path decides, before allocating for them
    if (pairs.size > partialLimit(topology.arcCount()))
      return false;
    final int[] rankOf = topology.rankOf;
    final int[] arcs = new int[pairs.size];
    final double[] directedUp = new double[pairs.size];
    final double[] directedDown = new double[pairs.size];
    final double[] undirectedCost = new double[pairs.size];
    int count = 0;
    final IntIntHashMap seen = new IntIntHashMap(Math.max(16, pairs.size * 2));
    for (int i = 0; i < pairs.size; i++) {
      final int u = DeltaOverlay.pairSource(pairs.items[i]);
      final int v = DeltaOverlay.pairTarget(pairs.items[i]);
      if (u == v)
        continue;
      final double uv = pairCost(snap, u, v);
      final double vu = pairCost(snap, v, u);
      if (Double.isNaN(uv) || Double.isNaN(vu))
        return false; // the view could not answer exactly for one of them
      final int nu = u < previous.denseToNode.length ? previous.denseToNode[u] : -1;
      final int nv = v < previous.denseToNode.length ? previous.denseToNode[v] : -1;
      final boolean walkable = uv < CCHMetric.INFINITY || vu < CCHMetric.INFINITY;
      if (nu < 0 || nv < 0) {
        if (walkable)
          return false; // a vertex the topology does not know now has edges
        continue;
      }
      final int ru = rankOf[nu];
      final int rv = rankOf[nv];
      final int arc = ru < rv ? topology.findArc(ru, rv) : topology.findArc(rv, ru);
      if (arc < 0) {
        if (walkable)
          return false; // a new connection the supergraph does not have: the topology must be rebuilt
        continue;
      }
      if (seen.put(arc, 1) != Integer.MIN_VALUE)
        continue;
      arcs[count] = arc;
      directedUp[count] = ru < rv ? uv : vu;
      directedDown[count] = ru < rv ? vu : uv;
      undirectedCost[count] = Math.min(uv, vu);
      count++;
    }

    // past the point where a full customization is cheaper, let the full path decide: it diffs every arc
    if (count > partialLimit(topology.arcCount()))
      return false;
    if (count > 0) {
      try {
        if (previous.directed != null)
          partialUpdate(previous.directed, arcs, directedUp, directedDown, count);
        if (previous.undirected != null)
          partialUpdate(previous.undirected, arcs, undirectedCost, undirectedCost, count);
      } catch (final RuntimeException e) {
        // The failed metric is marked broken and the other may be updated already, so neither may answer for the
        // previous snapshot any more: the preparation is detached from it, and the full path customizes the broken one
        // afresh and brings the other up to date by diffing it
        prepared = previous.detached();
        LogManager.instance().log(this, Level.WARNING, "Incremental update of contraction hierarchy on '%s' failed, "
            + "re-reading the view", e, weightProperty);
        return false;
      }
    } else
      customizationsSaved.increment();

    prepared = new Prepared(snap, previous.root, previous.denseToNode, previous.nodeToDense, previous.directed,
        previous.undirected);
    lastCatchUpMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin);
    statusReason = null;
    status = Status.READY;
    return true;
  }

  /**
   * The cheapest walkable edge from dense vertex {@code u} to {@code v} over the routed types in {@code snap}: infinite
   * when there is none, NaN when the view cannot price it exactly.
   */
  private double pairCost(final GraphAnalyticalView.Snapshot snap, final int u, final int v) {
    if (!view.isNodeLive(snap, u) || !view.isNodeLive(snap, v))
      return CCHMetric.INFINITY;
    final NodeEdgeWeights edges = view.edgeWeightsOf(snap, u, Vertex.DIRECTION.OUT, weightProperty, EdgeWeight.MISSING,
        edgeTypes);
    if (edges == null)
      return Double.NaN;
    double best = CCHMetric.INFINITY;
    final int[] neighbors = edges.neighbors();
    final double[] weights = edges.weights();
    for (int i = 0; i < neighbors.length; i++)
      if (neighbors[i] == v && EdgeWeight.isWalkable(weights[i]) && weights[i] < best)
        best = weights[i];
    return best;
  }

  /** How many changed arcs a partial customization handles before a full one becomes cheaper. */
  private static int partialLimit(final int arcs) {
    return Math.min(arcs, PARTIAL_LIMIT_MIN + arcs / PARTIAL_LIMIT_DIVISOR);
  }

  private void partialUpdate(final CCHMetric metric, final int[] arcs, final double[] ups, final double[] downs,
      final int count) {
    final long begin = System.nanoTime();
    lastPartialArcs = metric.update(arcs, ups, downs, count);
    lastPartialCustomizationMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - begin);
    partialCustomizations.increment();
  }

  /** A growable list of packed pairs. */
  private static final class LongPairs {
    long[] items = new long[16];
    int    size;

    void add(final long pair) {
      if (size == items.length)
        items = Arrays.copyOf(items, size * 2);
      items[size++] = pair;
    }
  }

  /**
   * The order of a topology a structural change (an edge between two vertices it does not join, a new vertex with edges)
   * made unusable, carried over to the dense ids of the new snapshot: vertices it did not know are ranked first, the rest
   * keep their relative order. Any order is a valid elimination order - the new edges only add some fill to the
   * contraction - so a structural change costs a contraction and a customization instead of a new nested dissection,
   * which on a road network is most of the build.
   */
  private static int[] keptOrder(final TopologyRoot topologyRoot, final int[] denseToNode, final int nodeCount) {
    final CCHTopology topology = topologyRoot.topology;
    final int[] nodeToDense = new int[topology.nodeCount];
    Arrays.fill(nodeToDense, -1);
    for (int d = 0; d < denseToNode.length; d++)
      if (denseToNode[d] >= 0)
        nodeToDense[denseToNode[d]] = d;
    final int[] order = new int[nodeCount];
    int r = 0;
    for (int d = 0; d < nodeCount; d++)
      if (d >= denseToNode.length || denseToNode[d] < 0)
        order[r++] = d;
    for (int rank = 0; rank < topology.nodeCount; rank++) {
      final int d = nodeToDense[topology.nodeAt[rank]];
      if (d >= 0 && d < nodeCount)
        order[r++] = d;
    }
    return r == nodeCount ? order : null;
  }

  /**
   * The topology node of each dense id of {@code snap}, -1 for one it does not know. A snapshot over the base the
   * topology was built from shares its ids; any other base renumbered them, and is mapped by RID.
   */
  private int[] mapOnto(final TopologyRoot topologyRoot, final GraphAnalyticalView.Snapshot snap, final int nodeCount) {
    final int known = topologyRoot.topology.nodeCount;
    // Same base mapping: the overlay that sits on it now grew out of the one the topology was built with, and an overlay
    // keeps every id it ever handed out (a deleted vertex's slot is never reused), so an id names the same vertex in both.
    // A vertex deleted since keeps its node and simply has no arcs left; no liveness check is needed, unlike below.
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

  /** Topologies re-contracted in the order of the one a structural change made unusable, instead of a new order. */
  public long getTopologyRecontractionCount() {
    return topologyRecontractions.sum();
  }

  /** Topologies contracted in an order persisted at the previous close, instead of a freshly computed one. */
  public long getTopologyRestoreCount() {
    return topologyRestores.sum();
  }

  /** Customizations that recomputed only the arcs a change could reach. */
  public long getPartialCustomizationCount() {
    return partialCustomizations.sum();
  }

  /**
   * Microseconds the last incremental update took, from reading what changed between two snapshots to the partially
   * customized metric: the latency of a weight change, a closed road or a removed edge.
   */
  public long getLastIncrementalUpdateMicros() {
    return lastCatchUpMicros;
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
    stats.put("topologyRecontractions", topologyRecontractions.sum());
    stats.put("lastTopologyBuildMs", lastTopologyBuildMs);
    stats.put("customizations", customizations.sum());
    stats.put("customizationsReused", customizationsSaved.sum());
    stats.put("lastCustomizationMs", lastCustomizationMs);
    stats.put("partialCustomizations", partialCustomizations.sum());
    stats.put("lastPartialCustomizationMicros", lastPartialCustomizationMicros);
    stats.put("lastPartialCustomizationArcs", lastPartialArcs);
    stats.put("lastIncrementalUpdateMicros", lastCatchUpMicros);
    stats.put("queries", queries.sum());
    stats.put("fallbacks", fallbacks.sum());
    return stats;
  }
}
