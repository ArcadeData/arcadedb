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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.NumberUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for algorithm procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractAlgoProcedure implements CypherProcedure {

  /**
   * Hard upper bound for every embedding-dimension-shaped parameter (`embeddingDimension`,
   * `dimensions`, ...). These size a per-node {@code double[]} row, so the allocation grows as
   * {@code nodeCount * dimension} with no graph-derived ceiling to clamp against - a single
   * in-range-but-huge value is an allocation-DoS on a graph of any size.
   * <p>
   * 4096 sits comfortably above every dimension in practical use (the widest mainstream text
   * embeddings are 3072-wide) while capping one embedding row at 32 KB.
   * </p>
   */
  public static final int MAX_EMBEDDING_DIMENSION = 4096;

  // ── Embedding math utilities ─────────────────────────────────────────────

  /** Normalises {@code vec} to unit L2 length in-place; no-op if the vector is zero. */
  protected static void normalizeL2(final double[] vec) {
    double norm = 0.0;
    for (final double v : vec)
      norm += v * v;
    if (norm == 0.0)
      return;
    norm = Math.sqrt(norm);
    for (int i = 0; i < vec.length; i++)
      vec[i] /= norm;
  }

  /** Returns the dot product of two equal-length vectors. */
  protected static double dot(final double[] a, final double[] b) {
    double s = 0.0;
    for (int i = 0; i < a.length; i++)
      s += a[i] * b[i];
    return s;
  }

  /** Logistic sigmoid: σ(x) = 1 / (1 + e^{-x}), clamped to avoid overflow. */
  protected static double sigmoid(final double x) {
    return 1.0 / (1.0 + Math.exp(-x));
  }

  /** Converts a {@code double[]} to an unmodifiable {@code List<Double>} for Cypher return. */
  protected static List<Double> toEmbeddingList(final double[] vec) {
    final List<Double> list = new ArrayList<>(vec.length);
    for (final double v : vec)
      list.add(v);
    return list;
  }

  // ── Argument extractors ──────────────────────────────────────────────────

  /**
   * Extracts a list of vertices from an argument that may be a {@code List<Vertex>},
   * a single {@code Vertex}, or similar.
   */
  @SuppressWarnings("unchecked")
  protected List<Vertex> extractVertexList(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    if (arg instanceof List<?> list) {
      final List<Vertex> result = new ArrayList<>(list.size());
      for (final Object item : list)
        result.add(extractVertex(item, paramName + "[*]"));
      return result;
    }
    if (arg instanceof Vertex v)
      return List.of(v);
    throw new IllegalArgumentException(getName() + "(): " + paramName + " must be a list of nodes");
  }

  protected Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    if (arg instanceof Vertex)
      return (Vertex) arg;
    if (arg instanceof Document && arg instanceof Vertex)
      return (Vertex) arg;
    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
  }

  protected String extractString(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    if (arg == null)
      return null;
    if (arg instanceof String s)
      return splitRelTypeString(s);
    if (arg instanceof Collection<?> coll)
      return coll.stream().map(Object::toString).toArray(String[]::new);
    return new String[]{arg.toString()};
  }

  private static String[] splitRelTypeString(final String s) {
    final String trimmedSource = s.trim();
    if (trimmedSource.isEmpty())
      return null;
    if (!trimmedSource.contains(",") && !trimmedSource.contains("|"))
      return new String[]{trimmedSource};
    final List<String> types = new ArrayList<>();
    for (final String part : trimmedSource.split("[,|]")) {
      final String trimmed = part.trim();
      if (!trimmed.isEmpty())
        types.add(trimmed);
    }
    return types.isEmpty() ? null : types.toArray(new String[0]);
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    else if (arg instanceof Map)
      return (Map<String, Object>) arg;

    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
  }

  /**
   * Narrows a config/argument {@link Number} to {@code int}, failing loudly instead of silently
   * saturating or wrapping when the value is out of range. Use for algorithm tuning knobs (iteration
   * counts, embedding dimensions, cluster/degree thresholds, ...) where an out-of-range value should
   * be rejected rather than reinterpreted - unlike a result-count bound (e.g. top-k, maxDepth), there
   * is no sensible "as many as possible" reading for these.
   */
  protected int extractInt(final Number value, final String paramName) {
    try {
      return NumberUtils.toIntExact(value);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(getName() + "(): " + paramName + " is out of range for an int: " + value, e);
    }
  }

  /**
   * Same as {@link #extractInt(Number, String)} plus an inclusive lower bound, rejected by name.
   * <p>
   * Use for the knobs that multiply the amount of work an algorithm performs - iteration counts, restarts,
   * simulation counts, walks per node, context window widths. Below its minimum such a knob does not mean
   * "a smaller run", it means an algorithm that cannot produce the result it was asked for, and without this
   * check the value is either silently absorbed (a zero restart count returns the untouched random assignment,
   * a zero epoch count returns untrained embeddings, a zero simulation count divides by zero and yields NaN) or
   * surfaces as a bare {@code NegativeArraySizeException} from the allocator that never names the parameter.
   *
   * @param minimum smallest accepted value, inclusive
   */
  protected int extractInt(final Number value, final String paramName, final int minimum) {
    final int extracted = extractInt(value, paramName);
    if (extracted < minimum)
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be at least " + minimum + ", got " + extracted);
    return extracted;
  }

  /**
   * Narrows an embedding-dimension-shaped config value to {@code int} and bounds it to
   * {@code [1, MAX_EMBEDDING_DIMENSION]}.
   * <p>
   * Unlike a top-k / result-count bound, which clamps against the graph's node count
   * ({@code Math.min(k, n)}) because "as many as exist" is the natural reading, an embedding
   * dimension has no graph-derived bound to clamp against: it multiplies the per-node allocation
   * regardless of how small the graph is. A large but perfectly in-range value such as
   * {@code {embeddingDimension: 1000000000}} therefore survives {@link #extractInt} and then asks
   * for a {@code new double[n][1000000000]} (~8 GB even at {@code n == 1}).
   * </p>
   * <p>
   * The value is rejected rather than silently clamped: there is no "correct" dimension to fall back
   * to, and quietly returning embeddings of a different width than the caller asked for would be a
   * worse failure than a clear error.
   * </p>
   */
  protected int extractEmbeddingDimension(final Number value, final String paramName) {
    final int dimension = extractInt(value, paramName);
    if (dimension < 1)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " must be at least 1, got " + dimension);
    if (dimension > MAX_EMBEDDING_DIMENSION)
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must not exceed " + MAX_EMBEDDING_DIMENSION + ", got " + dimension);
    return dimension;
  }

  /**
   * Narrows a result-count bound (top-k, number of paths, ...) to {@code int}, saturating at the int
   * bounds rather than wrapping, and rejects a negative value with a message naming the parameter.
   * <p>
   * Saturation is deliberate here - "more results than can possibly exist" has the sensible reading
   * "as many as exist", and each caller clamps against the graph afterwards. A negative count has no
   * such reading, and without this check it reaches an array/collection allocation and surfaces as a
   * bare {@code NegativeArraySizeException} or {@code IllegalArgumentException: Illegal Capacity},
   * neither of which names the offending parameter.
   * </p>
   */
  protected int extractCount(final Number value, final String paramName) {
    final int count = NumberUtils.saturateToInt(value);
    if (count < 0)
      throw new IllegalArgumentException(getName() + "(): " + paramName + " must not be negative, got " + count);
    return count;
  }

  // ── Work bounds ──────────────────────────────────────────────────────────

  /**
   * Heap cost of one row of a per-node {@code int} matrix on top of its payload: 16-byte array header, 4-byte
   * length, 4 bytes of padding and the 8-byte reference the enclosing array holds. Such rows are typically short,
   * so this overhead is a real part of the footprint rather than a rounding error.
   * <p>
   * A heuristic for a budget check, not a guarantee: the true figure moves with the JVM's object layout
   * (compressed oops on or off, alignment). It does not need to be exact - it only has to keep the estimate in
   * the right order of magnitude - so there is nothing to "correct" here short of measuring a specific JVM.
   */
  protected static final long WALK_ROW_OVERHEAD_BYTES = 32L;

  /** Heap cost of one entry of a walk buffer, which is an {@code int}. */
  protected static final long WALK_ENTRY_BYTES = 4L;

  /**
   * Rejects, before a single byte is allocated, a random-walk buffer whose estimated heap footprint exceeds
   * {@link GlobalConfiguration#CYPHER_ALGO_MAX_WALK_MEMORY}.
   *
   * @param db             database whose configuration carries the budget
   * @param estimatedBytes estimated footprint, computed with {@link #saturatingProduct(long, long)}
   * @param detail         breakdown of the knobs that produced the estimate, for the error message
   *
   * @see #checkBufferBudget(Database, long, String, String)
   */
  protected void checkWalkBudget(final Database db, final long estimatedBytes, final String detail) {
    checkBufferBudget(db, estimatedBytes, "random walk buffer", detail);
  }

  /**
   * Rejects, before a single byte is allocated, a per-node buffer whose estimated heap footprint exceeds
   * {@link GlobalConfiguration#CYPHER_ALGO_MAX_WALK_MEMORY}.
   * <p>
   * The knobs that size these buffers ({@code walksPerNode}, {@code walkLength}, {@code steps}, SLPA's
   * {@code iterations}) have no graph-derived ceiling to clamp against, so they are bounded here by the resource
   * they actually consume rather than by a guessed per-knob maximum: the budget scales with the JVM heap and is
   * tunable, and the estimate is computed in saturating {@code long} arithmetic, so a product that would wrap
   * {@code int} is caught here instead of surfacing as a {@code NegativeArraySizeException} from inside the
   * allocator.
   *
   * @param db             database whose configuration carries the budget
   * @param estimatedBytes estimated footprint, computed with {@link #saturatingProduct(long, long)}
   * @param what           name of the buffer, for the error message ("random walk buffer", ...)
   * @param detail         breakdown of the knobs that produced the estimate, for the error message
   */
  protected void checkBufferBudget(final Database db, final long estimatedBytes, final String what,
      final String detail) {
    final long budget = db.getConfiguration().getValueAsLong(GlobalConfiguration.CYPHER_ALGO_MAX_WALK_MEMORY);
    if (budget < 0 || estimatedBytes <= budget)
      return;
    throw new IllegalArgumentException(getName() + "(): the " + what + " would need "
        + (estimatedBytes == Long.MAX_VALUE ? "over " + Long.MAX_VALUE : Long.toString(estimatedBytes)) + " bytes ("
        + detail + "), more than the " + budget + " bytes allowed. Set "
        + GlobalConfiguration.CYPHER_ALGO_MAX_WALK_MEMORY.getKey() + " to raise the limit");
  }

  /** Multiplies two non-negative longs, saturating at {@link Long#MAX_VALUE} instead of wrapping. */
  protected static long saturatingProduct(final long a, final long b) {
    try {
      return Math.multiplyExact(a, b);
    } catch (final ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Adds two non-negative longs, saturating at {@link Long#MAX_VALUE} instead of wrapping.
   * <p>
   * The companion to {@link #saturatingProduct(long, long)}, and required wherever a footprint estimate mixes the
   * two: a saturated product plus a per-row overhead wraps to a large <em>negative</em> number, and a negative
   * estimate passes {@link #checkBufferBudget} unconditionally - the budget check would be silently disabled by
   * exactly the input it exists to refuse. No current caller can reach that (every estimate here is bounded by an
   * {@code int}-sized count), so this closes the shape rather than an instance.
   * </p>
   */
  protected static long saturatingSum(final long a, final long b) {
    try {
      return Math.addExact(a, b);
    } catch (final ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Cooperative abort check for the CPU-bound loops of the algorithm procedures.
   * <p>
   * The knobs that drive those loops ({@code iterations}, {@code restarts}, {@code simulations},
   * {@code walksPerNode}, ...) multiply time rather than a single allocation, and for time there is no honest
   * ceiling to pick: how long a run may legitimately take is a property of the graph, the hardware and the
   * caller's patience, not of the parameter. So a large value is not forbidden, it is made abortable - by
   * interrupting the query thread, and by the {@code arcadedb.command.timeout} deadline, which until now only
   * the SQL SELECT planner honoured.
   * <p>
   * The interrupt flag is CLEARED rather than restored, matching {@code ShortestPathStep} and
   * {@code SQLFunctionShortestPath}: the exception aborts the whole call, and leaving the flag set would poison
   * the next task to run on a pooled query thread. The clock is read only when a deadline is actually
   * configured, so the default (timeout disabled) costs one flag test per iteration and no syscall.
   */
  protected final class WorkGuard {
    /**
     * Iterations between two checks in {@link #checkPeriodically(int)}. One less than a power of two so the
     * throttle is a single AND, and small enough that the work between two checks stays well under a
     * millisecond for any realistic inner-loop body.
     */
    private static final int CHECK_INTERVAL_MASK = 1023;

    private final long timeoutMillis;
    private final long deadline;

    private WorkGuard(final long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      this.deadline = timeoutMillis > 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;
    }

    /**
     * Aborts the call if the query thread was interrupted or the command deadline has passed. Call from a
     * loop whose single iteration already costs enough to swallow a flag test.
     */
    public void check() {
      if (Thread.interrupted())
        throw new CommandExecutionException(getName() + "() has been interrupted");
      if (deadline < Long.MAX_VALUE && System.currentTimeMillis() > deadline)
        throw new TimeoutException(getName() + "() exceeded the " + GlobalConfiguration.COMMAND_TIMEOUT.getKey()
            + " of " + timeoutMillis + "ms");
    }

    /**
     * {@link #check()} throttled for a hot inner loop whose single iteration is too small to justify a flag
     * test of its own. Checking only at the enclosing loop leaves abort latency proportional to the inner
     * loop's length, which is exactly what these knobs make unbounded - node2vec's context window may span a
     * whole walk, so one walk alone is O(walkLength&sup2;). Testing every 1024 iterations instead bounds the
     * latency by a fixed amount of work at the cost of one AND and one branch per iteration.
     * <p>
     * "Every 1024" describes a counter that keeps climbing. The counter belongs to the caller, so a loop whose
     * counter <em>restarts</em> - the negative-sampling loop runs {@code ns} from 0 again for every
     * (position, context) pair - also tests on its first iteration every time round. That is deliberate rather
     * than a redundancy to remove: it costs one flag test per restart, and it is what keeps a small
     * {@code negSamples} responsive, since the enclosing context checkpoint fires only about once every 1024
     * positions when the window is narrow.
     *
     * @param iterationCounter the caller's loop counter; its absolute value does not matter, only that it
     *                         advances by one per iteration
     */
    public void checkPeriodically(final int iterationCounter) {
      if ((iterationCounter & CHECK_INTERVAL_MASK) == 0)
        check();
    }
  }

  /**
   * Creates a {@link WorkGuard} whose deadline starts now and honours
   * {@link GlobalConfiguration#COMMAND_TIMEOUT}.
   */
  protected WorkGuard newWorkGuard(final CommandContext context) {
    return new WorkGuard(context.getDatabase().getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT));
  }

  /** @see GraphEngine#getAllVertices(Database, String[]) */
  protected Iterator<Vertex> getAllVertices(final Database db, final String[] nodeLabels) {
    return GraphEngine.getAllVertices(db, nodeLabels);
  }

  /** @see GraphEngine#buildRidIndex(List) */
  protected Map<RID, Integer> buildRidIndex(final List<Vertex> vertices) {
    return GraphEngine.buildRidIndex(vertices);
  }

  /** @see GraphEngine#neighborRid(Edge, RID, Vertex.DIRECTION) */
  protected RID neighborRid(final Edge edge, final RID sourceRid, final Vertex.DIRECTION dir) {
    return GraphEngine.neighborRid(edge, sourceRid, dir);
  }

  /** @see GraphEngine#parseDirection(String) */
  protected Vertex.DIRECTION parseDirection(final String dir) {
    return GraphEngine.parseDirection(dir);
  }

  /** @see GraphEngine#buildAdjacencyList(List, Map, Vertex.DIRECTION, String[]) */
  protected int[][] buildAdjacencyList(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx,
      final Vertex.DIRECTION dir, final String[] relTypes) {
    return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes);
  }

  /**
   * Finds a {@link GraphTraversalProvider} suitable for graph algorithms.
   * When {@code relTypes} is null (whole-graph algorithms like PageRank, WCC, LCC), accepts any
   * ready provider even if it covers only specific types — the algorithm will use whatever the
   * CSR contains, which is the desired behavior for whole-graph analytics.
   *
   * @param db       the database
   * @param relTypes edge types to filter by (null = any provider)
   */
  protected GraphTraversalProvider findProvider(final Database db, final String[] relTypes) {
    // Try exact match first (covers all requested types)
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, relTypes);
    if (provider != null && provider.coversVertexType(null))
      return provider;

    // For whole-graph algorithms (null/empty relTypes), accept any ready provider that covers
    // all edge types. A partial-coverage provider would silently produce wrong results.
    if (relTypes == null || relTypes.length == 0) {
      for (final GraphTraversalProvider p : GraphTraversalProviderRegistry.getProviders(db))
        if (p.isReady() && p.coversEdgeType(null))
          return p;
    }
    return null;
  }

  /**
   * Builds an adjacency list (int[][]) from a {@link GraphTraversalProvider}'s CSR structure.
   * Each {@code result[i]} contains the dense neighbor IDs for node {@code i} in the given direction.
   */
  protected int[][] buildAdjacencyFromProvider(final GraphTraversalProvider provider, final Vertex.DIRECTION dir,
      final String[] relTypes) {
    final int n = provider.getNodeCount();
    final int[][] adj = new int[n][];
    for (int i = 0; i < n; i++)
      adj[i] = provider.getNeighborIds(i, dir, relTypes);
    return adj;
  }

  /**
   * Loads the graph structure, using CSR-backed adjacency from a {@link GraphTraversalProvider}
   * when available, otherwise falling back to OLTP (vertex/edge iteration).
   * <p>
   * Algorithms replace their manual vertex loading + {@code buildAdjacencyList()} calls with:
   * <pre>
   *   final GraphData graph = loadGraph(db, null, relTypes);
   *   final int[][] adj = graph.adjacency(Vertex.DIRECTION.OUT);
   *   // ... algorithm using adj[i] ...
   *   result.setProperty("node", graph.getVertex(i));
   * </pre>
   *
   * @param db         the database
   * @param nodeLabels vertex type filter (null = all types)
   * @param relTypes   edge type filter (null = all types)
   */
  protected GraphData loadGraph(final Database db, final String[] nodeLabels, final String[] relTypes) {
    return loadGraph(db, nodeLabels, relTypes, null);
  }

  protected GraphData loadGraph(final Database db, final String[] nodeLabels, final String[] relTypes,
      final CommandContext context) {
    if (nodeLabels == null || nodeLabels.length == 0) {
      final GraphTraversalProvider provider = findProvider(db, relTypes);
      if (provider != null) {
        if (context != null)
          context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        return new GraphData(provider, provider.getNodeCount());
      }
    }
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, nodeLabels);
    while (iter.hasNext())
      vertices.add(iter.next());
    return new GraphData(vertices, buildRidIndex(vertices));
  }

  /**
   * Encapsulates graph data that can be backed by either a CSR provider or OLTP vertex lists.
   * Provides uniform access to adjacency, vertex lookup, and RID resolution regardless of backing.
   */
  protected static class GraphData {
    private static final int[] EMPTY_NEIGHBORS = new int[0];

    public final int                     nodeCount;
    private final GraphTraversalProvider provider;
    private final List<Vertex>           vertices;
    private final Map<RID, Integer>      ridToIdx;

    private GraphData(final GraphTraversalProvider provider, final int nodeCount) {
      this.provider = provider;
      this.vertices = null;
      this.ridToIdx = null;
      this.nodeCount = nodeCount;
    }

    private GraphData(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx) {
      this.provider = null;
      this.vertices = vertices;
      this.ridToIdx = ridToIdx;
      this.nodeCount = vertices.size();
    }

    public int[][] adjacency(final Vertex.DIRECTION dir, final String... relTypes) {
      if (provider != null) {
        // Try zero-allocation NeighborView first
        final NeighborView nv = provider.getNeighborView(dir, relTypes);
        if (nv != null) {
          final int[][] adj = new int[nodeCount][];
          final int[] nbrs = nv.neighbors();
          for (int i = 0; i < nodeCount; i++) {
            final int start = nv.offset(i);
            final int end = nv.offsetEnd(i);
            adj[i] = start == end ? EMPTY_NEIGHBORS : Arrays.copyOfRange(nbrs, start, end);
          }
          return adj;
        }
        final int[][] adj = new int[nodeCount][];
        for (int i = 0; i < nodeCount; i++)
          adj[i] = provider.getNeighborIds(i, dir, relTypes);
        return adj;
      }
      return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes);
    }

    /**
     * Returns a zero-allocation {@link NeighborView} for offset-based iteration when CSR-backed,
     * or {@code null} if backed by OLTP vertices. Algorithms that iterate all neighbors should
     * prefer this over {@link #adjacency} to avoid O(N) array allocations.
     */
    public NeighborView neighborView(final Vertex.DIRECTION dir, final String... relTypes) {
      return provider != null ? provider.getNeighborView(dir, relTypes) : null;
    }

    public Vertex getVertex(final int i) {
      if (provider != null) {
        final RID rid = provider.getRID(i);
        if (rid == null)
          return null;
        try {
          return rid.asVertex();
        } catch (final RecordNotFoundException e) {
          // vertex deleted in OLTP since CSR was built — skip
          return null;
        }
      }
      return vertices.get(i);
    }

    public RID getRID(final int i) {
      if (provider != null) {
        final RID rid = provider.getRID(i);
        return rid; // may be null for overflow nodes
      }
      return vertices.get(i).getIdentity();
    }

    public int indexOf(final RID rid) {
      if (provider != null)
        return provider.getNodeId(rid);
      final Integer idx = ridToIdx.get(rid);
      return idx != null ? idx : -1;
    }

    public boolean isCSRBacked() {
      return provider != null;
    }

    /**
     * Returns true if this graph has edge properties available for the given weight property.
     */
    public boolean hasEdgeProperties() {
      return provider != null && provider.hasEdgeProperties();
    }

    /**
     * Builds weighted adjacency from CSR edge properties when available. Returns edge weights
     * aligned with the adjacency array returned by {@link #adjacency(Vertex.DIRECTION, String...)}.
     * <p>
     * Returns {@code null} if edge properties are not available in CSR (caller should extract
     * weights from OLTP edges in that case).
     * <p>
     * For each node {@code i}, {@code result[i][j]} is the weight of the edge to {@code adj[i][j]}.
     *
     * @param dir            traversal direction
     * @param weightProperty edge property name for weights
     * @param relTypes       edge types to filter
     * @return double[][] of weights aligned with adjacency, or null if not available
     */
    public double[][] edgeWeights(final Vertex.DIRECTION dir, final String weightProperty,
        final String... relTypes) {
      if (provider == null || !provider.hasEdgeProperties())
        return null;

      final double[][] weights = new double[nodeCount][];
      for (int i = 0; i < nodeCount; i++) {
        final String[] types = relTypes != null && relTypes.length > 0 ? relTypes : null;
        if (types != null && types.length == 1) {
          // Single edge type: direct per-neighbor weight extraction
          final int[] neighbors = provider.getNeighborIds(i, dir, types[0]);
          weights[i] = new double[neighbors.length];
          for (int j = 0; j < neighbors.length; j++) {
            final Object w = provider.getEdgeProperty(i, j, dir, types[0], weightProperty);
            weights[i][j] = w instanceof Number num ? num.doubleValue() : 1.0;
          }
        } else {
          // Multiple edge types: concatenate per-type weights to match adjacency order
          final List<Double> wts = new ArrayList<>();
          final String[] allTypes = types != null ? types : getProviderEdgeTypes();
          for (final String edgeType : allTypes) {
            final int[] neighbors = provider.getNeighborIds(i, dir, edgeType);
            for (int j = 0; j < neighbors.length; j++) {
              final Object w = provider.getEdgeProperty(i, j, dir, edgeType, weightProperty);
              wts.add(w instanceof Number num ? num.doubleValue() : 1.0);
            }
          }
          weights[i] = new double[wts.size()];
          for (int j = 0; j < wts.size(); j++)
            weights[i][j] = wts.get(j);
        }
      }
      return weights;
    }

    private String[] getProviderEdgeTypes() {
      // Fallback: return empty array to indicate all types
      return new String[0];
    }
  }

  /**
   * Builds a path representation from a list of RIDs.
   */
  protected Map<String, Object> buildPath(final List<RID> rids, final Database database) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final RID rid : rids) {
      final Document doc = database.lookupByRID(rid, true).asDocument();
      if (doc instanceof Vertex)
        nodes.add(doc);
      else if (doc instanceof Edge)
        relationships.add(doc);
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }
}
