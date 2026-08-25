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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.WorkGuard;
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
    return switch (arg) {
      case null -> throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
      case Vertex vertex -> vertex;
      case Document doc when doc instanceof Vertex v -> v;
      default -> throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
    };
  }

  protected String extractString(final Object arg, final String paramName) {
    if (arg == null)
      return null;
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    return switch (arg) {
      case null -> null;
      case String s -> splitRelTypeString(s);
      case Collection<?> coll -> coll.stream().map(Object::toString).toArray(String[]::new);
      default -> new String[]{arg.toString()};
    };
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
   * Heap cost of one row of a two-dimensional array on top of its payload: 16-byte array header, 4-byte length,
   * 4 bytes of padding and the 8-byte reference the enclosing array holds. Rows are often short - a walk of a
   * few steps, a label memory of a few iterations, an embedding of a few dozen doubles - so this overhead is a
   * real part of the footprint rather than a rounding error.
   * <p>
   * A heuristic for a budget check, not a guarantee: the true figure moves with the JVM's object layout
   * (compressed oops on or off, alignment). It does not need to be exact - it only has to keep the estimate in
   * the right order of magnitude - so there is nothing to "correct" here short of measuring a specific JVM.
   */
  protected static final long MATRIX_ROW_OVERHEAD_BYTES = 32L;

  /**
   * Entry-count checkpoint interval for the CSR-adjacency fallback branch of {@link GraphData#adjacency}, taken
   * when a {@link com.arcadedb.graph.GraphTraversalProvider} has no {@link com.arcadedb.graph.NeighborView} to
   * size the copy from up front.
   * <p>
   * That branch used to checkpoint the budget every 1024 <em>nodes</em>, a count with no relationship to how much
   * heap those nodes actually cost: a single supernode's neighbour list is fully materialised by one
   * {@code getNeighborIds()} call, so a node-count interval left the budget unable to refuse it until up to 1023
   * more (possibly tiny) rows had also been expanded. Checkpointing on entries-seen-so-far as well closes that -
   * the supernode's own row already crosses this threshold, so the very next checkpoint (right after that one
   * row) has a chance to refuse the call, rather than the one up to 1023 nodes later (issue #6444, following up
   * on the #6417 code review that first named this gap).
   * <p>
   * 1 Mi entries is 4 MB of {@code int[]} at {@link #INT_BYTES} each - generous enough that an ordinary,
   * moderate-degree graph still checkpoints on the node-count interval as before, while capping how much of a
   * single oversized row's cost can go unpriced past the previous checkpoint.
   */
  protected static final long ADJACENCY_CHECKPOINT_ENTRIES = 1_048_576L;

  /** Heap cost of one {@code boolean} array element, which the JVM stores as a byte. */
  protected static final long BOOLEAN_BYTES = 1L;

  /** Heap cost of one {@code int} array element, the entry type of the walk buffers and of SLPA's label memory. */
  protected static final long INT_BYTES = 4L;

  /** Heap cost of one {@code double} array element, the entry type of every embedding and distance matrix. */
  protected static final long DOUBLE_BYTES = 8L;

  /** Heap cost of one {@code long} array element, the word type of the {@link java.util.BitSet} neighbour matrices. */
  protected static final long LONG_BYTES = 8L;

  /**
   * Estimated heap one OLTP-loaded vertex costs the call, for the structures {@code loadGraph} builds around it:
   * the {@code List<Vertex>} slot, the {@code HashMap} table slot, the {@code HashMap.Node} and the boxed
   * {@code Integer} index of the RID index, and the loaded record's own object shell.
   * <p>
   * The record's payload is on top of this and is not estimated: it is whatever the user stored. The figure is
   * therefore a floor, which is the useful direction - it is what the call holds per vertex whatever the data
   * looks like, and at ten million vertices it is already most of a gigabyte for the index alone (issue #6317).
   */
  protected static final long OLTP_VERTEX_BYTES = 96L;

  /**
   * Heap cost of a {@link java.util.BitSet} object on top of the {@code long[]} of words it wraps: the object
   * header, the {@code words} reference, the {@code wordsInUse} int, the {@code sizeIsSticky} flag and padding.
   * <p>
   * Same order-of-magnitude heuristic as {@link #MATRIX_ROW_OVERHEAD_BYTES}, and it matters for the same reason:
   * a bit matrix is one BitSet per node, so the wrapper is paid {@code nodeCount} times.
   */
  protected static final long BITSET_OVERHEAD_BYTES = 32L;

  /**
   * Estimated heap footprint of a {@code rows}-long array of {@code new BitSet(bits)}, wrappers and row headers
   * included, in saturating {@code long} arithmetic.
   * <p>
   * {@code new BitSet(bits)} allocates {@code ceil(bits / 64)} longs immediately, whatever the node's real degree
   * turns out to be, so a {@code BitSet[n]} of {@code new BitSet(n)} is a genuine {@code n²/8}-byte structure -
   * the {@code nodeCount x nodeCount} shape {@link GlobalConfiguration#CYPHER_ALGO_MAX_WORKING_MEMORY} names in
   * its own documentation, eight times denser per element than the {@code double[n][n]} matrices and therefore
   * slipping through to a far larger graph before it exhausts the heap (issue #6375).
   *
   * @param rows how many bit sets are allocated, normally the node count
   * @param bits how many bits each of them is sized for, normally the node count
   */
  protected static long bitsetMatrixBytes(final long rows, final long bits) {
    return saturatingSum(matrixBytes(rows, (bits + 63) / 64, LONG_BYTES), saturatingProduct(rows, BITSET_OVERHEAD_BYTES));
  }

  /**
   * Estimated heap footprint of a {@code new T[rows][columns]} of an element type {@code elementBytes} wide,
   * row headers included, in saturating {@code long} arithmetic.
   */
  protected static long matrixBytes(final long rows, final long columns, final long elementBytes) {
    return saturatingProduct(rows, saturatingSum(MATRIX_ROW_OVERHEAD_BYTES, saturatingProduct(columns, elementBytes)));
  }

  /**
   * The heap an algorithm call is allowed to spend on the dense working set it builds beside the graph, as a
   * running total: walk buffers, {@code nodeCount x dimension} embedding matrices, {@code nodeCount x nodeCount}
   * distance/similarity/capacity matrices.
   * <p>
   * These allocations have no graph-derived ceiling to clamp against - an embedding dimension multiplies the
   * per-node cost however small the graph is, and a square matrix grows with the graph however modest the knobs
   * are - so they are bounded by the resource they actually consume rather than by a guessed per-knob maximum:
   * {@link GlobalConfiguration#CYPHER_ALGO_MAX_WORKING_MEMORY} scales with the JVM heap and is tunable, and
   * every estimate is computed in saturating {@code long} arithmetic, so a product that would wrap {@code int}
   * is caught here instead of surfacing as a {@code NegativeArraySizeException} from inside the algorithm.
   * <p>
   * The total accumulates because the budget answers one question - how much heap may this call take? - and a
   * single call routinely holds several of these at once: node2vec keeps its walk matrix alive while training
   * over two embedding matrices, and pricing each separately would let a call exceed the budget by however many
   * components it happens to have. A reservation is never released: the unit being bounded is the call, and
   * every component priced here lives to the end of it.
   */
  protected final class MemoryBudget {
    private final long limit;
    private       long reserved;

    private MemoryBudget(final long limit) {
      this.limit = limit;
    }

    /**
     * Adds {@code estimatedBytes} to this call's working set and rejects the call, before a single byte is
     * allocated, if the running total no longer fits the budget.
     *
     * @param estimatedBytes estimated footprint, computed with {@link #matrixBytes(long, long, long)} or
     *                       {@link #saturatingProduct(long, long)}
     * @param component      what is about to be allocated, e.g. "the embedding matrices"
     * @param detail         breakdown of the knobs and counts that produced the estimate, for the error message
     */
    public void reserve(final long estimatedBytes, final String component, final String detail) {
      if (limit < 0)
        // Negative means no limit: skip the bookkeeping too, so a disabled budget costs nothing.
        return;
      // Committed only once granted. A refused reservation was never granted, so recording it would make
      // `reserved` describe heap nobody is holding - and every later message quoting "the N bytes this call
      // already reserved" would quote a figure that includes an allocation the call was refused. No current
      // caller survives a refusal to observe that, since the exception aborts the call; this keeps the
      // invariant true for the one that eventually does.
      final long total = saturatingSum(reserved, estimatedBytes);
      if (total <= limit) {
        reserved = total;
        return;
      }
      throw new IllegalArgumentException(getName() + "(): " + component + " would need " + describe(estimatedBytes)
          + " bytes (" + detail + ")"
          + (reserved > 0 ? ", on top of the " + reserved + " bytes this call already reserved" : "")
          + ", more than the " + limit + " bytes allowed. Set "
          + GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey() + " to raise the limit");
    }

    /**
     * How many items of {@code bytesPerItem} this call may still reserve, or {@link Long#MAX_VALUE} when the
     * budget is disabled.
     * <p>
     * For a working set whose size is only known by walking the graph - {@code algo.mst}'s edge arrays are sized
     * by a count that takes a full traversal to obtain - this hands the walk its own stopping rule. The
     * alternative is to reserve once the count is known, which refuses exactly the same calls but only after
     * paying in full for a traversal it then throws away. The refusal itself still goes through
     * {@link #reserve}, so there is one place that decides and one shape of message, whichever end of the walk
     * it fires at.
     *
     * @param bytesPerItem estimated footprint of one item; a non-positive value means "no per-item cost"
     */
    public long capacityFor(final long bytesPerItem) {
      if (limit < 0 || bytesPerItem <= 0)
        return Long.MAX_VALUE;
      return (limit - reserved) / bytesPerItem;
    }

    /**
     * A saturated estimate is reported as "over Long.MAX_VALUE" rather than as the ceiling itself: the figure
     * is no longer representable, and quoting it as if it were exact would misstate by an unknown amount.
     */
    private static String describe(final long estimatedBytes) {
      return estimatedBytes == Long.MAX_VALUE ? "over " + Long.MAX_VALUE : Long.toString(estimatedBytes);
    }
  }

  /**
   * Returns {@code 0 .. count - 1} ordered by ascending {@code weights[i]}, without boxing an index.
   * <p>
   * Kruskal's is the reason this exists: both places that run it - {@code algo.mst} over the edge count and
   * {@code algo.steinerTree} over the {@code t(t-1)/2} terminal pairs - need the edge indices in weight order,
   * and the only ready-made way to sort an index array by an external key is
   * {@code Arrays.sort(Integer[], Comparator)}. That costs 24 bytes per entry where the {@code int} it carries
   * occupies 4, plus a comparator call and two unboxings per comparison, and {@code algo.steinerTree}'s entry
   * count is quadratic in a terminal list the caller supplies: ~2M entries and ~48 MB of boxed
   * {@code Integer} at 2000 terminals. Here it is 8 bytes per entry - the index array and the merge scratch -
   * and a primitive comparison.
   * </p>
   * <p>
   * A bottom-up merge sort rather than a quicksort, for two reasons that both matter on this path: the keys are
   * user data, and a quicksort on an adversarially ordered key array degrades to O(n²) on inputs a caller
   * chooses; and merging is stable, so equal weights keep index order exactly as the {@code TimSort} behind
   * {@code Arrays.sort(Integer[], ...)} did. {@link Double#compare} is the comparison, so {@code NaN} and
   * {@code -0.0} order exactly as they did through the comparator.
   * </p>
   *
   * @param weights key per index; only the first {@code count} entries are read
   * @param count   number of indices to order
   */
  protected static int[] sortedIndexesByWeight(final double[] weights, final int count) {
    final int[] index = new int[count];
    for (int i = 0; i < count; i++)
      index[i] = i;
    if (count < 2)
      return index;

    final int[] scratch = new int[count];
    int[] from = index;
    int[] to = scratch;
    // Widths are stepped in long: at a count near Integer.MAX_VALUE the last doubling and the run bounds it
    // produces overflow int, and a negative bound silently skips the merge instead of failing.
    for (long width = 1; width < count; width <<= 1) {
      for (long lo = 0; lo < count; lo += width << 1) {
        final int left = (int) lo;
        final int mid = (int) Math.min(lo + width, count);
        final int end = (int) Math.min(lo + (width << 1), count);
        int a = left, b = mid, out = left;
        while (a < mid && b < end)
          // Take the left run unless the right one is strictly smaller: that is what makes the sort stable.
          to[out++] = Double.compare(weights[from[b]], weights[from[a]]) < 0 ? from[b++] : from[a++];
        while (a < mid)
          to[out++] = from[a++];
        while (b < end)
          to[out++] = from[b++];
      }
      final int[] swap = from;
      from = to;
      to = swap;
    }
    return from;
  }

  /**
   * Creates the {@link MemoryBudget} of one algorithm call, carrying
   * {@link GlobalConfiguration#CYPHER_ALGO_MAX_WORKING_MEMORY}.
   */
  protected MemoryBudget newMemoryBudget(final Database db) {
    return new MemoryBudget(db.getConfiguration().getValueAsLong(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY));
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
   * estimate passes {@link MemoryBudget#reserve} unconditionally - the budget check would be silently disabled by
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
   * Creates a {@link WorkGuard} sharing this command's {@link GlobalConfiguration#COMMAND_TIMEOUT} deadline.
   * <p>
   * The knobs that drive the CPU-bound loops of the algorithm procedures ({@code iterations}, {@code restarts},
   * {@code simulations}, {@code walksPerNode}, ...) multiply time rather than a single allocation, and for time
   * there is no honest ceiling to pick: how long a run may legitimately take is a property of the graph, the
   * hardware and the caller's patience, not of the parameter. So a large value is not forbidden, it is made
   * abortable.
   */
  protected WorkGuard newWorkGuard(final CommandContext context) {
    return WorkGuard.forCommand(context, getName() + "()");
  }

  /** @see GraphEngine#getAllVertices(Database, String[]) */
  protected Iterator<Vertex> getAllVertices(final Database db, final String[] nodeLabels) {
    return GraphEngine.getAllVertices(db, nodeLabels);
  }

  /** @see GraphEngine#buildRidIndex(List) */
  protected Map<RID, Integer> buildRidIndex(final List<Vertex> vertices) {
    return GraphEngine.buildRidIndex(vertices);
  }

  /**
   * Loads every vertex the call will work over, charging each one to the call's budget as it arrives.
   * <p>
   * The list, the RID index built beside it and the loaded records are the first allocation of every procedure in
   * this package and the one no budget priced: a call could be refused for a 64 MB matrix having already
   * allocated a multi-gigabyte graph to measure it against, and a call that was accepted held that graph for its
   * whole duration with the budget none the wiser (issue #6317).
   * <p>
   * The bound is carried by the walk rather than by a reservation made once the count is known. Both refuse the
   * same calls, but a check afterwards first pays in full for a traversal it will throw away - the same argument
   * {@code algo.mst} settled the same way in issue #6300. The refusal itself still goes through
   * {@link MemoryBudget#reserve}, so there is one place that decides and one shape of message.
   *
   * @param db         the database
   * @param nodeLabels vertex type filter (null = all types)
   * @param memory     the call's budget
   */
  protected List<Vertex> loadVertices(final Database db, final String[] nodeLabels, final MemoryBudget memory) {
    final long maxVertices = memory.capacityFor(OLTP_VERTEX_BYTES);
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, nodeLabels);
    while (iter.hasNext()) {
      if (vertices.size() >= maxVertices)
        memory.reserve(saturatingProduct(vertices.size() + 1L, OLTP_VERTEX_BYTES), "the loaded graph",
            "at least " + (vertices.size() + 1) + " nodes");
      vertices.add(iter.next());
    }
    memory.reserve(saturatingProduct(vertices.size(), OLTP_VERTEX_BYTES), "the loaded graph", vertices.size() + " nodes");
    return vertices;
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
    if (relTypes != null && relTypes.length != 0)
      return null;
    // Coverage checked before readiness: coversEdgeType() is a pure config check, while a
    // GraphAnalyticalView's isReady() (see #6641) dispatches its deferred restore-from-disk as a
    // side effect when one is pending. Checking coverage first means a whole-graph algorithm only
    // ever pays that cost for a provider it could actually use, not for every registered one -
    // otherwise this loop would eagerly resolve every other view's deferred restore too, same
    // regression GraphTraversalProviderRegistry.findProvider() was fixed against.
    final Iterator<GraphTraversalProvider> iterator = GraphTraversalProviderRegistry.getProviders(db).iterator();
    GraphTraversalProvider found = null;
    while (found == null && iterator.hasNext()) {
      final GraphTraversalProvider p = iterator.next();
      if (p.coversEdgeType(null) && p.isReady())
        found = p;
    }
    return found;
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
    final MemoryBudget memory = newMemoryBudget(db);
    if (nodeLabels == null || nodeLabels.length == 0) {
      final GraphTraversalProvider provider = findProvider(db, relTypes);
      if (provider != null) {
        if (context != null)
          context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        // A Graph Analytical View is shared, pre-built state this call neither allocates nor frees, so its own
        // footprint is not the call's to price. What is the call's is the copy adjacency() takes out of it, and
        // that is reserved there.
        return new GraphData(provider, provider.getNodeCount(), memory);
      }
    }
    return new GraphData(loadVertices(db, nodeLabels, memory), memory);
  }

  /**
   * Encapsulates graph data that can be backed by either a CSR provider or OLTP vertex lists.
   * Provides uniform access to adjacency, vertex lookup, and RID resolution regardless of backing.
   */
  protected static class GraphData {
    private static final int[]    EMPTY_NEIGHBORS = new int[0];
    private static final double[] EMPTY_WEIGHTS   = new double[0];

    public final int                     nodeCount;
    private final GraphTraversalProvider provider;
    private final List<Vertex>           vertices;
    private final Map<RID, Integer>      ridToIdx;
    private final MemoryBudget           memory;

    private GraphData(final GraphTraversalProvider provider, final int nodeCount, final MemoryBudget memory) {
      this.provider = provider;
      this.vertices = null;
      this.ridToIdx = null;
      this.nodeCount = nodeCount;
      this.memory = memory;
    }

    private GraphData(final List<Vertex> vertices, final MemoryBudget memory) {
      this(vertices, GraphEngine.buildRidIndex(vertices), memory);
    }

    private GraphData(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx, final MemoryBudget memory) {
      this.provider = null;
      this.vertices = vertices;
      this.ridToIdx = ridToIdx;
      this.nodeCount = vertices.size();
      this.memory = memory;
    }

    /**
     * The budget this call is spending, already charged for the graph itself.
     * <p>
     * An algorithm that reserves a working set of its own takes it from here rather than opening a second budget:
     * the unit being bounded is the call, and a call that opened one budget per component could exceed the limit
     * by however many components it happens to have. The graph is the first and usually the largest of them
     * (issue #6317).
     */
    public MemoryBudget memory() {
      return memory;
    }

    public int[][] adjacency(final Vertex.DIRECTION dir, final String... relTypes) {
      if (provider != null) {
        // Try zero-allocation NeighborView first
        final NeighborView nv = provider.getNeighborView(dir, relTypes);
        if (nv != null) {
          long entries = 0;
          for (int i = 0; i < nodeCount; i++)
            entries += nv.offsetEnd(i) - nv.offset(i);
          reserveAdjacency(nodeCount, entries);
          final int[][] adj = new int[nodeCount][];
          final int[] nbrs = nv.neighbors();
          for (int i = 0; i < nodeCount; i++) {
            final int start = nv.offset(i);
            final int end = nv.offsetEnd(i);
            adj[i] = start == end ? EMPTY_NEIGHBORS : Arrays.copyOfRange(nbrs, start, end);
          }
          return adj;
        }
        // No view to size the copy from, so the rows are priced as they arrive. Unlike the NeighborView branch
        // above and the OLTP buildAdjacencyList below - both of which know the exact entry count before
        // allocating a single row - this loop cannot know a row's size before calling getNeighborIds(), so that
        // one call is unavoidably unpriced. What is avoidable is how long a checkpoint can then be postponed:
        // checkpointing on entries-seen-so-far as well as on the node-count interval means a single oversized
        // row (a supernode) crosses ADJACENCY_CHECKPOINT_ENTRIES by itself and gets checkpointed - and can be
        // refused - right after that one row, rather than only after another 1023 nodes reach the next
        // node-count boundary (issue #6444, following up on the #6317 code review that first named this gap).
        reserveAdjacency(nodeCount, 0);
        final int[][] adj = new int[nodeCount][];
        long entries = 0;
        for (int i = 0; i < nodeCount; i++) {
          adj[i] = provider.getNeighborIds(i, dir, relTypes);
          entries += adj[i].length;
          if (entries >= ADJACENCY_CHECKPOINT_ENTRIES || (i & 1023) == 1023) {
            reserveAdjacency(0, entries);
            entries = 0;
          }
        }
        reserveAdjacency(0, entries);
        return adj;
      }
      // buildAdjacencyList counts every entry before it allocates a single row, so the exact footprint is known
      // at the one moment it is still free to refuse: that count is handed here and reserved before the fill.
      return GraphEngine.buildAdjacencyList(vertices, ridToIdx, dir, relTypes,
          entries -> reserveAdjacency(nodeCount, entries));
    }

    /**
     * Charges {@code rows} neighbour-row headers and {@code entries} neighbour ids to the call's budget.
     * <p>
     * The adjacency is the second unpriced allocation issue #6317 names, and the larger one: 4 bytes per edge
     * plus a row header per node, with {@code Vertex.DIRECTION.BOTH} materialising each edge twice. An algorithm
     * that asks for it in two directions - {@code algo.cliques} does - pays for both, which is right: it holds
     * both at once.
     */
    private void reserveAdjacency(final long rows, final long entries) {
      if (rows == 0 && entries == 0)
        return;
      memory.reserve(saturatingSum(saturatingProduct(rows, MATRIX_ROW_OVERHEAD_BYTES),
              saturatingProduct(entries, INT_BYTES)), "the adjacency list",
          rows > 0 ? rows + " nodes, " + entries + " edge entries" : entries + " edge entries");
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
     * A neighbour list and the weight of each of its edges, produced together.
     * <p>
     * {@code weights[i][j]} is the weight of the edge to {@code neighbors[i][j]} <em>by construction</em>: both
     * arrays are filled from one walk of the same edges, so there is no later reconciliation step that can get
     * the pairing wrong. That is the whole point of returning the two together rather than offering weights
     * "aligned with" an adjacency the caller obtained separately - issue #6301 is what the second shape costs.
     *
     * @param neighbors dense neighbour ids per node
     * @param weights   weight of the edge to the neighbour at the same index
     */
    public record WeightedAdjacency(int[][] neighbors, double[][] weights) {
    }

    /**
     * Builds the neighbour lists and their edge weights in a single pass, from columnar edge properties when the
     * view has them and from the edge records otherwise.
     * <p>
     * The two sources produce the same multiset of {@code (neighbour, weight)} pairs per node - the same edges,
     * differing only in the order they are listed - which is the property that matters: a Graph Analytical View
     * is an accelerator, and an algorithm reading weights through it must reach the answer it would have reached
     * without it.
     * <p>
     * A {@code null} {@code weightProperty} means unit weights, and then this is {@link #adjacency} with a
     * {@code 1.0} beside every entry.
     *
     * @param guard          checkpoint for the walk; it visits every edge of the graph, so it is itself one of the
     *                       graph-driven loops that has to be abortable
     * @param dir            traversal direction; {@code BOTH} lists a node's outgoing and incoming edges alike
     * @param weightProperty edge property holding the weight, or {@code null} for unit weights
     * @param relTypes       edge types to filter by, empty/null for all of them
     */
    public WeightedAdjacency weightedAdjacency(final WorkGuard guard, final Vertex.DIRECTION dir,
        final String weightProperty, final String... relTypes) {
      if (weightProperty == null) {
        final int[][] neighbors = adjacency(dir, relTypes);
        final double[][] weights = new double[nodeCount][];
        for (int i = 0; i < nodeCount; i++) {
          guard.checkPeriodically(i);
          weights[i] = new double[neighbors[i].length];
          Arrays.fill(weights[i], 1.0);
        }
        return new WeightedAdjacency(neighbors, weights);
      }

      // getEdgeProperty() addresses an edge by (type, direction, position), so "all types" has to be resolved
      // to the actual list before the columnar path can be used at all. A provider that cannot enumerate its
      // types, or cannot serve THIS property for every one of them, sends us to the edge records - which are
      // exact. Asking only whether the provider has edge properties at all is not enough: a view built over
      // `distance` answers yes to a call asking for `cost`, and then every getEdgeProperty returns null and the
      // whole graph is silently unweighted - the same wrong answer as a misaligned weight, reached from the
      // other direction.
      if (provider != null && provider.servesEdgeProperty(weightProperty, relTypes)) {
        final String[] types = relTypes != null && relTypes.length > 0 ? relTypes
            : provider.getMaterializedEdgeTypes();
        return weightedAdjacencyFromColumns(guard, dir, weightProperty, types);
      }

      return weightedAdjacencyFromRecords(guard, dir, weightProperty, relTypes);
    }

    /**
     * Columnar build: one call per node into {@link GraphTraversalProvider#edgeWeightsOf}, which is where the
     * per-type, per-direction slicing that keeps a weight with its own edge lives. Nothing about that pairing is
     * re-derived here, so the CSR path of an {@code algo.*} procedure, of {@code astar} and of
     * {@code bellmanFord} cannot drift apart from one another.
     * <p>
     * {@code guard::checkPeriodically} is threaded into {@code edgeWeightsOf} itself rather than only checked
     * between two calls to it: one call already returns a fully-built row for one node, so a single supernode
     * would otherwise be one unabortable unit of work regardless of how tightly the per-node loop below is
     * checkpointed (issue #6715). The memory side of the same gap - a supernode's row is fully allocated before
     * {@link #reserveWeightedAdjacency} ever runs - cannot be closed the same way without handing this SPI a
     * dependency on {@link MemoryBudget}, so what is priced here is the cumulative cost across nodes, the same
     * bound {@link #reserveAdjacency} already gives the unweighted adjacency build; a single node whose row alone
     * exceeds the budget is still refused, just after that one row is built rather than during it.
     * <p>
     * The entries-seen checkpoint interval is capped by {@link MemoryBudget#capacityFor} against what is
     * actually left of the configured budget, not only by {@code ADJACENCY_CHECKPOINT_ENTRIES / 3} (a third of
     * {@link #adjacency}'s own constant, since a weighted entry costs {@code INT_BYTES + DOUBLE_BYTES} rather
     * than {@code INT_BYTES} alone): reusing the unweighted constant outright would let a tight budget be
     * overshot by three times as many bytes before a checkpoint ever fires (issue #6715 review).
     */
    private WeightedAdjacency weightedAdjacencyFromColumns(final WorkGuard guard, final Vertex.DIRECTION dir,
        final String weightProperty, final String[] types) {
      final int[][] neighbors = new int[nodeCount][];
      final double[][] weights = new double[nodeCount][];
      reserveWeightedAdjacency(nodeCount, 0);
      final long entryCheckpoint = Math.min(ADJACENCY_CHECKPOINT_ENTRIES / 3, memory.capacityFor(INT_BYTES + DOUBLE_BYTES));
      long entries = 0;
      for (int i = 0; i < nodeCount; i++) {
        guard.checkPeriodically(i);
        final NodeEdgeWeights edges = provider.edgeWeightsOf(i, dir, weightProperty, 1.0, guard::checkPeriodically, types);
        neighbors[i] = edges.neighbors();
        weights[i] = edges.weights();
        entries += edges.neighbors().length;
        if (entries >= entryCheckpoint || (i & 1023) == 1023) {
          reserveWeightedAdjacency(0, entries);
          entries = 0;
        }
      }
      reserveWeightedAdjacency(0, entries);
      return new WeightedAdjacency(neighbors, weights);
    }

    /**
     * Charges {@code rows} neighbour/weight row-header pairs and {@code entries} (neighbour id + weight) pairs to
     * the call's budget - the weighted counterpart of {@link #reserveAdjacency}, doubled because a
     * {@link WeightedAdjacency} holds a neighbour array AND a weight array per node rather than one.
     */
    private void reserveWeightedAdjacency(final long rows, final long entries) {
      if (rows == 0 && entries == 0)
        return;
      memory.reserve(saturatingSum(saturatingProduct(rows, MATRIX_ROW_OVERHEAD_BYTES * 2),
              saturatingProduct(entries, INT_BYTES + DOUBLE_BYTES)), "the weighted adjacency",
          rows > 0 ? rows + " nodes, " + entries + " edge entries" : entries + " edge entries");
    }

    /**
     * Record build: the neighbour and the weight come off the same {@link Edge}, so they cannot be mismatched.
     * Works for a CSR-backed graph too - {@link #getVertex} and {@link #indexOf} bridge back to the records -
     * which is what makes it the fallback whenever the columnar path cannot answer exactly.
     */
    private WeightedAdjacency weightedAdjacencyFromRecords(final WorkGuard guard, final Vertex.DIRECTION dir,
        final String weightProperty, final String[] relTypes) {
      final int[][] neighbors = new int[nodeCount][];
      final double[][] weights = new double[nodeCount][];
      // One growable pair of scratch buffers for the whole graph rather than a list per node: the degree is
      // unknown before the walk, and a per-node ArrayList<Double> would box every weight.
      int[] scratchNeighbors = new int[16];
      double[] scratchWeights = new double[16];
      int edgeStep = 0;

      for (int i = 0; i < nodeCount; i++) {
        final Vertex vertex = getVertex(i);
        if (vertex == null) {
          // Deleted since the CSR was built: no edges to read, and every other caller in this package skips it.
          neighbors[i] = EMPTY_NEIGHBORS;
          weights[i] = EMPTY_WEIGHTS;
          continue;
        }

        final RID vertexRid = vertex.getIdentity();
        final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
            vertex.getEdges(dir, relTypes) :
            vertex.getEdges(dir);
        int degree = 0;
        for (final Edge edge : edges) {
          // Throttled by EDGE rather than by vertex: one supernode can hold millions of them, and the walk
          // deserialises each, so a per-vertex checkpoint would leave that whole node unabortable.
          guard.checkPeriodically(edgeStep++);
          try {
            final RID neighborRid = GraphEngine.neighborRid(edge, vertexRid, dir);
            final int neighbor = neighborRid != null ? indexOf(neighborRid) : -1;
            if (neighbor < 0)
              continue;
            if (degree == scratchNeighbors.length) {
              scratchNeighbors = Arrays.copyOf(scratchNeighbors, degree * 2);
              scratchWeights = Arrays.copyOf(scratchWeights, degree * 2);
            }
            final Object weight = edge.get(weightProperty);
            scratchNeighbors[degree] = neighbor;
            scratchWeights[degree] = weight instanceof Number num ? num.doubleValue() : 1.0;
            degree++;
          } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e': 'edge' is the loop variable in this scope
            // Ghost edge: a dangling pointer to a record that is gone. Skipped, as everywhere else in the package.
            GhostEdgeReporter.reportSkipped(rnf);
          }
        }
        neighbors[i] = Arrays.copyOf(scratchNeighbors, degree);
        weights[i] = Arrays.copyOf(scratchWeights, degree);
      }
      return new WeightedAdjacency(neighbors, weights);
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
