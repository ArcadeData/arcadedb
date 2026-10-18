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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Count operator for a triangle written as one MATCH (issue #10255):
 * <pre>
 *   MATCH (a:L)-[:T]-(b:L)-[:T]-(c:L)-[:T]-(a) RETURN count(*)
 *   MATCH (a:L)-[:T]-(b:L)-[:T]-(c:L)-[:T]-(a) WHERE a.p &lt; b.p AND b.p &lt; c.p RETURN count(*)
 * </pre>
 * Every triangle of three distinct vertices is enumerated once, as the ascending node ids {@code u < v < w}, by the sorted
 * intersection of the two undirected adjacency ranges, and weighted by how many matches of the pattern it stands for: the
 * product of the parallel relationships between its three pairs (a parallel relationship is a match of its own, issue
 * #9298) times the number of ways its vertices can be bound to {@code a}, {@code b}, {@code c}.
 * <ul>
 *   <li>With no WHERE that is every one of the 6 orderings, and the walks that run over a self loop are added on top, as
 *   {@link PartitionedTriangleOp} counts them (issue #9486).</li>
 *   <li>With a WHERE that orders the three variables strictly on one property it is 1 when the three values are comparable
 *   and pairwise distinct and 0 otherwise: a strict order binds three distinct vertices, so no walk over a loop can match
 *   it. The values are read once per vertex, the way the property read of the row pipeline reads them, and turned into
 *   ranks, so the hot loop compares ints. A value whose order this operator does not rank itself (a temporal, a list, a
 *   FLOAT...) sends the count to the comparisons of the WHERE itself, applied to each triangle.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TriangleCountOp implements CountOp {
  private static final int  SERIAL_THRESHOLD = 1000;
  // VALUE FAMILIES OF THE RANKED PATH: VALUES OF TWO DIFFERENT FAMILIES ARE NOT COMPARABLE, SO A TRIANGLE MIXING THEM MATCHES NOTHING
  private static final byte EXCLUDED         = -1;
  private static final byte NUMBER           = 0;
  private static final byte STRING           = 1;
  private static final byte BOOLEAN          = 2;
  private static final long MAX_EXACT_DOUBLE = 1L << 53;
  // THE 6 BINDINGS OF THE VERTICES OF A TRIANGLE TO THE THREE VARIABLES
  private static final int[][] PERMUTATIONS = { { 0, 1, 2 }, { 0, 2, 1 }, { 1, 0, 2 }, { 1, 2, 0 }, { 2, 0, 1 }, { 2, 1, 0 } };

  private final String                 edgeType;
  private final String                 label;
  /** The property the WHERE orders the variables on, null when there is no WHERE: every walk is a match. */
  private final String                 property;
  /** The variables of the order, lowest first, for the plan. */
  private final String[]               orderVariables;
  /** The comparisons of the WHERE, each between the variable at {@code left[i]} and the one at {@code right[i]} (0..2). */
  private final ComparisonExpression[] comparisons;
  private final int[]                  left;
  private final int[]                  right;

  /** The count of every closed walk: no WHERE. */
  public TriangleCountOp(final String edgeType, final String label) {
    this(edgeType, label, null, null, null, null, null);
  }

  public TriangleCountOp(final String edgeType, final String label, final String property, final String[] orderVariables,
      final ComparisonExpression[] comparisons, final int[] left, final int[] right) {
    this.edgeType = edgeType;
    this.label = label;
    this.property = property;
    this.orderVariables = orderVariables;
    this.comparisons = comparisons;
    this.left = left;
    this.right = right;
  }

  @Override
  public String[] edgeTypes() {
    return new String[] { edgeType };
  }

  @Override
  public boolean requiresFullVertexCoverage() {
    return label == null;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    final int n = provider.getNodeIdUpperBound();
    final IntHashSet buckets = CSRCountUtils.buildValidBuckets(db, label);
    final boolean[] accepted = new boolean[n];
    for (int v = 0; v < n; v++) {
      guard.checkPeriodically(v);
      accepted[v] = provider.isNodeLive(v) && (buckets == null || buckets.contains(provider.getBucketId(v)));
    }

    NeighborView view = provider.getNeighborView(Vertex.DIRECTION.BOTH, edgeType);
    if (view == null) {
      // NO VIEW WHILE A DELTA OVERLAY IS ACTIVE (#6943): THE PER-NODE ACCESSOR ANSWERS IN EVERY STATE
      final int[][] adjacency = new int[n][];
      for (int u = 0; u < n; u++) {
        guard.checkPeriodically(u);
        adjacency[u] = accepted[u] ? provider.getNeighborIds(u, Vertex.DIRECTION.BOTH, edgeType) : new int[0];
      }
      view = pack(adjacency);
    }

    Object[] values = null;
    if (property != null) {
      values = new Object[n];
      for (int v = 0; v < n; v++) {
        guard.checkPeriodically(v);
        if (accepted[v]) {
          final RID rid = provider.getRID(v);
          // THE COLUMN OF THE VIEW WHEN IT HOLDS THE PROPERTY, THE RECORD OTHERWISE
          values[v] = rid == null ? null : TemporalUtil.convertFromStorage(new GAVVertex(rid, v, provider, db), property);
        }
      }
    }

    return count(view, accepted, values, guard, node -> loopsOf(provider.getNeighborIds(node, Vertex.DIRECTION.OUT, edgeType), node));
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    final ArrayList<Vertex> vertices = new ArrayList<>();
    final HashMap<RID, Integer> indexOf = new HashMap<>();
    for (final Iterator<? extends Identifiable> it = CSRCountUtils.iterateAnchors(db, label); it.hasNext(); ) {
      guard.check();
      final Vertex vertex = it.next().asVertex();
      indexOf.put(vertex.getIdentity(), vertices.size());
      vertices.add(vertex);
    }

    // THE EDGE LIST OF EVERY VERTEX IS READ ONCE, AS SORTED INDEXES OF THE NEIGHBOURS THE LABEL LETS THROUGH, ONE PER EDGE
    final int n = vertices.size();
    final int[][] adjacency = new int[n][];
    int[] buffer = new int[16];
    for (int i = 0; i < n; i++) {
      guard.check();
      int size = 0;
      for (final RID neighbor : vertices.get(i).getConnectedVertexRIDs(Vertex.DIRECTION.BOTH, edgeType)) {
        final Integer index = indexOf.get(neighbor);
        if (index == null)
          continue;
        if (size == buffer.length)
          buffer = Arrays.copyOf(buffer, size * 2);
        buffer[size++] = index;
      }
      final int[] sorted = Arrays.copyOf(buffer, size);
      Arrays.sort(sorted);
      adjacency[i] = sorted;
    }

    final boolean[] accepted = new boolean[n];
    Arrays.fill(accepted, true);
    Object[] values = null;
    if (property != null) {
      values = new Object[n];
      for (int i = 0; i < n; i++)
        values[i] = TemporalUtil.convertFromStorage(vertices.get(i), property);
    }

    return count(pack(adjacency), accepted, values, guard, node -> {
      final RID self = vertices.get(node).getIdentity();
      long loops = 0;
      for (final RID neighbor : vertices.get(node).getConnectedVertexRIDs(Vertex.DIRECTION.OUT, edgeType))
        if (neighbor.equals(self))
          loops++;
      return loops;
    });
  }

  private interface LoopCounter {
    long loopsOf(int node);
  }

  private long count(final NeighborView view, final boolean[] accepted, final Object[] values, final WorkGuard guard,
      final LoopCounter loops) {
    if (values == null)
      return countRange(view, accepted, null, null, null, loops, guard);

    final int n = accepted.length;
    final byte[] family = new byte[n];
    final int[] rank = new int[n];
    if (!rank(values, accepted, family, rank))
      // A VALUE THIS OPERATOR DOES NOT RANK: THE COMPARISONS OF THE WHERE DECIDE, TRIANGLE BY TRIANGLE. ON THE CALLER ONLY,
      // SINCE A COMPARISON MEMOIZES A TEMPORAL OPERAND
      return countSerial(view, accepted, null, null, values, loops, guard);
    return countRange(view, accepted, family, rank, null, loops, guard);
  }

  /** Counts over the whole id space, in the query pool's workers once it is large enough to pay for them. */
  private long countRange(final NeighborView view, final boolean[] accepted, final byte[] family, final int[] rank,
      final Object[] values, final LoopCounter loops, final WorkGuard guard) {
    final int n = accepted.length;
    final int stripes = Math.max(1, Runtime.getRuntime().availableProcessors());
    if (n < SERIAL_THRESHOLD || stripes == 1)
      return countSerial(view, accepted, family, rank, values, loops, guard);

    // STRIPED RATHER THAN CHUNKED: THE ENUMERATION RUNS FROM THE LOWEST ID OF A TRIANGLE, SO THE LOW IDS CARRY MORE WORK
    final long[] partial = new long[stripes];
    final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
    final Future<?>[] futures = new Future<?>[stripes - 1];
    int launched = 0;
    for (int s = 1; s < stripes; s++) {
      final int stripe = s;
      futures[launched++] = executor.submit(() -> partial[stripe] = countStripe(view, accepted, family, rank, values, loops,
          guard, stripe, stripes));
    }
    // THE CALLER RUNS STRIPE 0 AND ALWAYS AWAITS THE OTHERS, WHICH THE WAIT RECLAIMS WHEN STILL QUEUED (#4952, #5063, #6568)
    boolean ownCompleted = false;
    try {
      partial[0] = countStripe(view, accepted, family, rank, values, loops, guard, 0, stripes);
      ownCompleted = true;
    } finally {
      try {
        GraphAlgorithms.awaitFutures(futures, launched);
      } catch (final RuntimeException awaitError) {
        if (ownCompleted)
          throw awaitError;
      }
    }
    long total = 0;
    for (final long p : partial)
      total += p;
    return total;
  }

  private long countSerial(final NeighborView view, final boolean[] accepted, final byte[] family, final int[] rank,
      final Object[] values, final LoopCounter loops, final WorkGuard guard) {
    return countStripe(view, accepted, family, rank, values, loops, guard, 0, 1);
  }

  /**
   * The matches of the triangles whose lowest node id is in the stripe: for each wedge {@code u < v}, the common neighbours
   * {@code w > v} of both ranges, each run of equal ids being the parallel relationships to that neighbour.
   */
  private long countStripe(final NeighborView view, final boolean[] accepted, final byte[] family, final int[] rank,
      final Object[] values, final LoopCounter loops, final WorkGuard guard, final int stripe, final int stripes) {
    final int[] nbrs = view.neighbors();
    final int n = accepted.length;
    final Object[] bound = values != null ? new Object[3] : null;
    long count = 0;
    for (int u = stripe; u < n; u += stripes) {
      // SUPERLINEAR IN THE DEGREE: THE PER-ANCHOR CHECK IS WHAT BOUNDS THE WHOLE RANGE (#6266)
      guard.check();
      if (!accepted[u] || (family != null && family[u] == EXCLUDED))
        continue;
      final int uStart = view.offset(u), uEnd = view.offsetEnd(u);

      for (int i = upperBound(nbrs, uStart, uEnd, u); i < uEnd; ) {
        final int v = nbrs[i];
        final int vRunEnd = PartitionedTriangleOp.runEnd(nbrs, i, uEnd);
        final long uv = vRunEnd - i;
        i = vRunEnd;
        if (!accepted[v] || (family != null && (family[v] != family[u] || rank[v] == rank[u])))
          continue;

        final int vEnd = view.offsetEnd(v);
        int iu = i, iv = upperBound(nbrs, view.offset(v), vEnd, v);
        while (iu < uEnd && iv < vEnd) {
          final int nu = nbrs[iu], nv = nbrs[iv];
          if (nu < nv)
            iu++;
          else if (nu > nv)
            iv++;
          else {
            final int ue = PartitionedTriangleOp.runEnd(nbrs, iu, uEnd), ve = PartitionedTriangleOp.runEnd(nbrs, iv, vEnd);
            if (accepted[nu]) {
              final long weight;
              if (family != null)
                weight = family[nu] == family[u] && rank[nu] != rank[u] && rank[nu] != rank[v] ? 1 : 0;
              else if (values != null)
                weight = orderings(values[u], values[v], values[nu], bound);
              else
                weight = 6;
              // EXACT: ONCE PER TRIANGLE, AND A MULTIGRAPH'S PRODUCT OF PARALLEL RELATIONSHIPS MUST RAISE RATHER THAN WRAP
              if (weight != 0)
                count = Math.addExact(count, Math.multiplyExact(Math.multiplyExact(weight * uv, ue - iu), ve - iv));
            }
            iu = ue;
            iv = ve;
          }
        }
      }

      // THE WALKS OVER A SELF LOOP OF u, ONLY WITHOUT A WHERE: A STRICT ORDER BINDS THREE DISTINCT VERTICES
      if (values == null && family == null && Arrays.binarySearch(nbrs, uStart, uEnd, u) >= 0)
        count = Math.addExact(count, loopTerm(nbrs, uStart, uEnd, u, accepted, loops.loopsOf(u)));
    }
    return count;
  }

  /** The walks of the triangle over the self loops of {@code u}: see {@code PartitionedTriangleOp.loopTermOfPartition}. */
  private static long loopTerm(final int[] sorted, final int from, final int to, final int u, final boolean[] accepted,
      final long loops) {
    long term = PartitionedTriangleOp.orderedLoopTriples(loops);
    for (int i = from; i < to; ) {
      final int y = sorted[i];
      final int run = PartitionedTriangleOp.runEnd(sorted, i, to) - i;
      if (y != u && accepted[y])
        term = Math.addExact(term, PartitionedTriangleOp.repeatedPairTriples(loops, run));
      i += run;
    }
    return term;
  }

  private static long loopsOf(final int[] outNeighbors, final int node) {
    long loops = 0;
    for (final int n : outNeighbors)
      if (n == node)
        loops++;
    return loops;
  }

  /** How many of the 6 bindings of the three values to the variables satisfy every comparison of the WHERE. */
  private int orderings(final Object x, final Object y, final Object z, final Object[] bound) {
    int matches = 0;
    for (int p = 0; p < 6; p++) {
      bound[PERMUTATIONS[p][0]] = x;
      bound[PERMUTATIONS[p][1]] = y;
      bound[PERMUTATIONS[p][2]] = z;
      boolean all = true;
      for (int c = 0; c < comparisons.length && all; c++)
        all = Boolean.TRUE.equals(comparisons[c].evaluateWithValues(bound[left[c]], bound[right[c]]));
      if (all)
        matches++;
    }
    return matches;
  }

  /**
   * Ranks the values of the accepted nodes within their family, equal values sharing a rank, so a strict order between two
   * values of one family is the order of their ranks. A null or NaN value is comparable with nothing and excludes its node.
   *
   * @return false when some value is of a type this operator does not rank, which leaves the comparisons to the WHERE
   */
  private static boolean rank(final Object[] values, final boolean[] accepted, final byte[] family, final int[] rank) {
    final int n = values.length;
    int integrals = 0, doubles = 0, strings = 0;
    boolean beyondExactDouble = false;
    for (int v = 0; v < n; v++) {
      final Object value = values[v];
      family[v] = EXCLUDED;
      if (!accepted[v] || value == null)
        continue;
      if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
        final long l = ((Number) value).longValue();
        beyondExactDouble |= l > MAX_EXACT_DOUBLE || l < -MAX_EXACT_DOUBLE;
        family[v] = NUMBER;
        integrals++;
      } else if (value instanceof Double d) {
        if (d.isNaN())
          continue;
        family[v] = NUMBER;
        doubles++;
      } else if (value instanceof String) {
        family[v] = STRING;
        strings++;
      } else if (value instanceof Boolean b) {
        family[v] = BOOLEAN;
        rank[v] = b ? 1 : 0;
      } else
        return false;
    }
    // A LONG PAST 2^53 MEETS A DOUBLE ONLY IN AN EXACT COMPARISON, WHICH THE RANKS DO NOT MAKE
    if (doubles > 0 && beyondExactDouble)
      return false;

    if (doubles > 0) {
      final double[] keys = new double[integrals + doubles];
      int k = 0;
      for (int v = 0; v < n; v++)
        if (family[v] == NUMBER)
          keys[k++] = asDouble(values[v]);
      final int distinct = sortDistinct(keys);
      for (int v = 0; v < n; v++)
        if (family[v] == NUMBER)
          rank[v] = Arrays.binarySearch(keys, 0, distinct, asDouble(values[v]));
    } else if (integrals > 0) {
      final long[] keys = new long[integrals];
      int k = 0;
      for (int v = 0; v < n; v++)
        if (family[v] == NUMBER)
          keys[k++] = ((Number) values[v]).longValue();
      Arrays.sort(keys);
      int distinct = 0;
      for (int i = 0; i < keys.length; i++)
        if (i == 0 || keys[i] != keys[distinct - 1])
          keys[distinct++] = keys[i];
      for (int v = 0; v < n; v++)
        if (family[v] == NUMBER)
          rank[v] = Arrays.binarySearch(keys, 0, distinct, ((Number) values[v]).longValue());
    }

    if (strings > 0) {
      // BY CODE POINT, AS THE COMPARISON OF THE WHERE AND THE INDEX ORDER THEM (#8422)
      final String[] keys = new String[strings];
      int k = 0;
      for (int v = 0; v < n; v++)
        if (family[v] == STRING)
          keys[k++] = (String) values[v];
      Arrays.sort(keys, BinaryComparator::compareStrings);
      int distinct = 0;
      for (int i = 0; i < keys.length; i++)
        if (i == 0 || !keys[i].equals(keys[distinct - 1]))
          keys[distinct++] = keys[i];
      for (int v = 0; v < n; v++)
        if (family[v] == STRING)
          rank[v] = Arrays.binarySearch(keys, 0, distinct, (String) values[v], BinaryComparator::compareStrings);
    }
    return true;
  }

  /** The value as a double, -0.0 folded onto 0.0: the two are equal under {@code <}, unlike under {@link Double#compare}. */
  private static double asDouble(final Object value) {
    return ((Number) value).doubleValue() + 0.0;
  }

  /** Sorts {@code keys} and moves its distinct values to the front, returning how many there are. */
  private static int sortDistinct(final double[] keys) {
    Arrays.sort(keys);
    int distinct = 0;
    for (int i = 0; i < keys.length; i++)
      if (i == 0 || keys[i] != keys[distinct - 1])
        keys[distinct++] = keys[i];
    return distinct;
  }

  /** The first position in the sorted range {@code [from, to)} holding a value greater than {@code value}. */
  private static int upperBound(final int[] sorted, int from, int to, final int value) {
    while (from < to) {
      final int mid = (from + to) >>> 1;
      if (sorted[mid] <= value)
        from = mid + 1;
      else
        to = mid;
    }
    return from;
  }

  /** The adjacency lists as one view: offsets and the packed neighbours, each list sorted. */
  private static NeighborView pack(final int[][] adjacency) {
    final int n = adjacency.length;
    final int[] offsets = new int[n + 1];
    for (int i = 0; i < n; i++)
      offsets[i + 1] = offsets[i] + adjacency[i].length;
    final int[] neighbors = new int[offsets[n]];
    for (int i = 0; i < n; i++) {
      System.arraycopy(adjacency[i], 0, neighbors, offsets[i], adjacency[i].length);
      Arrays.sort(neighbors, offsets[i], offsets[i + 1]);
    }
    return new NeighborView(n, offsets, neighbors);
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT TRIANGLES (CSR sorted intersection)\n");
    sb.append(ind).append("  triangle edge: ").append(edgeType);
    if (label != null)
      sb.append(", label: ").append(label);
    if (property != null) {
      sb.append(", order: ");
      for (int i = 0; i < orderVariables.length; i++) {
        if (i > 0)
          sb.append(" < ");
        sb.append(orderVariables[i]).append('.').append(property);
      }
    }
    return sb.toString();
  }
}
