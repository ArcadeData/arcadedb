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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Count operator for chain patterns with an anti-join predicate.
 * <p>
 * Handles patterns like:
 * <pre>
 *   MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)
 *   WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3
 *   RETURN count(*) AS count
 * </pre>
 * <p>
 * The anti-join {@code NOT (n_i)-[:TYPE]-(n_j)} is evaluated efficiently using CSR sorted
 * neighbor arrays via merge-scan set difference, avoiding per-row OLTP edge scans.
 * <p>
 * Algorithm (CSR path):
 * <ol>
 *   <li>Propagate counts from position 0 to the anti-join source position (if source &gt; 0)</li>
 *   <li>For each active node v at the anti-join source position:
 *     <ul>
 *       <li>Get v's anti-join neighbors (sorted array from CSR)</li>
 *       <li>Enumerate paths from v through hops to the anti-join target position</li>
 *       <li>At each reached target node, merge-scan against anti-join neighbors to exclude matches</li>
 *       <li>For surviving targets, compute remaining chain contribution (degree products)</li>
 *     </ul>
 *   </li>
 * </ol>
 */
public final class AntiJoinChainOp implements CountOp {
  private final String[] nodeLabels;
  private final String[] edgeTypes;
  private final Vertex.DIRECTION[] directions;

  // Anti-join: NOT (node at sourceIdx)-[:antiJoinEdgeType]-(node at targetIdx)
  private final int antiJoinSourceIdx;
  private final int antiJoinTargetIdx;
  private final String antiJoinEdgeType;
  private final Vertex.DIRECTION antiJoinDirection;

  // Optional inequality filter (typically same as anti-join endpoints)
  private final int inequalityIdxA;
  private final int inequalityIdxB;

  public AntiJoinChainOp(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions,
      final int antiJoinSourceIdx, final int antiJoinTargetIdx,
      final String antiJoinEdgeType, final Vertex.DIRECTION antiJoinDirection,
      final int inequalityIdxA, final int inequalityIdxB) {
    this.nodeLabels = nodeLabels;
    this.edgeTypes = edgeTypes;
    this.directions = directions;
    this.antiJoinSourceIdx = antiJoinSourceIdx;
    this.antiJoinTargetIdx = antiJoinTargetIdx;
    this.antiJoinEdgeType = antiJoinEdgeType;
    this.antiJoinDirection = antiJoinDirection;
    this.inequalityIdxA = inequalityIdxA;
    this.inequalityIdxB = inequalityIdxB;
  }

  @Override
  public String[] edgeTypes() {
    // Include chain edge types + anti-join edge type
    final Set<String> types = new HashSet<>(Arrays.asList(edgeTypes));
    types.add(antiJoinEdgeType);
    return types.toArray(new String[0]);
  }

  /**
   * Every path here is enumerated from chain position 0. With no label there the anchors are every vertex, which
   * only a provider whose node domain <i>is</i> every vertex can enumerate (issue #5757).
   */
  @Override
  public boolean requiresFullVertexCoverage() {
    return nodeLabels[0] == null;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    final int hops = edgeTypes.length;

    // Pre-compute valid bucket sets for type filtering. null = unlabelled, so no filter.
    final IntHashSet[] validBuckets = new IntHashSet[hops + 1];
    for (int i = 0; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    // Determine the "check position" — the later of the two anti-join endpoints.
    // We always iterate from chain position 0 (anchor) and expand to the check position.
    // The anti-join filter is applied at the check position.
    final int earlierIdx = Math.min(antiJoinSourceIdx, antiJoinTargetIdx);
    final int laterIdx = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);

    // We need position 0 to be one of the anti-join endpoints for per-source iteration
    if (earlierIdx != 0)
      return executeGenericAntiJoin(db, guard);

    // The Q8 shape (see isOverlapShape) has its own exact count: one pass over the middle hop's edges.
    if (isOverlapShape()) {
      final long result = executeEdgeScanAlgebraic(provider, nodeIdUpperBound, validBuckets, guard);
      return result >= 0 ? result : executeOverlapOLTP(db, guard);
    }

    // Per-source iteration from anchor (position 0). null accepts every vertex, empty accepts none (issue #5757).
    final IntHashSet anchorBuckets = validBuckets[0];
    if (anchorBuckets != null && anchorBuckets.isEmpty())
      return 0;

    final boolean anchorIsSource = antiJoinSourceIdx == 0;

    // Two hops to the check position (Q9): count the paths algebraically instead of materializing the frontier.
    if (laterIdx == 2) {
      final long result = executeTwoHopAlgebraic(provider, nodeIdUpperBound, validBuckets, guard);
      if (result >= 0)
        return result;
    }

    // Pre-fetch NeighborViews for each hop up to the check position
    final NeighborView[] hopViews = new NeighborView[laterIdx];
    for (int h = 0; h < laterIdx; h++)
      hopViews[h] = CSRCountUtils.patternView(provider, directions[h], edgeTypes[h]);

    // Pre-compute bucket IDs for CSR-based anchor iteration and frontier filtering. A pattern that labels no
    // position needs none of them.
    final int[] bucketIds = anyLabelled(validBuckets)
        ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;

    long totalCount = 0;

    for (int anchorId = 0; anchorId < nodeIdUpperBound; anchorId++) {
      guard.check();
      if (!provider.isNodeLive(anchorId))
        continue;
      if (anchorBuckets != null && !anchorBuckets.contains(bucketIds[anchorId]))
        continue;

      // Pre-compute anti-join neighbors for Case A (anchor is the anti-join source)
      final int[] anchorAntiNbrs = anchorIsSource
          ? CSRCountUtils.hopNeighborIds(provider, anchorId, antiJoinDirection, antiJoinEdgeType)
          : null;

      final long count = countWithAntiJoin(provider, anchorId, anchorAntiNbrs,
          anchorIsSource, laterIdx, validBuckets, bucketIds, hopViews);
      totalCount += count;
    }

    return totalCount;
  }

  /**
   * Counts a chain whose anti-join check position is two hops from the anchor, without enumerating the two-hop frontier
   * (issue #9282).
   * <p>
   * For Q9, {@code (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:HAS_INTEREST]->(t) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3}, the
   * frontier of an anchor holds every friend of a friend, which over the whole graph is the number of two-hop paths:
   * allocating, filtering and binary-searching each of them was slower than the OLTP plan. The count is a sum over those
   * paths of {@code tail(p3)}, the degree product of the hops after the check position, and that sum factors:
   * <pre>
   *   count(p1) = sum over p2 in hop0(p1) of w[p2]                                    w[p2] = sum over p3 in hop1(p2) of tail(p3)
   *             - sum over distinct x in anti(p1) of tail(x) * paths(p1, x)           the anti-join
   *             - tail(p1) * paths(p1, p1)   when p1 is not in anti(p1)                the inequality
   * </pre>
   * {@code w} is one pass over the middle hop's edges, and {@code paths(p1, x)}, the number of p2 joining p1 to x, is a
   * multiset intersection of two sorted adjacency ranges, taken only for the few x the anti-join names. Parallel edges and
   * both directions of an undirected hop are counted the way the enumeration counts them, one path per edge pair; the
   * anti-join is an existence test, so each x is subtracted once however many edges join it to p1.
   *
   * @return the count, or -1 when the shape or the provider is not one this handles (the caller falls back)
   */
  private long executeTwoHopAlgebraic(final GraphTraversalProvider provider, final int nodeIdUpperBound,
      final IntHashSet[] validBuckets, final WorkGuard guard) {
    // The only inequality the enumeration applies is between the anchor and the check position
    final boolean inequality = inequalityIdxA >= 0 && inequalityIdxB >= 0;
    if (inequality && !(inequalityIdxA == 0 && inequalityIdxB == 2 || inequalityIdxA == 2 && inequalityIdxB == 0))
      return -1;

    // Anti-join neighbours of the anchor: when the anchor is the pattern's target, the edge points the other way round
    final Vertex.DIRECTION anchorAntiDirection = antiJoinSourceIdx == 0 ? antiJoinDirection : reverse(antiJoinDirection);
    // An undirected hop has no stored view: the provider merges and sorts the whole adjacency on each request, so the
    // views that ask for the same direction and edge type (all of them, on Q9) share one
    final Vertex.DIRECTION[] viewDirections = { directions[0], directions[1], reverse(directions[1]), anchorAntiDirection };
    final String[] viewTypes = { edgeTypes[0], edgeTypes[1], edgeTypes[1], antiJoinEdgeType };
    final NeighborView[] views = new NeighborView[4];
    for (int v = 0; v < views.length; v++) {
      for (int earlier = 0; earlier < v && views[v] == null; earlier++)
        if (viewDirections[earlier] == viewDirections[v] && viewTypes[earlier].equals(viewTypes[v]))
          views[v] = views[earlier];
      if (views[v] == null)
        views[v] = CSRCountUtils.patternView(provider, viewDirections[v], viewTypes[v]);
      if (views[v] == null)
        return -1;
    }
    final NeighborView hop0 = views[0];
    final NeighborView hop1 = views[1];
    final NeighborView hop1Reverse = views[2];
    final NeighborView anti = views[3];

    // tailViews[h - 2] is the view of hop h, and validBuckets[h + 1] the label of its target: the check position is 2 for every shape
    // that gets here, and bucketIds is set whenever a position is labelled
    final NeighborView[] tailViews = new NeighborView[edgeTypes.length - 2];
    for (int h = 2; h < edgeTypes.length; h++) {
      tailViews[h - 2] = CSRCountUtils.patternView(provider, directions[h], edgeTypes[h]);
      if (tailViews[h - 2] == null)
        return -1;
    }

    final IntHashSet anchorBuckets = validBuckets[0];
    final IntHashSet midBuckets = validBuckets[1];
    final IntHashSet checkBuckets = validBuckets[2];
    if ((anchorBuckets != null && anchorBuckets.isEmpty()) || (midBuckets != null && midBuckets.isEmpty())
        || (checkBuckets != null && checkBuckets.isEmpty()))
      return 0;
    final int[] bucketIds = anyLabelled(validBuckets) ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;

    // The snapshot's adjacency holds live nodes only (a view with a pending overlay hands out no NeighborView), so the
    // liveness test on weight[] and tail[] and its absence in pathsBetween agree.
    // tail[x]: the paths the hops after the check position add to a walk that reaches x; 0 for a node that cannot be there
    final long[] tail = new long[nodeIdUpperBound];
    for (int x = 0; x < nodeIdUpperBound; x++) {
      guard.checkPeriodically(x);
      if (!provider.isNodeLive(x) || checkBuckets != null && !checkBuckets.contains(bucketIds[x]))
        continue;
      long paths = 1;
      for (int h = 2; h < edgeTypes.length; h++) {
        paths *= targetDegree(tailViews[h - 2], x, validBuckets[h + 1], bucketIds);
        if (paths == 0)
          break;
      }
      tail[x] = paths;
    }

    // weight[m]: the weight of the walks that continue from a middle node m
    final long[] weight = new long[nodeIdUpperBound];
    final int[] hop1Neighbors = hop1.neighbors();
    for (int m = 0; m < nodeIdUpperBound; m++) {
      guard.checkPeriodically(m);
      if (!provider.isNodeLive(m) || midBuckets != null && !midBuckets.contains(bucketIds[m]))
        continue;
      long sum = 0;
      for (int j = hop1.offset(m), end = hop1.offsetEnd(m); j < end; j++)
        sum += tail[hop1Neighbors[j]];
      weight[m] = sum;
    }

    final int[] hop0Neighbors = hop0.neighbors();
    final int[] hop1ReverseNeighbors = hop1Reverse.neighbors();
    final int[] antiNeighbors = anti.neighbors();
    long total = 0;
    for (int anchor = 0; anchor < nodeIdUpperBound; anchor++) {
      guard.check();
      if (!provider.isNodeLive(anchor) || anchorBuckets != null && !anchorBuckets.contains(bucketIds[anchor]))
        continue;
      final int start = hop0.offset(anchor);
      final int end = hop0.offsetEnd(anchor);
      if (start == end)
        continue;

      long count = 0;
      for (int j = start; j < end; j++)
        count += weight[hop0Neighbors[j]];
      if (count == 0)
        continue;

      boolean anchorIsAntiJoined = false;
      int previous = -1;
      for (int j = anti.offset(anchor), antiEnd = anti.offsetEnd(anchor); j < antiEnd; j++) {
        final int x = antiNeighbors[j];
        if (x == previous)
          continue;
        previous = x;
        anchorIsAntiJoined |= x == anchor;
        if (tail[x] != 0)
          count -= tail[x] * pathsBetween(hop0Neighbors, start, end, hop1ReverseNeighbors, hop1Reverse.offset(x),
              hop1Reverse.offsetEnd(x), midBuckets, bucketIds);
      }
      if (inequality && !anchorIsAntiJoined && tail[anchor] != 0)
        count -= tail[anchor] * pathsBetween(hop0Neighbors, start, end, hop1ReverseNeighbors, hop1Reverse.offset(anchor),
            hop1Reverse.offsetEnd(anchor), midBuckets, bucketIds);
      total += count;
    }
    return total;
  }

  private static Vertex.DIRECTION reverse(final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.OUT ? Vertex.DIRECTION.IN
        : direction == Vertex.DIRECTION.IN ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
  }

  /**
   * The number of pairs of equal values in two sorted ranges, counting a value as often as the two ranges repeat it, and
   * restricted to the values a middle-position label keeps (null keeps all). The shorter range is looked up in the longer
   * by binary search when they differ a lot in length, so a hub is not scanned for each of its small neighbours.
   */
  private static long pathsBetween(final int[] a, final int aStart, final int aEnd, final int[] b, final int bStart,
      final int bEnd, final IntHashSet allowed, final int[] bucketIds) {
    if (aStart == aEnd || bStart == bEnd)
      return 0;
    final int aLength = aEnd - aStart;
    final int bLength = bEnd - bStart;
    long pairs = 0;
    if (aLength * 16L < bLength || bLength * 16L < aLength) {
      final boolean aIsShort = aLength < bLength;
      final int[] shortArray = aIsShort ? a : b;
      final int[] longArray = aIsShort ? b : a;
      final int longEnd = aIsShort ? bEnd : aEnd;
      int longFrom = aIsShort ? bStart : aStart;
      int i = aIsShort ? aStart : bStart;
      final int shortEnd = aIsShort ? aEnd : bEnd;
      while (i < shortEnd) {
        final int value = shortArray[i];
        int runEnd = i + 1;
        while (runEnd < shortEnd && shortArray[runEnd] == value)
          runEnd++;
        final int low = lowerBound(longArray, longFrom, longEnd, value);
        longFrom = low;
        if (low < longEnd && longArray[low] == value && (allowed == null || allowed.contains(bucketIds[value]))) {
          int high = low + 1;
          while (high < longEnd && longArray[high] == value)
            high++;
          pairs += (long) (runEnd - i) * (high - low);
        }
        i = runEnd;
      }
      return pairs;
    }

    int i = aStart;
    int j = bStart;
    while (i < aEnd && j < bEnd) {
      final int av = a[i];
      final int bv = b[j];
      if (av < bv)
        i++;
      else if (av > bv)
        j++;
      else {
        int iEnd = i + 1;
        while (iEnd < aEnd && a[iEnd] == av)
          iEnd++;
        int jEnd = j + 1;
        while (jEnd < bEnd && b[jEnd] == av)
          jEnd++;
        if (allowed == null || allowed.contains(bucketIds[av]))
          pairs += (long) (iEnd - i) * (jEnd - j);
        i = iEnd;
        j = jEnd;
      }
    }
    return pairs;
  }

  /** The first index in {@code [from, to)} whose value is not below {@code key}. */
  private static int lowerBound(final int[] array, final int from, final int to, final int key) {
    int low = from;
    int high = to;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (array[mid] < key)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  /**
   * Whether the chain is the one shape {@link #executeEdgeScanAlgebraic} and {@link #executeOverlapOLTP} count exactly (LSQB Q8):
   * <pre>
   *   (t1:T)&lt;-[:E]-(m:M)&lt;-[:R]-(c:C)-[:E]-&gt;(t2:T)  WHERE NOT (c)-[:E]-&gt;(t1) AND t1 &lt;&gt; t2
   * </pre>
   * Three hops, every position labelled, hops 0 and 2 of the negated pattern's type {@code E} and pointing the same way from
   * {@code m} and from {@code c}, the negated pattern running from {@code c} to {@code t1} in that same direction, and the
   * inequality between the two ends. The planner only builds the operator for this shape after checking that {@code R} is a
   * type that cannot be {@code E} (see CypherExecutionPlan#isOverlapShapeCountedExactly).
   */
  private boolean isOverlapShape() {
    if (edgeTypes.length != 3)
      return false;
    for (final String label : nodeLabels)
      if (label == null)
        return false;
    for (final Vertex.DIRECTION direction : directions)
      if (direction == Vertex.DIRECTION.BOTH)
        return false;
    if (Math.min(antiJoinSourceIdx, antiJoinTargetIdx) != 0 || Math.max(antiJoinSourceIdx, antiJoinTargetIdx) != 2)
      return false;
    if (Math.min(inequalityIdxA, inequalityIdxB) != 0 || Math.max(inequalityIdxA, inequalityIdxB) != 3)
      return false;
    if (!antiJoinEdgeType.equals(edgeTypes[0]) || !antiJoinEdgeType.equals(edgeTypes[2]) || antiJoinEdgeType.equals(edgeTypes[1]))
      return false;
    final Vertex.DIRECTION antiFromC = antiJoinSourceIdx == 2 ? antiJoinDirection : reverse(antiJoinDirection);
    return antiFromC == directions[2] && reverse(directions[0]) == directions[2];
  }

  /**
   * Exact count of the Q8 shape (see {@link #isOverlapShape}) on the CSR: one pass over the middle hop's edges (c, m).
   * <p>
   * A row is a choice of an {@code E} edge from m to t1, the {@code R} edge, and an {@code E} edge from c to t2. The negated
   * pattern removes every t1 that c has an {@code E} edge to; a t2 is such a target, so t1 differs from t2 whenever the row
   * survives, and the two {@code E} edges (m's and c's) are different edges. Hence for every pair (m, c)
   * <pre>
   *   rows(m, c) = #{ E edges m-&gt;t1 : t1 in T0 and t1 not in targets(c) } * #{ E edges c-&gt;t2 : t2 in T3 }
   * </pre>
   * where T0 and T3 are the labels of positions 0 and 3, targets(c) is every target of c's {@code E} edges (the negated pattern
   * has no label), and parallel edges count once per edge on both sides, as the row pipeline does.
   *
   * @return count, or -1 if a NeighborView is unavailable (the caller counts through the vertices instead)
   */
  private long executeEdgeScanAlgebraic(final GraphTraversalProvider provider,
      final int nodeIdUpperBound, final IntHashSet[] validBuckets, final WorkGuard guard) {
    for (final IntHashSet buckets : validBuckets)
      if (buckets.isEmpty())
        return 0;
    final NeighborView viewA = CSRCountUtils.patternView(provider, reverse(directions[0]), edgeTypes[0]);   // m -> t1
    final NeighborView viewC = CSRCountUtils.patternView(provider, directions[2], edgeTypes[2]);            // c -> t2 (and the negated pattern)
    final NeighborView viewR = CSRCountUtils.patternView(provider, reverse(directions[1]), edgeTypes[1]);   // c -> m
    if (viewA == null || viewC == null || viewR == null)
      return -1;

    final int[] aNbrs = viewA.neighbors();
    final int[] cNbrs = viewC.neighbors();
    final int[] rNbrs = viewR.neighbors();
    final int[] bucketIds = precomputeBucketIds(provider, nodeIdUpperBound, guard);
    final IntHashSet t1Buckets = validBuckets[0], mBuckets = validBuckets[1], cBuckets = validBuckets[2], t2Buckets = validBuckets[3];

    long total = 0;
    int[] targets = new int[16];
    for (int c = 0; c < nodeIdUpperBound; c++) {
      guard.checkPeriodically(c);
      if (!provider.isNodeLive(c) || !cBuckets.contains(bucketIds[c]))
        continue;
      final int cStart = viewC.offset(c), cEnd = viewC.offsetEnd(c);
      if (cStart == cEnd)
        continue;
      long t2 = 0;
      for (int k = cStart; k < cEnd; k++)
        if (t2Buckets.contains(bucketIds[cNbrs[k]]))
          t2++;
      if (t2 == 0)
        continue;
      final int rStart = viewR.offset(c), rEnd = viewR.offsetEnd(c);
      if (rStart == rEnd)
        continue;
      final int n = cEnd - cStart;
      if (targets.length < n)
        targets = new int[Math.max(n, targets.length * 2)];
      System.arraycopy(cNbrs, cStart, targets, 0, n);
      Arrays.sort(targets, 0, n);

      for (int j = rStart; j < rEnd; j++) {
        final int m = rNbrs[j];
        if (!mBuckets.contains(bucketIds[m]))
          continue;
        long t1 = 0;
        for (int k = viewA.offset(m), end = viewA.offsetEnd(m); k < end; k++) {
          final int a = aNbrs[k];
          if (t1Buckets.contains(bucketIds[a]) && Arrays.binarySearch(targets, 0, n, a) < 0)
            t1++;
        }
        total += t1 * t2;
      }
    }
    return total;
  }

  /** The same count as {@link #executeEdgeScanAlgebraic}, read through the vertices (no CSR view available). */
  private long executeOverlapOLTP(final Database db, final WorkGuard guard) {
    final IntHashSet[] buckets = new IntHashSet[4];
    for (int i = 0; i < 4; i++) {
      buckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);
      if (buckets[i].isEmpty())
        return 0;
    }
    final Vertex.DIRECTION cToTags = directions[2], cToM = reverse(directions[1]), mToTags = reverse(directions[0]);
    long total = 0;
    for (final Iterator<? extends Identifiable> it = CSRCountUtils.iterateAnchors(db, nodeLabels[2]); it.hasNext(); ) {
      guard.check();
      final Vertex c = it.next().asVertex();
      final Set<RID> targets = new HashSet<>();
      long t2 = 0;
      for (final RID tag : SelfLoops.connectedVertexRIDs(c, cToTags, edgeTypes[2])) {
        targets.add(tag);
        if (buckets[3].contains(tag.getBucketId()))
          t2++;
      }
      if (t2 == 0)
        continue;
      for (final RID m : SelfLoops.connectedVertexRIDs(c, cToM, edgeTypes[1])) {
        if (!buckets[1].contains(m.getBucketId()))
          continue;
        long t1 = 0;
        for (final RID a : SelfLoops.connectedVertexRIDs(m.asVertex(), mToTags, edgeTypes[0]))
          if (buckets[0].contains(a.getBucketId()) && !targets.contains(a))
            t1++;
        total += t1 * t2;
      }
    }
    return total;
  }

  /** Whether any position of the chain carries a label, i.e. whether per-node bucket ids are worth computing. */
  private static boolean anyLabelled(final IntHashSet[] validBuckets) {
    for (final IntHashSet buckets : validBuckets)
      if (buckets != null)
        return true;
    return false;
  }

  /** The edges of {@code view} out of {@code node} that reach a vertex of the labelled bucket set (null: any vertex, the plain degree). */
  private static long targetDegree(final NeighborView view, final int node, final IntHashSet targetBuckets, final int[] bucketIds) {
    if (targetBuckets == null)
      return view.degree(node);
    final int[] neighbors = view.neighbors();
    long degree = 0;
    for (int j = view.offset(node), end = view.offsetEnd(node); j < end; j++)
      if (targetBuckets.contains(bucketIds[neighbors[j]]))
        degree++;
    return degree;
  }

  /** The edges of {@code vertex} that reach a vertex of the labelled bucket set (null: any vertex, the plain degree). */
  private static long targetDegree(final Vertex vertex, final Vertex.DIRECTION direction, final String edgeType,
      final IntHashSet targetBuckets) {
    if (targetBuckets == null)
      return IncomingEdgeLookup.countPatternEdges(null, vertex, direction, edgeType);
    long degree = 0;
    for (final RID target : SelfLoops.connectedVertexRIDs(vertex, direction, edgeType))
      if (targetBuckets.contains(target.getBucketId()))
        degree++;
    return degree;
  }

  /** Pre-computes bucket IDs for all live CSR nodes. One-time O(node ID space) cost. */
  private static int[] precomputeBucketIds(final GraphTraversalProvider provider, final int nodeIdUpperBound,
      final WorkGuard guard) {
    final int[] bucketIds = new int[nodeIdUpperBound];
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      bucketIds[v] = provider.getRID(v).getBucketId();
    }
    return bucketIds;
  }

  /**
   * Fallback for anti-join patterns where neither endpoint is at position 0.
   * Uses dense propagation + per-node anti-join checking.
   */
  private long executeGenericAntiJoin(final Database db, final WorkGuard guard) {
    // Fall back to OLTP for this rare case
    return executeOLTP(db, guard);
  }

  /**
   * Counts paths from a single anchor node through the chain, applying anti-join
   * at the later anti-join position, then propagating through remaining hops.
   * <p>
   * Handles two anti-join directions:
   * <ul>
   *   <li>Case A (Q9): anchor is the anti-join source → precomputed merge-scan</li>
   *   <li>Case B (Q8): anchor is the anti-join target → per-frontier binary search</li>
   * </ul>
   */
  private long countWithAntiJoin(final GraphTraversalProvider provider, final int anchorId,
      final int[] anchorAntiNbrs, final boolean anchorIsSource,
      final int checkPosition, final IntHashSet[] validBuckets, final int[] bucketIds,
      final NeighborView[] hopViews) {
    long count = 0;

    // Expand from anchor through hops [0, checkPosition) using NeighborViews
    int[] frontier = new int[]{anchorId};
    for (int h = 0; h < checkPosition; h++) {
      final NeighborView view = hopViews[h];
      int totalNext = 0;
      if (view != null) {
        for (final int nid : frontier)
          totalNext += view.degree(nid);
      } else {
        for (final int nid : frontier)
          totalNext += CSRCountUtils.hopNeighborIds(provider, nid, directions[h], edgeTypes[h]).length;
      }
      if (totalNext == 0)
        return 0;

      final int[] nextFrontier = new int[totalNext];
      int pos = 0;
      if (view != null) {
        final int[] nbrs = view.neighbors();
        for (final int nid : frontier)
          for (int j = view.offset(nid), end = view.offsetEnd(nid); j < end; j++)
            nextFrontier[pos++] = nbrs[j];
      } else {
        for (final int nid : frontier) {
          final int[] neighbors = CSRCountUtils.hopNeighborIds(provider, nid, directions[h], edgeTypes[h]);
          System.arraycopy(neighbors, 0, nextFrontier, pos, neighbors.length);
          pos += neighbors.length;
        }
      }

      // Apply type filter using pre-computed bucket IDs. An empty set is a label that matches nothing, so it
      // empties the frontier rather than being read as "no filter" (issue #5757).
      final IntHashSet midBuckets = validBuckets[h + 1];
      if (midBuckets != null) {
        int writePos = 0;
        for (int i = 0; i < pos; i++) {
          if (midBuckets.contains(bucketIds[nextFrontier[i]]))
            nextFrontier[writePos++] = nextFrontier[i];
        }
        frontier = Arrays.copyOf(nextFrontier, writePos);
      } else {
        frontier = pos < nextFrontier.length ? Arrays.copyOf(nextFrontier, pos) : nextFrontier;
      }
    }

    // frontier now contains nodes at checkPosition.
    // Apply anti-join and inequality filters, then compute tail.
    if (anchorIsSource) {
      // Case A (Q9): anchor is anti-join source. Exclude frontier nodes that are
      // in anchor's anti-join neighbors. Use binary search on the sorted anti-join
      // neighbor array (the frontier is NOT sorted since it's concatenated from
      // multiple source expansions).
      for (final int target : frontier) {
        // Inequality check (anchor at position 0, target at checkPosition)
        if (inequalityIdxA >= 0 && inequalityIdxB >= 0
            && isInequalityViolation(anchorId, target, 0, checkPosition))
          continue;

        // Anti-join: check if target is in anchor's neighbors (via antiJoinDirection).
        // The anchorAntiNbrs array is sorted (from getNeighborIds with sorted merge).
        if (Arrays.binarySearch(anchorAntiNbrs, target) >= 0)
          continue;

        count += computeTailCount(provider, target, validBuckets, bucketIds);
      }
    } else {
      // Case B (Q8): anchor is anti-join target. For each frontier node, check
      // whether it has an anti-join edge to the anchor. Use pre-fetched NeighborView
      // + binary search on shared neighbors[] array to avoid per-node int[] allocation.
      final NeighborView antiView = CSRCountUtils.patternView(provider, antiJoinDirection, antiJoinEdgeType);
      if (antiView != null) {
        final int[] antiNbrs = antiView.neighbors();
        for (final int frontierNode : frontier) {
          if (inequalityIdxA >= 0 && inequalityIdxB >= 0
              && isInequalityViolation(anchorId, frontierNode, 0, checkPosition))
            continue;
          // Binary search for anchorId in frontierNode's sorted anti-join neighbor range
          final int aStart = antiView.offset(frontierNode);
          final int aEnd = antiView.offsetEnd(frontierNode);
          if (Arrays.binarySearch(antiNbrs, aStart, aEnd, anchorId) >= 0)
            continue; // anti-join hit — exclude
          count += computeTailCount(provider, frontierNode, validBuckets, bucketIds);
        }
      } else {
        for (final int frontierNode : frontier) {
          if (inequalityIdxA >= 0 && inequalityIdxB >= 0
              && isInequalityViolation(anchorId, frontierNode, 0, checkPosition))
            continue;
          final int[] frontierAntiNbrs = CSRCountUtils.hopNeighborIds(provider, frontierNode,
              antiJoinDirection, antiJoinEdgeType);
          if (Arrays.binarySearch(frontierAntiNbrs, anchorId) >= 0)
            continue;
          count += computeTailCount(provider, frontierNode, validBuckets, bucketIds);
        }
      }
    }
    return count;
  }

  /**
   * Checks if the (anchor, target) pair violates the inequality constraint.
   */
  private boolean isInequalityViolation(final int anchorId, final int targetId,
      final int anchorPos, final int targetPos) {
    return ((inequalityIdxA == anchorPos && inequalityIdxB == targetPos)
        || (inequalityIdxA == targetPos && inequalityIdxB == anchorPos))
        && anchorId == targetId;
  }

  /**
   * Computes the count contribution from the "tail" of the chain after the check position.
   * The check position is the later of the two anti-join endpoints.
   * For single remaining hops, uses O(1) degree lookup.
   */
  private long computeTailCount(final GraphTraversalProvider provider, final int nodeId,
      final IntHashSet[] validBuckets, final int[] bucketIds) {
    final int checkPos = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);
    long tailCount = 1;
    for (int h = checkPos; h < edgeTypes.length; h++) {
      final IntHashSet targetBuckets = validBuckets[h + 1];
      long degree = 0;
      if (targetBuckets == null)
        degree = CSRCountUtils.hopDegree(provider, nodeId, directions[h], edgeTypes[h]);
      else
        // the label of the hop's target node filters the edges it counts
        for (final int target : CSRCountUtils.hopNeighborIds(provider, nodeId, directions[h], edgeTypes[h]))
          if (targetBuckets.contains(bucketIds[target]))
            degree++;
      if (degree == 0)
        return 0;
      tailCount *= degree;
    }
    return tailCount;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    if (isOverlapShape())
      return executeOverlapOLTP(db, guard);
    final String anchorLabel = nodeLabels[0];
    final int hops = edgeTypes.length;
    final int checkPos = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);
    final boolean anchorIsSource = antiJoinSourceIdx == 0;

    // Verify all labels up to checkPos are defined (needed for map construction). An unlabelled position - the
    // anchor's included, since issue #5757 - is walked by the recursive path, which needs no per-label map.
    for (int i = 0; i <= checkPos; i++) {
      if (nodeLabels[i] == null || !db.getSchema().existsType(nodeLabels[i]))
        return executeOLTPRecursive(db, guard);
    }

    // Pre-compute valid bucket IDs for type filtering at each position
    final IntHashSet[] validBuckets = new IntHashSet[hops + 1];
    for (int i = 0; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    // Phase 1: Build neighbor RID maps for hops 0..checkPos-1.
    // Each map: vertex RID → RID[] of neighbors (filtered by target label buckets).
    @SuppressWarnings("unchecked")
    final Map<RID, RID[]>[] hopMaps = new Map[checkPos];
    for (int h = 0; h < checkPos; h++) {
      boolean reused = false;
      for (int prev = 0; prev < h; prev++) {
        if (Objects.equals(nodeLabels[h], nodeLabels[prev])
            && Objects.equals(edgeTypes[h], edgeTypes[prev])
            && directions[h] == directions[prev]
            // THE MAP KEEPS ONLY TARGETS OF THE NEXT POSITION'S LABEL, SO THE TARGET LABEL MUST MATCH TOO (issue #9277)
            && Objects.equals(nodeLabels[h + 1], nodeLabels[prev + 1])) {
          hopMaps[h] = hopMaps[prev];
          reused = true;
          break;
        }
      }
      if (!reused)
        hopMaps[h] = buildNeighborRIDMap(db, nodeLabels[h], edgeTypes[h], directions[h], validBuckets[h + 1], guard);
    }

    // Build anti-join neighbor map (reuse hop map if parameters match; hopMaps[0] keeps only targets of nodeLabels[1], hence the label test)
    final Map<RID, RID[]> antiJoinMap;
    if (anchorIsSource && checkPos > 0 && antiJoinEdgeType.equals(edgeTypes[0]) && antiJoinDirection == directions[0]
        && Objects.equals(nodeLabels[antiJoinTargetIdx], nodeLabels[1]))
      antiJoinMap = hopMaps[0];
    else if (anchorIsSource)
      antiJoinMap = buildNeighborRIDMap(db, anchorLabel, antiJoinEdgeType, antiJoinDirection,
          validBuckets[antiJoinTargetIdx], guard);
    else
      antiJoinMap = null;

    // Phase 2: Pre-compute tail counts for vertices at checkPos.
    // tailCount[rid] = product of countEdges for hops checkPos..end.
    final Map<RID, Long> tailCounts;
    if (checkPos < hops)
      tailCounts = buildTailCounts(db, nodeLabels[checkPos], checkPos, guard);
    else
      tailCounts = Collections.emptyMap();

    // Phase 3: Frontier expansion from each anchor.
    final RID[] EMPTY = new RID[0];
    long totalCount = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      guard.check();
      final RID anchorRid = it.next().getIdentity();

      // Get anti-join neighbors for this anchor
      final Set<RID> antiJoinSet;
      if (anchorIsSource) {
        final RID[] antiNbrs = antiJoinMap != null ? antiJoinMap.getOrDefault(anchorRid, EMPTY) : EMPTY;
        antiJoinSet = new HashSet<>(antiNbrs.length * 2);
        Collections.addAll(antiJoinSet, antiNbrs);
      } else {
        antiJoinSet = null;
      }

      // Expand frontier through hops 0..checkPos-1
      RID[] frontier = hopMaps[0] != null ? hopMaps[0].getOrDefault(anchorRid, EMPTY) : EMPTY;
      for (int h = 1; h < checkPos; h++) {
        final Map<RID, RID[]> map = hopMaps[h];
        int totalSize = 0;
        for (final RID rid : frontier)
          totalSize += map.getOrDefault(rid, EMPTY).length;
        if (totalSize == 0) {
          frontier = EMPTY;
          break;
        }
        final RID[] nextFrontier = new RID[totalSize];
        int pos = 0;
        for (final RID rid : frontier) {
          final RID[] nbrs = map.getOrDefault(rid, EMPTY);
          System.arraycopy(nbrs, 0, nextFrontier, pos, nbrs.length);
          pos += nbrs.length;
        }
        frontier = nextFrontier;
      }

      // Apply anti-join, inequality, and compute tail count
      for (final RID target : frontier) {
        // Inequality check
        if (inequalityIdxA >= 0 && inequalityIdxB >= 0
            && ((inequalityIdxA == 0 && inequalityIdxB == checkPos) || (inequalityIdxA == checkPos && inequalityIdxB == 0))
            && anchorRid.equals(target))
          continue;

        if (anchorIsSource) {
          if (antiJoinSet.contains(target))
            continue;
        } else {
          // Case B: anchor is anti-join target — check if frontier node has edge to anchor
          if (target.asVertex().isConnectedTo(anchorRid.asVertex(), antiJoinDirection, antiJoinEdgeType))
            continue;
        }

        if (checkPos < hops)
          totalCount += tailCounts.getOrDefault(target, 0L);
        else
          totalCount++;
      }
    }

    return totalCount;
  }

  /**
   * Builds a map: vertex RID → RID[] of neighbors for the given edge type and direction.
   * Uses the RID-only iterator to avoid loading neighbor vertex records from disk.
   */
  private Map<RID, RID[]> buildNeighborRIDMap(final Database db, final String sourceLabel,
      final String edgeType, final Vertex.DIRECTION direction, final IntHashSet targetBuckets,
      final WorkGuard guard) {
    final Map<RID, RID[]> map = new HashMap<>();
    if (sourceLabel == null || !db.getSchema().existsType(sourceLabel))
      return map;

    // Try GAV provider for accelerated neighbor lookups. One holding a subset of the vertex types is missing every
    // edge that leaves the view, so it cannot answer for the vertices it does map (issue #5757).
    final GraphTraversalProvider gavProvider = CSRCountUtils.findAcceleratingProvider(db, edgeType);

    for (final Iterator<? extends Identifiable> it = db.iterateType(sourceLabel, true); it.hasNext(); ) {
      guard.check();
      final RID vertexRid = it.next().getIdentity();
      final List<RID> neighbors = new ArrayList<>();

      if (gavProvider != null) {
        final int nodeId = gavProvider.getNodeId(vertexRid);
        if (nodeId >= 0) {
          for (final int nid : CSRCountUtils.hopNeighborIds(gavProvider, nodeId, direction, edgeType)) {
            final RID rid = gavProvider.getRID(nid);
            if (rid != null && (targetBuckets == null || targetBuckets.contains(rid.getBucketId())))
              neighbors.add(rid);
          }
          if (!neighbors.isEmpty())
            map.put(vertexRid, neighbors.toArray(new RID[0]));
          continue;
        }
      }
      // OLTP fallback
      final Vertex v = (Vertex) db.lookupByRID(vertexRid, true);
      for (final RID rid : SelfLoops.connectedVertexRIDs(v, direction, edgeType)) {
        if (targetBuckets == null || targetBuckets.contains(rid.getBucketId()))
          neighbors.add(rid);
      }
      if (!neighbors.isEmpty())
        map.put(vertexRid, neighbors.toArray(new RID[0]));
    }
    return map;
  }

  /**
   * Pre-computes the tail count (product of edge degrees for remaining hops) for each vertex.
   */
  private Map<RID, Long> buildTailCounts(final Database db, final String sourceLabel, final int fromHop,
      final WorkGuard guard) {
    final Map<RID, Long> counts = new HashMap<>();
    if (sourceLabel == null || !db.getSchema().existsType(sourceLabel))
      return counts;

    // Try GAV provider for accelerated degree counting. See findAcceleratingProvider: a partial view undercounts
    // the degree of every vertex it does map (issue #5757).
    final GraphTraversalProvider gavProvider = CSRCountUtils.findAcceleratingProvider(db, edgeTypes);
    // the label of a hop's target node filters the edges that hop counts
    final IntHashSet[] targetBuckets = new IntHashSet[edgeTypes.length];
    for (int h = fromHop; h < edgeTypes.length; h++)
      targetBuckets[h] = CSRCountUtils.buildValidBuckets(db, nodeLabels[h + 1]);

    for (final Iterator<? extends Identifiable> it = db.iterateType(sourceLabel, true); it.hasNext(); ) {
      guard.check();
      final RID vertexRid = it.next().getIdentity();
      long tailCount = 1;
      for (int h = fromHop; h < edgeTypes.length; h++) {
        final long degree;
        final int nodeId = gavProvider != null && targetBuckets[h] == null ? gavProvider.getNodeId(vertexRid) : -1;
        if (nodeId >= 0)
          degree = CSRCountUtils.hopDegree(gavProvider, nodeId, directions[h], edgeTypes[h]);
        else
          degree = targetDegree((Vertex) db.lookupByRID(vertexRid, true), directions[h], edgeTypes[h], targetBuckets[h]);
        if (degree == 0) {
          tailCount = 0;
          break;
        }
        tailCount *= degree;
      }
      if (tailCount > 0)
        counts.put(vertexRid, tailCount);
    }
    return counts;
  }

  /**
   * Recursive fallback for patterns where labels are missing or anti-join endpoints not at position 0.
   * Includes tail count optimization to avoid loading vertices at the last hops.
   */
  private long executeOLTPRecursive(final Database db, final WorkGuard guard) {
    // An unlabelled anchor starts from every vertex in the schema (issue #5757).
    final IntHashSet[] hopBuckets = new IntHashSet[edgeTypes.length + 1];
    for (int i = 0; i < hopBuckets.length; i++)
      hopBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);
    long total = 0;
    for (final Iterator<? extends Identifiable> it = CSRCountUtils.iterateAnchors(db, nodeLabels[0]); it.hasNext(); ) {
      guard.check();
      final Vertex anchor = it.next().asVertex();
      final Set<RID> antiJoinSet = new HashSet<>();
      for (final RID rid : SelfLoops.connectedVertexRIDs(anchor, antiJoinDirection, antiJoinEdgeType))
        antiJoinSet.add(rid);

      total += countPathsRec(anchor, 0, db, anchor.getIdentity(), antiJoinSet, hopBuckets);
    }
    return total;
  }

  private long countPathsRec(final Vertex vertex, final int hopIndex, final Database db,
      final RID sourceRid, final Set<RID> antiJoinSet, final IntHashSet[] hopBuckets) {
    if (hopIndex >= edgeTypes.length)
      return 1;

    final int checkPos = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);

    // Tail count optimization: after the anti-join check position, use degree multiplication
    if (hopIndex >= checkPos) {
      long tailCount = 1;
      for (int h = hopIndex; h < edgeTypes.length; h++) {
        final long degree = targetDegree(vertex, directions[h], edgeTypes[h], hopBuckets[h + 1]);
        if (degree == 0)
          return 0;
        tailCount *= degree;
      }
      return tailCount;
    }

    final IntHashSet targetBuckets = hopBuckets[hopIndex + 1];

    long count = 0;
    for (final RID neighborRid : SelfLoops.connectedVertexRIDs(vertex, directions[hopIndex], edgeTypes[hopIndex])) {
      if (targetBuckets != null && !targetBuckets.contains(neighborRid.getBucketId()))
        continue;
      if (hopIndex + 1 == antiJoinTargetIdx && antiJoinSet.contains(neighborRid))
        continue;
      if (inequalityIdxA >= 0 && inequalityIdxB >= 0
          && ((inequalityIdxA == antiJoinSourceIdx && (hopIndex + 1) == inequalityIdxB)
              || (inequalityIdxB == antiJoinSourceIdx && (hopIndex + 1) == inequalityIdxA))
          && neighborRid.equals(sourceRid))
        continue;
      count += countPathsRec(neighborRid.asVertex(), hopIndex + 1, db, sourceRid, antiJoinSet, hopBuckets);
    }
    return count;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("+ COUNT ANTI-JOIN CHAIN (CSR merge-scan anti-join)\n");
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("  chain: ");
    for (int i = 0; i < edgeTypes.length; i++) {
      if (i > 0)
        sb.append(" → ");
      sb.append("(").append(nodeLabels[i] != null ? nodeLabels[i] : "?").append(")");
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
      sb.append(edgeTypes[i]);
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "]->" : "]-");
    }
    sb.append("(").append(nodeLabels[edgeTypes.length] != null ? nodeLabels[edgeTypes.length] : "?").append(")");
    sb.append("\n").append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("  anti-join: NOT (").append(antiJoinSourceIdx).append(")-[:").append(antiJoinEdgeType)
        .append("]-(").append(antiJoinTargetIdx).append(")");
    return sb.toString();
  }
}
