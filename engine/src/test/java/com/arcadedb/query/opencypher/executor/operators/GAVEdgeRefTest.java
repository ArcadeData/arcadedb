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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The invariant relationship uniqueness on a Graph Analytical View rests on (issue #8394): the occurrence a hop gives a
 * parallel relationship is only a number in a bijection local to the adjacency slice it walked, and hops walking
 * differently ordered slices of the same parallel relationships still yield exactly the count of distinct-edge
 * assignments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GAVEdgeRefTest {
  private static final RID OUT = new RID(1, 1);
  private static final RID IN  = new RID(1, 2);

  @Test
  void independentlyOrderedSlicesYieldTheDistinctEdgeAssignmentCount() {
    final Random random = new Random(8394);
    for (int parallel = 1; parallel <= 6; parallel++)
      for (int trial = 0; trial < 20; trial++) {
        // One out-slice and one in-slice holding the same parallel relationships among other neighbours, each ordered
        // on its own: only the number of equal entries is shared
        final int[] outSlice = sliceWith(parallel, 2, random);
        final int[] inSlice = sliceWith(parallel, 1, random);

        // Two hops: first from the out-slice, then from the in-slice
        assertThat(countAccepted(outSlice, 2, inSlice, 1, null, 0)).isEqualTo((long) parallel * (parallel - 1));
        // Three hops alternating slices
        assertThat(countAccepted(outSlice, 2, inSlice, 1, outSlice, 2))
            .isEqualTo((long) parallel * (parallel - 1) * (parallel - 2));
      }
  }

  @Test
  void aSortedSliceIsRankedByItsRunOnly() {
    final int[] sorted = { 1, 3, 3, 3, 7, 7, 9 };
    assertThat(GAVEdgeRef.isSorted(sorted)).isTrue();
    for (int i = 0; i < sorted.length; i++)
      assertThat(GAVEdgeRef.rankInSlice(sorted, i, true)).isEqualTo(GAVEdgeRef.rankInSlice(sorted, i, false));
    final int[] unsorted = { 3, 1, 3, 7, 3 };
    assertThat(GAVEdgeRef.isSorted(unsorted)).isFalse();
    assertThat(GAVEdgeRef.rankInSlice(unsorted, 4, false)).isEqualTo(2);
  }

  /** Counts the tuples of relationships the hops accept, each refusing a number a previous hop bound. */
  private static long countAccepted(final int[] first, final int firstValue, final int[] second, final int secondValue,
      final int[] third, final int thirdValue) {
    final boolean firstSorted = GAVEdgeRef.isSorted(first);
    final boolean secondSorted = GAVEdgeRef.isSorted(second);
    long count = 0;
    for (int i = 0; i < first.length; i++) {
      if (first[i] != firstValue)
        continue;
      final GAVEdgeRef a = GAVEdgeRef.inSlice("K", OUT, IN, first, i, firstSorted);
      for (int j = 0; j < second.length; j++) {
        if (second[j] != secondValue || GAVEdgeRef.conflicts(new GAVEdgeRef[] { a }, "K", OUT, IN, second, j, secondSorted))
          continue;
        if (third == null) {
          ++count;
          continue;
        }
        final GAVEdgeRef b = GAVEdgeRef.inSlice("K", OUT, IN, second, j, secondSorted);
        for (int k = 0; k < third.length; k++)
          if (third[k] == thirdValue
              && !GAVEdgeRef.conflicts(new GAVEdgeRef[] { a, b }, "K", OUT, IN, third, k, GAVEdgeRef.isSorted(third)))
            ++count;
      }
    }
    return count;
  }

  /** A slice with {@code parallel} entries equal to {@code value} among others, sorted or shuffled at random. */
  private static int[] sliceWith(final int parallel, final int value, final Random random) {
    final int others = random.nextInt(5);
    final int[] slice = new int[parallel + others];
    for (int i = 0; i < parallel; i++)
      slice[i] = value;
    for (int i = 0; i < others; i++)
      slice[parallel + i] = 10 + random.nextInt(5);
    if (random.nextBoolean())
      Arrays.sort(slice);
    else
      for (int i = slice.length - 1; i > 0; i--) {
        final int j = random.nextInt(i + 1);
        final int t = slice[i];
        slice[i] = slice[j];
        slice[j] = t;
      }
    return slice;
  }
}
