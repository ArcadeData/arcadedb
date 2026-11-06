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
package com.arcadedb.index.columnar;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MergePolicyTest {
  /** Segment ids 0..n-1 with the given row counts and dead rows. */
  private static List<ColumnarManifest.SegmentEntry> segments(final long[] rows, final long[] dead) {
    final List<ColumnarManifest.SegmentEntry> result = new ArrayList<>();
    for (int i = 0; i < rows.length; i++)
      result.add(new ColumnarManifest.SegmentEntry(i, SegmentFormat.segmentFileName(i), rows[i], dead[i] == 0 ? 0 : 1, dead[i], 0, 0));
    return result;
  }

  @Test
  void deadRatioSelectsSingleSegment() {
    // 29/100 stays under the 0.3 ratio, 30/100 reaches it: the boundary is inclusive
    assertThat(MergePolicy.select(segments(new long[] { 100, 100, 100 }, new long[] { 29, 30, 50 }), 0.3f, 10)).containsExactly(1L);
    assertThat(MergePolicy.select(segments(new long[] { 100, 100 }, new long[] { 29, 0 }), 0.3f, 10)).isEmpty();
  }

  @Test
  void deadRatioWinsOverTooManySegments() {
    final long[] rows = new long[12];
    final long[] dead = new long[12];
    Arrays.fill(rows, 10);
    dead[7] = 9;
    assertThat(MergePolicy.select(segments(rows, dead), 0.3f, 10)).containsExactly(7L);
  }

  @Test
  void tooManySegmentsSelectsSmallestAdjacentRun() {
    // run of 10 / 2 + 1 = 6: the windows sum 282, 184, 95, 21, 86, 142, 189, so ids 3..8
    final long[] rows = { 100, 90, 80, 5, 4, 3, 2, 1, 6, 70, 60, 50 };
    assertThat(MergePolicy.select(segments(rows, new long[12]), 0.3f, 10)).containsExactly(3L, 4L, 5L, 6L, 7L, 8L);
    // the smallest run at the very end is found too (no off-by-one on the last window)
    final long[] tail = { 100, 90, 80, 70, 60, 50, 40, 1, 1, 1, 1, 1, 1 };
    assertThat(MergePolicy.select(segments(tail, new long[13]), 0.3f, 10)).containsExactly(7L, 8L, 9L, 10L, 11L, 12L);
  }

  @Test
  void healthySetSelectsNothing() {
    final long[] rows = new long[10];
    Arrays.fill(rows, 1000);
    // exactly maxSegments segments is not too many
    assertThat(MergePolicy.select(segments(rows, new long[10]), 0.3f, 10)).isEmpty();
    assertThat(MergePolicy.select(List.of(), 0.3f, 10)).isEmpty();
    // an empty segment never divides by zero
    assertThat(MergePolicy.select(segments(new long[] { 0 }, new long[] { 0 }), 0.3f, 10)).isEmpty();
  }
}
