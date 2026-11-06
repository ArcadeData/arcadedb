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

import java.util.List;

/**
 * Chooses the segments of one projection that the next merge rewrites ({@link ColumnarFlusher#merge}). A segment with too many dead
 * rows is rewritten alone, which drops them; otherwise a projection holding too many segments merges its cheapest adjacent run, so
 * the segment count stays bounded while the bytes rewritten per merge stay small.
 */
final class MergePolicy {
  private static final long[] NOTHING = new long[0];

  private MergePolicy() {
  }

  /**
   * @param segments    the manifest's segments, oldest first
   * @param deadRatio   the dead fraction (dead rows / rows) at which a segment is rewritten alone, {@code arcadedb.columnar.mergeDeadRatio}
   * @param maxSegments the segment count above which a run is merged, {@code arcadedb.columnar.mergeMaxSegments}
   *
   * @return the ids of the segments to merge, in manifest order: the first segment with at least one dead row and
   * {@code deadRows / rows >= deadRatio}; else, with more than {@code maxSegments} segments, the adjacent run of
   * {@code maxSegments / 2 + 1} segments (at least 2) with the fewest rows, the oldest run on a tie; else empty
   */
  static long[] select(final List<ColumnarManifest.SegmentEntry> segments, final float deadRatio, final int maxSegments) {
    for (final ColumnarManifest.SegmentEntry segment : segments)
      // in float, as the ratio is configured: 30 dead of 100 rows reaches 0.3f
      if (segment.deadRows() > 0 && (float) segment.deadRows() / segment.rows() >= deadRatio)
        return new long[] { segment.segmentId() };

    final int count = segments.size();
    if (count <= maxSegments)
      return NOTHING;
    final int run = Math.min(count, Math.max(2, maxSegments / 2 + 1));
    long sum = 0;
    for (int i = 0; i < run; i++)
      sum += segments.get(i).rows();
    long best = sum;
    int bestStart = 0;
    for (int start = 1; start + run <= count; start++) {
      sum += segments.get(start + run - 1).rows() - segments.get(start - 1).rows();
      if (sum < best) {
        best = sum;
        bestStart = start;
      }
    }
    final long[] ids = new long[run];
    for (int i = 0; i < run; i++)
      ids[i] = segments.get(bestStart + i).segmentId();
    return ids;
  }
}
