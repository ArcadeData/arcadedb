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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A page that is written at both ends (the layout of every index and bucket page) must be tracked as the disjoint
 * intervals it really modified, not as the hull of the first and the last byte: the WAL - and with it the Raft entry
 * that ships a transaction to the replicas - carries one segment per interval, and charging it the whole page made
 * the size of a transaction depend on the number of pages it touched instead of on the data it wrote (issue #5470).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MutablePageModifiedRangesTest extends TestHelper {

  private static final int PAGE_SIZE = 4_096;

  @Test
  void distantWritesAreTrackedAsSeparateIntervals() {
    final MutablePage page = newPage();

    page.writeInt(0, 42);
    page.writeInt(3_000, 43);

    assertThat(page.getModifiedRangeCount()).isEqualTo(2);
    assertThat(intervals(page)).containsExactly(
        BasePage.PAGE_HEADER_SIZE, BasePage.PAGE_HEADER_SIZE + 3,
        BasePage.PAGE_HEADER_SIZE + 3_000, BasePage.PAGE_HEADER_SIZE + 3_003);

    // The hull is still what it always was, for the callers that only need "where did this page change".
    assertThat(page.getModifiedRange()).containsExactly(BasePage.PAGE_HEADER_SIZE, BasePage.PAGE_HEADER_SIZE + 3_003);
  }

  @Test
  void adjacentAndOverlappingWritesAreCoalesced() {
    final MutablePage page = newPage();

    page.writeInt(100, 1);
    page.writeInt(104, 2);   // adjacent to the previous one
    page.writeInt(102, 3);   // overlapping both

    assertThat(page.getModifiedRangeCount()).isEqualTo(1);
    assertThat(intervals(page)).containsExactly(BasePage.PAGE_HEADER_SIZE + 100, BasePage.PAGE_HEADER_SIZE + 107);
  }

  @Test
  void aWriteBridgingTwoIntervalsMergesThem() {
    final MutablePage page = newPage();

    page.writeInt(0, 1);
    page.writeInt(2_000, 2);
    page.writeByteArray(0, new byte[2_100]);

    assertThat(page.getModifiedRangeCount()).isEqualTo(1);
    assertThat(intervals(page)).containsExactly(BasePage.PAGE_HEADER_SIZE, BasePage.PAGE_HEADER_SIZE + 2_099);
  }

  /**
   * Past the interval budget the two closest intervals are merged, so the tracking only ever gets coarser: the
   * gap between them is shipped needlessly, nothing is ever left out.
   */
  @Test
  void exceedingTheBudgetMergesTheClosestIntervalsAndNeverLosesAWrite() {
    final MutablePage page = newPage();

    final int[] offsets = { 0, 400, 800, 1_200, 1_600, 2_000, 2_400, 2_800, 2_810 };
    for (final int offset : offsets)
      page.writeInt(offset, offset);

    assertThat(page.getModifiedRangeCount()).isLessThanOrEqualTo(8);

    for (final int offset : offsets) {
      final int from = BasePage.PAGE_HEADER_SIZE + offset;
      assertThat(covers(page, from, from + 3)).as("write at %d must still be covered", offset).isTrue();
    }
  }

  /**
   * A page with two disjoint intervals is serialized as two segments of the same page, and applying the segments in
   * order over the previous image of the page reproduces it byte for byte - which is exactly what recovery and the
   * followers do.
   */
  @Test
  void theWalBufferShipsOneSegmentPerIntervalAndRoundTrips() {
    final MutablePage page = newPage();

    // Content region of a page written at both ends: a header/pointer area at the top, the payload at the tail.
    final byte[] head = "pointer-array".getBytes();
    final byte[] tail = "key-value-content".getBytes();
    page.writeByteArray(0, head);
    page.writeByteArray(PAGE_SIZE - BasePage.PAGE_HEADER_SIZE - tail.length, tail);

    final Binary buffer = WALFile.writeTransactionToBuffer(List.of(page), 1L);

    final List<int[]> segments = new ArrayList<>();
    final byte[] replayed = new byte[PAGE_SIZE];
    final ByteBuffer reader = ByteBuffer.wrap(buffer.toByteArray());
    reader.getLong();  // txId
    reader.getLong();  // timestamp
    final int segmentCount = reader.getInt();
    reader.getInt();   // segment size

    for (int i = 0; i < segmentCount; i++) {
      reader.getInt(); // fileId
      reader.getInt(); // page number
      final int from = reader.getInt();
      final int to = reader.getInt();
      reader.getInt(); // version
      reader.getInt(); // content size
      reader.get(replayed, from, to - from + 1);
      segments.add(new int[] { from, to });
    }

    assertThat(segmentCount).as("one segment per modified interval").isEqualTo(2);
    // The whole point: the two segments together are a fraction of the page they belong to.
    assertThat(segments.get(0)[1] - segments.get(0)[0] + segments.get(1)[1] - segments.get(1)[0] + 2)
        .isLessThan(PAGE_SIZE / 4);

    final byte[] expected = page.getContent().array();
    for (final int[] segment : segments)
      for (int i = segment[0]; i <= segment[1]; i++)
        assertThat(replayed[i]).as("byte %d", i).isEqualTo(expected[i]);
  }

  /**
   * Whatever the sequence of writes, the tracked intervals stay sorted, disjoint and non-adjacent, never fewer than the bytes
   * written need, and never more than the budget. Within the budget they are EXACTLY the maximal runs of written bytes (nothing
   * extra is shipped to the WAL); the write that goes over it merges one adjacent pair, one with the smallest gap, and nothing else.
   */
  @Test
  void randomWritesTrackTheMaximalRunsAndMergeOnlyTheClosestPairPastTheBudget() {
    for (int seed = 0; seed < 300; seed++) {
      final Random random = new Random(seed);
      final MutablePage page = newPage();
      final BitSet written = new BitSet();
      final int maxLength = seed % 3 == 0 ? 400 : 12;

      for (int step = 0; step < 60; step++) {
        final int start = random.nextInt(PAGE_SIZE);
        final int end = Math.min(PAGE_SIZE - 1, start + random.nextInt(maxLength));

        final List<int[]> expected = absorb(snapshot(page), start, end);
        page.updateModifiedRange(start, end);
        written.set(start, end + 1);
        final List<int[]> actual = snapshot(page);

        if (expected.size() <= 8)
          assertThat(actual).as("seed %d step %d: write [%d,%d]", seed, step, start, end).usingRecursiveComparison().isEqualTo(expected);
        else
          assertThat(mergeOneClosestPair(expected)).as("seed %d step %d: write [%d,%d] went over the budget", seed, step, start, end)
              .anySatisfy(candidate -> assertThat(actual).usingRecursiveComparison().isEqualTo(candidate));

        assertThat(covers(page, start, end)).as("seed %d step %d: the write [%d,%d] is not tracked", seed, step, start, end).isTrue();
      }

      for (int b = written.nextSetBit(0); b >= 0; b = written.nextSetBit(b + 1))
        assertThat(covers(page, b, b)).as("seed %d: byte %d is written but not tracked", seed, b).isTrue();
    }
  }

  @Test
  void aZeroLengthWriteTracksNothingAndAnInvertedOrOutOfPageRangeIsRefused() {
    final MutablePage page = newPage();

    page.updateModifiedRange(100, 99);
    page.updateModifiedRange(PAGE_SIZE, PAGE_SIZE - 1);
    assertThat(page.getModifiedRangeCount()).as("a zero-length write dirties no byte").isZero();

    page.updateModifiedRange(0, 0);
    page.updateModifiedRange(PAGE_SIZE - 1, PAGE_SIZE - 1);
    assertThat(intervals(page)).as("the first and the last byte of the page are valid").containsExactly(0, 0, PAGE_SIZE - 1, PAGE_SIZE - 1);

    assertThatThrownBy(() -> page.updateModifiedRange(-1, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> page.updateModifiedRange(PAGE_SIZE - 1, PAGE_SIZE)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> page.updateModifiedRange(10, 8)).isInstanceOf(IllegalArgumentException.class);
    assertThat(page.getModifiedRangeCount()).as("a refused range leaves the tracking as it was").isEqualTo(2);
  }

  private static List<int[]> snapshot(final MutablePage page) {
    final List<int[]> intervals = new ArrayList<>();
    final int[] ranges = page.getModifiedRanges();
    for (int i = 0; i < page.getModifiedRangeCount(); i++)
      intervals.add(new int[] { ranges[i * 2], ranges[i * 2 + 1] });
    return intervals;
  }

  /** The sorted, disjoint, non-adjacent intervals that result from marking [start,end] as written on top of {@code intervals}. */
  private static List<int[]> absorb(final List<int[]> intervals, final int start, final int end) {
    final List<int[]> result = new ArrayList<>();
    int from = start;
    int to = end;
    boolean placed = false;
    for (final int[] interval : intervals) {
      if (interval[1] < from - 1)
        result.add(interval);
      else if (interval[0] > to + 1) {
        if (!placed) {
          result.add(new int[] { from, to });
          placed = true;
        }
        result.add(interval);
      } else {
        from = Math.min(from, interval[0]);
        to = Math.max(to, interval[1]);
      }
    }
    if (!placed)
      result.add(new int[] { from, to });
    return result;
  }

  /** Every layout that merges one adjacent pair of {@code intervals} with the smallest gap between them. */
  private static List<List<int[]>> mergeOneClosestPair(final List<int[]> intervals) {
    int smallestGap = Integer.MAX_VALUE;
    for (int k = 0; k < intervals.size() - 1; k++)
      smallestGap = Math.min(smallestGap, intervals.get(k + 1)[0] - intervals.get(k)[1]);

    final List<List<int[]>> candidates = new ArrayList<>();
    for (int k = 0; k < intervals.size() - 1; k++)
      if (intervals.get(k + 1)[0] - intervals.get(k)[1] == smallestGap) {
        final List<int[]> merged = new ArrayList<>(intervals);
        merged.set(k, new int[] { intervals.get(k)[0], intervals.get(k + 1)[1] });
        merged.remove(k + 1);
        candidates.add(merged);
      }
    return candidates;
  }

  private MutablePage newPage() {
    return new MutablePage(new PageId((DatabaseInternal) database, 1, 0), PAGE_SIZE, new byte[PAGE_SIZE], 0,
        PAGE_SIZE - BasePage.PAGE_HEADER_SIZE);
  }

  /**
   * The tracked intervals flattened as [from0,to0, from1,to1, ...].
   */
  private static int[] intervals(final MutablePage page) {
    return Arrays.copyOf(page.getModifiedRanges(), page.getModifiedRangeCount() * 2);
  }

  private static boolean covers(final MutablePage page, final int from, final int to) {
    final int[] ranges = page.getModifiedRanges();
    for (int i = 0; i < page.getModifiedRangeCount(); i++)
      if (ranges[i * 2] <= from && ranges[i * 2 + 1] >= to)
        return true;
    return false;
  }
}
