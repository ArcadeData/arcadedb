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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
