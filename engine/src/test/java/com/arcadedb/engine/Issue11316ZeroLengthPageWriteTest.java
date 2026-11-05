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
import com.arcadedb.database.TrackableBinary;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11316: {@link MutablePage#updateModifiedRange}, the authority every write of a page passes
 * through, accepted the inverted interval {@code [start, start - 1]} that a zero-length write produces through both of
 * its callers ({@code checkBoundariesOnWrite} and {@code TrackableBinary.checkForAllocation}). The interval survived to
 * commit, where {@code WALFile.writeTransactionToBuffer} rejected it as a corrupt page and aborted the transaction,
 * naming the page instead of the write. A zero-length write now dirties nothing, and a genuinely inverted interval is
 * refused at the authority, where the faulty write is still on the stack.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11316ZeroLengthPageWriteTest extends TestHelper {

  private static final int PAGE_SIZE = 1_024;

  @Test
  void aZeroLengthWriteThroughThePageRecordsNothing() {
    final MutablePage page = newPage();
    page.writeInt(0, 1);

    page.writeByteArray(500, new byte[0]);
    page.writeByteArray(510, new byte[8], 3, 0);
    page.writeZeros(600, 0);

    assertThat(intervals(page)).containsExactly(BasePage.PAGE_HEADER_SIZE, BasePage.PAGE_HEADER_SIZE + 3);
    assertThat(WALFile.writeTransactionToBuffer(List.of(page), 1L)).isNotNull();
  }

  @Test
  void aZeroLengthWriteThroughTheTrackableRecordsNothing() {
    final MutablePage page = newPage();
    page.writeInt(0, 1);

    final TrackableBinary trackable = page.getTrackable();
    trackable.position(700);
    trackable.putByteArray(new byte[0]);
    trackable.putByteArray(710, new byte[0]);

    assertThat(intervals(page)).containsExactly(BasePage.PAGE_HEADER_SIZE, BasePage.PAGE_HEADER_SIZE + 3);
    final Binary wal = WALFile.writeTransactionToBuffer(List.of(page), 1L);
    assertThat(wal).isNotNull();
  }

  /** A page whose ONLY write was empty has nothing to ship, and nothing about how it was written changes either. */
  @Test
  void aPageWithOnlyAZeroLengthWriteStaysClean() {
    final MutablePage page = newPage();

    page.writeByteArray(500, new byte[0]);
    // the zero-length write at the very end of the page is in bounds, it touches nothing
    page.writeByteArray(page.getMaxContentSize(), new byte[0]);

    assertThat(page.getModifiedRangeCount()).isZero();
    assertThat(page.hasOnlyPackedWrites()).as("an empty write is not an unpacked write").isTrue();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_ALL_MERGES)).as("nor an undeclared one").isTrue();
  }

  /** A NEGATIVE length is a caller bug, not a no-op: refused where it happens, not at commit. */
  @Test
  void anInvertedIntervalIsRefusedAtTheAuthority() {
    final MutablePage page = newPage();

    assertThatThrownBy(() -> page.updateModifiedRange(100, 98)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> page.getTrackable().updateModifiedRange(100, 50)).isInstanceOf(IllegalArgumentException.class);
    assertThat(page.getModifiedRangeCount()).isZero();

    // the boundary itself: end == start - 1 is the zero-length write, accepted and dropped
    page.updateModifiedRange(100, 99);
    assertThat(page.getModifiedRangeCount()).isZero();
  }

  private MutablePage newPage() {
    return new MutablePage(new PageId((DatabaseInternal) database, 1, 0), PAGE_SIZE, new byte[PAGE_SIZE], 0,
        PAGE_SIZE - BasePage.PAGE_HEADER_SIZE);
  }

  private static int[] intervals(final MutablePage page) {
    return Arrays.copyOf(page.getModifiedRanges(), page.getModifiedRangeCount() * 2);
  }
}
