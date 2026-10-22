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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10346: the log a statement keeps of what it overwrote on a page its transaction had already modified. The page has to come
 * back byte for byte, with the bookkeeping the writes changed (content size, modified ranges), whatever the writes were.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageUndoLogTest extends TestHelper {
  private static final int PAGE_SIZE = 65536;

  private MutablePage newPage(final Random random) {
    final MutablePage page = new MutablePage(new PageId(database, 1, 0), PAGE_SIZE);
    page.setContentSize(1000);
    for (int i = 0; i < 20; i++)
      write(page, random);
    return page;
  }

  /** One random write, of any kind a page takes. */
  private static void write(final MutablePage page, final Random random) {
    final int max = page.getMaxContentSize() - 600;
    final int index = random.nextInt(max);
    switch (random.nextInt(8)) {
    case 0 -> page.writeInt(index, random.nextInt());
    case 1 -> page.writeLong(index, random.nextLong());
    case 2 -> page.writeByte(index, (byte) random.nextInt());
    case 3 -> {
      final byte[] bytes = new byte[1 + random.nextInt(500)];
      random.nextBytes(bytes);
      page.writeByteArray(index, bytes);
    }
    case 4 -> page.writeZeros(index, 1 + random.nextInt(500));
    case 5 -> page.move(index, random.nextInt(max), 1 + random.nextInt(500));
    case 6 -> {
      final Binary trackable = page.getTrackable();
      trackable.putInt(index, random.nextInt());
      trackable.move(index, random.nextInt(max), 1 + random.nextInt(300));
    }
    default -> page.setContentSize(random.nextInt(max));
    }
  }

  private static byte[] bytesOf(final MutablePage page) {
    return Arrays.copyOf(page.content.getContent(), page.content.getContent().length);
  }

  private static int[] rangesOf(final MutablePage page) {
    return Arrays.copyOf(page.getModifiedRanges(), page.getModifiedRangeCount() * 2);
  }

  @Test
  void aPageComesBackByteForByteWhateverWasWrittenToIt() {
    final Random random = new Random(10346);
    final PageUndoLog log = new PageUndoLog();

    for (int round = 0; round < 300; round++) {
      final MutablePage page = newPage(random);
      final byte[] bytes = bytesOf(page);
      final int[] ranges = rangesOf(page);
      final int contentSize = page.getContentSize();

      log.begin(page, null);
      for (int i = 0, writes = 1 + random.nextInt(60); i < writes; i++)
        write(page, random);
      log.restore();
      log.end();

      assertThat(bytesOf(page)).as("round %d: bytes", round).isEqualTo(bytes);
      assertThat(page.getContentSize()).as("round %d: content size", round).isEqualTo(contentSize);
      assertThat(rangesOf(page)).as("round %d: modified ranges", round).isEqualTo(ranges);
      assertThat(page.getUndoLog()).isNull();
    }
  }

  @Test
  void aGranuleIsLoggedOnceHoweverOftenItIsWritten() {
    final MutablePage page = newPage(new Random(1));
    final PageUndoLog log = new PageUndoLog();

    log.begin(page, null);
    for (int i = 0; i < 100; i++)
      page.writeLong(40, i);
    assertThat(log.getLoggedGranules()).isEqualTo(1);

    page.writeLong(40 + PageUndoLog.GRANULE_SIZE, 1L);
    assertThat(log.getLoggedGranules()).isEqualTo(2);

    // A WRITE ACROSS A GRANULE BOUNDARY LOGS BOTH SIDES
    page.writeLong(7 * PageUndoLog.GRANULE_SIZE - BasePage.PAGE_HEADER_SIZE - 4, 1L);
    assertThat(log.getLoggedGranules()).isEqualTo(4);
    log.end();
  }

  @Test
  void aLogLeavesThePageAloneOnceItIsEnded() {
    final MutablePage page = newPage(new Random(2));
    final PageUndoLog log = new PageUndoLog();

    log.begin(page, null);
    page.writeLong(40, 1L);
    log.end();
    page.writeLong(40 + 10 * PageUndoLog.GRANULE_SIZE, 2L);

    assertThat(log.getLoggedGranules()).isEqualTo(1);
    assertThat(page.getUndoLog()).isNull();
  }

  @Test
  void aPageTheStatementWroteToAtTheLastByteComesBack() {
    final MutablePage page = newPage(new Random(3));
    final byte[] bytes = bytesOf(page);
    final PageUndoLog log = new PageUndoLog();

    log.begin(page, null);
    page.writeByte(page.getMaxContentSize() - 1, (byte) 0x7F);
    page.writeLong(0, -1L);
    log.restore();
    log.end();

    assertThat(bytesOf(page)).isEqualTo(bytes);
  }

  @Test
  void aLogCanBeReusedForAnotherPage() {
    final Random random = new Random(4);
    final PageUndoLog log = new PageUndoLog();

    final MutablePage first = newPage(random);
    final byte[] firstBytes = bytesOf(first);
    log.begin(first, null);
    write(first, random);
    log.restore();
    log.end();
    assertThat(bytesOf(first)).isEqualTo(firstBytes);

    final MutablePage second = newPage(random);
    final byte[] secondBytes = bytesOf(second);
    log.begin(second, null);
    write(second, random);
    write(second, random);
    log.restore();
    log.end();
    assertThat(bytesOf(second)).isEqualTo(secondBytes);
  }
}
