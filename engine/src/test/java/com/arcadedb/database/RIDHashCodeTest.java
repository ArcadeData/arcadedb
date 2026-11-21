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
package com.arcadedb.database;

import com.arcadedb.engine.PageId;

import org.junit.jupiter.api.Test;

import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The hash codes of the keys the engine puts in its hash tables must spread over a table's bins. A {@code HashMap} picks the bin
 * from the low bits of the hash, so a hash that only adds the parts of a key (bucket * 31 + offset) leaves most bins empty when
 * the offsets are what a bucket really assigns: page * 2048 + slot, with a few dozen slots in use per page. The bins that remain
 * hold dozens of keys and the table turns them into trees; a full-text search builds one such map of RIDs per bucket and term.
 */
class RIDHashCodeTest {
  private static final int TABLE = 1 << 16;
  /** {@code HashMap.TREEIFY_THRESHOLD}: a bin this long becomes a tree. */
  private static final int TREEIFY = 8;

  /** How many bins of a {@code HashMap} table hold {@link #TREEIFY} keys or more, and how many bins are used at all. */
  private record Spread(int treeBins, int usedBins) {
  }

  private static Spread spread(final IntSupplier[] hashes) {
    final int[] bins = new int[TABLE];
    for (final IntSupplier hash : hashes) {
      final int h = hash.getAsInt();
      ++bins[(h ^ (h >>> 16)) & (TABLE - 1)];
    }
    int treeBins = 0;
    int used = 0;
    for (final int count : bins) {
      if (count > 0)
        ++used;
      if (count >= TREEIFY)
        ++treeBins;
    }
    return new Spread(treeBins, used);
  }

  @Test
  void theRidsOfOneBucketSpreadOverTheBinsOfAHashMap() {
    // 1,024 pages of 44 records, as a bucket assigns the offsets
    final IntSupplier[] hashes = new IntSupplier[1024 * 44];
    int i = 0;
    for (int page = 0; page < 1024; page++)
      for (int slot = 0; slot < 44; slot++) {
        final RID rid = new RID(3, page * 2048L + slot);
        hashes[i++] = rid::hashCode;
      }

    final Spread spread = spread(hashes);

    assertThat(spread.treeBins()).as("bins that would become trees").isLessThan(32);
    assertThat(spread.usedBins()).as("bins in use out of %d for %d keys", TABLE, hashes.length).isGreaterThan(TABLE / 3);
  }

  @Test
  void theRidsOfDifferentBucketsDoNotCollide() {
    final IntSupplier[] hashes = new IntSupplier[16 * 3000];
    int i = 0;
    for (int bucket = 40; bucket < 56; bucket++)
      for (int offset = 0; offset < 3000; offset++) {
        final RID rid = new RID(bucket, offset);
        hashes[i++] = rid::hashCode;
      }

    assertThat(spread(hashes).treeBins()).isLessThan(32);
  }

  @Test
  void theHashOfAnEqualRidIsTheSame() {
    assertThat(new RID(7, 123456L).hashCode()).isEqualTo(new RID(7, 123456L).hashCode());
    assertThat(new RID(7, 123456L)).isEqualTo(new RID(7, 123456L)).isNotEqualTo(new RID(8, 123456L));
  }

  @Test
  void thePagesOfAFileSpreadOverTheBinsOfAHashMap() {
    final BasicDatabase database = mock(BasicDatabase.class);
    // a page cache holds the pages of every file of a database: 16 files of 3,000 pages
    final IntSupplier[] hashes = new IntSupplier[16 * 3000];
    int i = 0;
    for (int file = 100; file < 116; file++)
      for (int page = 0; page < 3000; page++) {
        final PageId id = new PageId(database, file, page);
        hashes[i++] = id::hashCode;
      }

    final Spread spread = spread(hashes);

    assertThat(spread.treeBins()).as("bins that would become trees").isLessThan(32);
    assertThat(spread.usedBins()).as("bins in use out of %d for %d keys", TABLE, hashes.length).isGreaterThan(TABLE / 3);
  }
}
