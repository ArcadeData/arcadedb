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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The page cache is a {@code ConcurrentHashMap<PageId, ...>}, and a {@code HashMap} picks the bin from the low bits of the hash.
 * A hash that only adds the parts of the key ({@code 31 * fileId + pageNumber}) makes the pages {@code (f, p)} and
 * {@code (f + 1, p - 31)} hash alike, so the pages of a few files have a small fraction of that many distinct hashes and fill
 * a few bins with long chains.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PageIdHashCodeTest extends TestHelper {
  private static final int TABLE = 1 << 16;
  /** {@code HashMap.TREEIFY_THRESHOLD}: a bin this long becomes a tree. */
  private static final int TREEIFY = 8;

  @Test
  void thePagesOfManyFilesSpreadOverTheBinsOfAHashMap() {
    // a page cache holds the pages of every file of a database: 16 files of 3,000 pages
    final int files = 16;
    final int pages = 3000;
    final int[] bins = new int[TABLE];
    for (int file = 100; file < 100 + files; file++)
      for (int page = 0; page < pages; page++) {
        final int h = new PageId(database, file, page).hashCode();
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

    assertThat(treeBins).as("bins that would become trees").isLessThan(32);
    assertThat(used).as("bins in use out of %d for %d keys", TABLE, files * pages).isGreaterThan(TABLE / 3);
  }

  @Test
  void thePagesOfNeighbouringFilesDoNotCollide() {
    final Set<Integer> hashes = new HashSet<>();
    final int files = 16;
    final int pages = 3000;
    for (int file = 100; file < 100 + files; file++)
      for (int page = 0; page < pages; page++)
        hashes.add(new PageId(database, file, page).hashCode());

    // 31 * file + page gives fewer than 3,500 distinct values for these 48,000 pages
    assertThat(hashes.size()).as("distinct hashes of %d pages", files * pages).isGreaterThan(files * pages * 99 / 100);

    assertThat(new PageId(database, 101, 5).hashCode()).as("(f, p) against (f + 1, p - 31)")
        .isNotEqualTo(new PageId(database, 102, 5 - 31).hashCode());
    assertThat(new PageId(database, 101, 40).hashCode()).as("(f, p) against (f + 1, p - 31)")
        .isNotEqualTo(new PageId(database, 102, 40 - 31).hashCode());
  }

  @Test
  void theHashOfAnEqualPageIdIsTheSameAndStable() {
    final PageId page = new PageId(database, 7, 123456);
    final PageId same = new PageId(database, 7, 123456);

    assertThat(page).isEqualTo(same);
    assertThat(same.hashCode()).isEqualTo(page.hashCode());
    // the second call answers from the cached value and agrees
    assertThat(page.hashCode()).isEqualTo(same.hashCode());
    assertThat(new PageId(database, 8, 123456)).isNotEqualTo(page);
  }

  @Test
  void theBoundariesOfTheKeyDoNotAliasEachOther() {
    // a negative page number must not leak its sign into the file id half of the key
    assertThat(new PageId(database, 1, -1).hashCode()).isNotEqualTo(new PageId(database, 0, -1).hashCode());
    assertThat(new PageId(database, 1, 0).hashCode()).isNotEqualTo(new PageId(database, 0, 0).hashCode());
    assertThat(new PageId(database, 0, 1).hashCode()).isNotEqualTo(new PageId(database, 1, 0).hashCode());
  }
}
