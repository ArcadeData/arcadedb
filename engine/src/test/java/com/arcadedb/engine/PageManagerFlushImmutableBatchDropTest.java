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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test: LSM/vector compaction schedules async flushes from an immutable single-element list
 * ({@code writePages(List.of(page), true)}). The drop and file-delete purge paths mutate the batch list -
 * {@code removeAllPagesOfDatabase()} calls {@code clear()} and {@code removePagesOfFileFromBatch()} calls
 * {@code iterator.remove()} - so a {@link PagesToFlush} must expose a mutable list, otherwise the purge throws
 * {@link UnsupportedOperationException} mid-drop.
 */
class PageManagerFlushImmutableBatchDropTest extends TestHelper {

  private static final int FILE_ID   = 9_998;
  private static final int PAGE_SIZE = 4_096;

  @Test
  void immutableBatchListSupportsDropPurgeMutations() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageId pageId = new PageId(db, FILE_ID, 0);

    // clear() is what removeAllPagesOfDatabase() calls when a database is dropped (the path that failed in CI).
    final PagesToFlush dropBatch = new PagesToFlush(List.of(new MutablePage(pageId, PAGE_SIZE)));
    assertThatCode(dropBatch.pages::clear)
        .as("a batch built from an immutable list must be clearable when its database is dropped")
        .doesNotThrowAnyException();

    // iterator.remove() is what removePagesOfFileFromBatch() calls when a file is deleted.
    final PagesToFlush fileBatch = new PagesToFlush(List.of(new MutablePage(pageId, PAGE_SIZE)));
    assertThatCode(() -> {
      final Iterator<MutablePage> it = fileBatch.pages.iterator();
      it.next();
      it.remove();
    }).as("a batch built from an immutable list must support iterator removal when a file is deleted")
        .doesNotThrowAnyException();
    assertThat(fileBatch.pages).isEmpty();
  }
}
