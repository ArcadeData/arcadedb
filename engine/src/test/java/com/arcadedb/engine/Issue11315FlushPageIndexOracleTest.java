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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11315: {@link FlushPageIndex#scanPendingOf}, the oracle the regression test of #6133
 * compares the O(1) pending counter against, enumerated the RETAINED MAP KEYS and matched them with the path-based
 * {@code LocalDatabase.equals()} - the two enumerations #6440 and #6931 removed from the rest of the class. With two
 * instances open at one path, {@code Map.put()} keeps the first instance's key and swaps in the second one's page, so
 * the oracle attributed the second instance's page to the first, disagreed with the counter that was right, and would
 * have agreed with a counter regressed to key-based accounting.
 * <p>
 * The fixed oracle found exactly such a regression one method over: {@link FlushPageIndex#remove(PageId)} released the
 * counter of the database its ARGUMENT names, not of the page it removed, so the same aliasing drove the stale
 * instance's count negative and left the live one's above zero on an empty index - the "close() hangs on a pipeline
 * that is in fact empty" failure the class javadoc names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11315FlushPageIndexOracleTest extends TestHelper {
  private static final int PAGE_SIZE   = 1_024;
  private static final int FILE_ID     = 1;
  private static final int PAGE_NUMBER = 7;

  @Test
  void theOracleAttributesAPageToTheInstanceItsValueNames() {
    withSamePathInstances((stale, live) -> {
      final FlushPageIndex index = new FlushPageIndex();

      index.put(page(stale));
      assertCountsAgree(index, stale, 1);

      // The live instance indexes the same (fileId, pageNumber): the map keeps the stale instance's key object
      index.put(page(live));
      assertThat(index.get(new PageId(live, FILE_ID, PAGE_NUMBER)).getPageId().getDatabase()).isSameAs(live);

      assertCountsAgree(index, stale, 0);
      assertCountsAgree(index, live, 1);
    });
  }

  @Test
  void removingByPageIdReleasesTheCounterOfThePageItRemoved() {
    withSamePathInstances((stale, live) -> {
      final FlushPageIndex index = new FlushPageIndex();
      index.put(page(stale));
      index.put(page(live));

      // A detach by the STALE instance's PageId resolves to the slot holding the live instance's page (path equality)
      final MutablePage removed = index.remove(new PageId(stale, FILE_ID, PAGE_NUMBER));
      assertThat(removed.getPageId().getDatabase()).isSameAs(live);

      assertThat(index.isEmpty()).isTrue();
      assertCountsAgree(index, stale, 0);
      assertCountsAgree(index, live, 0);
    });
  }

  private interface SamePathBody {
    void run(DatabaseInternal stale, DatabaseInternal live);
  }

  /** Two {@code LocalDatabase} instances that are {@code equals()} (same path) but not the same instance. */
  private void withSamePathInstances(final SamePathBody body) {
    final String path = "target/databases/" + getClass().getSimpleName() + "-same-path";
    final Database stale = TestHelper.createDatabase(path);
    stale.drop();

    final Database live = TestHelper.createDatabase(path);
    try {
      assertThat(live).isEqualTo(stale);
      assertThat(live).isNotSameAs(stale);
      body.run((DatabaseInternal) stale, (DatabaseInternal) live);
    } finally {
      live.drop();
    }
  }

  private static MutablePage page(final DatabaseInternal database) {
    return new MutablePage(new PageId(database, FILE_ID, PAGE_NUMBER), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }

  private static void assertCountsAgree(final FlushPageIndex index, final DatabaseInternal database, final int expected) {
    assertThat(index.pendingOf(database)).as("pending count of instance %s", System.identityHashCode(database))
        .isEqualTo(expected);
    assertThat(index.scanPendingOf(database)).as("scan of the index for instance %s", System.identityHashCode(database))
        .isEqualTo(expected);
  }
}
