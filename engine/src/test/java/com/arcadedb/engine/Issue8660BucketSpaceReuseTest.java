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
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8660: space freed by a bulk delete was reused for one batch of pages (the ~100 the free-space map held) and never
 * again, because near-full pages stayed in the map and the map was only re-gathered when empty. Deleting 90% of a bucket and
 * loading the same number of records back nearly doubled the files.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8660BucketSpaceReuseTest extends TestHelper {
  private static final int TOTAL = 300_000;
  private static final int CHUNK = 54_000;

  @Test
  void bulkRefillAfterBulkDeleteReusesTheFreedPages() {
    final String pad = "x".repeat(100);
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.id LONG");
    database.command("sql", "CREATE INDEX ON D (id) NOTUNIQUE");
    insert(0, TOTAL, pad);

    for (long lo = 0; lo < TOTAL; lo += 10_000) {
      final long from = lo, to = lo + 10_000;
      database.transaction(() -> database.command("sql", "DELETE FROM D WHERE id >= ? AND id < ? AND id % 10 <> 0", from, to));
    }

    reopenDatabase();
    LocalBucket bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).get(0);
    final int pagesBefore = bucket.getTotalPages();

    long next = TOTAL;
    for (int chunk = 0; chunk < 5; chunk++) {
      insert(next, next + CHUNK, pad);
      next += CHUNK;
    }

    bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).get(0);
    // 270,000 records back into a bucket that lost 270,000: the files should stay close to their size, not double
    assertThat(bucket.getTotalPages()).as("pages before the refill: %d", pagesBefore).isLessThan((int) (pagesBefore * 1.25));
    assertThat(database.countType("D", false)).isEqualTo(TOTAL / 10 + 5L * CHUNK);
  }

  private void insert(final long from, final long to, final String pad) {
    database.begin();
    for (long i = from; i < to; i++) {
      final MutableDocument d = database.newDocument("D").set("id", i).set("pad", pad);
      d.save();
      if ((i + 1) % 10_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }
}
