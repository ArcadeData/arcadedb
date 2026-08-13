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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared fixtures for the tests that drive a bucket page into a specific PHYSICAL layout - a full page, a page with
 * no free tail at all - and then assert what the engine made of it. Extracted once the third such test class
 * ({@code Issue6149PlaceholderPrefersChunksTest}, next to {@code Issue5279ConcurrentUpdateTest} and
 * {@code Issue6129ChunkedSlotMergeTest}) would have copied these verbatim for a fourth time.
 * <p>
 * The layout these build is a contract of its own: change how a page fills up or how a spill claims the free tail,
 * and every test below has to be re-read, not just re-run. Keeping the fixtures in one place is what makes that
 * possible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class BucketPageLayoutTestSupport extends TestHelper {
  /**
   * Fills page 0 of a single-bucket type until a record no longer fits it: the next insert lands on another page,
   * which shows up as a RID position that is not the previous one plus one (a new page restarts at a multiple of the
   * page's slot count).
   * <p>
   * Joins the caller's transaction when there is one, so it can be used either inside a fixture transaction or on
   * its own.
   *
   * @return the RID of the last record that landed on page 0.
   */
  protected RID fillFirstPage(final String typeName) {
    final String filler = "f".repeat(8 * 1024);
    final RID[] result = new RID[1];
    database.transaction(() -> {
      RID previous = null;
      for (int i = 0; i < 64; i++) {
        final RID rid = database.newDocument(typeName).set("v", filler).save().getIdentity();
        if (previous != null && rid.getPosition() != previous.getPosition() + 1) {
          result[0] = previous;
          return;
        }
        previous = rid;
      }
      throw new AssertionError("Page 0 of " + typeName + " did not fill up");
    });
    return result[0];
  }

  /**
   * Leaves page 0 with a free tail of exactly ZERO bytes, the only page shape that still forces a record too small to
   * host a chunk header into a placeholder (#6149). Inserts cannot produce it - the allocator always keeps
   * {@code SPARE_SPACE_FOR_GROWTH} in hand - but a spill can: the head chunk of a record that outgrows its page while
   * being the LAST record of that page takes the record's own footprint plus the whole free tail, so the page ends
   * exactly at its maximum content size.
   * <p>
   * Must be called OUTSIDE a transaction: the fill has to be committed before the record that seals the page is
   * grown, so the spill sees the page the fill produced.
   * <p>
   * The postcondition is checked here rather than left to the callers: the sealing record must really have SPILLED,
   * because the spill is the only thing that eats the free tail. Should a page-geometry change ever let 70 KB stay
   * in the page, every test built on this fixture would otherwise go on passing while quietly testing a page that
   * still has room - the failure would surface as six confusing assertions elsewhere instead of one here.
   *
   * @return the RID of the record that seals the page, so a caller can free the tail again by deleting it.
   */
  protected RID sealFirstPage(final String typeName) {
    final RID last = fillFirstPage(typeName);
    database.transaction(() -> last.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());

    final Map<String, Object> layout = bucketStats(typeName);
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("sealing page 0 of " + typeName + " requires the last record to spill into chunks, taking the free tail "
            + "with it - it did not: " + layout)
        .isPositive();
    return last;
  }

  /** Physical layout of a single-bucket type: how many records are placeholders, chunked, and so on. */
  protected Map<String, Object> bucketStats(final String typeName) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).getFirst();
    final Map<String, Object>[] stats = new Map[1];
    database.transaction(() -> stats[0] = bucket.check(0, false));
    return stats[0];
  }

  /** The sanity net every layout test ends with: the whole database must check out with nothing to fix. */
  protected void checkDatabase() {
    try (final ResultSet rs = database.command("SQL", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(numberProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
        assertThat(numberProperty(row, "autoFix")).as("check database: " + row.toJSON()).isZero();
      }
    }
  }

  /** Null-tolerant read of a numeric check-database property, so a missing field fails clearly instead of NPE. */
  protected static long numberProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
