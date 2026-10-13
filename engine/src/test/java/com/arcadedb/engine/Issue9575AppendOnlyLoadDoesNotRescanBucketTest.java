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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9575 (discussion #9559): a GraphImporter load of 209M vertices slowed from 60,000 to 600 vertices a second while
 * the heap sat at 1% GC. The vertex pass only appends, and every append reported itself as "a change" to the bucket's free
 * space statistics. The map of reusable pages is empty each time an appended page fills up, so the next allocation asked for
 * a gather, and the gather - once its 5 s throttle was over - read EVERY page of the bucket looking for space that no append
 * can have freed. The scan grows with the file (6 minutes of disk reads at 1.3M pages, against 5 s of useful work between
 * two scans), so the load got slower the more it had written.
 * <p>
 * Counting scans needs no clock, so a stalled JVM cannot flip these tests; the 5 s throttle is expired through a hook
 * instead of slept through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575AppendOnlyLoadDoesNotRescanBucketTest extends TestHelper {
  private static final int    ROUNDS          = 5;
  private static final int    RECORDS_PER_RUN = 20_000;
  private static final String PAYLOAD         = "x".repeat(1_000);

  @Test
  void appendingNeverTriggersAFullBucketScan() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();

    append(RECORDS_PER_RUN);
    final long scansAfterFirstRun = bucket.getGatherScanCountForTesting();
    final int pagesAfterFirstRun = bucket.getTotalPages();
    // the premise: records of ~1 KB pack a page to within 2% of its end, so no page is left worth listing, and every
    // page that fills up empties the map
    assertThat(pagesAfterFirstRun).isGreaterThan(100);

    for (int round = 0; round < ROUNDS; round++) {
      bucket.expireGatherThrottleForTesting();
      append(RECORDS_PER_RUN);
    }

    assertThat(bucket.getTotalPages()).isGreaterThan(pagesAfterFirstRun * 3);
    assertThat(bucket.getGatherScanCountForTesting())
        .as("gather scans over %d pages after %d append-only rounds with the throttle expired before each", bucket.getTotalPages(),
            ROUNDS).isEqualTo(scansAfterFirstRun);
  }

  /**
   * The other side of the same rule: space that a delete frees IS something a scan can find, so a delete must still arm the
   * next gather and the pages it emptied must be reused rather than the file grown.
   */
  @Test
  void deletedSpaceIsStillFoundByTheNextGather() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();

    final List<RID> rids = append(RECORDS_PER_RUN);
    final int pagesBeforeDelete = bucket.getTotalPages();
    final long scansBeforeDelete = bucket.getGatherScanCountForTesting();

    database.transaction(() -> {
      for (int i = 0; i < rids.size(); i++)
        if (i % 2 == 0)
          database.deleteRecord(rids.get(i).asDocument());
    });

    bucket.expireGatherThrottleForTesting();
    append(RECORDS_PER_RUN / 4);

    assertThat(bucket.getGatherScanCountForTesting()).as("the delete armed a gather").isGreaterThan(scansBeforeDelete);
    assertThat(bucket.getTotalPages()).as("pages after re-inserting a quarter of what the delete freed, from %d", pagesBeforeDelete)
        .isLessThanOrEqualTo(pagesBeforeDelete + 5);
  }

  private List<RID> append(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    database.begin();
    for (int i = 0; i < count; i++) {
      final MutableDocument d = database.newDocument("Wide").set("i", i).set("payload", PAYLOAD);
      d.save();
      rids.add(d.getIdentity());
      if ((i + 1) % 2_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return rids;
  }

  private LocalBucket wideBucket() {
    return (LocalBucket) database.getSchema().getType("Wide").getBuckets(false).get(0);
  }
}
