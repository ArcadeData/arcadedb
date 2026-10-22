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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10547: {@code TransactionContext.setAppendOnlyInserts(true)} makes the records a transaction creates skip the
 * bucket free-space search (and its bucket-wide monitor) and land on the last page or a new one, for that transaction only.
 * A transaction without it still reuses the room a delete freed on an earlier page.
 */
class Issue10547AppendOnlyInsertsTest extends TestHelper {
  private static final int RECORDS = 5_000;

  @Test
  void anAppendOnlyTransactionSkipsTheFreedRoomThatASearchingOneReuses() {
    final LocalBucket bucket = fillThenFreeTheFirstPage();
    final int pagesBefore = bucket.getTotalPages();
    final long recordsPerPage = bucket.getMaxRecordsInPage();

    final RID[] appended = new RID[1];
    database.transaction(() -> {
      tx().setAppendOnlyInserts(true);
      appended[0] = database.newDocument("D").set("pad", "y").save().getIdentity();
    });
    assertThat(appended[0].getPosition() / recordsPerPage).as("page of the record the append-only transaction created")
        .isGreaterThanOrEqualTo(pagesBefore - 1L);

    final RID[] searched = new RID[1];
    database.transaction(() -> searched[0] = database.newDocument("D").set("pad", "y").save().getIdentity());
    assertThat(searched[0].getPosition() / recordsPerPage).as("page of the record a searching transaction created").isZero();
  }

  @Test
  void theFlagEndsWithItsTransaction() {
    database.command("sql", "CREATE DOCUMENT TYPE D");

    database.begin();
    tx().setAppendOnlyInserts(true);
    assertThat(tx().isAppendOnlyInserts()).isTrue();
    database.newDocument("D").set("pad", "y").save();
    database.commit();

    database.begin();
    assertThat(tx().isAppendOnlyInserts()).as("after a commit").isFalse();
    tx().setAppendOnlyInserts(true);
    database.rollback();

    database.begin();
    assertThat(tx().isAppendOnlyInserts()).as("after a rollback").isFalse();
    database.rollback();
  }

  private TransactionContext tx() {
    return ((DatabaseInternal) database).getTransaction();
  }

  /** Fills a one-bucket type over several pages, then deletes every record of the first page. */
  private LocalBucket fillThenFreeTheFirstPage() {
    database.command("sql", "CREATE DOCUMENT TYPE D BUCKETS 1");
    final String pad = "x".repeat(200);
    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument("D").set("i", i).set("pad", pad).save().getIdentity());
    });

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).getFirst();
    assertThat(bucket.getTotalPages()).as("the fixture must span several pages").isGreaterThan(2);

    final long recordsPerPage = bucket.getMaxRecordsInPage();
    database.transaction(() -> {
      for (final RID rid : rids)
        if (rid.getPosition() / recordsPerPage == 0)
          database.deleteRecord(rid.asDocument());
    });
    return bucket;
  }
}
