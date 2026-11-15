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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11787: a transaction that allocated a page in a bucket answers {@code getTotalPages()} with its own page counter, which
 * stops at the last page it allocated. A multi-page record another transaction committed afterwards has its chunks on pages past
 * that counter (the pages are reserved for each transaction from one shared counter), and the chunk-chain walk refused to follow
 * them: a read of the stable record failed with "was modified during read after 3 retries", or was reported as a corrupted record
 * and skipped by the scan.
 */
class ChunkChainPastOwnPageCounterTest extends TestHelper {
  @Test
  void aTransactionThatAllocatedAPageReadsARecordCommittedOnLaterPages() throws Exception {
    database.getSchema().createDocumentType("C", 1);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("C").getBuckets(false).getFirst();

    final CountDownLatch allocated = new CountDownLatch(1);
    final CountDownLatch otherCommitted = new CountDownLatch(1);
    final RID[] big = new RID[1];
    final FutureTask<Set<String>> reader = new FutureTask<>(() -> {
      database.begin();
      database.newDocument("C").set("who", "reader").save();
      assertThat(((DatabaseInternal) database).getTransaction().getPageCounter(bucket.getFileId()))
          .as("the reader allocated the first page of the bucket").isEqualTo(1);
      allocated.countDown();
      otherCommitted.await();

      final Set<String> seen = new HashSet<>();
      try (final ResultSet rows = database.query("sql", "SELECT who FROM C")) {
        rows.stream().map(row -> row.<String>getProperty("who")).forEach(seen::add);
      }
      final Document loaded = (Document) database.lookupByRID(big[0], true);
      assertThat(loaded.getString("big")).hasSize(400_000);
      assertThat(bucket.count()).as("the reader counts the other transaction's record too").isEqualTo(2);
      database.commit();
      return seen;
    });
    new Thread(reader).start();

    try {
      assertThat(allocated.await(30, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> {
        final MutableDocument document = database.newDocument("C").set("who", "big").set("big", "b".repeat(400_000));
        document.save();
        big[0] = document.getIdentity();
      });
      assertThat(bucket.getCommittedPageCount()).as("the record spans pages past the reader's counter").isGreaterThan(2);
    } finally {
      // released on every path, so a failed writer step cannot leave the reader parked on the latch
      otherCommitted.countDown();
    }

    assertThat(reader.get(30, TimeUnit.SECONDS)).contains("reader", "big");
    try (final ResultSet rows = database.query("sql", "SELECT count(*) AS c FROM C")) {
      assertThat(rows.next().<Number>getProperty("c").intValue()).isEqualTo(2);
    }
  }
}
