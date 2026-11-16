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
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11787: a transaction that allocated a page in a bucket answers {@code getTotalPages()} with its own page counter, which
 * stops at the last page it allocated. A multi-page record another transaction committed afterwards has its chunks on pages past
 * that counter (the pages are reserved for each transaction from one shared counter), and the chunk-chain walk refused to follow
 * them: a read of the stable record failed with "was modified during read after 3 retries", or was reported as a corrupted record
 * and skipped by the scan. A delete of it failed with "chunk 0 was modified concurrently", and a count stopped at the counter.
 */
class ChunkChainPastOwnPageCounterTest extends TestHelper {
  @Test
  void aTransactionThatAllocatedAPageReadsARecordCommittedOnLaterPages() throws Exception {
    final LocalBucket bucket = createBucket();
    final Set<String> seen = whileAnotherTransactionCommitsABigRecord(bucket, big -> {
      final Set<String> who = new HashSet<>();
      try (final ResultSet rows = database.query("sql", "SELECT who FROM C")) {
        rows.stream().map(row -> row.<String>getProperty("who")).forEach(who::add);
      }
      final Document loaded = (Document) database.lookupByRID(big, true);
      assertThat(loaded.getString("big")).hasSize(400_000);
      // the counter is kept on every commit, so count() would answer from it: drop it so the count walks the pages
      bucket.setCachedRecordCount(-1);
      assertThat(bucket.count()).as("the reader counts the other transaction's record too").isEqualTo(2);
      return who;
    });

    assertThat(seen).contains("reader", "big");
    try (final ResultSet rows = database.query("sql", "SELECT count(*) AS c FROM C")) {
      assertThat(rows.next().<Number>getProperty("c").intValue()).isEqualTo(2);
    }
  }

  @Test
  void aTransactionThatAllocatedAPageDeletesARecordCommittedOnLaterPages() throws Exception {
    final LocalBucket bucket = createBucket();
    final RID deleted = whileAnotherTransactionCommitsABigRecord(bucket, big -> {
      database.lookupByRID(big, true).delete();
      return big;
    });

    assertThat(database.existsRecord(deleted)).isFalse();
    try (final ResultSet rows = database.query("sql", "SELECT who FROM C")) {
      assertThat(rows.stream().map(row -> row.<String>getProperty("who")).toList()).containsExactly("reader");
    }
  }

  private LocalBucket createBucket() {
    database.getSchema().createDocumentType("C", 1);
    return (LocalBucket) database.getSchema().getType("C").getBuckets(false).getFirst();
  }

  /**
   * Runs {@code body} in a transaction that allocated the first page of {@code bucket} before another transaction committed a
   * record spanning the pages past it, and commits it. The body receives the other transaction's record.
   */
  private <T> T whileAnotherTransactionCommitsABigRecord(final LocalBucket bucket, final Function<RID, T> body)
      throws Exception {
    final CountDownLatch allocated = new CountDownLatch(1);
    final CountDownLatch otherCommitted = new CountDownLatch(1);
    final RID[] big = new RID[1];
    final FutureTask<T> reader = new FutureTask<>(() -> {
      database.begin();
      database.newDocument("C").set("who", "reader").save();
      assertThat(((DatabaseInternal) database).getTransaction().getPageCounter(bucket.getFileId()))
          .as("the reader allocated the first page of the bucket").isEqualTo(1);
      allocated.countDown();
      otherCommitted.await();

      final T result = body.apply(big[0]);
      database.commit();
      return result;
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

    return reader.get(30, TimeUnit.SECONDS);
  }
}
