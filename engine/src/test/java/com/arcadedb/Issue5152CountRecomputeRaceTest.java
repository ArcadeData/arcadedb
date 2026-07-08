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
package com.arcadedb;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for #5152: the lazy recompute of the cached per-bucket record counter
 * ({@code LocalBucket.count()} on the {@code cachedRecordCount == -1} branch) must be serialized against
 * concurrent commits. Before the fix, a recompute could scan pages while a commit published a record and
 * folded its delta; the fold saw the still-{@code -1} counter and dropped the delta, and the recompute then
 * stored a total that missed the record, leaving {@code count(*)} permanently below the real count.
 * <p>
 * Stress test: with a large pre-populated single bucket forced into the {@code -1} state, a reader triggers a
 * (long) recompute while a writer commits new records concurrently. Afterwards the cached {@code count(*)}
 * must equal the authoritative scan {@code count(@rid)}.
 */
class Issue5152CountRecomputeRaceTest extends TestHelper {
  private static final int BASE   = 50000;
  private static final int EXTRA  = 3000;
  private static final int ROUNDS = 3;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("Doc"))
        // single bucket so all records share one cached counter, concentrating the race
        database.getSchema().createDocumentType("Doc", 1);
    });
  }

  private long countStar() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Doc")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long countScan() {
    // count(@rid) bypasses the count(*) cached fast-path and performs a real record scan.
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Doc")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private LocalBucket bucket() {
    final Bucket b = database.getSchema().getType("Doc").getBuckets(false).getFirst();
    return (LocalBucket) b;
  }

  private void insert(final int from, final int count) {
    for (int i = 0; i < count; i++) {
      final int id = from + i;
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET n = ?", id));
    }
  }

  @Test
  void recomputeInsideTransactionDoesNotDoubleCountOnCommit() {
    insert(0, 10);
    bucket().setCachedRecordCount(-1); // force a recompute on the next count()

    database.begin();
    try {
      database.command("sql", "INSERT INTO Doc SET n = 999"); // pending, uncommitted insert in this transaction
      // The recompute scans the transaction view (11) but must cache only the committed base (10) so the
      // commit-time fold does not add the pending delta a second time.
      assertThat(countStar()).isEqualTo(11L);
    } finally {
      database.commit();
    }

    // After commit the counter must be exactly 11, not 12 (no double count of the pending insert).
    assertThat(countStar()).isEqualTo(11L);
    assertThat(countScan()).isEqualTo(11L);
  }

  @Test
  void nonTransactionalCountReturnsCachedValueWithoutScanning() {
    insert(0, 5);
    final LocalBucket b = bucket();
    b.count(); // prime the cached counter

    // Poison the cache with a value that disagrees with the real records. A non-transactional count() must
    // return the cached value directly (O(1)) rather than rescanning, which is the restored fast-path contract.
    b.setCachedRecordCount(42L);
    assertThat(b.count()).isEqualTo(42L);
  }

  @Test
  @Tag("slow")
  void cachedCounterStaysConsistentUnderConcurrentRecomputeAndCommits() throws InterruptedException {
    insert(0, BASE);
    int expected = BASE;

    for (int round = 0; round < ROUNDS; round++) {
      // Force the cached counter into the vulnerable "unknown" state, as happens after an unclean shutdown,
      // a fresh open without a statistics file, or CHECK ... FIX.
      bucket().setCachedRecordCount(-1);

      final int start = expected;
      final CountDownLatch go = new CountDownLatch(1);
      final AtomicReference<Throwable> error = new AtomicReference<>();

      final Thread writer = new Thread(() -> {
        try {
          go.await();
          insert(start, EXTRA);
        } catch (final Throwable t) {
          error.set(t);
        }
      }, "writer-" + round);

      final Thread reader = new Thread(() -> {
        try {
          go.await();
          // The first count() over BASE+ records is a long recompute that overlaps the writer's commits.
          countStar();
        } catch (final Throwable t) {
          error.set(t);
        }
      }, "reader-" + round);

      writer.start();
      reader.start();
      go.countDown();
      writer.join();
      reader.join();

      if (error.get() != null)
        fail("worker failed in round " + round, error.get());

      expected = start + EXTRA;

      // The authoritative scan is always correct.
      assertThat(countScan()).as("count(@rid) after round %d", round).isEqualTo((long) expected);
      // The cached count(*) must match it - i.e. the concurrent recompute did not lose any committed record.
      assertThat(countStar()).as("count(*) after round %d", round).isEqualTo((long) expected);
    }
  }
}
