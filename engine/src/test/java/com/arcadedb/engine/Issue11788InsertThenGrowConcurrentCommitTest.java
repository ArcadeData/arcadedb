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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two transactions that each insert records and then, before committing, grow them past what their page can host commit
 * side by side (issue #11788): no record is shared, so neither may be refused because the other wrote the same pages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11788InsertThenGrowConcurrentCommitTest extends TestHelper {
  private static final int RECORDS    = 50;
  private static final int DIMENSIONS = 768;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Chunk BUCKETS 1");
    // THE BUCKET HAS PAGES BOTH TRANSACTIONS REUSE
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Chunk").set("text", text(i)).save();
    });
  }

  @Test
  void twoTransactionsInsertingThenGrowingTheirOwnRecordsBothCommit() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      for (int round = 0; round < 5; round++) {
        final int base = 1000 * (round + 1);

        // THIS THREAD: INSERT, THEN GROW, NOT COMMITTED YET
        database.begin();
        final List<RID> mine = insertThenGrow(base);

        // THE OTHER THREAD DOES THE SAME ON THE SAME PAGES AND COMMITS FIRST
        final Future<List<RID>> theirs = other.submit(() -> {
          database.begin();
          final List<RID> rids = insertThenGrow(base + 500);
          database.commit();
          return rids;
        });
        final List<RID> committedByOther = theirs.get(60, TimeUnit.SECONDS);

        try {
          database.commit();
        } catch (final ConcurrentModificationException e) {
          throw new AssertionError("Round " + round + ": no record is shared, the commit must not be refused (issue #11788)", e);
        }

        database.begin();
        for (final List<RID> rids : List.of(mine, committedByOther))
          for (final RID rid : rids) {
            final double[] emb = (double[]) rid.asDocument().get("emb");
            assertThat(emb).as("record %s", rid).hasSize(DIMENSIONS);
            assertThat(emb[DIMENSIONS - 1]).isEqualTo(rid.asDocument().getInteger("n") + DIMENSIONS - 1.0);
          }
        database.rollback();
      }
    } finally {
      other.shutdownNow();
    }
    assertThat(database.countType("Chunk", false)).isEqualTo(20 + 5 * 2 * RECORDS);
  }

  /**
   * The workload of the report: several writers, each transaction inserting its records and then growing them. It refused
   * three commits out of four; the handful a run may still refuse (a page that filled between the claim and the commit) is
   * what the bound leaves room for.
   */
  @Test
  void concurrentWritersInsertingThenGrowingAreNotRefusedByEachOther() throws Exception {
    final int writers = 4;
    final int transactionsPerWriter = 40;
    final AtomicInteger committed = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int writer = w;
        futures.add(pool.submit(() -> {
          for (int n = 0; n < transactionsPerWriter; n++) {
            database.begin();
            try {
              insertThenGrow(1_000_000 * (writer + 1) + n * 100);
              database.commit();
              committed.incrementAndGet();
            } catch (final ConcurrentModificationException e) {
              refused.incrementAndGet();
              if (database.isTransactionActive())
                database.rollback();
            }
          }
        }));
      }
      for (final Future<?> future : futures)
        future.get(120, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(committed.get() + refused.get()).isEqualTo(writers * transactionsPerWriter);
    assertThat(refused.get()).as("commits refused out of %d although no two transactions wrote the same record (issue #11788)",
        writers * transactionsPerWriter).isLessThanOrEqualTo(writers * transactionsPerWriter / 10);

    database.begin();
    long records = 0;
    for (final Iterator<Record> it = database.iterateType("Chunk", false); it.hasNext(); ) {
      final Document doc = it.next().asDocument();
      ++records;
      if (doc.has("n")) {
        final double[] emb = (double[]) doc.get("emb");
        assertThat(emb).as("record %s", doc.getIdentity()).hasSize(DIMENSIONS);
        assertThat(emb[DIMENSIONS - 1]).isEqualTo(doc.getInteger("n") + DIMENSIONS - 1.0);
      }
    }
    database.rollback();
    assertThat(records).isEqualTo(20L + (long) committed.get() * RECORDS);
  }

  private List<RID> insertThenGrow(final int base) {
    final List<MutableDocument> docs = new ArrayList<>(RECORDS);
    for (int i = 0; i < RECORDS; i++)
      docs.add(database.newDocument("Chunk").set("n", base + i).set("text", text(base + i)).save());
    final List<RID> rids = new ArrayList<>(RECORDS);
    for (final MutableDocument doc : docs) {
      final int n = doc.getInteger("n");
      final double[] emb = new double[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        emb[d] = n + d;
      doc.set("emb", emb).save();
      rids.add(doc.getIdentity());
    }
    return rids;
  }

  private static String text(final int i) {
    return ("chunk " + i + " ").repeat(40).substring(0, 300);
  }
}
