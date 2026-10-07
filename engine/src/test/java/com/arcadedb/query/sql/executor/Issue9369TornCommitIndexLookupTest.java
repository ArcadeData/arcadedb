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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9369: a reader running while a transaction deletes a record and creates another with the same indexed value must see
 * that value in every answer, since every committed state holds it. Before the fix an index lookup could observe the commit half
 * published (the index page of one commit with the record page of the other) and lose the row, while a scan never did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9369TornCommitIndexLookupTest extends TestHelper {
  private static final int FIRST_ID = 200;
  private static final int IDS      = 50;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Product.views INTEGER");
  }

  @Test
  @Tag("slow")
  void uniqueHashLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    run("UNIQUE_HASH");
  }

  @Test
  @Tag("slow")
  void uniqueLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    run("UNIQUE");
  }

  @Test
  @Tag("slow")
  void notUniqueLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    run("NOTUNIQUE");
  }

  @Test
  void publicationSequenceMovesTwiceForACommitAndIsEvenAtRest() {
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long before = pageManager.getPublicationSequence();
    assertThat(before & 1).isZero();
    database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 1, views = 1").close());
    final long after = pageManager.getPublicationSequence();
    assertThat(after & 1).isZero();
    assertThat(after).isGreaterThan(before);
  }

  @Test
  void theSamePointLookupRunTwiceServesTheSameRows() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 1", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = List.of(1L, 3L, 5L);
    for (int run = 0; run < 3; run++) {
      final List<Integer> pids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids ORDER BY pid", Map.of("ids", ids))) {
        rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
      }
      assertThat(pids).as("run " + run).containsExactly(1, 3, 5);
    }
  }

  @Test
  void repeatableReadKeepsItsSnapshotWhenNoCommitOverlapsTheLookup() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 1, views = 1").close());
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      try (final ResultSet rs = database.query("sql", "SELECT views FROM Product WHERE pid = 1")) {
        assertThat(rs.next().<Integer>getProperty("views")).isEqualTo(1);
      }
      // another thread commits a change of the record the transaction has already read
      final Thread writer = new Thread(() -> database.transaction(() -> database.command("sql", "UPDATE Product SET views = 2 WHERE pid = 1").close()));
      writer.start();
      try {
        writer.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try (final ResultSet rs = database.query("sql", "SELECT views FROM Product WHERE pid = 1")) {
        assertThat(rs.next().<Integer>getProperty("views")).as("the snapshot the transaction pinned").isEqualTo(1);
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void largePointLookupIsServedInFull() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    final int count = 3000;
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 1", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = new ArrayList<>();
    for (long i = 0; i < count; i++)
      ids.add(i);
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids", Map.of("ids", ids))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    assertThat(rows).isEqualTo(count);
  }

  private void run(final String indexKind) throws Exception {
    database.command("sql", "CREATE INDEX ON Product (pid) " + indexKind);
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = new ArrayList<>();
    for (long i = FIRST_ID; i < FIRST_ID + IDS; i++)
      ids.add(i);

    final String[] modes = { "outside any transaction", "READ_COMMITTED transaction", "REPEATABLE_READ transaction" };
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong[] reads = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicLong[] missing = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicReference<Throwable>[] readerFailures = new AtomicReference[] { new AtomicReference<Throwable>(), new AtomicReference<Throwable>(),
        new AtomicReference<Throwable>() };
    final Thread[] readers = new Thread[3];
    for (int m = 0; m < 3; m++) {
      final int mode = m;
      readers[m] = new Thread(() -> {
        try {
          while (!stop.get()) {
            if (mode == 2)
              database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
            if (mode != 0)
              database.begin();
            int rows = 0;
            try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids", Map.of("ids", ids))) {
              while (rs.hasNext()) {
                rs.next();
                rows++;
              }
            } finally {
              if (mode != 0)
                database.rollback();
            }
            reads[mode].incrementAndGet();
            if (rows != IDS)
              missing[mode].incrementAndGet();
          }
        } catch (final Throwable e) {
          // a crashed reader must fail the test, not leave its counters at zero
          readerFailures[mode].set(e);
        }
      }, "reader-" + mode);
      readers[m].setDaemon(true);
      readers[m].start();
    }

    final long deadline = System.currentTimeMillis() + 8_000;
    long commits = 0;
    while (System.currentTimeMillis() < deadline) {
      final long x = FIRST_ID + commits % IDS;
      database.begin();
      database.command("sql", "DELETE FROM Product WHERE pid = :p", Map.of("p", x)).close();
      database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", x)).close();
      database.commit();
      commits++;
    }
    stop.set(true);
    for (final Thread t : readers)
      t.join();

    for (int m = 0; m < 3; m++)
      assertThat(readerFailures[m].get()).as(modes[m] + " reader failed").isNull();

    final StringBuilder report = new StringBuilder();
    for (int m = 0; m < 3; m++)
      if (missing[m].get() > 0)
        report.append(indexKind).append(", ").append(modes[m]).append(": ").append(missing[m]).append(" of ").append(reads[m])
            .append(" reads lost a row; ");
    assertThat(report.toString()).isEmpty();
  }
}
