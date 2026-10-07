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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
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
 * Issue #9397: the point-lookup fix of #9369 covered the SQL path only. An openCypher {@code MATCH (n:T) WHERE n.k IN $ids}
 * and {@code Database.lookupByKey()} still lost, doubled or failed on a key that one transaction deletes and re-creates, though
 * the key exists in every committed state: the index entry and the record were read at different commits, and the keys of an
 * {@code IN} list were read one after the other, so a record slot freed by one key and taken by another was served for both.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9397TornCommitIndexLookupTest extends TestHelper {
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
  void uniqueHashLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
    run("UNIQUE_HASH");
  }

  @Test
  @Tag("slow")
  void uniqueLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
    run("UNIQUE");
  }

  @Test
  @Tag("slow")
  void notUniqueLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
    run("NOTUNIQUE");
  }

  @Test
  void inListOfCypherAnswersEveryKeyOnce() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    fill(1000);
    final List<Long> ids = ids();
    for (int run = 0; run < 3; run++) {
      final List<Integer> pids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid ORDER BY pid",
          Map.of("ids", ids))) {
        rs.forEachRemaining(r -> pids.add(r.<Number>getProperty("pid").intValue()));
      }
      assertThat(pids).as("run " + run).hasSize(IDS).doesNotHaveDuplicates().first().isEqualTo(FIRST_ID);
    }
  }

  @Test
  void largeInListIsServedInFull() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    final int count = 1500;
    fill(count);
    final List<Long> ids = new ArrayList<>();
    for (long i = 0; i < count; i++)
      ids.add(i);
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid", Map.of("ids", ids))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    assertThat(rows).isEqualTo(count);
  }

  @Test
  void lookupByKeyReturnsTheRecordOfTheKey() {
    database.command("sql", "CREATE INDEX ON Product (pid) NOTUNIQUE");
    fill(100);
    database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 7, views = 5").close());

    final IndexCursor cursor = database.lookupByKey("Product", "pid", 7L);
    int found = 0;
    while (cursor.hasNext()) {
      final Identifiable record = cursor.next();
      assertThat(record.asVertex().getInteger("pid")).isEqualTo(7);
      found++;
    }
    assertThat(found).isEqualTo(2);
    assertThat(database.lookupByKey("Product", "pid", 99_999L).hasNext()).isFalse();
  }

  @Test
  void keyDeletedInTheSameTransactionIsNotFound() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    fill(10);
    database.begin();
    try {
      database.command("sql", "DELETE FROM Product WHERE pid = 3").close();
      assertThat(database.lookupByKey("Product", "pid", 3L).hasNext()).isFalse();
      assertThat(database.lookupByKey("Product", "pid", 4L).hasNext()).isTrue();
    } finally {
      database.rollback();
    }
    assertThat(database.lookupByKey("Product", "pid", 3L).hasNext()).isTrue();
  }

  @Test
  void repeatableReadTransactionStillLooksKeysUp() {
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    fill(100);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByKey("Product", "pid", 5L).hasNext()).isTrue();
      int rows = 0;
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN [1, 2, 3] RETURN n.pid AS pid")) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      assertThat(rows).isEqualTo(3);
    } finally {
      database.rollback();
    }
  }

  private void fill(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", (long) i)).close();
    });
  }

  private static List<Long> ids() {
    final List<Long> ids = new ArrayList<>();
    for (long i = FIRST_ID; i < FIRST_ID + IDS; i++)
      ids.add(i);
    return ids;
  }

  private void run(final String indexKind) throws Exception {
    database.command("sql", "CREATE INDEX ON Product (pid) " + indexKind);
    fill(1000);
    final List<Long> ids = ids();

    final String[] modes = { "Cypher outside any transaction", "Cypher inside a transaction", "lookupByKey" };
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong[] reads = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicLong[] wrong = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicReference<Throwable> crashed = new AtomicReference<>();
    final Thread[] readers = new Thread[3];
    for (int m = 0; m < 3; m++) {
      final int mode = m;
      readers[m] = new Thread(() -> {
        try {
          while (!stop.get()) {
            if (mode == 2) {
              for (final long x : ids) {
                int found = 0;
                try {
                  final IndexCursor cursor = database.lookupByKey("Product", "pid", x);
                  while (cursor.hasNext()) {
                    cursor.next().asDocument();
                    found++;
                  }
                } catch (final RuntimeException e) {
                  found = -1;
                }
                reads[mode].incrementAndGet();
                if (found != 1)
                  wrong[mode].incrementAndGet();
              }
              continue;
            }
            if (mode == 1)
              database.begin();
            int rows = 0;
            try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid",
                Map.of("ids", ids))) {
              while (rs.hasNext()) {
                rs.next();
                rows++;
              }
            } finally {
              if (mode == 1)
                database.rollback();
            }
            reads[mode].incrementAndGet();
            if (rows != IDS)
              wrong[mode].incrementAndGet();
          }
        } catch (final Throwable e) {
          // a crashed reader must fail the test, not leave its counters at zero
          crashed.set(e);
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

    assertThat(crashed.get()).as("a reader crashed").isNull();
    final StringBuilder report = new StringBuilder();
    for (int m = 0; m < 3; m++)
      if (wrong[m].get() > 0)
        report.append(indexKind).append(", ").append(modes[m]).append(": ").append(wrong[m]).append(" of ").append(reads[m])
            .append(" reads were wrong; ");
    assertThat(report.toString()).isEmpty();
  }
}
