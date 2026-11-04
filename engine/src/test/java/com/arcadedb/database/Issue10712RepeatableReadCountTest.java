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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10712, the counter half: at {@code REPEATABLE_READ} a scan repeats (its pages are pinned) but {@code count(*)} and
 * {@code countType()} answered the bucket's live counter plus the transaction's own delta, so after another thread's commit
 * they jumped to the new total while a scan of the same type, in the same transaction, still answered the old one. The
 * committed part of the counter is now pinned at the first count of the bucket, like a page at its first read.
 * <p>
 * The scan half of the issue - a scan reading its pages one after the other can see part of a concurrent multi-page commit
 * at both levels, because a page is pinned at its first read and not when the transaction begins - is the documented
 * contract ({@code Database.TRANSACTION_ISOLATION_LEVEL}), and {@link #lockTypeExcludesTheWritersOfAWholeTypeRead} pins the
 * way the documentation gives to read one committed state of a whole type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10712RepeatableReadCountTest extends TestHelper {
  private static final String TYPE = "T";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.command("sql", "INSERT INTO " + TYPE + " SET id = ?, v = 0", i);
    });
  }

  @Test
  void aCountRepeatsAtRepeatableReadAndAgreesWithTheScan() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(scan()).isEqualTo(100);
      assertThat(countStar()).isEqualTo(100);
      assertThat(database.countType(TYPE, true)).isEqualTo(100);

      foreignCommit();

      // THE REPORTER'S TABLE: SCAN, count(id) AND THE FILTERED COUNT STAYED AT 100, count(*) AND countType JUMPED TO 149
      assertThat(scan()).isEqualTo(100);
      assertThat(countStar()).as("count(*) repeats like the scan").isEqualTo(100);
      assertThat(database.countType(TYPE, true)).as("countType repeats like the scan").isEqualTo(100);
      assertThat(count("SELECT count(id) AS c FROM " + TYPE)).isEqualTo(100);

      // THE TRANSACTION STILL COUNTS ITS OWN WRITES ON TOP OF WHAT IT PINNED
      database.command("sql", "INSERT INTO " + TYPE + " SET id = 1000, v = 0");
      assertThat(countStar()).isEqualTo(101);
      assertThat(database.countType(TYPE, true)).isEqualTo(101);
    } finally {
      database.rollback();
    }

    // THE PIN DIES WITH THE TRANSACTION: OUTSIDE IT, AND IN THE NEXT ONE, THE COUNT IS THE COMMITTED ONE
    assertThat(countStar()).isEqualTo(149);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(countStar()).isEqualTo(149);
      assertThat(scan()).isEqualTo(149);
    } finally {
      database.rollback();
    }
  }

  /**
   * The other order, which the javadoc names: a count pins the counter at once and a scan pins its pages only when it reads
   * them, so a commit landing between the two makes them disagree inside one REPEATABLE_READ transaction. Each of the two
   * repeats; neither is a snapshot (#10792).
   */
  @Test
  void aCountBeforeTheFirstScanPinsEarlierThanTheScan() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(countStar()).isEqualTo(100);
      foreignCommit();
      assertThat(scan()).as("the pages are first read after the commit").isEqualTo(149);
      assertThat(countStar()).as("the counter was pinned before it").isEqualTo(100);
      assertThat(scan()).as("the scan repeats").isEqualTo(149);
    } finally {
      database.rollback();
    }
  }

  /**
   * The context outlives its transaction and keeps its REPEATABLE_READ level: a count between two transactions must not pin
   * a value the next transaction would start from.
   */
  @Test
  void aCountBetweenTwoTransactionsPinsNothing() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(countStar()).isEqualTo(100);
    database.rollback();

    // OUTSIDE A TRANSACTION, ON THE SAME (NOW INACTIVE) CONTEXT
    assertThat(database.countType(TYPE, true)).isEqualTo(100);
    foreignCommit();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(countStar()).as("the new transaction counts from the committed state it starts in").isEqualTo(149);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aCountFollowsTheCommitsAtReadCommitted() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      assertThat(countStar()).isEqualTo(100);
      foreignCommit();
      assertThat(countStar()).isEqualTo(149);
      assertThat(database.countType(TYPE, true)).isEqualTo(149);
      assertThat(scan()).isEqualTo(149);
    } finally {
      database.rollback();
    }
  }

  /**
   * What the documentation offers a reader that needs one committed state of a whole type: an explicit lock on the type
   * keeps every writer of it out until the reading transaction ends, so no commit can land between two of its page reads.
   */
  @Test
  void lockTypeExcludesTheWritersOfAWholeTypeRead() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        database.command("sql", "LOCK TYPE " + TYPE);
        assertThat(scan()).isEqualTo(100);

        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final Thread writer = new Thread(() -> {
          try {
            database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET id = 500, v = 0"), false, 1);
          } catch (final Throwable e) {
            writerFailure.set(e);
          }
        });
        writer.start();
        writer.join(30_000);

        assertThat(writerFailure.get()).as("a writer of the locked type cannot commit while the read runs")
            .isInstanceOf(LockTimeoutException.class);
        assertThat(scan()).isEqualTo(100);
      } finally {
        database.rollback();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
    }
    assertThat(countStar()).isEqualTo(100);
  }

  /** The reporter's foreign commit, from another thread: 50 inserts, one update and one delete, 100 rows become 149. */
  private void foreignCommit() throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> {
          for (int i = 100; i < 150; i++)
            database.command("sql", "INSERT INTO " + TYPE + " SET id = ?, v = 0", i);
          database.command("sql", "UPDATE " + TYPE + " SET v = 1 WHERE id = 5");
          database.command("sql", "DELETE FROM " + TYPE + " WHERE id = 6");
        });
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    writer.start();
    writer.join(30_000);
    assertThat(failure.get()).isNull();
  }

  private long scan() {
    long n = 0;
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }

  private long countStar() {
    return count("SELECT count(*) AS c FROM " + TYPE);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
