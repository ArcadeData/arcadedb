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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11835: a write racing a DDL on an index it uses answered with a client error or an internal error, while a retry of
 * the same request succeeds.
 * <ul>
 * <li>An UPDATE or DELETE reading through an index a concurrent {@code DROP INDEX} took away failed with the
 * {@link IndexException} a SELECT is planned again on. A write cannot be planned again halfway, so it reached the client as
 * an internal error (HTTP 500). It is now the retryable {@link ConcurrentModificationException} (HTTP 503), which
 * {@code Database.transaction(..., retries)} runs again.</li>
 * <li>{@code ALTER TYPE ... BUCKET +} inside a transaction (an HTTP command always runs in one) created the bucket's
 * sub-indexes in that transaction, so they were published to every writer before their first page was committed: a record
 * another transaction wrote into the new bucket and committed first failed with "the file is invalid" (HTTP 400). The
 * sub-indexes are now committed before the ALTER returns.</li>
 * </ul>
 * The function {@code race.interleave(row)}, called in the WHERE of the statement under test, runs the armed DDL from another
 * thread once, so it lands after the statement read the index and before it finished. The WHERE reads a range of the index: a
 * range read finds out at its end that the index was dropped under it, where a point lookup has already read every entry.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class Issue11835WriteRacingIndexDdlTest extends TestHelper {
  private static final String DROP_INDEX = "DROP INDEX `T[g]`";
  // A range of T[g] holding the five records with g = 1
  private static final String G_IS_1     = "g > 0 AND g < 2";

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE T BUCKETS 1; CREATE PROPERTY T.id STRING; CREATE INDEX ON T (id) UNIQUE;"
        + " CREATE PROPERTY T.g INTEGER; CREATE INDEX ON T (g) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO T SET id = ?, g = 1", "a" + i).close();
        database.command("sql", "INSERT INTO T SET id = ?, g = 3", "b" + i).close();
      }
    });
    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("race", Race.class));
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void endTest() {
    Race.PENDING.set(null);
    database.getSchema().unregisterFunctionLibrary("race");
  }

  @Test
  void sqlUpdateReadingAnIndexDroppedUnderItIsARetryableConflict() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);

    database.begin();
    try {
      assertThatThrownBy(
          () -> database.command("sql", "UPDATE T SET g = 2 WHERE " + G_IS_1 + " AND `race.interleave`(@rid)").close())
          .isInstanceOf(ConcurrentModificationException.class).hasCauseInstanceOf(StaleIndexException.class);
    } finally {
      database.rollback();
    }

    assertThat(Race.PENDING.get()).as("the DROP INDEX must have been interleaved").isNull();
    assertThat(ddlFailure.get()).isNull();
    assertThat(database.getSchema().existsIndex("T[g]")).isFalse();
    assertThat(countWhere("g = 1")).isEqualTo(5);
  }

  @Test
  void sqlDeleteReadingAnIndexDroppedUnderItIsARetryableConflict() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);

    database.begin();
    try {
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM T WHERE " + G_IS_1 + " AND `race.interleave`(@rid)").close())
          .isInstanceOf(ConcurrentModificationException.class).hasCauseInstanceOf(StaleIndexException.class);
    } finally {
      database.rollback();
    }

    assertThat(Race.PENDING.get()).isNull();
    assertThat(ddlFailure.get()).isNull();
    assertThat(countWhere("g = 1")).isEqualTo(5);
  }

  /**
   * The plan of the UPDATE picked the index, and the index was gone when the plan started to read it: the reported "is not
   * valid. Probably has been drop or rebuilt". A script plans all its statements before it runs the first, which puts the DROP
   * INDEX of its first statement in that window.
   */
  @Test
  void sqlUpdatePlannedOverAnIndexDroppedBeforeItRunsIsARetryableConflict() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);

    database.begin();
    try {
      assertThatThrownBy(() -> database.command("sqlscript", "SELECT `race.interleave`(1); UPDATE T SET g = 2 WHERE g = 1").close())
          .isInstanceOf(ConcurrentModificationException.class).hasCauseInstanceOf(StaleIndexException.class);
    } finally {
      database.rollback();
    }

    assertThat(Race.PENDING.get()).isNull();
    assertThat(ddlFailure.get()).isNull();
    assertThat(countWhere("g = 1")).isEqualTo(5);
  }

  /** The conflict is the one the retry loop runs again: the second attempt plans without the dropped index and writes once. */
  @Test
  void retriedSqlUpdateAppliesOnceWithoutTheDroppedIndex() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicLong updated = new AtomicLong(-1);

    database.transaction(() -> {
      attempts.incrementAndGet();
      try (final ResultSet rs = database.command("sql",
          "UPDATE T SET g = g + 10 WHERE " + G_IS_1 + " AND `race.interleave`(@rid)")) {
        updated.set(((Number) rs.next().getProperty("count")).longValue());
      }
    }, false, 3);

    assertThat(ddlFailure.get()).isNull();
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(updated.get()).isEqualTo(5);
    assertThat(countWhere("g = 1")).isZero();
    assertThat(countWhere("g = 11")).as("each record updated once").isEqualTo(5);
  }

  /**
   * openCypher SET over the same race: a MATCH that loses its index goes on with a scan, so the statement writes every record it
   * matched once, whether it looked the key up or read a range.
   */
  @Test
  void cypherSetOverAnIndexDroppedUnderItWritesEveryMatchOnce() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);

    database.transaction(() -> database.command("opencypher", "MATCH (t:T {g: 1}) WHERE race.interleave(t) SET t.g = t.g + 10",
        Map.of()).close(), false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(ddlFailure.get()).isNull();
    assertThat(countWhere("g = 1")).isZero();
    assertThat(countWhere("g = 11")).isEqualTo(5);
  }

  @Test
  void cypherSetOverARangeOfAnIndexDroppedUnderItWritesEveryMatchOnce() {
    final AtomicReference<Throwable> ddlFailure = armConcurrentCommand(DROP_INDEX);

    database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) WHERE t.g > 0 AND t.g < 2 AND race.interleave(t) SET t.g = t.g + 10", Map.of()).close(), false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(ddlFailure.get()).isNull();
    assertThat(countWhere("g = 1")).isZero();
    assertThat(countWhere("g = 11")).isEqualTo(5);
  }

  /**
   * The HTTP shape of the 400: the ALTER runs in the transaction of its request, and a writer that commits a record into the new
   * bucket before that transaction ends found the bucket's sub-indexes with no page.
   */
  @Test
  void sqlInsertIntoABucketAttachedInAnOpenTransactionCommits() {
    database.command("sql", "CREATE BUCKET b1").close();

    database.begin();
    try {
      database.command("sql", "ALTER TYPE T BUCKET +b1").close();
      final Throwable writerFailure = runOnAnotherThread(() -> database.transaction(
          () -> database.command("sql", "INSERT INTO BUCKET:b1 SET id = 'n1', g = 9").close()));
      assertThat(writerFailure).isNull();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(countWhere("g = 9")).isEqualTo(1);
    assertThat(indexEntries("T[g]", 9)).isEqualTo(1);
    assertThat(indexEntries("T[id]", "n1")).isEqualTo(1);
  }

  @Test
  void cypherCreateIntoABucketAttachedInAnOpenTransactionCommits() {
    database.command("sql", "CREATE BUCKET b1").close();

    database.begin();
    try {
      database.command("sql", "ALTER TYPE T BUCKET +b1").close();
      final Throwable writerFailure = runOnAnotherThread(() -> database.transaction(
          () -> database.command("opencypher", "UNWIND range(1, 4) AS i CREATE (:T {id: 'c' + i, g: 9})", Map.of()).close()));
      assertThat(writerFailure).isNull();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM BUCKET:b1")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("records the writer put in the new bucket").isPositive();
    }
    assertThat(countWhere("g = 9")).isEqualTo(4);
    assertThat(indexEntries("T[g]", 9)).isEqualTo(4);
  }

  /**
   * The sub-indexes are committed on their own, but their build still sees the records the caller wrote into the bucket and has
   * not committed: a bucket detached and attached again in one transaction (issue #6359).
   */
  @Test
  void bucketAttachedAgainInTheTransactionThatWroteIntoItIndexesTheseRecords() {
    database.command("sqlscript", "CREATE BUCKET b2; ALTER TYPE T BUCKET +b2").close();

    database.begin();
    try {
      database.command("sql", "INSERT INTO BUCKET:b2 SET id = 'p1', g = 11").close();
      database.command("sql", "ALTER TYPE T BUCKET -b2").close();
      database.command("sql", "ALTER TYPE T BUCKET +b2").close();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(countWhere("g = 11")).isEqualTo(1);
    assertThat(indexEntries("T[g]", 11)).isEqualTo(1);
    assertThat(indexEntries("T[id]", "p1")).isEqualTo(1);
  }

  /** Called in the WHERE of the statements under test: runs the armed command, once, and answers true. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable command = PENDING.getAndSet(null);
      if (command != null)
        command.run();
      return true;
    }
  }

  /** The next call of {@code race.interleave} runs {@code command} on another thread and waits for it. */
  private AtomicReference<Throwable> armConcurrentCommand(final String command) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    Race.PENDING.set(() -> failure.set(runOnAnotherThread(() -> database.command("sql", command).close())));
    return failure;
  }

  private static Throwable runOnAnotherThread(final Runnable body) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
    return failure.get();
  }

  private long countWhere(final String condition) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE " + condition)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long indexEntries(final String index, final Object key) {
    try (final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName(index)).get(new Object[] { key })) {
      long count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      return count;
    }
  }
}
