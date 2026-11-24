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
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic form of the lost-update contract of {@code SET} and of the actions of a {@code MERGE} (issues #5227, #8538,
 * #4474): a transaction commits a change to the record after the statement read it and before the statement writes it, and the
 * statement writes a value computed from the COMMITTED record, not from the older image its row carries. The function
 * {@code race.interleave(x)}, called between the read and the write, commits the concurrent change from another thread and waits
 * for it, so the commit lands at the same point on every run: {@link CypherSetWriteTargetsTest} and
 * {@link CypherConcurrentWriteTest} reach the same window with threads and a race.
 * <p>
 * The record starts as {@code {k: 'a', n: 0, c: 0}}, the concurrent commit sets {@code n = 10}, and every statement writes
 * {@code c = n + 1}, so a result of 11 shows the write built on the concurrent commit, and 1 shows the lost update. The other
 * properties show that the write kept what the concurrent commit wrote.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class CypherSetBuildsOnConcurrentCommitTest extends TestHelper {
  private static final String CONCURRENT_COMMIT = "UPDATE T SET n = 10, m = 'concurrent' WHERE k = 'a'";

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE T; CREATE PROPERTY T.k STRING; CREATE INDEX ON T (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET k = 'a', n = 0, c = 0").close());
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

  /** What the statement of a case runs between its MATCH and its SET: it commits the concurrent change from another thread. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }

  @Test
  void aPropertySetBuildsOnTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET t.c = t.n + 1");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMapMergeBuildsOnTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET t += {c: t.n + 1}");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMapReplaceReadsTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET t = {k: t.k, c: t.n + 1}");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("n")).isNull();
    assertThat(property("m")).isNull();
  }

  @Test
  void aDynamicKeySetBuildsOnTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET t['c'] = t.n + 1");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void anExpressionTargetBuildsOnTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET (CASE WHEN t.n >= 0 THEN t END).c = t.n + 1");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aSecondAliasOfTheTargetReadsTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) WITH t, t AS u SET t.c = u.n + 1");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void everyItemOfOneClauseReadsTheCommitThatLandedAfterTheMatch() {
    runWithConcurrentCommit("MATCH (t:T {k: 'a'}) WHERE race.interleave(t) SET t.c = t.n + 1, t += {d: t.n + 2}, t.e = t.m");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(number("d")).isEqualTo(12L);
    assertThat(property("e")).isEqualTo("concurrent");
  }

  private Long number(final String name) {
    return ((Number) property(name)).longValue();
  }

  @Test
  void aMergeActionPropertyBuildsOnTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c.c = c.n + 1, c.h = race.interleave(c)");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMergeActionMapMergeBuildsOnTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c += {c: c.n + 1, h: race.interleave(c)}");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMergeActionMapReplaceReadsTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c = {k: c.k, c: c.n + 1, h: race.interleave(c)}");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("n")).isNull();
    assertThat(property("m")).isNull();
  }

  @Test
  void aMergeActionDynamicKeyBuildsOnTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c['c'] = c.n + 1, c.h = race.interleave(c)");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMergeActionExpressionTargetBuildsOnTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit(
        "MERGE (c:T {k: 'a'}) ON MATCH SET (CASE WHEN c.n >= 0 THEN c END).c = c.n + 1, c.h = race.interleave(c)");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMergeActionThatReadsASecondAliasBuildsOnTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MATCH (d:T {k: 'a'}) MERGE (c:T {k: 'a'}) ON MATCH SET c.c = d.n + 1, c.h = race.interleave(c)");

    assertThat(number("c")).isEqualTo(11L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  /** The price of #4474 is deliberate: a write that changes nothing is skipped, so the commit that landed meanwhile is not undone. */
  @Test
  void aMergeActionThatWritesTheValueTheRecordHoldsLeavesTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c.n = 0 + CASE WHEN race.interleave(c) THEN 0 ELSE 1 END");

    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  /**
   * The second alias is read before the commit, the record the MERGE matches after it: the value the action computes from the
   * alias must be computed from the record it writes (issue #8538), or it would equal what the record holds and the write would
   * be skipped as a no-op (issue #4474), losing the increment.
   */
  @Test
  void aMergeActionThatReadsASecondAliasReadBeforeTheCommitComputesFromTheMatchedRecord() {
    runWithConcurrentCommit("MATCH (d:T {k: 'a'}) WHERE race.interleave(d) MERGE (c:T {k: 'a'}) ON MATCH SET c.n = d.n + 10");

    assertThat(number("n")).isEqualTo(20L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  @Test
  void aMergeActionDynamicKeyThatWritesTheValueTheRecordHoldsLeavesTheCommitThatLandedAfterItsMatch() {
    runWithConcurrentCommit("MERGE (c:T {k: 'a'}) ON MATCH SET c['n'] = 0 + CASE WHEN race.interleave(c) THEN 0 ELSE 1 END");

    assertThat(number("n")).isEqualTo(10L);
    assertThat(property("m")).isEqualTo("concurrent");
  }

  private Object property(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT " + name + " AS value FROM T WHERE k = 'a'")) {
      return rs.next().getProperty("value");
    }
  }

  /** Runs {@code statement} in a READ_COMMITTED transaction, with the concurrent commit landing when it calls race.interleave. */
  private void runWithConcurrentCommit(final String statement) {
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    Race.PENDING.set(() -> {
      final Thread concurrent = new Thread(() -> {
        try {
          database.transaction(() -> database.command("sql", CONCURRENT_COMMIT).close());
        } catch (final Throwable t) {
          concurrentFailure.set(t);
        }
      });
      concurrent.start();
      boolean interrupted = false;
      while (concurrent.isAlive())
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      if (interrupted)
        Thread.currentThread().interrupt();
    });
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      database.command("opencypher", statement, Map.of()).close();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
    assertThat(Race.PENDING.get()).as("the concurrent commit must have been interleaved").isNull();
    assertThat(concurrentFailure.get()).isNull();
  }
}
