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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12593: the write shapes #11818 left out. A guarded write (a SET, a REMOVE or a label write whose target a MATCH ... WHERE
 * selected) that a concurrent commit lands under, between the read that selected the record and the write, must evaluate the
 * predicates that selected it again on the content that commit left, and give up the write when one no longer holds: one claim
 * wins, the other is refused with a retryable conflict and its retry finds the record claimed.
 * <p>
 * Same harness as {@code Issue11818ConditionalUpdateTest}: the statement runs in a READ_COMMITTED transaction of the test thread,
 * and the function {@code race12593.interleave(x)}, called after the read that selected the record, commits the concurrent change
 * from another thread, so the commit lands after that read and before the write.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class Issue12593GuardedWriteShapesTest extends TestHelper {
  private static final String CONCURRENT_CLAIM = "UPDATE R SET claimed = 'A' WHERE k = 'r0' AND claimed IS NULL";

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE R; CREATE PROPERTY R.k STRING; CREATE INDEX ON R (k) UNIQUE; CREATE EDGE TYPE E");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET k = 'r0', n = 0, state = 'open'").close());
    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("race12593", Race.class));
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void endTest() {
    Race.PENDING.set(null);
    database.getSchema().unregisterFunctionLibrary("race12593");
  }

  // ---------------------------------------------------------------- label writes

  /** {@code SET n:Label} rewrites the vertex from its latest committed content: the claim committed in between is a predicate miss. */
  @Test
  void cypherLabelSetDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) SET r:Claimed", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(labelsOfR0()).containsExactly("R");
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** And a label write whose predicates still hold builds on the concurrent commit, as a property SET does (#5227, #8538). */
  @Test
  void cypherLabelSetWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) SET r:Claimed", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(labelsOfR0()).containsExactlyInAnyOrder("R", "Claimed");
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(5);
  }

  /** {@code REMOVE n:Label} releasing a lease the MATCH found held by B must not release the lease A took over in between. */
  @Test
  void cypherLabelRemoveDoesNotReleaseALeaseTakenOverAfterItsWhere() {
    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) SET r:Lease, r.owner = 'B'").close());

    final boolean committed = runWithConcurrentCommit("UPDATE R SET owner = 'A' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race12593.interleave(r) REMOVE r:Lease", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(labelsOfR0()).containsExactlyInAnyOrder("R", "Lease");
    assertThat(cypherValue("owner")).isEqualTo("A");
  }

  // ---------------------------------------------------------------- MERGE actions

  /** {@code ON MATCH SET} writes a record the MATCH before the MERGE selected: its WHERE is evaluated again on a reload. */
  @Test
  void cypherMergeOnMatchSetDoesNotClaimAVertexClaimedAfterTheMatchBeforeIt() {
    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) CREATE (r)-[:E]->(:R {k: 'r1'})").close());

    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) MERGE (r)-[:E]->(b:R {k: 'r1'}) "
            + "ON MATCH SET r.claimed = 'B'", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /**
   * The pattern of the MERGE selected the record it matched as much as a MATCH does: a concurrent commit that changed what its
   * inline map reads leaves a record the MERGE no longer matches, and {@code ON MATCH SET} must not write it.
   */
  @Test
  void cypherMergeOnMatchSetDoesNotWriteAVertexItsPatternNoLongerMatches() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> database.command(
        "opencypher", "MERGE (r:R {k: 'r0', state: 'open'}) ON MATCH SET r.claimed = CASE WHEN race12593.interleave(r) THEN 'B' END",
        Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isNull();
    assertThat(cypherValue("state")).isEqualTo("closed");
  }

  /** The guard: an ON MATCH SET whose predicates still hold builds on the concurrent commit (#8538). */
  @Test
  void cypherMergeOnMatchSetWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (x:R {k: 'r0'}) WHERE x.claimed IS NULL AND race12593.interleave(x) MERGE (r:R {k: 'r0', state: 'open'}) "
            + "ON MATCH SET r.n = r.n + 1", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(6);
  }

  /** A SET in a later clause writes what the pattern of the MERGE matched: checked the same way. */
  @Test
  void cypherSetAfterAMergeDoesNotWriteAVertexItsPatternNoLongerMatches() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> database.command(
        "opencypher", "MERGE (r:R {k: 'r0', state: 'open'}) WITH r WHERE race12593.interleave(r) SET r.claimed = 'B'",
        Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isNull();
  }

  // ---------------------------------------------------------------- FOREACH and CALL { } bodies

  @Test
  void cypherSetInsideForeachDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) FOREACH (i IN [1] | SET r.claimed = 'B')",
        Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  @Test
  void cypherRemoveInsideNestedForeachDoesNotReleaseALeaseTakenOverAfterItsWhere() {
    database.transaction(() -> database.command("sql", "UPDATE R SET owner = 'B' WHERE k = 'r0'").close());

    final boolean committed = runWithConcurrentCommit("UPDATE R SET owner = 'A' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race12593.interleave(r) FOREACH (i IN [1] | FOREACH (j IN [2] | REMOVE r.owner))",
        Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("owner")).isEqualTo("A");
  }

  /** A FOREACH variable that shadows nothing of the predicates leaves them in force; the guard: they still hold, so it builds. */
  @Test
  void cypherSetInsideForeachWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) FOREACH (i IN [1] | SET r.n = r.n + i)",
        Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(6);
  }

  @Test
  void cypherSetInsideACallBodyWithAnExplicitScopeDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) CALL (r) { SET r.claimed = 'B' }", Map.of())
        .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  @Test
  void cypherSetInsideACallBodyWithAnImportingWithDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) CALL { WITH r SET r.claimed = 'B' }", Map.of())
        .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  @Test
  void cypherLabelSetInsideACallBodyDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) CALL (*) { SET r:Claimed }", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(labelsOfR0()).containsExactly("R");
  }

  /** The guard: the body's predicates still hold on the reloaded record, so the body builds on the concurrent commit. */
  @Test
  void cypherSetInsideACallBodyWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) CALL (r) { SET r.n = r.n + 1 }", Map.of())
        .close());

    assertThat(committed).isTrue();
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(6);
  }

  /** A body that imports nothing cannot read the outer predicates, and does not write what they selected: nothing is checked. */
  @Test
  void cypherSetInsideACallBodyThatImportsNothingIsNotCheckedAgainstTheOuterPredicates() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET claimed = 'A' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) CALL () { MATCH (x:R {k: 'r0'}) SET x.n = 9 }",
        Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(9);
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  // ---------------------------------------------------------------- a WITH that renames the variable

  @Test
  void cypherSetBehindAWithThatRenamesTheVariableDoesNotClaimAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) WITH r AS x SET x.claimed = 'B'", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** The inline map follows the rename too, as does a WITH that renames twice and binds the old name to something else. */
  @Test
  void cypherSetBehindWithsThatRenameAndRebindTheVariableChecksItsInlineMap() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> database.command(
        "opencypher", "MATCH (r:R {k: 'r0', state: 'open'}) WHERE race12593.interleave(r) WITH r AS x WITH x AS y, 'other' AS r "
            + "SET y.claimed = 'B'", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(cypherValue("claimed")).isNull();
  }

  /** The guard: renamed predicates that still hold let the write build on the concurrent commit. */
  @Test
  void cypherSetBehindAWithThatRenamesTheVariableWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0', state: 'open'}) WHERE r.claimed IS NULL AND race12593.interleave(r) WITH r AS x, r.n AS old "
            + "SET x.n = x.n + 1", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(6);
  }

  /** The conflict is retryable: the auto-commit retry runs the statement against the new state and claims nothing. */
  @Test
  void retriedRenamedCypherClaimFindsTheRecordClaimed() {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(CONCURRENT_CLAIM);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicLong rows = new AtomicLong(-1);
    database.transaction(() -> {
      attempts.incrementAndGet();
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race12593.interleave(r) WITH r AS x SET x.claimed = 'B' RETURN x.k",
          Map.of())) {
        rows.set(rs.stream().count());
      }
    }, false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(concurrentFailure.get()).isNull();
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(rows.get()).isZero();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  // ---------------------------------------------------------------- SQL source that is a projection carrying @rid

  @Test
  void sqlUpdateOfAProjectedRidSourceDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE (SELECT @rid, claimed FROM R WHERE k = 'r0') SET claimed = 'B' WHERE claimed IS NULL AND `race12593.interleave`(@rid)"))));

    assertThat(committed).as("the record is skipped, as UPDATE R ... WHERE skips it").isTrue();
    assertThat(updated.get()).isZero();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** The WHERE reads a column the projection renamed: it is evaluated against the row made again from the record. */
  @Test
  void sqlUpdateOfAProjectedRidSourceWithAnAliasedColumnDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE (SELECT @rid, claimed AS c FROM R WHERE k = 'r0') SET claimed = 'B' WHERE c IS NULL AND `race12593.interleave`(@rid)"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** The WHERE of the SELECT selected the record as much as the WHERE of the UPDATE does. */
  @Test
  void sqlUpdateOfAProjectedRidSourceChecksTheWhereOfTheSelectAgain() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE (SELECT @rid FROM R WHERE claimed IS NULL AND `race12593.interleave`(@rid)) SET claimed = 'B'"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** And so it does for a SELECT that answers the records themselves, read before the concurrent commit. */
  @Test
  void sqlUpdateOfASelectSourceOfRecordsChecksTheWhereOfTheSelectAgain() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE (SELECT FROM R WHERE claimed IS NULL AND `race12593.interleave`(@rid)) SET claimed = 'B'"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  /** The guard: predicates that still hold on the record as the write finds it let the UPDATE build on the concurrent commit. */
  @Test
  void sqlUpdateOfAProjectedRidSourceWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> updated.set(countOf(database.command(
        "sql", "UPDATE (SELECT @rid, claimed FROM R WHERE k = 'r0') SET n = n + 1 WHERE claimed IS NULL AND `race12593.interleave`(@rid)"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isEqualTo(1);
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(6);
  }

  /** A source the row cannot be made again from (a SELECT over a SELECT) keeps its rows as they were read: the UPDATE still writes. */
  @Test
  void sqlUpdateOfASourceThatCannotBeMadeAgainStillWrites() {
    final AtomicLong updated = new AtomicLong(-1);
    database.transaction(() -> updated.set(countOf(database.command("sql",
        "UPDATE (SELECT @rid, claimed FROM (SELECT FROM R WHERE k = 'r0')) SET n = 3 WHERE claimed IS NULL"))));

    assertThat(updated.get()).isEqualTo(1);
    assertThat(((Number) cypherValue("n")).intValue()).isEqualTo(3);
  }

  @Test
  void sqlDeleteOfAProjectedRidSourceWhosePredicatesStillHoldDeletes() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("sql",
        "DELETE FROM (SELECT @rid, claimed FROM R WHERE k = 'r0') WHERE claimed IS NULL AND `race12593.interleave`(@rid)").close());

    assertThat(committed).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM R")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isZero();
    }
  }

  @Test
  void sqlDeleteOfAProjectedRidSourceDoesNotDeleteAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("sql",
        "DELETE FROM (SELECT @rid, claimed FROM R WHERE k = 'r0') WHERE claimed IS NULL AND `race12593.interleave`(@rid)").close());

    assertThat(committed).isTrue();
    assertThat(cypherValue("claimed")).isEqualTo("A");
  }

  // ---------------------------------------------------------------- helpers

  /** Called after the read that selected the record: runs the armed concurrent commit, once, and answers true. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }

  private List<String> labelsOfR0() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (r {k: 'r0'}) RETURN labels(r) AS labels")) {
      final Result row = rs.next();
      assertThat(rs.hasNext()).as("one vertex holds k = r0").isFalse();
      return row.getProperty("labels");
    }
  }

  private static long countOf(final ResultSet rs) {
    try (rs) {
      return ((Number) rs.next().getProperty("count")).longValue();
    }
  }

  private Object cypherValue(final String property) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (r {k: 'r0'}) RETURN r." + property + " AS value")) {
      return rs.next().getProperty("value");
    }
  }

  /** The next call of {@code race12593.interleave} commits {@code concurrentCommand} from another thread and waits for it. */
  private AtomicReference<Throwable> armConcurrentCommit(final String concurrentCommand) {
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    Race.PENDING.set(() -> {
      final Thread concurrent = new Thread(() -> {
        try {
          database.transaction(() -> database.command("sql", concurrentCommand).close());
        } catch (final Throwable t) {
          concurrentFailure.set(t);
        }
      });
      concurrent.start();
      try {
        concurrent.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    return concurrentFailure;
  }

  /**
   * Runs {@code body} in a READ_COMMITTED transaction, with {@code concurrentCommand} committed from another thread when the body
   * calls {@code race12593.interleave}.
   *
   * @return whether the transaction committed; false when it was refused with a retryable conflict
   */
  private boolean runWithConcurrentCommit(final String concurrentCommand, final Runnable body) {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(concurrentCommand);
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      body.run();
      database.commit();
      return true;
    } catch (final RuntimeException e) {
      // a query engine may report the conflict wrapped in its own exception
      for (Throwable t = e; t != null; t = t.getCause())
        if (t instanceof ConcurrentModificationException)
          return false;
      throw e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      assertThat(Race.PENDING.get()).as("the concurrent commit must have been interleaved").isNull();
      assertThat(concurrentFailure.get()).isNull();
    }
  }
}
