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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11818: a conditional update or delete was not atomic. When another transaction committed a change to a record after the
 * statement evaluated its WHERE on it and before the statement wrote it, the statement wrote it anyway: {@code modify()} reloads the
 * latest committed content (and pins the page the commit is checked against), SQL UPDATE, openCypher SET and REMOVE build on it
 * (issues #5227, #8538, #8610), and a DELETE pinned the page only when it deleted, but the WHERE that selected the record was never
 * evaluated again. Two concurrent {@code UPDATE R SET claimed = :me WHERE k = :k AND claimed IS NULL} both claimed the record and
 * both committed, and {@code DELETE FROM R WHERE k = :k AND claimed IS NULL} deleted a record claimed in between.
 * <p>
 * Each case runs the statement in a READ_COMMITTED transaction of the test thread; the function {@code race.interleave(row)},
 * called last in its WHERE, commits the concurrent claim from another thread, so the commit lands after the WHERE read the record
 * and before the write. The concurrent claim must survive: the statement writes nothing, or is refused with a retryable
 * conflict that a retry turns into "writes nothing".
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class Issue11818ConditionalUpdateTest extends TestHelper {
  private static final String CONCURRENT_CLAIM = "UPDATE R SET claimed = 'A' WHERE k = 'r0' AND claimed IS NULL";
  private static final String SQL_CLAIM        = "UPDATE R SET claimed = :who WHERE k = :k AND claimed IS NULL";
  private static final String CYPHER_CLAIM     = "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL SET r.claimed = $who RETURN r.k AS k";

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE R; CREATE PROPERTY R.k STRING; CREATE INDEX ON R (k) UNIQUE; CREATE DOCUMENT TYPE D; "
        + "CREATE DOCUMENT TYPE DI; CREATE PROPERTY DI.k STRING; CREATE INDEX ON DI (k) UNIQUE; CREATE EDGE TYPE E");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO R SET k = 'r0', n = 0, state = 'open'").close();
      database.command("sql", "INSERT INTO D SET k = 'd0', n = 0").close();
      database.command("sql", "INSERT INTO DI SET k = 'i0', n = 0").close();
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

  /**
   * The reported shape: the record is found through the unique index, and a vertex is reloaded by {@code modify()} every time.
   */
  @Test
  void sqlUpdateDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE R SET claimed = :who WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "r0", "who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).as("the record no longer matches the WHERE").isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void sqlUpdateDoesNotClaimAScannedVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE R SET claimed = :who WHERE n = 0 AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * A document read by a scan is reloaded by {@code modify()} when its page moved on (issue #8312), so it lost the claim the same way.
   */
  @Test
  void sqlUpdateDoesNotClaimAScannedDocumentClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE D SET claimed = 'A' WHERE k = 'd0' AND claimed IS NULL",
        () -> updated.set(countOf(database.command("sql",
            "UPDATE D SET claimed = :who WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "d0", "who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(valueOf("D", "claimed")).isEqualTo("A");
  }

  /**
   * #8610 must survive the fix: a WHERE that still holds on the reloaded record lets the UPDATE build on the concurrent commit.
   */
  @Test
  void sqlUpdateWhoseWhereStillHoldsBuildsOnTheConcurrentCommit() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> updated.set(countOf(database.command(
        "sql", "UPDATE R SET n = n + 1 WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isEqualTo(1);
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(6);
  }

  /**
   * An UPSERT that loses the record it found would neither update nor insert: it is refused with a retryable conflict instead.
   */
  @Test
  void sqlUpsertOfAVertexClaimedAfterItsWhereIsRefused() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("sql",
        "UPDATE R SET claimed = 'B' UPSERT WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)").close());

    assertThat(committed).isFalse();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The reported openCypher shape. A conflict is the only answer a SET can give once earlier clauses may have written for the row,
   * and the retry runs the statement against the new state (see {@link #retriedCypherClaimFindsTheRecordClaimed()}).
   */
  @Test
  void cypherSetDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> rows.set(countRows(database.command("opencypher",
        "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.claimed = $who RETURN r.k AS k",
        Map.of("k", "r0", "who", "B")))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The job-queue claim: the WHERE is in front of a WITH that passes the variable on.
   */
  @Test
  void cypherSetAfterAWithDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> rows.set(countRows(database.command("opencypher",
        "MATCH (j:R) WHERE j.claimed IS NULL AND race.interleave(j) WITH j LIMIT 1 SET j.claimed = $who RETURN j.k AS k",
        Map.of("who", "B")))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The inline property map of the pattern selects the record as much as the WHERE does.
   */
  @Test
  void cypherSetDoesNotWriteAVertexWhoseInlinePropertiesChangedAfterTheMatch() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> rows.set(countRows(
        database.command("opencypher", "MATCH (r:R {k: 'r0', state: 'open'}) WHERE race.interleave(r) SET r.claimed = 'B' RETURN r.k",
            Map.of()))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isNull();
    assertThat(valueOf("R", "state")).isEqualTo("closed");
  }

  /**
   * #5227 and #8538 must survive the fix: a SET whose predicates still hold on the reloaded record builds on the concurrent commit.
   */
  @Test
  void cypherSetWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.n = r.n + 1", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(6);
  }

  /**
   * A REMOVE builds on a concurrent commit as a SET does, so its predicates are checked the same way: releasing a lease the MATCH
   * found held by B must not release the lease A took over in between.
   */
  @Test
  void cypherRemoveDoesNotReleaseALeaseTakenOverAfterItsWhere() {
    database.transaction(() -> database.command("sql", "UPDATE R SET owner = 'B' WHERE k = 'r0'").close());
    runWithConcurrentCommit("UPDATE R SET owner = 'A' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race.interleave(r) REMOVE r.owner", Map.of()).close());

    assertThat(valueOf("R", "owner")).isEqualTo("A");
  }

  @Test
  void cypherRemoveWhosePredicatesStillHoldCommitsOverTheConcurrentCommit() {
    database.transaction(() -> database.command("sql", "UPDATE R SET owner = 'B' WHERE k = 'r0'").close());
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race.interleave(r) REMOVE r.owner", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(valueOf("R", "owner")).isNull();
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(5);
  }

  /**
   * A DELETE pins the record's page only when it deletes it, so a claim committed after its WHERE read the record was not seen: the
   * claimed record was deleted. Like an UPDATE, the DELETE now evaluates its WHERE again and leaves the record alone.
   */
  @Test
  void sqlDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    final AtomicLong deleted = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> deleted.set(countOf(database.command("sql",
        "DELETE FROM R WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "r0")))));

    assertThat(committed).isTrue();
    assertThat(deleted.get()).as("the record no longer matches the WHERE").isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void sqlDeleteDoesNotDeleteAScannedDocumentClaimedAfterItsWhere() {
    final AtomicLong deleted = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE D SET claimed = 'A' WHERE k = 'd0' AND claimed IS NULL",
        () -> deleted.set(countOf(database.command("sql", "DELETE FROM D WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)",
            Map.of("k", "d0")))));

    assertThat(committed).isTrue();
    assertThat(deleted.get()).isZero();
    assertThat(valueOf("D", "claimed")).isEqualTo("A");
  }

  /**
   * A document found through an index is not reloaded by {@code modify()}, but its delete was not checked against the image read
   * either: it was deleted all the same.
   */
  @Test
  void sqlDeleteDoesNotDeleteAnIndexedDocumentClaimedAfterItsWhere() {
    final AtomicLong deleted = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET claimed = 'A' WHERE k = 'i0' AND claimed IS NULL",
        () -> deleted.set(countOf(database.command("sql", "DELETE FROM DI WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)",
            Map.of("k", "i0")))));

    assertThat(committed).isTrue();
    assertThat(deleted.get()).isZero();
    assertThat(valueOf("DI", "claimed")).isEqualTo("A");
  }

  /**
   * A WHERE that still holds deletes the record as it is now, and so removes the index entries of that content: deleting the content
   * read left the entry of the key committed in between pointing to the deleted record (the integrity check of the fixture finds it).
   */
  @Test
  void sqlDeleteWhoseWhereStillHoldsRemovesTheIndexEntriesOfTheCurrentContent() {
    final AtomicLong deleted = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> deleted.set(countOf(
        database.command("sql", "DELETE FROM DI WHERE n = 0 AND `race.interleave`(@rid)"))));

    assertThat(committed).isTrue();
    assertThat(deleted.get()).isEqualTo(1);
    assertThat(recordsOf("DI")).isZero();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM index:`DI[k]`")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("index entries left").isZero();
    }
  }

  @Test
  void cypherDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DELETE r", Map.of()).close());

    assertThat(recordsOf("R")).isEqualTo(1);
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void cypherDetachDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) CREATE (r)-[:E]->(:R {k: 'r1'})").close());
    runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DETACH DELETE r", Map.of()).close());

    assertThat(recordsOf("R")).isEqualTo(2);
    assertThat(recordsOf("E")).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT claimed FROM R WHERE k = 'r0'")) {
      assertThat(rs.next().<String>getProperty("claimed")).isEqualTo("A");
    }
  }

  @Test
  void cypherDeleteWhosePredicatesStillHoldDeletesOverTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DELETE r", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(recordsOf("R")).isZero();
  }

  /**
   * A record deleted concurrently between the read and the delete is still the retryable conflict of #8984: reloading a vertex that
   * is gone must not leave it without content, which turned the conflict into a "record not found" no retry handles.
   */
  @Test
  void sqlDeleteOfAVertexDeletedAfterItsWhereIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("DELETE FROM R WHERE k = 'r0'", () -> database.command("sql",
        "DELETE FROM R WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)").close());

    assertThat(committed).isFalse();
    assertThat(recordsOf("R")).isZero();
  }

  /**
   * The conflict is retryable: the retry the auto-commit path runs finds the record claimed and claims nothing.
   */
  @Test
  void retriedCypherClaimFindsTheRecordClaimed() {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(CONCURRENT_CLAIM);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicLong rows = new AtomicLong(-1);
    database.transaction(() -> {
      attempts.incrementAndGet();
      rows.set(countRows(database.command("opencypher",
          "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.claimed = $who RETURN r.k AS k",
          Map.of("k", "r0", "who", "B"))));
    }, false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(concurrentFailure.get()).isNull();
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The reporter's shape, without forcing the interleaving: several claims of each record at once, each in an auto-retried
   * transaction. Exactly one of them claims the record, and the record holds its claim.
   */
  @Test
  void concurrentSqlClaimsHaveOneWinner() throws Exception {
    assertOneWinnerPerRecord("sql", SQL_CLAIM);
  }

  @Test
  void concurrentCypherClaimsHaveOneWinner() throws Exception {
    assertOneWinnerPerRecord("opencypher", CYPHER_CLAIM);
  }

  /** Called last in the WHERE of the statements under test: runs the armed concurrent commit, once, and answers true. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }

  private void assertOneWinnerPerRecord(final String language, final String claim) throws Exception {
    final int records = 40;
    final int claimants = 4;
    database.transaction(() -> {
      for (int i = 1; i <= records; i++)
        database.command("sql", "INSERT INTO R SET k = ?", "c" + i).close();
    });

    for (int i = 1; i <= records; i++) {
      final String key = "c" + i;
      final CyclicBarrier start = new CyclicBarrier(claimants);
      final List<String> winners = new ArrayList<>();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final List<Thread> threads = new ArrayList<>();
      for (int c = 0; c < claimants; c++) {
        final String who = "w" + c;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            final AtomicLong claimed = new AtomicLong();
            database.transaction(() -> {
              try (final ResultSet rs = database.command(language, claim, Map.of("k", key, "who", who))) {
                claimed.set(language.equals("sql") ? ((Number) rs.next().getProperty("count")).longValue() : rs.stream().count());
              }
            }, false, 50);
            if (claimed.get() > 0)
              synchronized (winners) {
                winners.add(who);
              }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
          }
        });
        threads.add(thread);
        thread.start();
      }
      for (final Thread thread : threads)
        thread.join();

      assertThat(failure.get()).isNull();
      assertThat(winners).as("claims of %s that committed", key).hasSize(1);
      try (final ResultSet rs = database.query("sql", "SELECT claimed FROM R WHERE k = ?", key)) {
        assertThat(rs.next().<String>getProperty("claimed")).isEqualTo(winners.getFirst());
      }
    }
  }

  private long recordsOf(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private Object valueOf(final String type, final String property) {
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS value FROM " + type)) {
      return rs.next().getProperty("value");
    }
  }

  private static long countOf(final ResultSet rs) {
    try (rs) {
      return ((Number) rs.next().getProperty("count")).longValue();
    }
  }

  private static long countRows(final ResultSet rs) {
    try (rs) {
      return rs.stream().count();
    }
  }

  /** The next call of {@code race.interleave} commits {@code concurrentCommand} from another thread and waits for it. */
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
   * calls {@code race.interleave}.
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
